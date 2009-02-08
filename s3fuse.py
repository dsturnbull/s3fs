#!/usr/bin/env python2.5
# Copyright (C) 2008 TILEFILE Limited
# All rights reserved.
# $Id: s3fuse.py 21937 2008-04-14 06:10:46Z davidt $
"""
Mounts S3 as a filesystem, to mountpoint and daemonizes.
"""

# ignore complaint about '*args **kwargs' magic.. :P
# pylint: disable-msg=W0142

from __future__ import with_statement
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

COMMON = os.path.abspath(os.path.join(ROOT, 'common'))
sys.path.insert(0, COMMON)

import re
import uuid
import errno
import fuse
import time
import stat
import threading
import logging
import statvfs

from common import shared
from common import boto
from common import constants as C

fuse.fuse_python_api = (0, 2)
mountPoint = shared.config.get(C.streamer, 'data-path')

# s3 connection
s3conn = boto.connect_s3(
    shared.config.get(C.main, C.tmsAccessKey),
    shared.config.get(C.main, C.tmsPrivateKey)
)

# s3 lock
s3lock = threading.Lock()

# main tms-media bucket, just in case
s3buckets = { 'tms-media': \
    s3conn.get_bucket(shared.config.get(C.s3, C.mediaBucket))
}

# tms-mediaN buckets
for i in xrange(10):
    b = shared.config.get(C.s3, C.mediaBucket) + str(i)
    s3buckets[b] = \
        s3conn.get_bucket(b)

_log = logging.getLogger('tms.streamer.fuse')

class S3Stat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 1
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0
        self.st_blocks = 0
        self.st_blksize = 0
        self.st_rdev = 0
        
class S3Fuse(fuse.Fuse):
    """
    Main S3Fuse class.
    """

    def __init__(self, *args, **kw):
        """
        Init main S3Fuse class.
        """

        self.file_class = None
        fuse.Fuse.__init__(self, *args, **kw)

    def getattr(self, path):
        """
        os.lstat wrapper. lstat because we don't want to make symlinks
        appear to be actual files.
        """

        _log.debug('getattr: %s' % path)

        try:
            if '.' not in path:
                # if we don't detect an extension i.e. format, it is a dir
                st = S3Stat()
                st.st_mode = stat.S_IFDIR | 0755
                return st
            else:
                # it's a file, set st_nlink and st_size
                parts = path.split('/')
                bucket = parts[1]
                key = '/'.join(parts[2:])

                _log.debug('%s in %s' % (key, bucket))
                try:
                    k = s3buckets[bucket].get_key(key)

                    st = S3Stat()
                    st.st_mode = stat.S_IFREG | 0444
                    st.st_nlink = 1
                    st.st_size = int(k.size)
                except Exception:
                    _log.error(sys.exc_info()[1])

                return st
        except:
            return -errno.ENOENT

    def readdir(self, path, offset):
        """
        Return files in dir, add in '.' and '..'.
        """

        _log.debug('readdir %s %s' % (path, offset))

        contents = {}

        if path == '/':
            # just list media buckets
            for bucket_name, bucket in s3buckets.iteritems():
                contents[bucket_name] = fuse.Direntry(bucket_name)
        elif os.path.basename(path) in s3buckets.keys():
            # list subdirs of media
            dirs = {}
            for k in s3buckets[os.path.basename(path)].list():
                dirs[k.name.split('/')[0]] = 1
            for i in dirs.keys():
                contents[i] = fuse.Direntry(i.encode('ascii'))
        else:
            # list files in format dir
            bucket = path.split('/')[1]
            prefix = '/'.join(path.split('/')[2:])
            for k in s3buckets[bucket].list(prefix=prefix):
                p = re.compile('^%s/' % prefix)
                f = p.sub('', k.name).split('/')[0]
                contents[f] = fuse.Direntry(f.encode('ascii'))

        contents['.'] = fuse.Direntry('.')
        contents['..'] = fuse.Direntry('..')

        for dirFile, entry in contents.iteritems():
            yield entry

    def unlink(self, path):
        """
        Unlink file. unimplemented
        """
        return

        _log.debug('unlink %s' % path)
        for node in unfsNodes:
            newPath = node + path
            try:
                os.unlink(newPath)
                _log.critical('del file: %s' % newPath)
            except OSError, why:
                _log.debug('unlink %s failed: %s' % (newPath, why))

    def rmdir(self, path):
        """
        rmdir. Removes dir on all nodes. unimplemented
        """
        return

        # FIXME: this will succeed on some nodes that don't have files yet...
        
        _log.debug('rmdir %s' % path)
        for node in unfsNodes:
            newPath = node + path
            try:
                os.rmdir(newPath)
            except OSError, why:
                _log.debug('rmdir %s failed: %s' % (newPath, why))

    def access(self, path, mode):
        """
        Check for access on a file with specific mode.
        Do nothing for success.
        We assume we can read everything in tms-media buckets
        """

        #return -errno.EACCES

    def statfs(self):
        """
        Returns info for use by 'df' etc.
        Basically we add up all the addable values from underlying node statfs
        calls, making sure to convert them into equal sized blocks. unimpl
        """

        _log.debug('statfs')
        my_bsize = 4096

        st = fuse.StatVfs()
        st.f_bsize = my_bsize
        st.f_frsize = my_bsize
        st.f_blocks = 0
        st.f_bfree = 0
        st.f_bavail = 0
        st.f_files = 0
        st.f_ffree = 0

        return st

    class S3File(object):
        """
        File object for S3Fuse.
        Handles opening, reading and fstat.
        """

        def findInBucket(self, path):
            """
            Return boto Key object
            """
            
            parts = path.split('/')
            bucket = parts[1]
            key = '/'.join(parts[2:])
            _log.debug('found %s in %s' % (key, bucket))
            try:
                k = s3buckets[bucket].get_key(key)
                return k
            except Exception:
                _log.error(sys.exc_info()[1])
                return None

        def download(self, src, dst):
            """
            Download remote file to dst
            """
            
            _log.debug('downloading %s to %s (%s bytes)' % (
                    src.name, dst, src.size
                )
            )
            src.get_contents_to_filename(dst) 

        def __init__(self, path, flags, *mode):
            """
            Initialise new file object.
            Download the file to ./tmp/s3/ before returning.
            """

            _log.debug('opening %s' % path)
            self.path = os.path.join(ROOT, 'tmp', 's3', path.replace('/', '_'))

            if not os.path.exists(self.path):
                _log.debug('remote fetch')
                self.k = self.findInBucket(path)
                _log.debug('got key: %s' % self.k)

                self.download(self.k, self.path)

            self.file = open(self.path, 'r')
            self.fd = self.file.fileno()

        def read(self, length, offset):
            """
            Read from self.file
            """

            self.file.seek(offset)
            return self.file.read(length)

        def release(self, flags):
            """
            Close file.
            """

            self.file.close()

        def fgetattr(self):
            """
            os.fstat wrapper for current file descriptor.
            """

            _log.debug('fgetattr on %s' % self.fd)
            return os.fstat(self.fd)

        def lock(self, cmd, owner, **kw):
            """
            Lock file? Dunno.
            """

            _log.debug('lock: %s %s %s' % (cmd, owner, kw))
            return -errno.EINVAL

    # ignore args differ from overridden method since we call it directly.
    # also ignore 'magic' complaint
    # pylint: disable-msg=W0221
    def main(self, *a, **kw):
        """
        Main entry point. Mounts filesystem and daemonizes.
        """

        self.file_class = self.S3File
        return fuse.Fuse.main(self, *a, **kw)
    # pylint: enable-msg=W0221

if __name__ == '__main__':
    usage = ""

    server = S3Fuse(version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()
