from __future__ import absolute_import
import os

class RandomReadFile(object):
    def __init__(self, storage_backend, filename):
        self._offset = 0
        self._f = storage_backend.make_random_read_file(filename)
        self._size = self._f.get_size()

    def _close_guard(self):
        if self._f is None:
            raise ValueError

    def _read_advance(self, size):
        s = self._f.read_offset(self._offset, size)
        self._offset += size
        return s

    def close(self):
        self._f = None

    def flush(self):
        raise NotImplementedError

    def next(self):
        raise NotImplementedError

    def read(self, size=-1):
        self._close_guard()
        if size <= 0 or size is None or self._offset + size > self._size:
            size = self._size - self._offset
        return self._read_advance(size)

    def readline(self, size=-1):
        raise NotImplementedError

    def readlines(self, sizehint=-1):
        raise NotImplementedError

    def seek(self, offset, whence=0):
        self._close_guard()
        if whence == 0:
            self._offset = offset
        elif whence == 1:
            self._offset += offset
            pass
        elif whence == 2:
            self._offset = self._size + offset
        else:
            raise ValueError

    def tell(self):
        return self._offset

    def truncate(self, size=-1):
        raise NotImplementedError

    def write(self, s):
        raise NotImplementedError

    def writelines(self, sequence):
        raise NotImplementedError
