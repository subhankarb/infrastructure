import os.path


class LocalFileHandler(object):
    def __init__(self, file_dir, filename, arc_ext=".gz"):
        self.file_dir = file_dir
        self.filename = filename
        self.arc_ext = arc_ext
        self.fh = None

        if not self.exists():
            raise RuntimeError("{} dir does not exist".format(self.file_dir))

    def open(self, mode="r"):
        self.fh = open(self.full_path, mode)
        return self.fh

    def close(self):
        self.fh.close()

    @property
    def full_path(self):
        return os.path.join(self.file_dir, self.filename)

    @property
    def full_arc_path(self):
        return os.path.join(self.full_path + self.arc_ext)

    def dir_exists(self):
        return os.path.isdir(self.file_dir)

    def exists(self):
        return os.path.exists(self.full_path)

    def arc_exists(self):
        return os.path.exists(self.full_arc_path)

    def finalize(self):
        pass


class S3FileHandler(object):
    pass
