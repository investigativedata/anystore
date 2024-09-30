class DoesNotExist(FileNotFoundError):
    pass


class WriteError(Exception):
    pass
