class DoesNotExist(FileNotFoundError):
    pass


class ReadOnlyError(Exception):
    pass
