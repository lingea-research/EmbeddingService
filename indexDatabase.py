import abc
from os import path
from tempfile import gettempdir


class IndexDatabase(metaclass=abc.ABCMeta):
    temp_index = {}

    @abc.abstractmethod
    def __init__(self, dirpath: str):
        pass
    # -------------------------------------------------------------------------
    @abc.abstractmethod
    def create_table_if_not_exists(self) -> None:
        """
        Create the 'OffsetIndex' table if it doesn't exist.
        """
        pass
    # -------------------------------------------------------------------------
    @abc.abstractmethod
    def add_row(self, document_hash: str, offset: int) -> bool:
        """
        Add a new row to the 'OffsetIndex' table.

        Args:
            document_hash (str): The document hash value.
            offset (int): The offset value.

        Returns:
            True: success, False: error
        """
        pass
    # -------------------------------------------------------------------------
    @abc.abstractmethod
    def read_offset(self, document_hash: str) -> int | None:
        """
        Read the offset value from the 'OffsetIndex' table based on the supplied document hash.

        Args:
            document_hash (str): The document hash value.

        Returns:
            int or None: The offset value if found, otherwise None.
        """
        pass
    # -------------------------------------------------------------------------

    """
    @abc.abstractmethod
    def read_all(self) -> None:
        "reads persistent DB into dict attribute (in memory)"
        pass

    @abc.abstractmethod
    def write_temp_index(self) -> None:
        "writes in-memory index attribute to real database"
        pass
    """

