import abc
from typing import Optional, Callable, Type, Tuple, List

from dbt.adapters.base import available


class AdapterTypes:
    @classmethod
    @abc.abstractmethod
    def convert_text_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.Text
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedError("`convert_text_type` is not implemented for this adapter!")

    @classmethod
    @abc.abstractmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.Number
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedError("`convert_number_type` is not implemented for this adapter!")

    @classmethod
    def convert_integer_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.Number
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        return "integer"

    @classmethod
    @abc.abstractmethod
    def convert_boolean_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.Boolean
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedError("`convert_boolean_type` is not implemented for this adapter!")

    @classmethod
    @abc.abstractmethod
    def convert_datetime_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.DateTime
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedError("`convert_datetime_type` is not implemented for this adapter!")

    @classmethod
    @abc.abstractmethod
    def convert_date_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.Date
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedError("`convert_date_type` is not implemented for this adapter!")

    @classmethod
    @abc.abstractmethod
    def convert_time_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Return the type in the database that best maps to the
        agate.TimeDelta type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedError("`convert_time_type` is not implemented for this adapter!")

    @available
    @classmethod
    def convert_type(cls, agate_table: "agate.Table", col_idx: int) -> Optional[str]:
        return cls.convert_agate_type(agate_table, col_idx)

    @classmethod
    def convert_agate_type(cls, agate_table: "agate.Table", col_idx: int) -> Optional[str]:
        import agate
        from dbt_common.clients.agate_helper import Integer

        agate_type: Type = agate_table.column_types[col_idx]
        conversions: List[Tuple[Type, Callable[..., str]]] = [
            (Integer, cls.convert_integer_type),
            (agate.Text, cls.convert_text_type),
            (agate.Number, cls.convert_number_type),
            (agate.Boolean, cls.convert_boolean_type),
            (agate.DateTime, cls.convert_datetime_type),
            (agate.Date, cls.convert_date_type),
            (agate.TimeDelta, cls.convert_time_type),
        ]
        for agate_cls, func in conversions:
            if isinstance(agate_type, agate_cls):
                return func(agate_table, col_idx)

        return None
