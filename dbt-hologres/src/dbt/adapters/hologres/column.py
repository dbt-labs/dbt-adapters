from dbt.adapters.base import Column


class HologresColumn(Column):
    @property
    def data_type(self):
        # on hologres, do not convert 'text' or 'varchar' to 'varchar()'
        # Similar to postgres behavior
        if self.dtype.lower() == "text" or (
            self.dtype.lower() == "character varying" and self.char_size is None
        ):
            return self.dtype
        return super().data_type
