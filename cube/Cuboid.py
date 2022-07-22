from cube.Cube import Cube


class Cuboid(Cube):
    def __init__(self, dimension_list, measure_list, engine, columns, previous):
        super().__init__(dimension_list, measure_list, engine, previous)
        self._column_list = columns

    def columns(self, value_list):
        return Cuboid(
            self._dimension_list,
            self._measure_list,
            self._engine,
            value_list,
            self
        )

    def rows(self, value_list):
        pass

    def pages(self, value_list):
        pass

    def where(self):
        pass

    def output(self):
        from cube.BaseCube import BaseCube
        if isinstance(self._previous, BaseCube):
            return self._previous.output(self._column_list)
        else:
            prev_results = self._previous.output()
            column_list_names = self._get_column_list_names()
            result = prev_results[column_list_names]
            return result

    def _get_column_list_names(self):
        return list(map(lambda x: x.name, self._column_list))
