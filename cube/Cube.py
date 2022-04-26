class Cube:
    def __init__(self, fact_table_name, dimension_list, measure_list, name, metadata):
        self._fact_table_name = fact_table_name
        self._dimension_list = dimension_list
        self._name = name
        self._metadata = metadata
        self._columns = []
        self._cursor = None
        if measure_list:
            self._default_measure = measure_list[0]
            self._measure_list = measure_list
        else:
            print("No measures! (Exception handling has not been implemented yet)")
        for dimension in dimension_list:
            dimension.metadata = self._metadata
            setattr(self, dimension.name, dimension)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, cursor):
        self._cursor = cursor

    def _drill_down(self, level_member):
        pass

    def columns(self, value_list):
        self._columns = value_list

    def rows(self, value_list):
        pass

    def where(self, slicer):
        pass

    def output(self):
        # select_stmt = f"SELECT {self._default_measure.aggregate_function.name}({self._fact_table_name}.{self._default_measure}) AS {self._columns[0].name} "
        # from_stmt = f"FROM {self._fact_table_name}"
        # hierarchy = self._columns[0]._level._dimension.hierarchies()
        # hierarchy_names = list(map(lambda x: x.name, hierarchy))
        # froms = ", ".join(hierarchy_names)
        # from_stmt = ", ".join([from_stmt, froms])
        # join_conditions = []
        # for i in range(len(hierarchy)):
        #     if i == 0:
        #         join_conditions.append(f"{self._fact_table_name}.{self._columns[0]._level._dimension._fact_table_fk} = {hierarchy[i].name}.{hierarchy[i]._pk_name}")
        #     else:
        #         join_conditions.append(f"{hierarchy[i - 1].name}.{hierarchy[i - 1]._fk_name} = {hierarchy[i].name}.{hierarchy[i]._pk_name}")

        # join_conditions = " AND ".join(join_conditions)
        # equality_conditions = [f"{self._columns[0]._level.name}.{self._columns[0]._level._member_name} = '{self._columns[0].name}'"]

        # parent = self._columns[0]._parent
        # while parent is not None:
        #     equality_conditions.append(f"{parent._level.name}.{parent._level._member_name} = {parent.name}")
        #     parent = parent._parent
        # equality_conditions = " AND ".join(equality_conditions)
        # print("This is the join conditions: ", join_conditions)
        # print("This is the equality conditions: ", equality_conditions)

        # attempt = select_stmt + from_stmt + " WHERE " + join_conditions + " AND " + equality_conditions + ";"
        # correct_for_month = """
        #     SELECT SUM(sales.total_sales_price) AS January
        #     FROM sales, date_day, date_month, date_year
        #     WHERE sales.sale_date = date_day.day_id
        #     AND date_day.month_id = date_month.month_id
        #     AND date_month.month = 'January'
        #     AND date_month.year_id = date_year.year_id
        #     AND date_year.year = 2022;
        # """

        # print("This is the attempt: ", attempt)
        # print("This is the correct_for_month: ", correct_for_month)
        # correct_for_day = """
        #     SELECT SUM(sales.total_sales_price) AS January
        #     FROM sales, date_day, date_month, date_year
        #     WHERE sales.sale_date = date_day.day_id
        #     AND date_day.month_id = date_month.month_id
        #     AND date_month.month = 'January'
        #     AND date_month.year_id = date_year.year_id
        #     AND date_year.year = 2022;"""
        # self._cursor.execute(attempt)

        # return self._cursor.fetchall()
        raise NotImplementedError

    def measures(self):
        return self._measure_list

    def dimensions(self):
        return self._dimension_list
