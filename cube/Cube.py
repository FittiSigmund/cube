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

    def columns(self, value_list):
        self._columns = value_list

    def rows(self, value_list):
        pass

    def where(self, slicer):
        pass

    def output(self):
        select_stmt = f"SELECT {self._default_measure.aggregate_function.name}({self._fact_table_name}.{self._default_measure}) AS {self._columns[0].name} "
        from_stmt = f"FROM {self._fact_table_name}"
        ## GET THE HIERARCHY OF THE LEVEL MEMBER FOUND IN THE COLUMN TO USE IN THE FROM STATEMENT
        attempt = select_stmt + from_stmt
        correct_for_month = """
            SELECT SUM(sales.total_sales_price) AS January
            FROM sales, date_day, date_month, date_year
            WHERE sales.sale_date = date_day.day_id
            AND date_day.month_id = date_month.month_id
            AND date_month.month = 'January'
            AND date_month.year_id = date_year.year_id
            AND date_year.year = 2022;
        """

        print("This is the attempt: ", attempt)
        print("This is the correct_for_month: ", correct_for_month)
        # correct_for_day = """
        #     SELECT SUM(sales.total_sales_price) AS January
        #     FROM sales, date_day, date_month, date_year
        #     WHERE sales.sale_date = date_day.day_id
        #     AND date_day.month_id = date_month.month_id
        #     AND date_month.month = 'January'
        #     AND date_month.year_id = date_year.year_id
        #     AND date_year.year = 2022;"""

        return self._columns

    def measures(self):
        return self._measure_list

    def dimensions(self):
        return self._dimension_list