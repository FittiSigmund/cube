from enum import Enum


class FilterOperator(Enum):
    LE = "<"
    LEQ = "<="
    EQ = "="
    GEQ = ">="
    GE = ">"