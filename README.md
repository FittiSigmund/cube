<!--hej
# Current limitations in the code
## Assumptions about the relational representation of the data source
* Snowflake schema
* Fact table only consists of foreign keys and measures
* No table refers to the fact table with a foreign key
* Measures cannot be unique 
    * Measures are found by taking all columns that are not foreign keys, primary keys, or have a unique constraint
    * This assumption should not be an issue
* Only one hierarchy per dimension
    * Multiple hierarchies get split up into two dimensions with one hierarchy each
        * So Date dimension with commitdate and orderdate hierarchies get split up into date dimension (with commitdate hierarchy) and date1 (with orderdate hierarchy)
* Only one fact table
* All tables which do not have any foreign keys are the coarsest non-T level of a hierarchy in a dimension
* Level tables are named 'DimensionName_LevelName'
    * Dimension names are automatically extracted from the name of a level table in the hierarchy

## General
* The only aggregate function available is `SUM`
* `SUM` is applied to all measures automatically
* No mechanism for the user to assign another aggregate function to a measure
* The cube inferral algorithm is Postgres specific
* Hierarchies have no `ALL` level yet
* No exception handling yet
-->
