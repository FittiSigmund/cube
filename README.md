In the following it will be implied that `cube` is a variable instanstiate with an actual cube, with an active dimension off `Supplier`.

# Methods on cubes

## Show Active Dimensions
```python
show_active_dimensions() -> List[str]
```
Returns the name of the "active" dimensions. 
### Example 
`cube.show_active_dimensions()` would return `["Supplier"]`.

## Drill Out
```python
drill_out(dimension_name: str) -> Cube
```
The parameter `dimension_name` is the name of a dimension that exists on the cube. The method returns a cube where the dimension has been added to the list of "active" dimensions.
### Example
`cube2 = cube.drill_out("Customer")` would return a cube such that `cube2.show_active_dimentions()` would return `["Supplier", "Customer"]`.

## Slice
```python
slice(dimension_name: str, value: T) -> Cube
```
Returns a new cube where `dimension_name` has been set to `value`.
### Example
`AnotherCube = cube.slice("Supplier", "Toyota")`


## Dice
```python
dice(dict[str, T]) -> Cube
```
The parameter is expected to follow the form: `{"dimension_name1": level_member1, "dimension_name2": level_member2, etc}`
Returns a new cube where `dimension_name1` has been set to `level_member1`, `dimension_name2` has been set to `level_member2` and so on.
### Example
Let `hypercube` be a cube with `Supplier`, `Customer`, `OrderDate` (among others) as dimensions, then `hypercube.dice({"Supplier": "Toyota", "Customer": "Honda", "OrderDate": June})` would return a new cube where `Supplier` is set to `Toyota`, `Customer` is set to `Honda`, and `OrderDate` is set to June

## Get Cell Value
```python
get_cell_value(level_members: List[str]) -> Number or None
``` 
The method `get_cell_value` expects a list of level members, one from each dimension on the cube. If one valid level member for every dimension in the cube is given, then the value of that cell is returned. The value of the cell can be empty, in which case `None` is returned.
### Example
Suppose `cube` only has `Supplier` and `Customer` as dimensions, then `cube.get_cell_value(["Toyota", "Honda"])` would return the cell value of that dimension combination

## Dimensions
```python
dimensions() -> List[str]
```
Gets all dimensions in the cube
### Example
`cube.dimensions()` would return `["Supplier", "Customer"]`.

# Methods on dimensions
## Show active level
```python
show_active_level() -> str
```
Returns the "active" level of the dimension
### Example
Suppose the current level of the `Supplier` dimension in `cube` is `Manufacturer` then `cube.Supplier.show_active_level()` would return `Manufacturer`

## Drill Down and Roll Up
```python 
drill_down() -> Cube
roll_up() -> Cube
``` 
The methods `drill_down()` and `roll_up()` returns a cube where the dimension on which the method has been called has been drilled down or rolled up respectively. Note that since we currently only support one hierarchy per dimension, the methods do not need to be given a hierarchy as parameter.
### Example
Suppose the current level of the `Supplier` dimension in `cube` is `Manufacturer` and the next step up is `City`, then `cube2 = cube.Supplier.roll_up()` would return another cube where `Supplier` has been rolled up to `City`, i.e., `cube2.Supplier.show_active_level()` would return `City`.
Conversely `cube3 = cube2.Supplier.drill_down()` would return a cube such that `cube3.Supplier.show_active_level()` would return `Manufacturer`.

# Methods on hierarchies
## Name
```python
name() -> str
```
Gets the name of the hierarchy
### Example
`cube.Supplier.hierarchy.name()` would return the name of the hierarchy

## Get Current Level
```python
get_current_level() -> str
```
Gets the "active" level
### Example
`cube.Supplier.hierarchy.get_current_level()` would return `Manufacturer` if `Manufacturer` is the current level

# Methods on levels
## Members
```python
members() -> List[T]
```
Returns all members of that level. The type returned depends on what the members in the `level` class has been initialized with.
### Example
`cube.Supplier.hierarchy.Manufacturer.members()` returns all dimension values that are contained in the `Manufacturer` level.



## Current limitations
* Only 1 hierarchy per dimension
* Dimensions have no dimension value
* No level or dimension attributes
* The structure of the hierarchy is only maintained by using a list, where lower index means higher in the hierarchy
* Hierarchies have no `ALL` level yet
* No exception handling yet

<!-- Only handle 1 measure-->






# Integration of the MDX axis concept 
`cube.rows()`
