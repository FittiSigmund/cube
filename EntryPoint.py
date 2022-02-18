from Cube import Level, Hierarchy, Dimension, Cube, Measure


def init_cube():
    backend = "MONDRIAN"
    l1 = Level("Region", backend)
    l2 = Level("Nation", backend)
    l3 = Level("City", backend)
    h1 = Hierarchy([l1, l2, l3], backend)
    d1 = Dimension("Supplier", h1, [l1, l2, l3], backend)
    l1 = Level("Commune", backend)
    l2 = Level("Address", backend)
    h2 = Hierarchy([l1, l2], backend)
    d2 = Dimension("Customer", h2, [l1, l2, l3], backend)
    l1 = Level("Category", backend)
    l2 = Level("Type", backend)
    h3 = Hierarchy([l1, l2], backend)
    d3 = Dimension("Order", h3, [l1, l2, l3], backend)
    m1 = Measure("unit sales")
    return Cube([d1, d2, d3], [], [], backend)


cube = init_cube()
cube2 = cube.columns(cube.Supplier.City.name)
cube3 = cube.rows(cube.Customer.Address.name)
cube4 = cube.columns(cube.Supplier.City.name).rows(cube.Customer.Address.name)
print(cube2.__str__())
print(cube3.__str__())
print(cube4.__str__())
