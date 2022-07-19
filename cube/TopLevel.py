from cube.Level import Level


class TopLevel(Level):
    def __init__(self):
        super().__init__(name="ALL", parent=None, child=None, dimension=None)

    def __repr__(self):
        return f"TopLevel({self.name})"
