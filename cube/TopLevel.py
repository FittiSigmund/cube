from cube.Level import Level


class TopLevel(Level):
    def __init__(self, name="ALL"):
        super().__init__(name)
        self.parent = None
        self.child = None
        self._dimension = None

    def __repr__(self):
        return self._name
