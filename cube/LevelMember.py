class LevelMember:
    def __init__(self, name, children):
        self.__name = name
        self.__children = children
        for child in children:
            setattr(self, f"{child}", child)

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, value):
        self.__name = value

    def children(self):
        return self.__children
