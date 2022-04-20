class Level:
    def __init__(self, name, level_member_values):
        self.__name = name
        self.__level_member_values = level_member_values
        for member in level_member_values:
            setattr(self, f"_{member.name}", member)
        # Initialize parent to None first in order to set it later using the parent.setter
        # If I didn't do this, I would have a never ending chain of Level initializations
        self.__parent = None

    def members(self):
        return self.__level_member_values

    @property
    def name(self):
        return self.__name

    @property
    def parent(self):
        return self.__parent

    @parent.setter
    def parent(self, value):
        self.__parent = value
