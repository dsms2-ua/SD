
class Casilla:
    def __init__(self):
        self.x = 1
        self.y = 1

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __str__(self):
        return f"({self.x}, {self.y})"