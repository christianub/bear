class SkillException(Exception):
    def __init__(self, message: str = "defeated bish"):
        self.message = message

    def __str__(self):
        return self.message
