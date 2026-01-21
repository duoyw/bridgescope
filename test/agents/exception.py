class ModelActionException(Exception):
    def __init__(self, response, message="Model action is not valid."):
        self.message = message + str(response)
        super().__init__(self.message)