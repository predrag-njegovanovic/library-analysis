class MissingConfigModuleImplementation(Exception):
    def __init__(
        self, config_module: str, message: str = "Missing config module implementation"
    ) -> None:
        super().__init__(f"{message} '{config_module}'")


class MissingClassImplementation(Exception):
    def __init__(
        self, class_name: str, message: str = "Missing class implementation"
    ) -> None:
        super().__init__(f"{message} '{class_name}'")
