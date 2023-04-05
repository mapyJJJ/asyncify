import logging
import sys


class Logger(logging.Logger):
    def __init__(self, name: str = "asyncify") -> None:
        """
        Initialize logger with logging level INFO. This is called by __init__ and should not be called directly

        @param name - name of logger to use

        @return None for backwards compatibility with logging. getLogger ( name ). note :: It is recommended to use logger. getLogger instead
        """
        super().__init__(name)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.handler = logging.StreamHandler(sys.stdout)
        self.handler.setFormatter(formatter)
        self.setLevel(logging.INFO)
        self.addHandler(self.handler)
