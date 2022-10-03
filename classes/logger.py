import logging
from datetime import datetime


class Logger:

    def __init__(self, level):
        self.logger = logging.getLogger("main")
        self.logger.setLevel(level)

    @staticmethod
    def get_current_datetime():
        now = datetime.now()
        date_now = now.strftime("%m-%d-%Y-%H-%M-%S")
        return date_now

    def set_handler(self, file=True):
        if file:
            f_handler = logging.FileHandler(f'./logs/logs_{self.get_current_datetime()}.log')
            f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            f_handler.setFormatter(f_format)
            self.logger.addHandler(f_handler)

    def set_level(self, level):
        self.logger.setLevel(level=logging.DEBUG)





