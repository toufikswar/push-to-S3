import json
import os
import logging
import inspect
import shutil


def define_logger(main=False, level=logging.INFO, config=None):
    """ Logging function.
    Function that creates and defines the logging level
    :param main: True if it's the root logger (on main script)
    :param level: level of debugging
    :param config: JSON config file
    :return: a logger object
    """
    # this only runs when defining the root logger in main script
    if main:
        logger = logging.getLogger()

        if config.get('logs_folder'):
            log_output = config.get('logs_folder')
        else:
            log_output = "./"

        logger.setLevel(level)
        # create file handler for logging in a file
        fh = logging.FileHandler(f'{log_output}push-to-S3.log')
        fh.setLevel(level)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(level)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(module)s - %(name)s.%(funcName)s() - '
                                      '%(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)

    # this runs if not root logger
    else:
        frame = inspect.currentframe().f_back
        self_obj = frame.f_locals['self']
        logger = logging.getLogger(str(type(self_obj).__name__))  # name the logger as module name

    return logger


def load_config(path_to_json):
    """ Load a JSON config file
    Function to load a JSON config app
    :param path_to_json: string with path to JSON config file
    :return:
    """
    try:
        with open(path_to_json, "r") as f:
            return json.load(f)
    except json.JSONDecoder as jerr:
        exit(1)
    except Exception as ex:
        exit(1)


def arrange_df_headers(df):
    """ Arrange headers
    Function to correct the headers of a dataframe
    :param df: a dataframe
    :return: a corrected dataframe
    """
    new_header = df.iloc[0]  # grab the first row for the header
    df = df[1:]  # take the data less the header row
    df.columns = new_header  # set the header row as the df header
    return df


def move_file(filepath, config, success=True):
    """ Move file from source to dest
    Function that moves a file to a dest or another based on a bool value.
    If success go to success folder otherwise to failure folder
    :param filepath: string with full file path
    :param config: app JSON config file
    :param success: bool
    :return: None
    """
    filename = os.path.split(filepath)[1]
    if success:
        shutil.move(filepath, config.get("success_folder") + filename)
    else:
        shutil.move(filepath, config.get("failure_folder") + filename)
