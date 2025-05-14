# src/utilities/logger.py
import logging
import os
import traceback
import logging.config
import yaml

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')
LOG_CONFIG_FILE = os.environ.get('LOG_CONFIG_FILE', 'log_config.yaml')

def get_error_msg(err, process_name=''):
    """
    Generate error message for logging
    :param err: Exception object
    :param process_name: Name of the process where the error occurred
    :return: Formatted error message
    """
    tb = err.__traceback__
    full_tb = traceback.extract_tb(tb)
    tb_msg = ''
    code_path = os.path.join(ROOT_DIR, 'src')

    for idx, sub_tb in enumerate(full_tb):
        absfile = os.path.abspath(sub_tb.filename)
        if code_path in absfile:
            msg = f"tb lvl:{idx}|funName:{sub_tb.name}|file:{absfile}|line:{sub_tb.lineno}|code:{sub_tb.line}"
            tb_msg += f"\n{msg}"

    error_msg = f"{type(err)} in running {process_name}:\n{err}\nTraceback:{tb_msg}"

    return error_msg


class CustomLogger(logging.Logger):
    """Extended Logger class with additional error reporting capabilities"""

    def error_with_details(self, err, process_name=''):
        """Log an error with detailed traceback"""
        error_message = get_error_msg(err, process_name)
        self.error(error_message)

# Register the custom logger class
logging.setLoggerClass(CustomLogger)

class LogFactory:
    is_configured = False

    @staticmethod
    def _ensure_configured():
        """ config logging config """
        if not LogFactory.is_configured:
            # Register custom logger class before configuring
            logging.setLoggerClass(CustomLogger)

            logging_conf_path = os.path.join(ROOT_DIR, 'config', 'log_config', LOG_CONFIG_FILE)
            with open(logging_conf_path, 'r') as f:
                logging_config = yaml.safe_load(f.read())
            logging.config.dictConfig(logging_config)
            LogFactory.is_configured = True

    @staticmethod
    def create_logger(name: str) -> logging.Logger:
        """ create logger """
        LogFactory._ensure_configured()
        logger = logging.getLogger(name)
        logger.setLevel(LOG_LEVEL)

        for name in logging.root.manager.loggerDict:
            logging.root.manager.loggerDict[name].disabled = False

        return logger

    @staticmethod
    def set_log_level(level):
        """ set logs level """
        LogFactory.synchronize_log_level()

    @staticmethod
    def synchronize_log_level():
        """Ensure all loggers have level set to LOG_LEVEL"""
        # Use print instead of log.info to avoid circular reference
        print(f"Synchronizing loggers to level {LOG_LEVEL}")
        for name in logging.root.manager.loggerDict:
            logger = logging.root.manager.loggerDict[name]
            if hasattr(logger, 'level'):  # Check if it's an actual logger and not a PlaceHolder
                logger.level = logging._nameToLevel(LOG_LEVEL)

log = LogFactory.create_logger('worldquants')

