import logging

class LoggerConfig(object):
    """ Logging class for all log related attributes and modifications """

    @staticmethod
    def create_logger(name: str, log_file: str, console_level: int = logging.DEBUG) -> logging.Logger:
        """ Creates a logger instance with logfile and console logs configured to display as per given user arguments

        Args:
            name (str) : Name of the logger instance
            log_file (str): Log file name
            console_level (int) : Default log level for console

        Returns:
            object: Log object with configured attributes

        Note:
            File level logs are set to DEBUG to capture all logs
        """

        logging_format= '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        # file log configs
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(logging_format)
        file_handler.setFormatter(file_format)

        # console log configs
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_format = logging.Formatter(logging_format)
        console_handler.setFormatter(console_format)

        if not logger.handlers:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        return logger
