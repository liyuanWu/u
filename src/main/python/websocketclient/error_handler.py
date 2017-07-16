import logging

class ErrorHandler(object):
    def on_error(self, error):
        logger = logging.getLogger(__name__)
        logger.error(error)
