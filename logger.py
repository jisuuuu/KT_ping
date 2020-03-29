# -*- coding: utf-8 -*-

import os
import logging.handlers

if 'nt' == os.name.lower():
    log_file_path = 'c:\\netis\\log\\'
else:
    log_file_path = '/home/netis/log/'


class Logger:
    def __init__(self):
        pass

    log = logging.getLogger("logger")
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')

    file_handler = None
    stream_handler = None

    @staticmethod
    def setLogLevel(level):
        if level == "INFO":
            Logger.log.setLevel(logging.INFO)
        elif level == "WARNING":
            Logger.log.setLevel(logging.WARNING)
        elif level == "ERROR":
            Logger.log.setLevel(logging.ERROR)
        elif level == "CRITICAL":
            Logger.log.setLevel(logging.CRITICAL)
        else:
            Logger.log.setLevel(logging.DEBUG)

    @staticmethod
    def setLogFile(logFile):
        if not os.path.exists(log_file_path):
            os.makedirs(log_file_path)

        filefullpath = log_file_path + logFile
        Logger.fileHandler = logging.FileHandler(filefullpath)
        Logger.fileHandler.setFormatter(Logger.formatter)
        Logger.log.addHandler(Logger.fileHandler)

        Logger.stream_handler = logging.StreamHandler()
        Logger.stream_handler.setFormatter(Logger.formatter)
        Logger.log.addHandler(Logger.stream_handler)

    @staticmethod
    def removeLogFile():
        Logger.log.removeHandler(Logger.file_handler)


if __name__ == '__main__':
    Logger.setLogLevel('INFO')
    Logger.setLogFile('Logger.log')
    Logger.log.debug('DEBUG')
    Logger.log.info('INFO')
    Logger.log.error('ERROR')
    Logger.log.critical('CRITICAL')
