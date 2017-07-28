#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ################################# Module Information #################################################################
#   Module Name         : logsetup
#   Purpose             : Setup the environment and assign value to all the variables required for logging
#   Pre-requisites      : Config variables should be present in log_setup json file
#   Last changed on     : 29 April 2016
#   Last changed by     : Kannan S
#   Reason for change   : Added TimedRotatingFileHandler feature
# ######################################################################################################################

# Library and external modules declaration
import logging
import logging.handlers
import logstash
import os
import socket
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from ConfigUtility import ConfigUtility

ENVIRONMENT_CONFIG_FILE = "log_setup.conf"
MODULE_NAME = "log_setup"

DEFAULT_LOG_FILE_NAME = "log_setup"
TIME_FORMAT = str(datetime.now().strftime("%Y%m%d_%H_%M_%S"))
file_name="log.log"

def get_logger(file_name=None, append_ts=True, timed_rotating_fh=False):
    configuration = ConfigUtility(os.path.normpath(os.path.dirname(os.path.realpath(__file__)))+'/'+ENVIRONMENT_CONFIG_FILE)
    public_machine_list = configuration.get_configuration(MODULE_NAME, "public_machine_list")
    logstash_port = configuration.get_configuration(MODULE_NAME, "logstash_port")
    logstash_schema_version = configuration.get_configuration(MODULE_NAME, "schema_version")
    logstash_public_host_name = configuration.get_configuration(MODULE_NAME, "logstash_public_ip")
    logstash_private_host_name = configuration.get_configuration(MODULE_NAME, "logstash_private_ip")
    console_logging_level = configuration.get_configuration(MODULE_NAME, "console_logging_level")
    file_logging_level = configuration.get_configuration(MODULE_NAME, "file_logging_level")
    logstash_logging_level = configuration.get_configuration(MODULE_NAME, "logstash_logging_level")
    timed_rotating_file_suffix = configuration.get_configuration(MODULE_NAME, "timed_rotating_file_suffix")
    logfile_path = configuration.get_configuration(MODULE_NAME, "logfile_path")
    logfile_name = configuration.get_configuration(MODULE_NAME, "logfile_name")
    timed_rotating_file_handler_interval = configuration.get_configuration(
        MODULE_NAME, "timed_rotating_file_handler_interval")
    timed_rotating_file_handler_cycle = configuration.get_configuration(
        MODULE_NAME, "timed_rotating_file_handler_cycle")
    # creating log file name based on the timestamp and file name
    if file_name is None:
        if append_ts:
            file_name = TIME_FORMAT
        else:
            file_name = DEFAULT_LOG_FILE_NAME
    else:
        if append_ts:
            file_name += "_" + TIME_FORMAT
    logger_obj = logging.getLogger(file_name)

    file_log_path = None
    if logfile_path is not None:
        file_log_path=logfile_path
    if logfile_name is not None:
        file_name=logfile_name



    if not logger_obj.handlers:
        # Set logging level across the logger. Set to INFO in production
        if logstash_logging_level is not None:
            if logstash_logging_level.lower() == "debug":
                logger_obj.setLevel(logging.DEBUG)
            elif logstash_logging_level.lower() == "warning":
                logger_obj.setLevel(logging.WARNING)
            elif logstash_logging_level.lower() == "error":
                logger_obj.setLevel(logging.ERROR)
            else:
                logger_obj.setLevel(logging.INFO)
        else:
            logger_obj.setLevel(logging.INFO)

        # create formatter
        log_formatter_keys = configuration.get_configuration(MODULE_NAME, "log_formatter_keys")
        if log_formatter_keys is not None:
            # if log_formatter_keys exists and its not a list or blank list default formatter will be created
            if len(log_formatter_keys) == 0 or not isinstance(log_formatter_keys, list):
                formatter_string = "'%(asctime)s - %(levelname)s - %(message)s '"
            else:
                # when log_formatter_keys in json contains keys, formatter using those keys will be created
                default_format_parameters = ["asctime", "levelname", "message"]
                default_format_parameters.extend(log_formatter_keys)
                formatter_string = "'"
                for keys in default_format_parameters:
                    formatter_string += "%(" + str(keys) + ")s - "
                formatter_string = formatter_string.rstrip(" - ")
                formatter_string += "'"
        else:
            # when log_formatter_keys does not exists default formatter will be created
            formatter_string = "%(asctime)s - %(levelname)s - %(message)s "

        formatter = logging.Formatter(formatter_string)

        # create file handler which logs even debug messages
        file_handler_flag = configuration.get_configuration(MODULE_NAME, "file_handler_flag")
        if file_handler_flag == "Y":
            file_handler_path = configuration.get_configuration(MODULE_NAME, "file_handler_path")
            # checking if file_handler_path key is present in json
            if file_handler_path != "" and file_handler_path is not None:
                if os.path.exists(file_handler_path):
                    # if key is present and its correct path log file will be created on that path
                    file_log_path = os.path.join(str(file_handler_path), file_name)
                else:
                    file_log_path = file_name
            else:
                # log file will be created on path where code runs
                file_log_path = file_name

            if timed_rotating_fh:
                file_handler = TimedRotatingFileHandler(file_log_path, when=timed_rotating_file_handler_cycle,
                                                        interval=timed_rotating_file_handler_interval)
                file_handler.suffix = timed_rotating_file_suffix
            else:
                file_log_path += '_file.log'
                file_handler = logging.FileHandler(file_log_path, delay=True)

            # Set logging level across the logger. Set to INFO in production
            if file_logging_level is not None:
                if file_logging_level.lower() == "debug":
                    file_handler.setLevel(logging.DEBUG)
                elif file_logging_level.lower() == "warning":
                    file_handler.setLevel(logging.WARNING)
                elif file_logging_level.lower() == "error":
                    file_handler.setLevel(logging.ERROR)
                else:
                    file_handler.setLevel(logging.INFO)
            else:
                file_handler.setLevel(logging.INFO)

            file_handler.setFormatter(formatter)

            logger_obj.addHandler(file_handler)

        # creating handler for logstash
        if (logstash_public_host_name != "" or logstash_private_host_name != "") and \
                (logstash_public_host_name is not None or logstash_private_host_name is not None):
            hostname = socket.gethostname()
            ip_address = hostname.replace('ip-', '').replace('-', '.')

            if ip_address in public_machine_list:
                logstash_host_name = logstash_public_host_name
            else:
                logstash_host_name = logstash_private_host_name
            logstash_handler = logstash.LogstashHandler(logstash_host_name, int(logstash_port),
                                                        version=int(logstash_schema_version))
            logger_obj.addHandler(logstash_handler)
        else:
            # No handler for logstash
            pass

        # create console handler with debug level
        console_handler_flag = configuration.get_configuration(MODULE_NAME, "console_handler_flag")
        if console_handler_flag != "N":
            console_handler = logging.StreamHandler()

            # Set logging level across the logger. Set to INFO in production
            if console_logging_level is not None:
                if console_logging_level.lower() == "debug":
                    console_handler.setLevel(logging.DEBUG)
                elif console_logging_level.lower() == "warning":
                    console_handler.setLevel(logging.WARNING)
                elif console_logging_level.lower() == "error":
                    console_handler.setLevel(logging.ERROR)
                else:
                    console_handler.setLevel(logging.INFO)
            else:
                console_handler.setLevel(logging.INFO)

            console_handler.setFormatter(formatter)

            logger_obj.addHandler(console_handler)

    return logger_obj, file_log_path

logger, log_path = get_logger()
