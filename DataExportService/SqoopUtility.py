#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Module Name         : Sqoop Utility
Purpose             : This class is used to execute Sqoop command
Input Parameters    : Sqoop command (String)
Output Value        : Successful execution will return a dictionary containing status of execution(SUCCESS/FAILED)
                      and total record count.
Dependencies        :
Predecessor Module  : None
Successor Module    : None
Pre-requisites      : All the dependent libraries should be present in same environment from where the module would
                      execute.
How to run          : Create its instance, call the main method and pass the Sqoop command
Last changed on     :
Last changed by     :
Reason for change   :
"""

"""Library and external modules declaration"""
import traceback
import sys
import errno
import json
from subprocess import Popen, PIPE, STDOUT
from LogSetup import logger
import SeviceConstants



"""
Utility Constants
"""
STATUS_FAILED = "FAILED"
STATUS_SUCCESS = "SUCCESS"
STATUS_KEY = "status"
ERROR_STATUS = {STATUS_KEY: STATUS_FAILED}
SUCCESS_STATUS={STATUS_KEY: STATUS_SUCCESS}


MODULE_NAME = "SqoopUtility"
STATUS_TYPE = ["SUCCESS", "FAILED", "RE-RUN"]
ERROR_IGNORE_LIST = ["hdfs.KeyProviderCache"]
ERROR_KEYWORD = "ERROR"
INVALID_COMMAND = "command not found"
SQOOP_KEY = "sqoop"
TOTAL_RECORD_COUNT = "Map output records"
RETURN_KEYS = ["status", "record_count", "error"]
HIVE_IMPORT_KEY = "--hive-import"
DELETE_KEY = "Deleted"
LOCATION_KEYS = ["Output directory", "already exists"]
FILE_EXCEPTION = "FileAlreadyExistsException"

"""
dictionary of possible driver connection based on database type
"""
driver_mapping = {
    "mysql": "com.mysql.jdbc.Driver",
    "sqlserver": "com.microsoft.jdbc.sqlserver.SQLServerDriver",
    "oracle": "oracle.jdbc.driver.OracleDriver"
}

"""
dictionary of possible connectors based on database type 
"""
connector_mapping = {
    "hsqldb": "jdbc:hsqldb://",
    "mysql": "jdbc:mysql://",
    "oracle": "jdbc:oracle://",
    "postgresql": "jdbc:postgresql://",
    "cubrid": "jdbc:cubrid://"
}

class SqoopUtility(object):
    def __init__(self):
        self.files = []
        logger.info(MODULE_NAME)
        self.status = {RETURN_KEYS[0]: STATUS_TYPE[1], RETURN_KEYS[1]: -1, RETURN_KEYS[2]: None}
        logger.debug(self.status)

    """"
    Purpose            :   To delete the directory in HDFS
    Input              :   Location (String)
    Output             :   Returns True if deleted else False
    """

    def delete_dir(self, location):
        status_message = ""
        try:
            status_message = "Starting function to delete directory: " + location
            logger.debug(status_message)
            delete_command = "hadoop fs -rm -r -skipTrash " + location
            logger.debug(delete_command)
            process = Popen(delete_command, stdout=PIPE, shell=True, stderr=STDOUT)
            del_log = process.communicate()[0]
            if del_log.find(DELETE_KEY) != 0:
                status_message = "Directory delete fail: " + del_log
                logger.error(status_message)
                raise Exception

            status_message = "Completing function to delete directory: " + location
            logger.info(status_message)
            return True

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except:
            logger.error(ERROR_KEYWORD)
            return False

    """
    Purpose            :   Read logs to check the status of sqoop job
    Input              :   Sqoop logs
    Output             :   Returns execution status and record count
    """

    def read_logs(self, process_logs):
        status_message = ""
        try:
            status_message = "Starting function to read sqoop logs"
            logger.debug(status_message)
            consolidated_log = ""
            while True:
                try:
                    ret_code = process_logs.poll()
                    log = process_logs.stdout.readline()
                    consolidated_log += log
                    if log != '':
                        logger.debug(log.strip(), )
                    if ret_code is not None:
                        break
                except IOError as e:
                    if e.errno == errno.EINTR:
                        logger.warning(str(e))
                        continue
                    else:
                        status_message = "IO Exception: " + str(e)
                        logger.error(status_message)
                        raise e

            """ 
            Reading Sqoop logs line by line
            """

            for log in consolidated_log.split('\n'):
                if INVALID_COMMAND in log:
                    status_message = "FAIL: " + log
                    logger.error(status_message)
                    raise Exception

                elif ERROR_KEYWORD in log:
                    error_flag = 'Y'
                    for i, error in enumerate(ERROR_IGNORE_LIST):
                        if error in log:
                            error_flag = 'N'

                            """
                            If error encountered is in the ignore error list then break
                            """
                            break
                    if error_flag == 'Y':
                        self.status[RETURN_KEYS[0]] = STATUS_TYPE[1]
                        self.status[RETURN_KEYS[2]] = str(log)
                        if log.find(FILE_EXCEPTION) > 0:
                            status_message = "Temp directory already exists: " + log[log.find(ERROR_KEYWORD): len(log)]
                            logger.error(status_message, FILE_EXCEPTION)
                            location = log[log.find(LOCATION_KEYS[0]) + len(LOCATION_KEYS[0]) + 1:log.find(
                                LOCATION_KEYS[1]) - 1]
                            if self.delete_dir(location.strip()):
                                self.status[RETURN_KEYS[0]] = STATUS_TYPE[2]
                                self.status[RETURN_KEYS[2]] = status_message
                        else:
                            status_message = "FAIL: " + log[log.find(ERROR_KEYWORD): len(log)]
                            logger.error(status_message)
                        """Error found then break and return fail"""
                        break

                elif TOTAL_RECORD_COUNT in log:
                    logger.info("TOTAL_RECORD_COUNT" + str(
                        int(log[log.find(TOTAL_RECORD_COUNT) + len(TOTAL_RECORD_COUNT) + 1:len(log)])))
                    self.status[RETURN_KEYS[1]] = int(
                        log[log.find(TOTAL_RECORD_COUNT) + len(TOTAL_RECORD_COUNT) + 1:len(log)])
                    self.status[RETURN_KEYS[0]] = STATUS_TYPE[0]

            status_message = "Completing function to read sqoop logs"
            logger.debug(status_message)

            return self.status

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            logger.error("failed to read the logs ")

            self.status[RETURN_KEYS[0]] = STATUS_TYPE[1]
            self.status[RETURN_KEYS[2]] = str(e)
            return self.status

    """
    Purpose   :   This method is used to execute sqoop command. It also calls read_log method to read Sqoop logs
    Input     :   Sqoop command (String)
    Output    :   Returns execution status and record count
    """

    def execute_sqoop_command(self, command):
        status_message = ""
        try:
            status_message = "Starting function to execute sqoop command"
            logger.debug(status_message)
            """Method to Validate the Input command --string validation """
            if not isinstance(command, str):
                status_message = "Input is not a valid string"
                logger.error(status_message)
                raise Exception
            logger.debug("Input is a valid string ")

            """Method  to Validate the Input command --Sqoop validation"""
            if command.find(SQOOP_KEY) != 0:
                status_message = "Input is not a valid Sqoop command"
                logger.error(status_message)
                raise Exception
            logger.debug("Input is a valid Sqoop command")

            sqoop_output = Popen(command, shell=True, stdout=PIPE, stderr=STDOUT)
            self.status = self.read_logs(sqoop_output)
            logger.debug(self.status)

            """ If status is ReRun, it will execute sqoop command again"""
            if self.status[RETURN_KEYS[0]] == STATUS_TYPE[2]:
                status_message = "Running the sqoop job again"
                logger.info(status_message)
                sqoop_output = Popen(command, shell=True, stdout=PIPE, stderr=STDOUT)
                self.status = self.read_logs(sqoop_output)

            """ If the status is ReRun again, set the status to Failed"""
            if self.status[RETURN_KEYS[0]] == STATUS_TYPE[2]:
                status_message = "Temporary directory still exists after initial successful deletion"
                logger.error(status_message)
                self.status[RETURN_KEYS[0]] = STATUS_TYPE[1]

            logger.debug(self.status)
            return self.status

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            logger.error(ERROR_KEYWORD)
            self.status[RETURN_KEYS[0]] = STATUS_TYPE[1]
            self.status[RETURN_KEYS[2]] = str(e)
            return self.status


    """  
    Purpose   :   This method to validate the database type
    Input     :   database type
    Output    :   True or False 
    """

    def validate_dbtype(self, db_type):
        try:
            if db_type in driver_mapping.keys():
                logger.debug("Entered db_type is a valid data type ")
                return True
            else:
                logger.debug("Entered db_type is a not valid data type ")
                return False


        except KeyboardInterrupt:
            raise KeyboardInterrupt


        except Exception as e:
            logger.error(ERROR_KEYWORD, "Invaid data type ")
            self.status[RETURN_KEYS[0]] = STATUS_TYPE[1]
            self.status[RETURN_KEYS[2]] = str(e)
            return self.status
            logger.debug(self.status)

    """  
    Purpose   :   This method to get the driverr with respect to valid database type
    Input     :   database type
    Output    :   True or False 
    """

    def get_driver(self, db_type):
        return driver_mapping[db_type]

    def get_connector(self, db_type):
        return connector_mapping[db_type]

    """
    Purpose   :   This method is to frame the sqoop command
    Input     :   user credentials, database info
    Output    :   Returns sqoop command
    """

    def generate_command(self, user_name, password, db_name, db_type, db_host, db_port, table_name, destination):

        try:
            while (self.validate_dbtype(db_type) == True):
                driver = self.get_driver(db_type)
                connector = self.get_connector(db_type)
                host_conn = connector + db_host + ":" + db_port + "/" + db_name

                command = "sqoop import --connect " + host_conn + " --username " + user_name + " --password " + password + " --driver " + driver + " --table " + table_name + " --target-dir " + destination
                command = str(command)
                logger.debug(command)
                return command



        except KeyboardInterrupt:
            raise KeyboardInterrupt


        except Exception as e:
            logger.error(ERROR_KEYWORD)
            self.status[RETURN_KEYS[0]] = STATUS_TYPE[1]
            self.status[RETURN_KEYS[2]] = str(e)
            return self.status



"""
Purpose   :   This method is to the sqooop utility 
Input     :   Configuration object 
Output    :   Returns the result of the execution
"""

def runSqoop(config):

    try:
        db_type = config["db_type"]
        db_name = config["db_name"]
        password = config["password"]
        user_name = config["user_name"]
        destination = config["destination"]
        table_name = config["table_name"]
        db_host = config["db_host"]
        db_port = config["db_port"]
    except Exception ,e:
        logger.error("Error Parsing Input Config ")
        ERROR_STATUS["message"]="Error Parsing Input Config"
        return ERROR_STATUS

    sqoop_utility = SqoopUtility()
    command = sqoop_utility.generate_command(user_name, password, db_name, db_type, db_host, db_port, table_name,
                                             destination)
    return_value = sqoop_utility.execute_sqoop_command(command)
    status_msg = return_value
    return status_msg






