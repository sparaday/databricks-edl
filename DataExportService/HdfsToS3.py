import traceback
import hadoopy
import errno
from LogSetup import logger
import subprocess
import boto3

ERROR_LIST = ["Exception in thread \"main\" java.lang.RuntimeException", "Job failed", "Access Denied", "Traceback"]
# Constants representing the status keys
STATUS_RUNNING = "RUNNING"
STATUS_FAILED = "FAILED"
STATUS_SUCCESS = "SUCCESS"
STATUS_RERUN = "RE-RUN"
STATUS_SKIPPED = "SKIPPED"
STATUS_COMPLETED = "COMPLETED"
STATUS_ERROR = "ERROR"
STATUS_KEY = "status"
ERROR_KEY = "error"
RECORD_COUNT_KEY = "record_count"
RESULT_KEY = "result"
FLAG_YES = "y"
FLAG_NO = "n"
FILES_COPIED_LIST_KEY = "files_copied_list"
FILE_NAME_KEY = "file_name"

ENCRYPTION_ALGORITHM = "AES256"

ACCESS_KEY = "aws_access_key_id"
SECRET_KEY = "aws_secret_access_key"
SOURCE_FOLDER_LOCATION_KEY = "source_folder_location"
TARGET_FOLDER_LOCATION_KEY = "target_folder_location"
AES_ENCRYPTION_ENABLED_KEY = "aes_encryption_enabled"

# S3 Copy Utility Constants
S3A_MULTIPART_UPLOADS_ENABLED = "true"
S3A_MULTIPART_UPLOADS_BLOCK_SIZE = "134217728"
MAPREDUCE_TASK_TIMEOUT = "175000000"
S3PUT_RETRIES = 3
MAPREDUCE_QUEUENAME = "mapreduce_queuename"

# Adding common configs for S3load Utility and S3toHDFS copy
DISTCP_COMMAND_OPTIONS_KEY = "distcp_command_options"
HADOOP_OPTIONS_KEY = "hadoop_options"
DISTCP_OPTIONS_KEY = "distcp_options"

MERGED_FILE_NAME_KEY = "merged_file_name"
FILE_SIZE_KEY = "file_size"

ERROR_STATUS = {STATUS_KEY: STATUS_FAILED, FILES_COPIED_LIST_KEY: []}
SUCCESS_STATUS={STATUS_KEY: STATUS_SUCCESS, FILES_COPIED_LIST_KEY: []}
STATUS_FAILED = "FAILED"
STATUS_SUCCESS = "SUCCESS"

class HdfsToS3(object):
    def __init__(self):
        self.atomic_transaction = FLAG_YES
        self.s3_cleanup_before_transfer = FLAG_YES

    """  
    Purpose   :   This method is used to set options used in the hadoop distcp command
                       1. set s3 access key & secret key
                       2. If encryption flag set is Y, set server side encryption option
                       3. Set Queue name given, set queue name option
                       4. Add default options
                       5. Add options given in distcp_command_options
    Input     :   s3_credentials_json
    Output    :   Returns a sting of options for command
    """

    def create_command_options_string(self, s3_credentials_json):
        status_message = ""
        try:
            # Add access key & secret key to options
            s3_access_key = s3_credentials_json[ACCESS_KEY]
            s3_secret_key = s3_credentials_json[SECRET_KEY]
            option_string = "-Dfs.s3a.awsAccessKeyId=" + s3_access_key + " -Dfs.s3a.awsSecretAccessKey=" + s3_secret_key
            option_string = option_string + " -Dfs.s3n.awsAccessKeyId=" + s3_access_key + " -Dfs.s3n.awsSecretAccessKey=" + s3_secret_key
            # Add server side encryption option based on s3_encryption_enabled key
            s3_encryption_enabled = s3_credentials_json[AES_ENCRYPTION_ENABLED_KEY]
            if s3_encryption_enabled and s3_encryption_enabled.lower() == "y":
                option_string = option_string + " -Dfs.s3a.server-side-encryption-algorithm=" + ENCRYPTION_ALGORITHM
            # Add Quene name if given
            if MAPREDUCE_QUEUENAME in s3_credentials_json:
                mapreduce_job_queuename = s3_credentials_json[MAPREDUCE_QUEUENAME]
                option_string = option_string + " -Dmapreduce.job.queuename=" + mapreduce_job_queuename.strip()
            # Add Default options
            option_string = \
                option_string + " -Dfs.s3a.multipart.uploads.enabled=" \
                + S3A_MULTIPART_UPLOADS_ENABLED + " -Dmapreduce.task.timeout=" \
                + MAPREDUCE_TASK_TIMEOUT + " -Dfs.s3a.multipart.uploads.block.size=" \
                + S3A_MULTIPART_UPLOADS_BLOCK_SIZE
            # Add options given in distcp_command_options
            if s3_credentials_json.get(DISTCP_COMMAND_OPTIONS_KEY):
                distcp_command_options = s3_credentials_json.get(DISTCP_COMMAND_OPTIONS_KEY)
                if distcp_command_options.get(HADOOP_OPTIONS_KEY):
                    hadoop_options = dict(distcp_command_options.get(HADOOP_OPTIONS_KEY))
                    for option, value in hadoop_options.items():
                        option_string = option_string + " " + option + "=" + value
                if distcp_command_options.get(DISTCP_OPTIONS_KEY):
                    distcp_options = list(distcp_command_options.get(DISTCP_OPTIONS_KEY))
                    for options in distcp_options:
                        option_string = option_string + " " + options
            return option_string
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            # self.execution_context.set_context({"traceback": error})
            logger.error(status_message)

            return None

    """ 
    Purpose   :   This method is called if the source type is Hdfs. It checks if the file/dir in the source exists
                  and also checks for the type. If type is a directory, it constructs a s3distcp command and if the
                  type is a file, it constructs a distcp command and submits it to the child method hdfs_to_s3_loader
                  for file loading. If any of the file transfer fails, the entire process fails cleaning up the s3
                  target path
    Input     :   The source Hdfs path, the file/directory path to transfer to s3, s3 target location and dictionary
                  containing the s3 credentials along with the s3distcp jar path
    Output    :   Returns a status json containing the status i.e. true or false and also the files_copied_list array
                  containing a json for each file/dir transferred with file_name and file_size
    """

    def hdfs_to_s3(self, source_path, files_list, target_path, s3_credentials_json):
        transferred_file_list = []
        status_message = ""
        try:
            status_message = "Executing function to load data from Hdfs to S3"
            logger.debug(status_message)
            overall_file_status = dict()
            overall_file_status[STATUS_KEY] = STATUS_SUCCESS
            files_transferred = []
            option_string = self.create_command_options_string(s3_credentials_json)

            if files_list == False:
                files_list = hadoopy.ls(source_path)




            if not option_string:
                status_message = "Error Occured while creating hadoop distcp options"
                raise Exception
            for file_name in files_list:
                hdfs_file = source_path + "/" + file_name.replace(source_path, "").strip("/")
                s3_file_path = target_path + "/" + file_name.replace(source_path, "").strip("/")

                if not hadoopy.exists(hdfs_file)and not hadoopy.isdir(file_name):
                    status_message = "Hdfs File :" + hdfs_file + " does not exists"
                    raise Exception
                status_message = "Loading file from Hdfs to S3. File name - " + hdfs_file
                logger.info(status_message)
                # transferred_file_list.append(s3_file_path)
                command = "hadoop distcp " + option_string + " " + hdfs_file + " " + s3_file_path
                # Just used for display purpose
                replace_param_list = [s3_credentials_json[ACCESS_KEY],
                                      s3_credentials_json[SECRET_KEY]]
                cmd_to_display = command
                for replace_param in replace_param_list:
                    cmd_to_display = cmd_to_display.replace(replace_param, "*********")
                status_message = "Running command - " + cmd_to_display
                logger.debug(status_message)
                status = self.hdfs_to_s3_loader(command, hdfs_file, s3_file_path, s3_credentials_json,
                                                transferred_file_list)
                transfer_status = status[FILE_NAME_KEY]
                if not transfer_status:
                    status_message = "Failed to transfer Hdfs file " + hdfs_file + " to S3"
                    raise Exception
                files_transferred.append(status)
                SUCCESS_STATUS[FILES_COPIED_LIST_KEY] = files_transferred
            return SUCCESS_STATUS

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            traceback.print_exc()
            logger.error(status_message)

            if self.atomic_transaction.lower() == FLAG_YES:
                if transferred_file_list:
                    deleted = self.s3_cleanup(transferred_file_list, s3_credentials_json)
                    # TODO - Handling retries in case of cleanup fails
                    if not deleted:
                        status_message = "Error in cleaning files already loaded to s3"
                        error = " ERROR MESSAGE: " + str(traceback.format_exc())
                        logger.error(status_message)

            else:
                status_message = "Clean up over transaction error is set to false. Skipping the clean up process"
                logger.info(status_message)

                return ERROR_STATUS

    """       
    Purpose   :   This method takes the command along with source path and target path and executes it. It checks
                  the output of the command and if the command fails, the execution is stopped.
                  After transferring the file/dir, it verifies that the load was successful by comparing the
                  size of Hdfs file/dir with the transferred s3 file/dir. If it cannot verify the size, it fails and
                  cleans up the transferred files from s3.
    Input     :   The command to execute, either s3distcp or distcp, the fully qualified source and target paths, the
                    list containing the files transferred
    Output    :   Returns a json containing file_name and file_size of the s3 target location
    """

    def hdfs_to_s3_loader(self, command_to_execute, source_file_path, target_file_path, s3_credentials_json,
                          transferred_file_list):
        status_message = ""
        try:
            status_message = "Starting function to Load data from HDFS to S3"
            logger.debug(status_message)

            process = subprocess.Popen(command_to_execute, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
            consolidated_log = ""
            while True:
                try:
                    ret_code = process.poll()
                    log = process.stdout.readline()
                    logger.debug(log)
                    consolidated_log += log
                    if ret_code is not None:
                        break
                except IOError as e:
                    if e.errno == errno.EINTR:
                        logger.error(str(e))
                        continue
                    else:
                        error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                                " ERROR MESSAGE: " + str(traceback.format_exc())
                        logger.error(status_message)
                        raise e
            error_status = self.log_parser(consolidated_log)
            if error_status:
                status_message = "Error executing the command - " + consolidated_log
                raise Exception
            transferred_relative_files_list = []
            transferred_absolute_files_list = []
            return_status = self.get_hdfs_files_list(source_file_path, source_file_path,
                                                     transferred_relative_files_list, transferred_absolute_files_list)

            if not return_status:
                status_message = "Error while fetching HDFS file list"
                raise Exception

            transferred_file_list.extend(list(set(transferred_absolute_files_list) - set(transferred_file_list)))

            s3_file_size = self.get_s3_folder_size(target_file_path, s3_credentials_json,
                                                   transferred_relative_files_list)
            hdfs_file_size = self.get_hdfs_folder_size(source_file_path)
            if s3_file_size is None or hdfs_file_size is None:
                status_message = "Could not calculate S3 or Hdfs size"
                raise Exception
            if s3_file_size != hdfs_file_size:
                status_message = "Size of source and target do not match while transferring hdfs file to s3. " \
                                 "S3 file size = " \
                                 + str(s3_file_size) + " Hdfs file size = " + str(hdfs_file_size)
                raise Exception

            status = {FILE_NAME_KEY: target_file_path, FILE_SIZE_KEY: str(s3_file_size)}
            return status

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message)
            status = {FILE_NAME_KEY: "", FILE_SIZE_KEY: 0}
            return status

    """ 
    Purpose   :   This method is used to calculate the file/dir size on s3.
    Input     :   The path for which the size is need to be calculated
    Output    :   Returns the size of the path in bytes
    """

    def get_s3_folder_size(self, target_file_path, s3_credentials_json, transferred_files_list):
        try:
            status_message = "Calculating s3 size for the file/directory - " + target_file_path
            logger.debug(status_message)
            s3_access_key = s3_credentials_json[ACCESS_KEY]
            s3_secret_key = s3_credentials_json[SECRET_KEY]
            s3 = boto3.client('s3', aws_access_key_id=s3_access_key,
                              aws_secret_access_key=s3_secret_key)

            bucket_name = target_file_path[(target_file_path.index("://") +
                                            3):(target_file_path.index("/", target_file_path.index("://") + 3))]
            status_message = "Extracted bucket name - " + bucket_name
            logger.debug(status_message)
            s3_target_path = target_file_path[target_file_path.index("/", target_file_path.index("//") + 2) + 1:]

            try:
                size = 0
                folder_key = s3.list_objects(Bucket=bucket_name, Delimiter=',')['Contents']
                for keys in folder_key:
                    if (keys["Key"] == s3_target_path) or (keys["Key"] in transferred_files_list):
                        size = size + keys["Size"]
                status_message = "S3 file size for file " + target_file_path + " : " + str(size)
                logger.debug(status_message)
                return size
            except Exception:
                status_message = "Unable to get the bucket. Kindly verify the bucket name, Access key and Secret key"
                error = " ERROR MESSAGE: " + str(traceback.format_exc())

                logger.error(status_message)

                return None

            folder_key = bucket.list(s3_target_path)
            size = 0




        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except:
            status_message = "Error getting S3 file size for the file - " + target_file_path
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message)
            return None

    """ 
    Purpose   :   This method is used to parse the logs to check if there is any error
    Input     :   Standard output of any process
    Output    :   Return True if errors are present else return False
    """

    def log_parser(self, logs):
        status_message = ""
        try:
            for error in ERROR_LIST:
                if logs.__contains__(error):
                    raise Exception
            else:
                return False

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())

            logger.error(status_message)
            return True

    """
    Purpose   :   This method is used to calculate the file/dir size on Hdfs.
    Input     :   The path for which the size is need to be calculated
    Output    :   Returns the size of the path in bytes or None in case of exception
    """

    def get_hdfs_folder_size(self, source_file_path):
        status_message = ""
        try:
            status_message = "Calculating Hdfs size for the file/directory - " + source_file_path
            logger.debug(status_message)
            cmd = "hadoop fs -du -s " + source_file_path
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
            size = 0
            standard_output, standard_error = process.communicate()
            if standard_output:
                error_status = self.log_parser(standard_output)
                if error_status:
                    status_message = "Error in executing command - " + cmd
                    raise Exception
                output = standard_output.split()
                size = output[0]
            elif standard_error:
                status_message = "Error in executing command - " + cmd
                raise Exception
            else:
                pass
            status_message = "Hdfs file size for file " + source_file_path + " : " + str(size)
            logger.debug(status_message)
            return int(size)

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message)
            return None

    """
    Purpose   :   This method is used to get the list of files present in the HDFS source folder
    Input     :   The base path of the folder, current path for traversal, list of files with relative paths and the
                     other with absolute paths
    Output    :   Return True if the traversal was successful else return False
    """

    def get_hdfs_files_list(self, base_path, path, relative_list, absolute_list):
        status_message = ""
        try:
            status_message = "Starting function to fetch HDFS files list."
            logger.debug(status_message)
            if hadoopy.isdir(path):
                curr_files_list = hadoopy.ls(path)
                for file_name in curr_files_list:
                    if hadoopy.isdir(file_name):
                        self.get_hdfs_files_list(base_path, file_name, relative_list, absolute_list)
                    else:
                        relative_list.append(file_name[len(base_path):])
                        absolute_list.append(file_name)
            else:
                absolute_list.append(path)
            return True

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except:
            error = " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message)
            return False


def runHdfsTOS3(config):
    hdfsToS3 = HdfsToS3()
    filelist = False
    if "file_list" in config:
        filelist = config['file_list']
    source_path = config["source_path"]
    target_path = config["target_path"]
    s3_credentials_json = config["s3_credentials"]
    return hdfsToS3.hdfs_to_s3(source_path=source_path, files_list=filelist,
                               target_path=target_path, s3_credentials_json=s3_credentials_json)
