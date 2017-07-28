#!/usr/bin/python
# -*- coding: utf-8 -*-


"""
Module Name         :   ConfigUtility
Purpose             :   This is used for reading values from configuration file
Input Parameters    :   Configuration file, Section in the configuration file and configuration name.
Output Value        :   This utility will return the value corresponding to a configuration parameter
                        in the configuration file
Execution Steps     :   1.Import this class in the class where we need to read values from a configuration file.
                        2.Instantiate this class
                        3.Pass the configuration file as input to get_configuration() method
Predecessor module  :   All modules which reads values from configuration files
Successor module    :
Pre-requisites      :
Last changed on     :   12th August 2015
Last changed by     :   Amal G Jose
Reason for change   :   Removed PEP 8 errors and Pychecker errors
"""

# Define all module level constants here
MODULE_NAME = "ConfigUtility"

import json


"""
Class contains all the functions related to ConfigUtility
"""
class ConfigUtility(object):
    from ConfigParser import SafeConfigParser
    # Instantiating SafeConfigParser()
    parser = SafeConfigParser()

    # Parametrized constructor with Configuration file as input
    def __init__(self, conf_file):
        try:
            self.parser.read(conf_file)
        except:
            pass

    """
    Purpose            :   This method will read the value of a configuration parameter corresponding to
                           a section in the configuration file
    Input              :   Section in the configuration file, Configuration Name
    
    Output             :   Returns the value of the configuration parameter present in the configuration file
    """
    def get_configuration(self, conf_section, conf_name):
        try:
            return self.parser.get(conf_section, conf_name)
        except:
            pass


"""
Class contains all the functions related to JsonConfigUtility
"""

class JsonConfigUtility(object):

    # Parametrized constructor with Configuration file as input
    def __init__(self, conf_file=None, conf=None):
        try:
            if conf_file is not None:
                config_fp = open(conf_file)
                self.json_configuration = json.load(config_fp)
            elif conf is not None:
                self.json_configuration = conf
            else:
                self.json_configuration = {}
        except:
            pass

    """
    Purpose            :   This method will read the value of a configuration parameter corresponding to
                           a section in the configuration file
    Input              :   Section in the configuration file, Configuration Name
    Output             :   Returns the value of the configuration parameter present in the configuration file
    """
    def get_configuration(self, conf_hierarchy):
        try:

            return reduce(lambda dictionary, key: dictionary[key], conf_hierarchy, self.json_configuration)
        except:
            pass