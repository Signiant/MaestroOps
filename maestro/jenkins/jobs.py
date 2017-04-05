"""
jobs.py

Contains various definitions of Jenkins jobs. All job types should be based off
of a JenkinsJobEntry.

The goal would be to have a full config parser, possibly using a factory if the performance of parsing a full config file for a few attributes is terrible.
"""

#TODO:
#  Make verbose/debug useful (they used to be)
#  Expand on config parsing

import sys,os,re
from xml.dom import minidom
from glob import glob

JENKINS_DEFAULT_JOB_CONFIG_FILE = "config.xml"

class InvalidEntryError(Exception):
    pass

class JenkinsJobEntry(object):
    """
    Contains basic information about a Jenkins job. All information parsed by this class comes from the folder structure, but that may change in the future.
    """
    #The location of the Jenkins build directory (/var/lib/jenkins/jobs/.../builds)
    build_path = None

    #The jenkins config file location
    config_file_path = None

    #The root folder of Jenkins (currently unpopulated)
    jenkins_root = None

    #Jenkins Job Name
    name = None

    def __init__(self, job_directory, verbose=False, debug=False):
        """
        Init verifies that the job_directory does indeed seem like a Jenkins job folder. If the config file is not found it will raise an exception.
        """

        if job_directory is None:
            raise TypeError("Unable to process a None type.")

        self.config_file_path = os.path.join(job_directory,JENKINS_DEFAULT_JOB_CONFIG_FILE)
        if not os.path.exists(self.config_file_path):
            raise InvalidEntryError("The provided job does not contain a " + JENKINS_DEFAULT_JOB_CONFIG_FILE + " file under " + str(job_directory))

        self.build_path = os.path.join(job_directory, "builds")

        self.name = os.path.basename(job_directory)

    def get_build_number_list(self, verbose=False, debug=False):

        #Get all numeric folder names in the builds folder, assigning it to self is a cheap way to cache it
        self.builds_in_jenkins = [os.path.basename(f) for f in glob(self.build_path + "/[0-9]*") if os.path.isdir(f)]

        if verbose is True:
            for b in self.builds_in_jenkins:
                print ("Found build " + b  + " in Jenkins")
        return self.builds_in_jenkins

def parse_build_into_environment_variable_job_entry(build_root):
    env_file = os.path.abspath(os.path.join(build_root, "builds","lastSuccessfulBuild","injectedEnvVars.txt"))
    entry = EnvironmentVariableJobEntry(build_root)
    entry.environment_variables = dict()
    try:
        with open(env_file, "r") as f:
            for line in f.readlines():
                try:
                    elements = line.strip().split("=")
                    entry.environment_variables[elements[0]] = elements[1]
                except IndexError:
                    continue
    except IOError as e:
        pass
        #print "Unable to open the environment variabiles file: " + str(env_file)
    return entry

class EnvironmentVariableJobEntry(JenkinsJobEntry):
    """
    Represents the  metadata of a Jenkin's job which uses the envinject plugin from Signiant's main Jenkins server

    Ideally we would want to seperate the xml parsing of the jenkins out and create a "config parser" type of thing
    and create a generic "JenkinsJobEntry", but it's so far from actually parsing the entire file it's not worth it
    until we need to parse more of the file.
    """

    #Environment variables dictionary
    environment_variables = None

    #This should be moved up into the base class, and we should probably make a factory
    disabled = None

    def __init__(self, job_directory, verbose=False, debug=False):

        #Call the superclass' init
        self.super = super(EnvironmentVariableJobEntry, self)
        self.super.__init__(job_directory,verbose,debug)

        try:
            self.__parse_environment_variables(self.config_file_path, verbose, debug)

        #Fatal Exceptions due to general XML Parsing
        except(TypeError, AttributeError):
            raise InvalidEntryError("Unable to parse the XML for this entry!")

        #Fatal Exception due to not having a plugin attribute
        except(KeyError):
            if debug is True:
                print "ERROR: It appears that the job using " + config_file + " does not have a section for environment variables."
            raise InvalidEntryError("Unable to find a plugin properties node")

    def __parse_environment_variables(self, config_file, verbose, debug):
        if debug is True:
            print ("Parsing " + config_file)
         #Parse initial xml document
        xml_document = minidom.parse(config_file)

        # Get Disabled Value (wtf XML syntax)
        disabled_exists = xml_document.getElementsByTagName("disabled")
        disabled_val = None
        if disabled_exists:
            disabled_val = xml_document.getElementsByTagName("disabled")[0].firstChild.nodeValue

        #Determine if it's disabled or not
        if disabled_val:
            if disabled_val == "True" or disabled_val == "true":
                self.disabled = True
            else:
                self.disabled = False
        else:
            self.disabled = False

        #Declare the node so it's in scope
        environment_variables_node = None

        #Declare the dictionary so it's in scope
        environment_variables_dict = dict()

        #Find the envinject node to get the environment variables
        for node in xml_document.getElementsByTagName("propertiesContent"):
            node_parent_parent = node.parentNode.parentNode
            if "envinject" not in str(node_parent_parent.attributes["plugin"].value):
                continue

            #The actual text inside this node is considered a "TEXT_NODE", so we extract that with the following statement
            environment_variables_string = " ".join(t.nodeValue for t in node.childNodes if t.nodeType == t.TEXT_NODE)

            #Parse the properties into a dictionary
            for item in environment_variables_string.splitlines():
                try:
                    key,val = item.split("=")
                    environment_variables_dict[key] = val
                except ValueError as e:
                    if debug:
                        print ("WARNING: Hit empty entry, continuing.")
                        print (str(e))
                    pass

        if len(environment_variables_dict) == 0:
            if debug is True:
                print ("ERROR: It appears that the job using " + config_file + " does not have a section for environment variables.")
            raise InvalidEntryError("Unable to find the envinject properties node")

        self.environment_variables = environment_variables_dict
