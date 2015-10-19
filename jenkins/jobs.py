"""
jobs.py

Contains various definitions of Jenkins jobs. All job types should be based off
of a JenkinsJobEntry.

The goal would be to have a full config parser, possibly using a factory if the performance of parsing a full config file for a few attributes is terrible.
"""

import sys,os,re
from xml.dom import minidom
from glob import glob

class InvalidEntryError(Exception):
    pass

class JenkinsJobEntry(object):

    #The location of the Jenkins build directory (/var/lib)
    jenkins_build_path = None

    #The jenkins config file location
    jenkins_config_file_path = None

    #The root folder of Jenkins
    jenkins_root = None

    #Jenkins Job Name
    name = None
    
    #TODO: Change around to accept a job directory rather than config.xml
    def __init__(self, config_file, verbose=False, debug=False):
        if config_file is None:
            raise TypeError("Unable to process a None type.")

        if not os.path.exists(config_file):
            raise InvalidEntryError("You must provide a valid Jenkins config.xml file path.")

        self.jenkins_config_file_path = config_file
        self.jenkins_build_path = os.path.join(os.path.dirname(config_file) + "/builds")
        self.name = os.path.basename(os.path.dirname(config_file))

    def get_build_number_list(self, verbose=False, debug=False):

        #Get all numeric folder names in the builds folder, assigning it to self is a cheap way to cache it
        self.builds_in_jenkins = [os.path.basename(f) for f in glob(self.jenkins_build_path + "/[0-9]*") if os.path.isdir(f)]
        if verbose is True:
            for b in self.builds_in_jenkins:
                print "Found build " + b  + " in Jenkins"
        return self.builds_in_jenkins
    

class EnvironmentVariableJobEntry(JenkinsJobEntry):
    """
    Represents the  metadata of a Jenkin's job which uses the envinject plugin from Signiant's main Jenkins server

    Ideally we would want to seperate the xml parsing of the jenkins out and create a "config parser" type of thing
    and create a generic "JenkinsJobEntry", but it's so far from actually parsing the entire file it's not worth it
    until we need to parse more of the file.
    """

    #Environment variables dictionary
    environment_variables = None

    def __init__(self, config_file=None, verbose=False, debug=False):
        
        #Call the superclass' init
        self.super = super(EnvironmentVariableJobEntry, self)
        self.super.__init__(config_file,verbose,debug)

        try:
            self.__parse_environment_variables(config_file, verbose, debug)

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
            print "Parsing " + config_file
         #Parse initial xml document
        xml_document = minidom.parse(config_file)

        #Declare the node so it's in scope 
        environment_variables_node = None
        
        #Find the envinject node to get the environment variables
        for node in xml_document.getElementsByTagName("propertiesContent"):
            node_parent_parent = node.parentNode.parentNode
            if "envinject" in str(node_parent_parent.attributes["plugin"].value):
                environment_variables_node = node
        if environment_variables_node is None:
            if debug is True:
                print "ERROR: It appears that the job using " + config_file + " does not have a section for environment variables."
            raise InvalidEntryError("Unable to find the envinject properties node")  
 
        #The actual text inside this node is considered a "TEXT_NODE", so we extract that with the following statement
        environment_variables_string = " ".join(t.nodeValue for t in environment_variables_node.childNodes if t.nodeType == t.TEXT_NODE)
        
        #Declare the dictionary so it's in scope
        environment_variables_dict = dict()
        
        #Parse the properties into a dictionary
        for item in environment_variables_string.splitlines():
            try:
                key,val = item.split("=")
                environment_variables_dict[key] = val 
            except ValueError as e:
                if debug:
                    print "WARNING: Hit empty entry, continuing."
                    print str(e)
                pass
        self.environment_variables = environment_variables_dict 

