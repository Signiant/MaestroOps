"""
Contains some S3 helper methods and classes, currently extending boto
"""

HELPTEXT = """
                ----- S3 Downloader -----

The S3 Downloader will retreive a file from an S3 bucket with anonymous
read access.

-b, --bucket <bucket_name>:             Specify the name of the bucket to access
                                            (required)
-d, --destination <destination_path>
-i, --case-insensitive:                 Specify if the path should be treated as case insensitive
                                            (default case sensitive)
-p, --path <path>:                      Specify the path within the bucket to access
                                            (required)
-r, --region <aws_region>:              Specify the amazon region to locate the bucket.
                                            (default 'us-east-1')
-h, --help:                             Display this help text

"""

BUCKET_KEYS = ["b", "bucket"]
CASE_INSENSITIVE_KEYS = ["c","case-insensitive"]
DESTINATION_KEYS = ["d", "destination"]
PATH_KEYS = ["p","path"]
REGION_KEYS = ["r", "region"]
HELP_KEYS = ["h", "help"]

import boto3, os, sys, botocore
from maestro.internal import module
from botocore.handlers import disable_signing

def find_files(bucket, prefix, case_sensitive = True, connection = None):
    
    if connection is None:
        #Connect to S3
        connection = boto3.resource('s3')

        #Configure anonymous access
        connection.meta.client.meta.events.register('choose-signer.s3.*',disable_signing)

    #Connect to the remote bucket
    remote_bucket = connection.Bucket(bucket)

    try:
        connection.meta.client.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            raise DownloadError("Unable to conenct to remote bucket! Please verify bucket with name " + str(bucket) + " exists.")
        elif error_code == 403:
            raise DownloadError("Unable to access s3 bucket with anonymous credentials, please verify the bucket provides 'List' permissions to 'Everyone'.")
        else:
            raise e
        
  
    #Look for matching files if case insensitive mode 
    if not case_sensitive:
        files = list()
        for object in remote_bucket.objects.all():
            if object.key.lower().startswith(prefix.lower()):
                files.append(object)
    else: #If we're case sensitive, it may return nothing
        files = remote_bucket.objects.filter(Prefix=prefix)

    return files
    
class DownloadError(Exception):
    pass

class AsyncS3Downloader(module.AsyncModule):

        bucket_name = None
        case_insensitive = False
        destination_path = None
        path = None
        region = None

        def run(self,kwargs):
            try:
                self.__parse_kwargs__(kwargs)
                self.__verify_arguments__()
                self.download()
            except Exception as e:
                self.exception = e
                print str(e)

        def __parse_kwargs__(self,kwargs):
            for key, val in kwargs.iteritems():
                if key in HELP_KEYS:
                    self.help()
                elif key in BUCKET_KEYS:
                    self.bucket_name = val
                elif key in CASE_INSENSITIVE_KEYS:
                    self.case_insensitive = True
                elif key in DESTINATION_KEYS:
                    self.destination_path = val
                elif key in PATH_KEYS:
                    self.path = val
                elif key in REGION_KEYS:
                    self.region = val

        def __verify_arguments__(self):
            if self.bucket_name is None:
                raise DownloadError("You need to specify a bucket name!")
            if self.path is None:
                raise DownloadError("You need to specify a prefix or path to the files!")
            if self.region is None:
                self.region = 'us-east-1'

        def download(self):
            #Connect to S3
            s3 = boto3.resource('s3')

            #Configure anonymous access
            s3.meta.client.meta.events.register('choose-signer.s3.*',disable_signing)
            
            self.log("Finding files...")
            found_files = find_files(self.bucket_name, self.path, case_sensitive = not self.case_insensitive, connection = s3)
            
            #Stupid s3 can't provide a length to their collections...
            count = 0

            #Loop through found files
            for obj in found_files:
                if obj.key.endswith("/"):
                    continue
                destination = self.destination_path

                #Case: Provided path exists
                if os.path.exists(destination):
                    #Case Provided path is a directory
                    if os.path.isdir(destination):
                        #Append file name to directory
                        destination = os.path.join(destination,os.path.split(obj.key)[1])
                    #Case Provided path is a file
                    else:
                        #TODO: do something
                        print "Overwriting file: " + str(destination)
                #Case: Provided path does not exist
                else:
                    #Case: Provided path ends with a path seperator
                    if destination.endswith(os.sep):
                        try:
                            #Make directories, and append file name
                            os.makedirs(destination)
                            destination = os.path.join(destination,os.path.split(obj.key)[1])
                        except OSError:
                            raise DownloadError("Unable to create directories for file: " + destination)
                    #Case: Provided path looks like it's a file
                    else:
                        #Check if parent directories exist, and if they don't, attempt to create them
                         head, tail = os.path.split(destination)
                         if not os.path.exists(head):
                            os.makedirs(head)
                print "Downloading... " + obj.key
                s3.meta.client.download_file(self.bucket_name, obj.key, destination)
                print "Downloaded " + obj.key
                
                #Increment counter
                count += 1

            if count == 0:
                raise DownloadError("No files found matching " + self.path)

if __name__ == "__main__":
    import time
    current_key = None
    keyvals = dict()

    for arg in sys.argv[1:]:
        if current_key is None:
            if arg.startswith('-'):
                current_key = arg.lstrip('-')
            continue
        if not arg.startswith("-"):
            keyvals[current_key] = arg
            current_key = None
        else:
            keyvals[current_key] = None
            current_key = arg.lstrip('-')
    if current_key is not None and current_key not in keyvals.keys():
        keyvals[current_key] = ""
    
    s3dl = AsyncS3Downloader(None)
    s3dl.start(keyvals)
    while s3dl.status != module.DONE:
        if s3dl.exception is not None:
            raise s3dl.exception
        time.sleep(2)
    if s3dl.exception is not None:
        raise s3dl.exception


