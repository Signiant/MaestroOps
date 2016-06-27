"""
Contains some S3 helper methods and classes, currently extending boto
"""


import boto3, os, sys, botocore, traceback, urlparse
from botocore.handlers import disable_signing
from botocore import UNSIGNED
from botocore.client import Config
from ..core import module
from ..tools import file

def find_files(bucket, prefix, case_sensitive = True, connection = None, anonymous=True):
    """
    find_files will connect and return files found in bucket with prefix, all other keys are ignored.

    Optional: case_sensitive, connection

    Returns a boto3 objectCollection containing matching files, or an empty collection. Will not return any non-file keys.
    Will raise a DownloadError with an appropriate error message when unable to return a collection.
    """

    s3client = None
    if connection is None:
        try:
            connection = get_s3_connection(anonymous=False)
            s3client = boto3.client('s3')
        except:
            connection = get_s3_connection(anonymous=True)
            s3client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    if anonymous:
        s3client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    else:
        s3client = boto3.client('s3')

    #Verify we can connect to remote bucket
    verify_bucket(bucket, connection=connection)

    #Connect to the remote bucket
    remote_bucket = connection.Bucket(bucket)

    #List of returned files
    files = list()

    #Look for matching files if case insensitive mode
    if not case_sensitive:
        #Iterate over objects, and append only ones that match lower case and don't end with '/'
        for obj in remote_bucket.objects.all():
            if obj.key.lower().startswith(prefix.lower()) and not obj.key.endswith("/"):
                objsum = s3client.get_object(Bucket=bucket, Key=obj.key)["ETag"][1:-1]
                files.append((obj, objsum))
    else: #If we're case sensitive, just use the filter
        files = remote_bucket.objects.filter(Prefix=prefix)
        sum_files = list()
        for f in files:
            objsum = s3client.get_object(Bucket=bucket, Key=f.key)["ETag"][1:-1]
            sum_files.append((f, objsum))
        files = sum_files

    return files

def get_s3_connection(anonymous = True):
    """
    Returns an s3 connection object. Configures anonymous access by default.
    """

    #Connect to S3
    connection = boto3.resource('s3')

    if anonymous is True:
        #Configure anonymous access
        connection.meta.client.meta.events.register('choose-signer.s3.*',disable_signing)

    return connection

def parse_s3_url(url):
    """
    Parses a url with format s3://bucket/prefix into a bucket and prefix
    """
    if not url.startswith("s3://"):
            raise ValueError("The provided URL does not follow s3://{bucket_name}/{path}")

    #Parse into bucket and prefix
    bucket = ""
    prefix = ""
    for index, char in enumerate(url[5:]):
        #Everything before this char should be the bucket name
        if char == "/":
            #Take rest of path less '/' and s3://
            prefix = url[(index+6):]
            break
        #Build bucket string
        bucket += char

    if not bucket:
        raise ValueError("The provided URL " + str(url) + " is not valid. Please enter a URL following s3://{bucket_name}/path")

    if not prefix:
        prefix = "/"

    return bucket, prefix

def join_s3_url(prefix, *elements):
    url_builder = prefix
    for element in elements:
        if url_builder[-1] == "/":
            url_builder += element
        else:
            url_builder += "/" + element
    return url_builder

def verify_bucket(bucket_name,connection = None):
    """
    Verifies that you have read access to bucket with bucket_name and current credentials.
    """

    if connection is None:
        try:
            connection = get_s3_connection(anonymous=False)
        except:
            connection = get_s3_connection(anonymous=True)

    #Verify we can connect to the bucket
    try:
        connection.meta.client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            raise DownloadError("Unable to connect to remote bucket. Please verify bucket with name " + str(bucket_name) + " exists.")
        elif error_code == 403:
            raise DownloadError("Unable to connect to remote bucket. Access is forbidden.")
        else:
            raise e


class DownloadError(Exception):
    pass

### MODULES ####

BUCKET_KEYS = ["b", "bucket"]
CASE_INSENSITIVE_KEYS = ["i","case-insensitive"]
DESTINATION_KEYS = ["d", "destination"]
PATH_KEYS = ["p","prefix"]
REGION_KEYS = ["r", "region"]
SOURCE_KEYS = ["s", "source"]
HELP_KEYS = ["h", "help"]

class AsyncS3Downloader(module.AsyncModule):
        """
        AsyncS3Downloader will download a set of files identified by 'prefix' to 'destination_path' from 'bucket_name' in 'region'. You may optionally use a source URL in the form of 's3://bucket/prefix' or set 'case_insensitive' to match a path/prefix ignoring case.

        """
        HELPTEXT = """
                ----- S3 Downloader -----

The S3 Downloader will retreive a file from an S3 bucket with anonymous
read access.

-b, --bucket <bucket_name>:         Specify the name of the bucket to access
                                            (required)
-d, --destination <dest_path>       Specify the destination on local filesystem
                                            (default  './')
-i, --case-insensitive:             Specify if the path should be treated as case insensitive
                                            (default case sensitive)
-p, --prefix <path>:                Specify the path prefix within the bucket to access
                                            (default '/")
-r, --region <aws_region>:          Specify the amazon region to locate the bucket
                                            (default 'us-east-1')
-s, --source <src_url>:             Specify the source URL
                                            (ignored when bucket is set)
-h, --help:                         Display this help text

"""

        bucket_name = None
        case_insensitive = False
        destination_path = None
        prefix = None
        region = None
        source_url = None
        anonymous = None

        def run(self,kwargs):
            if kwargs is not None and len(kwargs) > 0:
                if not self.__parse_kwargs__(kwargs):
                    return
            self.__verify_arguments__()
            return self.download()

        def __parse_kwargs__(self,kwargs):
            if kwargs is None:
                return True
            if len(kwargs) == 0 and self.bucket_name is None and self.source_url is None:
                return self.help()
            for key, val in kwargs.iteritems():
                if key in HELP_KEYS:
                    return self.help()
                elif key in BUCKET_KEYS:
                    self.bucket_name = val
                elif key in CASE_INSENSITIVE_KEYS:
                    self.case_insensitive = True
                elif key in DESTINATION_KEYS:
                    self.destination_path = val
                elif key in PATH_KEYS:
                    self.prefix = val
                elif key in REGION_KEYS:
                    self.region = val
                elif key in SOURCE_KEYS:
                    self.source_url = val
                else:
                    print("Invalid option: " + str(val))
                    return False
                return True
        def __verify_arguments__(self):
            if self.bucket_name is None and self.source_url is None:
                raise DownloadError("You need to specify a bucket name or a source url.")
            if self.prefix is None:
                self.prefix = "/"
            #TODO: Add region mapping
            if self.region is None:
                self.region = 'us-east-1'
            if self.destination_path is None:
                self.destination_path = "./"

        def download(self):
            #Determine if we're parsing a url
            if self.bucket_name is None:
                self.bucket_name, self.prefix = parse_s3_url(self.source_url)
            #Connect to S3
            s3 = None
            try:
                s3 = get_s3_connection(anonymous=False)
                #Verify bucket
                verify_bucket(self.bucket_name, s3)
                self.anonymous=False
            except:
                s3 = get_s3_connection(anonymous=True)
                self.anonymous=True
                #Verify bucket
                verify_bucket(self.bucket_name, s3)

            #Stupid s3 can't provide a length to their collections...
            count = 0
            #Return value
            destination_files = list()
            #Loop through found files
            for obj, checksum in find_files(self.bucket_name, self.prefix, case_sensitive = not self.case_insensitive, connection = s3, anonymous=self.anonymous):
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
                        print ("Unconfirmed case")
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
                #Perform download
                s3.meta.client.download_file(self.bucket_name, obj.key, destination)

                #Append downloaded file names
                destination_files.append((os.path.abspath(destination), checksum))

                #Increment counter
                count += 1

            if count == 0:
                raise DownloadError("No files found matching " + self.prefix)

            return destination_files

if __name__ == "__main__":
    import time,traceback
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
        time.sleep(1)
        print ("Waiting...")
    if s3dl.exception is not None:
        raise s3dl.exception
