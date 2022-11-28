import boto3
import os
from botocore.exceptions import UnauthorizedSSOTokenError
from boto3.exceptions import S3UploadFailedError


from utils import define_logger


class BucketController:
    def __init__(self, config):
        self.log = define_logger()
        self.config = config
        self.session = self._get_session()
        self._s3 = self._get_resource()
        self.bucket = None

    def _get_session(self):
        """ Method to create an AWS session
        :return: a session object
        """
        try:
            session = boto3.Session(profile_name=self.config.get('s3_profile_name'))
        except UnauthorizedSSOTokenError as ex:
            self.log.error(f"Cannot login to S3 {ex} - exiting")
            exit(1)
        except Exception as ex:
            self.log.error(f"Cannot login to S3 {ex} - exiting")
            exit(1)
        else:
            return session

    def _get_resource(self):
        """ Method to get the resouce
        :return: an AWS resource
        """
        return self.session.resource("s3")

    @property
    def bucket_list(self):
        """ Property to get all existing S3 buckets
        :return: a list with existing bucket names
        """
        buckets = [bucket.name for bucket in self._s3.buckets.all()]
        return buckets

    def create_bucket_obj(self):
        """ Method to create a bucket object
        Method to create a bucket object if bucket_name provided in JSON app config matches an existing S3 bucket.
        If not the program closes.
        The bucket object is created as an object attribute (self.bucket)
        :return: None
        """
        if self.config['bucket_name'] not in self.bucket_list:  # Exit if bucket not found in S3
            self.log.error(f"Bucket {self.config['bucket_name']} not found in S3. Program will close")
            exit(1)
        self.bucket = self._s3.Bucket(self.config['bucket_name'])

    def upload_json_to_s3(self, json_file):
        """ Upload a file to S3
        Method that uploads a file to S3 assigning the application/json content type.
        The file provided as input is opened as byte, read, and dropped in S3.
        put_object() method allows to perform that task.
        :param json_file:
        :return: True if operation was successful False otherwise
        """
        try:
            self.log.debug(f"Uploading file {json_file}")
            with open(json_file, "rb") as f:
                self.bucket.put_object(
                    Body=f,  # read the file and load it
                    Key=os.path.split(json_file)[1],  # Path(row['JSON']).stem + ".json",
                    ContentType="application/json")
        except S3UploadFailedError as S3UploadEx:
            self.log.error(f"Failed to upload file : {S3UploadEx}")
            return False
        except Exception as ex:
            self.log.error(f"Failed to upload file : {ex}")
            return False
        else:
            self.log.info(f"Successfully uploaded file {json_file}")
            return True
