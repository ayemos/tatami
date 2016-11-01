import six
import os
from multiprocessing import Process

from boto3 import resource, client

from tatami import downloader

class S3Downloader(downloader.Downloader):
    def __init__(self, bucket_name, root_prefix, data_directory_path='./tmp'):
        super(S3Downloader, self).__init__()

        self.__resource = None
        self.__client = None
        self.__bucket_name = bucket_name
        self.__root_prefix = root_prefix
        self.__data_directory_path = data_directory_path

    def download(self, dataset_name, target_dir):
        return self.__download_dir(self.__root_prefix, target_dir)

    def __download_dir(self, prefix, target_dir, num_threads=4):
        paginator = self.__get_client().get_paginator('list_objects')

        for result in paginator.paginate(Bucket=self.__bucket_name,
                Delimiter='/', Prefix=prefix):

            if result.get('CommonPrefixes') is not None:
                for subdir in result.get('CommonPrefixes'):
                    self.__download_dir(subdir.get('Prefix'), target_dir)
            elif result.get('Contents') is not None:
                for content in result.get('Contents'):
                    if content.get('Size') > 0:
                        local_file_path = target_dir + os.sep + os.path.basename(content.get('Key'))

                        if not os.path.exists(os.path.dirname(local_file_path)):
                            os.makedirs(os.path.dirname(local_file_path))

                        proc = Process(
                                target=self._download_file_for_key,
                                args=(content.get('Key'), local_file_path))

                        proc.start()

    def _download_file_for_key(self, key, path):
        self.__get_resource().Bucket(self.__bucket_name).download_file(key, path)

    def __get_resource(self):
        if self.__resource is None:
            self.__resource = resource('s3')

        return self.__resource

    def __get_client(self):
        if self.__client is None:
            self.__client = client('s3')

        return self.__client
