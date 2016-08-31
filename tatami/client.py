import six

import os
import json

from six.moves.urllib.request import urlopen
from tatami.downloaders import *

class Client(object):
    def __init__(self, tatami_host,
            data_directory_path="%s/.tatami/datasets" % os.environ.get("HOME")):
        self.__tatami_host = tatami_host
        self.__data_directory_path = data_directory_path

    def load_dataset(self, dataset_name, force=False):
        downloader = self._downloader_for_dataset_name(dataset_name)
        return downloader.maybe_download(dataset_name, self.__data_directory_path, force)

    def get_path_for_dataset(self, dataset_name):
        return "%s/%s" % (self.__data_directory_path, dataset_name)

    def _downloader_for_dataset_name(self, dataset_name):
        meta_data = self._retrieve_meta_data_for_dataset_name(dataset_name)

        if meta_data['type'] == 'S3Dataset':
            downloader = S3Downloader(
                    meta_data['bucket_name'],
                    meta_data['prefix'])
        # elif meta_data['type'] == 'RedshiftDataset':
        #    downloader = RedshiftDownloader(
        #            )
        else:
            # XXX: HogehogeException
            raise Exception("Unrecognized datasource type %s" % meta_data['type'])

        return downloader

    def _retrieve_meta_data_for_dataset_name(self, dataset_name):
        # XXX: Not Found Exception
        request_url = "%s/datasets/%s.json" % (self.__tatami_host, dataset_name)

        return json.loads(urlopen(request_url).read().decode('utf-8'))
