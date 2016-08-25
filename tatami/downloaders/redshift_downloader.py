import six
import psycopg2

from tatami.downloader import Downloader

class RedshiftDownloader(downloader.Downloader):
    def __init__(self, table_name, query, data_directory_path='./tmp'):
        super(RedshiftDownloader, self).__init__()

    def download(self, dataset_dir, target_dir):
        pass

