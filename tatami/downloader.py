import six
import os
import shutil

class Downloader(object):
    def maybe_download(self, dataset_name, target_dir, force=False):
        local_directory_path = "%s/%s" % (target_dir, dataset_name)

        if force and os.path.exists(local_directory_path):
            print('Removing existing local files:', local_directory_path)

            if input("Shall I (Y/n) ") == 'Y':
                shutil.rmtree(local_directory_path)

        if not os.path.exists(local_directory_path):
            print('Attempting to download:', dataset_name)
            self.download(dataset_name, local_directory_path)
        else:
            print('Using cache:', local_directory_path)

    def download(self, dataset_name, target_dir):
        raise NotImplementedError
