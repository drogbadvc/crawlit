import os
from urllib.parse import urlparse


class createDep:

    def path(self):
        return os.path.realpath('.') + '/crowl/data/'

    def url_to_name(self, url):
        t = urlparse(url).netloc
        return '.'.join(t.split('.')[-2:]).replace('.', '-')

    def mkdir(self, url):
        try:
            os.mkdir(self.path() + self.url_to_name(url), 0o777)
        except OSError as error:
            print(error)

    def pathProject(self, url):
        return self.path() + self.url_to_name(url)
