import os.path
from os import path
import shutil


class ValidAction:
    def projectIsset(self, file):
        if path.exists(file):
            return True
        return False

    def checkCrawlCache(self, root):
        dirName = os.path.realpath('.') + '/crawls/crowl/data/' + root
        print(dirName)
        if path.exists(dirName):
            shutil.rmtree(dirName)
