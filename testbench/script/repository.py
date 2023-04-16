from __future__ import absolute_import, division, print_function

import os

from tool import FileUtil
from manager import Manager


class RepositoryManager(Manager):

    RELATIVE_PATH = 'repository'

    def __init__(self, home_path, stdio=None):
        super(RepositoryManager, self).__init__(home_path, stdio=stdio)

    def get_repository(self, name):
        path = os.path.join(self.path, name)
        if not FileUtil.isexecutable(path):
            self.stdio.error('Cannot find repository for %s' % name)
            return None
        return path
