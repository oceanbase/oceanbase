# coding: utf-8
# OceanBase Deploy.
# Copyright (C) 2021 OceanBase
#
# This file is part of OceanBase Deploy.
#
# OceanBase Deploy is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# OceanBase Deploy is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with OceanBase Deploy.  If not, see <https://www.gnu.org/licenses/>.


from __future__ import absolute_import, division, print_function

import os
from tool import DirectoryUtil
from stdio import SafeStdio


class Manager(SafeStdio):

    RELATIVE_PATH = ''

    def __init__(self, home_path, stdio=None):
        self.stdio = stdio
        self.path = os.path.join(home_path, self.RELATIVE_PATH)
        self.is_init = self._mkdir(self.path)

    def _mkdir(self, path):
        return DirectoryUtil.mkdir(path, stdio=self.stdio)

    def _rm(self, path):
        return DirectoryUtil.rm(path, self.stdio)
