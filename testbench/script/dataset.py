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
import csv

from tool import FileUtil, YamlLoader, OrderedDict
from manager import Manager

yaml = YamlLoader()

class DatasetManager(Manager):
  RELATIVE_PATH = "dataset/"
  DATASET_YAML_NAME = "config.yaml"
  DATASET_NAME = "testbench.csv"
  
  def __init__(self, home_path, lock_manager=None, stdio=None):
    super(DatasetManager, self).__init__(home_path, stdio)
    self._component = None
    self._component_config = None
    self._dataset_config = OrderedDict()
    self._lock_manager = lock_manager
    self.stdio = stdio
    if self.yaml_init:
      self._load_config()
        
  @property
  def component(self):
    return self._component

  @property
  def yaml_path(self):
    return os.path.join(self.path, self.DATASET_YAML_NAME)

  @property
  def dataset_path(self):
    return os.path.join(self.path, self.DATASET_NAME)
  
  @property
  def yaml_init(self):
    return os.path.exists(self.yaml_path)
  
  @property
  def dataset_init(self):
    return os.path.exists(self.dataset_path)
  
  @property
  def dataset_config(self):
    return self._dataset_config
  
  def _lock(self, read_only=False):
    if self._lock_manager:
      if read_only:
        return self._lock_manager.dataset_sh_lock()
      else:
        return self._lock_manager.dataset_ex_lock()
    return True
  
  def create_yaml(self, src_yaml_path):
    self._lock()
    dst_yaml_path = self.yaml_path
    if not FileUtil.copy(src_yaml_path, dst_yaml_path, self.stdio):
      self.stdio.error(
          "Fail to copy yaml config file {} to {}.".format(
              src_yaml_path, dst_yaml_path
          )
      )
      return False
    self.stdio.verbose("copy yaml config file to {}.".format(dst_yaml_path))
    self._load_config()
    return True
  
  def _load_config(self):
    self._lock(read_only=True)
    yaml_loader = YamlLoader(stdio=self.stdio)
    if not self._parse_dataset_config(yaml_loader):
      return False
    return True
  
  def _parse_dataset_config(self, yaml_loader):
    f = open(self.yaml_path, "rb")
    src_data = yaml_loader.load(f)
    if len(src_data.keys()) <= 0:
      self.stdio.error(
        "There should be exactly one component in the dataset configuration file."
      )
      return False
    self._component = list(src_data.keys())[0]
    self._component_config = src_data[self._component]
    if "global" not in self._component_config:
      self.stdio.warn(
        "Cannot find global parameters in the cluster configuration file."
      )
    else:
      self._dataset_config = self._component_config["global"]
    
    for name, _ in self._component_config.items():
      if name != "global":
        self.stdio.warn(
          "non-global items in the dataset configuration file is not allowed."
        )
    return True
    
  def _remove_dataset(self):
    if not os.path.exists(self.dataset_path):
      return True
    if not FileUtil.rm(self.dataset_path):
      self.stdio.error("Fail to clear existing datasets")
      return False
    return True
      
  def generate_dataset(self):
    if not self._remove_dataset():
      return False
    try:
      csv_file = FileUtil.open(self.dataset_path, "w")
      csv_writer = csv.writer(csv_file)
      for partition in range(self._dataset_config["partitions"]):
        for row in range(self._dataset_config["rows"]):
          csv_writer.writerow([partition, row, 0, 0, 0])
    except Exception as e:
      self.stdio.error(
        "Fail to write csv dataset into the file, exception: {}.".format(e.message)
      )
      return False
    return True