#!/usr/bin/env python
# -*- coding: utf-8 -*-

ALL_MODULE = 'all'

MODULE_DDL = 'ddl'
MODULE_NORMAL_DML = 'normal_dml'
MODULE_EACH_TENANT_DML = 'each_tenant_dml'
MODULE_EACH_TENANT_DDL = 'each_tenant_ddl'
MODULE_SYSTEM_VARIABLE_DML = 'system_variable_dml'
MODULE_SPECIAL_ACTION = 'special_action'

def get_all_module_set():
  import run_modules
  module_set = set([])
  attrs_from_run_module = dir(run_modules)
  for attr in attrs_from_run_module:
    if attr.startswith('MODULE_'):
      module = getattr(run_modules, attr)
      module_set.add(module)
  return module_set

