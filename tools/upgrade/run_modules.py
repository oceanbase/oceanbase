#!/usr/bin/env python
# -*- coding: utf-8 -*-

ALL_MODULE = 'all'

# module for upgrade_pre.py
MODULE_BEGIN_UPGRADE          = 'begin_upgrade'
MODULE_BEGIN_ROLLING_UPGRADE  = 'begin_rolling_upgrade'
MODULE_SPECIAL_ACTION         = 'special_action'
#MODULE_HEALTH_CHECK          = 'health_check'

# module for upgrade_post.py
MODULE_HEALTH_CHECK           = 'health_check'
MODULE_END_ROLLING_UPGRADE    = 'end_rolling_upgrade'
MODULE_TENANT_UPRADE          = 'tenant_upgrade'
MODULE_END_UPRADE             = 'end_upgrade'
MODULE_POST_CHECK             = 'post_check'

def get_all_module_set():
  import run_modules
  module_set = set([])
  attrs_from_run_module = dir(run_modules)
  for attr in attrs_from_run_module:
    if attr.startswith('MODULE_'):
      module = getattr(run_modules, attr)
      module_set.add(module)
  return module_set

