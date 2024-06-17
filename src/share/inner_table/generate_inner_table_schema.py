#!/bin/env python2
# -*- coding: utf-8 -*-

# Copyright 2014 - 2018 Alibaba Inc. All Rights Reserved.
# Author:
#  config file is ob_inner_table_schema_def.py
#  shell> python2.6 generate_inner_table_schema.py
#

import copy
from ob_inner_table_init_data import *
import StringIO
import re
import os
import glob
import sys

kv_core_table_id         = int(1)
max_core_table_id        = int(100)
max_sys_table_id         = int(10000)
max_ob_virtual_table_id  = int(15000)
max_ora_virtual_table_id = int(20000)
max_sys_view_id          = int(30000)
base_lob_meta_table_id   = int(50000)
base_lob_piece_table_id  = int(60000)
max_lob_table_id         = int(70000)
min_sys_index_id         = int(100000)
max_core_index_id        = int(101000)
max_sys_index_id         = int(200000)

min_shadow_column_id     = int(32767)

def is_core_table(table_id):
  table_id = int(table_id)
  return table_id > 0 and table_id < max_core_table_id

def is_sys_table(table_id):
  table_id = int(table_id)
  return table_id > 0 and table_id < max_sys_table_id

def is_mysql_virtual_table(table_id):
  table_id = int(table_id)
  return table_id > max_sys_table_id and table_id < max_ob_virtual_table_id

def is_ora_virtual_table(table_id):
  table_id = int(table_id)
  return table_id > max_ob_virtual_table_id and table_id < max_ora_virtual_table_id

def is_virtual_table(table_id):
  table_id = int(table_id)
  return table_id > max_sys_table_id and table_id < max_ora_virtual_table_id

def is_sys_view(table_id):
  table_id = int(table_id)
  return table_id > max_ora_virtual_table_id and table_id < max_sys_view_id

def is_lob_table(table_id):
  table_id = int(table_id)
  return (table_id > base_lob_meta_table_id) and (table_id < max_lob_table_id)

def is_core_index_table(table_id):
  table_id = int(table_id)
  return (table_id > min_sys_index_id) and (table_id < max_core_index_id)

def is_sys_index_table(table_id):
  table_id = int(table_id)
  return (table_id > min_sys_index_id) and (table_id < max_sys_index_id)

new_keywords = {}
cpp_f = None
h_f = None
fileds = None
default_filed_values = None
table_name_ids = []
table_name_postfix_ids = []
table_name_postfix_table_names = []
index_name_ids = []
table_index=[]
lob_aux_ids = []
tenant_space_tables = []
tenant_space_table_names = []
only_rs_vtables = []
cluster_distributed_vtables = []
tenant_distributed_vtables = []
column_def_enum_array = []
all_def_keywords =  {}
all_agent_virtual_tables = []
all_iterate_virtual_tables = []
all_iterate_private_virtual_tables = []
all_ora_mapping_virtual_table_org_tables = []
all_ora_mapping_virtual_tables = []
real_table_virtual_table_names = []
cluster_private_tables = []
all_only_sys_table_name = {}
mysql_compat_agent_tables = {}
column_collation = 'CS_TYPE_INVALID'
# virtual tables only accessible by sys tenant or sys views.
restrict_access_virtual_tables = []
is_oracle_sys_table = False
sys_index_tables = []
copyright = """/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
"""

def print_method_start(table_name):
  global cpp_f
  head = """int ObInnerTableSchema::{0}_schema(ObTableSchema &table_schema)
{{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
"""
  # use {{ for string.foramt. string.format will translate {{ to {
  cpp_f.write(head.format(table_name.replace('$', '_').lower().strip('_')))

def add_method_end():
  global cpp_f
  end = """
  table_schema.set_max_used_column_id(column_id);
  return ret;
}

"""
  cpp_f.write(end)

def add_index_method_end(num_index):
  global cpp_f
  end = """
  table_schema.set_max_used_column_id(column_id + {0});
  return ret;
}}

"""
  cpp_f.write(end.format(str(num_index)))

def print_default_column(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, default_value, column_id, is_hidden, is_storing_column):
  global cpp_f
  set_op = "";
  if "NULL" == default_value or "null" == default_value:
    set_op = 'set_null()'
  elif column_type == 'ObIntType':
    set_op = 'set_int({0})'.format(default_value)
  elif column_type == 'ObUInt64Type':
    set_op = 'set_uint64({0})'.format(default_value)
  elif column_type == 'ObTinyIntType':
    set_op = 'set_tinyint({0})'.format(default_value)
  elif column_type == 'ObVarcharType':
      if column_collation_type == "CS_TYPE_BINARY":
        set_op = 'set_varbinary(ObString::make_string("{0}"))'.format(default_value)
      else:
        set_op = 'set_varchar(ObString::make_string("{0}"))'.format(default_value)
  elif column_type == 'ObTimestampType':
    if (default_value == 'CURRENT_TIMESTAMP') or (default_value == 'current_timestmap'):
      set_op = 'set_timestamp(ObTimeUtility::current_time())'
    else:
      set_op = 'set_timestamp({0})'.format(default_value)
  elif column_type == 'ObLongTextType':
    set_op = 'set_lob_value(ObLongTextType, "{0}", static_cast<int32_t>(strlen("{0}")))'.format(default_value)
    if column_collation_type == "CS_TYPE_BINARY":
      set_op += '; {0}_default.set_collation_type(CS_TYPE_BINARY);'.format(column_name.lower())
  else:
    raise IOError("ERROR column format: column_name={0} column_type={1}\n".format(column_name, column_type))
  if is_hidden == 'true' or is_storing_column == 'true':
    if column_id != 0:
      ## index
      line = """
  if (OB_SUCC(ret)) {{
    ObObj {12}_default;
    {12}_default.{13};
    ADD_COLUMN_SCHEMA_T_WITH_COLUMN_FLAGS("{0}", //column_name
      column_id + {1}, //column_id
      {2}, //rowkey_id
      {3}, //index_id
      {4}, //part_key_pos
      {5}, //column_type
      {6}, //column_collation_type
      {7}, //column_length
      {8}, //column_precision
      {9}, //column_scale
      {10}, //is_nullable
      {11}, //is_autoincrement
      {12}_default,
      {12}_default, //default_value
      {14}, //is_hidden
      {15}); //is_storing_column 
  }}
"""
      cpp_f.write(line.format(column_name, column_id, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, column_name.lower(), set_op, is_hidden, is_storing_column))
    else:
      line = """
  if (OB_SUCC(ret)) {{
    ObObj {11}_default;
    {11}_default.{12};
    ADD_COLUMN_SCHEMA_T_WITH_COLUMN_FLAGS("{0}", //column_name
      ++column_id, //column_id
      {1}, //rowkey_id
      {2}, //index_id
      {3}, //part_key_pos
      {4}, //column_type
      {5}, //column_collation_type
      {6}, //column_length
      {7}, //column_precision
      {8}, //column_scale
      {9}, //is_nullable
      {10}, //is_autoincrement
      {11}_default,
      {11}_default, //default_value
      {13}, //is_hidden
      {14}); //is_storing_column
  }}
"""
      cpp_f.write(line.format(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, column_name.lower(),set_op, is_hidden, is_storing_column))
  else:
    if column_id != 0:
      ## index
      line = """
  if (OB_SUCC(ret)) {{
    ObObj {12}_default;
    {12}_default.{13};
    ADD_COLUMN_SCHEMA_T("{0}", //column_name
      column_id + {1}, //column_id
      {2}, //rowkey_id
      {3}, //index_id
      {4}, //part_key_pos
      {5}, //column_type
      {6}, //column_collation_type
      {7}, //column_length
      {8}, //column_precision
      {9}, //column_scale
      {10}, //is_nullable
      {11}, //is_autoincrement
      {12}_default,
      {12}_default); //default_value
  }}
"""
      cpp_f.write(line.format(column_name, column_id, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, column_name.lower(), set_op))
    else:
      line = """
  if (OB_SUCC(ret)) {{
    ObObj {11}_default;
    {11}_default.{12};
    ADD_COLUMN_SCHEMA_T("{0}", //column_name
      ++column_id, //column_id
      {1}, //rowkey_id
      {2}, //index_id
      {3}, //part_key_pos
      {4}, //column_type
      {5}, //column_collation_type
      {6}, //column_length
      {7}, //column_precision
      {8}, //column_scale
      {9}, //is_nullable
      {10}, //is_autoincrement
      {11}_default,
      {11}_default); //default_value
  }}
"""
      cpp_f.write(line.format(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, column_name.lower(),set_op))
    
def print_column(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, column_id, is_hidden, is_storing_column):
  global cpp_f

  if is_hidden == 'true' or is_storing_column == 'true':
    if column_id != 0:
      ## index
      line = """
  if (OB_SUCC(ret)) {{
    ADD_COLUMN_SCHEMA_WITH_COLUMN_FLAGS("{0}", //column_name
      column_id + {1}, //column_id
      {2}, //rowkey_id
      {3}, //index_id
      {4}, //part_key_pos
      {5}, //column_type
      {6}, //column_collation_type
      {7}, //column_length
      {8}, //column_precision
      {9}, //column_scale
      {10},//is_nullable
      {11},//is_autoincrement
      {12},//is_hidden
      {13});//is_storing_column
  }}
"""
      cpp_f.write(line.format(column_name, column_id, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement,is_hidden ,is_storing_column))
    else:
      line = """
  if (OB_SUCC(ret)) {{
    ADD_COLUMN_SCHEMA_WITH_COLUMN_FLAGS("{0}", //column_name
      ++column_id, //column_id
      {1}, //rowkey_id
      {2}, //index_id
      {3}, //part_key_pos
      {4}, //column_type
      {5}, //column_collation_type
      {6}, //column_length
      {7}, //column_precision
      {8}, //column_scale
      {9}, //is_nullable
      {10},//is_autoincrement
      {11},//is_hidden
      {12});//is_storing_column 
  }}
"""
      cpp_f.write(line.format(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement,is_hidden ,is_storing_column))
  else:
    if column_id != 0:
      ## index
      line = """
  if (OB_SUCC(ret)) {{
    ADD_COLUMN_SCHEMA("{0}", //column_name
      column_id + {1}, //column_id
      {2}, //rowkey_id
      {3}, //index_id
      {4}, //part_key_pos
      {5}, //column_type
      {6}, //column_collation_type
      {7}, //column_length
      {8}, //column_precision
      {9}, //column_scale
      {10},//is_nullable
      {11}); //is_autoincrement
  }}
"""
      cpp_f.write(line.format(column_name, column_id, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement))
    else:
      line = """
  if (OB_SUCC(ret)) {{
    ADD_COLUMN_SCHEMA("{0}", //column_name
      ++column_id, //column_id
      {1}, //rowkey_id
      {2}, //index_id
      {3}, //part_key_pos
      {4}, //column_type
      {5}, //column_collation_type
      {6}, //column_length
      {7}, //column_precision
      {8}, //column_scale
      {9}, //is_nullable
      {10}); //is_autoincrement
  }}
"""
      cpp_f.write(line.format(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement))

    
def print_discard_column(column_name):
  global cpp_f
  line = """
  if (OB_SUCC(ret)) {{
    ++column_id; // for {0}
  }}
"""
  cpp_f.write(line.format(column_name))


def print_timestamp_column(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, column_id, is_on_update_for_timestamp,is_hidden, is_storing_column):
  global cpp_f
  if rowkey_id > 0:
    is_nullable = "false"

  if is_hidden == 'true' or is_storing_column == 'true':
    if column_id != 0:
      if column_scale > 0 :
        line = """
  if (OB_SUCC(ret)) {{
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T_WITH_COLUMN_FLAGS("{0}", //column_name
      column_id + {1}, //column_id
      {2}, //rowkey_id
      {3}, //index_id
      {4}, //part_key_pos
      {5}, //column_type
      {6}, //column_collation_type
      {7}, //column_length
      {8}, //column_precision
      {9}, //column_scale
      {10}, //is_nullable
      {11}, //is_autoincrement
      {12}, //is_on_update_for_timestamp
      gmt_default_null,
      gmt_default,
      {13}, //is_hidden
      {14}); //is_storing_column
  }}
"""
      else :
        line = """
  if (OB_SUCC(ret)) {{
    ADD_COLUMN_SCHEMA_TS_WITH_COLUMN_FLAGS("{0}", //column_name
      column_id + {1}, //column_id
      {2}, //rowkey_id
      {3}, //index_id
      {4}, //part_key_pos
      {5}, //column_type
      {6}, //column_collation_type
      {7}, //column_length
      {8}, //column_precision
      {9}, //column_scale
      {10}, //is_nullable
      {11}, //is_autoincrement
      {12}, //is_on_update_for_timestamp
      {13}, //is_hidden
      {14});//is_storing_column 
  }}
"""
      cpp_f.write(line.format(column_name, column_id, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, is_on_update_for_timestamp,is_hidden, is_storing_column))
    else:
      if column_scale > 0 :
        line = """
  if (OB_SUCC(ret)) {{
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T_WITH_COLUMN_FLAGS("{0}", //column_name
      ++column_id, //column_id
      {1}, //rowkey_id
      {2}, //index_id
      {3}, //part_key_pos
      {4}, //column_type
      {5}, //column_collation_type
      {6}, //column_length
      {7}, //column_precision
      {8}, //column_scale
      {9}, //is_nullable
      {10}, //is_autoincrement
      {11}, //is_on_update_for_timestamp
      gmt_default_null,
      gmt_default,
      {12}, //is_hidden
      {13})//is_storing_column
  }}
"""
      else :
        line = """
  if (OB_SUCC(ret)) {{
    ADD_COLUMN_SCHEMA_TS_WITH_COLUMN_FLAGS("{0}", //column_name
      ++column_id, //column_id
      {1}, //rowkey_id
      {2}, //index_id
      {3}, //part_key_pos
      {4}, //column_type
      {5}, //column_collation_type
      {6}, //column_length
      {7}, //column_precision
      {8}, //column_scale
      {9}, //is_nullable
      {10}, //is_autoincrement
      {11}, //is_on_update_for_timestamp
      {12}, //is_hidden
      {13});//is_storing_column 
  }}
"""
      cpp_f.write(line.format(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, is_on_update_for_timestamp, is_hidden, is_storing_column))
  else:
    if column_id != 0:
      if column_scale > 0 :
        line = """
  if (OB_SUCC(ret)) {{
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("{0}", //column_name
      column_id + {1}, //column_id
      {2}, //rowkey_id
      {3}, //index_id
      {4}, //part_key_pos
      {5}, //column_type
      {6}, //column_collation_type
      {7}, //column_length
      {8}, //column_precision
      {9}, //column_scale
      {10}, //is_nullable
      {11}, //is_autoincrement
      {12}, //is_on_update_for_timestamp
      gmt_default_null,
      gmt_default);
  }}
"""
      else :
        line = """
  if (OB_SUCC(ret)) {{
    ADD_COLUMN_SCHEMA_TS("{0}", //column_name
      column_id + {1}, //column_id
      {2}, //rowkey_id
      {3}, //index_id
      {4}, //part_key_pos
      {5}, //column_type
      {6}, //column_collation_type
      {7}, //column_length
      {8}, //column_precision
      {9}, //column_scale
      {10}, //is_nullable
      {11}, //is_autoincrement
      {12}); //is_on_update_for_timestamp
  }}
"""
      cpp_f.write(line.format(column_name, column_id, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, is_on_update_for_timestamp))
    else:
      if column_scale > 0 :
        line = """
  if (OB_SUCC(ret)) {{
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("{0}", //column_name
      ++column_id, //column_id
      {1}, //rowkey_id
      {2}, //index_id
      {3}, //part_key_pos
      {4}, //column_type
      {5}, //column_collation_type
      {6}, //column_length
      {7}, //column_precision
      {8}, //column_scale
      {9}, //is_nullable
      {10}, //is_autoincrement
      {11}, //is_on_update_for_timestamp
      gmt_default_null,
      gmt_default)
  }}
"""
      else :
        line = """
  if (OB_SUCC(ret)) {{
    ADD_COLUMN_SCHEMA_TS("{0}", //column_name
      ++column_id, //column_id
      {1}, //rowkey_id
      {2}, //index_id
      {3}, //part_key_pos
      {4}, //column_type
      {5}, //column_collation_type
      {6}, //column_length
      {7}, //column_precision
      {8}, //column_scale
      {9}, //is_nullable
      {10}, //is_autoincrement
      {11}); //is_on_update_for_timestamp
  }}
"""
      cpp_f.write(line.format(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, is_on_update_for_timestamp))



def add_gm_columns(columns):
  global cpp_f
  column_length = 'sizeof(int64_t)'
  is_nullable = 'false'
  line = "error"
  for column in columns:
    if column == 'gmt_create':
      line = """
  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType,  //column_type
      CS_TYPE_BINARY,//collation_type
      0, //column length
      -1, //column_precision
      6, //column_scale
      true,//is nullable
      false, //is_autoincrement
      false, //is_on_update_for_timestamp
      gmt_create_default_null,
      gmt_create_default)
  }
"""
    elif column == 'gmt_modified':
      line = """
  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType,  //column_type
      CS_TYPE_BINARY,//collation_type
      0, //column length
      -1, //column_precision
      6, //column_scale
      true,//is nullable
      false, //is_autoincrement
      true, //is_on_update_for_timestamp
      gmt_modified_default_null,
      gmt_modified_default)
  }
"""
    cpp_f.write(line)

def add_column(column, rowkey_id, index_id, part_key_pos, column_id=0, is_hidden='false', is_storing_column='false'):
  global column_collation
  global is_oracle_sys_table
  column_name = None
  column_type = None
  column_collation_type = 'CS_TYPE_INVALID';
  column_length = None
  column_precision = -1
  column_scale = -1
  is_nullable = "false"
  is_autoincrement = "false"
  is_on_update_for_timestamp = "false"
  default_value = None
  if len(column) >= 2:
    column_name = column[0]
    column_type = column[1]
    if column_type == 'int':
      column_type = 'ObIntType'
      column_length = 'sizeof(int64_t)'
    elif column_type[:6] == 'bigint':
      s = column_type.split(':')
      column_type = 'ObIntType'
      column_length = 'sizeof(int64_t)'
      column_precision = 20 if len(s) == 1 else s[1]
      column_scale = 0
    elif column_type[:6] == 'number':
      s = column_type.split(':')
      column_type = 'ObFloatType' if len(s) == 1 else 'ObNumberType'
      column_length = 'sizeof(float)' if len(s) == 1 else 38
      column_precision = -1 if len(s) == 1 else s[1]
      if len(s) == 3:
        column_scale = s[2]
      elif len(s) == 2:
        column_scale = 0
    elif column_type == 'uint':
      column_type = 'ObUInt64Type'
      column_length = 'sizeof(uint64_t)'
    elif column_type == 'uint32':
      column_type = 'ObUInt32Type'
      column_length = 'sizeof(uint32_t)'
    elif column_type == 'double':
      column_type = 'ObDoubleType'
      column_length = 'sizeof(double)'
    elif column_type == 'bool':
      column_type = 'ObTinyIntType'
      column_length = '1'
    elif column_type[:9] == 'timestamp':
      s = column_type.split(':')
      column_type = 'ObTimestampType'
      column_length = 'sizeof(ObPreciseDateTime)'
      if len(s) != 1:
        column_scale = s[1]
        column_length = 0
    elif column_type[:7]  == 'varchar':
      s = column_type.split(':')
      column_type = 'ObVarcharType'
      column_length = s[1]
      if True == is_oracle_sys_table:
        column_precision = 2
      column_collation_type = column_collation;
    elif column_type[:9]  == 'varbinary':
      s = column_type.split(':')
      column_type = 'ObVarcharType'
      column_length = s[1]
      column_collation_type = 'CS_TYPE_BINARY'
    elif column_type == 'otimestamp':
      column_type = 'ObTimestampLTZType'
      column_length = 0
    elif column_type == 'longtext':
      column_type = 'ObLongTextType'
      column_length = 0
    elif column_type == 'longblob':
      column_type = 'ObLongTextType'
      column_length = 0
      column_collation_type = 'CS_TYPE_BINARY'
    elif column_type == 'datetime':
      column_type = 'ObDateTimeType'
      column_length = 0

  if len(column) >= 3:
    is_nullable = column[2]

  if len(column) >= 4:
    default_value = column[3]

  if len(column) >= 5:
    is_autoincrement = column[4]

  if len(column) >= 6:
    is_on_update_for_timestamp = column[5]

  print column_name, rowkey_id, index_id, part_key_pos, column_type, column_length, is_nullable, is_autoincrement, default_value # remove
  if column_name.find("[discard]") == 0:
    print_discard_column(column_name)
  elif column_type == 'ObTimestampType':
    print_timestamp_column(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, column_id, is_on_update_for_timestamp, is_hidden, is_storing_column)
  elif default_value is None:
    print_column(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, column_id, is_hidden, is_storing_column)
  else:
    print_default_column(column_name, rowkey_id, index_id, part_key_pos, column_type, column_collation_type, column_length, column_precision, column_scale, is_nullable, is_autoincrement, default_value, column_id, is_hidden, is_storing_column)

def no_direct_access(keywords):
  global restrict_access_virtual_tables
  tid = table_name2tid(keywords['table_name'] + (keywords.has_key('name_postfix') and keywords['name_postfix'] or ''))
  if tid not in restrict_access_virtual_tables:
    restrict_access_virtual_tables.append(tid)
  return keywords

def find_column_def(keywords, column_name, is_shadow_pk_column):
  i = 1
  if is_shadow_pk_column:
    # for shadow_pk_column, need to find the mapping rowkey column in main table,
    # and use the rowkey column's column_id and data_type.
    for col in keywords['rowkey_columns']:
      if 'shadow_pk_' + str(i - 1) == column_name:
        return (i + min_shadow_column_id, (column_name, col[1], 'true'))
      else:
        i += 1
  else:
    for col in keywords['rowkey_columns']:
      if col[0].upper() == column_name.upper():
        return (i, col)
      else:
        i += 1
    for col in keywords['normal_columns']:
      if col[0].upper() == column_name.upper():
        return (i, col)
      else:
        i += 1

# For specified index column, rowkey_position != 0 and index_position != 0.
def add_index_column(keywords, rowkey_id, index_id, column):
  (idx, column_def) = find_column_def(keywords, column, False)
  add_column(column_def, rowkey_id, index_id, 0, idx)
  return idx

# For shadow pk column, rowkey_position != 0 and index_position = 0.
def add_shadow_pk_column(keywords, rowkey_id, column):
  (idx, column_def) = find_column_def(keywords, column, True)
  add_column(column_def, rowkey_id, 0, 0, idx, is_hidden='true')
  return idx

def add_index_columns(columns, **keywords):
  rowkey_id = 1
  is_unique_index = keywords['index_type'] == 'INDEX_TYPE_UNIQUE_LOCAL'
  max_used_column_idx = 1
  if is_unique_index:
    # specified index columns.
    for column in columns:
      column_idx = add_index_column(keywords, rowkey_id, rowkey_id, column)
      max_used_column_idx = max(max_used_column_idx, column_idx)
      rowkey_id += 1

    # generate shadow pk column whose number equals to rowkeys' number.
    shadow_pk_col_idx = 0
    for col in keywords['rowkey_columns']:
      shadow_pk_col_name = 'shadow_pk_' + str(shadow_pk_col_idx)
      column_idx = add_shadow_pk_column(keywords, rowkey_id, shadow_pk_col_name)
      max_used_column_idx = max(max_used_column_idx, column_idx)
      rowkey_id += 1
      shadow_pk_col_idx += 1

    # rowkey columns of main table are normal in the unique index table.
    # i.e., rowkey_position = 0, index_position = 0.
    for col in keywords['rowkey_columns']:
      if col[0].upper() not in [x.upper() for x in columns]:
        (idx, column_def) = find_column_def(keywords, col[0], False)
        add_column(column_def, 0, 0, 0, idx)
        max_used_column_idx = max(max_used_column_idx, idx)
  else:
    for column in columns:
      column_idx = add_index_column(keywords, rowkey_id, rowkey_id, column)
      max_used_column_idx = max(max_used_column_idx, column_idx)
      rowkey_id += 1
    for col in keywords['rowkey_columns']:
      if col[0].upper() not in [x.upper() for x in columns]:
        column_idx = add_index_column(keywords, rowkey_id, 0, col[0])
        max_used_column_idx = max(max_used_column_idx, column_idx)
        rowkey_id += 1
  return max_used_column_idx

def add_rowkey_columns(columns, *args):
  rowkey_id = 1
  index_id = 0
  if 0 < len(args):
    partition_columns = args[0]

  for column in columns:
    column_name = column[0]
    if column_name in partition_columns:
      part_key_pos = partition_columns.index(column_name)
      add_column(column, rowkey_id, index_id, part_key_pos + 1)
    else:
      add_column(column, rowkey_id, index_id, 0)
    rowkey_id += 1

def add_normal_columns(columns, *args):
  rowkey_id = 0
  index_id = 0
  partition_columns = []
  if 0 < len(args):
    partition_columns = args[0]

  for column in columns:
    column_name = column[0]
    if column_name in partition_columns:
      part_key_pos = partition_columns.index(column_name)
      add_column(column, rowkey_id, index_id, part_key_pos + 1)
    else:
      add_column(column, rowkey_id, index_id, 0)

def add_storing_column(keywords, column_name):
  (idx, column_def) = find_column_def(keywords, column_name, False)
  add_column(column_def, 0, 0, 0, idx, is_hidden='false' ,is_storing_column='true')
  return idx
def add_storing_columns(columns, max_used_column_idx, **keywords):
  for column_name in columns:
    max_used_column_idx = max(max_used_column_idx, add_storing_column(keywords, column_name))
  return max_used_column_idx

def add_field(kw, value):
  global cpp_f
  line = "  table_schema.set_{0}({1});\n".format(kw, value)
  cpp_f.write(line)

def add_char_field(kw, value):
  global cpp_f
  field = "table_schema.{0}".format(kw)
  line = """
  if (OB_SUCC(ret)) {{
    if (OB_FAIL(table_schema.set_{0}({2}))) {{
      LOG_ERROR("fail to set {0}", K(ret));
    }}
  }}
"""

  cpp_f.write(line.format(kw, field, value))

def add_list_partition_expr_field(value):
  global cpp_f
  type_str = ''
  expr_str = ''
  if 2 != len(value):
    raise IOError("partition_expr should in format [type, expr]");
  elif 'list' != value[0] and 'list_columns' != value[0]:
    raise IOError("partition_type is invalid", value[0]);
  else:
    expr_str = '"%s"' % value[1]
    if 'list' == value[0]:
      type_str = 'PARTITION_FUNC_TYPE_LIST'
    elif 'list_columns' == value[0]:
      type_str = 'PARTITION_FUNC_TYPE_LIST_COLUMNS'
    else:
      raise IOError("partition_expr type %s only list or list columns now" % value[0]);

    cpp_f.write("  if (OB_SUCC(ret)) {\n")
    line = "    table_schema.get_part_option().set_part_num(1);\n"
    cpp_f.write(line)
    line = "    table_schema.set_part_level(PARTITION_LEVEL_ONE);\n"
    cpp_f.write(line)
    line = "    table_schema.get_part_option().set_part_func_type(%s);\n" % type_str
    cpp_f.write(line)
    line = "    if (OB_FAIL(table_schema.get_part_option().set_part_expr(%s))) {\n" % expr_str
    cpp_f.write(line)
    line = "      LOG_WARN(\"set_part_expr failed\", K(ret));\n";
    cpp_f.write(line)
    line = "    } else if (OB_FAIL(table_schema.mock_list_partition_array())) {\n"
    cpp_f.write(line)
    line = "      LOG_WARN(\"mock list partition array failed\", K(ret));\n";
    cpp_f.write(line)
    cpp_f.write("    }\n")
    cpp_f.write("  }\n")


def add_partition_expr_field(value, table_id):
  global cpp_f
  type_str = ''
  expr_str = ''
  if (len(value) != 3):
    raise IOError("partition_expr should in format [type, expr, part_num]");
  else:
    if 'hash' == value[0]:
      type_str = 'PARTITION_FUNC_TYPE_HASH'
      expr_str = '"hash (%s)"' % value[1]
    elif 'key' == value[0]:
      type_str = 'PARTITION_FUNC_TYPE_KEY'
      expr_str = '"key (%s)"' % value[1]
    else:
      raise IOError("partition_expr type %s only support hash or key now" % value[0]);
    cpp_f.write("  if (OB_SUCC(ret)) {\n")
    line = "    table_schema.get_part_option().set_part_func_type(%s);\n" % type_str
    cpp_f.write(line)
    line = "    if (OB_FAIL(table_schema.get_part_option().set_part_expr(%s))) {\n" % expr_str
    cpp_f.write(line)
    line = "      LOG_WARN(\"set_part_expr failed\", K(ret));\n";
    cpp_f.write(line)
    cpp_f.write("    }\n")
    line = "    table_schema.get_part_option().set_part_num(%s);\n" % value[2]
    cpp_f.write(line)
    line = "    table_schema.set_part_level(PARTITION_LEVEL_ONE);\n"
    cpp_f.write(line)
    cpp_f.write("  }\n")

def calculate_rowkey_column_num(keywords):
  rowkey_columns = keywords['rowkey_columns']
  keywords['rowkey_column_num'] = len(rowkey_columns)

def check_fileds(fields, keywords):
  for field in fields:
    if field not in keywords and not keywords.has_key('index_name'):
      if not field in index_only_fields:
        raise IOError("no field {0} found in def_table_schema, table_name={1}".format(field, keywords["table_name"]))

  non_field_keywords = ('index', 'enable_column_def_enum', 'base_def_keywords',
                        'self_tid', 'mapping_tid', 'real_vt', 'meta_record_in_sys')
  for kw in keywords:
    if not kw.startswith("base_table_name") and kw not in fields and not keywords.has_key('index_name') and kw not in non_field_keywords and keywords['table_type'] != 'AUX_LOB_META' and keywords['table_type'] != 'AUX_LOB_PIECE':
      raise IOError("unknown field {0} found in def_table_schema, table_name={1}".format(kw, keywords["table_name"]))

def fill_default_values(default_filed_values, keywords, missing_fields, index_value=('', [])):
  for key in default_filed_values:
    if key not in keywords:
      if key == 'index_status':
        if index_value[0] != '':
          keywords[key] = default_filed_values[key]
      elif key == 'data_table_id':
        tid = table_name2tid(keywords['table_name'] + (keywords.has_key('name_postfix') and keywords['name_postfix'] or ''))
        add_field(field, tid)
      else:
        keywords[key] = default_filed_values[key]
        missing_fields[key] = True

def copy_keywords(keywords):
  tname = keywords["table_name"];
  tid = keywords["table_id"];
  base_tname = ''
  base_tname1 = ''
  base_tname2 = ''

  if "base_table_name" in keywords:
    base_tname = keywords["base_table_name"];
  else:
    keywords["base_table_name"] = ''

  if "base_table_name1" in keywords:
    base_tname1 = keywords["base_table_name1"];
  else:
    keywords["base_table_name1"] = ''

  if "base_table_name2" in keywords:
    base_tname2 = keywords["base_table_name2"];
  else:
    keywords["base_table_name2"] = ''

  print "copy_keywords in: table_id=", tid, ",  table_name=" + tname, ", base_table_name=" + base_tname, ", base_table_name1=" + base_tname1, ", base_table_name2=" + base_tname2

  # 默认base_table_name等于其表名
  # base_table_name[1,2] 记录了多层schema嵌套定义场景的原始基表名
  # 例如：15118号表schema，它嵌套定义了两层基表:
  # 真实表名：ALL_VIRTUAL_GLOBAL_TRANSACTION
  # 第一层基表：__all_tenant_global_transaction
  # 第二层基表: __all_virtual_global_transaction
  if base_tname == '':
    base_tname = tname;
    keywords["base_table_name"] = tname;
  elif base_tname1 == '' and tname != base_tname:
    base_tname1 = tname;
    keywords["base_table_name1"] = tname;
  elif base_tname2 == '' and base_tname1 != '' and tname != base_tname and tname != base_tname1:
    base_tname2 = tname;
    keywords["base_table_name2"] = tname;
  elif base_tname1 != '' and base_tname2 != '' and tname != base_tname and tname != base_tname1 and tname != base_tname2:
    print "ERROR: should not be here. need design new base_table_name"

  # 执行拷贝
  new_keywords = copy.deepcopy(keywords)

  print "copy_keywords out: table_id=", tid, ",  table_name=" + tname, ", base_table_name=" + base_tname, ", base_table_name1=" + base_tname1, ", base_table_name2=" + base_tname2

  return new_keywords

def gen_history_table_def(table_id, keywords):
  new_keywords = copy_keywords(keywords)

  new_keywords["table_id"] = table_id
  new_keywords["table_name"] = "%s_history" % new_keywords["table_name"]
  rowkey_columns = new_keywords["rowkey_columns"]
  rowkey_columns.append(("schema_version", "int"))

  cols = new_keywords["normal_columns"]
  to_del = None
  for i in range(len(cols)):
    col = cols[i]
    if "schema_version" == col[0]:
      to_del = col
      continue
    l = list(col)
    if (len(l) < 3):
      l.append('true')
    else:
      l[2] = 'true'
    cols[i] = tuple(l)
  if to_del is not None:
    cols.remove(to_del)
  cols.insert(0, ('is_deleted', 'int'))

  return new_keywords

def gen_history_table_def_of_task(table_id, keywords):
  new_keywords = copy_keywords(keywords)

  new_keywords["table_id"] = table_id
  new_keywords["table_name"] = "%s_history" % new_keywords["table_name"]

  cols = new_keywords["normal_columns"]
  cols.append(('create_time', 'timestamp', 'false'))
  cols.append(('finish_time', 'timestamp', 'false'))

  return new_keywords


def def_all_lob_aux_table():
  global lob_aux_ids
  # build lob meta for 30000 ~ 39999
  for line in lob_aux_ids:
    if line[3] == "AUX_LOB_META":
      def_table_schema(**gen_inner_lob_aux_table_def(line[1], line[2], line[3], line[4], line[5], line[6]))
  # build lob piece for 40000 ~ 49000
  for line in lob_aux_ids:
    if line[3] == "AUX_LOB_PIECE":
      def_table_schema(**gen_inner_lob_aux_table_def(line[1], line[2], line[3], line[4], line[5], line[6]))

def gen_inner_lob_aux_table_def(data_table_name, table_id, table_type, keywords, is_in_tenant_space = False, cluster_private = False):
  keywords["table_id"] = table_id
  keywords["table_name"] = data_table_name
  keywords["base_table_name"] = data_table_name
  keywords["base_table_name1"] = ''
  keywords["base_table_name2"] = ''

  new_keywords = copy_keywords(keywords)

  new_keywords["table_id"] = table_id
  new_keywords["table_type"] = table_type
  dtid = table_name2tid(data_table_name)
  new_keywords["data_table_id"] = dtid
  if is_in_tenant_space:
    new_keywords["in_tenant_space"] = is_in_tenant_space

  if cluster_private:
    new_keywords["is_cluster_private"] = cluster_private

  if table_type == "AUX_LOB_META":
    new_keywords["table_name"] = data_table_name + "_aux_lob_meta"
  else :
    new_keywords["table_name"] = data_table_name + "_aux_lob_piece"
  return new_keywords

def gen_iterate_core_inner_table_def(table_id, table_name, table_type, keywords):
  new_keywords = copy_keywords(keywords)

  new_keywords["table_id"] = table_id
  new_keywords["table_name"] = table_name
  new_keywords["table_type"] = table_type
  new_keywords["gm_columns"] = []

  if new_keywords.has_key('partition_expr'):
    del new_keywords["partition_expr"]
  if new_keywords.has_key('partition_columns'):
    del new_keywords["partition_columns"]
  if new_keywords.has_key('index'):
    del new_keywords["index"]

  # tenant_id must exists
  ten_idx = [i for i, x in enumerate(new_keywords['rowkey_columns']) if x[0] == 'tenant_id']
  if not ten_idx or ten_idx[0] != 0:
    raise Exception("tenant_id must be prefix of primary key", new_keywords['rowkey_columns'])

  new_keywords["vtable_route_policy"] = 'local'
  new_keywords["in_tenant_space"] = True
  return new_keywords

def replace_agent_table_columns_def(columns):
  for i in range(0, len(columns)):
    column = list(columns[i])
    column[0] = column[0].upper()
    t = column[1]
    if t in ("int", "uint", "bool", "bigint", "double"):
      t = "number:38"
    elif t in ("timestamp", "timestamp:6"):
      t = "otimestamp"
    elif t == "otimestamp":
      pass
    elif t == "longtext":
      pass
    elif t.startswith("varchar:") or t.startswith("varbinary:"):
      if len(column) >= 4 and "false" == column[2] and "" == column[3]:
        column[2] = "true"
    elif t.startswith("number:"):
      pass
    else:
      raise Exception("unsupported type", t)
    column[1] = t
    columns[i] = column[0:3] # ignore default value

def __gen_oracle_vt_base_on_mysql(table_id, keywords, table_name_suffix):
  in_tenant_space = keywords.has_key('in_tenant_space') and keywords['in_tenant_space']
  is_cluster_private = keywords.has_key('is_cluster_private') and keywords['is_cluster_private']
  if in_tenant_space and is_cluster_private:
    raise Exception("real table must be not cluster_private")
  new_keywords = copy_keywords(keywords)

  new_keywords["table_type"] = 'VIRTUAL_TABLE'
  new_keywords["in_tenant_space"] = True
  new_keywords["table_id"] = table_id
  new_keywords["database_id"] = "OB_ORA_SYS_DATABASE_ID"
  new_keywords["collation_type"] = "ObCollationType::CS_TYPE_UTF8MB4_BIN"
  name = keywords["table_name"]
  if name.startswith("__all_virtual_") or name.startswith("__all_tenant_virtual"):
    new_keywords["table_name"] = name.replace("__all_", "all_").upper() + table_name_suffix
  elif name.startswith("__tenant_virtual"):
    new_keywords["table_name"] = name.replace("__tenant_", "tenant_").upper() + table_name_suffix
  else:
    new_keywords["table_name"] = name.replace("__all_", "all_virtual_").upper() + table_name_suffix
  replace_agent_table_columns_def(new_keywords["rowkey_columns"])
  replace_agent_table_columns_def(new_keywords["normal_columns"])
  for column in new_keywords["gm_columns"]:
    new_keywords["normal_columns"].append([column.upper(), "otimestamp"])
  new_keywords["gm_columns"] = []
  if new_keywords.has_key('index'):
    new_idx = {}
    for (k, v) in new_keywords['index'].iteritems():
      v['index_columns'] = [ c.upper() for c in v['index_columns'] ]
      new_idx[k] = v
    new_keywords['index'] = new_idx
  if is_sys_table(keywords['table_id']):
    new_keywords['index_using_type'] = 'USING_BTREE'

  new_keywords["base_def_keywords"] = keywords
  return new_keywords

def gen_sys_agent_virtual_table_def(table_id, keywords):
  global all_agent_virtual_tables
  new_keywords = __gen_oracle_vt_base_on_mysql(table_id, keywords, "_SYS_AGENT")
  new_keywords["partition_expr"] = []
  new_keywords["partition_columns"] = []
  new_keywords["vtable_route_policy"] = "local"
  all_only_sys_table_name[keywords["table_name"]] = True
  all_agent_virtual_tables.append(new_keywords)
  return new_keywords

def __gen_mysql_vt(table_id, keywords, table_name_suffix):
  if keywords.has_key('in_tenant_space') and keywords['in_tenant_space']:
    raise Exception("base table should not in_tenant_space")
  elif 'SYSTEM_TABLE' != keywords['table_type'] and 'VIRTUAL_TABLE' != keywords['table_type']:
    raise Exception("unsupported table type", keywords['table_type'])
  new_keywords = copy_keywords(keywords)

  new_keywords["table_type"] = 'VIRTUAL_TABLE'
  new_keywords["in_tenant_space"] = True
  new_keywords["table_id"] = table_id
  new_keywords["database_id"] = "OB_SYS_DATABASE_ID"
  name = keywords["table_name"]
  if name.startswith("__all_"):
    new_keywords["table_name"] = name.replace("__all_", "__all_virtual_") + table_name_suffix
  if is_sys_table(keywords['table_id']):
    new_keywords['index_using_type'] = 'USING_BTREE'

  new_keywords["base_def_keywords"] = keywords
  return new_keywords

def gen_mysql_sys_agent_virtual_table_def(table_id, keywords):
  global all_agent_virtual_tables
  new_keywords = __gen_mysql_vt(table_id, keywords, "_mysql_sys_agent")
  new_keywords["partition_expr"] = []
  new_keywords["partition_columns"] = []
  new_keywords["vtable_route_policy"] = "local"
  mysql_compat_agent_tables[keywords["table_name"]] = True
  all_agent_virtual_tables.append(new_keywords)
  return new_keywords

def gen_agent_virtual_table_def(table_id, keywords):
  global all_agent_virtual_tables
  new_keywords = __gen_oracle_vt_base_on_mysql(table_id, keywords, "_AGENT")
  new_keywords["partition_expr"] = []
  new_keywords["partition_columns"] = []
  if new_keywords.has_key('vtable_route_policy'):
    del(new_keywords["vtable_route_policy"])
  all_agent_virtual_tables.append(new_keywords)
  return new_keywords

def gen_oracle_mapping_virtual_table_base_def(table_id, keywords, real_table):
  if True == real_table:
    new_keywords = __gen_oracle_vt_base_on_mysql(table_id, keywords, "_REAL_AGENT")
    new_keywords["is_real_virtual_table"] = True
  else :
    new_keywords = __gen_oracle_vt_base_on_mysql(table_id, keywords, "")

  new_keywords["name_postfix"] = "_ORA"
  new_keywords["partition_expr"] = []
  if new_keywords.has_key("partition_columns"):
    new_keywords["partition_columns"] = [ c.upper() for c in new_keywords["partition_columns"] ]

  if True == real_table :
    new_keywords["mapping_tid"] = table_name2tid(keywords['table_name'])
    new_keywords["self_tid"] = table_name2tid(new_keywords['table_name'] + new_keywords['name_postfix'])
    new_keywords["real_vt"] = True
    real_table_virtual_table_names.append(new_keywords)
  else :
    all_ora_mapping_virtual_table_org_tables.append(table_name2tid(keywords['table_name']))
    all_ora_mapping_virtual_tables.append(table_name2tid(new_keywords['table_name'] + new_keywords['name_postfix']))
  return new_keywords

def gen_oracle_mapping_virtual_table_def(table_id, keywords):
  return gen_oracle_mapping_virtual_table_base_def(table_id, keywords, False)

def gen_oracle_mapping_real_virtual_table_def(table_id, keywords):
  in_tenant_space = keywords.has_key('in_tenant_space') and keywords['in_tenant_space']
  if False == in_tenant_space:
    raise Exception("real table must be tenant space", keywords['rowkey_columns'])
  is_cluster_private = keywords.has_key('is_cluster_private') and keywords['is_cluster_private']
  if True == is_cluster_private:
    raise Exception("real table must be not cluster_private")
  new_keywords = gen_oracle_mapping_virtual_table_base_def(table_id, keywords, True)

  ## check tenant_id must be first key, if has tenant_id
  if new_keywords.has_key('rowkey_columns') and new_keywords['rowkey_columns']:
    key_tenant_id = -1
    nth_key = 0
    for key in new_keywords['rowkey_columns']:
      if key[0].upper() == 'TENANT_ID':
        key_tenant_id = nth_key
        break
      nth_key = nth_key + 1
    if -1 != key_tenant_id and 0 != key_tenant_id:
      raise Exception("tenant id of real table must be the first key", keywords['rowkey_columns'])
  return new_keywords

def gen_cluster_config_def(table_id, table_name, keywords):
  new_keywords = copy_keywords(keywords)

  new_keywords["table_id"] = table_id
  new_keywords["table_name"] = table_name
  return new_keywords

def generate_cluster_private_table(f):
  global cluster_private_tables
  all_tables = [x for x in cluster_private_tables]
  all_tables.sort(key = lambda x: x['table_name'])
  cluster_private_switch = '\n'
  for kw in all_tables:
    if kw.has_key('index_name'):
      cluster_private_switch += 'case ' + table_name2index_tid(kw['table_name'] + kw['name_postfix'], kw['index_name']) + ':\n'
    else:
      cluster_private_switch += 'case ' + table_name2tid(kw['table_name'] + kw['name_postfix']) + ':\n'
  f.write('\n\n#ifdef CLUSTER_PRIVATE_TABLE_SWITCH\n' + cluster_private_switch + '\n#endif\n')

def generate_sys_index_table_misc_data(f):
  global sys_index_tables

  data_table_dict = {}
  for kw in sys_index_tables:
    if not data_table_dict.has_key(kw['table_name']):
      data_table_dict[kw['table_name']] = []
    data_table_dict[kw['table_name']].append(kw)

  sys_index_table_id_switch = '\n'
  for kw in sys_index_tables:
    sys_index_table_id_switch += 'case ' + table_name2index_tid(kw['table_name'], kw['index_name']) + ':\n'
  f.write('\n\n#ifdef SYS_INDEX_TABLE_ID_SWITCH\n' + sys_index_table_id_switch + '\n#endif\n')

  sys_index_data_table_id_switch = '\n'
  for data_table_name in data_table_dict.keys():
    sys_index_data_table_id_switch += 'case ' + table_name2tid(data_table_name) + ':\n'
  f.write('\n\n#ifdef SYS_INDEX_DATA_TABLE_ID_SWITCH\n' + sys_index_data_table_id_switch + '\n#endif\n')

  sys_index_data_table_id_to_index_ids_switch = '\n'
  for data_table_name, sys_indexs in data_table_dict.items():
    sys_index_data_table_id_to_index_ids_switch += 'case ' + table_name2tid(data_table_name) + ': {\n'
    for kw in sys_indexs:
      sys_index_data_table_id_to_index_ids_switch += '  if (FAILEDx(index_tids.push_back(' + table_name2index_tid(kw['table_name'], kw['index_name']) +  '))) {\n'
      sys_index_data_table_id_to_index_ids_switch += '    LOG_WARN(\"fail to push back index tid\", KR(ret));\n'
      sys_index_data_table_id_to_index_ids_switch += '  }\n'
    sys_index_data_table_id_to_index_ids_switch += '  break;\n'
    sys_index_data_table_id_to_index_ids_switch += '}\n'
  f.write('\n\n#ifdef SYS_INDEX_DATA_TABLE_ID_TO_INDEX_IDS_SWITCH\n' + sys_index_data_table_id_to_index_ids_switch + '\n#endif\n')

  sys_index_data_table_id_to_index_schema_switch = '\n'
  for data_table_name, sys_indexs in data_table_dict.items():
    sys_index_data_table_id_to_index_schema_switch += 'case ' + table_name2tid(data_table_name) + ': {\n'
    for kw in sys_indexs:
      method_name = kw['table_name'].replace('$', '_').strip('_').lower() + '_' + kw['index_name'].lower() + '_schema'
      sys_index_data_table_id_to_index_schema_switch += '  index_schema.reset();\n'
      sys_index_data_table_id_to_index_schema_switch += '  if (FAILEDx(ObInnerTableSchema::' + method_name +'(index_schema))) {\n'
      sys_index_data_table_id_to_index_schema_switch += '    LOG_WARN(\"fail to create index schema\", KR(ret), K(tenant_id), K(data_table_id));\n'
      sys_index_data_table_id_to_index_schema_switch += '  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {\n'
      sys_index_data_table_id_to_index_schema_switch += '    LOG_WARN(\"fail to append\", KR(ret), K(tenant_id), K(data_table_id));\n'
      sys_index_data_table_id_to_index_schema_switch += '  }\n'
    sys_index_data_table_id_to_index_schema_switch += '  break;\n'
    sys_index_data_table_id_to_index_schema_switch += '}\n'

  f.write('\n\n#ifdef SYS_INDEX_DATA_TABLE_ID_TO_INDEX_SCHEMAS_SWITCH\n' + sys_index_data_table_id_to_index_schema_switch + '\n#endif\n')

  add_sys_index_id = '\n'
  for kw in sys_index_tables:
    index_id = table_name2index_tid(kw['table_name'], kw['index_name'])
    add_sys_index_id += '  } else if (OB_FAIL(table_ids.push_back(' + index_id +'))) {\n'
    add_sys_index_id += '    LOG_WARN(\"add index id failed\", KR(ret), K(tenant_id));\n'
  f.write('\n\n#ifdef ADD_SYS_INDEX_ID\n' + add_sys_index_id + '\n#endif\n')

def generate_virtual_agent_misc_data(f):
  global all_agent_virtual_tables
  all_agent = [x for x in all_agent_virtual_tables]
  all_agent.sort(key = lambda x: x['table_name'])
  # for ob_sql_partition_location_cache.cpp, switch agent virtual table location
  location_switch = '\n'
  for kw in all_agent:
    location_switch += 'case ' + table_name2tid(kw['table_name']) + ':\n'
  f.write('\n#ifdef AGENT_VIRTUAL_TABLE_LOCATION_SWITCH\n' + location_switch + '\n#endif\n')

  # for ob_virtual_table_iterator_factory.cpp, init agent virtual iterator
  count = 0
  iter_init = '\n'
  for kw in all_agent:
    if count % 20 == 0:
      if count != 0:
        iter_init += '  END_CREATE_VT_ITER_SWITCH_LAMBDA\n\n'
      iter_init += '  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA'

    count += 1

    tid = table_name2tid(kw['table_name'])
    base_kw = kw['base_def_keywords']
    base_tid = table_name2tid(base_kw['table_name'])
    in_tenant_space = base_kw.has_key('in_tenant_space') and base_kw['in_tenant_space']
    only_sys = all_only_sys_table_name.has_key(base_kw['table_name']) and all_only_sys_table_name[base_kw['table_name']] and "OB_SYS_DATABASE_ID" != kw['database_id']
    mysql_compat_agent_table_name = base_kw['table_name']
    mysql_compat_agent = (mysql_compat_agent_tables.has_key(mysql_compat_agent_table_name)
                          and mysql_compat_agent_tables[mysql_compat_agent_table_name]
                          and "OB_SYS_DATABASE_ID" == kw['database_id'])
    iter_init += """
    case %s: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = %s;
      const bool sys_tenant_base_table = %s;
      const bool only_sys_data = %s;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data%s))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        agent_iter->~ObAgentVirtualTable();
        allocator.free(agent_iter);
        agent_iter = NULL;
      } else {
       vt_iter = agent_iter;
      }
      break;
    }\n""" % (tid, base_tid, in_tenant_space and 'false' or 'true', only_sys and 'true' or 'false',
              ', Worker::CompatMode::MYSQL' if mysql_compat_agent else '')

  iter_init += '  END_CREATE_VT_ITER_SWITCH_LAMBDA\n'
  f.write('\n\n#ifdef AGENT_VIRTUAL_TABLE_CREATE_ITER\n' + iter_init + '\n#endif // AGENT_VIRTUAL_TABLE_CREATE_ITER\n\n')

def def_sys_index_table(index_name, index_table_id, index_columns, index_using_type, index_type, keywords):
  global cpp_f
  global cpp_f_tmp
  global StringIO
  global sys_index_tables

  kw = copy_keywords(keywords)

  if kw.has_key('index'):
    raise Exception("should not have index", kw['table_name'])
  if not is_sys_table(kw['table_id']):
    raise Exception("only support sys table", kw['table_name'])
  if not is_sys_index_table(index_table_id):
    raise Exception("index table id is invalid", index_table_id)
  if is_core_table(kw['table_id']) and not is_core_index_table(index_table_id):
    raise Exception("index table id for core table should be less than 101000", index_table_id, kw['table_id'])

  index_def = ''
  cpp_f_tmp = cpp_f
  cpp_f = StringIO.StringIO()
  kw['index_name'] = index_name
  kw['index_columns'] = index_columns
  kw['index_table_id'] = index_table_id
  kw['index_using_type'] = index_using_type
  kw['index_type'] = index_type
  kw['table_type'] = 'USER_INDEX'
  kw['index_status'] = 'INDEX_STATUS_AVAILABLE'
  dtid = table_name2tid(kw['table_name'])
  kw['data_table_id'] = dtid
  kw['partition_columns'] = []
  kw['partition_expr'] = []
  kw['storing_columns'] =[]
  sys_index_tables.append(kw)
  def_table_schema(**kw)
  index_def = cpp_f.getvalue()
  cpp_f = cpp_f_tmp
  cpp_f.write(index_def)

def def_agent_index_table(index_name, index_table_id, index_columns, index_using_type, index_type,
  real_table_name, real_index_name, keywords):
  global cpp_f
  global cpp_f_tmp
  global StringIO
  global sys_index_tables
  kw = copy_keywords(keywords)

  index_kw = copy_keywords(all_def_keywords[real_table_name + '_' + real_index_name])
  if kw.has_key('index'):
    raise Exception("should not have index", kw['table_name'])
  if not kw['real_vt']:
    raise Exception("only support oracle mapping table", kw['table_name'])
  if not index_name.endswith('_real_agent'):
    raise Exception("wrong index name", index_name)
  if not index_name.startswith(real_index_name):
    raise Exception("wrong index name", index_name, real_index_name)
  if not is_ora_virtual_table(index_table_id):
    raise Exception("index table id is invalid", index_table_id)
  if not kw['base_def_keywords']['table_name'] == real_table_name:
    raise Exception("table name mismatch", kw['base_def_keywords']['table_name'], real_table_name)
  if not index_kw['index_name'] == real_index_name:
    raise Exception("index name mismatch", index_kw['index_name'], real_index_name)
  if not index_kw['index_columns'] == index_columns:
    raise Exception("index column mismatch", index_kw['index_columns'], index_columns)


  index_def = ''
  cpp_f_tmp = cpp_f
  cpp_f = StringIO.StringIO()
  kw['index_name'] = index_name
  kw['index_columns'] = index_columns
  kw['index_table_id'] = index_table_id
  kw['index_using_type'] = index_using_type
  kw['index_type'] = index_type
  kw['table_type'] = 'USER_INDEX'
  kw['index_status'] = 'INDEX_STATUS_AVAILABLE'
  kw['name_postfix'] = '_ORA'
  dtid = table_name2tid(kw['table_name'] + kw['name_postfix'])
  kw['data_table_id'] = dtid
  kw['partition_columns'] = []
  kw['partition_expr'] = []
  kw['storing_columns'] =[]
  kw["mapping_tid"] = table_name2tid(real_table_name + '_' + real_index_name)
  kw["self_tid"] = table_name2index_tid(kw['table_name'] + kw['name_postfix'], kw['index_name'])
  kw["real_vt"] = True
  real_table_virtual_table_names.append(kw)

  #In order to upgrade compatibility,
  #the oracle inner table index cannot be added to the schema of the main table following the path of the main table.
  #Only the schema refresh triggered by the creation of the index table can add simple index info,
  #so the agent table index is not added to the sys index here

  #sys_index_tables.append(kw)
  def_table_schema(**kw)
  index_def = cpp_f.getvalue()
  cpp_f = cpp_f_tmp
  cpp_f.write(index_def)

def gen_iterate_private_virtual_table_def(
    table_id, table_name, keywords, in_tenant_space = False):
  global all_iterate_private_virtual_tables
  kw = copy_keywords(keywords)
  kw['table_id'] = table_id
  kw['table_name'] =  table_name

  # check base table:
  # 1. system table
  # 2. in tenant space & is cluster private
  # 3. rowkey columns should start with `tenant_id`
  if 'SYSTEM_TABLE' != kw['table_type']:
    raise Exception("unsupported table type", kw['table_type'])
  elif not kw.has_key('in_tenant_space') or not kw['in_tenant_space']:
    raise Exception("base table should be in_tenant_space")
  elif not kw.has_key('is_cluster_private') or not kw['is_cluster_private']:
    raise Exception("base table should be cluster private")
  else:
    ten_idx = [i for i, x in enumerate(kw['rowkey_columns']) if x[0] == 'tenant_id']
    if not ten_idx or ten_idx[0] != 0:
      raise Exception("tenant_id must be prefix of primary key", kw['rowkey_columns'])

  if kw.has_key('index'):
    for (k, v ) in kw['index'].iteritems():
      if v['index_columns'].find('tenant_id') >= 0:
        raise Exception("tenant_id must not exist in index", k, v, kw['table_name'])
      v['index_columns'].insert(0, 'tenant_id')
      v['index_using_type'] = 'USING_BTREE'

  del(kw['in_tenant_space'])
  del(kw['is_cluster_private'])
  kw['table_type'] = 'VIRTUAL_TABLE'
  kw['index_using_type'] = 'USING_BTREE'
  kw['partition_columns'] = []
  kw['partition_expr'] = []

  if kw.has_key('gm_columns'):
    for x in reversed(kw['gm_columns']):
      kw['normal_columns'].insert(0, (x, 'timestamp'))
    kw['gm_columns'] = []

  kw['base_def_keywords'] = keywords
  kw['in_tenant_space'] = in_tenant_space
  all_iterate_private_virtual_tables.append(kw)
  return kw

def generate_iterate_private_virtual_table_misc_data(f):
  global all_iterate_private_virtual_tables
  tables = [x for x in all_iterate_private_virtual_tables]
  tables.sort(key = lambda x: x['table_name'])
  # for ob_sql_partition_location_cache.cpp, switch iterate virtual table location
  location_switch = '\n'
  for kw in tables:
    location_switch += 'case ' + table_name2tid(kw['table_name']) + ':\n'
  f.write('\n\n#ifdef ITERATE_PRIVATE_VIRTUAL_TABLE_LOCATION_SWITCH\n' + location_switch + '\n#endif\n')

  # for ob_virtual_table_iterator_factory.cpp, init iterate virtual iterator
  count = 0
  iter_init = '\n'
  for kw in tables:
    if count % 20 == 0:
      if count != 0:
        iter_init += '  END_CREATE_VT_ITER_SWITCH_LAMBDA\n\n'
      iter_init += '  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA'

    count += 1

    tid = table_name2tid(kw['table_name'])
    base_kw = kw['base_def_keywords']
    base_tid = table_name2tid(base_kw['table_name'])

    iter_init += """
    case %s: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = %s;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(%s, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }\n""" % (tid, kw['meta_record_in_sys'] and 'true' or 'false', base_tid)

  iter_init += '  END_CREATE_VT_ITER_SWITCH_LAMBDA\n'
  f.write('\n\n#ifdef ITERATE_PRIVATE_VIRTUAL_TABLE_CREATE_ITER\n' + iter_init + '\n#endif // ITERATE_PRIVATE_VIRTUAL_TABLE_CREATE_ITER\n')

# Define virtual table to iterate one tenant space table's data of all tenant.
def gen_iterate_virtual_table_def(table_id, table_name, keywords, in_tenant_space = False):
  global all_iterate_virtual_tables

  kw = copy_keywords(keywords)
  kw['table_id'] = table_id
  kw['table_name'] =  table_name

  if 'SYSTEM_TABLE' != kw['table_type']:
    raise Exception("unsupported table type", kw['table_type'])
  elif not kw.has_key('in_tenant_space') or not kw['in_tenant_space']:
    raise Exception("base table should be in_tenant_space")
  elif kw.has_key('is_cluster_private') and kw['is_cluster_private']:
    raise Exception("base table should be not cluster private")
  kw['table_type'] = 'VIRTUAL_TABLE'
  kw['index_using_type'] = 'USING_BTREE'
  kw['in_tenant_space'] = in_tenant_space
  kw['partition_columns'] = []
  kw['partition_expr'] = []

  # check and add tenant_id in primary key and index.
  kw['normal_columns'] = filter(lambda x: x[0] != 'tenant_id', kw['normal_columns'])
  ten_idx = [i for i, x in enumerate(kw['rowkey_columns']) if x[0] == 'tenant_id']
  if ten_idx:
    if ten_idx[0] != 0:
      raise Exception("tenant_id must be prefix of primary key", kw['rowkey_columns'])
  else:
    kw['rowkey_columns'].insert(0, ('tenant_id', 'int', 'false'))

  if kw.has_key('index'):
    for (k, v ) in kw['index'].iteritems():
      if v['index_columns'].find('tenant_id') >= 0:
        raise Exception("tenant_id must not exist in index", k, v, kw['table_name'])
      v['index_columns'].insert(0, 'tenant_id')
      v['index_using_type'] = 'USING_BTREE'

  if kw.has_key('gm_columns'):
    for x in reversed(kw['gm_columns']):
      kw['normal_columns'].insert(0, (x, 'timestamp'))
    kw['gm_columns'] = []
  kw['base_def_keywords'] = keywords

  save_kw = copy_keywords(kw)
  all_iterate_virtual_tables.append(save_kw)
  return kw

def generate_iterate_virtual_table_misc_data(f):
  global all_iterate_virtual_tables
  tables = [x for x in all_iterate_virtual_tables]
  tables.sort(key = lambda x: x['table_name'])
  # for ob_sql_partition_location_cache.cpp, switch iterate virtual table location
  location_switch = '\n'
  for kw in tables:
    location_switch += 'case ' + table_name2tid(kw['table_name']) + ':\n'
  f.write('\n\n#ifdef ITERATE_VIRTUAL_TABLE_LOCATION_SWITCH\n' + location_switch + '\n#endif\n')

  # for ob_virtual_table_iterator_factory.cpp, init iterate virtual iterator
  count = 0
  iter_init = '\n'
  for kw in tables:
    if count % 20 == 0:
      if count != 0:
        iter_init += '  END_CREATE_VT_ITER_SWITCH_LAMBDA\n\n'
      iter_init += '  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA'

    count += 1

    tid = table_name2tid(kw['table_name'])
    base_kw = kw['base_def_keywords']
    base_tid = table_name2tid(base_kw['table_name'])

    iter_init += """
    case %s: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(%s, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }\n""" % (tid, base_tid)

  iter_init += '  END_CREATE_VT_ITER_SWITCH_LAMBDA\n'
  f.write('\n\n#ifdef ITERATE_VIRTUAL_TABLE_CREATE_ITER\n' + iter_init + '\n#endif // ITERATE_VIRTUAL_TABLE_CREATE_ITER\n')

def get_column_def_enum(**keywords):
  global column_def_enum_array
  columns = []
  normal_columns = keywords['normal_columns']
  rowkey_columns = keywords['rowkey_columns']

  columns.extend(map(lambda x : x[0], rowkey_columns))
  columns.extend(map(lambda x : x[0], normal_columns))

  table_name = keywords['table_name'] + keywords['name_postfix']
  t = map(lambda x : x.upper().replace('#', '_'), columns)
  if len(t) > 0 and 'enable_column_def_enum' in keywords and keywords['enable_column_def_enum']:
    t[0] = '%s = common::OB_APP_MIN_COLUMN_ID' % t[0]
    content = '''
struct %s {
  enum {
    %s
  };
};
''' % (table_name.replace('$', '_').upper().strip('_') + "_CDE", ",\n    ".join(t))
    column_def_enum_array.append(content)

def table_name2tid(name):
  return "OB_" + name.replace('$', '_').upper().strip('_') + "_TID"

def table_name2index_tid(table_name, idx_name):
  return "OB_" + table_name.replace('$', '_').upper().strip('_') + '_' + str(idx_name).upper() + "_TID";

def table_name2tname(name):
  return "OB_" + name.replace('$', '_').upper().strip('_') + "_TNAME"

def table_name2tname_ora(name):
  return "OB_" + name.replace('$', '_').upper().strip('_') + "_ORA_TNAME"

def table_name2index_tname(table_name, idx_name):
  return "OB_" + table_name.replace('$', '_').upper().strip('_') + '_' + str(idx_name).upper() + "_TNAME";

__current_range_idx = -1 
__def_cnt = 0 
__split_size = 50
def check_split_file(tid):
  global __current_range_idx
  global __def_cnt
  global cpp_f
  #sometimes cpp_f may modify to STRINGIO object
  if isinstance(cpp_f, file) or cpp_f == None:
    print "current schema cnt => %d" % __def_cnt
    range_idx = tid / __split_size
    if range_idx > __current_range_idx:
      if cpp_f != None:
        end_generate_cpp()
      fname = "ob_inner_table_schema.%d_%d.cpp" % (range_idx * __split_size + 1, (range_idx + 1) * __split_size)
      print "generate new file with name %s" % fname
      start_generate_cpp(fname)
      __current_range_idx = range_idx
    elif range_idx < __current_range_idx:
      print "unexcept table id seq"
      sys.exit(1)
    __def_cnt += 1

def def_table_schema(**keywords):
  tid = int(keywords['table_id'])
  check_split_file(tid)

  global fields
  global default_filed_values
  missing_fields = {}
  global table_name_ids
  global table_name_postfix_ids
  global table_name_postfix_table_names
  global index_name_ids
  global tenant_space_tables
  global all_ora_mapping_virtual_table_org_tables
  global all_ora_mapping_virtual_tables
  global tenant_space_table_names
  global only_rs_vtables
  global cluster_distributed_vtables
  global tenant_distributed_vtables
  global StringIO
  global ob_virtual_index_table_id
  global ora_virtual_index_table_id
  global index_only_id
  global index_idx
  global cpp_f
  global cpp_f_tmp
  global all_def_keywords
  global column_collation
  global is_oracle_sys_table
  global cluster_private_tables
  global lob_aux_data_def
  global lob_aux_meta_def

  if not keywords.has_key('index_name'):
    if 'name_postfix' in keywords:
      all_def_keywords[keywords['table_name'] + keywords['name_postfix']] = copy_keywords(keywords)
    else:
      all_def_keywords[keywords['table_name']] = copy_keywords(keywords)
      keywords = copy.deepcopy(all_def_keywords[keywords['table_name']])
  else:
    if 'name_postfix' in keywords:
      all_def_keywords[keywords['table_name'] + keywords['name_postfix'] + '_' + keywords['index_name']] = copy_keywords(keywords)
    else:
      all_def_keywords[keywords['table_name'] + '_' + keywords['index_name']] = copy_keywords(keywords)

  index_defs = []
  index_def = ''
  calculate_rowkey_column_num(keywords)
  is_oracle_sys_table = False
  column_collation = 'CS_TYPE_INVALID'

  ##virtual table will set index_using_type to USING_HASH by default
  if is_virtual_table(keywords['table_id']):
    if not keywords.has_key('index_using_type'):
      keywords['index_using_type'] = 'USING_HASH'

  if not is_mysql_virtual_table(tid) and not is_ora_virtual_table(tid):
    if keywords.has_key('partition_expr') and 0 != len(keywords['partition_expr']):
      raise Exception("partition_expr only works for virtual table after 4.0", tid)
    elif keywords.has_key('partition_columns') and 0 != len(keywords['partition_columns']):
      raise Exception("partition_columns only works for virtual table after 4.0", tid)

  if not is_mysql_virtual_table(tid) and not is_ora_virtual_table(tid):
    if keywords.has_key('partition_expr') and 0 != len(keywords['partition_expr']):
      raise Exception("partition_expr only works for virtual table after 4.0", tid)
    elif keywords.has_key('partition_columns') and 0 != len(keywords['partition_columns']):
      raise Exception("partition_columns only works for virtual table after 4.0", tid)
  if is_sys_view(tid):
    pattern = re.compile(r'^\s*SELECT\s+\*', re.IGNORECASE)
    if keywords.has_key('view_definition') and 0 != len(keywords['view_definition']) and pattern.match(keywords['view_definition'].upper().replace("\n", " ")):
      print(keywords['view_definition'])
      raise Exception("The system view definition cannot start with select *. Please specify the column name explicitly, ", tid)

  fill_default_values(default_filed_values, keywords, missing_fields)
  check_fileds(fields, keywords)

  get_column_def_enum(**keywords)

  if keywords.has_key('index_name'):
    print_method_start(keywords['table_name'] + keywords['name_postfix'] + '_' + keywords['index_name'])
    if True == is_ora_virtual_table(int(keywords['table_id'])):
      if keywords.has_key('real_vt') and True == keywords['real_vt']:
        index_name_ids.append([keywords['index_name'], int(keywords['index_table_id']), keywords['table_name'] + keywords['name_postfix'], keywords['tenant_id'], keywords['table_id'], keywords['base_table_name'], keywords['base_table_name1']])
      else:
        index_name_ids.append([keywords['index_name'], int(ora_virtual_index_table_id), keywords['table_name'] + keywords['name_postfix'], keywords['tenant_id'], keywords['table_id'], keywords['base_table_name'], keywords['base_table_name1']])
        ora_virtual_index_table_id -= 1
    elif True == is_mysql_virtual_table(int(keywords['table_id'])):
      index_name_ids.append([keywords['index_name'], int(ob_virtual_index_table_id), keywords['table_name'] + keywords['name_postfix'], keywords['tenant_id'], keywords['table_id'], keywords['base_table_name'], keywords['base_table_name1']])
      ob_virtual_index_table_id -= 1
    elif True == is_sys_table(int(keywords['table_id'])):
      if not keywords.has_key('index_table_id'):
        raise Exception("must specific index_table_id", int(keywords['table_id']))
      index_name_ids.append([keywords['index_name'], int(keywords['index_table_id']), keywords['table_name'] + keywords['name_postfix'], keywords['tenant_id'], keywords['table_id'], keywords['base_table_name'], keywords['base_table_name1']])
  else:
    print_method_start(keywords['table_name'] + keywords['name_postfix'])
    table_name_postfix_ids.append((keywords['table_name']+ keywords['name_postfix'], int(keywords['table_id'])))
    table_name_postfix_table_names.append((keywords['table_name']+ keywords['name_postfix'], keywords['table_name']))

    table_name_ids.append((keywords['table_name'], int(keywords['table_id']), keywords['base_table_name'], keywords['base_table_name1'], keywords['base_table_name2']))

  print "\table_id=",  keywords['table_id'], ", table_name=" + keywords['table_name'], ", base_table_name=", keywords['base_table_name'], ", base_table_name1=" + keywords['base_table_name1'], ", base_table_name2=" + keywords['base_table_name2']

  print "\nSTART TO GENERATE: " + keywords['table_name']+ keywords['name_postfix']
  if True == is_ora_virtual_table(int(keywords['table_id'])):
    column_collation = 'CS_TYPE_UTF8MB4_BIN'
    is_oracle_sys_table = True
  if keywords.has_key('index_name'):
    local_fields = fields + index_only_fields
  elif is_lob_table(keywords['table_id']):
    local_fields = fields + lob_fields
  else:
    local_fields = fields

  # Generate partition expr for virtual table.
  # We only support 'partition by hash(addr_to_partition_id(ip, port)) partitions 65536' in mysql mode,
  # and 'partition by hash(ip, port) partitions 65536' in oracle mode for virtual table.
  table_id = int(keywords['table_id']);
  if keywords['partition_columns'] and (is_mysql_virtual_table(table_id) or is_ora_virtual_table(table_id)) and False == keywords['is_real_virtual_table']:
    cols = keywords['partition_columns']

    # vtable with definition of partition_colums must be distributed
    if not keywords.has_key('vtable_route_policy') or 'distributed' != keywords['vtable_route_policy'].lower():
      raise Exception("vtable route policy must be distributed", keywords['table_name'])

    if len(cols) != 2:
      raise Exception("only support ip, port partition columns for virtual table", cols)
    types = []
    for col in cols:
      types.append([x[1] for x in keywords['rowkey_columns'] + keywords['normal_columns'] if x[0] == col][0])
    (ip, port) = types
    if is_mysql_virtual_table(table_id):
      if not ip.startswith("varchar:") or not port.startswith("int"):
        raise Exception("unexpected type of ip and port", cols, types);
      keywords['partition_expr'] = ['list_columns', ', '.join(cols)]
    else:
      if not ip.startswith("varchar:") or not port.startswith("number"):
        raise Exception("unexpected type of ip and port", cols, types);
      keywords['partition_expr'] = ['list', ', '.join(cols)]

  # owner must be defined
  if not keywords.has_key('owner') or 0 == len(keywords['owner'].strip()):
    raise Exception('owner must be specified')

  # vtable_route_policy' value must be valid
  if keywords.has_key('vtable_route_policy'):
    route_policy = keywords['vtable_route_policy'].lower()

    tid_str = ""
    if keywords.has_key('index_columns'):
      tid_str = table_name2index_tid(keywords['table_name']+ keywords['name_postfix'], keywords['index_name'])
    else:
      tid_str = table_name2tid(keywords['table_name']+ keywords['name_postfix'])

    if 'local' != route_policy and 'distributed' != route_policy and 'only_rs' != route_policy:
      raise Exception("vtable route policy is invalid", route_policy)
    elif not is_mysql_virtual_table(tid) and not is_ora_virtual_table(tid) and 'local' != route_policy:
      raise Exception("vtabl route policy is only work for virtual table", tid)
    else:
      if 'local' == route_policy or 'only_rs' == route_policy:
        if keywords.has_key('partition_columns') and 0 != len(keywords['partition_columns']):
          raise Exception("partition columns is not valid for local/only_rs virtual table", partition_columns)
        if 'only_rs' == route_policy:
          only_rs_vtables.append(tid_str)
      else:
        # distributed
        if not keywords.has_key('partition_columns') or 2 != len(keywords['partition_columns']):
          raise Exception("partition columns is not valid for distributed virtual table", partition_columns)
        if keywords.has_key('in_tenant_space') and keywords['in_tenant_space']:
          tenant_distributed_vtables.append(tid_str)
        else:
          cluster_distributed_vtables.append(tid_str)

  ## 1. reset non-sys table's tablegroup_id
  ## 2. set sys table's(includes index、lob) tablet_id
  if is_sys_table(tid) or is_lob_table(tid) or is_sys_index_table(tid):
    keywords['tablet_id'] = tid_str
  else:
    keywords['tablegroup_id'] = 'OB_INVALID_ID'

  for field in local_fields :
    value = keywords[field]
    if field == 'gm_columns':
      if keywords.has_key('index_table_id'):
        for column_name in value:
          print_discard_column(column_name)
      else:
        add_gm_columns(value)
    elif field == 'rowkey_columns':
      if not keywords.has_key('index_name'):
        if keywords.has_key('partition_columns'):
          add_rowkey_columns(value, keywords['partition_columns'])
        else:
          add_rowkey_columns(value)
    elif field == 'normal_columns':
      if not keywords.has_key('index_name'):
        if keywords['table_type'] != 'TABLE_TYPE_VIEW':
          if keywords.has_key('partition_columns'):
            add_normal_columns(value, keywords['partition_columns'])
          else:
            add_normal_columns(value)
    elif field == 'partition_columns':
      continue;
    elif field == 'table_id':
      if keywords.has_key('index_columns'):
        tid = table_name2index_tid(keywords['table_name']+ keywords['name_postfix'], keywords['index_name'])
      else:
        tid = table_name2tid(keywords['table_name']+ keywords['name_postfix'])
      add_field(field, tid)
    elif field == 'database_id' and field not in missing_fields:
      database_id = value
      add_field(field, database_id)
    elif field == 'table_name':
      if keywords.has_key('index_name') :
        add_char_field(field, table_name2index_tname(keywords['table_name'] + keywords['name_postfix'], keywords['index_name']))
      else:
        if keywords["name_postfix"] != '_ORA':
          add_char_field(field, table_name2tname(keywords['table_name']))
        else:
          add_char_field(field, table_name2tname_ora(keywords['table_name']))
    elif field in ('compress_func_name'):
      add_char_field(field, '{0}'.format(value))
    elif field in ('comment_str', 'part_func_expr', 'sub_part_func_expr'):
      add_char_field(field, '"{0}"'.format(value))
    elif field == 'in_tenant_space':
      if keywords[field]:
        if keywords.has_key('index_name') :
          tenant_space_tables.append(table_name2index_tid(keywords['table_name']+ keywords['name_postfix'], keywords['index_name']))
          tenant_space_table_names.append(table_name2index_tname(keywords['table_name'] + keywords['name_postfix'], keywords['index_name']))
        else:
          tenant_space_tables.append(table_name2tid(keywords['table_name']+ keywords['name_postfix']))
          tenant_space_table_names.append(table_name2tname(keywords['table_name'] + keywords['name_postfix']))
    elif field == 'view_definition':
      if keywords[field]:
        add_char_field(field, 'R"__({0})__"'.format(value))
    elif field == 'partition_expr':
      if keywords[field]:
        add_list_partition_expr_field(value)
    elif field == 'index':
      if type(value) == dict:
        # index defined in table definition
        cpp_f_tmp = cpp_f
        index_idx = 0
        del keywords['index']
        if keywords.has_key('index_using_type'):
            dt_using_type = keywords['index_using_type']
        for k, v in value.items():
          cpp_f = StringIO.StringIO()
          index_idx += 1
          keywords['index_name'] = k
          keywords['index_columns'] = v['index_columns']
          if v.has_key('index_table_id'):
              keywords['index_table_id'] = v['index_table_id']
          if v.has_key('index_using_type'):
              keywords['index_using_type'] = v['index_using_type']
          keywords['table_type'] = 'USER_INDEX'
          keywords['index_status'] = 'INDEX_STATUS_AVAILABLE'
          keywords['index_type'] = 'INDEX_TYPE_NORMAL_LOCAL';
          dtid = table_name2tid(keywords['table_name']+ keywords['name_postfix'])
          keywords['data_table_id'] = dtid
          if (is_virtual_table(keywords['table_id'])):
            keywords['storing_columns'] = [col[0] for col in keywords['normal_columns'] if col[0] not in v['index_columns']]
          def_table_schema(**keywords)
          index_def = cpp_f.getvalue()
          index_defs.append(index_def)

        keywords['index'] = value
        if dt_using_type is not None:
            keywords['index_using_type'] = dt_using_type
        else:
            del keywords['index_using_type']
        cpp_f = cpp_f_tmp
    elif field == 'index_columns':
      # only index generation will enter here
      max_used_column_idx = add_index_columns(value, **keywords)
    elif field == 'storing_columns':
      # only virtual table index generation will enter here
      max_used_column_idx = add_storing_columns(value, max_used_column_idx, **keywords)
    elif field in ('index_name', 'name_postfix',
                   'is_cluster_private', 'is_real_virtual_table',
                   'owner', 'vtable_route_policy'):
      # do nothing
      print "skip"
    else:
      add_field(field, value)

  ## add lob aux table except for __all_core_table
  if keywords['table_type'] == 'SYSTEM_TABLE' and int(keywords['table_id']) > 1:
    is_in_tenant_space = False
    cluster_private = False
    if keywords.has_key('in_tenant_space'):
      is_in_tenant_space = keywords['in_tenant_space']
    if keywords.has_key('is_cluster_private'):
      cluster_private = keywords['is_cluster_private']
    meta_tid = int(keywords['table_id']) + base_lob_meta_table_id
    lob_aux_ids.append([keywords['table_id'], keywords['table_name'], meta_tid, 'AUX_LOB_META', lob_aux_meta_def, is_in_tenant_space, cluster_private])
    mtid = table_name2tid(keywords['table_name'] + '_aux_lob_meta')
    add_field('aux_lob_meta_tid', mtid)
    piece_tid = int(keywords['table_id']) + base_lob_piece_table_id
    lob_aux_ids.append([keywords['table_id'], keywords['table_name'], piece_tid, 'AUX_LOB_PIECE', lob_aux_data_def, is_in_tenant_space, cluster_private])
    ptid = table_name2tid(keywords['table_name'] + '_aux_lob_piece')
    add_field('aux_lob_piece_tid', ptid)

  if keywords.has_key("index_name") and not type(keywords['index']) == dict:
    add_index_method_end(max_used_column_idx)
  else:
    add_method_end()

  if keywords.has_key('is_cluster_private') and keywords['is_cluster_private'] \
     and keywords.has_key('in_tenant_space') and keywords['in_tenant_space'] \
     and (is_sys_table(table_id) or is_sys_index_table(table_id) or is_lob_table(table_id)):
    if is_sys_table(table_id) and not keywords.has_key('meta_record_in_sys'):
      raise Exception("meta_record_in_sys must be defined when is_cluster_private = true")
    kw = copy_keywords(keywords)
    cluster_private_tables.append(kw)

  if keywords.has_key('index_name'):
    del keywords['index_name']
  if keywords.has_key('index_columns'):
    del keywords['index_columns']
  if keywords.has_key('index_status'):
    del keywords['index_status']
  if keywords.has_key('data_table_id'):
    del keywords['data_table_id']
  if keywords.has_key('index_type'):
    del keywords['index_type']
  if keywords.has_key('is_cluster_private'):
    del keywords['is_cluster_private']
  for index_def in index_defs:
    cpp_f.write(index_def)

def clean_files(globstr):
  print "clean files by glob [%s]" % globstr
  for f in glob.glob(os.path.join('.', globstr)):
      print "remove  %s ..." % f
      os.remove(f)

def start_generate_cpp(cpp_file_name):
  global cpp_f
  cpp_f = open(cpp_file_name, 'w')
  head = copyright + """
#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_inner_table_schema.h"

#include "share/schema/ob_schema_macro_define.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_table_schema.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;
namespace share
{

"""
  cpp_f.write(head)

def start_generate_h(h_file_name):
  global h_f
  h_f = open(h_file_name, 'w')
  head = copyright + """
#ifndef _OB_INNER_TABLE_SCHEMA_H_
#define _OB_INNER_TABLE_SCHEMA_H_

#include "share/ob_define.h"
#include "ob_inner_table_schema_constants.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}

namespace share
{
"""
  h_f.write(head)

def start_generate_constants_h(h_file_name):
  global constants_h_f
  global id_to_name_f
  constants_h_f = open(h_file_name, 'w')
  id_to_name_f = open("table_id_to_name", 'w')
  head = copyright + """
#ifndef _OB_INNER_TABLE_SCHEMA_CONSTANTS_H_
#define _OB_INNER_TABLE_SCHEMA_CONSTANTS_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}

namespace share
{
"""
  constants_h_f.write(head)

def print_class_head_h():
  global column_def_enum_array
  h_f.write("\n".join(column_def_enum_array))

  class_head="""
class ObInnerTableSchema
{
"""
  h_f.write(class_head)

def end_generate_cpp():
  global cpp_f
  end = """
} // end namespace share
} // end namespace oceanbase
"""
  cpp_f.write(end)
  cpp_f.close()

# 在生成constants.h的同时，生成table_id_to_name文件
def generate_constants_h_content():
  global constants_h_f
  global id_to_name_f
  last_table_id = 0;

  id_to_name_f.write("########## Table ID 到 Table Name 映射 ##########\n")
  id_to_name_f.write("# 为了方便分析占位情况，同一个ID可能会映射多个Name\n\n")

  ################# 生成xx_TID定义 ################
  table_id_line = 'const uint64_t OB_{0}_TID = {1}; // "{2}"\n'
  for (table_name, table_id) in table_name_postfix_ids:
    constants_h_f.write(table_id_line.format(table_name.replace('$', '_').upper().strip('_'), table_id, table_name))
    if table_id <= last_table_id:
        raise Exception("invalid table id", table_name, table_id, last_table_id)
    last_table_id = table_id
  for line in index_name_ids:
    constants_h_f.write(table_id_line.format(line[2].replace('$', '_').upper().strip('_')+'_'+line[0].upper(), line[1], line[2]))

  constants_h_f.write("\n")
  ###################################################

  ################# 生成xx_TNAME定义 ################
  # 同时生成table_id_to_name文件
  table_name_line = 'const char *const OB_{0}_TNAME = "{1}";\n'
  for (table_name_postfix, table_name) in table_name_postfix_table_names:
    constants_h_f.write(table_name_line.format(table_name_postfix.replace('$', '_').upper().strip('_'), table_name))

  table_id_to_name_line = '# {0}: {1}\n'
  base_table_id_to_name_line = '# {0}: {1}  # BASE_TABLE_NAME\n'
  base_table_id_to_name_line1 = '# {0}: {1}  # BASE_TABLE_NAME1\n'
  base_table_id_to_name_line2 = '# {0}: {1}  # BASE_TABLE_NAME2\n'
  for (table_name, table_id, base_table_name, base_table_name1, base_table_name2) in table_name_ids:
    # lob表不记录在table_id_to_name文件中
    if not is_lob_table(table_id):
      id_to_name_f.write(table_id_to_name_line.format(table_id, table_name))
      # 如果base_table_name不同，则输出base_table_name
      if base_table_name != table_name:
        id_to_name_f.write(base_table_id_to_name_line.format(table_id, base_table_name))
      if base_table_name1 != '' and base_table_name1 != table_name:
        id_to_name_f.write(base_table_id_to_name_line1.format(table_id, base_table_name1))
      if base_table_name2 != '' and base_table_name2 != table_name:
        id_to_name_f.write(base_table_id_to_name_line2.format(table_id, base_table_name2))

  index_table_name_format = "__idx_{0}_{1}"
  index_name_line = 'const char *const OB_{0}_TNAME = "{1}";\n'
  index_id_to_name_line = '# {0}: {1}\n'
  base_index_id_to_name_line = '# {0}: {1}  # INDEX_NAME\n'
  data_tname_id_to_name_line = '# {0}: {1}  # DATA_BASE_TABLE_NAME\n'
  data_tname_id_to_name_line1 = '# {0}: {1}  # DATA_BASE_TABLE_NAME1\n'

  for line in index_name_ids:
    index_name = line[0]
    table_id = line[1]
    data_table_id =  int(line[4]) & (0xFFFFFFFFFF)
    data_table_name = line[5]
    data_table_name1 = line[6]
    index_table_name = index_table_name_format.format(str(data_table_id), index_name)

    constants_h_f.write(index_name_line.format(line[2].replace('$', '_').upper().strip('_')+'_'+line[0].upper(), index_table_name))
    id_to_name_f.write(index_id_to_name_line.format(table_id, index_table_name))
    id_to_name_f.write(base_index_id_to_name_line.format(table_id, index_name))
    id_to_name_f.write(data_tname_id_to_name_line.format(table_id, data_table_name))
    if data_table_name1 != '':
      id_to_name_f.write(data_tname_id_to_name_line1.format(table_id, data_table_name1))

  constants_h_f.write("\n")
  ###################################################


  ########### 生成all_privilege_init_data ###########
  gen_all_privilege_init_data(constants_h_f);
  ###################################################


def generate_h_content():
  global table_name_ids
  global h_f
  core_table_count = 0
  sys_table_count = 0
  virtual_table_count = 0
  sys_view_count = 0

  print_class_head_h()
  new_table_name_postfix_ids = sorted(table_name_postfix_ids, key = lambda table : table[1])
  new_index_name_ids = sorted(index_name_ids, key = lambda index : index[1])

  h_f.write("\npublic:\n")
  method_line = "  static int {0}_schema(share::schema::ObTableSchema &table_schema);\n"
  for (table_name, table_id) in new_table_name_postfix_ids:
    h_f.write(method_line.format(table_name.replace('$', '_').lower().strip('_'), table_id))
  for line in new_index_name_ids:
    h_f.write(method_line.format(line[2].replace('$', '_').strip('_').lower()+'_'+line[0].lower(), line[1]))
  line = """
private:
  DISALLOW_COPY_AND_ASSIGN(ObInnerTableSchema);
};
"""
  h_f.write(line)

  h_f.write("\n")
  h_f.write("typedef int (*schema_create_func)(share::schema::ObTableSchema &table_schema);\n")
  h_f.write("\n")

  method_name = "  ObInnerTableSchema::{0}_schema,\n"
  h_f.write("const schema_create_func all_core_table_schema_creator [] = {\n")
  for (table_name, table_id) in table_name_postfix_ids:
    if table_id == kv_core_table_id:
      h_f.write(method_name.format(table_name.replace('$', '_').lower().strip('_'), table_name))
      core_table_count = core_table_count + 1
  h_f.write("  NULL,};\n\n")

  h_f.write("const schema_create_func core_table_schema_creators [] = {\n")
  for (table_name, table_id) in new_table_name_postfix_ids:
    if is_core_table(table_id) and table_id != kv_core_table_id:
      h_f.write(method_name.format(table_name.replace('$', '_').lower().strip('_'), table_name))
      core_table_count = core_table_count + 1
  h_f.write("  NULL,};\n\n")

  h_f.write("const schema_create_func sys_table_schema_creators [] = {\n")
  for (table_name, table_id) in new_table_name_postfix_ids:
    if is_sys_table(table_id) and not is_core_table(table_id):
      h_f.write(method_name.format(table_name.replace('$', '_').lower().strip('_'), table_name))
      sys_table_count = sys_table_count + 1
  h_f.write("  NULL,};\n\n")

  h_f.write("const schema_create_func virtual_table_schema_creators [] = {\n")
  for (table_name, table_id) in new_table_name_postfix_ids:
    if is_mysql_virtual_table(table_id):
      h_f.write(method_name.format(table_name.replace('$', '_').lower().strip('_'), table_name))
      virtual_table_count = virtual_table_count + 1
  for index_l in new_index_name_ids:
    if is_mysql_virtual_table(index_l[1]):
      h_f.write(method_name.format(index_l[2].replace('$', '_').strip('_').lower()+'_'+index_l[0].lower(), index_l[2]))
      virtual_table_count = virtual_table_count + 1
  for (table_name, table_id) in new_table_name_postfix_ids:
    if is_ora_virtual_table(table_id):
      h_f.write(method_name.format(table_name.replace('$', '_').lower().strip('_'), table_name))
      virtual_table_count = virtual_table_count + 1
  for index_l in new_index_name_ids:
    if is_ora_virtual_table(index_l[1]):
      h_f.write(method_name.format(index_l[2].replace('$', '_').strip('_').lower()+'_'+index_l[0].lower(), index_l[2]))
      virtual_table_count = virtual_table_count + 1
  h_f.write("  NULL,};\n\n")

  h_f.write("const schema_create_func sys_view_schema_creators [] = {\n")
  for (table_name, table_id) in new_table_name_postfix_ids:
    if is_sys_view(table_id):
      h_f.write(method_name.format(table_name.replace('$', '_').lower().strip('_'), table_name))
      sys_view_count = sys_view_count + 1
  h_f.write("  NULL,};\n\n")

  h_f.write("const schema_create_func core_index_table_schema_creators [] = {\n")
  for index_l in index_name_ids:
    if is_core_index_table(index_l[1]):
      h_f.write(method_name.format(index_l[2].replace('$', '_').strip('_').lower()+'_'+index_l[0].lower(), index_l[2]))
  h_f.write("  NULL,};\n\n")

  h_f.write("const schema_create_func sys_index_table_schema_creators [] = {\n")
  for index_l in index_name_ids:
    if not is_core_index_table(index_l[1]) and is_sys_index_table(index_l[1]):
      h_f.write(method_name.format(index_l[2].replace('$', '_').strip('_').lower()+'_'+index_l[0].lower(), index_l[2]))
  h_f.write("  NULL,};\n\n")

  # just to make test happy
  h_f.write("const schema_create_func information_schema_table_schema_creators[] = {\n")
  h_f.write("  NULL,};\n\n")
  h_f.write("const schema_create_func mysql_table_schema_creators[] = {\n")
  h_f.write("  NULL,};\n\n")


  h_f.write("const uint64_t tenant_space_tables [] = {")
  for name in tenant_space_tables:
    h_f.write("\n  {0},".format(name))
  h_f.write("  };\n\n")

  # define oracle virtual table mapping oceanbase virtual table, the schema must be same
  h_f.write("const uint64_t all_ora_mapping_virtual_table_org_tables [] = {")
  for name in all_ora_mapping_virtual_table_org_tables:
    h_f.write("\n  {0},".format(name))
  h_f.write("  };\n\n")

  h_f.write("const uint64_t all_ora_mapping_virtual_tables [] = {")
  for name in all_ora_mapping_virtual_tables:
    h_f.write("  {0}\n,".format(name))
  h_f.write("  };\n\n")

  # define oracle virtual table mapping oceanbase real table, the schema must be same
  h_f.write("/* start/end_pos is start/end postition for column with tenant id */\n")
  h_f.write("struct VTMapping\n")
  h_f.write("{\n")
  h_f.write("   uint64_t mapping_tid_;\n")
  h_f.write("   bool is_real_vt_;\n")
  h_f.write("   int64_t start_pos_;\n")
  h_f.write("   int64_t end_pos_;\n")
  h_f.write("};\n\n")
  #h_f.write("// define all columns with tenant id\n")
  #h_f.write("const char* const with_tenant_id_columns[] = {\n")
  #tmp_vt_tables = [x for x in real_table_virtual_table_names]
  #tmp_vt_tables.sort(key = lambda x: x['table_name'])
  #total_columns_with_tenant_id = 0
  #for tmp_kw in tmp_vt_tables:
  #  if tmp_kw.has_key("columns_with_tenant_id") and tmp_kw["columns_with_tenant_id"]:
  #    for column_name in tmp_kw["columns_with_tenant_id"]:
  #      h_f.write("\n  \"{0}\",".format(column_name.upper()))
  #      total_columns_with_tenant_id = total_columns_with_tenant_id + 1
  #h_f.write("\n};\n\n")
  h_f.write("extern VTMapping vt_mappings[5000];\n\n")

  h_f.write("const char* const tenant_space_table_names [] = {")
  for name in tenant_space_table_names:
    h_f.write("\n  {0},".format(name))
  h_f.write("  };\n\n")

  h_f.write("const uint64_t only_rs_vtables [] = {")
  for name in only_rs_vtables:
    h_f.write("\n  {0},".format(name))
  h_f.write("  };\n\n")

  h_f.write("const uint64_t cluster_distributed_vtables [] = {")
  for name in cluster_distributed_vtables:
    h_f.write("\n  {0},".format(name))
  h_f.write("  };\n\n")

  h_f.write("const uint64_t tenant_distributed_vtables [] = {")
  for name in tenant_distributed_vtables:
    h_f.write("\n  {0},".format(name))
  h_f.write("  };\n\n")

  global restrict_access_virtual_tables
  h_f.write("const uint64_t restrict_access_virtual_tables[] = {\n  "
      + ",\n  ".join(restrict_access_virtual_tables) + "  };\n\n")
  h_f.write("""
static inline bool is_restrict_access_virtual_table(const uint64_t tid)
{
  bool found = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(restrict_access_virtual_tables) && !found; i++) {
    if (tid == restrict_access_virtual_tables[i]) {
      found = true;
    }
  }
  return found;
}

""")

  h_f.write("static inline bool is_tenant_table(const uint64_t tid)\n");
  h_f.write("{\n");
  h_f.write("  bool in_tenant_space = false;\n");
  h_f.write("  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_space_tables); ++i) {\n");
  h_f.write("    if (tid == tenant_space_tables[i]) {\n");
  h_f.write("      in_tenant_space = true;\n");
  h_f.write("      break;\n");
  h_f.write("    }\n");
  h_f.write("  }\n");
  h_f.write("  return in_tenant_space;\n");
  h_f.write("}\n\n");

  h_f.write("static inline bool is_tenant_table_name(const common::ObString &tname)\n");
  h_f.write("{\n");
  h_f.write("  bool in_tenant_space = false;\n");
  h_f.write("  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_space_table_names); ++i) {\n");
  h_f.write("    if (0 == tname.case_compare(tenant_space_table_names[i])) {\n");
  h_f.write("      in_tenant_space = true;\n");
  h_f.write("      break;\n");
  h_f.write("    }\n");
  h_f.write("  }\n");
  h_f.write("  return in_tenant_space;\n");
  h_f.write("}\n\n");

  h_f.write("static inline bool is_global_virtual_table(const uint64_t tid)\n");
  h_f.write("{\n");
  h_f.write("  return common::is_virtual_table(tid) && !is_tenant_table(tid);\n");
  h_f.write("}\n\n");

  h_f.write("static inline bool is_tenant_virtual_table(const uint64_t tid)\n");
  h_f.write("{\n");
  h_f.write("  return common::is_virtual_table(tid) && is_tenant_table(tid);\n");
  h_f.write("}\n\n");

  # oracle virtual table get origin table id in oceanbase database
  h_f.write("static inline uint64_t get_origin_tid_by_oracle_mapping_tid(const uint64_t tid)\n");
  h_f.write("{\n")
  h_f.write("  uint64_t org_tid = common::OB_INVALID_ID;\n")
  h_f.write("  uint64_t idx = common::OB_INVALID_ID;\n")
  h_f.write("  for (uint64_t i = 0; common::OB_INVALID_ID == idx && i < ARRAYSIZEOF(all_ora_mapping_virtual_tables); ++i) {\n")
  h_f.write("    if (tid == all_ora_mapping_virtual_tables[i]) {\n")
  h_f.write("      idx = i;\n")
  h_f.write("    }\n")
  h_f.write("  }\n")
  h_f.write("  if (common::OB_INVALID_ID != idx) {\n")
  h_f.write("     org_tid = all_ora_mapping_virtual_table_org_tables[idx];\n")
  h_f.write("  }\n")
  h_f.write("  return org_tid;\n")
  h_f.write("}\n\n")

  ## it's oracle virtual table, it's not agent table!!!
  h_f.write("static inline bool is_oracle_mapping_virtual_table(const uint64_t tid)\n")
  h_f.write("{\n")
  h_f.write("  bool is_ora_vt = false;\n")
  h_f.write("  for (uint64_t i = 0; i < ARRAYSIZEOF(all_ora_mapping_virtual_tables); ++i) {\n")
  h_f.write("    if (tid == all_ora_mapping_virtual_tables[i]) {\n")
  h_f.write("      is_ora_vt = true;\n")
  h_f.write("    }\n")
  h_f.write("  }\n")
  h_f.write("  return is_ora_vt;\n")
  h_f.write("}\n\n")

  ## Mappping oceanbase real table to virtual table in Oracle mode
  # oracle virtual table get origin table id in oceanbase database
  ## it's oracle virtual table, it's not agent table!!!
  h_f.write("static inline uint64_t get_real_table_mappings_tid(const uint64_t tid)\n");
  h_f.write("{\n")
  h_f.write("  uint64_t org_tid = common::OB_INVALID_ID;\n")
  h_f.write("  uint64_t pure_id = tid;\n")
  h_f.write("  if (pure_id > common::OB_MAX_MYSQL_VIRTUAL_TABLE_ID && pure_id < common::OB_MAX_VIRTUAL_TABLE_ID) {\n")
  h_f.write("    int64_t idx = pure_id - common::OB_MAX_MYSQL_VIRTUAL_TABLE_ID - 1;\n")
  h_f.write("    VTMapping &tmp_vt_mapping = vt_mappings[idx];\n")
  h_f.write("    if (tmp_vt_mapping.is_real_vt_) {\n")
  h_f.write("      org_tid = tmp_vt_mapping.mapping_tid_;\n")
  h_f.write("    }\n")
  h_f.write("  }\n")
  h_f.write("  return org_tid;\n")
  h_f.write("}\n\n")

  h_f.write("static inline bool is_oracle_mapping_real_virtual_table(const uint64_t tid)\n")
  h_f.write("{\n")
  h_f.write("  return common::OB_INVALID_ID != get_real_table_mappings_tid(tid);\n")
  h_f.write("}\n\n")
  ## end Mapping oceanbase real table to virtual table in Oracle mode

  h_f.write("static inline void get_real_table_vt_mapping(const uint64_t tid, VTMapping *&vt_mapping)\n");
  h_f.write("{\n")
  h_f.write("  uint64_t pure_id = tid;\n")
  h_f.write("  vt_mapping = nullptr;\n")
  h_f.write("  if (pure_id > common::OB_MAX_MYSQL_VIRTUAL_TABLE_ID && pure_id < common::OB_MAX_VIRTUAL_TABLE_ID) {\n")
  h_f.write("    int64_t idx = pure_id - common::OB_MAX_MYSQL_VIRTUAL_TABLE_ID - 1;\n")
  h_f.write("    vt_mapping = &vt_mappings[idx];\n")
  h_f.write("  }\n")
  h_f.write("}\n\n")

  h_f.write("static inline bool is_only_rs_virtual_table(const uint64_t tid)\n");
  h_f.write("{\n");
  h_f.write("  bool bret = false;\n");
  h_f.write("  for (int64_t i = 0; !bret && i < ARRAYSIZEOF(only_rs_vtables); ++i) {\n");
  h_f.write("    if (tid == only_rs_vtables[i]) {\n");
  h_f.write("      bret = true;\n");
  h_f.write("    }\n");
  h_f.write("  }\n");
  h_f.write("  return bret;\n");
  h_f.write("}\n\n");

  h_f.write("static inline bool is_cluster_distributed_vtables(const uint64_t tid)\n");
  h_f.write("{\n");
  h_f.write("  bool bret = false;\n");
  h_f.write("  for (int64_t i = 0; !bret && i < ARRAYSIZEOF(cluster_distributed_vtables); ++i) {\n");
  h_f.write("    if (tid == cluster_distributed_vtables[i]) {\n");
  h_f.write("      bret = true;\n");
  h_f.write("    }\n");
  h_f.write("  }\n");
  h_f.write("  return bret;\n");
  h_f.write("}\n\n");

  h_f.write("static inline bool is_tenant_distributed_vtables(const uint64_t tid)\n");
  h_f.write("{\n");
  h_f.write("  bool bret = false;\n");
  h_f.write("  for (int64_t i = 0; !bret && i < ARRAYSIZEOF(tenant_distributed_vtables); ++i) {\n");
  h_f.write("    if (tid == tenant_distributed_vtables[i]) {\n");
  h_f.write("      bret = true;\n");
  h_f.write("    }\n");
  h_f.write("  }\n");
  h_f.write("  return bret;\n");
  h_f.write("}\n\n");

  # define lob aux mapping
  h_f.write("/* lob aux table mapping for sys table */\n")
  h_f.write("struct LOBMapping\n")
  h_f.write("{\n")
  h_f.write("  uint64_t data_table_tid_;\n")
  h_f.write("  uint64_t lob_meta_tid_;\n")
  h_f.write("  uint64_t lob_piece_tid_;\n")
  h_f.write("  schema_create_func lob_meta_func_;\n")
  h_f.write("  schema_create_func lob_piece_func_;\n")
  h_f.write("};\n\n")
  h_f.write("LOBMapping const lob_aux_table_mappings [] = {\n")
  for i in xrange(0, len(lob_aux_ids), 2):
    meta_info = lob_aux_ids[i]
    piece_info = lob_aux_ids[i + 1]
    dtid = table_name2tid(meta_info[1])
    mtid = table_name2tid(meta_info[1] + '_aux_lob_meta')
    ptid = table_name2tid(piece_info[1] + '_aux_lob_piece')
    h_f.write("  {\n")
    h_f.write("    {0},\n".format(dtid))
    h_f.write("    {0},\n".format(mtid))
    h_f.write("    {0},\n".format(ptid))
    meta_method_name = meta_info[1] + "_AUX_LOB_META"
    piece_method_name = piece_info[1] + "_AUX_LOB_PIECE"
    h_f.write("    ObInnerTableSchema::{0}_schema,\n".format(meta_method_name.replace('$', '_').lower().strip('_'), meta_method_name))
    h_f.write("    ObInnerTableSchema::{0}_schema\n".format(piece_method_name.replace('$', '_').lower().strip('_'), piece_method_name))
    h_f.write("  },\n\n")
  h_f.write("};\n\n")

  h_f.write("static inline bool get_sys_table_lob_aux_table_id(const uint64_t tid, uint64_t& meta_tid, uint64_t& piece_tid)\n");
  h_f.write("{\n");
  h_f.write("  bool bret = false;\n");
  h_f.write("  meta_tid = OB_INVALID_ID;\n");
  h_f.write("  piece_tid = OB_INVALID_ID;\n");
  h_f.write("  if (OB_ALL_CORE_TABLE_TID == tid) {\n");
  h_f.write("    // __all_core_table do not need lob aux table, return false\n");
  h_f.write("  } else if (is_system_table(tid)) {\n");
  h_f.write("    bret = true;\n");
  h_f.write("    meta_tid = tid + OB_MIN_SYS_LOB_META_TABLE_ID;\n");
  h_f.write("    piece_tid = tid + OB_MIN_SYS_LOB_PIECE_TABLE_ID;\n");
  h_f.write("  }\n");
  h_f.write("  return bret;\n");
  h_f.write("}\n\n");
  h_f.write("typedef common::hash::ObHashMap<uint64_t, LOBMapping> inner_lob_map_t;\n")
  h_f.write("extern inner_lob_map_t inner_lob_map;\n")
  h_f.write("extern bool inited_lob;\n")
  h_f.write("static inline int get_sys_table_lob_aux_schema(const uint64_t tid,\n");
  h_f.write("                                               share::schema::ObTableSchema& meta_schema,\n");
  h_f.write("                                               share::schema::ObTableSchema& piece_schema)\n");
  h_f.write("{\n");
  h_f.write("  int ret = OB_SUCCESS;\n");
  h_f.write("  LOBMapping item;\n");
  h_f.write("  if (OB_FAIL(inner_lob_map.get_refactored(tid, item))) {\n");
  h_f.write("    SERVER_LOG(WARN, \"fail to get lob mapping item\", K(ret), K(tid), K(inited_lob));\n");
  h_f.write("  } else if (OB_FAIL(item.lob_meta_func_(meta_schema))) {\n");
  h_f.write("    SERVER_LOG(WARN, \"fail to build lob meta schema\", K(ret), K(tid));\n");
  h_f.write("  } else if (OB_FAIL(item.lob_piece_func_(piece_schema))) {\n");
  h_f.write("    SERVER_LOG(WARN, \"fail to build lob piece schema\", K(ret), K(tid));\n");
  h_f.write("  }\n");
  h_f.write("  return ret;\n");
  h_f.write("}\n\n");

  sys_tenant_table_count = 1 + core_table_count + sys_table_count + virtual_table_count + sys_view_count
  core_schema_version = 1
  bootstrap_version = core_schema_version + sys_tenant_table_count + 2
  h_f.write("const int64_t OB_CORE_TABLE_COUNT = %d;\n" % core_table_count)
  h_f.write("const int64_t OB_SYS_TABLE_COUNT = %d;\n" % sys_table_count)
  h_f.write("const int64_t OB_VIRTUAL_TABLE_COUNT = %d;\n" % virtual_table_count)
  h_f.write("const int64_t OB_SYS_VIEW_COUNT = %d;\n" % sys_view_count)
  h_f.write("const int64_t OB_SYS_TENANT_TABLE_COUNT = %d;\n" % sys_tenant_table_count)
  h_f.write("const int64_t OB_CORE_SCHEMA_VERSION = %d;\n" % core_schema_version)
  h_f.write("const int64_t OB_BOOTSTRAP_SCHEMA_VERSION = %d;\n" % bootstrap_version)

  for (table_name, table_id, base_table_name, base_table_name1, base_table_name2) in table_name_ids:
    if table_id >= max_sys_index_id:
      raise IOError("invalid table_id: {0} table_name:{1}".format(table_id, table_name))

def end_generate_h():
  global h_f
  end = """
} // end namespace share
} // end namespace oceanbase
#endif /* _OB_INNER_TABLE_SCHEMA_H_ */
"""
  h_f.write(end)
  h_f.close()

def end_generate_constants_h():
  global constants_h_f
  global id_to_name_f
  end = """
} // end namespace share
} // end namespace oceanbase
#endif /* _OB_INNER_TABLE_SCHEMA_CONSTANTS_H_ */
"""
  constants_h_f.write(end)
  constants_h_f.close()
  id_to_name_f.close()

def write_vt_mapping_cpp(h_file_name):
  global cpp_f
  cpp_f = open(h_file_name, 'w')
  head = copyright + """
#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_inner_table_schema.h"

namespace oceanbase
{
namespace share
{
"""
  cpp_f.write(head)

  tmp_vt_tables = [x for x in real_table_virtual_table_names]
  tmp_vt_tables.sort(key = lambda x: x['table_name'])
  #total_columns_with_tenant_id = 0
  #for tmp_kw in tmp_vt_tables:
  #  if tmp_kw.has_key("columns_with_tenant_id") and tmp_kw["columns_with_tenant_id"]:
  #    for column_name in tmp_kw["columns_with_tenant_id"]:
  #      total_columns_with_tenant_id = total_columns_with_tenant_id + 1
  cpp_f.write("VTMapping vt_mappings[5000];\n")
  cpp_f.write("bool vt_mapping_init()\n")
  cpp_f.write("{\n")
  tmp_start_pos = 0
  tmp_end_pos = 0
  cpp_f.write("   int64_t start_idx = common::OB_MAX_MYSQL_VIRTUAL_TABLE_ID + 1;\n")
  for tmp_kw in tmp_vt_tables:
    cpp_f.write("   {\n")
    cpp_f.write("   int64_t idx = {0} - start_idx;\n".format(tmp_kw["self_tid"]))
    cpp_f.write("   VTMapping &tmp_vt_mapping = vt_mappings[idx];\n")
    if tmp_kw.has_key("mapping_tid") and tmp_kw["mapping_tid"]:
      cpp_f.write("   tmp_vt_mapping.mapping_tid_ = {0};\n".format(tmp_kw["mapping_tid"]))
    if tmp_kw.has_key("real_vt") and tmp_kw["real_vt"]:
      is_real_vt = "true"
      cpp_f.write("   tmp_vt_mapping.is_real_vt_ = {0};\n".format(is_real_vt))
    #if tmp_kw.has_key("columns_with_tenant_id") and tmp_kw["columns_with_tenant_id"]:
    #  for column_name in tmp_kw["columns_with_tenant_id"]:
    #    tmp_end_pos = tmp_end_pos + 1
    #  cpp_f.write("   tmp_vt_mapping.start_pos_ = {0};\n".format(tmp_start_pos))
    #  cpp_f.write("   tmp_vt_mapping.end_pos_ = {0};\n".format(tmp_end_pos))
    cpp_f.write("   }\n\n")
    tmp_start_pos = tmp_end_pos
    tmp_end_pos = tmp_start_pos
  cpp_f.write("   return true;\n")
  cpp_f.write("} // end define vt_mappings\n\n")

  cpp_f.write("bool inited_vt = vt_mapping_init();\n")
  #if total_columns_with_tenant_id != tmp_end_pos:
  #  raise Exception("columns with tenant id {0} is not match with {1}".format(total_columns_with_tenant_, tmp_end_pos))


  end = """
} // end namespace share
} // end namespace oceanbase
"""
  cpp_f.write(end)
  cpp_f.close()

def write_lob_mapping_cpp(h_file_name):
  global cpp_f
  cpp_f = open(h_file_name, 'w')
  head = copyright + """
#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_inner_table_schema.h"

namespace oceanbase
{
namespace share
{
"""
  cpp_f.write(head)

  cpp_f.write("inner_lob_map_t inner_lob_map;\n")
  cpp_f.write("bool lob_mapping_init()\n")
  cpp_f.write("{\n")
  cpp_f.write("  int ret = OB_SUCCESS;\n")
  bucket_cnt = len(lob_aux_ids)/2
  cpp_f.write("  if (OB_FAIL(inner_lob_map.create(%d, ObModIds::OB_INNER_LOB_HASH_SET))) {\n" % bucket_cnt);
  cpp_f.write("    SERVER_LOG(WARN, \"fail to create inner lob map\", K(ret));\n")
  cpp_f.write("  } else {\n")
  cpp_f.write("    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(lob_aux_table_mappings); ++i) {\n")
  cpp_f.write("      if (OB_FAIL(inner_lob_map.set_refactored(lob_aux_table_mappings[i].data_table_tid_, lob_aux_table_mappings[i]))) {\n")
  cpp_f.write("        SERVER_LOG(WARN, \"fail to set inner lob map\", K(ret), K(i));\n")
  cpp_f.write("      }\n")
  cpp_f.write("    }\n")
  # cpp_f.write("    if (OB_SUCC(ret)) {\n")
  # cpp_f.write("      has_init = true;\n")
  # cpp_f.write("    }\n")
  cpp_f.write("  }\n")
  cpp_f.write("  return (ret == OB_SUCCESS);\n")
  cpp_f.write("} // end define lob_mappings\n\n")

  cpp_f.write("bool inited_lob = lob_mapping_init();\n")
  end = """
} // end namespace share
} // end namespace oceanbase
"""
  cpp_f.write(end)
  cpp_f.close()


def start_generate_misc_data(fname):
  f = open(fname, 'w')
  f.write(copyright)
  return f

if __name__ == "__main__":
  global ob_virtual_index_table_id
  ob_virtual_index_table_id = max_ob_virtual_table_id - 1
  ora_virtual_index_table_id = max_ora_virtual_table_id - 1

  clean_files("ob_inner_table_schema.*")
  execfile("ob_inner_table_schema_def.py")
  def_all_lob_aux_table()
  end_generate_cpp()

  start_generate_h("ob_inner_table_schema.h")
  generate_h_content()
  end_generate_h()

  start_generate_constants_h("ob_inner_table_schema_constants.h")
  generate_constants_h_content()
  end_generate_constants_h()

  ## write virtual table for init virtual table information
  write_vt_mapping_cpp("ob_inner_table_schema.vt.cpp")
  write_lob_mapping_cpp("ob_inner_table_schema.lob.cpp")
  f = start_generate_misc_data("ob_inner_table_schema_misc.ipp")
  generate_virtual_agent_misc_data(f)
  generate_iterate_private_virtual_table_misc_data(f)
  generate_iterate_virtual_table_misc_data(f)
  generate_cluster_private_table(f)
  generate_sys_index_table_misc_data(f)

  f.close()

  print "\nSuccess\n"
