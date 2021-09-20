#_ -*- coding: utf-8 -*-

# Copyright (c) 2021 OceanBase
# OceanBase CE is licensed under Mulan PubL v2.
# You can use this software according to the terms and conditions of the Mulan PubL v2.
# You may obtain a copy of Mulan PubL v2 at:
#          http://license.coscl.org.cn/MulanPubL-2.0
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PubL v2 for more details.

#
# OB use disjoint table_id ranges to define different kinds of tables:
# - (0, 10000] : System Table
# - (10000, 15000] : MySQL Virtual Table
# - (15000, 20000] : Oracle Virtual Table
# - (20000, 25000] : MySQL System View
# - (25000, 30000] : Oracle System View
# - (50000, ~)     : User Table
#
# Here are some table_name definition principles.
# 1. Be defined by simple present tense, first person.
# 2. Be active and singular.
# 3. System table's table_name should be started with '__all_'.
# 4. Virtual table's table_name should be started with '__all_virtual' or '__tenant_virtual'.
#    Virtual table started with '__all_virtual' can be directly queried by SQL.
#    Virtual table started with '__tenant_virtual' is used for special cmd(such as show cmd), which can't be queried by SQL.
# 5. System view's table_name should be referred from MySQL/Oracle.
# 6. Definition of Oracle Virtual Table/Oracle System View can be referred from document:

global fields
fields = [
    'tenant_id',
    'tablegroup_id',
    'database_id',
    'table_id',
    'rowkey_split_pos',
    'is_use_bloomfilter',
    'progressive_merge_num',
    'rowkey_column_num', # This field will be calculated by rowkey_columns automatically.
    'load_type',
    'table_type',
    'index_type',
    'def_type',
    'table_name',
    'compress_func_name',
    'part_level',
    'charset_type',
    'collation_type',
    'create_mem_version',
    'gm_columns',
    'rowkey_columns',
    'normal_columns',
    'partition_columns',
    'in_tenant_space',
    'only_rs_vtable',
    'view_definition',
    'partition_expr',
    'index',
    'index_using_type',
    'name_postfix',
    'row_store_type',
    'store_format',
    'migrate_data_before_2200',
    'columns_with_tenant_id',
    'progressive_merge_round',
    'storage_format_version',
    'is_cluster_private',
    'is_backup_private',
    'rs_restart_related',
    'is_real_virtual_table',
]

global index_only_fields
index_only_fields = ['index_name', 'index_columns', 'index_status', 'index_type', 'data_table_id']


global default_filed_values
default_filed_values = {
    'tenant_id' : 'OB_SYS_TENANT_ID',
    'tablegroup_id' : 'combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID)',
    'database_id' : 'combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID)',
    'table_type' : 'MAX_TABLE_TYPE',
    'index_type' : 'INDEX_TYPE_IS_NOT',
    'load_type' : 'TABLE_LOAD_TYPE_IN_DISK',
    'def_type' : 'TABLE_DEF_TYPE_INTERNAL',
    'rowkey_split_pos' : '0',
    'create_mem_version' : '1',
    'compress_func_name' : 'OB_DEFAULT_COMPRESS_FUNC_NAME',
    'part_level' : 'PARTITION_LEVEL_ZERO',
    'is_use_bloomfilter' : 'false',
    'progressive_merge_num' : '0',
    'charset_type' : 'ObCharset::get_default_charset()',
    'collation_type' : 'ObCharset::get_default_collation(ObCharset::get_default_charset())',
    'in_tenant_space' : False,
    'only_rs_vtable' : False,
    'view_definition' : '',
    'partition_expr' : [],
    'partition_columns' : [],
    'index' : [],
    'index_using_type' : 'USING_BTREE',
    'name_postfix': '',
    'row_store_type': 'FLAT_ROW_STORE',
    'store_format': 'OB_STORE_FORMAT_COMPACT_MYSQL',
    'migrate_data_before_2200': False,
    'columns_with_tenant_id': [],
    'progressive_merge_round' : '1',
    'storage_format_version' : '3',
    'is_cluster_private': False,
    'is_backup_private': False,
    'rs_restart_related': False,
    'is_real_virtual_table': False,
}
#
# Column definition:
# - Use [column_name, column_type, nullable, default_value] to specify column definition.
# - Use lowercase to define column names.
# - Define primary keys in rowkey_columns, and define other columns in normal_columns.
#
# Partition definition:
# - Defined by partition_expr and partition_columns.
# - Use [partition_type, expr, partition_num] to define partition_expr.
# - Use [col1, col2, ...] to define partition_columns.
# - Two different partition_type are supported: hash/key
#   - hash: expr means expression.
#   - key: expr means list of partition columns.
# - Distributed virtual table's partition_columns should be [`svr_ip`, `svr_port`].
# - rowkey_columns must contains columns defined in partition_columns.

################################################################################
# System Table(0,10000]
################################################################################


#
# Core Table (0, 100]
#
def_table_schema(
    table_name    = '__all_core_table',
    table_id      = '1',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('table_name', 'varchar:OB_MAX_CORE_TALBE_NAME_LENGTH'),
        ('row_id', 'int'),
        ('column_name', 'varchar:OB_MAX_COLUMN_NAME_LENGTH'),
    ],
    rs_restart_related = True,

  normal_columns = [
      ('column_value', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH', 'true'),
  ],
)


all_root_def = dict(
    table_name    = '__all_root_table',
    table_id      = '2',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('partition_id', 'int'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
    ],
    rs_restart_related = True,

  normal_columns = [
      ('sql_port', 'int'),
      ('unit_id', 'int'),
      ('partition_cnt', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('role', 'int'),
      ('member_list', 'varchar:MAX_MEMBER_LIST_LENGTH'),
      ('row_count', 'int'),
      ('data_size', 'int'),
      ('data_version', 'int'),
      ('data_checksum', 'int'),
      ('row_checksum', 'int'),
      ('column_checksum', 'varchar:COLUMN_CHECKSUM_LENGTH'),
      ('is_original_leader', 'int', 'false', '0'),
      ('is_previous_leader', 'int', 'false', '0'),
      ('create_time', 'int'),
      ('rebuild', 'int', 'false', '0'),
      ('replica_type', 'int', 'false', '0'),
      ('required_size', 'int', 'false', '0'),
      ('status', 'varchar:MAX_REPLICA_STATUS_LENGTH', 'false', 'REPLICA_STATUS_NORMAL'),
      ('is_restore', 'int', 'false', '0'),
      ('partition_checksum', 'int', 'false', '0'),
      ('quorum', 'int', 'false', '-1'),
      ('fail_list', 'varchar:OB_MAX_FAILLIST_LENGTH', 'false', ''),
      ('recovery_timestamp', 'int', 'false', '0'),
      ('memstore_percent', 'int', 'false', '100'),
      ('data_file_id', 'int', 'false', '0')
  ],
)

def_table_schema(**all_root_def)

all_table_def = dict(
    table_name    = '__all_table',
    table_id      = '3',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
    ],
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('table_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'false', ''),
      ('database_id', 'int'),
      ('table_type', 'int'),
      ('load_type', 'int'),
      ('def_type', 'int'),
      ('rowkey_column_num', 'int'),
      ('index_column_num', 'int'),
      ('max_used_column_id', 'int'),
      ('replica_num', 'int'),
      ('autoinc_column_id', 'int'),
      ('auto_increment', 'uint', 'true', '1'),
      ('read_only', 'int'),
      ('rowkey_split_pos', 'int'),
      ('compress_func_name', 'varchar:OB_MAX_COMPRESSOR_NAME_LENGTH'),
      ('expire_condition', 'varchar:OB_MAX_EXPIRE_INFO_STRING_LENGTH'),
      ('is_use_bloomfilter', 'int'),
      ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH', 'false', ''),
      ('block_size', 'int'),
      ('collation_type', 'int'),
      ('data_table_id', 'int', 'true'),
      ('index_status', 'int'),
      ('tablegroup_id', 'int'),
      ('progressive_merge_num', 'int'),
      ('index_type', 'int'),
      ('part_level', 'int'),
      ('part_func_type', 'int'),
      ('part_func_expr', 'varchar:OB_MAX_PART_FUNC_EXPR_LENGTH'),
      ('part_num', 'int'),
      ('sub_part_func_type', 'int'),
      ('sub_part_func_expr', 'varchar:OB_MAX_PART_FUNC_EXPR_LENGTH'),
      ('sub_part_num', 'int'),
      ('create_mem_version', 'int'),
      ('schema_version', 'int'),
      ('view_definition', 'longtext'),
      ('view_check_option', 'int'),
      ('view_is_updatable', 'int'),
      ('zone_list', 'varchar:MAX_ZONE_LIST_LENGTH'),
      ('primary_zone', 'varchar:MAX_ZONE_LENGTH', 'true'),
      ('index_using_type', 'int', 'false', 'USING_BTREE'),
      ('parser_name', 'varchar:OB_MAX_PARSER_NAME_LENGTH', 'true'),
      ('index_attributes_set', 'int', 'true', 0),
      ('locality', 'varchar:MAX_LOCALITY_LENGTH', 'false', ''),
      ('tablet_size', 'int', 'false', 'OB_DEFAULT_TABLET_SIZE'),
      ('pctfree', 'int', 'false', 'OB_DEFAULT_PCTFREE'),
      ('previous_locality', 'varchar:MAX_LOCALITY_LENGTH', 'false', ''),
      ('max_used_part_id', 'int', 'false', '-1'),
      ('partition_cnt_within_partition_table', 'int', 'false', '-1'),
      ('partition_status', 'int', 'true', '0'),
      ('partition_schema_version', 'int', 'true', '0'),
      ('max_used_constraint_id', 'int', 'true'),
      ('session_id', 'int', 'true', '0'),
      ('pk_comment', 'varchar:MAX_TABLE_COMMENT_LENGTH', 'false', ''),
      ('sess_active_time', 'int', 'true', '0'),
      ('row_store_type', 'varchar:OB_MAX_STORE_FORMAT_NAME_LENGTH', 'true', 'FLAT_ROW_STORE'),
      ('store_format', 'varchar:OB_MAX_STORE_FORMAT_NAME_LENGTH', 'true', ''),
      ('duplicate_scope', 'int', 'true', '0'),
      ('binding', 'bool', 'true', 'false'),
      ('progressive_merge_round', 'int', 'true', '0'),
      ('storage_format_version', 'int', 'true', '2'),
      ('table_mode', 'int', 'false', '0'),
      ('encryption', 'varchar:OB_MAX_ENCRYPTION_NAME_LENGTH', 'true', ''),
      ('tablespace_id', 'int', 'false', '-1'),
      ('drop_schema_version', 'int', 'false', '-1'),
      ('is_sub_part_template', 'bool', 'false', 'true'),
      ("dop", 'int', 'false', '1'),
      ('character_set_client', 'int', 'false', '0'),
      ('collation_connection', 'int', 'false', '0'),
      ('auto_part_size', 'int', 'false', '-1'),
      ('auto_part', 'bool', 'false', 'false')
    ],
    #index = {'i1' :  { 'index_columns' : ['data_table_id'],
    #                 'index_using_type' : 'USING_HASH'}},

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['table_id', 'database_id', 'data_table_id', 'tablegroup_id', 'tablespace_id'],
)

def_table_schema(**all_table_def)

all_column_def = dict(
    table_name    = '__all_column',
    table_id      = '4',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('column_id', 'int'),
    ],
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('column_name', 'varchar:OB_MAX_COLUMN_NAME_LENGTH', 'false', ''),
      ('rowkey_position', 'int', 'false', '0'),
      ('index_position', 'int'),
      ('order_in_rowkey', 'int'),
      ('partition_key_position', 'int'),
      ('data_type', 'int'),
      ('data_length', 'int'),
      ('data_precision', 'int', 'true'),
      ('data_scale', 'int', 'true'),
      ('zero_fill', 'int'),
      ('nullable', 'int'),
      ('on_update_current_timestamp', 'int'),
      ('autoincrement', 'int'),
      ('is_hidden', 'int', 'false', '0'),
      ('collation_type', 'int'),
      ('orig_default_value', 'varchar:OB_MAX_DEFAULT_VALUE_LENGTH', 'true'),
      ('cur_default_value', 'varchar:OB_MAX_DEFAULT_VALUE_LENGTH', 'true'),
      ('comment', 'longtext', 'true'),
      ('schema_version', 'int'),
      ('column_flags', 'int', 'false', '0'),
      ('prev_column_id', 'int', 'false', '-1'),
      ('extended_type_info', 'varbinary:OB_MAX_VARBINARY_LENGTH', 'true'),
      ('orig_default_value_v2', 'varbinary:OB_MAX_DEFAULT_VALUE_LENGTH', 'true'),
      ('cur_default_value_v2', 'varbinary:OB_MAX_DEFAULT_VALUE_LENGTH', 'true'),
    ],
    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['table_id'],
)

def_table_schema(**all_column_def)

def_table_schema(
    table_name    = '__all_ddl_operation',
    table_id      = '5',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('schema_version', 'int'),
    ],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('tenant_id', 'int'),
      ('user_id', 'int'),
      ('database_id', 'int'),
      ('database_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
      ('tablegroup_id', 'int'),
      ('table_id', 'int'),
      ('table_name', 'varchar:OB_MAX_CORE_TALBE_NAME_LENGTH'),
      ('operation_type', 'int'),
      ('ddl_stmt_str', 'longtext'),
      ('exec_tenant_id', 'int', 'false', '1'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['user_id', 'database_id', 'tablegroup_id', 'table_id'],
)

freeze_info_def = dict(
  table_name     = '__all_freeze_info',
  table_id       = '6',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('frozen_version', 'int'),
  ],

  normal_columns = [
  ('frozen_timestamp', 'int'),
  ('schema_version', 'int', 'true'),
  ],
)

all_table_v2_def = dict(
    table_name    = '__all_table_v2',
    table_id      = '7',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
    ],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('table_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'false', ''),
      ('database_id', 'int'),
      ('table_type', 'int'),
      ('load_type', 'int'),
      ('def_type', 'int'),
      ('rowkey_column_num', 'int'),
      ('index_column_num', 'int'),
      ('max_used_column_id', 'int'),
      ('replica_num', 'int'),
      ('autoinc_column_id', 'int'),
      ('auto_increment', 'uint', 'true', '1'),
      ('read_only', 'int'),
      ('rowkey_split_pos', 'int'),
      ('compress_func_name', 'varchar:OB_MAX_COMPRESSOR_NAME_LENGTH'),
      ('expire_condition', 'varchar:OB_MAX_EXPIRE_INFO_STRING_LENGTH'),
      ('is_use_bloomfilter', 'int'),
      ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH', 'false', ''),
      ('block_size', 'int'),
      ('collation_type', 'int'),
      ('data_table_id', 'int', 'true'),
      ('index_status', 'int'),
      ('tablegroup_id', 'int'),
      ('progressive_merge_num', 'int'),
      ('index_type', 'int'),
      ('part_level', 'int'),
      ('part_func_type', 'int'),
      ('part_func_expr', 'varchar:OB_MAX_PART_FUNC_EXPR_LENGTH'),
      ('part_num', 'int'),
      ('sub_part_func_type', 'int'),
      ('sub_part_func_expr', 'varchar:OB_MAX_PART_FUNC_EXPR_LENGTH'),
      ('sub_part_num', 'int'),
      ('create_mem_version', 'int'),
      ('schema_version', 'int'),
      ('view_definition', 'longtext'),
      ('view_check_option', 'int'),
      ('view_is_updatable', 'int'),
      ('zone_list', 'varchar:MAX_ZONE_LIST_LENGTH'),
      ('primary_zone', 'varchar:MAX_ZONE_LENGTH', 'true'),
      ('index_using_type', 'int', 'false', 'USING_BTREE'),
      ('parser_name', 'varchar:OB_MAX_PARSER_NAME_LENGTH', 'true'),
      ('index_attributes_set', 'int', 'true', 0),
      ('locality', 'varchar:MAX_LOCALITY_LENGTH', 'false', ''),
      ('tablet_size', 'int', 'false', 'OB_DEFAULT_TABLET_SIZE'),
      ('pctfree', 'int', 'false', 'OB_DEFAULT_PCTFREE'),
      ('previous_locality', 'varchar:MAX_LOCALITY_LENGTH', 'false', ''),
      ('max_used_part_id', 'int', 'false', '-1'),
      ('partition_cnt_within_partition_table', 'int', 'false', '-1'),
      ('partition_status', 'int', 'true', '0'),
      ('partition_schema_version', 'int', 'true', '0'),
      ('max_used_constraint_id', 'int', 'true'),
      ('session_id', 'int', 'true', '0'),
      ('pk_comment', 'varchar:MAX_TABLE_COMMENT_LENGTH', 'false', ''),
      ('sess_active_time', 'int', 'true', '0'),
      ('row_store_type', 'varchar:OB_MAX_STORE_FORMAT_NAME_LENGTH', 'true', 'FLAT_ROW_STORE'),
      ('store_format', 'varchar:OB_MAX_STORE_FORMAT_NAME_LENGTH', 'true', ''),
      ('duplicate_scope', 'int', 'true', '0'),
      ('binding', 'bool', 'true', 'false'),
      ('progressive_merge_round', 'int', 'true', '0'),
      ('storage_format_version', 'int', 'true', '2'),
      ('table_mode', 'int', 'false', '0'),
      ('encryption', 'varchar:OB_MAX_ENCRYPTION_NAME_LENGTH', 'true', ''),
      ('tablespace_id', 'int', 'false', '-1'),
      ('drop_schema_version', 'int', 'false', '-1'),
      ('is_sub_part_template', 'bool', 'false', 'true'),
      ("dop", 'int', 'false', '1'),
      ('character_set_client', 'int', 'false', '0'),
      ('collation_connection', 'int', 'false', '0'),
      ('auto_part_size', 'int', 'false', '-1'),
      ('auto_part', 'bool', 'false', 'false')
    ],
    columns_with_tenant_id = ['table_id', 'database_id', 'data_table_id', 'tablegroup_id', 'tablespace_id'],
)

def_table_schema(**all_table_v2_def)

#def_table_schema(**freeze_info_def)

#
# System Table (100, 1000]
#

def_table_schema(
    table_name    = '__all_meta_table',
    table_id      = '101',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('partition_id', 'int'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
    ],
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],

  normal_columns = [
      ('sql_port', 'int'),
      ('unit_id', 'int'),
      ('partition_cnt', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('role', 'int'),
      ('member_list', 'varchar:MAX_MEMBER_LIST_LENGTH'),
      ('row_count', 'int'),
      ('data_size', 'int'),
      ('data_version', 'int'),
      ('data_checksum', 'int', 'true'),
      ('row_checksum', 'int'),
      ('column_checksum', 'varchar:COLUMN_CHECKSUM_LENGTH'),
      ('is_original_leader', 'int', 'false', '0'),
      ('is_previous_leader', 'int', 'false', '0'),
      ('create_time', 'int'),
      ('rebuild', 'int', 'false', '0'),
      ('replica_type', 'int', 'false', '0'),
      ('required_size', 'int', 'false', '0'),
      ('status', 'varchar:MAX_REPLICA_STATUS_LENGTH', 'false', 'REPLICA_STATUS_NORMAL'),
      ('is_restore', 'int', 'false', '0'),
      ('partition_checksum', 'int', 'false', '0'),
      ('quorum', 'int', 'false', '-1'),
      ('fail_list', 'varchar:OB_MAX_FAILLIST_LENGTH', 'false', ''),
      ('recovery_timestamp', 'int', 'false', '0'),
      ('memstore_percent', 'int', 'false', '100')
  ],
)

all_user_def = dict(
    table_name    = '__all_user',
    table_id      = '102',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('user_id', 'int'),
    ],
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,
    normal_columns = [
      ('user_name', 'varchar:OB_MAX_USER_NAME_LENGTH_STORE'),
      ('host', 'varchar:OB_MAX_HOST_NAME_LENGTH', 'false', '%'),
      ('passwd', 'varchar:OB_MAX_PASSWORD_LENGTH'),
      ('info', 'varchar:OB_MAX_USER_INFO_LENGTH'),
      ('priv_alter', 'int', 'false', '0'),
      ('priv_create', 'int', 'false', '0'),
      ('priv_delete', 'int', 'false', '0'),
      ('priv_drop', 'int', 'false', '0'),
      ('priv_grant_option', 'int', 'false', '0'),
      ('priv_insert', 'int', 'false', '0'),
      ('priv_update', 'int', 'false', '0'),
      ('priv_select', 'int', 'false', '0'),
      ('priv_index', 'int', 'false', '0'),
      ('priv_create_view', 'int', 'false', '0'),
      ('priv_show_view', 'int', 'false', '0'),
      ('priv_show_db', 'int', 'false', '0'),
      ('priv_create_user', 'int', 'false', '0'),
      ('priv_super', 'int', 'false', '0'),
      ('is_locked', 'int'),
      ('priv_process', 'int', 'false', '0'),
      ('priv_create_synonym', 'int', 'false', '0'),
      ('ssl_type', 'int', 'false', '0'),
      ('ssl_cipher', 'varchar:1024', 'false', ''),
      ('x509_issuer', 'varchar:1024', 'false', ''),
      ('x509_subject', 'varchar:1024', 'false', ''),
      ('type', 'int', 'true', 0), #0: user; 1: role
      ('profile_id', 'int', 'false', 'OB_INVALID_ID'),
      ('password_last_changed', 'timestamp', 'true'),
      ('priv_file', 'int', 'false', '0'),
      ('priv_alter_tenant', 'int', 'false', '0'),
      ('priv_alter_system', 'int', 'false', '0'),
      ('priv_create_resource_pool', 'int', 'false', '0'),
      ('priv_create_resource_unit', 'int', 'false', '0'),
      ('max_connections', 'int', 'false', '0'),
      ('max_user_connections', 'int', 'false', '0'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['user_id', 'profile_id'],
)

def_table_schema(**all_user_def)

def_table_schema(**gen_history_table_def(103, all_user_def))

all_database_def = dict(
    table_name    = '__all_database',
    table_id      = '104',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('database_id', 'int'),
    ],
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('database_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
      ('replica_num', 'int'),
      ('zone_list', 'varchar:MAX_ZONE_LIST_LENGTH'),
      ('primary_zone', 'varchar:MAX_ZONE_LENGTH', 'true'),
      ('collation_type', 'int'),
      ('comment', 'varchar:MAX_DATABASE_COMMENT_LENGTH'),
      ('read_only', 'int'),
      ('default_tablegroup_id', 'int', 'false', 'OB_INVALID_ID'),
      ('in_recyclebin', 'int', 'false', '0'),
      ('drop_schema_version', 'int', 'false', '-1')
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['database_id', 'default_tablegroup_id']
)

def_table_schema(**all_database_def)

def_table_schema(**gen_history_table_def(105, all_database_def))

all_tablegroup_def = dict(
    table_name    = '__all_tablegroup',
    table_id      = '106',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('tablegroup_id', 'int'),
    ],
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('tablegroup_name', 'varchar:OB_MAX_TABLEGROUP_NAME_LENGTH'),
      ('comment', 'varchar:MAX_TABLEGROUP_COMMENT_LENGTH'),
      ('primary_zone', 'varchar:MAX_ZONE_LENGTH', 'true'),
      ('locality', 'varchar:MAX_LOCALITY_LENGTH', 'true', ''),
      ('part_level', 'int', 'false', '0'),
      ('part_func_type', 'int', 'false', '0'),
      ('part_func_expr_num', 'int', 'false', '0'),
      ('part_num', 'int', 'false', '0'),
      ('sub_part_func_type', 'int', 'false', '0'),
      ('sub_part_func_expr_num', 'int', 'false', '0'),
      ('sub_part_num', 'int', 'false', '0'),
      ('max_used_part_id', 'int', 'false', '-1'),
      ('schema_version', 'int'),
      ('partition_status', 'int', 'true', '0'),
      ('previous_locality', 'varchar:MAX_LOCALITY_LENGTH', 'false', ''),
      ('partition_schema_version', 'int', 'true', '0'),
      ('binding', 'bool', 'true', 'false'),
      ('drop_schema_version', 'int', 'false', '-1'),
      ('is_sub_part_template', 'bool', 'false', 'true'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['tablegroup_id'],
)

def_table_schema(**all_tablegroup_def)

def_table_schema(**gen_history_table_def(107, all_tablegroup_def))

all_tenant_def = dict(
    table_name    = '__all_tenant',
    table_type = 'SYSTEM_TABLE',
    table_id      = '108',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
    ],

  normal_columns = [
      ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE'),
      ('replica_num', 'int'),
      ('zone_list', 'varchar:MAX_ZONE_LIST_LENGTH'),
      ('primary_zone', 'varchar:MAX_ZONE_LENGTH', 'true'),
      ('locked', 'int'),
      ('collation_type', 'int'),
      ('info', 'varchar:OB_MAX_TENANT_INFO_LENGTH'),
      ('read_only', 'int'),
      ('rewrite_merge_version', 'int', 'false', '0'),
      ('locality', 'varchar:MAX_LOCALITY_LENGTH', 'false', ''),
      ('logonly_replica_num', 'int', 'false', '0'),
      ('previous_locality', 'varchar:MAX_LOCALITY_LENGTH', 'false', ''),
      ('storage_format_version', 'int', 'false', '0'),
      ('storage_format_work_version', 'int', 'false', '0'),
      ('default_tablegroup_id', 'int', 'false', 'OB_INVALID_ID'),
      ('compatibility_mode', 'int', 'false', '0'),
      ('drop_tenant_time', 'int', 'false', 'OB_INVALID_TIMESTAMP'),
      ('status', 'varchar:MAX_TENANT_STATUS_LENGTH', 'false', 'TENANT_STATUS_NORMAL'),
      ('in_recyclebin', 'int', 'false', '0')
  ],
)

def_table_schema(**all_tenant_def)

def_table_schema(**gen_history_table_def(109, all_tenant_def))

all_table_privilege_def = dict(
    table_name    = '__all_table_privilege',
    table_id      = '110',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('user_id', 'int'),
        ('database_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
        ('table_name', 'varchar:OB_MAX_CORE_TALBE_NAME_LENGTH'),
    ],
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('priv_alter', 'int', 'false', '0'),
      ('priv_create', 'int', 'false', '0'),
      ('priv_delete', 'int', 'false', '0'),
      ('priv_drop', 'int', 'false', '0'),
      ('priv_grant_option', 'int', 'false', '0'),
      ('priv_insert', 'int', 'false', '0'),
      ('priv_update', 'int', 'false', '0'),
      ('priv_select', 'int', 'false', '0'),
      ('priv_index', 'int', 'false', '0'),
      ('priv_create_view', 'int', 'false', '0'),
      ('priv_show_view', 'int', 'false', '0'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['user_id'],
)

def_table_schema(**all_table_privilege_def)

def_table_schema(**gen_history_table_def(111, all_table_privilege_def))

all_database_privilege_def = dict(
    table_name    = '__all_database_privilege',
    table_id      = '112',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('user_id', 'int'),
        ('database_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
    ],
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('priv_alter', 'int', 'false', '0'),
      ('priv_create', 'int', 'false', '0'),
      ('priv_delete', 'int', 'false', '0'),
      ('priv_drop', 'int', 'false', '0'),
      ('priv_grant_option', 'int', 'false', '0'),
      ('priv_insert', 'int', 'false', '0'),
      ('priv_update', 'int', 'false', '0'),
      ('priv_select', 'int', 'false', '0'),
      ('priv_index', 'int', 'false', '0'),
      ('priv_create_view', 'int', 'false', '0'),
      ('priv_show_view', 'int', 'false', '0'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['user_id'],
)

def_table_schema(**all_database_privilege_def)

def_table_schema(**gen_history_table_def(113, all_database_privilege_def))

def_table_schema(**gen_history_table_def(114, all_table_def))

def_table_schema(**gen_history_table_def(115, all_column_def))

def_table_schema(
    table_name    = '__all_zone',
    table_id      = '116',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('zone', 'varchar:MAX_ZONE_LENGTH'),
        ('name', 'varchar:TABLE_MAX_KEY_LENGTH'),
    ],
    rs_restart_related = True,

  normal_columns = [
      ('value', 'int'),
      ('info', 'varchar:MAX_ZONE_INFO_LENGTH'),
  ],
)

def_table_schema(
    table_name    = '__all_server',
    table_id      = '117',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
    ],
    rs_restart_related = True,

  normal_columns = [
      ('id', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('inner_port', 'int'),
      ('with_rootserver', 'int'),
      ('status', 'varchar:OB_SERVER_STATUS_LENGTH'),
      ('block_migrate_in_time', 'int'),
      ('build_version', 'varchar:OB_SERVER_VERSION_LENGTH'),
      ('stop_time', 'int', 'false', '0'),
      ('start_service_time', 'int'),
      ('first_sessid', 'int', 'false', '0'),
      ('with_partition', 'int', 'false', '0'),
      ('last_offline_time', 'int', 'false', '0')
  ],
)

def_table_schema(
    table_name    = '__all_sys_parameter',
    table_id      = '118',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('zone', 'varchar:MAX_ZONE_LENGTH'),
        ('svr_type', 'varchar:SERVER_TYPE_LENGTH'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
        ('name', 'varchar:OB_MAX_CONFIG_NAME_LEN'),
    ],

  normal_columns = [
      ('data_type', 'varchar:OB_MAX_CONFIG_TYPE_LENGTH', 'true'),
      ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN'),
      ('value_strict', 'varchar:OB_MAX_EXTRA_CONFIG_LENGTH', 'true'),
      ('info', 'varchar:OB_MAX_CONFIG_INFO_LEN'),
      ('need_reboot', 'int', 'true'),
      ('section', 'varchar:OB_MAX_CONFIG_SECTION_LEN', 'true'),
      ('visible_level', 'varchar:OB_MAX_CONFIG_VISIBLE_LEVEL_LEN', 'true'),
      ('config_version', 'int'),
      ('scope', 'varchar:OB_MAX_CONFIG_SCOPE_LEN', 'true'),
      ('source', 'varchar:OB_MAX_CONFIG_SOURCE_LEN', 'true'),
      ('edit_level', 'varchar:OB_MAX_CONFIG_EDIT_LEVEL_LEN', 'true'),
  ],
)

def_table_schema(
    table_name = '__tenant_parameter',
    table_id   = '119',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('zone', 'varchar:MAX_ZONE_LENGTH'),
        ('svr_type', 'varchar:SERVER_TYPE_LENGTH'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
        ('name', 'varchar:OB_MAX_CONFIG_NAME_LEN'),
    ],
    in_tenant_space = True,
    is_cluster_private = True,
    normal_columns = [
        ('data_type', 'varchar:OB_MAX_CONFIG_TYPE_LENGTH', 'true'),
        ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN'),
        ('info', 'varchar:OB_MAX_CONFIG_INFO_LEN'),
        ('section', 'varchar:OB_MAX_CONFIG_SECTION_LEN', 'true'),
        ('scope', 'varchar:OB_MAX_CONFIG_SCOPE_LEN', 'true'),
        ('source', 'varchar:OB_MAX_CONFIG_SOURCE_LEN', 'true'),
        ('edit_level', 'varchar:OB_MAX_CONFIG_EDIT_LEVEL_LEN', 'true'),
        ('config_version', 'int'),
    ],
)

all_sys_variable_def= dict(
    table_name     = '__all_sys_variable',
    table_id       = '120',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('zone', 'varchar:MAX_ZONE_LENGTH'),
        ('name', 'varchar:OB_MAX_CONFIG_NAME_LEN', 'false', ''),
    ],
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('data_type', 'int'),
      ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN', 'true'),
      ('info', 'varchar:OB_MAX_CONFIG_INFO_LEN'),
      ('flags', 'int'),
      ('min_val', 'varchar:OB_MAX_CONFIG_VALUE_LEN', 'false', ''),
      ('max_val', 'varchar:OB_MAX_CONFIG_VALUE_LEN', 'false', ''),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = [],
)
def_table_schema(**all_sys_variable_def)

def_table_schema(
    table_name     = '__all_sys_stat',
    table_id       = '121',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('zone', 'varchar:MAX_ZONE_LENGTH'),
        ('name', 'varchar:OB_MAX_CONFIG_NAME_LEN'),
    ],
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    in_tenant_space = True,

    normal_columns = [
      ('data_type', 'int'),
      ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN'),
      ('info', 'varchar:OB_MAX_CONFIG_INFO_LEN'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['value'],
)

def_table_schema(
    table_name     = '__all_column_statistic',
    table_id       = '122',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('partition_id', 'int'),
        ('column_id', 'int'),
    ],
    partition_expr = ['key', 'table_id', 16 ],
    partition_columns = ['table_id'],
    rs_restart_related = True,
    in_tenant_space = True,
    is_cluster_private = True,

    normal_columns = [
        ('num_distinct', 'int'),
        ('num_null', 'int'),
        ('min_value', 'varchar:MAX_VALUE_LENGTH'),
        ('max_value', 'varchar:MAX_VALUE_LENGTH'),
        ('llc_bitmap', 'varchar:MAX_LLC_BITMAP_LENGTH'),
        ('llc_bitmap_size', 'int'),
        ('version', 'int'),
        ('last_rebuild_version', 'int'),
    ],
    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['table_id'],
)

def_table_schema(
    table_name    = '__all_unit',
    table_id      = '123',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('unit_id', 'int'),
    ],
    rs_restart_related = True,
    normal_columns = [
        ('resource_pool_id', 'int'),
        ('group_id', 'int'),
        ('zone', 'varchar:MAX_ZONE_LENGTH'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
        ('migrate_from_svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('migrate_from_svr_port', 'int'),
        ('[discard]temporary_unit_svr_ip' ,'varchar:MAX_IP_ADDR_LENGTH'),
        ('[discard]temporary_unit_svr_port', 'int'),
        ('manual_migrate', 'bool', 'true', '0'),
        ('status', 'varchar:MAX_UNIT_STATUS_LENGTH', 'false', 'ACTIVE'),
        ('replica_type', 'int', 'false', '0'),
    ],
)

def_table_schema(
    table_name    = '__all_unit_config',
    table_id      = '124',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('unit_config_id', 'int'),
    ],
    rs_restart_related = True,
    normal_columns = [
        ('name', 'varchar:MAX_UNIT_CONFIG_LENGTH'),
        ('max_cpu', 'double'),
        ('min_cpu', 'double'),
        ('max_memory', 'int'),
        ('min_memory', 'int'),
        ('max_iops', 'int'),
        ('min_iops', 'int'),
        ('max_disk_size', 'int'),
        ('max_session_num', 'int'),
    ],
)

def_table_schema(
    table_name    = '__all_resource_pool',
    table_id      = '125',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('resource_pool_id', 'int'),
    ],
    rs_restart_related = True,
    normal_columns = [
        ('name', 'varchar:MAX_RESOURCE_POOL_LENGTH'),
        ('unit_count', 'int'),
        ('unit_config_id', 'int'),
        ('zone_list', 'varchar:MAX_ZONE_LIST_LENGTH'),
        ('tenant_id', 'int'),
        ('replica_type', 'int', 'false', '0'),
        ('is_tenant_sys_pool', 'bool', 'false', 'false')
    ],
)

def_table_schema(
    table_name = '__all_tenant_resource_usage',
    table_id = '126',
    table_type = 'SYSTEM_TABLE',
    gm_columns = [],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
    ],

    normal_columns = [
        ('report_time', 'timestamp'),
        ('cpu_quota', 'double'),
        ('cpu_quota_used', 'double'),
        ('mem_quota', 'double'),
        ('mem_quota_used', 'double'),
        ('disk_quota', 'double'),
        ('disk_quota_used', 'double'),
        ('iops_quota', 'double'),
        ('iops_quota_used', 'double'),
    ]
)

def_table_schema(
    table_name     = '__all_sequence',
    table_id       = '127',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('sequence_key', 'int'),
        ('column_id', 'int'),
    ],
    in_tenant_space = False,
    partition_expr = ['key', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],

  normal_columns = [
      ('sequence_name', 'varchar:OB_MAX_SEQUENCE_NAME_LENGTH', 'true'),
      ('sequence_value', 'uint', 'true'),
      ('sync_value', 'uint'),
      ('migrated', 'int', 'false', '0'),
  ],
)

def_table_schema(
    table_name = '__all_charset',
    table_id = '128',
    table_type = 'SYSTEM_TABLE',
    gm_columns = [],
    rowkey_columns = [
        ('charset', 'varchar:MAX_CHARSET_LENGTH', 'false', ''),
    ],
    normal_columns = [
        ('description', 'varchar:MAX_CHARSET_DESCRIPTION_LENGTH', 'false', ''),
        ('default_collation', 'varchar:MAX_COLLATION_LENGTH', 'false', ''),
        ('max_length', 'int', 'false', '0'),
    ]
)

def_table_schema(
    table_name = '__all_collation',
    table_id = '129',
    table_type = 'SYSTEM_TABLE',
    gm_columns = [],
    rowkey_columns = [
        ('collation', 'varchar:MAX_COLLATION_LENGTH', 'false', ''),
    ],
    normal_columns = [
        ('charset', 'varchar:MAX_CHARSET_LENGTH', 'false', ''),
        ('id', 'int', 'false', '0'),
        ('is_default', 'varchar:MAX_BOOL_STR_LENGTH', 'false', ''),
        ('is_compiled', 'varchar:MAX_BOOL_STR_LENGTH', 'false', ''),
        ('sortlen', 'int', 'false', '0'),
    ]
)

def_table_schema(
  table_name     = '__all_local_index_status',
  table_id       = '134',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
  ('tenant_id', 'int'),
  ('index_table_id', 'int'),
  ('partition_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int')
  ],

  normal_columns = [
  ('index_status', 'int'),
  ('ret_code', 'int'),
  ('role', 'int', 'false', '2')
  ],
  partition_expr = ['key','tenant_id', '16' ],
  partition_columns = ['tenant_id'],
)

def_table_schema(
  table_name     = '__all_dummy',
  table_id       = '135',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('key', 'int')
  ],
  rs_restart_related = True,
  in_tenant_space = True,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [],
)

def_table_schema(
  table_name     = '__all_frozen_map',
  table_id       = '136',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('frozen_version', 'int'),
  ('frozen_timestamp', 'int')
  ],
  in_tenant_space = False,

  normal_columns = [],
)

def_table_schema(
  table_name     = '__all_clog_history_info',
  table_id       = '137',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('table_id', 'int'),
  ('partition_idx', 'int'),
  ('partition_cnt', 'int'),
  ('start_log_id', 'int'),
  ('start_log_timestamp', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ],

  normal_columns = [
  ('end_log_id', 'int'),
  ('end_log_timestamp', 'int')
  ],
  partition_expr = ['hash', 'addr_to_partition_id(svr_ip, svr_port)', '16'],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_clog_history_info_v2',
  table_id       = '139',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
  ('table_id', 'uint'),
  ('partition_idx', 'int'),
  ('partition_cnt', 'int'),
  ('start_log_id', 'uint'),
  ('start_log_timestamp', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ],
  normal_columns = [
  ('end_log_id', 'uint'),
  ('end_log_timestamp', 'int')
  ],
  partition_expr = ['key', 'table_id, partition_idx, partition_cnt', '16'],
  partition_columns = ['table_id', 'partition_idx', 'partition_cnt'],

  in_tenant_space = True,
  is_cluster_private = True,
  is_backup_private = True,
  migrate_data_before_2200 = True,
  columns_with_tenant_id = [],
)

def_table_schema(
    table_name = '__all_rootservice_event_history',
    table_id = '140',
    table_type = 'SYSTEM_TABLE',
    gm_columns = [],
    rowkey_columns = [
      ('gmt_create', 'timestamp:6', 'false')
    ],
    normal_columns = [
      ('module', 'varchar:MAX_ROOTSERVICE_EVENT_DESC_LENGTH', 'false'),
      ('event', 'varchar:MAX_ROOTSERVICE_EVENT_DESC_LENGTH', 'false'),
      ('name1', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value1', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('name2', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value2', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('name3', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value3', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('name4', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value4', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('name5', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value5', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('name6', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value6', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('extra_info', 'varchar:MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH', 'true', ''),
      ('rs_svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'true', ''),
      ('rs_svr_port', 'int', 'true', '0'),
    ],
)

def_table_schema(
  table_name = '__all_privilege',
  table_id = '141',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
    ('Privilege', 'varchar:MAX_COLUMN_PRIVILEGE_LENGTH'),
  ],
  normal_columns = [
    ('Context', 'varchar:MAX_PRIVILEGE_CONTEXT_LENGTH'),
    ('Comment', 'varchar:MAX_COLUMN_COMMENT_LENGTH'),
  ]
)

all_outline_def = dict(
    table_name    = '__all_outline',
    table_id      = '142',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('outline_id', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('database_id', 'int'),
      ('schema_version', 'int'),
      ('name', 'varchar:OB_MAX_OUTLINE_NAME_LENGTH', 'false', ''),
      ('signature', 'varbinary:OB_MAX_OUTLINE_SIGNATURE_LENGTH', 'false', ''),
      ('outline_content', 'longtext', 'false'),
      ('sql_text', 'longtext', 'false'),
      ('owner', 'varchar:OB_MAX_USERNAME_LENGTH', 'false', ''),
      ('used', 'int', 'false', '0'),
      ('version', 'varchar:OB_SERVER_VERSION_LENGTH', 'false', ''),
      ('compatible', 'int', 'false', '1'),
      ('enabled', 'int', 'false', '1'),
      ('format', 'int', 'false', '0'),
      ('outline_params', 'varbinary:OB_MAX_OUTLINE_PARAMS_LENGTH', 'false', ''),
      ('outline_target', 'longtext', 'false'),
      ('sql_id', 'varbinary:OB_MAX_SQL_ID_LENGTH', 'false', ''),
      ('owner_id', 'int', 'true'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['outline_id', 'database_id'],
)

def_table_schema(**all_outline_def)

def_table_schema(**gen_history_table_def(143, all_outline_def))

def_table_schema(
  table_name = '__all_election_event_history',
  table_id = '144',
  table_type = 'SYSTEM_TABLE',
  part_level = 'PARTITION_LEVEL_ONE',
  gm_columns = [],
  rowkey_columns = [
    ('gmt_create', 'timestamp:6', 'false'),
    ('table_id', 'uint'),
    ('partition_idx', 'int'),
    ('partition_cnt', 'int'),
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
  ],
  normal_columns = [
    ('event', 'varchar:MAX_ELECTION_EVENT_DESC_LENGTH', 'false'),
    ('leader', 'varchar:OB_IP_PORT_STR_BUFF', 'false', '0.0.0.0'),
    ('info', 'varchar:MAX_ELECTION_EVENT_EXTRA_INFO_LENGTH', 'false', ''),
  ],
)

def_table_schema(
  table_name = '__all_recyclebin',
  table_id = '145',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create'],
  partition_expr = ['key_v2', 'tenant_id', 16 ],
  partition_columns = ['tenant_id'],
  in_tenant_space = True,
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('object_name', 'varchar:OB_MAX_OBJECT_NAME_LENGTH'),
    ('type', 'int'),
  ],

  normal_columns = [
    ('database_id', 'int'),
    ('table_id', 'int'),
    ('tablegroup_id', 'int'),
    ('original_name', 'varchar:OB_MAX_ORIGINAL_NANE_LENGTH'),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['database_id', 'table_id', 'tablegroup_id'],
)

all_part_def = dict(
    table_name    = '__all_part',
    table_id      = '146',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('part_id', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('part_name', 'varchar:OB_MAX_PARTITION_NAME_LENGTH', 'false', ''),
      ('schema_version', 'int'),
      ('high_bound_val', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('b_high_bound_val', 'varchar:OB_MAX_B_HIGH_BOUND_VAL_LENGTH', 'true'),
      ('sub_part_num', 'int', 'true'),
      ('sub_part_space', 'int', 'true'),
      ('new_sub_part_num', 'int', 'true'),
      ('new_sub_part_space', 'int', 'true'),
      ('sub_part_interval', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('sub_interval_start', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('new_sub_part_interval', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('new_sub_interval_start', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('block_size', 'int', 'true'),
      ('replica_num', 'int', 'true'),
      ('compress_func_name', 'varchar:OB_MAX_COMPRESSOR_NAME_LENGTH', 'true'),
      ('status', 'int', 'true'),
      ('spare1', 'int', 'true'),
      ('spare2', 'int', 'true'),
      ('spare3', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH', 'true'),
      ('comment', 'varchar:OB_MAX_PARTITION_COMMENT_LENGTH', 'true'),
      ('list_val', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('b_list_val', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH', 'true'),
      ('part_idx', 'int', 'true'),
      ('source_partition_id', 'varchar:MAX_VALUE_LENGTH', 'true', ''),
      ('mapping_pg_part_id', 'int', 'true', '-1'),
      ('tablespace_id', 'int', 'false', '-1'),
      ('drop_schema_version', 'int', 'false', '-1'),
      ('max_used_sub_part_id', 'int', 'false', '-1'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['table_id', 'tablespace_id'],
)

def_table_schema(**all_part_def)

def_table_schema(**gen_history_table_def(147, all_part_def))

all_sub_part_def = dict(
    table_name    = '__all_sub_part',
    table_id      = '148',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('part_id', 'int'),
        ('sub_part_id', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    in_tenant_space = True,

    normal_columns = [
      ('sub_part_name', 'varchar:OB_MAX_PARTITION_NAME_LENGTH', 'false', ''),
      ('schema_version', 'int'),
      ('high_bound_val', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('b_high_bound_val', 'varchar:OB_MAX_B_HIGH_BOUND_VAL_LENGTH', 'true'),
      ('block_size', 'int', 'true'),
      ('replica_num', 'int', 'true'),
      ('compress_func_name', 'varchar:OB_MAX_COMPRESSOR_NAME_LENGTH', 'true'),
      ('status', 'int', 'true'),
      ('spare1', 'int', 'true'),
      ('spare2', 'int', 'true'),
      ('spare3', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH', 'true'),
      ('comment', 'varchar:OB_MAX_PARTITION_COMMENT_LENGTH', 'true'),
      ('list_val', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('b_list_val', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH', 'true'),
      ('tablespace_id', 'int', 'false', '-1'),
      ('sub_part_idx', 'int', 'false', '-1'),
      ('source_partition_id', 'varchar:MAX_VALUE_LENGTH', 'false', ''),
      ('mapping_pg_sub_part_id', 'int', 'false', '-1'),
      ('drop_schema_version', 'int', 'false', '-1'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['table_id', 'tablespace_id'],
)

def_table_schema(**all_sub_part_def)

def_table_schema(**gen_history_table_def(149, all_sub_part_def))

all_part_info_def = dict(
    table_name    = '__all_part_info',
    table_id      = '150',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    in_tenant_space = True,

    normal_columns = [
      ('part_type', 'int', 'false'),
      ('schema_version', 'int'),
      ('part_num', 'int', 'false'),
      ('part_space', 'int', 'false'),
      ('new_part_num', 'int', 'true'),
      ('new_part_space', 'int', 'true'),
      ('sub_part_type', 'int', 'true'),
      ('def_sub_part_num', 'int', 'true'),
      ('part_expr', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('sub_part_expr', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('part_interval', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('interval_start', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('new_part_interval', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('new_interval_start', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('def_sub_part_interval', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('def_sub_interval_start', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('new_def_sub_part_interval', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('new_def_sub_interval_start', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('block_size', 'int', 'true'),
      ('replica_num', 'int', 'true'),
      ('compress_func_name', 'varchar:OB_MAX_COMPRESSOR_NAME_LENGTH', 'true'),
      ('spare1', 'int', 'true'),
      ('spare2', 'int', 'true'),
      ('spare3', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH', 'true'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['table_id'],
)

def_table_schema(**all_part_info_def)

def_table_schema(**gen_history_table_def(151, all_part_info_def))

all_def_sub_part_def = dict(
    table_name    = '__all_def_sub_part',
    table_id      = '152',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('sub_part_id', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('sub_part_name', 'varchar:OB_MAX_PARTITION_NAME_LENGTH', 'false', ''),
      ('schema_version', 'int'),
      ('high_bound_val', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('b_high_bound_val', 'varchar:OB_MAX_B_HIGH_BOUND_VAL_LENGTH', 'true'),
      ('block_size', 'int', 'true'),
      ('replica_num', 'int', 'true'),
      ('compress_func_name', 'varchar:OB_MAX_COMPRESSOR_NAME_LENGTH', 'true'),
      ('spare1', 'int', 'true'),
      ('spare2', 'int', 'true'),
      ('spare3', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH', 'true'),
      ('comment', 'varchar:OB_MAX_PARTITION_COMMENT_LENGTH', 'true'),
      ('list_val', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
      ('b_list_val', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH', 'true'),
      ('sub_part_idx', 'int', 'true'),
      ('source_partition_id', 'varchar:MAX_VALUE_LENGTH', 'true', ''),
      ('mapping_pg_sub_part_id', 'int', 'true', '-1'),
      ('tablespace_id', 'int', 'false', '-1')
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['table_id', 'tablespace_id'],
)

def_table_schema(**all_def_sub_part_def)

def_table_schema(**gen_history_table_def(153, all_def_sub_part_def))

def_table_schema(
    table_name = '__all_server_event_history',
    table_id = '154',
    table_type = 'SYSTEM_TABLE',
    gm_columns = [],
    rowkey_columns = [
        ('gmt_create', 'timestamp:6', 'false'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
    ],
    partition_expr = ['key_v2', 'svr_ip, svr_port', '16'],
    partition_columns = ['svr_ip', 'svr_port'],

    normal_columns = [
      ('module', 'varchar:MAX_ROOTSERVICE_EVENT_DESC_LENGTH', 'false'),
      ('event', 'varchar:MAX_ROOTSERVICE_EVENT_DESC_LENGTH', 'false'),
      ('name1', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value1', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('name2', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value2', 'longtext', 'true'),
      ('name3', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value3', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('name4', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value4', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('name5', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value5', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('name6', 'varchar:MAX_ROOTSERVICE_EVENT_NAME_LENGTH', 'true', ''),
      ('value6', 'varchar:MAX_ROOTSERVICE_EVENT_VALUE_LENGTH', 'true', ''),
      ('extra_info', 'varchar:MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH', 'true', ''),
    ],
)

def_table_schema(
    table_name = '__all_rootservice_job',
    table_id = '155',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('job_id', 'int')
    ],
    normal_columns = [
        ('job_type', 'varchar:128', 'false'),
        ('job_status', 'varchar:128', 'false'),
        ('return_code', 'int', 'true'),
        ('progress', 'int', 'false', '0'),
        ('tenant_id', 'int', 'true'),
        ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE', 'true'),
        ('database_id', 'int', 'true'),
        ('database_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'true'),
        ('table_id', 'int', 'true'),
        ('table_name', 'varchar:OB_MAX_CORE_TALBE_NAME_LENGTH', 'true'),
        ('partition_id', 'int', 'true'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'true'),
        ('svr_port', 'int', 'true'),
        ('unit_id', 'int', 'true'),
        ('rs_svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
        ('rs_svr_port', 'int', 'false'),
        ('sql_text', 'longtext', 'true'),
        ('extra_info', 'varchar:MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH', 'true'),
        ('resource_pool_id', 'int', 'true'),
        ('tablegroup_id', 'int', 'true'),
        ('tablegroup_name', 'varchar:OB_MAX_TABLEGROUP_NAME_LENGTH', 'true'),
    ],
)

def_table_schema(
  table_name     = '__all_unit_load_history',
  table_id       = '156',
  table_type = 'SYSTEM_TABLE',
  gm_columns     = [],
  rowkey_columns = [
      ('gmt_create', 'timestamp:6', 'false'),
  ],
  normal_columns = [
      ('tenant_id', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'true'),
      ('svr_port', 'int', 'true'),
      ('unit_id', 'int'),
      ('load', 'double'),
      ('disk_usage_rate', 'double'),
      ('memory_usage_rate', 'double'),
      ('cpu_usage_rate', 'double'),
      ('iops_usage_rate', 'double'),
      ('disk_weight', 'double'),
      ('memory_weight', 'double'),
      ('cpu_weight', 'double'),
      ('iops_weight', 'double'),
      ('rs_svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'true'),
      ('rs_svr_port', 'int', 'true'),
  ],
)

all_sys_variable_history_def= dict(
    table_name     = '__all_sys_variable_history',
    table_id       = '157',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('zone', 'varchar:MAX_ZONE_LENGTH'),
        ('name', 'varchar:OB_MAX_CONFIG_NAME_LEN', 'false', ''),
        ('schema_version', 'int')
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,
    normal_columns = [
      ('is_deleted', 'int', 'false'),
      ('data_type', 'int'),
      ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN', 'true'),
      ('info', 'varchar:OB_MAX_CONFIG_INFO_LEN'),
      ('flags', 'int'),
      ('min_val', 'varchar:OB_MAX_CONFIG_VALUE_LEN', 'false', ''),
      ('max_val', 'varchar:OB_MAX_CONFIG_VALUE_LEN', 'false', ''),
    ],
    migrate_data_before_2200 = True,
    columns_with_tenant_id = [],
)
def_table_schema(**all_sys_variable_history_def)

def_table_schema(
    table_name = '__all_restore_job',
    table_id = '158',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
      ('job_id', 'int')
    ],
    normal_columns = [
      ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH'),
      ('start_time', 'int'),
      ('backup_uri', 'varchar:2048'),
      ('backup_end_time', 'int'),
      ('recycle_end_time', 'int'),
      ('level', 'int'),
      ('status', 'int')
    ],
)

def_table_schema(
    table_name = '__all_restore_task',
    table_id = '159',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int')
    ],
    normal_columns = [
      ('backup_table_id', 'int'),
      ('index_map', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH', 'true'),
      ('start_time', 'int'),
      ('status', 'int'),
      ('job_id', 'int')
    ],
)

def_table_schema(
    table_name = '__all_restore_job_history',
    table_id = '160',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
      ('job_id', 'int')
    ],
    normal_columns = [
      ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH'),
      ('start_time', 'int'),
      ('backup_uri', 'varchar:2048'),
      ('backup_end_time', 'int'),
      ('recycle_end_time', 'int'),
      ('level', 'int'),
      ('status', 'int')
    ],
)

def_table_schema(
  table_name     = '__all_time_zone',
  table_id       = '161',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
    ('Time_zone_id', 'int','false', 'NULL', 'true'),
  ],
  in_tenant_space = False,

  normal_columns = [
  ('Use_leap_seconds', 'varchar:8','false', 'N'),
  ('Version', 'int', 'true'),
  ],
)

def_table_schema(
  table_name     = '__all_time_zone_name',
  table_id       = '162',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
   ('Name', 'varchar:64', 'false')
  ],
  in_tenant_space = False,

  normal_columns = [
  ('Time_zone_id', 'int', 'false'),
  ('Version', 'int', 'true'),
  ],
)

def_table_schema(
  table_name     = '__all_time_zone_transition',
  table_id       = '163',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('Time_zone_id', 'int', 'false'),
  ('Transition_time', 'int', 'false'),
  ],
  in_tenant_space = False,

  normal_columns = [
  ('Transition_type_id', 'int', 'false'),
  ('Version', 'int', 'true'),
  ],
)

def_table_schema(
  table_name     = '__all_time_zone_transition_type',
  table_id       = '164',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('Time_zone_id', 'int', 'false'),
  ('Transition_type_id', 'int', 'false'),
  ],
  in_tenant_space = False,

  normal_columns = [
  ('Offset', 'int', 'false', '0'),
  ('Is_DST', 'int', 'false', '0'),
  ('Abbreviation', 'varchar:8', 'false', ''),
  ('Version', 'int', 'true'),
  ],
)

def_table_schema(
  table_name     = '__all_ddl_id',
  table_id       = '165',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
      ('tenant_id', 'int', 'false'),
      ('ddl_id_str', 'varchar:OB_MAX_DDL_ID_STR_LENGTH', 'false'),
  ],
  partition_expr = ['key_v2', 'tenant_id', 16 ],
  partition_columns = ['tenant_id'],
  in_tenant_space = True,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
  ('ddl_stmt_str', 'longtext'),
  ],
  migrate_data_before_2200 = True,
)

all_foreign_key_def = dict(
  table_name    = '__all_foreign_key',
  table_id      = '166',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('foreign_key_id', 'int'),
  ],
  partition_expr = ['key_v2', 'tenant_id', 16 ],
  partition_columns = ['tenant_id'],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
    ('foreign_key_name', 'varchar:OB_MAX_CONSTRAINT_NAME_LENGTH', 'false', ''),
    ('child_table_id', 'int'),
    ('parent_table_id', 'int'),
    ('update_action', 'int'),
    ('delete_action', 'int'),
    ('enable_flag', 'bool', 'false', 'true'),
    ('ref_cst_type', 'int', 'false', '0'),
    ('ref_cst_id', 'int', 'false', '-1'),
    ('validate_flag', 'bool', 'false', 'true'),
    ('rely_flag', 'bool', 'false', 'false'),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['foreign_key_id', 'child_table_id', 'parent_table_id'],
)

def_table_schema(**all_foreign_key_def)

def_table_schema(**gen_history_table_def(167, all_foreign_key_def))

all_foreign_key_column_def = dict(
  table_name    = '__all_foreign_key_column',
  table_id      = '168',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('foreign_key_id', 'int'),
    ('child_column_id', 'int'),
    ('parent_column_id', 'int'),
  ],
  partition_expr = ['key_v2', 'tenant_id', 16 ],
  partition_columns = ['tenant_id'],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
   ('position', 'int', 'false', '0'),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['foreign_key_id'],
)

def_table_schema(**all_foreign_key_column_def)

def_table_schema(**gen_history_table_def(169, all_foreign_key_column_def))

all_synonym_def = dict(
  table_name    = '__all_synonym',
  table_id      = '180',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
  ('tenant_id', 'int'),
  ('synonym_id', 'int'),
  ],
  partition_expr = ['key_v2', 'tenant_id', 16 ],
  partition_columns = ['tenant_id'],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
  ('database_id', 'int'),
  ('schema_version', 'int'),
  ('synonym_name', 'varchar:OB_MAX_SYNONYM_NAME_LENGTH', 'false', ''),
  ('object_name', 'varchar:OB_MAX_SYNONYM_NAME_LENGTH', 'false', ''),
  ('object_database_id', 'int'),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['synonym_id', 'database_id', 'object_database_id'],
)

def_table_schema(**all_synonym_def)

def_table_schema(**gen_history_table_def(181, all_synonym_def))

def_table_schema(
    table_name     = '__all_sequence_v2',
    table_id       = '182',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('sequence_key', 'int'),
        ('column_id', 'int'),
    ],
    in_tenant_space = True,

    normal_columns = [
      ('tenant_id', 'int'),
      ('sequence_name', 'varchar:OB_MAX_SEQUENCE_NAME_LENGTH', 'true'),
      ('sequence_value', 'uint', 'true'),
      ('sync_value', 'uint'),
    ],

    columns_with_tenant_id = ['sequence_key'],
)

def_table_schema(
    table_name    = '__all_tenant_meta_table',
    table_id      = '183',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('partition_id', 'int'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
    ],

    rs_restart_related = True,
    in_tenant_space = True,
    is_cluster_private = True,
    is_backup_private = True,

  normal_columns = [
      ('sql_port', 'int'),
      ('unit_id', 'int'),
      ('partition_cnt', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('role', 'int'),
      ('member_list', 'varchar:MAX_MEMBER_LIST_LENGTH'),
      ('row_count', 'int'),
      ('data_size', 'int'),
      ('data_version', 'int'),
      ('data_checksum', 'int'),
      ('row_checksum', 'int'),
      ('column_checksum', 'varchar:COLUMN_CHECKSUM_LENGTH'),
      ('is_original_leader', 'int', 'false', '0'),
      ('is_previous_leader', 'int', 'false', '0'),
      ('create_time', 'int'),
      ('rebuild', 'int', 'false', '0'),
      ('replica_type', 'int', 'false', '0'),
      ('required_size', 'int', 'false', '0'),
      ('status', 'varchar:MAX_REPLICA_STATUS_LENGTH', 'false', 'REPLICA_STATUS_NORMAL'),
      ('is_restore', 'int', 'false', '0'),
      ('partition_checksum', 'int', 'false', '0'),
      ('quorum', 'int', 'false', '-1'),
      ('fail_list', 'varchar:OB_MAX_FAILLIST_LENGTH', 'false', ''),
      ('recovery_timestamp', 'int', 'false', '0'),
      ('memstore_percent', 'int', 'false', '100'),
      ('data_file_id', 'int', 'false', '0')
  ],
  columns_with_tenant_id = [],
)

def_table_schema(
  table_name     = '__all_index_wait_transaction_status',
  table_id       = '186',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
  ('tenant_id', 'int'),
  ('index_table_id', 'int'),
  ('svr_type', 'int'),
  ('partition_id', 'int'),
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('trans_status', 'int'),
  ('snapshot_version', 'int'),
  ('frozen_version', 'int'),
  ('schema_version', 'int')
  ],
  partition_expr = ['key_v2','tenant_id', '16' ],
  partition_columns = ['tenant_id'],
)

def_table_schema(
  table_name     = '__all_index_schedule_task',
  table_id       = '187',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
  ('tenant_id', 'int'),
  ('index_table_id', 'int'),
  ('partition_id', 'int'),
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('frozen_version', 'int'),
  ('snapshot_version', 'int'),
  ],
  partition_expr = ['key_v2','tenant_id', '16' ],
  partition_columns = ['tenant_id'],
)

def_table_schema(
  table_name     = '__all_index_checksum',
  table_id       = '188',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
  ('execution_id', 'int'),
  ('tenant_id', 'int'),
  ('table_id', 'int'),
  ('partition_id', 'int'),
  ('column_id', 'int'),
  ('task_id', 'int'),
  ],

  normal_columns = [
  ('checksum', 'int'),
  ('checksum_method', 'int', 'false', 0)
  ],
  partition_expr = ['key_v2','tenant_id', '16' ],
  partition_columns = ['tenant_id'],
)

all_routine_def = dict(
    table_name    = '__all_routine',
    table_id      = '189',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('routine_id', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('database_id', 'int', 'false'),
      ('package_id', 'int', 'false'),
      ('routine_name', 'varchar:OB_MAX_ROUTINE_NAME_LENGTH'),
      ('overload', 'int'),
      ('subprogram_id', 'int', 'false'),
      ('schema_version', 'int'),
      ('routine_type', 'int', 'false'),
      ('flag', 'int', 'false'),
      ('owner_id', 'int', 'false'),
      ('priv_user', 'varchar:OB_MAX_USER_NAME_LENGTH_STORE', 'true'),
      ('comp_flag', 'int', 'true'),
      ('exec_env', 'varchar:OB_MAX_PROC_ENV_LENGTH', 'true'),
      ('routine_body', 'longtext', 'true'),
      ('comment', 'varchar:MAX_TENANT_COMMENT_LENGTH', 'true'),
      ('route_sql', 'longtext', 'true')
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['routine_id', 'package_id', 'database_id', 'owner_id'],
)

def_table_schema(**all_routine_def)

def_table_schema(**gen_history_table_def(190, all_routine_def))

all_routine_param_def = dict(
    table_name    = '__all_routine_param',
    table_id      = '191',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('routine_id', 'int'),
        ('sequence', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('subprogram_id', 'int', 'false'),
      ('param_position', 'int', 'false'),
      ('param_level', 'int', 'false'),
      ('param_name', 'varchar:OB_MAX_COLUMN_NAME_LENGTH', 'true', ''),
      ('schema_version', 'int'),
      ('param_type', 'int', 'false'),
      ('param_length', 'int'),
      ('param_precision', 'int', 'true'),
      ('param_scale', 'int', 'true'),
      ('param_zero_fill', 'int'),
      ('param_charset', 'int', 'true'),
      ('param_coll_type', 'int'),
      ('flag', 'int', 'false'),
      ('default_value', 'varchar:OB_MAX_DEFAULT_VALUE_LENGTH', 'true'),
      ('type_owner', 'int', 'true'),
      ('type_name', 'varchar:OB_MAX_COLUMN_NAME_LENGTH', 'true', ''),
      ('type_subname', 'varchar:OB_MAX_COLUMN_NAME_LENGTH', 'true', ''),
      ('extended_type_info', "varbinary:OB_MAX_VARBINARY_LENGTH", 'true', '')
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['routine_id', 'type_owner'],
)

def_table_schema(**all_routine_param_def)
def_table_schema(**gen_history_table_def(192, all_routine_param_def))

def_table_schema(
  table_name = '__all_table_stat',
  table_id = '193',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
  ],
  partition_expr = ['key_v2', 'table_id', 16],
  partition_columns = ['table_id'],
  rs_restart_related = True,
  in_tenant_space = True,
  is_cluster_private = True,

  normal_columns = [
      ('object_type', 'int'),
      ('last_analyzed', 'timestamp'),
      ('sstable_row_cnt', 'int'),
      ('sstable_avg_row_len', 'int'),
      ('macro_blk_cnt', 'int'),
      ('micro_blk_cnt', 'int'),
      ('memtable_row_cnt', 'int'),
      ('memtable_avg_row_len', 'int'),
  ],
  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['table_id'],
)

def_table_schema(
  table_name = '__all_column_stat',
  table_id = '194',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
      ('column_id', 'int'),
  ],
  partition_expr = ['key_v2', 'table_id', 16],
  partition_columns = ['table_id'],
  rs_restart_related = True,
  in_tenant_space = True,
  is_cluster_private = True,

  normal_columns = [
      ('object_type', 'int'),
      ('last_analyzed', 'timestamp'),
      ('distinct_cnt', 'int'),
      ('null_cnt', 'int'),
      ('max_value', 'varchar:MAX_VALUE_LENGTH'),
      ('b_max_value', 'varchar:MAX_VALUE_LENGTH'),
      ('min_value', 'varchar:MAX_VALUE_LENGTH'),
      ('b_min_value', 'varchar:MAX_VALUE_LENGTH'),
      ('avg_len', 'int'),
      ('distinct_cnt_synopsis','varchar:MAX_LLC_BITMAP_LENGTH'),
      ('distinct_cnt_synopsis_size', 'int'),
      ('sample_size', 'double'),
      ('density', 'double'),
      ('bucket_cnt', 'int'),
      ('histogram_type', 'int'),
  ],
  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['table_id'],
)

def_table_schema(
  table_name = '__all_histogram_stat',
  table_id = '195',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
      ('column_id', 'int'),
      ('endpoint_num', 'int'),
  ],
  partition_expr = ['key_v2', 'table_id', 16],
  partition_columns = ['table_id'],
  rs_restart_related = True,
  in_tenant_space = True,
  is_cluster_private = True,

  normal_columns = [
      ('object_type', 'int'),
      ('endpoint_value', 'varchar:MAX_VALUE_LENGTH'),
      ('b_endpoint_value', 'varchar:MAX_VALUE_LENGTH'),
      ('endpoint_repeat_cnt', 'int'),
  ],
  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['table_id'],
)

all_package_def = dict(
    table_name    = '__all_package',
    table_id      = '196',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('package_id', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('database_id', 'int', 'false'),
      ('package_name', 'varchar:OB_MAX_PACKAGE_NAME_LENGTH', 'false', ''),
      ('schema_version', 'int', 'false'),
      ('type', 'int', 'false'),
      ('flag', 'int', 'false'),
      ('owner_id', 'int', 'false'),
      ('comp_flag', 'int', 'true'),
      ('exec_env', 'varchar:OB_MAX_PROC_ENV_LENGTH', 'true'),
      ('source', 'longtext', 'true'),
      ('comment', 'varchar:MAX_TENANT_COMMENT_LENGTH', 'true'),
      ('route_sql', 'longtext', 'true')
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['package_id', 'database_id', 'owner_id'],
)

def_table_schema(**all_package_def)
def_table_schema(**gen_history_table_def(197, all_package_def))

def_table_schema(
  table_name = '__all_sql_execute_task',
  table_id = '198',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
      ('job_id', 'int'),
      ('execution_id', 'int'),
      ('sql_job_id', 'int'),
      ('task_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int')
  ],

  normal_columns = [
    ('slice_count', 'int'),
    ('task_stat', 'varchar:64'),
    ('task_result', 'int'),
    ('task_info', 'varchar:MAX_VALUE_LENGTH')
  ],
)

def_table_schema(
  table_name = '__all_index_build_stat',
  table_id = '199',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('data_table_id', 'int'),
    ('index_table_id', 'int'),
  ],
  rs_restart_related = True,

  normal_columns = [
    ('status', 'int'),
    ('snapshot', 'int'),
    ('schema_version', 'int'),
  ],
)

def_table_schema(
  table_name = '__all_build_index_param',
  table_id = '200',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
      ('job_id', 'int'),
      ('snapshot_version', 'int'),
      ('execution_id', 'int'),
      ('seq_no', 'int'),
  ],

  normal_columns = [
      ('param', 'varchar:MAX_VALUE_LENGTH')
  ],
)

def_table_schema(
  table_name = '__all_global_index_data_src',
  table_id = '201',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('index_table_id', 'int'),
    ('data_table_id', 'int'),
    ('data_partition_id', 'int'),
  ],

  normal_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
  ],
)


def_table_schema(
  table_name     = '__all_acquired_snapshot',
  table_id       = '202',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
    ('gmt_create', 'timestamp:6', 'false')
    ],
  rs_restart_related = True,
  normal_columns = [
    ('snapshot_type', 'int'),
    ('snapshot_ts', 'int'),
    ('schema_version', 'int', 'true'),
    ('tenant_id', 'int', 'true'),
    ('table_id', 'int', 'true'),
    ('extra_info', 'varchar:MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH', 'true', ''),
    ],
)

def_table_schema(
  table_name = '__all_immediate_effect_index_sstable',
  table_id = '203',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('index_table_id', 'int'),
    ('partition_id,', 'int'),
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
  ],

  normal_columns = [
    ('snapshot', 'int'),
    ('partition_cnt', 'int'),
    ('data_size', 'int'),
  ],
)

def_table_schema(
    table_name    = '__all_sstable_checksum',
    table_id      = '204',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('data_table_id', 'int'),
        ('sstable_id', 'int'),
        ('partition_id', 'int'),
        ('sstable_type', 'int'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],

  normal_columns = [
      ('row_checksum', 'int'),
      ('data_checksum', 'int'),
      ('row_count', 'int'),
      ('snapshot_version', 'int'),
      ('replica_type', 'int')
  ],
)

def_table_schema(
    table_name = '__all_tenant_gc_partition_info',
    table_id = '205',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
    ],

    in_tenant_space = True,

    normal_columns = [],

    columns_with_tenant_id = ['table_id'],
)
all_constraint_def = dict(
    table_name    = '__all_constraint',
    table_id      = '206',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('constraint_id', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('constraint_name', 'varchar:OB_MAX_CONSTRAINT_NAME_LENGTH', 'false'),
      ('check_expr', 'varchar:OB_MAX_CONSTRAINT_EXPR_LENGTH', 'false'),
      ('schema_version', 'int'),
      ('constraint_type', 'int'),
      ('rely_flag', 'bool', 'false', 'false'),
      ('enable_flag', 'bool', 'false', 'true'),
      ('validate_flag', 'bool', 'false', 'true'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['table_id'],
)

def_table_schema(**all_constraint_def)

def_table_schema(**gen_history_table_def(207, all_constraint_def))

def_table_schema(
  table_name     = '__all_ori_schema_version',
  table_id       = '208',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
      ('tenant_id', 'int', 'false'),
      ('table_id', 'int', 'false'),
  ],
  partition_expr = ['key_v2', 'tenant_id', 16 ],
  partition_columns = ['tenant_id'],
  in_tenant_space = True,

  normal_columns = [
  ('ori_schema_version', 'int'),
  ('building_snapshot', 'int', 'false', '0'),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['table_id'],
)

all_func_def = dict(
    table_name    = '__all_func',
    table_id      = '209',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('name', 'varchar:OB_MAX_UDF_NAME_LENGTH', 'false'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('ret', 'int'),
      ('dl', 'varchar:OB_MAX_DL_NAME_LENGTH', 'false'),
      #TODO  the inner table python generator do not support enum at this time
      #('type', 'enum(\'function\',\'aggregate\')'),
      ('udf_id', 'int'),
      # 1 for normal function; 2 for aggregate function.
      ('type', 'int'),
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['udf_id'],
)

def_table_schema(**all_func_def)

def_table_schema(**gen_history_table_def(210, all_func_def))


def_table_schema(
  table_name     = '__all_temp_table',
  table_id       = '211',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
      ('tenant_id', 'int', 'false'),
      ('table_id', 'int', 'false'),
  ],
  partition_expr = ['key_v2', 'tenant_id', 16 ],
  partition_columns = ['tenant_id'],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
    ('create_host', 'varchar:OB_MAX_HOST_NAME_LENGTH', 'false', ''),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['table_id'],
)

def_table_schema(
    table_name    = '__all_sstable_column_checksum',
    table_id      = '212',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('data_table_id', 'int'),
        ('index_id', 'int'),
        ('partition_id', 'int'),
        ('sstable_type', 'int'),
        ('column_id', 'int'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],

  normal_columns = [
      ('column_checksum', 'int'),
      ('checksum_method', 'int'),
      ('snapshot_version', 'int'),
      ('replica_type', 'int'),
      ('major_version', 'int')
  ],
)


def_table_schema(
  table_name    = '__all_sequence_object',
  table_id      = '213',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
  ('tenant_id', 'int', 'false'),
  ('sequence_id', 'int', 'false'),
  ],
  partition_expr = ['key_v2', 'tenant_id', 16 ],
  partition_columns = ['tenant_id'],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
  ('schema_version', 'int', 'false'),
  ('database_id', 'int', 'false'),
  ('sequence_name', 'varchar:OB_MAX_SEQUENCE_NAME_LENGTH', 'false'),
  ('min_value', 'number:28:0', 'false'),
  ('max_value', 'number:28:0', 'false'),
  ('increment_by', 'number:28:0', 'false'),
  ('start_with', 'number:28:0', 'false'),
  ('cache_size', 'number:28:0', 'false'),
  ('order_flag', 'bool', 'false'),
  ('cycle_flag', 'bool', 'false'),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['sequence_id', 'database_id'],
)

def_table_schema(
  table_name    = '__all_sequence_object_history',
  table_id      = '214',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
  ('tenant_id', 'int', 'false'),
  ('sequence_id', 'int', 'false'),
  ('schema_version', 'int', 'false'),
  ],
  partition_expr = ['key_v2', 'tenant_id', 16 ],
  partition_columns = ['tenant_id'],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
  ('is_deleted', 'int', 'false'),
  ('database_id', 'int', 'true'),
  ('sequence_name', 'varchar:OB_MAX_SEQUENCE_NAME_LENGTH', 'true'),
  ('min_value', 'number:28:0', 'true'),
  ('max_value', 'number:28:0', 'true'),
  ('increment_by', 'number:28:0', 'true'),
  ('start_with', 'number:28:0', 'true'),
  ('cache_size', 'number:28:0', 'true'),
  ('order_flag', 'bool', 'true'),
  ('cycle_flag', 'bool', 'true'),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['sequence_id', 'database_id'],
)


def_table_schema(
    table_name     = '__all_sequence_value',
    table_id       = '215',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('sequence_id', 'int', 'false'),
    ],
    in_tenant_space = True,

  normal_columns = [
      ('next_value', 'number:38:0', 'false')
  ],
  columns_with_tenant_id = ['sequence_id'],
)

all_tenant_plan_baseline_def = dict(
    table_name    = '__all_tenant_plan_baseline',
    table_id      = '216',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('plan_baseline_id', 'int'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],
    rs_restart_related = True,
    in_tenant_space = True,

    normal_columns = [
      ('database_id', 'uint', 'false', 0),
      ('schema_version', 'int', 'false', 0),
      ('sql_id', 'varbinary:OB_MAX_SQL_ID_LENGTH', 'false', ''),
      ('plan_hash_value', 'uint', 'false'),

      ('params_info', 'longblob', 'false'),
      ('outline_data', 'longtext', 'false'),
      ('sql_text', 'longtext', 'false'),
      ('fixed', 'bool', 'false', 'false'),
      ('enabled', 'bool', 'false', 'true'),
      ('hints_info', 'longtext', 'false'),
      ('hints_all_worked', 'bool', 'false', 'true')
    ],

    migrate_data_before_2200 = True,
    columns_with_tenant_id = ['plan_baseline_id', 'database_id'],
)
def_table_schema(**all_tenant_plan_baseline_def)

def_table_schema(**gen_history_table_def(217, all_tenant_plan_baseline_def))

def_table_schema(
    table_name     = '__all_ddl_helper',
    table_id       = '218',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int', 'false'),
        ('schema_version', 'int', 'false'),
    ],
    partition_expr = ['key_v2', 'tenant_id', 16 ],
    partition_columns = ['tenant_id'],

    normal_columns = [
      ('schema_id', 'int', 'false'),
      ('ddl_type', 'int', 'false'),
  ],
)

def_table_schema(
    table_name     = '__all_freeze_schema_version',
    table_id       = '219',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('frozen_version', 'int', 'false'),
        ('tenant_id', 'int', 'false'),
    ],
    rs_restart_related = True,

    normal_columns = [
      ('schema_version', 'int', 'false'),
  ],
)

all_type_def = dict(
  table_name = '__all_type',
  table_id   = '220',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('type_id', 'int', 'false'),
  ],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
    ('database_id', 'int'),
    ('schema_version', 'int'),
    ('typecode', 'int'),
    ('properties', 'int'),
    ('attributes', 'int'),
    ('methods', 'int'),
    ('hiddenmethods', 'int'),
    ('supertypes', 'int'),
    ('subtypes', 'int'),
    ('externtype', 'int'),
    ('externname', 'varchar:OB_MAX_TABLE_TYPE_LENGTH', 'true', ''),
    ('helperclassname', 'varchar:OB_MAX_TABLE_TYPE_LENGTH', 'true', ''),
    ('local_attrs', 'int'),
    ('local_methods', 'int'),
    ('supertypeid', 'int'),
    ('type_name', 'varchar:OB_MAX_TABLE_TYPE_LENGTH'),
    ('package_id', 'int'),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['type_id', 'database_id', 'supertypeid', 'package_id'],
)

def_table_schema(**all_type_def)

def_table_schema(**gen_history_table_def(221, all_type_def))

all_type_attr_def = dict (
  table_name = '__all_type_attr',
  table_id = '222',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('type_id', 'int', 'false'),
    ('attribute', 'int', 'false'),
  ],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
    ('schema_version', 'int'),
    ('type_attr_id', 'int'),
    ('name', 'varchar:OB_MAX_TABLE_TYPE_LENGTH'),
    ('properties', 'int', 'false'),
    ('charset_id', 'int'),
    ('charset_form', 'int'),
    ('length', 'int'),
    ('number_precision', 'int'),
    ('scale', 'int'),
    ('zero_fill', 'int'),
    ('coll_type', 'int'),
    ('externname', 'varchar:OB_MAX_TABLE_TYPE_LENGTH', 'true', ''),
    ('xflags', 'int'),
    ('setter', 'int'),
    ('getter', 'int'),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['type_id', 'type_attr_id'],
)

def_table_schema(**all_type_attr_def)

def_table_schema(**gen_history_table_def(223, all_type_attr_def))

all_coll_type_def = dict(
  table_name = '__all_coll_type',
  table_id = '224',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('coll_type_id', 'int', 'false'),
  ],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
    ('schema_version', 'int'),
    ('elem_type_id', 'int'),
    ('elem_schema_version', 'int'),
    ('properties', 'int'),
    ('charset_id', 'int'),
    ('charset_form', 'int'),
    ('length', 'int'),
    ('number_precision', 'int'),
    ('scale', 'int'),
    ('zero_fill', 'int'),
    ('coll_type', 'int'),
    ('upper_bound', 'int'),
    ('package_id', 'int'),
    ('coll_name', 'varchar:OB_MAX_TABLE_TYPE_LENGTH'),
  ],

  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['coll_type_id', 'elem_type_id', 'package_id'],
)

def_table_schema(**all_coll_type_def)

def_table_schema(**gen_history_table_def(225, all_coll_type_def))

def_table_schema(
    table_name    = '__all_weak_read_service',
    table_id      = '226',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('level_id', 'int'),
        ('level_value', 'varchar:OB_WRS_LEVEL_VALUE_LENGTH'),
    ],

    in_tenant_space = True,
    is_cluster_private = True,
    is_backup_private = True,

  normal_columns = [
      ('level_name', 'varchar:OB_WRS_LEVEL_NAME_LENGTH', 'false'),
      ('min_version', 'int', 'true'),
      ('max_version', 'int', 'true'),
  ],
)

def_table_schema(
    table_name = '__all_gts',
    table_id = '229',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [('gts_id', 'int')],
    rs_restart_related = True,
    in_tenant_space = False,
    normal_columns = [
      ('gts_name', 'varchar:MAX_GTS_NAME_LENGTH'),
      ('region', 'varchar:MAX_REGION_LENGTH'),
      ('epoch_id', 'int'),
      ('member_list', 'varchar:MAX_MEMBER_LIST_LENGTH'),
      ('standby_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('standby_port', 'int'),
      ('heartbeat_ts', 'int'),
    ],
)

def_table_schema(
    table_name = '__all_tenant_gts',
    table_id = '230',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [('tenant_id', 'int')],
    rs_restart_related = True,
    in_tenant_space = False,
    normal_columns = [
      ('gts_id', 'int', 'false', '0'),
      ('orig_gts_id', 'int', 'false', '0'),
      ('gts_invalid_ts', 'int', 'false', '0'),
    ],
)

def_table_schema(
    table_name = '__all_partition_member_list',
    table_id = '231',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
      ],
    in_tenant_space = False,
    normal_columns = [
      ('member_list', 'varchar:MAX_MEMBER_LIST_LENGTH'),
      ('schema_version', 'int', 'false', 0),
      ('partition_status', 'int', 'false', 0),
    ],
)

all_dblink_def = dict(
  table_name    = '__all_dblink',
  table_id      = '232',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
      ('tenant_id', 'int'),
      ('dblink_id', 'int'),
  ],
  in_tenant_space = True,
  normal_columns = [
    ('dblink_name', 'varchar:OB_MAX_DBLINK_NAME_LENGTH', 'false'),
    ('owner_id', 'int', 'false'),
    ('host_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
    ('host_port', 'int', 'false'),
    ('cluster_name', 'varchar:OB_MAX_CLUSTER_NAME_LENGTH', 'true'),
    ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE', 'false'),
    ('user_name', 'varchar:OB_MAX_USER_NAME_LENGTH_STORE', 'false'),
    ('password', 'varchar:OB_MAX_PASSWORD_LENGTH', 'false'),
  ],
  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['dblink_id', 'owner_id'],
)

def_table_schema(**all_dblink_def)
def_table_schema(**gen_history_table_def(233, all_dblink_def))

def_table_schema(
    table_name    = '__all_tenant_partition_meta_table',
    table_id      = '234',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('partition_id', 'int'),
        ('svr_ip', 'varchar:32'), # use varchar:32 for compatibilty
        ('svr_port', 'int'),
    ],

    in_tenant_space = True,
    is_cluster_private = True,
    is_backup_private = True,
    migrate_data_before_2200 = True,

  normal_columns = [
      ('role', 'int'),
      ('row_count', 'int'),
      ('data_size', 'int'),
      ('data_version', 'int'),
      ('required_size', 'int', 'false', '0'),
      ('replica_type', 'int', 'false', '0'),
      ('status', 'varchar:MAX_REPLICA_STATUS_LENGTH', 'false', 'REPLICA_STATUS_NORMAL'),
      ('data_checksum', 'int', 'false', '0')
  ],
  columns_with_tenant_id = [],
)

all_tenant_role_grantee_map_def = dict(
  table_name = '__all_tenant_role_grantee_map',
  table_id = '235',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('grantee_id', 'int', 'false'),
    ('role_id', 'int', 'false'),
  ],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
    ('admin_option', 'int', 'false', '0'),
    ('disable_flag', 'int', 'false', '0')
  ],
  migrate_data_before_2200 = True,
  columns_with_tenant_id = ['grantee_id', 'role_id']
)
def_table_schema(**all_tenant_role_grantee_map_def)
def_table_schema(**gen_history_table_def(236, all_tenant_role_grantee_map_def))

all_tenant_keystore_def = dict(
  table_name = '__all_tenant_keystore',
  table_id = '237',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('keystore_id', 'int', 'false'),
  ],
  rs_restart_related = True,
  in_tenant_space = True,
  normal_columns = [
    ('keystore_name', 'varchar:OB_MAX_KEYSTORE_NAME_LENGTH'),
    ('password', 'varchar:OB_MAX_PASSWORD_LENGTH'),
    ('status', 'int', 'false', 0),
    ('master_key_id', 'int'),
    ('master_key', 'varchar:OB_MAX_MASTER_KEY_LENGTH'),
  ],
  columns_with_tenant_id = ['keystore_id', 'master_key_id']
)
def_table_schema(**all_tenant_keystore_def)
def_table_schema(**gen_history_table_def(238, all_tenant_keystore_def))

all_tenant_tablespace_def = dict(
  table_name = '__all_tenant_tablespace',
    table_id = '247',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('tablespace_id', 'int', 'false'),
  ],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
    ('tablespace_name', 'varchar:MAX_ORACLE_NAME_LENGTH'),
    ('encryption_name', 'varchar:MAX_ORACLE_NAME_LENGTH'),
    ('encrypt_key', 'varbinary:OB_MAX_MASTER_KEY_LENGTH'),
    ('master_key_id', 'uint'),
  ],
  columns_with_tenant_id = ['tablespace_id', 'master_key_id']
)
def_table_schema(**all_tenant_tablespace_def)
def_table_schema(**gen_history_table_def(248, all_tenant_tablespace_def))

def_table_schema(
  table_name    = '__all_tenant_user_failed_login_stat',
  table_id      = '249',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('user_id', 'int'),
  ],
  in_tenant_space = True,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('user_name', 'varchar:OB_MAX_USER_NAME_LENGTH'),
    ('failed_login_attempts', 'int'),
    ('last_failed_login_svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'true', ''),
  ],
  columns_with_tenant_id = [],
)

all_profile_def = dict(
  table_name    = '__all_tenant_profile',
  table_id      = '250',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('profile_id', 'int'),
  ],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
    ('profile_name', 'varchar:OB_MAX_SQL_LENGTH'),
    ('failed_login_attempts', 'int'),
    ('password_lock_time', 'int'),
    ('password_verify_function', 'varchar:MAX_ORACLE_NAME_LENGTH', 'true'),
    ('password_life_time', 'int', 'false', '-1'),
    ('password_grace_time', 'int', 'false', '-1'),
    ('password_reuse_time', 'int', 'false', '-1'),
    ('password_reuse_max', 'int', 'false', '-1'),
    ('inactive_account_time', 'int', 'false', '-1'),
  ],
  columns_with_tenant_id = ['profile_id'],
)
def_table_schema(**all_profile_def)
def_table_schema(**gen_history_table_def(251,  all_profile_def))

all_tenant_security_audit_def = dict(
    table_name = '__all_tenant_security_audit',
    table_id = '252',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
                      ('tenant_id', 'int', 'false'),
                      ('audit_id', 'int', 'false'),
                     ],
    rs_restart_related = True,
    in_tenant_space = True,
    normal_columns = [
      ('audit_type', 'uint', 'false'),
      ('owner_id', 'uint', 'false'),
      ('operation_type', 'uint', 'false'),
      ('in_success', 'uint'),
      ('in_failure', 'uint'),
    ],
    columns_with_tenant_id = ['audit_id', 'owner_id'],
)
def_table_schema(**all_tenant_security_audit_def)
def_table_schema(**gen_history_table_def(253, all_tenant_security_audit_def))

all_trigger_def = dict(
  table_name    = '__all_tenant_trigger',
  table_id      = '254',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
      ('tenant_id', 'int'),
      ('trigger_id', 'int'),
  ],
  rs_restart_related = True,
  in_tenant_space = True,
  normal_columns = [
    ('trigger_name', 'varchar:OB_MAX_TRIGGER_NAME_LENGTH', 'false'),
    ('database_id', 'int', 'false'),
    ('owner_id', 'int', 'false'),
    ('schema_version', 'int', 'false'),
    ('trigger_type', 'int', 'false'),
    ('trigger_events', 'int', 'false'),
    ('timing_points', 'int', 'false'),
    ('base_object_type', 'int', 'false'),
    ('base_object_id', 'int', 'false'),
    ('trigger_flags', 'int', 'false'),
    ('update_columns', 'varchar:OB_MAX_UPDATE_COLUMNS_LENGTH', 'true'),
    ('ref_old_name', 'varchar:OB_MAX_TRIGGER_NAME_LENGTH', 'false'),
    ('ref_new_name', 'varchar:OB_MAX_TRIGGER_NAME_LENGTH', 'false'),
    ('ref_parent_name', 'varchar:OB_MAX_TRIGGER_NAME_LENGTH', 'false'),
    ('when_condition', 'varchar:OB_MAX_WHEN_CONDITION_LENGTH', 'true'),
    ('trigger_body', 'varchar:OB_MAX_TRIGGER_BODY_LENGTH', 'false'),
    ('package_spec_source', 'varchar:OB_MAX_TRIGGER_BODY_LENGTH', 'false'),
    ('package_body_source', 'varchar:OB_MAX_TRIGGER_BODY_LENGTH', 'false'),
    ('package_flag', 'int', 'false'),
    ('package_comp_flag', 'int', 'false'),
    ('package_exec_env', 'varchar:OB_MAX_PROC_ENV_LENGTH', 'true'),
    ('sql_mode', 'int', 'false'),
  ],
  columns_with_tenant_id = ['trigger_id', 'database_id', 'owner_id', 'base_object_id'],
)

def_table_schema(**all_trigger_def)
def_table_schema(**gen_history_table_def(255, all_trigger_def))

def_table_schema(
    table_name = '__all_seed_parameter',
    table_id   = '256',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('zone', 'varchar:MAX_ZONE_LENGTH'),
        ('svr_type', 'varchar:SERVER_TYPE_LENGTH'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
        ('name', 'varchar:OB_MAX_CONFIG_NAME_LEN'),
    ],
    normal_columns = [
        ('data_type', 'varchar:OB_MAX_CONFIG_TYPE_LENGTH', 'true'),
        ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN'),
        ('info', 'varchar:OB_MAX_CONFIG_INFO_LEN'),
        ('section', 'varchar:OB_MAX_CONFIG_SECTION_LEN', 'true'),
        ('scope', 'varchar:OB_MAX_CONFIG_SCOPE_LEN', 'true'),
        ('source', 'varchar:OB_MAX_CONFIG_SOURCE_LEN', 'true'),
        ('edit_level', 'varchar:OB_MAX_CONFIG_EDIT_LEVEL_LEN', 'true'),
        ('config_version', 'int'),
    ],
)

def_table_schema(
   table_name    = '__all_tenant_sstable_column_checksum',
   table_id      = '258',
   table_type = 'SYSTEM_TABLE',
   gm_columns = ['gmt_create', 'gmt_modified'],
   rowkey_columns = [
         ('tenant_id', 'int'),
         ('data_table_id', 'int'),
         ('index_id', 'int'),
         ('partition_id', 'int'),
         ('sstable_type', 'int'),
         ('column_id', 'int'),
         ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
         ('svr_port', 'int'),
     ],
   in_tenant_space = True,
   is_cluster_private = True,
   normal_columns = [
       ('column_checksum', 'int'),
       ('checksum_method', 'int'),
       ('snapshot_version', 'int'),
       ('replica_type', 'int'),
       ('major_version', 'int')
   ],
   columns_with_tenant_id = ['data_table_id', 'index_id'],
)

# sys index schema def, only for compatible

all_tenant_security_audit_record_def = dict(
    table_name = '__all_tenant_security_audit_record',
    table_id = '259',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
                      ('tenant_id', 'int', 'false'),
                      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
                      ('svr_port', 'int', 'false'),
                      ('record_timestamp_us', 'timestamp', 'false'),
                      ],
    normal_columns = [
      ('user_id', 'uint'),
      ('user_name', 'varchar:OB_MAX_USER_NAME_LENGTH'),
      ('effective_user_id', 'uint'),
      ('effective_user_name', 'varchar:OB_MAX_USER_NAME_LENGTH'),
      ('client_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('user_client_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('proxy_session_id', 'uint'),
      ('session_id', 'uint'),
      ('entry_id', 'uint'),
      ('statement_id', 'uint'),
      ('trans_id', 'varchar:512'),
      ('commit_version', 'int'),
      ('trace_id', 'varchar:64'),
      ('db_id', 'uint'),
      ('cur_db_id', 'uint'),
      ('sql_timestamp_us', 'timestamp'),
      ('audit_id', 'uint'),
      ('audit_type', 'uint'),
      ('operation_type', 'uint'),
      ('action_id', 'uint'),
      ('return_code', 'int'),
      ('obj_owner_name', 'varchar:OB_MAX_USER_NAME_LENGTH', 'true'),
      ('obj_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'true'),
      ('new_obj_owner_name', 'varchar:OB_MAX_USER_NAME_LENGTH', 'true'),
      ('new_obj_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'true'),
      ('auth_privileges', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'true'),
      ('auth_grantee', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'true'),
      ('logoff_logical_read', 'uint'),
      ('logoff_physical_read', 'uint'),
      ('logoff_logical_write', 'uint'),
      ('logoff_lock_count', 'uint'),
      ('logoff_dead_lock', 'varchar:40', 'true'),
      ('logoff_cpu_time_us', 'uint'),
      ('logoff_exec_time_us', 'uint'),
      ('logoff_alive_time_us', 'uint'),
      ('comment_text', 'longtext', 'true'),
      ('sql_bind', 'longtext', 'true'),
      ('sql_text', 'longtext', 'true'),
    ],
    in_tenant_space = True,
    columns_with_tenant_id = ['user_id',
                              'effective_user_id',
                              'db_id',
                              'cur_db_id',
                              'audit_id'],
#    index = {'all_tenant_security_audit_record_i1' : { 'index_columns' : ['tenant_id'],
#                      'index_using_type' : 'USING_HASH'}},
)

def_table_schema(**all_tenant_security_audit_record_def)

all_sysauth_def = dict(
    table_name     = '__all_tenant_sysauth',
    table_id       = '260',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rs_restart_related = True,
    in_tenant_space = True,

    rowkey_columns = [
        ('tenant_id', 'int', 'false'),
        ('grantee_id', 'int', 'false'),
        ('priv_id', 'int', 'false'),

    ],
    normal_columns = [
      ('priv_option', 'int', 'false'),
  ],
  columns_with_tenant_id = ['grantee_id'],
)

def_table_schema(**all_sysauth_def)
def_table_schema(**gen_history_table_def(261, all_sysauth_def))

all_objauth_def = dict(
    table_name     = '__all_tenant_objauth',
    table_id       = '262',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rs_restart_related = True,
    in_tenant_space = True,

    rowkey_columns = [
        ('tenant_id', 'int', 'false'),
        ('obj_id', 'int', 'false'),
        ('objtype', 'int', 'false'),
        ('col_id', 'int', 'false'),
        ('grantor_id', 'int', 'false'),
        ('grantee_id', 'int', 'false'),
        ('priv_id', 'int', 'false'),

    ],
    normal_columns = [
      ('priv_option', 'int', 'false'),
  ],
  columns_with_tenant_id = ['obj_id', 'grantor_id', 'grantee_id'],
)

def_table_schema(**all_objauth_def)
def_table_schema(**gen_history_table_def(263, all_objauth_def))


# __all_tenant_backup_info
all_tenant_backup_info_def = dict(
  table_name    = '__all_tenant_backup_info',
  table_id      = '264',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('name', 'varchar:OB_INNER_TABLE_DEFAULT_KEY_LENTH'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('value', 'varchar:OB_INNER_TABLE_DEFAULT_VALUE_LENTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_backup_info_def)

# __all_restore_info
all_restore_info_def = dict(
  table_name    = '__all_restore_info',
  table_id      = '265',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('job_id', 'int'),
    ('name', 'varchar:OB_INNER_TABLE_DEFAULT_KEY_LENTH'),
  ],
  rs_restart_related = False,

  normal_columns = [
    ('value', 'longtext'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_restore_info_def)

# __all_tenant_backup_log_archive_status
all_tenant_backup_log_archive_status_def = dict(
  table_name    = '__all_tenant_backup_log_archive_status',
  table_id      = '266',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('log_archive_round', 'int'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('min_first_time', 'timestamp'),
    ('max_next_time', 'timestamp'),
    ('input_bytes', 'int', 'false', '0'),
    ('output_bytes', 'int', 'false', '0'),
    ('deleted_input_bytes', 'int', 'false', '0'),
    ('deleted_output_bytes', 'int', 'false', '0'),
    ('pg_count', 'int', 'false', '0'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH', 'false', ''),
    ('is_mount_file_created', 'int', 'false', '0'),
    ('compatible', 'int', 'false', '0'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_backup_log_archive_status_def)

# __all_backup_log_archive_status_history
all_backup_log_archive_status_def = dict(
  table_name    = '__all_backup_log_archive_status_history',
  table_id      = '267',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('log_archive_round', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('min_first_time', 'timestamp'),
    ('max_next_time', 'timestamp'),
    ('input_bytes', 'int', 'false', '0'),
    ('output_bytes', 'int', 'false', '0'),
    ('deleted_input_bytes', 'int', 'false', '0'),
    ('deleted_output_bytes', 'int', 'false', '0'),
    ('pg_count', 'int', 'false', '0'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('is_mark_deleted', 'bool', 'true'),
    ('compatible', 'int', 'false', '0'),
    ('start_piece_id', 'int', 'false', '0'),
    ('backup_piece_id', 'int', 'false', '0'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_log_archive_status_def)

# all_tenant_backup_task
all_tenant_backup_task_def = dict(
  table_name    = '__all_tenant_backup_task',
  table_id      = '268',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('backup_type', 'varchar:OB_INNER_TABLE_BACKUP_TYPE_LENTH'),
    ('device_type', 'varchar:OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH'),
    ('snapshot_version', 'int'),
    ('prev_full_backup_set_id', 'int'),
    ('prev_inc_backup_set_id', 'int'),
    ('prev_backup_data_version', 'int'),
    ('pg_count', 'int'),
    ('macro_block_count', 'int'),
    ('finish_pg_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ('input_bytes', 'int'),
    ('output_bytes', 'int'),
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('compatible', 'int'),
    ('cluster_version', 'int'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('result', 'int'),
    ('cluster_id', 'int', 'true'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('backup_data_version', 'int', 'true'),
    ('backup_schema_version', 'int', 'true'),
    ('cluster_version_display', 'varchar:OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH', 'true'),
    ('partition_count', 'int', 'true'),
    ('finish_partition_count', 'int', 'true'),
    ('encryption_mode', 'varchar:OB_MAX_ENCRYPTION_MODE_LENGTH', 'false', 'None'),
    ('passwd', 'varchar:OB_MAX_PASSWORD_LENGTH', 'false', ''),
    ('start_replay_log_ts', 'int', 'false', 0),
    ('date', 'int', 'false', 0),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_backup_task_def)

# all_backup_task_history
all_backup_task_history_def = dict(
  table_name    = '__all_backup_task_history',
  table_id      = '269',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('backup_type', 'varchar:OB_INNER_TABLE_BACKUP_TYPE_LENTH'),
    ('device_type', 'varchar:OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH'),
    ('snapshot_version', 'int'),
    ('prev_full_backup_set_id', 'int'),
    ('prev_inc_backup_set_id', 'int'),
    ('prev_backup_data_version', 'int'),
    ('pg_count', 'int'),
    ('macro_block_count', 'int'),
    ('finish_pg_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ('input_bytes', 'int'),
    ('output_bytes', 'int'),
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('compatible', 'int'),
    ('cluster_version', 'int'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('result', 'int'),
    ('cluster_id', 'int', 'true'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('backup_data_version', 'int', 'true'),
    ('backup_schema_version', 'int', 'true'),
    ('cluster_version_display', 'varchar:OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH', 'true'),
    ('partition_count', 'int', 'true'),
    ('finish_partition_count', 'int', 'true'),
    ('is_mark_deleted', 'bool', 'true'),
    ('encryption_mode', 'varchar:OB_MAX_ENCRYPTION_MODE_LENGTH', 'false', 'None'),
    ('passwd', 'varchar:OB_MAX_PASSWORD_LENGTH', 'false', ''),
    ('start_replay_log_ts', 'int', 'false', 0),
    ('date', 'int', 'false', 0),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_task_history_def)

# all_tenant_backup_task
all_tenant_pg_backup_task_def = dict(
  table_name    = '__all_tenant_pg_backup_task',
  table_id      = '270',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('table_id', 'int'),
    ('partition_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('backup_type', 'varchar:OB_INNER_TABLE_BACKUP_TYPE_LENTH'),
    ('snapshot_version', 'int'),
    ('partition_count', 'int'),
    ('macro_block_count', 'int'),
    ('finish_partition_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ('input_bytes', 'int'),
    ('output_bytes', 'int'),
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('retry_count', 'int'),
    ('replica_role', 'int'),
    ('replica_type', 'int'),
    ('svr_ip', 'varchar:OB_MAX_SERVER_ADDR_SIZE'),
    ('svr_port', 'int'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('task_id', 'int'),
    ('result', 'int'),
    ('trace_id', 'varchar:OB_MAX_TRACE_ID_BUFFER_SIZE', 'true'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_pg_backup_task_def)

all_tenant_error_def = dict(
    table_name = '__all_tenant_error',
    table_id = '272',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
                      ('tenant_id', 'int', 'false'),
                      ('obj_id', 'int', 'false'),
                      ('obj_seq', 'int', 'false'),
                      ('obj_type', 'int', 'false'),
                     ],
    rs_restart_related = False,
    in_tenant_space = True,
    is_cluster_private = False,
    is_backup_private = False,
    normal_columns = [
      ('line', 'int', 'false'),
      ('position', 'int', 'false'),
      ('text_length', 'int', 'false'),
      ('text', 'varchar:MAX_ORACLE_COMMENT_LENGTH'),
      ('property', 'int', 'true'),
      ('error_number', 'int', 'true'),
      ('schema_version', 'int', 'false')
    ],
    columns_with_tenant_id = ['obj_id'],
)
def_table_schema(**all_tenant_error_def)

all_server_recovery_status_def = dict(
    table_name = '__all_server_recovery_status',
    table_id = '273',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
    ],

    normal_columns = [
      ('rescue_svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
      ('rescue_svr_port', 'int', 'false'),
      ('rescue_progress', 'int', 'false'),
    ],
)
def_table_schema(**all_server_recovery_status_def)

all_datafile_recovery_status_def = dict(
    table_name = '__all_datafile_recovery_status',
    table_id = '274',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('tenant_id', 'int'),
      ('file_id', 'int'),
    ],
    normal_columns = [
      ('dest_svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
      ('dest_svr_port', 'int', 'false'),
      ('dest_unit_id', 'int', 'false'),
      ('status', 'int', 'false', '0'),
    ],
)
def_table_schema(**all_datafile_recovery_status_def)

# all_tenant_backup_clean_info
all_tenant_backup_clean_info_def = dict(
  table_name    = '__all_tenant_backup_clean_info',
  table_id      = '276',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('job_id', 'int'),
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('incarnation', 'int'),
    ('type', 'varchar:OB_INNER_TABLE_BACKUP_CLEAN_TYPE_LENGTH'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('parameter', 'varchar:OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH'),
    ('error_msg', 'varchar:OB_MAX_ERROR_MSG_LEN'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
    ('clog_gc_snapshot', 'int', 'true'),
    ('result', 'int', 'true'),
    ('copy_id', 'int'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_backup_clean_info_def)

# all_backup_clean_info_history
all_backup_clean_info_history_def = dict(
  table_name    = '__all_backup_clean_info_history',
  table_id      = '277',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('job_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('incarnation', 'int'),
    ('type', 'varchar:OB_INNER_TABLE_BACKUP_CLEAN_TYPE_LENGTH'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('parameter', 'varchar:OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH'),
    ('error_msg', 'varchar:OB_MAX_ERROR_MSG_LEN'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
    ('clog_gc_snapshot', 'int', 'true'),
    ('result', 'int', 'true'),
    ('copy_id', 'int'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_clean_info_history_def)

# all_backup_task_clean_history
all_backup_task_clean_history_def = dict(
  table_name    = '__all_backup_task_clean_history',
  table_id      = '278',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
    ('job_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('backup_type', 'varchar:OB_INNER_TABLE_BACKUP_TYPE_LENTH'),
    ('device_type', 'varchar:OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH'),
    ('snapshot_version', 'int'),
    ('prev_full_backup_set_id', 'int'),
    ('prev_inc_backup_set_id', 'int'),
    ('prev_backup_data_version', 'int'),
    ('pg_count', 'int'),
    ('macro_block_count', 'int'),
    ('finish_pg_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ('input_bytes', 'int'),
    ('output_bytes', 'int'),
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('compatible', 'int'),
    ('cluster_version', 'int'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('result', 'int'),
    ('cluster_id', 'int'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH'),
    ('backup_data_version', 'int'),
    ('backup_schema_version', 'int'),
    ('cluster_version_display', 'varchar:OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH'),
    ('partition_count', 'int'),
    ('finish_partition_count', 'int'),
    ('copy_id', 'int'),
    ('start_replay_log_ts', 'int', 'false', 0),
    ('date', 'int', 'false', 0),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_task_clean_history_def)

def_table_schema(
    table_name     = '__all_restore_progress',
    table_id       = '279',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('job_id', 'int', 'false'),
    ],
    normal_columns = [
      ('external_job_id', 'int', 'false', '-1'),
      ('tenant_id', 'int', 'false', '-1'),
      ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE', 'false', ''),
      ('status', 'varchar:64', 'false'),
      ('start_time', 'timestamp', 'true'),
      ('completion_time', 'timestamp', 'true'),
      ('pg_count', 'int', 'false', '0'),
      ('finish_pg_count', 'int', 'false', '0'),
      ('partition_count', 'int', 'false', '0'),
      ('finish_partition_count', 'int', 'false', '0'),
      ('macro_block_count', 'int', 'false', '0'),
      ('finish_macro_block_count', 'int', 'false', '0'),
      ('restore_start_timestamp', 'timestamp', 'true'),
      ('restore_finish_timestamp', 'timestamp', 'true'),
      ('restore_current_timestamp', 'timestamp', 'true'),
      ('info', 'varchar:OB_INNER_TABLE_DEFAULT_VALUE_LENTH', 'false', ''),
      ('backup_cluster_id', 'int', 'false', '-1'),
      ('backup_cluster_name', 'varchar:OB_MAX_CLUSTER_NAME_LENGTH', 'false', ''),
      ('backup_tenant_id', 'int', 'false', '-1'),
      ('backup_tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE', 'false', ''),
      ('white_list', 'longtext', 'true'),
      ('backup_set_list', 'longtext', 'true'),
      ('backup_piece_list', 'longtext', 'true')
  ],
)

def_table_schema(
    table_name     = '__all_restore_history',
    table_id       = '280',
    table_type = 'SYSTEM_TABLE',
    gm_columns = ['gmt_create', 'gmt_modified'],
    rowkey_columns = [
        ('job_id', 'int', 'false'),
    ],
    normal_columns = [
      ('external_job_id', 'int', 'false', '-1'),
      ('tenant_id', 'int', 'false', '-1'),
      ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE', 'false', ''),
      ('status', 'varchar:64', 'false'),
      ('start_time', 'timestamp', 'true'),
      ('completion_time', 'timestamp', 'true'),
      ('pg_count', 'int', 'false', '0'),
      ('finish_pg_count', 'int', 'false', '0'),
      ('partition_count', 'int', 'false', '0'),
      ('finish_partition_count', 'int', 'false', '0'),
      ('macro_block_count', 'int', 'false', '0'),
      ('finish_macro_block_count', 'int', 'false', '0'),
      ('restore_start_timestamp', 'timestamp', 'true'),
      ('restore_finish_timestamp', 'timestamp', 'true'),
      ('restore_current_timestamp', 'timestamp', 'true'),
      ('restore_data_version', 'int', 'false', '-1'),
      ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'false'),
      ('restore_option', 'varchar:OB_INNER_TABLE_DEFAULT_VALUE_LENTH', 'false'),
      ('info', 'varchar:OB_INNER_TABLE_DEFAULT_VALUE_LENTH', 'false', ''),
      ('backup_cluster_id', 'int', 'false', '-1'),
      ('backup_cluster_name', 'varchar:OB_MAX_CLUSTER_NAME_LENGTH', 'false', ''),
      ('backup_tenant_id', 'int', 'false', '-1'),
      ('backup_tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE', 'false', ''),
      ('white_list', 'longtext', 'true'),
      ('backup_set_list', 'longtext', 'true'),
      ('backup_piece_list', 'longtext', 'true')
  ],
)

# __all_tenant_restore_pg_info
all_backup_log_archive_status_def = dict(
  table_name    = '__all_tenant_restore_pg_info',
  table_id      = '281',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('table_id', 'int'),
    ('partition_id', 'int'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('macro_block_count', 'int', 'false', '0'),
    ('finish_macro_block_count', 'int', 'false', '0'),
    ('partition_count', 'int', 'false', '0'),
    ('finish_partition_count', 'int', 'false', '0'),
    ('restore_info', 'varchar:OB_INNER_TABLE_DEFAULT_VALUE_LENTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_log_archive_status_def)

def_table_schema(**gen_history_table_def(282, all_table_v2_def))

all_tenant_object_type_def = dict(
  table_name = '__all_tenant_object_type',
  table_id = '283',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('object_type_id', 'int', 'false'),
    ('type', 'int', 'false'),
  ],
  rs_restart_related = True,
  in_tenant_space = True,

  normal_columns = [
    ('schema_version', 'int'),
    ('properties', 'int'),
    ('charset_id', 'int'),
    ('charset_form', 'int'),
    ('length', 'int'),
    ('number_precision', 'int'),
    ('scale', 'int'),
    ('zero_fill', 'int'),
    ('coll_type', 'int'),
    ('database_id', 'int'),
    ('flag', 'int', 'false'),
    ('owner_id', 'int', 'false'),
    ('comp_flag', 'int', 'true'),
    ('object_name', 'varchar:OB_MAX_TABLE_TYPE_LENGTH', 'false'),
    ('exec_env', 'varchar:OB_MAX_PROC_ENV_LENGTH', 'true'),
    ('source', 'longtext', 'true'),
    ('comment', 'varchar:MAX_TENANT_COMMENT_LENGTH', 'true'),
    ('route_sql', 'longtext', 'true')
  ],

  columns_with_tenant_id = ['database_id', 'owner_id', 'object_type_id'],
)

def_table_schema(**all_tenant_object_type_def)

def_table_schema(**gen_history_table_def(284, all_tenant_object_type_def))

all_backup_validation_job_def = dict(
  table_name = '__all_backup_validation_job',
  table_id   = '285',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('job_id', 'int'),
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE'),
    ('progress_percent', 'int'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_validation_job_def)

all_backup_validation_job_history_def = dict(
  table_name = '__all_backup_validation_job_history',
  table_id   = '286',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('job_id', 'int'),
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE'),
    ('progress_percent', 'int'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_validation_job_history_def)

all_tenant_backup_validation_task_def = dict(
  table_name = '__all_tenant_backup_validation_task',
  table_id   = '287',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('job_id', 'int'),
    ('task_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('total_pg_count', 'int'),
    ('finish_pg_count', 'int'),
    ('total_partition_count', 'int'),
    ('finish_partition_count', 'int'),
    ('total_macro_block_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ("log_size", 'int'),
    ('result', 'int'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_backup_validation_task_def)

all_backup_validation_task_history_def = dict(
  table_name = '__all_backup_validation_task_history',
  table_id   = '288',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('job_id', 'int'),
    ('task_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('total_pg_count', 'int'),
    ('finish_pg_count', 'int'),
    ('total_partition_count', 'int'),
    ('finish_partition_count', 'int'),
    ('total_macro_block_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ("log_size", 'int'),
    ('result', 'int'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_validation_task_history_def)

all_tenant_pg_backup_validation_task_def = dict(
  table_name = '__all_tenant_pg_backup_validation_task',
  table_id   = '289',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('job_id', 'int'),
    ('task_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
    ('table_id', 'int'),
    ('partition_id', 'int'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('archive_round', 'int'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('trace_id', 'varchar:OB_MAX_TRACE_ID_BUFFER_SIZE', 'true'),
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('total_partition_count', 'int'),
    ('finish_partition_count', 'int'),
    ('total_macro_block_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ('log_info', 'varchar:64'),
    ("log_size", 'int'),
    ('result', 'int'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_pg_backup_validation_task_def)

def_table_schema(
  table_name     = '__all_tenant_time_zone',
  table_id       = '290',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
    ('tenant_id', 'int', 'false', '-1'),
    ('time_zone_id', 'int', 'false', 'NULL')
  ],
  in_tenant_space = True,
  is_cluster_private = False,
  is_backup_private = False,
  normal_columns = [
  ('use_leap_seconds', 'varchar:8', 'false', 'N'),
  ('version', 'int', 'true'),
  ],
  columns_with_tenant_id = [],
)

def_table_schema(
  table_name     = '__all_tenant_time_zone_name',
  table_id       = '291',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
    ('tenant_id', 'int', 'false', '-1'),
    ('name', 'varchar:64', 'false', 'NULL')
  ],
  in_tenant_space = True,
  is_cluster_private = False,
  is_backup_private = False,

  normal_columns = [
  ('time_zone_id', 'int', 'false', 'NULL'),
  ('version', 'int', 'true'),
  ],
  columns_with_tenant_id = [],
)

def_table_schema(
  table_name     = '__all_tenant_time_zone_transition',
  table_id       = '292',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
    ('tenant_id', 'int', 'false', '-1'),
    ('time_zone_id', 'int', 'false', 'NULL'),
    ('transition_time', 'int', 'false', 'NULL')
  ],
  in_tenant_space = True,
  is_cluster_private = False,
  is_backup_private = False,

  normal_columns = [
  ('transition_type_id', 'int', 'false', 'NULL'),
  ('version', 'int', 'true'),
  ],
  columns_with_tenant_id = [],
)

def_table_schema(
  table_name     = '__all_tenant_time_zone_transition_type',
  table_id       = '293',
  table_type = 'SYSTEM_TABLE',
  gm_columns = [],
  rowkey_columns = [
    ('tenant_id', 'int', 'false', '-1'),
    ('time_zone_id', 'int', 'false', 'NULL'),
    ('transition_type_id', 'int', 'false', 'NULL')
  ],
  in_tenant_space = True,
  is_cluster_private = False,
  is_backup_private = False,

  normal_columns = [
  ('offset', 'int', 'false', '0'),
  ('is_dst', 'int', 'false', '0'),
  ('abbreviation', 'varchar:8', 'false', ''),
  ('version', 'int', 'true'),
  ],
  columns_with_tenant_id = [],
)

all_tenant_constraint_column_def = dict(
  table_name    = '__all_tenant_constraint_column',
  table_id      = '294',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('table_id', 'int', 'false'),
    ('constraint_id', 'int', 'false'),
    ('column_id', 'int', 'false'),
  ],
  rs_restart_related = True,
  in_tenant_space = True,
  normal_columns = [
    ('schema_version', 'int', 'false'),
  ],
  columns_with_tenant_id = ['table_id'],
)
def_table_schema(**all_tenant_constraint_column_def)
def_table_schema(**gen_history_table_def(295,  all_tenant_constraint_column_def))

def_table_schema(
  table_name    = '__all_tenant_global_transaction',
  table_id      = '296',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('gtrid', 'varbinary:128'),
    ('bqual', 'varbinary:128'),
    ('format_id', 'int', 'false', '1'),
  ],
  in_tenant_space = True,
  is_cluster_private = False,
  is_backup_private = True,
  normal_columns = [
    ('trans_id', 'varchar:512'),
    ('coordinator', 'varchar:128', 'true'),
    ('scheduler_ip', 'varchar:OB_MAX_SERVER_ADDR_SIZE'),
    ('scheduler_port', 'int'),
    ('is_readonly', 'bool', 'false', '0'),
    ('state', 'int'),
    ('end_flag', 'int'),
  ],
  columns_with_tenant_id = [],
)

all_tenant_dependency_def = dict(
  table_name = '__all_tenant_dependency',
  table_id = '297',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('dep_obj_type', 'int'),
    ('dep_obj_id', 'int'),
    ('dep_order', 'int'),
  ],
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  in_tenant_space = True,

  normal_columns = [
    ('schema_version', 'int'),
    ('dep_timestamp', 'int'),
    ('ref_obj_type', 'int'),
    ('ref_obj_id', 'int'),
    ('ref_timestamp', 'int'),
    ('dep_obj_owner_id', 'int', 'true'),
    ('property', 'int'),
    ('dep_attrs', 'varbinary:OB_MAX_ORACLE_RAW_SQL_COL_LENGTH', 'true'),
    ('dep_reason', 'varbinary:OB_MAX_ORACLE_RAW_SQL_COL_LENGTH', 'true'),
    ('ref_obj_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'true')
  ],

  columns_with_tenant_id = ['dep_obj_id', 'ref_obj_id', 'dep_obj_owner_id'],
)

def_table_schema(**all_tenant_dependency_def)

all_backup_backupset_job_def = dict(
  table_name = '__all_backup_backupset_job',
  table_id   = '298',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('job_id', 'int'),
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
    ('copy_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('backup_backupset_type', 'varchar:OB_INNER_TABLE_BACKUP_TYPE_LENTH'),
    ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('max_backup_times', 'int'),
    ('result', 'int'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_backupset_job_def)

all_backup_backupset_job_history_def = dict(
  table_name = '__all_backup_backupset_job_history',
  table_id   = '299',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('job_id', 'int'),
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
    ('copy_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('backup_backupset_type', 'varchar:OB_INNER_TABLE_BACKUP_TYPE_LENTH'),
    ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('max_backup_times', 'int'),
    ('result', 'int'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_backupset_job_history_def)

all_tenant_backup_backupset_task_def = dict(
  table_name = '__all_tenant_backup_backupset_task',
  table_id   = '300',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('job_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
    ('copy_id', 'int'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('backup_type', 'varchar:OB_INNER_TABLE_BACKUP_TYPE_LENTH'),
    ('snapshot_version', 'int'),
    ('prev_full_backup_set_id', 'int'),
    ('prev_inc_backup_set_id', 'int'),
    ('prev_backup_data_version', 'int'),
    ('input_bytes', 'int'),
    ('output_bytes', 'int'),
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('compatible', 'int'),
    ('cluster_id', 'int', 'true'),
    ('cluster_version', 'int'),
    ('cluster_version_display', 'varchar:OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH', 'true'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('src_backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('dst_backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('src_device_type', 'varchar:OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH'),
    ('dst_device_type', 'varchar:OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH'),
    ('backup_data_version', 'int', 'true'),
    ('backup_schema_version', 'int', 'true'),
    ('total_pg_count', 'int'),
    ('finish_pg_count', 'int'),
    ('total_partition_count', 'int'),
    ('finish_partition_count', 'int'),
    ('total_macro_block_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ('result', 'int'),
    ('encryption_mode', 'varchar:OB_MAX_ENCRYPTION_MODE_LENGTH', 'false', 'None'),
    ('passwd', 'varchar:OB_MAX_PASSWORD_LENGTH', 'false', ''),
    ('start_replay_log_ts', 'int', 'false', 0),
    ('date', 'int', 'false', 0),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_backup_backupset_task_def)

all_backup_backupset_task_history_def = dict(
  table_name = '__all_backup_backupset_task_history',
  table_id   = '301',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('job_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
    ('copy_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('backup_type', 'varchar:OB_INNER_TABLE_BACKUP_TYPE_LENTH'),
    ('snapshot_version', 'int'),
    ('prev_full_backup_set_id', 'int'),
    ('prev_inc_backup_set_id', 'int'),
    ('prev_backup_data_version', 'int'),
    ('input_bytes', 'int'),
    ('output_bytes', 'int'),
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('compatible', 'int'),
    ('cluster_id', 'int', 'true'),
    ('cluster_version', 'int'),
    ('cluster_version_display', 'varchar:OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH', 'true'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('src_backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('dst_backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('src_device_type', 'varchar:OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH'),
    ('dst_device_type', 'varchar:OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH'),
    ('backup_data_version', 'int', 'true'),
    ('backup_schema_version', 'int', 'true'),
    ('total_pg_count', 'int'),
    ('finish_pg_count', 'int'),
    ('total_partition_count', 'int'),
    ('finish_partition_count', 'int'),
    ('total_macro_block_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ('result', 'int'),
    ('encryption_mode', 'varchar:OB_MAX_ENCRYPTION_MODE_LENGTH', 'false', 'None'),
    ('passwd', 'varchar:OB_MAX_PASSWORD_LENGTH', 'false', ''),
    ('is_mark_deleted', 'bool', 'true'),
    ('start_replay_log_ts', 'int', 'false', 0),
    ('date', 'int', 'false', 0),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_backupset_task_history_def)

all_tenant_pg_backup_backupset_task_def = dict(
  table_name = '__all_tenant_pg_backup_backupset_task',
  table_id   = '302',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('job_id', 'int'),
    ('incarnation', 'int'),
    ('backup_set_id', 'int'),
    ('copy_id', 'int'),
    ('table_id', 'int'),
    ('partition_id', 'int'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('trace_id', 'varchar:OB_MAX_TRACE_ID_BUFFER_SIZE', 'true'),
    ('svr_ip', 'varchar:OB_MAX_SERVER_ADDR_SIZE'),
    ('svr_port', 'int'),
    ('total_partition_count', 'int'),
    ('finish_partition_count', 'int'),
    ('total_macro_block_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ('result', 'int'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_pg_backup_backupset_task_def)

# __all_tenant_backup_backup_log_archive_status
all_tenant_backup_backup_log_archive_status_def = dict(
  table_name    = '__all_tenant_backup_backup_log_archive_status',
  table_id      = '303',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('log_archive_round', 'int'),
    ('copy_id', 'int'),
  ],
  in_tenant_space = True,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('min_first_time', 'timestamp'),
    ('max_next_time', 'timestamp'),
    ('input_bytes', 'int', 'false', '0'),
    ('output_bytes', 'int', 'false', '0'),
    ('deleted_input_bytes', 'int', 'false', '0'),
    ('deleted_output_bytes', 'int', 'false', '0'),
    ('pg_count', 'int', 'false', '0'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH', 'false', ''),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_backup_backup_log_archive_status_def)


# __all_backup_backup_log_archive_status_history
all_backup_backup_log_archive_status_def = dict(
  table_name    = '__all_backup_backup_log_archive_status_history',
  table_id      = '304',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('log_archive_round', 'int'),
    ('copy_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('min_first_time', 'timestamp'),
    ('max_next_time', 'timestamp'),
    ('input_bytes', 'int', 'false', '0'),
    ('output_bytes', 'int', 'false', '0'),
    ('deleted_input_bytes', 'int', 'false', '0'),
    ('deleted_output_bytes', 'int', 'false', '0'),
    ('pg_count', 'int', 'false', '0'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('is_mark_deleted', 'bool', 'true'),
    ('compatible', 'int', 'false', '0'),
    ('start_piece_id', 'int', 'false', '0'),
    ('backup_piece_id', 'int', 'false', '0'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_backup_log_archive_status_def)

def_table_schema(
  table_name    = '__all_res_mgr_plan',
  table_id      = '305',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('plan', 'varchar:128', 'false')
  ],
  in_tenant_space = True,
  is_cluster_private = True,
  is_backup_private = False,
  normal_columns = [
    ('comments', 'varchar:2000', 'true')
  ],
  columns_with_tenant_id = [],
)

def_table_schema(
  table_name     = '__all_res_mgr_directive',
  table_id       = '306',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('plan', 'varchar:OB_MAX_RESOURCE_PLAN_NAME_LENGTH', 'false'),
    ('group_or_subplan', 'varchar:OB_MAX_RESOURCE_PLAN_NAME_LENGTH', 'false')
  ],
  in_tenant_space = True,
  is_cluster_private = True,
  is_backup_private = False,
  normal_columns = [
    ('comments', 'varchar:2000', 'true'),
    ('mgmt_p1', 'int', 'false', 100),
    ('utilization_limit', 'int', 'false', 100)
  ],
  columns_with_tenant_id = [],
)

def_table_schema(
  table_name     = '__all_res_mgr_mapping_rule',
  table_id       = '307',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('attribute', 'varchar:OB_MAX_RESOURCE_PLAN_NAME_LENGTH', 'false'),
    ('value', 'varbinary:OB_MAX_RESOURCE_PLAN_NAME_LENGTH', 'false')
  ],
  in_tenant_space = True,
  is_cluster_private = True,
  is_backup_private = False,
  normal_columns = [
    ('consumer_group', 'varchar:OB_MAX_RESOURCE_PLAN_NAME_LENGTH', 'true'),
    ('status', 'int', 'true')
  ],
  columns_with_tenant_id = [],
)


all_backup_backuppiece_job_def = dict(
  table_name = '__all_backup_backuppiece_job',
  table_id   = '310',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('job_id', 'int'),
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('backup_piece_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('max_backup_times', 'int'),
    ('result', 'int'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
    ('type', 'int'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_backuppiece_job_def)

all_backup_backuppiece_job_history_def = dict(
  table_name = '__all_backup_backuppiece_job_history',
  table_id   = '311',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('job_id', 'int'),
    ('tenant_id', 'int'),
    ('incarnation', 'int'),
    ('backup_piece_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('max_backup_times', 'int'),
    ('result', 'int'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
    ('type', 'int'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_backuppiece_job_history_def)

all_backup_backuppiece_task_def = dict(
  table_name = '__all_backup_backuppiece_task',
  table_id   = '312',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('job_id', 'int'),
    ('incarnation', 'int'),
    ('tenant_id', 'int'),
    ('round_id', 'int'),
    ('backup_piece_id', 'int'),
    ('copy_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('result', 'int'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_backuppiece_task_def)

all_backup_backuppiece_task_history_def = dict(
  table_name = '__all_backup_backuppiece_task_history',
  table_id   = '313',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('job_id', 'int'),
    ('incarnation', 'int'),
    ('tenant_id', 'int'),
    ('round_id', 'int'),
    ('backup_piece_id', 'int'),
    ('copy_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,
  normal_columns = [
    ('start_time', 'timestamp'),
    ('end_time', 'timestamp'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('result', 'int'),
    ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_backuppiece_task_history_def)

# __all_backup_piece_files
all_backup_piece_files_def = dict(
  table_name    = '__all_backup_piece_files',
  table_id      = '314',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('incarnation', 'int'),
    ('tenant_id', 'int'),
    ('round_id', 'int'),
    ('backup_piece_id', 'int'),
    ('copy_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('create_date', 'int', 'false', '0'),
    ('start_ts', 'int', 'false', '0'),
    ('checkpoint_ts', 'int', 'false', '0'),
    ('max_ts', 'int', 'false', '0'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH', 'false', ''),
    ('file_status', 'varchar:OB_DEFAULT_STATUS_LENTH', 'false', ''),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
    ('compatible', 'int', 'false', '0'), # same with round
    ('start_piece_id', 'int', 'false', '0'), # the start piece of a round
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_piece_files_def)

all_backup_set_files_def = dict(
  table_name = '__all_backup_set_files',
  table_id   = '315',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('incarnation', 'int'),
    ('tenant_id', 'int'),
    ('backup_set_id', 'int'),
    ('copy_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('backup_type', 'varchar:OB_INNER_TABLE_BACKUP_TYPE_LENTH'),
    ('snapshot_version', 'int'),
    ('prev_full_backup_set_id', 'int'),
    ('prev_inc_backup_set_id', 'int'),
    ('prev_backup_data_version', 'int'),
    ('pg_count', 'int'),
    ('macro_block_count', 'int'),
    ('finish_pg_count', 'int'),
    ('finish_macro_block_count', 'int'),
    ('input_bytes', 'int'),
    ('output_bytes', 'int'),
    ('start_time', 'int'),
    ('end_time', 'int'),
    ('compatible', 'int'),
    ('cluster_version', 'int'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('result', 'int'),
    ('cluster_id', 'int'),
    ('backup_data_version', 'int'),
    ('backup_schema_version', 'int'),
    ('cluster_version_display', 'varchar:OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH'),
    ('partition_count', 'int'),
    ('finish_partition_count', 'int'),
    ('encryption_mode', 'varchar:OB_MAX_ENCRYPTION_MODE_LENGTH', 'false', 'None'),
    ('passwd', 'varchar:OB_MAX_PASSWORD_LENGTH', 'false', ''),
    ('file_status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH'),
    ('start_replay_log_ts', 'int', 'false', 0),
    ('date', 'int', 'false', 0),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_set_files_def);

def_table_schema(
  table_name    = '__all_res_mgr_consumer_group',
  table_id      = '316',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int', 'false'),
    ('consumer_group', 'varchar:OB_MAX_RESOURCE_PLAN_NAME_LENGTH')
  ],
  in_tenant_space = True,
  is_cluster_private = True,
  is_backup_private = False,
  normal_columns = [
    ('consumer_group_id', 'int'),
    ('comments', 'varchar:2000', 'true')
  ],
  columns_with_tenant_id = [],
)
# __all_backup_info
all_backup_info_def = dict(
  table_name    = '__all_backup_info',
  table_id      = '317',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('name', 'varchar:OB_INNER_TABLE_DEFAULT_KEY_LENTH'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('value', 'longtext'),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_info_def)

# __all_backup_log_archive_status
all_backup_log_archive_status_def = dict(
  table_name    = '__all_backup_log_archive_status_v2',
  table_id      = '318',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('incarnation', 'int'),
    ('log_archive_round', 'int'),
    ('min_first_time', 'timestamp'),
    ('max_next_time', 'timestamp'),
    ('input_bytes', 'int', 'false', '0'),
    ('output_bytes', 'int', 'false', '0'),
    ('deleted_input_bytes', 'int', 'false', '0'),
    ('deleted_output_bytes', 'int', 'false', '0'),
    ('pg_count', 'int', 'false', '0'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('is_mount_file_created', 'int', 'false', '0'),
    ('compatible', 'int', 'false', '0'),
    ('start_piece_id', 'int', 'false', '0'),
    ('backup_piece_id', 'int', 'false', '0'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'false', ''),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_backup_log_archive_status_def)

# __all_backup_backup_log_archive_status
all_tenant_backup_backup_log_archive_status_def = dict(
  table_name    = '__all_backup_backup_log_archive_status_v2',
  table_id      = '321',
  table_type = 'SYSTEM_TABLE',
  gm_columns = ['gmt_create', 'gmt_modified'],
  rowkey_columns = [
    ('tenant_id', 'int'),
  ],
  in_tenant_space = False,
  rs_restart_related = False,
  is_cluster_private = True,
  is_backup_private = True,

  normal_columns = [
    ('copy_id', 'int'),
    ('incarnation', 'int'),
    ('log_archive_round', 'int'),
    ('min_first_time', 'timestamp'),
    ('max_next_time', 'timestamp'),
    ('input_bytes', 'int', 'false', '0'),
    ('output_bytes', 'int', 'false', '0'),
    ('deleted_input_bytes', 'int', 'false', '0'),
    ('deleted_output_bytes', 'int', 'false', '0'),
    ('pg_count', 'int', 'false', '0'),
    ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
    ('is_mount_file_created', 'int', 'false', '0'),
    ('compatible', 'int', 'false', '0'),
    ('start_piece_id', 'int', 'false', '0'),
    ('backup_piece_id', 'int', 'false', '0'),
    ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'false', ''),
  ],
  columns_with_tenant_id = [],
)
def_table_schema(**all_tenant_backup_backup_log_archive_status_def)

# sys index schema def, only for compatible

def_sys_index_table(
  index_name = 'idx_data_table_id',
  index_table_id = 9997,
  index_columns = ['incarnation', 'backup_piece_id', 'copy_id'],
  index_using_type = 'USING_BTREE',
  keywords = all_def_keywords['__all_backup_piece_files'])

# sys index schema def, only for compatible
def_sys_index_table(
  index_name = 'idx_data_table_id',
  index_table_id = 9998,
  index_columns = ['data_table_id'],
  index_using_type = 'USING_BTREE',
  keywords = all_def_keywords['__all_table_v2_history'])

def_sys_index_table(
  index_name = 'idx_data_table_id',
  index_table_id = 9999,
  index_columns = ['data_table_id'],
  index_using_type = 'USING_BTREE',
  keywords = all_def_keywords['__all_table_history'])

################################################################################
# Virtual Table (10000, 20000]
# Normally, virtual table's index_using_type should be USING_HASH.
################################################################################

def_table_schema(
  table_name     = '__tenant_virtual_all_table',
  table_id       = '10001',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  in_tenant_space = True,

  rowkey_columns = [
  ('database_id', 'int'),
  ('table_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH'),
  ],
  normal_columns = [
  ('table_type', 'varchar:OB_MAX_TABLE_TYPE_LENGTH'),
  ('engine', 'varchar:MAX_ENGINE_LENGTH'),
  ('version', 'int'),
  ('row_format', 'varchar:ROW_FORMAT_LENGTH'),
  ('rows', 'int'),
  ('avg_row_length', 'int'),
  ('data_length', 'int'),
  ('max_data_length', 'int'),
  ('index_length', 'int'),
  ('data_free', 'int'),
  ('auto_increment', 'uint'),
  ('create_time', 'timestamp'),
  ('update_time', 'timestamp'),
  ('check_time', 'timestamp'),
  ('collation', 'varchar:MAX_COLLATION_LENGTH'),
  ('checksum', 'int'),
  ('create_options', 'varchar:MAX_TABLE_STATUS_CREATE_OPTION_LENGTH'),
  ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_table_column',
  table_id       = '10002',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('table_id', 'int'),
  ('field', 'varchar:OB_MAX_COLUMN_NAME_LENGTH'),
  ],
  in_tenant_space = True,

  normal_columns = [
  ('type', 'varchar:COLUMN_TYPE_LENGTH'),
  ('collation', 'varchar:MAX_COLLATION_LENGTH'),
  ('null', 'varchar:COLUMN_NULLABLE_LENGTH'),
  ('key', 'varchar:COLUMN_KEY_LENGTH'),
  ('default', 'varchar:COLUMN_DEFAULT_LENGTH'),
  ('extra', 'varchar:COLUMN_EXTRA_LENGTH'),
  ('privileges', 'varchar:MAX_COLUMN_PRIVILEGE_LENGTH'),
  ('comment', 'varchar:MAX_COLUMN_COMMENT_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_table_index',
  table_id       = '10003',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('table_id', 'int'),
  ('key_name', 'varchar:OB_MAX_COLUMN_NAME_LENGTH', 'false', ''),
  ('seq_in_index', 'int', 'false', '0'),
  ],
  in_tenant_space = True,

  normal_columns = [
  ('table_schema', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
  ('table', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'false', ''),
  ('non_unique', 'int', 'false', '0'),
  ('index_schema', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
  ('column_name', 'varchar:OB_MAX_COLUMN_NAME_LENGTH', 'false', ''),
  ('collation', 'varchar:MAX_COLLATION_LENGTH', 'true'),
  ('cardinality', 'int', 'true'),
  ('sub_part', 'varchar:INDEX_SUB_PART_LENGTH', 'true'),
  ('packed', 'varchar:INDEX_PACKED_LENGTH', 'true'),
  ('null', 'varchar:INDEX_NULL_LENGTH', 'false', ''),
  ('index_type', 'varchar:INDEX_NULL_LENGTH', 'false', ''),
  ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH', 'true'),
  ('index_comment', 'varchar:MAX_TABLE_COMMENT_LENGTH', 'false', ''),
  ('is_visible', 'varchar:MAX_COLUMN_YES_NO_LENGTH', 'false', ''),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_show_create_database',
  table_id       = '10004',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('database_id', 'int'),
  ],
  in_tenant_space = True,

  normal_columns = [
  ('database_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
  ('create_database', 'varchar:DATABASE_DEFINE_LENGTH'),
  ('create_database_with_if_not_exists', 'varchar:DATABASE_DEFINE_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_show_create_table',
  table_id       = '10005',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('table_id', 'int'),
  ],
  in_tenant_space = True,

  normal_columns = [
  ('table_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH'),
  ('create_table', 'longtext'),
  ('character_set_client', 'varchar:MAX_CHARSET_LENGTH'),
  ('collation_connection', 'varchar:MAX_CHARSET_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_session_variable',
  table_id       = '10006',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('variable_name', 'varchar:OB_MAX_CONFIG_NAME_LEN', 'false', ''),
  ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN', 'true'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_privilege_grant',
  table_id       = '10007',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('user_id', 'int'),
  ],
  in_tenant_space = True,

  normal_columns = [
  ('grants', 'varchar:MAX_GRANT_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_processlist',
  table_id       = '10008',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],

  normal_columns = [
  ('id', 'uint', 'false', '0'),
  ('user', 'varchar:OB_MAX_USERNAME_LENGTH', 'false', ''),
  ('tenant', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE'),
  ('host', 'varchar:OB_MAX_HOST_NAME_LENGTH', 'false', ''),
  ('db', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'true'),
  ('command', 'varchar:OB_MAX_COMMAND_LENGTH', 'false', ''),
  ('sql_id', 'varchar:OB_MAX_SQL_ID_LENGTH', 'false', ''),
  ('time', 'int', 'false', '0'),
  ('state', 'varchar:OB_MAX_SESSION_STATE_LENGTH', 'true'),
  ('info', 'varchar:MAX_COLUMN_VARCHAR_LENGTH', 'true'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('sql_port', 'int'),
  ('proxy_sessid', 'uint', 'true'),
  ('master_sessid', 'uint', 'true'),
  ('user_client_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'true'),
  ('user_host', 'varchar:OB_MAX_HOST_NAME_LENGTH', 'true'),
  ('trans_id', 'uint'),
  ('thread_id', 'uint'),
  ('ssl_cipher', 'varchar:OB_MAX_COMMAND_LENGTH', 'true'),
  ('trace_id', 'varchar:OB_MAX_TRACE_ID_BUFFER_SIZE', 'true', ''),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__tenant_virtual_warning',
  table_id       = '10009',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('level', 'varchar:32'),
  ('code', 'int'),
  ('message', 'varchar:512'),# the same as warning buffer length
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_current_tenant',
  table_id       = '10010',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('tenant_id', 'int'),
  ],
  in_tenant_space = True,
  normal_columns = [
  ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE'),
  ('create_stmt', 'varchar:TENANT_DEFINE_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_database_status',
  table_id       = '10011',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('db', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('read_only', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__tenant_virtual_tenant_status',
  table_id       = '10012',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('tenant', 'varchar:OB_MAX_TENANT_NAME_LENGTH'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('read_only', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)



def_table_schema(
  table_name     = '__tenant_virtual_interm_result',
  table_id       = '10013',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,
  enable_column_def_enum = True,

  normal_columns = [
  ('job_id', 'int'),
  ('task_id', 'int'),
  ('slice_id', 'int'),
  ('execution_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('expire_time', 'int'),
  ('row_count', 'int'),
  ('scanner_count', 'int'),
  ('used_memory_size', 'int'),
  ('used_disk_size', 'int'),
  ('partition_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('partition_port', 'int'),
  ],
  partition_columns = ['partition_ip', 'partition_port'],
)

def_table_schema(
  table_name     = '__tenant_virtual_partition_stat',
  table_id       = '10014',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  in_tenant_space = True,

  normal_columns = [
  ('table_id', 'int'),
  ('partition_id', 'int'),
  ('partition_cnt', 'int'),
  ('row_count', 'int'),
  ('diff_percentage', 'int'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_statname',
  table_id       = '10015',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  in_tenant_space = True,

  normal_columns = [
  ('tenant_id', 'int'),
  ('stat_id', 'int'),
  ('statistic#', 'int'),
  ('name', 'varchar:64'),
  ('display_name', 'varchar:64'),
  ('class','int'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_event_name',
  table_id       = '10016',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  in_tenant_space = True,

  normal_columns = [
  ('tenant_id', 'int'),
  ('event_id', 'int'),
  ('event#', 'int'),
  ('name', 'varchar:64'),
  ('display_name', 'varchar:64'),
  ('parameter1', 'varchar:64'),
  ('parameter2', 'varchar:64'),
  ('parameter3', 'varchar:64'),
  ('wait_class_id','int'),
  ('wait_class#','int'),
  ('wait_class','varchar:64'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_global_variable',
  table_id       = '10017',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('variable_name', 'varchar:OB_MAX_CONFIG_NAME_LEN', 'false', ''),
  ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN', 'true'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_show_tables',
  table_id       = '10018',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  in_tenant_space = True,

  rowkey_columns = [
  ('database_id', 'int'),
  ('table_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH'),
  ],
  normal_columns = [
  ('table_type', 'varchar:OB_MAX_TABLE_TYPE_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_show_create_procedure',
  table_id       = '10019',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('routine_id', 'int'),
  ],
  in_tenant_space = True,

  normal_columns = [
  ('routine_name', 'varchar:OB_MAX_ROUTINE_NAME_LENGTH'),
  ('create_routine', 'longtext'),
  ('proc_type', 'int'),
  ('character_set_client', 'varchar:MAX_CHARSET_LENGTH'),
  ('collation_connection', 'varchar:MAX_CHARSET_LENGTH'),
  ('collation_database', 'varchar:MAX_CHARSET_LENGTH'),
  ('sql_mode', 'varchar:MAX_CHARSET_LENGTH'),
  ],
)

def_table_schema(
    table_name    = '__all_virtual_core_meta_table',
    table_id      = '11001',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
    ],
    only_rs_vtable = True,

  normal_columns = [
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('sql_port', 'int'),
      ('unit_id', 'int'),
      ('partition_cnt', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('role', 'int'),
      ('member_list', 'varchar:MAX_MEMBER_LIST_LENGTH'),
      ('row_count', 'int'),
      ('data_size', 'int'),
      ('data_version', 'int'),
      ('data_checksum', 'int'),
      ('row_checksum', 'int'),
      ('column_checksum', 'varchar:COLUMN_CHECKSUM_LENGTH'),
      ('is_original_leader', 'int', 'false', '0'),
      ('is_previous_leader', 'int', 'false', '0'),
      ('create_time', 'int'),
      ('rebuild', 'int', 'false', '0'),
      ('replica_type', 'int', 'false', '0'),
      ('required_size', 'int', 'false', '0'),
      ('status', 'varchar:MAX_REPLICA_STATUS_LENGTH', 'false', 'REPLICA_STATUS_NORMAL'),
      ('is_restore', 'int', 'false', '0'),
      ('partition_checksum', 'int', 'false', '0'),
      ('quorum', 'int', 'false', '-1'),
      ('fail_list', 'varchar:OB_MAX_FAILLIST_LENGTH', 'false', ''),
      ('recovery_timestamp', 'int', 'false', '0'),
      ('memstore_percent', 'int', 'false', '100'),
  ],
)

def_table_schema(
    table_name    = '__all_virtual_zone_stat',
    table_id      = '11002',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [],
    only_rs_vtable = False,

  normal_columns = [
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('is_merging', 'int'),
      ('status', 'varchar:MAX_ZONE_STATUS_LENGTH'),
      ('server_count', 'int'),
      ('resource_pool_count', 'int'),
      ('unit_count', 'int'),
      ('cluster', 'varchar:MAX_ZONE_INFO_LENGTH'),
      ('region', 'varchar:MAX_REGION_LENGTH'),
      ('spare1', 'int'),
      ('spare2', 'int'),
      ('spare3', 'int'),
      ('spare4', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH'),
      ('spare5', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH'),
      ('spare6', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_plan_cache_stat',
  table_id       = '11003',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int')
  ],

  normal_columns = [
    ('sql_num', 'int'),
    ('mem_used', 'int'),
    ('mem_hold', 'int'),
    ('access_count', 'int'),
    ('hit_count', 'int'),
    ('hit_rate', 'int'),
    ('plan_num', 'int'),
    ('mem_limit', 'int'),
    ('hash_bucket', 'int'),
    ('stmtkey_num', 'int'),
    ('pc_ref_plan_local', 'int'),
    ('pc_ref_plan_remote', 'int'),
    ('pc_ref_plan_dist', 'int'),
    ('pc_ref_plan_arr', 'int'),
    ('pc_ref_plan_stat', 'int'),
    ('pc_ref_pl', 'int'),
    ('pc_ref_pl_stat', 'int'),
    ('plan_gen', 'int'),
    ('cli_query', 'int'),
    ('outline_exec', 'int'),
    ('plan_explain', 'int'),
    ('asyn_baseline', 'int'),
    ('load_baseline', 'int'),
    ('ps_exec', 'int'),
    ('gv_sql', 'int'),
    ('pl_anon', 'int'),
    ('pl_routine', 'int'),
    ('package_var', 'int'),
    ('package_type', 'int'),
    ('package_spec', 'int'),
    ('package_body', 'int'),
    ('package_resv', 'int'),
    ('get_pkg', 'int'),
    ('index_builder', 'int'),
    ('pcv_set', 'int'),
    ('pcv_rd', 'int'),
    ('pcv_wr', 'int'),
    ('pcv_get_plan_key', 'int'),
    ('pcv_get_pl_key', 'int'),
    ('pcv_expire_by_used', 'int'),
    ('pcv_expire_by_mem', 'int')
  ],
  partition_columns = ['svr_ip', 'svr_port'],
  index = {'all_virtual_plan_cache_stat_i1' :  { 'index_columns' : ['tenant_id'],
                     'index_using_type' : 'USING_HASH'}},
)

def_table_schema(
    table_name     = '__all_virtual_plan_stat',
    table_id       = '11004',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
    ],
  enable_column_def_enum = True,

  normal_columns = [
      ('tenant_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('plan_id', 'int'),
      ('sql_id', 'varchar:OB_MAX_SQL_ID_LENGTH'),
      ('type', 'int'),
      ('is_bind_sensitive', 'int'),
      ('is_bind_aware', 'int'),
      ('statement', 'longtext'),
      ('query_sql', 'longtext'),
      ('special_params', 'varchar:OB_MAX_COMMAND_LENGTH'),
      ('param_infos', 'longtext'),
      ('sys_vars', 'varchar:OB_MAX_COMMAND_LENGTH'),
      ('plan_hash', 'uint'),
      ('first_load_time', 'timestamp'),
      ('schema_version', 'int'),
      ('merged_version', 'int'),
      ('last_active_time', 'timestamp'),
      ('avg_exe_usec', 'int'),
      ('slowest_exe_time', 'timestamp'),
      ('slowest_exe_usec', 'int'),
      ('slow_count', 'int'),
      ('hit_count', 'int'),
      ('plan_size', 'int'),
      ('executions', 'int'),
      ('disk_reads', 'int'),
      ('direct_writes', 'int'),
      ('buffer_gets', 'int'),
      ('application_wait_time', 'uint'),
      ('concurrency_wait_time', 'uint'),
      ('user_io_wait_time', 'uint'),
      ('rows_processed', 'int'),
      ('elapsed_time', 'uint'),
      ('cpu_time', 'uint'),
      ('large_querys', 'int'),
      ('delayed_large_querys', 'int'),
      ('outline_version', 'int'),
      ('outline_id', 'int'),
      ('outline_data', 'longtext', 'false'),
      ('acs_sel_info', 'longtext', 'false'),
      ('table_scan', 'bool'),
      ('db_id', 'uint'),
      ('evolution', 'bool'),
      ('evo_executions', 'int'),
      ('evo_cpu_time', 'uint'),
      ('timeout_count', 'int'),
      ('ps_stmt_id', 'int'),
      ('delayed_px_querys', 'int'),
      ('sessid', 'uint'),
      ('temp_tables', 'longtext', 'false'),
      ('is_use_jit', 'bool'),
      ('object_type', 'longtext', 'false'),
      ('enable_bf_cache', 'bool'),
      ('bf_filter_cnt', 'int'),
      ('bf_access_cnt', 'int'),
      ('enable_row_cache', 'bool'),
      ('row_cache_hit_cnt', 'int'),
      ('row_cache_miss_cnt', 'int'),
      ('enable_fuse_row_cache', 'bool'),
      ('fuse_row_cache_hit_cnt', 'int'),
      ('fuse_row_cache_miss_cnt', 'int'),
      ('hints_info', 'longtext', 'false'),
      ('hints_all_worked', 'bool'),
      ('pl_schema_id', 'uint'),
      ('is_batched_multi_stmt', 'bool')
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name    = '__all_virtual_mem_leak_checker_info',
  table_id      = '11006',
  table_type = 'VIRTUAL_TABLE',
  gm_columns    = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('mod_name', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('mod_type', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('alloc_count', 'int'),
  ('alloc_size', 'int'),
  ('back_trace', 'varchar:DEFAULT_BUF_LENGTH'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name    = '__all_virtual_latch',
  table_id      = '11007',
  table_type = 'VIRTUAL_TABLE',
  gm_columns    = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int', 'false'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int'),
  ('latch_id', 'int', 'false'),
  ('name', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('addr', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('level', 'int'),
  ('hash', 'int'),
  ('gets', 'int'),
  ('misses', 'int'),
  ('sleeps', 'int'),
  ('immediate_gets', 'int'),
  ('immediate_misses', 'int'),
  ('spin_gets', 'int'),
  ('wait_time', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name     = '__all_virtual_kvcache_info',
  table_id       = '11008',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int', 'false'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int'),
  ('cache_name', 'varchar:OB_MAX_KVCACHE_NAME_LENGTH', 'false'),
  ('cache_id', 'int', 'false'),
  ('priority', 'int', 'false'),
  ('cache_size', 'int', 'false'),
  ('cache_store_size', 'int', 'false'),
  ('cache_map_size', 'int', 'false'),
  ('kv_cnt', 'int', 'false'),
  ('hit_ratio', 'number:38:3', 'false'),
  ('total_put_cnt', 'int', 'false'),
  ('total_hit_cnt', 'int', 'false'),
  ('total_miss_cnt', 'int', 'false'),
  ('hold_size', 'int', 'false'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name    = '__all_virtual_data_type_class',
  table_id      = '11009',
  table_type = 'VIRTUAL_TABLE',
  gm_columns    = [],
  rowkey_columns = [
  ('data_type_class', 'int'),
  ],
  in_tenant_space = True,

  normal_columns = [
  ('data_type_class_str', 'varchar:OB_MAX_SYS_PARAM_NAME_LENGTH'),
  ],
)

def_table_schema(
  table_name    = '__all_virtual_data_type',
  table_id      = '11010',
  table_type = 'VIRTUAL_TABLE',
  gm_columns    = [],
  rowkey_columns = [
  ('data_type', 'int'),
  ],
  in_tenant_space = True,

  normal_columns = [
  ('data_type_str', 'varchar:OB_MAX_SYS_PARAM_NAME_LENGTH'),
  ('data_type_class', 'int'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_server_stat',
  table_id       = '11011',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  only_rs_vtable = True,

  normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('cpu_total', 'double'),
      ('cpu_assigned', 'double'),
      ('cpu_assigned_percent', 'int'),
      ('mem_total', 'int'),
      ('mem_assigned', 'int'),
      ('mem_assigned_percent', 'int'),
      ('disk_total', 'int'),
      ('disk_assigned', 'int'),
      ('disk_assigned_percent', 'int'),
      ('unit_num', 'int'),
      ('migrating_unit_num', 'int'),
      ('[discard]temporary_unit_num', 'int'),
      ('merged_version', 'int'),
      ('leader_count', 'int'),
      # fields added on V1.4
      ('load', 'double'),
      ('cpu_weight', 'double'),
      ('memory_weight', 'double'),
      ('disk_weight', 'double'),
      # fields added on V1.4.3 :
      ('id', 'int'),
      ('inner_port', 'int'),
      ('build_version', 'varchar:OB_SERVER_VERSION_LENGTH'),
      ('register_time', 'int'),
      ('last_heartbeat_time', 'int'),
      ('block_migrate_in_time', 'int'),
      ('start_service_time', 'int'),
      ('last_offline_time', 'int'),
      ('stop_time', 'int'),
      ('force_stop_heartbeat', 'int'),
      ('admin_status', 'varchar:OB_SERVER_STATUS_LENGTH'),
      ('heartbeat_status', 'varchar:OB_SERVER_STATUS_LENGTH'),
      ('with_rootserver', 'int'),
      ('with_partition', 'int'),
      ('mem_in_use', 'int'),
      ('disk_in_use', 'int'),
      ('clock_deviation', 'int'),
      ('heartbeat_latency', 'int'),
      ('clock_sync_status', 'varchar:OB_SERVER_STATUS_LENGTH'),
      # fields added on 2.0
      ('cpu_capacity', 'double'),
      ('cpu_max_assigned', 'double'),
      ('mem_capacity', 'int'),
      ('mem_max_assigned', 'int'),
      ('ssl_key_expired_time', 'int')
  ],
)

def_table_schema(
  table_name     = '__all_virtual_rebalance_task_stat',
  table_id       = '11012',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  only_rs_vtable = True,

  normal_columns = [
  ('tenant_id', 'uint'),
  ('table_id', 'uint'),
  ('partition_id', 'int'),
  ('partition_count', 'int'),
  ('source', 'varchar:OB_IP_PORT_STR_BUFF'),
  ('data_source', 'varchar:OB_IP_PORT_STR_BUFF'),
  ('destination', 'varchar:OB_IP_PORT_STR_BUFF'),
  ('offline', 'varchar:OB_IP_PORT_STR_BUFF'),
  ('is_replicate', 'varchar:MAX_BOOL_STR_LENGTH'),
  ('task_type', 'varchar:128'),
  ('is_scheduled', 'varchar:MAX_BOOL_STR_LENGTH'),
  ('is_manual', 'varchar:MAX_BOOL_STR_LENGTH'),
  ('waiting_time', 'int'),
  ('executing_time', 'int'),
  ],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name     = '__all_virtual_session_event',
  table_id       = '11013',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('session_id', 'int', 'false'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int'),
  ('event_id', 'int', 'false'),
  ],

  normal_columns = [
  ('tenant_id', 'int', 'false'),
  ('event', 'varchar:OB_MAX_WAIT_EVENT_NAME_LENGTH', 'false'),
  ('wait_class_id', 'int', 'false'),
  ('wait_class#', 'int', 'false'),
  ('wait_class', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
  ('total_waits', 'int', 'false'),
  ('total_timeouts', 'int', 'false'),
  ('time_waited', 'double', 'false'),
  ('max_wait', 'double', 'false'),
  ('average_wait', 'double', 'false'),
  ('time_waited_micro', 'int', 'false'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
  index = {'all_virtual_session_event_i1' : { 'index_columns' : ['session_id'],
                    'index_using_type' : 'USING_HASH'}},
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name     = '__all_virtual_session_wait',
  table_id       = '11014',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('session_id', 'int', 'false'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int'),
  ],

  normal_columns = [
  ('tenant_id', 'int', 'false'),
  ('event', 'varchar:OB_MAX_WAIT_EVENT_NAME_LENGTH', 'false'),
  ('p1text', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
  ('p1', 'uint', 'false'),
  ('p2text', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
  ('p2', 'uint', 'false'),
  ('p3text', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
  ('p3', 'uint', 'false'),
  ('level', 'int', 'false'),
  ('wait_class_id', 'int', 'false'),
  ('wait_class#', 'int', 'false'),
  ('wait_class', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
  ('state', 'varchar:19', 'false'),
  ('wait_time_micro', 'int', 'false'),
  ('time_remaining_micro', 'int', 'false'),
  ('time_since_last_wait_micro', 'int', 'false'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
  index = {'all_virtual_session_wait_i1' : { 'index_columns' : ['session_id'],
                    'index_using_type' : 'USING_HASH'}},
)


def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name     = '__all_virtual_session_wait_history',
  table_id       = '11015',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('session_id', 'int', 'false'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int'),
  ('seq#', 'int', 'false'),
],
normal_columns = [
  ('tenant_id', 'int', 'false'),
  ('event#', 'int', 'false'),
  ('event', 'varchar:OB_MAX_WAIT_EVENT_NAME_LENGTH', 'false'),
  ('p1text', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
  ('p1', 'uint', 'false'),
  ('p2text', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
  ('p2', 'uint', 'false'),
  ('p3text', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
  ('p3', 'uint', 'false'),
  ('level', 'int', 'false'),
  ('wait_time_micro', 'int', 'false'),
  ('time_since_last_wait_micro', 'int', 'false'),
  ('wait_time', 'double', 'false'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
  index = {'all_virtual_session_wait_history_i1' : { 'index_columns' : ['session_id'],
                    'index_using_type' : 'USING_HASH'}},
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  table_name     = '__all_virtual_system_event',
  table_id       = '11017',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('tenant_id', 'int', 'false'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int'),
  ('event_id', 'int', 'false'),
  ],

  normal_columns = [
  ('event', 'varchar:OB_MAX_WAIT_EVENT_NAME_LENGTH', 'false'),
  ('wait_class_id', 'int', 'false'),
  ('wait_class#', 'int', 'false'),
  ('wait_class', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
  ('total_waits', 'int', 'false'),
  ('total_timeouts', 'int', 'false'),
  ('time_waited', 'double', 'false'),
  ('max_wait', 'double', 'false'),
  ('average_wait', 'double', 'false'),
  ('time_waited_micro', 'int', 'false'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
  index = {'all_virtual_system_event_i1' : { 'index_columns' : ['tenant_id'],
                    'index_using_type' : 'USING_HASH'}},
)


def_table_schema(
  table_name     = '__all_virtual_tenant_memstore_info',
  table_id       = '11018',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('active_memstore_used', 'int'),
  ('total_memstore_used', 'int'),
  ('major_freeze_trigger', 'int'),
  ('memstore_limit', 'int'),
  ('freeze_cnt', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_concurrency_object_pool',
  table_id       = '11019',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ],

  normal_columns = [
  ('free_list_name', 'varchar:OB_MAX_SYS_PARAM_VALUE_LENGTH'),
  ('allocated', 'int'),
  ('in_use', 'int'),
  ('count', 'int'),
  ('type_size', 'int'),
  ('chunk_count', 'int'),
  ('chunk_byte_size', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name     = '__all_virtual_sesstat',
  table_id       = '11020',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('session_id', 'int', 'false'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int'),
  ('statistic#', 'int', 'false'),
  ],

  normal_columns = [
  ('tenant_id', 'int', 'false'),
  ('value', 'int', 'false'),
  ('can_visible', 'bool', 'false'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
  index = {'all_virtual_sesstat_i1' : { 'index_columns' : ['session_id'],
                    'index_using_type' : 'USING_HASH'}},
)



def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  table_name     = '__all_virtual_sysstat',
  table_id       = '11021',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('tenant_id', 'int', 'false'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int'),
  ('statistic#', 'int', 'false'),
  ],

  normal_columns = [
  ('value', 'int', 'false'),
  ('stat_id', 'int', 'false'),
  ('name', 'varchar:64', 'false'),
  ('class', 'int', 'false'),
  ('can_visible', 'bool', 'false'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
  index = {'all_virtual_sysstat_i1' : { 'index_columns' : ['tenant_id'],
                    'index_using_type' : 'USING_HASH'}},
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name     = '__all_virtual_storage_stat',
  table_type = 'VIRTUAL_TABLE',
  table_id       = '11022',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int', 'false'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int', 'false'),
  ('table_id', 'int', 'false'),
  ('partition_cnt', 'int', 'false'),
  ('partition_id', 'int', 'false'),
  ('major_version', 'int', 'false'),
  ('minor_version', 'int', 'false'),
  ('sstable_id', 'int', 'false'),
  ('role', 'int', 'false'),
  ('data_checksum', 'int', 'false'),
  ('column_checksum', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH', 'false'),
  ('macro_blocks', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH', 'false'),
  ('occupy_size', 'int', 'false'),
  ('used_size', 'int', 'false'),
  ('row_count', 'int', 'false'),
  ('store_type', 'int', 'false'),
  ('progressive_merge_start_version', 'int', 'false'),
  ('progressive_merge_end_version', 'int', 'false'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name     = '__all_virtual_disk_stat',
  table_id       = '11023',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int', 'false'),
  ('total_size', 'int', 'false'),
  ('used_size', 'int', 'false'),
  ('free_size', 'int', 'false'),
  ('is_disk_valid', 'int', 'false'),
  ('disk_error_begin_ts', 'int', 'false'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_memstore_info',
  table_id       = '11024',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('table_id', 'int'),
  ('partition_idx', 'int'),
  ('partition_cnt', 'int'),
  ('version', 'varchar:MAX_VERSION_LENGTH'),
  ('base_version', 'int'),
  ('multi_version_start', 'int'),
  ('snapshot_version', 'int'),
  ('is_active', 'int'),
  ('mem_used', 'int'),
  ('hash_item_count', 'int'),
  ('hash_mem_used', 'int'),
  ('btree_item_count', 'int'),
  ('btree_mem_used', 'int'),
  ('insert_row_count', 'int'),
  ('update_row_count', 'int'),
  ('delete_row_count', 'int'),
  ('purge_row_count', 'int'),
  ('purge_queue_count', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_partition_info',
  table_id       = '11025',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_idx', 'int'),
      ('max_decided_trans_version', 'int'),
      ('max_passed_trans_ts', 'int'),
      ('freeze_ts', 'int'),
      ('allow_gc', 'bool'),
      ('partition_state', 'varchar:TABLE_MAX_VALUE_LENGTH'),
      ('sstable_read_rate', 'double'),
      ('sstable_read_bytes_rate', 'double'),
      ('sstable_write_rate', 'double'),
      ('sstable_write_bytes_rate', 'double'),
      ('log_write_rate', 'double'),
      ('log_write_bytes_rate', 'double'),
      ('memtable_bytes', 'int'),
      ('cpu_utime_rate', 'double'),
      ('cpu_stime_rate', 'double'),
      ('net_in_rate', 'double'),
      ('net_in_bytes_rate', 'double'),
      ('net_out_rate', 'double'),
      ('net_out_bytes_rate', 'double'),
      ('min_log_service_ts', 'int', 'false', '-1'),
      ('min_trans_service_ts', 'int', 'false', '-1'),
      ('min_replay_engine_ts', 'int', 'false', '-1'),
      ('is_need_rebuild', 'bool'),
      ('pg_partition_count', 'int'),
      ('is_pg', 'bool'),
      ('weak_read_timestamp', 'int', 'false', '-1'),
      ('replica_type', 'int', 'false', '0'),
      ('last_replay_log_id', 'int', 'false', '0'),
      ('schema_version', 'int', 'false', '0'),
      ('last_replay_log_ts', 'int', 'false', '0'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_upgrade_inspection',
  table_id       = '11026',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  only_rs_vtable = True,

  normal_columns = [
  ('name', 'varchar:TABLE_MAX_KEY_LENGTH'),
  ('info', 'varchar:MAX_ZONE_INFO_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_trans_stat',
  table_id       = '11027',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('inc_num', 'bigint:20'),
  ('session_id', 'bigint:20'),
  ('proxy_id', 'varchar:512'),
  ('trans_type', 'int'),
  ('trans_id', 'varchar:512'),
  ('is_exiting', 'int'),
  ('is_readonly', 'int'),
  ('is_decided', 'int'),
  ('trans_mode', 'varchar:64'),
  ('active_memstore_version', 'varchar:64'),
  ('partition', 'varchar:64'),
  ('participants', 'varchar:1024'),
  ('autocommit', 'int'),
  ('trans_consistency', 'int'),
  ('ctx_create_time', 'timestamp', 'true'),
  ('expired_time', 'timestamp', 'true'),
  ('refer', 'bigint:20'),
  ('sql_no', 'bigint:20'),
  ('state', 'bigint:20'),
  ('part_trans_action', 'bigint:20'),
  ('lock_for_read_retry_count', 'bigint:20'),
  ('ctx_addr', 'bigint:20'),
  ('prev_trans_arr', 'varchar:1024'),
  ('prev_trans_count', 'bigint:20'),
  ('next_trans_arr', 'varchar:1024'),
  ('next_trans_count', 'bigint:20'),
  ('prev_trans_commit_count', 'bigint:20'),
  ('ctx_id', 'bigint:20'),
  ('pending_log_size', 'bigint:20'),
  ('flushed_log_size', 'bigint:20'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_trans_mgr_stat',
  table_id       = '11028',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('table_id', 'bigint:20'),
  ('partition_idx', 'bigint:20'),
  ('ctx_type', 'bigint:20'),
  ('is_master', 'int'),
  ('is_frozen', 'int'),
  ('is_stopped', 'int'),
  ('read_only_count', 'int'),
  ('active_read_write_count', 'int'),
  ('active_memstore_version', 'varchar:64'),
  ('total_ctx_count', 'int'),
  ('with_dep_trans_count', 'int'),
  ('without_dep_trans_count', 'int'),
  ('endtrans_by_prev_count', 'int'),
  ('endtrans_by_checkpoint_count', 'int'),
  ('endtrans_by_self_count', 'int'),
  ('mgr_addr', 'bigint:20'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_election_info',
  table_id       = '11029',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('table_id', 'bigint:20'),
  ('partition_idx', 'bigint:20'),
  ('is_running', 'int'),
  ('is_changing_leader', 'int'),
  ('current_leader', 'varchar:64'),
  ('previous_leader', 'varchar:64'),
  ('proposal_leader', 'varchar:64'),
  ('member_list', 'varchar:MAX_MEMBER_LIST_LENGTH'),
  ('replica_num', 'bigint:20'),
  ('lease_start', 'bigint:20'),
  ('lease_end', 'bigint:20'),
  ('time_offset', 'bigint:20'),
  ('active_timestamp', 'bigint:20'),
  ('T1_timestamp', 'bigint:20'),
  ('leader_epoch', 'bigint:20'),
  ('state', 'bigint:20'),
  ('role', 'bigint:20'),
  ('stage', 'bigint:20'),
  ('eg_id', 'uint'),
  ('remaining_time_in_blacklist','int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_election_mem_stat',
  table_id       = '11030',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('type', 'varchar:64'),
    ('alloc_count', 'bigint:20'),
    ('release_count', 'bigint:20'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name    = '__all_virtual_sql_audit',
  table_id      = '11031',
  table_type = 'VIRTUAL_TABLE',
  index_using_type = 'USING_BTREE',
  gm_columns    = [],
  rowkey_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('tenant_id', 'int'),
    ('request_id', 'int'),
  ],
  normal_columns = [
    ('trace_id', 'varchar:OB_MAX_HOST_NAME_LENGTH'),
    ('client_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('client_port', 'int'),
    ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH'),
    ('effective_tenant_id', 'int'),
    ('user_id', 'int'),
    ('user_name', 'varchar:OB_MAX_USER_NAME_LENGTH'),
    ('db_id', 'uint'),
    ('db_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
    ('sql_id', 'varchar:OB_MAX_SQL_ID_LENGTH'),
    ('query_sql', 'longtext'),
    ('plan_id', 'int'),
    ('affected_rows', 'int'),
    ('return_rows', 'int'),
    ('partition_cnt', 'int'),
    ('ret_code', 'int'),
    ('qc_id', 'uint'),
    ('dfo_id', 'int'),
    ('sqc_id', 'int'),
    ('worker_id', 'int'),

    ('event', 'varchar:OB_MAX_WAIT_EVENT_NAME_LENGTH', 'false'),
    ('p1text', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
    ('p1', 'uint', 'false'),
    ('p2text', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
    ('p2', 'uint', 'false'),
    ('p3text', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
    ('p3', 'uint', 'false'),
    ('level', 'int', 'false'),
    ('wait_class_id', 'int', 'false'),
    ('wait_class#', 'int', 'false'),
    ('wait_class', 'varchar:OB_MAX_WAIT_EVENT_PARAM_LENGTH', 'false'),
    ('state', 'varchar:19', 'false'),
    ('wait_time_micro', 'int', 'false'),
    ('total_wait_time_micro', 'int', 'false'),
    ('total_waits', 'int'),

    ('rpc_count', 'int'),
    ('plan_type', 'int'),

    ('is_inner_sql', 'bool'),
    ('is_executor_rpc', 'bool'),
    ('is_hit_plan', 'bool'),

    ('request_time', 'int'),
    ('elapsed_time', 'int'),
    ('net_time', 'int'),
    ('net_wait_time', 'int'),
    ('queue_time', 'int'),
    ('decode_time','int'),
    ('get_plan_time', 'int'),
    ('execute_time', 'int'),
    ('application_wait_time', 'uint'),
    ('concurrency_wait_time', 'uint'),
    ('user_io_wait_time', 'uint'),
    ('schedule_time', 'uint'),
    ('row_cache_hit', 'int'),
    ('bloom_filter_cache_hit', 'int'),
    ('block_cache_hit', 'int'),
    ('block_index_cache_hit', 'int'),
    ('disk_reads', 'int'),
    ('execution_id', 'int'),
    ('session_id', 'uint'),
    ('retry_cnt', 'int'),
    ('table_scan', 'bool'),
    ('consistency_level', 'int'),
    ('memstore_read_row_count', 'int'),
    ('ssstore_read_row_count', 'int'),
    ('request_memory_used', 'bigint'),
    ('expected_worker_count', 'int'),
    ('used_worker_count', 'int'),
    ('sched_info', 'varchar:16384', 'true'),
    ('fuse_row_cache_hit', 'int'),

    ('user_client_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('ps_stmt_id', 'int'),
    ('transaction_hash', 'uint'),
    ('request_type', 'int'),
    ('is_batched_multi_stmt', 'bool'),
    ('ob_trace_info', 'varchar:4096'),
    ('plan_hash', 'uint'),
    ('user_group', 'int', 'true'),
    ('lock_for_read_time', 'bigint'),
    ('wait_trx_migrate_time', 'bigint')
  ],
  partition_columns = ['svr_ip', 'svr_port'],
  index = {'all_virtual_sql_audit_i1' :  { 'index_columns' : ['tenant_id', 'request_id'],
                     'index_using_type' : 'USING_BTREE'}},
)

def_table_schema(
  table_name     = '__all_virtual_trans_mem_stat',
  table_id       = '11032',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('type', 'varchar:64'),
    ('alloc_count', 'bigint'),
    ('release_count', 'bigint'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)



def_table_schema(
  table_name     = '__all_virtual_partition_sstable_image_info',
  table_id       = '11033',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('zone', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('major_version', 'int'),
  ('min_version', 'int'),
  ('ss_store_count', 'int'),
  ('merged_ss_store_count', 'int'),
  ('modified_ss_store_count', 'int'),
  ('macro_block_count', 'int'),
  ('use_old_macro_block_count', 'int'),
  ('merge_start_time', 'timestamp'),
  ('merge_finish_time', 'timestamp'),
  ('merge_process', 'int'),
  ('rewrite_macro_old_micro_block_count', 'int'),
  ('rewrite_macro_total_micro_block_count', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(**gen_core_inner_table_def(11034, '__all_virtual_core_root_table', 'VIRTUAL_TABLE', all_root_def))

def_table_schema(**gen_core_inner_table_def(11035, '__all_virtual_core_all_table', 'VIRTUAL_TABLE', all_table_def))

def_table_schema(**gen_core_inner_table_def(11036, '__all_virtual_core_column_table', 'VIRTUAL_TABLE', all_column_def))


def_table_schema(
  table_name     = '__all_virtual_memory_info',
  table_id       = '11037',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ('tenant_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('ctx_id', 'int'),
  ('label', 'varchar:OB_MAX_CHAR_LENGTH'),
  ],

  normal_columns = [
  ('ctx_name', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('mod_type', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('mod_id', 'int'),
  ('mod_name', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('zone', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('hold', 'int'),
  ('used', 'int'),
  ('count', 'int'),
  ('alloc_count', 'int'),
  ('free_count', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_tenant_stat',
  table_id       = '11038',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  only_rs_vtable = True,

  normal_columns = [
  ('tenant_id', 'int'),
  ('table_count', 'int'),
  ('row_count', 'int'),
  ('total_size', 'int'),
  ],
)

def_table_schema(
    table_name     = '__all_virtual_sys_parameter_stat',
    table_id       = '11039',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
    ],

  normal_columns = [
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('svr_type', 'varchar:SERVER_TYPE_LENGTH'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('name', 'varchar:OB_MAX_CONFIG_NAME_LEN'),
      ('data_type', 'varchar:OB_MAX_CONFIG_TYPE_LENGTH', 'true'),
      ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN'),
      ('value_strict', 'varchar:OB_MAX_EXTRA_CONFIG_LENGTH', 'true'),
      ('info', 'varchar:OB_MAX_CONFIG_INFO_LEN'),
      ('need_reboot', 'int'),
      ('section', 'varchar:OB_MAX_CONFIG_SECTION_LEN'),
      ('visible_level', 'varchar:OB_MAX_CONFIG_VISIBLE_LEVEL_LEN'),
      ('scope', 'varchar:OB_MAX_CONFIG_SCOPE_LEN'),
      ('source', 'varchar:OB_MAX_CONFIG_SOURCE_LEN'),
      ('edit_level', 'varchar:OB_MAX_CONFIG_EDIT_LEVEL_LEN'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name       = '__all_virtual_partition_replay_status',
  table_id     = '11040',
  table_type   = 'VIRTUAL_TABLE',
  gm_columns       = [],
  rowkey_columns   = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('tenant_id', 'int'),
  ('table_id', 'int'),
  ('partition_idx', 'int'),
  ('partition_cnt','int'),
  ('pending_task_count', 'int'),
  ('retried_task_count', 'int'),
  ('post_barrier_status', 'varchar:MAX_FREEZE_SUBMIT_STATUS_LENGTH'),
  ('is_enabled', 'int'),
  ('max_confirmed_log_id','uint'),
  ('last_replay_log_id','uint'),
  ('last_replay_log_type','varchar:MAX_REPLAY_LOG_TYPE_LENGTH'),
  ('total_submmited_task_count', 'int'),
  ('total_replayed_task_count', 'int'),
  ('next_submit_log_id','uint'),
  ('next_submit_log_ts','int'),
  ('last_slide_out_log_id','uint'),
  ('last_slide_out_log_ts','int'),
  ],

  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name = '__all_virtual_clog_stat',
  table_id = '11041',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('table_id', 'int'),
  ('partition_idx', 'int'),
  ('partition_cnt','int'),
  ('role', 'varchar:32'),
  ('status', 'varchar:32'),
  ('leader', 'varchar:32'),
  ('last_index_log_id', 'uint'),
  ('last_index_log_timestamp', 'timestamp'),
  ('last_log_id', 'uint'),
  ('active_freeze_version', 'varchar:32'),
  ('curr_member_list', 'varchar:1024'),
  ('member_ship_log_id', 'uint'),
  ('is_offline', 'bool'),
  ('is_in_sync', 'bool'),
  ('start_id', 'uint'),
  ('parent', 'varchar:32'),
  ('children_list', 'varchar:1024'),
  ('accu_log_count', 'uint'),
  ('accu_log_delay', 'uint'),
  ('replica_type', 'int', 'false', '0'),
  ('allow_gc', 'bool'),
  ('quorum', 'int'),
  ('is_need_rebuild', 'bool'),
  ('next_replay_ts_delta', 'uint'),
  ],

  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_trace_log',
  table_id       = '11042',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('title', 'varchar:256'),
  ('key_value', 'varchar:1024'),
  ('time', 'varchar:10'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_engine',
  table_id       = '11043',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('Engine', 'varchar:MAX_ENGINE_LENGTH'),
  ('Support', 'varchar:MAX_BOOL_STR_LENGTH'),
  ('Comment', 'varchar:MAX_COLUMN_COMMENT_LENGTH'),
  ('Transactions', 'varchar:MAX_BOOL_STR_LENGTH'),
  ('XA', 'varchar:MAX_BOOL_STR_LENGTH'),
  ('Savepoints', 'varchar:MAX_BOOL_STR_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_proxy_server_stat',
  table_id       = '11045',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('start_service_time', 'int'),
      ('stop_time', 'int'),
      ('status', 'varchar:OB_SERVER_STATUS_LENGTH'),
  ],
)

def_table_schema(
    table_name     = '__all_virtual_proxy_sys_variable',
    table_id       = '11046',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
    ],

  normal_columns = [
      ('name', 'varchar:OB_MAX_CONFIG_NAME_LEN'),
      ('tenant_id', 'int'),
      ('data_type', 'int'),
      ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN'),
      ('flags', 'int'),
      ('modified_time','int'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_proxy_schema',
  table_id       = '11047',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
    ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE'),
    ('database_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
    ('table_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH'),
    ('partition_id', 'int'),
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('sql_port', 'int'),
    ],

  normal_columns = [
  ('table_id', 'int'),
  ('role', 'int'),
  ('part_num', 'int'),
  ('replica_num', 'int'),
  ('table_type', 'int'),
  ('schema_version', 'int'),
  ('spare1', 'int'),
  ('spare2', 'int'),
  ('spare3', 'int'),
  ('spare4', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
  ('spare5', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
  ('spare6', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
  ('complex_table_type', 'int'),
  ('level1_decoded_db_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
  ('level1_decoded_table_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH'),
  ('level2_decoded_db_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
  ('level2_decoded_table_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH'),
  ]
)

def_table_schema(
  table_name     = '__all_virtual_plan_cache_plan_explain',
  table_id       = '11048',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('plan_id', 'int'),
  ],

  normal_columns = [
    ('operator', 'varchar:OB_MAX_OPERATOR_NAME_LENGTH'),
    ('name', 'varchar:OB_MAX_PLAN_EXPLAIN_NAME_LENGTH'),
    ('rows', 'int'),
    ('cost', 'int'),
    ('property', 'varchar:OB_MAX_OPERATOR_PROPERTY_LENGTH'),
    ('plan_depth', 'int'),
    ('plan_line_id', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name     = '__all_virtual_obrpc_stat',
    table_id       = '11049',
    table_type = 'VIRTUAL_TABLE',
    gm_columns     = [],
    rowkey_columns = [
    ],

  normal_columns = [
      ('tenant_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('dest_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('dest_port', 'int'),
      ('index', 'int'),
      ('zone', 'varchar:OB_MAX_CHAR_LENGTH'),
      ('pcode', 'int'),
      ('pcode_name', 'varchar:OB_MAX_CHAR_LENGTH'),
      ('count', 'int'),
      ('total_time', 'int'),
      ('total_size', 'int'),
      ('max_time', 'int'),
      ('min_time', 'int'),
      ('max_size', 'int'),
      ('min_size', 'int'),
      ('failure', 'int'),
      ('timeout', 'int'),
      ('sync', 'int'),
      ('async', 'int'),
      ('last_timestamp', 'timestamp'),
      ('isize', 'int'),
      ('icount', 'int'),
      ('net_time', 'int'),
      ('wait_time', 'int'),
      ('queue_time', 'int'),
      ('process_time', 'int'),
      ('ilast_timestamp', 'timestamp'),
  ],
    partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name     = '__all_virtual_partition_sstable_merge_info',
    table_id       = '11051',
    table_type     = 'VIRTUAL_TABLE',
    gm_columns     = [],
    rowkey_columns = [
      ],

    normal_columns = [
      ('version', 'varchar:OB_SYS_TASK_TYPE_LENGTH'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
      ('merge_type', 'varchar:OB_SYS_TASK_TYPE_LENGTH'),
      ('snapshot_version', 'int'),
      ('table_type', 'int'),
      ('major_table_id', 'int'),
      ('merge_start_time', 'int'),
      ('merge_finish_time', 'int'),
      ('merge_cost_time', 'int'),
      ('estimate_cost_time', 'int'),
      ('occupy_size', 'int'),
      ('macro_block_count', 'int'),
      ('use_old_macro_block_count', 'int'),
      ('build_bloomfilter_count', 'int'),
      ('total_row_count', 'int'),
      ('delete_row_count', 'int'),
      ('insert_row_count', 'int'),
      ('update_row_count', 'int'),
      ('base_row_count', 'int'),
      ('use_base_row_count', 'int'),
      ('memtable_row_count', 'int'),
      ('purged_row_count', 'int'),
      ('output_row_count', 'int'),
      ('merge_level', 'int'),
      ('rewrite_macro_old_micro_block_count', 'int'),
      ('rewrite_macro_total_micro_block_count', 'int'),
      ('total_child_task', 'int'),
      ('finish_child_task', 'int'),
      ('step_merge_percentage', 'int'),
      ('merge_percentage', 'int'),
      ('error_code', 'int'),
      ('table_count', 'int'),
      ],

    partition_columns = ['svr_ip', 'svr_port'],
  )

def_table_schema(
  table_name     = '__all_virtual_sql_monitor',
  table_id       = '11052',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
      ('tenant_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('request_id', 'int'),
      ('job_id', 'int'),
      ('task_id', 'int'),
      ],

    normal_columns = [
      ('plan_id', 'int'),
      ('scheduler_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('scheduler_port', 'int'),
      ('monitor_info', 'varchar:OB_MAX_MONITOR_INFO_LENGTH'),
      ('extend_info', 'varchar:OB_MAX_MONITOR_INFO_LENGTH'),
      ('sql_exec_start', 'timestamp'),
   ],
    partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__tenant_virtual_outline',
  table_id       = '11053',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],
  in_tenant_space = True,

  normal_columns = [
      ('tenant_id', 'int'),
      ('database_id', 'int'),
      ('outline_id', 'int'),
      ('database_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
      ('outline_name', 'varchar:OB_MAX_OUTLINE_NAME_LENGTH', 'false', ''),
      ('visible_signature', 'longtext', 'false'),
      ('sql_text', 'longtext', 'false'),
      ('outline_target', 'longtext', 'false'),
      ('outline_sql', 'longtext', 'false'),
  ],
)

def_table_schema(
  table_name     = '__tenant_virtual_concurrent_limit_sql',
  table_id       = '11054',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
      ('tenant_id', 'int'),
      ('database_id', 'int'),
      ('outline_id', 'int'),
      ('database_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
      ('outline_name', 'varchar:OB_MAX_OUTLINE_NAME_LENGTH', 'false', ''),
      ('outline_content', 'longtext', 'false'),
      ('visible_signature', 'longtext', 'false'),
      ('sql_text', 'longtext', 'false'),
      ('concurrent_num', 'int', 'false', '-1'),
      ('limit_target', 'longtext', 'false'),
  ],
)



def_table_schema(
  table_name     = '__all_virtual_sql_plan_statistics',
  table_id       = '11055',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
      ('tenant_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('plan_id', 'int'),
      ('operation_id', 'int'),
      ],

    normal_columns = [
      ('executions', 'int'),
      ('output_rows', 'int'),
      ('input_rows', 'int'),
      ('rescan_times', 'int'),
      ('buffer_gets', 'int'),
      ('disk_reads', 'int'),
      ('disk_writes', 'int'),
      ('elapsed_time', 'int'),
      ('extend_info1', 'varchar:OB_MAX_MONITOR_INFO_LENGTH'),
      ('extend_info2', 'varchar:OB_MAX_MONITOR_INFO_LENGTH'),
   ],
    partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_partition_sstable_macro_info',
  table_id       = '11056',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
      ('data_version', 'int'),
      ('base_version', 'int'),
      ('multi_version_start', 'int'),
      ('snapshot_version', 'int'),
      ('macro_idx_in_sstable', 'int'),
      ],

    normal_columns = [
      ('major_table_id', 'int'),
      ('macro_data_version', 'int'),
      ('macro_idx_in_data_file', 'int'),
      ('data_seq', 'int'),
      ('row_count', 'int'),
      ('occupy_size', 'int'),
      ('micro_block_count', 'int'),
      ('data_checksum', 'int'),
      ('schema_version', 'int'),
      ('macro_range', 'varchar:OB_MAX_RANGE_LENGTH'),
      ('row_count_delta', 'int'),
      ('macro_block_type', 'varchar:MAX_VALUE_LENGTH'),
      ('compressor_name', 'varchar:OB_MAX_COMPRESSOR_NAME_LENGTH'),
   ],
    partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_proxy_partition_info',
  table_id       = '11057',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
    ('table_id', 'int'),
  ],

  normal_columns = [
    ('tenant_id', 'int'),

    ('part_level', 'int'),
    ('all_part_num', 'int'),
    ('template_num', 'int'),
    ('part_id_rule_ver', 'int'),

    ('part_type', 'int'),
    ('part_num', 'int'),
    ('is_column_type', 'bool'),
    ('part_space', 'int'),
    ('part_expr', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('part_expr_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),
    ('part_range_type', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),
    ('part_interval', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('part_interval_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),
    ('interval_start', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('interval_start_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),

    ('sub_part_type', 'int'),
    ('sub_part_num', 'int'),
    ('is_sub_column_type', 'bool'),
    ('sub_part_space', 'int'),
    ('sub_part_expr', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('sub_part_expr_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),
    ('sub_part_range_type', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),
    ('def_sub_part_interval', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('def_sub_part_interval_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),
    ('def_sub_interval_start', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('def_sub_interval_start_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),

    ('part_key_num', 'int'),
    ('part_key_name', 'varchar:OB_MAX_COLUMN_NAME_LENGTH'),
    ('part_key_type', 'int'),
    ('part_key_idx', 'int'),
    ('part_key_level', 'int'),
    ('part_key_extra', 'varchar:COLUMN_EXTRA_LENGTH'),

    ('spare1', 'int'),
    ('spare2', 'int'),
    ('spare3', 'int'),
    ('spare4', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('spare5', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('spare6', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_proxy_partition',
  table_id       = '11058',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
    ('table_id', 'int'),
    ('part_id', 'int'),
  ],

  normal_columns = [
    ('tenant_id', 'int'),
    ('part_name', 'varchar:OB_MAX_PARTITION_NAME_LENGTH'),
    ('status', 'int'),
    ('low_bound_val', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('low_bound_val_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),
    ('high_bound_val', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('high_bound_val_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),
    ('part_idx', 'int'),


    ('sub_part_num', 'int'),
    ('sub_part_space', 'int'),
    ('sub_part_interval', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('sub_part_interval_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),
    ('sub_interval_start', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('sub_interval_start_bin', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),

    ('spare1', 'int'),
    ('spare2', 'int'),
    ('spare3', 'int'),
    ('spare4', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('spare5', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('spare6', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
  ],
)

def_table_schema(
  table_name    = '__all_virtual_proxy_sub_partition',
  table_id      = '11059',
  table_type    = 'VIRTUAL_TABLE',
  gm_columns    = [],
  rowkey_columns = [
    ('table_id', 'int'),
    ('part_id', 'int'),
    ('sub_part_id', 'int'),
  ],

  normal_columns = [
    ('tenant_id', 'int'),
    ('part_name', 'varchar:OB_MAX_PARTITION_NAME_LENGTH'),
    ('status', 'int'),
    ('low_bound_val', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('low_bound_val_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),
    ('high_bound_val', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('high_bound_val_bin', 'varchar:OB_MAX_B_PARTITION_EXPR_LENGTH'),

    ('spare1', 'int'),
    ('spare2', 'int'),
    ('spare3', 'int'),
    ('spare4', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('spare5', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
    ('spare6', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_proxy_route',
  table_id       = '11060',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
    ('sql_string', 'varchar:OB_MAX_PROXY_SQL_STORE_LENGTH'),
    ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE'),
    ('database_name', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
  ],

  normal_columns = [
    ('table_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH'),
    ('table_id', 'int'),
    ('calculator_bin', 'varchar:OB_MAX_CALCULATOR_SERIALIZE_LENGTH'),
    ('result_status', 'int'),

    ('spare1', 'int'),
    ('spare2', 'int'),
    ('spare3', 'int'),
    ('spare4', 'varchar:OB_MAX_CALCULATOR_SERIALIZE_LENGTH'),
    ('spare5', 'varchar:OB_MAX_CALCULATOR_SERIALIZE_LENGTH'),
    ('spare6', 'varchar:OB_MAX_CALCULATOR_SERIALIZE_LENGTH'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_rebalance_tenant_stat',
  table_id       = '11061',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  only_rs_vtable = True,
  normal_columns = [
      ('tenant_id', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('cpu_weight', 'double'),
      ('disk_weight', 'double'),
      ('iops_weight', 'double'),
      ('memory_weight', 'double'),
      ('load_imbalance', 'double'),
      ('load_avg', 'double'),
      ('cpu_imbalance', 'double'),
      ('cpu_avg', 'double'),
      ('disk_imbalance', 'double'),
      ('disk_avg', 'double'),
      ('iops_imbalance', 'double'),
      ('iops_avg', 'double'),
      ('memory_imbalance', 'double'),
      ('memory_avg', 'double'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_rebalance_unit_stat',
  table_id       = '11062',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  only_rs_vtable = True,
  normal_columns = [
      ('tenant_id', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('unit_id', 'int'),
      ('load', 'double'),
      ('cpu_usage_rate', 'double'),
      ('disk_usage_rate', 'double'),
      ('iops_usage_rate', 'double'),
      ('memory_usage_rate', 'double'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_rebalance_replica_stat',
  table_id       = '11063',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  only_rs_vtable = True,
  normal_columns = [
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('cpu_usage', 'double'),
      ('disk_usage', 'double'),
      ('iops_usage', 'double'),
      ('memory_usage', 'double'),
      ('net_packet_usage', 'double'),
      ('net_throughput_usage', 'double'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('unit_id', 'int'),

  ],
)

def_table_schema(
    table_name     = '__all_virtual_partition_amplification_stat',
    table_id       = '11064',
    table_type     = 'VIRTUAL_TABLE',
    gm_columns     = [],
    rowkey_columns = [
    ],

    normal_columns = [
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
        ('tenant_id', 'int'),
        ('table_id', 'int'),
        ('partition_idx', 'int'),
        ('partition_cnt', 'int'),
        ('dirty_ratio_1', 'int'),
        ('dirty_ratio_3', 'int'),
        ('dirty_ratio_5', 'int'),
        ('dirty_ratio_10', 'int'),
        ('dirty_ratio_15', 'int'),
        ('dirty_ratio_20', 'int'),
        ('dirty_ratio_30', 'int'),
        ('dirty_ratio_50', 'int'),
        ('dirty_ratio_75', 'int'),
        ('dirty_ratio_100', 'int'),
        ('macro_block_cnt', 'int'),
    ],
    partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_election_event_history',
  table_id       = '11067',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
    ('gmt_create', 'timestamp', 'false'),
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('table_id', 'bigint:20'),
    ('partition_idx', 'bigint:20'),
    ('event', 'varchar:MAX_ELECTION_EVENT_DESC_LENGTH', 'false'),
    ('leader', 'varchar:OB_IP_PORT_STR_BUFF', 'false', '0.0.0.0'),
    ('info', 'varchar:MAX_ELECTION_EVENT_EXTRA_INFO_LENGTH', 'false'),
  ],
  partition_expr = ['hash', 'addr_to_partition_id(svr_ip, svr_port)', '65536'],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_partition_store_info',
  table_id       = '11068',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('tenant_id', 'int'),
  ('table_id', 'int'),
  ('partition_idx', 'int'),
  ('partition_cnt', 'int'),
  ('is_restore', 'int'),
  ('migrate_status', 'int'),
  ('migrate_timestamp', 'int'),
  ('replica_type', 'int'),
  ('split_state', 'int'),
  ('multi_version_start', 'int'),
  ('report_version', 'int'),
  ('report_row_count', 'int'),
  ('report_data_checksum', 'int'),
  ('report_data_size', 'int'),
  ('report_required_size', 'int'),
  ('readable_ts', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_leader_stat',
  table_id       = '11069',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  only_rs_vtable = True,
  normal_columns = [
      ('tenant_id', 'int'),
      ('tablegroup_id', 'int'),
      ('partition_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('primary_zone', 'varchar:MAX_ZONE_LENGTH'),
      ('region', 'varchar:MAX_REGION_LENGTH'),
      ('region_score', 'int'),
      ('not_merging', 'int'),
      ('candidate_count', 'int'),
      ('is_candidate', 'int'),
      ('migrate_out_or_transform_count', 'int'),
      ('in_normal_unit_count', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('zone_score', 'int'),
      ('original_leader_count', 'int'),
      ('random_score', 'int'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_partition_migration_status',
  table_id       = '11070',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('task_id', 'varchar:OB_TRACE_STAT_BUFFER_SIZE'),
  ('tenant_id', 'int'),
  ('table_id', 'int'),
  ('partition_idx', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('migrate_type', 'varchar:MAX_VALUE_LENGTH'),
  ('parent_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('parent_port', 'int'),
  ('src_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('src_port', 'int'),
  ('dest_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('dest_port', 'int'),
  ('result', 'int'),
  ('start_time', 'timestamp'),
  ('finish_time', 'timestamp'),
  ('action', 'varchar:OB_MIGRATE_ACTION_LENGTH'),
  ('replica_state', 'varchar:OB_MIGRATE_REPLICA_STATE_LENGTH'),
  ('rebuild_count', 'int'),
  ('total_macro_block', 'int'),
  ('ready_macro_block', 'int'),
  ('major_count', 'int'),
  ('mini_minor_count', 'int'),
  ('normal_minor_count', 'int'),
  ('buf_minor_count', 'int'),
  ('reuse_count', 'int'),
  ('comment', 'varchar:OB_MAX_TASK_COMMENT_LENGTH', 'false', ''),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_sys_task_status',
  table_id       = '11071',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('start_time', 'timestamp'),
  ('task_type', 'varchar:OB_SYS_TASK_TYPE_LENGTH'),
  ('task_id', 'varchar:OB_TRACE_STAT_BUFFER_SIZE'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('tenant_id', 'int'),
  ('comment', 'varchar:OB_MAX_TASK_COMMENT_LENGTH', 'false', ''),
  ('is_cancel', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_macro_block_marker_status',
  table_id       = '11072',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ],

  normal_columns = [
  ('total_count', 'int'),
  ('reserved_count', 'int'),
  ('macro_meta_count', 'int'),
  ('partition_meta_count', 'int'),
  ('data_count', 'int'),
  ('second_index_count', 'int'),
  ('lob_data_count', 'int'),
  ('lob_second_index_count', 'int'),
  ('bloomfilter_count', 'int'),
  ('hold_count', 'int'),
  ('pending_free_count', 'int'),
  ('free_count', 'int'),
  ('mark_cost_time', 'int'),
  ('sweep_cost_time', 'int'),
  ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH', 'false', ''),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name     = '__all_virtual_server_clog_stat',
  table_id       = '11073',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int', 'false'),
  ],

  normal_columns = [
  ('system_clog_min_using_file_id', 'int'),
  ('user_clog_min_using_file_id', 'int'),
  ('system_ilog_min_using_file_id', 'int'),
  ('user_ilog_min_using_file_id', 'int'),
  ('zone', 'varchar:MAX_ZONE_LENGTH'),
  ('region', 'varchar:MAX_REGION_LENGTH'),
  ('idc', 'varchar:MAX_ZONE_INFO_LENGTH'),
  ('zone_type', 'varchar:MAX_ZONE_INFO_LENGTH'),
  ('merge_status', 'varchar:MAX_ZONE_INFO_LENGTH'),
  ('zone_status', 'varchar:MAX_ZONE_INFO_LENGTH'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

# create table __all_virtual_rootservice_stat (`statistic#` bigint, value bigint, stat_id bigint, name varchar(64), `class` bigint, can_visible boolean, primary key(`statistic#`));
def_table_schema(
  table_name     = '__all_virtual_rootservice_stat',
  table_id       = '11074',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  only_rs_vtable = True,

  normal_columns = [
  ('statistic#', 'int', 'false'),
  ('value', 'int', 'false'),
  ('stat_id', 'int', 'false'),
  ('name', 'varchar:64', 'false'),
  ('class', 'int', 'false'),
  ('can_visible', 'bool', 'false'),
  ],
)

def_table_schema(
  table_name = '__all_virtual_election_priority',
  table_id = '11075',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('table_id', 'int'),
  ('partition_idx', 'int'),
  ('partition_cnt','int'),
  ('role', 'int'),
  ('is_candidate', 'bool'),
  ('membership_version', 'int'),
  ('log_id', 'uint'),
  ('locality', 'uint'),
  ('system_score', 'int'),
  ('is_tenant_active', 'bool'),
  ('on_revoke_blacklist', 'bool'),
  ('on_loop_blacklist', 'bool'),
  ('replica_type', 'int'),
  ('server_status', 'int'),
  ('is_clog_disk_full', 'bool'),
  ('is_offline', 'bool'),
  ('is_need_rebuild', 'bool'),
  ('is_partition_candidate', 'bool'),
  ('is_disk_error', 'bool'),
  ('memstore_percent', 'int'),
  ],

  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name = '__all_virtual_tenant_disk_stat',
  table_id = '11076',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('zone', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('block_type', 'varchar:OB_MAX_CHAR_LENGTH'),
  ('block_size', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_rebalance_map_stat',
  table_id       = '11078',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [],
  only_rs_vtable = True,
  normal_columns = [
      ('tenant_id', 'int'),
      ('map_type', 'int'),
      ('is_valid', 'bool'),
      ('row_size', 'int'),
      ('col_size', 'int'),
      ('tables', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH'),
  ],
)


def_table_schema(
  table_name     = '__all_virtual_rebalance_map_item_stat',
  table_id       = '11079',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [],
  only_rs_vtable = True,
  normal_columns = [
      ('tenant_id', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('tablegroup_id', 'int'),
      ('table_id', 'int'),
      ('map_type', 'int'),
      ('row_size', 'int'),
      ('col_size', 'int'),
      ('part_idx', 'int'),
      ('designated_role', 'int'),
      ('unit_id', 'int'),
      ('dest_unit_id', 'int'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_io_stat',
  table_id       = '11080',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
      ],

  normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
      ('svr_port', 'int', 'false'),
      ('fd', 'int', 'false'),
      ('disk_type', 'varchar:OB_MAX_DISK_TYPE_LENGTH'),
      ('sys_io_up_limit_in_mb', 'int'),
      ('sys_io_bandwidth_in_mb', 'int'),
      ('sys_io_low_watermark_in_mb', 'int'),
      ('sys_io_high_watermark_in_mb', 'int'),
      ('io_bench_result', 'varchar:OB_MAX_IO_BENCH_RESULT_LENGTH'),
      ],

  partition_columns = ['svr_ip', 'svr_port'],
)


def_table_schema(
    table_name     = '__all_virtual_long_ops_status',
    table_id       = '11081',
    table_type     = 'VIRTUAL_TABLE',
    gm_columns     = [],
    rowkey_columns = [
      ],

    normal_columns = [
      ('tenant_id', 'int'),
      ('sid', 'int'),
      ('op_name', 'varchar:MAX_LONG_OPS_NAME_LENGTH'),
      ('target', 'varchar:MAX_LONG_OPS_TARGET_LENGTH'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('start_time', 'int'),
      ('finish_time', 'int'),
      ('elapsed_time', 'int'),
      ('remaining_time', 'int'),
      ('last_update_time', 'int'),
      ('percentage', 'int'),
      ('message', 'varchar:MAX_LONG_OPS_MESSAGE_LENGTH'),
      ],

    partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_rebalance_unit_migrate_stat',
  table_id       = '11082',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [],
  only_rs_vtable = True,
  normal_columns = [
      ('unit_id', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('src_svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('src_svr_port', 'int'),
      ('dst_svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('dst_svr_port', 'int'),
  ],
)


def_table_schema(
  table_name     = '__all_virtual_rebalance_unit_distribution_stat',
  table_id       = '11083',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [],
  only_rs_vtable = True,
  normal_columns = [
      ('unit_id', 'int'),
      ('tenant_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('max_cpu', 'double'),
      ('min_cpu', 'double'),
      ('max_memory', 'int'),
      ('min_memory', 'int'),
      ('max_iops', 'int'),
      ('min_iops', 'int'),
      ('max_disk_size', 'int'),
      ('max_session_num', 'int'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_server_object_pool',
  table_id       = '11084',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('object_type', 'varchar:OB_MAX_SYS_PARAM_VALUE_LENGTH'),
      ('arena_id', 'int'),
  ],

  normal_columns = [
      ('lock', 'int'),
      ('borrow_count', 'int'),
      ('return_count', 'int'),
      ('miss_count', 'int'),
      ('miss_return_count', 'int'),
      ('free_num', 'int'),
      ('last_borrow_ts', 'int'),
      ('last_return_ts', 'int'),
      ('last_miss_ts', 'int'),
      ('last_miss_return_ts', 'int'),
      ('next', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_trans_lock_stat',
  table_id       = '11085',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int'),
  ('trans_id', 'varchar:512'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('partition', 'varchar:64'),
  ('table_id', 'int'),
  ('rowkey', 'varchar:512'),
  ('session_id', 'int'),
  ('proxy_id', 'varchar:512'),
  ('ctx_create_time', 'timestamp', 'true'),
  ('expired_time', 'timestamp', 'true'),
  ('row_lock_addr', 'uint', 'true'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_election_group_info',
  table_id       = '11086',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('eg_id_hash', 'uint'),
  ('is_running', 'bool'),
  ('eg_create_time', 'bigint:20'),
  ('eg_version', 'bigint:20'),
  ('eg_part_cnt', 'bigint:20'),
  ('is_all_part_merged_in', 'bool'),
  ('is_priority_allow_reappoint', 'bool'),
  ('tenant_id', 'int'),
  ('is_candidate', 'bool'),
  ('system_score', 'bigint:20'),
  ('current_leader', 'varchar:64'),
  ('member_list', 'varchar:MAX_MEMBER_LIST_LENGTH'),
  ('replica_num', 'bigint:20'),
  ('takeover_t1_timestamp_', 'bigint:20'),
  ('T1_timestamp', 'bigint:20'),
  ('lease_start', 'bigint:20'),
  ('lease_end', 'bigint:20'),
  ('role', 'bigint:20'),
  ('state', 'bigint:20'),
  ('pre_destroy_state', 'bool'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__tenant_virtual_show_create_tablegroup',
  table_id       = '11087',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('tablegroup_id', 'int'),
  ],
  in_tenant_space = True,

  normal_columns = [
  ('tablegroup_name', 'varchar:OB_MAX_TABLEGROUP_NAME_LENGTH'),
  ('create_tablegroup', 'varchar:TABLEGROUP_DEFINE_LENGTH'),
  ],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name     = '__all_virtual_server_blacklist',
  table_id       = '11088',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('svr_port', 'int', 'false'),
  ('dst_ip', 'varchar:MAX_IP_ADDR_LENGTH', 'false'),
  ('dst_port', 'int', 'false'),
  ('is_in_blacklist', 'bool'),
  ('is_clockdiff_error', 'bool'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_partition_split_info',
  table_id       = '11089',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('zone', 'varchar:MAX_ZONE_LENGTH'),
  ('table_id', 'int'),
  ('partition_id', 'int'),
  ('split_state', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH'),
  ('merge_version', 'int')],

  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_trans_result_info_stat',
  table_id       = '11090',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('trans_id', 'varchar:512'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('partition', 'varchar:64'),
  ('state', 'bigint:20'),
  ('commit_version', 'bigint:20'),
  ('min_log_id', 'bigint:20'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_duplicate_partition_mgr_stat',
  table_id       = '11091',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('table_id', 'int'),
  ('partition_id', 'int'),
  ('partition_lease_list', 'varchar:OB_OLD_MAX_VARCHAR_LENGTH'),
  ('is_master', 'int'),
  ('cur_log_id', 'bigint:20')],

  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name     = '__all_virtual_tenant_parameter_stat',
    table_id       = '11092',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
    ],
    in_tenant_space = True,
    enable_column_def_enum = True,

  normal_columns = [
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('svr_type', 'varchar:SERVER_TYPE_LENGTH'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('name', 'varchar:OB_MAX_CONFIG_NAME_LEN'),
      ('data_type', 'varchar:OB_MAX_CONFIG_TYPE_LENGTH', 'true'),
      ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN'),
      ('info', 'varchar:OB_MAX_CONFIG_INFO_LEN'),
      ('section', 'varchar:OB_MAX_CONFIG_SECTION_LEN'),
      ('scope', 'varchar:OB_MAX_CONFIG_SCOPE_LEN'),
      ('source', 'varchar:OB_MAX_CONFIG_SOURCE_LEN'),
      ('edit_level', 'varchar:OB_MAX_CONFIG_EDIT_LEVEL_LEN'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name = '__all_virtual_server_schema_info',
  table_id = '11093',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('tenant_id', 'int'),
    ("refreshed_schema_version", 'int'),
    ("received_schema_version", 'int'),
    ("schema_count", 'int'),
    ("schema_size", 'int'),
    ("min_sstable_schema_version", 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_memory_context_stat',
  table_id       = '11094',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('entity', 'varchar:128'),
    ('p_entity', 'varchar:128'),
    ('hold', 'bigint:20'),
    ('malloc_hold', 'bigint:20'),
    ('malloc_used', 'bigint:20'),
    ('arena_hold', 'bigint:20'),
    ('arena_used', 'bigint:20'),
    ('create_time', 'timestamp'),
    ('location', 'varchar:512'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_dump_tenant_info',
  table_id       = '11095',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('tenant_id', 'bigint:20'),
  ],

  normal_columns = [
    ('compat_mode', 'bigint:20'),
    ('unit_min_cpu', 'double'),
    ('unit_max_cpu', 'double'),
    ('slice', 'double'),
    ('remain_slice', 'double'),
    ('token_cnt', 'bigint:20'),
    ('ass_token_cnt', 'bigint:20'),
    ('lq_tokens', 'bigint:20'),
    ('used_lq_tokens', 'bigint:20'),
    ('stopped', 'bigint:20'),
    ('idle_us', 'bigint:20'),
    ('recv_hp_rpc_cnt', 'bigint:20'),
    ('recv_np_rpc_cnt', 'bigint:20'),
    ('recv_lp_rpc_cnt', 'bigint:20'),
    ('recv_mysql_cnt', 'bigint:20'),
    ('recv_task_cnt', 'bigint:20'),
    ('recv_large_req_cnt', 'bigint:20'),
    ('recv_large_queries', 'bigint:20'),
    ('actives', 'bigint:20'),
    ('workers', 'bigint:20'),
    ('lq_waiting_workers', 'bigint:20'),
    ('req_queue_total_size', 'bigint:20'),
    ('queue_0', 'bigint:20'),
    ('queue_1', 'bigint:20'),
    ('queue_2', 'bigint:20'),
    ('queue_3', 'bigint:20'),
    ('queue_4', 'bigint:20'),
    ('queue_5', 'bigint:20'),
    ('large_queued', 'bigint:20'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name     = '__all_virtual_tenant_parameter_info',
    table_id       = '11096',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
        ('tenant_id', 'int'),
        ('zone', 'varchar:MAX_ZONE_LENGTH'),
        ('svr_type', 'varchar:SERVER_TYPE_LENGTH'),
        ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
        ('svr_port', 'int'),
        ('name', 'varchar:OB_MAX_CONFIG_NAME_LEN'),
    ],
  normal_columns = [
      ('data_type', 'varchar:OB_MAX_CONFIG_TYPE_LENGTH', 'true'),
      ('value', 'varchar:OB_MAX_CONFIG_VALUE_LEN'),
      ('info', 'varchar:OB_MAX_CONFIG_INFO_LEN'),
      ('section', 'varchar:OB_MAX_CONFIG_SECTION_LEN'),
      ('scope', 'varchar:OB_MAX_CONFIG_SCOPE_LEN'),
      ('source', 'varchar:OB_MAX_CONFIG_SOURCE_LEN'),
      ('edit_level', 'varchar:OB_MAX_CONFIG_EDIT_LEVEL_LEN'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name     = '__all_virtual_dag_warning_history',
    table_id       = '11099',
    table_type     = 'VIRTUAL_TABLE',
    gm_columns     = [],
    rowkey_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('tenant_id', 'int'),
      ('task_id', 'varchar:OB_MAX_TRACE_ID_BUFFER_SIZE'),
      ],

    normal_columns = [
      ('module', 'varchar:OB_MODULE_NAME_LENGTH'),
      ('type', 'varchar:OB_SYS_TASK_TYPE_LENGTH'),
      ('ret', 'varchar:OB_RET_STR_LENGTH'),
      ('status', 'varchar:OB_STATUS_STR_LENGTH'),
      ('gmt_create', 'timestamp'),
      ('gmt_modified', 'timestamp'),
      ('warning_info', 'varchar:OB_DAG_WARNING_INFO_LENGTH'),
      ],

    partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name     = '__virtual_show_restore_preview',
    table_id       = '11102',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
      ('backup_type', 'varchar:20'),
      ('backup_id', 'int'),
      ('copy_id', 'int'),
    ],
  normal_columns = [
      ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
      ('file_status', 'varchar:OB_DEFAULT_STATUS_LENTH', 'false', ''),
  ],
)

def_table_schema(
  table_name    = '__all_virtual_master_key_version_info',
  table_id      = '11104',
  table_type = 'VIRTUAL_TABLE',
  gm_columns    = [],
  rowkey_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('tenant_id', 'int'),
  ],
  normal_columns = [
    ('max_active_version', 'int'),
    ('max_stored_version', 'int'),
    ('expect_version', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)


################################################################
################################################################
# INFORMATION SCHEMA
################################################################
def_table_schema(
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'SESSION_VARIABLES',
  table_id       = '12001',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('VARIABLE_NAME', 'varchar:OB_MAX_SYS_PARAM_NAME_LENGTH', 'false', ''),
  ('VARIABLE_VALUE', 'varchar:OB_MAX_SYS_PARAM_VALUE_LENGTH', 'true', 'NULL'),
  ],
)

def_table_schema(
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'TABLE_PRIVILEGES',
  table_id       = '12002',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('GRANTEE',        'varchar:OB_MAX_INFOSCHEMA_GRANTEE_LEN', 'false', ''),
  ('TABLE_CATALOG',  'varchar:MAX_TABLE_CATALOG_LENGTH', 'false', ''),
  ('TABLE_SCHEMA',   'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
  ('TABLE_NAME',     'varchar:OB_MAX_INFOSCHEMA_TABLE_NAME_LENGTH', 'false', ''),
  ('PRIVILEGE_TYPE', 'varchar:MAX_INFOSCHEMA_COLUMN_PRIVILEGE_LENGTH', 'false', ''),
  ('IS_GRANTABLE',   'varchar:MAX_COLUMN_YES_NO_LENGTH', 'false', ''),
  ],
)

def_table_schema(
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'USER_PRIVILEGES',
  table_id       = '12003',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('GRANTEE',        'varchar:OB_MAX_INFOSCHEMA_GRANTEE_LEN', 'false', ''),
  ('TABLE_CATALOG',  'varchar:MAX_TABLE_CATALOG_LENGTH', 'false', ''),
  ('PRIVILEGE_TYPE', 'varchar:MAX_INFOSCHEMA_COLUMN_PRIVILEGE_LENGTH', 'false', ''),
  ('IS_GRANTABLE',   'varchar:MAX_COLUMN_YES_NO_LENGTH', 'false', ''),
  ],
)

def_table_schema(
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'SCHEMA_PRIVILEGES',
  table_id       = '12004',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('GRANTEE',        'varchar:OB_MAX_INFOSCHEMA_GRANTEE_LEN', 'false', ''),
  ('TABLE_CATALOG',  'varchar:MAX_TABLE_CATALOG_LENGTH', 'false', ''),
  ('TABLE_SCHEMA',   'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
  ('PRIVILEGE_TYPE', 'varchar:MAX_INFOSCHEMA_COLUMN_PRIVILEGE_LENGTH', 'false', ''),
  ('IS_GRANTABLE',   'varchar:MAX_COLUMN_YES_NO_LENGTH', 'false', ''),
  ],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  database_id   = 'OB_INFORMATION_SCHEMA_ID',
  table_name    = 'TABLE_CONSTRAINTS',
  table_id      = '12005',
  table_type = 'VIRTUAL_TABLE',
  gm_columns    = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('CONSTRAINT_CATALOG', 'varchar:MAX_TABLE_CATALOG_LENGTH', 'false', ''),
  ('CONSTRAINT_SCHEMA', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
  ('CONSTRAINT_NAME', 'varchar:OB_MAX_COLUMN_NAME_LENGTH', 'false', ''),
  ('TABLE_SCHEMA', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
  ('TABLE_NAME', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'false', ''),
  ('CONSTRAINT_TYPE', 'varchar:INDEX_NULL_LENGTH', 'false', ''),
  ]
)

def_table_schema(
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'GLOBAL_STATUS',
  table_id       = '12006',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('VARIABLE_NAME', 'varchar:OB_MAX_SYS_PARAM_NAME_LENGTH', 'false', ''),
  ('VARIABLE_VALUE', 'varchar:OB_MAX_SYS_PARAM_VALUE_LENGTH', 'true', 'NULL'),
  ],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'PARTITIONS',
  table_id       = '12007 ',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('TABLE_CATALOG', 'varchar:MAX_TABLE_CATALOG_LENGTH', 'false', ''),
  ('TABLE_SCHEMA', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
  ('TABLE_NAME', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'false', ''),
  ('PARTITION_NAME', 'varchar:OB_MAX_PARTITION_NAME_LENGTH', 'true'),
  ('SUBPARTITION_NAME', 'varchar:OB_MAX_PARTITION_NAME_LENGTH', 'true'),
  ('PARTITION_ORDINAL_POSITION', 'uint', 'true'),
  ('SUBPARTITION_ORDINAL_POSITION', 'uint', 'true'),
  ('PARTITION_METHOD', 'varchar:OB_MAX_PARTITION_METHOD_LENGTH', 'true'),
  ('SUBPARTITION_METHOD', 'varchar:OB_MAX_PARTITION_METHOD_LENGTH', 'true'),
  ('PARTITION_EXPRESSION', 'varchar:OB_MAX_PART_FUNC_EXPR_LENGTH', 'true'),
  ('SUBPARTITION_EXPRESSION', 'varchar:OB_MAX_PART_FUNC_EXPR_LENGTH', 'true'),
  ('PARTITION_DESCRIPTION', 'varchar:OB_MAX_PARTITION_DESCRIPTION_LENGTH', 'true'),
  ('TABLE_ROWS', 'uint', 'false', '0'),
  ('AVG_ROW_LENGTH', 'uint', 'false', '0'),
  ('DATA_LENGTH', 'uint', 'false', '0'),
  ('MAX_DATA_LENGTH', 'uint', 'true'),
  ('INDEX_LENGTH', 'uint', 'false', '0'),
  ('DATA_FREE', 'uint', 'false', '0'),
  ('CREATE_TIME', 'timestamp', 'true'),
  ('UPDATE_TIME', 'timestamp', 'true'),
  ('CHECK_TIME', 'timestamp', 'true'),
  ('CHECKSUM', 'int', 'true'),
  ('PARTITION_COMMENT', 'varchar:OB_MAX_PARTITION_COMMENT_LENGTH', 'false', ''),
  ('NODEGROUP', 'varchar:OB_MAX_NODEGROUP_LENGTH', 'false', ''),
  ('TABLESPACE_NAME', 'varchar:OB_MAX_TABLEGROUP_NAME_LENGTH','true'),
  ],
)

def_table_schema(
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'SESSION_STATUS',
  tablegroup_id = 'OB_INVALID_ID',
  table_id       = '12008',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('VARIABLE_NAME', 'varchar:OB_MAX_SYS_PARAM_NAME_LENGTH', 'false', ''),
  ('VARIABLE_VALUE', 'varchar:OB_MAX_SYS_PARAM_VALUE_LENGTH', 'true', 'NULL'),
  ],
)

def_table_schema(
  database_id    = 'OB_MYSQL_SCHEMA_ID',
  table_name    = 'user',
  table_id      = '12009',
  table_type    = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],
  in_tenant_space = True,

  normal_columns = [
  ('host', 'varchar:OB_MAX_HOST_NAME_LENGTH'),
  ('user', 'varchar:OB_MAX_USER_NAME_LENGTH_STORE'),
  ('password', 'varchar:OB_MAX_PASSWORD_LENGTH'),
  ('select_priv', 'varchar:1'),
  ('insert_priv', 'varchar:1'),
  ('update_priv', 'varchar:1'),
  ('delete_priv', 'varchar:1'),
  ('create_priv', 'varchar:1'),
  ('drop_priv', 'varchar:1'),
  ('reload_priv', 'varchar:1'),
  ('shutdown_priv', 'varchar:1'),
  ('process_priv', 'varchar:1'),
  ('file_priv', 'varchar:1'),
  ('grant_priv', 'varchar:1'),
  ('reference_priv', 'varchar:1'),
  ('index_priv', 'varchar:1'),
  ('alter_priv', 'varchar:1'),
  ('show_db_priv', 'varchar:1'),
  ('super_priv', 'varchar:1'),
  ('create_tmp_table_priv', 'varchar:1'),
  ('lock_tables_priv', 'varchar:1'),
  ('execute_priv', 'varchar:1'),
  ('repl_slave_priv', 'varchar:1'),
  ('repl_client_priv', 'varchar:1'),
  ('create_view_priv', 'varchar:1'),
  ('show_view_priv', 'varchar:1'),
  ('create_routine_priv', 'varchar:1'),
  ('alter_routine_priv', 'varchar:1'),
  ('create_user_priv', 'varchar:1'),
  ('event_priv', 'varchar:1'),
  ('trigger_priv', 'varchar:1'),
  ('create_tablespace_priv', 'varchar:1'),
  ('ssl_type', 'varchar:10', 'false', ''),
  ('ssl_cipher', 'varchar:1024', 'false', ''),
  ('x509_issuer', 'varchar:1024', 'false', ''),
  ('x509_subject', 'varchar:1024', 'false', ''),
  ('max_questions', 'int', 'false', '0'),
  ('max_updates', 'int', 'false', '0'),
  ('max_connections', 'int', 'false', '0'),
  ('max_user_connections', 'int', 'false', '0'),
  ('plugin', 'varchar:1024'),
  ('authentication_string', 'varchar:1024'),
  ('password_expired', 'varchar:1'),
  ],
)

def_table_schema(
  database_id    = 'OB_MYSQL_SCHEMA_ID',
  table_name    = 'db',
  table_id      = '12010',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],
  in_tenant_space = True,

  normal_columns = [
  ('host', 'varchar:OB_MAX_HOST_NAME_LENGTH'),
  ('db', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
  ('user', 'varchar:OB_MAX_USER_NAME_LENGTH_STORE'),
  ('select_priv', 'varchar:1'),
  ('insert_priv', 'varchar:1'),
  ('update_priv', 'varchar:1'),
  ('delete_priv', 'varchar:1'),
  ('create_priv', 'varchar:1'),
  ('drop_priv', 'varchar:1'),
  ('grant_priv', 'varchar:1'),
  ('reference_priv', 'varchar:1'),
  ('index_priv', 'varchar:1'),
  ('alter_priv', 'varchar:1'),
  ('create_tmp_table_priv', 'varchar:1'),
  ('lock_tables_priv', 'varchar:1'),
  ('create_view_priv', 'varchar:1'),
  ('show_view_priv', 'varchar:1'),
  ('create_routine_priv', 'varchar:1'),
  ('alter_routine_priv', 'varchar:1'),
  ('execute_priv', 'varchar:1'),
  ('event_priv', 'varchar:1'),
  ('trigger_priv', 'varchar:1'),
  ],
)

def_table_schema(
  table_name     = '__all_virtual_server_memory_info',
  table_id       = '12011',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ],

  normal_columns = [
  ('server_memory_hold', 'int'),
  ('server_memory_limit', 'int'),
  ('system_reserved', 'int'),
  ('active_memstore_used', 'int'),
  ('total_memstore_used', 'int'),
  ('major_freeze_trigger', 'int'),
  ('memstore_limit', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name    = '__all_virtual_partition_table',
    table_id      = '12012',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
    ],
    only_rs_vtable = True,

  normal_columns = [
      ('sql_port', 'int'),
      ('unit_id', 'int'),
      ('partition_cnt', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('role', 'int'),
      ('member_list', 'varchar:MAX_MEMBER_LIST_LENGTH'),
      ('row_count', 'int'),
      ('data_size', 'int'),
      ('data_version', 'int'),
      ('data_checksum', 'int'),
      ('row_checksum', 'int'),
      ('column_checksum', 'varchar:COLUMN_CHECKSUM_LENGTH'),
      ('is_original_leader', 'int', 'false', '0'),
      ('to_leader_time', 'int', 'false', '0'),
      ('create_time', 'int'),
      ('rebuild', 'int', 'false', '0'),
      ('replica_type', 'int', 'false', '0'),
      ('status', 'varchar:MAX_REPLICA_STATUS_LENGTH', 'false', 'REPLICA_STATUS_NORMAL'),
      ('partition_checksum', 'int', 'false', '0'),
      ('required_size', 'int', 'false', '0'),
      ('is_restore', 'int', 'false', '0'),
      ('quorum', 'int', 'false', '-1'),
      ('fail_list', 'varchar:OB_MAX_FAILLIST_LENGTH', 'false', ''),
      ('recovery_timestamp', 'int', 'false', '0'),
      ('memstore_percent', 'int', 'false', '100'),
  ],
)

def_table_schema(
  table_name = '__all_virtual_lock_wait_stat',
  table_id = '12013',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('table_id', 'int'),
  ('rowkey', 'varchar:512'),
  ('addr', 'uint'),
  ('need_wait', 'bool'),
  ('recv_ts', 'int'),
  ('lock_ts', 'int'),
  ('abs_timeout', 'int'),
  ('try_lock_times', 'int'),
  ('time_after_recv', 'int'),
  ('session_id', 'int'),
  ('block_session_id', 'int'),
  ('type', 'int'),# 0 for ROW_LOCK
  ('lock_mode', 'int'), # 0 for write lock
  ('total_update_cnt', 'int')
  ],

  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name = '__all_virtual_partition_item',
  table_id = '12014',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int'),
  ('table_id', 'int'),
  ('partition_id', 'int'),
  ('tenant_name', 'varchar:OB_MAX_TENANT_NAME_LENGTH_STORE'),
  ('table_name', 'varchar:OB_MAX_TABLE_NAME_LENGTH'),
  ('partition_level', 'int'),
  ('partition_num', 'int'),
  ('partition_idx', 'int'),
  ('part_func_type', 'varchar:5'),
  ('part_num', 'int'),
  ('part_name', 'varchar:OB_MAX_PARTITION_NAME_LENGTH'),
  ('part_idx', 'int'),
  ('part_id', 'int'),
  ('part_high_bound', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
  ('subpart_func_type', 'varchar:5'),
  ('subpart_num', 'int'),
  ('subpart_name', 'varchar:OB_MAX_PARTITION_NAME_LENGTH'),
  ('subpart_idx', 'int'),
  ('subpart_id', 'int'),
  ('subpart_high_bound', 'varchar:OB_MAX_PARTITION_EXPR_LENGTH', 'true'),
  ],
)

def_table_schema(
    table_name    = '__all_virtual_replica_task',
    table_id      = '12015',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [],
    only_rs_vtable = True,

  normal_columns = [
      ('tenant_id', 'int', 'false'),
      ('table_id', 'int', 'false'),
      ('partition_id', 'int', 'false'),
      ('src_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('src_port', 'int'),
      ('src_replica_type', 'int'),
      ('zone', 'varchar:MAX_ZONE_LENGTH'),
      ('region', 'varchar:MAX_REGION_LENGTH'),
      ('dst_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('dst_port', 'int'),
      ('dst_replica_type', 'int'),
      ('cmd_type', 'varchar:MAX_ROOTSERVICE_EVENT_DESC_LENGTH', 'false'),
      ('comment', 'varchar:MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH', 'true', ''),
  ],
  index = {'all_virtual_replica_task_i1' : {
           'index_columns' : ["tenant_id"],
           'index_using_type' : 'USING_HASH'}}
)


def_table_schema(
  table_name     = '__all_virtual_partition_location',
  table_id       = '12016',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
    ('tenant_id', 'int'),
    ('table_id', 'int'),
    ('partition_id', 'int'),
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ],

  normal_columns = [
    ('unit_id', 'int'),
    ('partition_cnt', 'int'),
    ('zone', 'varchar:MAX_ZONE_LENGTH'),
    ('role', 'int'),
    ('member_list', 'varchar:MAX_MEMBER_LIST_LENGTH'),
    ('replica_type', 'int', 'false', '0'),
    ('status', 'varchar:MAX_REPLICA_STATUS_LENGTH', 'false', 'REPLICA_STATUS_NORMAL'),
    ('data_version', 'int'),
  ]
)

def_table_schema(
   database_id    = 'OB_MYSQL_SCHEMA_ID',
   table_name    = 'proc',
   table_id      = '12030',
   table_type = 'VIRTUAL_TABLE',
   gm_columns = [],
   rowkey_columns = [
   ],
   in_tenant_space = True,

   normal_columns = [
   ('db', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
   ('name', 'varchar:OB_MAX_ROUTINE_NAME_LENGTH'),
   ('type', 'varchar:10'),
   ('specific_name', 'varchar:OB_MAX_INFOSCHEMA_TABLE_NAME_LENGTH'),
   ('language', 'varchar:4', 'false', 'SQL'),
   ('sql_data_access', 'varchar:32', 'false', 'CONTAINS_SQL'),
   ('is_deterministic', 'varchar:4', 'false', 'NO'),
   ('security_type', 'varchar:10', 'false', 'DEFINER'),
   ('param_list', 'varchar:OB_MAX_VARCHAR_LENGTH', 'false', ''),
   ('returns', 'varchar:OB_MAX_VARCHAR_LENGTH', 'false', ''),
   ('body', 'varchar:OB_MAX_VARCHAR_LENGTH', 'false', ''),
   ('definer', 'varchar:77', 'false', ''),
   ('created', 'timestamp'),
   ('modified', 'timestamp',),
   ('sql_mode', 'varchar:32', 'false', ''),
   ('comment', 'varchar:OB_MAX_VARCHAR_LENGTH', 'false', ''),
   ('character_set_client', 'varchar:MAX_CHARSET_LENGTH'),
   ('collation_connection', 'varchar:MAX_CHARSET_LENGTH'),
   ('collation_database', 'varchar:MAX_CHARSET_LENGTH'),
   ('body_utf8', 'varchar:OB_MAX_VARCHAR_LENGTH'),
   ],
)

def_table_schema(
    table_name    = '__tenant_virtual_collation',
    table_id      = '12031',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [
    ],
    normal_columns = [
        ('collation', 'varchar:MAX_COLLATION_LENGTH', 'false', ''),
        ('charset', 'varchar:MAX_CHARSET_LENGTH', 'false', ''),
        ('id', 'int', 'false', '0'),
        ('is_default', 'varchar:MAX_BOOL_STR_LENGTH', 'false', ''),
        ('is_compiled', 'varchar:MAX_BOOL_STR_LENGTH', 'false', ''),
        ('sortlen', 'int', 'false', '0'),
  ],
)

def_table_schema(
    table_name    = '__tenant_virtual_charset',
    table_id      = '12032',
    table_type = 'VIRTUAL_TABLE',
    in_tenant_space = True,
    gm_columns = [],
    rowkey_columns = [
    ],
    normal_columns = [
        ('charset', 'varchar:MAX_CHARSET_LENGTH', 'false', ''),
        ('description', 'varchar:MAX_CHARSET_DESCRIPTION_LENGTH', 'false', ''),
        ('default_collation', 'varchar:MAX_COLLATION_LENGTH', 'false', ''),
        ('max_length', 'int', 'false', '0'),
  ],
)

def_table_schema(
  table_name = '__all_virtual_tenant_memstore_allocator_info',
  table_id = '12033',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('tenant_id', 'int'),
  ('table_id', 'int'),
  ('partition_id', 'int'),
  ('mt_base_version', 'int'),
  ('retire_clock', 'int'),
  ('mt_is_frozen', 'int'),
  ('mt_protection_clock', 'int'),
  ('mt_snapshot_version', 'int'),
  ],

  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name    = '__all_virtual_table_mgr',
    table_id      = '12034',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
    ],
    normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('tenant_id', 'int'),
      ('table_type', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
      ('index_id', 'int'),
      ('base_version', 'int'),
      ('multi_version_start', 'int'),
      ('snapshot_version', 'int'),
      ('max_merged_version', 'int'),
      ('upper_trans_version', 'int'),
      ('start_log_ts', 'uint'),
      ('end_log_ts', 'uint'),
      ('max_log_ts', 'uint'),
      ('version', 'int'),
      ('logical_data_version', 'int'),
      ('size', 'int'),
      ('compact_row', 'int'),
      ('is_active', 'int'),
      ('timestamp', 'int'),
      ('ref', 'int'),
      ('write_ref', 'int'),
      ('trx_count', 'int'),
      ('pending_log_persisting_row_cnt', 'int'),
      ('contain_uncommitted_row', 'bool'),
    ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12035',
  real_tenant_id=True,
  table_name = '__all_virtual_meta_table',
  keywords = all_def_keywords['__all_tenant_meta_table']))

def_table_schema(**gen_core_inner_table_def(12036, '__all_virtual_freeze_info', 'VIRTUAL_TABLE', freeze_info_def))

def_table_schema(
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'PARAMETERS',
  table_id       = '12037',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('SPECIFIC_CATALOG', 'varchar:MAX_TABLE_CATALOG_LENGTH', 'false', 'def'),
  ('SPECIFIC_SCHEMA', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
  ('SPECIFIC_NAME', 'varchar:OB_MAX_PARAMETERS_NAME_LENGTH', 'false', ''),
  ('ORDINAL_POSITION', 'int', 'false', '0'),
  ('PARAMETER_MODE', 'varchar:OB_MAX_PARAMETERS_NAME_LENGTH', 'true'),
  ('PARAMETER_NAME', 'varchar:OB_MAX_DEFAULT_VALUE_LENGTH', 'true'),
  ('DATA_TYPE', 'varchar:OB_MAX_PARAMETERS_NAME_LENGTH',  'false', ''),
  ('CHARACTER_MAXIMUM_LENGTH', 'uint',  'true'),
  ('CHARACTER_OCTET_LENGTH', 'uint', 'true'),
  ('NUMERIC_PRECISION', 'uint', 'true'),
  ('NUMERIC_SCALE','uint', 'true'),
  ('DATETIME_PRECISION', 'uint', 'true'),
  ('CHARACTER_SET_NAME', 'varchar:MAX_CHARSET_LENGTH', 'true'),
  ('COLLATION_NAME', 'varchar:MAX_COLLATION_LENGTH', 'true'),
  ('DTD_IDENTIFIER', 'varchar:OB_MAX_SYS_PARAM_NAME_LENGTH', 'false'),
  ('ROUTINE_TYPE', 'varchar:9', 'false', ''),
  ],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name      = '__all_virtual_bad_block_table',
  table_id        = '12038',
  table_type      = 'VIRTUAL_TABLE',
  gm_columns      = [],

  rowkey_columns  = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('disk_id', 'int'),
  ('store_file_path', 'varchar:MAX_PATH_SIZE'),
  ('macro_block_index', 'int'),
  ('error_type', 'int'),
  ('error_msg', 'varchar:OB_MAX_ERROR_MSG_LEN'),
  ('check_time', 'timestamp'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name      = '__all_virtual_px_worker_stat',
  table_id        = '12039',
  table_type      = 'VIRTUAL_TABLE',
  gm_columns      = [],
  rowkey_columns  = [
  ],
  normal_columns = [
  ('session_id', 'int'),
  ('tenant_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('trace_id', 'varchar:OB_MAX_HOST_NAME_LENGTH'),
  ('qc_id', 'int'),
  ('sqc_id', 'int'),
  ('worker_id', 'int'),
  ('dfo_id', 'int'),
  ('start_time', 'timestamp'),
  ('thread_id', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_trans_audit',
  table_id       = '12040',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  normal_columns = [
      ('tenant_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('trans_id', 'varchar:512'),
      ('pkey','varchar:64'),
      ('session_id', 'bigint:20'),
      ('proxy_id', 'varchar:512'),
      ('trans_type', 'int'),
      ('ctx_create_time', 'timestamp', 'true'),
      ('expired_time', 'timestamp', 'true'),
      ('trans_param', 'varchar:64'),
      ('total_sql_no', 'bigint:20'),
      ('refer', 'int'),
      ('prev_trans_arr', 'varchar:512'),
      ('next_trans_arr', 'varchar:512'),
      ('ctx_addr', 'bigint:20'),
      ('ctx_type', 'int'),
      ('trace_log', 'varchar:2048'),
      ('status', 'int'),
      ('for_replay', 'bool'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_trans_sql_audit',
  table_id       = '12041',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  normal_columns = [
      ('tenant_id', 'int'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('trans_id', 'varchar:512'),
      ('pkey','varchar:64'),
      ('sql_no', 'bigint:20'),
      ('trace_id', 'varchar:64'),
      ('phy_plan_type', 'bigint:20'),
      ('proxy_receive_us', 'bigint:20'),
      ('server_receive_us', 'bigint:20'),
      ('trans_receive_us', 'bigint:20'),
      ('trans_execute_us', 'bigint:20'),
      ('lock_for_read_cnt', 'bigint:20'),
      ('ctx_addr', 'bigint:20'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
	table_name     = '__all_virtual_weak_read_stat',
	table_id       = '12042',
	table_type = 'VIRTUAL_TABLE',
	gm_columns = [],
	rowkey_columns = [
	],

	in_tenant_space = True,
 	normal_columns = [
      ('tenant_id', 'bigint:20'),
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      #server
      ('server_version', 'bigint:20'),
      ('server_version_delta', 'bigint:20'),
      ('local_cluster_version', 'bigint:20'),
      ('local_cluster_version_delta', 'bigint:20'),
      ('total_part_count', 'bigint:20'),
      ('valid_inner_part_count', 'bigint:20'),
      ('valid_user_part_count', 'bigint:20'),
      #heartbeat
      ('cluster_master_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('cluster_master_port', 'int'),
      ('cluster_heartbeat_post_tstamp', 'bigint:20'),
      ('cluster_heartbeat_post_count', 'bigint:20'),
      ('cluster_heartbeat_succ_tstamp', 'bigint:20'),
      ('cluster_heartbeat_succ_count', 'bigint:20'),
      #self_check
      ('self_check_tstamp', 'bigint:20'),
      ('local_current_tstamp', 'bigint:20'),
      #cluster
      ('in_cluster_service', 'bigint:20'),
      ('is_cluster_service_master', 'bigint:20'),
      ('cluster_service_epoch', 'bigint:20'),
      ('cluster_total_server_count', 'bigint:20'),
      ('cluster_skipped_server_count', 'bigint:20'),
      ('cluster_version_gen_tstamp', 'bigint:20'),
      ('cluster_version', 'bigint:20'),
      ('cluster_version_delta', 'bigint:20'),
      ('min_cluster_version', 'bigint:20'),
      ('max_cluster_version', 'bigint:20'),
	],
  partition_columns = ['svr_ip', 'svr_port'],
)


def_table_schema(
  table_name     = '__all_virtual_partition_audit',
  table_id       = '12054',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('tenant_id', 'int'),
  ('table_id', 'int'),
  ('partition_id', 'int'),
  ('partition_status', 'int'),
  ('base_row_count', 'int'),
  ('insert_row_count', 'int'),
  ('delete_row_count', 'int'),
  ('update_row_count', 'int'),
  ('query_row_count', 'int'),
  ('insert_sql_count', 'int'),
  ('delete_sql_count', 'int'),
  ('update_sql_count', 'int'),
  ('query_sql_count', 'int'),
  ('trans_count', 'int'),
  ('sql_count', 'int'),
  ('rollback_insert_row_count', 'int'),
  ('rollback_delete_row_count', 'int'),
  ('rollback_update_row_count', 'int'),
  ('rollback_insert_sql_count', 'int'),
  ('rollback_delete_sql_count', 'int'),
  ('rollback_update_sql_count', 'int'),
  ('rollback_trans_count', 'int'),
  ('rollback_sql_count', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12055',
  real_tenant_id = False,
  table_name = '__all_virtual_sequence_v2',
  keywords = all_def_keywords['__all_sequence_v2']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12056',
  real_tenant_id = False,
  table_name = '__all_virtual_sequence_value',
  keywords = all_def_keywords['__all_sequence_value']))


def_table_schema(
  table_name     = '__all_virtual_cluster',
  table_id       = '12057',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],
  normal_columns = [
    ('cluster_id', 'int'),
    ('cluster_name', 'varchar:MAX_ZONE_INFO_LENGTH'),
    ('created', 'timestamp'),
    ('cluster_role', 'varchar:MAX_ZONE_INFO_LENGTH'),
    ('cluster_status', 'varchar:MAX_ZONE_INFO_LENGTH'),
    ('switchover#', 'int'),
    ('switchover_status', 'varchar:MAX_ZONE_INFO_LENGTH'),
    ('switchover_info', 'varchar:MAX_ZONE_INFO_LENGTH'),
    ('current_scn', 'int'),
    ('standby_became_primary_scn', 'int'),
    ('primary_cluster_id', 'int', 'true'),
    ('protection_mode', 'varchar:MAX_ZONE_INFO_LENGTH'),
    ('protection_level', 'varchar:MAX_ZONE_INFO_LENGTH'),
    ('redo_transport_options', 'varchar:MAX_ZONE_INFO_LENGTH'),
  ],
    only_rs_vtable = True,
)

def_table_schema(
  table_name     = '__all_virtual_partition_table_store_stat',
  table_id       = '12058',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
    ],
  normal_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('tenant_id', 'int'),
    ('table_id', 'int'),
    ('partition_id', 'int'),
    ('row_cache_hit_count', 'int'),
    ('row_cache_miss_count', 'int'),
    ('row_cache_put_count', 'int'),
    ('bf_filter_count', 'int'),
    ('bf_empty_read_count', 'int'),
    ('bf_access_count', 'int'),
    ('block_cache_hit_count', 'int'),
    ('block_cache_miss_count', 'int'),
    ('access_row_count', 'int'),
    ('output_row_count', 'int'),
    ('fuse_row_cache_hit_count', 'int'),
    ('fuse_row_cache_miss_count', 'int'),
    ('fuse_row_cache_put_count', 'int'),
    ('single_get_call_count', 'int'),
    ('single_get_output_row_count', 'int'),
    ('multi_get_call_count', 'int'),
    ('multi_get_output_row_count', 'int'),
    ('index_back_call_count', 'int'),
    ('index_back_output_row_count', 'int'),
    ('single_scan_call_count', 'int'),
    ('single_scan_output_row_count', 'int'),
    ('multi_scan_call_count', 'int'),
    ('multi_scan_output_row_count', 'int'),
    ('exist_row_effect_read_count', 'int'),
    ('exist_row_empty_read_count', 'int'),
    ('get_row_effect_read_count', 'int'),
    ('get_row_empty_read_count', 'int'),
    ('scan_row_effect_read_count', 'int'),
    ('scan_row_empty_read_count', 'int'),
    ('rowkey_prefix_access_info', 'varchar:COLUMN_DEFAULT_LENGTH'),
    ],
  partition_columns = ['svr_ip', 'svr_port'],
)

# Because of implementation problems, tenant schema's ddl operations can't be found in __all_virtual_ddl_operation.
def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12059',
  real_tenant_id=False,
  table_name = '__all_virtual_ddl_operation',
  keywords = all_def_keywords['__all_ddl_operation']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12060',
  real_tenant_id=False,
  table_name = '__all_virtual_outline',
  keywords = all_def_keywords['__all_outline']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12061',
  real_tenant_id=False,
  table_name = '__all_virtual_outline_history',
  keywords = all_def_keywords['__all_outline_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12062',
  real_tenant_id=False,
  table_name = '__all_virtual_synonym',
  keywords = all_def_keywords['__all_synonym']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12063',
  real_tenant_id=False,
  table_name = '__all_virtual_synonym_history',
  keywords = all_def_keywords['__all_synonym_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12064',
  real_tenant_id=False,
  table_name = '__all_virtual_database_privilege',
  keywords = all_def_keywords['__all_database_privilege']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12065',
  real_tenant_id=False,
  table_name = '__all_virtual_database_privilege_history',
  keywords = all_def_keywords['__all_database_privilege_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12066',
  real_tenant_id=False,
  table_name = '__all_virtual_table_privilege',
  keywords = all_def_keywords['__all_table_privilege']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12067',
  real_tenant_id=False,
  table_name = '__all_virtual_table_privilege_history',
  keywords = all_def_keywords['__all_table_privilege_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12068',
  real_tenant_id=False,
  table_name = '__all_virtual_database',
  keywords = all_def_keywords['__all_database']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12069',
  real_tenant_id=False,
  table_name = '__all_virtual_database_history',
  keywords = all_def_keywords['__all_database_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12070',
  real_tenant_id=False,
  table_name = '__all_virtual_tablegroup',
  keywords = all_def_keywords['__all_tablegroup']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12071',
  real_tenant_id=False,
  table_name = '__all_virtual_tablegroup_history',
  keywords = all_def_keywords['__all_tablegroup_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12072',
  real_tenant_id=False,
  table_name = '__all_virtual_table',
  keywords = all_def_keywords['__all_table']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12073',
  real_tenant_id=False,
  table_name = '__all_virtual_table_history',
  keywords = all_def_keywords['__all_table_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12074',
  real_tenant_id=False,
  table_name = '__all_virtual_column',
  keywords = all_def_keywords['__all_column']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12075',
  real_tenant_id=False,
  table_name = '__all_virtual_column_history',
  keywords = all_def_keywords['__all_column_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12076',
  real_tenant_id=False,
  table_name = '__all_virtual_part',
  keywords = all_def_keywords['__all_part']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12077',
  real_tenant_id=False,
  table_name = '__all_virtual_part_history',
  keywords = all_def_keywords['__all_part_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12078',
  real_tenant_id=False,
  table_name = '__all_virtual_part_info',
  keywords = all_def_keywords['__all_part_info']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12079',
  real_tenant_id=False,
  table_name = '__all_virtual_part_info_history',
  keywords = all_def_keywords['__all_part_info_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12080',
  real_tenant_id=False,
  table_name = '__all_virtual_def_sub_part',
  keywords = all_def_keywords['__all_def_sub_part']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12081',
  real_tenant_id=False,
  table_name = '__all_virtual_def_sub_part_history',
  keywords = all_def_keywords['__all_def_sub_part_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12082',
  real_tenant_id=False,
  table_name = '__all_virtual_sub_part',
  keywords = all_def_keywords['__all_sub_part']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12083',
  real_tenant_id=False,
  table_name = '__all_virtual_sub_part_history',
  keywords = all_def_keywords['__all_sub_part_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12084',
  real_tenant_id=False,
  table_name = '__all_virtual_constraint',
  keywords = all_def_keywords['__all_constraint']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12085',
  real_tenant_id=False,
  table_name = '__all_virtual_constraint_history',
  keywords = all_def_keywords['__all_constraint_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12086',
  real_tenant_id=False,
  table_name = '__all_virtual_foreign_key',
  keywords = all_def_keywords['__all_foreign_key']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12087',
  real_tenant_id=False,
  table_name = '__all_virtual_foreign_key_history',
  keywords = all_def_keywords['__all_foreign_key_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12088',
  real_tenant_id=False,
  table_name = '__all_virtual_foreign_key_column',
  keywords = all_def_keywords['__all_foreign_key_column']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12089',
  real_tenant_id=False,
  table_name = '__all_virtual_foreign_key_column_history',
  keywords = all_def_keywords['__all_foreign_key_column_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12090',
  real_tenant_id=False,
  table_name = '__all_virtual_temp_table',
  keywords = all_def_keywords['__all_temp_table']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12091',
  real_tenant_id=False,
  table_name = '__all_virtual_ori_schema_version',
  keywords = all_def_keywords['__all_ori_schema_version']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12092',
  real_tenant_id=False,
  table_name = '__all_virtual_sys_stat',
  keywords = all_def_keywords['__all_sys_stat']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12093',
  real_tenant_id=False,
  table_name = '__all_virtual_user',
  keywords = all_def_keywords['__all_user']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12094',
  real_tenant_id=False,
  table_name = '__all_virtual_user_history',
  keywords = all_def_keywords['__all_user_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12095',
  real_tenant_id=False,
  table_name = '__all_virtual_sys_variable',
  keywords = all_def_keywords['__all_sys_variable']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12096',
  real_tenant_id=False,
  table_name = '__all_virtual_sys_variable_history',
  keywords = all_def_keywords['__all_sys_variable_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12097',
  real_tenant_id=False,
  table_name = '__all_virtual_func',
  keywords = all_def_keywords['__all_func']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12098',
  real_tenant_id=False,
  table_name = '__all_virtual_func_history',
  keywords = all_def_keywords['__all_func_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12099',
  real_tenant_id=False,
  table_name = '__all_virtual_package',
  keywords = all_def_keywords['__all_package']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12100',
  real_tenant_id=False,
  table_name = '__all_virtual_package_history',
  keywords = all_def_keywords['__all_package_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12101',
  real_tenant_id=False,
  table_name = '__all_virtual_routine',
  keywords = all_def_keywords['__all_routine']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12102',
  real_tenant_id=False,
  table_name = '__all_virtual_routine_history',
  keywords = all_def_keywords['__all_routine_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12103',
  real_tenant_id=False,
  table_name = '__all_virtual_routine_param',
  keywords = all_def_keywords['__all_routine_param']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12104',
  real_tenant_id=False,
  table_name = '__all_virtual_routine_param_history',
  keywords = all_def_keywords['__all_routine_param_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12105',
  real_tenant_id=False,
  table_name = '__all_virtual_type',
  keywords = all_def_keywords['__all_type']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12106',
  real_tenant_id=False,
  table_name = '__all_virtual_type_history',
  keywords = all_def_keywords['__all_type_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12107',
  real_tenant_id=False,
  table_name = '__all_virtual_type_attr',
  keywords = all_def_keywords['__all_type_attr']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12108',
  real_tenant_id=False,
  table_name = '__all_virtual_type_attr_history',
  keywords = all_def_keywords['__all_type_attr_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12109',
  real_tenant_id=False,
  table_name = '__all_virtual_coll_type',
  keywords = all_def_keywords['__all_coll_type']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12110',
  real_tenant_id=False,
  table_name = '__all_virtual_coll_type_history',
  keywords = all_def_keywords['__all_coll_type_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12111',
  real_tenant_id=False,
  table_name = '__all_virtual_column_stat',
  keywords = all_def_keywords['__all_column_stat']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12112',
  real_tenant_id=False,
  table_name = '__all_virtual_table_stat',
  keywords = all_def_keywords['__all_table_stat']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12113',
  real_tenant_id=False,
  table_name = '__all_virtual_histogram_stat',
  keywords = all_def_keywords['__all_histogram_stat']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12114',
  real_tenant_id=False,
  table_name = '__all_virtual_column_statistic',
  keywords = all_def_keywords['__all_column_statistic']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12115',
  real_tenant_id=False,
  table_name = '__all_virtual_recyclebin',
  keywords = all_def_keywords['__all_recyclebin']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12116',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_gc_partition_info',
  keywords = all_def_keywords['__all_tenant_gc_partition_info']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12117',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_plan_baseline',
  keywords = all_def_keywords['__all_tenant_plan_baseline']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12118',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_plan_baseline_history',
  keywords = all_def_keywords['__all_tenant_plan_baseline_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12119',
  real_tenant_id=False,
  table_name = '__all_virtual_sequence_object',
  keywords = all_def_keywords['__all_sequence_object']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12120',
  real_tenant_id=False,
  table_name = '__all_virtual_sequence_object_history',
  keywords = all_def_keywords['__all_sequence_object_history']))

def_table_schema(
    table_name    = '__all_virtual_raid_stat',
    table_id      = '12121',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
    ],
    normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('disk_idx', 'int'),
      ('install_seq', 'int'),
      ('data_num', 'int'),
      ('parity_num', 'int'),
      ('create_ts', 'int'),
      ('finish_ts', 'int'),
      ('alias_name', 'varchar:MAX_PATH_SIZE'),
      ('status', 'varchar:OB_STATUS_LENGTH'),
      ('percent', 'int'),
    ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12122',
  real_tenant_id=True,
  table_name = '__all_virtual_server_log_meta',
  keywords = all_def_keywords['__all_clog_history_info_v2']))

# start for DTL
def_table_schema(
    table_name    = '__all_virtual_dtl_channel',
    table_id      = '12123',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
    ],
    normal_columns = [
      ('channel_id', 'int'),
      ('op_id', 'int'),
      ('peer_id', 'int'),
      ('tenant_id', 'int'),
      ('is_local', 'bool'),
      ('is_data', 'bool'),
      ('is_transmit', 'bool'),
      ('alloc_buffer_cnt', 'int'),
      ('free_buffer_cnt', 'int'),
      ('send_buffer_cnt', 'int'),
      ('recv_buffer_cnt', 'int'),
      ('processed_buffer_cnt', 'int'),
      ('send_buffer_size', 'int'),
      ('hash_val', 'int'),
      ('buffer_pool_id', 'int'),
      ('pins', 'int'),
      ('first_in_ts', 'timestamp'),
      ('first_out_ts', 'timestamp'),
      ('last_int_ts', 'timestamp'),
      ('last_out_ts', 'timestamp'),
      ('status', 'int')
    ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name    = '__all_virtual_dtl_memory',
    table_id      = '12124',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
    ],
    normal_columns = [
      ('tenant_id', 'int'),
      ('channel_total_cnt', 'int'),
      ('channel_block_cnt', 'int'),
      ('max_parallel_cnt', 'int'),
      ('max_blocked_buffer_size', 'int'),
      ('accumulated_blocked_cnt', 'int'),
      ('current_buffer_used', 'int'),
      ('seqno', 'int'),
      ('alloc_cnt', 'int'),
      ('free_cnt', 'int'),
      ('free_queue_len', 'int'),
      ('total_memory_size', 'int'),
      ('real_alloc_cnt', 'int'),
      ('real_free_cnt', 'int'),
    ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name    = '__all_virtual_dtl_first_cached_buffer',
    table_id      = '12125',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
    ],
    normal_columns = [
      ('tenant_id', 'int'),
      ('channel_id', 'int'),
      ('calced_val', 'int'),
      ('buffer_pool_id', 'int'),
      ('timeout_ts', 'timestamp'),
    ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12126',
  real_tenant_id=False,
  table_name = '__all_virtual_dblink',
  keywords = all_def_keywords['__all_dblink']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12127',
  real_tenant_id=False,
  table_name = '__all_virtual_dblink_history',
  keywords = all_def_keywords['__all_dblink_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12128',
  real_tenant_id=True,
  table_name = '__all_virtual_tenant_partition_meta_table',
  keywords = all_def_keywords['__all_tenant_partition_meta_table']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12129',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_role_grantee_map',
  keywords = all_def_keywords['__all_tenant_role_grantee_map']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12130',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_role_grantee_map_history',
  keywords = all_def_keywords['__all_tenant_role_grantee_map_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12131',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_keystore',
  keywords = all_def_keywords['__all_tenant_keystore']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12132',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_keystore_history',
  keywords = all_def_keywords['__all_tenant_keystore_history']))

def_table_schema(
  table_name = '__all_virtual_deadlock_stat',
  table_id = '12141',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ],
  normal_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('cycle_id', 'uint'), # id of a cycle
    ('cycle_seq', 'int'), # seq id in a cycle
    ('session_id', 'int'),
    ('table_id', 'int'),
    ('row_key', 'varchar:512'),
    # ('row_id', 'int'),
    ('waiter_trans_id', 'varchar:512'),
    ('holder_trans_id', 'varchar:512'),
    ('deadlock_rollbacked', 'bool'), # need rollback beacuse of deadlock
    ('cycle_detect_ts', 'int'), # timestamp when the cycle is first detected
    ('lock_wait_ts', 'int'), # timestamp when the wait for relationship is started
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

# tablespace begin
def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12142',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_tablespace',
  keywords = all_def_keywords['__all_tenant_tablespace']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12143',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_tablespace_history',
  keywords = all_def_keywords['__all_tenant_tablespace_history']))

#tablespace end

def_table_schema(
  table_name     = '__ALL_VIRTUAL_INFORMATION_COLUMNS',
  table_id       = '12144',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  in_tenant_space = True,
  rowkey_columns = [
  ('TABLE_SCHEMA', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
  ('TABLE_NAME', 'varchar:OB_MAX_TABLE_NAME_LENGTH'),
  ],

  normal_columns = [
  ('TABLE_CATALOG', 'varchar:MAX_TABLE_CATALOG_LENGTH', 'false', ''),
  ('COLUMN_NAME', 'varchar:OB_MAX_COLUMN_NAME_LENGTH', 'false', ''),
  ('ORDINAL_POSITION', 'uint', 'false', '0'),
  ('COLUMN_DEFAULT', 'varchar:OB_MAX_DEFAULT_VALUE_LENGTH', 'true'),
  ('IS_NULLABLE', 'varchar:COLUMN_NULLABLE_LENGTH',  'false', ''),
  ('DATA_TYPE', 'varchar:COLUMN_TYPE_LENGTH',  'false', ''),
  ('CHARACTER_MAXIMUM_LENGTH', 'uint', 'true'),
  ('CHARACTER_OCTET_LENGTH', 'uint', 'true'),
  ('NUMERIC_PRECISION', 'uint', 'true'),
  ('NUMERIC_SCALE','uint', 'true'),
  ('DATETIME_PRECISION', 'uint', 'true'),
  ('CHARACTER_SET_NAME', 'varchar:MAX_CHARSET_LENGTH', 'true'),
  ('COLLATION_NAME', 'varchar:MAX_COLLATION_LENGTH', 'true'),
  ('COLUMN_TYPE', 'varchar:COLUMN_TYPE_LENGTH'),
  ('COLUMN_KEY', 'varchar:MAX_COLUMN_KEY_LENGTH', 'false', ''),
  ('EXTRA', 'varchar:COLUMN_EXTRA_LENGTH', 'false', ''),
  ('PRIVILEGES', 'varchar:MAX_COLUMN_PRIVILEGE_LENGTH', 'false', ''),
  ('COLUMN_COMMENT', 'varchar:MAX_COLUMN_COMMENT_LENGTH', 'false', ''),
  ('GENERATION_EXPRESSION', 'varchar:OB_MAX_DEFAULT_VALUE_LENGTH', 'false', '')
  ],
)

def_table_schema(
  table_name     = '__all_virtual_pg_partition_info',
  table_id       = '12145',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_idx', 'int'),
      ('tg_id', 'int'),
      ('pg_idx', 'int'),
      ('max_decided_trans_version', 'int'),
      ('max_passed_trans_ts', 'int'),
      ('freeze_ts', 'int'),
      ('allow_gc', 'bool'),
      ('partition_state', 'varchar:TABLE_MAX_VALUE_LENGTH'),
      ('min_log_service_ts', 'int', 'false', '-1'),
      ('min_trans_service_ts', 'int', 'false', '-1'),
      ('min_replay_engine_ts', 'int', 'false', '-1'),
      ('is_pg', 'bool'),
      ('weak_read_timestamp', 'int', 'false', '-1'),
      ('replica_type', 'int', 'false', '0'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12146',
  real_tenant_id=True,
  table_name = '__all_virtual_tenant_user_failed_login_stat',
  keywords = all_def_keywords['__all_tenant_user_failed_login_stat']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12147',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_profile',
  keywords = all_def_keywords['__all_tenant_profile']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12148',
  real_tenant_id=False,
  table_name = '__all_virtual_tenant_profile_history',
  keywords = all_def_keywords['__all_tenant_profile_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12149',
  real_tenant_id=False,
  table_name = '__all_virtual_security_audit',
  keywords = all_def_keywords['__all_tenant_security_audit']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12150',
  real_tenant_id=False,
  table_name = '__all_virtual_security_audit_history',
  keywords = all_def_keywords['__all_tenant_security_audit_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12151',
  real_tenant_id=False,
  table_name = '__all_virtual_trigger',
  keywords = all_def_keywords['__all_tenant_trigger']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12152',
  real_tenant_id=False,
  table_name = '__all_virtual_trigger_history',
  keywords = all_def_keywords['__all_tenant_trigger_history']))

def_table_schema(
  table_name     = '__all_virtual_cluster_stats',
  table_id       = '12153',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
      ('tenant_id', 'int'),
  ],
  only_rs_vtable = True,

  normal_columns = [
      ('refreshed_schema_version', 'int'),
      ('ddl_lag', 'int'),
      ('min_sys_table_scn', 'int'),
      ('min_user_table_scn', 'int'),
  ],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12154',
  real_tenant_id=False,
  table_name = '__all_virtual_sstable_column_checksum',
  keywords = all_def_keywords['__all_tenant_sstable_column_checksum']))

def_table_schema(
  table_name     = '__all_virtual_ps_stat',
  table_id       = '12155',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  enable_column_def_enum = True,

  normal_columns = [
    ('tenant_id', 'int'),
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('stmt_count', 'int'),
    ('hit_count', 'int'),
    ('access_count', 'int'),
    ('mem_hold', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_ps_item_info',
  table_id       = '12156',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  enable_column_def_enum = True,

  normal_columns = [
    ('tenant_id', 'int'),
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('stmt_id', 'int'),
    ('db_id', 'int'),
    ('ps_sql', 'longtext'),
    ('param_count', 'int'),
    ('stmt_item_ref_count', 'int'),
    ('stmt_info_ref_count', 'int'),
    ('mem_hold', 'int'),
    ('stmt_type', 'int'),
    ('checksum', 'int'),
    ('expired', 'bool')
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_sql_workarea_history_stat',
  table_id       = '12158',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [],
  normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('plan_id', 'int'),
      ('sql_id', 'varchar:OB_MAX_SQL_ID_LENGTH'),
      ('operation_type', 'varchar:40'),
      ('operation_id', 'int'),
      ('estimated_optimal_size', 'int'),
      ('estimated_onepass_size', 'int'),
      ('last_memory_used', 'int'),
      ('last_execution', 'varchar:10'),
      ('last_degree', 'int'),
      ('total_executions', 'int'),
      ('optimal_executions', 'int'),
      ('onepass_executions', 'int'),
      ('multipasses_executions', 'int'),
      ('active_time', 'int'),
      ('max_tempseg_size', 'int'),
      ('last_tempseg_size', 'int'),
      ('tenant_id', 'int'),
      ('policy', 'varchar:10'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_sql_workarea_active',
  table_id       = '12159',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [],
  normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('plan_id', 'int'),
      ('sql_id', 'varchar:OB_MAX_SQL_ID_LENGTH'),
      ('sql_exec_id', 'int'),
      ('operation_type', 'varchar:40'),
      ('operation_id', 'int'),
      ('sid', 'int'),
      ('active_time', 'int'),
      ('work_area_size', 'int'),
      ('expect_size', 'int'),
      ('actual_mem_used', 'int'),
      ('max_mem_used', 'int'),
      ('number_passes', 'int'),
      ('tempseg_size', 'int'),
      ('tenant_id', 'int'),
      ('policy', 'varchar:6'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_sql_workarea_histogram',
  table_id       = '12160',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [],
  normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('low_optimal_size', 'int'),
      ('high_optimal_size', 'int'),
      ('optimal_executions', 'int'),
      ('onepass_executions', 'int'),
      ('multipasses_executions', 'int'),
      ('total_executions', 'int'),
      ('tenant_id', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_sql_workarea_memory_info',
  table_id       = '12161',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [],
  normal_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('max_workarea_size', 'int'),
      ('workarea_hold_size', 'int'),
      ('max_auto_workarea_size', 'int'),
      ('mem_target', 'int'),
      ('total_mem_used', 'int'),
      ('global_mem_bound', 'int'),
      ('drift_size', 'int'),
      ('workarea_count', 'int'),
      ('manual_calc_count', 'int'),
      ('tenant_id', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12162',
  real_tenant_id=False,
  table_name = '__all_virtual_security_audit_record',
  keywords = all_def_keywords['__all_tenant_security_audit_record']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12163',
  real_tenant_id=False,
  table_name = '__all_virtual_sysauth',
  keywords = all_def_keywords['__all_tenant_sysauth']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12164',
  real_tenant_id=False,
  table_name = '__all_virtual_sysauth_history',
  keywords = all_def_keywords['__all_tenant_sysauth_history']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12165',
  real_tenant_id=False,
  table_name = '__all_virtual_objauth',
  keywords = all_def_keywords['__all_tenant_objauth']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12166',
  real_tenant_id=False,
  table_name = '__all_virtual_objauth_history',
  keywords = all_def_keywords['__all_tenant_objauth_history']))
def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12167',
  real_tenant_id=True,
  table_name = '__all_virtual_backup_info',
  keywords = all_def_keywords['__all_tenant_backup_info']))

# 12168 used for __all_virtual_backup_log_archive_status

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12170',
  real_tenant_id=True,
  table_name = '__all_virtual_backup_task',
  keywords = all_def_keywords['__all_tenant_backup_task']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12171',
  real_tenant_id=True,
  table_name = '__all_virtual_pg_backup_task',
  keywords = all_def_keywords['__all_tenant_pg_backup_task']))

def_table_schema(
  table_name     = '__all_virtual_pg_backup_log_archive_status',
  table_id       = '12173',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('tenant_id', 'int'),
    ('table_id', 'int'),
    ('partition_id', 'int'),
  ],
  normal_columns = [
    ('incarnation', 'int'),
    ('log_archive_round', 'int'),
    ('log_archive_start_ts', 'int'),
    ('log_archive_status', 'int'),
    ('log_archive_cur_log_id', 'int'),
    ('log_archive_cur_ts', 'int'),
    ('max_log_id', 'int'),
    ('max_log_ts', 'int'),
    ('cur_piece_id', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_server_backup_log_archive_status',
  table_id       = '12174',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('tenant_id', 'int'),
  ],
  normal_columns = [
    ('incarnation', 'int'),
    ('log_archive_round', 'int'),
    ('log_archive_start_ts', 'int'),
    ('log_archive_cur_ts', 'int'),
    ('pg_count', 'int'),
    ('cur_piece_id', 'int')
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)
def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12175',
  real_tenant_id=False,
  table_name = '__all_virtual_error',
  keywords = all_def_keywords['__all_tenant_error']))

def_table_schema(
  table_name     = '__all_virtual_timestamp_service',
  table_id       = '12176',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ('tenant_id', 'int'),
  ],

  normal_columns = [
      ('ts_type', 'int'),
      ('ts_value', 'int'),
  ],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  database_id   = 'OB_INFORMATION_SCHEMA_ID',
  table_name    = 'REFERENTIAL_CONSTRAINTS',
  table_id      = '12177',
  table_type = 'VIRTUAL_TABLE',
  gm_columns    = [],
  rowkey_columns = [],
  in_tenant_space = True,

  normal_columns = [
  ('CONSTRAINT_CATALOG', 'varchar:MAX_TABLE_CATALOG_LENGTH', 'false', ''),
  ('CONSTRAINT_SCHEMA', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
  ('CONSTRAINT_NAME', 'varchar:OB_MAX_CONSTRAINT_NAME_LENGTH', 'false', ''),
  ('UNIQUE_CONSTRAINT_CATALOG', 'varchar:MAX_TABLE_CATALOG_LENGTH', 'false', ''),
  ('UNIQUE_CONSTRAINT_SCHEMA', 'varchar:OB_MAX_DATABASE_NAME_LENGTH', 'false', ''),
  ('UNIQUE_CONSTRAINT_NAME', 'varchar:OB_MAX_CONSTRAINT_NAME_LENGTH', 'true', 'NULL'),
  ('MATCH_OPTION', 'varchar:64', 'false', ''),
  ('UPDATE_RULE', 'varchar:64', 'false', ''),
  ('DELETE_RULE', 'varchar:64', 'false', ''),
  ('TABLE_NAME', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'false', ''),
  ('REFERENCED_TABLE_NAME', 'varchar:OB_MAX_TABLE_NAME_LENGTH', 'false', '')
  ]
)

def_table_schema(
    table_name    = '__all_virtual_table_modifications',
    table_id      = '12179',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
      ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
      ('svr_port', 'int'),
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('partition_id', 'int'),
    ],
    normal_columns = [
      ('insert_row_count', 'int'),
      ('update_row_count', 'int'),
      ('delete_row_count', 'int'),
      ('max_snapshot_version', 'int'),
    ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name    = '__all_virtual_backup_clean_info',
    table_id      = '12180',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
      ('tenant_id', 'int'),
    ],

    only_rs_vtable = True,

    normal_columns = [
      ('job_id', 'int'),
      ('start_time', 'int'),
      ('end_time', 'int'),
      ('incarnation', 'int'),
      ('type', 'varchar:OB_INNER_TABLE_BACKUP_CLEAN_TYPE_LENGTH'),
      ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
      ('parameter', 'int'),
      ('error_msg', 'varchar:OB_MAX_ERROR_MSG_LEN'),
      ('comment', 'varchar:MAX_TABLE_COMMENT_LENGTH'),
      ('clog_gc_snapshot', 'int', 'true'),
      ('result', 'int', 'true'),
      ('copy_id', 'int')
  ],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12181',
  real_tenant_id=True,
  table_name = '__all_virtual_restore_pg_info',
  keywords = all_def_keywords['__all_tenant_restore_pg_info']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12182',
  real_tenant_id=False,
  table_name = '__all_virtual_object_type',
  keywords = all_def_keywords['__all_tenant_object_type']))

def_table_schema(
  table_name     = '__all_virtual_trans_table_status',
  table_id       = '12183',
  table_type     = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
  ],

  normal_columns = [
  ('tenant_id', 'int'),
  ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
  ('svr_port', 'int'),
  ('zone', 'varchar:MAX_ZONE_LENGTH'),
  ('table_id', 'int'),
  ('partition_id', 'int'),
  ('end_log_id', 'int'),
  ('trans_cnt', 'int')],

  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  table_name     = '__all_virtual_pg_log_archive_stat',
  table_id       = '12184',
  table_type = 'VIRTUAL_TABLE',
  gm_columns     = [],
  rowkey_columns = [
    ('svr_ip', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('svr_port', 'int'),
    ('tenant_id', 'int'),
    ('table_id', 'int'),
    ('partition_id', 'int'),
  ],
  normal_columns = [
    ('incarnation', 'int'),
    ('log_archive_round', 'int'),
    ('epoch', 'int'),
    ('been_deleted', 'bool'),
    ('is_first_record_finish', 'bool'),
    ('encount_error', 'bool'),
    ('current_ilog_id', 'int'),
    ('max_log_id', 'int'),
    ('round_start_log_id', 'int'),
    ('round_start_ts', 'int'),
    ('round_snapshot_version', 'int'),
    ('cur_start_log_id', 'int'),
    ('fetcher_max_split_log_id', 'int'),
    ('clog_split_max_log_id', 'int'),
    ('clog_split_max_log_ts', 'int'),
    ('clog_split_checkpoint_ts', 'int'),
    ('max_archived_log_id', 'int'),
    ('max_archived_log_ts', 'int'),
    ('max_archived_checkpoint_ts', 'int'),
    ('clog_epoch', 'int'),
    ('clog_accum_checksum', 'int'),
    ('cur_index_file_id', 'int'),
    ('cur_index_file_offset', 'int'),
    ('cur_data_file_id', 'int'),
    ('cur_data_file_offset', 'int'),
    ('clog_split_task_num', 'int'),
    ('send_task_num', 'int'),
    ('cur_piece_id', 'int'),
    #('observer_archive_status', 'int'),
  ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  table_name    = '__all_virtual_sql_plan_monitor',
  table_id      = '12185',
  table_type = 'VIRTUAL_TABLE',
  index_using_type = 'USING_BTREE',
  gm_columns    = [],
  rowkey_columns = [
    ('SVR_IP', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('SVR_PORT', 'int'),
    ('TENANT_ID', 'int'),
    ('REQUEST_ID', 'int'),
  ],
  normal_columns = [
    ('TRACE_ID', 'varchar:OB_MAX_TRACE_ID_BUFFER_SIZE'),
    ('FIRST_REFRESH_TIME', 'timestamp'),
    ('LAST_REFRESH_TIME' ,'timestamp'),
    ('FIRST_CHANGE_TIME','timestamp'),
    ('LAST_CHANGE_TIME','timestamp'),
    ('OTHERSTAT_1_ID', 'int'),
    ('OTHERSTAT_1_VALUE', 'int'),
    ('OTHERSTAT_2_ID', 'int'),
    ('OTHERSTAT_2_VALUE', 'int'),
    ('OTHERSTAT_3_ID', 'int'),
    ('OTHERSTAT_3_VALUE', 'int'),
    ('OTHERSTAT_4_ID', 'int'),
    ('OTHERSTAT_4_VALUE', 'int'),
    ('OTHERSTAT_5_ID', 'int'),
    ('OTHERSTAT_5_VALUE', 'int'),
    ('OTHERSTAT_6_ID', 'int'),
    ('OTHERSTAT_6_VALUE', 'int'),
    ('OTHERSTAT_7_ID', 'int'),
    ('OTHERSTAT_7_VALUE', 'int'),
    ('OTHERSTAT_8_ID', 'int'),
    ('OTHERSTAT_8_VALUE', 'int'),
    ('OTHERSTAT_9_ID', 'int'),
    ('OTHERSTAT_9_VALUE', 'int'),
    ('OTHERSTAT_10_ID', 'int'),
    ('OTHERSTAT_10_VALUE', 'int'),
    ('THREAD_ID', 'int'),
    ('PLAN_OPERATION', 'varchar:OB_MAX_OPERATOR_NAME_LENGTH'),
    ('STARTS', 'int'),
    ('OUTPUT_ROWS', 'int'),
    ('PLAN_LINE_ID', 'int'),
    ('PLAN_DEPTH', 'int'),
  ],
  partition_columns = ['SVR_IP', 'SVR_PORT'],
  index = {'all_virtual_sql_plan_monitor_i1' :  { 'index_columns' : ['TENANT_ID', 'REQUEST_ID'],
                     'index_using_type' : 'USING_BTREE'}},
)


def_table_schema(
  table_name    = '__all_virtual_sql_monitor_statname',
  table_id      = '12186',
  table_type = 'VIRTUAL_TABLE',
  in_tenant_space = True,
  gm_columns    = [],
  rowkey_columns = [
  ],
  normal_columns = [
    ('ID', 'int'),
    ('GROUP_ID', 'int'),
    ('NAME', 'varchar:40'),
    ('DESCRIPTION', 'varchar:200'),
  ]
)

def_table_schema(
  table_name    = '__all_virtual_open_cursor',
  table_id      = '12187',
  table_type = 'VIRTUAL_TABLE',
  in_tenant_space = True,
  gm_columns    = [],
  rowkey_columns = [
  ],
  normal_columns = [
    ('TENANT_ID', 'int'),
    ('SVR_IP', 'varchar:MAX_IP_ADDR_LENGTH'),
    ('SVR_PORT', 'int'),
    ('SADDR', 'varchar:8'),
    ('SID', 'int'),
    ('USER_NAME', 'varchar:30'),
    ('ADDRESS', 'varchar:8'),
    ('HASH_VALUE', 'int'),
    ('SQL_ID', 'varchar:OB_MAX_SQL_ID_LENGTH'),
    ('SQL_TEXT', 'varchar:60'),
    ('LAST_SQL_ACTIVE_TIME', 'timestamp'),
    ('SQL_EXEC_ID', 'int'),
  ],
  partition_columns = ['SVR_IP', 'SVR_PORT'],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12188',
  real_tenant_id=True,
  table_name = '__all_virtual_backup_validation_task',
  keywords = all_def_keywords['__all_tenant_backup_validation_task']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12189',
  real_tenant_id=True,
  table_name = '__all_virtual_pg_backup_validation_task',
  keywords = all_def_keywords['__all_tenant_pg_backup_validation_task']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12190',
  real_tenant_id=True,
  table_name = '__all_virtual_time_zone',
  keywords = all_def_keywords['__all_tenant_time_zone']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12191',
  real_tenant_id=True,
  table_name = '__all_virtual_time_zone_name',
  keywords = all_def_keywords['__all_tenant_time_zone_name']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12192',
  real_tenant_id=True,
  table_name = '__all_virtual_time_zone_transition',
  keywords = all_def_keywords['__all_tenant_time_zone_transition']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12193',
  real_tenant_id=True,
  table_name = '__all_virtual_time_zone_transition_type',
  keywords = all_def_keywords['__all_tenant_time_zone_transition_type']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12194',
  real_tenant_id=False,
  table_name = '__all_virtual_constraint_column',
  keywords = all_def_keywords['__all_tenant_constraint_column']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12195',
  real_tenant_id=False,
  table_name = '__all_virtual_constraint_column_history',
  keywords = all_def_keywords['__all_tenant_constraint_column_history']))


def_table_schema(
  table_name     = '__all_virtual_files',
  table_id       = '12196',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,
  normal_columns = [
    ('FILE_ID','bigint:4','false','0'),
    ('FILE_NAME','varchar:64','true','NULL'),
    ('FILE_TYPE','varchar:20','false',''),
    ('TABLESPACE_NAME','varchar:64','true','NULL'),
    ('TABLE_CATALOG','varchar:64','false',''),
    ('TABLE_SCHEMA','varchar:64','true','NULL'),
    ('TABLE_NAME','varchar:64','true','NULL'),
    ('LOGFILE_GROUP_NAME','varchar:64','true','NULL'),
    ('LOGFILE_GROUP_NUMBER','bigint:4','true','NULL'),
    ('ENGINE','varchar:64','false',''),
    ('FULLTEXT_KEYS','varchar:64','true','NULL'),
    ('DELETED_ROWS','bigint:4','true','NULL'),
    ('UPDATE_COUNT','bigint:4','true','NULL'),
    ('FREE_EXTENTS','bigint:4','true','NULL'),
    ('TOTAL_EXTENTS','bigint:4','true','NULL'),
    ('EXTENT_SIZE','bigint:4','false','0'),
    ('INITIAL_SIZE','uint','true','NULL'),
    ('MAXIMUM_SIZE','uint','true','NULL'),
    ('AUTOEXTEND_SIZE','uint','true','NULL'),
    ('CREATION_TIME','timestamp','true','NULL'),
    ('LAST_UPDATE_TIME','timestamp','true','NULL'),
    ('LAST_ACCESS_TIME','timestamp','true','NULL'),
    ('RECOVER_TIME','bigint:4','true','NULL'),
    ('TRANSACTION_COUNTER','bigint:4','true','NULL'),
    ('VERSION','uint','true','NULL'),
    ('ROW_FORMAT','varchar:10','true','NULL'),
    ('TABLE_ROWS','uint','true','NULL'),
    ('AVG_ROW_LENGTH','uint','true','NULL'),
    ('DATA_LENGTH','uint','true','NULL'),
    ('MAX_DATA_LENGTH','uint','true','NULL'),
    ('INDEX_LENGTH','uint','true','NULL'),
    ('DATA_FREE','uint','true','NULL'),
    ('CREATE_TIME','timestamp','true','NULL'),
    ('UPDATE_TIME','timestamp','true','NULL'),
    ('CHECK_TIME','timestamp','true','NULL'),
    ('CHECKSUM','uint','true','NULL'),
    ('STATUS','varchar:20','false',''),
    ('EXTRA','varchar:255','true','NULL')
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'FILES',
  table_id       = '12197',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,
  view_definition = """SELECT * FROM oceanbase.__all_virtual_files""".replace("\n", " "),
  normal_columns = [
  ],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12198',
  real_tenant_id=False,
  table_name = '__all_virtual_dependency',
  keywords = all_def_keywords['__all_tenant_dependency']))

def_table_schema(
  table_name     = '__tenant_virtual_object_definition',
  table_id       = '12199',
  table_type = 'VIRTUAL_TABLE',
  gm_columns = [],
  rowkey_columns = [
  ('object_type', 'int'),
  ('object_name', 'varchar:OB_MAX_ORIGINAL_NANE_LENGTH'),
  ('schema', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
  ('version', 'varchar:10'),
  ('model', 'varchar:OB_MAX_DATABASE_NAME_LENGTH'),
  ('transform', 'varchar:8'),
  ],
  in_tenant_space = True,

  normal_columns = [
  ('definition', 'longtext'),
  ('create_database_with_if_not_exists', 'varchar:DATABASE_DEFINE_LENGTH'),
  ('character_set_client', 'varchar:MAX_CHARSET_LENGTH'),
  ('collation_connection', 'varchar:MAX_CHARSET_LENGTH'),
  ('proc_type', 'int'),
  ('collation_database', 'varchar:MAX_CHARSET_LENGTH'),
  ('sql_mode', 'varchar:MAX_CHARSET_LENGTH'),
  ],
)

def_table_schema(
    table_name    = '__all_virtual_reserved_table_mgr',
    table_id      = '12200',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
      ('svr_ip', 'varchar:OB_MAX_SERVER_ADDR_SIZE'),
      ('svr_port', 'int'),
      ('tenant_id', 'int'),
      ('table_id', 'int'),
      ('table_type', 'int'),
      ('partition_id', 'int'),
      ('index_id', 'int'),
      ('base_version', 'int'),
      ('multi_version_start', 'int'),
      ('snapshot_version', 'int'),
      ('version', 'int'),
    ],
    normal_columns = [
      ('size', 'int'),
      ('ref', 'int'),
      ('reserve_type', 'varchar:OB_MAX_RESERVED_POINT_TYPE_LENGTH'),
      ('reserve_point_version', 'int'),
    ],
  partition_columns = ['svr_ip', 'svr_port'],
)

def_table_schema(
    table_name    = '__all_virtual_backupset_history_mgr',
    table_id      = '12201',
    table_type = 'VIRTUAL_TABLE',
    gm_columns = [],
    rowkey_columns = [
      ('tenant_id', 'int'),
      ('incarnation', 'int'),
      ('backup_set_id', 'int'),
    ],
    only_rs_vtable = True,

    normal_columns = [
      ('backup_type', 'varchar:OB_INNER_TABLE_BACKUP_TYPE_LENTH'),
      ('device_type', 'varchar:OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH'),
      ('snapshot_version', 'int'),
      ('prev_full_backup_set_id', 'int'),
      ('prev_inc_backup_set_id', 'int'),
      ('prev_backup_data_version', 'int'),
      ('pg_count', 'int'),
      ('macro_block_count', 'int'),
      ('finish_pg_count', 'int'),
      ('finish_macro_block_count', 'int'),
      ('input_bytes', 'int'),
      ('output_bytes', 'int'),
      ('start_time', 'timestamp'),
      ('end_time', 'timestamp'),
      ('compatible', 'int'),
      ('cluster_version', 'int'),
      ('status', 'varchar:OB_DEFAULT_STATUS_LENTH'),
      ('result', 'int'),
      ('cluster_id', 'int', 'true'),
      ('backup_dest', 'varchar:OB_MAX_BACKUP_DEST_LENGTH', 'true'),
      ('backup_data_version', 'int', 'true'),
      ('backup_schema_version', 'int', 'true'),
      ('cluster_version_display', 'varchar:OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH', 'true'),
      ('partition_count', 'int', 'true'),
      ('finish_partition_count', 'int', 'true'),
      ('is_mark_deleted', 'bool', 'true'),
      ('encryption_mode', 'varchar:OB_MAX_ENCRYPTION_MODE_LENGTH', 'false', 'None'),
      ('passwd', 'varchar:OB_MAX_PASSWORD_LENGTH', 'false', ''),
      ('backup_recovery_window', 'int'),
  ],
)

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12202',
  real_tenant_id=True,
  table_name = '__all_virtual_backup_backupset_task',
  keywords = all_def_keywords['__all_tenant_backup_backupset_task']))

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12203',
  real_tenant_id=True,
  table_name = '__all_virtual_pg_backup_backupset_task',
  keywords = all_def_keywords['__all_tenant_pg_backup_backupset_task']))

# 12204 used for __all_virtual_backup_backup_log_archive_status

def_table_schema(**gen_iterate_virtual_table_def(
  table_id = '12206',
  real_tenant_id=True,
  table_name = '__all_virtual_global_transaction',
  keywords = all_def_keywords['__all_tenant_global_transaction']))


################################################################################
# Oracle Virtual Table(15000,20000]
################################################################################
def_table_schema(**gen_agent_virtual_table_def('15001', all_def_keywords['__all_virtual_table']))
def_table_schema(**gen_agent_virtual_table_def('15002', all_def_keywords['__all_virtual_column']))
def_table_schema(**gen_agent_virtual_table_def('15003', all_def_keywords['__all_virtual_database']))
def_table_schema(**gen_agent_virtual_table_def('15004', all_def_keywords['__all_virtual_sequence_v2']))
def_table_schema(**gen_agent_virtual_table_def('15005', all_def_keywords['__all_virtual_part']))
def_table_schema(**gen_agent_virtual_table_def('15006', all_def_keywords['__all_virtual_sub_part']))
def_table_schema(**gen_agent_virtual_table_def('15007', all_def_keywords['__all_virtual_package']))
def_table_schema(**gen_agent_virtual_table_def('15008', all_def_keywords['__all_tenant_meta_table']))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15009', all_def_keywords['__all_virtual_sql_audit'])))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15010', all_def_keywords['__all_virtual_plan_stat'])))
def_table_schema(**gen_agent_virtual_table_def('15011', all_def_keywords['__all_virtual_sql_plan_statistics']))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15012', all_def_keywords['__all_virtual_plan_cache_plan_explain'])))
def_table_schema(**gen_agent_virtual_table_def('15013', all_def_keywords['__all_virtual_sequence_value']))
def_table_schema(**gen_agent_virtual_table_def('15014', all_def_keywords['__all_virtual_sequence_object']))
def_table_schema(**gen_agent_virtual_table_def('15015', all_def_keywords['__all_virtual_user']))
def_table_schema(**gen_agent_virtual_table_def('15016', all_def_keywords['__all_virtual_synonym']))
def_table_schema(**gen_agent_virtual_table_def('15017', all_def_keywords['__all_virtual_foreign_key']))
def_table_schema(**gen_agent_virtual_table_def('15018', all_def_keywords['__all_virtual_column_stat']))
def_table_schema(**gen_agent_virtual_table_def('15019', all_def_keywords['__all_virtual_column_statistic']))
def_table_schema(**gen_agent_virtual_table_def('15020', all_def_keywords['__all_virtual_partition_table']))
def_table_schema(**gen_agent_virtual_table_def('15021', all_def_keywords['__all_virtual_table_stat']))
def_table_schema(**gen_agent_virtual_table_def('15022', all_def_keywords['__all_virtual_recyclebin']))
def_table_schema(**gen_agent_virtual_table_def('15023', all_def_keywords['__tenant_virtual_outline']))
def_table_schema(**gen_agent_virtual_table_def('15024', all_def_keywords['__all_virtual_routine']))
def_table_schema(**gen_agent_virtual_table_def('15025', all_def_keywords['__all_virtual_tablegroup']))
def_table_schema(**gen_agent_virtual_table_def('15026', all_def_keywords['__all_privilege']))
def_table_schema(**gen_agent_virtual_table_def('15027', all_def_keywords['__all_virtual_sys_parameter_stat']))
def_table_schema(**gen_agent_virtual_table_def('15028', all_def_keywords['__tenant_virtual_table_index']))
def_table_schema(**gen_agent_virtual_table_def('15029', all_def_keywords['__tenant_virtual_charset']))
def_table_schema(**gen_agent_virtual_table_def('15030', all_def_keywords['__tenant_virtual_all_table']))
def_table_schema(**gen_agent_virtual_table_def('15031', all_def_keywords['__tenant_virtual_collation']))
def_table_schema(**gen_agent_virtual_table_def('15032', all_def_keywords['__all_virtual_foreign_key_column']))
def_table_schema(**gen_agent_virtual_table_def('15033', all_def_keywords['__all_server']))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15034', all_def_keywords['__all_virtual_plan_cache_stat'])))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15035', all_def_keywords['__all_virtual_processlist']))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15036', all_def_keywords['__all_virtual_session_wait'])))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15037', all_def_keywords['__all_virtual_session_wait_history'])))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15038', all_def_keywords['__all_virtual_memory_info'])))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15039', all_def_keywords['__all_virtual_tenant_memstore_info'])))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15040', all_def_keywords['__all_virtual_memstore_info'])))
def_table_schema(**gen_agent_virtual_table_def('15041', all_def_keywords['__all_virtual_server_memory_info']))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15042', all_def_keywords['__all_virtual_sesstat'])))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15043', all_def_keywords['__all_virtual_sysstat'])))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15044', all_def_keywords['__all_virtual_system_event'])))
def_table_schema(**gen_agent_virtual_table_def('15045', all_def_keywords['__all_virtual_tenant_memstore_allocator_info']))

def_table_schema(**gen_oracle_mapping_virtual_table_def('15046', all_def_keywords['__tenant_virtual_session_variable']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15047', all_def_keywords['__tenant_virtual_global_variable']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15048', all_def_keywords['__tenant_virtual_show_create_table']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15049', all_def_keywords['__tenant_virtual_show_create_procedure']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15050', all_def_keywords['__tenant_virtual_show_create_tablegroup']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15051', all_def_keywords['__tenant_virtual_privilege_grant']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15052', all_def_keywords['__tenant_virtual_table_column']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15053', all_def_keywords['__all_virtual_trace_log']))

def_table_schema(**gen_agent_virtual_table_def('15054',all_def_keywords['__tenant_virtual_concurrent_limit_sql']))
def_table_schema(**gen_agent_virtual_table_def('15055', all_def_keywords['__all_virtual_constraint']))
def_table_schema(**gen_agent_virtual_table_def('15056', all_def_keywords['__all_virtual_type']))
def_table_schema(**gen_agent_virtual_table_def('15057', all_def_keywords['__all_virtual_type_attr']))
def_table_schema(**gen_agent_virtual_table_def('15058', all_def_keywords['__all_virtual_coll_type']))
def_table_schema(**gen_agent_virtual_table_def('15059', all_def_keywords['__all_virtual_routine_param']))

def_table_schema(**gen_oracle_mapping_virtual_table_def('15060', all_def_keywords['__all_virtual_data_type']))
#discard 15061, rename to 15083
#def_table_schema(**gen_agent_virtual_table_def('15061', all_def_keywords['__all_virtual_tenant_parameter_stat']))
def_table_schema(**gen_sys_agent_virtual_table_def('15062', all_def_keywords['__all_table']));

def_table_schema(**gen_agent_virtual_table_def('15063', all_def_keywords['__all_sstable_checksum']))
def_table_schema(**gen_agent_virtual_table_def('15064', all_def_keywords['__all_virtual_partition_info']))
def_table_schema(**gen_agent_virtual_table_def('15065', all_def_keywords['__all_virtual_tenant_partition_meta_table']))
def_table_schema(**gen_agent_virtual_table_def('15066', all_def_keywords['__all_virtual_tenant_keystore']))
def_table_schema(**gen_agent_virtual_table_def('15071', all_def_keywords['__all_virtual_tenant_tablespace']))

def_table_schema(**gen_agent_virtual_table_def('15072', all_def_keywords['__all_virtual_tenant_profile']))
def_table_schema(**gen_agent_virtual_table_def('15073', all_def_keywords['__all_virtual_tenant_role_grantee_map']))
def_table_schema(**gen_agent_virtual_table_def('15074', all_def_keywords['__all_virtual_table_privilege']))

def_table_schema(**gen_agent_virtual_table_def('15075', all_def_keywords['__all_virtual_security_audit']))
def_table_schema(**gen_agent_virtual_table_def('15076', all_def_keywords['__all_virtual_security_audit_history']))

def_table_schema(**gen_agent_virtual_table_def('15079', all_def_keywords['__all_virtual_trigger']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15080', all_def_keywords['__all_virtual_px_worker_stat']))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15081', all_def_keywords['__all_virtual_ps_stat'])))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15082', all_def_keywords['__all_virtual_ps_item_info'])))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15083', all_def_keywords['__all_virtual_tenant_parameter_stat']))

## sql workarea
def_table_schema(**gen_agent_virtual_table_def('15084', all_def_keywords['__all_virtual_sql_workarea_history_stat']))
def_table_schema(**gen_agent_virtual_table_def('15085', all_def_keywords['__all_virtual_sql_workarea_active']))
def_table_schema(**gen_agent_virtual_table_def('15086', all_def_keywords['__all_virtual_sql_workarea_histogram']))
def_table_schema(**gen_agent_virtual_table_def('15087', all_def_keywords['__all_virtual_sql_workarea_memory_info']))
def_table_schema(**gen_agent_virtual_table_def('15088', all_def_keywords['__all_virtual_security_audit_record']))

#sysauth
def_table_schema(**gen_agent_virtual_table_def('15089', all_def_keywords['__all_virtual_sysauth']))
def_table_schema(**gen_agent_virtual_table_def('15090', all_def_keywords['__all_virtual_sysauth_history']))

#objauth
def_table_schema(**gen_agent_virtual_table_def('15091', all_def_keywords['__all_virtual_objauth']))
def_table_schema(**gen_agent_virtual_table_def('15092', all_def_keywords['__all_virtual_objauth_history']))


def_table_schema(**gen_agent_virtual_table_def('15093', all_def_keywords['__all_virtual_error']))
def_table_schema(**gen_agent_virtual_table_def('15094', all_def_keywords['__all_virtual_table_mgr']))

def_table_schema(**gen_agent_virtual_table_def('15095', all_def_keywords['__all_virtual_def_sub_part']))
def_table_schema(**gen_agent_virtual_table_def('15096', all_def_keywords['__all_virtual_object_type']))
def_table_schema(**gen_agent_virtual_table_def('15097', all_def_keywords['__all_virtual_server_schema_info']))

def_table_schema(**gen_agent_virtual_table_def('15098', all_def_keywords['__all_virtual_dblink']))
def_table_schema(**gen_agent_virtual_table_def('15099', all_def_keywords['__all_virtual_dblink_history']))
def_table_schema(**no_direct_access(gen_oracle_mapping_virtual_table_def('15100', all_def_keywords['__all_virtual_sql_plan_monitor'])))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15101', all_def_keywords['__all_virtual_sql_monitor_statname']))

def_table_schema(**gen_oracle_mapping_virtual_table_def('15102', all_def_keywords['__all_virtual_lock_wait_stat']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15103', all_def_keywords['__all_virtual_open_cursor']))

def_table_schema(**gen_agent_virtual_table_def('15104', all_def_keywords['__all_virtual_constraint_column']))
def_table_schema(**gen_agent_virtual_table_def('15105', all_def_keywords['__all_virtual_dependency']))
def_table_schema(**gen_agent_virtual_table_def('15106', all_def_keywords['__all_tenant_time_zone']))
def_table_schema(**gen_agent_virtual_table_def('15107', all_def_keywords['__all_tenant_time_zone_name']))
def_table_schema(**gen_agent_virtual_table_def('15108', all_def_keywords['__all_tenant_time_zone_transition']))
def_table_schema(**gen_agent_virtual_table_def('15109', all_def_keywords['__all_tenant_time_zone_transition_type']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15110', all_def_keywords['__tenant_virtual_object_definition']))

def_table_schema(**gen_sys_agent_virtual_table_def('15111', all_def_keywords['__all_routine_param']))
def_table_schema(**gen_sys_agent_virtual_table_def('15112', all_def_keywords['__all_type']))
def_table_schema(**gen_sys_agent_virtual_table_def('15113', all_def_keywords['__all_type_attr']))
def_table_schema(**gen_sys_agent_virtual_table_def('15114', all_def_keywords['__all_coll_type']))
def_table_schema(**gen_sys_agent_virtual_table_def('15115', all_def_keywords['__all_package']))
def_table_schema(**gen_sys_agent_virtual_table_def('15116', all_def_keywords['__all_tenant_trigger']))
def_table_schema(**gen_sys_agent_virtual_table_def('15117', all_def_keywords['__all_routine']))

def_table_schema(**gen_agent_virtual_table_def('15118', all_def_keywords['__all_tenant_global_transaction']))
def_table_schema(**gen_agent_virtual_table_def('15119', all_def_keywords['__all_acquired_snapshot']))

## define oracle virtual table that is mapping real table
## real table must has in_tenant_space=True if use gen_oracle_mapping_real_virtual_table_def
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15120', False, all_def_keywords['__all_table']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15121', False, all_def_keywords['__all_column']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15122', False, all_def_keywords['__all_database']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15123', False, all_def_keywords['__all_sequence_v2']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15124', False, all_def_keywords['__all_part']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15125', False, all_def_keywords['__all_sub_part']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15126', False, all_def_keywords['__all_package']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15127', False, all_def_keywords['__all_sequence_value']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15128', False, all_def_keywords['__all_sequence_object']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15129', False, all_def_keywords['__all_user']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15130', False, all_def_keywords['__all_synonym']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15131', False, all_def_keywords['__all_foreign_key']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15132', False, all_def_keywords['__all_column_stat']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15133', False, all_def_keywords['__all_column_statistic']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15134', False, all_def_keywords['__all_table_stat']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15135', False, all_def_keywords['__all_recyclebin']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15136', False, all_def_keywords['__all_routine']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15137', False, all_def_keywords['__all_tablegroup']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15138', False, all_def_keywords['__all_foreign_key_column']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15139', False, all_def_keywords['__all_constraint']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15140', False, all_def_keywords['__all_type']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15141', False, all_def_keywords['__all_type_attr']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15142', False, all_def_keywords['__all_coll_type']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15143', False, all_def_keywords['__all_routine_param']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15144', True, all_def_keywords['__all_tenant_partition_meta_table']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15145', False, all_def_keywords['__all_tenant_keystore']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15150', False, all_def_keywords['__all_tenant_tablespace']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15151', False, all_def_keywords['__all_tenant_profile']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15152', False, all_def_keywords['__all_tenant_role_grantee_map']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15153', False, all_def_keywords['__all_table_privilege']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15154', False, all_def_keywords['__all_tenant_security_audit']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15155', False, all_def_keywords['__all_tenant_security_audit_history']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15156', False, all_def_keywords['__all_tenant_trigger']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15157', False, all_def_keywords['__all_tenant_security_audit_record']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15158', False, all_def_keywords['__all_tenant_sysauth']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15159', False, all_def_keywords['__all_tenant_sysauth_history']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15160', False, all_def_keywords['__all_tenant_objauth']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15161', False, all_def_keywords['__all_tenant_objauth_history']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15162', False, all_def_keywords['__all_tenant_error']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15163', False, all_def_keywords['__all_def_sub_part']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15164', False, all_def_keywords['__all_tenant_object_type']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15165', False, all_def_keywords['__all_dblink']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15166', False, all_def_keywords['__all_dblink_history']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15167', False, all_def_keywords['__all_tenant_constraint_column']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15168', False, all_def_keywords['__all_tenant_dependency']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15169', True, all_def_keywords['__all_tenant_meta_table']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15170', True, all_def_keywords['__all_tenant_time_zone']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15171', True, all_def_keywords['__all_tenant_time_zone_name']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15172', True, all_def_keywords['__all_tenant_time_zone_transition']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15173', True, all_def_keywords['__all_tenant_time_zone_transition_type']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15174', True, all_def_keywords['__all_res_mgr_plan']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15175', True, all_def_keywords['__all_res_mgr_directive']))
def_table_schema(**gen_oracle_mapping_virtual_table_def('15176', all_def_keywords['__all_virtual_trans_lock_stat']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15177', True, all_def_keywords['__all_res_mgr_mapping_rule']))
def_table_schema(**gen_oracle_mapping_real_virtual_table_def('15179', True, all_def_keywords['__all_res_mgr_consumer_group']))



################################################################################
# System View (20000,30000]
# MySQL System View (20000, 25000]
# Oracle System View (25000, 30000]
################################################################################

def_table_schema(
  table_name     = 'gv$plan_cache_stat',
  table_id       = '20001',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  in_tenant_space = True,
  rowkey_columns = [],
  view_definition = """
  SELECT tenant_id,svr_ip,svr_port,sql_num,mem_used,mem_hold,access_count,
  hit_count,hit_rate,plan_num,mem_limit,hash_bucket,stmtkey_num
  FROM oceanbase.__all_virtual_plan_cache_stat WHERE
    is_serving_tenant(svr_ip, svr_port, effective_tenant_id())
    and (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),


  normal_columns = [
  ],
)

def_table_schema(
    table_name     = 'gv$plan_cache_plan_stat',
    table_id       = '20002',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """
    SELECT tenant_id,svr_ip,svr_port,plan_id,sql_id,type,is_bind_sensitive,is_bind_aware,db_id,statement,query_sql,special_params,param_infos, sys_vars,plan_hash,first_load_time,schema_version,merged_version,last_active_time,avg_exe_usec,slowest_exe_time,slowest_exe_usec,slow_count,hit_count,plan_size,executions,disk_reads,direct_writes,buffer_gets,application_wait_time,concurrency_wait_time,user_io_wait_time,rows_processed,elapsed_time,cpu_time,large_querys,delayed_large_querys,delayed_px_querys,outline_version,outline_id,outline_data,acs_sel_info,table_scan,evolution, evo_executions, evo_cpu_time, timeout_count, ps_stmt_id, sessid, temp_tables, is_use_jit,object_type,hints_info,hints_all_worked, pl_schema_id, is_batched_multi_stmt FROM oceanbase.__all_virtual_plan_stat WHERE is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),


    normal_columns = [
    ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'SCHEMATA',
  table_id       = '20003',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  view_definition = """
  SELECT 'def' AS CATALOG_NAME, DATABASE_NAME AS SCHEMA_NAME, 'utf8mb4' AS DEFAULT_CHARACTER_SET_NAME, 'utf8mb4_general_ci' AS DEFAULT_COLLATION_NAME, NULL AS SQL_PATH, 'NO' as DEFAULT_ENCRYPTION FROM oceanbase.__all_virtual_database a WHERE a.tenant_id = effective_tenant_id() and in_recyclebin = 0 and database_name != '__recyclebin'
""".replace("\n", " "),

  in_tenant_space = True,

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'CHARACTER_SETS',
  table_id       = '20004',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  view_definition = """
  SELECT CHARSET AS CHARACTER_SET_NAME, DEFAULT_COLLATION AS DEFAULT_COLLATE_NAME, DESCRIPTION, max_length AS MAXLEN FROM oceanbase.__tenant_virtual_charset
""".replace("\n", " "),

  in_tenant_space = True,

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'GLOBAL_VARIABLES',
  table_id       = '20005',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  view_definition = """
  SELECT `variable_name` as VARIABLE_NAME, `value` as VARIABLE_VALUE  FROM oceanbase.__tenant_virtual_global_variable
""".replace("\n", " "),

  in_tenant_space = True,

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  database_id   = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'STATISTICS',
  table_id       = '20006',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,
  view_definition = """
  SELECT 'def' as TABLE_CATALOG, table_schema AS TABLE_SCHEMA,
      `table` as TABLE_NAME, non_unique AS NON_UNIQUE, index_schema as INDEX_SCHEMA,
      key_name as INDEX_NAME, seq_in_index as SEQ_IN_INDEX, column_name as COLUMN_NAME,
      collation as COLLATION, cardinality as CARDINALITY, sub_part as SUB_PART,
      packed as PACKED, `null` as NULLABLE, index_type as INDEX_TYPE, COMMENT,
      index_comment as INDEX_COMMENT, is_visible as IS_VISIBLE FROM oceanbase.__tenant_virtual_table_index
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'VIEWS',
  table_id       = '20007',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  view_definition = """select /*+ READ_CONSISTENCY(WEAK) */ 'def' AS TABLE_CATALOG,
                   d.database_name as TABLE_SCHEMA,
                   t.table_name as TABLE_NAME,
                   t.view_definition as VIEW_DEFINITION,
                   'NONE' as CHECK_OPTION,
                   case t.view_is_updatable when 1 then 'YES' else 'NO' end as IS_UPDATABLE,
                   'NONE' as DEFINER,
                   'NONE' AS SECURITY_TYPE,
                   case t.collation_type when 45 then 'utf8mb4' else 'NONE' end AS CHARACTER_SET_CLIENT,
                   case t.collation_type when 45 then 'utf8mb4_general_ci' else 'NONE' end AS COLLATION_CONNECTION
                   from oceanbase.__all_virtual_table as t join oceanbase.__all_virtual_database as d on t.tenant_id = effective_tenant_id() and d.tenant_id = effective_tenant_id() and t.database_id = d.database_id
                   where (t.table_type = 1 or t.table_type = 4) and d.in_recyclebin = 0 and d.database_name != '__recyclebin' and d.database_name != 'information_schema' and d.database_name != 'oceanbase' and 0 = sys_privilege_check('table_acc', effective_tenant_id(), d.database_name, t.table_name)
""".replace("\n", " "),


  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'TABLES',
  table_id       = '20008',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  view_definition = """select /*+ READ_CONSISTENCY(WEAK), use_merge(b, c, d, e)*/ 'def' as TABLE_CATALOG,
                    b.database_name as TABLE_SCHEMA,
                    a.table_name as TABLE_NAME,
                    case when a.database_id & 0xFFFFFFFFFF = 2 then 'SYSTEM VIEW' when (a.table_type = 1 or a.table_type = 4) then 'VIEW' when a.table_type = 2 then 'SYSTEM TABLE' when a.table_type = 1 then 'INDEX' else 'BASE TABLE' end as TABLE_TYPE,
                    NULL as ENGINE,
                    NULL as VERSION,
                    NULL as ROW_FORMAT,
                    sum(c.row_count) as TABLE_ROWS,
                    case when sum(c.row_count) = 0 then 0 else sum(c.data_size)/sum(c.row_count) end as AVG_ROW_LENGTH,
                    sum(c.data_size) as DATA_LENGTH,
                    NULL as MAX_DATA_LENGTH,
                    NULL as INDEX_LENGTH,
                    NULL as DATA_FREE,
                    NULL as AUTO_INCREMENT,
                    a.gmt_create as CREATE_TIME,
                    a.gmt_modified as UPDATE_TIME,
                    NULL as CHECK_TIME,
                    d.collation as TABLE_COLLATION,
                    cast(NULL as unsigned) as CHECKSUM,
                    NULL as CREATE_OPTIONS,
                    a.comment as TABLE_COMMENT
                    from oceanbase.__all_virtual_table a
                    inner join oceanbase.__all_virtual_database b on a.database_id = b.database_id
                    left join oceanbase.__all_virtual_tenant_partition_meta_table c on a.table_id = c.table_id and c.tenant_id = effective_tenant_id() and a.tenant_id = c.tenant_id and c.role = 1
                    inner join oceanbase.__all_collation d on a.collation_type = d.id
                    where a.tenant_id = effective_tenant_id() and b.tenant_id = effective_tenant_id() and a.table_type != 5 and b.database_name != '__recyclebin' and b.in_recyclebin = 0 and 0 = sys_privilege_check('table_acc', effective_tenant_id(), b.database_name, a.table_name)
                    group by a.table_id, b.database_name, a.table_name, a.table_type, a.gmt_create, a.gmt_modified, d.collation, a.comment
""".replace("\n", " "),


  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'COLLATIONS',
  table_id       = '20009',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  view_definition = """select collation as COLLATION_NAME, charset as CHARACTER_SET_NAME, id as ID, `is_default` as IS_DEFAULT, is_compiled as IS_COMPILED, sortlen as SORTLEN from oceanbase.__tenant_virtual_collation
""".replace("\n", " "),

  in_tenant_space = True,

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'COLLATION_CHARACTER_SET_APPLICABILITY',
  table_id       = '20010',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  view_definition = """select collation as COLLATION_NAME, charset as CHARACTER_SET_NAME from oceanbase.__tenant_virtual_collation
""".replace("\n", " "),

  in_tenant_space = True,

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'PROCESSLIST',
  table_id       = '20011',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  view_definition = """SELECT id AS ID, user AS USER, host AS HOST, db AS DB, command AS COMMAND, time AS TIME, state AS STATE, info AS INFO FROM oceanbase.__all_virtual_processlist WHERE  is_serving_tenant(svr_ip, svr_port, effective_tenant_id())
""".replace("\n", " "),

  in_tenant_space = True,

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'KEY_COLUMN_USAGE',
  table_id       = '20012',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  view_definition = """(select /*+ READ_CONSISTENCY(WEAK) */ 'def' as CONSTRAINT_CATALOG,
                    c.database_name as  CONSTRAINT_SCHEMA,
                    'PRIMARY' as CONSTRAINT_NAME, 'def' as TABLE_CATALOG,
                    c.database_name as TABLE_SCHEMA,
                    a.table_name as TABLE_NAME,
                    b.column_name as COLUMN_NAME,
                    b.rowkey_position as ORDINAL_POSITION,
                    NULL as POSITION_IN_UNIQUE_CONSTRAINT,
                    NULL as REFERENCED_TABLE_SCHEMA,
                    NULL as REFERENCED_TABLE_NAME,
                    NULL as REFERENCED_COLUMN_NAME
                    from oceanbase.__all_virtual_table a
                    inner join oceanbase.__all_virtual_column b on a.table_id = b.table_id
                    inner join oceanbase.__all_virtual_database c on a.database_id = c.database_id
                    where a.tenant_id = effective_tenant_id() and b.tenant_id = effective_tenant_id() and c.tenant_id = effective_tenant_id() and c.in_recyclebin = 0 and c.database_name != '__recyclebin' and b.rowkey_position > 0 and b.column_id >= 16 and a.table_type != 5 and b.column_flags & (0x1 << 8) = 0)
                    union all (select /*+ READ_CONSISTENCY(WEAK) */ 'def' as CONSTRAINT_CATALOG,
                    d.database_name as CONSTRAINT_SCHEMA,
                    substr(a.table_name, 2 + length(substring_index(a.table_name,'_',4))) as CONSTRAINT_NAME,
                    'def' as TABLE_CATALOG,
                    d.database_name as TABLE_SCHEMA,
                    c.table_name as TABLE_NAME,
                    b.column_name as COLUMN_NAME,
                    b.index_position as ORDINAL_POSITION,
                    NULL as POSITION_IN_UNIQUE_CONSTRAINT,
                    NULL as REFERENCED_TABLE_SCHEMA,
                    NULL as REFERENCED_TABLE_NAME,
                    NULL as REFERENCED_COLUMN_NAME
                    from oceanbase.__all_virtual_table a
                    inner join oceanbase.__all_virtual_column b on a.table_id = b.table_id
                    inner join oceanbase.__all_virtual_table c on a.data_table_id = c.table_id
                    inner join oceanbase.__all_virtual_database d on c.database_id = d.database_id
                    where a.tenant_id = effective_tenant_id() and b.tenant_id = effective_tenant_id() and c.tenant_id = effective_tenant_id() and d.in_recyclebin = 0 and d.tenant_id = effective_tenant_id() and d.database_name != '__recyclebin' and a.table_type = 5 and a.index_type in (2, 4, 8) and b.index_position > 0)
                    union all (select /*+ READ_CONSISTENCY(WEAK) */ 'def' as CONSTRAINT_CATALOG,
                    d.database_name as CONSTRAINT_SCHEMA,
                    f.foreign_key_name as CONSTRAINT_NAME,
                    'def' as TABLE_CATALOG,
                    d.database_name as TABLE_SCHEMA,
                    t.table_name as TABLE_NAME,
                    c.column_name as COLUMN_NAME,
                    fc.position as ORDINAL_POSITION,
                    NULL as POSITION_IN_UNIQUE_CONSTRAINT, /* POSITION_IN_UNIQUE_CONSTRAINT is not supported now */
                    d2.database_name as REFERENCED_TABLE_SCHEMA,
                    t2.table_name as REFERENCED_TABLE_NAME,
                    c2.column_name as REFERENCED_COLUMN_NAME
                    from
                    oceanbase.__all_virtual_foreign_key f
                    inner join oceanbase.__all_virtual_table t on f.child_table_id = t.table_id
                    inner join oceanbase.__all_virtual_database d on t.database_id = d.database_id
                    inner join oceanbase.__all_virtual_foreign_key_column fc on f.foreign_key_id = fc.foreign_key_id
                    inner join oceanbase.__all_virtual_column c on fc.child_column_id = c.column_id and t.table_id = c.table_id
                    inner join oceanbase.__all_virtual_table t2 on f.parent_table_id = t2.table_id
                    inner join oceanbase.__all_virtual_database d2 on t2.database_id = d2.database_id
                    inner join oceanbase.__all_virtual_column c2 on fc.parent_column_id = c2.column_id and t2.table_id = c2.table_id
                    where f.tenant_id = effective_tenant_id() and fc.tenant_id = effective_tenant_id()
                          and t.tenant_id = effective_tenant_id() and d.tenant_id = effective_tenant_id() and c.tenant_id = effective_tenant_id()
                          and t2.tenant_id = effective_tenant_id() and d2.tenant_id = effective_tenant_id() and c2.tenant_id = effective_tenant_id())
                    """.replace("\n", " "),

  in_tenant_space = True,

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'DBA_OUTLINES',
  table_id       = '20013',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,

  view_definition = """select /*+ READ_CONSISTENCY(WEAK)*/ a.name as NAME, a.owner as OWNER, b.database_name as DB_NAME, NULL as CATEGORY,
                     a.used as USED, a.gmt_create as TIMESTAMP, a.version as VERSION, a.sql_text as SQL_TEXT, a.signature as SIGNATURE, a.compatible as COMPATIBLE,
                     a.enabled as ENABLED, a.format as FORMAT, a.outline_content as OUTLINE_CONTENT, a.outline_target as OUTLINE_TARGET, a.owner_id as OWNER_ID from oceanbase.__all_virtual_outline a inner join oceanbase.__all_virtual_database b on a.database_id = b.database_id
                     where a.tenant_id = effective_tenant_id() and b.tenant_id = effective_tenant_id() and b.in_recyclebin = 0
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  database_id     = 'OB_INFORMATION_SCHEMA_ID',
  table_name      = 'ENGINES',
  table_id        = '20014',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT *
    FROM oceanbase.__all_virtual_engine
""".replace("\n", " ")
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  database_id     = 'OB_INFORMATION_SCHEMA_ID',
  table_name      = 'ROUTINES',
  table_id        = '20015',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """ select
                            SPECIFIC_NAME,
                            'def' as ROUTINE_CATALOG,
                            db as ROUTINE_SCHEMA,
                            name as ROUTINE_NAME,
                            type as ROUTINE_TYPE,
                            '' as DATA_TYPE,
                            NULL as CHARACTER_MAXIMUM_LENGTH,
                            NULL as CHARACTER_OCTET_LENGTH,
                            NULL as NUMERIC_PRECISION,
                            NULL as NUMERIC_SCALE,
                            NULL as DATETIME_PRECISION,
                            NULL as CHARACTER_SET_NAME,
                            NULL as COLLATION_NAME,
                            NULL as DTD_IDENTIFIER,
                            'SQL' as ROUTINE_BODY,
                            body as ROUTINE_DEFINITION,
                            NULL as EXTERNAL_NAME,
                            NULL as EXTERNAL_LANGUAGE,
                            'SQL' as PARAMETER_STYLE,
                            IS_DETERMINISTIC,
                            SQL_DATA_ACCESS,
                            NULL as SQL_PATH,
                            SECURITY_TYPE,
                            CREATED,
                            modified as LAST_ALTERED,
                            SQL_MODE,
                            comment as ROUTINE_COMMENT,
                            DEFINER,
                            CHARACTER_SET_CLIENT,
                            COLLATION_CONNECTION,
                            collation_database as DATABASE_COLLATION
                            from mysql.proc;
""".replace("\n", " ")
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  table_name     = 'gv$session_event',
  table_id       = '21000',
  gm_columns = [],
  rowkey_columns = [],
  table_type = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select session_id as SID,
                   tenant_id as CON_ID,
                   svr_ip as SVR_IP,
                   svr_port as SVR_PORT,
                   event_id as EVENT_ID,
                   event as EVENT,
                   wait_class_id as WAIT_CLASS_ID,
                   `wait_class#` as `WAIT_CLASS#`,
                   wait_class as WAIT_CLASS,
                   total_waits as TOTAL_WAITS,
                   total_timeouts as TOTAL_TIMEOUTS,
                   time_waited as TIME_WAITED,
                   max_wait as MAX_WAIT,
                   average_wait as AVERAGE_WAIT,
                   time_waited_micro as TIME_WAITED_MICRO
                   from oceanbase.__all_virtual_session_event where
                   is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and
                   (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  table_name     = 'gv$session_wait',
  table_id       = '21001',
  gm_columns = [],
  rowkey_columns = [],
  table_type = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select session_id as SID,
                   tenant_id as CON_ID,
                   svr_ip as SVR_IP,
                   svr_port as SVR_PORT,
                   event as EVENT,
                   p1text as P1TEXT,
                   p1 as P1,
                   p2text as P2TEXT,
                   p2 as P2,
                   p3text as P3TEXT,
                   p3 as P3,
                   wait_class_id as WAIT_CLASS_ID,
                   `wait_class#` as `WAIT_CLASS#`,
                   wait_class as WAIT_CLASS,
                   state as STATE,
                   wait_time_micro as WAIT_TIME_MICRO,
                   time_remaining_micro as TIME_REMAINING_MICRO,
                   time_since_last_wait_micro as TIME_SINCE_LAST_WAIT_MICRO
                   from oceanbase.__all_virtual_session_wait where
                   (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  table_name     = 'gv$session_wait_history',
  table_id       = '21002',
  gm_columns = [],
  rowkey_columns = [],
  table_type = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select session_id as SID,
                   tenant_id as CON_ID,
                   svr_ip as SVR_IP,
                   svr_port as SVR_PORT,
                   `seq#` as `SEQ#`,
                   `event#` as `EVENT#`,
                   event as EVENT,
                   p1text as P1TEXT,
                   p1 as P1,
                   p2text as P2TEXT,
                   p2 as P2,
                   p3text as P3TEXT,
                   p3 as P3,
                   wait_time_micro as WAIT_TIME_MICRO,
                   time_since_last_wait_micro as TIME_SINCE_LAST_WAIT_MICRO,
                   wait_time as WAIT_TIME
                   from oceanbase.__all_virtual_session_wait_history where
                   is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and
                   (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  table_name     = 'gv$system_event',
  table_id       = '21003',
  gm_columns = [],
  rowkey_columns = [],
  table_type = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select tenant_id as CON_ID,
                   svr_ip as SVR_IP,
                   svr_port as SVR_PORT,
                   event_id as EVENT_ID,
                   event as EVENT,
                   wait_class_id as WAIT_CLASS_ID,
                   `wait_class#` as `WAIT_CLASS#`,
                   wait_class as WAIT_CLASS,
                   total_waits as TOTAL_WAITS,
                   total_timeouts as TOTAL_TIMEOUTS,
                   time_waited as TIME_WAITED,
                   average_wait as AVERAGE_WAIT,
                   time_waited_micro as TIME_WAITED_MICRO
                   from oceanbase.__all_virtual_system_event  where
                   is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and
                   (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  table_name     = 'gv$sesstat',
  table_id       = '21004',
  gm_columns = [],
  rowkey_columns = [],
  table_type = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select session_id as SID,
                   tenant_id as CON_ID,
                   svr_ip as SVR_IP,
                   svr_port as SVR_PORT,
                   `statistic#` as `STATISTIC#`,
                   value as VALUE
                   from oceanbase.__all_virtual_sesstat
                   where is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and can_visible = true and
                   (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  table_name     = 'gv$sysstat',
  table_id       = '21005',
  gm_columns = [],
  rowkey_columns = [],
  table_type = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select tenant_id as CON_ID,
                   svr_ip as SVR_IP,
                   svr_port as SVR_PORT,
                   `statistic#` as `STATISTIC#`,
                   value as VALUE,
                   stat_id as STAT_ID,
                   name as NAME,
                   class as CLASS
                   from oceanbase.__all_virtual_sysstat
                   where is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and can_visible = true and
                  (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  table_name     = 'v$statname',
  table_id       = '21006',
  gm_columns = [],
  rowkey_columns = [],
  in_tenant_space = True,
  table_type = 'SYSTEM_VIEW',
  view_definition = """select tenant_id as CON_ID,
                   stat_id as STAT_ID,
                   `statistic#` as `STATISTIC#`,
                   name as NAME,
                   display_name as DISPLAY_NAME,
                   class as CLASS
                   from oceanbase.__tenant_virtual_statname
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id  = 'OB_INVALID_ID',
  table_name     = 'v$event_name',
  table_id       = '21007',
  gm_columns = [],
  rowkey_columns = [],
  table_type = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select tenant_id as CON_ID,
                   event_id as EVENT_ID,
                   `event#` as `EVENT#`,
                   name as NAME,
                   display_name as DISPLAY_NAME,
                   parameter1 as PARAMETER1,
                   parameter2 as PARAMETER2,
                   parameter3 as PARAMETER3,
                   wait_class_id as WAIT_CLASS_ID,
                   `wait_class#` as `WAIT_CLASS#`,
                   wait_class as WAIT_CLASS
                   from oceanbase.__tenant_virtual_event_name
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  table_name      = 'v$session_event',
  table_id        = '21008',
  gm_columns      = [],
  rowkey_columns  = [],
  table_type      = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select SID,CON_ID,EVENT_ID,EVENT,WAIT_CLASS_ID,
                    `WAIT_CLASS#`,WAIT_CLASS,TOTAL_WAITS,TOTAL_TIMEOUTS,TIME_WAITED,
                     MAX_WAIT,AVERAGE_WAIT,TIME_WAITED_MICRO
                     from oceanbase.gv$session_event
                     where SVR_IP=host_ip() and SVR_PORT=rpc_port()
""".replace("\n", " "),

  normal_columns  = [],
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  table_name      = 'v$session_wait',
  table_id        = '21009',
  gm_columns      = [],
  rowkey_columns  = [],
  table_type      = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select SID,CON_ID,EVENT,P1TEXT,P1,
                     P2TEXT,P2,P3TEXT,P3,WAIT_CLASS_ID,
                     `WAIT_CLASS#`,WAIT_CLASS,STATE,WAIT_TIME_MICRO,TIME_REMAINING_MICRO,
                     TIME_SINCE_LAST_WAIT_MICRO
                     from oceanbase.gv$session_wait
                     where SVR_IP=host_ip() and SVR_PORT=rpc_port()
""".replace("\n", " "),

  normal_columns  = [],
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  table_name      = 'v$session_wait_history',
  table_id        = '21010',
  gm_columns      = [],
  rowkey_columns  = [],
  table_type      = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select SID,CON_ID,`SEQ#`,`EVENT#`,EVENT,
                     P1TEXT,P1,P2TEXT,P2,P3TEXT,
                     P3,WAIT_TIME_MICRO,TIME_SINCE_LAST_WAIT_MICRO,WAIT_TIME
                     from oceanbase.gv$session_wait_history
                     where SVR_IP=host_ip() and SVR_PORT=rpc_port()
""".replace("\n", " "),

  normal_columns  = [],
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  table_name      = 'v$sesstat',
  table_id        = '21011',
  gm_columns      = [],
  rowkey_columns  = [],
  table_type      = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select SID,CON_ID,`STATISTIC#`,VALUE from oceanbase.gv$sesstat
                     where SVR_IP=host_ip() and SVR_PORT=rpc_port()
""".replace("\n", " "),

  normal_columns  = [],
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  table_name      = 'v$sysstat',
  table_id        = '21012',
  gm_columns      = [],
  rowkey_columns  = [],
  table_type      = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select CON_ID,`STATISTIC#`,VALUE,STAT_ID,NAME,CLASS from oceanbase.gv$sysstat
                     where SVR_IP=host_ip() and SVR_PORT=rpc_port()
""".replace("\n", " "),

  normal_columns  = [],
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  table_name      = 'v$system_event',
  table_id        = '21013',
  gm_columns      = [],
  rowkey_columns  = [],
  table_type      = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select CON_ID,EVENT_ID,EVENT,WAIT_CLASS_ID,`WAIT_CLASS#`,
                     WAIT_CLASS,TOTAL_WAITS,TOTAL_TIMEOUTS,TIME_WAITED,AVERAGE_WAIT,
                     TIME_WAITED_MICRO
                     from oceanbase.gv$system_event
                     where SVR_IP=host_ip() and SVR_PORT=rpc_port()
""".replace("\n", " "),

  normal_columns  = [],
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  table_name      = 'gv$sql_audit',
  table_id        = '21014',
  gm_columns      = [],
  rowkey_columns  = [],
  table_type      = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select
                         svr_ip as SVR_IP,
                         svr_port as SVR_PORT,
                         request_id as REQUEST_ID,
                         execution_id as SQL_EXEC_ID,
                         trace_id as TRACE_ID,
                         session_id as SID,
                         client_ip as CLIENT_IP,
                         client_port as CLIENT_PORT,
                         tenant_id as TENANT_ID,
                         tenant_name as TENANT_NAME,
                         effective_tenant_id as EFFECTIVE_TENANT_ID,
                         user_id as USER_ID,
                         user_name as USER_NAME,
                         user_group as USER_GROUP,
                         user_client_ip as USER_CLIENT_IP,
                         db_id as DB_ID,
                         db_name as DB_NAME,
                         sql_id as SQL_ID,
                         query_sql as QUERY_SQL,
                         plan_id as PLAN_ID,
                         affected_rows as AFFECTED_ROWS,
                         return_rows as RETURN_ROWS,
                         partition_cnt as PARTITION_CNT,
                         ret_code as RET_CODE,
                         qc_id as QC_ID,
                         dfo_id as DFO_ID,
                         sqc_id as SQC_ID,
                         worker_id as WORKER_ID,
                         event as EVENT,
                         p1text as P1TEXT,
                         p1 as P1,
                         p2text as P2TEXT,
                         p2 as P2,
                         p3text as P3TEXT,
                         p3 as P3,
                         `level` as `LEVEL`,
                         wait_class_id as WAIT_CLASS_ID,
                         `wait_class#` as `WAIT_CLASS#`,
                         wait_class as WAIT_CLASS,
                         state as STATE,
                         wait_time_micro as WAIT_TIME_MICRO,
                         total_wait_time_micro as TOTAL_WAIT_TIME_MICRO,
                         total_waits as TOTAL_WAITS,
                         rpc_count as RPC_COUNT,
                         plan_type as PLAN_TYPE,
                         is_inner_sql as IS_INNER_SQL,
                         is_executor_rpc as IS_EXECUTOR_RPC,
                         is_hit_plan as IS_HIT_PLAN,
                         request_time as REQUEST_TIME,
                         elapsed_time as ELAPSED_TIME,
                         net_time as NET_TIME,
                         net_wait_time as NET_WAIT_TIME,
                         queue_time as QUEUE_TIME,
                         decode_time as DECODE_TIME,
                         get_plan_time as GET_PLAN_TIME,
                         execute_time as EXECUTE_TIME,
                         application_wait_time as APPLICATION_WAIT_TIME,
                         concurrency_wait_time as CONCURRENCY_WAIT_TIME,
                         user_io_wait_time as USER_IO_WAIT_TIME,
                         schedule_time as SCHEDULE_TIME,
                         row_cache_hit as ROW_CACHE_HIT,
                         bloom_filter_cache_hit as BLOOM_FILTER_CACHE_HIT,
                         block_cache_hit as BLOCK_CACHE_HIT,
                         block_index_cache_hit as BLOCK_INDEX_CACHE_HIT,
                         disk_reads as DISK_READS,
                         retry_cnt as RETRY_CNT,
                         table_scan as TABLE_SCAN,
                         consistency_level as CONSISTENCY_LEVEL,
                         memstore_read_row_count as MEMSTORE_READ_ROW_COUNT,
                         ssstore_read_row_count as SSSTORE_READ_ROW_COUNT,
                         request_memory_used as REQUEST_MEMORY_USED,
                         expected_worker_count as EXPECTED_WORKER_COUNT,
                         used_worker_count as USED_WORKER_COUNT,
                         sched_info as SCHED_INFO,
                         fuse_row_cache_hit as FUSE_ROW_CACHE_HIT,
                         ps_stmt_id as PS_STMT_ID,
                         transaction_hash as TRANSACTION_HASH,
                         request_type as REQUEST_TYPE,
                         is_batched_multi_stmt as IS_BATCHED_MULTI_STMT,
                         ob_trace_info as OB_TRACE_INFO,
                         plan_hash as PLAN_HASH,
                         lock_for_read_time as LOCK_FOR_READ_TIME,
                         wait_trx_migrate_time as WAIT_TRX_MIGRATE_TIME
                     from oceanbase.__all_virtual_sql_audit
                     where is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

  normal_columns  = [],
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  table_name      = 'gv$latch',
  table_id        = '21015',
  gm_columns      = [],
  rowkey_columns  = [],
  table_type      = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """select tenant_id as CON_ID,
                     svr_ip as SVR_IP,
                     svr_port as SVR_PORT,
                     addr as ADDR,
                     latch_id as `LATCH#`,
                     `level` as `LEVEL#`,
                     name as NAME,
                     hash as HASH,
                     gets as GETS,
                     misses as MISSES,
                     sleeps as SLEEPS,
                     immediate_gets as IMMEDIATE_GETS,
                     immediate_misses as IMMEDIATE_MISSES,
                     spin_gets as SPIN_GETS,
                     wait_time as WAIT_TIME from oceanbase.__all_virtual_latch where
                     is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and
                     (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

  normal_columns  = [],
)

def_table_schema(
  table_name      = 'gv$memory',
  table_id        = '21016',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
     TENANT_ID,
     svr_ip AS IP,
     svr_port AS PORT,
     mod_name AS CONTEXT,
     sum(COUNT) AS COUNT,
     sum(USED) AS USED,
     sum(ALLOC_COUNT) AS ALLOC_COUNT,
     sum(FREE_COUNT) AS FREE_COUNT
FROM
    oceanbase.__all_virtual_memory_info
WHERE
        (
            effective_tenant_id()=1
        OR
            tenant_id=effective_tenant_id()
        )
    AND
        mod_type='user'
    AND
        is_serving_tenant(svr_ip, svr_port, effective_tenant_id())
GROUP BY tenant_id, svr_ip, svr_port, mod_name
ORDER BY tenant_id, svr_ip, svr_port, mod_name
""".replace("\n", " "),

)

def_table_schema(
  table_name      = 'v$memory',
  table_id        = '21017',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    TENANT_ID, CONTEXT, COUNT, USED, ALLOC_COUNT, FREE_COUNT
FROM
    oceanbase.gv$memory
WHERE
        IP=HOST_IP()
    AND
        PORT=RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$memstore',
  table_id        = '21018',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    TENANT_ID,
    SVR_IP AS IP,
    SVR_PORT AS PORT,
    ACTIVE_MEMSTORE_USED AS ACTIVE,
    TOTAL_MEMSTORE_USED AS TOTAL,
    MAJOR_FREEZE_TRIGGER AS `FREEZE_TRIGGER`,
    MEMSTORE_LIMIT AS `MEM_LIMIT`,
    FREEZE_CNT
FROM
    oceanbase.__all_virtual_tenant_memstore_info
WHERE
        (EFFECTIVE_TENANT_ID()=1
    OR
        TENANT_ID=EFFECTIVE_TENANT_ID())
    AND
     is_serving_tenant(svr_ip, svr_port, effective_tenant_id())
""".replace("\n", " "),

)

def_table_schema(
  table_name      = 'v$memstore',
  table_id        = '21019',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    TENANT_ID,
    ACTIVE,
    TOTAL,
    `FREEZE_TRIGGER`,
    `MEM_LIMIT`
FROM
    oceanbase.gv$memstore
WHERE
        IP=HOST_IP()
    AND
        PORT=RPC_PORT()
""".replace("\n", " "),

)

def_table_schema(
  table_name      = 'gv$memstore_info',
  table_id        = '21020',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    TENANT_ID,
    SVR_IP AS IP,
    SVR_PORT AS PORT,
    table_id AS TABLE_ID,
    partition_idx AS PARTITION_ID,
    VERSION,
    BASE_VERSION,
    MULTI_VERSION_START,
    SNAPSHOT_VERSION,
    IS_ACTIVE,
    MEM_USED as USED,
    hash_item_count as HASH_ITEMS,
    btree_item_count as BTREE_ITEMS
FROM
    oceanbase.__all_virtual_memstore_info
WHERE
        (EFFECTIVE_TENANT_ID()=1
    OR
        TENANT_ID=EFFECTIVE_TENANT_ID())
    AND
      is_serving_tenant(svr_ip, svr_port, effective_tenant_id())
""".replace("\n", " "),

)

def_table_schema(
  table_name      = 'v$memstore_info',
  table_id        = '21021',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    TENANT_ID,
    IP,
    PORT,
    TABLE_ID,
    PARTITION_ID,
    VERSION,
    BASE_VERSION,
    MULTI_VERSION_START,
    SNAPSHOT_VERSION,
    IS_ACTIVE,
    USED,
    HASH_ITEMS,
    BTREE_ITEMS
FROM
    oceanbase.gv$memstore_info
WHERE
        IP=HOST_IP()
    AND
        PORT=RPC_PORT()
""".replace("\n", " "),

)

def_table_schema(
  table_name     = 'v$plan_cache_stat',
  table_id       = '21022',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  in_tenant_space = True,
  rowkey_columns = [],
  view_definition = """
  SELECT tenant_id,svr_ip,svr_port,sql_num,mem_used, mem_hold, access_count,hit_count,
  hit_rate,plan_num,mem_limit,hash_bucket,stmtkey_num
  FROM oceanbase.gv$plan_cache_stat WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT()
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
    table_name     = 'v$plan_cache_plan_stat',
    table_id       = '21023',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """
    SELECT tenant_id,svr_ip,svr_port,plan_id,sql_id,type,is_bind_sensitive,is_bind_aware,db_id,statement,query_sql,special_params,param_infos,sys_vars,plan_hash,first_load_time,schema_version,merged_version,last_active_time,avg_exe_usec,slowest_exe_time,slowest_exe_usec,slow_count,hit_count,plan_size,executions,disk_reads,direct_writes,buffer_gets,application_wait_time,concurrency_wait_time,user_io_wait_time,rows_processed,elapsed_time,cpu_time,large_querys,delayed_large_querys,delayed_px_querys,outline_version,outline_id,outline_data,acs_sel_info,table_scan,evolution, evo_executions, evo_cpu_time, timeout_count, ps_stmt_id, sessid, temp_tables, is_use_jit, object_type,hints_info,hints_all_worked, pl_schema_id,is_batched_multi_stmt FROM oceanbase.gv$plan_cache_plan_stat WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT()
""".replace("\n", " "),


    normal_columns = [
    ],
)

def_table_schema(
    table_name     = 'gv$plan_cache_plan_explain',
    table_id       = '21024',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """
    SELECT TENANT_ID,
                            SVR_IP as IP,
                            SVR_PORT as PORT,
                            PLAN_ID,
                            PLAN_DEPTH,
                            PLAN_LINE_ID,
                            OPERATOR,
                            NAME,
                            ROWS,
                            COST,
                            PROPERTY
                            FROM oceanbase.__all_virtual_plan_cache_plan_explain
                            WHERE is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and (EFFECTIVE_TENANT_ID()=1 or TENANT_ID=EFFECTIVE_TENANT_ID())
""".replace("\n", " "),


    normal_columns = [
    ],
)

def_table_schema(
    table_name     = 'v$plan_cache_plan_explain',
    table_id       = '21025',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """
    SELECT * FROM oceanbase.gv$plan_cache_plan_explain WHERE IP =HOST_IP() AND PORT = RPC_PORT()
""".replace("\n", " "),


    normal_columns = [
    ],
)

def_table_schema(
    tablegroup_id  = 'OB_INVALID_ID',
    table_name     = 'v$sql_audit',
    table_id       = '21026',
    gm_columns     = [],
    rowkey_columns = [],
    table_type     = 'SYSTEM_VIEW',
    in_tenant_space = True,
    view_definition = """SELECT * FROM oceanbase.gv$sql_audit WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT()
""".replace("\n", " "),

    normal_columns = [
    ],
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  table_name      = 'v$latch',
  table_id        = '21027',
  gm_columns      = [],
  rowkey_columns  = [],
  table_type      = 'SYSTEM_VIEW',
  in_tenant_space = True,
  view_definition = """SELECT * FROM oceanbase.gv$latch where SVR_IP=host_ip() and SVR_PORT=rpc_port()
""".replace("\n", " "),

  normal_columns  = [],
)

def_table_schema(
  table_name      = 'gv$obrpc_outgoing',
  table_id        = '21028',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    TENANT_ID,
    SVR_IP AS IP,
    SVR_PORT AS PORT,
    PCODE,
    PCODE_NAME,
    COUNT,
    TOTAL_TIME,
    TOTAL_SIZE,
    FAILURE,
    TIMEOUT,
    SYNC,
    ASYNC,
    LAST_TIMESTAMP
FROM
    oceanbase.__all_virtual_obrpc_stat
WHERE
    is_serving_tenant(svr_ip, svr_port, effective_tenant_id())
    AND
        (EFFECTIVE_TENANT_ID()=1
    OR
        TENANT_ID=EFFECTIVE_TENANT_ID())
""".replace("\n", " "),

)

def_table_schema(
  table_name      = 'v$obrpc_outgoing',
  table_id        = '21029',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    *
FROM
    gv$obrpc_outgoing
WHERE
        IP=HOST_IP()
    AND
        PORT=RPC_PORT()
""".replace("\n", " "),

)

def_table_schema(
  table_name      = 'gv$obrpc_incoming',
  table_id        = '21030',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    TENANT_ID,
    SVR_IP AS IP,
    SVR_PORT AS PORT,
    PCODE,
    PCODE_NAME,
    ICOUNT AS COUNT,
    ISIZE AS TOTAL_SIZE,
    NET_TIME,
    WAIT_TIME,
    QUEUE_TIME,
    PROCESS_TIME,
    ILAST_TIMESTAMP AS LAST_TIMESTAMP
FROM
    oceanbase.__all_virtual_obrpc_stat
WHERE
        EFFECTIVE_TENANT_ID()=1
    OR
        TENANT_ID=EFFECTIVE_TENANT_ID()
""".replace("\n", " "),

)


def_table_schema(
  table_name      = 'v$obrpc_incoming',
  table_id        = '21031',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    *
FROM
    oceanbase.gv$obrpc_incoming
WHERE
        IP=HOST_IP()
    AND
        PORT=RPC_PORT()
""".replace("\n", " "),

)

def_table_schema(
    table_name     = 'gv$sql',
    table_id       = '21032',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """SELECT tenant_id AS CON_ID,
        svr_ip AS SVR_IP,
        svr_port AS SVR_PORT,
        plan_id AS PLAN_ID,
        sql_id AS SQL_ID,
        type AS TYPE,
        statement AS SQL_TEXT,
        plan_hash AS PLAN_HASH_VALUE,
        first_load_time AS FIRST_LOAD_TIME,
        last_active_time AS LAST_ACTIVE_TIME,
        avg_exe_usec AS AVG_EXE_USEC,
        slowest_exe_time AS SLOWEST_EXE_TIME,
        slowest_exe_usec as SLOWEST_EXE_USEC,
        slow_count as SLOW_COUNT,
        hit_count as HIT_COUNT,
        plan_size as PLAN_SIZE,
        executions as EXECUTIONS,
        disk_reads as DISK_READS,
        direct_writes as DIRECT_WRITES,
        buffer_gets as BUFFER_GETS,
        application_wait_time as APPLICATION_WAIT_TIME,
        concurrency_wait_time as CONCURRENCY_WAIT_TIME,
        user_io_wait_time as USER_IO_WAIT_TIME,
        rows_processed as ROWS_PROCESSED,
        elapsed_time as ELAPSED_TIME,
        cpu_time as CPU_TIME
        FROM oceanbase.__all_virtual_plan_stat
        WHERE is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),


    normal_columns = [
    ],
)

def_table_schema(
    table_name     = 'v$sql',
    table_id       = '21033',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """SELECT * FROM oceanbase.gv$sql
        WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT()
""".replace("\n", " "),


    normal_columns = [
    ],
)

def_table_schema(
    table_name     = 'gv$sql_monitor',
    table_id       = '21034',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """SELECT tenant_id as CON_ID,
        request_id as SQL_EXEC_ID,
        job_id as JOB_ID,
        task_id as TASK_ID,
        svr_ip as SVR_IP,
        svr_port as SVR_PORT,
        sql_exec_start as SQL_EXEC_START,
        plan_id as PLAN_ID,
        scheduler_ip as SCHEDULER_IP,
        scheduler_port as SCHEDULER_PORT,
        monitor_info as MONITOR_INFO,
        extend_info as EXTEND_INFO FROM oceanbase.__all_virtual_sql_monitor
        WHERE is_serving_tenant(svr_ip, svr_port, effective_tenant_id())
        and (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

    normal_columns = [
    ],
)
def_table_schema(
    table_name     = 'v$sql_monitor',
    table_id       = '21035',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """SELECT * from oceanbase.gv$sql_monitor  WHERE svr_ip=HOST_IP() AND
    svr_port=RPC_Port()
""".replace("\n", " "),

    normal_columns = [
    ],
)

def_table_schema(
    table_name     = 'gv$sql_plan_monitor',
    table_id       = '21036',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """SELECT
          TENANT_ID as CON_ID,
          REQUEST_ID,
          NULL `KEY`,
          NULL STATUS,
          SVR_IP,
          SVR_PORT,
          TRACE_ID,
          FIRST_REFRESH_TIME,
          LAST_REFRESH_TIME,
          FIRST_CHANGE_TIME,
          LAST_CHANGE_TIME,
          NULL REFRESH_COUNT,
          NULL SID,
          THREAD_ID  PROCESS_NAME,
          NULL SQL_ID,
          NULL SQL_EXEC_START,
          NULL SQL_EXEC_ID,
          NULL SQL_PLAN_HASH_VALUE,
          NULL SQL_CHILD_ADDRESS,
          NULL PLAN_PARENT_ID,
          PLAN_LINE_ID,
          PLAN_OPERATION,
          NULL PLAN_OPTIONS,
          NULL PLAN_OBJECT_OWNER,
          NULL PLAN_OBJECT_NAME,
          NULL PLAN_OBJECT_TYPE,
          PLAN_DEPTH,
          NULL PLAN_POSITION,
          NULL PLAN_COST,
          NULL PLAN_CARDINALITY,
          NULL PLAN_BYTES,
          NULL PLAN_TIME,
          NULL PLAN_PARTITION_START,
          NULL PLAN_PARTITION_STOP,
          NULL PLAN_CPU_COST,
          NULL PLAN_IO_COST,
          NULL PLAN_TEMP_SPACE,
          STARTS,
          OUTPUT_ROWS,
          NULL IO_INTERCONNECT_BYTES,
          NULL PHYSICAL_READ_REQUESTS,
          NULL PHYSICAL_READ_BYTES,
          NULL PHYSICAL_WRITE_REQUESTS,
          NULL PHYSICAL_WRITE_BYTES,
          NULL WORKAREA_MEM,
          NULL WORKAREA_MAX_MEM,
          NULL WORKAREA_TEMPSEG,
          NULL WORKAREA_MAX_TEMPSEG,
          NULL OTHERSTAT_GROUP_ID,
          OTHERSTAT_1_ID,
          NULL OTHERSTAT_1_TYPE,
          OTHERSTAT_1_VALUE,
          OTHERSTAT_2_ID,
          NULL OTHERSTAT_2_TYPE,
          OTHERSTAT_2_VALUE,
          OTHERSTAT_3_ID,
          NULL OTHERSTAT_3_TYPE,
          OTHERSTAT_3_VALUE,
          OTHERSTAT_4_ID,
          NULL OTHERSTAT_4_TYPE,
          OTHERSTAT_4_VALUE,
          OTHERSTAT_5_ID,
          NULL OTHERSTAT_5_TYPE,
          OTHERSTAT_5_VALUE,
          OTHERSTAT_6_ID,
          NULL OTHERSTAT_6_TYPE,
          OTHERSTAT_6_VALUE,
          OTHERSTAT_7_ID,
          NULL OTHERSTAT_7_TYPE,
          OTHERSTAT_7_VALUE,
          OTHERSTAT_8_ID,
          NULL OTHERSTAT_8_TYPE,
          OTHERSTAT_8_VALUE,
          OTHERSTAT_9_ID,
          NULL OTHERSTAT_9_TYPE,
          OTHERSTAT_9_VALUE,
          OTHERSTAT_10_ID,
          NULL OTHERSTAT_10_TYPE,
          OTHERSTAT_10_VALUE,
          NULL OTHER_XML,
          NULL PLAN_OPERATION_INACTIVE
        FROM oceanbase.__all_virtual_sql_plan_monitor
        WHERE is_serving_tenant(svr_ip, svr_port, effective_tenant_id())
        and (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

    normal_columns = [
    ],
)

def_table_schema(
    table_name     = 'v$sql_plan_monitor',
    table_id       = '21037',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """
    SELECT * from oceanbase.gv$sql_plan_monitor  WHERE svr_ip=HOST_IP() AND
    svr_port=RPC_Port()
""".replace("\n", " "),

    normal_columns = [
    ],
),

def_table_schema(
  table_name      = 'USER_RECYCLEBIN',
  table_id        = '21038',
  database_id     = 'OB_INFORMATION_SCHEMA_ID',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    OBJECT_NAME,
    ORIGINAL_NAME,
    case TYPE when 1 then 'TABLE' when 2 then 'INDEX' when 3 then 'VIEW' when 4 then 'DATABASE' else 'INVALID' end as TYPE,
    gmt_create as CREATETIME
FROM
    oceanbase.__all_virtual_recyclebin where tenant_id = effective_tenant_id()
""".replace("\n", " "),

)

def_table_schema(
    table_name     = 'gv$outline',
    table_id       = '21039',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """
    SELECT * from oceanbase.__tenant_virtual_outline
""".replace("\n", " "),

    normal_columns = [
    ],
),

def_table_schema(
    table_name     = 'gv$concurrent_limit_sql',
    table_id       = '21040',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """
    SELECT * from oceanbase.__tenant_virtual_concurrent_limit_sql
""".replace("\n", " "),

    normal_columns = [
    ],
),

def_table_schema(
    table_name     = 'gv$sql_plan_statistics',
    table_id       = '21041',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """SELECT tenant_id as CON_ID,
        svr_ip as SVR_IP,
        svr_port as SVR_PORT,
        plan_id as PLAN_ID,
        operation_id as OPERATION_ID,
        executions as EXECUTIONS,
        output_rows as OUTPUT_ROWS,
        input_rows as INPUT_ROWS,
        rescan_times as RESCAN_TIMES,
        buffer_gets as BUFFER_GETS,
        disk_reads as DISK_READS,
        disk_writes as DISK_WRITES,
        elapsed_time as ELAPSED_TIME,
        extend_info1 as EXTEND_INFO1,
        extend_info2 as EXTEND_INFO2
        FROM oceanbase.__all_virtual_sql_plan_statistics
        WHERE is_serving_tenant(svr_ip, svr_port, effective_tenant_id())
        and tenant_id = effective_tenant_id()
""".replace("\n", " "),

    normal_columns = [
    ],
),
def_table_schema(
    table_name     = 'v$sql_plan_statistics',
    table_id       = '21042',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """SELECT * from oceanbase.gv$sql_plan_statistics WHERE svr_ip=HOST_IP() AND
    svr_port=RPC_Port()
""".replace("\n", " "),

    normal_columns = [
    ],
),

def_table_schema(
  table_name      = 'gv$server_memstore',
  table_id        = '21043',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
    SVR_IP AS IP,
    SVR_PORT AS PORT,
    ACTIVE_MEMSTORE_USED AS ACTIVE,
    TOTAL_MEMSTORE_USED AS TOTAL,
    MAJOR_FREEZE_TRIGGER AS `FREEZE_TRIGGER`,
    MEMSTORE_LIMIT AS `MEM_LIMIT`
FROM
    oceanbase.__all_virtual_server_memory_info
WHERE
        (EFFECTIVE_TENANT_ID()=1)
""".replace("\n", " "),

),

def_table_schema(
    table_name     = 'gv$unit_load_balance_event_history',
    table_id       = '21044',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    normal_columns = [],
    view_definition = """
SELECT
  gmt_create,
  substring_index(substring_index(value3, 'zone:"', -1), '"', 1) AS zone,
  substring_index(substring_index(value1, 'tid:', -1), ',', 1) AS table_id,
  substring_index(substring_index(value1, 'partition_id:', -1), ',', 1) AS partition_id,
  substring_index(substring_index(value1, 'data_size:', -1), '}', 1) AS data_size,
  substring_index(substring_index(value3, 'replica_type:', -1), '}', 1) AS replica_type,
  substring_index(substring_index(value2, '"', -2), ':', 1) AS src_ip,
  substring_index(substring_index(value2, '"', 2), ':', -1) AS src_port,
  substring_index(substring_index(value3, 'unit_id:', -1), ',', 1)  AS dest_unit_id,
  substring_index(substring_index(value3, 'server:"', -1), ':', 1) AS dest_ip,
  substring_index(substring_index(value3, '"', 4), ':', -1) AS dest_port,
  IF (value5 = '"0.0.0.0"' , "0.0.0.0", substring_index(substring_index(value5, '"', -2), ':', 1)) AS data_src_ip,
  IF (value5 = '"0.0.0.0"', "0", substring_index(substring_index(value5, '"', 2), ':', -1)) AS data_src_port,
  value4 AS result_code,
  CASE value4
     when "-4012" then "OB_TIMEOUT"
     when "-4013" then "OB_ALLOCATE_MEMORY_FAILED"
     when "-4018" then "OB_ENTRY_NOT_EXIST"
     when "-4023" then "OB_EAGAIN"
     when "-4070" then "OB_INVALID_DATA"
     when "-4184" then "OB_CS_OUTOF_DISK_SPACE"
     when "-4109" then "OB_STATE_NOT_MATCH"
     when "-4209" then "OB_LEADER_NOT_EXIST"
     when "-4552" then "OB_REBALANCE_TASK_CANT_EXEC"
     when "-4551" then "OB_SERVER_MIGRATE_IN_DENIED"
     when "-4600" then "OB_DATA_SOURCE_NOT_EXIST"
     else "" END AS result_str,
  value6 AS comment,
  rs_svr_ip,
  rs_svr_port
 FROM
  oceanbase.__all_rootservice_event_history
 WHERE
  module = 'balancer' and event = 'finish_migrate_replica'
""".replace("\n", " "),

)

def_table_schema(
  table_name      = 'gv$tenant',
  table_id        = '21045',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT tenant_id,
           tenant_name,
           zone_list,
           primary_zone,
           collation_type,
           info,
           read_only,
           locality
    FROM oceanbase.__all_tenant
    WHERE effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$database',
  table_id        = '21046',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT t.tenant_id,
           t.tenant_name,
           d.database_id,
           d.database_name,
           IF(d.zone_list = '' OR d.zone_list is NULL, t.zone_list, d.zone_list) AS zone_list,
           IF(d.primary_zone = '' OR d.primary_zone is NULL, t.primary_zone, d.primary_zone) AS primary_zone,
           IF(d.collation_type is NULL, t.collation_type, d.collation_type) AS collation_type,
           d.comment,
           d.read_only,
           d.default_tablegroup_id,
           d.in_recyclebin
    FROM oceanbase.__all_virtual_database AS d
    LEFT JOIN oceanbase.__all_tenant AS t ON d.tenant_id = t.tenant_id
    WHERE effective_tenant_id() = 1 OR t.tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$table',
  table_id        = '21047',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT t.tenant_id,
           a.tenant_name,
           t.table_id,
           t.table_name,
           d.database_id,
           d.database_name,
           t.tablegroup_id,
           tg.tablegroup_name,
           t.table_type,
           IF(t.zone_list = '' OR t.zone_list is NULL, d.zone_list, t.zone_list) AS zone_list,
           IF(t.primary_zone = '' OR t.primary_zone is NULL, d.primary_zone, t.primary_zone) AS primary_zone,
           IF(t.collation_type is NULL, d.collation_type, t.collation_type) AS collation_type,
           IF(t.locality = '' OR t.locality is NULL, a.locality, t.locality) AS locality,
           t.schema_version,
           t.read_only,
           t.comment,
           t.index_status,
           t.index_type,
           t.part_level,
           t.part_func_type,
           t.part_func_expr,
           t.part_num,
           t.sub_part_func_type,
           t.sub_part_func_expr,
           t.sub_part_num,
           t.dop,
           t.auto_part,
           t.auto_part_size
    FROM oceanbase.__all_virtual_table AS t
    LEFT JOIN oceanbase.__all_tenant AS a ON t.tenant_id = a.tenant_id
    LEFT JOIN oceanbase.gv$database AS d ON t.database_id = d.database_id
    LEFT JOIN oceanbase.__all_virtual_tablegroup AS tg ON t.tablegroup_id = tg.tablegroup_id
    WHERE effective_tenant_id() = 1 OR t.tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$unit',
  table_id        = '21048',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT a.unit_id,
           b.unit_config_id,
           b.name as unit_config_name,
           c.resource_pool_id,
           c.name as resource_pool_name,
           a.zone,
           t.tenant_id,
           t.tenant_name,
           a.svr_ip,
           a.svr_port,
           a.migrate_from_svr_ip,
           a.migrate_from_svr_port,
           b.max_cpu,
           b.min_cpu,
           b.max_memory,
           b.min_memory,
           b.max_iops,
           b.min_iops,
           b.max_disk_size,
           b.max_session_num
    FROM
    (
      (SELECT unit_id, zone, resource_pool_id, svr_ip, svr_port, migrate_from_svr_ip, migrate_from_svr_port
       FROM oceanbase.__all_unit)
      UNION
      (SELECT unit_id, zone, resource_pool_id, migrate_from_svr_ip as svr_ip, migrate_from_svr_port as svr_port,
              '' as migrate_from_svr_ip, 0 as migrate_from_svr_port
       FROM oceanbase.__all_unit
       WHERE migrate_from_svr_ip != '')
    ) AS a
    LEFT JOIN oceanbase.__all_resource_pool AS c ON a.resource_pool_id = c.resource_pool_id
    LEFT JOIN oceanbase.__all_unit_config AS b ON c.unit_config_id = b.unit_config_id
    LEFT JOIN oceanbase.__all_tenant AS t ON c.tenant_id = t.tenant_id
    WHERE effective_tenant_id() = 1 OR c.tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$unit',
  table_id        = '21049',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT *
    FROM oceanbase.gv$unit
    WHERE svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$partition',
  table_id        = '21050',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT p.tenant_id,
           p.table_id,
           t.tablegroup_id,
           p.partition_id,
           p.svr_ip,
           p.svr_port,
           p.sql_port,
           p.unit_id,
           p.partition_cnt,
           p.zone,
           p.role,
           p.member_list,
           p.row_count,
           p.data_size,
           p.data_version,
           p.partition_checksum,
           p.data_checksum,
           p.row_checksum,
           p.column_checksum,
           p.rebuild,
           p.replica_type,
           p.required_size,
           p.status,
           p.is_restore,
           p.quorum
    FROM oceanbase.__all_virtual_partition_table as p
    LEFT JOIN oceanbase.gv$table AS t ON p.table_id = t.table_id
    WHERE effective_tenant_id() = 1 OR p.tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$partition',
  table_id        = '21051',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT *
    FROM oceanbase.gv$partition
    WHERE svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$lock_wait_stat',
  table_id        = '21052',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT t.tenant_id as TENANT_ID,
           l.table_id as TABLE_ID,
           t.table_name as TABLE_NAME,
           l.rowkey as ROWKEY,
           l.svr_ip as SVR_IP,
           l.svr_port as SVR_PORT,
           l.session_id as SESSION_ID,
           l.need_wait as NEED_WAIT,
           l.recv_ts as RECV_TS,
           l.lock_ts as LOCK_TS,
           l.abs_timeout as ABS_TIMEOUT,
           l.try_lock_times as TRY_LOCK_TIMES,
           l.time_after_recv as TIME_AFTER_RECV
    FROM oceanbase.__all_virtual_lock_wait_stat as l
    LEFT JOIN oceanbase.gv$table AS t ON l.table_id = t.table_id
    WHERE effective_tenant_id() = 1 OR t.tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$lock_wait_stat',
  table_id        = '21053',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT *
    FROM oceanbase.gv$lock_wait_stat
    WHERE svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
  database_id    = 'OB_MYSQL_SCHEMA_ID',
  table_name      = 'time_zone',
  table_id        = '21054',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT time_zone_id as Time_zone_id,
           use_leap_seconds as Use_leap_seconds
    FROM oceanbase.__all_tenant_time_zone
""".replace("\n", " ")

)

def_table_schema(
  database_id    = 'OB_MYSQL_SCHEMA_ID',
  table_name      = 'time_zone_name',
  table_id        = '21055',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT name as Name,
           time_zone_id as Time_zone_id
    FROM oceanbase.__all_tenant_time_zone_name
""".replace("\n", " ")
)

def_table_schema(
  database_id    = 'OB_MYSQL_SCHEMA_ID',
  table_name      = 'time_zone_transition',
  table_id        = '21056',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT time_zone_id as Time_zone_id,
           transition_time as Transition_time,
           transition_type_id as Transition_type_id
    FROM oceanbase.__all_tenant_time_zone_transition
""".replace("\n", " ")
)

def_table_schema(
  database_id    = 'OB_MYSQL_SCHEMA_ID',
  table_name      = 'time_zone_transition_type',
  table_id        = '21057',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT time_zone_id as Time_zone_id,
           transition_type_id as Transition_type_id,
           offset as Offset,
           is_dst as Is_DST,
           abbreviation as Abbreviation
    FROM oceanbase.__all_tenant_time_zone_transition_type
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$session_longops',
  table_id        = '21059',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT sid as SID,
           op_name as OPNAME,
           target as TARGET,
           svr_ip as SVR_IP,
           svr_port as SVR_PORT,
           start_time as START_TIME,
           elapsed_time/1000000 as ELAPSED_SECONDS,
           remaining_time as TIME_REMAINING,
           last_update_time as LAST_UPDATE_TIME,
           message as MESSAGE
    FROM oceanbase.__all_virtual_long_ops_status
    WHERE effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$session_longops',
  table_id        = '21060',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT *
    FROM oceanbase.gv$session_longops
    WHERE svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$tenant_memstore_allocator_info',
  table_id        = '21064',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT svr_ip as SVR_IP,
           svr_port as SVR_PORT,
           tenant_id as TENANT_ID,
           table_id as TABLE_ID,
           partition_id as PARTITION_ID,
           mt_base_version as MT_BASE_VERSION,
           retire_clock as RETIRE_CLOCK,
           mt_is_frozen as MT_IS_FROZEN,
           mt_protection_clock as MT_PROTECTION_CLOCK,
           mt_snapshot_version as MT_SNAPSHOT_VERSION
    FROM oceanbase.__all_virtual_tenant_memstore_allocator_info
    WHERE effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
    ORDER BY SVR_IP, SVR_PORT, TENANT_ID, TABLE_ID, PARTITION_ID, MT_BASE_VERSION
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$tenant_memstore_allocator_info',
  table_id        = '21065',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT *
    FROM oceanbase.gv$tenant_memstore_allocator_info
    WHERE svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$tenant_sequence_object',
  table_id        = '21066',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT *
    FROM oceanbase.__all_virtual_sequence_object
    WHERE tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

# 21067 is abandoned

def_table_schema(
  tablegroup_id = 'OB_INVALID_ID',
  database_id    = 'OB_INFORMATION_SCHEMA_ID',
  table_name     = 'COLUMNS',
  table_id       = '21068',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  rowkey_columns = [],
  view_definition = """SELECT TABLE_CATALOG,
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    COLUMN_NAME,
                    ORDINAL_POSITION,
                    COLUMN_DEFAULT,
                    IS_NULLABLE,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    CHARACTER_OCTET_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    DATETIME_PRECISION,
                    CHARACTER_SET_NAME,
                    COLLATION_NAME,
                    COLUMN_TYPE,
                    COLUMN_KEY,
                    EXTRA,
                    PRIVILEGES,
                    COLUMN_COMMENT,
                    GENERATION_EXPRESSION
  		    FROM OCEANBASE.__ALL_VIRTUAL_INFORMATION_COLUMNS where 0 = sys_privilege_check('table_acc', effective_tenant_id(), table_schema, table_name)""",
  in_tenant_space = True,
  normal_columns = [ ],
)

def_table_schema(
  table_name      = 'gv$minor_merge_info',
  table_id        = '21069',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
    A.SVR_IP AS SVR_IP,
    A.SVR_PORT AS SVR_PORT,
    A.VALUE1 AS TENANT_ID,
    A.VALUE2 AS FREEZE_SNAPSHOT,
    A.GMT_CREATE AS START_TIME,
    B.GMT_CREATE AS FINISH_TIME
  FROM
    (SELECT
       SVR_IP,
       SVR_PORT,
       VALUE1,
       VALUE2,
       GMT_CREATE
     FROM
       OCEANBASE.__ALL_SERVER_EVENT_HISTORY
     WHERE
       EVENT = 'minor merge start'
       AND (EFFECTIVE_TENANT_ID() = 1 OR VALUE1 = EFFECTIVE_TENANT_ID())) A
    LEFT JOIN
    (SELECT
       SVR_IP,
       SVR_PORT,
       VALUE1,
       VALUE2,
       GMT_CREATE
     FROM
       OCEANBASE.__ALL_SERVER_EVENT_HISTORY
     WHERE
       EVENT = 'minor merge finish'
       AND (EFFECTIVE_TENANT_ID() = 1 OR VALUE1 = EFFECTIVE_TENANT_ID())) B
    ON
      A.SVR_IP = B.SVR_IP AND A.SVR_PORT = B.SVR_PORT AND A.VALUE1 = B.VALUE1 AND A.VALUE2 = B.VALUE2
  ORDER BY
    SVR_IP, SVR_PORT, TENANT_ID, FREEZE_SNAPSHOT
""".replace("\n", " ")
)

# 21070 for v$minor_merge_info is absoleted from 2_2_3_release
# def_table_schema(
#   table_name      = 'v$minor_merge_info',
#   table_id        = '21070',
#   table_type      = 'SYSTEM_VIEW',
#   rowkey_columns  = [],
#   normal_columns  = [],
#   gm_columns      = [],
#   in_tenant_space = True,
#   view_definition = """
#     SELECT *
#     FROM OCEANBASE.gv$minor_merge_info
#     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
# """.replace("\n", " ")
# )

def_table_schema(
  table_name      = 'gv$tenant_px_worker_stat',
  table_id        = '21071',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    select
      session_id,
      tenant_id,
      svr_ip,
      svr_port,
      trace_id,
      qc_id,
      sqc_id,
      worker_id,
      dfo_id,
      start_time
    from oceanbase.__all_virtual_px_worker_stat
    where effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
    order by session_id, svr_ip, svr_port
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$tenant_px_worker_stat',
  table_id        = '21072',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    select
      session_id,
      tenant_id,
      svr_ip,
      svr_port,
      trace_id,
      qc_id,
      sqc_id,
      worker_id,
      dfo_id,
      start_time
    from oceanbase.gv$tenant_px_worker_stat
    where svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$partition_audit',
  table_id        = '21073',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT *
    FROM oceanbase.__all_virtual_partition_audit
    WHERE effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$partition_audit',
  table_id        = '21074',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT *
    FROM oceanbase.gv$partition_audit
    WHERE svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
  table_name = 'v$ob_cluster',
  table_id = '21075',
  table_type = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = False,
  view_definition = """
      SELECT cluster_id,
             cluster_name,
             created,
             cluster_role,
             cluster_status,
             `switchover#`,
             switchover_status,
             switchover_info,
             current_scn,
             standby_became_primary_scn,
             primary_cluster_id,
             protection_mode,
             protection_level,
             redo_transport_options
      FROM oceanbase.__all_virtual_cluster
      """.replace("\n", " ")
)

def_table_schema(
  table_name     = 'gv$ps_stat',
  table_id       = '21079',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  in_tenant_space = True,
  rowkey_columns = [],
  view_definition = """
  SELECT tenant_id, svr_ip, svr_port, stmt_count,
         hit_count, access_count, mem_hold
  FROM oceanbase.__all_virtual_ps_stat
  WHERE is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and
        (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
    table_name     = 'v$ps_stat',
    table_id       = '21080',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """
  SELECT tenant_id, svr_ip, svr_port, stmt_count,
         hit_count, access_count, mem_hold
  FROM oceanbase.gv$ps_stat
  WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT()
""".replace("\n", " "),


    normal_columns = [
    ],
)

def_table_schema(
  table_name     = 'gv$ps_item_info',
  table_id       = '21081',
  table_type = 'SYSTEM_VIEW',
  gm_columns = [],
  in_tenant_space = True,
  rowkey_columns = [],
  view_definition = """
  SELECT tenant_id, svr_ip, svr_port, stmt_id,
         db_id, ps_sql, param_count, stmt_item_ref_count,
         stmt_info_ref_count, mem_hold, stmt_type, checksum, expired
  FROM oceanbase.__all_virtual_ps_item_info
  WHERE is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) and
        (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),

  normal_columns = [
  ],
)

def_table_schema(
    table_name     = 'v$ps_item_info',
    table_id       = '21082',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    view_definition = """
  SELECT tenant_id, svr_ip, svr_port, stmt_id,
         db_id, ps_sql, param_count, stmt_item_ref_count,
         stmt_info_ref_count, mem_hold, stmt_type, checksum, expired
  FROM oceanbase.gv$ps_item_info
  WHERE svr_ip=HOST_IP() AND svr_port=RPC_PORT()
""".replace("\n", " "),


    normal_columns = [
    ],
)


def_table_schema(
  table_name      = 'gv$sql_workarea',
  table_id        = '21083',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    select
      cast(null as binary(8)) as address,
      cast(null as signed) as hash_value,
      sql_id,
      cast(null as signed) as child_number,
      cast(null as binary(8)) as workarea_address,
      operation_type,
      operation_id,
      policy,
      estimated_optimal_size,
      estimated_onepass_size,
      last_memory_used,
      last_execution,
      last_degree,
      total_executions,
      optimal_executions,
      onepass_executions,
      multipasses_executions,
      active_time,
      max_tempseg_size,
      last_tempseg_size,
      tenant_id as con_id
    from oceanbase.__all_virtual_sql_workarea_history_stat
    where effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$sql_workarea',
  table_id        = '21084',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    select
      cast(null as binary(8)) as address,
      cast(null as signed) as hash_value,
      sql_id,
      cast(null as signed) as child_number,
      cast(null as binary(8)) as workarea_address,
      operation_type,
      operation_id,
      policy,
      estimated_optimal_size,
      estimated_onepass_size,
      last_memory_used,
      last_execution,
      last_degree,
      total_executions,
      optimal_executions,
      onepass_executions,
      multipasses_executions,
      active_time,
      max_tempseg_size,
      last_tempseg_size,
      tenant_id as con_id
    from oceanbase.__all_virtual_sql_workarea_history_stat
    where (effective_tenant_id() = 1 OR tenant_id = effective_tenant_id())
    and svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$sql_workarea_active',
  table_id        = '21085',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    select
      cast(null as signed) as sql_hash_value,
      sql_id,
      cast(null as date) as sql_exec_start,
      sql_exec_id,
      cast(null as binary(8)) as workarea_address,
      operation_type,
      operation_id,
      policy,
      sid,
      cast(null as signed) as qcinst_id,
      cast(null as signed) as qcsid,
      active_time,
      work_area_size,
      expect_size,
      actual_mem_used,
      max_mem_used,
      number_passes,
      tempseg_size,
      cast(null as char(20)) as tablespace,
      cast(null as signed) as `segrfno#`,
      cast(null as signed) as `segblk#`,
      tenant_id as con_id
    from oceanbase.__all_virtual_sql_workarea_active
    where effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$sql_workarea_active',
  table_id        = '21086',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    select
      cast(null as signed) as sql_hash_value,
      sql_id,
      cast(null as date) as sql_exec_start,
      sql_exec_id,
      cast(null as binary(8)) as workarea_address,
      operation_type,
      operation_id,
      policy,
      sid,
      cast(null as signed) as qcinst_id,
      cast(null as signed) as qcsid,
      active_time,
      work_area_size,
      expect_size,
      actual_mem_used,
      max_mem_used,
      number_passes,
      tempseg_size,
      cast(null as char(20)) as tablespace,
      cast(null as signed) as `segrfno#`,
      cast(null as signed) as `segblk#`,
      tenant_id as con_id
    from oceanbase.__all_virtual_sql_workarea_active
    where (effective_tenant_id() = 1 OR tenant_id = effective_tenant_id())
    and svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$sql_workarea_histogram',
  table_id        = '21087',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    select
      low_optimal_size,
      high_optimal_size,
      optimal_executions,
      onepass_executions,
      multipasses_executions,
      total_executions,
      tenant_id as con_id
    from oceanbase.__all_virtual_sql_workarea_histogram
    where effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$sql_workarea_histogram',
  table_id        = '21088',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    select
      low_optimal_size,
      high_optimal_size,
      optimal_executions,
      onepass_executions,
      multipasses_executions,
      total_executions,
      tenant_id as con_id
    from oceanbase.__all_virtual_sql_workarea_histogram
    where (effective_tenant_id() = 1 OR tenant_id = effective_tenant_id())
    and svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$ob_sql_workarea_memory_info',
  table_id        = '21089',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    select
      max_workarea_size,
      workarea_hold_size,
      max_auto_workarea_size,
      mem_target,
      total_mem_used,
      global_mem_bound,
      drift_size,
      workarea_count,
      manual_calc_count
    from oceanbase.__all_virtual_sql_workarea_memory_info
    where effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$ob_sql_workarea_memory_info',
  table_id        = '21090',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    select
      max_workarea_size,
      workarea_hold_size,
      max_auto_workarea_size,
      mem_target,
      total_mem_used,
      global_mem_bound,
      drift_size,
      workarea_count,
      manual_calc_count
    from oceanbase.__all_virtual_sql_workarea_memory_info
    where (effective_tenant_id() = 1 OR tenant_id = effective_tenant_id())
    and svr_ip = host_ip() AND svr_port = rpc_port()
""".replace("\n", " ")
)

def_table_schema(
table_name      = 'gv$plan_cache_reference_info',
table_id        = '21097',
table_type      = 'SYSTEM_VIEW',
rowkey_columns  = [],
normal_columns  = [],
gm_columns      = [],
in_tenant_space = True,
view_definition = """
SELECT SVR_IP,
SVR_PORT,
TENANT_ID,
PC_REF_PLAN_LOCAL,
PC_REF_PLAN_REMOTE,
PC_REF_PLAN_DIST,
PC_REF_PLAN_ARR,
PC_REF_PL,
PC_REF_PL_STAT,
PLAN_GEN,
CLI_QUERY,
OUTLINE_EXEC,
PLAN_EXPLAIN,
ASYN_BASELINE,
LOAD_BASELINE,
PS_EXEC,
GV_SQL,
PL_ANON,
PL_ROUTINE,
PACKAGE_VAR,
PACKAGE_TYPE,
PACKAGE_SPEC,
PACKAGE_BODY,
PACKAGE_RESV,
GET_PKG,
INDEX_BUILDER,
PCV_SET,
PCV_RD,
PCV_WR,
PCV_GET_PLAN_KEY,
PCV_GET_PL_KEY,
PCV_EXPIRE_BY_USED,
PCV_EXPIRE_BY_MEM
FROM oceanbase.__all_virtual_plan_cache_stat WHERE
IS_SERVING_TENANT(SVR_IP, SVR_PORT, EFFECTIVE_TENANT_ID())
AND (TENANT_ID = EFFECTIVE_TENANT_ID() OR EFFECTIVE_TENANT_ID() = 1)
""".replace("\n", " ")
)

def_table_schema(
table_name      = 'v$plan_cache_reference_info',
table_id        = '21098',
table_type      = 'SYSTEM_VIEW',
rowkey_columns  = [],
normal_columns  = [],
gm_columns      = [],
in_tenant_space = True,
view_definition = """
SELECT * FROM oceanbase.gv$plan_cache_reference_info
WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT()
"""
)

def_table_schema(
  table_name      = 'v$ob_timestamp_service',
  table_id        = '21099',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT tenant_id as tenant_id,
           case when ts_type=0 then 'Local'
                when ts_type=1 then 'Global'
                when ts_type=2 then 'HA Global'
           ELSE NULL END as ts_type,
           ts_value as ts_value
    FROM oceanbase.__all_virtual_timestamp_service where (effective_tenant_id() = 1 OR tenant_id = effective_tenant_id())
""".replace("\n", " ")
)

def_table_schema(
 table_name      = 'gv$sstable',
 table_id        = '21100',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
SELECT
  M.SVR_IP,
  M.SVR_PORT,
  M.TABLE_TYPE,
  M.TABLE_ID,
  T.TABLE_NAME,
  T.TENANT_ID,
  M.PARTITION_ID,
  M.INDEX_ID,
  M.BASE_VERSION,
  M.MULTI_VERSION_START,
  M.SNAPSHOT_VERSION,
  M.START_LOG_TS,
  M.END_LOG_TS,
  M.MAX_LOG_TS,
  M.VERSION,
  M.LOGICAL_DATA_VERSION,
  M.SIZE,
  M.IS_ACTIVE,
  M.REF,
  M.WRITE_REF,
  M.TRX_COUNT,
  M.PENDING_LOG_PERSISTING_ROW_CNT,
  M.UPPER_TRANS_VERSION,
  M.CONTAIN_UNCOMMITTED_ROW
FROM
  oceanbase.__all_virtual_table_mgr M JOIN oceanbase.__all_virtual_table T ON M.TABLE_ID = T.TABLE_ID
WHERE
  effective_tenant_id() = 1 OR T.tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
 table_name      = 'v$sstable',
 table_id        = '21101',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
SELECT
  M.TABLE_TYPE,
  M.TABLE_ID,
  T.TABLE_NAME,
  T.TENANT_ID,
  M.PARTITION_ID,
  M.INDEX_ID,
  M.BASE_VERSION,
  M.MULTI_VERSION_START,
  M.SNAPSHOT_VERSION,
  M.START_LOG_TS,
  M.END_LOG_TS,
  M.MAX_LOG_TS,
  M.VERSION,
  M.LOGICAL_DATA_VERSION,
  M.SIZE,
  M.IS_ACTIVE,
  M.REF,
  M.WRITE_REF,
  M.TRX_COUNT,
  M.PENDING_LOG_PERSISTING_ROW_CNT,
  M.UPPER_TRANS_VERSION,
  M.CONTAIN_UNCOMMITTED_ROW
FROM
  oceanbase.__all_virtual_table_mgr M JOIN oceanbase.__all_virtual_table T ON M.TABLE_ID = T.TABLE_ID
WHERE
  M.SVR_IP=HOST_IP()
AND
  M.SVR_PORT=RPC_PORT()
AND
  effective_tenant_id() = 1 OR T.tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_ARCHIVELOG_SUMMARY',
  table_id        = '21102',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
   SELECT
    INCARNATION,
    LOG_ARCHIVE_ROUND,
    TENANT_ID,
    STATUS,
    START_PIECE_ID,
    BACKUP_PIECE_ID,
    CASE
      WHEN time_to_usec(MIN_FIRST_TIME) = 0
        THEN ''
      ELSE
        MIN_FIRST_TIME
      END AS MIN_FIRST_TIME,
    CASE
      WHEN time_to_usec(MAX_NEXT_TIME) = 0
        THEN ''
      ELSE
        MAX_NEXT_TIME
      END AS MAX_NEXT_TIME,
    INPUT_BYTES,
    OUTPUT_BYTES,
    ROUND(OUTPUT_BYTES / INPUT_BYTES, 2) AS COMPRESSION_RATIO,
    CASE
        WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN INPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN INPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')
        END  AS INPUT_BYTES_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')
        END  AS OUTPUT_BYTES_DISPLAY
    FROM
(
select tenant_id,
       incarnation,
       log_archive_round,
       min_first_time,
       max_next_time,
       input_bytes,
       output_bytes,
       deleted_input_bytes,
       deleted_output_bytes,
       pg_count,
       'STOP' as status,
       start_piece_id,
       backup_piece_id
       from oceanbase.__all_backup_log_archive_status_history 
union
select tenant_id,
       incarnation,
       log_archive_round,
       min_first_time,
       max_next_time,
       input_bytes,
       output_bytes,
       deleted_input_bytes,
       deleted_output_bytes,
       pg_count ,
       status,
       start_piece_id,
       backup_piece_id
       from oceanbase.__all_backup_log_archive_status_v2 where status != 'STOP')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_JOB_DETAILS',
  table_id        = '21103',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    INCARNATION,
    TENANT_ID,
    BACKUP_SET_ID AS BS_KEY,
    BACKUP_TYPE,
    ENCRYPTION_MODE,
    START_TIME,
    END_TIME,
    INPUT_BYTES,
    OUTPUT_BYTES,
    DEVICE_TYPE AS OUTPUT_DEVICE_TYPE,
    ROUND((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000,0) AS ELAPSED_SECONDES,
    ROUND(OUTPUT_BYTES / INPUT_BYTES, 2) AS COMPRESSION_RATIO,
    INPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000) AS INPUT_BYTES_PER_SEC,
    OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000) AS OUTPUT_BYTES_PER_SEC,
    CASE
        WHEN STATUS = 'FINISH' AND RESULT != 0
            THEN 'FAILED'
        WHEN STATUS = 'FINISH' AND RESULT = 0
            THEN 'COMPLETED'
        ELSE
            'RUNNING'
        END AS STATUS,
    CASE
        WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN INPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN INPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')
        END  AS INPUT_BYTES_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')
        END  AS OUTPUT_BYTES_DISPLAY,
    CASE
        WHEN INPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000) >= 1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES / ((END_TIME - START_TIME)/1000/1000) /1024/1024/1024,2), 'GB/S')
        ELSE
            CONCAT(ROUND(INPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000) /1024/1024,2), 'MB/S')
        END  AS INPUT_BYTES_PER_SEC_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000) >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000)/1024/1024/1024,2), 'GB/S')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000)/1024/1024,2), 'MB/S')
        END  AS OUTPUT_BYTES_PER_SEC_DISPLAY,
    TIMEDIFF(END_TIME, START_TIME) AS TIME_TAKEN_DISPLAY,
    START_REPLAY_LOG_TS,
    DATE
    FROM oceanbase.__all_virtual_backup_task
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_SET_DETAILS',
  table_id        = '21104',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    INCARNATION,
    TENANT_ID,
    BACKUP_SET_ID AS BS_KEY,
    '0' AS COPY_ID,
    BACKUP_TYPE,
    ENCRYPTION_MODE,
    START_TIME,
    END_TIME AS COMPLETION_TIME,
    ROUND((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000,0) AS ELAPSED_SECONDES,
    'NO' AS KEEP,
    '' AS KEEP_UNTIL,
    DEVICE_TYPE,
    'NO' AS COMPRESSED,
    OUTPUT_BYTES,
    OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000)  AS  OUTPUT_RATE_BYTES,
    ROUND(OUTPUT_BYTES / INPUT_BYTES, 2) AS COMPRESSION_RATIO,
    CASE
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')
        END  AS OUTPUT_BYTES_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000) >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000)/1024/1024/1024,2), 'GB/S')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000)/1024/1024,2), 'MB/S')
        END  AS OUTPUT_RATE_BYTES_DISPLAY,
    TIMEDIFF(END_TIME, START_TIME) AS TIME_TAKEN_DISPLAY,
    CASE
        WHEN IS_MARK_DELETED = 1
            THEN 'DELETING'
        WHEN RESULT != 0
            THEN 'FAILED'
        ELSE
            'COMPLETED'
        END AS STATUS
    FROM oceanbase.__all_backup_task_history
    WHERE status = 'FINISH' 
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_SET_EXPIRED',
  table_id        = '21105',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    INCARNATION,
    TENANT_ID,
    BACKUP_SET_ID AS BS_KEY,
    '0' AS COPY_ID,
    BACKUP_TYPE,
    ENCRYPTION_MODE,
    START_TIME,
    END_TIME AS COMPLETION_TIME,
    ROUND((END_TIME - START_TIME)/1000/1000,0) AS ELAPSED_SECONDES,
    'NO' AS KEEP,
    '' AS KEEP_UNTIL,
    DEVICE_TYPE,
    'NO' AS COMPRESSED,
    OUTPUT_BYTES,
    OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000)  AS  OUTPUT_RATE_BYTES,
    ROUND(OUTPUT_BYTES / INPUT_BYTES, 2) AS COMPRESSION_RATIO,
    CASE
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')
        END  AS OUTPUT_BYTES_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000) >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000)/1024/1024/1024,2), 'GB/S')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES / ((TIME_TO_USEC(END_TIME) - TIME_TO_USEC(START_TIME))/1000/1000)/1024/1024,2), 'MB/S')
        END  AS OUTPUT_RATE_BYTES_DISPLAY,
    TIMEDIFF(END_TIME, START_TIME) AS TIME_TAKEN_DISPLAY
    FROM oceanbase.__all_backup_task_history
    WHERE INCARNATION = 0
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_PROGRESS',
  table_id        = '21106',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    INCARNATION,
    BACKUP_SET_ID AS BS_KEY,
    BACKUP_TYPE,
    TENANT_ID,
    PARTITION_COUNT,
    MACRO_BLOCK_COUNT,
    FINISH_PARTITION_COUNT,
    FINISH_MACRO_BLOCK_COUNT,
    INPUT_BYTES,
    OUTPUT_BYTES,
    START_TIME,
    END_TIME AS COMPLETION_TIME,
    CASE
        WHEN STATUS = 'FINISH' AND RESULT != 0
            THEN 'FAILED'
        WHEN STATUS = 'FINISH' AND RESULT = 0
            THEN 'COMPLETED'
        ELSE
            'RUNNING'
        END AS STATUS
    FROM oceanbase.__all_virtual_backup_task
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_ARCHIVELOG_PROGRESS',
  table_id        = '21107',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    INCARNATION,
    TENANT_ID,
    LOG_ARCHIVE_ROUND,
    CUR_PIECE_ID,
    SVR_IP,
    SVR_PORT,
    TABLE_ID,
    PARTITION_ID,
    usec_to_time(log_archive_start_ts) as MIN_FIRST_TIME,
    usec_to_time(log_archive_cur_ts) as MAX_NEXT_TIME,
    CASE
        WHEN log_archive_status = 1
            THEN 'STOP'
        WHEN log_archive_status = 2
            THEN 'BEGINNING'
        WHEN log_archive_status = 3
            THEN 'DOING'
        WHEN log_archive_status = 4
            THEN 'STOPPING'
        WHEN log_archive_status = 5
            THEN 'INTERRUPTED'
        WHEN log_archive_status = 6
            THEN 'MIXED'
        ELSE
            'INVALID'
        END as STATUS
    FROM oceanbase.__all_virtual_pg_backup_log_archive_status
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_CLEAN_HISTORY',
  table_id        = '21108',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    TENANT_ID,
    JOB_ID AS BS_KEY,
    COPY_ID,
    START_TIME,
    END_TIME,
    INCARNATION,
    TYPE,
    STATUS,
    PARAMETER,
    ERROR_MSG,
    COMMENT
    FROM oceanbase.__all_backup_clean_info_history
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_TASK_CLEAN_HISTORY',
  table_id        = '21109',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    TENANT_ID,
    INCARNATION,
    BACKUP_SET_ID AS BS_KEY,
    COPY_ID,
    BACKUP_TYPE,
    PARTITION_COUNT,
    MACRO_BLOCK_COUNT,
    FINISH_PARTITION_COUNT,
    FINISH_MACRO_BLOCK_COUNT,
    INPUT_BYTES,
    OUTPUT_BYTES,
    START_TIME,
    END_TIME AS COMPLETION_TIME,
    STATUS
    FROM oceanbase.__all_backup_task_clean_history
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_RESTORE_PROGRESS',
  table_id        = '21110',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
      JOB_ID,
      EXTERNAL_JOB_ID,
      TENANT_ID,
      TENANT_NAME,
      BACKUP_TENANT_ID,
      BACKUP_TENANT_NAME,
      BACKUP_CLUSTER_ID,
      BACKUP_CLUSTER_NAME,
      WHITE_LIST,
      STATUS,
      START_TIME,
      COMPLETION_TIME,
      PARTITION_COUNT,
      MACRO_BLOCK_COUNT,
      FINISH_PARTITION_COUNT,
      FINISH_MACRO_BLOCK_COUNT,
      RESTORE_START_TIMESTAMP,
      RESTORE_FINISH_TIMESTAMP,
      RESTORE_CURRENT_TIMESTAMP,
      BACKUP_SET_LIST,
      BACKUP_PIECE_LIST,
      INFO
    FROM oceanbase.__all_restore_progress
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_RESTORE_HISTORY',
  table_id        = '21111',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
      JOB_ID,
      EXTERNAL_JOB_ID,
      TENANT_ID,
      TENANT_NAME,
      BACKUP_TENANT_ID,
      BACKUP_TENANT_NAME,
      BACKUP_CLUSTER_ID,
      BACKUP_CLUSTER_NAME,
      WHITE_LIST,
      STATUS,
      START_TIME,
      COMPLETION_TIME,
      PARTITION_COUNT,
      MACRO_BLOCK_COUNT,
      FINISH_PARTITION_COUNT,
      FINISH_MACRO_BLOCK_COUNT,
      RESTORE_START_TIMESTAMP,
      RESTORE_FINISH_TIMESTAMP,
      RESTORE_CURRENT_TIMESTAMP,
      BACKUP_SET_LIST,
      BACKUP_PIECE_LIST,
      INFO      
    FROM oceanbase.__all_restore_history
""".replace("\n", " ")
)

def_table_schema(
 table_name      = 'gv$server_schema_info',
 table_id        = '21112',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
SELECT
  SVR_IP,
  SVR_PORT,
  TENANT_ID,
  REFRESHED_SCHEMA_VERSION,
  RECEIVED_SCHEMA_VERSION,
  SCHEMA_COUNT,
  SCHEMA_SIZE,
  MIN_SSTABLE_SCHEMA_VERSION
FROM
  oceanbase.__all_virtual_server_schema_info
WHERE
  effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
 table_name      = 'v$server_schema_info',
 table_id        = '21113',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
SELECT
  TENANT_ID,
  REFRESHED_SCHEMA_VERSION,
  RECEIVED_SCHEMA_VERSION,
  SCHEMA_COUNT,
  SCHEMA_SIZE,
  MIN_SSTABLE_SCHEMA_VERSION
FROM
  oceanbase.__all_virtual_server_schema_info
WHERE
  SVR_IP=HOST_IP()
AND
  SVR_PORT=RPC_PORT()
AND
  effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_CKPT_HISTORY',
  table_id        = '21114',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
    A.SVR_IP AS SVR_IP,
    A.SVR_PORT AS SVR_PORT,
    A.VALUE1 AS TENANT_ID,
    A.VALUE2 AS CHECKPOINT_SNAPSHOT,
    A.VALUE3 AS CHECKPOINT_TYPE,
    A.VALUE4 AS CHECKPOINT_CLUSTER_VERSION,
    A.GMT_CREATE AS START_TIME,
    B.GMT_CREATE AS FINISH_TIME
  FROM
    (SELECT
       SVR_IP,
       SVR_PORT,
       EVENT,
       VALUE1,
       VALUE2,
       VALUE3,
       VALUE4,
       GMT_CREATE
     FROM
       OCEANBASE.__ALL_SERVER_EVENT_HISTORY
     WHERE
       (EVENT = 'minor merge start' OR EVENT = 'write checkpoint start')
       AND (EFFECTIVE_TENANT_ID() = 1 OR VALUE1 = EFFECTIVE_TENANT_ID())) A
    LEFT JOIN
    (SELECT
       SVR_IP,
       SVR_PORT,
       EVENT,
       VALUE1,
       VALUE2,
       GMT_CREATE
     FROM
       OCEANBASE.__ALL_SERVER_EVENT_HISTORY
     WHERE
       (EVENT = 'minor merge finish' OR EVENT = 'write checkpoint finish')
       AND (EFFECTIVE_TENANT_ID() = 1 OR VALUE1 = EFFECTIVE_TENANT_ID())) B
    ON
      A.SVR_IP = B.SVR_IP AND A.SVR_PORT = B.SVR_PORT AND A.VALUE1 = B.VALUE1 AND A.VALUE2 = B.VALUE2 AND ((A.EVENT = 'minor merge start' AND B.EVENT = 'minor merge finish') OR (A.EVENT = 'write checkpoint start' AND B.EVENT = 'write checkpoint finish'))
  ORDER BY
    SVR_IP, SVR_PORT, TENANT_ID, CHECKPOINT_SNAPSHOT
""".replace("\n", " ")
)

def_table_schema(
 table_name      = 'gv$ob_trans_table_status',
 table_id        = '21115',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
SELECT
  SVR_IP,
  SVR_PORT,
  TENANT_ID,
  TABLE_ID,
  PARTITION_ID,
  END_LOG_ID,
  TRANS_CNT
FROM
  oceanbase.__all_virtual_trans_table_status
WHERE
  effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)

def_table_schema(
 table_name      = 'v$ob_trans_table_status',
 table_id        = '21116',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
SELECT
  TABLE_ID,
  PARTITION_ID,
  END_LOG_ID,
  TRANS_CNT
FROM
  oceanbase.__all_virtual_trans_table_status
WHERE
  SVR_IP=HOST_IP()
AND
  SVR_PORT=RPC_PORT()
AND
  effective_tenant_id() = 1 OR tenant_id = effective_tenant_id()
""".replace("\n", " ")
)


def_table_schema(
    table_name     = 'v$sql_monitor_statname',
    table_id       = '21117',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    normal_columns = [],
    view_definition = """
    SELECT
      NULL CON_ID,
      ID,
      GROUP_ID,
      NAME,
      DESCRIPTION,
      0 TYPE,
      0 FLAGS
    FROM oceanbase.__all_virtual_sql_monitor_statname
""".replace("\n", " "),
)

def_table_schema(
 table_name      = 'gv$merge_info',
 table_id        = '21118',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
    SELECT
        SVR_IP,
        SVR_PORT,
        TENANT_ID,
        TABLE_ID,
        PARTITION_ID,
        CASE MERGE_TYPE WHEN 'MAJOR MERGE' THEN 'major' ELSE 'minor' END AS TYPE,
        MERGE_TYPE AS ACTION,
        CASE MERGE_TYPE WHEN 'MAJOR MERGE' THEN VERSION ELSE SNAPSHOT_VERSION END AS VERSION,
        USEC_TO_TIME(MERGE_START_TIME) AS START_TIME,
        USEC_TO_TIME(MERGE_FINISH_TIME) AS END_TIME,
        MACRO_BLOCK_COUNT,
        CASE MACRO_BLOCK_COUNT WHEN 0 THEN 0.00 ELSE ROUND(USE_OLD_MACRO_BLOCK_COUNT/MACRO_BLOCK_COUNT*100, 2) END AS REUSE_PCT,
        TOTAL_CHILD_TASK AS PARALLEL_DEGREE
    FROM __ALL_VIRTUAL_PARTITION_SSTABLE_MERGE_INFO
    WHERE
        EFFECTIVE_TENANT_ID() = 1 OR tenant_id  = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$merge_info',
  table_id        = '21119',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT *
    FROM OCEANBASE.gv$merge_info
    WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'gv$lock',
  table_id        = '21120',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      distinct
      a.svr_ip AS SVR_IP,
      a.svr_port AS SVR_PORT,
      a.table_id AS TABLE_ID,
      a.rowkey AS ADDR,
      b.row_lock_addr AS KADDR,
      b.session_id AS SID,
      a.type AS TYPE,
      a.lock_mode AS LMODE,
      CAST(NULL AS SIGNED) AS REQUEST,
      a.time_after_recv AS CTIME,
      a.block_session_id AS BLOCK,
      (a.table_id >> 40) AS CON_ID
      FROM __all_virtual_lock_wait_stat a
      JOIN __all_virtual_trans_lock_stat b
      ON a.svr_ip = b.svr_ip and a.svr_port = b.svr_port
      and a.table_id = b.table_id and substr(a.rowkey, 1, 512) = substr(b.rowkey, 1, 512)
      where ((a.table_id >> 40) = effective_tenant_id() or effective_tenant_id() = 1)
      and a.session_id = a.block_session_id
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$lock',
  table_id        = '21121',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      TABLE_ID,
      ADDR,
      KADDR,
      SID,
      TYPE,
      LMODE,
      REQUEST,
      CTIME,
      BLOCK,
      CON_ID FROM gv$lock
      WHERE svr_ip = host_ip() AND svr_port = rpc_port()
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_VALIDATION_JOB',
  table_id        = '21122',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
      JOB_ID,
      TENANT_ID,
      TENANT_NAME,
      INCARNATION,
      BACKUP_SET_ID,
      PROGRESS_PERCENT,
      STATUS
    FROM oceanbase.__all_backup_validation_job
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_VALIDATION_JOB_HISTORY',
  table_id        = '21123',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
      JOB_ID,
      TENANT_ID,
      TENANT_NAME,
      INCARNATION,
      BACKUP_SET_ID,
      PROGRESS_PERCENT,
      STATUS
    FROM oceanbase.__all_backup_validation_job_history
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_TENANT_BACKUP_VALIDATION_TASK',
  table_id        = '21124',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
      JOB_ID,
      TASK_ID,
      TENANT_ID,
      INCARNATION,
      BACKUP_SET_ID,
      STATUS,
      BACKUP_DEST,
      START_TIME,
      END_TIME,
      TOTAL_PG_COUNT,
      FINISH_PG_COUNT,
      TOTAL_PARTITION_COUNT,
      FINISH_PARTITION_COUNT,
      TOTAL_MACRO_BLOCK_COUNT,
      FINISH_MACRO_BLOCK_COUNT,
      LOG_SIZE
    FROM oceanbase.__all_virtual_backup_validation_task 
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_VALIDATION_TASK_HISTORY',
  table_id        = '21125',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
      JOB_ID,
      TASK_ID,
      TENANT_ID,
      INCARNATION,
      BACKUP_SET_ID,
      STATUS,
      BACKUP_DEST,
      START_TIME,
      END_TIME,
      TOTAL_PG_COUNT,
      FINISH_PG_COUNT,
      TOTAL_PARTITION_COUNT,
      FINISH_PARTITION_COUNT,
      TOTAL_MACRO_BLOCK_COUNT,
      FINISH_MACRO_BLOCK_COUNT,
      LOG_SIZE
    FROM oceanbase.__all_backup_validation_task_history
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'v$restore_point',
  table_id        = '21126',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      TENANT_ID,
      SNAPSHOT_TS as SNAPSHOT,
      GMT_CREATE as TIME,
      EXTRA_INFO as NAME FROM oceanbase.__all_acquired_snapshot
      WHERE snapshot_type = 3 and (effective_tenant_id() = 1 OR tenant_id = effective_tenant_id())
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_SET_OBSOLETE',
  table_id        = '21127',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    INCARNATION,
    TENANT_ID,
    BACKUP_SET_ID AS BS_KEY,
    BACKUP_TYPE,
    ENCRYPTION_MODE,
    START_TIME,
    END_TIME AS COMPLETION_TIME,
    ROUND((END_TIME - START_TIME)/1000/1000,0) AS ELAPSED_SECONDES,
    'NO' AS KEEP,
    '' AS KEEP_UNTIL,
    DEVICE_TYPE,
    'NO' AS COMPRESSED,
    OUTPUT_BYTES,
    OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000)  AS  OUTPUT_RATE_BYTES,
    ROUND(OUTPUT_BYTES / INPUT_BYTES, 2) AS COMPRESSION_RATIO,
    CASE
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')
        END  AS OUTPUT_BYTES_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000) >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000)/1024/1024/1024,2), 'GB/S')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000)/1024/1024,2), 'MB/S')
        END  AS OUTPUT_RATE_BYTES_DISPLAY,
    TIMEDIFF(END_TIME, START_TIME) AS TIME_TAKEN_DISPLAY,
    CASE
        WHEN IS_MARK_DELETED = 1
            THEN 'DELETING'
        WHEN RESULT != 0
            THEN 'FAILED'
        ELSE
            'COMPLETED'
        END AS STATUS
    FROM oceanbase.__all_virtual_backupset_history_mgr
    WHERE backup_recovery_window > 0
        and (backup_recovery_window + snapshot_version <= time_to_usec(now(6)))
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_BACKUPSET_JOB',
  table_id        = '21128',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
      JOB_ID,
      TENANT_ID,
      INCARNATION,
      BACKUP_SET_ID,
      CASE
          WHEN BACKUP_BACKUPSET_TYPE = 'A'
              THEN 'ALL_BACKUP_SET'
          WHEN BACKUP_BACKUPSET_TYPE = 'S'
              THEN 'SINGLE_BACKUP_SET'
          ELSE
              'UNKNOWN'
          END AS TYPE,
      TENANT_NAME,
      STATUS,
      BACKUP_DEST,
      MAX_BACKUP_TIMES,
      RESULT
    FROM oceanbase.__all_backup_backupset_job 
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_BACKUPSET_JOB_HISTORY',
  table_id        = '21129',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
      JOB_ID,
      TENANT_ID,
      INCARNATION,
      BACKUP_SET_ID,
      CASE
          WHEN BACKUP_BACKUPSET_TYPE = 'A'
              THEN 'ALL_BACKUP_SET'
          WHEN BACKUP_BACKUPSET_TYPE = 'S'
              THEN 'SINGLE_BACKUP_SET'
          ELSE
              'UNKNOWN'
          END AS TYPE,
      TENANT_NAME,
      STATUS,
      BACKUP_DEST,
      MAX_BACKUP_TIMES,
      RESULT
    FROM oceanbase.__all_backup_backupset_job_history
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_BACKUPSET_TASK',
  table_id        = '21130',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    JOB_ID,
    INCARNATION,
    BACKUP_SET_ID AS BS_KEY,
    COPY_ID,
    BACKUP_TYPE,
    TENANT_ID,
    TOTAL_PG_COUNT,
    FINISH_PG_COUNT,
    TOTAL_PARTITION_COUNT,
    TOTAL_MACRO_BLOCK_COUNT,
    FINISH_PARTITION_COUNT,
    FINISH_MACRO_BLOCK_COUNT,
    INPUT_BYTES,
    OUTPUT_BYTES,
    START_TIME,
    END_TIME AS COMPLETION_TIME,
    CASE
        WHEN STATUS = 'FINISH' AND RESULT != 0
            THEN 'FAILED'
        WHEN STATUS = 'FINISH' AND RESULT = 0
            THEN 'COMPLETED'
        ELSE
            'RUNNING'
        END AS STATUS
    FROM oceanbase.__all_virtual_backup_backupset_task 
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_BACKUPSET_TASK_HISTORY',
  table_id        = '21131',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    JOB_ID,
    INCARNATION,
    TENANT_ID,
    BACKUP_SET_ID AS BS_KEY,
    COPY_ID,
    BACKUP_TYPE,
    ENCRYPTION_MODE,
    START_TIME,
    END_TIME AS COMPLETION_TIME,
    ROUND((END_TIME - START_TIME)/1000/1000,0) AS ELAPSED_SECONDES,
    'NO' AS KEEP,
    '' AS KEEP_UNTIL,
    SRC_DEVICE_TYPE,
    DST_DEVICE_TYPE,
    'NO' AS COMPRESSED,
    OUTPUT_BYTES,
    OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000)  AS  OUTPUT_RATE_BYTES,
    ROUND(OUTPUT_BYTES / INPUT_BYTES, 2) AS COMPRESSION_RATIO,
    CASE
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')
        END  AS OUTPUT_BYTES_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000) >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000)/1024/1024/1024,2), 'GB/S')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000)/1024/1024,2), 'MB/S')
        END  AS OUTPUT_RATE_BYTES_DISPLAY,
    TIMEDIFF(END_TIME, START_TIME) AS TIME_TAKEN_DISPLAY,
    CASE
        WHEN IS_MARK_DELETED = 1
            THEN 'DELETING'
        WHEN RESULT != 0
            THEN 'FAILED'
        ELSE
            'COMPLETED'
        END AS STATUS
    FROM oceanbase.__all_backup_backupset_task_history
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_BACKUP_ARCHIVELOG_SUMMARY',
  table_id        = '21132',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    INCARNATION,
    LOG_ARCHIVE_ROUND,
    COPY_ID,
    TENANT_ID,
    STATUS,
    CASE
      WHEN time_to_usec(MIN_FIRST_TIME) = 0
        THEN ''
      ELSE
        MIN_FIRST_TIME
      END AS MIN_FIRST_TIME,
    CASE
      WHEN time_to_usec(MAX_NEXT_TIME) = 0
        THEN ''
      ELSE
        MAX_NEXT_TIME
      END AS MAX_NEXT_TIME,
    INPUT_BYTES,
    OUTPUT_BYTES,
    ROUND(OUTPUT_BYTES / INPUT_BYTES, 2) AS COMPRESSION_RATIO,
    CASE
        WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN INPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN INPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')
        END  AS INPUT_BYTES_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')
        END  AS OUTPUT_BYTES_DISPLAY
    FROM
(
select tenant_id,
       incarnation,
       log_archive_round,
       copy_id,
       min_first_time,
       max_next_time,
       input_bytes,
       output_bytes,
       deleted_input_bytes,
       deleted_output_bytes,
       pg_count,
       'STOP' as status
       from oceanbase.__all_backup_backup_log_archive_status_history
union
select tenant_id,
       incarnation,
       log_archive_round,
       copy_id,
       min_first_time,
       max_next_time,
       input_bytes,
       output_bytes,
       deleted_input_bytes,
       deleted_output_bytes,
       pg_count,
       status
       from oceanbase.__all_backup_backup_log_archive_status_v2 where status != 'STOP')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_PIECE_FILES',
  table_id        = '21136 ',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
   SELECT
    INCARNATION,
    TENANT_ID,
    ROUND_ID,
    BACKUP_PIECE_ID,
    COPY_ID,
    CREATE_DATE,
    CASE 
        WHEN START_TS = 0
          THEN ''
        ELSE
          USEC_TO_TIME(START_TS)
        END AS START_TS,
    CASE 
        WHEN CHECKPOINT_TS = 0
          THEN ''
        ELSE
          USEC_TO_TIME(CHECKPOINT_TS)
        END AS CHECKPOINT_TS,
    CASE
        WHEN MAX_TS = 0
          THEN ''
        WHEN MAX_TS = 9223372036854775807
          THEN 'MAX'
        ELSE
          USEC_TO_TIME(MAX_TS)
        END AS MAX_TS,
    STATUS,
    FILE_STATUS,
    COMPATIBLE,
    START_PIECE_ID
    FROM oceanbase.__all_backup_piece_files;
""".replace("\n", " ")
)


def_table_schema(
  table_name      = 'CDB_OB_BACKUP_SET_FILES',
  table_id        = '21137',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    INCARNATION,
    TENANT_ID,
    BACKUP_SET_ID AS BS_KEY,
    COPY_ID,
    BACKUP_TYPE,
    ENCRYPTION_MODE,
    STATUS,
    FILE_STATUS,
    USEC_TO_TIME(START_TIME) AS START_TIME,
    USEC_TO_TIME(END_TIME) AS COMPLETION_TIME,
    ROUND((END_TIME - START_TIME)/1000/1000,0) AS ELAPSED_SECONDES,
    'NO' AS KEEP,
    '' AS KEEP_UNTIL,
    'NO' AS COMPRESSED,
    OUTPUT_BYTES,
    OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000)  AS  OUTPUT_RATE_BYTES,
    ROUND(OUTPUT_BYTES / INPUT_BYTES, 2) AS COMPRESSION_RATIO,
    CASE
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')
        END  AS OUTPUT_BYTES_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000) >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000)/1024/1024/1024,2), 'GB/S')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES / ((END_TIME - START_TIME)/1000/1000)/1024/1024,2), 'MB/S')
        END  AS OUTPUT_RATE_BYTES_DISPLAY,
    TIMEDIFF(USEC_TO_TIME(END_TIME), USEC_TO_TIME(START_TIME)) AS TIME_TAKEN_DISPLAY
    FROM oceanbase.__all_backup_set_files
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_BACKUPPIECE_JOB',
  table_id        = '21138',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    JOB_ID,
    TENANT_ID,
    INCARNATION,
    BACKUP_PIECE_ID,
    MAX_BACKUP_TIMES,
    RESULT,
    STATUS,
    BACKUP_DEST,
    COMMENT,
    TYPE
    FROM oceanbase.__all_backup_backuppiece_job
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_BACKUPPIECE_JOB_HISTORY',
  table_id        = '21139',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    JOB_ID,
    TENANT_ID,
    INCARNATION,
    BACKUP_PIECE_ID,
    MAX_BACKUP_TIMES,
    RESULT,
    STATUS,
    BACKUP_DEST,
    COMMENT,
    TYPE
    FROM oceanbase.__all_backup_backuppiece_job_history
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_BACKUPPIECE_TASK',
  table_id        = '21140',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    JOB_ID,
    INCARNATION,
    TENANT_ID,
    ROUND_ID,
    BACKUP_PIECE_ID,
    COPY_ID,
    START_TIME,
    END_TIME,
    STATUS,
    BACKUP_DEST,
    RESULT,
    COMMENT
    FROM oceanbase.__all_backup_backuppiece_task
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_BACKUPPIECE_TASK_HISTORY',
  table_id        = '21141',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    JOB_ID,
    INCARNATION,
    TENANT_ID,
    ROUND_ID,
    BACKUP_PIECE_ID,
    COPY_ID,
    START_TIME,
    END_TIME,
    STATUS,
    BACKUP_DEST,
    RESULT,
    COMMENT
    FROM oceanbase.__all_backup_backuppiece_task_history
""".replace("\n", " ")
)

def_table_schema(
  table_name = 'v$ob_all_clusters',
  table_id = '21142',
  table_type = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = False,
  view_definition = """
      SELECT cluster_id,
             cluster_name,
             cluster_role,
             cluster_status,
             rootservice_list,
             redo_transport_options
      FROM oceanbase.__all_virtual_all_clusters
      """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_ARCHIVELOG',
  table_id        = '21143 ',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
   SELECT
    INCARNATION,
    LOG_ARCHIVE_ROUND,
    TENANT_ID,
    STATUS,
    START_PIECE_ID,
    BACKUP_PIECE_ID,
    CASE
      WHEN time_to_usec(MIN_FIRST_TIME) = 0
        THEN ''
      ELSE
        MIN_FIRST_TIME
      END AS MIN_FIRST_TIME,
    CASE
      WHEN time_to_usec(MAX_NEXT_TIME) = 0
        THEN ''
      ELSE
        MAX_NEXT_TIME
      END AS MAX_NEXT_TIME,
    INPUT_BYTES,
    OUTPUT_BYTES,
    ROUND(OUTPUT_BYTES / INPUT_BYTES, 2) AS COMPRESSION_RATIO,
    CASE
        WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN INPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN INPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')
        END  AS INPUT_BYTES_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')
        END  AS OUTPUT_BYTES_DISPLAY
    FROM oceanbase.__all_backup_log_archive_status_v2;
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'CDB_OB_BACKUP_BACKUP_ARCHIVELOG',
  table_id        = '21144',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  view_definition = """
    SELECT
    INCARNATION,
    LOG_ARCHIVE_ROUND,
    COPY_ID,
    TENANT_ID,
    STATUS,
    START_PIECE_ID,
    BACKUP_PIECE_ID,
    CASE
      WHEN time_to_usec(MIN_FIRST_TIME) = 0
        THEN ''
      ELSE
        MIN_FIRST_TIME
      END AS MIN_FIRST_TIME,
    CASE
      WHEN time_to_usec(MAX_NEXT_TIME) = 0
        THEN ''
      ELSE
        MAX_NEXT_TIME
      END AS MAX_NEXT_TIME,
    INPUT_BYTES,
    OUTPUT_BYTES,
    ROUND(OUTPUT_BYTES / INPUT_BYTES, 2) AS COMPRESSION_RATIO,
    CASE
        WHEN INPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN INPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN INPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(INPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(INPUT_BYTES/1024/1024,2), 'MB')
        END  AS INPUT_BYTES_DISPLAY,
    CASE
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024/1024,2), 'PB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024/1024,2), 'TB')
        WHEN OUTPUT_BYTES >= 1024*1024*1024
            THEN CONCAT(ROUND(OUTPUT_BYTES/1024/1024/1024,2), 'GB')
        ELSE
            CONCAT(ROUND(OUTPUT_BYTES/1024/1024,2), 'MB')
        END  AS OUTPUT_BYTES_DISPLAY
    FROM oceanbase.__all_backup_backup_log_archive_status_v2;
""".replace("\n", " ")
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  database_id     = 'OB_INFORMATION_SCHEMA_ID',
  table_name      = 'COLUMN_PRIVILEGES',
  table_id        = '21150',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT
      CAST(NULL AS VARCHAR(292)) AS GRANTEE,
      CAST('def' AS VARCHAR(512)) AS TABLE_CATALOG,
      CAST(NULL AS VARCHAR(64)) AS TABLE_SCHEMA,
      CAST(NULL AS VARCHAR(64)) AS TABLE_NAME,
      CAST(NULL AS VARCHAR(64)) AS COLUMN_NAME,
      CAST(NULL AS VARCHAR(64)) AS PRIVILEGE_TYPE,
      CAST(NULL AS VARCHAR(3))  AS IS_GRANTABLE
    FROM DUAL
    WHERE 1 = 0
""".replace("\n", " "),
)

def_table_schema(
  tablegroup_id   = 'OB_INVALID_ID',
  database_id     = 'OB_SYS_DATABASE_ID',
  table_name      = 'SHOW_RABBIT',
  table_id        = '21151',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CONCAT('
            ,-\\'\\'\\$|', (rand() * 10000000) & 0x6de9,'
           (\\_ /+
            \\_\\(
            .-  \\
            |    }
           (| { //
            \\' \\'-/
            /  (
           //  \\\\
          / |   :\\'
          \\'\\'/\\   :
           (| \"  |\\
            |  ) \\' )
            | (_  (
            \\'   \"\\_\\
             \\     )\\'.
             |\\
  (\_/)      | \\\\
 (=\\'.\\'=)     |  )_
 (")_(")     / _/|
            -\\'
') RABBIT FROM DUAL
""",
)

################################################################################
# Oracle System View (25000, 30000]
# Data Dictionary View (25000, 28000]
# Performance View (28000, 30000]
################################################################################
def_table_schema(
  table_name      = 'DBA_SYNONYMS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25001',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CASE WHEN
      A.DATABASE_NAME = '__public' THEN
      'PUBLIC' ELSE A.DATABASE_NAME END AS OWNER,
      A.SYNONYM_NAME AS SYNONYM_NAME,
      CAST(CASE WHEN INSTR(A.OBJECT_NAME, '@') = 0
           THEN B.DATABASE_NAME
           ELSE SUBSTR(A.OBJECT_NAME, 1, INSTR(A.OBJECT_NAME, '.') -1)
           END
           AS VARCHAR2(128)) AS TABLE_OWNER,
      CAST(CASE WHEN INSTR(A.OBJECT_NAME, '@') = 0
           THEN A.OBJECT_NAME
           ELSE SUBSTR(A.OBJECT_NAME, INSTR(A.OBJECT_NAME, '.') + 1, INSTR(A.OBJECT_NAME, '@') - INSTR(A.OBJECT_NAME, '.') -1)
           END
           AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(CASE WHEN INSTR(A.OBJECT_NAME, '@') = 0
                THEN NULL
                ELSE SUBSTR(A.OBJECT_NAME, INSTR(A.OBJECT_NAME, '@')+1)
                END
                AS VARCHAR2(128)) AS DB_LINK
    FROM
      (SELECT BB.DATABASE_NAME, AA.SYNONYM_NAME,
      AA.OBJECT_NAME, AA.SYNONYM_ID
      FROM
      SYS.ALL_VIRTUAL_SYNONYM_REAL_AGENT AA,
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT BB
      WHERE AA.DATABASE_ID = BB.DATABASE_ID
        AND AA.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND BB.TENANT_ID = EFFECTIVE_TENANT_ID()) A,
      (SELECT BB.DATABASE_NAME, AA.SYNONYM_ID
      FROM
      SYS.ALL_VIRTUAL_SYNONYM_REAL_AGENT AA,
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT BB
      WHERE AA.OBJECT_DATABASE_ID = BB.DATABASE_ID
        AND AA.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND BB.TENANT_ID = EFFECTIVE_TENANT_ID()) B
    WHERE
      A.SYNONYM_ID = B.SYNONYM_ID
""".replace("\n", " ")
)

# oracle view/synonym DBA/ALL/USER_OBJECTS
def_table_schema(
  table_name      = 'DBA_OBJECTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25002',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
     B.DATABASE_NAME AS OWNER
    ,CAST((CASE WHEN B.DATABASE_NAME = '__recyclebin' THEN A.OBJECT_NAME
                WHEN A.OBJECT_TYPE = 'INDEX' THEN SUBSTR(A.OBJECT_NAME, 7 + INSTR(SUBSTR(A.OBJECT_NAME, 7), '_'))
           ELSE A.OBJECT_NAME END) AS VARCHAR2(128)) AS OBJECT_NAME
    ,CAST(A.SUBOBJECT_NAME AS VARCHAR2(128)) AS SUBOBJECT_NAME
    ,A.OBJECT_ID AS OBJECT_ID
    ,CAST(A.DATA_OBJECT_ID AS NUMBER) AS DATA_OBJECT_ID
    ,CAST(A.OBJECT_TYPE AS VARCHAR2(23)) AS OBJECT_TYPE
    ,CAST(A.GMT_CREATE AS DATE) AS CREATED
    ,CAST(A.GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
    ,TO_CHAR(A.GMT_CREATE) AS TIMESTAMP
    ,CAST(A.STATUS AS VARCHAR2(7)) AS STATUS
    ,CAST(A.TEMPORARY AS VARCHAR2(1)) AS TEMPORARY
    ,CAST("GENERATED" AS VARCHAR2(1)) AS "GENERATED"
    ,CAST(A.SECONDARY AS VARCHAR2(1)) AS SECONDARY
    ,CAST(A.NAMESPACE AS NUMBER) AS NAMESPACE
    ,CAST(A.EDITION_NAME AS VARCHAR2(128)) AS EDITION_NAME
    FROM (
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,TABLE_NAME OBJECT_NAME
    ,NULL SUBOBJECT_NAME
    ,TABLE_ID OBJECT_ID
    ,NULL DATA_OBJECT_ID
    ,CASE WHEN TABLE_TYPE IN (0,2,3,6,8,9,10) THEN 'TABLE'
          WHEN TABLE_TYPE IN (1,4) THEN 'VIEW'
          WHEN TABLE_TYPE IN (5) THEN 'INDEX'
          WHEN TABLE_TYPE IN (7) THEN 'MATERIALIZED VIEW'
          ELSE NULL END AS OBJECT_TYPE
    ,CAST(CASE WHEN TABLE_TYPE IN (5) THEN CASE WHEN INDEX_STATUS = 2 THEN 'VALID'
            WHEN INDEX_STATUS = 3 THEN 'CHECKING'
            WHEN INDEX_STATUS = 4 THEN 'INELEGIBLE'
            WHEN INDEX_STATUS = 5 THEN 'ERROR'
            ELSE 'UNUSABLE' END
          ELSE  'VALID' END AS VARCHAR2(10)) AS STATUS
    ,CASE WHEN TABLE_TYPE IN (6,8,9,10) THEN 'Y'
        ELSE 'N' END AS TEMPORARY
    ,CASE WHEN TABLE_TYPE IN (0,1) THEN 'Y'
        ELSE 'N' END AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM
    SYS.ALL_VIRTUAL_TABLE_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    P.GMT_CREATE
    ,P.GMT_MODIFIED
    ,T.DATABASE_ID
    ,T.TABLE_NAME OBJECT_NAME
    ,P.PART_NAME SUBOBJECT_NAME
    ,P.PART_ID OBJECT_ID
    ,CASE WHEN P.PART_IDX != -1 THEN P.PART_ID ELSE NULL END AS DATA_OBJECT_ID
    ,'TABLE PARTITION' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,CASE WHEN P.PART_IDX != -1 THEN 'Y'
        ELSE 'N' END AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON T.TABLE_ID = P.TABLE_ID
    WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID() AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    SUBP.GMT_CREATE
    ,SUBP.GMT_MODIFIED
    ,T.DATABASE_ID
    ,T.TABLE_NAME OBJECT_NAME
    ,SUBP.SUB_PART_NAME SUBOBJECT_NAME
    ,SUBP.PART_ID OBJECT_ID
    ,SUBP.PART_ID AS DATA_OBJECT_ID
    ,'TABLE SUBPARTITION' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'Y' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_PART_REAL_AGENT P,SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUBP
    WHERE T.TABLE_ID =P.TABLE_ID AND P.TABLE_ID=SUBP.TABLE_ID AND P.PART_ID =SUBP.PART_ID
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID() AND P.TENANT_ID = EFFECTIVE_TENANT_ID() AND SUBP.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    P.GMT_CREATE
    ,P.GMT_MODIFIED
    ,P.DATABASE_ID
    ,P.PACKAGE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,P.PACKAGE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,CASE WHEN TYPE = 1 THEN 'PACKAGE'
          WHEN TYPE = 2 THEN 'PACKAGE BODY'
          ELSE NULL END AS OBJECT_TYPE
    ,CASE WHEN EXISTS
                (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E
                  WHERE P.TENANT_ID = E.TENANT_ID AND P.PACKAGE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 3 OR E.OBJ_TYPE = 5))
               THEN 'INVALID'
          ELSE 'VALID' END AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P
    WHERE P.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    R.GMT_CREATE
    ,R.GMT_MODIFIED
    ,R.DATABASE_ID
    ,R.ROUTINE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,R.ROUTINE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,CASE WHEN ROUTINE_TYPE = 1 THEN 'PROCEDURE'
          WHEN ROUTINE_TYPE = 2 THEN 'FUNCTION'
          ELSE NULL END AS OBJECT_TYPE
    ,CASE WHEN EXISTS
                (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E
                  WHERE R.TENANT_ID = E.TENANT_ID AND R.ROUTINE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 9 OR E.OBJ_TYPE = 12))
               THEN 'INVALID'
          ELSE 'VALID' END AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT R
    WHERE (ROUTINE_TYPE = 1 OR ROUTINE_TYPE = 2) AND R.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    T.GMT_CREATE
    ,T.GMT_MODIFIED
    ,T.DATABASE_ID
    ,T.TRIGGER_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,T.TRIGGER_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TRIGGER' OBJECT_TYPE
    ,CASE WHEN EXISTS
                (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_ERROR_AGENT E
                  WHERE T.TENANT_ID = E.TENANT_ID AND T.TRIGGER_ID = E.OBJ_ID AND (E.OBJ_TYPE = 7))
               THEN 'INVALID'
          ELSE 'VALID' END AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_TRIGGER_AGENT T
    WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,SYNONYM_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,SYNONYM_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'SYNONYM' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_SYNONYM_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,SEQUENCE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,SEQUENCE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'SEQUENCE' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,TYPE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,TYPE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TYPE' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_TYPE_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,OBJECT_TYPE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TYPE BODY' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_OBJECT_TYPE_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID() and TYPE = 2
    )A
    JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B
    ON A.DATABASE_ID = B.DATABASE_ID
    AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
      'SYS' AS OWNER
      ,TS.TYPE_NAME AS OBJECT_NAME
      ,NULL AS SUBOBJECT_NAME
      ,TS.TYPE_ID AS OBJECT_ID
      ,NULL AS DATA_OBJECT_ID
      ,'TYPE' AS OBJECT_TYPE
      ,CAST(TS.GMT_CREATE AS DATE) AS CREATED
      ,CAST(TS.GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
      ,TO_CHAR(TS.GMT_CREATE) AS TIMESTAMP
      ,'VALID' AS STATUS
      ,'N' AS TEMPORARY
      ,'N' AS "GENERATED"
      ,'N' AS SECONDARY
      ,0 AS NAMESPACE
      ,NULL AS EDITION_NAME
  FROM SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS
  UNION ALL
  SELECT
    'SYS' AS OWNER
    ,PACKAGE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,PACKAGE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,CASE WHEN TYPE = 1 THEN 'PACKAGE'
          WHEN TYPE = 2 THEN 'PACKAGE BODY'
          ELSE NULL END AS OBJECT_TYPE
    ,CAST(GMT_CREATE AS DATE) AS CREATED
    ,CAST(GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
    ,TO_CHAR(GMT_CREATE) AS TIMESTAMP
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    ,0 AS NAMESPACE
    ,NULL AS EDITION_NAME
  FROM SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT
  UNION ALL
  SELECT
     'SYS' AS OWNER
     ,ROUTINE_NAME AS OBJECT_NAME
     ,NULL AS SUBOBJECT_NAME
     ,ROUTINE_ID OBJECT_ID
     ,NULL AS DATA_OBJECT_ID
     ,CASE WHEN ROUTINE_TYPE = 1 THEN 'PROCEDURE'
           WHEN ROUTINE_TYPE = 2 THEN 'FUNCTION'
           ELSE NULL END AS OBJECT_TYPE
     ,CAST(GMT_CREATE AS DATE) AS CREATED
     ,CAST(GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
     ,TO_CHAR(GMT_CREATE) AS TIMESTAMP
     ,'VALID' AS STATUS
     ,'N' AS TEMPORARY
     ,'N' AS "GENERATED"
     ,'N' AS SECONDARY
     ,0 AS NAMESPACE
     ,NULL AS EDITION_NAME
   FROM SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT
   UNION ALL
   SELECT
     'SYS' AS OWNER
     ,TRIGGER_NAME AS OBJECT_NAME
     ,NULL AS SUBOBJECT_NAME
     ,TRIGGER_ID OBJECT_ID
     ,NULL AS DATA_OBJECT_ID
     ,'TRIGGER' AS OBJECT_TYPE
     ,CAST(GMT_CREATE AS DATE) AS CREATED
     ,CAST(GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
     ,TO_CHAR(GMT_CREATE) AS TIMESTAMP
     ,'VALID' AS STATUS
     ,'N' AS TEMPORARY
     ,'N' AS "GENERATED"
     ,'N' AS SECONDARY
     ,0 AS NAMESPACE
     ,NULL AS EDITION_NAME
   FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_OBJECTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25003',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
     B.DATABASE_NAME AS OWNER
     ,CAST((CASE WHEN B.DATABASE_NAME = '__recyclebin' THEN A.OBJECT_NAME
                 WHEN A.OBJECT_TYPE = 'INDEX' THEN SUBSTR(A.OBJECT_NAME, 7 + INSTR(SUBSTR(A.OBJECT_NAME, 7), '_'))
            ELSE A.OBJECT_NAME END) AS VARCHAR2(128)) AS OBJECT_NAME
    ,CAST(A.SUBOBJECT_NAME AS VARCHAR2(128)) AS SUBOBJECT_NAME
    ,A.OBJECT_ID AS OBJECT_ID
    ,CAST(A.DATA_OBJECT_ID AS NUMBER) AS DATA_OBJECT_ID
    ,CAST(A.OBJECT_TYPE AS VARCHAR2(23)) AS OBJECT_TYPE
    ,CAST(A.GMT_CREATE AS DATE) AS CREATED
    ,CAST(A.GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
    ,TO_CHAR(A.GMT_CREATE) AS TIMESTAMP
    ,CAST(A.STATUS AS VARCHAR2(7)) AS STATUS
    ,CAST(A.TEMPORARY AS VARCHAR2(1)) AS TEMPORARY
    ,CAST("GENERATED" AS VARCHAR2(1)) AS "GENERATED"
    ,CAST(A.SECONDARY AS VARCHAR2(1)) AS SECONDARY
    ,CAST(A.NAMESPACE AS NUMBER) AS NAMESPACE
    ,CAST(A.EDITION_NAME AS VARCHAR2(128)) AS EDITION_NAME
    FROM (
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,TABLE_NAME OBJECT_NAME
    ,NULL SUBOBJECT_NAME
    ,TABLE_ID OBJECT_ID
    ,NULL DATA_OBJECT_ID
    ,CASE WHEN TABLE_TYPE IN (0,2,3,6,8,9,10) THEN 'TABLE'
          WHEN TABLE_TYPE IN (1,4) THEN 'VIEW'
          WHEN TABLE_TYPE IN (5) THEN 'INDEX'
          WHEN TABLE_TYPE IN (7) THEN 'MATERIALIZED VIEW'
          ELSE NULL END AS OBJECT_TYPE
    ,CAST(CASE WHEN TABLE_TYPE IN (5) THEN CASE WHEN INDEX_STATUS = 2 THEN 'VALID'
            WHEN INDEX_STATUS = 3 THEN 'CHECKING'
            WHEN INDEX_STATUS = 4 THEN 'INELEGIBLE'
            WHEN INDEX_STATUS = 5 THEN 'ERROR'
            ELSE 'UNUSABLE' END
          ELSE  'VALID' END AS VARCHAR2(10)) AS STATUS
    ,CASE WHEN TABLE_TYPE IN (6,8,9,10) THEN 'Y'
        ELSE 'N' END AS TEMPORARY
    ,CASE WHEN TABLE_TYPE IN (0,1) THEN 'Y'
        ELSE 'N' END AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    , CASE WHEN TABLE_TYPE IN (5) THEN DATA_TABLE_ID
           ELSE TABLE_ID END AS PRIV_OBJECT_ID
    FROM
    SYS.ALL_VIRTUAL_TABLE_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    P.GMT_CREATE
    ,P.GMT_MODIFIED
    ,T.DATABASE_ID
    ,T.TABLE_NAME OBJECT_NAME
    ,P.PART_NAME SUBOBJECT_NAME
    ,P.PART_ID OBJECT_ID
    ,CASE WHEN P.PART_IDX != -1 THEN P.PART_ID ELSE NULL END AS DATA_OBJECT_ID
    ,'TABLE PARTITION' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,CASE WHEN P.PART_IDX != -1 THEN 'Y'
        ELSE 'N' END AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    ,T.TABLE_ID AS PRIV_OBJECT_ID
    FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON T.TABLE_ID = P.TABLE_ID
    WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID() AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    SUBP.GMT_CREATE
    ,SUBP.GMT_MODIFIED
    ,T.DATABASE_ID
    ,T.TABLE_NAME OBJECT_NAME
    ,SUBP.SUB_PART_NAME SUBOBJECT_NAME
    ,SUBP.PART_ID OBJECT_ID
    ,SUBP.PART_ID AS DATA_OBJECT_ID
    ,'TABLE SUBPARTITION' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'Y' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    ,T.TABLE_ID AS PRIV_OBJECT_ID
    FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_PART_REAL_AGENT P,SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUBP
    WHERE T.TABLE_ID =P.TABLE_ID AND P.TABLE_ID=SUBP.TABLE_ID AND P.PART_ID =SUBP.PART_ID
      AND T.TENANT_ID = EFFECTIVE_TENANT_ID() AND P.TENANT_ID = EFFECTIVE_TENANT_ID() AND SUBP.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    P.GMT_CREATE
    ,P.GMT_MODIFIED
    ,P.DATABASE_ID
    ,P.PACKAGE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,P.PACKAGE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,CASE WHEN TYPE = 1 THEN 'PACKAGE'
          WHEN TYPE = 2 THEN 'PACKAGE BODY'
          ELSE NULL END AS OBJECT_TYPE
    ,CASE WHEN EXISTS
                (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E
                  WHERE P.TENANT_ID = E.TENANT_ID AND P.PACKAGE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 3 OR E.OBJ_TYPE = 5))
               THEN 'INVALID'
          ELSE 'VALID' END AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    ,P.PACKAGE_ID AS PRIV_OBJECT_ID
    FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P
    WHERE P.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    R.GMT_CREATE
    ,R.GMT_MODIFIED
    ,R.DATABASE_ID
    ,R.ROUTINE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,ROUTINE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,CASE WHEN ROUTINE_TYPE = 1 THEN 'PROCEDURE'
          WHEN ROUTINE_TYPE = 2 THEN 'FUNCTION'
          ELSE NULL END AS OBJECT_TYPE
    ,CASE WHEN EXISTS
                (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E
                  WHERE R.TENANT_ID = E.TENANT_ID AND R.ROUTINE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 9 OR E.OBJ_TYPE = 12))
               THEN 'INVALID'
          ELSE 'VALID' END AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    ,ROUTINE_ID AS PRIV_OBJECT_ID
    FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT R
    WHERE (ROUTINE_TYPE = 1 OR ROUTINE_TYPE = 2) AND R.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    T.GMT_CREATE
    ,T.GMT_MODIFIED
    ,T.DATABASE_ID
    ,T.TRIGGER_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,T.TRIGGER_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TRIGGER' OBJECT_TYPE
    ,CASE WHEN EXISTS
                (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_ERROR_AGENT E
                  WHERE T.TENANT_ID = E.TENANT_ID AND T.TRIGGER_ID = E.OBJ_ID AND (E.OBJ_TYPE = 7))
               THEN 'INVALID'
          ELSE 'VALID' END AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    ,T.TRIGGER_ID AS PRIV_OBJECT_ID
    FROM SYS.ALL_VIRTUAL_TRIGGER_AGENT T
    WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,SYNONYM_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,SYNONYM_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'SYNONYM' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    ,SYNONYM_ID AS PRIV_OBJECT_ID
    FROM SYS.ALL_VIRTUAL_SYNONYM_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,SEQUENCE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,SEQUENCE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'SEQUENCE' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    ,SEQUENCE_ID AS PRIV_OBJECT_ID
    FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,TYPE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,TYPE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TYPE' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    ,TYPE_ID AS PRIV_OBJECT_ID
    FROM SYS.ALL_VIRTUAL_TYPE_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,OBJECT_TYPE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TYPE BODY' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    ,OBJECT_TYPE_ID AS PRIV_OBJECT_ID
    FROM SYS.ALL_VIRTUAL_OBJECT_TYPE_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID() and TYPE = 2
    )A
    JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B
    ON A.DATABASE_ID = B.DATABASE_ID
    AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND (A.DATABASE_ID = USERENV('SCHEMAID')
             or USER_CAN_ACCESS_OBJ(DECODE(OBJECT_TYPE, 'TABLE', 1,
                                                        'VIEW', 1,
                                                        'INDEX', 1,
                                                        'MATERIALIZED VIEW',9,
                                                        'TABLE PARTITION',1,
                                                        'TABLE SUBPARTITION', 1,
                                                        'PACKAGE', 3,
                                                        'PACKAGE BODY', 3,
                                                        'PROCEDURE', 12,
                                                        'FUNCTION', 9,
                                                        'SYNONYM', 13,
                                                        'SEQUENCE', 2,
                                                        'TYPE', 4,
                                                        1), A.PRIV_OBJECT_ID, A.DATABASE_ID) =1
      )
  UNION ALL
  SELECT
    'SYS' AS OWNER
    ,TS.TYPE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,TS.TYPE_ID AS OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TYPE' AS OBJECT_TYPE
    ,CAST(TS.GMT_CREATE AS DATE) AS CREATED
    ,CAST(TS.GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
    ,TO_CHAR(TS.GMT_CREATE) AS TIMESTAMP
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    ,0 AS NAMESPACE
    ,NULL AS EDITION_NAME
  FROM SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS
  UNION ALL
  SELECT
    'SYS' AS OWNER
    ,PACKAGE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,PACKAGE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,CASE WHEN TYPE = 1 THEN 'PACKAGE'
          WHEN TYPE = 2 THEN 'PACKAGE BODY'
          ELSE NULL END AS OBJECT_TYPE
    ,CAST(GMT_CREATE AS DATE) AS CREATED
    ,CAST(GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
    ,TO_CHAR(GMT_CREATE) AS TIMESTAMP
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    ,0 AS NAMESPACE
    ,NULL AS EDITION_NAME
  FROM SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT
  UNION ALL
  SELECT
    'SYS' AS OWNER
    ,ROUTINE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,ROUTINE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,CASE WHEN ROUTINE_TYPE = 1 THEN 'PROCEDURE'
          WHEN ROUTINE_TYPE = 2 THEN 'FUNCTION'
          ELSE NULL END AS OBJECT_TYPE
    ,CAST(GMT_CREATE AS DATE) AS CREATED
    ,CAST(GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
    ,TO_CHAR(GMT_CREATE) AS TIMESTAMP
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    ,0 AS NAMESPACE
    ,NULL AS EDITION_NAME
  FROM SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT
  UNION ALL
  SELECT
    'SYS' AS OWNER
    ,TRIGGER_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,TRIGGER_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TRIGGER' AS OBJECT_TYPE
    ,CAST(GMT_CREATE AS DATE) AS CREATED
    ,CAST(GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
    ,TO_CHAR(GMT_CREATE) AS TIMESTAMP
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    ,0 AS NAMESPACE
    ,NULL AS EDITION_NAME
  FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_OBJECTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25004',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
    CAST((CASE WHEN A.OBJECT_TYPE = 'INDEX' THEN SUBSTR(A.OBJECT_NAME, 7 + INSTR(SUBSTR(A.OBJECT_NAME, 7), '_'))
               ELSE A.OBJECT_NAME END) AS VARCHAR2(128)) AS OBJECT_NAME
    ,CAST(A.SUBOBJECT_NAME AS VARCHAR2(128)) AS SUBOBJECT_NAME
    ,A.OBJECT_ID AS OBJECT_ID
    ,CAST(A.DATA_OBJECT_ID AS NUMBER) AS DATA_OBJECT_ID
    ,CAST(A.OBJECT_TYPE AS VARCHAR2(23)) AS OBJECT_TYPE
    ,CAST(A.GMT_CREATE AS DATE) AS CREATED
    ,CAST(A.GMT_MODIFIED AS DATE) AS LAST_DDL_TIME
    ,TO_CHAR(A.GMT_CREATE) AS TIMESTAMP
    ,CAST(A.STATUS AS VARCHAR2(7)) AS STATUS
    ,CAST(A.TEMPORARY AS VARCHAR2(1)) AS TEMPORARY
    ,CAST("GENERATED" AS VARCHAR2(1)) AS "GENERATED"
    ,CAST(A.SECONDARY AS VARCHAR2(1)) AS SECONDARY
    ,CAST(A.NAMESPACE AS NUMBER) AS NAMESPACE
    ,CAST(A.EDITION_NAME AS VARCHAR2(128)) AS EDITION_NAME
    FROM (
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,TABLE_NAME OBJECT_NAME
    ,NULL SUBOBJECT_NAME
    ,TABLE_ID OBJECT_ID
    ,NULL DATA_OBJECT_ID
    ,CASE WHEN TABLE_TYPE IN (0,2,3,6,8,9,10) THEN 'TABLE'
          WHEN TABLE_TYPE IN (1,4) THEN 'VIEW'
          WHEN TABLE_TYPE IN (5) THEN 'INDEX'
          WHEN TABLE_TYPE IN (7) THEN 'MATERIALIZED VIEW'
          ELSE NULL END AS OBJECT_TYPE
    ,CAST(CASE WHEN TABLE_TYPE IN (5) THEN CASE WHEN INDEX_STATUS = 2 THEN 'VALID'
            WHEN INDEX_STATUS = 3 THEN 'CHECKING'
            WHEN INDEX_STATUS = 4 THEN 'INELEGIBLE'
            WHEN INDEX_STATUS = 5 THEN 'ERROR'
            ELSE 'UNUSABLE' END
           ELSE  'VALID' END AS VARCHAR2(10)) AS STATUS
    ,CASE WHEN TABLE_TYPE IN (6,8,9,10) THEN 'Y'
        ELSE 'N' END AS TEMPORARY
    ,CASE WHEN TABLE_TYPE IN (0,1) THEN 'Y'
        ELSE 'N' END AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM
    SYS.ALL_VIRTUAL_TABLE_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    P.GMT_CREATE
    ,P.GMT_MODIFIED
    ,T.DATABASE_ID
    ,T.TABLE_NAME OBJECT_NAME
    ,P.PART_NAME SUBOBJECT_NAME
    ,P.PART_ID OBJECT_ID
    ,CASE WHEN P.PART_IDX != -1 THEN P.PART_ID ELSE NULL END AS DATA_OBJECT_ID
    ,'TABLE PARTITION' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,CASE WHEN P.PART_IDX != -1 THEN 'Y'
        ELSE 'N' END AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON T.TABLE_ID = P.TABLE_ID
    WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID() AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    SUBP.GMT_CREATE
    ,SUBP.GMT_MODIFIED
    ,T.DATABASE_ID
    ,T.TABLE_NAME OBJECT_NAME
    ,SUBP.SUB_PART_NAME SUBOBJECT_NAME
    ,SUBP.PART_ID OBJECT_ID
    ,SUBP.PART_ID AS DATA_OBJECT_ID
    ,'TABLE SUBPARTITION' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'Y' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_PART_REAL_AGENT P,SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUBP
    WHERE T.TABLE_ID =P.TABLE_ID AND P.TABLE_ID=SUBP.TABLE_ID AND P.PART_ID =SUBP.PART_ID
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID() AND P.TENANT_ID = EFFECTIVE_TENANT_ID() AND SUBP.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    P.GMT_CREATE
    ,P.GMT_MODIFIED
    ,P.DATABASE_ID
    ,P.PACKAGE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,P.PACKAGE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,CASE WHEN TYPE = 1 THEN 'PACKAGE'
          WHEN TYPE = 2 THEN 'PACKAGE BODY'
          ELSE NULL END AS OBJECT_TYPE
    ,CASE WHEN EXISTS
                (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E
                  WHERE P.TENANT_ID = E.TENANT_ID AND P.PACKAGE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 3 OR E.OBJ_TYPE = 5))
               THEN 'INVALID'
          ELSE 'VALID' END AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P
    WHERE P.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    R.GMT_CREATE
    ,R.GMT_MODIFIED
    ,R.DATABASE_ID
    ,R.ROUTINE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,R.ROUTINE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,CASE WHEN ROUTINE_TYPE = 1 THEN 'PROCEDURE'
          WHEN ROUTINE_TYPE = 2 THEN 'FUNCTION'
          ELSE NULL END AS OBJECT_TYPE
    ,CASE WHEN EXISTS
                (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E
                  WHERE R.TENANT_ID = E.TENANT_ID AND R.ROUTINE_ID = E.OBJ_ID AND (E.OBJ_TYPE = 12 OR E.OBJ_TYPE = 9))
               THEN 'INVALID'
          ELSE 'VALID' END AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT R
    WHERE (ROUTINE_TYPE = 1 OR ROUTINE_TYPE = 2) AND R.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    T.GMT_CREATE
    ,T.GMT_MODIFIED
    ,T.DATABASE_ID
    ,T.TRIGGER_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,T.TRIGGER_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TRIGGER' OBJECT_TYPE
    ,CASE WHEN EXISTS
                (SELECT OBJ_ID FROM SYS.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT E
                  WHERE T.TENANT_ID = E.TENANT_ID AND T.TRIGGER_ID = E.OBJ_ID AND (E.OBJ_TYPE = 7))
               THEN 'INVALID'
          ELSE 'VALID' END AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_TRIGGER_AGENT T
    WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,SYNONYM_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,SYNONYM_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'SYNONYM' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_SYNONYM_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,SEQUENCE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,SEQUENCE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'SEQUENCE' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,TYPE_NAME AS OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,TYPE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TYPE' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_TYPE_REAL_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    GMT_CREATE
    ,GMT_MODIFIED
    ,DATABASE_ID
    ,OBJECT_NAME
    ,NULL AS SUBOBJECT_NAME
    ,OBJECT_TYPE_ID OBJECT_ID
    ,NULL AS DATA_OBJECT_ID
    ,'TYPE BODY' AS OBJECT_TYPE
    ,'VALID' AS STATUS
    ,'N' AS TEMPORARY
    ,'N' AS "GENERATED"
    ,'N' AS SECONDARY
    , 0 AS NAMESPACE
    ,NULL AS EDITION_NAME
    FROM SYS.ALL_VIRTUAL_OBJECT_TYPE_AGENT
    WHERE TENANT_ID = EFFECTIVE_TENANT_ID() and TYPE = 2
    )A
    WHERE DATABASE_ID=USERENV('SCHEMAID')
""".replace("\n", " ")
)

# end oracle view/synonym DBA/ALL/USER_OBJECTS


def_table_schema(
  table_name      = 'DBA_SEQUENCES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25005',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
       C.DATABASE_NAME AS SEQUENCE_OWNER
      ,A.SEQUENCE_NAME AS SEQUENCE_NAME
      ,A.MIN_VALUE AS MIN_VALUE
      ,A.MAX_VALUE AS MAX_VALUE
      ,A.INCREMENT_BY AS INCREMENT_BY
      ,CASE A.CYCLE_FLAG WHEN 1 THEN 'Y'
                         WHEN 0 THEN  'N'
                         ELSE NULL END AS CYCLE_FLAG
      ,CASE A.ORDER_FLAG WHEN 1 THEN 'Y'
                         WHEN 0 THEN  'N'
                         ELSE NULL END AS ORDER_FLAG
      ,A.CACHE_SIZE AS CACHE_SIZE
      ,CAST(COALESCE(B.NEXT_VALUE,A.START_WITH) AS NUMBER(38,0)) AS LAST_NUMBER
    FROM
       SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT A
    INNER JOIN
       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C
    ON A.DATABASE_ID = C.DATABASE_ID AND A.TENANT_ID = C.TENANT_ID
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
    LEFT JOIN
      SYS.ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT B
    ON B.SEQUENCE_ID = A.SEQUENCE_ID
""".replace("\n", " ")
)


def_table_schema(
  table_name      = 'ALL_SEQUENCES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25006',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
       C.DATABASE_NAME AS SEQUENCE_OWNER
      ,A.SEQUENCE_NAME AS SEQUENCE_NAME
      ,A.MIN_VALUE AS MIN_VALUE
      ,A.MAX_VALUE AS MAX_VALUE
      ,A.INCREMENT_BY AS INCREMENT_BY
      ,CASE A.CYCLE_FLAG WHEN 1 THEN 'Y'
                         WHEN 0 THEN  'N'
                         ELSE NULL END AS CYCLE_FLAG
      ,CASE A.ORDER_FLAG WHEN 1 THEN 'Y'
                         WHEN 0 THEN  'N'
                         ELSE NULL END AS ORDER_FLAG
      ,A.CACHE_SIZE AS CACHE_SIZE
      ,CAST(COALESCE(B.NEXT_VALUE,A.START_WITH) AS NUMBER(38,0)) AS LAST_NUMBER
    FROM
       SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT A
    INNER JOIN
       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C
    ON A.DATABASE_ID = C.DATABASE_ID AND A.TENANT_ID = C.TENANT_ID
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND (A.DATABASE_ID = USERENV('SCHEMAID')
         OR USER_CAN_ACCESS_OBJ(2, A.SEQUENCE_ID, A.DATABASE_ID) = 1)
    LEFT JOIN
      SYS.ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT B
    ON B.SEQUENCE_ID = A.SEQUENCE_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_SEQUENCES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25007',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
       A.SEQUENCE_NAME AS SEQUENCE_NAME
      ,A.MIN_VALUE AS MIN_VALUE
      ,A.MAX_VALUE AS MAX_VALUE
      ,A.INCREMENT_BY AS INCREMENT_BY
      ,CASE A.CYCLE_FLAG WHEN 1 THEN 'Y'
                         WHEN 0 THEN  'N'
                         ELSE NULL END AS CYCLE_FLAG
      ,CASE A.ORDER_FLAG WHEN 1 THEN 'Y'
                         WHEN 0 THEN  'N'
                         ELSE NULL END AS ORDER_FLAG
      ,A.CACHE_SIZE AS CACHE_SIZE
      ,CAST(COALESCE(B.NEXT_VALUE,A.START_WITH) AS NUMBER(38,0)) AS LAST_NUMBER
    FROM
       SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT A
    LEFT JOIN
      SYS.ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT B
    ON B.SEQUENCE_ID = A.SEQUENCE_ID
    WHERE
      A.DATABASE_ID = USERENV('SCHEMAID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_USERS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25008',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      A.DATABASE_NAME AS USERNAME,
      A.DATABASE_ID AS USERID,
      B.PASSWD AS PASSWORD,
      CAST(NULL as VARCHAR2(30)) AS ACCOUNT_STATUS,
      CAST(NULL as DATE) AS LOCK_DATE,
      CAST(NULL as DATE) AS EXPIRY_DATE,
      CAST(NULL as VARCHAR2(30)) AS DEFAULT_TABLESPACE,
      CAST(NULL as VARCHAR2(30)) AS TEMPORARY_TABLESPACE,
      CAST(A.GMT_CREATE AS DATE) AS CREATED,
      CAST(NULL as VARCHAR2(30)) AS INITIAL_RSRC_CONSUMER_GROUP,
      CAST(NULL as VARCHAR2(4000)) AS EXTERNAL_NAME
    FROM
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,
      SYS.ALL_VIRTUAL_USER_REAL_AGENT B
    WHERE
      A.DATABASE_NAME = B.USER_NAME
      AND A.TENANT_ID = B.TENANT_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_USERS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25009',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      A.DATABASE_NAME AS USERNAME,
      A.DATABASE_ID AS USERID,
      CAST(A.GMT_CREATE AS DATE) AS CREATED
    FROM
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,
      SYS.ALL_VIRTUAL_USER_REAL_AGENT B
    WHERE
      A.DATABASE_NAME = B.USER_NAME
      AND A.TENANT_ID = B.TENANT_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_SYNONYMS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25010',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CASE WHEN
      A.DATABASE_NAME = '__public' THEN
      'PUBLIC' ELSE A.DATABASE_NAME END AS OWNER,
      A.SYNONYM_NAME AS SYNONYM_NAME,
      CAST(CASE WHEN INSTR(A.OBJECT_NAME, '@') = 0
           THEN B.DATABASE_NAME
           ELSE SUBSTR(A.OBJECT_NAME, 1, INSTR(A.OBJECT_NAME, '.') -1)
           END
           AS VARCHAR2(128)) AS TABLE_OWNER,
      CAST(CASE WHEN INSTR(A.OBJECT_NAME, '@') = 0
           THEN A.OBJECT_NAME
           ELSE SUBSTR(A.OBJECT_NAME, INSTR(A.OBJECT_NAME, '.') + 1, INSTR(A.OBJECT_NAME, '@') - INSTR(A.OBJECT_NAME, '.') -1)
           END
           AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(CASE WHEN INSTR(A.OBJECT_NAME, '@') = 0
                THEN NULL
                ELSE SUBSTR(A.OBJECT_NAME, INSTR(A.OBJECT_NAME, '@')+1)
                END
                AS VARCHAR2(128)) AS DB_LINK
    FROM
      (SELECT BB.DATABASE_NAME, AA.SYNONYM_NAME,
      AA.OBJECT_NAME, AA.SYNONYM_ID
      FROM
      SYS.ALL_VIRTUAL_SYNONYM_REAL_AGENT AA,
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT BB
      WHERE AA.DATABASE_ID = BB.DATABASE_ID
            AND AA.TENANT_ID = BB.TENANT_ID AND AA.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND (AA.DATABASE_ID = USERENV('SCHEMAID')
                OR USER_CAN_ACCESS_OBJ(13, AA.SYNONYM_ID, AA.DATABASE_ID) = 1)) A,
      (SELECT BB.DATABASE_NAME, AA.SYNONYM_ID
      FROM
      SYS.ALL_VIRTUAL_SYNONYM_REAL_AGENT AA,
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT BB
      WHERE AA.OBJECT_DATABASE_ID = BB.DATABASE_ID
            AND AA.TENANT_ID = BB.TENANT_ID AND AA.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND (AA.DATABASE_ID = USERENV('SCHEMAID')
                OR USER_CAN_ACCESS_OBJ(13, AA.SYNONYM_ID, AA.DATABASE_ID) = 1)) B
    WHERE
      A.SYNONYM_ID = B.SYNONYM_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_SYNONYMS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25011',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      A.SYNONYM_NAME AS SYNONYM_NAME,
      CAST(CASE WHEN INSTR(A.OBJECT_NAME, '@') = 0
           THEN B.DATABASE_NAME
           ELSE SUBSTR(A.OBJECT_NAME, 1, INSTR(A.OBJECT_NAME, '.') -1)
           END
           AS VARCHAR2(128)) AS TABLE_OWNER,
      CAST(CASE WHEN INSTR(A.OBJECT_NAME, '@') = 0
           THEN A.OBJECT_NAME
           ELSE SUBSTR(A.OBJECT_NAME, INSTR(A.OBJECT_NAME, '.') + 1, INSTR(A.OBJECT_NAME, '@') - INSTR(A.OBJECT_NAME, '.') -1)
           END
           AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(CASE WHEN INSTR(A.OBJECT_NAME, '@') = 0
                THEN NULL
                ELSE SUBSTR(A.OBJECT_NAME, INSTR(A.OBJECT_NAME, '@')+1)
                END
                AS VARCHAR2(128)) AS DB_LINK,
      CAST (0 AS number) AS ORIGIN_CON_ID
    FROM
      (SELECT BB.DATABASE_NAME, AA.SYNONYM_NAME,
      AA.OBJECT_NAME, AA.SYNONYM_ID
      FROM
      SYS.ALL_VIRTUAL_SYNONYM_AGENT AA,
      SYS.ALL_VIRTUAL_DATABASE_AGENT BB
      WHERE AA.DATABASE_ID = BB.DATABASE_ID
            AND (AA.DATABASE_ID = USERENV('SCHEMAID')
                OR USER_CAN_ACCESS_OBJ(13, AA.SYNONYM_ID, AA.DATABASE_ID) = 1)) A,
      (SELECT BB.DATABASE_NAME, AA.SYNONYM_ID
      FROM
      SYS.ALL_VIRTUAL_SYNONYM_AGENT AA,
      SYS.ALL_VIRTUAL_DATABASE_AGENT BB
      WHERE AA.OBJECT_DATABASE_ID = BB.DATABASE_ID
           AND (AA.DATABASE_ID = USERENV('SCHEMAID')
                OR USER_CAN_ACCESS_OBJ(13, AA.SYNONYM_ID, AA.DATABASE_ID) = 1)) B
    WHERE
      A.SYNONYM_ID = B.SYNONYM_ID
    AND
      A.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')
""".replace("\n", " ")
)


# oracle view/synonym dba/all/user_ind_columns
def_table_schema(
  table_name      = 'DBA_IND_COLUMNS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25012',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,
      CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
      CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,
      CAST(TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(ROWKEY_POSITION AS NUMBER) AS COLUMN_POSITION,
      CASE WHEN DATA_TYPE >= 1 AND DATA_TYPE <= 16 THEN CAST(22 AS NUMBER)
           WHEN DATA_TYPE = 17 THEN CAST(7 AS NUMBER)
           WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 2 THEN CAST(DATA_LENGTH AS NUMBER)
           WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 1 AND F.COLLATION_TYPE IN (45, 46, 224, 54, 55, 101) THEN CAST(DATA_LENGTH * 4 AS NUMBER)
           WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 1 AND F.COLLATION_TYPE IN (28, 87) THEN CAST(DATA_LENGTH * 2 AS NUMBER)
           WHEN DATA_TYPE = 36 THEN CAST(12 AS NUMBER)
           WHEN DATA_TYPE IN (37, 38) THEN CAST(11 AS NUMBER)
           WHEN DATA_TYPE = 39 THEN CAST(DATA_LENGTH AS NUMBER)
           WHEN DATA_TYPE = 40 THEN CAST(5 AS NUMBER)
           WHEN DATA_TYPE = 41 THEN CAST(11 AS NUMBER)
        ELSE CAST(0 AS NUMBER) END AS COLUMN_LENGTH,
      CASE WHEN DATA_TYPE IN (22, 23) THEN CAST(DATA_LENGTH AS NUMBER)
        ELSE CAST(0 AS NUMBER) END AS CHAR_LENGTH,
      CAST('ASC' AS VARCHAR2(4)) AS DESCEND
      FROM
        (SELECT
        INDEX_OWNER,
        INDEX_NAME,
        TABLE_OWNER,
        TABLE_NAME,
        INDEX_ID,
        IDX_TYPE
        FROM
          (SELECT
          DATABASE_NAME AS INDEX_OWNER,
          CASE WHEN (TABLE_TYPE = 5) THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))
            ELSE (CONS_TAB.CONSTRAINT_NAME) END AS INDEX_NAME,
          DATABASE_NAME AS TABLE_OWNER,
          CASE WHEN (TABLE_TYPE = 3) THEN A.TABLE_ID
            ELSE A.DATA_TABLE_ID END AS TABLE_ID,
          A.TABLE_ID AS INDEX_ID,
          TABLE_TYPE AS IDX_TYPE
          FROM
            SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A
            JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID
            AND TABLE_TYPE IN (5, 3)
            AND B.DATABASE_NAME != '__recyclebin'
            AND A.TENANT_ID = B.TENANT_ID AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
            LEFT JOIN SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT CONS_TAB
            ON (CONS_TAB.TABLE_ID = A.TABLE_ID AND A.TENANT_ID = CONS_TAB.TENANT_ID
                AND CONS_TAB.TENANT_ID = EFFECTIVE_TENANT_ID())
          WHERE
            NOT(
            TABLE_TYPE = 3
            AND CONSTRAINT_NAME IS NULL
            )
          ) C
        JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D ON C.TABLE_ID = D.TABLE_ID
              AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
        ) E
        JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON E.INDEX_ID = F.TABLE_ID
              AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
      WHERE
        F.ROWKEY_POSITION != 0 AND (CASE WHEN IDX_TYPE = 5 THEN INDEX_POSITION ELSE 1 END) != 0
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_IND_COLUMNS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25013',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,
      CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
      CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,
      CAST(TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(ROWKEY_POSITION AS NUMBER) AS COLUMN_POSITION,
      CASE WHEN DATA_TYPE >= 1 AND DATA_TYPE <= 16 THEN CAST(22 AS NUMBER)
           WHEN DATA_TYPE = 17 THEN CAST(7 AS NUMBER)
           WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 2 THEN CAST(DATA_LENGTH AS NUMBER)
           WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 1 AND F.COLLATION_TYPE IN (45, 46, 224, 54, 55, 101) THEN CAST(DATA_LENGTH * 4 AS NUMBER)
           WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 1 AND F.COLLATION_TYPE IN (28, 87) THEN CAST(DATA_LENGTH * 2 AS NUMBER)
           WHEN DATA_TYPE = 36 THEN CAST(12 AS NUMBER)
           WHEN DATA_TYPE IN (37, 38) THEN CAST(11 AS NUMBER)
           WHEN DATA_TYPE = 39 THEN CAST(DATA_LENGTH AS NUMBER)
           WHEN DATA_TYPE = 40 THEN CAST(5 AS NUMBER)
           WHEN DATA_TYPE = 41 THEN CAST(11 AS NUMBER)
        ELSE CAST(0 AS NUMBER) END AS COLUMN_LENGTH,
      CASE WHEN DATA_TYPE IN (22, 23) THEN CAST(DATA_LENGTH AS NUMBER)
        ELSE CAST(0 AS NUMBER) END AS CHAR_LENGTH,
      CAST('ASC' AS VARCHAR2(4)) AS DESCEND
      FROM
        (SELECT
        INDEX_OWNER,
        INDEX_NAME,
        TABLE_OWNER,
        TABLE_NAME,
        INDEX_ID,
        IDX_TYPE
        FROM
          (SELECT
          DATABASE_NAME AS INDEX_OWNER,
          CASE WHEN (TABLE_TYPE = 5) THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))
            ELSE (CONS_TAB.CONSTRAINT_NAME) END AS INDEX_NAME,
          DATABASE_NAME AS TABLE_OWNER,
          CASE WHEN (TABLE_TYPE = 3) THEN A.TABLE_ID
            ELSE A.DATA_TABLE_ID END AS TABLE_ID,
          A.TABLE_ID AS INDEX_ID,
          TABLE_TYPE AS IDX_TYPE
          FROM
            SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A
            JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID
            AND TABLE_TYPE IN (5, 3)
            AND B.DATABASE_NAME != '__recyclebin'
            AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND (A.DATABASE_ID = USERENV('SCHEMAID')
                 OR USER_CAN_ACCESS_OBJ(1, DECODE(TABLE_TYPE, 3, TABLE_ID, 5, DATA_TABLE_ID), A.DATABASE_ID) = 1)
            LEFT JOIN SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT CONS_TAB
              ON (CONS_TAB.TABLE_ID = A.TABLE_ID AND CONS_TAB.TENANT_ID = EFFECTIVE_TENANT_ID())
          WHERE
            NOT(
            TABLE_TYPE = 3
            AND CONSTRAINT_NAME IS NULL
            )
          ) C
        JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D ON C.TABLE_ID = D.TABLE_ID
              AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
        ) E
        JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON E.INDEX_ID = F.TABLE_ID
              AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
      WHERE
        F.ROWKEY_POSITION != 0 AND (CASE WHEN IDX_TYPE = 5 THEN INDEX_POSITION ELSE 1 END) != 0
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_IND_COLUMNS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25014',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
      CAST(TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(ROWKEY_POSITION AS NUMBER) AS COLUMN_POSITION,
      CASE WHEN DATA_TYPE >= 1 AND DATA_TYPE <= 16 THEN CAST(22 AS NUMBER)
           WHEN DATA_TYPE = 17 THEN CAST(7 AS NUMBER)
           WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 2 THEN CAST(DATA_LENGTH AS NUMBER)
           WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 1 AND F.COLLATION_TYPE IN (45, 46, 224, 54, 55, 101) THEN CAST(DATA_LENGTH * 4 AS NUMBER)
           WHEN DATA_TYPE IN (22, 23) AND F.DATA_PRECISION = 1 AND F.COLLATION_TYPE IN (28, 87) THEN CAST(DATA_LENGTH * 2 AS NUMBER)
           WHEN DATA_TYPE = 36 THEN CAST(12 AS NUMBER)
           WHEN DATA_TYPE IN (37, 38) THEN CAST(11 AS NUMBER)
           WHEN DATA_TYPE = 39 THEN CAST(DATA_LENGTH AS NUMBER)
           WHEN DATA_TYPE = 40 THEN CAST(5 AS NUMBER)
           WHEN DATA_TYPE = 41 THEN CAST(11 AS NUMBER)
        ELSE CAST(0 AS NUMBER) END AS COLUMN_LENGTH,
      CASE WHEN DATA_TYPE IN (22, 23) THEN CAST(DATA_LENGTH AS NUMBER)
        ELSE CAST(0 AS NUMBER) END AS CHAR_LENGTH,
      CAST('ASC' AS VARCHAR2(4)) AS DESCEND
      FROM
        (SELECT
        INDEX_OWNER,
        INDEX_NAME,
        TABLE_OWNER,
        TABLE_NAME,
        INDEX_ID,
        IDX_TYPE
        FROM
          (SELECT
          DATABASE_NAME AS INDEX_OWNER,
          CASE WHEN (TABLE_TYPE = 5) THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))
            ELSE (CONS_TAB.CONSTRAINT_NAME) END AS INDEX_NAME,
          DATABASE_NAME AS TABLE_OWNER,
          CASE WHEN (TABLE_TYPE = 3) THEN A.TABLE_ID
            ELSE A.DATA_TABLE_ID END AS TABLE_ID,
          A.TABLE_ID AS INDEX_ID,
          TABLE_TYPE AS IDX_TYPE
          FROM
            SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A
            JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID
            AND TABLE_TYPE IN (5, 3)
            AND A.DATABASE_ID = USERENV('SCHEMAID')
            AND B.DATABASE_NAME != '__recyclebin'
            AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
            LEFT JOIN SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT CONS_TAB
              ON (CONS_TAB.TABLE_ID = A.TABLE_ID AND CONS_TAB.TENANT_ID = EFFECTIVE_TENANT_ID())
          WHERE
            NOT(
            TABLE_TYPE = 3
            AND CONSTRAINT_NAME IS NULL
            )
          ) C
        JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D ON C.TABLE_ID = D.TABLE_ID
              AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
        ) E
        JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON E.INDEX_ID = F.TABLE_ID
              AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
      WHERE
        F.ROWKEY_POSITION != 0 AND (CASE WHEN IDX_TYPE = 5 THEN INDEX_POSITION ELSE 1 END) != 0
""".replace("\n", " ")
)

# end oracle view/synonym dba/all/user_ind_columns


# oracle view/synonym dba/all/user_constraints
def_table_schema(
  table_name      = 'DBA_CONSTRAINTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25015',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(SUBSTR(A.TABLE_NAME, 7 + INSTR(SUBSTR(A.TABLE_NAME, 7), '_')) AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('U' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(NULL AS VARCHAR2(128)) AS R_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(NULL AS VARCHAR2(9)) AS DELETE_RULE,
    CAST('ENABLED' AS VARCHAR2(8)) AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CAST('VALIDATED' AS VARCHAR2(13)) AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CAST(NULL AS VARCHAR2(4)) AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(SUBSTR(A.TABLE_NAME, 7 + INSTR(SUBSTR(A.TABLE_NAME, 7), '_')) AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C WHERE A.DATA_TABLE_ID = B.TABLE_ID AND A.DATABASE_ID = C.DATABASE_ID
      AND A.INDEX_TYPE IN (2, 4, 8) AND C.DATABASE_NAME != '__recyclebin'
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('R' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS R_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(CASE WHEN DELETE_ACTION = 1 THEN 'NO ACTION'
         WHEN DELETE_ACTION = 2 THEN 'CASCADE'
      ELSE 'SET NULL' END AS VARCHAR2(9)) AS DELETE_RULE,
    CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
         ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
         ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
         ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C
    WHERE A.CHILD_TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND A.REF_CST_TYPE = 0 AND A.REF_CST_ID = -1
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('R' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(E.DATABASE_NAME AS VARCHAR2(128)) AS R_OWNER,
    CAST(CASE WHEN A.REF_CST_TYPE = 2 THEN SUBSTR(F.TABLE_NAME, 7 + INSTR(SUBSTR(F.TABLE_NAME, 7), '_'))
         ELSE NULL END AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(CASE WHEN DELETE_ACTION = 1 THEN 'NO ACTION'
         WHEN DELETE_ACTION = 2 THEN 'CASCADE'
         ELSE 'SET NULL' END AS VARCHAR2(9)) AS DELETE_RULE,
    CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
         ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
         ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
         ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT F
      WHERE A.CHILD_TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND A.PARENT_TABLE_ID = D.TABLE_ID AND D.DATABASE_ID = E.DATABASE_ID AND (A.REF_CST_ID = F.TABLE_ID AND A.REF_CST_TYPE = 2)
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('R' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(E.DATABASE_NAME AS VARCHAR2(128)) AS R_OWNER,
    CAST(CASE WHEN A.REF_CST_TYPE = 1 THEN F.CONSTRAINT_NAME
        ELSE NULL END AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(CASE WHEN DELETE_ACTION = 1 THEN 'NO ACTION'
         WHEN DELETE_ACTION = 2 THEN 'CASCADE'
         ELSE 'SET NULL' END AS VARCHAR2(9)) AS DELETE_RULE,
    CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
         ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
         ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
         ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E, SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT F
      WHERE A.CHILD_TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND A.PARENT_TABLE_ID = D.TABLE_ID AND D.DATABASE_ID = E.DATABASE_ID AND (A.PARENT_TABLE_ID = F.TABLE_ID AND A.REF_CST_TYPE = 1 AND F.CONSTRAINT_TYPE = 1 AND A.REF_CST_ID = F.CONSTRAINT_ID)
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(CONSTRAINT_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CASE WHEN A.CONSTRAINT_TYPE = 1 THEN CAST('P' AS VARCHAR2(1))
        ELSE CAST('C' AS VARCHAR2(1)) END AS CONSTRAINT_TYPE,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CASE WHEN A.CONSTRAINT_TYPE = 1 THEN CAST(NULL AS VARCHAR2(4000))
        ELSE CAST(A.CHECK_EXPR AS VARCHAR2(4000)) END AS SEARCH_CONDITION,
      CAST(NULL AS VARCHAR2(128)) AS R_OWNER,
      CAST(NULL AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
      CAST(NULL AS VARCHAR2(9)) AS DELETE_RULE,
      CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
        ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
      CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
      CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
      CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
        ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
      CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
      CAST(NULL AS VARCHAR2(3)) AS BAD,
      CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
        ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
      CAST(NULL AS DATE) AS LAST_CHANGE,
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS INDEX_OWNER,
      CASE WHEN A.CONSTRAINT_TYPE = 1 THEN CAST(A.CONSTRAINT_NAME AS VARCHAR2(128))
        ELSE CAST(NULL AS VARCHAR2(128)) END AS INDEX_NAME,
      CAST(NULL AS VARCHAR2(7)) AS INVALID,
      CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C
    WHERE A.TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND C.DATABASE_NAME != '__recyclebin'
    AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_CONSTRAINTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25016',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(SUBSTR(A.TABLE_NAME, 7 + INSTR(SUBSTR(A.TABLE_NAME, 7), '_')) AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('U' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(NULL AS VARCHAR2(128)) AS R_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(NULL AS VARCHAR2(9)) AS DELETE_RULE,
    CAST('ENABLED' AS VARCHAR2(8)) AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CAST('VALIDATED' AS VARCHAR2(13)) AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CAST(NULL AS VARCHAR2(4)) AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(SUBSTR(A.TABLE_NAME, 7 + INSTR(SUBSTR(A.TABLE_NAME, 7), '_')) AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C
    WHERE A.DATA_TABLE_ID = B.TABLE_ID
      AND A.DATABASE_ID = C.DATABASE_ID
      AND (A.DATABASE_ID = USERENV('SCHEMAID')
          OR USER_CAN_ACCESS_OBJ(1, A.DATA_TABLE_ID, A.DATABASE_ID) = 1)
      AND A.INDEX_TYPE IN (2, 4, 8) AND C.DATABASE_NAME != '__recyclebin'
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('R' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS R_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(CASE WHEN DELETE_ACTION = 1 THEN 'NO ACTION'
         WHEN DELETE_ACTION = 2 THEN 'CASCADE'
      ELSE 'SET NULL' END AS VARCHAR2(9)) AS DELETE_RULE,
    CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
         ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
         ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
         ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C
    WHERE A.CHILD_TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND A.REF_CST_TYPE = 0 AND A.REF_CST_ID = -1
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND (B.DATABASE_ID = USERENV('SCHEMAID')
             OR USER_CAN_ACCESS_OBJ(1, B.TABLE_ID, B.DATABASE_ID) = 1)
    UNION ALL
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('R' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(E.DATABASE_NAME AS VARCHAR2(128)) AS R_OWNER,
    CAST(CASE WHEN A.REF_CST_TYPE = 2 THEN SUBSTR(F.TABLE_NAME, 7 + INSTR(SUBSTR(F.TABLE_NAME, 7), '_'))
         ELSE NULL END AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(CASE WHEN DELETE_ACTION = 1 THEN 'NO ACTION'
         WHEN DELETE_ACTION = 2 THEN 'CASCADE'
         ELSE 'SET NULL' END AS VARCHAR2(9)) AS DELETE_RULE,
    CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
         ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
         ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
         ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT F
      WHERE A.CHILD_TABLE_ID = B.TABLE_ID
        AND B.DATABASE_ID = C.DATABASE_ID
        AND A.PARENT_TABLE_ID = D.TABLE_ID
        AND D.DATABASE_ID = E.DATABASE_ID
        AND (D.DATABASE_ID = USERENV('SCHEMAID')
             OR USER_CAN_ACCESS_OBJ(1, D.TABLE_ID, D.DATABASE_ID) = 1)
        AND (A.REF_CST_ID = F.TABLE_ID AND A.REF_CST_TYPE = 2)
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('R' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(E.DATABASE_NAME AS VARCHAR2(128)) AS R_OWNER,
    CAST(CASE WHEN A.REF_CST_TYPE = 1 THEN F.CONSTRAINT_NAME
        ELSE NULL END AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(CASE WHEN DELETE_ACTION = 1 THEN 'NO ACTION'
         WHEN DELETE_ACTION = 2 THEN 'CASCADE'
         ELSE 'SET NULL' END AS VARCHAR2(9)) AS DELETE_RULE,
    CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
         ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
         ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
         ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E, SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT F
      WHERE A.CHILD_TABLE_ID = B.TABLE_ID
        AND B.DATABASE_ID = C.DATABASE_ID
        AND A.PARENT_TABLE_ID = D.TABLE_ID
        AND D.DATABASE_ID = E.DATABASE_ID
        AND (D.DATABASE_ID = USERENV('SCHEMAID')
             OR USER_CAN_ACCESS_OBJ(1, D.TABLE_ID, D.DATABASE_ID) = 1)
        AND (A.PARENT_TABLE_ID = F.TABLE_ID
             AND A.REF_CST_TYPE = 1
             AND F.CONSTRAINT_TYPE = 1
             AND A.REF_CST_ID = F.CONSTRAINT_ID)
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(CONSTRAINT_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CASE WHEN A.CONSTRAINT_TYPE = 1 THEN CAST('P' AS VARCHAR2(1))
        ELSE CAST('C' AS VARCHAR2(1)) END AS CONSTRAINT_TYPE,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CASE WHEN A.CONSTRAINT_TYPE = 1 THEN CAST(NULL AS VARCHAR2(4000))
        ELSE CAST(A.CHECK_EXPR AS VARCHAR2(4000)) END AS SEARCH_CONDITION,
      CAST(NULL AS VARCHAR2(128)) AS R_OWNER,
      CAST(NULL AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
      CAST(NULL AS VARCHAR2(9)) AS DELETE_RULE,
      CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
        ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
      CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
      CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
      CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
        ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
      CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
      CAST(NULL AS VARCHAR2(3)) AS BAD,
      CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
        ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
      CAST(NULL AS DATE) AS LAST_CHANGE,
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS INDEX_OWNER,
      CASE WHEN A.CONSTRAINT_TYPE = 1 THEN CAST(A.CONSTRAINT_NAME AS VARCHAR2(128))
        ELSE CAST(NULL AS VARCHAR2(128)) END AS INDEX_NAME,
      CAST(NULL AS VARCHAR2(7)) AS INVALID,
      CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C
    WHERE A.TABLE_ID = B.TABLE_ID
      AND B.DATABASE_ID = C.DATABASE_ID
      AND (B.DATABASE_ID = USERENV('SCHEMAID')
             OR USER_CAN_ACCESS_OBJ(1, B.TABLE_ID, B.DATABASE_ID) = 1)
      AND C.DATABASE_NAME != '__recyclebin'
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_CONSTRAINTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25017',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(SUBSTR(A.TABLE_NAME, 7 + INSTR(SUBSTR(A.TABLE_NAME, 7), '_')) AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('U' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(NULL AS VARCHAR2(128)) AS R_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(NULL AS VARCHAR2(9)) AS DELETE_RULE,
    CAST('ENABLED' AS VARCHAR2(8)) AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CAST('VALIDATED' AS VARCHAR2(13)) AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CAST(NULL AS VARCHAR2(4)) AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(SUBSTR(A.TABLE_NAME, 7 + INSTR(SUBSTR(A.TABLE_NAME, 7), '_')) AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C WHERE A.DATA_TABLE_ID = B.TABLE_ID AND A.DATABASE_ID = C.DATABASE_ID
      AND A.DATABASE_ID = USERENV('SCHEMAID') AND A.INDEX_TYPE IN (2, 4, 8)
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('R' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS R_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(CASE WHEN DELETE_ACTION = 1 THEN 'NO ACTION'
         WHEN DELETE_ACTION = 2 THEN 'CASCADE'
      ELSE 'SET NULL' END AS VARCHAR2(9)) AS DELETE_RULE,
    CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
         ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
         ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CAST(NULL AS VARCHAR2(4)) AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C
      WHERE A.CHILD_TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND A.REF_CST_TYPE = 0 AND A.REF_CST_ID = -1 AND B.DATABASE_ID = USERENV('SCHEMAID')
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('R' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(E.DATABASE_NAME AS VARCHAR2(128)) AS R_OWNER,
    CAST(CASE WHEN A.REF_CST_TYPE = 2 THEN SUBSTR(F.TABLE_NAME, 7 + INSTR(SUBSTR(F.TABLE_NAME, 7), '_'))
         ELSE NULL END AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(CASE WHEN DELETE_ACTION = 1 THEN 'NO ACTION'
         WHEN DELETE_ACTION = 2 THEN 'CASCADE'
         ELSE 'SET NULL' END AS VARCHAR2(9)) AS DELETE_RULE,
    CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
         ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
         ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
         ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT F
      WHERE A.CHILD_TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND A.PARENT_TABLE_ID = D.TABLE_ID AND D.DATABASE_ID = E.DATABASE_ID AND (A.REF_CST_ID = F.TABLE_ID AND A.REF_CST_TYPE = 2) AND B.DATABASE_ID = USERENV('SCHEMAID')
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
    CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
    CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
    CAST('R' AS VARCHAR2(1)) AS CONSTRAINT_TYPE,
    CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(NULL AS VARCHAR2(4000)) AS SEARCH_CONDITION,
    CAST(E.DATABASE_NAME AS VARCHAR2(128)) AS R_OWNER,
    CAST(CASE WHEN A.REF_CST_TYPE = 1 THEN F.CONSTRAINT_NAME
        ELSE NULL END AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
    CAST(CASE WHEN DELETE_ACTION = 1 THEN 'NO ACTION'
         WHEN DELETE_ACTION = 2 THEN 'CASCADE'
         ELSE 'SET NULL' END AS VARCHAR2(9)) AS DELETE_RULE,
    CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
         ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
    CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
    CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
    CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
         ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
    CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
    CAST(NULL AS VARCHAR2(3)) AS BAD,
    CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
         ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
    CAST(NULL AS DATE) AS LAST_CHANGE,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(NULL AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(NULL AS VARCHAR2(7)) AS INVALID,
    CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E, SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT F
      WHERE A.CHILD_TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND A.PARENT_TABLE_ID = D.TABLE_ID AND D.DATABASE_ID = E.DATABASE_ID AND (A.PARENT_TABLE_ID = F.TABLE_ID AND A.REF_CST_TYPE = 1 AND F.CONSTRAINT_TYPE = 1 AND A.REF_CST_ID = F.CONSTRAINT_ID) AND B.DATABASE_ID = USERENV('SCHEMAID')
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(CONSTRAINT_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CASE WHEN A.CONSTRAINT_TYPE = 1 THEN CAST('P' AS VARCHAR2(1))
        ELSE CAST('C' AS VARCHAR2(1)) END AS CONSTRAINT_TYPE,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CASE WHEN A.CONSTRAINT_TYPE = 1 THEN CAST(NULL AS VARCHAR2(4000))
        ELSE CAST(A.CHECK_EXPR AS VARCHAR2(4000)) END AS SEARCH_CONDITION,
      CAST(NULL AS VARCHAR2(128)) AS R_OWNER,
      CAST(NULL AS VARCHAR2(128)) AS R_CONSTRAINT_NAME,
      CAST(NULL AS VARCHAR2(9)) AS DELETE_RULE,
      CASE WHEN A.ENABLE_FLAG = 1 THEN CAST('ENABLED' AS VARCHAR2(8))
        ELSE CAST('DISABLED' AS VARCHAR2(8)) END AS STATUS,
      CAST('NOT DEFERRABLE' AS VARCHAR2(14)) AS DEFERRABLE,
      CAST('IMMEDIATE' AS VARCHAR2(9)) AS DEFERRED,
      CASE WHEN A.VALIDATE_FLAG = 1 THEN CAST('VALIDATED' AS VARCHAR2(13))
        ELSE CAST('NOT VALIDATED' AS VARCHAR2(13)) END AS VALIDATED,
      CAST(NULL AS VARCHAR2(14)) AS "GENERATED",
      CAST(NULL AS VARCHAR2(3)) AS BAD,
      CASE WHEN A.RELY_FLAG = 1 THEN CAST('RELY' AS VARCHAR2(4))
        ELSE CAST(NULL AS VARCHAR2(4)) END AS RELY,
      CAST(NULL AS DATE) AS LAST_CHANGE,
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS INDEX_OWNER,
      CASE WHEN A.CONSTRAINT_TYPE = 1 THEN CAST(A.CONSTRAINT_NAME AS VARCHAR2(128))
        ELSE CAST(NULL AS VARCHAR2(128)) END AS INDEX_NAME,
      CAST(NULL AS VARCHAR2(7)) AS INVALID,
      CAST(NULL AS VARCHAR2(14)) AS VIEW_RELATED
    FROM SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C
    WHERE C.DATABASE_ID = USERENV('SCHEMAID') AND A.TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND C.DATABASE_NAME != '__recyclebin'
    AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)
# end oracle view/synonym dba/all/user_constraints

def_table_schema(
  table_name      = 'ALL_TAB_COLS_V$',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25018',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  cast(db.database_name as VARCHAR2(128)) as OWNER,
  cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,
  cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,
  cast(decode(c.data_type,
        0, 'NULL',

        1, 'NUMBER',
        2, 'NUMBER',
        3, 'NUMBER',
        4, 'NUMBER',
        5, 'NUMBER',

        6, 'NUMBER',
        7, 'NUMBER',
        8, 'NUMBER',
        9, 'NUMBER',
        10, 'NUMBER',

        11, 'BINARY_FLOAT',
        12, 'BINARY_DOUBLE',

        13, 'NUMBER',
        14, 'NUMBER',

        15, 'NUMBER',
        16, 'NUMBER',

        17, 'DATE',
        18, 'TIMESTAMP',
        19, 'DATE',
        20, 'TIME',
        21, 'YEAR',

        22, 'VARCHAR2',
        23, 'CHAR',
        24, 'HEX_STRING',

        25, 'EXT',
        26, 'UNKNOWN',

        27, 'TINYTEXT',
        28, 'TEXT',
        29, 'MEDIUMTEXT',
        30, decode(c.collation_type, 63, 'BLOB', 'CLOB'),
        31, 'BIT',
        32, 'ENUM',
        33, 'SET',
        34, 'ENUM_INNER',
        35, 'SET_INNER',
        36, concat('TIMESTAMP(', concat(c.data_scale, ') WITH TIME ZONE')),
        37, concat('TIMESTAMP(', concat(c.data_scale, ') WITH LOCAL TIME ZONE')),
        38, concat('TIMESTAMP(', concat(c.data_scale, ')')),
        39, 'RAW',
        40, concat('INTERVAL YEAR(', concat(c.data_scale, ') TO MONTH')),
        41, concat('INTERVAL DAY(', concat(trunc(c.data_scale/10), concat(') TO SECOND(', concat(mod(c.data_scale, 10), ')')))),
        42, 'FLOAT',
        43, 'NVARCHAR2',
        44, 'NCHAR',
        45, 'UROWID',
        46, '',
        'UNDEFINED') as VARCHAR2(128)) as  DATA_TYPE,
  cast(NULL as VARCHAR2(3)) as  DATA_TYPE_MOD,
  cast(NULL as VARCHAR2(128)) as  DATA_TYPE_OWNER,
  cast(c.data_length * CASE WHEN c.data_type in (22,23,30,43,44,46) and c.data_precision = 1
                            THEN decode(c.collation_type, 63, 1, 249, 4, 248, 4, 87, 2, 28, 2, 55, 4, 54, 4, 101, 2, 46, 4, 45, 4, 224, 4, 1)
                            ELSE 1 END
                            as NUMBER) as DATA_LENGTH,
  cast(CASE WHEN c.data_type in (11,12,17,18,19,22,23,27,28,29,30,36,37,38,43,44)
            THEN NULL
            ELSE CASE WHEN c.data_precision < 0 THEN NULL ELSE c.data_precision END
       END as NUMBER) as  DATA_PRECISION,
  cast(CASE WHEN c.data_type in (11,12,17,19,22,23,27,28,29,30,42,43,44)
            THEN NULL
            ELSE CASE WHEN c.data_scale < -84 THEN NULL ELSE c.data_scale END
       END as NUMBER) as  DATA_SCALE,
  cast(decode(c.nullable, 1, 'Y', 'N') as VARCHAR2(1)) as  NULLABLE,
  cast(decode(BITAND(c.column_flags, 64), 0, c.column_id, NULL) as NUMBER) as  COLUMN_ID,
  cast(LENGTHB(c.cur_default_value_v2) as NUMBER) as  DEFAULT_LENGTH,
  cast(c.cur_default_value_v2 as /* TODO: LONG() */ VARCHAR(128)) as  DATA_DEFAULT,
  cast(NULL as NUMBER) as  NUM_DISTINCT,
  cast(NULL as /* TODO: RAW */ varchar(128)) as  LOW_VALUE,
  cast(NULL as /* TODO: RAW */ varchar(128)) as  HIGH_VALUE,
  cast(NULL as NUMBER) as  DENSITY,
  cast(NULL as NUMBER) as  NUM_NULLS,
  cast(NULL as NUMBER) as  NUM_BUCKETS,
  cast(NULL as DATE) as  LAST_ANALYZED,
  cast(NULL as NUMBER) as  SAMPLE_SIZE,
  cast(decode(c.data_type,
         22, 'CHAR_CS',
         23, 'CHAR_CS',
         30, decode(c.collation_type, 63, 'NULL', 'CHAR_CS'),
         43, 'NCHAR_CS',
         44, 'NCHAR_CS',
         '') as VARCHAR2(44)) as CHARACTER_SET_NAME,
  cast(NULL as NUMBER) as  CHAR_COL_DECL_LENGTH,
  cast(NULL as VARCHAR2(3)) as  GLOBAL_STATS,
  cast(NULL as VARCHAR2(3)) as  USER_STATS,
  cast(NULL as VARCHAR2(80)) as  NOTES,
  cast(NULL as NUMBER) as  AVG_COL_LEN,
  cast(CASE WHEN c.data_type in (22,23,43,44) THEN c.data_length ELSE 0 END as NUMBER) as  CHAR_LENGTH,
  cast(decode(c.data_type,
         22, decode(c.data_precision, 1, 'C', 'B'),
         23, decode(c.data_precision, 1, 'C', 'B'),
         43, decode(c.data_precision, 1, 'C', 'B'),
         44, decode(c.data_precision, 1, 'C', 'B'),
         NULL) as VARCHAR2(1)) as  CHAR_USED,
  cast(NULL as VARCHAR2(3)) as  V80_FMT_IMAGE,
  cast(NULL as VARCHAR2(3)) as  DATA_UPGRADED,
  cast(decode(BITAND(c.column_flags, 64), 0, 'NO', 'YES') as VARCHAR2(3)) as HIDDEN_COLUMN,
  cast(decode(BITAND(c.column_flags, 1), 1, 'YES', 'NO') as VARCHAR2(3)) as  VIRTUAL_COLUMN,
  cast(NULL as NUMBER) as  SEGMENT_COLUMN_ID,
  cast(NULL as NUMBER) as  INTERNAL_COLUMN_ID,
  cast(NULL as VARCHAR2(15)) as  HISTOGRAM,
  cast(c.column_name as VARCHAR2(4000)) as  QUALIFIED_COL_NAME,
  cast('YES' as VARCHAR2(3)) as  USER_GENERATED,
  cast(NULL as VARCHAR2(3)) as  DEFAULT_ON_NULL,
  cast(NULL as VARCHAR2(3)) as  IDENTITY_COLUMN,
  cast(NULL as VARCHAR2(128)) as  EVALUATION_EDITION,
  cast(NULL as VARCHAR2(128)) as  UNUSABLE_BEFORE,
  cast(NULL as VARCHAR2(128)) as  UNUSABLE_BEGINNING,
  cast(NULL as VARCHAR2(100)) as  COLLATION,
  cast(NULL as NUMBER) as  COLLATED_COLUMN_ID
FROM
    sys.ALL_VIRTUAL_TABLE_REAL_AGENT t
  JOIN
    sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db
    ON db.tenant_id = t.tenant_id
    AND db.database_id = t.database_id
    AND (t.database_id = userenv('SCHEMAID')
         OR user_can_access_obj(1, t.table_id, t.database_id) = 1)
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
  JOIN
    sys.ALL_VIRTUAL_COLUMN_REAL_AGENT c
    ON c.tenant_id = t.tenant_id
    AND c.table_id = t.table_id
    AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
WHERE
  c.is_hidden = 0
  AND t.table_type in (0,2,3,8,9)
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_TAB_COLS_V$',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25019',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  cast(db.database_name as VARCHAR2(128)) as OWNER,
  cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,
  cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,
  cast(decode(c.data_type,
        0, 'NULL',

        1, 'NUMBER',
        2, 'NUMBER',
        3, 'NUMBER',
        4, 'NUMBER',
        5, 'NUMBER',

        6, 'NUMBER',
        7, 'NUMBER',
        8, 'NUMBER',
        9, 'NUMBER',
        10, 'NUMBER',

        11, 'BINARY_FLOAT',
        12, 'BINARY_DOUBLE',

        13, 'NUMBER',
        14, 'NUMBER',

        15, 'NUMBER',
        16, 'NUMBER',

        17, 'DATE',
        18, 'TIMESTAMP',
        19, 'DATE',
        20, 'TIME',
        21, 'YEAR',

        22, 'VARCHAR2',
        23, 'CHAR',
        24, 'HEX_STRING',

        25, 'EXT',
        26, 'UNKNOWN',

        27, 'TINYTEXT',
        28, 'TEXT',
        29, 'MEDIUMTEXT',
        30, decode(c.collation_type, 63, 'BLOB', 'CLOB'),
        31, 'BIT',
        32, 'ENUM',
        33, 'SET',
        34, 'ENUM_INNER',
        35, 'SET_INNER',
        36, concat('TIMESTAMP(', concat(c.data_scale, ') WITH TIME ZONE')),
        37, concat('TIMESTAMP(', concat(c.data_scale, ') WITH LOCAL TIME ZONE')),
        38, concat('TIMESTAMP(', concat(c.data_scale, ')')),
        39, 'RAW',
        40, concat('INTERVAL YEAR(', concat(c.data_scale, ') TO MONTH')),
        41, concat('INTERVAL DAY(', concat(trunc(c.data_scale/10), concat(') TO SECOND(', concat(mod(c.data_scale, 10), ')')))),
        42, 'FLOAT',
        43, 'NVARCHAR2',
        44, 'NCHAR',
        45, '',
        'UNDEFINED') as VARCHAR2(128)) as  DATA_TYPE,
  cast(NULL as VARCHAR2(3)) as  DATA_TYPE_MOD,
  cast(NULL as VARCHAR2(128)) as  DATA_TYPE_OWNER,
  cast(c.data_length * CASE WHEN c.data_type in (22,23,30,43,44,46) and c.data_precision = 1
                            THEN decode(c.collation_type, 63, 1, 249, 4, 248, 4, 87, 2, 28, 2, 55, 4, 54, 4, 101, 2, 46, 4, 45, 4, 224, 4, 1)
                            ELSE 1 END
                            as NUMBER) as  DATA_LENGTH,
  cast(CASE WHEN c.data_type in (11,12,17,18,19,22,23,27,28,29,30,36,37,38,43,44)
            THEN NULL
            ELSE CASE WHEN c.data_precision < 0 THEN NULL ELSE c.data_precision END
       END as NUMBER) as  DATA_PRECISION,
  cast(CASE WHEN c.data_type in (11,12,17,19,22,23,27,28,29,30,42,43,44)
            THEN NULL
            ELSE CASE WHEN c.data_scale < -84 THEN NULL ELSE c.data_scale END
       END as NUMBER) as  DATA_SCALE,
  cast(decode(c.nullable, 1, 'Y', 'N') as VARCHAR2(1)) as  NULLABLE,
  cast(decode(BITAND(c.column_flags, 64), 0, c.column_id, NULL) as NUMBER) as  COLUMN_ID,
  cast(LENGTHB(c.cur_default_value_v2) as NUMBER) as  DEFAULT_LENGTH,
  cast(c.cur_default_value_v2 as /* TODO: LONG() */ VARCHAR(128)) as  DATA_DEFAULT,
  cast(NULL as NUMBER) as  NUM_DISTINCT,
  cast(NULL as /* TODO: RAW */ varchar(128)) as  LOW_VALUE,
  cast(NULL as /* TODO: RAW */ varchar(128)) as  HIGH_VALUE,
  cast(NULL as NUMBER) as  DENSITY,
  cast(NULL as NUMBER) as  NUM_NULLS,
  cast(NULL as NUMBER) as  NUM_BUCKETS,
  cast(NULL as DATE) as  LAST_ANALYZED,
  cast(NULL as NUMBER) as  SAMPLE_SIZE,
  cast(decode(c.data_type,
         22, 'CHAR_CS',
         23, 'CHAR_CS',
         30, decode(c.collation_type, 63, 'NULL', 'CHAR_CS'),
         43, 'NCHAR_CS',
         44, 'NCHAR_CS',
         '') as VARCHAR2(44)) as  CHARACTER_SET_NAME,
  cast(NULL as NUMBER) as  CHAR_COL_DECL_LENGTH,
  cast(NULL as VARCHAR2(3)) as  GLOBAL_STATS,
  cast(NULL as VARCHAR2(3)) as  USER_STATS,
  cast(NULL as VARCHAR2(80)) as  NOTES,
  cast(NULL as NUMBER) as  AVG_COL_LEN,
  cast(CASE WHEN c.data_type in (22,23,43,44) THEN c.data_length ELSE 0 END as NUMBER) as  CHAR_LENGTH,
  cast(decode(c.data_type,
         22, decode(c.data_precision, 1, 'C', 'B'),
         23, decode(c.data_precision, 1, 'C', 'B'),
         43, decode(c.data_precision, 1, 'C', 'B'),
         44, decode(c.data_precision, 1, 'C', 'B'),
         NULL) as VARCHAR2(1)) as  CHAR_USED,
  cast(NULL as VARCHAR2(3)) as  V80_FMT_IMAGE,
  cast(NULL as VARCHAR2(3)) as  DATA_UPGRADED,
  cast(decode(BITAND(c.column_flags, 64), 0, 'NO', 'YES') as VARCHAR2(3)) as HIDDEN_COLUMN,
  cast(decode(BITAND(c.column_flags, 1), 1, 'YES', 'NO') as VARCHAR2(3)) as  VIRTUAL_COLUMN,
  cast(NULL as NUMBER) as  SEGMENT_COLUMN_ID,
  cast(NULL as NUMBER) as  INTERNAL_COLUMN_ID,
  cast(NULL as VARCHAR2(15)) as  HISTOGRAM,
  cast(c.column_name as VARCHAR2(4000)) as  QUALIFIED_COL_NAME,
  cast('YES' as VARCHAR2(3)) as  USER_GENERATED,
  cast(NULL as VARCHAR2(3)) as  DEFAULT_ON_NULL,
  cast(NULL as VARCHAR2(3)) as  IDENTITY_COLUMN,
  cast(NULL as VARCHAR2(128)) as  EVALUATION_EDITION,
  cast(NULL as VARCHAR2(128)) as  UNUSABLE_BEFORE,
  cast(NULL as VARCHAR2(128)) as  UNUSABLE_BEGINNING,
  cast(NULL as VARCHAR2(100)) as  COLLATION,
  cast(NULL as NUMBER) as  COLLATED_COLUMN_ID
FROM
    sys.ALL_VIRTUAL_TABLE_REAL_AGENT t
  JOIN
    sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db
    ON db.tenant_id = t.tenant_id
    AND db.database_id = t.database_id
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
  JOIN
    sys.ALL_VIRTUAL_COLUMN_REAL_AGENT c
    ON c.tenant_id = t.tenant_id
    AND c.table_id = t.table_id
    AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
WHERE
  c.is_hidden = 0
  AND t.table_type in (0,2,3,8,9)
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_TAB_COLS_V$',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25020',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  cast(t.table_name as VARCHAR2(128)) as  TABLE_NAME,
  cast(c.column_name as VARCHAR2(128)) as  COLUMN_NAME,
  cast(decode(c.data_type,
        0, 'NULL',

        1, 'NUMBER',
        2, 'NUMBER',
        3, 'NUMBER',
        4, 'NUMBER',
        5, 'NUMBER',

        6, 'NUMBER',
        7, 'NUMBER',
        8, 'NUMBER',
        9, 'NUMBER',
        10, 'NUMBER',

        11, 'BINARY_FLOAT',
        12, 'BINARY_DOUBLE',

        13, 'NUMBER',
        14, 'NUMBER',

        15, 'NUMBER',
        16, 'NUMBER',

        17, 'DATE',
        18, 'TIMESTAMP',
        19, 'DATE',
        20, 'TIME',
        21, 'YEAR',

        22, 'VARCHAR2',
        23, 'CHAR',
        24, 'HEX_STRING',

        25, 'EXT',
        26, 'UNKNOWN',

        27, 'TINYTEXT',
        28, 'TEXT',
        29, 'MEDIUMTEXT',
        30, decode(c.collation_type, 63, 'BLOB', 'CLOB'),
        31, 'BIT',
        32, 'ENUM',
        33, 'SET',
        34, 'ENUM_INNER',
        35, 'SET_INNER',
        36, concat('TIMESTAMP(', concat(c.data_scale, ') WITH TIME ZONE')),
        37, concat('TIMESTAMP(', concat(c.data_scale, ') WITH LOCAL TIME ZONE')),
        38, concat('TIMESTAMP(', concat(c.data_scale, ')')),
        39, 'RAW',
        40, concat('INTERVAL YEAR(', concat(c.data_scale, ') TO MONTH')),
        41, concat('INTERVAL DAY(', concat(trunc(c.data_scale/10), concat(') TO SECOND(', concat(mod(c.data_scale, 10), ')')))),
        42, 'FLOAT',
        43, 'NVARCHAR2',
        44, 'NCHAR',
        45, '',
        'UNDEFINED') as VARCHAR2(128)) as  DATA_TYPE,
  cast(NULL as VARCHAR2(3)) as  DATA_TYPE_MOD,
  cast(NULL as VARCHAR2(128)) as  DATA_TYPE_OWNER,
  cast(c.data_length * CASE WHEN c.data_type in (22,23,30,43,44,46) and c.data_precision = 1
                            THEN decode(c.collation_type, 63, 1, 249, 4, 248, 4, 87, 2, 28, 2, 55, 4, 54, 4, 101, 2, 46, 4, 45, 4, 224, 4, 1)
                            ELSE 1 END
                            as NUMBER) as  DATA_LENGTH,
  cast(CASE WHEN c.data_type in (11,12,17,18,19,22,23,27,28,29,30,36,37,38,43,44)
            THEN NULL
            ELSE CASE WHEN c.data_precision < 0 THEN NULL ELSE c.data_precision END
       END as NUMBER) as  DATA_PRECISION,
  cast(CASE WHEN c.data_type in (11,12,17,19,22,23,27,28,29,30,42,43,44)
            THEN NULL
            ELSE CASE WHEN c.data_scale < -84 THEN NULL ELSE c.data_scale END
       END as NUMBER) as  DATA_SCALE,
  cast(decode(c.nullable, 1, 'Y', 'N') as VARCHAR2(1)) as  NULLABLE,
  cast(decode(BITAND(c.column_flags, 64), 0, c.column_id, NULL) as NUMBER) as  COLUMN_ID,
  cast(LENGTHB(c.cur_default_value_v2) as NUMBER) as  DEFAULT_LENGTH,
  cast(c.cur_default_value_v2 as /* TODO: LONG() */ VARCHAR(128)) as  DATA_DEFAULT,
  cast(NULL as NUMBER) as  NUM_DISTINCT,
  cast(NULL as /* TODO: RAW */ varchar(128)) as  LOW_VALUE,
  cast(NULL as /* TODO: RAW */ varchar(128)) as  HIGH_VALUE,
  cast(NULL as NUMBER) as  DENSITY,
  cast(NULL as NUMBER) as  NUM_NULLS,
  cast(NULL as NUMBER) as  NUM_BUCKETS,
  cast(NULL as DATE) as  LAST_ANALYZED,
  cast(NULL as NUMBER) as  SAMPLE_SIZE,
  cast(decode(c.data_type,
         22, 'CHAR_CS',
         23, 'CHAR_CS',
         30, decode(c.collation_type, 63, 'NULL', 'CHAR_CS'),
         43, 'NCHAR_CS',
         44, 'NCHAR_CS',
         '') as VARCHAR2(44)) as  CHARACTER_SET_NAME,
  cast(NULL as NUMBER) as  CHAR_COL_DECL_LENGTH,
  cast(NULL as VARCHAR2(3)) as  GLOBAL_STATS,
  cast(NULL as VARCHAR2(3)) as  USER_STATS,
  cast(NULL as VARCHAR2(80)) as  NOTES,
  cast(NULL as NUMBER) as  AVG_COL_LEN,
  cast(CASE WHEN c.data_type in (22,23,43,44) THEN c.data_length ELSE 0 END as NUMBER) as  CHAR_LENGTH,
  cast(decode(c.data_type,
         22, decode(c.data_precision, 1, 'C', 'B'),
         23, decode(c.data_precision, 1, 'C', 'B'),
         43, decode(c.data_precision, 1, 'C', 'B'),
         44, decode(c.data_precision, 1, 'C', 'B'),
         NULL) as VARCHAR2(1)) as  CHAR_USED,
  cast(NULL as VARCHAR2(3)) as  V80_FMT_IMAGE,
  cast(NULL as VARCHAR2(3)) as  DATA_UPGRADED,
  cast(decode(BITAND(c.column_flags, 64), 0, 'NO', 'YES') as VARCHAR2(3)) as HIDDEN_COLUMN,
  cast(decode(BITAND(c.column_flags, 1), 1, 'YES', 'NO') as VARCHAR2(3)) as  VIRTUAL_COLUMN,
  cast(NULL as NUMBER) as  SEGMENT_COLUMN_ID,
  cast(NULL as NUMBER) as  INTERNAL_COLUMN_ID,
  cast(NULL as VARCHAR2(15)) as  HISTOGRAM,
  cast(c.column_name as VARCHAR2(4000)) as  QUALIFIED_COL_NAME,
  cast('YES' as VARCHAR2(3)) as  USER_GENERATED,
  cast(NULL as VARCHAR2(3)) as  DEFAULT_ON_NULL,
  cast(NULL as VARCHAR2(3)) as  IDENTITY_COLUMN,
  cast(NULL as VARCHAR2(128)) as  EVALUATION_EDITION,
  cast(NULL as VARCHAR2(128)) as  UNUSABLE_BEFORE,
  cast(NULL as VARCHAR2(128)) as  UNUSABLE_BEGINNING,
  cast(NULL as VARCHAR2(100)) as  COLLATION,
  cast(NULL as NUMBER) as  COLLATED_COLUMN_ID
FROM
    sys.ALL_VIRTUAL_TABLE_REAL_AGENT t
  JOIN
    sys.ALL_VIRTUAL_COLUMN_REAL_AGENT c
    ON c.tenant_id = t.tenant_id
    AND c.table_id = t.table_id
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
WHERE
  c.is_hidden = 0
  AND t.table_type in (0,2,3,8,9)
  AND t.database_id = USERENV('SCHEMAID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_TAB_COLS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25021',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """select
  OWNER, TABLE_NAME,
  COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER,
  DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID,
  DEFAULT_LENGTH, DATA_DEFAULT, NUM_DISTINCT, LOW_VALUE, HIGH_VALUE,
  DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE,
  CHARACTER_SET_NAME, CHAR_COL_DECL_LENGTH,
  GLOBAL_STATS,
  USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED,
  V80_FMT_IMAGE, DATA_UPGRADED, HIDDEN_COLUMN, VIRTUAL_COLUMN,
  SEGMENT_COLUMN_ID, INTERNAL_COLUMN_ID, HISTOGRAM, QUALIFIED_COL_NAME
  from SYS.all_tab_cols_v$
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_TAB_COLS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25022',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
select
  OWNER, TABLE_NAME,
  COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER,
  DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID,
  DEFAULT_LENGTH, DATA_DEFAULT, NUM_DISTINCT, LOW_VALUE, HIGH_VALUE,
  DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE,
  CHARACTER_SET_NAME, CHAR_COL_DECL_LENGTH,
  GLOBAL_STATS,
  USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED,
  V80_FMT_IMAGE, DATA_UPGRADED, HIDDEN_COLUMN, VIRTUAL_COLUMN,
  SEGMENT_COLUMN_ID, INTERNAL_COLUMN_ID, HISTOGRAM, QUALIFIED_COL_NAME
from SYS.dba_tab_cols_v$
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_TAB_COLS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25023',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """select
  TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER,
  DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID,
  DEFAULT_LENGTH, DATA_DEFAULT, NUM_DISTINCT, LOW_VALUE, HIGH_VALUE,
  DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE,
  CHARACTER_SET_NAME, CHAR_COL_DECL_LENGTH,
  GLOBAL_STATS,
  USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED,
  V80_FMT_IMAGE, DATA_UPGRADED, HIDDEN_COLUMN, VIRTUAL_COLUMN,
  SEGMENT_COLUMN_ID, INTERNAL_COLUMN_ID, HISTOGRAM, QUALIFIED_COL_NAME
  from SYS.user_tab_cols_v$
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_TAB_COLUMNS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25024',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """select
  OWNER, TABLE_NAME,
  COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER,
  DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID,
  DEFAULT_LENGTH, DATA_DEFAULT, NUM_DISTINCT, LOW_VALUE, HIGH_VALUE,
  DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE,
  CHARACTER_SET_NAME, CHAR_COL_DECL_LENGTH,
  GLOBAL_STATS, USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED,
  V80_FMT_IMAGE, DATA_UPGRADED, HISTOGRAM
  from SYS.ALL_TAB_COLS
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_TAB_COLUMNS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25025',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
select
  OWNER, TABLE_NAME,
  COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER,
  DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID,
  DEFAULT_LENGTH, DATA_DEFAULT, NUM_DISTINCT, LOW_VALUE, HIGH_VALUE,
  DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE,
  CHARACTER_SET_NAME, CHAR_COL_DECL_LENGTH,
  GLOBAL_STATS, USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED,
  V80_FMT_IMAGE, DATA_UPGRADED, HISTOGRAM
from SYS.DBA_TAB_COLS
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_TAB_COLUMNS', \
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25026', \
  table_type      = 'SYSTEM_VIEW', \
  rowkey_columns  = [], \
  normal_columns  = [], \
  gm_columns      = [], \
  in_tenant_space = True, \
  view_definition = """select
  TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER,
  DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID,
  DEFAULT_LENGTH, DATA_DEFAULT, NUM_DISTINCT, LOW_VALUE, HIGH_VALUE,
  DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE,
  CHARACTER_SET_NAME, CHAR_COL_DECL_LENGTH,
  GLOBAL_STATS, USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED,
  V80_FMT_IMAGE, DATA_UPGRADED, HISTOGRAM
  from SYS.USER_TAB_COLS
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_TABLES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25027',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(db.database_name AS VARCHAR2(128)) AS OWNER,
  CAST(t.table_name AS VARCHAR2(128)) AS TABLE_NAME,
  CAST(tp.tablespace_name AS VARCHAR2(30)) AS TABLESPACE_NAME,
  CAST(NULL AS VARCHAR2(128)) AS CLUSTER_NAME,
  CAST(NULL AS VARCHAR2(128)) AS IOT_NAME,
  CAST('VALID' AS VARCHAR2(8)) AS STATUS,
  CAST(t."PCTFREE" AS NUMBER) AS PCT_FREE,
  CAST(NULL AS NUMBER) AS PCT_USED,
  CAST(NULL AS NUMBER) AS INI_TRANS,
  CAST(NULL AS NUMBER) AS MAX_TRANS,
  CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
  CAST(NULL AS NUMBER) AS NEXT_EXTENT,
  CAST(NULL AS NUMBER) AS MIN_EXTENTS,
  CAST(NULL AS NUMBER) AS MAX_EXTENTS,
  CAST(NULL AS NUMBER) AS PCT_INCREASE,
  CAST(NULL AS NUMBER) AS FREELISTS,
  CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
  CAST(NULL AS VARCHAR2(3)) AS LOGGING,
  CAST(NULL AS VARCHAR2(1)) AS BACKED_UP,
  CAST(info.row_count AS NUMBER) AS NUM_ROWS,
  CAST(NULL AS NUMBER) AS BLOCKS,
  CAST(NULL AS NUMBER) AS EMPTY_BLOCKS,
  CAST(NULL AS NUMBER) AS AVG_SPACE,
  CAST(NULL AS NUMBER) AS CHAIN_CNT,
  CAST(NULL AS NUMBER) AS AVG_ROW_LEN,
  CAST(NULL AS NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,
  CAST(NULL AS NUMBER) AS NUM_FREELIST_BLOCKS,
  CAST(NULL AS VARCHAR2(40)) AS DEGREE,
  CAST(NULL AS VARCHAR2(40)) AS INSTANCES,
  CAST(NULL AS VARCHAR2(20)) AS CACHE,
  CAST(NULL AS VARCHAR2(8)) AS TABLE_LOCK,
  CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
  CAST(NULL AS DATE) AS LAST_ANALYZED,
  CAST(
  CASE
    WHEN
      t.part_level = 0
    THEN
      'NO'
    ELSE
      'YES'
  END
  AS VARCHAR2(3)) AS PARTITIONED,
  CAST(NULL AS VARCHAR2(12)) AS IOT_TYPE,
  CAST(decode (t.table_type, 8, 'YES', 9, 'YES', 'NO') AS VARCHAR2(1)) AS TEMPORARY,
  CAST(NULL AS VARCHAR2(1)) AS SECONDARY,
  CAST('NO' AS VARCHAR2(3)) AS NESTED,
  CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
  CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
  CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
  CAST(NULL AS VARCHAR2(8)) AS ROW_MOVEMENT,
  CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
  CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
  CAST( decode (t.table_type, 8, 'SYS$SESSION', 9, 'SYS$TRANSACTION', NULL) AS VARCHAR2(15)) AS DURATION,
  CAST(NULL AS VARCHAR2(8)) AS SKIP_CORRUPT,
  CAST(NULL AS VARCHAR2(3)) AS MONITORING,
  CAST(NULL AS VARCHAR2(30)) AS CLUSTER_OWNER,
  CAST(NULL AS VARCHAR2(8)) AS DEPENDENCIES,
  CAST(NULL AS VARCHAR2(8)) AS COMPRESSION,
  CAST(NULL AS VARCHAR2(12)) AS COMPRESS_FOR,
  CAST(
  CASE
    WHEN
      db.database_name =  '__recyclebin'
    THEN 'YES'
    ELSE
      'NO'
  END
  AS VARCHAR2(3)) AS DROPPED,
  CAST(NULL AS VARCHAR2(3)) AS READ_ONLY,
  CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,
  CAST(NULL AS VARCHAR2(7)) AS RESULT_CACHE
FROM
  (
    SELECT
      tenant_id,
      table_id,
      SUM(row_count) AS row_count
    FROM
      sys.ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT p
    WHERE
      p.role = 1
      AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    GROUP BY
      tenant_id,
      table_id
  )
  info
  RIGHT JOIN
    sys.ALL_VIRTUAL_TABLE_REAL_AGENT t
    ON t.tenant_id = info.tenant_id
    AND t.table_id = info.table_id
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
JOIN
  sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db
ON
  db.tenant_id = t.tenant_id
  AND db.database_id = t.database_id
  AND db.database_name != '__recyclebin'
  AND t.table_type in (3, 8, 9, 11)
  AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
  AND (t.database_id = USERENV('SCHEMAID')
         or user_can_access_obj(1, t.table_id, t.database_id) =1)
LEFT JOIN
  sys.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT tp
ON
  tp.tablespace_id = t.tablespace_id
  AND t.tenant_id = tp.tenant_id
  AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
WHERE t.session_id = 0
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_TABLES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25028',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(db.database_name AS VARCHAR2(128)) AS OWNER,
  CAST(t.table_name AS VARCHAR2(128)) AS TABLE_NAME,
  CAST(tp.tablespace_name AS VARCHAR2(30)) AS TABLESPACE_NAME,
  CAST(NULL AS VARCHAR2(128)) AS CLUSTER_NAME,
  CAST(NULL AS VARCHAR2(128)) AS IOT_NAME,
  CAST('VALID' AS VARCHAR2(8)) AS STATUS,
  CAST(t."PCTFREE" AS NUMBER) AS PCT_FREE,
  CAST(NULL AS NUMBER) AS PCT_USED,
  CAST(NULL AS NUMBER) AS INI_TRANS,
  CAST(NULL AS NUMBER) AS MAX_TRANS,
  CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
  CAST(NULL AS NUMBER) AS NEXT_EXTENT,
  CAST(NULL AS NUMBER) AS MIN_EXTENTS,
  CAST(NULL AS NUMBER) AS MAX_EXTENTS,
  CAST(NULL AS NUMBER) AS PCT_INCREASE,
  CAST(NULL AS NUMBER) AS FREELISTS,
  CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
  CAST(NULL AS VARCHAR2(3)) AS LOGGING,
  CAST(NULL AS VARCHAR2(1)) AS BACKED_UP,
  CAST(info.row_count AS NUMBER) AS NUM_ROWS,
  CAST(NULL AS NUMBER) AS BLOCKS,
  CAST(NULL AS NUMBER) AS EMPTY_BLOCKS,
  CAST(NULL AS NUMBER) AS AVG_SPACE,
  CAST(NULL AS NUMBER) AS CHAIN_CNT,
  CAST(NULL AS NUMBER) AS AVG_ROW_LEN,
  CAST(NULL AS NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,
  CAST(NULL AS NUMBER) AS NUM_FREELIST_BLOCKS,
  CAST(NULL AS VARCHAR2(40)) AS DEGREE,
  CAST(NULL AS VARCHAR2(40)) AS INSTANCES,
  CAST(NULL AS VARCHAR2(20)) AS CACHE,
  CAST(NULL AS VARCHAR2(8)) AS TABLE_LOCK,
  CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
  CAST(NULL AS DATE) AS LAST_ANALYZED,
  CAST(
  CASE
    WHEN
      t.part_level = 0
    THEN
      'NO'
    ELSE
      'YES'
  END
  AS VARCHAR2(3)) AS PARTITIONED,
  CAST(NULL AS VARCHAR2(12)) AS IOT_TYPE,
  CAST(decode (t.table_type, 8, 'YES', 9, 'YES', 'NO') AS VARCHAR2(1)) AS TEMPORARY,
  CAST(NULL AS VARCHAR2(1)) AS SECONDARY,
  CAST('NO' AS VARCHAR2(3)) AS NESTED,
  CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
  CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
  CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
  CAST(NULL AS VARCHAR2(8)) AS ROW_MOVEMENT,
  CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
  CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
  CAST( decode (t.table_type, 8, 'SYS$SESSION', 9, 'SYS$TRANSACTION', NULL) AS VARCHAR2(15)) AS DURATION,
  CAST(NULL AS VARCHAR2(8)) AS SKIP_CORRUPT,
  CAST(NULL AS VARCHAR2(3)) AS MONITORING,
  CAST(NULL AS VARCHAR2(30)) AS CLUSTER_OWNER,
  CAST(NULL AS VARCHAR2(8)) AS DEPENDENCIES,
  CAST(NULL AS VARCHAR2(8)) AS COMPRESSION,
  CAST(NULL AS VARCHAR2(12)) AS COMPRESS_FOR,
  CAST(
  CASE
    WHEN
      db.database_name =  '__recyclebin'
    THEN 'YES'
    ELSE
      'NO'
  END
  AS VARCHAR2(3)) AS DROPPED,
  CAST(NULL AS VARCHAR2(3)) AS READ_ONLY,
  CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,
  CAST(NULL AS VARCHAR2(7)) AS RESULT_CACHE
FROM
  (
    SELECT
      tenant_id,
      table_id,
      SUM(row_count) AS row_count
    FROM
      sys.ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT p
    WHERE
      p.role = 1
      AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    GROUP BY
      tenant_id,
      table_id
  )
  info
  RIGHT JOIN
    sys.ALL_VIRTUAL_TABLE_REAL_AGENT t
    ON t.tenant_id = info.tenant_id
    AND t.table_id = info.table_id
  JOIN
    sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db
  ON
    db.tenant_id = t.tenant_id
    AND db.database_id = t.database_id
    AND db.database_name != '__recyclebin'
    AND t.table_type in (3, 8, 9, 11)
    AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
  LEFT JOIN
    sys.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT tp
  ON
    tp.tablespace_id = t.tablespace_id
    AND t.tenant_id = tp.tenant_id
    AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
  WHERE t.session_id = 0
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_TABLES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25029',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(t.table_name AS VARCHAR2(128)) AS TABLE_NAME,
  CAST(tp.tablespace_name AS VARCHAR2(30)) AS TABLESPACE_NAME,
  CAST(NULL AS VARCHAR2(128)) AS CLUSTER_NAME,
  CAST(NULL AS VARCHAR2(128)) AS IOT_NAME,
  CAST('VALID' AS VARCHAR2(8)) AS STATUS,
  CAST(t."PCTFREE" AS NUMBER) AS PCT_FREE,
  CAST(NULL AS NUMBER) AS PCT_USED,
  CAST(NULL AS NUMBER) AS INI_TRANS,
  CAST(NULL AS NUMBER) AS MAX_TRANS,
  CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
  CAST(NULL AS NUMBER) AS NEXT_EXTENT,
  CAST(NULL AS NUMBER) AS MIN_EXTENTS,
  CAST(NULL AS NUMBER) AS MAX_EXTENTS,
  CAST(NULL AS NUMBER) AS PCT_INCREASE,
  CAST(NULL AS NUMBER) AS FREELISTS,
  CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
  CAST(NULL AS VARCHAR2(3)) AS LOGGING,
  CAST(NULL AS VARCHAR2(1)) AS BACKED_UP,
  CAST(info.row_count AS NUMBER) AS NUM_ROWS,
  CAST(NULL AS NUMBER) AS BLOCKS,
  CAST(NULL AS NUMBER) AS EMPTY_BLOCKS,
  CAST(NULL AS NUMBER) AS AVG_SPACE,
  CAST(NULL AS NUMBER) AS CHAIN_CNT,
  CAST(NULL AS NUMBER) AS AVG_ROW_LEN,
  CAST(NULL AS NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,
  CAST(NULL AS NUMBER) AS NUM_FREELIST_BLOCKS,
  CAST(NULL AS VARCHAR2(40)) AS DEGREE,
  CAST(NULL AS VARCHAR2(40)) AS INSTANCES,
  CAST(NULL AS VARCHAR2(20)) AS CACHE,
  CAST(NULL AS VARCHAR2(8)) AS TABLE_LOCK,
  CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
  CAST(NULL AS DATE) AS LAST_ANALYZED,
  CAST(
  CASE
    WHEN
      t.part_level = 0
    THEN
      'NO'
    ELSE
      'YES'
  END
  AS VARCHAR2(3)) AS PARTITIONED,
  CAST(NULL AS VARCHAR2(12)) AS IOT_TYPE,
  CAST(decode (t.table_type, 8, 'YES', 9, 'YES', 'NO') AS VARCHAR2(1)) AS TEMPORARY,
  CAST(NULL AS VARCHAR2(1)) AS SECONDARY,
  CAST('NO' AS VARCHAR2(3)) AS NESTED,
  CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
  CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
  CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
  CAST(NULL AS VARCHAR2(8)) AS ROW_MOVEMENT,
  CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
  CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
  CAST( decode (t.table_type, 8, 'SYS$SESSION', 9, 'SYS$TRANSACTION', NULL) AS VARCHAR2(15)) AS DURATION,
  CAST(NULL AS VARCHAR2(8)) AS SKIP_CORRUPT,
  CAST(NULL AS VARCHAR2(3)) AS MONITORING,
  CAST(NULL AS VARCHAR2(30)) AS CLUSTER_OWNER,
  CAST(NULL AS VARCHAR2(8)) AS DEPENDENCIES,
  CAST(NULL AS VARCHAR2(8)) AS COMPRESSION,
  CAST(NULL AS VARCHAR2(12)) AS COMPRESS_FOR,
  CAST(
  CASE
    WHEN
      db.database_name =  '__recyclebin'
    THEN 'YES'
    ELSE
      'NO'
  END
  AS VARCHAR2(3)) AS DROPPED,
  CAST(NULL AS VARCHAR2(3)) AS READ_ONLY,
  CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,
  CAST(NULL AS VARCHAR2(7)) AS RESULT_CACHE
FROM
  (
    SELECT
      tenant_id,
      table_id,
      SUM(row_count) AS row_count
    FROM
      sys.ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT p
    WHERE
      p.role = 1
      AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    GROUP BY
      tenant_id,
      table_id
  )
  info
  RIGHT JOIN
    sys.ALL_VIRTUAL_TABLE_REAL_AGENT t
    ON t.tenant_id = info.tenant_id
    AND t.table_id = info.table_id
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
  JOIN
    sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db
  ON
    db.tenant_id = t.tenant_id
    AND db.database_id = t.database_id
    AND t.database_id = USERENV('SCHEMAID')
    AND t.table_type in (3, 8, 9, 11)
    AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
  LEFT JOIN
    sys.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT tp
  ON
    tp.tablespace_id = t.tablespace_id
    AND t.tenant_id = tp.tenant_id
    AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
  WHERE t.session_id = 0
""".replace("\n", " ")
)

# start oracle view dba/all/user/tab_comments

def_table_schema(
  table_name      = 'DBA_TAB_COMMENTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25030',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      A.DATABASE_NAME AS OWNER,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(CASE WHEN TABLE_TYPE = 0 OR TABLE_TYPE = 2 OR TABLE_TYPE = 3 OR TABLE_TYPE = 5 OR TABLE_TYPE = 8 OR TABLE_TYPE = 9 THEN   'TABLE' WHEN TABLE_TYPE = 1 OR TABLE_TYPE = 4 OR TABLE_TYPE = 7 THEN 'VIEW' ELSE NULL END AS VARCHAR2(11)) AS TABLE_TYPE,
      CAST(B."COMMENT" AS VARCHAR(4000)) AS COMMENTS
    FROM
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A
      JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B
    ON A.DATABASE_ID = B.DATABASE_ID
    AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_TAB_COMMENTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25031',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      A.DATABASE_NAME AS OWNER,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(CASE WHEN TABLE_TYPE = 0 OR TABLE_TYPE = 2 OR TABLE_TYPE = 3 OR TABLE_TYPE = 5 OR TABLE_TYPE = 8 OR TABLE_TYPE = 9 THEN   'TABLE' WHEN TABLE_TYPE = 1 OR TABLE_TYPE = 4 OR TABLE_TYPE = 7 THEN 'VIEW' ELSE NULL END AS VARCHAR2(11)) AS TABLE_TYPE,
      CAST(B."COMMENT" AS VARCHAR(4000)) AS COMMENTS
    FROM
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A
      JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B
    ON A.DATABASE_ID = B.DATABASE_ID
    AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND (A.DATABASE_ID = USERENV('SCHEMAID')
        OR USER_CAN_ACCESS_OBJ(1, TABLE_ID, A.DATABASE_ID) = 1)
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_TAB_COMMENTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25032',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(CASE WHEN TABLE_TYPE = 0 OR TABLE_TYPE = 2 OR TABLE_TYPE = 3 OR TABLE_TYPE = 5 OR TABLE_TYPE = 8 OR TABLE_TYPE = 9 THEN   'TABLE' WHEN TABLE_TYPE = 1 OR TABLE_TYPE = 4 OR TABLE_TYPE = 7 THEN 'VIEW' ELSE NULL END AS VARCHAR2(11)) AS TABLE_TYPE,
      CAST(B."COMMENT" AS VARCHAR(4000)) AS COMMENTS
    FROM
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A
      JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B
    ON A.DATABASE_ID = B.DATABASE_ID
    AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND A.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')
""".replace("\n", " ")
)
# end oracle view dba/all/user/tab_comments

# start oracle view dba/all/user/col_comments
def_table_schema(
  table_name      = 'DBA_COL_COMMENTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25033',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      A.DATABASE_NAME AS OWNER,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      C.COLUMN_NAME AS COLUMN_NAME,
      CAST(C."COMMENT" AS VARCHAR(4000)) AS COMMENTS
    FROM
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,
      SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,
      SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
    WHERE
      A.DATABASE_ID = B.DATABASE_ID
      AND B.TABLE_ID = C.TABLE_ID
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_COL_COMMENTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25034',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      A.DATABASE_NAME AS OWNER,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      C.COLUMN_NAME AS COLUMN_NAME,
      CAST(C."COMMENT" AS VARCHAR(4000)) AS COMMENTS
    FROM
    SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,
      SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,
      SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
    WHERE
      A.DATABASE_ID = B.DATABASE_ID
      AND B.TABLE_ID = C.TABLE_ID
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND (A.DATABASE_ID = USERENV('SCHEMAID')
           OR USER_CAN_ACCESS_OBJ(1, B.TABLE_ID, B.DATABASE_ID) = 1)
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_COL_COMMENTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25035',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      C.COLUMN_NAME AS COLUMN_NAME,
      CAST(C."COMMENT" AS VARCHAR(4000)) AS COMMENTS
    FROM
      SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT A,
      SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B,
      SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
    WHERE
      A.DATABASE_ID = B.DATABASE_ID
      AND B.TABLE_ID = C.TABLE_ID
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND A.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_INDEXES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25036',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(INDEX_OWNER AS VARCHAR2(128)) AS OWNER,
      CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
      CAST(INDEX_TYPE_NAME AS VARCHAR2(27)) AS INDEX_TYPE,
      CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,
      CAST(TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST('TABLE' AS CHAR(5)) AS TABLE_TYPE,
      CAST(UNIQUENESS AS VARCHAR2(9)) AS UNIQUENESS,
      CAST(COMPRESSION AS VARCHAR2(8)) AS COMPRESSION,
      CAST(NULL AS NUMBER) AS PREFIX_LENGTH,
      CAST(TABLESPACE_NAME AS VARCHAR2(30)) AS TABLESPACE_NAME,
      CAST(NULL AS NUMBER) AS INI_TRANS,
      CAST(NULL AS NUMBER) AS MAX_TRANS,
      CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
      CAST(NULL AS NUMBER) AS NEXT_EXTENT,
      CAST(NULL AS NUMBER) AS MIN_EXTENTS,
      CAST(NULL AS NUMBER) AS MAX_EXTENTS,
      CAST(NULL AS NUMBER) AS PCT_INCREASE,
      CAST(NULL AS NUMBER) AS PCT_THRESHOLD,
      CAST(NULL AS NUMBER) AS INCLUDE_COLUMN,
      CAST(NULL AS NUMBER) AS FREELISTS,
      CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
      CAST(NULL AS NUMBER) AS PCT_FREE,
      CAST(NULL AS VARCHAR2(3)) AS LOGGING,
      CAST(NULL AS NUMBER) AS BLEVEL,
      CAST(NULL AS NUMBER) AS LEAF_BLOCKS,
      CAST(NULL AS NUMBER) AS DISTINCT_KEYS,
      CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,
      CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,
      CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,
      CAST(STATUS AS VARCHAR2(10)) AS STATUS,
      CAST(NULL AS NUMBER) AS NUM_ROWS,
      CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
      CAST(NULL AS DATE) AS LAST_ANALYZED,
      CAST(NULL AS VARCHAR2(40)) AS DEGREE,
      CAST(NULL AS VARCHAR2(40)) AS INSTANCES,
      CAST(CASE WHEN A_TABLE_TYPE = 3 THEN 'NO'
                WHEN A_INDEX_TYPE = 1 OR A_INDEX_TYPE = 2
                THEN (CASE WHEN D.PART_LEVEL = 0 THEN 'NO' ELSE 'YES' END)
           ELSE (CASE WHEN A_PART_LEVEL = 0 THEN 'NO' ELSE 'YES' END) END  AS VARCHAR2(3)) AS PARTITIONED,
      CAST(NULL AS VARCHAR2(1)) AS TEMPORARY,
      CAST(NULL AS VARCHAR2(1)) AS "GENERATED",
      CAST(NULL AS VARCHAR2(1)) AS SECONDARY,
      CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
      CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
      CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
      CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
      CAST(NULL AS VARCHAR2(15)) AS DURATION,
      CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,
      CAST(NULL AS VARCHAR2(30)) AS ITYP_OWNER,
      CAST(NULL AS VARCHAR2(30)) AS ITYP_NAME,
      CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,
      CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
      CAST(NULL AS VARCHAR2(12)) AS DOMIDX_STATUS,
      CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,
      CAST(FUNCIDX_STATUS AS VARCHAR2(8)) AS FUNCIDX_STATUS,
      CAST('NO' AS VARCHAR2(3)) AS JOIN_INDEX,
      CAST(NULL AS VARCHAR2(3)) AS IOT_REDUNDANT_PKEY_ELIM,
      CAST(DROPPED AS VARCHAR2(9)) AS DROPPED,
      CAST(VISIBILITY AS VARCHAR2(10)) AS VISIBILITY,
      CAST(NULL AS VARCHAR2(14)) AS DOMIDX_MANAGEMENT,
      CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED
      FROM
        (SELECT
        DATABASE_NAME AS INDEX_OWNER,
        CASE WHEN (TABLE_TYPE = 5 AND B.DATABASE_NAME !=  '__recyclebin') THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))
             WHEN (TABLE_TYPE = 5 AND B.DATABASE_NAME =  '__recyclebin') THEN TABLE_NAME
          ELSE (CONS_TAB.CONSTRAINT_NAME) END AS INDEX_NAME,

        CASE
          WHEN A.TABLE_TYPE = 5 AND EXISTS (
            SELECT 1
            FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT T_COL_INDEX,
                 SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT T_COL_BASE
            WHERE T_COL_BASE.TABLE_ID = A.DATA_TABLE_ID
              AND T_COL_BASE.COLUMN_NAME = T_COL_INDEX.COLUMN_NAME
              AND T_COL_INDEX.TABLE_ID = A.TABLE_ID
              AND T_COL_BASE.TENANT_ID = A.TENANT_ID
              AND T_COL_INDEX.TENANT_ID = A.TENANT_ID
              AND BITAND(T_COL_BASE.COLUMN_FLAGS,3) > 0
          ) THEN 'FUNCTION-BASED NORMAL'
          ELSE 'NORMAL'
        END AS INDEX_TYPE_NAME,

        DATABASE_NAME AS TABLE_OWNER,
        CASE WHEN (TABLE_TYPE = 3) THEN A.TABLE_ID
          ELSE A.DATA_TABLE_ID END AS TABLE_ID,
        A.TABLE_ID AS INDEX_ID,
        CASE WHEN TABLE_TYPE = 3 THEN 'UNIQUE'
             WHEN A.INDEX_TYPE IN (2, 4, 8) THEN 'UNIQUE'
          ELSE 'NONUNIQUE' END AS UNIQUENESS,
        CASE WHEN A.COMPRESS_FUNC_NAME = NULL THEN 'DISABLED'
          ELSE 'ENABLED' END AS COMPRESSION,
        CASE WHEN TABLE_TYPE = 3 THEN 'VALID'
             WHEN A.INDEX_STATUS = 2 THEN 'VALID'
             WHEN A.INDEX_STATUS = 3 THEN 'CHECKING'
             WHEN A.INDEX_STATUS = 4 THEN 'INELEGIBLE'
             WHEN A.INDEX_STATUS = 5 THEN 'ERROR'
          ELSE 'UNUSABLE' END AS STATUS,
        A.INDEX_TYPE AS A_INDEX_TYPE,
        A.PART_LEVEL AS A_PART_LEVEL,
        A.TABLE_TYPE AS A_TABLE_TYPE,
        CASE WHEN 0 = (SELECT COUNT(1) FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT
                      WHERE TABLE_ID = A.TABLE_ID AND IS_HIDDEN = 0
                        AND TENANT_ID = EFFECTIVE_TENANT_ID()) THEN 'ENABLED'
          ELSE 'NULL' END AS FUNCIDX_STATUS,
        CASE WHEN B.DATABASE_NAME = '__recyclebin' THEN 'YES'
          ELSE 'NO' END AS DROPPED,
        CASE WHEN BITAND(A.INDEX_ATTRIBUTES_SET, 1) = 0 THEN 'VISIBLE'
          ELSE 'INVISIBLE' END AS VISIBILITY,
        A.TABLESPACE_ID
        FROM
          SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A
          JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID
          AND TABLE_TYPE IN (5, 3)
          AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
          LEFT JOIN SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT CONS_TAB ON (CONS_TAB.TABLE_ID = A.TABLE_ID)
              AND CONS_TAB.TENANT_ID = EFFECTIVE_TENANT_ID()
        WHERE
          NOT(TABLE_TYPE = 3 AND CONSTRAINT_NAME IS NULL) AND (CONS_TAB.CONSTRAINT_TYPE IS NULL OR CONS_TAB.CONSTRAINT_TYPE = 1)
        ) C
      JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D ON C.TABLE_ID = D.TABLE_ID
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP ON C.TABLESPACE_ID = TP.TABLESPACE_ID
          AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_INDEXES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25037',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(INDEX_OWNER AS VARCHAR2(128)) AS OWNER,
      CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
      CAST(INDEX_TYPE_NAME AS VARCHAR2(27)) AS INDEX_TYPE,
      CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,
      CAST(TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST('TABLE' AS CHAR(5)) AS TABLE_TYPE,
      CAST(UNIQUENESS AS VARCHAR2(9)) AS UNIQUENESS,
      CAST(COMPRESSION AS VARCHAR2(8)) AS COMPRESSION,
      CAST(NULL AS NUMBER) AS PREFIX_LENGTH,
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) AS TABLESPACE_NAME,
      CAST(NULL AS NUMBER) AS INI_TRANS,
      CAST(NULL AS NUMBER) AS MAX_TRANS,
      CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
      CAST(NULL AS NUMBER) AS NEXT_EXTENT,
      CAST(NULL AS NUMBER) AS MIN_EXTENTS,
      CAST(NULL AS NUMBER) AS MAX_EXTENTS,
      CAST(NULL AS NUMBER) AS PCT_INCREASE,
      CAST(NULL AS NUMBER) AS PCT_THRESHOLD,
      CAST(NULL AS NUMBER) AS INCLUDE_COLUMN,
      CAST(NULL AS NUMBER) AS FREELISTS,
      CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
      CAST(NULL AS NUMBER) AS PCT_FREE,
      CAST(NULL AS VARCHAR2(3)) AS LOGGING,
      CAST(NULL AS NUMBER) AS BLEVEL,
      CAST(NULL AS NUMBER) AS LEAF_BLOCKS,
      CAST(NULL AS NUMBER) AS DISTINCT_KEYS,
      CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,
      CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,
      CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,
      CAST(STATUS AS VARCHAR2(10)) AS STATUS,
      CAST(NULL AS NUMBER) AS NUM_ROWS,
      CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
      CAST(NULL AS DATE) AS LAST_ANALYZED,
      CAST(NULL AS VARCHAR2(40)) AS DEGREE,
      CAST(NULL AS VARCHAR2(40)) AS INSTANCES,
      CAST(CASE WHEN A_TABLE_TYPE = 3 THEN 'NO'
                WHEN A_INDEX_TYPE = 1 OR A_INDEX_TYPE = 2
                THEN (CASE WHEN D.PART_LEVEL = 0 THEN 'NO' ELSE 'YES' END)
           ELSE (CASE WHEN A_PART_LEVEL = 0 THEN 'NO' ELSE 'YES' END) END  AS VARCHAR2(3)) AS PARTITIONED,
      CAST(NULL AS VARCHAR2(1)) AS TEMPORARY,
      CAST(NULL AS VARCHAR2(1)) AS "GENERATED",
      CAST(NULL AS VARCHAR2(1)) AS SECONDARY,
      CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
      CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
      CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
      CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
      CAST(NULL AS VARCHAR2(15)) AS DURATION,
      CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,
      CAST(NULL AS VARCHAR2(30)) AS ITYP_OWNER,
      CAST(NULL AS VARCHAR2(30)) AS ITYP_NAME,
      CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,
      CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
      CAST(NULL AS VARCHAR2(12)) AS DOMIDX_STATUS,
      CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,
      CAST(FUNCIDX_STATUS AS VARCHAR2(8)) AS FUNCIDX_STATUS,
      CAST('NO' AS VARCHAR2(3)) AS JOIN_INDEX,
      CAST(NULL AS VARCHAR2(3)) AS IOT_REDUNDANT_PKEY_ELIM,
      CAST(DROPPED AS VARCHAR2(9)) AS DROPPED,
      CAST(VISIBILITY AS VARCHAR2(10)) AS VISIBILITY,
      CAST(NULL AS VARCHAR2(14)) AS DOMIDX_MANAGEMENT,
      CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED
      FROM
        (SELECT
        DATABASE_NAME AS INDEX_OWNER,
        CASE WHEN (TABLE_TYPE = 5 AND B.DATABASE_NAME !=  '__recyclebin') THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))
             WHEN (TABLE_TYPE = 5 AND B.DATABASE_NAME =  '__recyclebin') THEN TABLE_NAME
          ELSE (CONS_TAB.CONSTRAINT_NAME) END AS INDEX_NAME,

        CASE
          WHEN A.TABLE_TYPE = 5 AND EXISTS (
            SELECT 1
            FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT T_COL_INDEX,
                 SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT T_COL_BASE
            WHERE T_COL_BASE.TABLE_ID = A.DATA_TABLE_ID
              AND T_COL_BASE.COLUMN_NAME = T_COL_INDEX.COLUMN_NAME
              AND T_COL_INDEX.TABLE_ID = A.TABLE_ID
              AND T_COL_BASE.TENANT_ID = A.TENANT_ID
              AND T_COL_INDEX.TENANT_ID = A.TENANT_ID
              AND BITAND(T_COL_BASE.COLUMN_FLAGS,3) > 0
          ) THEN 'FUNCTION-BASED NORMAL'
          ELSE 'NORMAL'
        END AS INDEX_TYPE_NAME,

        DATABASE_NAME AS TABLE_OWNER,
        CASE WHEN (TABLE_TYPE = 3) THEN A.TABLE_ID
          ELSE A.DATA_TABLE_ID END AS TABLE_ID,
        A.TABLE_ID AS INDEX_ID,
        CASE WHEN TABLE_TYPE = 3 THEN 'UNIQUE'
             WHEN A.INDEX_TYPE IN (2, 4, 8) THEN 'UNIQUE'
          ELSE 'NONUNIQUE' END AS UNIQUENESS,
        CASE WHEN A.COMPRESS_FUNC_NAME = NULL THEN 'DISABLED'
          ELSE 'ENABLED' END AS COMPRESSION,
        CASE WHEN TABLE_TYPE = 3 THEN 'VALID'
             WHEN A.INDEX_STATUS = 2 THEN 'VALID'
             WHEN A.INDEX_STATUS = 3 THEN 'CHECKING'
             WHEN A.INDEX_STATUS = 4 THEN 'INELEGIBLE'
             WHEN A.INDEX_STATUS = 5 THEN 'ERROR'
          ELSE 'UNUSABLE' END AS STATUS,
        A.INDEX_TYPE AS A_INDEX_TYPE,
        A.PART_LEVEL AS A_PART_LEVEL,
        A.TABLE_TYPE AS A_TABLE_TYPE,
        CASE WHEN 0 = (SELECT COUNT(1) FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT
                        WHERE TABLE_ID = A.TABLE_ID AND IS_HIDDEN = 0
                          AND TENANT_ID = EFFECTIVE_TENANT_ID()) THEN 'ENABLED'
          ELSE 'NULL' END AS FUNCIDX_STATUS,
        CASE WHEN B.DATABASE_NAME = '__recyclebin' THEN 'YES'
          ELSE 'NO' END AS DROPPED,
        CASE WHEN BITAND(A.INDEX_ATTRIBUTES_SET, 1) = 0 THEN 'VISIBLE'
          ELSE 'INVISIBLE' END AS VISIBILITY,
        A.TABLESPACE_ID
        FROM
          SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A
          JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID
              AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
              AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND (A.DATABASE_ID = USERENV('SCHEMAID')
               OR USER_CAN_ACCESS_OBJ(1, DECODE(TABLE_TYPE, 3, A.TABLE_ID, 5, DATA_TABLE_ID), A.DATABASE_ID) = 1)
          AND TABLE_TYPE IN (5, 3)
          LEFT JOIN SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT CONS_TAB
            ON (CONS_TAB.TABLE_ID = A.TABLE_ID AND CONS_TAB.TENANT_ID = EFFECTIVE_TENANT_ID())
        WHERE
          NOT(TABLE_TYPE = 3 AND CONSTRAINT_NAME IS NULL) AND (CONS_TAB.CONSTRAINT_TYPE IS NULL OR CONS_TAB.CONSTRAINT_TYPE = 1)
        ) C
      JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D ON C.TABLE_ID = D.TABLE_ID
                AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP ON C.TABLESPACE_ID = TP.TABLESPACE_ID
                AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_INDEXES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25038',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
      CAST(INDEX_TYPE_NAME AS VARCHAR2(27)) AS INDEX_TYPE,
      CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,
      CAST(TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST('TABLE' AS CHAR(5)) AS TABLE_TYPE,
      CAST(UNIQUENESS AS VARCHAR2(9)) AS UNIQUENESS,
      CAST(COMPRESSION AS VARCHAR2(8)) AS COMPRESSION,
      CAST(NULL AS NUMBER) AS PREFIX_LENGTH,
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) AS TABLESPACE_NAME,
      CAST(NULL AS NUMBER) AS INI_TRANS,
      CAST(NULL AS NUMBER) AS MAX_TRANS,
      CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
      CAST(NULL AS NUMBER) AS NEXT_EXTENT,
      CAST(NULL AS NUMBER) AS MIN_EXTENTS,
      CAST(NULL AS NUMBER) AS MAX_EXTENTS,
      CAST(NULL AS NUMBER) AS PCT_INCREASE,
      CAST(NULL AS NUMBER) AS PCT_THRESHOLD,
      CAST(NULL AS NUMBER) AS INCLUDE_COLUMN,
      CAST(NULL AS NUMBER) AS FREELISTS,
      CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
      CAST(NULL AS NUMBER) AS PCT_FREE,
      CAST(NULL AS VARCHAR2(3)) AS LOGGING,
      CAST(NULL AS NUMBER) AS BLEVEL,
      CAST(NULL AS NUMBER) AS LEAF_BLOCKS,
      CAST(NULL AS NUMBER) AS DISTINCT_KEYS,
      CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,
      CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,
      CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,
      CAST(STATUS AS VARCHAR2(10)) AS STATUS,
      CAST(NULL AS NUMBER) AS NUM_ROWS,
      CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
      CAST(NULL AS DATE) AS LAST_ANALYZED,
      CAST(NULL AS VARCHAR2(40)) AS DEGREE,
      CAST(NULL AS VARCHAR2(40)) AS INSTANCES,
      CAST(CASE WHEN A_TABLE_TYPE = 3 THEN 'NO'
                WHEN A_INDEX_TYPE = 1 OR A_INDEX_TYPE = 2
                THEN (CASE WHEN D.PART_LEVEL = 0 THEN 'NO' ELSE 'YES' END)
           ELSE (CASE WHEN A_PART_LEVEL = 0 THEN 'NO' ELSE 'YES' END) END  AS VARCHAR2(3)) AS PARTITIONED,
      CAST(NULL AS VARCHAR2(1)) AS TEMPORARY,
      CAST(NULL AS VARCHAR2(1)) AS "GENERATED",
      CAST(NULL AS VARCHAR2(1)) AS SECONDARY,
      CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
      CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
      CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
      CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
      CAST(NULL AS VARCHAR2(15)) AS DURATION,
      CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,
      CAST(NULL AS VARCHAR2(30)) AS ITYP_OWNER,
      CAST(NULL AS VARCHAR2(30)) AS ITYP_NAME,
      CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,
      CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
      CAST(NULL AS VARCHAR2(12)) AS DOMIDX_STATUS,
      CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,
      CAST(FUNCIDX_STATUS AS VARCHAR2(8)) AS FUNCIDX_STATUS,
      CAST('NO' AS VARCHAR2(3)) AS JOIN_INDEX,
      CAST(NULL AS VARCHAR2(3)) AS IOT_REDUNDANT_PKEY_ELIM,
      CAST(DROPPED AS VARCHAR2(9)) AS DROPPED,
      CAST(VISIBILITY AS VARCHAR2(10)) AS VISIBILITY,
      CAST(NULL AS VARCHAR2(14)) AS DOMIDX_MANAGEMENT,
      CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED
      FROM
        (SELECT
        DATABASE_NAME AS INDEX_OWNER,
        CASE WHEN (TABLE_TYPE = 5 AND B.DATABASE_NAME !=  '__recyclebin') THEN SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_'))
             WHEN (TABLE_TYPE = 5 AND B.DATABASE_NAME =  '__recyclebin') THEN TABLE_NAME
          ELSE (CONS_TAB.CONSTRAINT_NAME) END AS INDEX_NAME,

        CASE
          WHEN A.TABLE_TYPE = 5 AND EXISTS (
            SELECT 1
            FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT T_COL_INDEX,
                 SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT T_COL_BASE
            WHERE T_COL_BASE.TABLE_ID = A.DATA_TABLE_ID
              AND T_COL_BASE.COLUMN_NAME = T_COL_INDEX.COLUMN_NAME
              AND T_COL_INDEX.TABLE_ID = A.TABLE_ID
              AND T_COL_BASE.TENANT_ID = A.TENANT_ID
              AND T_COL_INDEX.TENANT_ID = A.TENANT_ID
              AND BITAND(T_COL_BASE.COLUMN_FLAGS,3) > 0
          ) THEN 'FUNCTION-BASED NORMAL'
          ELSE 'NORMAL'
        END AS INDEX_TYPE_NAME,

        DATABASE_NAME AS TABLE_OWNER,
        CASE WHEN (TABLE_TYPE = 3) THEN A.TABLE_ID
          ELSE A.DATA_TABLE_ID END AS TABLE_ID,
        A.TABLE_ID AS INDEX_ID,
        CASE WHEN TABLE_TYPE = 3 THEN 'UNIQUE'
             WHEN A.INDEX_TYPE IN (2, 4, 8) THEN 'UNIQUE'
          ELSE 'NONUNIQUE' END AS UNIQUENESS,
        CASE WHEN A.COMPRESS_FUNC_NAME = NULL THEN 'DISABLED'
          ELSE 'ENABLED' END AS COMPRESSION,
        CASE WHEN TABLE_TYPE = 3 THEN 'VALID'
             WHEN A.INDEX_STATUS = 2 THEN 'VALID'
             WHEN A.INDEX_STATUS = 3 THEN 'CHECKING'
             WHEN A.INDEX_STATUS = 4 THEN 'INELEGIBLE'
             WHEN A.INDEX_STATUS = 5 THEN 'ERROR'
          ELSE 'UNUSABLE' END AS STATUS,
        A.INDEX_TYPE AS A_INDEX_TYPE,
        A.PART_LEVEL AS A_PART_LEVEL,
        A.TABLE_TYPE AS A_TABLE_TYPE,
        CASE WHEN 0 = (SELECT COUNT(1) FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT
                      WHERE TABLE_ID = A.TABLE_ID AND IS_HIDDEN = 0
                        AND TENANT_ID = EFFECTIVE_TENANT_ID()) THEN 'ENABLED'
          ELSE 'NULL' END AS FUNCIDX_STATUS,
        CASE WHEN B.DATABASE_NAME = '__recyclebin' THEN 'YES'
          ELSE 'NO' END AS DROPPED,
        CASE WHEN BITAND(A.INDEX_ATTRIBUTES_SET, 1) = 0 THEN 'VISIBLE'
          ELSE 'INVISIBLE' END AS VISIBILITY,
        A.TABLESPACE_ID
        FROM
          SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A
          JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = USERENV('SCHEMAID') AND A.DATABASE_ID = B.DATABASE_ID
          AND TABLE_TYPE IN (5, 3)
          AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
          LEFT JOIN SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT CONS_TAB ON (CONS_TAB.TABLE_ID = A.TABLE_ID)
              AND CONS_TAB.TENANT_ID = EFFECTIVE_TENANT_ID()
        WHERE
          NOT(TABLE_TYPE = 3 AND CONSTRAINT_NAME IS NULL) AND (CONS_TAB.CONSTRAINT_TYPE IS NULL OR CONS_TAB.CONSTRAINT_TYPE = 1)
        ) C
      JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT D ON C.TABLE_ID = D.TABLE_ID
              AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP ON C.TABLESPACE_ID = TP.TABLESPACE_ID
              AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)
def_table_schema(
  table_name      = 'DBA_CONS_COLUMNS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25039',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(SUBSTR(A.TABLE_NAME, 7 + INSTR(SUBSTR(A.TABLE_NAME, 7), '_')) AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(D.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(D.INDEX_POSITION AS NUMBER) AS POSITION
      FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D
      WHERE A.DATA_TABLE_ID = B.TABLE_ID AND A.DATABASE_ID = C.DATABASE_ID AND D.TABLE_ID = A.TABLE_ID AND A.INDEX_TYPE IN (2, 4, 8) AND C.DATABASE_NAME != '__recyclebin' AND D.IS_HIDDEN = 0 AND D.INDEX_POSITION != 0
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(E.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(D.POSITION AS NUMBER) AS POSITION
      FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT D, SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT E
      WHERE A.CHILD_TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND A.FOREIGN_KEY_ID = D.FOREIGN_KEY_ID AND D.CHILD_COLUMN_ID = E.COLUMN_ID AND B.TABLE_ID = E.TABLE_ID
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(D.CONSTRAINT_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CAST(A.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(B.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(B.ROWKEY_POSITION AS NUMBER) AS POSITION
      FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A, SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT D
      WHERE A.TABLE_ID = D.TABLE_ID AND D.CONSTRAINT_TYPE = 1 AND A.TABLE_ID = B.TABLE_ID AND A.DATABASE_ID = C.DATABASE_ID AND B.ROWKEY_POSITION > 0 AND C.DATABASE_NAME != '__recyclebin' AND B.IS_HIDDEN = 0
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_CONS_COLUMNS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25040',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(SUBSTR(A.TABLE_NAME, 7 + INSTR(SUBSTR(A.TABLE_NAME, 7), '_')) AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(D.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(D.INDEX_POSITION AS NUMBER) AS POSITION
      FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D
      WHERE A.DATA_TABLE_ID = B.TABLE_ID
        AND A.DATABASE_ID = C.DATABASE_ID
        AND (A.DATABASE_ID = USERENV('SCHEMAID')
            OR USER_CAN_ACCESS_OBJ(1, A.DATA_TABLE_ID, A.DATABASE_ID) = 1)
        AND D.TABLE_ID = A.TABLE_ID
        AND A.INDEX_TYPE IN (2, 4, 8)
        AND C.DATABASE_NAME != '__recyclebin'
        AND D.IS_HIDDEN = 0
        AND D.INDEX_POSITION != 0
    UNION ALL
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(E.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(D.POSITION AS NUMBER) AS POSITION
      FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT D, SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT E
      WHERE A.CHILD_TABLE_ID = B.TABLE_ID
        AND B.DATABASE_ID = C.DATABASE_ID
        AND (B.DATABASE_ID = USERENV('SCHEMAID')
            OR USER_CAN_ACCESS_OBJ(1, A.CHILD_TABLE_ID, 1) = 1)
        AND A.FOREIGN_KEY_ID = D.FOREIGN_KEY_ID
        AND D.CHILD_COLUMN_ID = E.COLUMN_ID
        AND B.TABLE_ID = E.TABLE_ID
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(D.CONSTRAINT_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CAST(A.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(B.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(B.ROWKEY_POSITION AS NUMBER) AS POSITION
      FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A, SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT D
      WHERE A.TABLE_ID = D.TABLE_ID
        AND D.CONSTRAINT_TYPE = 1
        AND A.TABLE_ID = B.TABLE_ID
        AND A.DATABASE_ID = C.DATABASE_ID
        AND (A.DATABASE_ID = USERENV('SCHEMAID')
            OR USER_CAN_ACCESS_OBJ(1, A.TABLE_ID, 1) = 1)
        AND B.ROWKEY_POSITION > 0
        AND C.DATABASE_NAME != '__recyclebin' AND B.IS_HIDDEN = 0
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_CONS_COLUMNS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25041',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(SUBSTR(A.TABLE_NAME, 7 + INSTR(SUBSTR(A.TABLE_NAME, 7), '_')) AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(D.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(D.INDEX_POSITION AS NUMBER) AS POSITION
      FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D
      WHERE A.DATA_TABLE_ID = B.TABLE_ID AND A.DATABASE_ID = C.DATABASE_ID AND D.TABLE_ID = A.TABLE_ID AND A.INDEX_TYPE IN (2, 4, 8) AND C.DATABASE_NAME != '__recyclebin' AND D.IS_HIDDEN = 0 AND C.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER') AND D.INDEX_POSITION != 0
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(A.FOREIGN_KEY_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CAST(B.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(E.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(D.POSITION AS NUMBER) AS POSITION
      FROM SYS.ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT D, SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT E
      WHERE A.CHILD_TABLE_ID = B.TABLE_ID AND B.DATABASE_ID = C.DATABASE_ID AND A.FOREIGN_KEY_ID = D.FOREIGN_KEY_ID AND D.CHILD_COLUMN_ID = E.COLUMN_ID AND B.TABLE_ID = E.TABLE_ID AND C.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
      CAST(C.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
      CAST(D.CONSTRAINT_NAME AS VARCHAR2(128)) AS CONSTRAINT_NAME,
      CAST(A.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(B.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
      CAST(B.ROWKEY_POSITION AS NUMBER) AS POSITION
      FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A, SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT B, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT C, SYS.ALL_VIRTUAL_CONSTRAINT_REAL_AGENT D
      WHERE A.TABLE_ID = D.TABLE_ID AND D.CONSTRAINT_TYPE = 1 AND A.TABLE_ID = B.TABLE_ID AND A.DATABASE_ID = C.DATABASE_ID AND B.ROWKEY_POSITION > 0 AND C.DATABASE_NAME != '__recyclebin' AND B.IS_HIDDEN = 0 AND C.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_SEGMENTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25042',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(A.SEGMENT_NAME AS VARCHAR2(128)) AS SEGMENT_NAME
      ,CAST(A.PARTITION_NAME AS VARCHAR2(128)) AS PARTITION_NAME
      ,CAST(A.SEGMENT_TYPE AS VARCHAR2(18)) AS SEGMENT_TYPE
      ,CAST(NULL AS VARCHAR2(10)) AS SEGMENT_SUBTYPE
      ,CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME
      ,A.DATA_SIZE AS BYTES
      ,A.BLOCK_SIZE AS BLOCKS
      ,CAST(NULL AS NUMBER) AS EXTENTS
      ,CAST(NULL AS NUMBER) AS INITIAL_EXTENT
      ,CAST(NULL AS NUMBER) AS NEXT_EXTENT
      ,CAST(NULL AS NUMBER) AS MIN_EXTENTS
      ,CAST(NULL AS NUMBER) AS MAX_EXTENTS
      ,CAST(NULL AS NUMBER) AS MAX_SIZE
      ,CAST(NULL AS VARCHAR(7)) AS RETENTION
      ,CAST(NULL AS NUMBER) AS MINRETENTION
      ,CAST(NULL AS NUMBER) AS PCT_INCREASE
      ,CAST(NULL AS NUMBER) AS FREELISTS
      ,CAST(NULL AS NUMBER) AS FREELIST_GROUPS
      ,CAST('DEFAULT' AS VARCHAR2(7)) AS BUFFER_POOL
      ,CAST('DEFAULT' AS VARCHAR2(7)) AS FLASH_CACHE
      ,CAST('DEFAULT' AS VARCHAR2(7)) AS CELL_FLASH_CACHE
      FROM (
      SELECT
      T.TABLE_ID
      ,T.DATABASE_ID
      ,T.TABLE_NAME SEGMENT_NAME
      ,NULL PARTITION_NAME
      ,CASE WHEN T.TABLE_TYPE IN (0,2,3,6,8,9,10) THEN 'TABLE'
          WHEN T.TABLE_TYPE IN (1,4) THEN 'VIEW'
          WHEN T.TABLE_TYPE IN (5) THEN 'INDEX'
          WHEN T.TABLE_TYPE IN (7) THEN 'MATERIALIZED VIEW'
          ELSE NULL END AS SEGMENT_TYPE
      ,T.BLOCK_SIZE
      ,MT.DATA_SIZE
      FROM
      SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT MT
      ON T.TABLE_ID = MT.TABLE_ID WHERE T.PART_LEVEL=0 AND MT.ROLE = 1
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND MT.TENANT_ID = EFFECTIVE_TENANT_ID()
      UNION ALL
      SELECT
          T.TABLE_ID
          ,T.DATABASE_ID
          ,T.TABLE_NAME SEGMENT_NAME
          ,P.PART_NAME PARTITION_NAME
          ,'TABLE PARTITION' AS SEGMENT_TYPE
          ,T.BLOCK_SIZE
          ,MT.DATA_SIZE
          FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON T.TABLE_ID = P.TABLE_ID
          JOIN SYS.ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT MT
          ON T.TABLE_ID = MT.TABLE_ID AND P.PART_ID = MT.PARTITION_ID WHERE T.PART_LEVEL=1 AND MT.ROLE = 1
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND MT.TENANT_ID = EFFECTIVE_TENANT_ID()
      UNION ALL
      SELECT
          T.TABLE_ID
          ,T.DATABASE_ID
          ,T.TABLE_NAME SEGMENT_NAME
          ,SUBP.SUB_PART_NAME PARTITION_NAME
          ,'TABLE SUBPARTITION' AS OBJECT_TYPE
          ,T.BLOCK_SIZE
          ,MT.DATA_SIZE
          FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON T.TABLE_ID =P.TABLE_ID JOIN SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUBP
          ON P.TABLE_ID=SUBP.TABLE_ID AND P.PART_ID =SUBP.PART_ID
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND SUBP.TENANT_ID = EFFECTIVE_TENANT_ID()
          JOIN (SELECT A.PARTITION_ID,A.TABLE_ID,A.DATA_SIZE,A.PART_ID,PARTITION_ID - 1152921504606846976 - PART_ID *4294967296 - 268435456 AS SUB_PART_ID FROM
          (SELECT A.PARTITION_ID,A.TABLE_ID,A.DATA_SIZE,FLOOR((PARTITION_ID - 1152921504606846976 - 268435456)/4294967296) AS PART_ID FROM SYS.ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT A
                WHERE A.PARTITION_ID != 0 AND A.ROLE = 1 AND A.TENANT_ID = EFFECTIVE_TENANT_ID())A) MT
          ON SUBP.TABLE_ID = MT.TABLE_ID AND SUBP.PART_ID = MT.PART_ID AND SUBP.SUB_PART_ID=MT.SUB_PART_ID WHERE T.PART_LEVEL=2
    )A WHERE DATABASE_ID=USERENV('SCHEMAID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_SEGMENTS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25043',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      B.DATABASE_NAME OWNER
      ,CAST(A.SEGMENT_NAME AS VARCHAR2(128)) AS SEGMENT_NAME
      ,CAST(A.PARTITION_NAME AS VARCHAR2(128)) AS PARTITION_NAME
      ,CAST(A.SEGMENT_TYPE AS VARCHAR2(18)) AS SEGMENT_TYPE
      ,CAST(NULL AS VARCHAR2(10)) AS SEGMENT_SUBTYPE
      ,CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME
      ,CAST(NULL AS NUMBER) AS HEADER_FILE
      ,CAST(NULL AS NUMBER) AS HEADER_BLOCK
      ,A.DATA_SIZE AS BYTES
      ,A.BLOCK_SIZE AS BLOCKS
      ,CAST(NULL AS NUMBER) AS EXTENTS
      ,CAST(NULL AS NUMBER) AS INITIAL_EXTENT
      ,CAST(NULL AS NUMBER) AS NEXT_EXTENT
      ,CAST(NULL AS NUMBER) AS MIN_EXTENTS
      ,CAST(NULL AS NUMBER) AS MAX_EXTENTS
      ,CAST(NULL AS NUMBER) AS MAX_SIZE
      ,CAST(NULL AS VARCHAR(7)) AS RETENTION
      ,CAST(NULL AS NUMBER) AS MINRETENTION
      ,CAST(NULL AS NUMBER) AS PCT_INCREASE
      ,CAST(NULL AS NUMBER) AS FREELISTS
      ,CAST(NULL AS NUMBER) AS FREELIST_GROUPS
      ,CAST(NULL AS NUMBER) AS RELATIVE_FNO
      ,CAST('DEFAULT' AS VARCHAR2(7)) AS BUFFER_POOL
      ,CAST('DEFAULT' AS VARCHAR2(7)) AS FLASH_CACHE
      ,CAST('DEFAULT' AS VARCHAR2(7)) AS CELL_FLASH_CACHE
      FROM (
      SELECT
      T.TABLE_ID
      ,T.DATABASE_ID
      ,T.TABLE_NAME SEGMENT_NAME
      ,NULL PARTITION_NAME
      ,CASE WHEN T.TABLE_TYPE IN (0,2,3,6,8,9,10) THEN 'TABLE'
          WHEN T.TABLE_TYPE IN (1,4) THEN 'VIEW'
          WHEN T.TABLE_TYPE IN (5) THEN 'INDEX'
          WHEN T.TABLE_TYPE IN (7) THEN 'MATERIALIZED VIEW'
          ELSE NULL END AS SEGMENT_TYPE
      ,T.BLOCK_SIZE
      ,MT.DATA_SIZE
      FROM
      SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT MT
      ON T.TABLE_ID = MT.TABLE_ID WHERE T.PART_LEVEL=0 AND MT.ROLE = 1
      AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND MT.TENANT_ID = EFFECTIVE_TENANT_ID()
      UNION ALL
      SELECT
          T.TABLE_ID
          ,T.DATABASE_ID
          ,T.TABLE_NAME SEGMENT_NAME
          ,P.PART_NAME PARTITION_NAME
          ,'TABLE PARTITION' AS SEGMENT_TYPE
          ,T.BLOCK_SIZE
          ,MT.DATA_SIZE
          FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON T.TABLE_ID = P.TABLE_ID
            AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
          JOIN SYS.ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT MT
          ON T.TABLE_ID = MT.TABLE_ID AND P.PART_ID = MT.PARTITION_ID WHERE T.PART_LEVEL=1 AND MT.ROLE = 1
          AND MT.TENANT_ID = EFFECTIVE_TENANT_ID()
      UNION ALL
      SELECT
          T.TABLE_ID
          ,T.DATABASE_ID
          ,T.TABLE_NAME SEGMENT_NAME
          ,SUBP.SUB_PART_NAME PARTITION_NAME
          ,'TABLE SUBPARTITION' AS OBJECT_TYPE
          ,T.BLOCK_SIZE
          ,MT.DATA_SIZE
          FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT P ON T.TABLE_ID =P.TABLE_ID JOIN SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUBP
          ON P.TABLE_ID=SUBP.TABLE_ID AND P.PART_ID =SUBP.PART_ID
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND SUBP.TENANT_ID = EFFECTIVE_TENANT_ID()
          JOIN (SELECT A.PARTITION_ID,A.TABLE_ID,A.DATA_SIZE,A.PART_ID,PARTITION_ID - 1152921504606846976 - PART_ID *4294967296 - 268435456 AS SUB_PART_ID FROM
          (SELECT A.PARTITION_ID,A.TABLE_ID,A.DATA_SIZE,FLOOR((PARTITION_ID - 1152921504606846976 - 268435456)/4294967296) AS PART_ID FROM SYS.ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT A
              WHERE A.PARTITION_ID != 0 AND A.ROLE = 1 AND A.TENANT_ID = EFFECTIVE_TENANT_ID())A) MT
          ON SUBP.TABLE_ID = MT.TABLE_ID AND SUBP.PART_ID = MT.PART_ID AND SUBP.SUB_PART_ID=MT.SUB_PART_ID WHERE T.PART_LEVEL=2
    ) A JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B
      ON A.DATABASE_ID = B.DATABASE_ID
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='DBA_TYPES',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25044',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
    SELECT
      D.DATABASE_NAME AS OWNER,
      T.TYPE_NAME AS TYPE_NAME,
      T.TYPE_ID AS TYPE_OID,
      CAST(
        CASE T.TYPECODE
        WHEN 1 THEN 'COLLECTION'
        WHEN 2 THEN 'OBJECT' END AS VARCHAR2(10)) AS TYPECODE,
      T.ATTRIBUTES AS ATTRIBUTES,
      T.METHODS AS METHODS,
      CAST('NO' AS CHAR(2)) AS PREDEFINED,
      CAST('NO' AS CHAR(2)) AS INCOMPLETE,
      CAST('YES' AS CHAR(3)) AS FINAL,
      CAST('YES' AS CHAR(3)) AS INSTANTIABLE,
      CAST(NULL AS VARCHAR2(30)) AS SUPERTYPE_OWNER,
      CAST(NULL AS VARCHAR2(30)) AS SUPERTYPE_NAME,
      T.LOCAL_ATTRS AS LOCAL_ATTRIBUTES,
      T.LOCAL_METHODS AS LOCAL_METHODS,
      T.TYPE_ID AS TYPEID
    FROM
      SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
        ON D.DATABASE_ID = T.DATABASE_ID
        AND T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      TS.TYPE_NAME AS TYPE_NAME,
      TS.TYPE_ID AS TYPE_OID,
      CAST(
        CASE TS.TYPECODE
        WHEN 1 THEN 'COLLECTION'
        WHEN 2 THEN 'OBJECT' END AS VARCHAR2(10)) AS TYPECODE,
      TS.ATTRIBUTES AS ATTRIBUTES,
      TS.METHODS AS METHODS,
      CAST('NO' AS CHAR(2)) AS PREDEFINED,
      CAST('NO' AS CHAR(2)) AS INCOMPLETE,
      CAST('YES' AS CHAR(3)) AS FINAL,
      CAST('YES' AS CHAR(3)) AS INSTANTIABLE,
      CAST(NULL AS VARCHAR2(30)) AS SUPERTYPE_OWNER,
      CAST(NULL AS VARCHAR2(30)) AS SUPERTYPE_NAME,
      TS.LOCAL_ATTRS AS LOCAL_ATTRIBUTES,
      TS.LOCAL_METHODS AS LOCAL_METHODS,
      TS.TYPE_ID AS TYPEID
    FROM
      SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS
""".replace("\n", " ")
)

def_table_schema(
  table_name='ALL_TYPES',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25045',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
    SELECT
      D.DATABASE_NAME AS OWNER,
      T.TYPE_NAME AS TYPE_NAME,
      T.TYPE_ID AS TYPE_OID,
      CAST(
        CASE T.TYPECODE
        WHEN 1 THEN 'COLLECTION'
        WHEN 2 THEN 'OBJECT' END AS VARCHAR2(10)) AS TYPECODE,
      T.ATTRIBUTES AS ATTRIBUTES,
      T.METHODS AS METHODS,
      CAST('NO' AS CHAR(2)) AS PREDEFINED,
      CAST('NO' AS CHAR(2)) AS INCOMPLETE,
      CAST('YES' AS CHAR(3)) AS FINAL,
      CAST('YES' AS CHAR(3)) AS INSTANTIABLE,
      CAST(NULL AS VARCHAR2(30)) AS SUPERTYPE_OWNER,
      CAST(NULL AS VARCHAR2(30)) AS SUPERTYPE_NAME,
      T.LOCAL_ATTRS AS LOCAL_ATTRIBUTES,
      T.LOCAL_METHODS AS LOCAL_METHODS,
      T.TYPE_ID AS TYPEID
    FROM
      SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
        ON D.DATABASE_ID = T.DATABASE_ID
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND (T.DATABASE_ID = USERENV('SCHEMAID')
             or USER_CAN_ACCESS_OBJ(4, T.TYPE_ID, T.DATABASE_ID) = 1)
        AND T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
    UNION ALL
    SELECT
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      TS.TYPE_NAME AS TYPE_NAME,
      TS.TYPE_ID AS TYPE_OID,
      CAST(
        CASE TS.TYPECODE
        WHEN 1 THEN 'COLLECTION'
        WHEN 2 THEN 'OBJECT' END AS VARCHAR2(10)) AS TYPECODE,
      TS.ATTRIBUTES AS ATTRIBUTES,
      TS.METHODS AS METHODS,
      CAST('NO' AS CHAR(2)) AS PREDEFINED,
      CAST('NO' AS CHAR(2)) AS INCOMPLETE,
      CAST('YES' AS CHAR(3)) AS FINAL,
      CAST('YES' AS CHAR(3)) AS INSTANTIABLE,
      CAST(NULL AS VARCHAR2(30)) AS SUPERTYPE_OWNER,
      CAST(NULL AS VARCHAR2(30)) AS SUPERTYPE_NAME,
      TS.LOCAL_ATTRS AS LOCAL_ATTRIBUTES,
      TS.LOCAL_METHODS AS LOCAL_METHODS,
      TS.TYPE_ID AS TYPEID
    FROM
      SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_TYPES',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25046',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
    SELECT
      T.TYPE_NAME AS TYPE_NAME,
      T.TYPE_ID AS TYPE_OID,
      CAST(
        CASE T.TYPECODE
        WHEN 1 THEN 'COLLECTION'
        WHEN 2 THEN 'OBJECT' END AS VARCHAR2(10)) AS TYPECODE,
      T.ATTRIBUTES AS ATTRIBUTES,
      T.METHODS AS METHODS,
      CAST('NO' AS CHAR(2)) AS PREDEFINED,
      CAST('NO' AS CHAR(2)) AS INCOMPLETE,
      CAST('YES' AS CHAR(3)) AS FINAL,
      CAST('YES' AS CHAR(3)) AS INSTANTIABLE,
      CAST(NULL AS VARCHAR2(30)) AS SUPERTYPE_OWNER,
      CAST(NULL AS VARCHAR2(30)) AS SUPERTYPE_NAME,
      T.LOCAL_ATTRS AS LOCAL_ATTRIBUTES,
      T.LOCAL_METHODS AS LOCAL_METHODS,
      T.TYPE_ID AS TYPEID
    FROM
      SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
        ON D.DATABASE_ID = T.DATABASE_ID
           AND T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
           AND D.DATABASE_ID = USERENV('SCHEMAID')
           AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='DBA_TYPE_ATTRS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25047',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
    SELECT /*+ USE_MERGE(T, A, D, T1, D1) */
      D.DATABASE_NAME AS OWNER,
      T.TYPE_NAME AS TYPE_NAME,
      A.NAME AS ATTR_NAME,
      CAST(NULL AS VARCHAR2(7)) AS ATTR_TYPE_MOD,
      CAST(
        CASE A.PROPERTIES
        WHEN 3 THEN NULL
        ELSE D1.DATABASE_NAME END AS VARCHAR2(128)) AS ATTR_TYPE_OWNER,
      CAST(
        CASE A.PROPERTIES
        WHEN 3
          THEN DECODE (BITAND(A.TYPE_ATTR_ID,1099511627775),
            0,  'NULL',
            1,  'NUMBER',
            2,  'NUMBER',
            3,  'NUMBER',
            4,  'NUMBER',
            5,  'NUMBER',
            6,  'NUMBER',
            7,  'NUMBER',
            8,  'NUMBER',
            9,  'NUMBER',
            10, 'NUMBER',
            11, 'BINARY_FLOAT',
            12, 'BINARY_DOUBLE',
            13, 'NUMBER',
            14, 'NUMBER',
            15, 'NUMBER',
            16, 'NUMBER',
            17, 'DATE',
            18, 'TIMESTAMP',
            19, 'DATE',
            20, 'TIME',
            21, 'YEAR',
            22, 'VARCHAR2',
            23, 'CHAR',
            24, 'HEX_STRING',
            25, 'EXT',
            26, 'UNKNOWN',
            27, 'TINYTEXT',
            28, 'TEXT',
            29, 'MEDIUMTEXT',
            30,  DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            31, 'BIT',
            32, 'ENUM',
            33, 'SET',
            34, 'ENUM_INNER',
            35, 'SET_INNER',
            36, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH TIME ZONE')),
            37, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH LOCAL TIME ZONE')),
            38, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ')')),
            39, 'RAW',
            40, CONCAT('INTERVAL YEAR(', CONCAT(A.SCALE, ') TO MONTH')),
            41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(A.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(A.SCALE, 10), ')')))),
            42, 'FLOAT',
            43, 'NVARCHAR2',
            44, 'NCHAR',
            45, '',
            46, DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            'NOT_SUPPORT')
        ELSE t1.TYPE_NAME END AS VARCHAR2(324)) AS ATTR_TYPE_NAME,
      A.LENGTH AS LENGTH,
      A.NUMBER_PRECISION AS NUMBER_PRECISION,
      A.SCALE AS SCALE,
      CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,
      A.ATTRIBUTE AS ATTR_NO,
      CAST('NO' AS CHAR(2)) AS INHERITED,
      CAST('C' AS CHAR(1)) AS CHAR_USED
    FROM
      SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT A
        ON T.TYPE_ID = A.TYPE_ID
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
        ON T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
          AND t.database_id = d.database_id
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T1
        ON T1.TYPE_ID = A.TYPE_ATTR_ID
        AND T1.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D1
        ON T1.DATABASE_ID = D1.DATABASE_ID
        AND D1.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT /*+ USE_MERGE(TS, TAS, TS1) */
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      TS.TYPE_NAME AS TYPE_NAME,
      TAS.NAME AS ATTR_NAME,
      CAST(NULL AS VARCHAR2(7)) AS ATTR_TYPE_MOD,
      CAST(
        CASE TAS.PROPERTIES
        WHEN 3 THEN NULL
        ELSE 'SYS' END AS VARCHAR2(128)) AS ATTR_TYPE_OWNER,
      CAST(
        CASE TAS.PROPERTIES
        WHEN 3
          THEN DECODE (BITAND(TAS.TYPE_ATTR_ID,1099511627775),
            0,  'NULL',
            1,  'NUMBER',
            2,  'NUMBER',
            3,  'NUMBER',
            4,  'NUMBER',
            5,  'NUMBER',
            6,  'NUMBER',
            7,  'NUMBER',
            8,  'NUMBER',
            9,  'NUMBER',
            10, 'NUMBER',
            11, 'BINARY_FLOAT',
            12, 'BINARY_DOUBLE',
            13, 'NUMBER',
            14, 'NUMBER',
            15, 'NUMBER',
            16, 'NUMBER',
            17, 'DATE',
            18, 'TIMESTAMP',
            19, 'DATE',
            20, 'TIME',
            21, 'YEAR',
            22, 'VARCHAR2',
            23, 'CHAR',
            24, 'HEX_STRING',
            25, 'EXT',
            26, 'UNKNOWN',
            27, 'TINYTEXT',
            28, 'TEXT',
            29, 'MEDIUMTEXT',
            30,  DECODE(TAS.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            31, 'BIT',
            32, 'ENUM',
            33, 'SET',
            34, 'ENUM_INNER',
            35, 'SET_INNER',
            36, CONCAT('TIMESTAMP(', CONCAT(TAS.SCALE, ') WITH TIME ZONE')),
            37, CONCAT('TIMESTAMP(', CONCAT(TAS.SCALE, ') WITH LOCAL TIME ZONE')),
            38, CONCAT('TIMESTAMP(', CONCAT(TAS.SCALE, ')')),
            39, 'RAW',
            40, CONCAT('INTERVAL YEAR(', CONCAT(TAS.SCALE, ') TO MONTH')),
            41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(TAS.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(TAS.SCALE, 10), ')')))),
            42, 'FLOAT',
            43, 'NVARCHAR2',
            44, 'NCHAR',
            45, '',
            46, DECODE(TAS.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            'NOT_SUPPORT')
        ELSE TS1.TYPE_NAME END AS VARCHAR2(324)) AS ATTR_TYPE_NAME,
      TAS.LENGTH AS LENGTH,
      TAS.NUMBER_PRECISION AS NUMBER_PRECISION,
      TAS.SCALE AS SCALE,
      CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,
      TAS.ATTRIBUTE AS ATTR_NO,
      CAST('NO' AS CHAR(2)) AS INHERITED,
      CAST('C' AS CHAR(1)) AS CHAR_USED
    FROM
      SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS JOIN SYS.ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT TAS
        ON TS.TYPE_ID = TAS.TYPE_ID
      LEFT JOIN SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS1
        ON TS1.TYPE_ID = TAS.TYPE_ATTR_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name='ALL_TYPE_ATTRS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25048',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
    SELECT /*+ USE_MERGE(T, A, D, T1, D1) */
      D.DATABASE_NAME AS OWNER,
      T.TYPE_NAME AS TYPE_NAME,
      A.NAME AS ATTR_NAME,
      CAST(NULL AS VARCHAR2(7)) AS ATTR_TYPE_MOD,
      CAST(
        CASE A.PROPERTIES
        WHEN 3 THEN NULL
        ELSE D1.DATABASE_NAME END AS VARCHAR2(128)) AS ATTR_TYPE_OWNER,
      CAST(
        CASE A.PROPERTIES
        WHEN 3
          THEN DECODE (BITAND(A.TYPE_ATTR_ID,1099511627775),
            0,  'NULL',
            1,  'NUMBER',
            2,  'NUMBER',
            3,  'NUMBER',
            4,  'NUMBER',
            5,  'NUMBER',
            6,  'NUMBER',
            7,  'NUMBER',
            8,  'NUMBER',
            9,  'NUMBER',
            10, 'NUMBER',
            11, 'BINARY_FLOAT',
            12, 'BINARY_DOUBLE',
            13, 'NUMBER',
            14, 'NUMBER',
            15, 'NUMBER',
            16, 'NUMBER',
            17, 'DATE',
            18, 'TIMESTAMP',
            19, 'DATE',
            20, 'TIME',
            21, 'YEAR',
            22, 'VARCHAR2',
            23, 'CHAR',
            24, 'HEX_STRING',
            25, 'EXT',
            26, 'UNKNOWN',
            27, 'TINYTEXT',
            28, 'TEXT',
            29, 'MEDIUMTEXT',
            30,  DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            31, 'BIT',
            32, 'ENUM',
            33, 'SET',
            34, 'ENUM_INNER',
            35, 'SET_INNER',
            36, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH TIME ZONE')),
            37, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH LOCAL TIME ZONE')),
            38, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ')')),
            39, 'RAW',
            40, CONCAT('INTERVAL YEAR(', CONCAT(A.SCALE, ') TO MONTH')),
            41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(A.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(A.SCALE, 10), ')')))),
            42, 'FLOAT',
            43, 'NVARCHAR2',
            44, 'NCHAR',
            45, '',
            46, DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            'NOT_SUPPORT')
        ELSE t1.TYPE_NAME END AS VARCHAR2(324)) AS ATTR_TYPE_NAME,
      A.LENGTH AS LENGTH,
      A.NUMBER_PRECISION AS NUMBER_PRECISION,
      A.SCALE AS SCALE,
      CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,
      A.ATTRIBUTE AS ATTR_NO,
      CAST('NO' AS CHAR(2)) AS INHERITED,
      CAST('C' AS CHAR(1)) AS CHAR_USED
    FROM
      SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT A
        ON T.TYPE_ID = A.TYPE_ID
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
        ON T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
          AND t.database_id = d.database_id
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
          and (t.database_id = USERENV('SCHEMAID')
               or USER_CAN_ACCESS_OBJ(4, t.type_id, 1) = 1)
      LEFT JOIN SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T1
        ON T1.TYPE_ID = A.TYPE_ATTR_ID
        AND T1.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D1
        ON T1.DATABASE_ID = D1.DATABASE_ID
        AND D1.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
  SELECT /*+ USE_MERGE(TS, TAS, TS1) */
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      TS.TYPE_NAME AS TYPE_NAME,
      TAS.NAME AS ATTR_NAME,
      CAST(NULL AS VARCHAR2(7)) AS ATTR_TYPE_MOD,
      CAST(
        CASE TAS.PROPERTIES
        WHEN 3 THEN NULL
        ELSE 'SYS' END AS VARCHAR2(128)) AS ATTR_TYPE_OWNER,
      CAST(
        CASE TAS.PROPERTIES
        WHEN 3
          THEN DECODE (BITAND(TAS.TYPE_ATTR_ID,1099511627775),
            0,  'NULL',
            1,  'NUMBER',
            2,  'NUMBER',
            3,  'NUMBER',
            4,  'NUMBER',
            5,  'NUMBER',
            6,  'NUMBER',
            7,  'NUMBER',
            8,  'NUMBER',
            9,  'NUMBER',
            10, 'NUMBER',
            11, 'BINARY_FLOAT',
            12, 'BINARY_DOUBLE',
            13, 'NUMBER',
            14, 'NUMBER',
            15, 'NUMBER',
            16, 'NUMBER',
            17, 'DATE',
            18, 'TIMESTAMP',
            19, 'DATE',
            20, 'TIME',
            21, 'YEAR',
            22, 'VARCHAR2',
            23, 'CHAR',
            24, 'HEX_STRING',
            25, 'EXT',
            26, 'UNKNOWN',
            27, 'TINYTEXT',
            28, 'TEXT',
            29, 'MEDIUMTEXT',
            30,  DECODE(TAS.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            31, 'BIT',
            32, 'ENUM',
            33, 'SET',
            34, 'ENUM_INNER',
            35, 'SET_INNER',
            36, CONCAT('TIMESTAMP(', CONCAT(TAS.SCALE, ') WITH TIME ZONE')),
            37, CONCAT('TIMESTAMP(', CONCAT(TAS.SCALE, ') WITH LOCAL TIME ZONE')),
            38, CONCAT('TIMESTAMP(', CONCAT(TAS.SCALE, ')')),
            39, 'RAW',
            40, CONCAT('INTERVAL YEAR(', CONCAT(TAS.SCALE, ') TO MONTH')),
            41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(TAS.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(TAS.SCALE, 10), ')')))),
            42, 'FLOAT',
            43, 'NVARCHAR2',
            44, 'NCHAR',
            45, '',
            46, DECODE(TAS.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            'NOT_SUPPORT')
        ELSE TS1.TYPE_NAME END AS VARCHAR2(324)) AS ATTR_TYPE_NAME,
      TAS.LENGTH AS LENGTH,
      TAS.NUMBER_PRECISION AS NUMBER_PRECISION,
      TAS.SCALE AS SCALE,
      CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,
      TAS.ATTRIBUTE AS ATTR_NO,
      CAST('NO' AS CHAR(2)) AS INHERITED,
      CAST('C' AS CHAR(1)) AS CHAR_USED
    FROM
      SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS JOIN SYS.ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT TAS
        ON TS.TYPE_ID = TAS.TYPE_ID
      LEFT JOIN SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS1
        ON TS1.TYPE_ID = TAS.TYPE_ATTR_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_TYPE_ATTRS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25049',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
    SELECT /*+ USE_MERGE(T, A, D, T1, D1) */
      T.TYPE_NAME AS TYPE_NAME,
      A.NAME AS ATTR_NAME,
      CAST(NULL AS VARCHAR2(7)) AS ATTR_TYPE_MOD,
      CAST(
        CASE A.PROPERTIES
        WHEN 3 THEN NULL
        ELSE D1.DATABASE_NAME END AS VARCHAR2(128)) AS ATTR_TYPE_OWNER,
      CAST(
        CASE A.PROPERTIES
        WHEN 3
          THEN DECODE (BITAND(A.TYPE_ATTR_ID,1099511627775),
            0,  'NULL',
            1,  'NUMBER',
            2,  'NUMBER',
            3,  'NUMBER',
            4,  'NUMBER',
            5,  'NUMBER',
            6,  'NUMBER',
            7,  'NUMBER',
            8,  'NUMBER',
            9,  'NUMBER',
            10, 'NUMBER',
            11, 'BINARY_FLOAT',
            12, 'BINARY_DOUBLE',
            13, 'NUMBER',
            14, 'NUMBER',
            15, 'NUMBER',
            16, 'NUMBER',
            17, 'DATE',
            18, 'TIMESTAMP',
            19, 'DATE',
            20, 'TIME',
            21, 'YEAR',
            22, 'VARCHAR2',
            23, 'CHAR',
            24, 'HEX_STRING',
            25, 'EXT',
            26, 'UNKNOWN',
            27, 'TINYTEXT',
            28, 'TEXT',
            29, 'MEDIUMTEXT',
            30,  DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            31, 'BIT',
            32, 'ENUM',
            33, 'SET',
            34, 'ENUM_INNER',
            35, 'SET_INNER',
            36, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH TIME ZONE')),
            37, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ') WITH LOCAL TIME ZONE')),
            38, CONCAT('TIMESTAMP(', CONCAT(A.SCALE, ')')),
            39, 'RAW',
            40, CONCAT('INTERVAL YEAR(', CONCAT(A.SCALE, ') TO MONTH')),
            41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(A.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(A.SCALE, 10), ')')))),
            42, 'FLOAT',
            43, 'NVARCHAR2',
            44, 'NCHAR',
            45, '',
            46, DECODE(A.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            'NOT_SUPPORT')
        ELSE t1.TYPE_NAME END AS VARCHAR2(324)) AS ATTR_TYPE_NAME,
      A.LENGTH AS LENGTH,
      A.NUMBER_PRECISION AS NUMBER_PRECISION,
      A.SCALE AS SCALE,
      CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,
      A.ATTRIBUTE AS ATTR_NO,
      CAST('NO' AS CHAR(2)) AS INHERITED,
      CAST('C' AS CHAR(1)) AS CHAR_USED
    FROM
      SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT A
        ON T.TYPE_ID = A.TYPE_ID
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
      JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
        ON T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
        AND T.DATABASE_ID = D.DATABASE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND D.DATABASE_ID = USERENV('SCHEMAID')
      LEFT JOIN SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T1
        ON T1.TYPE_ID = A.TYPE_ATTR_ID
        AND T1.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D1
        ON T1.DATABASE_ID = D1.DATABASE_ID
        AND D1.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='DBA_COLL_TYPES',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25050',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
    SELECT /*+ USE_MERGE(T, C, D, T1, D1) */
      D.DATABASE_NAME AS OWNER,
      T.TYPE_NAME AS TYPE_NAME,
      CAST(
        CASE C.UPPER_BOUND
        WHEN 0 THEN 'COLLECTION'
        ELSE 'TABLE' END AS VARCHAR2(10)) AS COLL_TYPE,
      C.UPPER_BOUND AS UPPER_BOUND,
      CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD,
      CAST(
        CASE C.PROPERTIES
        WHEN 3 THEN NULL
        ELSE D1.DATABASE_NAME END AS VARCHAR2(128)) AS ELEM_TYPE_OWNER,
      CAST(
        CASE C.PROPERTIES
        WHEN 3
          THEN DECODE (BITAND(C.ELEM_TYPE_ID, 1099511627775),
            0,  'NULL',
            1,  'NUMBER',
            2,  'NUMBER',
            3,  'NUMBER',
            4,  'NUMBER',
            5,  'NUMBER',
            6,  'NUMBER',
            7,  'NUMBER',
            8,  'NUMBER',
            9,  'NUMBER',
            10, 'NUMBER',
            11, 'BINARY_FLOAT',
            12, 'BINARY_DOUBLE',
            13, 'NUMBER',
            14, 'NUMBER',
            15, 'NUMBER',
            16, 'NUMBER',
            17, 'DATE',
            18, 'TIMESTAMP',
            19, 'DATE',
            20, 'TIME',
            21, 'YEAR',
            22, 'VARCHAR2',
            23, 'CHAR',
            24, 'HEX_STRING',
            25, 'EXT',
            26, 'UNKNOWN',
            27, 'TINYTEXT',
            28, 'TEXT',
            29, 'MEDIUMTEXT',
            30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            31, 'BIT',
            32, 'ENUM',
            33, 'SET',
            34, 'ENUM_INNER',
            35, 'SET_INNER',
            36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),
            37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),
            38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),
            39, 'RAW',
            40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),
            41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),
            42, 'FLOAT',
            43, 'NVARCHAR2',
            44, 'NCHAR',
            45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),
            46, DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            'NOT_SUPPORT')
        ELSE t1.TYPE_NAME END AS VARCHAR2(324)) AS ELEM_TYPE_NAME,
      C.LENGTH AS LENGTH,
      C.NUMBER_PRECISION AS NUMBER_PRECISION,
      C.SCALE AS SCALE,
      CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,
      CAST('YES' AS CHAR(3)) AS ELEM_STORAGE,
      CAST('B' AS CHAR(1)) AS NULLS_STORED
    FROM
      SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_COLL_TYPE_REAL_AGENT C
        ON T.TYPE_ID = C.COLL_TYPE_ID
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
      JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
        ON T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
          AND T.DATABASE_ID = D.DATABASE_ID
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T1
        ON T1.TYPE_ID = C.ELEM_TYPE_ID
        AND T1.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D1
        ON T1.DATABASE_ID = D1.DATABASE_ID
        AND D1.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT /*+ USE_MERGE(TS, CS, TS1) */
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      TS.TYPE_NAME AS TYPE_NAME,
      CAST(
        CASE CS.UPPER_BOUND
        WHEN 0 THEN 'COLLECTION'
        ELSE 'TABLE' END AS VARCHAR2(10)) AS COLL_TYPE,
      CS.UPPER_BOUND AS UPPER_BOUND,
      CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD,
      CAST(
        CASE CS.PROPERTIES
        WHEN 3 THEN NULL
        ELSE 'SYS' END AS VARCHAR2(128)) AS ELEM_TYPE_OWNER,
      CAST(
        CASE CS.PROPERTIES
        WHEN 3
          THEN DECODE (BITAND(CS.ELEM_TYPE_ID, 1099511627775),
            0,  'NULL',
            1,  'NUMBER',
            2,  'NUMBER',
            3,  'NUMBER',
            4,  'NUMBER',
            5,  'NUMBER',
            6,  'NUMBER',
            7,  'NUMBER',
            8,  'NUMBER',
            9,  'NUMBER',
            10, 'NUMBER',
            11, 'BINARY_FLOAT',
            12, 'BINARY_DOUBLE',
            13, 'NUMBER',
            14, 'NUMBER',
            15, 'NUMBER',
            16, 'NUMBER',
            17, 'DATE',
            18, 'TIMESTAMP',
            19, 'DATE',
            20, 'TIME',
            21, 'YEAR',
            22, 'VARCHAR2',
            23, 'CHAR',
            24, 'HEX_STRING',
            25, 'EXT',
            26, 'UNKNOWN',
            27, 'TINYTEXT',
            28, 'TEXT',
            29, 'MEDIUMTEXT',
            30,  DECODE(CS.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            31, 'BIT',
            32, 'ENUM',
            33, 'SET',
            34, 'ENUM_INNER',
            35, 'SET_INNER',
            36, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ') WITH TIME ZONE')),
            37, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ') WITH LOCAL TIME ZONE')),
            38, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ')')),
            39, 'RAW',
            40, CONCAT('INTERVAL YEAR(', CONCAT(CS.SCALE, ') TO MONTH')),
            41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(CS.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(CS.SCALE, 10), ')')))),
            42, 'FLOAT',
            43, 'NVARCHAR2',
            44, 'NCHAR',
            45, CONCAT('UROWID(', CONCAT(CS.LENGTH, ')')),
            46, DECODE(CS.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            'NOT_SUPPORT')
        ELSE TS1.TYPE_NAME END AS VARCHAR2(324)) AS ELEM_TYPE_NAME,
      CS.LENGTH AS LENGTH,
      CS.NUMBER_PRECISION AS NUMBER_PRECISION,
      CS.SCALE AS SCALE,
      CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,
      CAST('YES' AS CHAR(3)) AS ELEM_STORAGE,
      CAST('B' AS CHAR(1)) AS NULLS_STORED
    FROM
      SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS JOIN SYS.ALL_VIRTUAL_COLL_TYPE_SYS_AGENT CS
        ON TS.TYPE_ID = CS.COLL_TYPE_ID
      LEFT JOIN SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS1
        ON TS1.TYPE_ID = CS.ELEM_TYPE_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name='ALL_COLL_TYPES',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25051',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
    SELECT /*+ USE_MERGE(T, C, D, T1, D1) */
      D.DATABASE_NAME AS OWNER,
      T.TYPE_NAME AS TYPE_NAME,
      CAST(
        CASE C.UPPER_BOUND
        WHEN 0 THEN 'COLLECTION'
        ELSE 'TABLE' END AS VARCHAR2(10)) AS COLL_TYPE,
      C.UPPER_BOUND AS UPPER_BOUND,
      CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD,
      CAST(
        CASE C.PROPERTIES
        WHEN 3 THEN NULL
        ELSE D1.DATABASE_NAME END AS VARCHAR2(128)) AS ELEM_TYPE_OWNER,
      CAST(
        CASE C.PROPERTIES
        WHEN 3
          THEN DECODE (BITAND(C.ELEM_TYPE_ID, 1099511627775),
            0,  'NULL',
            1,  'NUMBER',
            2,  'NUMBER',
            3,  'NUMBER',
            4,  'NUMBER',
            5,  'NUMBER',
            6,  'NUMBER',
            7,  'NUMBER',
            8,  'NUMBER',
            9,  'NUMBER',
            10, 'NUMBER',
            11, 'BINARY_FLOAT',
            12, 'BINARY_DOUBLE',
            13, 'NUMBER',
            14, 'NUMBER',
            15, 'NUMBER',
            16, 'NUMBER',
            17, 'DATE',
            18, 'TIMESTAMP',
            19, 'DATE',
            20, 'TIME',
            21, 'YEAR',
            22, 'VARCHAR2',
            23, 'CHAR',
            24, 'HEX_STRING',
            25, 'EXT',
            26, 'UNKNOWN',
            27, 'TINYTEXT',
            28, 'TEXT',
            29, 'MEDIUMTEXT',
            30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            31, 'BIT',
            32, 'ENUM',
            33, 'SET',
            34, 'ENUM_INNER',
            35, 'SET_INNER',
            36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),
            37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),
            38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),
            39, 'RAW',
            40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),
            41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),
            42, 'FLOAT',
            43, 'NVARCHAR2',
            44, 'NCHAR',
            45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),
            46, DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            'NOT_SUPPORT')
        ELSE t1.TYPE_NAME END AS VARCHAR2(324)) AS ELEM_TYPE_NAME,
      C.LENGTH AS LENGTH,
      C.NUMBER_PRECISION AS NUMBER_PRECISION,
      C.SCALE AS SCALE,
      CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,
      CAST('YES' AS CHAR(3)) AS ELEM_STORAGE,
      CAST('B' AS CHAR(1)) AS NULLS_STORED
    FROM
      SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_COLL_TYPE_REAL_AGENT C
        ON T.TYPE_ID = C.COLL_TYPE_ID
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
      JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
        ON T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
          AND T.DATABASE_ID = D.DATABASE_ID
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND (T.DATABASE_ID = USERENV('SCHEMAID')
             or USER_CAN_ACCESS_OBJ(4, T.TYPE_ID, 1) = 1)
      LEFT JOIN SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T1
        ON T1.TYPE_ID = C.ELEM_TYPE_ID
        AND T1.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D1
        ON T1.DATABASE_ID = D1.DATABASE_ID
        AND D1.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION ALL
    SELECT /*+ USE_MERGE(TS, CS, TS1) */
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      TS.TYPE_NAME AS TYPE_NAME,
      CAST(
        CASE CS.UPPER_BOUND
        WHEN 0 THEN 'COLLECTION'
        ELSE 'TABLE' END AS VARCHAR2(10)) AS COLL_TYPE,
      CS.UPPER_BOUND AS UPPER_BOUND,
      CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD,
      CAST(
        CASE CS.PROPERTIES
        WHEN 3 THEN NULL
        ELSE 'SYS' END AS VARCHAR2(128)) AS ELEM_TYPE_OWNER,
      CAST(
        CASE CS.PROPERTIES
        WHEN 3
          THEN DECODE (BITAND(CS.ELEM_TYPE_ID, 1099511627775),
            0,  'NULL',
            1,  'NUMBER',
            2,  'NUMBER',
            3,  'NUMBER',
            4,  'NUMBER',
            5,  'NUMBER',
            6,  'NUMBER',
            7,  'NUMBER',
            8,  'NUMBER',
            9,  'NUMBER',
            10, 'NUMBER',
            11, 'BINARY_FLOAT',
            12, 'BINARY_DOUBLE',
            13, 'NUMBER',
            14, 'NUMBER',
            15, 'NUMBER',
            16, 'NUMBER',
            17, 'DATE',
            18, 'TIMESTAMP',
            19, 'DATE',
            20, 'TIME',
            21, 'YEAR',
            22, 'VARCHAR2',
            23, 'CHAR',
            24, 'HEX_STRING',
            25, 'EXT',
            26, 'UNKNOWN',
            27, 'TINYTEXT',
            28, 'TEXT',
            29, 'MEDIUMTEXT',
            30,  DECODE(CS.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            31, 'BIT',
            32, 'ENUM',
            33, 'SET',
            34, 'ENUM_INNER',
            35, 'SET_INNER',
            36, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ') WITH TIME ZONE')),
            37, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ') WITH LOCAL TIME ZONE')),
            38, CONCAT('TIMESTAMP(', CONCAT(CS.SCALE, ')')),
            39, 'RAW',
            40, CONCAT('INTERVAL YEAR(', CONCAT(CS.SCALE, ') TO MONTH')),
            41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(CS.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(CS.SCALE, 10), ')')))),
            42, 'FLOAT',
            43, 'NVARCHAR2',
            44, 'NCHAR',
            45, CONCAT('UROWID(', CONCAT(CS.LENGTH, ')')),
            46, '',
            'NOT_SUPPORT')
        ELSE TS1.TYPE_NAME END AS VARCHAR2(324)) AS ELEM_TYPE_NAME,
      CS.LENGTH AS LENGTH,
      CS.NUMBER_PRECISION AS NUMBER_PRECISION,
      CS.SCALE AS SCALE,
      CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,
      CAST('YES' AS CHAR(3)) AS ELEM_STORAGE,
      CAST('B' AS CHAR(1)) AS NULLS_STORED
    FROM
      SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS JOIN SYS.ALL_VIRTUAL_COLL_TYPE_SYS_AGENT CS
        ON TS.TYPE_ID = CS.COLL_TYPE_ID
      LEFT JOIN SYS.ALL_VIRTUAL_TYPE_SYS_AGENT TS1
        ON TS1.TYPE_ID = CS.ELEM_TYPE_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_COLL_TYPES',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25052',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
    SELECT /*+ USE_MERGE(T, C, D, T1, D1) */
      T.TYPE_NAME AS TYPE_NAME,
      CAST(
        CASE C.UPPER_BOUND
        WHEN 0 THEN 'COLLECTION'
        ELSE 'TABLE' END AS VARCHAR2(10)) AS COLL_TYPE,
      C.UPPER_BOUND AS UPPER_BOUND,
      CAST(NULL AS VARCHAR2(7)) AS ELEM_TYPE_MOD,
      CAST(
        CASE C.PROPERTIES
        WHEN 3 THEN NULL
        ELSE d1.DATABASE_NAME END AS VARCHAR2(128)) AS ELEM_TYPE_OWNER,
      CAST(
        CASE C.PROPERTIES
        WHEN 3
          THEN DECODE (BITAND(C.ELEM_TYPE_ID, 1099511627775),
            0,  'NULL',
            1,  'NUMBER',
            2,  'NUMBER',
            3,  'NUMBER',
            4,  'NUMBER',
            5,  'NUMBER',
            6,  'NUMBER',
            7,  'NUMBER',
            8,  'NUMBER',
            9,  'NUMBER',
            10, 'NUMBER',
            11, 'BINARY_FLOAT',
            12, 'BINARY_DOUBLE',
            13, 'NUMBER',
            14, 'NUMBER',
            15, 'NUMBER',
            16, 'NUMBER',
            17, 'DATE',
            18, 'TIMESTAMP',
            19, 'DATE',
            20, 'TIME',
            21, 'YEAR',
            22, 'VARCHAR2',
            23, 'CHAR',
            24, 'HEX_STRING',
            25, 'EXT',
            26, 'UNKNOWN',
            27, 'TINYTEXT',
            28, 'TEXT',
            29, 'MEDIUMTEXT',
            30,  DECODE(C.COLL_TYPE, 63, 'BLOB', 'CLOB'),
            31, 'BIT',
            32, 'ENUM',
            33, 'SET',
            34, 'ENUM_INNER',
            35, 'SET_INNER',
            36, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH TIME ZONE')),
            37, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ') WITH LOCAL TIME ZONE')),
            38, CONCAT('TIMESTAMP(', CONCAT(C.SCALE, ')')),
            39, 'RAW',
            40, CONCAT('INTERVAL YEAR(', CONCAT(C.SCALE, ') TO MONTH')),
            41, CONCAT('INTERVAL DAY(', CONCAT(TRUNC(C.SCALE / 10), CONCAT(') TO SECOND(', CONCAT(MOD(C.SCALE, 10), ')')))),
            42, 'FLOAT',
            43, 'NVARCHAR2',
            44, 'NCHAR',
            45, CONCAT('UROWID(', CONCAT(C.LENGTH, ')')),
            46, '',
            'NOT_SUPPORT')
        ELSE t1.TYPE_NAME END AS VARCHAR2(324)) AS ELEM_TYPE_NAME,
      C.LENGTH AS LENGTH,
      C.NUMBER_PRECISION AS NUMBER_PRECISION,
      C.SCALE AS SCALE,
      CAST('CHAR_CS' AS CHAR(7)) AS CHARACTER_SET_NAME,
      CAST('YES' AS CHAR(7)) AS ELEM_STORAGE,
      CAST('B' AS CHAR(7)) AS NULLS_STORED
    FROM
      SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T JOIN SYS.ALL_VIRTUAL_COLL_TYPE_REAL_AGENT C
        ON T.TYPE_ID = C.COLL_TYPE_ID
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
      JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
        ON T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
           AND T.DATABASE_ID = D.DATABASE_ID
           AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
           AND D.DATABASE_ID = USERENV('SCHEMAID')
      LEFT JOIN SYS.ALL_VIRTUAL_TYPE_REAL_AGENT T1
        ON T1.TYPE_ID = C.ELEM_TYPE_ID
        AND T1.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D1
        ON T1.DATABASE_ID = D1.DATABASE_ID
        AND D1.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='DBA_PROCEDURES',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25053',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT
      U.USER_NAME AS OWNER,
      CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_NAME ELSE P.PACKAGE_NAME END AS OBJECT_NAME,
      CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE R.ROUTINE_NAME END AS PROCEDURE_NAME,
      CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,
      CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,
      CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,
      CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS OBJECT_TYPE,
      CAST('NO' AS VARCHAR2(3)) AS AGGREGATE,
      CAST(DECODE(BITAND(R.FLAG, 128), 128, 'YES', 'NO') AS VARCHAR2(3)) AS PIPELINED,
      CAST(NULL AS VARCHAR2(30)) AS IMPLTYPEOWNER,
      CAST(NULL AS VARCHAR2(30)) AS IMPLTYPENAME,
      CAST(DECODE(BITAND(R.FLAG, 8), 8, 'YES', 'NO') AS VARCHAR2(3)) AS PARALLEL,
      CAST('NO' AS VARCHAR2(3)) AS INTERFACE,
      CAST(DECODE(BITAND(R.FLAG, 4), 4, 'YES', 'NO') AS VARCHAR2(3)) AS DETERMINISTIC,
      CAST(DECODE(BITAND(R.FLAG, 16), 16, 'INVOKER', 'DEFINER') AS VARCHAR2(12)) AS AUTHID,
      R.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT
          WHERE TENANT_ID = EFFECTIVE_TENANT_ID())R
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID
          AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID
          AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1) AND D.IN_RECYCLEBIN = 0
    UNION ALL
    SELECT
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_NAME ELSE PS.PACKAGE_NAME END AS OBJECT_NAME,
      CASE RS.PACKAGE_ID WHEN -1 THEN NULL ELSE RS.ROUTINE_NAME END AS PROCEDURE_NAME,
      CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_ID ELSE RS.PACKAGE_ID END AS OBJECT_ID,
      CASE RS.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE RS.SUBPROGRAM_ID END AS SUBPROGRAM_ID,
      CASE RS.OVERLOAD WHEN 0 THEN NULL ELSE RS.OVERLOAD END AS OVERLOAD,
      CASE RS.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS OBJECT_TYPE,
      CAST('NO' AS VARCHAR2(3)) AS AGGREGATE,
      CAST(DECODE(BITAND(RS.FLAG, 128), 128, 'YES', 'NO') AS VARCHAR2(3)) AS PIPELINED,
      CAST(NULL AS VARCHAR2(30)) AS IMPLTYPEOWNER,
      CAST(NULL AS VARCHAR2(30)) AS IMPLTYPENAME,
      CAST(DECODE(BITAND(RS.FLAG, 8), 8, 'YES', 'NO') AS VARCHAR2(3)) AS PARALLEL,
      CAST('NO' AS VARCHAR2(3)) AS INTERFACE,
      CAST(DECODE(BITAND(RS.FLAG, 4), 4, 'YES', 'NO') AS VARCHAR2(3)) AS DETERMINISTIC,
      CAST(DECODE(BITAND(RS.FLAG, 16), 16, 'INVOKER', 'DEFINER') AS VARCHAR2(12)) AS AUTHID,
      RS.TENANT_ID AS ORIGIN_CON_ID
    FROM
      SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS
      LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS ON RS.PACKAGE_ID = PS.PACKAGE_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name='DBA_ARGUMENTS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25054',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT
    U.USER_NAME AS OWNER,
    R.ROUTINE_NAME AS OBJECT_NAME,
    CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE P.PACKAGE_NAME END AS PACKAGE_NAME,
    CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,
    CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,
    CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,
    RP.PARAM_NAME AS ARGUMENT_NAME,
    RP.PARAM_POSITION AS POSITION,
    RP.SEQUENCE AS SEQUENCE,
    RP.PARAM_LEVEL AS DATA_LEVEL,
    V.DATA_TYPE_STR AS DATA_TYPE,
    'NO' AS DEFAULTED,
    RP.PARAM_LENGTH AS DATA_LENGTH,
    DECODE(BITAND(RP.FLAG, 3), 1, 'IN', 2, 'OUT', 3, 'INOUT', 0, 'OUT') AS IN_OUT,
    RP.PARAM_PRECISION AS DATA_PRECISION,
    RP.PARAM_SCALE AS DATA_SCALE,
    CASE RP.PARAM_CHARSET WHEN 1 THEN 'BINARY' WHEN 2 THEN 'UTF8MB4' ELSE NULL END AS CHARACTER_SET_NAME,
    CASE RP.PARAM_COLL_TYPE WHEN 45 THEN 'UTF8MB4_GENERAL_CI' WHEN 46 THEN 'UTF8MB4_BIN' WHEN 63 THEN 'BINARY' ELSE NULL END AS COLLATION,
    RP.TYPE_OWNER AS TYPE_OWNER,
    RP.TYPE_NAME AS TYPE_NAME,
    RP.TYPE_SUBNAME AS TYPE_SUBNAME,
    RP.TENANT_ID AS ORIGIN_CON_ID
  FROM
    (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT
        WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) RP
    LEFT JOIN SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT R ON RP.ROUTINE_ID = R.ROUTINE_ID
        AND R.TENANT_ID = EFFECTIVE_TENANT_ID()
    LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID
        AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
    LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID
        AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    LEFT JOIN SYS.ALL_VIRTUAL_DATA_TYPE V ON RP.PARAM_TYPE = V.DATA_TYPE
  WHERE
    (SYS_CONTEXT('USERENV', 'CON_ID') = RP.TENANT_ID OR RP.TENANT_ID = 1) AND D.IN_RECYCLEBIN = 0
  UNION ALL
  SELECT
    CAST('SYS' AS VARCHAR2(30)) AS OWNER,
    RS.ROUTINE_NAME AS OBJECT_NAME,
    CASE RS.PACKAGE_ID WHEN -1 THEN NULL ELSE PS.PACKAGE_NAME END AS PACKAGE_NAME,
    CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_ID ELSE RS.PACKAGE_ID END AS OBJECT_ID,
    CASE RS.OVERLOAD WHEN 0 THEN NULL ELSE RS.OVERLOAD END AS OVERLOAD,
    CASE RS.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE RS.SUBPROGRAM_ID END AS SUBPROGRAM_ID,
    RPS.PARAM_NAME AS ARGUMENT_NAME,
    RPS.PARAM_POSITION AS POSITION,
    RPS.SEQUENCE AS SEQUENCE,
    RPS.PARAM_LEVEL AS DATA_LEVEL,
    VV.DATA_TYPE_STR AS DATA_TYPE,
    'NO' AS DEFAULTED,
    RPS.PARAM_LENGTH AS DATA_LENGTH,
    DECODE(BITAND(RPS.FLAG, 3), 1, 'IN', 2, 'OUT', 3, 'INOUT') AS IN_OUT,
    RPS.PARAM_PRECISION AS DATA_PRECISION,
    RPS.PARAM_SCALE AS DATA_SCALE,
    CASE RPS.PARAM_CHARSET WHEN 1 THEN 'BINARY' WHEN 2 THEN 'UTF8MB4' ELSE NULL END AS CHARACTER_SET_NAME,
    CASE RPS.PARAM_COLL_TYPE WHEN 45 THEN 'UTF8MB4_GENERAL_CI' WHEN 46 THEN 'UTF8MB4_BIN' WHEN 63 THEN 'BINARY' ELSE NULL END AS COLLATION,
    RPS.TYPE_OWNER AS TYPE_OWNER,
    RPS.TYPE_NAME AS TYPE_NAME,
    RPS.TYPE_SUBNAME AS TYPE_SUBNAME,
    RPS.TENANT_ID AS ORIGIN_CON_ID
  FROM
    SYS.ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT RPS
    LEFT JOIN SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS ON RPS.ROUTINE_ID = RS.ROUTINE_ID
    LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS ON RS.PACKAGE_ID = PS.PACKAGE_ID
    LEFT JOIN SYS.ALL_VIRTUAL_DATA_TYPE VV ON RPS.PARAM_TYPE = VV.DATA_TYPE
""".replace("\n", " ")
)

def_table_schema(
  table_name='DBA_SOURCE',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25055',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT
      CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,
      CAST(P.PACKAGE_NAME AS VARCHAR2(30)) AS NAME,
      CAST(CASE P.TYPE WHEN 1 THEN 'PACKAGE' WHEN 2 THEN 'PACKAGE BODY' END AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(P.SOURCE) AS TEXT,
      P.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT
          WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) P
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON P.DATABASE_ID = D.DATABASE_ID
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON P.OWNER_ID = U.USER_ID
          AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = P.TENANT_ID OR P.TENANT_ID = 1) AND D.IN_RECYCLEBIN = 0
    UNION ALL
    SELECT
      CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,
      CAST(R.ROUTINE_NAME AS VARCHAR2(30)) AS NAME,
      CAST(CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(R.ROUTINE_BODY) AS TEXT,
      R.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT
          WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) R
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID
          AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1) AND D.IN_RECYCLEBIN = 0 AND R.PACKAGE_ID = -1
    UNION ALL
    SELECT
      CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,
      CAST(T.TRIGGER_NAME AS VARCHAR2(30)) AS NAME,
      CAST('TRIGGER' AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(T.TRIGGER_BODY) AS TEXT,
      T.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT
        WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) T
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON T.DATABASE_ID = D.DATABASE_ID
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON T.OWNER_ID = U.USER_ID
          AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = T.TENANT_ID OR T.TENANT_ID = 1)
      AND D.IN_RECYCLEBIN = 0
    UNION ALL
    SELECT
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      CAST(PS.PACKAGE_NAME AS VARCHAR2(30)) AS NAME,
      CAST(CASE PS.TYPE WHEN 1 THEN 'PACKAGE' WHEN 2 THEN 'PACKAGE BODY' END AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(PS.SOURCE) AS TEXT,
      PS.TENANT_ID AS ORIGIN_CON_ID
    FROM
      SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS
    UNION ALL
    SELECT
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      CAST(RS.ROUTINE_NAME AS VARCHAR2(30)) AS NAME,
      CAST(CASE RS.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(RS.ROUTINE_BODY) AS TEXT,
      RS.TENANT_ID AS ORIGIN_CON_ID
    FROM
      SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS WHERE RS.ROUTINE_TYPE != 3
    UNION ALL
    SELECT
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      CAST(TS.TRIGGER_NAME AS VARCHAR2(30)) AS NAME,
      CAST('TRIGGER' AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(TS.TRIGGER_BODY) AS TEXT,
      TS.TENANT_ID AS ORIGIN_CON_ID
    FROM
      SYS.ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT TS
""".replace("\n", " ")
)

def_table_schema(
  table_name='ALL_PROCEDURES',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25056',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT
      U.USER_NAME AS OWNER,
      CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_NAME ELSE P.PACKAGE_NAME END AS OBJECT_NAME,
      CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE R.ROUTINE_NAME END AS PROCEDURE_NAME,
      CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,
      CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,
      CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,
      CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS OBJECT_TYPE,
      CAST('NO' AS VARCHAR2(3)) AS AGGREGATE,
      CAST(DECODE(BITAND(R.FLAG, 128), 128, 'YES', 'NO') AS VARCHAR2(3)) AS PIPELINED,
      CAST(NULL AS VARCHAR2(30)) AS IMPLTYPEOWNER,
      CAST(NULL AS VARCHAR2(30)) AS IMPLTYPENAME,
      CAST(DECODE(BITAND(R.FLAG, 8), 8, 'YES', 'NO') AS VARCHAR2(3)) AS PARALLEL,
      CAST('NO' AS VARCHAR2(3)) AS INTERFACE,
      CAST(DECODE(BITAND(R.FLAG, 4), 4, 'YES', 'NO') AS VARCHAR2(3)) AS DETERMINISTIC,
      CAST(DECODE(BITAND(R.FLAG, 16), 16, 'INVOKER', 'DEFINER') AS VARCHAR2(12)) AS AUTHID,
      R.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT
          WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) R
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID
          AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID
          AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (R.DATABASE_ID = USERENV('SCHEMAID')
        OR USER_CAN_ACCESS_OBJ(12, R.ROUTINE_ID, R.DATABASE_ID) = 1)
      AND (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1)
      AND D.IN_RECYCLEBIN = 0
  UNION ALL
  SELECT
    CAST('SYS' AS VARCHAR2(30)) AS OWNER,
    CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_NAME ELSE PS.PACKAGE_NAME END AS OBJECT_NAME,
    CASE RS.PACKAGE_ID WHEN -1 THEN NULL ELSE RS.ROUTINE_NAME END AS PROCEDURE_NAME,
    CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_ID ELSE RS.PACKAGE_ID END AS OBJECT_ID,
    CASE RS.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE RS.SUBPROGRAM_ID END AS SUBPROGRAM_ID,
    CASE RS.OVERLOAD WHEN 0 THEN NULL ELSE RS.OVERLOAD END AS OVERLOAD,
    CASE RS.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS OBJECT_TYPE,
    CAST('NO' AS VARCHAR2(3)) AS AGGREGATE,
    CAST(DECODE(BITAND(RS.FLAG, 128), 128, 'YES', 'NO') AS VARCHAR2(3)) AS PIPELINED,
    CAST(NULL AS VARCHAR2(30)) AS IMPLTYPEOWNER,
    CAST(NULL AS VARCHAR2(30)) AS IMPLTYPENAME,
    CAST(DECODE(BITAND(RS.FLAG, 8), 8, 'YES', 'NO') AS VARCHAR2(3)) AS PARALLEL,
    CAST('NO' AS VARCHAR2(3)) AS INTERFACE,
    CAST(DECODE(BITAND(RS.FLAG, 4), 4, 'YES', 'NO') AS VARCHAR2(3)) AS DETERMINISTIC,
    CAST(DECODE(BITAND(RS.FLAG, 16), 16, 'INVOKER', 'DEFINER') AS VARCHAR2(12)) AS AUTHID,
      RS.TENANT_ID AS ORIGIN_CON_ID
  FROM
    SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS
    LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS ON RS.PACKAGE_ID = PS.PACKAGE_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name='ALL_ARGUMENTS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25057',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT
    U.USER_NAME AS OWNER,
    R.ROUTINE_NAME AS OBJECT_NAME,
    CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE P.PACKAGE_NAME END AS PACKAGE_NAME,
    CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,
    CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,
    CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,
    RP.PARAM_NAME AS ARGUMENT_NAME,
    RP.PARAM_POSITION AS POSITION,
    RP.SEQUENCE AS SEQUENCE,
    RP.PARAM_LEVEL AS DATA_LEVEL,
    V.DATA_TYPE_STR AS DATA_TYPE,
    'NO' AS DEFAULTED,
    RP.PARAM_LENGTH AS DATA_LENGTH,
    DECODE(BITAND(RP.FLAG, 3), 1, 'IN', 2, 'OUT', 3, 'INOUT', 0, 'OUT') AS IN_OUT,
    RP.PARAM_PRECISION AS DATA_PRECISION,
    RP.PARAM_SCALE AS DATA_SCALE,
    CASE RP.PARAM_CHARSET WHEN 1 THEN 'BINARY' WHEN 2 THEN 'UTF8MB4' ELSE NULL END AS CHARACTER_SET_NAME,
    CASE RP.PARAM_COLL_TYPE WHEN 45 THEN 'UTF8MB4_GENERAL_CI' WHEN 46 THEN 'UTF8MB4_BIN' WHEN 63 THEN 'BINARY' ELSE NULL END AS COLLATION,
    RP.TYPE_OWNER AS TYPE_OWNER,
    RP.TYPE_NAME AS TYPE_NAME,
    RP.TYPE_SUBNAME AS TYPE_SUBNAME,
    RP.TENANT_ID AS ORIGIN_CON_ID
  FROM
    (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT
      WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) RP
    LEFT JOIN SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT R ON RP.ROUTINE_ID = R.ROUTINE_ID
      AND R.TENANT_ID = EFFECTIVE_TENANT_ID()
    LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID
      AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID
      AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
    LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID
      AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    LEFT JOIN SYS.ALL_VIRTUAL_DATA_TYPE V ON RP.PARAM_TYPE = V.DATA_TYPE
  WHERE
    (SYS_CONTEXT('USERENV', 'CON_ID') = RP.TENANT_ID OR RP.TENANT_ID = 1)
    AND D.IN_RECYCLEBIN = 0
    AND (R.DATABASE_ID = USERENV('SCHEMAID')
      OR USER_CAN_ACCESS_OBJ(12, RP.ROUTINE_ID, 1) = 1)
  UNION ALL
  SELECT
    CAST('SYS' AS VARCHAR2(30)) AS OWNER,
    RS.ROUTINE_NAME AS OBJECT_NAME,
    CASE RS.PACKAGE_ID WHEN -1 THEN NULL ELSE PS.PACKAGE_NAME END AS PACKAGE_NAME,
    CASE RS.PACKAGE_ID WHEN -1 THEN RS.ROUTINE_ID ELSE RS.PACKAGE_ID END AS OBJECT_ID,
    CASE RS.OVERLOAD WHEN 0 THEN NULL ELSE RS.OVERLOAD END AS OVERLOAD,
    CASE RS.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE RS.SUBPROGRAM_ID END AS SUBPROGRAM_ID,
    RPS.PARAM_NAME AS ARGUMENT_NAME,
    RPS.PARAM_POSITION AS POSITION,
    RPS.SEQUENCE AS SEQUENCE,
    RPS.PARAM_LEVEL AS DATA_LEVEL,
    VV.DATA_TYPE_STR AS DATA_TYPE,
    'NO' AS DEFAULTED,
    RPS.PARAM_LENGTH AS DATA_LENGTH,
    DECODE(BITAND(RPS.FLAG, 3), 1, 'IN', 2, 'OUT', 3, 'INOUT') AS IN_OUT,
    RPS.PARAM_PRECISION AS DATA_PRECISION,
    RPS.PARAM_SCALE AS DATA_SCALE,
    CASE RPS.PARAM_CHARSET WHEN 1 THEN 'BINARY' WHEN 2 THEN 'UTF8MB4' ELSE NULL END AS CHARACTER_SET_NAME,
    CASE RPS.PARAM_COLL_TYPE WHEN 45 THEN 'UTF8MB4_GENERAL_CI' WHEN 46 THEN 'UTF8MB4_BIN' WHEN 63 THEN 'BINARY' ELSE NULL END AS COLLATION,
    RPS.TYPE_OWNER AS TYPE_OWNER,
    RPS.TYPE_NAME AS TYPE_NAME,
    RPS.TYPE_SUBNAME AS TYPE_SUBNAME,
    RPS.TENANT_ID AS ORIGIN_CON_ID
  FROM
    SYS.ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT RPS
    LEFT JOIN SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS ON RPS.ROUTINE_ID = RS.ROUTINE_ID
    LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS ON RS.PACKAGE_ID = PS.PACKAGE_ID
    LEFT JOIN SYS.ALL_VIRTUAL_DATA_TYPE VV ON RPS.PARAM_TYPE = VV.DATA_TYPE
""".replace("\n", " ")
)

def_table_schema(
  table_name='ALL_SOURCE',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25058',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT
      CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,
      CAST(P.PACKAGE_NAME AS VARCHAR2(30)) AS NAME,
      CAST(CASE P.TYPE WHEN 1 THEN 'PACKAGE' WHEN 2 THEN 'PACKAGE BODY' END AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(P.SOURCE) AS TEXT,
      P.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT
        WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) P
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON P.DATABASE_ID = D.DATABASE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON P.OWNER_ID = U.USER_ID
        AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = P.TENANT_ID OR P.TENANT_ID = 1)
      AND D.IN_RECYCLEBIN = 0
      AND (P.DATABASE_ID = USERENV('SCHEMAID')
          OR USER_CAN_ACCESS_OBJ(12, P.PACKAGE_ID, P.DATABASE_ID) = 1)
    UNION ALL
    SELECT
      CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,
      CAST(R.ROUTINE_NAME AS VARCHAR2(30)) AS NAME,
      CAST(CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(R.ROUTINE_BODY) AS TEXT,
      R.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT
        WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) R
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID
        AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1)
      AND D.IN_RECYCLEBIN = 0
      AND R.PACKAGE_ID = -1
      AND (R.DATABASE_ID = USERENV('SCHEMAID')
          OR USER_CAN_ACCESS_OBJ(12, R.ROUTINE_ID, R.DATABASE_ID) = 1)
    UNION ALL
    SELECT
      CAST(U.USER_NAME AS VARCHAR2(30)) AS OWNER,
      CAST(T.TRIGGER_NAME AS VARCHAR2(30)) AS NAME,
      CAST('TRIGGER' AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(T.TRIGGER_BODY) AS TEXT,
      T.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT
        WHERE TENANT_ID = EFFECTIVE_TENANT_ID())T
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON T.DATABASE_ID = D.DATABASE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON T.OWNER_ID = U.USER_ID
        AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = T.TENANT_ID OR T.TENANT_ID = 1)
      AND D.IN_RECYCLEBIN = 0
      AND (T.DATABASE_ID = USERENV('SCHEMAID')
          OR USER_CAN_ACCESS_OBJ(12, T.TRIGGER_ID, T.DATABASE_ID) = 1)
    UNION ALL
    SELECT
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      CAST(PS.PACKAGE_NAME AS VARCHAR2(30)) AS NAME,
      CAST(CASE PS.TYPE WHEN 1 THEN 'PACKAGE' WHEN 2 THEN 'PACKAGE BODY' END AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(PS.SOURCE) AS TEXT,
      PS.TENANT_ID AS ORIGIN_CON_ID
    FROM
      SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT PS
    UNION ALL
    SELECT
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      CAST(RS.ROUTINE_NAME AS VARCHAR2(30)) AS NAME,
      CAST(CASE RS.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(RS.ROUTINE_BODY) AS TEXT,
      RS.TENANT_ID AS ORIGIN_CON_ID
    FROM
      SYS.ALL_VIRTUAL_ROUTINE_SYS_AGENT RS WHERE RS.ROUTINE_TYPE != 3
    UNION ALL
    SELECT
      CAST('SYS' AS VARCHAR2(30)) AS OWNER,
      CAST(TS.TRIGGER_NAME AS VARCHAR2(30)) AS NAME,
      CAST('TRIGGER' AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(TS.TRIGGER_BODY) AS TEXT,
      TS.TENANT_ID AS ORIGIN_CON_ID
    FROM
      SYS.ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT TS
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_PROCEDURES',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25059',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT
      CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_NAME ELSE P.PACKAGE_NAME END AS OBJECT_NAME,
      CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE R.ROUTINE_NAME END AS PROCEDURE_NAME,
      CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,
      CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,
      CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,
      CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS OBJECT_TYPE,
      CAST('NO' AS VARCHAR2(3)) AS AGGREGATE,
      CAST(DECODE(BITAND(R.FLAG, 128), 128, 'YES', 'NO') AS VARCHAR2(3)) AS PIPELINED,
      CAST(NULL AS VARCHAR2(30)) AS IMPLTYPEOWNER,
      CAST(NULL AS VARCHAR2(30)) AS IMPLTYPENAME,
      CAST(DECODE(BITAND(R.FLAG, 8), 8, 'YES', 'NO') AS VARCHAR2(3)) AS PARALLEL,
      CAST('NO' AS VARCHAR2(3)) AS INTERFACE,
      CAST(DECODE(BITAND(R.FLAG, 4), 4, 'YES', 'NO') AS VARCHAR2(3)) AS DETERMINISTIC,
      CAST(DECODE(BITAND(R.FLAG, 16), 16, 'INVOKER', 'DEFINER') AS VARCHAR2(12)) AS AUTHID,
      R.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT
        WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) R
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID
        AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID
        AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1)
      AND D.IN_RECYCLEBIN = 0
      AND R.DATABASE_ID = USERENV('SCHEMAID')
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_ARGUMENTS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25060',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT
      R.ROUTINE_NAME AS OBJECT_NAME,
      CASE R.PACKAGE_ID WHEN -1 THEN NULL ELSE P.PACKAGE_NAME END AS PACKAGE_NAME,
      CASE R.PACKAGE_ID WHEN -1 THEN R.ROUTINE_ID ELSE R.PACKAGE_ID END AS OBJECT_ID,
      CASE R.OVERLOAD WHEN 0 THEN NULL ELSE R.OVERLOAD END AS OVERLOAD,
      CASE R.SUBPROGRAM_ID WHEN 0 THEN 1 ELSE R.SUBPROGRAM_ID END AS SUBPROGRAM_ID,
      RP.PARAM_NAME AS ARGUMENT_NAME,
      RP.PARAM_POSITION AS POSITION,
      RP.SEQUENCE AS SEQUENCE,
      RP.PARAM_LEVEL AS DATA_LEVEL,
      V.DATA_TYPE_STR AS DATA_TYPE,
      'NO' AS DEFAULTED,
      RP.PARAM_LENGTH AS DATA_LENGTH,
      DECODE(BITAND(RP.FLAG, 3), 1, 'IN', 2, 'OUT', 3, 'INOUT', 0, 'OUT') AS IN_OUT,
      RP.PARAM_PRECISION AS DATA_PRECISION,
      RP.PARAM_SCALE AS DATA_SCALE,
      CASE RP.PARAM_CHARSET WHEN 1 THEN 'BINARY' WHEN 2 THEN 'UTF8MB4' ELSE NULL END AS CHARACTER_SET_NAME,
      CASE RP.PARAM_COLL_TYPE WHEN 45 THEN 'UTF8MB4_GENERAL_CI' WHEN 46 THEN 'UTF8MB4_BIN' WHEN 63 THEN 'BINARY' ELSE NULL END AS COLLATION,
      RP.TYPE_OWNER AS TYPE_OWNER,
      RP.TYPE_NAME AS TYPE_NAME,
      RP.TYPE_SUBNAME AS TYPE_SUBNAME,
      RP.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT
        WHERE TENANT_ID = EFFECTIVE_TENANT_ID())RP
      LEFT JOIN SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT R ON RP.ROUTINE_ID = R.ROUTINE_ID
        AND R.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT P ON R.PACKAGE_ID = P.PACKAGE_ID
        AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID
        AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_DATA_TYPE V ON RP.PARAM_TYPE = V.DATA_TYPE
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = RP.TENANT_ID OR RP.TENANT_ID = 1)
      AND D.IN_RECYCLEBIN = 0
      AND R.DATABASE_ID = USERENV('SCHEMAID')
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_SOURCE',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25061',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT
      CAST(P.PACKAGE_NAME AS VARCHAR2(30)) AS NAME,
      CAST(CASE P.TYPE WHEN 1 THEN 'PACKAGE' WHEN 2 THEN 'PACKAGE BODY' END AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(P.SOURCE) AS TEXT,
      P.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT
        WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) P
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON P.DATABASE_ID = D.DATABASE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON P.OWNER_ID = U.USER_ID
        AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = P.TENANT_ID OR P.TENANT_ID = 1) AND D.IN_RECYCLEBIN = 0
    UNION ALL
    SELECT
      CAST(R.ROUTINE_NAME AS VARCHAR2(30)) AS NAME,
      CAST(CASE R.ROUTINE_TYPE WHEN 1 THEN 'PROCEDURE' WHEN 2 THEN 'FUNCTION' WHEN 3 THEN 'PACKAGE' END AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(R.ROUTINE_BODY) AS TEXT,
      R.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT
        WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) R
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON R.DATABASE_ID = D.DATABASE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON R.OWNER_ID = U.USER_ID
        AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = R.TENANT_ID OR R.TENANT_ID = 1)
      AND D.IN_RECYCLEBIN = 0
      AND R.PACKAGE_ID = -1
      AND R.DATABASE_ID = USERENV('SCHEMAID')
    UNION ALL
    SELECT
      CAST(T.TRIGGER_NAME AS VARCHAR2(30)) AS NAME,
      CAST('TRIGGER' AS VARCHAR2(12)) AS TYPE,
      CAST(1 AS NUMBER) AS LINE,
      TO_CLOB(T.TRIGGER_BODY) AS TEXT,
      T.TENANT_ID AS ORIGIN_CON_ID
    FROM
      (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT
        WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) T
      LEFT JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D ON T.DATABASE_ID = D.DATABASE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN SYS.ALL_VIRTUAL_USER_REAL_AGENT U ON T.OWNER_ID = U.USER_ID
        AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
    WHERE
      (SYS_CONTEXT('USERENV', 'CON_ID') = T.TENANT_ID OR T.TENANT_ID = 1)
      AND D.IN_RECYCLEBIN = 0
      AND T.DATABASE_ID = USERENV('SCHEMAID');
""".replace("\n", " ")
)

# start DBA/ALL/USER_PART_KEY_COLUMNS
def_table_schema(
  table_name='DBA_PART_KEY_COLUMNS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25062',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,
            'TABLE' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0
          AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND T.TABLE_TYPE = 5
          AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(-1 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.DATA_TABLE_ID
          AND T.TABLE_TYPE = 5
          AND T.INDEX_TYPE IN (1,2)
          AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='ALL_PART_KEY_COLUMNS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25063',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,
            'TABLE' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0
          AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND (T.DATABASE_ID = USERENV('SCHEMAID')
           OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)
    UNION
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND T.TABLE_TYPE = 5
          AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND (T.DATABASE_ID = USERENV('SCHEMAID')
           OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)
    UNION
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(-1 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.DATA_TABLE_ID
          AND T.TABLE_TYPE = 5
          AND T.INDEX_TYPE IN (1,2)
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0
          AND (T.DATABASE_ID = USERENV('SCHEMAID')
           OR USER_CAN_ACCESS_OBJ(1, T.DATA_TABLE_ID, T.DATABASE_ID) = 1)
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_PART_KEY_COLUMNS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25064',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT  CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,
            'TABLE' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T
    WHERE C.TENANT_ID = T.TENANT_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0
          AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)
          AND T.DATABASE_ID = USERENV('SCHEMAID')
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION
    SELECT  CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 255) AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND T.TABLE_TYPE = 5
          AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0
          AND T.DATABASE_ID = USERENV('SCHEMAID')
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION
    SELECT  CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(-1 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.DATA_TABLE_ID
          AND T.TABLE_TYPE = 5
          AND T.INDEX_TYPE IN (1,2)
          AND BITAND(C.PARTITION_KEY_POSITION, 255) > 0
          AND T.DATABASE_ID = USERENV('SCHEMAID')
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

# end DBA/ALL/USER_PART_KEY_COLUMNS

# start DBA/ALL/USER_SUBPART_KEY_COLUMNS
def_table_schema(
  table_name='DBA_SUBPART_KEY_COLUMNS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25065',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,
            'TABLE' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0
          AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND T.TABLE_TYPE = 5
          AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(-1 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.DATA_TABLE_ID
          AND T.TABLE_TYPE = 5
          AND T.INDEX_TYPE IN (1,2)
          AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='ALL_SUBPART_KEY_COLUMNS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25066',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
     SELECT  D.DATABASE_NAME AS OWNER,
            CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,
            'TABLE' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0
          AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND (T.DATABASE_ID = USERENV('SCHEMAID')
               OR USER_CAN_ACCESS_OBJ(1, T.TABLE_ID, T.DATABASE_ID) = 1)
    UNION
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND T.TABLE_TYPE = 5
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0
          AND (T.DATABASE_ID = USERENV('SCHEMAID')
               OR USER_CAN_ACCESS_OBJ(1, T.DATA_TABLE_ID, T.DATABASE_ID) = 1)
    UNION
    SELECT  D.DATABASE_NAME AS OWNER,
            CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(-1 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.DATA_TABLE_ID
          AND T.TABLE_TYPE = 5
          AND T.INDEX_TYPE IN (1,2)
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0
          AND (T.DATABASE_ID = USERENV('SCHEMAID')
               OR USER_CAN_ACCESS_OBJ(1, T.DATA_TABLE_ID, T.DATABASE_ID) = 1)
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_SUBPART_KEY_COLUMNS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25067',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
    SELECT  CAST(T.TABLE_NAME AS VARCHAR2(128)) AS NAME,
            'TABLE' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T
    WHERE C.TENANT_ID = T.TENANT_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0
          AND T.TABLE_TYPE IN (0, 2, 3, 8, 9, 10, 11)
          AND T.DATABASE_ID = USERENV('SCHEMAID')
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
    UNION
    SELECT  CAST(CASE WHEN D.DATABASE_NAME = '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(BITAND(C.PARTITION_KEY_POSITION, 65280)/256 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.TABLE_ID
          AND T.TABLE_TYPE = 5
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0
          AND T.DATABASE_ID = USERENV('SCHEMAID')
    UNION
    SELECT  CAST(CASE WHEN D.DATABASE_NAME =  '__recyclebin' THEN T.TABLE_NAME
                ELSE SUBSTR(T.TABLE_NAME, 7 + INSTR(SUBSTR(T.TABLE_NAME, 7), '_')) END AS VARCHAR2(128)) AS NAME,
            'INDEX' AS OBJECT_TYPE,
            CAST(C.COLUMN_NAME AS VARCHAR2(4000)) AS COLUMN_NAME,
            CAST(-1 AS NUMBER) AS COLUMN_POSITION,
            CAST(NULL AS NUMBER) AS COLLATED_COLUMN_ID
    FROM SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT D
    WHERE C.TENANT_ID = T.TENANT_ID
          AND T.TENANT_ID = D.TENANT_ID
          AND T.DATABASE_ID = D.DATABASE_ID
          AND C.TABLE_ID = T.DATA_TABLE_ID
          AND T.TABLE_TYPE = 5
          AND T.INDEX_TYPE IN (1,2)
          AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND BITAND(C.PARTITION_KEY_POSITION, 65280) > 0
          AND T.DATABASE_ID = USERENV('SCHEMAID')
""".replace("\n", " ")
)

def_table_schema(
  table_name='DBA_VIEWS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25068',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT CAST('SYS' AS VARCHAR2(128)) OWNER,\
       CAST(TABLE_NAME AS VARCHAR2(128)) VIEW_NAME,\
       CAST(LENGTH(VIEW_DEFINITION) AS NUMBER) TEXT_LENGTH,\
       TO_CLOB(VIEW_DEFINITION) TEXT,\
       CAST(NULL AS NUMBER) OID_TEXT_LENGTH,\
       CAST(NULL AS VARCHAR2(4000)) OID_TEXT,\
       CAST(NULL AS VARCHAR2(30)) VIEW_TYPE,\
       CAST(NULL AS VARCHAR2(30)) SUPERVIEW_NAME,\
       CAST(NULL AS VARCHAR2(1)) EDITIONING_VIEW,\
       CAST(NULL AS VARCHAR2(1)) READ_ONLY\
  FROM SYS.ALL_VIRTUAL_TABLE_SYS_AGENT\
  WHERE BITAND(TABLE_ID,  1099511627775) > 25000\
      AND BITAND(TABLE_ID, 1099511627775) <= 28000\
      AND TABLE_TYPE = 1\
  UNION ALL\
  SELECT CAST(B.DATABASE_NAME AS VARCHAR2(128)) OWNER,\
         CAST(A.TABLE_NAME AS VARCHAR2(128)) VIEW_NAME,\
         CAST(LENGTH(A.VIEW_DEFINITION) AS NUMBER) TEXT_LENGTH,\
         TO_CLOB(VIEW_DEFINITION) TEXT,\
         CAST(NULL AS NUMBER) OID_TEXT_LENGTH,\
         CAST(NULL AS VARCHAR2(4000)) OID_TEXT,\
         CAST(NULL AS VARCHAR2(30)) VIEW_TYPE,\
         CAST(NULL AS VARCHAR2(30)) SUPERVIEW_NAME,\
         CAST(NULL AS VARCHAR2(1)) EDITIONING_VIEW,\
         CAST(NULL AS VARCHAR2(1)) READ_ONLY\
  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A,\
       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B\
  WHERE A.TABLE_TYPE = 4\
        AND A.DATABASE_ID = B.DATABASE_ID\
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()\
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)
def_table_schema(
  table_name='ALL_VIEWS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25069',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT CAST('SYS' AS VARCHAR2(128)) OWNER,\
       CAST(TABLE_NAME AS VARCHAR2(128)) VIEW_NAME,\
       CAST(LENGTH(VIEW_DEFINITION) AS NUMBER) TEXT_LENGTH,\
       TO_CLOB(VIEW_DEFINITION) TEXT,\
       CAST(NULL AS NUMBER) OID_TEXT_LENGTH,\
       CAST(NULL AS VARCHAR2(4000)) OID_TEXT,\
       CAST(NULL AS VARCHAR2(30)) VIEW_TYPE,\
       CAST(NULL AS VARCHAR2(30)) SUPERVIEW_NAME,\
       CAST(NULL AS VARCHAR2(1)) EDITIONING_VIEW,\
       CAST(NULL AS VARCHAR2(1)) READ_ONLY\
  FROM SYS.ALL_VIRTUAL_TABLE_SYS_AGENT\
  WHERE BITAND(TABLE_ID, 1099511627775) > 25000
      AND BITAND(TABLE_ID, 1099511627775) <= 28000
      AND TABLE_TYPE = 1
      AND ((SUBSTR(TABLE_NAME,1,3) = 'DBA' AND USER_CAN_ACCESS_OBJ(1, TABLE_ID, DATABASE_ID) =1)
           OR SUBSTR(TABLE_NAME,1,3) != 'DBA')
  UNION ALL\
  SELECT CAST(B.DATABASE_NAME AS VARCHAR2(128)) OWNER,\
         CAST(A.TABLE_NAME AS VARCHAR2(128)) VIEW_NAME,\
         CAST(LENGTH(A.VIEW_DEFINITION) AS NUMBER) TEXT_LENGTH,\
         TO_CLOB(VIEW_DEFINITION) TEXT,\
         CAST(NULL AS NUMBER) OID_TEXT_LENGTH,\
         CAST(NULL AS VARCHAR2(4000)) OID_TEXT,\
         CAST(NULL AS VARCHAR2(30)) VIEW_TYPE,\
         CAST(NULL AS VARCHAR2(30)) SUPERVIEW_NAME,\
         CAST(NULL AS VARCHAR2(1)) EDITIONING_VIEW,\
         CAST(NULL AS VARCHAR2(1)) READ_ONLY\
  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A,\
       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B\
  WHERE A.TABLE_TYPE = 4
        AND A.DATABASE_ID = B.DATABASE_ID
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND (A.DATABASE_ID = USERENV('SCHEMAID')
             OR USER_CAN_ACCESS_OBJ(1, A.TABLE_ID, A.DATABASE_ID) =1)
""".replace("\n", " ")
)
def_table_schema(
  table_name='USER_VIEWS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25070',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT CAST(A.TABLE_NAME AS VARCHAR2(128)) VIEW_NAME,\
         CAST(LENGTH(A.VIEW_DEFINITION) AS NUMBER) TEXT_LENGTH,\
         TO_CLOB(VIEW_DEFINITION) TEXT,\
         CAST(NULL AS NUMBER) OID_TEXT_LENGTH,\
         CAST(NULL AS VARCHAR2(4000)) OID_TEXT,\
         CAST(NULL AS VARCHAR2(30)) VIEW_TYPE,\
         CAST(NULL AS VARCHAR2(30)) SUPERVIEW_NAME,\
         CAST(NULL AS VARCHAR2(1)) EDITIONING_VIEW,\
         CAST(NULL AS VARCHAR2(1)) READ_ONLY\
  FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A, \
       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B \
  WHERE A.TABLE_TYPE = 4 \
        AND A.DATABASE_ID = B.DATABASE_ID \
        AND B.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')\
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()\
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_TAB_PARTITIONS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25071',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CAST(DB_TB.DATABASE_NAME AS VARCHAR2(128)) TABLE_OWNER,\
      CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,\
      CAST(CASE WHEN\
      PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) COMPOSITE,\
      CAST(PART.PART_NAME AS VARCHAR(128)) PARTITION_NAME,\
      CAST(PART.SUB_PART_NUM AS NUMBER)  SUBPARTITION_COUNT,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,\
      CAST(PART.PART_ID + 1 AS NUMBER) PARTITION_POSITION,\
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,\
      CAST(NULL AS NUMBER) PCT_FREE,\
      CAST(NULL AS NUMBER) PCT_USED,\
      CAST(NULL AS NUMBER) INI_TRANS,\
      CAST(NULL AS NUMBER) MAX_TRANS,\
      CAST(NULL AS NUMBER) INITIAL_EXTENT,\
      CAST(NULL AS NUMBER) NEXT_EXTENT,\
      CAST(NULL AS NUMBER) MIN_EXTENT,\
      CAST(NULL AS NUMBER) MAX_EXTENT,\
      CAST(NULL AS NUMBER) MAX_SIZE,\
      CAST(NULL AS NUMBER) PCT_INCREASE,\
      CAST(NULL AS NUMBER) FREELISTS,\
      CAST(NULL AS NUMBER) FREELIST_GROUPS,\
      CAST(NULL AS VARCHAR2(7)) LOGGING,\
      CAST(CASE WHEN\
      PART.COMPRESS_FUNC_NAME IS NULL THEN\
      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,\
      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,\
      CAST(NULL AS NUMBER) NUM_ROWS,\
      CAST(NULL AS NUMBER) BLOCKS,\
      CAST(NULL AS NUMBER) EMPTY_BLOCKS,\
      CAST(NULL AS NUMBER) AVG_SPACE,\
      CAST(NULL AS NUMBER) CHAIN_CNT,\
      CAST(NULL AS NUMBER) AVG_ROW_LEN,\
      CAST(NULL AS NUMBER) SAMPLE_SIZE,\
      CAST(NULL AS DATE) LAST_ANALYZED,\
      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,\
      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,\
      CAST(NULL AS VARCHAR2(3)) USER_STATS,\
      CAST(NULL AS VARCHAR2(3)) IS_NESTED,\
      CAST(NULL AS VARCHAR2(30)) PARENT_TABLE_PARTITION,\
      CAST(NULL AS VARCHAR2(3)) "INTERVAL",\
      CAST(NULL AS VARCHAR2(4)) SEGMENT_CREATED\
      FROM\
      (SELECT DB.DATABASE_NAME,\
              DB.DATABASE_ID,\
              TB.TABLE_ID,\
              TB.TABLE_NAME\
       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,\
             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB\
       WHERE TB.DATABASE_ID = DB.DATABASE_ID
            AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND (TB.DATABASE_ID = USERENV('SCHEMAID')
                OR USER_CAN_ACCESS_OBJ(1, TB.TABLE_ID, TB.DATABASE_ID) = 1)) DB_TB\
      JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART\
      ON   DB_TB.TABLE_ID = PART.TABLE_ID\
        AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()\
      LEFT JOIN\
        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP\
      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID\
      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_TAB_SUBPARTITIONS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25072',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CAST(DB_TB.DATABASE_NAME AS VARCHAR2(128)) TABLE_OWNER,\
      CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,\
      CAST(PART.PART_NAME AS VARCHAR2(128)) PARTITION_NAME,\
      CAST(PART.SUB_PART_NAME AS VARCHAR2(128))  SUBPARTITION_NAME,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,\
      CAST(PART.SUB_PART_ID + 1 AS NUMBER) SUBPARTITION_POSITION,\
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,\
      CAST(NULL AS NUMBER) PCT_FREE,\
      CAST(NULL AS NUMBER) PCT_USED,\
      CAST(NULL AS NUMBER) INI_TRANS,\
      CAST(NULL AS NUMBER) MAX_TRANS,\
      CAST(NULL AS NUMBER) INITIAL_EXTENT,\
      CAST(NULL AS NUMBER) NEXT_EXTENT,\
      CAST(NULL AS NUMBER) MIN_EXTENT,\
      CAST(NULL AS NUMBER) MAX_EXTENT,\
      CAST(NULL AS NUMBER) MAX_SIZE,\
      CAST(NULL AS NUMBER) PCT_INCREASE,\
      CAST(NULL AS NUMBER) FREELISTS,\
      CAST(NULL AS NUMBER) FREELIST_GROUPS,\
      CAST(NULL AS VARCHAR2(3)) LOGGING,\
      CAST(CASE WHEN\
      PART.COMPRESS_FUNC_NAME IS NULL THEN\
      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,\
      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,\
      CAST(NULL AS NUMBER) NUM_ROWS,\
      CAST(NULL AS NUMBER) BLOCKS,\
      CAST(NULL AS NUMBER) EMPTY_BLOCKS,\
      CAST(NULL AS NUMBER) AVG_SPACE,\
      CAST(NULL AS NUMBER) CHAIN_CNT,\
      CAST(NULL AS NUMBER) AVG_ROW_LEN,\
      CAST(NULL AS NUMBER) SAMPLE_SIZE,\
      CAST(NULL AS DATE) LAST_ANALYZED,\
      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,\
      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,\
      CAST(NULL AS VARCHAR2(3)) USER_STATS,\
      CAST(NULL AS VARCHAR2(3)) "INTERVAL",\
      CAST(NULL AS VARCHAR2(3)) SEGMENT_CREATED\
      FROM\
      (SELECT DB.DATABASE_NAME,\
              DB.DATABASE_ID,\
              TB.TABLE_ID,\
              TB.TABLE_NAME\
       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,\
             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB\
       WHERE TB.DATABASE_ID = DB.DATABASE_ID
            AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND (TB.DATABASE_ID = USERENV('SCHEMAID')
                OR USER_CAN_ACCESS_OBJ(1, TB.TABLE_ID, TB.DATABASE_ID) = 1)) DB_TB\
      JOIN\
      (SELECT P_PART.PART_NAME,\
              P_PART.SUB_PART_NUM,\
              P_PART.TABLE_ID,\
              S_PART.SUB_PART_NAME,\
              S_PART.HIGH_BOUND_VAL,\
              S_PART.LIST_VAL,\
              S_PART.COMPRESS_FUNC_NAME,\
              S_PART.SUB_PART_ID,\
              S_PART.TABLESPACE_ID
       FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT P_PART,\
            SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT S_PART\
       WHERE P_PART.PART_ID = S_PART.PART_ID AND\
             P_PART.TABLE_ID = S_PART.TABLE_ID
             AND P_PART.TENANT_ID = EFFECTIVE_TENANT_ID()\
             AND S_PART.TENANT_ID = EFFECTIVE_TENANT_ID()) PART\
      ON\
      DB_TB.TABLE_ID = PART.TABLE_ID
      LEFT JOIN\
        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP\
      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID
      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()\
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_PART_TABLES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25073',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CAST(DB.DATABASE_NAME AS VARCHAR2(128)) OWNER,\
      CAST(TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,\
      CAST(CASE TB.PART_FUNC_TYPE WHEN 0 THEN\
      'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'\
      WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'\
      WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'\
      WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END\
      AS VARCHAR2(9)) PARTITIONING_TYPE,\
      CAST (DECODE(TB.PART_LEVEL, 1, 'NONE',\
                                  2, DECODE(TB.SUB_PART_FUNC_TYPE, 0, 'HASH',\
                                                                   1, 'KEY',\
                                                                   2, 'KEY',\
                                                                   3, 'RANGE',\
                                                                   4, 'RANGE',\
                                                                   5, 'LIST',\
                                                                   6, 'KEY',\
                                                                   7, 'LIST',\
                                                                   8, 'HASH',\
                                                                   9, 'KEY',\
                                                                   10, 'KEY'))\
           AS VARCHAR2(9)) SUBPARTITIONING_TYPE,\
      CAST(TB.PART_NUM AS NUMBER) PARTITION_COUNT,\
      CAST (DECODE (TB.PART_LEVEL, 1, 0,\
                                   2, TB.SUB_PART_NUM) AS NUMBER) DEF_SUBPARTITION_COUNT,\
      CAST(PART_INFO.PART_KEY_COUNT AS NUMBER) PARTITIONING_KEY_COUNT,\
      CAST (DECODE (TB.PART_LEVEL, 1, 0,\
                                   2, PART_INFO.SUBPART_KEY_COUNT) AS NUMBER) SUBPARTITIONING_KEY_COUNT,\
      CAST(NULL AS VARCHAR2(8)) STATUS,\
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) DEF_TABLESPACE_NAME,\
      CAST(NULL AS NUMBER) DEF_PCT_FREE,\
      CAST(NULL AS NUMBER) DEF_PCT_USED,\
      CAST(NULL AS NUMBER) DEF_INI_TRANS,\
      CAST(NULL AS NUMBER) DEF_MAX_TRANS,\
      CAST(NULL AS VARCHAR2(40)) DEF_INITIAL_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) DEF_NEXT_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) DEF_MIN_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) MAX_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) DEF_MAX_SIZE,\
      CAST(NULL AS VARCHAR2(40)) DEF_PCT_INCREASE,\
      CAST(NULL AS NUMBER) DEF_FREELISTS,\
      CAST(NULL AS NUMBER) DEF_FREELIST_GROUPS,\
      CAST(NULL AS VARCHAR2(7)) DEF_LOGGING,\
      CAST(CASE WHEN\
      TB.COMPRESS_FUNC_NAME IS NULL THEN\
      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,\
      CAST(TB.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,\
      CAST(NULL AS VARCHAR2(7)) DEF_BUFFER_POOL,\
      CAST(NULL AS VARCHAR2(7)) DEF_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(7)) DEF_CELL_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(30)) REF_PTN_CONSTRAINT_NAME,\
      CAST(NULL AS VARCHAR2(1000)) "INTERVAL",\
      CAST(NULL AS VARCHAR2(3)) IS_NESTED,\
      CAST(NULL AS VARCHAR2(4)) DEF_SEGMENT_CREATED,\
      CAST(CASE TB.AUTO_PART WHEN 1 THEN 'AUTO_PART'\
      WHEN 0 THEN '' END AS VARCHAR(10)) AUTO_PART,\
      CAST(TB.AUTO_PART_SIZE AS NUMBER) AUTO_PART_SIZE\
      FROM   SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB\
      JOIN\
        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB\
      ON\
        TB.DATABASE_ID = DB.DATABASE_ID
        AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()\
        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()\
        AND (TB.DATABASE_ID = USERENV('SCHEMAID')
            OR USER_CAN_ACCESS_OBJ(1, TB.TABLE_ID, TB.DATABASE_ID) = 1)\
      JOIN\
        (select table_id,\
                sum(case when BITAND(PARTITION_KEY_POSITION, 255) > 0 then 1 else 0 end) as PART_KEY_COUNT,\
                sum(case when BITAND(PARTITION_KEY_POSITION, 65280) > 0 then 0 else 1 end) as SUBPART_KEY_COUNT\
         from SYS.ALL_VIRTUAL_COLUMN_AGENT\
         where PARTITION_KEY_POSITION > 0\
         group by table_id) PART_INFO\
      ON\
        TB.TABLE_ID = PART_INFO.TABLE_ID\
      LEFT JOIN\
        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP\
      ON   TP.TABLESPACE_ID = TB.TABLESPACE_ID\
      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()\
      WHERE TB.TABLE_TYPE != 5
      AND TB.PART_LEVEL != 0""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_PART_TABLES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25074',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CAST(DB.DATABASE_NAME AS VARCHAR2(128)) OWNER,\
      CAST(TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,\
      CAST(CASE TB.PART_FUNC_TYPE WHEN 0 THEN\
      'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'\
      WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'\
      WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'\
      WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END\
      AS VARCHAR2(9)) PARTITIONING_TYPE,\
      CAST (DECODE(TB.PART_LEVEL, 1, 'NONE',\
                                  2, DECODE(TB.SUB_PART_FUNC_TYPE, 0, 'HASH',\
                                                                   1, 'KEY',\
                                                                   2, 'KEY',\
                                                                   3, 'RANGE',\
                                                                   4, 'RANGE',\
                                                                   5, 'LIST',\
                                                                   6, 'KEY',\
                                                                   7, 'LIST',\
                                                                   8, 'HASH',\
                                                                   9, 'KEY',\
                                                                   10, 'KEY'))\
           AS VARCHAR2(9)) SUBPARTITIONING_TYPE,\
      CAST (TB.PART_NUM AS NUMBER) PARTITION_COUNT,\
      CAST (DECODE (TB.PART_LEVEL, 1, 0,\
                                   2, TB.SUB_PART_NUM) AS NUMBER) DEF_SUBPARTITION_COUNT,\
      CAST(PART_INFO.PART_KEY_COUNT AS NUMBER) PARTITIONING_KEY_COUNT,\
      CAST (DECODE (TB.PART_LEVEL, 1, 0,\
                                   2, PART_INFO.SUBPART_KEY_COUNT) AS NUMBER) SUBPARTITIONING_KEY_COUNT,\
      CAST(NULL AS VARCHAR2(8)) STATUS,\
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) DEF_TABLESPACE_NAME,\
      CAST(NULL AS NUMBER) DEF_PCT_FREE,\
      CAST(NULL AS NUMBER) DEF_PCT_USED,\
      CAST(NULL AS NUMBER) DEF_INI_TRANS,\
      CAST(NULL AS NUMBER) DEF_MAX_TRANS,\
      CAST(NULL AS VARCHAR2(40)) DEF_INITIAL_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) DEF_NEXT_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) DEF_MIN_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) MAX_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) DEF_MAX_SIZE,\
      CAST(NULL AS VARCHAR2(40)) DEF_PCT_INCREASE,\
      CAST(NULL AS NUMBER) DEF_FREELISTS,\
      CAST(NULL AS NUMBER) DEF_FREELIST_GROUPS,\
      CAST(NULL AS VARCHAR2(7)) DEF_LOGGING,\
      CAST(CASE WHEN\
      TB.COMPRESS_FUNC_NAME IS NULL THEN\
      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,\
      CAST(TB.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,\
      CAST(NULL AS VARCHAR2(7)) DEF_BUFFER_POOL,\
      CAST(NULL AS VARCHAR2(7)) DEF_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(7)) DEF_CELL_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(30)) REF_PTN_CONSTRAINT_NAME,\
      CAST(NULL AS VARCHAR2(1000)) "INTERVAL",\
      CAST(NULL AS VARCHAR2(3)) IS_NESTED,\
      CAST(NULL AS VARCHAR2(4)) DEF_SEGMENT_CREATED\
      FROM   SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB\
      JOIN\
        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB\
      ON\
        TB.DATABASE_ID = DB.DATABASE_ID\
        AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()\
        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()\
      JOIN\
        (select table_id,\
                sum(case when BITAND(PARTITION_KEY_POSITION, 255) > 0 then 1 else 0 end) as PART_KEY_COUNT,\
                sum(case when BITAND(PARTITION_KEY_POSITION, 65280) > 0 then 0 else 1 end) as SUBPART_KEY_COUNT\
         from SYS.ALL_VIRTUAL_COLUMN_AGENT\
         where PARTITION_KEY_POSITION > 0\
         group by table_id) PART_INFO\
      ON\
        TB.TABLE_ID = PART_INFO.TABLE_ID\
      LEFT JOIN\
        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP\
      ON   TP.TABLESPACE_ID = TB.TABLESPACE_ID
      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()\
      WHERE TB.TABLE_TYPE != 5
      AND TB.PART_LEVEL != 0""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_PART_TABLES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25075',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CAST(TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,\
      CAST(CASE TB.PART_FUNC_TYPE WHEN 0 THEN\
      'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'\
      WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'\
      WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'\
      WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END\
      AS VARCHAR2(9)) PARTITIONING_TYPE,\
      CAST (DECODE(TB.PART_LEVEL, 1, 'NONE',\
                                  2, DECODE(TB.SUB_PART_FUNC_TYPE, 0, 'HASH',\
                                                                   1, 'KEY',\
                                                                   2, 'KEY',\
                                                                   3, 'RANGE',\
                                                                   4, 'RANGE',\
                                                                   5, 'LIST',\
                                                                   6, 'KEY',\
                                                                   7, 'LIST',\
                                                                   8, 'HASH',\
                                                                   9, 'KEY',\
                                                                   10, 'KEY'))\
           AS VARCHAR2(9)) SUBPARTITIONING_TYPE,\
      CAST(TB.PART_NUM AS NUMBER) PARTITION_COUNT,\
      CAST (DECODE (TB.PART_LEVEL, 1, 0,\
                                   2, TB.SUB_PART_NUM) AS NUMBER) DEF_SUBPARTITION_COUNT,\
      CAST(PART_INFO.PART_KEY_COUNT AS NUMBER) PARTITIONING_KEY_COUNT,\
      CAST (DECODE (TB.PART_LEVEL, 1, 0,\
                                   2, PART_INFO.SUBPART_KEY_COUNT) AS NUMBER) SUBPARTITIONING_KEY_COUNT,\
      CAST(NULL AS VARCHAR2(8)) STATUS,\
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) DEF_TABLESPACE_NAME,\
      CAST(NULL AS NUMBER) DEF_PCT_FREE,\
      CAST(NULL AS NUMBER) DEF_PCT_USED,\
      CAST(NULL AS NUMBER) DEF_INI_TRANS,\
      CAST(NULL AS NUMBER) DEF_MAX_TRANS,\
      CAST(NULL AS VARCHAR2(40)) DEF_INITIAL_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) DEF_NEXT_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) DEF_MIN_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) MAX_EXTENT,\
      CAST(NULL AS VARCHAR2(40)) DEF_MAX_SIZE,\
      CAST(NULL AS VARCHAR2(40)) DEF_PCT_INCREASE,\
      CAST(NULL AS NUMBER) DEF_FREELISTS,\
      CAST(NULL AS NUMBER) DEF_FREELIST_GROUPS,\
      CAST(NULL AS VARCHAR2(7)) DEF_LOGGING,\
      CAST(CASE WHEN\
      TB.COMPRESS_FUNC_NAME IS NULL THEN\
      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,\
      CAST(TB.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,\
      CAST(NULL AS VARCHAR2(7)) DEF_BUFFER_POOL,\
      CAST(NULL AS VARCHAR2(7)) DEF_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(7)) DEF_CELL_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(30)) REF_PTN_CONSTRAINT_NAME,\
      CAST(NULL AS VARCHAR2(1000)) "INTERVAL",\
      CAST(NULL AS VARCHAR2(3)) IS_NESTED,\
      CAST(NULL AS VARCHAR2(4)) DEF_SEGMENT_CREATED\
      FROM   SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB\
      JOIN\
        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB\
      ON\
        TB.DATABASE_ID = DB.DATABASE_ID\
        AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()\
        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()\
        AND DB.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')
      JOIN\
        (select table_id,\
                sum(case when BITAND(PARTITION_KEY_POSITION, 255) > 0 then 1 else 0 end) as PART_KEY_COUNT,\
                sum(case when BITAND(PARTITION_KEY_POSITION, 65280) > 0 then 0 else 1 end) as SUBPART_KEY_COUNT\
         from SYS.ALL_VIRTUAL_COLUMN_AGENT\
         where PARTITION_KEY_POSITION > 0\
         group by table_id) PART_INFO\
      ON\
        TB.TABLE_ID = PART_INFO.TABLE_ID\
      LEFT JOIN\
        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP\
      ON   TP.TABLESPACE_ID = TB.TABLESPACE_ID
      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()\
      WHERE TB.TABLE_TYPE != 5
      AND TB.PART_LEVEL != 0""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_TAB_PARTITIONS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25076',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CAST(DB_TB.DATABASE_NAME AS VARCHAR2(128)) TABLE_OWNER,\
      CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,\
      CAST(CASE WHEN\
      PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) COMPOSITE,\
      CAST(PART.PART_NAME AS VARCHAR(128)) PARTITION_NAME,\
      CAST(PART.SUB_PART_NUM AS NUMBER)  SUBPARTITION_COUNT,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,\
      CAST(PART.PART_ID + 1 AS NUMBER) PARTITION_POSITION,\
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,\
      CAST(NULL AS NUMBER) PCT_FREE,\
      CAST(NULL AS NUMBER) PCT_USED,\
      CAST(NULL AS NUMBER) INI_TRANS,\
      CAST(NULL AS NUMBER) MAX_TRANS,\
      CAST(NULL AS NUMBER) INITIAL_EXTENT,\
      CAST(NULL AS NUMBER) NEXT_EXTENT,\
      CAST(NULL AS NUMBER) MIN_EXTENT,\
      CAST(NULL AS NUMBER) MAX_EXTENT,\
      CAST(NULL AS NUMBER) MAX_SIZE,\
      CAST(NULL AS NUMBER) PCT_INCREASE,\
      CAST(NULL AS NUMBER) FREELISTS,\
      CAST(NULL AS NUMBER) FREELIST_GROUPS,\
      CAST(NULL AS VARCHAR2(7)) LOGGING,\
      CAST(CASE WHEN\
      PART.COMPRESS_FUNC_NAME IS NULL THEN\
      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,\
      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,\
      CAST(NULL AS NUMBER) NUM_ROWS,\
      CAST(NULL AS NUMBER) BLOCKS,\
      CAST(NULL AS NUMBER) EMPTY_BLOCKS,\
      CAST(NULL AS NUMBER) AVG_SPACE,\
      CAST(NULL AS NUMBER) CHAIN_CNT,\
      CAST(NULL AS NUMBER) AVG_ROW_LEN,\
      CAST(NULL AS NUMBER) SAMPLE_SIZE,\
      CAST(NULL AS DATE) LAST_ANALYZED,\
      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,\
      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,\
      CAST(NULL AS VARCHAR2(3)) USER_STATS,\
      CAST(NULL AS VARCHAR2(3)) IS_NESTED,\
      CAST(NULL AS VARCHAR2(30)) PARENT_TABLE_PARTITION,\
      CAST(NULL AS VARCHAR2(3)) "INTERVAL",\
      CAST(NULL AS VARCHAR2(4)) SEGMENT_CREATED\
      FROM\
      (SELECT DB.DATABASE_NAME,\
              DB.DATABASE_ID,\
              TB.TABLE_ID,\
              TB.TABLE_NAME\
       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,\
             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB\
       WHERE TB.DATABASE_ID = DB.DATABASE_ID
        AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()\
        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()) DB_TB\
      JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART\
      ON   DB_TB.TABLE_ID = PART.TABLE_ID\
      AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()
      LEFT JOIN\
        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP\
      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID
      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_TAB_PARTITIONS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25077',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT \
      CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,\
      CAST(CASE WHEN\
      PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) COMPOSITE,\
      CAST(PART.PART_NAME AS VARCHAR(128)) PARTITION_NAME,\
      CAST(PART.SUB_PART_NUM AS NUMBER)  SUBPARTITION_COUNT,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,\
      CAST(PART.PART_ID + 1 AS NUMBER) PARTITION_POSITION,\
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,\
      CAST(NULL AS NUMBER) PCT_FREE,\
      CAST(NULL AS NUMBER) PCT_USED,\
      CAST(NULL AS NUMBER) INI_TRANS,\
      CAST(NULL AS NUMBER) MAX_TRANS,\
      CAST(NULL AS NUMBER) INITIAL_EXTENT,\
      CAST(NULL AS NUMBER) NEXT_EXTENT,\
      CAST(NULL AS NUMBER) MIN_EXTENT,\
      CAST(NULL AS NUMBER) MAX_EXTENT,\
      CAST(NULL AS NUMBER) MAX_SIZE,\
      CAST(NULL AS NUMBER) PCT_INCREASE,\
      CAST(NULL AS NUMBER) FREELISTS,\
      CAST(NULL AS NUMBER) FREELIST_GROUPS,\
      CAST(NULL AS VARCHAR2(7)) LOGGING,\
      CAST(CASE WHEN\
      PART.COMPRESS_FUNC_NAME IS NULL THEN\
      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,\
      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,\
      CAST(NULL AS NUMBER) NUM_ROWS,\
      CAST(NULL AS NUMBER) BLOCKS,\
      CAST(NULL AS NUMBER) EMPTY_BLOCKS,\
      CAST(NULL AS NUMBER) AVG_SPACE,\
      CAST(NULL AS NUMBER) CHAIN_CNT,\
      CAST(NULL AS NUMBER) AVG_ROW_LEN,\
      CAST(NULL AS NUMBER) SAMPLE_SIZE,\
      CAST(NULL AS DATE) LAST_ANALYZED,\
      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,\
      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,\
      CAST(NULL AS VARCHAR2(3)) USER_STATS,\
      CAST(NULL AS VARCHAR2(3)) IS_NESTED,\
      CAST(NULL AS VARCHAR2(30)) PARENT_TABLE_PARTITION,\
      CAST(NULL AS VARCHAR2(3)) "INTERVAL",\
      CAST(NULL AS VARCHAR2(4)) SEGMENT_CREATED\
      FROM\
      (SELECT DB.DATABASE_NAME,\
              DB.DATABASE_ID,\
              TB.TABLE_ID,\
              TB.TABLE_NAME\
       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,\
             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB\
       WHERE TB.DATABASE_ID = DB.DATABASE_ID
        AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()) DB_TB\
      JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART\
      ON   DB_TB.TABLE_ID = PART.TABLE_ID\
          AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()\
          AND DB_TB.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')\
      LEFT JOIN\
        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP\
      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID
      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_TAB_SUBPARTITIONS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25078',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CAST(DB_TB.DATABASE_NAME AS VARCHAR2(128)) TABLE_OWNER,\
      CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,\
      CAST(PART.PART_NAME AS VARCHAR2(128)) PARTITION_NAME,\
      CAST(PART.SUB_PART_NAME AS VARCHAR2(128))  SUBPARTITION_NAME,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,\
      CAST(PART.SUB_PART_ID + 1 AS NUMBER) SUBPARTITION_POSITION,\
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,\
      CAST(NULL AS NUMBER) PCT_FREE,\
      CAST(NULL AS NUMBER) PCT_USED,\
      CAST(NULL AS NUMBER) INI_TRANS,\
      CAST(NULL AS NUMBER) MAX_TRANS,\
      CAST(NULL AS NUMBER) INITIAL_EXTENT,\
      CAST(NULL AS NUMBER) NEXT_EXTENT,\
      CAST(NULL AS NUMBER) MIN_EXTENT,\
      CAST(NULL AS NUMBER) MAX_EXTENT,\
      CAST(NULL AS NUMBER) MAX_SIZE,\
      CAST(NULL AS NUMBER) PCT_INCREASE,\
      CAST(NULL AS NUMBER) FREELISTS,\
      CAST(NULL AS NUMBER) FREELIST_GROUPS,\
      CAST(NULL AS VARCHAR2(3)) LOGGING,\
      CAST(CASE WHEN\
      PART.COMPRESS_FUNC_NAME IS NULL THEN\
      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,\
      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,\
      CAST(NULL AS NUMBER) NUM_ROWS,\
      CAST(NULL AS NUMBER) BLOCKS,\
      CAST(NULL AS NUMBER) EMPTY_BLOCKS,\
      CAST(NULL AS NUMBER) AVG_SPACE,\
      CAST(NULL AS NUMBER) CHAIN_CNT,\
      CAST(NULL AS NUMBER) AVG_ROW_LEN,\
      CAST(NULL AS NUMBER) SAMPLE_SIZE,\
      CAST(NULL AS DATE) LAST_ANALYZED,\
      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,\
      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,\
      CAST(NULL AS VARCHAR2(3)) USER_STATS,\
      CAST(NULL AS VARCHAR2(3)) "INTERVAL",\
      CAST(NULL AS VARCHAR2(3)) SEGMENT_CREATED\
      FROM\
      (SELECT DB.DATABASE_NAME,\
              DB.DATABASE_ID,\
              TB.TABLE_ID,\
              TB.TABLE_NAME\
       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,\
             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB\
       WHERE TB.DATABASE_ID = DB.DATABASE_ID\
        AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()\
        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()) DB_TB\
      JOIN\
      (SELECT P_PART.PART_NAME,\
              P_PART.SUB_PART_NUM,\
              P_PART.TABLE_ID,\
              S_PART.SUB_PART_NAME,\
              S_PART.HIGH_BOUND_VAL,\
              S_PART.LIST_VAL,\
              S_PART.COMPRESS_FUNC_NAME,\
              S_PART.SUB_PART_ID,\
              S_PART.TABLESPACE_ID
       FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT P_PART,\
            SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT S_PART\
       WHERE P_PART.PART_ID = S_PART.PART_ID AND\
             P_PART.TABLE_ID = S_PART.TABLE_ID
             AND P_PART.TENANT_ID = EFFECTIVE_TENANT_ID()
             AND S_PART.TENANT_ID = EFFECTIVE_TENANT_ID()) PART\
      ON\
      DB_TB.TABLE_ID = PART.TABLE_ID\
      LEFT JOIN\
        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP\
      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID\
      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_TAB_SUBPARTITIONS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25079',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT \
      CAST(DB_TB.TABLE_NAME AS VARCHAR2(128)) TABLE_NAME,\
      CAST(PART.PART_NAME AS VARCHAR2(128)) PARTITION_NAME,\
      CAST(PART.SUB_PART_NAME AS VARCHAR2(128))  SUBPARTITION_NAME,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN PART.HIGH_BOUND_VAL ELSE PART.LIST_VAL END AS VARCHAR2(1024)) HIGH_VALUE,\
      CAST(CASE WHEN\
      length(PART.HIGH_BOUND_VAL) > 0 THEN length(PART.HIGH_BOUND_VAL) ELSE length(PART.LIST_VAL) END AS NUMBER) HIGH_VALUE_LENGTH,\
      CAST(PART.SUB_PART_ID + 1 AS NUMBER) SUBPARTITION_POSITION,\
      CAST(TP.TABLESPACE_NAME AS VARCHAR2(30)) TABLESPACE_NAME,\
      CAST(NULL AS NUMBER) PCT_FREE,\
      CAST(NULL AS NUMBER) PCT_USED,\
      CAST(NULL AS NUMBER) INI_TRANS,\
      CAST(NULL AS NUMBER) MAX_TRANS,\
      CAST(NULL AS NUMBER) INITIAL_EXTENT,\
      CAST(NULL AS NUMBER) NEXT_EXTENT,\
      CAST(NULL AS NUMBER) MIN_EXTENT,\
      CAST(NULL AS NUMBER) MAX_EXTENT,\
      CAST(NULL AS NUMBER) MAX_SIZE,\
      CAST(NULL AS NUMBER) PCT_INCREASE,\
      CAST(NULL AS NUMBER) FREELISTS,\
      CAST(NULL AS NUMBER) FREELIST_GROUPS,\
      CAST(NULL AS VARCHAR2(3)) LOGGING,\
      CAST(CASE WHEN\
      PART.COMPRESS_FUNC_NAME IS NULL THEN\
      'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) COMPRESSION,\
      CAST(PART.COMPRESS_FUNC_NAME AS VARCHAR2(12)) COMPRESS_FOR,\
      CAST(NULL AS NUMBER) NUM_ROWS,\
      CAST(NULL AS NUMBER) BLOCKS,\
      CAST(NULL AS NUMBER) EMPTY_BLOCKS,\
      CAST(NULL AS NUMBER) AVG_SPACE,\
      CAST(NULL AS NUMBER) CHAIN_CNT,\
      CAST(NULL AS NUMBER) AVG_ROW_LEN,\
      CAST(NULL AS NUMBER) SAMPLE_SIZE,\
      CAST(NULL AS DATE) LAST_ANALYZED,\
      CAST(NULL AS VARCHAR2(7)) BUFFER_POOL,\
      CAST(NULL AS VARCHAR2(7)) FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(7)) CELL_FLASH_CACHE,\
      CAST(NULL AS VARCHAR2(3)) GLOBAL_STATS,\
      CAST(NULL AS VARCHAR2(3)) USER_STATS,\
      CAST(NULL AS VARCHAR2(3)) "INTERVAL",\
      CAST(NULL AS VARCHAR2(3)) SEGMENT_CREATED\
      FROM\
      (SELECT DB.DATABASE_NAME,\
              DB.DATABASE_ID,\
              TB.TABLE_ID,\
              TB.TABLE_NAME\
       FROM  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB,\
             SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB\
       WHERE TB.DATABASE_ID = DB.DATABASE_ID\
       AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()\
       AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()) DB_TB\
      JOIN\
      (SELECT P_PART.PART_NAME,\
              P_PART.SUB_PART_NUM,\
              P_PART.TABLE_ID,\
              S_PART.SUB_PART_NAME,\
              S_PART.HIGH_BOUND_VAL,\
              S_PART.LIST_VAL,\
              S_PART.COMPRESS_FUNC_NAME,\
              S_PART.SUB_PART_ID,\
              S_PART.TABLESPACE_ID
       FROM SYS.ALL_VIRTUAL_PART_REAL_AGENT P_PART,\
            SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT S_PART\
       WHERE P_PART.PART_ID = S_PART.PART_ID AND\
             P_PART.TABLE_ID = S_PART.TABLE_ID\
             AND P_PART.TENANT_ID = EFFECTIVE_TENANT_ID()\
             AND S_PART.TENANT_ID = EFFECTIVE_TENANT_ID()) PART\
      ON\
      DB_TB.TABLE_ID = PART.TABLE_ID AND\
      DB_TB.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')\
      LEFT JOIN\
        SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP\
      ON   TP.TABLESPACE_ID = PART.TABLESPACE_ID\
      AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_SUBPARTITION_TEMPLATES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25080',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT
      CAST(DB.DATABASE_NAME AS VARCHAR2(128)) USER_NAME,
      TB.TABLE_NAME AS TABLE_NAME,
      SP.SUB_PART_NAME AS SUBPARTITION_NAME,
      SP.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,
      TP.TABLESPACE_NAME AS TABLESPACE_NAME,
      CAST(CASE WHEN SP.HIGH_BOUND_VAL is NULL THEN SP.LIST_VAL ELSE SP.HIGH_BOUND_VAL END AS VARCHAR2(1024)) HIGH_BOUND
      FROM (SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB join SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB
            on DB.database_id = TB.database_id
              AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
              AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()
            join SYS.ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT SP on tb.table_id = sp.table_id
                AND SP.TENANT_ID = EFFECTIVE_TENANT_ID())
            left join SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP
              ON TP.TABLESPACE_ID = SP.TABLESPACE_ID
              AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
      """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_SUBPARTITION_TEMPLATES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25081',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT
      CAST(DB.DATABASE_NAME AS VARCHAR2(128)) USER_NAME,
      TB.TABLE_NAME AS TABLE_NAME,
      SP.SUB_PART_NAME AS SUBPARTITION_NAME,
      SP.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,
      TP.TABLESPACE_NAME AS TABLESPACE_NAME,
      CAST(CASE WHEN SP.HIGH_BOUND_VAL is NULL THEN SP.LIST_VAL ELSE SP.HIGH_BOUND_VAL END AS VARCHAR2(1024)) HIGH_BOUND
      FROM (SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB join SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB
            on DB.database_id = TB.database_id
            AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND (TB.DATABASE_ID = USERENV('SCHEMAID')
                 OR USER_CAN_ACCESS_OBJ(1, TB.TABLE_ID, TB.DATABASE_ID) = 1)
            join SYS.ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT SP on tb.table_id = sp.table_id
                AND SP.TENANT_ID = EFFECTIVE_TENANT_ID())
            left join SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP
              ON TP.TABLESPACE_ID = SP.TABLESPACE_ID
              AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
      """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_SUBPARTITION_TEMPLATES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25082',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT
      TB.TABLE_NAME AS USER_NAME,
      SP.SUB_PART_NAME AS SUBPARTITION_NAME,
      SP.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,
      TP.TABLESPACE_NAME AS TABLESPACE_NAME,
      CAST(CASE WHEN SP.HIGH_BOUND_VAL is NULL THEN SP.LIST_VAL ELSE SP.HIGH_BOUND_VAL END AS VARCHAR2(1024)) HIGH_BOUND
      FROM (SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB join SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB
            on DB.database_id = TB.database_id
              AND TB.TENANT_ID = EFFECTIVE_TENANT_ID()
              AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
            join SYS.ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT SP on tb.table_id = sp.table_id
              AND SP.TENANT_ID = EFFECTIVE_TENANT_ID())
            left join SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP
              ON TP.TABLESPACE_ID = SP.TABLESPACE_ID
                AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
      WHERE DB.database_id = USERENV('SCHEMAID')
      """.replace("\n", " ")
)


def_table_schema(
  table_name      = 'DBA_PART_INDEXES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25083',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition =
    """
with
PARTITIONED_INDEXES as (
SELECT
CASE when I.INDEX_TYPE IN (1, 2) then 1 else 0 end as IS_LOCAL,
DB.DATABASE_NAME AS I_OWNER,
CASE WHEN DB.DATABASE_NAME !=  '__recyclebin'
THEN SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_'))
ELSE I.TABLE_NAME END AS I_NAME,
T.TABLE_NAME AS T_NAME,
I.DATA_TABLE_ID AS T_ID,
I.TABLE_ID AS I_ID,
TP.TABLESPACE_NAME,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN I.DATA_TABLE_ID ELSE I.TABLE_ID END AS PART_INFO_T_ID,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_LEVEL ELSE I.PART_LEVEL END AS I_PART_LEVEL,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_FUNC_TYPE ELSE I.PART_FUNC_TYPE END AS I_PART_FUNC_TYPE,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_FUNC_TYPE ELSE I.SUB_PART_FUNC_TYPE END AS I_SUB_PART_FUNC_TYPE,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_NUM ELSE I.PART_NUM END AS I_PART_NUM,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_NUM ELSE I.SUB_PART_NUM END AS I_SUB_PART_NUM
FROM
SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I
JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB ON I.DATABASE_ID = DB.DATABASE_ID
  AND I.TENANT_ID = EFFECTIVE_TENANT_ID()
  AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T ON I.DATA_TABLE_ID = T.TABLE_ID
  AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
LEFT JOIN
SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP
ON  TP.TABLESPACE_ID = I.TABLESPACE_ID
  AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
WHERE I.TABLE_TYPE = 5 AND
(I.INDEX_TYPE NOT IN (1, 2) and I.PART_LEVEL != 0
 or
 I.INDEX_TYPE IN (1, 2) and T.PART_LEVEL != 0)
),
PART_KEY_COUNT as (
select
PI.I_ID,
SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 255) != 0 THEN 1 ELSE 0 END) AS PARTITIONING_KEY_COUNT,
SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 65280)/256 != 0 THEN 1 ELSE 0 END) AS SUBPARTITIONING_KEY_COUNT
from PARTITIONED_INDEXES PI
join SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
on PI.PART_INFO_T_ID = C.TABLE_ID
AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
group by I_ID
),
LOCAL_INDEX_PREFIXED as
(
select I.TABLE_ID AS I_ID, 1 AS IS_PREFIXED
from SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I
where I.TABLE_TYPE = 5 AND I.INDEX_TYPE in (1,2)
  AND I.TENANT_ID = EFFECTIVE_TENANT_ID()
and not exists
(select * from
(
select *
from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
where C.TABLE_ID = I.DATA_TABLE_ID
AND C.PARTITION_KEY_POSITION != 0
AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
) PART_COLUMNS
left join
(
select *
from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
WHERE C.TABLE_ID = I.TABLE_ID
AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
AND C.INDEX_POSITION != 0
) INDEX_COLUMNS
ON PART_COLUMNS.COLUMN_ID = INDEX_COLUMNS.COLUMN_ID
where
(BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != 0
AND
(INDEX_COLUMNS.INDEX_POSITION is null
 or BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != INDEX_COLUMNS.INDEX_POSITION))
or
(BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 65280)/256 != 0
 AND (INDEX_COLUMNS.INDEX_POSITION is null))
)
)
SELECT
CAST(PI.I_OWNER AS VARCHAR2(128)) AS OWNER,
CAST(PI.I_NAME AS VARCHAR2(128)) AS INDEX_NAME,
CAST(PI.T_NAME AS VARCHAR2(128)) AS TABLE_NAME,

CAST(CASE PI.I_PART_FUNC_TYPE WHEN 0 THEN
'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'
WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'
WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'
WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END
AS VARCHAR2(9)) AS PARTITIONING_TYPE,
CAST(
CASE WHEN PI.I_PART_LEVEL < 2 THEN 'NONE'
ELSE
CASE PI.I_SUB_PART_FUNC_TYPE WHEN 0 THEN 'HASH'
WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'
WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'
WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'
WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END
END
AS VARCHAR2(9)) AS SUBPARTITIONING_TYPE,

CAST(PI.I_PART_NUM AS NUMBER) AS PARTITION_COUNT,
CAST(CASE WHEN PI.I_PART_LEVEL < 2 THEN 0 ELSE PI.I_SUB_PART_NUM END AS NUMBER) AS DEF_SUBPARTITION_COUNT,
CAST(PKC.PARTITIONING_KEY_COUNT AS NUMBER) AS PARTITIONING_KEY_COUNT,
CAST(PKC.SUBPARTITIONING_KEY_COUNT AS NUMBER) AS SUBPARTITIONING_KEY_COUNT,

CAST(CASE WHEN PI.IS_LOCAL = 1 THEN 'LOCAL' ELSE 'GLOBAL' END AS VARCHAR2(6)) AS LOCALITY,

CAST(CASE WHEN (PI.IS_LOCAL = 0 or (PI.IS_LOCAL = 1 and LIP.IS_PREFIXED = 1)) THEN 'PREFIXED'
ELSE 'NON_PREFIXED' END AS VARCHAR2(12)) AS ALIGNMENT,

CAST(PI.TABLESPACE_NAME AS VARCHAR2(30)) AS DEF_TABLESPACE_NAME,
CAST(0 AS NUMBER) AS DEF_PCT_FREE,
CAST(0 AS NUMBER) AS DEF_INI_TRANS,
CAST(0 AS NUMBER) AS DEF_MAX_TRANS,
CAST(NULL AS VARCHAR2(40)) AS DEF_INITIAL_EXTENT,
CAST(NULL AS VARCHAR2(40)) AS DEF_NEXT_EXTENT,
CAST(NULL AS VARCHAR2(40)) AS DEF_MIN_EXTENTS,
CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_EXTENTS,
CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_SIZE,
CAST(NULL AS VARCHAR2(40)) AS DEF_PCT_INCREASE,
CAST(0 AS NUMBER) AS DEF_FREELISTS,
CAST(0 AS NUMBER) AS DEF_FREELIST_GROUPS,
CAST(NULL AS VARCHAR2(7)) AS DEF_LOGGING,
CAST(NULL AS VARCHAR2(7)) AS DEF_BUFFER_POOL,
CAST(NULL AS VARCHAR2(7)) AS DEF_FLASH_CACHE,
CAST(NULL AS VARCHAR2(7)) AS DEF_CELL_FLASH_CACHE,
CAST(NULL AS VARCHAR2(1000)) AS DEF_PARAMETERS,
CAST(NULL AS VARCHAR2(1000)) AS INTERVAL

from PARTITIONED_INDEXES PI
join PART_KEY_COUNT PKC on PI.I_ID = PKC.I_ID
left join LOCAL_INDEX_PREFIXED LIP on PI.I_ID = LIP.I_ID
    """
 .replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_PART_INDEXES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25084',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition =
    """
with
PARTITIONED_INDEXES as (
SELECT
CASE when I.INDEX_TYPE IN (1, 2) then 1 else 0 end as IS_LOCAL,
DB.DATABASE_NAME AS I_OWNER,
CASE WHEN DB.DATABASE_NAME !=  '__recyclebin'
THEN SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_'))
ELSE I.TABLE_NAME END AS I_NAME,
T.TABLE_NAME AS T_NAME,
I.DATA_TABLE_ID AS T_ID,
I.TABLE_ID AS I_ID,
TP.TABLESPACE_NAME,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN I.DATA_TABLE_ID ELSE I.TABLE_ID END AS PART_INFO_T_ID,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_LEVEL ELSE I.PART_LEVEL END AS I_PART_LEVEL,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_FUNC_TYPE ELSE I.PART_FUNC_TYPE END AS I_PART_FUNC_TYPE,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_FUNC_TYPE ELSE I.SUB_PART_FUNC_TYPE END AS I_SUB_PART_FUNC_TYPE,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_NUM ELSE I.PART_NUM END AS I_PART_NUM,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_NUM ELSE I.SUB_PART_NUM END AS I_SUB_PART_NUM
FROM
SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I
JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB ON I.DATABASE_ID = DB.DATABASE_ID
  AND I.TENANT_ID = EFFECTIVE_TENANT_ID()
  AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T ON I.DATA_TABLE_ID = T.TABLE_ID
                                     AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
                                     AND (I.DATABASE_ID = USERENV('SCHEMAID')
                                          OR USER_CAN_ACCESS_OBJ(1, I.DATA_TABLE_ID, 1) = 1)
LEFT JOIN
SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP
ON  TP.TABLESPACE_ID = I.TABLESPACE_ID
AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
WHERE I.TABLE_TYPE = 5 AND
(I.INDEX_TYPE NOT IN (1, 2) and I.PART_LEVEL != 0
 or
 I.INDEX_TYPE IN (1, 2) and T.PART_LEVEL != 0)
),
PART_KEY_COUNT as (
select
PI.I_ID,
SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 255) != 0 THEN 1 ELSE 0 END) AS PARTITIONING_KEY_COUNT,
SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 65280)/256 != 0 THEN 1 ELSE 0 END) AS SUBPARTITIONING_KEY_COUNT
from PARTITIONED_INDEXES PI
join SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
on PI.PART_INFO_T_ID = C.TABLE_ID
AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
group by I_ID
),
LOCAL_INDEX_PREFIXED as
(
select I.TABLE_ID AS I_ID, 1 AS IS_PREFIXED
from SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I
where I.TABLE_TYPE = 5 AND I.INDEX_TYPE in (1,2)
AND I.TENANT_ID = EFFECTIVE_TENANT_ID()
AND (I.DATABASE_ID = USERENV('SCHEMAID')
    OR USER_CAN_ACCESS_OBJ(1, I.DATA_TABLE_ID, I.DATABASE_ID) = 1)
and not exists
(select * from
(
select *
from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
where C.TABLE_ID = I.DATA_TABLE_ID
AND C.PARTITION_KEY_POSITION != 0
AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
) PART_COLUMNS
left join
(
select *
from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
WHERE C.TABLE_ID = I.TABLE_ID
AND C.INDEX_POSITION != 0
AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
) INDEX_COLUMNS
ON PART_COLUMNS.COLUMN_ID = INDEX_COLUMNS.COLUMN_ID
where
(BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != 0
AND
(INDEX_COLUMNS.INDEX_POSITION is null
 or BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != INDEX_COLUMNS.INDEX_POSITION))
or
(BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 65280)/256 != 0
 AND (INDEX_COLUMNS.INDEX_POSITION is null))
)
)
SELECT
CAST(PI.I_OWNER AS VARCHAR2(128)) AS OWNER,
CAST(PI.I_NAME AS VARCHAR2(128)) AS INDEX_NAME,
CAST(PI.T_NAME AS VARCHAR2(128)) AS TABLE_NAME,

CAST(CASE PI.I_PART_FUNC_TYPE WHEN 0 THEN
'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'
WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'
WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'
WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END
AS VARCHAR2(9)) AS PARTITIONING_TYPE,

CAST(
CASE WHEN PI.I_PART_LEVEL < 2 THEN 'NONE'
ELSE
CASE PI.I_SUB_PART_FUNC_TYPE WHEN 0 THEN 'HASH'
WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'
WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'
WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'
WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END
END
AS VARCHAR2(9)) AS SUBPARTITIONING_TYPE,

CAST(PI.I_PART_NUM AS NUMBER) AS PARTITION_COUNT,
CAST(CASE WHEN PI.I_PART_LEVEL < 2 THEN 0 ELSE PI.I_SUB_PART_NUM END AS NUMBER) AS DEF_SUBPARTITION_COUNT,
CAST(PKC.PARTITIONING_KEY_COUNT AS NUMBER) AS PARTITIONING_KEY_COUNT,
CAST(PKC.SUBPARTITIONING_KEY_COUNT AS NUMBER) AS SUBPARTITIONING_KEY_COUNT,

CAST(CASE WHEN PI.IS_LOCAL = 1 THEN 'LOCAL' ELSE 'GLOBAL' END AS VARCHAR2(6)) AS LOCALITY,

CAST(CASE WHEN (PI.IS_LOCAL = 0 or (PI.IS_LOCAL = 1 and LIP.IS_PREFIXED = 1)) THEN 'PREFIXED'
ELSE 'NON_PREFIXED' END AS VARCHAR2(12)) AS ALIGNMENT,

CAST(PI.TABLESPACE_NAME AS VARCHAR2(30)) AS DEF_TABLESPACE_NAME,
CAST(0 AS NUMBER) AS DEF_PCT_FREE,
CAST(0 AS NUMBER) AS DEF_INI_TRANS,
CAST(0 AS NUMBER) AS DEF_MAX_TRANS,
CAST(NULL AS VARCHAR2(40)) AS DEF_INITIAL_EXTENT,
CAST(NULL AS VARCHAR2(40)) AS DEF_NEXT_EXTENT,
CAST(NULL AS VARCHAR2(40)) AS DEF_MIN_EXTENTS,
CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_EXTENTS,
CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_SIZE,
CAST(NULL AS VARCHAR2(40)) AS DEF_PCT_INCREASE,
CAST(0 AS NUMBER) AS DEF_FREELISTS,
CAST(0 AS NUMBER) AS DEF_FREELIST_GROUPS,
CAST(NULL AS VARCHAR2(7)) AS DEF_LOGGING,
CAST(NULL AS VARCHAR2(7)) AS DEF_BUFFER_POOL,
CAST(NULL AS VARCHAR2(7)) AS DEF_FLASH_CACHE,
CAST(NULL AS VARCHAR2(7)) AS DEF_CELL_FLASH_CACHE,
CAST(NULL AS VARCHAR2(1000)) AS DEF_PARAMETERS,
CAST(NULL AS VARCHAR2(1000)) AS INTERVAL

from PARTITIONED_INDEXES PI
join PART_KEY_COUNT PKC on PI.I_ID = PKC.I_ID
left join LOCAL_INDEX_PREFIXED LIP on PI.I_ID = LIP.I_ID
    """
 .replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_PART_INDEXES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25085',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition =
    """
with
PARTITIONED_INDEXES as (
SELECT
CASE when I.INDEX_TYPE IN (1, 2) then 1 else 0 end as IS_LOCAL,
DB.DATABASE_NAME AS I_OWNER,
CASE WHEN DB.DATABASE_NAME !=  '__recyclebin'
THEN SUBSTR(I.TABLE_NAME, 7 + INSTR(SUBSTR(I.TABLE_NAME, 7), '_'))
ELSE I.TABLE_NAME END AS I_NAME,
T.TABLE_NAME AS T_NAME,
I.DATA_TABLE_ID AS T_ID,
I.TABLE_ID AS I_ID,
TP.TABLESPACE_NAME,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN I.DATA_TABLE_ID ELSE I.TABLE_ID END AS PART_INFO_T_ID,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_LEVEL ELSE I.PART_LEVEL END AS I_PART_LEVEL,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_FUNC_TYPE ELSE I.PART_FUNC_TYPE END AS I_PART_FUNC_TYPE,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_FUNC_TYPE ELSE I.SUB_PART_FUNC_TYPE END AS I_SUB_PART_FUNC_TYPE,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.PART_NUM ELSE I.PART_NUM END AS I_PART_NUM,
CASE WHEN I.INDEX_TYPE IN (1, 2) THEN T.SUB_PART_NUM ELSE I.SUB_PART_NUM END AS I_SUB_PART_NUM
FROM
SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I
JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB ON I.DATABASE_ID = DB.DATABASE_ID
  AND I.TENANT_ID = EFFECTIVE_TENANT_ID()
  AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
AND I.DATABASE_ID = USERENV('SCHEMAID')
JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T ON I.DATA_TABLE_ID = T.TABLE_ID
  AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
LEFT JOIN
SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT TP
ON  TP.TABLESPACE_ID = I.TABLESPACE_ID
AND TP.TENANT_ID = EFFECTIVE_TENANT_ID()
WHERE I.TABLE_TYPE = 5 AND
(I.INDEX_TYPE NOT IN (1, 2) and I.PART_LEVEL != 0
 or
 I.INDEX_TYPE IN (1, 2) and T.PART_LEVEL != 0)
),
PART_KEY_COUNT as (
select
PI.I_ID,
SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 255) != 0 THEN 1 ELSE 0 END) AS PARTITIONING_KEY_COUNT,
SUM(CASE WHEN BITAND(C.PARTITION_KEY_POSITION, 65280)/256 != 0 THEN 1 ELSE 0 END) AS SUBPARTITIONING_KEY_COUNT
from PARTITIONED_INDEXES PI
join SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
on PI.PART_INFO_T_ID = C.TABLE_ID
AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
group by I_ID
),
LOCAL_INDEX_PREFIXED as
(
select I.TABLE_ID AS I_ID, 1 AS IS_PREFIXED
from SYS.ALL_VIRTUAL_TABLE_REAL_AGENT I
where I.TABLE_TYPE = 5 AND I.INDEX_TYPE in (1,2)
AND I.TENANT_ID = EFFECTIVE_TENANT_ID()
and not exists
(select * from
(
select *
from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
where C.TABLE_ID = I.DATA_TABLE_ID
AND C.PARTITION_KEY_POSITION != 0
AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
) PART_COLUMNS
left join
(
select *
from SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
WHERE C.TABLE_ID = I.TABLE_ID
AND C.INDEX_POSITION != 0
AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
) INDEX_COLUMNS
ON PART_COLUMNS.COLUMN_ID = INDEX_COLUMNS.COLUMN_ID
where
(BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != 0
AND
(INDEX_COLUMNS.INDEX_POSITION is null
 or BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 255) != INDEX_COLUMNS.INDEX_POSITION))
or
(BITAND(PART_COLUMNS.PARTITION_KEY_POSITION, 65280)/256 != 0
 AND (INDEX_COLUMNS.INDEX_POSITION is null))
)
)
SELECT
CAST(PI.I_NAME AS VARCHAR2(128)) AS INDEX_NAME,
CAST(PI.T_NAME AS VARCHAR2(128)) AS TABLE_NAME,

CAST(CASE PI.I_PART_FUNC_TYPE WHEN 0 THEN
'HASH' WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'
WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'
WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'
WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END
AS VARCHAR2(9)) AS PARTITIONING_TYPE,

CAST(
CASE WHEN PI.I_PART_LEVEL < 2 THEN 'NONE'
ELSE
CASE PI.I_SUB_PART_FUNC_TYPE WHEN 0 THEN 'HASH'
WHEN 1 THEN 'KEY' WHEN 2 THEN 'KEY'
WHEN 3 THEN 'RANGE' WHEN 4 THEN 'RANGE'
WHEN 5 THEN 'LIST' WHEN 6 THEN 'KEY' WHEN 7 THEN 'LIST'
WHEN 8 THEN 'HASH' WHEN 9 THEN 'KEY' WHEN 10 THEN 'KEY' END
END
AS VARCHAR2(9)) AS SUBPARTITIONING_TYPE,

CAST(PI.I_PART_NUM AS NUMBER) AS PARTITION_COUNT,
CAST(CASE WHEN PI.I_PART_LEVEL < 2 THEN 0 ELSE PI.I_SUB_PART_NUM END AS NUMBER) AS DEF_SUBPARTITION_COUNT,
CAST(PKC.PARTITIONING_KEY_COUNT AS NUMBER) AS PARTITIONING_KEY_COUNT,
CAST(PKC.SUBPARTITIONING_KEY_COUNT AS NUMBER) AS SUBPARTITIONING_KEY_COUNT,

CAST(CASE WHEN PI.IS_LOCAL = 1 THEN 'LOCAL' ELSE 'GLOBAL' END AS VARCHAR2(6)) AS LOCALITY,

CAST(CASE WHEN (PI.IS_LOCAL = 0 or (PI.IS_LOCAL = 1 and LIP.IS_PREFIXED = 1)) THEN 'PREFIXED'
ELSE 'NON_PREFIXED' END AS VARCHAR2(12)) AS ALIGNMENT,

CAST(PI.TABLESPACE_NAME AS VARCHAR2(30)) AS DEF_TABLESPACE_NAME,
CAST(0 AS NUMBER) AS DEF_PCT_FREE,
CAST(0 AS NUMBER) AS DEF_INI_TRANS,
CAST(0 AS NUMBER) AS DEF_MAX_TRANS,
CAST(NULL AS VARCHAR2(40)) AS DEF_INITIAL_EXTENT,
CAST(NULL AS VARCHAR2(40)) AS DEF_NEXT_EXTENT,
CAST(NULL AS VARCHAR2(40)) AS DEF_MIN_EXTENTS,
CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_EXTENTS,
CAST(NULL AS VARCHAR2(40)) AS DEF_MAX_SIZE,
CAST(NULL AS VARCHAR2(40)) AS DEF_PCT_INCREASE,
CAST(0 AS NUMBER) AS DEF_FREELISTS,
CAST(0 AS NUMBER) AS DEF_FREELIST_GROUPS,
CAST(NULL AS VARCHAR2(7)) AS DEF_LOGGING,
CAST(NULL AS VARCHAR2(7)) AS DEF_BUFFER_POOL,
CAST(NULL AS VARCHAR2(7)) AS DEF_FLASH_CACHE,
CAST(NULL AS VARCHAR2(7)) AS DEF_CELL_FLASH_CACHE,
CAST(NULL AS VARCHAR2(1000)) AS DEF_PARAMETERS,
CAST(NULL AS VARCHAR2(1000)) AS INTERVAL

from PARTITIONED_INDEXES PI
join PART_KEY_COUNT PKC on PI.I_ID = PKC.I_ID
left join LOCAL_INDEX_PREFIXED LIP on PI.I_ID = LIP.I_ID
    """
 .replace("\n", " ")
)

def_table_schema(
  table_name      = "ALL_ALL_TABLES",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25086',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(db.database_name AS VARCHAR2(128)) AS OWNER,
  CAST(t.table_name AS VARCHAR2(128)) AS TABLE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS CLUSTER_NAME,
  CAST(NULL AS VARCHAR2(30)) AS IOT_NAME,
  CAST('VALID' AS VARCHAR2(8)) AS STATUS,
  CAST(t."PCTFREE" AS NUMBER) AS PCT_FREE,
  CAST(NULL AS NUMBER) AS PCT_USED,
  CAST(NULL AS NUMBER) AS INI_TRANS,
  CAST(NULL AS NUMBER) AS MAX_TRANS,
  CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
  CAST(NULL AS NUMBER) AS NEXT_EXTENT,
  CAST(NULL AS NUMBER) AS MIN_EXTENTS,
  CAST(NULL AS NUMBER) AS MAX_EXTENTS,
  CAST(NULL AS NUMBER) AS PCT_INCREASE,
  CAST(NULL AS NUMBER) AS FREELISTS,
  CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
  CAST(NULL AS VARCHAR2(3)) AS LOGGING,
  CAST(NULL AS VARCHAR2(1)) AS BACKED_UP,
  CAST(info.row_count AS NUMBER) AS NUM_ROWS,
  CAST(NULL AS NUMBER) AS BLOCKS,
  CAST(NULL AS NUMBER) AS EMPTY_BLOCKS,
  CAST(NULL AS NUMBER) AS AVG_SPACE,
  CAST(NULL AS NUMBER) AS CHAIN_CNT,
  CAST(NULL AS NUMBER) AS AVG_ROW_LEN,
  CAST(NULL AS NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,
  CAST(NULL AS NUMBER) AS NUM_FREELIST_BLOCKS,
  CAST(NULL AS VARCHAR2(40)) AS DEGREE,
  CAST(NULL AS VARCHAR2(40)) AS INSTANCES,
  CAST(NULL AS VARCHAR2(20)) AS CACHE,
  CAST(NULL AS VARCHAR2(8)) AS TABLE_LOCK,
  CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
  CAST(NULL AS DATE) AS LAST_ANALYZED,
  CAST(
  CASE
    WHEN
      t.part_level = 0
    THEN
      'NO'
    ELSE
      'YES'
  END
  AS VARCHAR2(3)) AS PARTITIONED,
  CAST(NULL AS VARCHAR2(12)) AS IOT_TYPE,
  CAST(NULL AS VARCHAR2(16)) AS OBJECT_ID_TYPE,
  CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE_OWNER,
  CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE,
  CAST(decode (t.table_type, 8, 'YES', 9, 'YES', 'NO') AS VARCHAR2(1)) AS TEMPORARY,
  CAST(NULL AS VARCHAR2(1)) AS SECONDARY,
  CAST(NULL AS VARCHAR2(3)) AS NESTED,
  CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
  CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
  CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
  CAST(NULL AS VARCHAR2(8)) AS ROW_MOVEMENT,
  CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
  CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
  CAST( decode (t.table_type, 8, 'SYS$SESSION', 9, 'SYS$TRANSACTION', NULL) AS VARCHAR2(15)) AS DURATION,
  CAST(NULL AS VARCHAR2(8)) AS SKIP_CORRUPT,
  CAST(NULL AS VARCHAR2(3)) AS MONITORING,
  CAST(NULL AS VARCHAR2(30)) AS CLUSTER_OWNER,
  CAST(NULL AS VARCHAR2(8)) AS DEPENDENCIES,
  CAST(NULL AS VARCHAR2(8)) AS COMPRESSION,
  CAST(NULL AS VARCHAR2(12)) AS COMPRESS_FOR,
  CAST(
  CASE
    WHEN
      db.database_name =  '__recyclebin'
    THEN 'YES'
    ELSE
      'NO'
  END
  AS VARCHAR2(3)) AS DROPPED,
  CAST(NULL AS VARCHAR2(3)) AS READ_ONLY,
  CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,
  CAST(NULL AS VARCHAR2(7)) AS RESULT_CACHE
FROM
  (
    SELECT
      tenant_id,
      table_id,
      SUM(row_count) AS row_count
    FROM
      sys.ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT p
    WHERE
      p.role = 1
      AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    GROUP BY
      tenant_id,
      table_id
  )
  info
  RIGHT JOIN
    sys.ALL_VIRTUAL_TABLE_REAL_AGENT t
    ON t.tenant_id = info.tenant_id
    AND t.table_id = info.table_id
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID(),
    sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db
 WHERE
    db.tenant_id = t.tenant_id
    AND db.database_id = t.database_id
    AND db.database_name != '__recyclebin'
    AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND t.table_type != 5
    AND (t.database_id = USERENV('SCHEMAID')
         or user_can_access_obj(1, t.table_id, t.database_id) =1
    )
""".replace("\n", " ")

)
def_table_schema(
  table_name      = "DBA_ALL_TABLES",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25087',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(db.database_name AS VARCHAR2(128)) AS OWNER,
  CAST(t.table_name AS VARCHAR2(128)) AS TABLE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS CLUSTER_NAME,
  CAST(NULL AS VARCHAR2(30)) AS IOT_NAME,
  CAST('VALID' AS VARCHAR2(8)) AS STATUS,
  CAST(t."PCTFREE" AS NUMBER) AS PCT_FREE,
  CAST(NULL AS NUMBER) AS PCT_USED,
  CAST(NULL AS NUMBER) AS INI_TRANS,
  CAST(NULL AS NUMBER) AS MAX_TRANS,
  CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
  CAST(NULL AS NUMBER) AS NEXT_EXTENT,
  CAST(NULL AS NUMBER) AS MIN_EXTENTS,
  CAST(NULL AS NUMBER) AS MAX_EXTENTS,
  CAST(NULL AS NUMBER) AS PCT_INCREASE,
  CAST(NULL AS NUMBER) AS FREELISTS,
  CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
  CAST(NULL AS VARCHAR2(3)) AS LOGGING,
  CAST(NULL AS VARCHAR2(1)) AS BACKED_UP,
  CAST(info.row_count AS NUMBER) AS NUM_ROWS,
  CAST(NULL AS NUMBER) AS BLOCKS,
  CAST(NULL AS NUMBER) AS EMPTY_BLOCKS,
  CAST(NULL AS NUMBER) AS AVG_SPACE,
  CAST(NULL AS NUMBER) AS CHAIN_CNT,
  CAST(NULL AS NUMBER) AS AVG_ROW_LEN,
  CAST(NULL AS NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,
  CAST(NULL AS NUMBER) AS NUM_FREELIST_BLOCKS,
  CAST(NULL AS VARCHAR2(40)) AS DEGREE,
  CAST(NULL AS VARCHAR2(40)) AS INSTANCES,
  CAST(NULL AS VARCHAR2(20)) AS CACHE,
  CAST(NULL AS VARCHAR2(8)) AS TABLE_LOCK,
  CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
  CAST(NULL AS DATE) AS LAST_ANALYZED,
  CAST(
  CASE
    WHEN
      t.part_level = 0
    THEN
      'NO'
    ELSE
      'YES'
  END
  AS VARCHAR2(3)) AS PARTITIONED,
  CAST(NULL AS VARCHAR2(12)) AS IOT_TYPE,
  CAST(NULL AS VARCHAR2(16)) AS OBJECT_ID_TYPE,
  CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE_OWNER,
  CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE,
  CAST(decode (t.table_type, 8, 'YES', 9, 'YES', 'NO') AS VARCHAR2(1)) AS TEMPORARY,
  CAST(NULL AS VARCHAR2(1)) AS SECONDARY,
  CAST(NULL AS VARCHAR2(3)) AS NESTED,
  CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
  CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
  CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
  CAST(NULL AS VARCHAR2(8)) AS ROW_MOVEMENT,
  CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
  CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
  CAST( decode (t.table_type, 8, 'SYS$SESSION', 9, 'SYS$TRANSACTION', NULL) AS VARCHAR2(15)) AS DURATION,
  CAST(NULL AS VARCHAR2(8)) AS SKIP_CORRUPT,
  CAST(NULL AS VARCHAR2(3)) AS MONITORING,
  CAST(NULL AS VARCHAR2(30)) AS CLUSTER_OWNER,
  CAST(NULL AS VARCHAR2(8)) AS DEPENDENCIES,
  CAST(NULL AS VARCHAR2(8)) AS COMPRESSION,
  CAST(NULL AS VARCHAR2(12)) AS COMPRESS_FOR,
  CAST(
  CASE
    WHEN
      db.database_name =  '__recyclebin'
    THEN 'YES'
    ELSE
      'NO'
  END
  AS VARCHAR2(3)) AS DROPPED,
  CAST(NULL AS VARCHAR2(3)) AS READ_ONLY,
  CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,
  CAST(NULL AS VARCHAR2(7)) AS RESULT_CACHE
FROM
  (
    SELECT
      tenant_id,
      table_id,
      SUM(row_count) AS row_count
    FROM
      sys.ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT p
    WHERE
      p.role = 1
      AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    GROUP BY
      tenant_id,
      table_id
  )
  info
  RIGHT JOIN
    sys.ALL_VIRTUAL_TABLE_REAL_AGENT t
    ON t.tenant_id = info.tenant_id
    AND t.table_id = info.table_id
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID(),
    sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db
 WHERE
    db.tenant_id = t.tenant_id
    AND db.database_id = t.database_id
    AND db.database_name != '__recyclebin'
    AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND t.table_type != 5
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "USER_ALL_TABLES",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25088',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(t.table_name AS VARCHAR2(128)) AS TABLE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS CLUSTER_NAME,
  CAST(NULL AS VARCHAR2(30)) AS IOT_NAME,
  CAST('VALID' AS VARCHAR2(8)) AS STATUS,
  CAST(t."PCTFREE" AS NUMBER) AS PCT_FREE,
  CAST(NULL AS NUMBER) AS PCT_USED,
  CAST(NULL AS NUMBER) AS INI_TRANS,
  CAST(NULL AS NUMBER) AS MAX_TRANS,
  CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
  CAST(NULL AS NUMBER) AS NEXT_EXTENT,
  CAST(NULL AS NUMBER) AS MIN_EXTENTS,
  CAST(NULL AS NUMBER) AS MAX_EXTENTS,
  CAST(NULL AS NUMBER) AS PCT_INCREASE,
  CAST(NULL AS NUMBER) AS FREELISTS,
  CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
  CAST(NULL AS VARCHAR2(3)) AS LOGGING,
  CAST(NULL AS VARCHAR2(1)) AS BACKED_UP,
  CAST(info.row_count AS NUMBER) AS NUM_ROWS,
  CAST(NULL AS NUMBER) AS BLOCKS,
  CAST(NULL AS NUMBER) AS EMPTY_BLOCKS,
  CAST(NULL AS NUMBER) AS AVG_SPACE,
  CAST(NULL AS NUMBER) AS CHAIN_CNT,
  CAST(NULL AS NUMBER) AS AVG_ROW_LEN,
  CAST(NULL AS NUMBER) AS AVG_SPACE_FREELIST_BLOCKS,
  CAST(NULL AS NUMBER) AS NUM_FREELIST_BLOCKS,
  CAST(NULL AS VARCHAR2(40)) AS DEGREE,
  CAST(NULL AS VARCHAR2(40)) AS INSTANCES,
  CAST(NULL AS VARCHAR2(20)) AS CACHE,
  CAST(NULL AS VARCHAR2(8)) AS TABLE_LOCK,
  CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
  CAST(NULL AS DATE) AS LAST_ANALYZED,
  CAST(
  CASE
    WHEN
      t.part_level = 0
    THEN
      'NO'
    ELSE
      'YES'
  END
  AS VARCHAR2(3)) AS PARTITIONED,
  CAST(NULL AS VARCHAR2(12)) AS IOT_TYPE,
  CAST(NULL AS VARCHAR2(16)) AS OBJECT_ID_TYPE,
  CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE_OWNER,
  CAST(NULL AS VARCHAR2(128)) AS TABLE_TYPE,
  CAST(decode (t.table_type, 8, 'YES', 9, 'YES', 'NO') AS VARCHAR2(1)) AS TEMPORARY,
  CAST(NULL AS VARCHAR2(1)) AS SECONDARY,
  CAST(NULL AS VARCHAR2(3)) AS NESTED,
  CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
  CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
  CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
  CAST(NULL AS VARCHAR2(8)) AS ROW_MOVEMENT,
  CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
  CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
  CAST( decode (t.table_type, 8, 'SYS$SESSION', 9, 'SYS$TRANSACTION', NULL) AS VARCHAR2(15)) AS DURATION,
  CAST(NULL AS VARCHAR2(8)) AS SKIP_CORRUPT,
  CAST(NULL AS VARCHAR2(3)) AS MONITORING,
  CAST(NULL AS VARCHAR2(30)) AS CLUSTER_OWNER,
  CAST(NULL AS VARCHAR2(8)) AS DEPENDENCIES,
  CAST(NULL AS VARCHAR2(8)) AS COMPRESSION,
  CAST(NULL AS VARCHAR2(12)) AS COMPRESS_FOR,
  CAST(
  CASE
    WHEN
      db.database_name =  '__recyclebin'
    THEN 'YES'
    ELSE
      'NO'
  END
  AS VARCHAR2(3)) AS DROPPED,
  CAST(NULL AS VARCHAR2(3)) AS READ_ONLY,
  CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED,
  CAST(NULL AS VARCHAR2(7)) AS RESULT_CACHE
FROM
  (
    SELECT
      tenant_id,
      table_id,
      SUM(row_count) AS row_count
    FROM
      sys.ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT p
    WHERE
      p.role = 1
      AND P.TENANT_ID = EFFECTIVE_TENANT_ID()
    GROUP BY
      tenant_id,
      table_id
  )
  info
  RIGHT JOIN
    sys.ALL_VIRTUAL_TABLE_REAL_AGENT t
    ON t.tenant_id = info.tenant_id
    AND t.table_id = info.table_id
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID(),
    sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db
WHERE
    db.tenant_id = t.tenant_id
    AND db.database_id = t.database_id
    AND t.database_id = USERENV('SCHEMAID')
    AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND t.table_type != 5
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "DBA_PROFILES",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25089',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  PROFILE,
  RESOURCE_NAME,
  RESOURCE_TYPE,
  LIMIT
FROM
  (SELECT
    PROFILE_NAME AS PROFILE,
    CAST('FAILED_LOGIN_ATTEMPTS' AS VARCHAR2(32)) AS RESOURCE_NAME,
    CAST('PASSWORD' AS VARCHAR2(8)) AS RESOURCE_TYPE,
    CAST(DECODE(FAILED_LOGIN_ATTEMPTS, -1, 'UNLIMITED',
      9223372036854775807, 'UNLIMITED',
      10, 'DEFAULT',
      FAILED_LOGIN_ATTEMPTS) AS VARCHAR2(128)) AS LIMIT
  FROM
    sys.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT
  UNION ALL
  SELECT
    PROFILE_NAME AS PROFILE,
    CAST('PASSWORD_LOCK_TIME' AS VARCHAR2(32)) AS RESOURCE_NAME,
    CAST('PASSWORD' AS VARCHAR2(8)) AS RESOURCE_TYPE,
    CAST(DECODE(PASSWORD_LOCK_TIME, -1, 'UNLIMITED',
      9223372036854775807, 'UNLIMITED',
      86400000000, 'DEFAULT',
      PASSWORD_LOCK_TIME) AS VARCHAR2(128)) AS LIMIT
  FROM
    sys.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT
  UNION ALL
  SELECT
    PROFILE_NAME AS PROFILE,
    CAST('PASSWORD_VERIFY_FUNCTION' AS VARCHAR2(32)) AS RESOURCE_NAME,
    CAST('PASSWORD' AS VARCHAR2(8)) AS RESOURCE_TYPE,
    CAST(DECODE(PASSWORD_VERIFY_FUNCTION, NULL, 'NULL',
      PASSWORD_VERIFY_FUNCTION) AS VARCHAR2(128)) AS LIMIT
  FROM
    sys.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT
  UNION ALL
  SELECT
    PROFILE_NAME AS PROFILE,
    CAST('PASSWORD_LIFE_TIME' AS VARCHAR2(32)) AS RESOURCE_NAME,
    CAST('PASSWORD' AS VARCHAR2(8)) AS RESOURCE_TYPE,
    CAST(DECODE(PASSWORD_LIFE_TIME, -1, 'UNLIMITED',
      9223372036854775807, 'UNLIMITED',
      15552000000000, 'DEFAULT',
      PASSWORD_LIFE_TIME) AS VARCHAR2(128)) AS LIMIT
  FROM
    sys.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT
  UNION ALL
  SELECT
    PROFILE_NAME AS PROFILE,
    CAST('PASSWORD_GRACE_TIME' AS VARCHAR2(32)) AS RESOURCE_NAME,
    CAST('PASSWORD' AS VARCHAR2(8)) AS RESOURCE_TYPE,
    CAST(DECODE(PASSWORD_GRACE_TIME, -1, 'UNLIMITED',
      9223372036854775807, 'UNLIMITED',
      604800000000, 'DEFAULT',
      PASSWORD_GRACE_TIME) AS VARCHAR2(128)) AS LIMIT
  FROM
    sys.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT)
ORDER BY PROFILE, RESOURCE_NAME
""".replace("\n", " ")
)
# This view doesn't exist in oracle.
def_table_schema(
  table_name      = "USER_PROFILES",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25090',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(t.profile_name AS VARCHAR2(30)) AS PROFILE,
  CAST(NULL AS VARCHAR2(32)) AS RESOURCE_NAME,
  CAST(NULL AS VARCHAR2(8)) AS RESOURCE_TYPE,
  CAST(NULL AS VARCHAR2(40)) AS LIMIT_ON_RESOURCE
FROM
  SYS.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT t
  WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)
# This view doesn't exist in oracle.
def_table_schema(
  table_name      = "ALL_PROFILES",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25091',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(t.profile_name AS VARCHAR2(30)) AS PROFILE,
  CAST(NULL AS VARCHAR2(32)) AS RESOURCE_NAME,
  CAST(NULL AS VARCHAR2(8)) AS RESOURCE_TYPE,
  CAST(NULL AS VARCHAR2(40)) AS LIMIT_ON_RESOURCE
FROM
  SYS.ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT t
  WHERE T.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "ALL_MVIEW_COMMENTS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25092',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  db.DATABASE_NAME AS OWNER,
  CAST(t.TABLE_NAME AS VARCHAR2(128)) AS MVIEW_NAME,
  CAST(t."COMMENT" AS VARCHAR(4000)) AS COMMENTS
FROM
  SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db,
  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t
WHERE
    db.DATABASE_ID = t.DATABASE_ID
    AND t.TABLE_TYPE = 7
    AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "USER_MVIEW_COMMENTS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25093',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  db.DATABASE_NAME AS OWNER,
  CAST(t.TABLE_NAME AS VARCHAR2(128)) AS MVIEW_NAME,
  CAST(t."COMMENT" AS VARCHAR(4000)) AS COMMENTS
FROM
  SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db,
  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t
WHERE
    db.DATABASE_ID = t.DATABASE_ID
    AND t.TABLE_TYPE = 7
    AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND db.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "DBA_MVIEW_COMMENTS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25094',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  db.DATABASE_NAME AS OWNER,
  CAST(t.TABLE_NAME AS VARCHAR2(128)) AS MVIEW_NAME,
  CAST(t."COMMENT" AS VARCHAR(4000)) AS COMMENTS
FROM
  SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db,
  SYS.ALL_VIRTUAL_TABLE_REAL_AGENT t
WHERE
    db.DATABASE_ID = t.DATABASE_ID
    AND t.TABLE_TYPE = 7
    AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "ALL_SCHEDULER_PROGRAM_ARGS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25095',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS OWNER,
  CAST(NULL AS VARCHAR2(30)) AS PROGRAM_NAME,
  CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,
  CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,
  CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,
  CAST(NULL AS VARCHAR2(19)) AS METADATA_ATTRIBUTE,
  CAST(NULL AS VARCHAR2(4000)) AS DEFAULT_VALUE,
  CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,
  CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "DBA_SCHEDULER_PROGRAM_ARGS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25096',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS OWNER,
  CAST(NULL AS VARCHAR2(30)) AS PROGRAM_NAME,
  CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,
  CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,
  CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,
  CAST(NULL AS VARCHAR2(19)) AS METADATA_ATTRIBUTE,
  CAST(NULL AS VARCHAR2(4000)) AS DEFAULT_VALUE,
  CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,
  CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "USER_SCHEDULER_PROGRAM_ARGS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25097',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS PROGRAM_NAME,
  CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,
  CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,
  CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,
  CAST(NULL AS VARCHAR2(19)) AS METADATA_ATTRIBUTE,
  CAST(NULL AS VARCHAR2(4000)) AS DEFAULT_VALUE,
  CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,
  CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "ALL_SCHEDULER_JOB_ARGS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25098',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS OWNER,
  CAST(NULL AS VARCHAR2(30)) AS JOB_NAME,
  CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,
  CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,
  CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,
  CAST(NULL AS VARCHAR2(4000)) AS VALUE,
  CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,
  CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "DBA_SCHEDULER_JOB_ARGS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25099',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS OWNER,
  CAST(NULL AS VARCHAR2(30)) AS JOB_NAME,
  CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,
  CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,
  CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,
  CAST(NULL AS VARCHAR2(4000)) AS VALUE,
  CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,
  CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "USER_SCHEDULER_JOB_ARGS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25100',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS JOB_NAME,
  CAST(NULL AS VARCHAR2(30)) AS ARGUMENT_NAME,
  CAST(NULL AS NUMBER) AS ARGUMENT_POSITION,
  CAST(NULL AS VARCHAR2(61)) AS ARGUMENT_TYPE,
  CAST(NULL AS VARCHAR2(4000)) AS VALUE,
  CAST(NULL as /* TODO: RAW */ VARCHAR(128)) AS DEFAULT_ANYDATA_VALUE,
  CAST(NULL AS VARCHAR2(5)) AS OUT_ARGUMENT
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)

def_table_schema(
  table_name      = "ALL_ERRORS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25101',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(o.owner AS VARCHAR2(128)) AS OWNER,
  CAST(o.object_name AS VARCHAR2(128)) AS NAME,
  CAST(o.object_type AS VARCHAR2(19)) AS TYPE,
  CAST(e.obj_seq AS NUMBER) AS SEQUENCE,
  CAST(e.line AS NUMBER) AS LINE,
  CAST(e.position AS NUMBER) AS POSITION,
  CAST(e.text as VARCHAR2(4000)) AS TEXT,
  CAST(DECODE(e.property, 0, 'ERROR', 1, 'WARNING', 'UNDEFINED') AS VARCHAR2(9)) AS ATTRIBUTE,
  CAST(e.error_number AS NUMBER) AS MESSAGE_NUMBER
FROM
  all_objects o,
 (select obj_id, obj_seq, line, position, text, property, error_number, CAST( UPPER(decode(obj_type,
                                   3, 'PACKAGE',
                                   4, 'TYPE',
                                   5, 'PACKAGE BODY',
                                   6, 'TYPE BODY',
                                   7, 'TRIGGER',
                                   8, 'VIEW',
                                   9, 'FUNCTION',
                                   12, 'PROCEDURE',
                                   'MAXTYPE')) AS VARCHAR2(23)) object_type from sys.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT
                      WHERE TENANT_ID = EFFECTIVE_TENANT_ID())  e
WHERE
  o.object_id = e.obj_id
  AND o.object_type like e.object_type
  AND o.object_type IN (UPPER('package'),
                        UPPER('type'),
                        UPPER('procedure'),
                        UPPER('function'),
                        UPPER('package body'),
                        UPPER('view'),
                        UPPER('trigger'),
                        UPPER('type body'),
                        UPPER('library'),
                        UPPER('queue'),
                        UPPER('java source'),
                        UPPER('java class'),
                        UPPER('dimension'),
                        UPPER('assembly'),
                        UPPER('hierarchy'),
                        UPPER('arrtibute dimension'),
                        UPPER('analytic view'))
  """.replace("\n", " ")
)
def_table_schema(
  table_name      = "DBA_ERRORS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25102',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(o.owner AS VARCHAR2(128)) AS OWNER,
  CAST(o.object_name AS VARCHAR2(128)) AS NAME,
  CAST(o.object_type AS VARCHAR2(19)) AS TYPE,
  CAST(e.obj_seq AS NUMBER) AS SEQUENCE,
  CAST(e.line AS NUMBER) AS LINE,
  CAST(e.position AS NUMBER) AS POSITION,
  CAST(e.text as VARCHAR2(4000)) AS TEXT,
  CAST(DECODE(e.property, 0, 'ERROR', 1, 'WARNING', 'UNDEFINED') AS VARCHAR2(9)) AS ATTRIBUTE,
  CAST(e.error_number AS NUMBER) AS MESSAGE_NUMBER
FROM
  all_objects o,
 (select obj_id, obj_seq, line, position, text, property, error_number, CAST( UPPER(decode(obj_type,
                                   3, 'PACKAGE',
                                   4, 'TYPE',
                                   5, 'PACKAGE BODY',
                                   6, 'TYPE BODY',
                                   7, 'TRIGGER',
                                   8, 'VIEW',
                                   9, 'FUNCTION',
                                   12, 'PROCEDURE',
                                   'MAXTYPE')) AS VARCHAR2(23)) object_type from sys.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT
                            WHERE TENANT_ID = EFFECTIVE_TENANT_ID())  e
WHERE
  o.object_id = e.obj_id
  AND o.object_type like e.object_type
  AND o.object_type IN (UPPER('package'),
                        UPPER('type'),
                        UPPER('procedure'),
                        UPPER('function'),
                        UPPER('package body'),
                        UPPER('view'),
                        UPPER('trigger'),
                        UPPER('type body'),
                        UPPER('library'),
                        UPPER('queue'),
                        UPPER('java source'),
                        UPPER('java class'),
                        UPPER('dimension'),
                        UPPER('assembly'),
                        UPPER('hierarchy'),
                        UPPER('arrtibute dimension'),
                        UPPER('analytic view'))
  """.replace("\n", " ")
)
def_table_schema(
  table_name      = "USER_ERRORS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25103',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(o.owner AS VARCHAR2(128)) AS OWNER,
  CAST(o.object_name AS VARCHAR2(128)) AS NAME,
  CAST(o.object_type AS VARCHAR2(19)) AS TYPE,
  CAST(e.obj_seq AS NUMBER) AS SEQUENCE,
  CAST(e.line AS NUMBER) AS LINE,
  CAST(e.position AS NUMBER) AS POSITION,
  CAST(e.text as VARCHAR2(4000)) AS TEXT,
  CAST(DECODE(e.property, 0, 'ERROR', 1, 'WARNING', 'UNDEFINED') AS VARCHAR2(9)) AS ATTRIBUTE,
  CAST(e.error_number AS NUMBER) AS MESSAGE_NUMBER
FROM
  all_objects o,
 (select obj_id, obj_seq, line, position, text, property, error_number, CAST( UPPER(decode(obj_type,
                                   3, 'PACKAGE',
                                   4, 'TYPE',
                                   5, 'PACKAGE BODY',
                                   6, 'TYPE BODY',
                                   7, 'TRIGGER',
                                   8, 'VIEW',
                                   9, 'FUNCTION',
                                   12, 'PROCEDURE',
                                   'MAXTYPE')) AS VARCHAR2(23)) object_type from sys.ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT
                          WHERE TENANT_ID = EFFECTIVE_TENANT_ID())  e,
  all_users u
WHERE
  o.object_id = e.obj_id
  AND o.object_type like e.object_type
  AND o.object_type IN (UPPER('package'),
                        UPPER('type'),
                        UPPER('procedure'),
                        UPPER('function'),
                        UPPER('package body'),
                        UPPER('view'),
                        UPPER('trigger'),
                        UPPER('type body'),
                        UPPER('library'),
                        UPPER('queue'),
                        UPPER('java source'),
                        UPPER('java class'),
                        UPPER('dimension'),
                        UPPER('assembly'),
                        UPPER('hierarchy'),
                        UPPER('arrtibute dimension'),
                        UPPER('analytic view'))
  AND u.username=o.owner
  AND u.userid IN (USERENV('SCHEMAID'))
  """.replace("\n", " ")
)
def_table_schema(
  table_name      = "ALL_TYPE_METHODS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25104',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS OWNER,
  CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,
  CAST(NULL AS NUMBER) AS METHOD_NO,
  CAST(NULL AS VARCHAR2(6)) AS METHOD_TYPE,
  CAST(NULL AS NUMBER) AS PARAMETERS,
  CAST(NULL AS NUMBER) AS RESULTS,
  CAST(NULL AS VARCHAR2(3)) AS FINAL,
  CAST(NULL AS VARCHAR2(3)) AS INSTANTIABLE,
  CAST(NULL AS VARCHAR2(3)) AS OVERRIDING,
  CAST(NULL AS VARCHAR2(3)) AS INHERITED
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "DBA_TYPE_METHODS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25105',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS OWNER,
  CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,
  CAST(NULL AS NUMBER) AS METHOD_NO,
  CAST(NULL AS VARCHAR2(6)) AS METHOD_TYPE,
  CAST(NULL AS NUMBER) AS PARAMETERS,
  CAST(NULL AS NUMBER) AS RESULTS,
  CAST(NULL AS VARCHAR2(3)) AS FINAL,
  CAST(NULL AS VARCHAR2(3)) AS INSTANTIABLE,
  CAST(NULL AS VARCHAR2(3)) AS OVERRIDING,
  CAST(NULL AS VARCHAR2(3)) AS INHERITED
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "USER_TYPE_METHODS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25106',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,
  CAST(NULL AS NUMBER) AS METHOD_NO,
  CAST(NULL AS VARCHAR2(6)) AS METHOD_TYPE,
  CAST(NULL AS NUMBER) AS PARAMETERS,
  CAST(NULL AS NUMBER) AS RESULTS,
  CAST(NULL AS VARCHAR2(3)) AS FINAL,
  CAST(NULL AS VARCHAR2(3)) AS INSTANTIABLE,
  CAST(NULL AS VARCHAR2(3)) AS OVERRIDING,
  CAST(NULL AS VARCHAR2(3)) AS INHERITED
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "ALL_METHOD_PARAMS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25107',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS OWNER,
  CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,
  CAST(NULL AS NUMBER) AS METHOD_NO,
  CAST(NULL AS VARCHAR2(30)) AS PARAM_NAME,
  CAST(NULL AS NUMBER) AS PARAM_NO,
  CAST(NULL AS VARCHAR2(6)) AS PARAM_MODE,
  CAST(NULL AS VARCHAR2(7)) AS PARAM_TYPE_MOD,
  CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_OWNER,
  CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_NAME,
  CAST(NULL AS VARCHAR2(44)) AS CHARACTER_SET_NAME
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "DBA_METHOD_PARAMS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25108',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS OWNER,
  CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,
  CAST(NULL AS NUMBER) AS METHOD_NO,
  CAST(NULL AS VARCHAR2(30)) AS PARAM_NAME,
  CAST(NULL AS NUMBER) AS PARAM_NO,
  CAST(NULL AS VARCHAR2(6)) AS PARAM_MODE,
  CAST(NULL AS VARCHAR2(7)) AS PARAM_TYPE_MOD,
  CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_OWNER,
  CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_NAME,
  CAST(NULL AS VARCHAR2(44)) AS CHARACTER_SET_NAME
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)
def_table_schema(
  table_name      = "USER_METHOD_PARAMS",
  name_postfix    = "_ORA",
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25109',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
SELECT
  CAST(NULL AS VARCHAR2(30)) AS TYPE_NAME,
  CAST(NULL AS VARCHAR2(30)) AS METHOD_NAME,
  CAST(NULL AS NUMBER) AS METHOD_NO,
  CAST(NULL AS VARCHAR2(30)) AS PARAM_NAME,
  CAST(NULL AS NUMBER) AS PARAM_NO,
  CAST(NULL AS VARCHAR2(6)) AS PARAM_MODE,
  CAST(NULL AS VARCHAR2(7)) AS PARAM_TYPE_MOD,
  CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_OWNER,
  CAST(NULL AS VARCHAR2(30)) AS PARAM_TYPE_NAME,
  CAST(NULL AS VARCHAR2(44)) AS CHARACTER_SET_NAME
FROM
  DUAL
WHERE
  1 = 0
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_TABLESPACES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25110',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT
      TABLESPACE_NAME,
      CAST(NULL AS NUMBER) BLOCK_SIZE,
      CAST(NULL AS NUMBER) INITIAL_EXTENT,
      CAST(NULL AS NUMBER) NEXT_EXTENT,
      CAST(NULL AS NUMBER) MIN_EXTENT,
      CAST(NULL AS NUMBER) MAX_EXTENT,
      CAST(NULL AS NUMBER) MAX_SIZE,
      CAST(NULL AS NUMBER) PCT_INCREASE,
      CAST(NULL AS NUMBER) MIN_EXTLEN,
      CAST(NULL AS VARCHAR2(9)) STATUS,
      CAST(NULL AS VARCHAR2(9)) CONTENTS,
      CAST(NULL AS VARCHAR2(9)) LOGGING,
      CAST(NULL AS VARCHAR2(3)) FORCE_LOGGING,
      CAST(NULL AS VARCHAR2(10)) EXTENT_MANAGEMENT,
      CAST(NULL AS VARCHAR2(9)) ALLOCATION_TYPE,
      CAST(NULL AS VARCHAR2(3)) PLUGGED_IN,
      CAST(NULL AS VARCHAR2(6)) SEGMENT_SPACE_MANAGEMENT,
      CAST(NULL AS VARCHAR2(8)) DEF_TAB_COMPRESSION,
      CAST(NULL AS VARCHAR2(11)) RETENTION,
      CAST(NULL AS VARCHAR2(3)) BIGFILE,
      CAST(NULL AS VARCHAR2(7)) PREDICATE_EVALUATION,
      CAST(NULL AS VARCHAR2(3)) ENCRYPTED,
      CAST(NULL AS VARCHAR2(12)) COMPRESS_FOR
      FROM
      SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT
      WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
      """.replace("\n", " ")
)
def_table_schema(
  table_name      = 'USER_TABLESPACES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25111',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT
      TABLESPACE_NAME,
      CAST(NULL AS NUMBER) BLOCK_SIZE,
      CAST(NULL AS NUMBER) INITIAL_EXTENT,
      CAST(NULL AS NUMBER) NEXT_EXTENT,
      CAST(NULL AS NUMBER) MIN_EXTENT,
      CAST(NULL AS NUMBER) MAX_EXTENT,
      CAST(NULL AS NUMBER) MAX_SIZE,
      CAST(NULL AS NUMBER) PCT_INCREASE,
      CAST(NULL AS NUMBER) MIN_EXTLEN,
      CAST(NULL AS VARCHAR2(9)) STATUS,
      CAST(NULL AS VARCHAR2(9)) CONTENTS,
      CAST(NULL AS VARCHAR2(9)) LOGGING,
      CAST(NULL AS VARCHAR2(3)) FORCE_LOGGING,
      CAST(NULL AS VARCHAR2(10)) EXTENT_MANAGEMENT,
      CAST(NULL AS VARCHAR2(9)) ALLOCATION_TYPE,
      CAST(NULL AS VARCHAR2(6)) SEGMENT_SPACE_MANAGEMENT,
      CAST(NULL AS VARCHAR2(8)) DEF_TAB_COMPRESSION,
      CAST(NULL AS VARCHAR2(11)) RETENTION,
      CAST(NULL AS VARCHAR2(3)) BIGFILE,
      CAST(NULL AS VARCHAR2(7)) PREDICATE_EVALUATION,
      CAST(NULL AS VARCHAR2(3)) ENCRYPTED,
      CAST(NULL AS VARCHAR2(12)) COMPRESS_FOR
      FROM
      SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT
      WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
      """.replace("\n", " ")
)
# end DBA/ALL/USER_SUBPART_KEY_COLUMNS

# begin ALL/DBA/USER_IND_EXPRESSIONS
def_table_schema(
  table_name='DBA_IND_EXPRESSIONS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25112',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT CAST(INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,
    CAST(H.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(COLUMN_EXPRESSION /* TODO: LONG */ AS VARCHAR2(1000)) AS COLUMN_EXPRESSION,
    COLUMN_POSITION
  FROM
  (
  SELECT INDEX_OWNER,
    INDEX_NAME,
    TABLE_OWNER,
    F.CUR_DEFAULT_VALUE_V2 AS COLUMN_EXPRESSION,
    E.INDEX_POSITION AS  COLUMN_POSITION,
    E.TABLE_ID AS TABLE_ID
    FROM
      (SELECT INDEX_OWNER,
              INDEX_NAME,
              TABLE_OWNER,
              C.TABLE_ID AS TABLE_ID,
              C.INDEX_ID AS INDEX_ID,
              D.COLUMN_ID AS COLUMN_ID,
              D.COLUMN_NAME AS COLUMN_NAME, D.INDEX_POSITION AS INDEX_POSITION
      FROM
        (SELECT DATABASE_NAME AS INDEX_OWNER,
                SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,
                DATABASE_NAME AS TABLE_OWNER,
                A.DATA_TABLE_ID AS TABLE_ID,
                A.TABLE_ID AS INDEX_ID
          FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A
          JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID
          AND B.DATABASE_NAME != '__recyclebin'
          AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
          WHERE TABLE_TYPE=5 ) C
      JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D ON C.INDEX_ID=D.TABLE_ID
          AND D.TENANT_ID = EFFECTIVE_TENANT_ID()) E
    JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON F.TABLE_ID=E.TABLE_ID
        AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND F.COLUMN_ID=E.COLUMN_ID
    AND F.COLUMN_FLAGS=1) G
  JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT H ON G.TABLE_ID=H.TABLE_ID
      AND H.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_IND_EXPRESSIONS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25113',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT
    CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(H.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(COLUMN_EXPRESSION /* TODO: LONG */ AS VARCHAR2(1000)) AS COLUMN_EXPRESSION,
    COLUMN_POSITION
  FROM
  (
  SELECT
    INDEX_NAME,
    F.CUR_DEFAULT_VALUE_V2 AS COLUMN_EXPRESSION,
    E.INDEX_POSITION AS  COLUMN_POSITION,
    E.TABLE_ID AS TABLE_ID
    FROM
      (SELECT
              INDEX_NAME,
              C.TABLE_ID AS TABLE_ID,
              C.INDEX_ID AS INDEX_ID,
              D.COLUMN_ID AS COLUMN_ID,
              D.COLUMN_NAME AS COLUMN_NAME, D.INDEX_POSITION AS INDEX_POSITION
      FROM
        (SELECT
                SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,
                A.DATA_TABLE_ID AS TABLE_ID,
                A.TABLE_ID AS INDEX_ID
          FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A
          JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID
          AND B.DATABASE_NAME != '__recyclebin' AND A.DATABASE_ID = USERENV('SCHEMAID')
          AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
          WHERE TABLE_TYPE=5 ) C
      JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D ON C.INDEX_ID=D.TABLE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()) E
    JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON F.TABLE_ID=E.TABLE_ID
        AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND F.COLUMN_ID=E.COLUMN_ID
    AND F.COLUMN_FLAGS=1) G
  JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT H ON G.TABLE_ID=H.TABLE_ID
      AND H.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='ALL_IND_EXPRESSIONS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25114',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT CAST(INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(TABLE_OWNER AS VARCHAR2(128)) AS TABLE_OWNER,
    CAST(H.TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
    CAST(COLUMN_EXPRESSION /* TODO: LONG */ AS VARCHAR2(1000)) AS COLUMN_EXPRESSION,
    COLUMN_POSITION
  FROM
  (
  SELECT INDEX_OWNER,
    INDEX_NAME,
    TABLE_OWNER,
    F.CUR_DEFAULT_VALUE_V2 AS COLUMN_EXPRESSION,
    E.INDEX_POSITION AS  COLUMN_POSITION,
    E.TABLE_ID AS TABLE_ID
    FROM
      (SELECT INDEX_OWNER,
              INDEX_NAME,
              TABLE_OWNER,
              C.TABLE_ID AS TABLE_ID,
              C.INDEX_ID AS INDEX_ID,
              D.COLUMN_ID AS COLUMN_ID,
              D.COLUMN_NAME AS COLUMN_NAME, D.INDEX_POSITION AS INDEX_POSITION
      FROM
        (SELECT DATABASE_NAME AS INDEX_OWNER,
                        SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')) AS INDEX_NAME,
                DATABASE_NAME AS TABLE_OWNER,
                A.DATA_TABLE_ID AS TABLE_ID,
                A.TABLE_ID AS INDEX_ID
          FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT A
          JOIN SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B ON A.DATABASE_ID = B.DATABASE_ID
          AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND (A.DATABASE_ID = USERENV('SCHEMAID')
               OR USER_CAN_ACCESS_OBJ(1, A.DATA_TABLE_ID, A.DATABASE_ID) = 1)
          AND B.DATABASE_NAME != '__recyclebin'
          WHERE TABLE_TYPE=5 ) C
      JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT D ON C.INDEX_ID=D.TABLE_ID
        AND D.TENANT_ID = EFFECTIVE_TENANT_ID()) E
    JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT F ON F.TABLE_ID=E.TABLE_ID
        AND F.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND F.COLUMN_ID=E.COLUMN_ID
    AND F.COLUMN_FLAGS=1) G
  JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT H ON G.TABLE_ID=H.TABLE_ID
      AND H.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)


# end ALL/DBA/USER_IND_EXPRESSIONS

# begin DBA/ALL/USER_IND_PARTITIONS
# DBA/ALL/USER_IND_PARTITIONS will occupy table_ids in (25115, 25116, 25117).
def_table_schema(
  table_name='ALL_IND_PARTITIONS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25115',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT
    CAST(B.INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(CASE WHEN PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) AS COMPOSITE,
    CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,
    PART.SUB_PART_NUM AS SUBPARTITION_COUNT,
    CAST(PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,
    CAST(LENGTH(PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,
    PART.PART_ID + 1 AS PARTITION_POSITION,
    CAST(NULL AS VARCHAR2(8)) AS STATUS,
    CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,
    CAST(NULL AS NUMBER) AS PCT_FREE,
    CAST(NULL AS NUMBER) AS INI_TRANS,
    CAST(NULL AS NUMBER) AS MAX_TRANS,
    CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
    CAST(NULL AS NUMBER) AS NEXT_EXTENT,
    CAST(NULL AS NUMBER) AS MIN_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_SIZE,
    CAST(NULL AS NUMBER) AS PCT_INCREASE,
    CAST(NULL AS NUMBER) AS FREELISTS,
    CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
    CAST(NULL AS VARCHAR2(7)) AS LOGGING,
    CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,
    CAST(NULL AS NUMBER) AS BLEVEL,
    CAST(NULL AS NUMBER) AS LEAF_BLOCKS,
    CAST(NULL AS NUMBER) AS DISTINCT_KEYS,
    CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,
    CAST(NULL AS NUMBER) AS NUM_ROWS,
    CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
    CAST(NULL AS DATE) AS LAST_ANALYZED,
    CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
    CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
    CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
    CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
    CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,
    CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
    CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,
    CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,
    CAST(NULL AS VARCHAR2(3)) AS INTERVAL,
    CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED
    FROM(
      SELECT
      A.INDEX_OWNER AS INDEX_OWNER,
      A.INDEX_NAME AS INDEX_NAME,
      A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID*/
      A.DATA_TABLE_ID AS DATA_TABLE_ID,
      CAST(CASE WHEN A.PART_LEVEL=1 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL
      FROM(
        SELECT
          DB.DATABASE_NAME AS INDEX_OWNER,
          SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,
          TB1.TABLE_ID AS INDEX_TABLE_ID,
          TB1.DATA_TABLE_ID AS DATA_TABLE_ID,
          TB1.INDEX_TYPE AS INDEX_TYPE,
          TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/
        FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB
        WHERE DB.DATABASE_NAME!='__recyclebin'
         AND TB1.DATABASE_ID=DB.DATABASE_ID
         AND TB1.TABLE_TYPE=5
         AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()
         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
         AND (TB1.DATABASE_ID = USERENV('SCHEMAID')
             OR USER_CAN_ACCESS_OBJ(1, TB1.DATA_TABLE_ID, TB1.DATABASE_ID) = 1)
      ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID
          AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()
      WHERE A.PART_LEVEL=1 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=1)
    ) B JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART ON
    ((B.TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='FALSE') OR (B.DATA_TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='TRUE'))
    AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_IND_PARTITIONS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25116',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT
    CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(CASE WHEN PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) AS COMPOSITE,
    CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,
    PART.SUB_PART_NUM AS SUBPARTITION_COUNT,
    CAST(PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,
    CAST(LENGTH(PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,
    PART.PART_ID + 1 AS PARTITION_POSITION,
    CAST(NULL AS VARCHAR2(8)) AS STATUS,
    CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,
    CAST(NULL AS NUMBER) AS PCT_FREE,
    CAST(NULL AS NUMBER) AS INI_TRANS,
    CAST(NULL AS NUMBER) AS MAX_TRANS,
    CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
    CAST(NULL AS NUMBER) AS NEXT_EXTENT,
    CAST(NULL AS NUMBER) AS MIN_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_SIZE,
    CAST(NULL AS NUMBER) AS PCT_INCREASE,
    CAST(NULL AS NUMBER) AS FREELISTS,
    CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
    CAST(NULL AS VARCHAR2(7)) AS LOGGING,
    CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,
    CAST(NULL AS NUMBER) AS BLEVEL,
    CAST(NULL AS NUMBER) AS LEAF_BLOCKS,
    CAST(NULL AS NUMBER) AS DISTINCT_KEYS,
    CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,
    CAST(NULL AS NUMBER) AS NUM_ROWS,
    CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
    CAST(NULL AS DATE) AS LAST_ANALYZED,
    CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
    CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
    CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
    CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
    CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,
    CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
    CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,
    CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,
    CAST(NULL AS VARCHAR2(3)) AS INTERVAL,
    CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED
    FROM(
      SELECT
      A.INDEX_OWNER AS INDEX_OWNER,
      A.INDEX_NAME AS INDEX_NAME,
      A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID */
      A.DATA_TABLE_ID AS DATA_TABLE_ID,
      CAST(CASE WHEN A.PART_LEVEL=1 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL
      FROM(
        SELECT
          DB.DATABASE_NAME AS INDEX_OWNER,
          SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,
          TB1.TABLE_ID AS INDEX_TABLE_ID,
          TB1.DATA_TABLE_ID AS DATA_TABLE_ID,
          TB1.INDEX_TYPE AS INDEX_TYPE,
          TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/
        FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB
        WHERE DB.DATABASE_NAME!='__recyclebin' AND TB1.DATABASE_ID=DB.DATABASE_ID AND TB1.TABLE_TYPE=5
              AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()
              AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
      ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID
          AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()
      WHERE A.PART_LEVEL=1 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=1)
    ) B JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART ON
    ((B.TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='FALSE') OR (B.DATA_TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='TRUE'))
    AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='DBA_IND_PARTITIONS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25117',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT
    CAST(B.INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(CASE WHEN PART.SUB_PART_NUM <=0 THEN 'NO' ELSE 'YES' END AS VARCHAR(3)) AS COMPOSITE,
    CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,
    PART.SUB_PART_NUM AS SUBPARTITION_COUNT,
    CAST(PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,
    CAST(LENGTH(PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,
    PART.PART_ID + 1 AS PARTITION_POSITION,
    CAST(NULL AS VARCHAR2(8)) AS STATUS,
    CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,
    CAST(NULL AS NUMBER) AS PCT_FREE,
    CAST(NULL AS NUMBER) AS INI_TRANS,
    CAST(NULL AS NUMBER) AS MAX_TRANS,
    CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
    CAST(NULL AS NUMBER) AS NEXT_EXTENT,
    CAST(NULL AS NUMBER) AS MIN_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_SIZE,
    CAST(NULL AS NUMBER) AS PCT_INCREASE,
    CAST(NULL AS NUMBER) AS FREELISTS,
    CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
    CAST(NULL AS VARCHAR2(7)) AS LOGGING,
    CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,
    CAST(NULL AS NUMBER) AS BLEVEL,
    CAST(NULL AS NUMBER) AS LEAF_BLOCKS,
    CAST(NULL AS NUMBER) AS DISTINCT_KEYS,
    CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,
    CAST(NULL AS NUMBER) AS NUM_ROWS,
    CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
    CAST(NULL AS DATE) AS LAST_ANALYZED,
    CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
    CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
    CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
    CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
    CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,
    CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
    CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,
    CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,
    CAST(NULL AS VARCHAR2(3)) AS INTERVAL,
    CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED
    FROM(
      SELECT
      A.INDEX_OWNER AS INDEX_OWNER,
      A.INDEX_NAME AS INDEX_NAME,
      A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID*/
      A.DATA_TABLE_ID AS DATA_TABLE_ID,
      CAST(CASE WHEN A.PART_LEVEL=1 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL
      FROM(
        SELECT
          DB.DATABASE_NAME AS INDEX_OWNER,
          SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,
          TB1.TABLE_ID AS INDEX_TABLE_ID,
          TB1.DATA_TABLE_ID AS DATA_TABLE_ID,
          TB1.INDEX_TYPE AS INDEX_TYPE,
          TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/
        FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB
        WHERE DB.DATABASE_NAME!='__recyclebin' AND TB1.DATABASE_ID=DB.DATABASE_ID AND TB1.TABLE_TYPE=5
            AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
      ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID
          AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()
      WHERE A.PART_LEVEL=1 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=1)
    ) B JOIN SYS.ALL_VIRTUAL_PART_REAL_AGENT PART ON
    ((B.TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='FALSE') OR (B.DATA_TABLE_ID=PART.TABLE_ID AND B.IS_LOCAL='TRUE'))
    AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

# end DBA/USER/ALL_IND_PARTITIONS

# begin DBA/ALL/USER_IND_SUBPARTITIONS
# DBA/ALL/USER_IND_SUBPARTITIONS will occupy table_ids in (25118, 25119, 25120).
def_table_schema(
  table_name='DBA_IND_SUBPARTITIONS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25118',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT
    CAST(B.INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,
    CAST(SUB_PART.SUB_PART_NAME AS VARCHAR2(128)) AS SUBPARTITION_NAME,
    CAST(SUB_PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,
    CAST(LENGTH(SUB_PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,
    SUB_PART.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,
    CAST(NULL AS VARCHAR2(8)) AS STATUS,
    CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,
    CAST(NULL AS NUMBER) AS PCT_FREE,
    CAST(NULL AS NUMBER) AS INI_TRANS,
    CAST(NULL AS NUMBER) AS MAX_TRANS,
    CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
    CAST(NULL AS NUMBER) AS NEXT_EXTENT,
    CAST(NULL AS NUMBER) AS MIN_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_SIZE,
    CAST(NULL AS NUMBER) AS PCT_INCREASE,
    CAST(NULL AS NUMBER) AS FREELISTS,
    CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
    CAST(NULL AS VARCHAR2(7)) AS LOGGING,
    CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,
    CAST(NULL AS NUMBER) AS BLEVEL,
    CAST(NULL AS NUMBER) AS LEAF_BLOCKS,
    CAST(NULL AS NUMBER) AS DISTINCT_KEYS,
    CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,
    CAST(NULL AS NUMBER) AS NUM_ROWS,
    CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
    CAST(NULL AS DATE) AS LAST_ANALYZED,
    CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
    CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
    CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
    CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
    CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,
    CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
    CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,
    CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,
    CAST(NULL AS VARCHAR2(3)) AS INTERVAL,
    CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED
    FROM(
      SELECT
      A.INDEX_OWNER AS INDEX_OWNER,
      A.INDEX_NAME AS INDEX_NAME,
      A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID*/
      A.DATA_TABLE_ID AS DATA_TABLE_ID,
      A.PART_LEVEL AS INDEX_TABLE_PART_LEVEL,
      TB2.PART_LEVEL AS DATA_TABLE_PART_LEVEL,
      /* CAST(CASE WHEN A.PART_LEVEL=2 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL */
      CAST('TRUE' AS VARCHAR2(5)) AS IS_LOCAL
      FROM(
        SELECT
          DB.DATABASE_NAME AS INDEX_OWNER,
          SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,
          TB1.TABLE_ID AS INDEX_TABLE_ID,
          TB1.DATA_TABLE_ID AS DATA_TABLE_ID,
          TB1.INDEX_TYPE AS INDEX_TYPE,
          TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/
        FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB
        WHERE DB.DATABASE_NAME!='__recyclebin' AND TB1.DATABASE_ID=DB.DATABASE_ID AND TB1.TABLE_TYPE=5
            AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
      ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID
            AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()
      WHERE A.PART_LEVEL=2 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=2)
      ) B, SYS.ALL_VIRTUAL_PART_REAL_AGENT PART, SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUB_PART
      WHERE ((B.IS_LOCAL='FALSE' AND B.TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID)
      OR (B.IS_LOCAL='TRUE' AND B.DATA_TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID))
      AND SUB_PART.TENANT_ID = EFFECTIVE_TENANT_ID()
      AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)
def_table_schema(
  table_name='ALL_IND_SUBPARTITIONS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25119',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT
    CAST(B.INDEX_OWNER AS VARCHAR2(128)) AS INDEX_OWNER,
    CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(PART.PART_NAME AS VARCHAR2(128)) AS PARTITION_NAME,
    CAST(SUB_PART.SUB_PART_NAME AS VARCHAR2(128)) AS SUBPARTITION_NAME,
    CAST(SUB_PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,
    CAST(LENGTH(SUB_PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,
    SUB_PART.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,
    CAST(NULL AS VARCHAR2(8)) AS STATUS,
    CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,
    CAST(NULL AS NUMBER) AS PCT_FREE,
    CAST(NULL AS NUMBER) AS INI_TRANS,
    CAST(NULL AS NUMBER) AS MAX_TRANS,
    CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
    CAST(NULL AS NUMBER) AS NEXT_EXTENT,
    CAST(NULL AS NUMBER) AS MIN_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_SIZE,
    CAST(NULL AS NUMBER) AS PCT_INCREASE,
    CAST(NULL AS NUMBER) AS FREELISTS,
    CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
    CAST(NULL AS VARCHAR2(7)) AS LOGGING,
    CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,
    CAST(NULL AS NUMBER) AS BLEVEL,
    CAST(NULL AS NUMBER) AS LEAF_BLOCKS,
    CAST(NULL AS NUMBER) AS DISTINCT_KEYS,
    CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,
    CAST(NULL AS NUMBER) AS NUM_ROWS,
    CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
    CAST(NULL AS DATE) AS LAST_ANALYZED,
    CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
    CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
    CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
    CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
    CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,
    CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
    CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,
    CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,
    CAST(NULL AS VARCHAR2(3)) AS INTERVAL,
    CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED
    FROM(
      SELECT
      A.INDEX_OWNER AS INDEX_OWNER,
      A.INDEX_NAME AS INDEX_NAME,
      A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID*/
      A.DATA_TABLE_ID AS DATA_TABLE_ID,
      A.PART_LEVEL AS INDEX_TABLE_PART_LEVEL,
      TB2.PART_LEVEL AS DATA_TABLE_PART_LEVEL,
      /* CAST(CASE WHEN A.PART_LEVEL=2 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL */
      CAST('TRUE' AS VARCHAR2(5)) AS IS_LOCAL
      FROM(
        SELECT
          DB.DATABASE_NAME AS INDEX_OWNER,
          SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,
          TB1.TABLE_ID AS INDEX_TABLE_ID,
          TB1.DATA_TABLE_ID AS DATA_TABLE_ID,
          TB1.INDEX_TYPE AS INDEX_TYPE,
          TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/
        FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB
        WHERE DB.DATABASE_NAME!='__recyclebin'
          AND TB1.DATABASE_ID=DB.DATABASE_ID
          AND TB1.TABLE_TYPE=5
          AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND (TB1.DATABASE_ID = USERENV('SCHEMAID')
                 OR USER_CAN_ACCESS_OBJ(1, TB1.DATA_TABLE_ID, TB1.DATABASE_ID) = 1)

      ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID
            AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()
      WHERE A.PART_LEVEL=2 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=2)
      ) B, SYS.ALL_VIRTUAL_PART_REAL_AGENT PART, SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUB_PART
      WHERE ((B.IS_LOCAL='FALSE' AND B.TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID)
              OR (B.IS_LOCAL='TRUE' AND B.DATA_TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID))
          AND SUB_PART.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)
def_table_schema(
  table_name='USER_IND_SUBPARTITIONS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25120',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition = """
  SELECT
    CAST(B.INDEX_NAME AS VARCHAR2(128)) AS INDEX_NAME,
    CAST(PART.PART_NAME AS VARCHAR2(30)) AS PARTITION_NAME,
    CAST(SUB_PART.SUB_PART_NAME AS VARCHAR2(128)) AS SUBPARTITION_NAME,
    CAST(SUB_PART.HIGH_BOUND_VAL AS VARCHAR2(1024)) AS HIGH_VALUE,
    CAST(LENGTH(SUB_PART.HIGH_BOUND_VAL) AS NUMBER) AS HIGH_VALUE_LENGTH,
    SUB_PART.SUB_PART_ID + 1 AS SUBPARTITION_POSITION,
    CAST(NULL AS VARCHAR2(8)) AS STATUS,
    CAST(NULL AS VARCHAR2(30)) AS TABLESPACE_NAME,
    CAST(NULL AS NUMBER) AS PCT_FREE,
    CAST(NULL AS NUMBER) AS INI_TRANS,
    CAST(NULL AS NUMBER) AS MAX_TRANS,
    CAST(NULL AS NUMBER) AS INITIAL_EXTENT,
    CAST(NULL AS NUMBER) AS NEXT_EXTENT,
    CAST(NULL AS NUMBER) AS MIN_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_EXTENT,
    CAST(NULL AS NUMBER) AS MAX_SIZE,
    CAST(NULL AS NUMBER) AS PCT_INCREASE,
    CAST(NULL AS NUMBER) AS FREELISTS,
    CAST(NULL AS NUMBER) AS FREELIST_GROUPS,
    CAST(NULL AS VARCHAR2(7)) AS LOGGING,
    CAST(CASE WHEN PART.COMPRESS_FUNC_NAME IS NULL THEN 'DISABLED' ELSE 'ENABLED' END AS VARCHAR2(8)) AS COMPRESSION,
    CAST(NULL AS NUMBER) AS BLEVEL,
    CAST(NULL AS NUMBER) AS LEAF_BLOCKS,
    CAST(NULL AS NUMBER) AS DISTINCT_KEYS,
    CAST(NULL AS NUMBER) AS AVG_LEAF_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS AVG_DATA_BLOCKS_PER_KEY,
    CAST(NULL AS NUMBER) AS CLUSTERING_FACTOR,
    CAST(NULL AS NUMBER) AS NUM_ROWS,
    CAST(NULL AS NUMBER) AS SAMPLE_SIZE,
    CAST(NULL AS DATE) AS LAST_ANALYZED,
    CAST(NULL AS VARCHAR2(7)) AS BUFFER_POOL,
    CAST(NULL AS VARCHAR2(7)) AS FLASH_CACHE,
    CAST(NULL AS VARCHAR2(7)) AS CELL_FLASH_CACHE,
    CAST(NULL AS VARCHAR2(3)) AS USER_STATS,
    CAST(NULL AS NUMBER) AS PCT_DIRECT_ACCESS,
    CAST(NULL AS VARCHAR2(3)) AS GLOBAL_STATS,
    CAST(NULL AS VARCHAR2(6)) AS DOMIDX_OPSTATUS,
    CAST(NULL AS VARCHAR2(1000)) AS PARAMETERS,
    CAST(NULL AS VARCHAR2(3)) AS INTERVAL,
    CAST(NULL AS VARCHAR2(3)) AS SEGMENT_CREATED
    FROM(
      SELECT
      A.INDEX_OWNER AS INDEX_OWNER,
      A.INDEX_NAME AS INDEX_NAME,
      A.INDEX_TABLE_ID AS TABLE_ID, /*INDEX TABLE ID */
      A.DATA_TABLE_ID AS DATA_TABLE_ID,
      A.PART_LEVEL AS INDEX_TABLE_PART_LEVEL,
      TB2.PART_LEVEL AS DATA_TABLE_PART_LEVEL,
      /* CAST(CASE WHEN A.PART_LEVEL=2 THEN 'FALSE' ELSE 'TRUE' END AS VARCHAR2(5)) IS_LOCAL */
      CAST('TRUE' AS VARCHAR2(5)) AS IS_LOCAL
      FROM(
        SELECT
          DB.DATABASE_NAME AS INDEX_OWNER,
          SUBSTR(TB1.TABLE_NAME, 7 + INSTR(SUBSTR(TB1.TABLE_NAME, 7), '_')) AS INDEX_NAME,
          TB1.TABLE_ID AS INDEX_TABLE_ID,
          TB1.DATA_TABLE_ID AS DATA_TABLE_ID,
          TB1.INDEX_TYPE AS INDEX_TYPE,
          TB1.PART_LEVEL AS PART_LEVEL /*USE DATA TABLE'S PART_LEVEL IF INDEX IS LOCAL INDEX*/
        FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB1, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB
        WHERE DB.DATABASE_NAME!='__recyclebin' AND TB1.DATABASE_ID=DB.DATABASE_ID AND TB1.TABLE_TYPE=5
            AND TB1.TENANT_ID = EFFECTIVE_TENANT_ID()
            AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
      ) A JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TB2 ON A.DATA_TABLE_ID=TB2.TABLE_ID
            AND TB2.TENANT_ID = EFFECTIVE_TENANT_ID()
      WHERE A.PART_LEVEL=2 OR ((A.INDEX_TYPE=1 OR A.INDEX_TYPE=2) AND TB2.PART_LEVEL=2)
      ) B, SYS.ALL_VIRTUAL_PART_REAL_AGENT PART, SYS.ALL_VIRTUAL_SUB_PART_REAL_AGENT SUB_PART
      WHERE ((B.IS_LOCAL='FALSE' AND B.TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID)
              OR (B.IS_LOCAL='TRUE' AND B.DATA_TABLE_ID=PART.TABLE_ID AND PART.TABLE_ID=SUB_PART.TABLE_ID AND PART.PART_ID=SUB_PART.PART_ID))
          AND PART.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND SUB_PART.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_ROLES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25121',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CAST(USER_NAME AS VARCHAR2(30)) ROLE,\
      CAST('NO' AS VARCHAR2(8)) PASSWORD_REQUIRED,\
      CAST('NONE' AS VARCHAR2(11)) AUTHENTICATION_TYPE\
      FROM   SYS.ALL_VIRTUAL_USER_REAL_AGENT U\
      WHERE\
             U.TYPE = 1 AND U.TENANT_ID = EFFECTIVE_TENANT_ID()""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_ROLE_PRIVS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25122',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CAST(A.USER_NAME AS VARCHAR2(30)) GRANTEE,\
      CAST(B.USER_NAME AS VARCHAR2(30)) GRANTED_ROLE,\
      DECODE(R.ADMIN_OPTION, 0, 'NO', 1, 'YES', '') AS ADMIN_OPTION ,\
      DECODE(R.DISABLE_FLAG, 0, 'YES', 1, 'NO', '') AS DEFAULT_ROLE\
      FROM SYS.ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_AGENT R,\
      SYS.ALL_VIRTUAL_USER_REAL_AGENT A,\
      SYS.ALL_VIRTUAL_USER_REAL_AGENT B\
      WHERE R.GRANTEE_ID = A.USER_ID\
      AND R.ROLE_ID = B.USER_ID\
      AND B.TYPE = 1\
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()\
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_ROLE_PRIVS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25123',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT CAST(A.USER_NAME AS VARCHAR2(30)) GRANTEE,\
      CAST(B.USER_NAME AS VARCHAR2(30)) GRANTED_ROLE,\
      DECODE(R.ADMIN_OPTION, 0, 'NO', 1, 'YES', '') AS ADMIN_OPTION ,\
      DECODE(R.DISABLE_FLAG, 0, 'YES', 1, 'NO', '') AS DEFAULT_ROLE\
      FROM SYS.ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_AGENT R,\
      SYS.ALL_VIRTUAL_USER_REAL_AGENT A,\
      SYS.ALL_VIRTUAL_USER_REAL_AGENT B\
      WHERE R.GRANTEE_ID = A.USER_ID\
      AND R.ROLE_ID = B.USER_ID\
      AND B.TYPE = 1\
      AND A.TENANT_ID = EFFECTIVE_TENANT_ID()\
      AND B.TENANT_ID = EFFECTIVE_TENANT_ID()\
      AND A.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_TAB_PRIVS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25124',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT C.USER_NAME AS GRANTEE,
       E.DATABASE_NAME AS OWNER,
       CAST (DECODE(A.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),
                        D.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,
       B.USER_NAME AS GRANTOR,
       CAST (DECODE(A.PRIV_ID, 1, 'ALTER',
                         2, 'AUDIT',
                         3, 'COMMENT',
                         4, 'DELETE',
                         5, 'GRANT',
                         6, 'INDEX',
                         7, 'INSERT',
                         8, 'LOCK',
                         9, 'RENAME',
                         10, 'SELECT',
                         11, 'UPDATE',
                         12, 'REFERENCES',
                         13, 'EXECUTE',
                         14, 'CREATE',
                         15, 'FLASHBACK',
                         16, 'READ',
                         17, 'WRITE',
                         'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,
       DECODE(A.PRIV_OPTION,0,'NO', 1,'YES','') AS GRANTABLE,
       CAST('NO' AS VARCHAR(10)) AS  HIERARCHY
      FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT A ,
              SYS.ALL_VIRTUAL_USER_REAL_AGENT B,
              SYS.ALL_VIRTUAL_USER_REAL_AGENT C,
              (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID
                 FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT
                 WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
               UNION ALL
               SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID
                  FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT
                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
               UNION ALL
               SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID
                  FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT
                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
               UNION ALL
               SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID
                  FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT
                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
              ) D,
              SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E
      WHERE A.GRANTOR_ID = B.USER_ID
        AND A.GRANTEE_ID = C.USER_ID
        AND A.COL_ID = 65535
        AND A.OBJ_ID = D.TABLE_ID
        AND  D.DATABASE_ID=E.DATABASE_ID
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.DATABASE_NAME != '__recyclebin'
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_TAB_PRIVS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25125',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT B.USER_NAME AS GRANTOR,
       C.USER_NAME AS GRANTEE,
       E.DATABASE_NAME AS TABLE_SCHEMA,
       CAST (DECODE(A.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),
                        D.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,
       CAST (DECODE(A.PRIV_ID, 1, 'ALTER',
                         2, 'AUDIT',
                         3, 'COMMENT',
                         4, 'DELETE',
                         5, 'GRANT',
                         6, 'INDEX',
                         7, 'INSERT',
                         8, 'LOCK',
                         9, 'RENAME',
                         10, 'SELECT',
                         11, 'UPDATE',
                         12, 'REFERENCES',
                         13, 'EXECUTE',
                         14, 'CREATE',
                         15, 'FLASHBACK',
                         16, 'READ',
                         17, 'WRITE',
                         'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,
       DECODE(A.PRIV_OPTION,0,'NO', 1,'YES','') AS GRANTABLE,
       CAST('NO' AS VARCHAR(10)) AS  HIERARCHY
      FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT A ,
              SYS.ALL_VIRTUAL_USER_REAL_AGENT B,
              SYS.ALL_VIRTUAL_USER_REAL_AGENT C,
              (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID
                 FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT
                 WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
               UNION ALL
               SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID
                  FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT
                WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
               UNION ALL
               SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID
                  FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT
                WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
               UNION ALL
               SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID
                  FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT
                WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
              ) D,
              SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E
      WHERE A.GRANTOR_ID = B.USER_ID
        AND A.GRANTEE_ID = C.USER_ID
        AND A.COL_ID = 65535
        AND A.OBJ_ID = D.TABLE_ID
        AND D.DATABASE_ID=E.DATABASE_ID
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.DATABASE_NAME != '__recyclebin'
        AND (C.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')
             OR E.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')
             OR B.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER'))
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_TAB_PRIVS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25126',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT C.USER_NAME AS GRANTEE,
       E.DATABASE_NAME AS OWNER,
       CAST (DECODE(A.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),
                        D.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,
       B.USER_NAME AS GRANTOR,
       CAST (DECODE(A.PRIV_ID, 1, 'ALTER',
                         2, 'AUDIT',
                         3, 'COMMENT',
                         4, 'DELETE',
                         5, 'GRANT',
                         6, 'INDEX',
                         7, 'INSERT',
                         8, 'LOCK',
                         9, 'RENAME',
                         10, 'SELECT',
                         11, 'UPDATE',
                         12, 'REFERENCES',
                         13, 'EXECUTE',
                         14, 'CREATE',
                         15, 'FLASHBACK',
                         16, 'READ',
                         17, 'WRITE',
                         'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,
       DECODE(A.PRIV_OPTION,0,'NO', 1,'YES','') AS GRANTABLE,
       CAST('NO' AS VARCHAR(10)) AS  HIERARCHY
      FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT A ,
              SYS.ALL_VIRTUAL_USER_REAL_AGENT B,
              SYS.ALL_VIRTUAL_USER_REAL_AGENT C,
              (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID
                 FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT
                 WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
               UNION ALL
               SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID
                  FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT
                  WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
               UNION ALL
               SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID
                  FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT
               WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
               UNION ALL
               SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID
                  FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT
               WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
              ) D,
              SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT E
      WHERE A.GRANTOR_ID = B.USER_ID
        AND A.GRANTEE_ID = C.USER_ID
        AND A.COL_ID = 65535
        AND A.OBJ_ID = D.TABLE_ID
        AND D.DATABASE_ID=E.DATABASE_ID
        AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND E.DATABASE_NAME != '__recyclebin'
        AND (SYS_CONTEXT('USERENV','CURRENT_USER') IN (C.USER_NAME, B.USER_NAME)
             OR E.DATABASE_NAME = SYS_CONTEXT('USERENV','CURRENT_USER'))
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_SYS_PRIVS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25127',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT B.USER_NAME AS GRANTEE,
           CAST (DECODE(A.PRIV_ID,
                1, 'CREATE SESSION',
                2, 'EXEMPT REDACT POLICY',
                3, 'SYSDBA',
                4, 'SYSOPER',
                5, 'SYSBACKUP',
                6, 'CREATE TABLE',
                7, 'CREATE ANY TABLE',
                8, 'ALTER ANY TABLE',
                9, 'BACKUP ANY TABLE',
                10, 'DROP ANY TABLE',
                11, 'LOCK ANY TABLE',
                12, 'COMMENT ANY TABLE',
                13, 'SELECT ANY TABLE',
                14, 'INSERT ANY TABLE',
                15, 'UPDATE ANY TABLE',
                16, 'DELETE ANY TABLE',
                17, 'FLASHBACK ANY TABLE',
                18, 'CREATE ROLE',
                19, 'DROP ANY ROLE',
                20, 'GRANT ANY ROLE',
                21, 'ALTER ANY ROLE',
                22, 'AUDIT ANY',
                23, 'GRANT ANY PRIVILEGE',
                24, 'GRANT ANY OBJECT PRIVILEGE',
                25, 'CREATE ANY INDEX',
                26, 'ALTER ANY INDEX',
                27, 'DROP ANY INDEX',
                28, 'CREATE ANY VIEW',
                29, 'DROP ANY VIEW',
                30, 'CREATE VIEW',
                31, 'SELECT ANY DICTIONARY',
                32, 'CREATE PROCEDURE',
                33, 'CREATE ANY PROCEDURE',
                34, 'ALTER ANY PROCEDURE',
                35, 'DROP ANY PROCEDURE',
                36, 'EXECUTE ANY PROCEDURE',
                37, 'CREATE SYNONYM',
                38, 'CREATE ANY SYNONYM',
                39, 'DROP ANY SYNONYM',
                40, 'CREATE PUBLIC SYNONYM',
                41, 'DROP PUBLIC SYNONYM',
                42, 'CREATE SEQUENCE',
                43, 'CREATE ANY SEQUENCE',
                44, 'ALTER ANY SEQUENCE',
                45, 'DROP ANY SEQUENCE',
                46, 'SELECT ANY SEQUENCE',
                47, 'CREATE TRIGGER',
                48, 'CREATE ANY TRIGGER',
                49, 'ALTER ANY TRIGGER',
                50, 'DROP ANY TRIGGER',
                51, 'CREATE PROFILE',
                52, 'ALTER PROFILE',
                53, 'DROP PROFILE',
                54, 'CREATE USER',
                55, 'BECOME USER',
                56, 'ALTER USER',
                57, 'DROP USER',
                58, 'CREATE TYPE',
                59, 'CREATE ANY TYPE',
                60, 'ALTER ANY TYPE',
                61, 'DROP ANY TYPE',
                62, 'EXECUTE ANY TYPE',
                63, 'UNDER ANY TYPE',
                64, 'PURGE DBA_RECYCLEBIN',
                65, 'CREATE ANY OUTLINE',
                66, 'ALTER ANY OUTLINE',
                67, 'DROP ANY OUTLINE',
                68, 'SYSKM',
                69, 'CREATE TABLESPACE',
                70, 'ALTER TABLESPACE',
                71, 'DROP TABLESPACE',
                72, 'SHOW PROCESS',
                73, 'ALTER SYSTEM',
                74, 'CREATE DATABASE LINK',
                75, 'CREATE PUBLIC DATABASE LINK',
                76, 'DROP DATABASE LINK',
                77, 'ALTER SESSION',
                78, 'ALTER DATABASE',
                'OTHER') AS VARCHAR(40)) AS PRIVILEGE,
        CASE PRIV_OPTION
          WHEN 0 THEN 'NO'
          ELSE 'YES'
          END AS ADMIN_OPTION
    FROM SYS.ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT A,
          SYS.ALL_VIRTUAL_USER_REAL_AGENT B
    WHERE A.GRANTEE_ID = B.USER_ID
    AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
    AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_SYS_PRIVS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25128',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT B.USER_NAME AS USERNAME,
           CAST (DECODE(A.PRIV_ID,
                1, 'CREATE SESSION',
                2, 'EXEMPT REDACT POLICY',
                3, 'SYSDBA',
                4, 'SYSOPER',
                5, 'SYSBACKUP',
                6, 'CREATE TABLE',
                7, 'CREATE ANY TABLE',
                8, 'ALTER ANY TABLE',
                9, 'BACKUP ANY TABLE',
                10, 'DROP ANY TABLE',
                11, 'LOCK ANY TABLE',
                12, 'COMMENT ANY TABLE',
                13, 'SELECT ANY TABLE',
                14, 'INSERT ANY TABLE',
                15, 'UPDATE ANY TABLE',
                16, 'DELETE ANY TABLE',
                17, 'FLASHBACK ANY TABLE',
                18, 'CREATE ROLE',
                19, 'DROP ANY ROLE',
                20, 'GRANT ANY ROLE',
                21, 'ALTER ANY ROLE',
                22, 'AUDIT ANY',
                23, 'GRANT ANY PRIVILEGE',
                24, 'GRANT ANY OBJECT PRIVILEGE',
                25, 'CREATE ANY INDEX',
                26, 'ALTER ANY INDEX',
                27, 'DROP ANY INDEX',
                28, 'CREATE ANY VIEW',
                29, 'DROP ANY VIEW',
                30, 'CREATE VIEW',
                31, 'SELECT ANY DICTIONARY',
                32, 'CREATE PROCEDURE',
                33, 'CREATE ANY PROCEDURE',
                34, 'ALTER ANY PROCEDURE',
                35, 'DROP ANY PROCEDURE',
                36, 'EXECUTE ANY PROCEDURE',
                37, 'CREATE SYNONYM',
                38, 'CREATE ANY SYNONYM',
                39, 'DROP ANY SYNONYM',
                40, 'CREATE PUBLIC SYNONYM',
                41, 'DROP PUBLIC SYNONYM',
                42, 'CREATE SEQUENCE',
                43, 'CREATE ANY SEQUENCE',
                44, 'ALTER ANY SEQUENCE',
                45, 'DROP ANY SEQUENCE',
                46, 'SELECT ANY SEQUENCE',
                47, 'CREATE TRIGGER',
                48, 'CREATE ANY TRIGGER',
                49, 'ALTER ANY TRIGGER',
                50, 'DROP ANY TRIGGER',
                51, 'CREATE PROFILE',
                52, 'ALTER PROFILE',
                53, 'DROP PROFILE',
                54, 'CREATE USER',
                55, 'BECOME USER',
                56, 'ALTER USER',
                57, 'DROP USER',
                58, 'CREATE TYPE',
                59, 'CREATE ANY TYPE',
                60, 'ALTER ANY TYPE',
                61, 'DROP ANY TYPE',
                62, 'EXECUTE ANY TYPE',
                63, 'UNDER ANY TYPE',
                64, 'PURGE DBA_RECYCLEBIN',
                65, 'CREATE ANY OUTLINE',
                66, 'ALTER ANY OUTLINE',
                67, 'DROP ANY OUTLINE',
                68, 'SYSKM',
                69, 'CREATE TABLESPACE',
                70, 'ALTER TABLESPACE',
                71, 'DROP TABLESPACE',
                72, 'SHOW PROCESS',
                73, 'ALTER SYSTEM',
                74, 'CREATE DATABASE LINK',
                75, 'CREATE PUBLIC DATABASE LINK',
                76, 'DROP DATABASE LINK',
                77, 'ALTER SESSION',
                78, 'ALTER DATABASE',
                'OTHER') AS VARCHAR(40)) AS PRIVILEGE,
        CASE PRIV_OPTION
          WHEN 0 THEN 'NO'
          ELSE 'YES'
          END AS ADMIN_OPTION
    FROM SYS.ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT A,
          SYS.ALL_VIRTUAL_USER_REAL_AGENT B
    WHERE B.TYPE = 0 AND
          A.GRANTEE_ID =B.USER_ID
          AND A.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND B.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND B.USER_NAME = SYS_CONTEXT('USERENV','CURRENT_USER')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_COL_PRIVS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25143',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
      SELECT
        u_grantee.user_name as GRANTEE,
        db.database_name as OWNER,
        decode(auth.objtype, 1, t.table_name, '') as TABLE_NAME,
        c.column_name as COLUMN_NAME,
        u_grantor.user_name as GRANTOR,
        cast (decode(auth.priv_id, 1, 'ALTER',
                             2, 'AUDIT',
                             3, 'COMMENT',
                             4, 'DELETE',
                             5, 'GRANT',
                             6, 'INDEX',
                             7, 'INSERT',
                             8, 'LOCK',
                             9, 'RENAME',
                             10, 'SELECT',
                             11, 'UPDATE',
                             12, 'REFERENCES',
                             13, 'EXECUTE',
                             14, 'CREATE',
                             15, 'FLASHBACK',
                             16, 'READ',
                             17, 'WRITE',
                             'OTHERS') as varchar(40)) as PRIVILEGE,
        decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as GRANTABLE
      FROM
        sys.all_virtual_objauth_agent auth,
        sys.ALL_VIRTUAL_COLUMN_REAL_AGENT c,
        sys.ALL_VIRTUAL_TABLE_REAL_AGENT t,
        sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db,
        sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantor,
        sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantee
      WHERE
        auth.col_id = c.column_id and
        auth.obj_id = t.table_id and
        auth.objtype = 1 and
        auth.obj_id = c.table_id and
        db.database_id = t.database_id and
        u_grantor.user_id = auth.grantor_id and
        u_grantee.user_id = auth.grantee_id
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND U_GRANTOR.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()
	      AND c.column_id != 65535
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_COL_PRIVS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25144',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
      SELECT
        u_grantee.user_name as GRANTEE,
        db.database_name as OWNER,
        decode(auth.objtype, 1, t.table_name, '') as TABLE_NAME,
        c.column_name as COLUMN_NAME,
        u_grantor.user_name as GRANTOR,
        cast (decode(auth.priv_id, 1, 'ALTER',
                             2, 'AUDIT',
                             3, 'COMMENT',
                             4, 'DELETE',
                             5, 'GRANT',
                             6, 'INDEX',
                             7, 'INSERT',
                             8, 'LOCK',
                             9, 'RENAME',
                             10, 'SELECT',
                             11, 'UPDATE',
                             12, 'REFERENCES',
                             13, 'EXECUTE',
                             14, 'CREATE',
                             15, 'FLASHBACK',
                             16, 'READ',
                             17, 'WRITE',
                             'OTHERS') as varchar(40)) as PRIVILEGE,
        decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as GRANTABLE
      FROM
        sys.all_virtual_objauth_agent auth,
        sys.ALL_VIRTUAL_COLUMN_REAL_AGENT c,
        sys.ALL_VIRTUAL_TABLE_REAL_AGENT t,
        sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db,
        sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantor,
        sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantee
      WHERE
        auth.col_id = c.column_id and
        auth.obj_id = t.table_id and
        auth.objtype = 1 and
        auth.obj_id = c.table_id and
        db.database_id = t.database_id and
        u_grantor.user_id = auth.grantor_id and
        u_grantee.user_id = auth.grantee_id
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND U_GRANTOR.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND c.column_id != 65535 and
	      (db.database_name = SYS_CONTEXT('USERENV','CURRENT_USER') or
         u_grantee.user_name = SYS_CONTEXT('USERENV','CURRENT_USER') or
         u_grantor.user_name = SYS_CONTEXT('USERENV','CURRENT_USER')
        )
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'ALL_COL_PRIVS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25145',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
      SELECT
	u_grantor.user_name as GRANTOR,
        u_grantee.user_name as GRANTEE,
	db.database_name as OWNER,
	decode(auth.objtype, 1, t.table_name, '') as TABLE_NAME,
        c.column_name as COLUMN_NAME,
        cast (decode(auth.priv_id, 1, 'ALTER',
                             2, 'AUDIT',
                             3, 'COMMENT',
                             4, 'DELETE',
                             5, 'GRANT',
                             6, 'INDEX',
                             7, 'INSERT',
                             8, 'LOCK',
                             9, 'RENAME',
                             10, 'SELECT',
                             11, 'UPDATE',
                             12, 'REFERENCES',
                             13, 'EXECUTE',
                             14, 'CREATE',
                             15, 'FLASHBACK',
                             16, 'READ',
                             17, 'WRITE',
                             'OTHERS') as varchar(40)) as PRIVILEGE,
        decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as GRANTABLE
      FROM
        sys.all_virtual_objauth_agent auth,
        sys.ALL_VIRTUAL_COLUMN_REAL_AGENT c,
        sys.ALL_VIRTUAL_TABLE_REAL_AGENT t,
        sys.ALL_VIRTUAL_DATABASE_REAL_AGENT db,
        sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantor,
        sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantee
      WHERE
        auth.col_id = c.column_id and
        auth.obj_id = t.table_id and
        auth.objtype = 1 and
        auth.obj_id = c.table_id and
        db.database_id = t.database_id and
        u_grantor.user_id = auth.grantor_id and
        u_grantee.user_id = auth.grantee_id
        AND C.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND U_GRANTOR.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND c.column_id != 65535
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'ROLE_TAB_PRIVS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25146',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
      SELECT
        U_GRANTEE.USER_NAME AS ROLE,
        DB.DATABASE_NAME AS OWNER,
          CAST (DECODE(AUTH.OBJTYPE,11, SUBSTR(TABLE_NAME, 7 + INSTR(SUBSTR(TABLE_NAME, 7), '_')),
                        T.TABLE_NAME) AS VARCHAR(128)) AS TABLE_NAME,
        C.COLUMN_NAME AS COLUMN_NAME,
        CAST (DECODE(AUTH.PRIV_ID, 1, 'ALTER',
                             2, 'AUDIT',
                             3, 'COMMENT',
                             4, 'DELETE',
                             5, 'GRANT',
                             6, 'INDEX',
                             7, 'INSERT',
                             8, 'LOCK',
                             9, 'RENAME',
                             10, 'SELECT',
                             11, 'UPDATE',
                             12, 'REFERENCES',
                             13, 'EXECUTE',
                             14, 'CREATE',
                             15, 'FLASHBACK',
                             16, 'READ',
                             17, 'WRITE',
                             'OTHERS') AS VARCHAR(40)) AS PRIVILEGE,
        DECODE(AUTH.PRIV_OPTION, 0, 'NO', 1, 'YES', '') AS GRANTABLE
      FROM
        (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT
          WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) AUTH
        LEFT JOIN SYS.ALL_VIRTUAL_COLUMN_REAL_AGENT C
          ON AUTH.COL_ID = C.COLUMN_ID AND AUTH.OBJ_ID = C.TABLE_ID
            AND C.TENANT_ID = EFFECTIVE_TENANT_ID(),
        (SELECT TABLE_ID, TABLE_NAME, DATABASE_ID
            FROM SYS.ALL_VIRTUAL_TABLE_REAL_AGENT
            WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
         UNION ALL
         SELECT PACKAGE_ID AS TABLE_ID, PACKAGE_NAME AS TABLE_NAME, DATABASE_ID
           FROM SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT
           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
         UNION ALL
         SELECT ROUTINE_ID AS TABLE_ID, ROUTINE_NAME AS TABLE_NAME, DATABASE_ID
           FROM SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT
           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
         UNION ALL
         SELECT SEQUENCE_ID AS TABLE_ID, SEQUENCE_NAME AS TABLE_NAME, DATABASE_ID
           FROM SYS.ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT
           WHERE TENANT_ID = EFFECTIVE_TENANT_ID()
        ) T,
        SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB,
        SYS.ALL_VIRTUAL_USER_REAL_AGENT U_GRANTEE
      WHERE
        AUTH.OBJ_ID = T.TABLE_ID
         AND DB.DATABASE_ID = T.DATABASE_ID
         AND U_GRANTEE.USER_ID = AUTH.GRANTEE_ID
         AND U_GRANTEE.TYPE = 1
         AND DB.TENANT_ID = EFFECTIVE_TENANT_ID()
         AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'ROLE_SYS_PRIVS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25147',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
      select
	u.user_name as ROLE,
        CAST (DECODE(AUTH.PRIV_ID,
                1, 'CREATE SESSION',
                2, 'EXEMPT REDACT POLICY',
                3, 'SYSDBA',
                4, 'SYSOPER',
                5, 'SYSBACKUP',
                6, 'CREATE TABLE',
                7, 'CREATE ANY TABLE',
                8, 'ALTER ANY TABLE',
                9, 'BACKUP ANY TABLE',
                10, 'DROP ANY TABLE',
                11, 'LOCK ANY TABLE',
                12, 'COMMENT ANY TABLE',
                13, 'SELECT ANY TABLE',
                14, 'INSERT ANY TABLE',
                15, 'UPDATE ANY TABLE',
                16, 'DELETE ANY TABLE',
                17, 'FLASHBACK ANY TABLE',
                18, 'CREATE ROLE',
                19, 'DROP ANY ROLE',
                20, 'GRANT ANY ROLE',
                21, 'ALTER ANY ROLE',
                22, 'AUDIT ANY',
                23, 'GRANT ANY PRIVILEGE',
                24, 'GRANT ANY OBJECT PRIVILEGE',
                25, 'CREATE ANY INDEX',
                26, 'ALTER ANY INDEX',
                27, 'DROP ANY INDEX',
                28, 'CREATE ANY VIEW',
                29, 'DROP ANY VIEW',
                30, 'CREATE VIEW',
                31, 'SELECT ANY DICTIONARY',
                32, 'CREATE PROCEDURE',
                33, 'CREATE ANY PROCEDURE',
                34, 'ALTER ANY PROCEDURE',
                35, 'DROP ANY PROCEDURE',
                36, 'EXECUTE ANY PROCEDURE',
                37, 'CREATE SYNONYM',
                38, 'CREATE ANY SYNONYM',
                39, 'DROP ANY SYNONYM',
                40, 'CREATE PUBLIC SYNONYM',
                41, 'DROP PUBLIC SYNONYM',
                42, 'CREATE SEQUENCE',
                43, 'CREATE ANY SEQUENCE',
                44, 'ALTER ANY SEQUENCE',
                45, 'DROP ANY SEQUENCE',
                46, 'SELECT ANY SEQUENCE',
                47, 'CREATE TRIGGER',
                48, 'CREATE ANY TRIGGER',
                49, 'ALTER ANY TRIGGER',
                50, 'DROP ANY TRIGGER',
                51, 'CREATE PROFILE',
                52, 'ALTER PROFILE',
                53, 'DROP PROFILE',
                54, 'CREATE USER',
                55, 'BECOME USER',
                56, 'ALTER USER',
                57, 'DROP USER',
                58, 'CREATE TYPE',
                59, 'CREATE ANY TYPE',
                60, 'ALTER ANY TYPE',
                61, 'DROP ANY TYPE',
                62, 'EXECUTE ANY TYPE',
                63, 'UNDER ANY TYPE',
                64, 'PURGE DBA_RECYCLEBIN',
                65, 'CREATE ANY OUTLINE',
                66, 'ALTER ANY OUTLINE',
                67, 'DROP ANY OUTLINE',
                68, 'SYSKM',
                69, 'CREATE TABLESPACE',
                70, 'ALTER TABLESPACE',
                71, 'DROP TABLESPACE',
                72, 'SHOW PROCESS',
                73, 'ALTER SYSTEM',
                74, 'CREATE DATABASE LINK',
                75, 'CREATE PUBLIC DATABASE LINK',
                76, 'DROP DATABASE LINK',
                77, 'ALTER SESSION',
                78, 'ALTER DATABASE',
                'OTHER') AS VARCHAR(40)) AS PRIVILEGE ,
       	decode(auth.priv_option, 0, 'NO', 1, 'YES', '') as ADMIN_OPTION
      FROM
	sys.ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT auth,
	sys.ALL_VIRTUAL_USER_REAL_AGENT u
      WHERE
	auth.grantee_id = u.user_id and
  u.type = 1
  AND U.TENANT_ID = EFFECTIVE_TENANT_ID()
  AND AUTH.TENANT_ID = EFFECTIVE_TENANT_ID()
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'ROLE_ROLE_PRIVS',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25148',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
     select
	u_role.user_name as ROLE,
        u_grantee.user_name as GRANTED_ROLE,
        DECODE(R.ADMIN_OPTION, 0, 'NO', 1, 'YES', '') as ADMIN_OPTION
     FROM
	sys.ALL_VIRTUAL_USER_REAL_AGENT u_grantee,
	sys.ALL_VIRTUAL_USER_REAL_AGENT u_role,
	(select * from sys.all_virtual_tenant_role_grantee_map_agent
	 connect by prior role_id = grantee_id
                   and bitand(role_id, power(2,40) - 1) != 9
	 start with grantee_id = uid
              and bitand(role_id, power(2,40) - 1) != 9 ) r
     WHERE
	r.grantee_id = u_role.user_id and
	r.role_id = u_grantee.user_id and
	u_role.type = 1 and
  u_grantee.type = 1
  AND U_GRANTEE.TENANT_ID = EFFECTIVE_TENANT_ID()
  AND U_ROLE.TENANT_ID = EFFECTIVE_TENANT_ID()
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'DICTIONARY',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25149',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(TABLE_NAME AS VARCHAR2(128)) AS TABLE_NAME,
      CAST(CASE TABLE_NAME
        WHEN 'DBA_COL_PRIVS' THEN 'All grants on columns in the database'
        WHEN 'USER_COL_PRIVS' THEN 'Grants on columns for which the user is the owner, grantor or grantee'
        WHEN 'ALL_COL_PRIVS' THEN 'Grants on columns for which the user is the grantor, grantee, owner, or an enabled role or PUBLIC is the grantee'
        WHEN 'ROLE_TAB_PRIVS' THEN 'Table privileges granted to roles'
        WHEN 'ROLE_SYS_PRIVS' THEN 'System privileges granted to roles'
        WHEN 'ROLE_ROLE_PRIVS' THEN 'Roles which are granted to roles'
        WHEN 'DBA_SYNONYMS' THEN 'All synonyms in the database'
        WHEN 'DBA_OBJECTS' THEN 'All objects in the database'
        WHEN 'ALL_OBJECTS' THEN 'Objects accessible to the user'
        WHEN 'USER_OBJECTS' THEN 'Objects owned by the user'
        WHEN 'DBA_SEQUENCES' THEN 'Description of all SEQUENCEs in the database'
        WHEN 'ALL_SEQUENCES' THEN 'Description of SEQUENCEs accessible to the user'
        WHEN 'USER_SEQUENCES' THEN 'Description of the user''s own SEQUENCEs'
        WHEN 'DBA_USERS' THEN 'Information about all users of the database'
        WHEN 'ALL_USERS' THEN 'Information about all users of the database'
        WHEN 'ALL_SYNONYMS' THEN 'All synonyms for base objects accessible to the user and session'
        WHEN 'USER_SYNONYMS' THEN 'The user''s private synonyms'
        WHEN 'DBA_IND_COLUMNS' THEN 'COLUMNs comprising INDEXes on all TABLEs and CLUSTERs'
        WHEN 'ALL_IND_COLUMNS' THEN 'COLUMNs comprising INDEXes on accessible TABLES'
        WHEN 'USER_IND_COLUMNS' THEN 'COLUMNs comprising user''s INDEXes and INDEXes on user''s TABLES'
        WHEN 'DBA_CONSTRAINTS' THEN 'Constraint definitions on all tables'
        WHEN 'ALL_CONSTRAINTS' THEN 'Constraint definitions on accessible tables'
        WHEN 'USER_CONSTRAINTS' THEN 'Constraint definitions on user''s own tables'
        WHEN 'ALL_TAB_COLS' THEN 'Columns of user''s tables, views and clusters'
        WHEN 'DBA_TAB_COLS' THEN 'Columns of user''s tables, views and clusters'
        WHEN 'USER_TAB_COLS' THEN 'Columns of user''s tables, views and clusters'
        WHEN 'ALL_TAB_COLUMNS' THEN 'Columns of user''s tables, views and clusters'
        WHEN 'DBA_TAB_COLUMNS' THEN 'Columns of user''s tables, views and clusters'
        WHEN 'USER_TAB_COLUMNS' THEN 'Columns of user''s tables, views and clusters'
        WHEN 'ALL_TABLES' THEN 'Description of relational tables accessible to the user'
        WHEN 'DBA_TABLES' THEN 'Description of all relational tables in the database'
        WHEN 'USER_TABLES' THEN 'Description of the user''s own relational tables'
        WHEN 'DBA_TAB_COMMENTS' THEN 'Comments on all tables and views in the database'
        WHEN 'ALL_TAB_COMMENTS' THEN 'Comments on tables and views accessible to the user'
        WHEN 'USER_TAB_COMMENTS' THEN 'Comments on the tables and views owned by the user'
        WHEN 'DBA_COL_COMMENTS' THEN 'Comments on columns of all tables and views'
        WHEN 'ALL_COL_COMMENTS' THEN 'Comments on columns of accessible tables and views'
        WHEN 'USER_COL_COMMENTS' THEN 'Comments on columns of user''s tables and views'
        WHEN 'DBA_INDEXES' THEN 'Description for all indexes in the database'
        WHEN 'ALL_INDEXES' THEN 'Descriptions of indexes on tables accessible to the user'
        WHEN 'USER_INDEXES' THEN 'Description of the user''s own indexes'
        WHEN 'DBA_CONS_COLUMNS' THEN 'Information about accessible columns in constraint definitions'
        WHEN 'ALL_CONS_COLUMNS' THEN 'Information about accessible columns in constraint definitions'
        WHEN 'USER_CONS_COLUMNS' THEN 'Information about accessible columns in constraint definitions'
        WHEN 'USER_SEGMENTS' THEN 'Storage allocated for all database segments'
        WHEN 'DBA_SEGMENTS' THEN 'Storage allocated for all database segments'
        WHEN 'DBA_TYPES' THEN 'Description of all types in the database'
        WHEN 'ALL_TYPES' THEN 'Description of types accessible to the user'
        WHEN 'USER_TYPES' THEN 'Description of the user''s own types'
        WHEN 'DBA_TYPE_ATTRS' THEN 'Description of attributes of all types in the database'
        WHEN 'ALL_TYPE_ATTRS' THEN 'Description of attributes of types accessible to the user'
        WHEN 'USER_TYPE_ATTRS' THEN 'Description of attributes of the user''s own types'
        WHEN 'DBA_COLL_TYPES' THEN 'Description of all named collection types in the database'
        WHEN 'ALL_COLL_TYPES' THEN 'Description of named collection types accessible to the user'
        WHEN 'USER_COLL_TYPES' THEN 'Description of the user''s own named collection types'
        WHEN 'DBA_PROCEDURES' THEN 'Description of the dba functions/procedures/packages/types/triggers'
        WHEN 'DBA_ARGUMENTS' THEN 'All arguments for objects in the database'
        WHEN 'DBA_SOURCE' THEN 'Source of all stored objects in the database'
        WHEN 'ALL_PROCEDURES' THEN 'Functions/procedures/packages/types/triggers available to the user'
        WHEN 'ALL_ARGUMENTS' THEN 'Arguments in object accessible to the user'
        WHEN 'ALL_SOURCE' THEN 'Current source on stored objects that user is allowed to create'
        WHEN 'USER_PROCEDURES' THEN 'Description of the user functions/procedures/packages/types/triggers'
        WHEN 'USER_ARGUMENTS' THEN 'Arguments in object accessible to the user'
        WHEN 'USER_SOURCE' THEN 'Source of stored objects accessible to the user'
        WHEN 'ALL_ALL_TABLES' THEN 'Description of all object and relational tables accessible to the user'
        WHEN 'DBA_ALL_TABLES' THEN 'Description of all object and relational tables in the database'
        WHEN 'USER_ALL_TABLES' THEN 'Description of all object and relational tables owned by the user''s'
        WHEN 'DBA_PROFILES' THEN 'Display all profiles and their limits'
        WHEN 'ALL_MVIEW_COMMENTS' THEN 'Comments on materialized views accessible to the user'
        WHEN 'USER_MVIEW_COMMENTS' THEN 'Comments on materialized views owned by the user'
        WHEN 'DBA_MVIEW_COMMENTS' THEN 'Comments on all materialized views in the database'
        WHEN 'ALL_SCHEDULER_PROGRAM_ARGS' THEN 'All arguments of all scheduler programs visible to the user'
        WHEN 'ALL_SCHEDULER_JOB_ARGS' THEN 'All arguments with set values of all scheduler jobs in the database'
        WHEN 'DBA_SCHEDULER_JOB_ARGS' THEN 'All arguments with set values of all scheduler jobs in the database'
        WHEN 'USER_SCHEDULER_JOB_ARGS' THEN 'All arguments with set values of all scheduler jobs in the database'
        WHEN 'DBA_VIEWS' THEN 'Description of all views in the database'
        WHEN 'ALL_VIEWS' THEN 'Description of views accessible to the user'
        WHEN 'USER_VIEWS' THEN 'Description of the user''s own views'
        WHEN 'ALL_ERRORS' THEN 'Current errors on stored objects that user is allowed to create'
        WHEN 'USER_ERRORS' THEN 'Current errors on stored objects owned by the user'
        WHEN 'ALL_TYPE_METHODS' THEN 'Description of methods of types accessible to the user'
        WHEN 'DBA_TYPE_METHODS' THEN 'Description of methods of all types in the database'
        WHEN 'USER_TYPE_METHODS' THEN 'Description of methods of the user''s own types'
        WHEN 'ALL_METHOD_PARAMS' THEN 'Description of method parameters of types accessible to the user'
        WHEN 'DBA_METHOD_PARAMS' THEN 'Description of method parameters of all types in the database'
        WHEN 'USER_TABLESPACES' THEN 'Description of accessible tablespaces'
        WHEN 'DBA_IND_EXPRESSIONS' THEN 'FUNCTIONAL INDEX EXPRESSIONs on all TABLES and CLUSTERS'
        WHEN 'ALL_IND_EXPRESSIONS' THEN 'FUNCTIONAL INDEX EXPRESSIONs on accessible TABLES'
        WHEN 'DBA_ROLE_PRIVS' THEN 'Roles granted to users and roles'
        WHEN 'USER_ROLE_PRIVS' THEN 'Roles granted to current user'
        WHEN 'DBA_TAB_PRIVS' THEN 'All grants on objects in the database'
        WHEN 'ALL_TAB_PRIVS' THEN 'Grants on objects for which the user is the grantor, grantee, owner,'
        WHEN 'DBA_SYS_PRIVS' THEN 'System privileges granted to users and roles'
        WHEN 'USER_SYS_PRIVS' THEN 'System privileges granted to current user'
        WHEN 'AUDIT_ACTIONS' THEN 'Description table for audit trail action type codes.  Maps action type numbers to action type names'
        WHEN 'ALL_DEF_AUDIT_OPTS' THEN 'Auditing options for newly created objects'
        WHEN 'DBA_STMT_AUDIT_OPTS' THEN 'Describes current system auditing options across the system and by user'
        WHEN 'DBA_OBJ_AUDIT_OPTS' THEN 'Auditing options for all tables and views with atleast one option set'
        WHEN 'DBA_AUDIT_TRAIL' THEN 'All audit trail entries'
        WHEN 'USER_AUDIT_TRAIL' THEN 'Audit trail entries relevant to the user'
        WHEN 'DBA_AUDIT_EXISTS' THEN 'Lists audit trail entries produced by AUDIT NOT EXISTS and AUDIT EXISTS'
        WHEN 'DBA_AUDIT_STATEMENT' THEN 'Audit trail records concerning grant, revoke, audit, noaudit and alter system'
        WHEN 'USER_AUDIT_STATEMENT' THEN 'Audit trail records concerning  grant, revoke, audit, noaudit and alter system'
        WHEN 'DBA_AUDIT_OBJECT' THEN 'Audit trail records for statements concerning objects, specifically: table, cluster, view, index, sequence,  [public] database link, [public] synonym, procedure, trigger, rollback segment, tablespace, role, user'
        WHEN 'USER_AUDIT_OBJECT' THEN 'Audit trail records for statements concerning objects, specifically: table, cluster, view, index, sequence,  [public] database link, [public] synonym, procedure, trigger, rollback segment, tablespace, role, user'
        WHEN 'ALL_DEPENDENCIES' THEN 'Describes dependencies between procedures, packages,functions, package bodies, and triggers accessible to the current user,including dependencies on views created without any database links'
        WHEN 'DBA_DEPENDENCIES' THEN 'Describes all dependencies in the database between procedures,packages, functions, package bodies, and triggers, including dependencies on views created without any database links'
        WHEN 'USER_DEPENDENCIES' THEN 'Describes dependencies between procedures, packages, functions, package bodies, and triggers owned by the current user, including dependencies on views created without any database links'
        WHEN 'GV$INSTANCE' THEN 'Synonym for GV_$INSTANCE'
        WHEN 'V$INSTANCE' THEN 'Synonym for V_$INSTANCE'
        WHEN 'GV$SESSION_WAIT' THEN 'Synonym for GV_$SESSION_WAIT'
        WHEN 'V$SESSION_WAIT' THEN 'Synonym for V_$SESSION_WAIT'
        WHEN 'GV$SESSION_WAIT_HISTORY' THEN 'Synonym for GV_$SESSION_WAIT_HISTORY'
        WHEN 'V$SESSION_WAIT_HISTORY' THEN 'Synonym for V_$SESSION_WAIT_HISTORY'
        WHEN 'GV$SESSTAT' THEN 'Synonym for GV_$SESSTAT'
        WHEN 'V$SESSTAT' THEN 'Synonym for V_$SESSTAT'
        WHEN 'GV$SYSSTAT' THEN 'Synonym for GV_$SYSSTAT'
        WHEN 'V$SYSSTAT' THEN 'Synonym for V_$SYSSTAT'
        WHEN 'GV$SYSTEM_EVENT' THEN 'Synonym for GV_$SYSTEM_EVENT'
        WHEN 'V$SYSTEM_EVENT' THEN 'Synonym for V_$SYSTEM_EVENT'
        WHEN 'NLS_SESSION_PARAMETERS' THEN 'NLS parameters of the user session'
        WHEN 'NLS_DATABASE_PARAMETERS' THEN 'Permanent NLS parameters of the database'
        WHEN 'V$NLS_PARAMETERS' THEN 'Synonym for V_$NLS_PARAMETERS'
        WHEN 'V$VERSION' THEN 'Synonym for V_$VERSION'
        WHEN 'GV$SQL_WORKAREA' THEN 'Synonym for GV_$SQL_WORKAREA'
        WHEN 'V$SQL_WORKAREA' THEN 'Synonym for V_$SQL_WORKAREA'
        WHEN 'GV$SQL_WORKAREA_ACTIVE' THEN 'Synonym for GV_$SQL_WORKAREA_ACTIVE'
        WHEN 'V$SQL_WORKAREA_ACTIVE' THEN 'Synonym for V_$SQL_WORKAREA_ACTIVE'
        WHEN 'GV$SQL_WORKAREA_HISTOGRAM' THEN 'Synonym for GV_$SQL_WORKAREA_HISTOGRAM'
        WHEN 'V$SQL_WORKAREA_HISTOGRAM' THEN 'Synonym for V_$SQL_WORKAREA_HISTOGRAM'
        WHEN 'DICT' THEN 'Synonym for DICTIONARY'
        WHEN 'DICTIONARY' THEN 'Description of data dictionary tables and views'
        WHEN 'DBA_RECYCLEBIN' THEN 'Description of the Recyclebin view accessible to the user'
        WHEN 'USER_RECYCLEBIN' THEN 'User view of his recyclebin'
        WHEN 'V$TENANT_PX_WORKER_STAT' THEN ''
        WHEN 'GV$PS_STAT' THEN ''
        WHEN 'V$PS_STAT' THEN ''
        WHEN 'GV$PS_ITEM_INFO' THEN ''
        WHEN 'V$PS_ITEM_INFO' THEN ''
        WHEN 'GV$OB_SQL_WORKAREA_MEMORY_INFO' THEN ''
        WHEN 'V$OB_SQL_WORKAREA_MEMORY_INFO' THEN ''
        WHEN 'DBA_PART_KEY_COLUMNS' THEN ''
        WHEN 'ALL_PART_KEY_COLUMNS' THEN ''
        WHEN 'USER_PART_KEY_COLUMNS' THEN ''
        WHEN 'DBA_SUBPART_KEY_COLUMNS' THEN ''
        WHEN 'ALL_SUBPART_KEY_COLUMNS' THEN ''
        WHEN 'USER_SUBPART_KEY_COLUMNS' THEN ''
        WHEN 'ALL_TAB_PARTITIONS' THEN ''
        WHEN 'ALL_TAB_SUBPARTITIONS' THEN ''
        WHEN 'ALL_PART_TABLES' THEN ''
        WHEN 'DBA_PART_TABLES' THEN ''
        WHEN 'USER_PART_TABLES' THEN ''
        WHEN 'DBA_TAB_PARTITIONS' THEN ''
        WHEN 'USER_TAB_PARTITIONS' THEN ''
        WHEN 'DBA_TAB_SUBPARTITIONS' THEN ''
        WHEN 'USER_TAB_SUBPARTITIONS' THEN ''
        WHEN 'DBA_SUBPARTITION_TEMPLATES' THEN ''
        WHEN 'ALL_SUBPARTITION_TEMPLATES' THEN ''
        WHEN 'USER_SUBPARTITION_TEMPLATES' THEN ''
        WHEN 'DBA_PART_INDEXES' THEN ''
        WHEN 'ALL_PART_INDEXES' THEN ''
        WHEN 'USER_PART_INDEXES' THEN ''
        WHEN 'ALL_TAB_COLS_V$' THEN ''
        WHEN 'DBA_TAB_COLS_V$' THEN ''
        WHEN 'USER_TAB_COLS_V$' THEN ''
        WHEN 'USER_PROFILES' THEN ''
        WHEN 'ALL_PROFILES' THEN ''
        WHEN 'DBA_SCHEDULER_PROGRAM_ARGS' THEN ''
        WHEN 'USER_SCHEDULER_PROGRAM_ARGS' THEN ''
        WHEN 'USER_IND_EXPRESSIONS' THEN ''
        WHEN 'DBA_ERRORS' THEN ''
        WHEN 'USER_METHOD_PARAMS' THEN ''
        WHEN 'DBA_TABLESPACES' THEN ''
        WHEN 'ALL_IND_PARTITIONS' THEN ''
        WHEN 'USER_IND_PARTITIONS' THEN ''
        WHEN 'DBA_IND_PARTITIONS' THEN ''
        WHEN 'DBA_IND_SUBPARTITIONS' THEN ''
        WHEN 'ALL_IND_SUBPARTITIONS' THEN ''
        WHEN 'USER_IND_SUBPARTITIONS' THEN ''
        WHEN 'DBA_ROLES' THEN ''
        WHEN 'USER_TAB_PRIVS' THEN ''
        WHEN 'STMT_AUDIT_OPTION_MAP' THEN ''
        WHEN 'GV$OUTLINE' THEN ''
        WHEN 'GV$SQL_AUDIT' THEN ''
        WHEN 'V$SQL_AUDIT' THEN ''
        WHEN 'DBA_AUDIT_SESSION' THEN ''
        WHEN 'USER_AUDIT_SESSION' THEN ''
        WHEN 'GV$PLAN_CACHE_PLAN_STAT' THEN ''
        WHEN 'V$PLAN_CACHE_PLAN_STAT' THEN ''
        WHEN 'GV$PLAN_CACHE_PLAN_EXPLAIN' THEN ''
        WHEN 'V$PLAN_CACHE_PLAN_EXPLAIN' THEN ''
        WHEN 'GV$MEMSTORE' THEN ''
        WHEN 'V$MEMSTORE' THEN ''
        WHEN 'GV$MEMSTORE_INFO' THEN ''
        WHEN 'V$MEMSTORE_INFO' THEN ''
        WHEN 'GV$MEMORY' THEN ''
        WHEN 'V$MEMORY' THEN ''
        WHEN 'GV$SERVER_MEMSTORE' THEN ''
        WHEN 'GV$TENANT_MEMSTORE_ALLOCATOR_INFO' THEN ''
        WHEN 'V$TENANT_MEMSTORE_ALLOCATOR_INFO' THEN ''
        WHEN 'GV$PLAN_CACHE_STAT' THEN ''
        WHEN 'V$PLAN_CACHE_STAT' THEN ''
        WHEN 'GV$CONCURRENT_LIMIT_SQL' THEN ''
        WHEN 'NLS_INSTANCE_PARAMETERS' THEN ''
        WHEN 'GV$TENANT_PX_WORKER_STAT' THEN ''
        ELSE NULL END AS VARCHAR2(4000)) AS COMMENTS
    FROM SYS.ALL_VIRTUAL_TABLE_SYS_AGENT
    WHERE BITAND(TABLE_ID,  1099511627775) > 25000
        AND BITAND(TABLE_ID, 1099511627775) <= 30000
        AND TABLE_TYPE = 1
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DICT',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25150',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT * FROM SYS.DICTIONARY
""".replace("\n", " ")
)

def_table_schema(
  table_name='ALL_TRIGGERS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25151',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
SELECT DB1.DATABASE_NAME AS OWNER,
       TRG.TRIGGER_NAME AS TRIGGER_NAME,
       CAST(DECODE(BITAND(TRG.TIMING_POINTS, 30),
                   4, 'BEFORE EACH ROW',
                   8, 'AFTER EACH ROW')
            AS VARCHAR2(16)) AS TRIGGER_TYPE,
       CAST(DECODE(TRG.TRIGGER_EVENTS,
                   2, 'INSERT',
                   4, 'UPDATE',
                   8, 'DELETE',
                   2 + 4, 'INSERT OR UPDATE',
                   2 + 8, 'INSERT OR DELETE',
                   4 + 8, 'UPDATE OR DELETE',
                   2 + 4 + 8, 'INSERT OR UPDATE OR DELETE')
            AS VARCHAR2(246)) AS TRIGGERING_EVENT,
       DB2.DATABASE_NAME AS TABLE_OWNER,
       CAST(DECODE(TRG.BASE_OBJECT_TYPE,
                   5, 'TABLE')
            AS VARCHAR2(18)) AS BASE_OBJECT_TYPE,
       TBL.TABLE_NAME AS TABLE_NAME,
       CAST(NULL AS VARCHAR2(4000)) AS COLUMN_NAME,
       CAST(CONCAT('REFERENCING', CONCAT(CONCAT(' NEW AS ', REF_NEW_NAME), CONCAT(' OLD AS ', REF_OLD_NAME)))
            AS VARCHAR2(422)) AS REFERENCING_NAMES,
       WHEN_CONDITION AS WHEN_CLAUSE,
       CAST(decode(BITAND(TRG.trigger_flags, 1), 1, 'ENABLED', 'DISABLED') AS VARCHAR2(8)) AS STATUS,
       TRIGGER_BODY AS DESCRIPTION,
       CAST('PL/SQL' AS VARCHAR2(11)) AS ACTION_TYPE,
       TRIGGER_BODY AS TRIGGER_BODY,
       CAST('NO' AS VARCHAR2(7)) AS CROSSEDITION,
       CAST('NO' AS VARCHAR2(3)) AS BEFORE_STATEMENT,
       CAST('NO' AS VARCHAR2(3)) AS BEFORE_ROW,
       CAST('NO' AS VARCHAR2(3)) AS AFTER_ROW,
       CAST('NO' AS VARCHAR2(3)) AS AFTER_STATEMENT,
       CAST('NO' AS VARCHAR2(3)) AS INSTEAD_OF_ROW,
       CAST('YES' AS VARCHAR2(3)) AS FIRE_ONCE,
       CAST('NO' AS VARCHAR2(3)) AS APPLY_SERVER_ONLY
  FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT TRG
       INNER JOIN
       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB1
       ON TRG.DATABASE_ID = DB1.DATABASE_ID
          AND TRG.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND DB1.TENANT_ID = EFFECTIVE_TENANT_ID()
          AND (TRG.DATABASE_ID = USERENV('SCHEMAID')
              OR USER_CAN_ACCESS_OBJ(1, abs(nvl(TRG.BASE_OBJECT_ID,0)), TRG.DATABASE_ID) = 1)
       LEFT JOIN
       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TBL
       ON TRG.BASE_OBJECT_ID = TBL.TABLE_ID
        AND TBL.TENANT_ID = EFFECTIVE_TENANT_ID()
       INNER JOIN
       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB2
       ON TBL.DATABASE_ID = DB2.DATABASE_ID
        AND DB2.TENANT_ID = EFFECTIVE_TENANT_ID()
""".replace("\n", " ")
)

def_table_schema(
  table_name='DBA_TRIGGERS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25152',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
SELECT * FROM ALL_TRIGGERS
""".replace("\n", " ")
)

def_table_schema(
  table_name='USER_TRIGGERS',
  database_id='OB_ORA_SYS_DATABASE_ID',
  table_id='25153',
  table_type='SYSTEM_VIEW',
  rowkey_columns=[],
  normal_columns=[],
  gm_columns=[],
  in_tenant_space=True,
  view_definition="""
SELECT TRG.TRIGGER_NAME AS TRIGGER_NAME,
       CAST(DECODE(BITAND(TRG.TIMING_POINTS, 30),
                   4, 'BEFORE EACH ROW',
                   8, 'AFTER EACH ROW')
            AS VARCHAR2(16)) AS TRIGGER_TYPE,
       CAST(DECODE(TRG.TRIGGER_EVENTS,
                   2, 'INSERT',
                   4, 'UPDATE',
                   8, 'DELETE',
                   2 + 4, 'INSERT OR UPDATE',
                   2 + 8, 'INSERT OR DELETE',
                   4 + 8, 'UPDATE OR DELETE',
                   2 + 4 + 8, 'INSERT OR UPDATE OR DELETE')
            AS VARCHAR2(246)) AS TRIGGERING_EVENT,
       DB2.DATABASE_NAME AS TABLE_OWNER,
       CAST(DECODE(TRG.BASE_OBJECT_TYPE,
                   5, 'TABLE')
            AS VARCHAR2(18)) AS BASE_OBJECT_TYPE,
       TBL.TABLE_NAME AS TABLE_NAME,
       CAST(NULL AS VARCHAR2(4000)) AS COLUMN_NAME,
       CAST(CONCAT('REFERENCING', CONCAT(CONCAT(' NEW AS ', REF_NEW_NAME), CONCAT(' OLD AS ', REF_OLD_NAME)))
            AS VARCHAR2(422)) AS REFERENCING_NAMES,
       WHEN_CONDITION AS WHEN_CLAUSE,
       CAST(decode(BITAND(TRG.trigger_flags, 1), 1, 'ENABLED', 'DISABLED') AS VARCHAR2(8)) AS STATUS,
       TRIGGER_BODY AS DESCRIPTION,
       CAST('PL/SQL' AS VARCHAR2(11)) AS ACTION_TYPE,
       TRIGGER_BODY AS TRIGGER_BODY,
       CAST('NO' AS VARCHAR2(7)) AS CROSSEDITION,
       CAST('NO' AS VARCHAR2(3)) AS BEFORE_STATEMENT,
       CAST('NO' AS VARCHAR2(3)) AS BEFORE_ROW,
       CAST('NO' AS VARCHAR2(3)) AS AFTER_ROW,
       CAST('NO' AS VARCHAR2(3)) AS AFTER_STATEMENT,
       CAST('NO' AS VARCHAR2(3)) AS INSTEAD_OF_ROW,
       CAST('YES' AS VARCHAR2(3)) AS FIRE_ONCE,
       CAST('NO' AS VARCHAR2(3)) AS APPLY_SERVER_ONLY
  FROM (SELECT * FROM SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT
          WHERE TENANT_ID = EFFECTIVE_TENANT_ID())TRG
       LEFT JOIN
       SYS.ALL_VIRTUAL_TABLE_REAL_AGENT TBL
       ON TRG.BASE_OBJECT_ID = TBL.TABLE_ID
        AND TBL.TENANT_ID = EFFECTIVE_TENANT_ID()
       INNER JOIN
       SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT DB2
       ON TBL.DATABASE_ID = DB2.DATABASE_ID
        AND DB2.TENANT_ID = EFFECTIVE_TENANT_ID()
 WHERE TRG.DATABASE_ID = USERENV('SCHEMAID')
""".replace("\n", " ")
)

# all/dba/user DEPENDENCY
def_table_schema(
  table_name      = 'ALL_DEPENDENCIES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25154',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
     o.OWNER AS OWNER,
     o.OBJECT_NAME AS NAME,
     o.OBJECT_TYPE AS TYPE,
     ro.REFERENCED_OWNER AS REFERENCED_OWNER,
     ro.REFERENCED_NAME AS REFERENCED_NAME,
     DECODE(ro.REFERENCED_TYPE, NULL, ' NON-EXISTENT', ro.REFERENCED_TYPE) AS REFERENCED_TYPE,
     CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,
     CAST(DECODE(BITAND(o.PROPERTY, 3), 2, 'REF', 'HARD') AS VARCHAR2(4)) AS DEPENDENCY_TYPE
     FROM (select
            OWNER,
            OBJECT_NAME,
            OBJECT_TYPE,
            REF_OBJ_NAME,
            ref_obj_type,
            dep_obj_id,
            dep_obj_type,
            dep_order,
            property
            from SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d
            WHERE CAST(UPPER(decode(d.dep_obj_type,
                      1, 'TABLE',
                      2, 'SEQUENCE',
                      3, 'PACKAGE',
                      4, 'TYPE',
                      5, 'PACKAGE BODY',
                      6, 'TYPE BODY',
                      7, 'TRIGGER',
                      8, 'VIEW',
                      9, 'FUNCTION',
                      10, 'DIRECTORY',
                      11, 'INDEX',
                      12, 'PROCEDURE',
                      13, 'SYNONYM',
                'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.DEP_OBJ_ID = o.OBJECT_ID) o
                LEFT OUTER JOIN
                (SELECT DISTINCT
                  CAST(OWNER AS VARCHAR2(128)) AS REFERENCED_OWNER,
                  CAST(OBJECT_NAME AS VARCHAR2(128)) AS REFERENCED_NAME,
                  CAST(OBJECT_TYPE AS VARCHAR2(18)) AS REFERENCED_TYPE,
                  dep_obj_id,
                  dep_obj_type,
                  dep_order FROM
                  SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d
                  WHERE CAST(UPPER(decode(d.ref_obj_type,
                      1, 'TABLE',
                      2, 'SEQUENCE',
                      3, 'PACKAGE',
                      4, 'TYPE',
                      5, 'PACKAGE BODY',
                      6, 'TYPE BODY',
                      7, 'TRIGGER',
                      8, 'VIEW',
                      9, 'FUNCTION',
                      10, 'DIRECTORY',
                      11, 'INDEX',
                      12, 'PROCEDURE',
                      13, 'SYNONYM',
                      'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.REF_OBJ_ID = o.OBJECT_ID) ro
                    on ro.dep_obj_id = o.dep_obj_id AND ro.dep_obj_type = o.dep_obj_type
                      AND ro.dep_order = o.dep_order
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_DEPENDENCIES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25155',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
     o.OWNER AS OWNER,
     o.OBJECT_NAME AS NAME,
     o.OBJECT_TYPE AS TYPE,
     ro.REFERENCED_OWNER AS REFERENCED_OWNER,
     ro.REFERENCED_NAME AS REFERENCED_NAME,
     DECODE(ro.REFERENCED_TYPE, NULL, ' NON-EXISTENT', ro.REFERENCED_TYPE) AS REFERENCED_TYPE,
     CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,
     CAST(DECODE(BITAND(o.PROPERTY, 3), 2, 'REF', 'HARD') AS VARCHAR2(4)) AS DEPENDENCY_TYPE
     FROM (select
            OWNER,
            OBJECT_NAME,
            OBJECT_TYPE,
            REF_OBJ_NAME,
            ref_obj_type,
            dep_obj_id,
            dep_obj_type,
            dep_order,
            property
            from SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d
            WHERE CAST(UPPER(decode(d.dep_obj_type,
                      1, 'TABLE',
                      2, 'SEQUENCE',
                      3, 'PACKAGE',
                      4, 'TYPE',
                      5, 'PACKAGE BODY',
                      6, 'TYPE BODY',
                      7, 'TRIGGER',
                      8, 'VIEW',
                      9, 'FUNCTION',
                      10, 'DIRECTORY',
                      11, 'INDEX',
                      12, 'PROCEDURE',
                      13, 'SYNONYM',
                'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.DEP_OBJ_ID = o.OBJECT_ID) o
                LEFT OUTER JOIN
                (SELECT DISTINCT
                  CAST(OWNER AS VARCHAR2(128)) AS REFERENCED_OWNER,
                  CAST(OBJECT_NAME AS VARCHAR2(128)) AS REFERENCED_NAME,
                  CAST(OBJECT_TYPE AS VARCHAR2(18)) AS REFERENCED_TYPE,
                  dep_obj_id,
                  dep_obj_type,
                  dep_order FROM
                  SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d
                  WHERE CAST(UPPER(decode(d.ref_obj_type,
                      1, 'TABLE',
                      2, 'SEQUENCE',
                      3, 'PACKAGE',
                      4, 'TYPE',
                      5, 'PACKAGE BODY',
                      6, 'TYPE BODY',
                      7, 'TRIGGER',
                      8, 'VIEW',
                      9, 'FUNCTION',
                      10, 'DIRECTORY',
                      11, 'INDEX',
                      12, 'PROCEDURE',
                      13, 'SYNONYM',
                      'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.REF_OBJ_ID = o.OBJECT_ID) ro
                    on ro.dep_obj_id = o.dep_obj_id AND ro.dep_obj_type = o.dep_obj_type
                      AND ro.dep_order = o.dep_order
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_DEPENDENCIES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25156',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
     o.OBJECT_NAME AS NAME,
     o.OBJECT_TYPE AS TYPE,
     ro.REFERENCED_OWNER AS REFERENCED_OWNER,
     ro.REFERENCED_NAME AS REFERENCED_NAME,
     DECODE(ro.REFERENCED_TYPE, NULL, ' NON-EXISTENT', ro.REFERENCED_TYPE) AS REFERENCED_TYPE,
     CAST(NULL AS VARCHAR2(128)) AS REFERENCED_LINK_NAME,
     CAST(USERENV('SCHEMAID') AS NUMBER) AS SCHEMAID,
     CAST(DECODE(BITAND(o.PROPERTY, 3), 2, 'REF', 'HARD') AS VARCHAR2(4)) AS DEPENDENCY_TYPE
     FROM (select
            OWNER,
            OBJECT_NAME,
            OBJECT_TYPE,
            REF_OBJ_NAME,
            ref_obj_type,
            dep_obj_id,
            dep_obj_type,
            dep_order,
            property
            from SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d
            WHERE CAST(UPPER(decode(d.dep_obj_type,
                      1, 'TABLE',
                      2, 'SEQUENCE',
                      3, 'PACKAGE',
                      4, 'TYPE',
                      5, 'PACKAGE BODY',
                      6, 'TYPE BODY',
                      7, 'TRIGGER',
                      8, 'VIEW',
                      9, 'FUNCTION',
                      10, 'DIRECTORY',
                      11, 'INDEX',
                      12, 'PROCEDURE',
                      13, 'SYNONYM',
                'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.DEP_OBJ_ID = o.OBJECT_ID) o
                LEFT OUTER JOIN
                (SELECT DISTINCT
                  CAST(OWNER AS VARCHAR2(128)) AS REFERENCED_OWNER,
                  CAST(OBJECT_NAME AS VARCHAR2(128)) AS REFERENCED_NAME,
                  CAST(OBJECT_TYPE AS VARCHAR2(18)) AS REFERENCED_TYPE,
                  dep_obj_id,
                  dep_obj_type,
                  dep_order FROM
                  SYS.ALL_OBJECTS o, SYS.ALL_VIRTUAL_DEPENDENCY_AGENT d
                  WHERE CAST(UPPER(decode(d.ref_obj_type,
                      1, 'TABLE',
                      2, 'SEQUENCE',
                      3, 'PACKAGE',
                      4, 'TYPE',
                      5, 'PACKAGE BODY',
                      6, 'TYPE BODY',
                      7, 'TRIGGER',
                      8, 'VIEW',
                      9, 'FUNCTION',
                      10, 'DIRECTORY',
                      11, 'INDEX',
                      12, 'PROCEDURE',
                      13, 'SYNONYM',
                      'MAXTYPE')) AS VARCHAR2(23)) = o.OBJECT_TYPE AND d.REF_OBJ_ID = o.OBJECT_ID) ro
                    on ro.dep_obj_id = o.dep_obj_id AND ro.dep_obj_type = o.dep_obj_type
                      AND ro.dep_order = o.dep_order
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_RSRC_PLANS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25157',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT /*+ READ_CONSISTENCY(WEAK) */
      CAST(NULL AS NUMBER) AS PLAN_ID,
      PLAN,
      CAST(NULL AS NUMBER) AS NUM_PLAN_DIRECTIVES,
      CAST(NULL AS VARCHAR2(128)) AS CPU_METHOD,
      CAST(NULL AS VARCHAR2(128)) AS MGMT_METHOD,
      CAST(NULL AS VARCHAR2(128)) AS ACTIVE_SESS_POOL_MTH,
      CAST(NULL AS VARCHAR2(128)) AS PARALLEL_DEGREE_LIMIT_MTH,
      CAST(NULL AS VARCHAR2(128)) AS QUEUING_MTH,
      CAST(NULL AS VARCHAR2(3)) AS SUB_PLAN,
      COMMENTS,
      CAST(NULL AS VARCHAR2(128)) AS STATUS,
      CAST(NULL AS VARCHAR2(3)) AS MANDATORY
    FROM
       SYS.ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_RSRC_PLAN_DIRECTIVES',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25158',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT /*+ READ_CONSISTENCY(WEAK) */
      PLAN,
      GROUP_OR_SUBPLAN,
      CAST(NULL AS VARCHAR2(14)) AS TYPE,
      CAST(NULL AS NUMBER) AS CPU_P1,
      CAST(NULL AS NUMBER) AS CPU_P2,
      CAST(NULL AS NUMBER) AS CPU_P3,
      CAST(NULL AS NUMBER) AS CPU_P4,
      CAST(NULL AS NUMBER) AS CPU_P5,
      CAST(NULL AS NUMBER) AS CPU_P6,
      CAST(NULL AS NUMBER) AS CPU_P7,
      CAST(NULL AS NUMBER) AS CPU_P8,
      MGMT_P1,
      CAST(NULL AS NUMBER) AS MGMT_P2,
      CAST(NULL AS NUMBER) AS MGMT_P3,
      CAST(NULL AS NUMBER) AS MGMT_P4,
      CAST(NULL AS NUMBER) AS MGMT_P5,
      CAST(NULL AS NUMBER) AS MGMT_P6,
      CAST(NULL AS NUMBER) AS MGMT_P7,
      CAST(NULL AS NUMBER) AS MGMT_P8,
      CAST(NULL AS NUMBER) AS ACTIVE_SESS_POOL_P1,
      CAST(NULL AS NUMBER) AS QUEUEING_P1,
      CAST(NULL AS NUMBER) AS PARALLEL_TARGET_PERCENTAGE,
      CAST(NULL AS NUMBER) AS PARALLEL_DEGREE_LIMIT_P1,
      CAST(NULL AS VARCHAR2(128)) AS SWITCH_GROUP,
      CAST(NULL AS VARCHAR2(5)) AS SWITCH_FOR_CALL,
      CAST(NULL AS NUMBER) AS SWITCH_TIME,
      CAST(NULL AS NUMBER) AS SWITCH_IO_MEGABYTES,
      CAST(NULL AS NUMBER) AS SWITCH_IO_REQS,
      CAST(NULL AS VARCHAR2(5)) AS SWITCH_ESTIMATE,
      CAST(NULL AS NUMBER) AS MAX_EST_EXEC_TIME,
      CAST(NULL AS NUMBER) AS UNDO_POOL,
      CAST(NULL AS NUMBER) AS MAX_IDLE_TIME,
      CAST(NULL AS NUMBER) AS MAX_IDLE_BLOCKER_TIME,
      CAST(NULL AS NUMBER) AS MAX_UTILIZATION_LIMIT,
      CAST(NULL AS NUMBER) AS PARALLEL_QUEUE_TIMEOUT,
      CAST(NULL AS NUMBER) AS SWITCH_TIME_IN_CALL,
      CAST(NULL AS NUMBER) AS SWITCH_IO_LOGICAL,
      CAST(NULL AS NUMBER) AS SWITCH_ELAPSED_TIME,
      CAST(NULL AS NUMBER) AS PARALLEL_SERVER_LIMIT,
      UTILIZATION_LIMIT,
      CAST(NULL AS VARCHAR2(12)) AS PARALLEL_STMT_CRITICAL,
      CAST(NULL AS NUMBER) AS SESSION_PGA_LIMIT,
      CAST(NULL AS VARCHAR2(6)) AS PQ_TIMEOUT_ACTION,
      COMMENTS,
      CAST(NULL AS VARCHAR2(128)) AS STATUS,
      CAST('YES' AS VARCHAR2(3)) AS MANDATORY
    FROM
       SYS.ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_RSRC_GROUP_MAPPINGS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25159',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT /*+ READ_CONSISTENCY(WEAK) */
      ATTRIBUTE,
      VALUE,
      CONSUMER_GROUP,
      CAST(NULL AS VARCHAR2(128)) AS STATUS
    FROM
       SYS.ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_RECYCLEBIN',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25160',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT OWNER, OBJECT_NAME, ORIGINAL_NAME, OPERATION, TYPE, CAST(TABLESPACE_NAME AS VARCHAR2(30)) AS TS_NAME, CREATETIME, DROPTIME, DROPSCN, PARTITION_NAME, CAN_UNDROP, CAN_PURGE, RELATED, BASE_OBJECT, PURGE_OBJECT, SPACE
    FROM
      (SELECT
        CAST(B.DATABASE_NAME AS VARCHAR2(128)) AS OWNER,
        CAST(A.OBJECT_NAME AS VARCHAR2(128)) AS OBJECT_NAME,
        CAST(A.ORIGINAL_NAME AS VARCHAR2(128)) AS ORIGINAL_NAME,
        CAST(NULL AS VARCHAR2(9)) AS OPERATION,
        CAST(CASE A.TYPE
             WHEN 1 THEN 'TABLE'
             WHEN 2 THEN 'NORMAL INDEX'
             WHEN 3 THEN 'VIEW'
             WHEN 4 THEN 'DATABASE'
             WHEN 5 THEN 'AUX_VP'
             WHEN 6 THEN 'TRIGGER'
             ELSE NULL END AS VARCHAR2(25)) AS TYPE,
        CAST(NULL AS VARCHAR2(30)) AS TS_NAME,
        CAST(C.GMT_CREATE AS VARCHAR(30)) AS CREATETIME,
        CAST(C.GMT_MODIFIED AS VARCHAR(30)) AS DROPTIME,
        CAST(NULL AS NUMBER) AS DROPSCN,
        CAST(NULL AS VARCHAR2(128)) AS PARTITION_NAME,
        CAST('YES' AS VARCHAR2(3)) AS CAN_UNDROP,
        CAST('YES' AS VARCHAR2(3)) AS CAN_PURGE,
        CAST(NULL AS NUMBER) AS RELATED,
        CAST(NULL AS NUMBER) AS BASE_OBJECT,
        CAST(NULL AS NUMBER) AS PURGE_OBJECT,
        CAST(NULL AS NUMBER) AS SPACE,
        C.TABLE_ID AS TABLE_ID,
        C.TABLESPACE_ID AS TABLESPACE_ID
        FROM SYS.ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT A, SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT B, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT C
        WHERE A.DATABASE_ID = B.DATABASE_ID AND A.TABLE_ID = C.TABLE_ID AND A.TENANT_ID = EFFECTIVE_TENANT_ID() AND B.TENANT_ID = EFFECTIVE_TENANT_ID() AND C.TENANT_ID = EFFECTIVE_TENANT_ID()) LEFT_TABLE
      LEFT JOIN
        (SELECT TABLESPACE_NAME, TABLESPACE_ID FROM SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT) RIGHT_TABLE
      ON LEFT_TABLE.TABLESPACE_ID = RIGHT_TABLE.TABLESPACE_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'USER_RECYCLEBIN',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25161',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT OBJECT_NAME, ORIGINAL_NAME, OPERATION, TYPE, CAST(TABLESPACE_NAME AS VARCHAR2(30)) AS TS_NAME, CREATETIME, DROPTIME, DROPSCN, PARTITION_NAME, CAN_UNDROP, CAN_PURGE, RELATED, BASE_OBJECT, PURGE_OBJECT, SPACE
    FROM
      (SELECT
        CAST(A.OBJECT_NAME AS VARCHAR2(128)) AS OBJECT_NAME,
        CAST(A.ORIGINAL_NAME AS VARCHAR2(128)) AS ORIGINAL_NAME,
        CAST(NULL AS VARCHAR2(9)) AS OPERATION,
        CAST(CASE A.TYPE
             WHEN 1 THEN 'TABLE'
             WHEN 2 THEN 'NORMAL INDEX'
             WHEN 3 THEN 'VIEW'
             WHEN 4 THEN 'DATABASE'
             WHEN 5 THEN 'AUX_VP'
             WHEN 6 THEN 'TRIGGER'
             ELSE NULL END AS VARCHAR2(25)) AS TYPE,
        CAST(NULL AS VARCHAR2(30)) AS TS_NAME,
        CAST(C.GMT_CREATE AS VARCHAR(30)) AS CREATETIME,
        CAST(C.GMT_MODIFIED AS VARCHAR(30)) AS DROPTIME,
        CAST(NULL AS NUMBER) AS DROPSCN,
        CAST(NULL AS VARCHAR2(128)) AS PARTITION_NAME,
        CAST('YES' AS VARCHAR2(3)) AS CAN_UNDROP,
        CAST('YES' AS VARCHAR2(3)) AS CAN_PURGE,
        CAST(NULL AS NUMBER) AS RELATED,
        CAST(NULL AS NUMBER) AS BASE_OBJECT,
        CAST(NULL AS NUMBER) AS PURGE_OBJECT,
        CAST(NULL AS NUMBER) AS SPACE,
        C.TABLE_ID AS TABLE_ID,
        C.TABLESPACE_ID AS TABLESPACE_ID
        FROM SYS.ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT A, SYS.ALL_VIRTUAL_TABLE_REAL_AGENT C WHERE A.TABLE_ID = C.TABLE_ID AND A.DATABASE_ID = USERENV('SCHEMAID') AND A.TENANT_ID = EFFECTIVE_TENANT_ID() AND C.TENANT_ID = EFFECTIVE_TENANT_ID()) LEFT_TABLE
      LEFT JOIN
        (SELECT TABLESPACE_NAME, TABLESPACE_ID FROM SYS.ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT WHERE TENANT_ID = EFFECTIVE_TENANT_ID()) RIGHT_TABLE
      ON LEFT_TABLE.TABLESPACE_ID = RIGHT_TABLE.TABLESPACE_ID
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'DBA_RSRC_CONSUMER_GROUPS',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '25162',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT /*+ READ_CONSISTENCY(WEAK) */
      CONSUMER_GROUP_ID,
      CONSUMER_GROUP,
      CAST(NULL AS VARCHAR2(128)) AS CPU_METHOD,
      CAST(NULL AS VARCHAR2(128)) AS MGMT_METHOD,
      CAST(NULL AS VARCHAR2(3)) AS INTERNAL_USE,
      COMMENTS,
      CAST(NULL AS VARCHAR2(128)) AS CATEGORY,
      CAST(NULL AS VARCHAR2(128)) AS STATUS,
      CAST(NULL AS VARCHAR2(3)) AS MANDATORY
    FROM
       SYS.ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT
""".replace("\n", " ")
)

#### End Data Dictionary View
################################################################################

################################################################################
#### Performance View (28000, 30000]
################################################################################
def_table_schema(
  table_name      = 'GV$OUTLINE',
  name_postfix = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28001',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT tenant_id  TENANT_ID,
                          database_id  DATABASE_ID,
                          outline_id OUTLINE_ID,
                          database_name DATABASE_NAME,
                          outline_name OUTLINE_NAME,
                          visible_signature VISIBLE_SIGNATURE,
                          sql_text SQL_TEXT,
                          outline_target OUTLINE_TARGET,
                          outline_sql OUTLINE_SQL
                   from SYS.TENANT_VIRTUAL_OUTLINE_AGENT
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$SQL_AUDIT',
  name_postfix = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28002',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT
                         svr_ip SVR_IP,
                         svr_port SVR_PORT,
                         request_id REQUEST_ID,
                         execution_id SQL_EXEC_ID,
                         trace_id TRACE_ID,
                         session_id SID,
                         client_ip CLIENT_IP,
                         client_port CLIENT_PORT,
                         tenant_id TENANT_ID,
                         effective_tenant_id EFFECTIVE_TENANT_ID,
                         tenant_name TENANT_NAME,
                         user_id USER_ID,
                         user_name USER_NAME,
                         user_group as USER_GROUP,
                         user_client_ip as USER_CLIENT_IP,
                         db_id DB_ID,
                         db_name DB_NAME,
                         sql_id SQL_ID,
                         query_sql QUERY_SQL,
                         plan_id PLAN_ID,
                         affected_rows AFFECTED_ROWS,
                         return_rows RETURN_ROWS,
                         partition_cnt PARTITION_CNT,
                         ret_code RET_CODE,
                         qc_id QC_ID,
                         dfo_id DFO_ID,
                         sqc_id SQC_ID,
                         worker_id WORKER_ID,
                         event EVENT,
                         p1text P1TEXT,
                         p1 P1,
                         p2text P2TEXT,
                         p2 P2,
                         p3text P3TEXT,
                         p3 P3,
                         "LEVEL" "LEVEL",
                         wait_class_id WAIT_CLASS_ID,
                         "WAIT_CLASS#" "WAIT_CLASS#",
                         wait_class WAIT_CLASS,
                         state STATE,
                         wait_time_micro WAIT_TIME_MICRO,
                         total_wait_time_micro TOTAL_WAIT_TIME_MICRO,
                         total_waits TOTAL_WAITS,
                         rpc_count RPC_COUNT,
                         plan_type PLAN_TYPE,
                         is_inner_sql IS_INNER_SQL,
                         is_executor_rpc IS_EXECUTOR_RPC,
                         is_hit_plan IS_HIT_PLAN,
                         request_time REQUEST_TIME,
                         elapsed_time ELAPSED_TIME,
                         net_time NET_TIME,
                         net_wait_time NET_WAIT_TIME,
                         queue_time QUEUE_TIME,
                         decode_time DECODE_TIME,
                         get_plan_time GET_PLAN_TIME,
                         execute_time EXECUTE_TIME,
                         application_wait_time APPLICATION_WAIT_TIME,
                         concurrency_wait_time CONCURRENCY_WAIT_TIME,
                         user_io_wait_time USER_IO_WAIT_TIME,
                         schedule_time SCHEDULE_TIME,
                         row_cache_hit ROW_CACHE_HIT,
                         bloom_filter_cache_hit BLOOM_FILTER_CACHE_HIT,
                         block_cache_hit BLOCK_CACHE_HIT,
                         block_index_cache_hit BLOCK_INDEX_CACHE_HIT,
                         disk_reads DISK_READS,
                         retry_cnt RETRY_CNT,
                         table_scan TABLE_SCAN,
                         consistency_level CONSISTENCY_LEVEL,
                         memstore_read_row_count MEMSTORE_READ_ROW_COUNT,
                         ssstore_read_row_count SSSTORE_READ_ROW_COUNT,
                         request_memory_used REQUEST_MEMORY_USED,
                         expected_worker_count EXPECTED_WORKER_COUNT,
                         used_worker_count USED_WORKER_COUNT,
                         sched_info SCHED_INFO,
                         ps_stmt_id PS_STMT_ID,
                         transaction_hash TRANSACTION_HASH,
                         request_type as REQUEST_TYPE,
                         is_batched_multi_stmt as IS_BATCHED_MULTI_STMT,
                         ob_trace_info as OB_TRACE_INFO,
                         plan_hash as PLAN_HASH
                    FROM SYS.ALL_VIRTUAL_SQL_AUDIT
                    WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$SQL_AUDIT',
  name_postfix = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28003',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """SELECT * FROM SYS.GV$SQL_AUDIT WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'GV$INSTANCE',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28004',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT
      CAST(ID AS NUMBER) INST_ID,
      CAST(NULL AS NUMBER) INSTANCE_NUMBER,
      CAST(NULL AS VARCHAR2(16)) INSTANCE_NAME,
      CAST(CONCAT(SVR_IP, CONCAT(':', SVR_PORT)) AS VARCHAR2(64)) HOST_NAME,
      CAST(SUBSTR(BUILD_VERSION, 1, 11) AS VARCHAR2(17)) VERSION,
      CAST(NULL AS DATE) STARTUP_TIME,
      CAST(NULL AS VARCHAR2(12)) STATUS,
      CAST(NULL AS VARCHAR2(3)) PARALLEL,
      CAST(NULL AS NUMBER) THREAD#,
      CAST(NULL AS VARCHAR2(7)) ARCHIVER,
      CAST(NULL AS VARCHAR2(15)) LOG_SWITCH_WAIT,
      CAST(NULL AS VARCHAR2(10)) LOGINS,
      CAST(NULL AS VARCHAR2(3)) SHUTDOWN_PENDING,
      CAST(STATUS AS VARCHAR2(17)) DATABASE_STATUS,
      CAST(NULL AS VARCHAR2(18)) INSTANCE_ROLE,
      CAST(NULL AS VARCHAR2(9)) ACTIVE_STATE,
      CAST(NULL AS VARCHAR2(2)) BLOCKED,
      CAST(NULL AS NUMBER) CON_ID,
      CAST(NULL AS VARCHAR2(11)) INSTANCE_MODE,
      CAST(NULL AS VARCHAR2(7)) EDITION,
      CAST(NULL AS VARCHAR2(80)) FAMILY,
      CAST(NULL AS VARCHAR2(15)) DATABASE_TYPE
      FROM
      SYS.ALL_VIRTUAL_SERVER_AGENT WHERE
      IS_SERVING_TENANT(SVR_IP, SVR_PORT, SYS_CONTEXT('USERENV', 'CON_ID')) = 1
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'V$INSTANCE',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28005',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT
      CAST(ID AS NUMBER) INST_ID,
      CAST(NULL AS NUMBER) INSTANCE_NUMBER,
      CAST(NULL AS VARCHAR2(16)) INSTANCE_NAME,
      CAST(CONCAT(SVR_IP, CONCAT(':', SVR_PORT)) AS VARCHAR2(64)) HOST_NAME,
      CAST(SUBSTR(BUILD_VERSION, 1, 11) AS VARCHAR2(17)) VERSION,
      CAST(NULL AS DATE) STARTUP_TIME,
      CAST(NULL AS VARCHAR2(12)) STATUS,
      CAST(NULL AS VARCHAR2(3)) PARALLEL,
      CAST(NULL AS NUMBER) THREAD#,
      CAST(NULL AS VARCHAR2(7)) ARCHIVER,
      CAST(NULL AS VARCHAR2(15)) LOG_SWITCH_WAIT,
      CAST(NULL AS VARCHAR2(10)) LOGINS,
      CAST(NULL AS VARCHAR2(3)) SHUTDOWN_PENDING,
      CAST(STATUS AS VARCHAR2(17)) DATABASE_STATUS,
      CAST(NULL AS VARCHAR2(18)) INSTANCE_ROLE,
      CAST(NULL AS VARCHAR2(9)) ACTIVE_STATE,
      CAST(NULL AS VARCHAR2(2)) BLOCKED,
      CAST(NULL AS NUMBER) CON_ID,
      CAST(NULL AS VARCHAR2(11)) INSTANCE_MODE,
      CAST(NULL AS VARCHAR2(7)) EDITION,
      CAST(NULL AS VARCHAR2(80)) FAMILY,
      CAST(NULL AS VARCHAR2(15)) DATABASE_TYPE
      FROM
      SYS.ALL_VIRTUAL_SERVER_AGENT WHERE
      IS_SERVING_TENANT(SVR_IP, SVR_PORT, SYS_CONTEXT('USERENV', 'CON_ID')) = 1 AND
      SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'GV$PLAN_CACHE_PLAN_STAT',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28006',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT
      TENANT_ID AS TENANT_ID,
      SVR_IP AS SVR_IP,
      SVR_PORT AS SVR_PORT,
      PLAN_ID AS PLAN_ID,
      SQL_ID AS SQL_ID,
      TYPE AS TYPE,
      IS_BIND_SENSITIVE AS IS_BIND_SENSITIVE,
      IS_BIND_AWARE AS IS_BIND_AWARE,
      DB_ID AS DB_ID,
      STATEMENT AS STATEMENT,
      QUERY_SQL AS QUERY_SQL,
      SPECIAL_PARAMS AS SPECIAL_PARAMS,
      PARAM_INFOS AS PARAM_INFOS,
      SYS_VARS AS SYS_VARS,
      PLAN_HASH AS PLAN_HASH,
      FIRST_LOAD_TIME AS FIRST_LOAD_TIME,
      SCHEMA_VERSION AS SCHEMA_VERSION,
      MERGED_VERSION AS MERGED_VERSION,
      LAST_ACTIVE_TIME AS LAST_ACTIVE_TIME,
      AVG_EXE_USEC AS AVG_EXE_USEC,
      SLOWEST_EXE_TIME AS SLOWEST_EXE_TIME,
      SLOWEST_EXE_USEC AS SLOWEST_EXE_USEC,
      SLOW_COUNT AS SLOW_COUNT,
      HIT_COUNT AS HIT_COUNT,
      PLAN_SIZE AS PLAN_SIZE,
      EXECUTIONS AS EXECUTIONS,
      DISK_READS AS DISK_READS,
      DIRECT_WRITES AS DIRECT_WRITES,
      BUFFER_GETS AS BUFFERS_GETS,
      APPLICATION_WAIT_TIME AS APPLICATION_WATI_TIME,
      CONCURRENCY_WAIT_TIME AS CONCURRENCY_WAIT_TIME,
      USER_IO_WAIT_TIME AS USER_IO_WAIT_TIME,
      ROWS_PROCESSED AS ROWS_PROCESSED,
      ELAPSED_TIME AS ELAPSED_TIME,
      CPU_TIME AS CPU_TIME,
      LARGE_QUERYS AS LARGE_QUERYS,
      DELAYED_LARGE_QUERYS AS DELAYED_LARGE_QUERYS,
      DELAYED_PX_QUERYS AS DELAYED_PX_QUERYS,
      OUTLINE_VERSION AS OUTLINE_VERSION,
      OUTLINE_ID AS OUTLINE_ID,
      OUTLINE_DATA AS OUTLINE_DATA,
      HINTS_INFO AS HINTS_INFO,
      HINTS_ALL_WORKED AS HINTS_ALL_WORKED,
      ACS_SEL_INFO AS ACS_SEL_INFO,
      TABLE_SCAN AS TABLE_SCAN,
      EVOLUTION AS EVOLUTION,
      EVO_EXECUTIONS AS EVO_EXECUTIONS,
      EVO_CPU_TIME AS EVO_CPU_TIME,
      TIMEOUT_COUNT AS TIMEOUT_COUNT,
      PS_STMT_ID AS PS_STMT_ID,
      SESSID AS SESSID,
      TEMP_TABLES AS TEMP_TABLES,
      IS_USE_JIT AS IS_USE_JIT,
      OBJECT_TYPE AS OBJECT_TYPE,
      PL_SCHEMA_ID AS PL_SCHEMA_ID,
      IS_BATCHED_MULTI_STMT AS IS_BATCHED_MULTI_STMT
      FROM SYS.ALL_VIRTUAL_PLAN_STAT WHERE
      TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'V$PLAN_CACHE_PLAN_STAT',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28007',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT * FROM SYS.GV$PLAN_CACHE_PLAN_STAT WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_PORT()
""".replace("\n", " ")
)


def_table_schema(
    table_name      = 'GV$PLAN_CACHE_PLAN_EXPLAIN',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28008',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT TENANT_ID AS TENANT_ID,
      SVR_IP as SVR_IP,
      SVR_PORT as SVR_PORT,
      PLAN_ID AS PLAN_ID,
      PLAN_DEPTH as PLAN_DEPTH,
      PLAN_LINE_ID as PLAN_LINE_ID,
      OPERATOR AS OPERATOR,
      NAME AS NAME,
      "ROWS" AS "ROWS",
      COST AS COST,
      PROPERTY AS PROPERTY
      FROM SYS.ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN WHERE
      TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'V$PLAN_CACHE_PLAN_EXPLAIN',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28009',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT * FROM SYS.GV$PLAN_CACHE_PLAN_EXPLAIN WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$SESSION_WAIT',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28010',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT SESSION_ID AS SID,
    TENANT_ID AS CON_ID,
    SVR_IP AS SVR_IP,
    SVR_PORT AS SVR_PORT,
    EVENT AS EVENT,
    P1TEXT AS P1TEXT,
    P1 AS P1,
    P2TEXT AS P2TEXT,
    P2 AS P2,
    P3TEXT AS P3TEXT,
    P3 AS P3,
    WAIT_CLASS_ID AS WAIT_CLASS_ID,
    "WAIT_CLASS#" AS "WAIT_CLASS#",
    WAIT_CLASS AS WAIT_CLASS,
    STATE AS STATE,
    WAIT_TIME_MICRO AS WAIT_TIME_MICRO,
    TIME_REMAINING_MICRO AS TIME_REMAINING_MICRO,
    TIME_SINCE_LAST_WAIT_MICRO AS TIME_SINCE_LAST_WAIT_MICRO
    FROM SYS.ALL_VIRTUAL_SESSION_WAIT
    WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$SESSION_WAIT',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28011',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT SID,CON_ID,EVENT,P1TEXT,P1,
     P2TEXT,P2,P3TEXT,P3,WAIT_CLASS_ID,
     "WAIT_CLASS#",WAIT_CLASS,STATE,WAIT_TIME_MICRO,TIME_REMAINING_MICRO,
     TIME_SINCE_LAST_WAIT_MICRO
     FROM SYS.GV$SESSION_WAIT
     WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$SESSION_WAIT_HISTORY',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28012',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT SESSION_ID AS SID,
    TENANT_ID AS CON_ID,
    SVR_IP AS SVR_IP,
    SVR_PORT AS SVR_PORT,
    "SEQ#" AS "SEQ#",
    "EVENT#" AS "EVENT#",
    EVENT AS EVENT,
    P1TEXT AS P1TEXT,
    P1 AS P1,
    P2TEXT AS P2TEXT,
    P2 AS P2,
    P3TEXT AS P3TEXT,
    P3 AS P3,
    WAIT_TIME_MICRO AS WAIT_TIME_MICRO,
    TIME_SINCE_LAST_WAIT_MICRO AS TIME_SINCE_LAST_WAIT_MICRO,
    WAIT_TIME AS WAIT_TIME
    FROM SYS.ALL_VIRTUAL_SESSION_WAIT_HISTORY
    WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$SESSION_WAIT_HISTORY',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28013',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT SID,CON_ID,"SEQ#" AS "SEQ#","EVENT#" AS "EVENT#",EVENT,
    P1TEXT,P1,P2TEXT,P2,P3TEXT,
    P3,WAIT_TIME_MICRO,TIME_SINCE_LAST_WAIT_MICRO,WAIT_TIME
    FROM SYS.GV$SESSION_WAIT_HISTORY
    WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$MEMORY',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28014',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT TENANT_ID AS CON_ID,
    SVR_IP AS SVR_IP,
    SVR_PORT AS SVR_PORT,
    MOD_NAME AS CONTEXT,
    SUM(COUNT) AS COUNT,
    SUM(USED) AS USED,
    SUM(ALLOC_COUNT) AS ALLOC_COUNT,
    SUM(FREE_COUNT) AS FREE_COUNT
    FROM SYS.ALL_VIRTUAL_MEMORY_INFO
    WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
    AND MOD_TYPE = 'USER' GROUP BY TENANT_ID, SVR_IP, SVR_PORT, MOD_NAME
    ORDER BY TENANT_ID, SVR_IP, SVR_PORT, MOD_NAME
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$MEMORY',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28015',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT CON_ID, CONTEXT, COUNT, USED, ALLOC_COUNT, FREE_COUNT
    FROM SYS.GV$MEMORY
    WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)


def_table_schema(
  table_name      = 'GV$MEMSTORE',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28016',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT TENANT_ID AS CON_ID,
    SVR_IP AS SVR_IP,
    SVR_PORT AS SVR_PORT,
    ACTIVE_MEMSTORE_USED AS ACTIVE,
    TOTAL_MEMSTORE_USED AS TOTAL,
    MAJOR_FREEZE_TRIGGER AS "FREEZE_TRIGGER",
    MEMSTORE_LIMIT AS "MEM_LIMIT",
    FREEZE_CNT AS FREEZE_CNT
    FROM SYS.ALL_VIRTUAL_TENANT_MEMSTORE_INFO
    WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$MEMSTORE',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28017',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT CON_ID, ACTIVE, TOTAL, "FREEZE_TRIGGER", "MEM_LIMIT", FREEZE_CNT
    FROM SYS.GV$MEMSTORE
    WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)


def_table_schema(
  table_name      = 'GV$MEMSTORE_INFO',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28018',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT TENANT_ID AS CON_ID,
    SVR_IP AS SVR_IP,
    SVR_PORT AS SVR_PORT,
    TABLE_ID AS TABLE_ID,
    PARTITION_IDX AS PARTITION_ID,
    VERSION,
    BASE_VERSION,
    MULTI_VERSION_START,
    SNAPSHOT_VERSION,
    IS_ACTIVE,
    MEM_USED as USED,
    HASH_ITEM_COUNT as HASH_ITEMS,
    BTREE_ITEM_COUNT as BTREE_ITEMS
    FROM SYS.ALL_VIRTUAL_MEMSTORE_INFO
    WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$MEMSTORE_INFO',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28019',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT CON_ID, PARTITION_ID,
    VERSION,
    BASE_VERSION,
    MULTI_VERSION_START,
    SNAPSHOT_VERSION,
    IS_ACTIVE,
    USED,
    HASH_ITEMS,
    BTREE_ITEMS
    FROM SYS.GV$MEMSTORE_INFO
    WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)


def_table_schema(
  table_name      = 'GV$SERVER_MEMSTORE',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28020',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      SVR_IP AS SVR_IP,
      SVR_PORT AS SVR_PORT,
      ACTIVE_MEMSTORE_USED AS ACTIVE,
      TOTAL_MEMSTORE_USED AS TOTAL,
      MAJOR_FREEZE_TRIGGER AS "FREEZE_TRIGGER",
      MEMSTORE_LIMIT AS "MEM_LIMIT"
    FROM SYS.ALL_VIRTUAL_SERVER_MEMORY_INFO_AGENT
    WHERE SYS_CONTEXT('USERENV', 'CON_ID') = 1
""".replace("\n", " ")
)


def_table_schema(
  table_name      = 'GV$SESSTAT',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28021',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT SESSION_ID as SID,
       TENANT_ID as CON_ID,
       SVR_IP as SVR_IP,
       SVR_PORT as SVR_PORT,
       "STATISTIC#" as "STATISTIC#",
       VALUE as VALUE
    FROM SYS.ALL_VIRTUAL_SESSTAT
    WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$SESSTAT',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28022',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT SID, CON_ID, "STATISTIC#", VALUE
    FROM SYS.GV$SESSTAT
    WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)


def_table_schema(
  table_name      = 'GV$SYSSTAT',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28023',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT TENANT_ID as CON_ID,
      SVR_IP as SVR_IP,
      SVR_PORT as SVR_PORT,
      "STATISTIC#" as "STATISTIC#",
      VALUE as VALUE,
      STAT_ID as STAT_ID,
      NAME as NAME,
      CLASS as CLASS
    FROM SYS.ALL_VIRTUAL_SYSSTAT
    WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$SYSSTAT',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28024',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT CON_ID, "STATISTIC#", VALUE, STAT_ID, NAME, CLASS
    FROM SYS.GV$SYSSTAT
    WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$SYSTEM_EVENT',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28025',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT TENANT_ID as CON_ID,
      SVR_IP as SVR_IP,
      SVR_PORT as SVR_PORT,
      EVENT_ID as EVENT_ID,
      EVENT as EVENT,
      WAIT_CLASS_ID as WAIT_CLASS_ID,
      "WAIT_CLASS#" as "WAIT_CLASS#",
      WAIT_CLASS as WAIT_CLASS,
      TOTAL_WAITS as TOTAL_WAITS,
      TOTAL_TIMEOUTS as TOTAL_TIMEOUTS,
      TIME_WAITED as TIME_WAITED,
      AVERAGE_WAIT as AVERAGE_WAIT,
      TIME_WAITED_MICRO as TIME_WAITED_MICRO
    FROM SYS.ALL_VIRTUAL_SYSTEM_EVENT
    WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$SYSTEM_EVENT',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28026',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT CON_ID, EVENT_ID, EVENT, WAIT_CLASS_ID, "WAIT_CLASS#", WAIT_CLASS, TOTAL_WAITS, TOTAL_TIMEOUTS, TIME_WAITED, AVERAGE_WAIT, TIME_WAITED_MICRO
    FROM SYS.GV$SYSTEM_EVENT
    WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$TENANT_MEMSTORE_ALLOCATOR_INFO',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28027',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT TENANT_ID as CON_ID,
           SVR_IP as SVR_IP,
           SVR_PORT as SVR_PORT,
           TABLE_ID as TABLE_ID,
           PARTITION_ID as PARTITION_ID,
           MT_BASE_VERSION as MT_BASE_VERSION,
           RETIRE_CLOCK as RETIRE_CLOCK,
           MT_IS_FROZEN as MT_IS_FROZEN,
           MT_PROTECTION_CLOCK as MT_PROTECTION_CLOCK,
           MT_SNAPSHOT_VERSION as MT_SNAPSHOT_VERSION
    FROM SYS.ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO
    WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)


def_table_schema(
  table_name      = 'V$TENANT_MEMSTORE_ALLOCATOR_INFO',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28028',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT CON_ID, TABLE_ID, PARTITION_ID, MT_BASE_VERSION, RETIRE_CLOCK, MT_IS_FROZEN, MT_PROTECTION_CLOCK, MT_SNAPSHOT_VERSION
    FROM SYS.GV$TENANT_MEMSTORE_ALLOCATOR_INFO
    WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)


# end define (g)v$session_wait and (g)v$session_wait_history


def_table_schema(
    table_name      = 'GV$PLAN_CACHE_STAT',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28029',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT
      SVR_IP,
      SVR_PORT,
      SQL_NUM,
      MEM_USED,
      MEM_HOLD,
      ACCESS_COUNT,
      HIT_COUNT,
      HIT_RATE,
      PLAN_NUM,
      MEM_LIMIT,
      HASH_BUCKET,
      STMTKEY_NUM
      FROM SYS.ALL_VIRTUAL_PLAN_CACHE_STAT WHERE
      TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'V$PLAN_CACHE_STAT',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28030',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT SQL_NUM,
      MEM_USED,
      MEM_HOLD,
      ACCESS_COUNT,
      HIT_COUNT,
      HIT_RATE,
      PLAN_NUM,
      MEM_LIMIT,
      HASH_BUCKET,
      STMTKEY_NUM
      FROM SYS.GV$PLAN_CACHE_STAT WHERE
      SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$CONCURRENT_LIMIT_SQL',
  name_postfix = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28031',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition='SELECT tenant_id TENANT_ID,\
           database_id DATABASE_ID,\
           outline_id OUTLINE_ID,\
           database_name DATABASE_NAME,\
           outline_name OUTLINE_NAME,\
           outline_content OUTLINE_CONTENT,\
           visible_signature VISIBLE_SIGNATURE,\
           sql_text SQL_TEXT,\
           concurrent_num CONCURRENT_NUM,\
           limit_target LIMIT_TARGET\
    FROM SYS.TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT'
)

#### End Performance View

def_table_schema(
    table_name      = 'NLS_SESSION_PARAMETERS',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28032',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT
        CAST(UPPER(VARIABLE_NAME) AS VARCHAR(30)) AS PARAMETER,
        CAST(VALUE AS VARCHAR(64)) AS VALUE
      FROM
        SYS.TENANT_VIRTUAL_SESSION_VARIABLE
      WHERE
        VARIABLE_NAME LIKE 'nls_%'
        AND VARIABLE_NAME != 'nls_characterset'
        AND VARIABLE_NAME != 'nls_nchar_characterset'
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'NLS_INSTANCE_PARAMETERS',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28033',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT
        CAST(UPPER(VARIABLE_NAME) AS VARCHAR(30)) AS PARAMETER,
        CAST(VALUE AS VARCHAR(64)) AS VALUE
      FROM
        SYS.TENANT_VIRTUAL_GLOBAL_VARIABLE
      WHERE
        VARIABLE_NAME LIKE 'nls_%'
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'NLS_DATABASE_PARAMETERS',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28034',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
      SELECT
        CAST(UPPER(VARIABLE_NAME) AS VARCHAR(128)) AS PARAMETER,
        CAST(VALUE AS VARCHAR(64)) AS VALUE
      FROM
        SYS.TENANT_VIRTUAL_GLOBAL_VARIABLE
      WHERE
        VARIABLE_NAME LIKE 'nls_%'
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$NLS_PARAMETERS',
  name_postfix = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28035',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      CAST(UPPER(VARIABLE_NAME) AS VARCHAR(64)) AS PARAMETER,
      CAST(VALUE AS VARCHAR(64)) AS VALUE,
      0  AS CON_ID
    FROM
      SYS.TENANT_VIRTUAL_SESSION_VARIABLE
    WHERE
      VARIABLE_NAME LIKE 'nls_%'
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$VERSION',
  name_postfix = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28036',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      VALUE AS BANNER,
      0  AS CON_ID
    FROM
      SYS.TENANT_VIRTUAL_SESSION_VARIABLE
    WHERE
      VARIABLE_NAME = 'version_comment'
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$TENANT_PX_WORKER_STAT',
  name_postfix = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28037',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
    SESSION_ID,
    TENANT_ID,
    SVR_IP,
    SVR_PORT,
    TRACE_ID,
    QC_ID,
    SQC_ID,
    WORKER_ID,
    DFO_ID,
    START_TIME
  FROM SYS.ALL_VIRTUAL_PX_WORKER_STAT
  WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$TENANT_PX_WORKER_STAT',
  name_postfix = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28038',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
    SESSION_ID,
    TENANT_ID,
    SVR_IP,
    SVR_PORT,
    TRACE_ID,
    QC_ID,
    SQC_ID,
    WORKER_ID,
    DFO_ID,
    START_TIME
  FROM SYS.GV$TENANT_PX_WORKER_STAT
  WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'GV$PS_STAT',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28039',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
  SELECT svr_ip, svr_port, stmt_count,
         hit_count, access_count, mem_hold
      FROM SYS.ALL_VIRTUAL_PS_STAT WHERE
      TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'V$PS_STAT',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28040',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
  SELECT svr_ip, svr_port, stmt_count,
         hit_count, access_count, mem_hold
      FROM SYS.GV$PS_STAT WHERE
      SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
    table_name      = 'GV$PS_ITEM_INFO',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28041',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
  SELECT svr_ip, svr_port, stmt_id, db_id,
         ps_sql, param_count, stmt_item_ref_count,
         stmt_info_ref_count, mem_hold, stmt_type, checksum, expired
      FROM SYS.ALL_VIRTUAL_PS_ITEM_INFO WHERE
      TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
table_name      = 'V$PS_ITEM_INFO',
    name_postfix    = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id        = '28042',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    in_tenant_space = True,
    view_definition = """
  SELECT svr_ip, svr_port, stmt_id, db_id
         ps_sql, param_count, stmt_item_ref_count,
         stmt_info_ref_count, mem_hold, stmt_type, checksum, expired
      FROM SYS.GV$PS_ITEM_INFO WHERE
      SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$SQL_WORKAREA_ACTIVE',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28045',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
      CAST(NULL AS NUMBER) AS SQL_HASH_VALUE,
      SQL_ID,
      CAST(NULL AS DATE) AS SQL_EXEC_START,
      SQL_EXEC_ID,
      CAST(NULL AS RAW(8)) AS WORKAREA_ADDRESS,
      OPERATION_TYPE,
      OPERATION_ID,
      POLICY,
      SID,
      CAST(NULL AS NUMBER) AS QCINST_ID,
      CAST(NULL AS NUMBER) AS QCSID,
      ACTIVE_TIME,
      WORK_AREA_SIZE,
      EXPECT_SIZE,
      ACTUAL_MEM_USED,
      MAX_MEM_USED,
      NUMBER_PASSES,
      TEMPSEG_SIZE,
      CAST(NULL AS VARCHAR2(20)) AS TABLESPACE,
      CAST(NULL AS NUMBER) AS "SEGRFNO#",
      CAST(NULL AS NUMBER) AS "SEGBLK#",
      TENANT_ID AS CON_ID
  FROM SYS.ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_AGENT
  WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$SQL_WORKAREA_ACTIVE',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28046',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
      CAST(NULL AS NUMBER) AS SQL_HASH_VALUE,
      SQL_ID,
      CAST(NULL AS DATE) AS SQL_EXEC_START,
      SQL_EXEC_ID,
      CAST(NULL AS RAW(8)) AS WORKAREA_ADDRESS,
      OPERATION_TYPE,
      OPERATION_ID,
      POLICY,
      SID,
      CAST(NULL AS NUMBER) AS QCINST_ID,
      CAST(NULL AS NUMBER) AS QCSID,
      ACTIVE_TIME,
      WORK_AREA_SIZE,
      EXPECT_SIZE,
      ACTUAL_MEM_USED,
      MAX_MEM_USED,
      NUMBER_PASSES,
      TEMPSEG_SIZE,
      CAST(NULL AS VARCHAR2(20)) AS TABLESPACE,
      CAST(NULL AS NUMBER) AS "SEGRFNO#",
      CAST(NULL AS NUMBER) AS "SEGBLK#",
      TENANT_ID AS CON_ID
  FROM SYS.ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_AGENT
  WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
  AND SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$SQL_WORKAREA_HISTOGRAM',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28047',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
      LOW_OPTIMAL_SIZE,
      HIGH_OPTIMAL_SIZE,
      OPTIMAL_EXECUTIONS,
      ONEPASS_EXECUTIONS,
      MULTIPASSES_EXECUTIONS,
      TOTAL_EXECUTIONS,
      TENANT_ID AS CON_ID
  FROM SYS.ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_AGENT
  WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$SQL_WORKAREA_HISTOGRAM',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28048',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
      LOW_OPTIMAL_SIZE,
      HIGH_OPTIMAL_SIZE,
      OPTIMAL_EXECUTIONS,
      ONEPASS_EXECUTIONS,
      MULTIPASSES_EXECUTIONS,
      TOTAL_EXECUTIONS,
      TENANT_ID AS CON_ID
  FROM SYS.ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_AGENT
  WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
  AND SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$OB_SQL_WORKAREA_MEMORY_INFO',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28049',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
      MAX_WORKAREA_SIZE,
      WORKAREA_HOLD_SIZE,
      MAX_AUTO_WORKAREA_SIZE,
      MEM_TARGET,
      TOTAL_MEM_USED,
      GLOBAL_MEM_BOUND,
      DRIFT_SIZE,
      WORKAREA_COUNT,
      MANUAL_CALC_COUNT
  FROM SYS.ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_AGENT
  WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$OB_SQL_WORKAREA_MEMORY_INFO',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28050',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
      MAX_WORKAREA_SIZE,
      WORKAREA_HOLD_SIZE,
      MAX_AUTO_WORKAREA_SIZE,
      MEM_TARGET,
      TOTAL_MEM_USED,
      GLOBAL_MEM_BOUND,
      DRIFT_SIZE,
      WORKAREA_COUNT,
      MANUAL_CALC_COUNT
  FROM SYS.ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_AGENT
  WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
  AND SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
table_name      = 'GV$PLAN_CACHE_REFERENCE_INFO',
name_postfix    = '_ORA',
database_id     = 'OB_ORA_SYS_DATABASE_ID',
table_id        = '28051',
table_type      = 'SYSTEM_VIEW',
rowkey_columns  = [],
normal_columns  = [],
gm_columns      = [],
in_tenant_space = True,
view_definition = """
SELECT SVR_IP,
SVR_PORT,
TENANT_ID,
PC_REF_PLAN_LOCAL,
PC_REF_PLAN_REMOTE,
PC_REF_PLAN_DIST,
PC_REF_PLAN_ARR,
PC_REF_PL,
PC_REF_PL_STAT,
PLAN_GEN,
CLI_QUERY,
OUTLINE_EXEC,
PLAN_EXPLAIN,
ASYN_BASELINE,
LOAD_BASELINE,
PS_EXEC,
GV_SQL,
PL_ANON,
PL_ROUTINE,
PACKAGE_VAR,
PACKAGE_TYPE,
PACKAGE_SPEC,
PACKAGE_BODY,
PACKAGE_RESV,
GET_PKG,
INDEX_BUILDER,
PCV_SET,
PCV_RD,
PCV_WR,
PCV_GET_PLAN_KEY,
PCV_GET_PL_KEY,
PCV_EXPIRE_BY_USED,
PCV_EXPIRE_BY_MEM
FROM SYS.ALL_VIRTUAL_PLAN_CACHE_STAT WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
table_name      = 'V$PLAN_CACHE_REFERENCE_INFO',
name_postfix    = '_ORA',
database_id     = 'OB_ORA_SYS_DATABASE_ID',
table_id        = '28052',
table_type      = 'SYSTEM_VIEW',
rowkey_columns  = [],
normal_columns  = [],
gm_columns      = [],
in_tenant_space = True,
view_definition = """
SELECT * FROM GV$PLAN_CACHE_REFERENCE_INFO
WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'GV$SQL_WORKAREA',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28053',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
      CAST(NULL AS RAW(8)) AS ADDRESS,
      CAST(NULL AS NUMBER) AS HASH_VALUE,
      SQL_ID,
      CAST(NULL AS NUMBER) AS CHILD_NUMBER,
      CAST(NULL AS RAW(8)) AS WORKAREA_ADDRESS,
      OPERATION_TYPE,
      OPERATION_ID,
      POLICY,
      ESTIMATED_OPTIMAL_SIZE,
      ESTIMATED_ONEPASS_SIZE,
      LAST_MEMORY_USED,
      LAST_EXECUTION,
      LAST_DEGREE,
      TOTAL_EXECUTIONS,
      OPTIMAL_EXECUTIONS,
      ONEPASS_EXECUTIONS,
      multipasses_executions,
      ACTIVE_TIME,
      MAX_TEMPSEG_SIZE,
      LAST_TEMPSEG_SIZE,
      TENANT_ID AS CON_ID
  FROM SYS.ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_AGENT
  WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$SQL_WORKAREA',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28054',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
  SELECT
      CAST(NULL AS RAW(8)) AS ADDRESS,
      CAST(NULL AS NUMBER) AS HASH_VALUE,
      SQL_ID,
      CAST(NULL AS NUMBER) AS CHILD_NUMBER,
      CAST(NULL AS RAW(8)) AS WORKAREA_ADDRESS,
      OPERATION_TYPE,
      OPERATION_ID,
      POLICY,
      ESTIMATED_OPTIMAL_SIZE,
      ESTIMATED_ONEPASS_SIZE,
      LAST_MEMORY_USED,
      LAST_EXECUTION,
      LAST_DEGREE,
      TOTAL_EXECUTIONS,
      OPTIMAL_EXECUTIONS,
      ONEPASS_EXECUTIONS,
      multipasses_executions,
      ACTIVE_TIME,
      MAX_TEMPSEG_SIZE,
      LAST_TEMPSEG_SIZE,
      TENANT_ID AS CON_ID
  FROM SYS.ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_AGENT
  WHERE TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
  AND SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
""".replace("\n", " ")
)

def_table_schema(
 table_name      = 'GV$SSTABLE',
 name_postfix    = '_ORA',
 database_id     = 'OB_ORA_SYS_DATABASE_ID',
 table_id        = '28055',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
SELECT
  M.SVR_IP,
  M.SVR_PORT,
  M.TABLE_TYPE,
  M.TABLE_ID,
  T.TABLE_NAME,
  T.TENANT_ID,
  M.PARTITION_ID,
  M.INDEX_ID,
  M.BASE_VERSION,
  M.MULTI_VERSION_START,
  M.SNAPSHOT_VERSION,
  M.START_LOG_ID,
  M.END_LOG_ID,
  M.MAX_LOG_ID,
  M.VERSION,
  M.LOGICAL_DATA_VERSION,
  M."SIZE",
  M.IS_ACTIVE,
  M.REF,
  M.WRITE_REF,
  M.TRX_COUNT,
  M.PENDING_LOG_PERSISTING_ROW_CNT,
  M.UPPER_TRANS_VERSION,
  M.CONTAIN_UNCOMMITTED_ROW
FROM
  SYS.ALL_VIRTUAL_TABLE_MGR_AGENT M JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T
    ON M.TABLE_ID = T.TABLE_ID
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
AND
  T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
 table_name      = 'V$SSTABLE',
 name_postfix    = '_ORA',
 database_id     = 'OB_ORA_SYS_DATABASE_ID',
 table_id        = '28056',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
SELECT
  M.TABLE_TYPE,
  M.TABLE_ID,
  T.TABLE_NAME,
  T.TENANT_ID,
  M.PARTITION_ID,
  M.INDEX_ID,
  M.BASE_VERSION,
  M.MULTI_VERSION_START,
  M.SNAPSHOT_VERSION,
  M.START_LOG_ID,
  M.END_LOG_ID,
  M.MAX_LOG_ID,
  M.VERSION,
  M.LOGICAL_DATA_VERSION,
  M."SIZE",
  M.IS_ACTIVE,
  M.REF,
  M.WRITE_REF,
  M.TRX_COUNT,
  M.PENDING_LOG_PERSISTING_ROW_CNT,
  M.UPPER_TRANS_VERSION,
  M.CONTAIN_UNCOMMITTED_ROW
FROM
  SYS.ALL_VIRTUAL_TABLE_MGR_AGENT M JOIN SYS.ALL_VIRTUAL_TABLE_REAL_AGENT T
  ON M.TABLE_ID = T.TABLE_ID
    AND T.TENANT_ID = EFFECTIVE_TENANT_ID()
WHERE
  M.SVR_IP=HOST_IP()
AND
  M.SVR_PORT=RPC_PORT()
AND
  T.TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
 table_name      = 'GV$SERVER_SCHEMA_INFO',
 name_postfix    = '_ORA',
 database_id     = 'OB_ORA_SYS_DATABASE_ID',
 table_id        = '28057',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
SELECT
  SVR_IP,
  SVR_PORT,
  TENANT_ID,
  REFRESHED_SCHEMA_VERSION,
  RECEIVED_SCHEMA_VERSION,
  SCHEMA_COUNT,
  SCHEMA_SIZE,
  MIN_SSTABLE_SCHEMA_VERSION
FROM
  SYS.ALL_VIRTUAL_SERVER_SCHEMA_INFO_AGENT
WHERE
  TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)

def_table_schema(
 table_name      = 'V$SERVER_SCHEMA_INFO',
 name_postfix    = '_ORA',
 database_id     = 'OB_ORA_SYS_DATABASE_ID',
 table_id        = '28058',
 table_type      = 'SYSTEM_VIEW',
 rowkey_columns  = [],
 normal_columns  = [],
 gm_columns      = [],
 in_tenant_space = True,
 view_definition = """
SELECT
  TENANT_ID,
  REFRESHED_SCHEMA_VERSION,
  RECEIVED_SCHEMA_VERSION,
  SCHEMA_COUNT,
  SCHEMA_SIZE,
  MIN_SSTABLE_SCHEMA_VERSION
FROM
  SYS.ALL_VIRTUAL_SERVER_SCHEMA_INFO_AGENT
WHERE
  SVR_IP=HOST_IP()
AND
  SVR_PORT=RPC_PORT()
AND
  TENANT_ID = SYS_CONTEXT('USERENV', 'CON_ID')
""".replace("\n", " ")
)


def_table_schema(
    table_name     = 'GV$SQL_PLAN_MONITOR',
    name_postfix = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id       = '28059',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    normal_columns = [],
    view_definition = """SELECT
          TENANT_ID CON_ID,
          REQUEST_ID,
          NULL KEY,
          NULL STATUS,
          SVR_IP,
          SVR_PORT,
          TRACE_ID,
          FIRST_REFRESH_TIME,
          LAST_REFRESH_TIME,
          FIRST_CHANGE_TIME,
          LAST_CHANGE_TIME,
          NULL REFRESH_COUNT,
          NULL SID,
          THREAD_ID  PROCESS_NAME,
          NULL SQL_ID,
          NULL SQL_EXEC_START,
          NULL SQL_EXEC_ID,
          NULL SQL_PLAN_HASH_VALUE,
          NULL SQL_CHILD_ADDRESS,
          NULL PLAN_PARENT_ID,
          PLAN_LINE_ID,
          PLAN_OPERATION,
          NULL PLAN_OPTIONS,
          NULL PLAN_OBJECT_OWNER,
          NULL PLAN_OBJECT_NAME,
          NULL PLAN_OBJECT_TYPE,
          PLAN_DEPTH,
          NULL PLAN_POSITION,
          NULL PLAN_COST,
          NULL PLAN_CARDINALITY,
          NULL PLAN_BYTES,
          NULL PLAN_TIME,
          NULL PLAN_PARTITION_START,
          NULL PLAN_PARTITION_STOP,
          NULL PLAN_CPU_COST,
          NULL PLAN_IO_COST,
          NULL PLAN_TEMP_SPACE,
          STARTS,
          OUTPUT_ROWS,
          NULL IO_INTERCONNECT_BYTES,
          NULL PHYSICAL_READ_REQUESTS,
          NULL PHYSICAL_READ_BYTES,
          NULL PHYSICAL_WRITE_REQUESTS,
          NULL PHYSICAL_WRITE_BYTES,
          NULL WORKAREA_MEM,
          NULL WORKAREA_MAX_MEM,
          NULL WORKAREA_TEMPSEG,
          NULL WORKAREA_MAX_TEMPSEG,
          NULL OTHERSTAT_GROUP_ID,
          OTHERSTAT_1_ID,
          NULL OTHERSTAT_1_TYPE,
          OTHERSTAT_1_VALUE,
          OTHERSTAT_2_ID,
          NULL OTHERSTAT_2_TYPE,
          OTHERSTAT_2_VALUE,
          OTHERSTAT_3_ID,
          NULL OTHERSTAT_3_TYPE,
          OTHERSTAT_3_VALUE,
          OTHERSTAT_4_ID,
          NULL OTHERSTAT_4_TYPE,
          OTHERSTAT_4_VALUE,
          OTHERSTAT_5_ID,
          NULL OTHERSTAT_5_TYPE,
          OTHERSTAT_5_VALUE,
          OTHERSTAT_6_ID,
          NULL OTHERSTAT_6_TYPE,
          OTHERSTAT_6_VALUE,
          OTHERSTAT_7_ID,
          NULL OTHERSTAT_7_TYPE,
          OTHERSTAT_7_VALUE,
          OTHERSTAT_8_ID,
          NULL OTHERSTAT_8_TYPE,
          OTHERSTAT_8_VALUE,
          OTHERSTAT_9_ID,
          NULL OTHERSTAT_9_TYPE,
          OTHERSTAT_9_VALUE,
          OTHERSTAT_10_ID,
          NULL OTHERSTAT_10_TYPE,
          OTHERSTAT_10_VALUE,
          NULL OTHER_XML,
          NULL PLAN_OPERATION_INACTIVE
        FROM SYS.ALL_VIRTUAL_SQL_PLAN_MONITOR
        WHERE (is_serving_tenant(svr_ip, svr_port, effective_tenant_id()) = 1)
        and (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),
)

def_table_schema(
    table_name     = 'V$SQL_PLAN_MONITOR',
    name_postfix = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id       = '28060',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    normal_columns = [],
    view_definition = """
    SELECT * from SYS.GV$SQL_PLAN_MONITOR  WHERE SVR_IP=HOST_IP() AND SVR_PORT=RPC_Port()
""".replace("\n", " "),
)

def_table_schema(
    table_name     = 'V$SQL_MONITOR_STATNAME',
    name_postfix = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id       = '28061',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    normal_columns = [],
    view_definition = """
    SELECT
      NULL CON_ID,
      ID,
      GROUP_ID,
      NAME,
      DESCRIPTION,
      0 TYPE,
      0 FLAGS
    FROM SYS.ALL_VIRTUAL_SQL_MONITOR_STATNAME
""".replace("\n", " "),
)

def_table_schema(
  table_name      = 'GV$LOCK',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28062',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      DISTINCT
      A.SVR_IP AS SVR_IP,
      A.SVR_PORT AS SVR_PORT,
      A.TABLE_ID AS TABLE_ID,
      A.ROWKEY AS ADDR,
      B.ROW_LOCK_ADDR AS KADDR,
      B.SESSION_ID AS SID,
      A.TYPE AS TYPE,
      A.LOCK_MODE AS LMODE,
      CAST(NULL AS NUMBER) AS REQUEST,
      A.TIME_AFTER_RECV AS CTIME,
      A.BLOCK_SESSION_ID AS BLOCK,
      TRUNC(A.TABLE_ID / POWER(2, 40)) AS CON_ID
      FROM SYS.ALL_VIRTUAL_LOCK_WAIT_STAT A
      JOIN SYS.ALL_VIRTUAL_TRANS_LOCK_STAT B
      ON A.SVR_IP = B.SVR_IP AND A.SVR_PORT = B.SVR_PORT
      AND A.TABLE_ID = B.TABLE_ID AND SUBSTR(A.ROWKEY, 1, 512) = SUBSTR(B.ROWKEY, 1, 512)
      WHERE (SYS_CONTEXT('USERENV', 'CON_ID') = 1 OR SYS_CONTEXT('USERENV', 'CON_ID') = TRUNC(A.TABLE_ID / POWER(2, 40)))
      AND A.SESSION_ID = A.BLOCK_SESSION_ID
  """.replace("\n", " ")
)
def_table_schema(
  table_name      = 'V$LOCK',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28063',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      TABLE_ID,
      ADDR,
      KADDR,
      SID,
      TYPE,
      LMODE,
      REQUEST,
      CTIME,
      BLOCK,
      CON_ID FROM GV$LOCK
      WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
  """.replace("\n", " ")
)


def_table_schema(
  table_name      = 'GV$OPEN_CURSOR',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28064',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      SVR_IP,
      SVR_PORT,
      SADDR,
      SID,
      USER_NAME,
      ADDRESS,
      HASH_VALUE,
      SQL_ID,
      SQL_TEXT,
      CAST(LAST_SQL_ACTIVE_TIME as DATE) LAST_SQL_ACTIVE_TIME,
      SQL_EXEC_ID FROM SYS.ALL_VIRTUAL_OPEN_CURSOR
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$OPEN_CURSOR',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28065',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      SADDR,
      SID,
      USER_NAME,
      ADDRESS,
      HASH_VALUE,
      SQL_ID,
      SQL_TEXT,
      LAST_SQL_ACTIVE_TIME,
      SQL_EXEC_ID FROM GV$OPEN_CURSOR
      WHERE svr_ip = host_ip() AND svr_port = rpc_port()
  """.replace("\n", " ")
)

def_table_schema(
  table_name      = 'V$TIMEZONE_NAMES',
  name_postfix    = '_ORA',
  database_id     = 'OB_ORA_SYS_DATABASE_ID',
  table_id        = '28066',
  table_type      = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      NA.NAME AS TZNAME,
      TR.ABBREVIATION AS TZABBREV,
      NA.TENANT_ID AS CON_ID
    FROM SYS.ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT NA
        JOIN SYS.ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT TR
        ON NA.TIME_ZONE_ID = TR.TIME_ZONE_ID
        AND NA.TENANT_ID = EFFECTIVE_TENANT_ID()
        AND TR.TENANT_ID = EFFECTIVE_TENANT_ID()
  """.replace("\n", " ")
)

def_table_schema(
  table_name = 'GV$GLOBAL_TRANSACTION',
  name_postfix = '_ORA',
  database_id = 'OB_ORA_SYS_DATABASE_ID',
  table_id = '28067',
  table_type = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT A.FORMAT_ID AS FORMATID,
      A.SCHEDULER_IP AS SVR_IP,
      A.SCHEDULER_PORT AS SVR_PORT,
      RAWTOHEX(A.GTRID) AS GLOBALID,
      RAWTOHEX(A.BQUAL) AS BRANCHID,
      B.BRANCHES AS BRANCHES,
      B.BRANCHES AS REFCOUNT,
      NVL(C.PREPARECOUNT, 0) AS PREPARECOUNT,
      'ACTIVE' AS STATE,
      0 AS FLAGS,
      CASE WHEN bitand(end_flag,65536)=65536 THEN 'LOOSELY COUPLED' ELSE 'TIGHTLY COUPLED' END AS COUPLING,
      SYS_CONTEXT('USERENV', 'CON_ID') AS CON_ID
    FROM SYS.ALL_VIRTUAL_TENANT_GLOBAL_TRANSACTION_AGENT A
    LEFT JOIN (SELECT GTRID, FORMAT_ID, COUNT(BQUAL) AS BRANCHES FROM SYS.ALL_VIRTUAL_TENANT_GLOBAL_TRANSACTION_AGENT GROUP BY GTRID, FORMAT_ID) B
    ON A.GTRID = B.GTRID AND A.FORMAT_ID = B.FORMAT_ID
    LEFT JOIN (SELECT GTRID, FORMAT_ID, COUNT(BQUAL) AS PREPARECOUNT FROM SYS.ALL_VIRTUAL_TENANT_GLOBAL_TRANSACTION_AGENT WHERE STATE = 3 GROUP BY GTRID, FORMAT_ID) C
    ON B.GTRID = C.GTRID AND B.FORMAT_ID = C.FORMAT_ID
    WHERE A.format_id != -2
  """.replace("\n", " ")
)

def_table_schema(
  table_name = 'V$GLOBAL_TRANSACTION',
  name_postfix = '_ORA',
  database_id = 'OB_ORA_SYS_DATABASE_ID',
  table_id = '28068',
  table_type = 'SYSTEM_VIEW',
  rowkey_columns  = [],
  normal_columns  = [],
  gm_columns      = [],
  in_tenant_space = True,
  view_definition = """
    SELECT
      FORMATID,
      GLOBALID,
      BRANCHID,
      BRANCHES,
      REFCOUNT,
      PREPARECOUNT,
      STATE,
      FLAGS,
      COUPLING,
      CON_ID
    FROM GV$GLOBAL_TRANSACTION
    WHERE SVR_IP = HOST_IP() AND SVR_PORT = RPC_PORT()
  """.replace("\n", " ")
)

def_table_schema(
    table_name     = 'V$RESTORE_POINT',
    name_postfix = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id       = '28069',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    normal_columns = [],
    view_definition = """SELECT
          TENANT_ID,
          SNAPSHOT_TS as SNAPSHOT,
          GMT_CREATE as TIME,
          EXTRA_INFO as NAME
        FROM SYS.ALL_VIRTUAL_ACQUIRED_SNAPSHOT_AGENT
        WHERE snapshot_type = 3 and (tenant_id = effective_tenant_id() or effective_tenant_id() = 1)
""".replace("\n", " "),
)

def_table_schema(
    table_name     = 'V$RSRC_PLAN',
    name_postfix = '_ORA',
    database_id     = 'OB_ORA_SYS_DATABASE_ID',
    table_id       = '28070',
    table_type = 'SYSTEM_VIEW',
    gm_columns = [],
    in_tenant_space = True,
    rowkey_columns = [],
    normal_columns = [],
    view_definition = """SELECT
          CAST(NULL as NUMBER) AS ID,
          B.plan NAME,
          CAST('TRUE' AS VARCHAR2(5)) AS IS_TOP_PLAN,
          CAST('ON' AS VARCHAR2(3)) AS CPU_MANAGED,
          CAST(NULL AS VARCHAR2(3)) AS INSTANCE_CAGING,
          CAST(NULL AS NUMBER) AS PARALLEL_SERVERS_ACTIVE,
          CAST(NULL AS NUMBER) AS PARALLEL_SERVERS_TOTAL,
          CAST(NULL AS VARCHAR2(32)) AS PARALLEL_EXECUTION_MANAGED
        FROM SYS.tenant_virtual_global_variable A, SYS.DBA_RSRC_PLANS B
        WHERE A.variable_name = 'resource_manager_plan' AND A.value = B.plan
""".replace("\n", " "),
)

#
# EVENT_MANIPULATION: enum('INSERT','UPDATE','DELETE') => NULL
# ACTION_TIMING: enum('BEFORE','AFTER')                => NULL
# CREATED:  timestamp(2)                               => TIME(2)
# SQL_MODE: set('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED',
#               'ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','NOT_USED_9',
#               'NOT_USED_10','NOT_USED_11','NOT_USED_12','NOT_USED_13','NOT_USED_14','NOT_USED_15',
#               'NOT_USED_16','NOT_USED_17','NOT_USED_18','ANSI','NO_AUTO_VALUE_ON_ZERO',
#               'NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE',
#               'NO_ZERO_DATE','ALLOW_INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL',
#               'NOT_USED_29','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH',
#               'TIME_TRUNCATE_FRACTIONAL')            => NULL
# OTHERS: varchar                                      => CHARACTER
def_table_schema(
    table_name      = 'TRIGGERS',
    table_id        = '28071',
    database_id     = 'OB_INFORMATION_SCHEMA_ID',
    table_type      = 'SYSTEM_VIEW',
    rowkey_columns  = [],
    normal_columns  = [],
    gm_columns      = [],
    view_definition = """SELECT
          CAST(NULL AS CHARACTER(64)) AS TRIGGER_CATALOG,
          CAST(NULL AS CHARACTER(64)) AS TRIGGER_SCHEMA,
          CAST(NULL AS CHARACTER(64)) AS TRIGGER_NAME,
          NULL AS EVENT_MANIPULATION,
          CAST(NULL AS CHARACTER(64)) AS EVENT_OBJECT_CATALOG,
          CAST(NULL AS CHARACTER(64)) AS EVENT_OBJECT_SCHEMA,
          CAST(NULL AS CHARACTER(64)) AS EVENT_OBJECT_TABLE,
          CAST(NULL AS UNSIGNED) AS ACTION_ORDER,
          CAST(NULL AS BINARY(0)) AS ACTION_CONDITION,
          NULL AS ACTION_STATEMENT,
          CAST(NULL AS CHARACTER(3))  AS ACTION_ORIENTATION,
          NULL AS ACTION_TIMING,
          CAST(NULL AS BINARY(0)) AS ACTION_REFERENCE_OLD_TABLE,
          CAST(NULL AS BINARY(0)) AS ACTION_REFERENCE_NEW_TABLE,
          CAST(NULL AS CHARACTER(3))  AS ACTION_REFERENCE_OLD_ROW,
          CAST(NULL AS CHARACTER(3))  AS ACTION_REFERENCE_NEW_ROW,
          CAST(NULL AS TIME(2)) AS CREATED,
          NULL AS SQL_MODE,
          CAST(NULL AS CHARACTER(288)) AS DEFINER,
          CAST(NULL AS CHARACTER(64)) AS CHARACTER_SET_CLIENT,
          CAST(NULL AS CHARACTER(64)) AS COLLATION_CONNECTION,
          CAST(NULL AS CHARACTER(64)) AS DATABASE_COLLATION
        FROM DUAL
        WHERE 1 = 0
""".replace("\n", " "),
)
