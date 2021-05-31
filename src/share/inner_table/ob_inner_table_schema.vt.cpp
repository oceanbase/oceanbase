/**
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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_inner_table_schema.h"

namespace oceanbase {
namespace share {
VTMapping vt_mappings[5000];
bool vt_mapping_init()
{
  int64_t start_idx = common::OB_MIN_VIRTUAL_TABLE_ID;
  {
    int64_t idx = OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_COLL_TYPE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 0;
    tmp_vt_mapping.end_pos_ = 3;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_COLUMN_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 3;
    tmp_vt_mapping.end_pos_ = 4;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_COLUMN_STATISTIC_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_COLUMN_STATISTIC_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 4;
    tmp_vt_mapping.end_pos_ = 5;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_COLUMN_STAT_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 5;
    tmp_vt_mapping.end_pos_ = 6;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_CONSTRAINT_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 6;
    tmp_vt_mapping.end_pos_ = 7;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_DATABASE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 7;
    tmp_vt_mapping.end_pos_ = 9;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_DBLINK_HISTORY_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_DBLINK_HISTORY_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 9;
    tmp_vt_mapping.end_pos_ = 11;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_DBLINK_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 11;
    tmp_vt_mapping.end_pos_ = 13;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_DEF_SUB_PART_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 13;
    tmp_vt_mapping.end_pos_ = 15;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_FOREIGN_KEY_COLUMN_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 15;
    tmp_vt_mapping.end_pos_ = 16;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_FOREIGN_KEY_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 16;
    tmp_vt_mapping.end_pos_ = 19;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_PACKAGE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 19;
    tmp_vt_mapping.end_pos_ = 22;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_PART_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 22;
    tmp_vt_mapping.end_pos_ = 24;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_RECYCLEBIN_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 24;
    tmp_vt_mapping.end_pos_ = 27;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_RES_MGR_CONSUMER_GROUP_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.use_real_tenant_id_ = true;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_RES_MGR_DIRECTIVE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.use_real_tenant_id_ = true;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_RES_MGR_MAPPING_RULE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.use_real_tenant_id_ = true;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_RES_MGR_PLAN_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.use_real_tenant_id_ = true;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_ROUTINE_PARAM_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 27;
    tmp_vt_mapping.end_pos_ = 29;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_ROUTINE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 29;
    tmp_vt_mapping.end_pos_ = 33;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_SEQUENCE_OBJECT_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 33;
    tmp_vt_mapping.end_pos_ = 35;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_SEQUENCE_V2_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_SEQUENCE_V2_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 35;
    tmp_vt_mapping.end_pos_ = 36;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_SEQUENCE_VALUE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 36;
    tmp_vt_mapping.end_pos_ = 37;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_SUB_PART_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 37;
    tmp_vt_mapping.end_pos_ = 39;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_SYNONYM_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 39;
    tmp_vt_mapping.end_pos_ = 42;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLEGROUP_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 42;
    tmp_vt_mapping.end_pos_ = 43;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_PRIVILEGE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 43;
    tmp_vt_mapping.end_pos_ = 44;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 44;
    tmp_vt_mapping.end_pos_ = 49;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TABLE_STAT_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TABLE_STAT_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 49;
    tmp_vt_mapping.end_pos_ = 50;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_CONSTRAINT_COLUMN_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_CONSTRAINT_COLUMN_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 50;
    tmp_vt_mapping.end_pos_ = 51;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_DEPENDENCY_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 51;
    tmp_vt_mapping.end_pos_ = 54;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_ERROR_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 54;
    tmp_vt_mapping.end_pos_ = 55;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_KEYSTORE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 55;
    tmp_vt_mapping.end_pos_ = 57;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_META_TABLE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.use_real_tenant_id_ = true;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_OBJAUTH_HISTORY_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OBJAUTH_HISTORY_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 57;
    tmp_vt_mapping.end_pos_ = 60;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OBJAUTH_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 60;
    tmp_vt_mapping.end_pos_ = 63;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_OBJECT_TYPE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 63;
    tmp_vt_mapping.end_pos_ = 66;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_PARTITION_META_TABLE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.use_real_tenant_id_ = true;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_PROFILE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 66;
    tmp_vt_mapping.end_pos_ = 67;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 67;
    tmp_vt_mapping.end_pos_ = 69;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_HISTORY_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 69;
    tmp_vt_mapping.end_pos_ = 71;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SECURITY_AUDIT_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 71;
    tmp_vt_mapping.end_pos_ = 73;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 73;
    tmp_vt_mapping.end_pos_ = 78;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_SYSAUTH_HISTORY_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SYSAUTH_HISTORY_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 78;
    tmp_vt_mapping.end_pos_ = 79;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_SYSAUTH_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 79;
    tmp_vt_mapping.end_pos_ = 80;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TABLESPACE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 80;
    tmp_vt_mapping.end_pos_ = 82;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TIME_ZONE_NAME_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.use_real_tenant_id_ = true;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_TIME_ZONE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TIME_ZONE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.use_real_tenant_id_ = true;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.use_real_tenant_id_ = true;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.use_real_tenant_id_ = true;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TENANT_TRIGGER_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 82;
    tmp_vt_mapping.end_pos_ = 86;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TYPE_ATTR_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 86;
    tmp_vt_mapping.end_pos_ = 88;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_TYPE_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 88;
    tmp_vt_mapping.end_pos_ = 92;
  }

  {
    int64_t idx = OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_TID - start_idx;
    VTMapping& tmp_vt_mapping = vt_mappings[idx];
    tmp_vt_mapping.mapping_tid_ = OB_ALL_USER_TID;
    tmp_vt_mapping.is_real_vt_ = true;
    tmp_vt_mapping.start_pos_ = 92;
    tmp_vt_mapping.end_pos_ = 94;
  }

  return true;
}  // end define vt_mappings

bool inited_vt = vt_mapping_init();

}  // end namespace share
}  // end namespace oceanbase
