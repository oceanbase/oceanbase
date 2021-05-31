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

#ifdef AGENT_VIRTUAL_TABLE_LOCATION_SWITCH

case OB_ALL_VIRTUAL_ACQUIRED_SNAPSHOT_AGENT_TID:
case OB_ALL_VIRTUAL_COLL_TYPE_AGENT_TID:
case OB_ALL_VIRTUAL_COLL_TYPE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_COLUMN_AGENT_TID:
case OB_ALL_VIRTUAL_COLUMN_STATISTIC_AGENT_TID:
case OB_ALL_VIRTUAL_COLUMN_STAT_AGENT_TID:
case OB_ALL_VIRTUAL_CONSTRAINT_AGENT_TID:
case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_AGENT_TID:
case OB_ALL_VIRTUAL_DATABASE_AGENT_TID:
case OB_ALL_VIRTUAL_DBLINK_AGENT_TID:
case OB_ALL_VIRTUAL_DBLINK_HISTORY_AGENT_TID:
case OB_ALL_VIRTUAL_DEF_SUB_PART_AGENT_TID:
case OB_ALL_VIRTUAL_DEPENDENCY_AGENT_TID:
case OB_ALL_VIRTUAL_ERROR_AGENT_TID:
case OB_ALL_VIRTUAL_FOREIGN_KEY_AGENT_TID:
case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_AGENT_TID:
case OB_ALL_VIRTUAL_OBJAUTH_AGENT_TID:
case OB_ALL_VIRTUAL_OBJAUTH_HISTORY_AGENT_TID:
case OB_ALL_VIRTUAL_OBJECT_TYPE_AGENT_TID:
case OB_ALL_VIRTUAL_PACKAGE_AGENT_TID:
case OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_PARTITION_INFO_AGENT_TID:
case OB_ALL_VIRTUAL_PARTITION_TABLE_AGENT_TID:
case OB_ALL_VIRTUAL_PART_AGENT_TID:
case OB_ALL_VIRTUAL_PRIVILEGE_AGENT_TID:
case OB_ALL_VIRTUAL_RECYCLEBIN_AGENT_TID:
case OB_ALL_VIRTUAL_ROUTINE_AGENT_TID:
case OB_ALL_VIRTUAL_ROUTINE_PARAM_AGENT_TID:
case OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_ROUTINE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_AGENT_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_AGENT_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_AGENT_TID:
case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_AGENT_TID:
case OB_ALL_VIRTUAL_SEQUENCE_V2_AGENT_TID:
case OB_ALL_VIRTUAL_SEQUENCE_VALUE_AGENT_TID:
case OB_ALL_VIRTUAL_SERVER_AGENT_TID:
case OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_AGENT_TID:
case OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_AGENT_TID:
case OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_AGENT_TID:
case OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_AGENT_TID:
case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_AGENT_TID:
case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_AGENT_TID:
case OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_AGENT_TID:
case OB_ALL_VIRTUAL_SSTABLE_CHECKSUM_AGENT_TID:
case OB_ALL_VIRTUAL_SUB_PART_AGENT_TID:
case OB_ALL_VIRTUAL_SYNONYM_AGENT_TID:
case OB_ALL_VIRTUAL_SYSAUTH_AGENT_TID:
case OB_ALL_VIRTUAL_SYSAUTH_HISTORY_AGENT_TID:
case OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TID:
case OB_ALL_VIRTUAL_TABLEGROUP_AGENT_TID:
case OB_ALL_VIRTUAL_TABLE_AGENT_TID:
case OB_ALL_VIRTUAL_TABLE_MGR_AGENT_TID:
case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_AGENT_TID:
case OB_ALL_VIRTUAL_TABLE_STAT_AGENT_TID:
case OB_ALL_VIRTUAL_TABLE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_GLOBAL_TRANSACTION_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_KEYSTORE_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_META_TABLE_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_PROFILE_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_TABLESPACE_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TRIGGER_AGENT_TID:
case OB_ALL_VIRTUAL_TYPE_AGENT_TID:
case OB_ALL_VIRTUAL_TYPE_ATTR_AGENT_TID:
case OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TYPE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_USER_AGENT_TID:
case OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID:
case OB_TENANT_VIRTUAL_CHARSET_AGENT_TID:
case OB_TENANT_VIRTUAL_COLLATION_AGENT_TID:
case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TID:
case OB_TENANT_VIRTUAL_OUTLINE_AGENT_TID:
case OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TID:

#endif

#ifdef AGENT_VIRTUAL_TABLE_CREATE_ITER

case OB_ALL_VIRTUAL_ACQUIRED_SNAPSHOT_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_ACQUIRED_SNAPSHOT_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLL_TYPE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_COLL_TYPE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLL_TYPE_SYS_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_COLL_TYPE_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLUMN_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_COLUMN_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLUMN_STATISTIC_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_COLUMN_STATISTIC_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLUMN_STAT_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_COLUMN_STAT_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_CONSTRAINT_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_CONSTRAINT_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DATABASE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_DATABASE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DBLINK_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_DBLINK_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DBLINK_HISTORY_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_DBLINK_HISTORY_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DEF_SUB_PART_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_DEF_SUB_PART_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DEPENDENCY_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_DEPENDENCY_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ERROR_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_ERROR_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_FOREIGN_KEY_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_FOREIGN_KEY_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_OBJAUTH_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_OBJAUTH_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_OBJAUTH_HISTORY_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_OBJAUTH_HISTORY_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_OBJECT_TYPE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_OBJECT_TYPE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PACKAGE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_PACKAGE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_PACKAGE_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PARTITION_INFO_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_PARTITION_INFO_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PARTITION_TABLE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_PARTITION_TABLE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PART_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_PART_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PRIVILEGE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_PRIVILEGE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_RECYCLEBIN_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_RECYCLEBIN_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ROUTINE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_ROUTINE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ROUTINE_PARAM_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_ROUTINE_PARAM_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_ROUTINE_PARAM_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ROUTINE_SYS_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_ROUTINE_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SECURITY_AUDIT_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SECURITY_AUDIT_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SEQUENCE_OBJECT_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SEQUENCE_V2_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SEQUENCE_V2_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SEQUENCE_VALUE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SEQUENCE_VALUE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SERVER_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_SERVER_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SSTABLE_CHECKSUM_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_SSTABLE_CHECKSUM_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SUB_PART_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SUB_PART_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYNONYM_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SYNONYM_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYSAUTH_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SYSAUTH_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYSAUTH_HISTORY_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SYSAUTH_HISTORY_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLEGROUP_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TABLEGROUP_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TABLE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLE_MGR_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TABLE_MGR_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TABLE_PRIVILEGE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLE_STAT_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TABLE_STAT_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLE_SYS_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_TABLE_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_GLOBAL_TRANSACTION_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_TENANT_GLOBAL_TRANSACTION_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_KEYSTORE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TENANT_KEYSTORE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_META_TABLE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_TENANT_META_TABLE_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_PROFILE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TENANT_PROFILE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_TABLESPACE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TENANT_TABLESPACE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_TENANT_TIME_ZONE_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_TENANT_TIME_ZONE_NAME_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_TENANT_TRIGGER_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TRIGGER_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TRIGGER_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TYPE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TYPE_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TYPE_ATTR_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_TYPE_ATTR_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_TYPE_ATTR_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TYPE_SYS_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_TYPE_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_ALL_VIRTUAL_USER_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_ALL_VIRTUAL_USER_TID;
  const bool sys_tenant_base_table = true;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_TENANT_VIRTUAL_ALL_TABLE_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_TENANT_VIRTUAL_CHARSET_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_TENANT_VIRTUAL_CHARSET_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_TENANT_VIRTUAL_COLLATION_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_TENANT_VIRTUAL_COLLATION_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_TENANT_VIRTUAL_OUTLINE_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_TENANT_VIRTUAL_OUTLINE_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

case OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TID: {
  ObAgentVirtualTable* agent_iter = NULL;
  const uint64_t base_tid = OB_TENANT_VIRTUAL_TABLE_INDEX_TID;
  const bool sys_tenant_base_table = false;
  const bool only_sys_data = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    agent_iter->~ObAgentVirtualTable();
    allocator.free(agent_iter);
    agent_iter = NULL;
  } else {
    vt_iter = agent_iter;
  }
  break;
}

#endif

#ifdef ITERATE_VIRTUAL_TABLE_LOCATION_SWITCH

case OB_ALL_VIRTUAL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TID:
case OB_ALL_VIRTUAL_BACKUP_BACKUPSET_TASK_TID:
case OB_ALL_VIRTUAL_BACKUP_CLEAN_INFO_TID:
case OB_ALL_VIRTUAL_BACKUP_INFO_TID:
case OB_ALL_VIRTUAL_BACKUP_LOG_ARCHIVE_STATUS_TID:
case OB_ALL_VIRTUAL_BACKUP_TASK_TID:
case OB_ALL_VIRTUAL_BACKUP_VALIDATION_TASK_TID:
case OB_ALL_VIRTUAL_COLL_TYPE_TID:
case OB_ALL_VIRTUAL_COLL_TYPE_HISTORY_TID:
case OB_ALL_VIRTUAL_COLUMN_TID:
case OB_ALL_VIRTUAL_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_COLUMN_STAT_TID:
case OB_ALL_VIRTUAL_COLUMN_STATISTIC_TID:
case OB_ALL_VIRTUAL_CONSTRAINT_TID:
case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_TID:
case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_CONSTRAINT_HISTORY_TID:
case OB_ALL_VIRTUAL_DATABASE_TID:
case OB_ALL_VIRTUAL_DATABASE_HISTORY_TID:
case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_TID:
case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_HISTORY_TID:
case OB_ALL_VIRTUAL_DBLINK_TID:
case OB_ALL_VIRTUAL_DBLINK_HISTORY_TID:
case OB_ALL_VIRTUAL_DDL_OPERATION_TID:
case OB_ALL_VIRTUAL_DEF_SUB_PART_TID:
case OB_ALL_VIRTUAL_DEF_SUB_PART_HISTORY_TID:
case OB_ALL_VIRTUAL_DEPENDENCY_TID:
case OB_ALL_VIRTUAL_ERROR_TID:
case OB_ALL_VIRTUAL_FOREIGN_KEY_TID:
case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_TID:
case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_FOREIGN_KEY_HISTORY_TID:
case OB_ALL_VIRTUAL_FUNC_TID:
case OB_ALL_VIRTUAL_FUNC_HISTORY_TID:
case OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TID:
case OB_ALL_VIRTUAL_HISTOGRAM_STAT_TID:
case OB_ALL_VIRTUAL_META_TABLE_TID:
case OB_ALL_VIRTUAL_OBJAUTH_TID:
case OB_ALL_VIRTUAL_OBJAUTH_HISTORY_TID:
case OB_ALL_VIRTUAL_OBJECT_TYPE_TID:
case OB_ALL_VIRTUAL_ORI_SCHEMA_VERSION_TID:
case OB_ALL_VIRTUAL_OUTLINE_TID:
case OB_ALL_VIRTUAL_OUTLINE_HISTORY_TID:
case OB_ALL_VIRTUAL_PACKAGE_TID:
case OB_ALL_VIRTUAL_PACKAGE_HISTORY_TID:
case OB_ALL_VIRTUAL_PART_TID:
case OB_ALL_VIRTUAL_PART_HISTORY_TID:
case OB_ALL_VIRTUAL_PART_INFO_TID:
case OB_ALL_VIRTUAL_PART_INFO_HISTORY_TID:
case OB_ALL_VIRTUAL_PG_BACKUP_BACKUPSET_TASK_TID:
case OB_ALL_VIRTUAL_PG_BACKUP_TASK_TID:
case OB_ALL_VIRTUAL_PG_BACKUP_VALIDATION_TASK_TID:
case OB_ALL_VIRTUAL_RECYCLEBIN_TID:
case OB_ALL_VIRTUAL_RESTORE_PG_INFO_TID:
case OB_ALL_VIRTUAL_ROUTINE_TID:
case OB_ALL_VIRTUAL_ROUTINE_HISTORY_TID:
case OB_ALL_VIRTUAL_ROUTINE_PARAM_TID:
case OB_ALL_VIRTUAL_ROUTINE_PARAM_HISTORY_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_TID:
case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_TID:
case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_HISTORY_TID:
case OB_ALL_VIRTUAL_SEQUENCE_V2_TID:
case OB_ALL_VIRTUAL_SEQUENCE_VALUE_TID:
case OB_ALL_VIRTUAL_SERVER_LOG_META_TID:
case OB_ALL_VIRTUAL_SSTABLE_COLUMN_CHECKSUM_TID:
case OB_ALL_VIRTUAL_SUB_PART_TID:
case OB_ALL_VIRTUAL_SUB_PART_HISTORY_TID:
case OB_ALL_VIRTUAL_SYNONYM_TID:
case OB_ALL_VIRTUAL_SYNONYM_HISTORY_TID:
case OB_ALL_VIRTUAL_SYS_STAT_TID:
case OB_ALL_VIRTUAL_SYS_VARIABLE_TID:
case OB_ALL_VIRTUAL_SYS_VARIABLE_HISTORY_TID:
case OB_ALL_VIRTUAL_SYSAUTH_TID:
case OB_ALL_VIRTUAL_SYSAUTH_HISTORY_TID:
case OB_ALL_VIRTUAL_TABLE_TID:
case OB_ALL_VIRTUAL_TABLE_HISTORY_TID:
case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_TID:
case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_HISTORY_TID:
case OB_ALL_VIRTUAL_TABLE_STAT_TID:
case OB_ALL_VIRTUAL_TABLEGROUP_TID:
case OB_ALL_VIRTUAL_TABLEGROUP_HISTORY_TID:
case OB_ALL_VIRTUAL_TEMP_TABLE_TID:
case OB_ALL_VIRTUAL_TENANT_GC_PARTITION_INFO_TID:
case OB_ALL_VIRTUAL_TENANT_KEYSTORE_TID:
case OB_ALL_VIRTUAL_TENANT_KEYSTORE_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_TID:
case OB_ALL_VIRTUAL_TENANT_PLAN_BASELINE_TID:
case OB_ALL_VIRTUAL_TENANT_PLAN_BASELINE_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_PROFILE_TID:
case OB_ALL_VIRTUAL_TENANT_PROFILE_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_TID:
case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_TABLESPACE_TID:
case OB_ALL_VIRTUAL_TENANT_TABLESPACE_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_USER_FAILED_LOGIN_STAT_TID:
case OB_ALL_VIRTUAL_TIME_ZONE_TID:
case OB_ALL_VIRTUAL_TIME_ZONE_NAME_TID:
case OB_ALL_VIRTUAL_TIME_ZONE_TRANSITION_TID:
case OB_ALL_VIRTUAL_TIME_ZONE_TRANSITION_TYPE_TID:
case OB_ALL_VIRTUAL_TRIGGER_TID:
case OB_ALL_VIRTUAL_TRIGGER_HISTORY_TID:
case OB_ALL_VIRTUAL_TYPE_TID:
case OB_ALL_VIRTUAL_TYPE_ATTR_TID:
case OB_ALL_VIRTUAL_TYPE_ATTR_HISTORY_TID:
case OB_ALL_VIRTUAL_TYPE_HISTORY_TID:
case OB_ALL_VIRTUAL_USER_TID:
case OB_ALL_VIRTUAL_USER_HISTORY_TID:

#endif

#ifdef ITERATE_VIRTUAL_TABLE_CREATE_ITER

case OB_ALL_VIRTUAL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(
                 OB_ALL_TENANT_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_BACKUP_BACKUPSET_TASK_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(
                 iter->init(OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_BACKUP_CLEAN_INFO_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_BACKUP_CLEAN_INFO_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_BACKUP_INFO_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_BACKUP_INFO_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_BACKUP_LOG_ARCHIVE_STATUS_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(
                 OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_BACKUP_TASK_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_BACKUP_TASK_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_BACKUP_VALIDATION_TASK_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(
                 iter->init(OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLL_TYPE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_COLL_TYPE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("coll_type_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "coll_type_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("elem_type_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "elem_type_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("package_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "package_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLL_TYPE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_COLL_TYPE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("coll_type_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "coll_type_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("elem_type_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "elem_type_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("package_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "package_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLUMN_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLUMN_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLUMN_STAT_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_STAT_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_COLUMN_STATISTIC_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_STATISTIC_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_CONSTRAINT_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_CONSTRAINT_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_CONSTRAINT_COLUMN_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(
                 OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_CONSTRAINT_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_CONSTRAINT_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DATABASE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_DATABASE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("default_tablegroup_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "default_tablegroup_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DATABASE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_DATABASE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("default_tablegroup_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "default_tablegroup_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_DATABASE_PRIVILEGE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("user_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "user_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("user_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "user_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DBLINK_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_DBLINK_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("dblink_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "dblink_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DBLINK_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_DBLINK_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("dblink_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "dblink_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DDL_OPERATION_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_DDL_OPERATION_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("user_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "user_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablegroup_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablegroup_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DEF_SUB_PART_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_DEF_SUB_PART_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablespace_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablespace_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DEF_SUB_PART_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_DEF_SUB_PART_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablespace_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablespace_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_DEPENDENCY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_DEPENDENCY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("dep_obj_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "dep_obj_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("ref_obj_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "ref_obj_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("dep_obj_owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "dep_obj_owner_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ERROR_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_ERROR_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("obj_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "obj_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_FOREIGN_KEY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_FOREIGN_KEY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("foreign_key_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "foreign_key_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("child_table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "child_table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("parent_table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "parent_table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_FOREIGN_KEY_COLUMN_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("foreign_key_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "foreign_key_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("foreign_key_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "foreign_key_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_FOREIGN_KEY_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_FOREIGN_KEY_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("foreign_key_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "foreign_key_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("child_table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "child_table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("parent_table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "parent_table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_FUNC_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_FUNC_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("udf_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "udf_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_FUNC_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_FUNC_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("udf_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "udf_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_GLOBAL_TRANSACTION_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_HISTOGRAM_STAT_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_HISTOGRAM_STAT_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_META_TABLE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_META_TABLE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_OBJAUTH_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OBJAUTH_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("obj_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "obj_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("grantor_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "grantor_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("grantee_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "grantee_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_OBJAUTH_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OBJAUTH_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("obj_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "obj_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("grantor_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "grantor_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("grantee_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "grantee_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_OBJECT_TYPE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OBJECT_TYPE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("object_type_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "object_type_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ORI_SCHEMA_VERSION_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_ORI_SCHEMA_VERSION_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_OUTLINE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_OUTLINE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("outline_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "outline_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_OUTLINE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_OUTLINE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("outline_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "outline_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PACKAGE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_PACKAGE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("package_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "package_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PACKAGE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_PACKAGE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("package_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "package_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PART_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_PART_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablespace_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablespace_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PART_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_PART_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablespace_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablespace_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PART_INFO_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_PART_INFO_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PART_INFO_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_PART_INFO_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PG_BACKUP_BACKUPSET_TASK_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(
                 iter->init(OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PG_BACKUP_TASK_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_PG_BACKUP_TASK_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_PG_BACKUP_VALIDATION_TASK_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(
                 OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_RECYCLEBIN_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_RECYCLEBIN_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablegroup_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablegroup_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_RESTORE_PG_INFO_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_RESTORE_PG_INFO_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ROUTINE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_ROUTINE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("routine_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "routine_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("package_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "package_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ROUTINE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_ROUTINE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("routine_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "routine_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("package_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "package_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ROUTINE_PARAM_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_ROUTINE_PARAM_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("routine_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "routine_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("type_owner"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "type_owner");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_ROUTINE_PARAM_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_ROUTINE_PARAM_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("routine_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "routine_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("type_owner"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "type_owner");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SECURITY_AUDIT_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SECURITY_AUDIT_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("audit_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "audit_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(
                 iter->init(OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("audit_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "audit_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(
                 iter->init(OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("user_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "user_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("effective_user_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "effective_user_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("db_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "db_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("cur_db_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "cur_db_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("audit_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "audit_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SEQUENCE_OBJECT_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("sequence_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "sequence_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SEQUENCE_OBJECT_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("sequence_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "sequence_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SEQUENCE_V2_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SEQUENCE_V2_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("sequence_key"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "sequence_key");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SEQUENCE_VALUE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SEQUENCE_VALUE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("sequence_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "sequence_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SERVER_LOG_META_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_CLOG_HISTORY_INFO_V2_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SSTABLE_COLUMN_CHECKSUM_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(
                 iter->init(OB_ALL_TENANT_SSTABLE_COLUMN_CHECKSUM_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("data_table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "data_table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("index_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "index_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SUB_PART_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SUB_PART_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablespace_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablespace_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SUB_PART_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SUB_PART_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablespace_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablespace_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYNONYM_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SYNONYM_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("synonym_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "synonym_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("object_database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "object_database_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYNONYM_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SYNONYM_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("synonym_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "synonym_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("object_database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "object_database_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYS_STAT_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SYS_STAT_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("value"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "value");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYS_VARIABLE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SYS_VARIABLE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYS_VARIABLE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_SYS_VARIABLE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYSAUTH_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SYSAUTH_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("grantee_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "grantee_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_SYSAUTH_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SYSAUTH_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("grantee_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "grantee_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TABLE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("data_table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "data_table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablegroup_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablegroup_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablespace_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablespace_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TABLE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("data_table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "data_table_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablegroup_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablegroup_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablespace_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablespace_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TABLE_PRIVILEGE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("user_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "user_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TABLE_PRIVILEGE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("user_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "user_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLE_STAT_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TABLE_STAT_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLEGROUP_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TABLEGROUP_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablegroup_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablegroup_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TABLEGROUP_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TABLEGROUP_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablegroup_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablegroup_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TEMP_TABLE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TEMP_TABLE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_GC_PARTITION_INFO_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_GC_PARTITION_INFO_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("table_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "table_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_KEYSTORE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_KEYSTORE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("keystore_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "keystore_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("master_key_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "master_key_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_KEYSTORE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_KEYSTORE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("keystore_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "keystore_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("master_key_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "master_key_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_PARTITION_META_TABLE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_PLAN_BASELINE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_PLAN_BASELINE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("plan_baseline_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "plan_baseline_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_PLAN_BASELINE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(
                 iter->init(OB_ALL_TENANT_PLAN_BASELINE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("plan_baseline_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "plan_baseline_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_PROFILE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_PROFILE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("profile_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "profile_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_PROFILE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_PROFILE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("profile_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "profile_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("grantee_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "grantee_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("role_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "role_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(
                 iter->init(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("grantee_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "grantee_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("role_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "role_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_TABLESPACE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TABLESPACE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablespace_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablespace_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("master_key_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "master_key_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_TABLESPACE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TABLESPACE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("tablespace_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "tablespace_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("master_key_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "master_key_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TENANT_USER_FAILED_LOGIN_STAT_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(
                 iter->init(OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TIME_ZONE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TIME_ZONE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TIME_ZONE_NAME_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TIME_ZONE_NAME_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TIME_ZONE_TRANSITION_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TIME_ZONE_TRANSITION_TYPE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = true;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(
                 OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TRIGGER_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TRIGGER_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("trigger_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "trigger_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("base_object_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "base_object_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TRIGGER_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TRIGGER_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("trigger_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "trigger_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("owner_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "owner_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("base_object_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "base_object_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TYPE_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TYPE_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("type_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "type_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("supertypeid"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "supertypeid");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("package_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "package_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TYPE_ATTR_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TYPE_ATTR_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("type_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "type_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("type_attr_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "type_attr_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TYPE_ATTR_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TYPE_ATTR_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("type_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "type_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("type_attr_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "type_attr_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_TYPE_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_TYPE_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("type_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "type_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("database_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "database_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("supertypeid"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "supertypeid");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("package_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "package_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_USER_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_USER_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("user_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "user_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("profile_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "profile_id");
  } else {
    vt_iter = iter;
  }
  break;
}

case OB_ALL_VIRTUAL_USER_HISTORY_TID: {
  ObIterateVirtualTable* iter = NULL;
  const bool record_real_tenant_id = false;
  if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
    SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
  } else if (OB_FAIL(iter->init(OB_ALL_USER_HISTORY_TID, record_real_tenant_id, index_schema, params))) {
    SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
    iter->~ObIterateVirtualTable();
    allocator.free(iter);
    iter = NULL;
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("user_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "user_id");
  } else if (OB_FAIL(iter->set_column_name_with_tenant_id("profile_id"))) {
    SERVER_LOG(WARN, "set column_name with tenant_id failed", K(ret), "column_name", "profile_id");
  } else {
    vt_iter = iter;
  }
  break;
}

#endif

#ifdef MIGRATE_TABLE_BEFORE_2200_SWITCH

case OB_ALL_CLOG_HISTORY_INFO_V2_TID:
case OB_ALL_COLL_TYPE_TID:
case OB_ALL_COLL_TYPE_HISTORY_TID:
case OB_ALL_COLUMN_TID:
case OB_ALL_COLUMN_HISTORY_TID:
case OB_ALL_COLUMN_STAT_TID:
case OB_ALL_COLUMN_STATISTIC_TID:
case OB_ALL_CONSTRAINT_TID:
case OB_ALL_CONSTRAINT_HISTORY_TID:
case OB_ALL_DATABASE_TID:
case OB_ALL_DATABASE_HISTORY_TID:
case OB_ALL_DATABASE_PRIVILEGE_TID:
case OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID:
case OB_ALL_DBLINK_TID:
case OB_ALL_DBLINK_HISTORY_TID:
case OB_ALL_DDL_ID_TID:
case OB_ALL_DDL_OPERATION_TID:
case OB_ALL_DEF_SUB_PART_TID:
case OB_ALL_DEF_SUB_PART_HISTORY_TID:
case OB_ALL_FOREIGN_KEY_TID:
case OB_ALL_FOREIGN_KEY_COLUMN_TID:
case OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID:
case OB_ALL_FOREIGN_KEY_HISTORY_TID:
case OB_ALL_FUNC_TID:
case OB_ALL_FUNC_HISTORY_TID:
case OB_ALL_HISTOGRAM_STAT_TID:
case OB_ALL_ORI_SCHEMA_VERSION_TID:
case OB_ALL_OUTLINE_TID:
case OB_ALL_OUTLINE_HISTORY_TID:
case OB_ALL_PACKAGE_TID:
case OB_ALL_PACKAGE_HISTORY_TID:
case OB_ALL_PART_TID:
case OB_ALL_PART_HISTORY_TID:
case OB_ALL_PART_INFO_TID:
case OB_ALL_PART_INFO_HISTORY_TID:
case OB_ALL_RECYCLEBIN_TID:
case OB_ALL_ROUTINE_TID:
case OB_ALL_ROUTINE_HISTORY_TID:
case OB_ALL_ROUTINE_PARAM_TID:
case OB_ALL_ROUTINE_PARAM_HISTORY_TID:
case OB_ALL_SEQUENCE_OBJECT_TID:
case OB_ALL_SEQUENCE_OBJECT_HISTORY_TID:
case OB_ALL_SUB_PART_TID:
case OB_ALL_SUB_PART_HISTORY_TID:
case OB_ALL_SYNONYM_TID:
case OB_ALL_SYNONYM_HISTORY_TID:
case OB_ALL_SYS_STAT_TID:
case OB_ALL_SYS_VARIABLE_TID:
case OB_ALL_SYS_VARIABLE_HISTORY_TID:
case OB_ALL_TABLE_TID:
case OB_ALL_TABLE_HISTORY_TID:
case OB_ALL_TABLE_PRIVILEGE_TID:
case OB_ALL_TABLE_PRIVILEGE_HISTORY_TID:
case OB_ALL_TABLE_STAT_TID:
case OB_ALL_TABLEGROUP_TID:
case OB_ALL_TABLEGROUP_HISTORY_TID:
case OB_ALL_TEMP_TABLE_TID:
case OB_ALL_TENANT_PARTITION_META_TABLE_TID:
case OB_ALL_TENANT_PLAN_BASELINE_TID:
case OB_ALL_TENANT_PLAN_BASELINE_HISTORY_TID:
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID:
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID:
case OB_ALL_TYPE_TID:
case OB_ALL_TYPE_ATTR_TID:
case OB_ALL_TYPE_ATTR_HISTORY_TID:
case OB_ALL_TYPE_HISTORY_TID:
case OB_ALL_USER_TID:
case OB_ALL_USER_HISTORY_TID:

#endif

#ifdef CLUSTER_PRIVATE_TABLE_SWITCH

case OB_ALL_CLOG_HISTORY_INFO_V2_TID:
case OB_ALL_COLUMN_STAT_TID:
case OB_ALL_COLUMN_STATISTIC_TID:
case OB_ALL_DDL_ID_TID:
case OB_ALL_DUMMY_TID:
case OB_ALL_HISTOGRAM_STAT_TID:
case OB_ALL_RES_MGR_CONSUMER_GROUP_TID:
case OB_ALL_RES_MGR_DIRECTIVE_TID:
case OB_ALL_RES_MGR_MAPPING_RULE_TID:
case OB_ALL_RES_MGR_PLAN_TID:
case OB_ALL_TABLE_STAT_TID:
case OB_ALL_TENANT_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TID:
case OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TID:
case OB_ALL_TENANT_BACKUP_CLEAN_INFO_TID:
case OB_ALL_TENANT_BACKUP_INFO_TID:
case OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TID:
case OB_ALL_TENANT_BACKUP_TASK_TID:
case OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TID:
case OB_ALL_TENANT_DEPENDENCY_TID:
case OB_ALL_TENANT_META_TABLE_TID:
case OB_ALL_TENANT_PARTITION_META_TABLE_TID:
case OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TID:
case OB_ALL_TENANT_PG_BACKUP_TASK_TID:
case OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TID:
case OB_ALL_TENANT_RESTORE_PG_INFO_TID:
case OB_ALL_TENANT_SSTABLE_COLUMN_CHECKSUM_TID:
case OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TID:
case OB_ALL_WEAK_READ_SERVICE_TID:
case OB_TENANT_PARAMETER_TID:

#endif

#ifdef BACKUP_PRIVATE_TABLE_SWITCH

case OB_ALL_CLOG_HISTORY_INFO_V2_TID:
case OB_ALL_DDL_ID_TID:
case OB_ALL_DUMMY_TID:
case OB_ALL_TENANT_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TID:
case OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TID:
case OB_ALL_TENANT_BACKUP_CLEAN_INFO_TID:
case OB_ALL_TENANT_BACKUP_INFO_TID:
case OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TID:
case OB_ALL_TENANT_BACKUP_TASK_TID:
case OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TID:
case OB_ALL_TENANT_DEPENDENCY_TID:
case OB_ALL_TENANT_GLOBAL_TRANSACTION_TID:
case OB_ALL_TENANT_META_TABLE_TID:
case OB_ALL_TENANT_PARTITION_META_TABLE_TID:
case OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TID:
case OB_ALL_TENANT_PG_BACKUP_TASK_TID:
case OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TID:
case OB_ALL_TENANT_RESTORE_PG_INFO_TID:
case OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TID:
case OB_ALL_WEAK_READ_SERVICE_TID:

#endif

#ifdef RS_RESTART_RELATED

case OB_ALL_ACQUIRED_SNAPSHOT_TID:
case OB_ALL_COLL_TYPE_TID:
case OB_ALL_COLL_TYPE_HISTORY_TID:
case OB_ALL_COLUMN_TID:
case OB_ALL_COLUMN_HISTORY_TID:
case OB_ALL_COLUMN_STAT_TID:
case OB_ALL_COLUMN_STATISTIC_TID:
case OB_ALL_CONSTRAINT_TID:
case OB_ALL_CONSTRAINT_HISTORY_TID:
case OB_ALL_CORE_TABLE_TID:
case OB_ALL_DATABASE_TID:
case OB_ALL_DATABASE_HISTORY_TID:
case OB_ALL_DATABASE_PRIVILEGE_TID:
case OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID:
case OB_ALL_DDL_OPERATION_TID:
case OB_ALL_DEF_SUB_PART_TID:
case OB_ALL_DEF_SUB_PART_HISTORY_TID:
case OB_ALL_DUMMY_TID:
case OB_ALL_FOREIGN_KEY_TID:
case OB_ALL_FOREIGN_KEY_COLUMN_TID:
case OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID:
case OB_ALL_FOREIGN_KEY_HISTORY_TID:
case OB_ALL_FREEZE_SCHEMA_VERSION_TID:
case OB_ALL_FUNC_TID:
case OB_ALL_FUNC_HISTORY_TID:
case OB_ALL_GTS_TID:
case OB_ALL_HISTOGRAM_STAT_TID:
case OB_ALL_INDEX_BUILD_STAT_TID:
case OB_ALL_OUTLINE_TID:
case OB_ALL_OUTLINE_HISTORY_TID:
case OB_ALL_PACKAGE_TID:
case OB_ALL_PACKAGE_HISTORY_TID:
case OB_ALL_PART_TID:
case OB_ALL_PART_HISTORY_TID:
case OB_ALL_RESOURCE_POOL_TID:
case OB_ALL_ROOT_TABLE_TID:
case OB_ALL_ROUTINE_TID:
case OB_ALL_ROUTINE_HISTORY_TID:
case OB_ALL_ROUTINE_PARAM_TID:
case OB_ALL_ROUTINE_PARAM_HISTORY_TID:
case OB_ALL_SEQUENCE_OBJECT_TID:
case OB_ALL_SEQUENCE_OBJECT_HISTORY_TID:
case OB_ALL_SERVER_TID:
case OB_ALL_SYNONYM_TID:
case OB_ALL_SYNONYM_HISTORY_TID:
case OB_ALL_SYS_VARIABLE_TID:
case OB_ALL_SYS_VARIABLE_HISTORY_TID:
case OB_ALL_TABLE_TID:
case OB_ALL_TABLE_HISTORY_TID:
case OB_ALL_TABLE_PRIVILEGE_TID:
case OB_ALL_TABLE_PRIVILEGE_HISTORY_TID:
case OB_ALL_TABLE_STAT_TID:
case OB_ALL_TABLE_V2_TID:
case OB_ALL_TABLE_V2_HISTORY_TID:
case OB_ALL_TABLEGROUP_TID:
case OB_ALL_TABLEGROUP_HISTORY_TID:
case OB_ALL_TEMP_TABLE_TID:
case OB_ALL_TENANT_CONSTRAINT_COLUMN_TID:
case OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TID:
case OB_ALL_TENANT_GTS_TID:
case OB_ALL_TENANT_KEYSTORE_TID:
case OB_ALL_TENANT_KEYSTORE_HISTORY_TID:
case OB_ALL_TENANT_META_TABLE_TID:
case OB_ALL_TENANT_OBJAUTH_TID:
case OB_ALL_TENANT_OBJAUTH_HISTORY_TID:
case OB_ALL_TENANT_OBJECT_TYPE_TID:
case OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TID:
case OB_ALL_TENANT_PLAN_BASELINE_TID:
case OB_ALL_TENANT_PLAN_BASELINE_HISTORY_TID:
case OB_ALL_TENANT_PROFILE_TID:
case OB_ALL_TENANT_PROFILE_HISTORY_TID:
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID:
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID:
case OB_ALL_TENANT_SECURITY_AUDIT_TID:
case OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TID:
case OB_ALL_TENANT_SYSAUTH_TID:
case OB_ALL_TENANT_SYSAUTH_HISTORY_TID:
case OB_ALL_TENANT_TABLESPACE_TID:
case OB_ALL_TENANT_TABLESPACE_HISTORY_TID:
case OB_ALL_TENANT_TRIGGER_TID:
case OB_ALL_TENANT_TRIGGER_HISTORY_TID:
case OB_ALL_TYPE_TID:
case OB_ALL_TYPE_ATTR_TID:
case OB_ALL_TYPE_ATTR_HISTORY_TID:
case OB_ALL_TYPE_HISTORY_TID:
case OB_ALL_UNIT_TID:
case OB_ALL_UNIT_CONFIG_TID:
case OB_ALL_USER_TID:
case OB_ALL_USER_HISTORY_TID:
case OB_ALL_ZONE_TID:

#endif
