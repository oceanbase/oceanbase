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

case OB_ALL_VIRTUAL_COLL_TYPE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_LONG_OPS_STATUS_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_RESOURCE_POOL_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_ROUTINE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_SERVER_AGENT_TID:
case OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TYPE_SYS_AGENT_TID:
case OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID:
case OB_TENANT_VIRTUAL_CHARSET_AGENT_TID:
case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TID:
case OB_TENANT_VIRTUAL_OUTLINE_AGENT_TID:
case OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TID:
case OB_ALL_VIRTUAL_RESOURCE_POOL_MYSQL_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_MYSQL_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_VIRTUAL_LONG_OPS_STATUS_MYSQL_SYS_AGENT_TID:

#endif


#ifdef AGENT_VIRTUAL_TABLE_CREATE_ITER

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_COLL_TYPE_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
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

    case OB_ALL_VIRTUAL_LONG_OPS_STATUS_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_VIRTUAL_LONG_OPS_STATUS_TID;
      const bool sys_tenant_base_table = true;
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

    case OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
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

    case OB_ALL_VIRTUAL_RESOURCE_POOL_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_RESOURCE_POOL_TID;
      const bool sys_tenant_base_table = true;
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

    case OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
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
      ObAgentVirtualTable *agent_iter = NULL;
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

    case OB_ALL_VIRTUAL_SERVER_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
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

    case OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
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

    case OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_TENANT_OBJECT_TYPE_TID;
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

    case OB_ALL_VIRTUAL_TENANT_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_TENANT_TID;
      const bool sys_tenant_base_table = true;
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

    case OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
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

    case OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
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
      ObAgentVirtualTable *agent_iter = NULL;
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

    case OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
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
      ObAgentVirtualTable *agent_iter = NULL;
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

    case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
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
      ObAgentVirtualTable *agent_iter = NULL;
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
      ObAgentVirtualTable *agent_iter = NULL;
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

    case OB_ALL_VIRTUAL_RESOURCE_POOL_MYSQL_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_RESOURCE_POOL_TID;
      const bool sys_tenant_base_table = true;
      const bool only_sys_data = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data, Worker::CompatMode::MYSQL))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        agent_iter->~ObAgentVirtualTable();
        allocator.free(agent_iter);
        agent_iter = NULL;
      } else {
       vt_iter = agent_iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_MYSQL_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_TENANT_TID;
      const bool sys_tenant_base_table = true;
      const bool only_sys_data = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data, Worker::CompatMode::MYSQL))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        agent_iter->~ObAgentVirtualTable();
        allocator.free(agent_iter);
        agent_iter = NULL;
      } else {
       vt_iter = agent_iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_VIRTUAL_LONG_OPS_STATUS_MYSQL_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_VIRTUAL_LONG_OPS_STATUS_TID;
      const bool sys_tenant_base_table = true;
      const bool only_sys_data = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAgentVirtualTable, agent_iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(agent_iter->init(base_tid, sys_tenant_base_table, index_schema, params, only_sys_data, Worker::CompatMode::MYSQL))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        agent_iter->~ObAgentVirtualTable();
        allocator.free(agent_iter);
        agent_iter = NULL;
      } else {
       vt_iter = agent_iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

#endif // AGENT_VIRTUAL_TABLE_CREATE_ITER



#ifdef ITERATE_PRIVATE_VIRTUAL_TABLE_LOCATION_SWITCH

case OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_TID:
case OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_TID:
case OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_TID:
case OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_HISTORY_TID:
case OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_TID:
case OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_TID:
case OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_TID:
case OB_ALL_VIRTUAL_BACKUP_INFO_TID:
case OB_ALL_VIRTUAL_BACKUP_JOB_TID:
case OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_TID:
case OB_ALL_VIRTUAL_BACKUP_LS_TASK_TID:
case OB_ALL_VIRTUAL_BACKUP_LS_TASK_HISTORY_TID:
case OB_ALL_VIRTUAL_BACKUP_LS_TASK_INFO_TID:
case OB_ALL_VIRTUAL_BACKUP_LS_TASK_INFO_HISTORY_TID:
case OB_ALL_VIRTUAL_BACKUP_PARAMETER_TID:
case OB_ALL_VIRTUAL_BACKUP_SET_FILES_TID:
case OB_ALL_VIRTUAL_BACKUP_SKIPPED_TABLET_TID:
case OB_ALL_VIRTUAL_BACKUP_SKIPPED_TABLET_HISTORY_TID:
case OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_TID:
case OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_TID:
case OB_ALL_VIRTUAL_BACKUP_TASK_TID:
case OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_TID:
case OB_ALL_VIRTUAL_BALANCE_GROUP_LS_STAT_TID:
case OB_ALL_VIRTUAL_BALANCE_TASK_HELPER_TID:
case OB_ALL_VIRTUAL_COLUMN_CHECKSUM_ERROR_INFO_TID:
case OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_TID:
case OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_TID:
case OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TID:
case OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_TID:
case OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_TID:
case OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_TID:
case OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_TID:
case OB_ALL_VIRTUAL_KV_TTL_TASK_TID:
case OB_ALL_VIRTUAL_KV_TTL_TASK_HISTORY_TID:
case OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_TID:
case OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_TID:
case OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_TID:
case OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_TID:
case OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_TID:
case OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_TID:
case OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_TID:
case OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_TID:
case OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_TID:
case OB_ALL_VIRTUAL_LS_META_TABLE_TID:
case OB_ALL_VIRTUAL_LS_RECOVERY_STAT_TID:
case OB_ALL_VIRTUAL_LS_REPLICA_TASK_TID:
case OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_TID:
case OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_TID:
case OB_ALL_VIRTUAL_LS_STATUS_TID:
case OB_ALL_VIRTUAL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TID:
case OB_ALL_VIRTUAL_MERGE_INFO_TID:
case OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_TID:
case OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_TID:
case OB_ALL_VIRTUAL_RESTORE_JOB_TID:
case OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_TID:
case OB_ALL_VIRTUAL_RESTORE_PROGRESS_TID:
case OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_TID:
case OB_ALL_VIRTUAL_TABLET_META_TABLE_TID:
case OB_ALL_VIRTUAL_TABLET_REPLICA_CHECKSUM_TID:
case OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_INFO_TID:
case OB_ALL_VIRTUAL_TENANT_PARAMETER_TID:
case OB_ALL_VIRTUAL_TENANT_USER_FAILED_LOGIN_STAT_TID:
case OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_TID:
case OB_ALL_VIRTUAL_WR_CONTROL_TID:
case OB_ALL_VIRTUAL_WR_SNAPSHOT_TID:
case OB_ALL_VIRTUAL_WR_STATNAME_TID:
case OB_ALL_VIRTUAL_WR_SYSSTAT_TID:
case OB_ALL_VIRTUAL_ZONE_MERGE_INFO_TID:

#endif


#ifdef ITERATE_PRIVATE_VIRTUAL_TABLE_CREATE_ITER

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_DELETE_JOB_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_DELETE_JOB_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_DELETE_LS_TASK_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_DELETE_POLICY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_DELETE_TASK_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_DELETE_TASK_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_INFO_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_INFO_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_JOB_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_JOB_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_JOB_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_LS_TASK_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_LS_TASK_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_LS_TASK_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_LS_TASK_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_LS_TASK_INFO_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_LS_TASK_INFO_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_LS_TASK_INFO_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_PARAMETER_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_PARAMETER_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_SET_FILES_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_SET_FILES_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_SKIPPED_TABLET_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_SKIPPED_TABLET_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_SKIPPED_TABLET_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_STORAGE_INFO_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_STORAGE_INFO_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_BACKUP_TASK_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_TASK_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BACKUP_TASK_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BALANCE_GROUP_LS_STAT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BALANCE_GROUP_LS_STAT_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BALANCE_TASK_HELPER_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BALANCE_TASK_HELPER_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_COLUMN_CHECKSUM_ERROR_INFO_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DEADLOCK_EVENT_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_GLOBAL_CONTEXT_VALUE_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_GLOBAL_TRANSACTION_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_IMPORT_TABLE_JOB_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_IMPORT_TABLE_JOB_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_IMPORT_TABLE_TASK_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_IMPORT_TABLE_TASK_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_KV_TTL_TASK_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_KV_TTL_TASK_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_KV_TTL_TASK_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_KV_TTL_TASK_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LOG_ARCHIVE_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LOG_ARCHIVE_PROGRESS_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LOG_RESTORE_SOURCE_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_ARB_REPLICA_TASK_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = true;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_ELECTION_REFERENCE_INFO_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_META_TABLE_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = true;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_META_TABLE_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_RECOVERY_STAT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = true;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_RECOVERY_STAT_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_REPLICA_TASK_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_REPLICA_TASK_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_RESTORE_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_RESTORE_PROGRESS_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_STATUS_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = true;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_STATUS_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MERGE_INFO_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MERGE_INFO_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RECOVER_TABLE_JOB_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RECOVER_TABLE_JOB_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RESTORE_JOB_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RESTORE_JOB_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RESTORE_JOB_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RESTORE_PROGRESS_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RESTORE_PROGRESS_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLET_META_TABLE_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLET_META_TABLE_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLET_REPLICA_CHECKSUM_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLET_REPLICA_CHECKSUM_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_EVENT_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_INFO_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_INFO_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_PARAMETER_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_TENANT_PARAMETER_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_USER_FAILED_LOGIN_STAT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_ACTIVE_SESSION_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_WR_CONTROL_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_CONTROL_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_WR_SNAPSHOT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_SNAPSHOT_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_WR_STATNAME_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_STATNAME_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_WR_SYSSTAT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_SYSSTAT_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_ZONE_MERGE_INFO_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_ZONE_MERGE_INFO_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

#endif // ITERATE_PRIVATE_VIRTUAL_TABLE_CREATE_ITER


#ifdef ITERATE_VIRTUAL_TABLE_LOCATION_SWITCH

case OB_ALL_VIRTUAL_AUTO_INCREMENT_TID:
case OB_ALL_VIRTUAL_BALANCE_JOB_TID:
case OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_TID:
case OB_ALL_VIRTUAL_BALANCE_TASK_TID:
case OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_TID:
case OB_ALL_VIRTUAL_COLL_TYPE_TID:
case OB_ALL_VIRTUAL_COLL_TYPE_HISTORY_TID:
case OB_ALL_VIRTUAL_COLUMN_TID:
case OB_ALL_VIRTUAL_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_COLUMN_STAT_TID:
case OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_TID:
case OB_ALL_VIRTUAL_COLUMN_USAGE_TID:
case OB_ALL_VIRTUAL_CONSTRAINT_TID:
case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_TID:
case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_CONSTRAINT_HISTORY_TID:
case OB_ALL_VIRTUAL_CORE_TABLE_TID:
case OB_ALL_VIRTUAL_DAM_CLEANUP_JOBS_TID:
case OB_ALL_VIRTUAL_DAM_LAST_ARCH_TS_TID:
case OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_TID:
case OB_ALL_VIRTUAL_DATABASE_TID:
case OB_ALL_VIRTUAL_DATABASE_HISTORY_TID:
case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_TID:
case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_HISTORY_TID:
case OB_ALL_VIRTUAL_DBLINK_TID:
case OB_ALL_VIRTUAL_DBLINK_HISTORY_TID:
case OB_ALL_VIRTUAL_DDL_CHECKSUM_TID:
case OB_ALL_VIRTUAL_DDL_ERROR_MESSAGE_TID:
case OB_ALL_VIRTUAL_DDL_OPERATION_TID:
case OB_ALL_VIRTUAL_DDL_TASK_STATUS_TID:
case OB_ALL_VIRTUAL_DEF_SUB_PART_TID:
case OB_ALL_VIRTUAL_DEF_SUB_PART_HISTORY_TID:
case OB_ALL_VIRTUAL_DEPENDENCY_TID:
case OB_ALL_VIRTUAL_ERROR_TID:
case OB_ALL_VIRTUAL_EXTERNAL_TABLE_FILE_TID:
case OB_ALL_VIRTUAL_FOREIGN_KEY_TID:
case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_TID:
case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_FOREIGN_KEY_HISTORY_TID:
case OB_ALL_VIRTUAL_FREEZE_INFO_TID:
case OB_ALL_VIRTUAL_FUNC_TID:
case OB_ALL_VIRTUAL_FUNC_HISTORY_TID:
case OB_ALL_VIRTUAL_HISTOGRAM_STAT_TID:
case OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_TID:
case OB_ALL_VIRTUAL_JOB_TID:
case OB_ALL_VIRTUAL_JOB_LOG_TID:
case OB_ALL_VIRTUAL_LS_TID:
case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_TID:
case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_COLUMN_TID:
case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_HISTORY_TID:
case OB_ALL_VIRTUAL_MONITOR_MODIFIED_TID:
case OB_ALL_VIRTUAL_OBJAUTH_TID:
case OB_ALL_VIRTUAL_OBJAUTH_HISTORY_TID:
case OB_ALL_VIRTUAL_OBJECT_TYPE_TID:
case OB_ALL_VIRTUAL_OPTSTAT_GLOBAL_PREFS_TID:
case OB_ALL_VIRTUAL_OPTSTAT_USER_PREFS_TID:
case OB_ALL_VIRTUAL_ORI_SCHEMA_VERSION_TID:
case OB_ALL_VIRTUAL_OUTLINE_TID:
case OB_ALL_VIRTUAL_OUTLINE_HISTORY_TID:
case OB_ALL_VIRTUAL_PACKAGE_TID:
case OB_ALL_VIRTUAL_PACKAGE_HISTORY_TID:
case OB_ALL_VIRTUAL_PART_TID:
case OB_ALL_VIRTUAL_PART_HISTORY_TID:
case OB_ALL_VIRTUAL_PART_INFO_TID:
case OB_ALL_VIRTUAL_PART_INFO_HISTORY_TID:
case OB_ALL_VIRTUAL_PENDING_TRANSACTION_TID:
case OB_ALL_VIRTUAL_PLAN_BASELINE_TID:
case OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_TID:
case OB_ALL_VIRTUAL_RECYCLEBIN_TID:
case OB_ALL_VIRTUAL_RLS_ATTRIBUTE_TID:
case OB_ALL_VIRTUAL_RLS_ATTRIBUTE_HISTORY_TID:
case OB_ALL_VIRTUAL_RLS_CONTEXT_TID:
case OB_ALL_VIRTUAL_RLS_CONTEXT_HISTORY_TID:
case OB_ALL_VIRTUAL_RLS_GROUP_TID:
case OB_ALL_VIRTUAL_RLS_GROUP_HISTORY_TID:
case OB_ALL_VIRTUAL_RLS_POLICY_TID:
case OB_ALL_VIRTUAL_RLS_POLICY_HISTORY_TID:
case OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_TID:
case OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_ROUTINE_TID:
case OB_ALL_VIRTUAL_ROUTINE_HISTORY_TID:
case OB_ALL_VIRTUAL_ROUTINE_PARAM_TID:
case OB_ALL_VIRTUAL_ROUTINE_PARAM_HISTORY_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_TID:
case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_TID:
case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_HISTORY_TID:
case OB_ALL_VIRTUAL_SEQUENCE_VALUE_TID:
case OB_ALL_VIRTUAL_SPM_CONFIG_TID:
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
case OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_TID:
case OB_ALL_VIRTUAL_TABLEGROUP_TID:
case OB_ALL_VIRTUAL_TABLEGROUP_HISTORY_TID:
case OB_ALL_VIRTUAL_TABLET_TO_LS_TID:
case OB_ALL_VIRTUAL_TABLET_TO_TABLE_HISTORY_TID:
case OB_ALL_VIRTUAL_TEMP_TABLE_TID:
case OB_ALL_VIRTUAL_TENANT_CONTEXT_TID:
case OB_ALL_VIRTUAL_TENANT_CONTEXT_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_DIRECTORY_TID:
case OB_ALL_VIRTUAL_TENANT_DIRECTORY_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_KEYSTORE_TID:
case OB_ALL_VIRTUAL_TENANT_KEYSTORE_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_TID:
case OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_OLS_LABEL_TID:
case OB_ALL_VIRTUAL_TENANT_OLS_LABEL_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_OLS_POLICY_TID:
case OB_ALL_VIRTUAL_TENANT_OLS_POLICY_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_TID:
case OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_PROFILE_TID:
case OB_ALL_VIRTUAL_TENANT_PROFILE_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_TID:
case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_TID:
case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_CLASS_TID:
case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TID:
case OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_TID:
case OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TID:
case OB_ALL_VIRTUAL_TENANT_TABLESPACE_TID:
case OB_ALL_VIRTUAL_TENANT_TABLESPACE_HISTORY_TID:
case OB_ALL_VIRTUAL_TIME_ZONE_TID:
case OB_ALL_VIRTUAL_TIME_ZONE_NAME_TID:
case OB_ALL_VIRTUAL_TIME_ZONE_TRANSITION_TID:
case OB_ALL_VIRTUAL_TIME_ZONE_TRANSITION_TYPE_TID:
case OB_ALL_VIRTUAL_TRANSFER_TASK_TID:
case OB_ALL_VIRTUAL_TRANSFER_TASK_HISTORY_TID:
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

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_AUTO_INCREMENT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_AUTO_INCREMENT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BALANCE_JOB_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BALANCE_JOB_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BALANCE_JOB_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BALANCE_TASK_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BALANCE_TASK_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_BALANCE_TASK_HISTORY_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLL_TYPE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_COLL_TYPE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLL_TYPE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_COLUMN_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_COLUMN_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_COLUMN_STAT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_STAT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_STAT_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_COLUMN_USAGE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_USAGE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CONSTRAINT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CONSTRAINT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_CONSTRAINT_COLUMN_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CONSTRAINT_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CONSTRAINT_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CORE_TABLE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CORE_TABLE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DAM_CLEANUP_JOBS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DAM_CLEANUP_JOBS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DAM_LAST_ARCH_TS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DAM_LAST_ARCH_TS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DATA_DICTIONARY_IN_LOG_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_DATABASE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DATABASE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DATABASE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DATABASE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DATABASE_PRIVILEGE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DBLINK_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DBLINK_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DBLINK_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DBLINK_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DDL_CHECKSUM_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DDL_CHECKSUM_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DDL_ERROR_MESSAGE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DDL_ERROR_MESSAGE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DDL_OPERATION_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DDL_OPERATION_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DDL_TASK_STATUS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DDL_TASK_STATUS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DEF_SUB_PART_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DEF_SUB_PART_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DEF_SUB_PART_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DEF_SUB_PART_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_DEPENDENCY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_DEPENDENCY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_ERROR_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_ERROR_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_EXTERNAL_TABLE_FILE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_EXTERNAL_TABLE_FILE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_FOREIGN_KEY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_FOREIGN_KEY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_FOREIGN_KEY_COLUMN_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_FOREIGN_KEY_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_FOREIGN_KEY_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_FREEZE_INFO_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_FREEZE_INFO_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_FUNC_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_FUNC_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_FUNC_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_FUNC_HISTORY_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_HISTOGRAM_STAT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_HISTOGRAM_STAT_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_JOB_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_JOB_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_JOB_LOG_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_JOB_LOG_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_LS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MOCK_FK_PARENT_TABLE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_COLUMN_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MONITOR_MODIFIED_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MONITOR_MODIFIED_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OBJAUTH_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_OBJAUTH_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OBJAUTH_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_OBJECT_TYPE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OBJECT_TYPE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_OPTSTAT_GLOBAL_PREFS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_OPTSTAT_GLOBAL_PREFS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_OPTSTAT_USER_PREFS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_OPTSTAT_USER_PREFS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_ORI_SCHEMA_VERSION_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_ORI_SCHEMA_VERSION_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_OUTLINE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_OUTLINE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_OUTLINE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_OUTLINE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_PACKAGE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PACKAGE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PACKAGE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PACKAGE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PART_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PART_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PART_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PART_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PART_INFO_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PART_INFO_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PART_INFO_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PART_INFO_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PENDING_TRANSACTION_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PENDING_TRANSACTION_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PLAN_BASELINE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PLAN_BASELINE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PLAN_BASELINE_ITEM_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RECYCLEBIN_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RLS_ATTRIBUTE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RLS_ATTRIBUTE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RLS_ATTRIBUTE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RLS_ATTRIBUTE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RLS_CONTEXT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RLS_CONTEXT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RLS_CONTEXT_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RLS_CONTEXT_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RLS_GROUP_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RLS_GROUP_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RLS_GROUP_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RLS_GROUP_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RLS_POLICY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RLS_POLICY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RLS_POLICY_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RLS_POLICY_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RLS_SECURITY_COLUMN_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RLS_SECURITY_COLUMN_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_ROUTINE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_ROUTINE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_ROUTINE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_ROUTINE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_ROUTINE_PARAM_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_ROUTINE_PARAM_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_ROUTINE_PARAM_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_ROUTINE_PARAM_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SECURITY_AUDIT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SECURITY_AUDIT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SEQUENCE_OBJECT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SEQUENCE_OBJECT_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SEQUENCE_VALUE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SEQUENCE_VALUE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SPM_CONFIG_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SPM_CONFIG_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SUB_PART_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SUB_PART_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SUB_PART_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SUB_PART_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SYNONYM_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SYNONYM_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SYNONYM_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SYNONYM_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SYS_STAT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SYS_STAT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SYS_VARIABLE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SYS_VARIABLE_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SYS_VARIABLE_HISTORY_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SYSAUTH_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SYSAUTH_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SYSAUTH_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_TABLE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLE_PRIVILEGE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLE_PRIVILEGE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLE_STAT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLE_STAT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLE_STAT_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLEGROUP_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLEGROUP_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLEGROUP_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLEGROUP_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLET_TO_LS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLET_TO_LS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TABLET_TO_TABLE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLET_TO_TABLE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TEMP_TABLE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TEMP_TABLE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_CONTEXT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CONTEXT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_CONTEXT_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CONTEXT_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_DIRECTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_DIRECTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_DIRECTORY_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_DIRECTORY_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_KEYSTORE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_KEYSTORE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_KEYSTORE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_KEYSTORE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OLS_COMPONENT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OLS_COMPONENT_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_OLS_LABEL_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OLS_LABEL_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_TENANT_OLS_LABEL_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OLS_LABEL_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_OLS_POLICY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OLS_POLICY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_OLS_POLICY_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OLS_POLICY_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OLS_USER_LEVEL_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_PROFILE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_PROFILE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_PROFILE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_PROFILE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SCHEDULER_JOB_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_CLASS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SCHEDULER_PROGRAM_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_TABLESPACE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TABLESPACE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_TABLESPACE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TABLESPACE_HISTORY_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TIME_ZONE_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TIME_ZONE_NAME_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_TRANSFER_TASK_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TRANSFER_TASK_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TRANSFER_TASK_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TRANSFER_TASK_HISTORY_TID, index_schema, params))) {
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
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TRIGGER_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TRIGGER_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_TRIGGER_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TYPE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TYPE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TYPE_ATTR_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TYPE_ATTR_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TYPE_ATTR_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TYPE_ATTR_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TYPE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TYPE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_USER_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_USER_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_USER_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_USER_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }
  END_CREATE_VT_ITER_SWITCH_LAMBDA

#endif // ITERATE_VIRTUAL_TABLE_CREATE_ITER


#ifdef CLUSTER_PRIVATE_TABLE_SWITCH

case OB_ALL_BACKUP_DELETE_JOB_TID:
case OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_META_TID:
case OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_DELETE_JOB_HISTORY_TID:
case OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_META_TID:
case OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_DELETE_LS_TASK_TID:
case OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_META_TID:
case OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_TID:
case OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_META_TID:
case OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_DELETE_POLICY_TID:
case OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_META_TID:
case OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_DELETE_TASK_TID:
case OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_META_TID:
case OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_DELETE_TASK_HISTORY_TID:
case OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_META_TID:
case OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_INFO_TID:
case OB_ALL_BACKUP_INFO_AUX_LOB_META_TID:
case OB_ALL_BACKUP_INFO_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_JOB_TID:
case OB_ALL_BACKUP_JOB_AUX_LOB_META_TID:
case OB_ALL_BACKUP_JOB_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_JOB_HISTORY_TID:
case OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_META_TID:
case OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_LS_TASK_TID:
case OB_ALL_BACKUP_LS_TASK_AUX_LOB_META_TID:
case OB_ALL_BACKUP_LS_TASK_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_LS_TASK_HISTORY_TID:
case OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_META_TID:
case OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_LS_TASK_INFO_TID:
case OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_META_TID:
case OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_TID:
case OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_META_TID:
case OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_PARAMETER_TID:
case OB_ALL_BACKUP_PARAMETER_AUX_LOB_META_TID:
case OB_ALL_BACKUP_PARAMETER_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_SET_FILES_TID:
case OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID:
case OB_ALL_BACKUP_SET_FILES_AUX_LOB_META_TID:
case OB_ALL_BACKUP_SET_FILES_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_SKIPPED_TABLET_TID:
case OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_META_TID:
case OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_TID:
case OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_META_TID:
case OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_STORAGE_INFO_TID:
case OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_META_TID:
case OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_STORAGE_INFO_HISTORY_TID:
case OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_META_TID:
case OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_TASK_TID:
case OB_ALL_BACKUP_TASK_AUX_LOB_META_TID:
case OB_ALL_BACKUP_TASK_AUX_LOB_PIECE_TID:
case OB_ALL_BACKUP_TASK_HISTORY_TID:
case OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_META_TID:
case OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_BALANCE_GROUP_LS_STAT_TID:
case OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_META_TID:
case OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_PIECE_TID:
case OB_ALL_BALANCE_TASK_HELPER_TID:
case OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_META_TID:
case OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_PIECE_TID:
case OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_TID:
case OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_META_TID:
case OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_PIECE_TID:
case OB_ALL_DEADLOCK_EVENT_HISTORY_TID:
case OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_META_TID:
case OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_GLOBAL_CONTEXT_VALUE_TID:
case OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_META_TID:
case OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_PIECE_TID:
case OB_ALL_IMPORT_TABLE_JOB_TID:
case OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_META_TID:
case OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_PIECE_TID:
case OB_ALL_IMPORT_TABLE_JOB_HISTORY_TID:
case OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_META_TID:
case OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_IMPORT_TABLE_TASK_TID:
case OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_META_TID:
case OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_PIECE_TID:
case OB_ALL_IMPORT_TABLE_TASK_HISTORY_TID:
case OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_META_TID:
case OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_KV_TTL_TASK_TID:
case OB_ALL_KV_TTL_TASK_IDX_KV_TTL_TASK_TABLE_ID_TID:
case OB_ALL_KV_TTL_TASK_AUX_LOB_META_TID:
case OB_ALL_KV_TTL_TASK_AUX_LOB_PIECE_TID:
case OB_ALL_KV_TTL_TASK_HISTORY_TID:
case OB_ALL_KV_TTL_TASK_HISTORY_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_TID:
case OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_META_TID:
case OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TID:
case OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_META_TID:
case OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_PIECE_TID:
case OB_ALL_LOG_ARCHIVE_HISTORY_TID:
case OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_META_TID:
case OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID:
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID:
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_META_TID:
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_PIECE_TID:
case OB_ALL_LOG_ARCHIVE_PROGRESS_TID:
case OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_TID:
case OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_TID:
case OB_ALL_LOG_RESTORE_SOURCE_TID:
case OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_META_TID:
case OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_PIECE_TID:
case OB_ALL_LS_ARB_REPLICA_TASK_TID:
case OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_META_TID:
case OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_PIECE_TID:
case OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_TID:
case OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_META_TID:
case OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_LS_ELECTION_REFERENCE_INFO_TID:
case OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_META_TID:
case OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_PIECE_TID:
case OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TID:
case OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_TID:
case OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_TID:
case OB_ALL_LS_META_TABLE_TID:
case OB_ALL_LS_META_TABLE_AUX_LOB_META_TID:
case OB_ALL_LS_META_TABLE_AUX_LOB_PIECE_TID:
case OB_ALL_LS_RECOVERY_STAT_TID:
case OB_ALL_LS_RECOVERY_STAT_AUX_LOB_META_TID:
case OB_ALL_LS_RECOVERY_STAT_AUX_LOB_PIECE_TID:
case OB_ALL_LS_REPLICA_TASK_TID:
case OB_ALL_LS_REPLICA_TASK_AUX_LOB_META_TID:
case OB_ALL_LS_REPLICA_TASK_AUX_LOB_PIECE_TID:
case OB_ALL_LS_RESTORE_HISTORY_TID:
case OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_META_TID:
case OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_LS_RESTORE_PROGRESS_TID:
case OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_META_TID:
case OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_PIECE_TID:
case OB_ALL_LS_STATUS_TID:
case OB_ALL_LS_STATUS_AUX_LOB_META_TID:
case OB_ALL_LS_STATUS_AUX_LOB_PIECE_TID:
case OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TID:
case OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_META_TID:
case OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_PIECE_TID:
case OB_ALL_MERGE_INFO_TID:
case OB_ALL_MERGE_INFO_AUX_LOB_META_TID:
case OB_ALL_MERGE_INFO_AUX_LOB_PIECE_TID:
case OB_ALL_RECOVER_TABLE_JOB_TID:
case OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_META_TID:
case OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_PIECE_TID:
case OB_ALL_RECOVER_TABLE_JOB_HISTORY_TID:
case OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_META_TID:
case OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_RESERVED_SNAPSHOT_TID:
case OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_META_TID:
case OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_PIECE_TID:
case OB_ALL_RESTORE_INFO_TID:
case OB_ALL_RESTORE_INFO_AUX_LOB_META_TID:
case OB_ALL_RESTORE_INFO_AUX_LOB_PIECE_TID:
case OB_ALL_RESTORE_JOB_TID:
case OB_ALL_RESTORE_JOB_AUX_LOB_META_TID:
case OB_ALL_RESTORE_JOB_AUX_LOB_PIECE_TID:
case OB_ALL_RESTORE_JOB_HISTORY_TID:
case OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_META_TID:
case OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_RESTORE_PROGRESS_TID:
case OB_ALL_RESTORE_PROGRESS_AUX_LOB_META_TID:
case OB_ALL_RESTORE_PROGRESS_AUX_LOB_PIECE_TID:
case OB_ALL_SERVICE_EPOCH_TID:
case OB_ALL_SERVICE_EPOCH_AUX_LOB_META_TID:
case OB_ALL_SERVICE_EPOCH_AUX_LOB_PIECE_TID:
case OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TID:
case OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TID:
case OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_TABLET_META_TABLE_TID:
case OB_ALL_TABLET_META_TABLE_AUX_LOB_META_TID:
case OB_ALL_TABLET_META_TABLE_AUX_LOB_PIECE_TID:
case OB_ALL_TABLET_REPLICA_CHECKSUM_TID:
case OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_META_TID:
case OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_PIECE_TID:
case OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_TID:
case OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TID:
case OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_TENANT_EVENT_HISTORY_TID:
case OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_META_TID:
case OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_TENANT_GLOBAL_TRANSACTION_TID:
case OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TID:
case OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_META_TID:
case OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_PIECE_TID:
case OB_ALL_TENANT_INFO_TID:
case OB_ALL_TENANT_INFO_AUX_LOB_META_TID:
case OB_ALL_TENANT_INFO_AUX_LOB_PIECE_TID:
case OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TID:
case OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_META_TID:
case OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_PIECE_TID:
case OB_ALL_WEAK_READ_SERVICE_TID:
case OB_ALL_WEAK_READ_SERVICE_AUX_LOB_META_TID:
case OB_ALL_WEAK_READ_SERVICE_AUX_LOB_PIECE_TID:
case OB_ALL_ZONE_MERGE_INFO_TID:
case OB_ALL_ZONE_MERGE_INFO_AUX_LOB_META_TID:
case OB_ALL_ZONE_MERGE_INFO_AUX_LOB_PIECE_TID:
case OB_TENANT_PARAMETER_TID:
case OB_TENANT_PARAMETER_AUX_LOB_META_TID:
case OB_TENANT_PARAMETER_AUX_LOB_PIECE_TID:
case OB_WR_ACTIVE_SESSION_HISTORY_TID:
case OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_META_TID:
case OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_PIECE_TID:
case OB_WR_CONTROL_TID:
case OB_WR_CONTROL_AUX_LOB_META_TID:
case OB_WR_CONTROL_AUX_LOB_PIECE_TID:
case OB_WR_SNAPSHOT_TID:
case OB_WR_SNAPSHOT_AUX_LOB_META_TID:
case OB_WR_SNAPSHOT_AUX_LOB_PIECE_TID:
case OB_WR_STATNAME_TID:
case OB_WR_STATNAME_AUX_LOB_META_TID:
case OB_WR_STATNAME_AUX_LOB_PIECE_TID:
case OB_WR_SYSSTAT_TID:
case OB_WR_SYSSTAT_AUX_LOB_META_TID:
case OB_WR_SYSSTAT_AUX_LOB_PIECE_TID:

#endif


#ifdef SYS_INDEX_TABLE_ID_SWITCH

case OB_ALL_TABLE_IDX_DATA_TABLE_ID_TID:
case OB_ALL_TABLE_IDX_DB_TB_NAME_TID:
case OB_ALL_TABLE_IDX_TB_NAME_TID:
case OB_ALL_COLUMN_IDX_TB_COLUMN_NAME_TID:
case OB_ALL_COLUMN_IDX_COLUMN_NAME_TID:
case OB_ALL_DDL_OPERATION_IDX_DDL_TYPE_TID:
case OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID:
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID:
case OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID:
case OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TID:
case OB_ALL_USER_IDX_UR_NAME_TID:
case OB_ALL_DATABASE_IDX_DB_NAME_TID:
case OB_ALL_TABLEGROUP_IDX_TG_NAME_TID:
case OB_ALL_TENANT_HISTORY_IDX_TENANT_DELETED_TID:
case OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_MODULE_TID:
case OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_EVENT_TID:
case OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_DB_TYPE_TID:
case OB_ALL_PART_IDX_PART_NAME_TID:
case OB_ALL_SUB_PART_IDX_SUB_PART_NAME_TID:
case OB_ALL_DEF_SUB_PART_IDX_DEF_SUB_PART_NAME_TID:
case OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_MODULE_TID:
case OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_EVENT_TID:
case OB_ALL_ROOTSERVICE_JOB_IDX_RS_JOB_TYPE_TID:
case OB_ALL_FOREIGN_KEY_IDX_FK_CHILD_TID_TID:
case OB_ALL_FOREIGN_KEY_IDX_FK_PARENT_TID_TID:
case OB_ALL_FOREIGN_KEY_IDX_FK_NAME_TID:
case OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_CHILD_TID_TID:
case OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_PARENT_TID_TID:
case OB_ALL_SYNONYM_IDX_DB_SYNONYM_NAME_TID:
case OB_ALL_SYNONYM_IDX_SYNONYM_NAME_TID:
case OB_ALL_DDL_CHECKSUM_IDX_DDL_CHECKSUM_TASK_TID:
case OB_ALL_ROUTINE_IDX_DB_ROUTINE_NAME_TID:
case OB_ALL_ROUTINE_IDX_ROUTINE_NAME_TID:
case OB_ALL_ROUTINE_IDX_ROUTINE_PKG_ID_TID:
case OB_ALL_ROUTINE_PARAM_IDX_ROUTINE_PARAM_NAME_TID:
case OB_ALL_PACKAGE_IDX_DB_PKG_NAME_TID:
case OB_ALL_PACKAGE_IDX_PKG_NAME_TID:
case OB_ALL_ACQUIRED_SNAPSHOT_IDX_SNAPSHOT_TABLET_TID:
case OB_ALL_CONSTRAINT_IDX_CST_NAME_TID:
case OB_ALL_TYPE_IDX_DB_TYPE_NAME_TID:
case OB_ALL_TYPE_IDX_TYPE_NAME_TID:
case OB_ALL_TYPE_ATTR_IDX_TYPE_ATTR_NAME_TID:
case OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TID:
case OB_ALL_DBLINK_IDX_OWNER_DBLINK_NAME_TID:
case OB_ALL_DBLINK_IDX_DBLINK_NAME_TID:
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TID:
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_IDX_GRANTEE_HIS_ROLE_ID_TID:
case OB_ALL_TENANT_KEYSTORE_IDX_KEYSTORE_MASTER_KEY_ID_TID:
case OB_ALL_TENANT_KEYSTORE_HISTORY_IDX_KEYSTORE_HIS_MASTER_KEY_ID_TID:
case OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_NAME_TID:
case OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_COL_NAME_TID:
case OB_ALL_TENANT_OLS_COMPONENT_IDX_OLS_COM_POLICY_ID_TID:
case OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_POLICY_ID_TID:
case OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TAG_TID:
case OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TID:
case OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_UID_TID:
case OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_POLICY_ID_TID:
case OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TID:
case OB_ALL_TENANT_SECURITY_AUDIT_IDX_AUDIT_TYPE_TID:
case OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_BASE_OBJ_ID_TID:
case OB_ALL_TENANT_TRIGGER_IDX_DB_TRIGGER_NAME_TID:
case OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_NAME_TID:
case OB_ALL_TENANT_TRIGGER_HISTORY_IDX_TRIGGER_HIS_BASE_OBJ_ID_TID:
case OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTOR_TID:
case OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTEE_TID:
case OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_DB_OBJ_NAME_TID:
case OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_OBJ_NAME_TID:
case OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TID:
case OB_ALL_TENANT_DEPENDENCY_IDX_DEPENDENCY_REF_OBJ_TID:
case OB_ALL_DDL_ERROR_MESSAGE_IDX_DDL_ERROR_OBJECT_TID:
case OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TID:
case OB_ALL_COLUMN_STAT_HISTORY_IDX_COLUMN_STAT_HIS_SAVTIME_TID:
case OB_ALL_HISTOGRAM_STAT_HISTORY_IDX_HISTOGRAM_STAT_HIS_SAVTIME_TID:
case OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TID:
case OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TID:
case OB_ALL_PENDING_TRANSACTION_IDX_PENDING_TX_ID_TID:
case OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TID:
case OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_SQL_ID_TID:
case OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_VALUE_TID:
case OB_ALL_TENANT_DIRECTORY_IDX_DIRECTORY_NAME_TID:
case OB_ALL_JOB_IDX_JOB_POWNER_TID:
case OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_DB_NAME_TID:
case OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_NAME_TID:
case OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_ORI_NAME_TID:
case OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_DB_NAME_TID:
case OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_TB_NAME_TID:
case OB_ALL_DATABASE_PRIVILEGE_IDX_DB_PRIV_DB_NAME_TID:
case OB_ALL_RLS_POLICY_IDX_RLS_POLICY_TABLE_ID_TID:
case OB_ALL_RLS_POLICY_IDX_RLS_POLICY_GROUP_ID_TID:
case OB_ALL_RLS_POLICY_HISTORY_IDX_RLS_POLICY_HIS_TABLE_ID_TID:
case OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TID:
case OB_ALL_RLS_GROUP_HISTORY_IDX_RLS_GROUP_HIS_TABLE_ID_TID:
case OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TID:
case OB_ALL_RLS_CONTEXT_HISTORY_IDX_RLS_CONTEXT_HIS_TABLE_ID_TID:
case OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_TID:
case OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_TID:
case OB_ALL_KV_TTL_TASK_IDX_KV_TTL_TASK_TABLE_ID_TID:
case OB_ALL_KV_TTL_TASK_HISTORY_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_TID:

#endif


#ifdef SYS_INDEX_DATA_TABLE_ID_SWITCH

case OB_ALL_TABLET_TO_LS_TID:
case OB_ALL_RLS_GROUP_TID:
case OB_ALL_DEF_SUB_PART_TID:
case OB_ALL_RLS_CONTEXT_HISTORY_TID:
case OB_ALL_RLS_POLICY_HISTORY_TID:
case OB_ALL_TENANT_OLS_USER_LEVEL_TID:
case OB_ALL_RECYCLEBIN_TID:
case OB_ALL_TENANT_KEYSTORE_HISTORY_TID:
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID:
case OB_ALL_TABLE_STAT_HISTORY_TID:
case OB_ALL_CONTEXT_TID:
case OB_ALL_TABLE_PRIVILEGE_TID:
case OB_ALL_DDL_CHECKSUM_TID:
case OB_ALL_RLS_POLICY_TID:
case OB_ALL_TENANT_OBJAUTH_TID:
case OB_ALL_PACKAGE_TID:
case OB_ALL_ROOTSERVICE_EVENT_HISTORY_TID:
case OB_ALL_TENANT_OLS_COMPONENT_TID:
case OB_ALL_TENANT_TRIGGER_TID:
case OB_ALL_TYPE_ATTR_TID:
case OB_ALL_TYPE_TID:
case OB_ALL_DBLINK_TID:
case OB_ALL_ROUTINE_PARAM_TID:
case OB_ALL_DATABASE_PRIVILEGE_TID:
case OB_ALL_TENANT_OLS_LABEL_TID:
case OB_ALL_TENANT_KEYSTORE_TID:
case OB_ALL_COLUMN_TID:
case OB_ALL_KV_TTL_TASK_TID:
case OB_ALL_RLS_CONTEXT_TID:
case OB_ALL_BACKUP_SET_FILES_TID:
case OB_ALL_SEQUENCE_OBJECT_TID:
case OB_ALL_DDL_OPERATION_TID:
case OB_ALL_TABLE_TID:
case OB_ALL_ACQUIRED_SNAPSHOT_TID:
case OB_ALL_TENANT_PROFILE_TID:
case OB_ALL_USER_TID:
case OB_ALL_TENANT_OLS_POLICY_TID:
case OB_ALL_SERVER_EVENT_HISTORY_TID:
case OB_ALL_DDL_TASK_STATUS_TID:
case OB_ALL_PART_TID:
case OB_ALL_HISTOGRAM_STAT_HISTORY_TID:
case OB_ALL_DBMS_LOCK_ALLOCATED_TID:
case OB_ALL_PLAN_BASELINE_ITEM_TID:
case OB_ALL_TENANT_GLOBAL_TRANSACTION_TID:
case OB_ALL_DATABASE_TID:
case OB_ALL_TABLE_HISTORY_TID:
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID:
case OB_ALL_JOB_TID:
case OB_ALL_TENANT_DEPENDENCY_TID:
case OB_ALL_TABLEGROUP_TID:
case OB_ALL_TENANT_HISTORY_TID:
case OB_ALL_SUB_PART_TID:
case OB_ALL_ROUTINE_TID:
case OB_ALL_TENANT_DIRECTORY_TID:
case OB_ALL_DDL_ERROR_MESSAGE_TID:
case OB_ALL_TENANT_TRIGGER_HISTORY_TID:
case OB_ALL_CONSTRAINT_TID:
case OB_ALL_COLL_TYPE_TID:
case OB_ALL_COLUMN_STAT_HISTORY_TID:
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID:
case OB_ALL_TENANT_OBJECT_TYPE_TID:
case OB_ALL_FOREIGN_KEY_HISTORY_TID:
case OB_ALL_RLS_GROUP_HISTORY_TID:
case OB_ALL_SYNONYM_TID:
case OB_ALL_TENANT_SECURITY_AUDIT_TID:
case OB_ALL_ROOTSERVICE_JOB_TID:
case OB_ALL_PENDING_TRANSACTION_TID:
case OB_ALL_KV_TTL_TASK_HISTORY_TID:
case OB_ALL_FOREIGN_KEY_TID:

#endif


#ifdef SYS_INDEX_DATA_TABLE_ID_TO_INDEX_IDS_SWITCH

case OB_ALL_TABLET_TO_LS_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_RLS_GROUP_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_DEF_SUB_PART_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DEF_SUB_PART_IDX_DEF_SUB_PART_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_RLS_CONTEXT_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_RLS_CONTEXT_HISTORY_IDX_RLS_CONTEXT_HIS_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_RLS_POLICY_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_RLS_POLICY_HISTORY_IDX_RLS_POLICY_HIS_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_OLS_USER_LEVEL_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_UID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_POLICY_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_RECYCLEBIN_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_DB_TYPE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_ORI_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_KEYSTORE_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_KEYSTORE_HISTORY_IDX_KEYSTORE_HIS_MASTER_KEY_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_IDX_GRANTEE_HIS_ROLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TABLE_STAT_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_CONTEXT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TABLE_PRIVILEGE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_DB_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_TB_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_DDL_CHECKSUM_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DDL_CHECKSUM_IDX_DDL_CHECKSUM_TASK_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_RLS_POLICY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_RLS_POLICY_IDX_RLS_POLICY_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_RLS_POLICY_IDX_RLS_POLICY_GROUP_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_OBJAUTH_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTOR_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTEE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_PACKAGE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_PACKAGE_IDX_DB_PKG_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_PACKAGE_IDX_PKG_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_ROOTSERVICE_EVENT_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_MODULE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_EVENT_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_OLS_COMPONENT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OLS_COMPONENT_IDX_OLS_COM_POLICY_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_TRIGGER_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_BASE_OBJ_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_TRIGGER_IDX_DB_TRIGGER_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TYPE_ATTR_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TYPE_ATTR_IDX_TYPE_ATTR_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TYPE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TYPE_IDX_DB_TYPE_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TYPE_IDX_TYPE_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_DBLINK_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DBLINK_IDX_OWNER_DBLINK_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_DBLINK_IDX_DBLINK_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_ROUTINE_PARAM_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_ROUTINE_PARAM_IDX_ROUTINE_PARAM_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_DATABASE_PRIVILEGE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DATABASE_PRIVILEGE_IDX_DB_PRIV_DB_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_OLS_LABEL_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_POLICY_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TAG_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_KEYSTORE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_KEYSTORE_IDX_KEYSTORE_MASTER_KEY_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_COLUMN_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_COLUMN_IDX_TB_COLUMN_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_COLUMN_IDX_COLUMN_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_KV_TTL_TASK_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_KV_TTL_TASK_IDX_KV_TTL_TASK_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_RLS_CONTEXT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_BACKUP_SET_FILES_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_SEQUENCE_OBJECT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_DB_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_DDL_OPERATION_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DDL_OPERATION_IDX_DDL_TYPE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TABLE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLE_IDX_DATA_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLE_IDX_DB_TB_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLE_IDX_TB_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_ACQUIRED_SNAPSHOT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_ACQUIRED_SNAPSHOT_IDX_SNAPSHOT_TABLET_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_PROFILE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_USER_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_USER_IDX_UR_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_OLS_POLICY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_COL_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_SERVER_EVENT_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_MODULE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_EVENT_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_DDL_TASK_STATUS_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_PART_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_PART_IDX_PART_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_HISTOGRAM_STAT_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_HISTOGRAM_STAT_HISTORY_IDX_HISTOGRAM_STAT_HIS_SAVTIME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_DBMS_LOCK_ALLOCATED_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_PLAN_BASELINE_ITEM_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_SQL_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_VALUE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_GLOBAL_TRANSACTION_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_DATABASE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DATABASE_IDX_DB_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TABLE_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_JOB_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_JOB_IDX_JOB_POWNER_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_DEPENDENCY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_DEPENDENCY_IDX_DEPENDENCY_REF_OBJ_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TABLEGROUP_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLEGROUP_IDX_TG_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_HISTORY_IDX_TENANT_DELETED_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_SUB_PART_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_SUB_PART_IDX_SUB_PART_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_ROUTINE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_ROUTINE_IDX_DB_ROUTINE_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_ROUTINE_IDX_ROUTINE_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_ROUTINE_IDX_ROUTINE_PKG_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_DIRECTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_DIRECTORY_IDX_DIRECTORY_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_DDL_ERROR_MESSAGE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DDL_ERROR_MESSAGE_IDX_DDL_ERROR_OBJECT_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_TRIGGER_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_TRIGGER_HISTORY_IDX_TRIGGER_HIS_BASE_OBJ_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_CONSTRAINT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_CONSTRAINT_IDX_CST_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_COLL_TYPE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_COLUMN_STAT_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_COLUMN_STAT_HISTORY_IDX_COLUMN_STAT_HIS_SAVTIME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_OBJECT_TYPE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_DB_OBJ_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_OBJ_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_FOREIGN_KEY_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_CHILD_TID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_PARENT_TID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_RLS_GROUP_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_RLS_GROUP_HISTORY_IDX_RLS_GROUP_HIS_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_SYNONYM_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_SYNONYM_IDX_DB_SYNONYM_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_SYNONYM_IDX_SYNONYM_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TENANT_SECURITY_AUDIT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_SECURITY_AUDIT_IDX_AUDIT_TYPE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_ROOTSERVICE_JOB_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_ROOTSERVICE_JOB_IDX_RS_JOB_TYPE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_PENDING_TRANSACTION_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_PENDING_TRANSACTION_IDX_PENDING_TX_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_KV_TTL_TASK_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_KV_TTL_TASK_HISTORY_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_FOREIGN_KEY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_FOREIGN_KEY_IDX_FK_CHILD_TID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_FOREIGN_KEY_IDX_FK_PARENT_TID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_FOREIGN_KEY_IDX_FK_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}

#endif


#ifdef SYS_INDEX_DATA_TABLE_ID_TO_INDEX_SCHEMAS_SWITCH

case OB_ALL_TABLET_TO_LS_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tablet_to_ls_idx_tablet_to_ls_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tablet_to_ls_idx_tablet_to_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_RLS_GROUP_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rls_group_idx_rls_group_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_DEF_SUB_PART_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_def_sub_part_idx_def_sub_part_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_RLS_CONTEXT_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rls_context_history_idx_rls_context_his_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_RLS_POLICY_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rls_policy_history_idx_rls_policy_his_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_OLS_USER_LEVEL_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_ols_user_level_idx_ols_level_uid_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_ols_user_level_idx_ols_level_policy_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_RECYCLEBIN_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_recyclebin_idx_recyclebin_db_type_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_recyclebin_idx_recyclebin_ori_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_KEYSTORE_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_keystore_history_idx_keystore_his_master_key_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_role_grantee_map_history_idx_grantee_his_role_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TABLE_STAT_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_table_stat_history_idx_table_stat_his_savtime_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_CONTEXT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_context_idx_ctx_namespace_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TABLE_PRIVILEGE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_table_privilege_idx_tb_priv_db_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_table_privilege_idx_tb_priv_tb_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_DDL_CHECKSUM_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_ddl_checksum_idx_ddl_checksum_task_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_RLS_POLICY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rls_policy_idx_rls_policy_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rls_policy_idx_rls_policy_group_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_OBJAUTH_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_objauth_idx_objauth_grantor_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_objauth_idx_objauth_grantee_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_PACKAGE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_package_idx_db_pkg_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_package_idx_pkg_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_ROOTSERVICE_EVENT_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rootservice_event_history_idx_rs_module_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rootservice_event_history_idx_rs_event_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_OLS_COMPONENT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_ols_component_idx_ols_com_policy_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_TRIGGER_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_trigger_idx_trigger_base_obj_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_trigger_idx_db_trigger_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_trigger_idx_trigger_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TYPE_ATTR_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_type_attr_idx_type_attr_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TYPE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_type_idx_db_type_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_type_idx_type_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_DBLINK_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_dblink_idx_owner_dblink_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_dblink_idx_dblink_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_ROUTINE_PARAM_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_routine_param_idx_routine_param_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_DATABASE_PRIVILEGE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_database_privilege_idx_db_priv_db_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_OLS_LABEL_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_ols_label_idx_ols_lab_policy_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_ols_label_idx_ols_lab_tag_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_ols_label_idx_ols_lab_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_KEYSTORE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_keystore_idx_keystore_master_key_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_COLUMN_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_column_idx_tb_column_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_column_idx_column_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_KV_TTL_TASK_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_kv_ttl_task_idx_kv_ttl_task_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_RLS_CONTEXT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rls_context_idx_rls_context_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_BACKUP_SET_FILES_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_backup_set_files_idx_status_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_SEQUENCE_OBJECT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_sequence_object_idx_seq_obj_db_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_sequence_object_idx_seq_obj_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_DDL_OPERATION_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_ddl_operation_idx_ddl_type_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TABLE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_table_idx_data_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_table_idx_db_tb_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_table_idx_tb_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_ACQUIRED_SNAPSHOT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_acquired_snapshot_idx_snapshot_tablet_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_PROFILE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_profile_idx_profile_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_USER_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_user_idx_ur_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_OLS_POLICY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_ols_policy_idx_ols_policy_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_ols_policy_idx_ols_policy_col_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_SERVER_EVENT_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_server_event_history_idx_server_module_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_server_event_history_idx_server_event_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_DDL_TASK_STATUS_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_ddl_task_status_idx_task_key_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_PART_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_part_idx_part_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_HISTOGRAM_STAT_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_histogram_stat_history_idx_histogram_stat_his_savtime_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_DBMS_LOCK_ALLOCATED_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_dbms_lock_allocated_idx_dbms_lock_allocated_lockhandle_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_dbms_lock_allocated_idx_dbms_lock_allocated_expiration_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_PLAN_BASELINE_ITEM_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_plan_baseline_item_idx_spm_item_sql_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_plan_baseline_item_idx_spm_item_value_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_GLOBAL_TRANSACTION_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_global_transaction_idx_xa_trans_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_DATABASE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_database_idx_db_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TABLE_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_table_history_idx_data_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_log_archive_piece_files_idx_status_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_JOB_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_job_idx_job_powner_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_DEPENDENCY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_dependency_idx_dependency_ref_obj_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TABLEGROUP_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tablegroup_idx_tg_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_history_idx_tenant_deleted_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_SUB_PART_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_sub_part_idx_sub_part_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_ROUTINE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_routine_idx_db_routine_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_routine_idx_routine_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_routine_idx_routine_pkg_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_DIRECTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_directory_idx_directory_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_DDL_ERROR_MESSAGE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_ddl_error_message_idx_ddl_error_object_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_TRIGGER_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_trigger_history_idx_trigger_his_base_obj_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_CONSTRAINT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_constraint_idx_cst_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_COLL_TYPE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_coll_type_idx_coll_name_type_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_COLUMN_STAT_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_column_stat_history_idx_column_stat_his_savtime_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_role_grantee_map_idx_grantee_role_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_OBJECT_TYPE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_object_type_idx_obj_type_db_obj_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_object_type_idx_obj_type_obj_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_FOREIGN_KEY_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_foreign_key_history_idx_fk_his_child_tid_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_foreign_key_history_idx_fk_his_parent_tid_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_RLS_GROUP_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rls_group_history_idx_rls_group_his_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_SYNONYM_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_synonym_idx_db_synonym_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_synonym_idx_synonym_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TENANT_SECURITY_AUDIT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_security_audit_idx_audit_type_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_ROOTSERVICE_JOB_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rootservice_job_idx_rs_job_type_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_PENDING_TRANSACTION_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_pending_transaction_idx_pending_tx_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_KV_TTL_TASK_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_kv_ttl_task_history_idx_kv_ttl_task_history_upd_time_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_FOREIGN_KEY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_foreign_key_idx_fk_child_tid_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_foreign_key_idx_fk_parent_tid_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_foreign_key_idx_fk_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}

#endif


#ifdef ADD_SYS_INDEX_ID

  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLE_IDX_DATA_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLE_IDX_DB_TB_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLE_IDX_TB_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_COLUMN_IDX_TB_COLUMN_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_COLUMN_IDX_COLUMN_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DDL_OPERATION_IDX_DDL_TYPE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_USER_IDX_UR_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DATABASE_IDX_DB_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLEGROUP_IDX_TG_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_HISTORY_IDX_TENANT_DELETED_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_MODULE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_EVENT_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_DB_TYPE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PART_IDX_PART_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_SUB_PART_IDX_SUB_PART_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DEF_SUB_PART_IDX_DEF_SUB_PART_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_MODULE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_EVENT_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_ROOTSERVICE_JOB_IDX_RS_JOB_TYPE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_FOREIGN_KEY_IDX_FK_CHILD_TID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_FOREIGN_KEY_IDX_FK_PARENT_TID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_FOREIGN_KEY_IDX_FK_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_CHILD_TID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_PARENT_TID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_SYNONYM_IDX_DB_SYNONYM_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_SYNONYM_IDX_SYNONYM_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DDL_CHECKSUM_IDX_DDL_CHECKSUM_TASK_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_ROUTINE_IDX_DB_ROUTINE_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_ROUTINE_IDX_ROUTINE_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_ROUTINE_IDX_ROUTINE_PKG_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_ROUTINE_PARAM_IDX_ROUTINE_PARAM_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PACKAGE_IDX_DB_PKG_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PACKAGE_IDX_PKG_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_ACQUIRED_SNAPSHOT_IDX_SNAPSHOT_TABLET_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_CONSTRAINT_IDX_CST_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TYPE_IDX_DB_TYPE_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TYPE_IDX_TYPE_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TYPE_ATTR_IDX_TYPE_ATTR_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DBLINK_IDX_OWNER_DBLINK_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DBLINK_IDX_DBLINK_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_IDX_GRANTEE_HIS_ROLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_KEYSTORE_IDX_KEYSTORE_MASTER_KEY_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_KEYSTORE_HISTORY_IDX_KEYSTORE_HIS_MASTER_KEY_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_COL_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OLS_COMPONENT_IDX_OLS_COM_POLICY_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_POLICY_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TAG_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_UID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_POLICY_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_SECURITY_AUDIT_IDX_AUDIT_TYPE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_BASE_OBJ_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_TRIGGER_IDX_DB_TRIGGER_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_TRIGGER_HISTORY_IDX_TRIGGER_HIS_BASE_OBJ_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTOR_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTEE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_DB_OBJ_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_OBJ_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_DEPENDENCY_IDX_DEPENDENCY_REF_OBJ_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DDL_ERROR_MESSAGE_IDX_DDL_ERROR_OBJECT_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_COLUMN_STAT_HISTORY_IDX_COLUMN_STAT_HIS_SAVTIME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_HISTOGRAM_STAT_HISTORY_IDX_HISTOGRAM_STAT_HIS_SAVTIME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PENDING_TRANSACTION_IDX_PENDING_TX_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_SQL_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_VALUE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_DIRECTORY_IDX_DIRECTORY_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_JOB_IDX_JOB_POWNER_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_DB_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_ORI_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_DB_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_TB_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DATABASE_PRIVILEGE_IDX_DB_PRIV_DB_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_RLS_POLICY_IDX_RLS_POLICY_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_RLS_POLICY_IDX_RLS_POLICY_GROUP_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_RLS_POLICY_HISTORY_IDX_RLS_POLICY_HIS_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_RLS_GROUP_HISTORY_IDX_RLS_GROUP_HIS_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_RLS_CONTEXT_HISTORY_IDX_RLS_CONTEXT_HIS_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_KV_TTL_TASK_IDX_KV_TTL_TASK_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_KV_TTL_TASK_HISTORY_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));

#endif
