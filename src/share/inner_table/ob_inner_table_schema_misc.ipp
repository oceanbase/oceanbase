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
case OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_ROUTINE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_SERVER_AGENT_TID:
case OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TYPE_SYS_AGENT_TID:
case OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID:
case OB_TENANT_VIRTUAL_CHARSET_AGENT_TID:
case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TID:
case OB_TENANT_VIRTUAL_OUTLINE_AGENT_TID:
case OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TID:

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
case OB_ALL_VIRTUAL_COLUMN_CHECKSUM_ERROR_INFO_TID:
case OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_TID:
case OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_TID:
case OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TID:
case OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_TID:
case OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_TID:
case OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_TID:
case OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_TID:
case OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_TID:
case OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_TID:
case OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_TID:
case OB_ALL_VIRTUAL_LS_META_TABLE_TID:
case OB_ALL_VIRTUAL_LS_RECOVERY_STAT_TID:
case OB_ALL_VIRTUAL_LS_REPLICA_TASK_TID:
case OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_TID:
case OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_TID:
case OB_ALL_VIRTUAL_LS_STATUS_TID:
case OB_ALL_VIRTUAL_MERGE_INFO_TID:
case OB_ALL_VIRTUAL_RESTORE_JOB_TID:
case OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_TID:
case OB_ALL_VIRTUAL_RESTORE_PROGRESS_TID:
case OB_ALL_VIRTUAL_TABLET_META_TABLE_TID:
case OB_ALL_VIRTUAL_TABLET_REPLICA_CHECKSUM_TID:
case OB_ALL_VIRTUAL_TENANT_INFO_TID:
case OB_ALL_VIRTUAL_TENANT_USER_FAILED_LOGIN_STAT_TID:
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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
case OB_ALL_VIRTUAL_DAM_CLEANUP_JOBS_TID:
case OB_ALL_VIRTUAL_DAM_LAST_ARCH_TS_TID:
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
case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TID:
case OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_TID:
case OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TID:
case OB_ALL_VIRTUAL_TENANT_TABLESPACE_TID:
case OB_ALL_VIRTUAL_TENANT_TABLESPACE_HISTORY_TID:
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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
case OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_TID:
case OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_META_TID:
case OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_PIECE_TID:
case OB_ALL_DEADLOCK_EVENT_HISTORY_TID:
case OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_META_TID:
case OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_GLOBAL_CONTEXT_VALUE_TID:
case OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_META_TID:
case OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_PIECE_TID:
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
case OB_ALL_MERGE_INFO_TID:
case OB_ALL_MERGE_INFO_AUX_LOB_META_TID:
case OB_ALL_MERGE_INFO_AUX_LOB_PIECE_TID:
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
case OB_ALL_TABLET_META_TABLE_TID:
case OB_ALL_TABLET_META_TABLE_AUX_LOB_META_TID:
case OB_ALL_TABLET_META_TABLE_AUX_LOB_PIECE_TID:
case OB_ALL_TABLET_REPLICA_CHECKSUM_TID:
case OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_META_TID:
case OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_PIECE_TID:
case OB_ALL_TENANT_GLOBAL_TRANSACTION_TID:
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

#endif


#ifdef SYS_INDEX_TABLE_ID_SWITCH

case OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID:
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID:
case OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID:
case OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TID:

#endif


#ifdef SYS_INDEX_DATA_TABLE_ID_SWITCH

case OB_ALL_TABLE_HISTORY_TID:
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID:
case OB_ALL_BACKUP_SET_FILES_TID:
case OB_ALL_DDL_TASK_STATUS_TID:

#endif


#ifdef SYS_INDEX_DATA_TABLE_ID_TO_INDEX_IDS_SWITCH

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
case OB_ALL_DDL_TASK_STATUS_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TID))) {
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

#endif


#ifdef SYS_INDEX_DATA_TABLE_ID_TO_INDEX_SCHEMAS_SWITCH

case OB_ALL_TABLE_HISTORY_TID: {
  if (FAILEDx(ObInnerTableSchema::all_table_history_idx_data_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (!is_sys_tenant(tenant_id) && OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(tenant_id, index_schema))) {
    LOG_WARN("fail to construct full table", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(tables.push_back(index_schema))) {
    LOG_WARN("fail to push back index", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID: {
  if (FAILEDx(ObInnerTableSchema::all_log_archive_piece_files_idx_status_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (!is_sys_tenant(tenant_id) && OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(tenant_id, index_schema))) {
    LOG_WARN("fail to construct full table", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(tables.push_back(index_schema))) {
    LOG_WARN("fail to push back index", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_DDL_TASK_STATUS_TID: {
  if (FAILEDx(ObInnerTableSchema::all_ddl_task_status_idx_task_key_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (!is_sys_tenant(tenant_id) && OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(tenant_id, index_schema))) {
    LOG_WARN("fail to construct full table", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(tables.push_back(index_schema))) {
    LOG_WARN("fail to push back index", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_BACKUP_SET_FILES_TID: {
  if (FAILEDx(ObInnerTableSchema::all_backup_set_files_idx_status_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (!is_sys_tenant(tenant_id) && OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(tenant_id, index_schema))) {
    LOG_WARN("fail to construct full table", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(tables.push_back(index_schema))) {
    LOG_WARN("fail to push back index", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}

#endif


#ifdef ADD_SYS_INDEX_ID

  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));

#endif
