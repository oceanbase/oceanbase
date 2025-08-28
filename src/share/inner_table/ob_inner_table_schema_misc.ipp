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
case OB_ALL_VIRTUAL_PKG_COLL_TYPE_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_PKG_TYPE_ATTR_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_PKG_TYPE_SYS_AGENT_TID:
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
case OB_ALL_VIRTUAL_ZONE_STORAGE_SYS_AGENT_TID:
case OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID:
case OB_TENANT_VIRTUAL_CHARSET_AGENT_TID:
case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TID:
case OB_TENANT_VIRTUAL_OUTLINE_AGENT_TID:
case OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TID:
case OB_ALL_VIRTUAL_RESOURCE_POOL_MYSQL_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_TENANT_MYSQL_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_VIRTUAL_LONG_OPS_STATUS_MYSQL_SYS_AGENT_TID:
case OB_ALL_VIRTUAL_ZONE_STORAGE_MYSQL_SYS_AGENT_TID:

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

    case OB_ALL_VIRTUAL_PKG_COLL_TYPE_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_PKG_COLL_TYPE_TID;
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

    case OB_ALL_VIRTUAL_PKG_TYPE_ATTR_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_PKG_TYPE_ATTR_TID;
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

    case OB_ALL_VIRTUAL_PKG_TYPE_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_PKG_TYPE_TID;
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

    case OB_ALL_VIRTUAL_ZONE_STORAGE_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_ZONE_STORAGE_TID;
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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

    case OB_ALL_VIRTUAL_ZONE_STORAGE_MYSQL_SYS_AGENT_TID: {
      ObAgentVirtualTable *agent_iter = NULL;
      const uint64_t base_tid = OB_ALL_ZONE_STORAGE_TID;
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
case OB_ALL_VIRTUAL_CLONE_JOB_TID:
case OB_ALL_VIRTUAL_CLONE_JOB_HISTORY_TID:
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
case OB_ALL_VIRTUAL_LS_REPLICA_TASK_HISTORY_TID:
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
case OB_ALL_VIRTUAL_SERVICE_TID:
case OB_ALL_VIRTUAL_SPM_EVO_RESULT_TID:
case OB_ALL_VIRTUAL_STORAGE_IO_USAGE_TID:
case OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_TID:
case OB_ALL_VIRTUAL_TABLET_CHECKSUM_ERROR_INFO_TID:
case OB_ALL_VIRTUAL_TABLET_META_TABLE_TID:
case OB_ALL_VIRTUAL_TABLET_REPLICA_CHECKSUM_TID:
case OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_INFO_TID:
case OB_ALL_VIRTUAL_TENANT_PARAMETER_TID:
case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_TID:
case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_JOB_TID:
case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_TID:
case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_TID:
case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TID:
case OB_ALL_VIRTUAL_TENANT_USER_FAILED_LOGIN_STAT_TID:
case OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_TID:
case OB_ALL_VIRTUAL_WR_CONTROL_TID:
case OB_ALL_VIRTUAL_WR_EVENT_NAME_TID:
case OB_ALL_VIRTUAL_WR_RES_MGR_SYSSTAT_TID:
case OB_ALL_VIRTUAL_WR_SNAPSHOT_TID:
case OB_ALL_VIRTUAL_WR_SQL_PLAN_TID:
case OB_ALL_VIRTUAL_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_TID:
case OB_ALL_VIRTUAL_WR_SQLSTAT_TID:
case OB_ALL_VIRTUAL_WR_SQLTEXT_TID:
case OB_ALL_VIRTUAL_WR_STATNAME_TID:
case OB_ALL_VIRTUAL_WR_SYSSTAT_TID:
case OB_ALL_VIRTUAL_WR_SYSTEM_EVENT_TID:
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

    case OB_ALL_VIRTUAL_CLONE_JOB_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CLONE_JOB_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CLONE_JOB_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CLONE_JOB_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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

    case OB_ALL_VIRTUAL_LS_REPLICA_TASK_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_LS_REPLICA_TASK_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_SERVICE_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SERVICE_TID, meta_record_in_sys, index_schema, params))) {
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
    case OB_ALL_VIRTUAL_SPM_EVO_RESULT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SPM_EVO_RESULT_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_STORAGE_IO_USAGE_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_STORAGE_IO_USAGE_TID, meta_record_in_sys, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_TABLET_CHECKSUM_ERROR_INFO_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLET_CHECKSUM_ERROR_INFO_TID, meta_record_in_sys, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SNAPSHOT_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_JOB_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SNAPSHOT_JOB_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SNAPSHOT_LS_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TID, meta_record_in_sys, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_WR_EVENT_NAME_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_EVENT_NAME_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_WR_RES_MGR_SYSSTAT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_RES_MGR_SYSSTAT_TID, meta_record_in_sys, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_WR_SQL_PLAN_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_SQL_PLAN_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_WR_SQLSTAT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_SQLSTAT_TID, meta_record_in_sys, index_schema, params))) {
        SERVER_LOG(WARN, "iterate private virtual table iter init failed", KR(ret));
        iter->~ObIteratePrivateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_WR_SQLTEXT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_SQLTEXT_TID, meta_record_in_sys, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_WR_SYSTEM_EVENT_TID: {
      ObIteratePrivateVirtualTable *iter = NULL;
      const bool meta_record_in_sys = false;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIteratePrivateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create iterate private virtual table iterator failed", KR(ret));
      } else if (OB_FAIL(iter->init(OB_WR_SYSTEM_EVENT_TID, meta_record_in_sys, index_schema, params))) {
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

case OB_ALL_VIRTUAL_AUDIT_LOG_FILTER_TID:
case OB_ALL_VIRTUAL_AUDIT_LOG_USER_TID:
case OB_ALL_VIRTUAL_AUTO_INCREMENT_TID:
case OB_ALL_VIRTUAL_AUX_STAT_TID:
case OB_ALL_VIRTUAL_BALANCE_JOB_TID:
case OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_TID:
case OB_ALL_VIRTUAL_BALANCE_TASK_TID:
case OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_TID:
case OB_ALL_VIRTUAL_CATALOG_TID:
case OB_ALL_VIRTUAL_CATALOG_HISTORY_TID:
case OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_TID:
case OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_HISTORY_TID:
case OB_ALL_VIRTUAL_CCL_RULE_TID:
case OB_ALL_VIRTUAL_CCL_RULE_HISTORY_TID:
case OB_ALL_VIRTUAL_CLIENT_TO_SERVER_SESSION_INFO_TID:
case OB_ALL_VIRTUAL_COLL_TYPE_TID:
case OB_ALL_VIRTUAL_COLL_TYPE_HISTORY_TID:
case OB_ALL_VIRTUAL_COLUMN_TID:
case OB_ALL_VIRTUAL_COLUMN_GROUP_TID:
case OB_ALL_VIRTUAL_COLUMN_GROUP_HISTORY_TID:
case OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_TID:
case OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_HISTORY_TID:
case OB_ALL_VIRTUAL_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_COLUMN_PRIVILEGE_TID:
case OB_ALL_VIRTUAL_COLUMN_PRIVILEGE_HISTORY_TID:
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
case OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_TID:
case OB_ALL_VIRTUAL_DDL_CHECKSUM_TID:
case OB_ALL_VIRTUAL_DDL_ERROR_MESSAGE_TID:
case OB_ALL_VIRTUAL_DDL_OPERATION_TID:
case OB_ALL_VIRTUAL_DDL_TASK_STATUS_TID:
case OB_ALL_VIRTUAL_DEF_SUB_PART_TID:
case OB_ALL_VIRTUAL_DEF_SUB_PART_HISTORY_TID:
case OB_ALL_VIRTUAL_DEPENDENCY_TID:
case OB_ALL_VIRTUAL_DETECT_LOCK_INFO_TID:
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
case OB_ALL_VIRTUAL_INDEX_USAGE_INFO_TID:
case OB_ALL_VIRTUAL_JOB_TID:
case OB_ALL_VIRTUAL_JOB_LOG_TID:
case OB_ALL_VIRTUAL_KV_REDIS_TABLE_TID:
case OB_ALL_VIRTUAL_LS_TID:
case OB_ALL_VIRTUAL_MLOG_TID:
case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_TID:
case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_COLUMN_TID:
case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_HISTORY_TID:
case OB_ALL_VIRTUAL_MONITOR_MODIFIED_TID:
case OB_ALL_VIRTUAL_MVIEW_TID:
case OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_TID:
case OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_TID:
case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_TID:
case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_TID:
case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TID:
case OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_TID:
case OB_ALL_VIRTUAL_NCOMP_DLL_V2_TID:
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
case OB_ALL_VIRTUAL_PKG_COLL_TYPE_TID:
case OB_ALL_VIRTUAL_PKG_TYPE_TID:
case OB_ALL_VIRTUAL_PKG_TYPE_ATTR_TID:
case OB_ALL_VIRTUAL_PL_RECOMPILE_OBJINFO_TID:
case OB_ALL_VIRTUAL_PLAN_BASELINE_TID:
case OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_TID:
case OB_ALL_VIRTUAL_RECYCLEBIN_TID:
case OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_TID:
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
case OB_ALL_VIRTUAL_ROUTINE_PRIVILEGE_TID:
case OB_ALL_VIRTUAL_ROUTINE_PRIVILEGE_HISTORY_TID:
case OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_TID:
case OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_TID:
case OB_ALL_VIRTUAL_SENSITIVE_COLUMN_TID:
case OB_ALL_VIRTUAL_SENSITIVE_COLUMN_HISTORY_TID:
case OB_ALL_VIRTUAL_SENSITIVE_RULE_TID:
case OB_ALL_VIRTUAL_SENSITIVE_RULE_HISTORY_TID:
case OB_ALL_VIRTUAL_SENSITIVE_RULE_PRIVILEGE_TID:
case OB_ALL_VIRTUAL_SENSITIVE_RULE_PRIVILEGE_HISTORY_TID:
case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_TID:
case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_HISTORY_TID:
case OB_ALL_VIRTUAL_SEQUENCE_VALUE_TID:
case OB_ALL_VIRTUAL_SPATIAL_REFERENCE_SYSTEMS_TID:
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
case OB_ALL_VIRTUAL_TABLET_REORGANIZE_HISTORY_TID:
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
case OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_TID:
case OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_HISTORY_TID:
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
case OB_ALL_VIRTUAL_USER_PROXY_INFO_TID:
case OB_ALL_VIRTUAL_USER_PROXY_INFO_HISTORY_TID:
case OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_TID:
case OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_HISTORY_TID:
case OB_ALL_VIRTUAL_VECTOR_INDEX_TASK_TID:
case OB_ALL_VIRTUAL_VECTOR_INDEX_TASK_HISTORY_TID:

#endif


#ifdef ITERATE_VIRTUAL_TABLE_CREATE_ITER

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
    case OB_ALL_VIRTUAL_AUDIT_LOG_FILTER_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_AUDIT_LOG_FILTER_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_AUDIT_LOG_USER_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_AUDIT_LOG_USER_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

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

    case OB_ALL_VIRTUAL_AUX_STAT_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_AUX_STAT_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_CATALOG_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CATALOG_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CATALOG_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CATALOG_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CATALOG_PRIVILEGE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CATALOG_PRIVILEGE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CCL_RULE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CCL_RULE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CCL_RULE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CCL_RULE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_CLIENT_TO_SERVER_SESSION_INFO_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_COLUMN_GROUP_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_GROUP_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_COLUMN_GROUP_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_GROUP_HISTORY_TID, index_schema, params))) {
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
    case OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_GROUP_MAPPING_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_COLUMN_PRIVILEGE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_PRIVILEGE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_COLUMN_PRIVILEGE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_COLUMN_PRIVILEGE_HISTORY_TID, index_schema, params))) {
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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

    case OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DBMS_LOCK_ALLOCATED_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_DETECT_LOCK_INFO_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_DETECT_LOCK_INFO_TID, index_schema, params))) {
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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

    case OB_ALL_VIRTUAL_INDEX_USAGE_INFO_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_INDEX_USAGE_INFO_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_KV_REDIS_TABLE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_KV_REDIS_TABLE_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_MLOG_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MLOG_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_MVIEW_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MVIEW_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MVIEW_REFRESH_RUN_STATS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MVIEW_REFRESH_STATS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MVIEW_REFRESH_STATS_PARAMS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_MVIEW_REFRESH_STMT_STATS_TID, index_schema, params))) {
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
    case OB_ALL_VIRTUAL_NCOMP_DLL_V2_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_NCOMP_DLL_V2_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_PKG_COLL_TYPE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PKG_COLL_TYPE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PKG_TYPE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PKG_TYPE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PKG_TYPE_ATTR_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PKG_TYPE_ATTR_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_PL_RECOMPILE_OBJINFO_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_PL_RECOMPILE_OBJINFO_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_RES_MGR_DIRECTIVE_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_ROUTINE_PRIVILEGE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_ROUTINE_PRIVILEGE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_ROUTINE_PRIVILEGE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_ROUTINE_PRIVILEGE_HISTORY_TID, index_schema, params))) {
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
    case OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_SENSITIVE_COLUMN_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SENSITIVE_COLUMN_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SENSITIVE_COLUMN_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SENSITIVE_COLUMN_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SENSITIVE_RULE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SENSITIVE_RULE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SENSITIVE_RULE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SENSITIVE_RULE_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SENSITIVE_RULE_PRIVILEGE_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SENSITIVE_RULE_PRIVILEGE_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_SENSITIVE_RULE_PRIVILEGE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SENSITIVE_RULE_PRIVILEGE_HISTORY_TID, index_schema, params))) {
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

    case OB_ALL_VIRTUAL_SPATIAL_REFERENCE_SYSTEMS_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TID, index_schema, params))) {
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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

    case OB_ALL_VIRTUAL_TABLET_REORGANIZE_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TABLET_REORGANIZE_HISTORY_TID, index_schema, params))) {
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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
  END_CREATE_VT_ITER_SWITCH_LAMBDA

  BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
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

    case OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TRANSFER_PARTITION_TASK_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

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

    case OB_ALL_VIRTUAL_USER_PROXY_INFO_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_USER_PROXY_INFO_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_USER_PROXY_INFO_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_USER_PROXY_INFO_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_USER_PROXY_ROLE_INFO_TID, index_schema, params))) {
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
    case OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_VECTOR_INDEX_TASK_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_VECTOR_INDEX_TASK_TID, index_schema, params))) {
        SERVER_LOG(WARN, "virtual table iter init failed", K(ret));
        iter->~ObIterateVirtualTable();
        allocator.free(iter);
        iter = NULL;
      } else {
       vt_iter = iter;
      }
      break;
    }

    case OB_ALL_VIRTUAL_VECTOR_INDEX_TASK_HISTORY_TID: {
      ObIterateVirtualTable *iter = NULL;
      if (OB_FAIL(NEW_VIRTUAL_TABLE(ObIterateVirtualTable, iter))) {
        SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
      } else if (OB_FAIL(iter->init(OB_ALL_VECTOR_INDEX_TASK_HISTORY_TID, index_schema, params))) {
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
case OB_ALL_CLONE_JOB_TID:
case OB_ALL_CLONE_JOB_AUX_LOB_META_TID:
case OB_ALL_CLONE_JOB_AUX_LOB_PIECE_TID:
case OB_ALL_CLONE_JOB_HISTORY_TID:
case OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_META_TID:
case OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_PIECE_TID:
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
case OB_ALL_LS_REPLICA_TASK_HISTORY_TID:
case OB_ALL_LS_REPLICA_TASK_HISTORY_AUX_LOB_META_TID:
case OB_ALL_LS_REPLICA_TASK_HISTORY_AUX_LOB_PIECE_TID:
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
case OB_ALL_SERVICE_TID:
case OB_ALL_SERVICE_AUX_LOB_META_TID:
case OB_ALL_SERVICE_AUX_LOB_PIECE_TID:
case OB_ALL_SERVICE_EPOCH_TID:
case OB_ALL_SERVICE_EPOCH_AUX_LOB_META_TID:
case OB_ALL_SERVICE_EPOCH_AUX_LOB_PIECE_TID:
case OB_ALL_SPM_EVO_RESULT_TID:
case OB_ALL_SPM_EVO_RESULT_AUX_LOB_META_TID:
case OB_ALL_SPM_EVO_RESULT_AUX_LOB_PIECE_TID:
case OB_ALL_STORAGE_IO_USAGE_TID:
case OB_ALL_STORAGE_IO_USAGE_AUX_LOB_META_TID:
case OB_ALL_STORAGE_IO_USAGE_AUX_LOB_PIECE_TID:
case OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TID:
case OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TID:
case OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TID:
case OB_ALL_TABLET_CHECKSUM_ERROR_INFO_TID:
case OB_ALL_TABLET_CHECKSUM_ERROR_INFO_AUX_LOB_META_TID:
case OB_ALL_TABLET_CHECKSUM_ERROR_INFO_AUX_LOB_PIECE_TID:
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
case OB_ALL_TENANT_SNAPSHOT_TID:
case OB_ALL_TENANT_SNAPSHOT_IDX_TENANT_SNAPSHOT_NAME_TID:
case OB_ALL_TENANT_SNAPSHOT_AUX_LOB_META_TID:
case OB_ALL_TENANT_SNAPSHOT_AUX_LOB_PIECE_TID:
case OB_ALL_TENANT_SNAPSHOT_JOB_TID:
case OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_META_TID:
case OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_PIECE_TID:
case OB_ALL_TENANT_SNAPSHOT_LS_TID:
case OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_META_TID:
case OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_PIECE_TID:
case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TID:
case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_META_TID:
case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_PIECE_TID:
case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TID:
case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_META_TID:
case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_PIECE_TID:
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
case OB_WR_EVENT_NAME_TID:
case OB_WR_EVENT_NAME_AUX_LOB_META_TID:
case OB_WR_EVENT_NAME_AUX_LOB_PIECE_TID:
case OB_WR_RES_MGR_SYSSTAT_TID:
case OB_WR_RES_MGR_SYSSTAT_AUX_LOB_META_TID:
case OB_WR_RES_MGR_SYSSTAT_AUX_LOB_PIECE_TID:
case OB_WR_SNAPSHOT_TID:
case OB_WR_SNAPSHOT_AUX_LOB_META_TID:
case OB_WR_SNAPSHOT_AUX_LOB_PIECE_TID:
case OB_WR_SQL_PLAN_TID:
case OB_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_TID:
case OB_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_AUX_LOB_META_TID:
case OB_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_AUX_LOB_PIECE_TID:
case OB_WR_SQL_PLAN_AUX_LOB_META_TID:
case OB_WR_SQL_PLAN_AUX_LOB_PIECE_TID:
case OB_WR_SQLSTAT_TID:
case OB_WR_SQLSTAT_AUX_LOB_META_TID:
case OB_WR_SQLSTAT_AUX_LOB_PIECE_TID:
case OB_WR_SQLTEXT_TID:
case OB_WR_SQLTEXT_AUX_LOB_META_TID:
case OB_WR_SQLTEXT_AUX_LOB_PIECE_TID:
case OB_WR_STATNAME_TID:
case OB_WR_STATNAME_AUX_LOB_META_TID:
case OB_WR_STATNAME_AUX_LOB_PIECE_TID:
case OB_WR_SYSSTAT_TID:
case OB_WR_SYSSTAT_AUX_LOB_META_TID:
case OB_WR_SYSSTAT_AUX_LOB_PIECE_TID:
case OB_WR_SYSTEM_EVENT_TID:
case OB_WR_SYSTEM_EVENT_AUX_LOB_META_TID:
case OB_WR_SYSTEM_EVENT_AUX_LOB_PIECE_TID:

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
case OB_ALL_TENANT_SNAPSHOT_IDX_TENANT_SNAPSHOT_NAME_TID:
case OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_TID:
case OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_TID:
case OB_ALL_TABLET_REORGANIZE_HISTORY_IDX_TABLET_HIS_TABLE_ID_SRC_TID:
case OB_ALL_KV_TTL_TASK_IDX_KV_TTL_TASK_TABLE_ID_TID:
case OB_ALL_KV_TTL_TASK_HISTORY_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_TID:
case OB_ALL_MVIEW_REFRESH_RUN_STATS_IDX_MVIEW_REFRESH_RUN_STATS_NUM_MVS_CURRENT_TID:
case OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_END_TIME_TID:
case OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_MVIEW_END_TIME_TID:
case OB_ALL_TRANSFER_PARTITION_TASK_IDX_TRANSFER_PARTITION_KEY_TID:
case OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_IDX_CLIENT_TO_SERVER_SESSION_INFO_CLIENT_SESSION_ID_TID:
case OB_ALL_COLUMN_PRIVILEGE_IDX_COLUMN_PRIVILEGE_NAME_TID:
case OB_ALL_USER_PROXY_INFO_IDX_USER_PROXY_INFO_PROXY_USER_ID_TID:
case OB_ALL_USER_PROXY_INFO_HISTORY_IDX_USER_PROXY_INFO_PROXY_USER_ID_HISTORY_TID:
case OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_IDX_USER_PROXY_ROLE_INFO_PROXY_USER_ID_HISTORY_TID:
case OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_TID:
case OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_TID:
case OB_ALL_PKG_TYPE_IDX_PKG_DB_TYPE_NAME_TID:
case OB_ALL_PKG_TYPE_IDX_PKG_TYPE_NAME_TID:
case OB_ALL_PKG_TYPE_ATTR_IDX_PKG_TYPE_ATTR_NAME_TID:
case OB_ALL_PKG_TYPE_ATTR_IDX_PKG_TYPE_ATTR_ID_TID:
case OB_ALL_PKG_COLL_TYPE_IDX_PKG_COLL_NAME_TYPE_TID:
case OB_ALL_PKG_COLL_TYPE_IDX_PKG_COLL_NAME_ID_TID:
case OB_ALL_CATALOG_IDX_CATALOG_NAME_TID:
case OB_ALL_CATALOG_PRIVILEGE_IDX_CATALOG_PRIV_CATALOG_NAME_TID:
case OB_ALL_CCL_RULE_IDX_CCL_RULE_ID_TID:
case OB_ALL_TABLET_REORGANIZE_HISTORY_IDX_TABLET_HIS_TABLE_ID_DEST_TID:

#endif


#ifdef SYS_INDEX_DATA_TABLE_ID_SWITCH

case OB_ALL_RLS_GROUP_TID:
case OB_ALL_TENANT_OLS_USER_LEVEL_TID:
case OB_ALL_RECYCLEBIN_TID:
case OB_ALL_CONTEXT_TID:
case OB_ALL_RLS_POLICY_TID:
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID:
case OB_ALL_RLS_POLICY_HISTORY_TID:
case OB_ALL_ROUTINE_PARAM_TID:
case OB_ALL_DATABASE_PRIVILEGE_TID:
case OB_ALL_RLS_CONTEXT_TID:
case OB_ALL_SEQUENCE_OBJECT_TID:
case OB_ALL_ACQUIRED_SNAPSHOT_TID:
case OB_ALL_TENANT_OLS_POLICY_TID:
case OB_ALL_MVIEW_REFRESH_STATS_TID:
case OB_ALL_CATALOG_TID:
case OB_ALL_DATABASE_TID:
case OB_ALL_TRANSFER_PARTITION_TASK_TID:
case OB_ALL_SUB_PART_TID:
case OB_ALL_TENANT_SNAPSHOT_TID:
case OB_ALL_TENANT_OBJECT_TYPE_TID:
case OB_ALL_CATALOG_PRIVILEGE_TID:
case OB_ALL_USER_PROXY_INFO_TID:
case OB_ALL_FOREIGN_KEY_TID:
case OB_ALL_KV_TTL_TASK_HISTORY_TID:
case OB_ALL_DEF_SUB_PART_TID:
case OB_ALL_DDL_CHECKSUM_TID:
case OB_ALL_TENANT_OBJAUTH_TID:
case OB_ALL_PACKAGE_TID:
case OB_ALL_TYPE_TID:
case OB_ALL_DBLINK_TID:
case OB_ALL_MVIEW_REFRESH_RUN_STATS_TID:
case OB_ALL_SERVER_EVENT_HISTORY_TID:
case OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TID:
case OB_ALL_DBMS_LOCK_ALLOCATED_TID:
case OB_ALL_PLAN_BASELINE_ITEM_TID:
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID:
case OB_ALL_TENANT_DEPENDENCY_TID:
case OB_ALL_CCL_RULE_TID:
case OB_ALL_ROUTINE_TID:
case OB_ALL_CONSTRAINT_TID:
case OB_ALL_SYNONYM_TID:
case OB_ALL_PKG_TYPE_TID:
case OB_ALL_TABLET_TO_LS_TID:
case OB_ALL_TENANT_KEYSTORE_HISTORY_TID:
case OB_ALL_TENANT_HISTORY_TID:
case OB_ALL_TABLE_PRIVILEGE_TID:
case OB_ALL_FOREIGN_KEY_HISTORY_TID:
case OB_ALL_USER_PROXY_INFO_HISTORY_TID:
case OB_ALL_ROOTSERVICE_EVENT_HISTORY_TID:
case OB_ALL_TENANT_TRIGGER_TID:
case OB_ALL_TENANT_PROFILE_TID:
case OB_ALL_PKG_COLL_TYPE_TID:
case OB_ALL_TABLET_REORGANIZE_HISTORY_TID:
case OB_ALL_KV_TTL_TASK_TID:
case OB_ALL_DDL_OPERATION_TID:
case OB_ALL_TABLE_TID:
case OB_ALL_USER_TID:
case OB_ALL_DDL_TASK_STATUS_TID:
case OB_ALL_PART_TID:
case OB_ALL_TENANT_GLOBAL_TRANSACTION_TID:
case OB_ALL_TENANT_DIRECTORY_TID:
case OB_ALL_JOB_TID:
case OB_ALL_COLUMN_STAT_HISTORY_TID:
case OB_ALL_TABLEGROUP_TID:
case OB_ALL_TYPE_ATTR_TID:
case OB_ALL_COLL_TYPE_TID:
case OB_ALL_RLS_CONTEXT_HISTORY_TID:
case OB_ALL_TABLE_STAT_HISTORY_TID:
case OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TID:
case OB_ALL_TENANT_OLS_COMPONENT_TID:
case OB_ALL_PKG_TYPE_ATTR_TID:
case OB_ALL_TENANT_OLS_LABEL_TID:
case OB_ALL_TENANT_KEYSTORE_TID:
case OB_ALL_COLUMN_TID:
case OB_ALL_BACKUP_SET_FILES_TID:
case OB_ALL_HISTOGRAM_STAT_HISTORY_TID:
case OB_ALL_TABLE_HISTORY_TID:
case OB_ALL_COLUMN_PRIVILEGE_TID:
case OB_ALL_PENDING_TRANSACTION_TID:
case OB_ALL_DDL_ERROR_MESSAGE_TID:
case OB_ALL_TENANT_TRIGGER_HISTORY_TID:
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID:
case OB_ALL_RLS_GROUP_HISTORY_TID:
case OB_ALL_TENANT_SECURITY_AUDIT_TID:
case OB_ALL_ROOTSERVICE_JOB_TID:
case OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TID:

#endif


#ifdef SYS_INDEX_DATA_TABLE_ID_TO_INDEX_IDS_SWITCH

case OB_ALL_RLS_GROUP_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TID))) {
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
case OB_ALL_CONTEXT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TID))) {
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
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_IDX_GRANTEE_HIS_ROLE_ID_TID))) {
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
case OB_ALL_RLS_CONTEXT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TID))) {
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
case OB_ALL_ACQUIRED_SNAPSHOT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_ACQUIRED_SNAPSHOT_IDX_SNAPSHOT_TABLET_TID))) {
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
case OB_ALL_MVIEW_REFRESH_STATS_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_END_TIME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_MVIEW_END_TIME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_CATALOG_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_CATALOG_IDX_CATALOG_NAME_TID))) {
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
case OB_ALL_TRANSFER_PARTITION_TASK_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TRANSFER_PARTITION_TASK_IDX_TRANSFER_PARTITION_KEY_TID))) {
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
case OB_ALL_TENANT_SNAPSHOT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_SNAPSHOT_IDX_TENANT_SNAPSHOT_NAME_TID))) {
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
case OB_ALL_CATALOG_PRIVILEGE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_CATALOG_PRIVILEGE_IDX_CATALOG_PRIV_CATALOG_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_USER_PROXY_INFO_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_USER_PROXY_INFO_IDX_USER_PROXY_INFO_PROXY_USER_ID_TID))) {
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
case OB_ALL_KV_TTL_TASK_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_KV_TTL_TASK_HISTORY_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_TID))) {
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
case OB_ALL_DDL_CHECKSUM_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_DDL_CHECKSUM_IDX_DDL_CHECKSUM_TASK_TID))) {
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
case OB_ALL_MVIEW_REFRESH_RUN_STATS_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_MVIEW_REFRESH_RUN_STATS_IDX_MVIEW_REFRESH_RUN_STATS_NUM_MVS_CURRENT_TID))) {
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
case OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_TID))) {
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
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID))) {
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
case OB_ALL_CCL_RULE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_CCL_RULE_IDX_CCL_RULE_ID_TID))) {
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
case OB_ALL_CONSTRAINT_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_CONSTRAINT_IDX_CST_NAME_TID))) {
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
case OB_ALL_PKG_TYPE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_PKG_TYPE_IDX_PKG_DB_TYPE_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_PKG_TYPE_IDX_PKG_TYPE_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TABLET_TO_LS_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TID))) {
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
case OB_ALL_TENANT_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_HISTORY_IDX_TENANT_DELETED_TID))) {
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
case OB_ALL_FOREIGN_KEY_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_CHILD_TID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_PARENT_TID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_USER_PROXY_INFO_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_USER_PROXY_INFO_HISTORY_IDX_USER_PROXY_INFO_PROXY_USER_ID_HISTORY_TID))) {
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
case OB_ALL_TENANT_PROFILE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_PKG_COLL_TYPE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_PKG_COLL_TYPE_IDX_PKG_COLL_NAME_TYPE_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_PKG_COLL_TYPE_IDX_PKG_COLL_NAME_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_TABLET_REORGANIZE_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLET_REORGANIZE_HISTORY_IDX_TABLET_HIS_TABLE_ID_SRC_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLET_REORGANIZE_HISTORY_IDX_TABLET_HIS_TABLE_ID_DEST_TID))) {
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
case OB_ALL_USER_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_USER_IDX_UR_NAME_TID))) {
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
case OB_ALL_TENANT_GLOBAL_TRANSACTION_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TID))) {
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
case OB_ALL_JOB_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_JOB_IDX_JOB_POWNER_TID))) {
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
case OB_ALL_TABLEGROUP_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLEGROUP_IDX_TG_NAME_TID))) {
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
case OB_ALL_COLL_TYPE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TID))) {
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
case OB_ALL_TABLE_STAT_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_IDX_CLIENT_TO_SERVER_SESSION_INFO_CLIENT_SESSION_ID_TID))) {
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
case OB_ALL_PKG_TYPE_ATTR_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_PKG_TYPE_ATTR_IDX_PKG_TYPE_ATTR_NAME_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  if (FAILEDx(index_tids.push_back(OB_ALL_PKG_TYPE_ATTR_IDX_PKG_TYPE_ATTR_ID_TID))) {
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
case OB_ALL_BACKUP_SET_FILES_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID))) {
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
case OB_ALL_TABLE_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}
case OB_ALL_COLUMN_PRIVILEGE_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_COLUMN_PRIVILEGE_IDX_COLUMN_PRIVILEGE_NAME_TID))) {
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
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TID))) {
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
case OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TID: {
  if (FAILEDx(index_tids.push_back(OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_IDX_USER_PROXY_ROLE_INFO_PROXY_USER_ID_HISTORY_TID))) {
    LOG_WARN("fail to push back index tid", KR(ret));
  }
  break;
}

#endif


#ifdef SYS_INDEX_DATA_TABLE_ID_TO_INDEX_SCHEMAS_SWITCH

case OB_ALL_RLS_GROUP_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rls_group_idx_rls_group_table_id_schema(index_schema))) {
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
case OB_ALL_CONTEXT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_context_idx_ctx_namespace_schema(index_schema))) {
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
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_role_grantee_map_history_idx_grantee_his_role_id_schema(index_schema))) {
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
case OB_ALL_RLS_CONTEXT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_rls_context_idx_rls_context_table_id_schema(index_schema))) {
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
case OB_ALL_ACQUIRED_SNAPSHOT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_acquired_snapshot_idx_snapshot_tablet_schema(index_schema))) {
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
case OB_ALL_MVIEW_REFRESH_STATS_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_mview_refresh_stats_idx_mview_refresh_stats_end_time_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_mview_refresh_stats_idx_mview_refresh_stats_mview_end_time_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_CATALOG_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_catalog_idx_catalog_name_schema(index_schema))) {
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
case OB_ALL_TRANSFER_PARTITION_TASK_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_transfer_partition_task_idx_transfer_partition_key_schema(index_schema))) {
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
case OB_ALL_TENANT_SNAPSHOT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_snapshot_idx_tenant_snapshot_name_schema(index_schema))) {
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
case OB_ALL_CATALOG_PRIVILEGE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_catalog_privilege_idx_catalog_priv_catalog_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_USER_PROXY_INFO_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_user_proxy_info_idx_user_proxy_info_proxy_user_id_schema(index_schema))) {
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
case OB_ALL_KV_TTL_TASK_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_kv_ttl_task_history_idx_kv_ttl_task_history_upd_time_schema(index_schema))) {
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
case OB_ALL_DDL_CHECKSUM_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_ddl_checksum_idx_ddl_checksum_task_schema(index_schema))) {
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
case OB_ALL_MVIEW_REFRESH_RUN_STATS_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_mview_refresh_run_stats_idx_mview_refresh_run_stats_num_mvs_current_schema(index_schema))) {
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
case OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_scheduler_job_run_detail_v2_idx_scheduler_job_run_detail_v2_time_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_scheduler_job_run_detail_v2_idx_scheduler_job_run_detail_v2_job_class_time_schema(index_schema))) {
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
case OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_log_archive_piece_files_idx_status_schema(index_schema))) {
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
case OB_ALL_CCL_RULE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_ccl_rule_idx_ccl_rule_id_schema(index_schema))) {
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
case OB_ALL_CONSTRAINT_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_constraint_idx_cst_name_schema(index_schema))) {
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
case OB_ALL_PKG_TYPE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_pkg_type_idx_pkg_db_type_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_pkg_type_idx_pkg_type_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
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
case OB_ALL_TENANT_KEYSTORE_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_keystore_history_idx_keystore_his_master_key_id_schema(index_schema))) {
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
case OB_ALL_USER_PROXY_INFO_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_user_proxy_info_history_idx_user_proxy_info_proxy_user_id_history_schema(index_schema))) {
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
case OB_ALL_TENANT_PROFILE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_profile_idx_profile_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_PKG_COLL_TYPE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_pkg_coll_type_idx_pkg_coll_name_type_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_pkg_coll_type_idx_pkg_coll_name_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_TABLET_REORGANIZE_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tablet_reorganize_history_idx_tablet_his_table_id_src_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tablet_reorganize_history_idx_tablet_his_table_id_dest_schema(index_schema))) {
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
case OB_ALL_USER_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_user_idx_ur_name_schema(index_schema))) {
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
case OB_ALL_TENANT_GLOBAL_TRANSACTION_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_global_transaction_idx_xa_trans_id_schema(index_schema))) {
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
case OB_ALL_JOB_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_job_idx_job_powner_schema(index_schema))) {
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
case OB_ALL_TABLEGROUP_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tablegroup_idx_tg_name_schema(index_schema))) {
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
case OB_ALL_COLL_TYPE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_coll_type_idx_coll_name_type_schema(index_schema))) {
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
case OB_ALL_TABLE_STAT_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_table_stat_history_idx_table_stat_his_savtime_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_client_to_server_session_info_idx_client_to_server_session_info_client_session_id_schema(index_schema))) {
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
case OB_ALL_PKG_TYPE_ATTR_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_pkg_type_attr_idx_pkg_type_attr_name_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_pkg_type_attr_idx_pkg_type_attr_id_schema(index_schema))) {
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
case OB_ALL_BACKUP_SET_FILES_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_backup_set_files_idx_status_schema(index_schema))) {
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
case OB_ALL_TABLE_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_table_history_idx_data_table_id_schema(index_schema))) {
    LOG_WARN("fail to create index schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(append_table_(tenant_id, index_schema, tables))) {
    LOG_WARN("fail to append", KR(ret), K(tenant_id), K(data_table_id));
  }
  break;
}
case OB_ALL_COLUMN_PRIVILEGE_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_column_privilege_idx_column_privilege_name_schema(index_schema))) {
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
case OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_tenant_role_grantee_map_idx_grantee_role_id_schema(index_schema))) {
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
case OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TID: {
  index_schema.reset();
  if (FAILEDx(ObInnerTableSchema::all_user_proxy_role_info_history_idx_user_proxy_role_info_proxy_user_id_history_schema(index_schema))) {
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
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TENANT_SNAPSHOT_IDX_TENANT_SNAPSHOT_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLET_REORGANIZE_HISTORY_IDX_TABLET_HIS_TABLE_ID_SRC_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_KV_TTL_TASK_IDX_KV_TTL_TASK_TABLE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_KV_TTL_TASK_HISTORY_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_MVIEW_REFRESH_RUN_STATS_IDX_MVIEW_REFRESH_RUN_STATS_NUM_MVS_CURRENT_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_END_TIME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_MVIEW_END_TIME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TRANSFER_PARTITION_TASK_IDX_TRANSFER_PARTITION_KEY_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_IDX_CLIENT_TO_SERVER_SESSION_INFO_CLIENT_SESSION_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_COLUMN_PRIVILEGE_IDX_COLUMN_PRIVILEGE_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_USER_PROXY_INFO_IDX_USER_PROXY_INFO_PROXY_USER_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_USER_PROXY_INFO_HISTORY_IDX_USER_PROXY_INFO_PROXY_USER_ID_HISTORY_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_IDX_USER_PROXY_ROLE_INFO_PROXY_USER_ID_HISTORY_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PKG_TYPE_IDX_PKG_DB_TYPE_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PKG_TYPE_IDX_PKG_TYPE_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PKG_TYPE_ATTR_IDX_PKG_TYPE_ATTR_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PKG_TYPE_ATTR_IDX_PKG_TYPE_ATTR_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PKG_COLL_TYPE_IDX_PKG_COLL_NAME_TYPE_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_PKG_COLL_TYPE_IDX_PKG_COLL_NAME_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_CATALOG_IDX_CATALOG_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_CATALOG_PRIVILEGE_IDX_CATALOG_PRIV_CATALOG_NAME_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_CCL_RULE_IDX_CCL_RULE_ID_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_ids.push_back(OB_ALL_TABLET_REORGANIZE_HISTORY_IDX_TABLET_HIS_TABLE_ID_DEST_TID))) {
    LOG_WARN("add index id failed", KR(ret), K(tenant_id));

#endif

#ifdef INNER_TABLE_HARD_CODE_SCHEMA_MAPPING_SWITCH
    case OB_ALL_CORE_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CORE_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_DDL_OPERATION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_OPERATION_SCHEMA_VERSION); break;
    case OB_ALL_USER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_SCHEMA_VERSION); break;
    case OB_ALL_USER_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TABLEGROUP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLEGROUP_SCHEMA_VERSION); break;
    case OB_ALL_TABLEGROUP_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLEGROUP_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_SCHEMA_VERSION); break;
    case OB_ALL_SERVER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVER_SCHEMA_VERSION); break;
    case OB_ALL_SYS_PARAMETER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_PARAMETER_SCHEMA_VERSION); break;
    case OB_TENANT_PARAMETER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_PARAMETER_SCHEMA_VERSION); break;
    case OB_ALL_SYS_VARIABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_VARIABLE_SCHEMA_VERSION); break;
    case OB_ALL_SYS_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_STAT_SCHEMA_VERSION); break;
    case OB_ALL_UNIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_UNIT_SCHEMA_VERSION); break;
    case OB_ALL_UNIT_CONFIG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_UNIT_CONFIG_SCHEMA_VERSION); break;
    case OB_ALL_RESOURCE_POOL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESOURCE_POOL_SCHEMA_VERSION); break;
    case OB_ALL_CHARSET_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CHARSET_SCHEMA_VERSION); break;
    case OB_ALL_COLLATION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLLATION_SCHEMA_VERSION); break;
    case OB_HELP_TOPIC_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_TOPIC_SCHEMA_VERSION); break;
    case OB_HELP_CATEGORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_CATEGORY_SCHEMA_VERSION); break;
    case OB_HELP_KEYWORD_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_KEYWORD_SCHEMA_VERSION); break;
    case OB_HELP_RELATION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_RELATION_SCHEMA_VERSION); break;
    case OB_ALL_DUMMY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DUMMY_SCHEMA_VERSION); break;
    case OB_ALL_ROOTSERVICE_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROOTSERVICE_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_OUTLINE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OUTLINE_SCHEMA_VERSION); break;
    case OB_ALL_OUTLINE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OUTLINE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_RECYCLEBIN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RECYCLEBIN_SCHEMA_VERSION); break;
    case OB_ALL_PART_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_SCHEMA_VERSION); break;
    case OB_ALL_PART_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SUB_PART_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SUB_PART_SCHEMA_VERSION); break;
    case OB_ALL_SUB_PART_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SUB_PART_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_PART_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_INFO_SCHEMA_VERSION); break;
    case OB_ALL_PART_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_DEF_SUB_PART_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEF_SUB_PART_SCHEMA_VERSION); break;
    case OB_ALL_DEF_SUB_PART_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEF_SUB_PART_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SERVER_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVER_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_ROOTSERVICE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROOTSERVICE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_SYS_VARIABLE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_VARIABLE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_DDL_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_ID_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SYNONYM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYNONYM_SCHEMA_VERSION); break;
    case OB_ALL_SYNONYM_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYNONYM_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_AUTO_INCREMENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUTO_INCREMENT_SCHEMA_VERSION); break;
    case OB_ALL_DDL_CHECKSUM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_CHECKSUM_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PARAM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PARAM_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PARAM_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PARAM_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_PACKAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PACKAGE_SCHEMA_VERSION); break;
    case OB_ALL_PACKAGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PACKAGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_ACQUIRED_SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ACQUIRED_SNAPSHOT_SCHEMA_VERSION); break;
    case OB_ALL_CONSTRAINT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONSTRAINT_SCHEMA_VERSION); break;
    case OB_ALL_CONSTRAINT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONSTRAINT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_ORI_SCHEMA_VERSION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ORI_SCHEMA_VERSION_SCHEMA_VERSION); break;
    case OB_ALL_FUNC_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FUNC_SCHEMA_VERSION); break;
    case OB_ALL_FUNC_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FUNC_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TEMP_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TEMP_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_OBJECT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEQUENCE_OBJECT_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_OBJECT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEQUENCE_OBJECT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_VALUE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEQUENCE_VALUE_SCHEMA_VERSION); break;
    case OB_ALL_FREEZE_SCHEMA_VERSION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FREEZE_SCHEMA_VERSION_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_ATTR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_ATTR_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_ATTR_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_ATTR_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_COLL_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLL_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_COLL_TYPE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLL_TYPE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_WEAK_READ_SERVICE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_WEAK_READ_SERVICE_SCHEMA_VERSION); break;
    case OB_ALL_DBLINK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DBLINK_SCHEMA_VERSION); break;
    case OB_ALL_DBLINK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DBLINK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_ROLE_GRANTEE_MAP_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_KEYSTORE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_KEYSTORE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_KEYSTORE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_KEYSTORE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_POLICY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_POLICY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_POLICY_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_POLICY_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_COMPONENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_COMPONENT_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_COMPONENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_COMPONENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_LABEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_LABEL_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_LABEL_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_LABEL_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_USER_LEVEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_USER_LEVEL_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TABLESPACE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TABLESPACE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TABLESPACE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TABLESPACE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_PROFILE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_PROFILE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_PROFILE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_PROFILE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SECURITY_AUDIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SECURITY_AUDIT_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TRIGGER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TRIGGER_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TRIGGER_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TRIGGER_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SEED_PARAMETER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEED_PARAMETER_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SECURITY_AUDIT_RECORD_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SYSAUTH_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SYSAUTH_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SYSAUTH_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SYSAUTH_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJAUTH_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJAUTH_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJAUTH_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJAUTH_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ERROR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_ERROR_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_PROGRESS_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJECT_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJECT_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJECT_TYPE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_TRANSITION_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_CONSTRAINT_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_CONSTRAINT_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_GLOBAL_TRANSACTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_GLOBAL_TRANSACTION_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DEPENDENCY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_DEPENDENCY_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_PLAN_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_DIRECTIVE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_DIRECTIVE_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_MAPPING_RULE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_MAPPING_RULE_SCHEMA_VERSION); break;
    case OB_ALL_DDL_ERROR_MESSAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_ERROR_MESSAGE_SCHEMA_VERSION); break;
    case OB_ALL_SPACE_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPACE_USAGE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_SET_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_SET_FILES_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_CONSUMER_GROUP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_CONSUMER_GROUP_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_INFO_SCHEMA_VERSION); break;
    case OB_ALL_DDL_TASK_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_TASK_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_REGION_NETWORK_BANDWIDTH_LIMIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_REGION_NETWORK_BANDWIDTH_LIMIT_SCHEMA_VERSION); break;
    case OB_ALL_DEADLOCK_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEADLOCK_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_USAGE_SCHEMA_VERSION); break;
    case OB_ALL_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_JOB_SCHEMA_VERSION); break;
    case OB_ALL_JOB_LOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_JOB_LOG_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DIRECTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_DIRECTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DIRECTORY_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_DIRECTORY_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_STAT_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_STAT_SCHEMA_VERSION); break;
    case OB_ALL_HISTOGRAM_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_HISTOGRAM_STAT_SCHEMA_VERSION); break;
    case OB_ALL_MONITOR_MODIFIED_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MONITOR_MODIFIED_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_STAT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_STAT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_STAT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_STAT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_HISTOGRAM_STAT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_HISTOGRAM_STAT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_OPTSTAT_GLOBAL_PREFS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OPTSTAT_GLOBAL_PREFS_SCHEMA_VERSION); break;
    case OB_ALL_OPTSTAT_USER_PREFS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OPTSTAT_USER_PREFS_SCHEMA_VERSION); break;
    case OB_ALL_LS_META_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_META_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_TO_LS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_TO_LS_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_META_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_META_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_LS_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_PROGRESS_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_PIECE_FILES_SCHEMA_VERSION); break;
    case OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_LOG_ARCHIVE_PROGRESS_SCHEMA_VERSION); break;
    case OB_ALL_LS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_STORAGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_STORAGE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_DAM_LAST_ARCH_TS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DAM_LAST_ARCH_TS_SCHEMA_VERSION); break;
    case OB_ALL_DAM_CLEANUP_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DAM_CLEANUP_JOBS_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_JOB_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_TASK_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_INFO_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_SKIPPED_TABLET_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_SKIPPED_TABLET_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_INFO_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_TO_TABLE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_TO_TABLE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_LS_RECOVERY_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_RECOVERY_STAT_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_REPLICA_CHECKSUM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_REPLICA_CHECKSUM_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_CHECKSUM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_CHECKSUM_SCHEMA_VERSION); break;
    case OB_ALL_LS_REPLICA_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_REPLICA_TASK_SCHEMA_VERSION); break;
    case OB_ALL_PENDING_TRANSACTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PENDING_TRANSACTION_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_GROUP_LS_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_GROUP_LS_STAT_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_JOB_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_PROGRAM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_PROGRAM_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_SCHEMA_VERSION); break;
    case OB_ALL_CONTEXT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONTEXT_SCHEMA_VERSION); break;
    case OB_ALL_CONTEXT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONTEXT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_GLOBAL_CONTEXT_VALUE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_GLOBAL_CONTEXT_VALUE_SCHEMA_VERSION); break;
    case OB_ALL_LS_ELECTION_REFERENCE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_ELECTION_REFERENCE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_TASK_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_LS_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_LS_TASK_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_MERGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_MERGE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_MERGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MERGE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_FREEZE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FREEZE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_DISK_IO_CALIBRATION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DISK_IO_CALIBRATION_SCHEMA_VERSION); break;
    case OB_ALL_PLAN_BASELINE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PLAN_BASELINE_SCHEMA_VERSION); break;
    case OB_ALL_PLAN_BASELINE_ITEM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PLAN_BASELINE_ITEM_SCHEMA_VERSION); break;
    case OB_ALL_SPM_CONFIG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPM_CONFIG_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_PARAMETER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_PARAMETER_SCHEMA_VERSION); break;
    case OB_ALL_LS_RESTORE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_RESTORE_PROGRESS_SCHEMA_VERSION); break;
    case OB_ALL_LS_RESTORE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_RESTORE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_STORAGE_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_STORAGE_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_POLICY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_POLICY_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_LOG_RESTORE_SOURCE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_RESTORE_SOURCE_SCHEMA_VERSION); break;
    case OB_ALL_KV_TTL_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_KV_TTL_TASK_SCHEMA_VERSION); break;
    case OB_ALL_KV_TTL_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_KV_TTL_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SERVICE_EPOCH_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVICE_EPOCH_SCHEMA_VERSION); break;
    case OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPATIAL_REFERENCE_SYSTEMS_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_MAPPING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_MAPPING_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_TASK_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_TASK_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_ARBITRATION_SERVICE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ARBITRATION_SERVICE_SCHEMA_VERSION); break;
    case OB_ALL_LS_ARB_REPLICA_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_ARB_REPLICA_TASK_SCHEMA_VERSION); break;
    case OB_ALL_DATA_DICTIONARY_IN_LOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATA_DICTIONARY_IN_LOG_SCHEMA_VERSION); break;
    case OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_RLS_POLICY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_POLICY_SCHEMA_VERSION); break;
    case OB_ALL_RLS_POLICY_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_POLICY_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_RLS_SECURITY_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_SECURITY_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_RLS_SECURITY_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_SECURITY_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_RLS_GROUP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_GROUP_SCHEMA_VERSION); break;
    case OB_ALL_RLS_GROUP_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_GROUP_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_RLS_CONTEXT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_CONTEXT_SCHEMA_VERSION); break;
    case OB_ALL_RLS_CONTEXT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_CONTEXT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_RLS_ATTRIBUTE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_ATTRIBUTE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_ATTRIBUTE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_ATTRIBUTE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_REWRITE_RULES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_REWRITE_RULES_SCHEMA_VERSION); break;
    case OB_ALL_RESERVED_SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESERVED_SNAPSHOT_SCHEMA_VERSION); break;
    case OB_ALL_CLUSTER_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLUSTER_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_SCHEMA_VERSION); break;
    case OB_ALL_EXTERNAL_TABLE_FILE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_EXTERNAL_TABLE_FILE_SCHEMA_VERSION); break;
    case OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_STORAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_STORAGE_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_STORAGE_OPERATION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_STORAGE_OPERATION_SCHEMA_VERSION); break;
    case OB_WR_ACTIVE_SESSION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_ACTIVE_SESSION_HISTORY_SCHEMA_VERSION); break;
    case OB_WR_SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SNAPSHOT_SCHEMA_VERSION); break;
    case OB_WR_STATNAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_STATNAME_SCHEMA_VERSION); break;
    case OB_WR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_TASK_HELPER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_TASK_HELPER_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_LS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_LS_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_SCHEMA_VERSION); break;
    case OB_ALL_MLOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MLOG_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_PARAMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STATS_PARAMS_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_RUN_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_RUN_STATS_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STATS_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_CHANGE_STATS_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STMT_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STMT_STATS_SCHEMA_VERSION); break;
    case OB_ALL_DBMS_LOCK_ALLOCATED_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DBMS_LOCK_ALLOCATED_SCHEMA_VERSION); break;
    case OB_WR_CONTROL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_CONTROL_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_JOB_CLASS_SCHEMA_VERSION); break;
    case OB_ALL_RECOVER_TABLE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RECOVER_TABLE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_RECOVER_TABLE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RECOVER_TABLE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_TASK_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_REORGANIZE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_REORGANIZE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_CLONE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLONE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_CLONE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLONE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_WR_SYSTEM_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SYSTEM_EVENT_SCHEMA_VERSION); break;
    case OB_WR_EVENT_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_EVENT_NAME_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_WR_SQLSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQLSTAT_SCHEMA_VERSION); break;
    case OB_ALL_NCOMP_DLL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_NCOMP_DLL_SCHEMA_VERSION); break;
    case OB_ALL_AUX_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUX_STAT_SCHEMA_VERSION); break;
    case OB_ALL_INDEX_USAGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_INDEX_USAGE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_DETECT_LOCK_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DETECT_LOCK_INFO_SCHEMA_VERSION); break;
    case OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_PARTITION_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_PARTITION_TASK_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_JOB_SCHEMA_VERSION); break;
    case OB_WR_SQLTEXT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQLTEXT_SCHEMA_VERSION); break;
    case OB_ALL_TRUSTED_ROOT_CERTIFICATE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRUSTED_ROOT_CERTIFICATE_SCHEMA_VERSION); break;
    case OB_ALL_AUDIT_LOG_FILTER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUDIT_LOG_FILTER_SCHEMA_VERSION); break;
    case OB_ALL_AUDIT_LOG_USER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUDIT_LOG_USER_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_LS_REPLICA_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_REPLICA_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_CHECKSUM_ERROR_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_CHECKSUM_ERROR_INFO_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_INFO_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_ROLE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_ROLE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SERVICE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVICE_SCHEMA_VERSION); break;
    case OB_ALL_STORAGE_IO_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_STORAGE_IO_USAGE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_DEP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_DEP_SCHEMA_VERSION); break;
    case OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_SCHEMA_VERSION); break;
    case OB_ALL_SPM_EVO_RESULT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPM_EVO_RESULT_SCHEMA_VERSION); break;
    case OB_ALL_DETECT_LOCK_INFO_V2_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DETECT_LOCK_INFO_V2_SCHEMA_VERSION); break;
    case OB_ALL_PKG_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PKG_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_PKG_TYPE_ATTR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PKG_TYPE_ATTR_SCHEMA_VERSION); break;
    case OB_ALL_PKG_COLL_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PKG_COLL_TYPE_SCHEMA_VERSION); break;
    case OB_WR_SQL_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQL_PLAN_SCHEMA_VERSION); break;
    case OB_WR_RES_MGR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_RES_MGR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_ALL_KV_REDIS_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_KV_REDIS_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_NCOMP_DLL_V2_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_NCOMP_DLL_V2_SCHEMA_VERSION); break;
    case OB_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_SCHEMA_VERSION); break;
    case OB_FT_DICT_IK_UTF8_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FT_DICT_IK_UTF8_SCHEMA_VERSION); break;
    case OB_FT_STOPWORD_IK_UTF8_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FT_STOPWORD_IK_UTF8_SCHEMA_VERSION); break;
    case OB_FT_QUANTIFIER_IK_UTF8_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FT_QUANTIFIER_IK_UTF8_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_LICENSE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LICENSE_SCHEMA_VERSION); break;
    case OB_ALL_PL_RECOMPILE_OBJINFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PL_RECOMPILE_OBJINFO_SCHEMA_VERSION); break;
    case OB_ALL_VECTOR_INDEX_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VECTOR_INDEX_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VECTOR_INDEX_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VECTOR_INDEX_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_CCL_RULE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CCL_RULE_SCHEMA_VERSION); break;
    case OB_ALL_CCL_RULE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CCL_RULE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_ALL_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_ALL_TABLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_TABLE_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_TABLE_COLUMN_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_TABLE_INDEX_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_TABLE_INDEX_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SESSION_VARIABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SESSION_VARIABLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROCESSLIST_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROCESSLIST_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_WARNING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_WARNING_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_CURRENT_TENANT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_CURRENT_TENANT_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_DATABASE_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_DATABASE_STATUS_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_TENANT_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_TENANT_STATUS_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_STATNAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_STATNAME_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_EVENT_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_EVENT_NAME_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_TABLES_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CORE_META_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CORE_META_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_CACHE_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLAN_CACHE_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_11003_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLAN_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MEM_LEAK_CHECKER_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MEM_LEAK_CHECKER_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LATCH_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LATCH_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_KVCACHE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_KVCACHE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATA_TYPE_CLASS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATA_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATA_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSION_EVENT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_EVENT_ALL_VIRTUAL_SESSION_EVENT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_11013_ALL_VIRTUAL_SESSION_EVENT_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_WAIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSION_WAIT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_WAIT_ALL_VIRTUAL_SESSION_WAIT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_11014_ALL_VIRTUAL_SESSION_WAIT_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_11015_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYSTEM_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYSTEM_EVENT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYSTEM_EVENT_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_11017_ALL_VIRTUAL_SYSTEM_EVENT_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CONCURRENCY_OBJECT_POOL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CONCURRENCY_OBJECT_POOL_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSTAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSTAT_ALL_VIRTUAL_SESSTAT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_11020_ALL_VIRTUAL_SESSTAT_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYSSTAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYSSTAT_ALL_VIRTUAL_SYSSTAT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_11021_ALL_VIRTUAL_SYSSTAT_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DISK_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DISK_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MEMSTORE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MEMSTORE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_UPGRADE_INSPECTION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANS_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANS_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANS_CTX_MGR_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANS_CTX_MGR_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANS_SCHEDULER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANS_SCHEDULER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_AUDIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_AUDIT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_AUDIT_ALL_VIRTUAL_SQL_AUDIT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_11031_ALL_VIRTUAL_SQL_AUDIT_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CORE_ALL_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MEMORY_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MEMORY_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRACE_SPAN_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRACE_SPAN_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ENGINE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ENGINE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_SERVER_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_SERVER_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_SYS_VARIABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_SYS_VARIABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_SCHEMA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_SCHEMA_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OBRPC_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OBRPC_STAT_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_OUTLINE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_OUTLINE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_PARTITION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_PARTITION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYS_TASK_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYS_TASK_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IO_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IO_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LONG_OPS_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LONG_OPS_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANS_LOCK_STAT_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVER_BLACKLIST_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVER_BLACKLIST_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DUMP_TENANT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DUMP_TENANT_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_AUDIT_OPERATION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_AUDIT_OPERATION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_AUDIT_ACTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_AUDIT_ACTION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DAG_WARNING_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DAG_WARNING_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_RESTORE_PREVIEW_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_RESTORE_PREVIEW_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MASTER_KEY_VERSION_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MASTER_KEY_VERSION_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DAG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DAG_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DAG_SCHEDULER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DAG_SCHEDULER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSION_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IO_CALIBRATION_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IO_CALIBRATION_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IO_BENCHMARK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IO_BENCHMARK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IO_QUOTA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IO_QUOTA_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVER_COMPACTION_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVER_COMPACTION_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DDL_SIM_POINT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DDL_SIM_POINT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DDL_SIM_POINT_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DDL_SIM_POINT_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RES_MGR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RES_MGR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_SESSION_VARIABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_SESSION_VARIABLES_SCHEMA_VERSION); break;
    case OB_GLOBAL_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GLOBAL_STATUS_SCHEMA_VERSION); break;
    case OB_SESSION_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_SESSION_STATUS_SCHEMA_VERSION); break;
    case OB_USER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SCHEMA_VERSION); break;
    case OB_DB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOCK_WAIT_STAT_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_COLLATION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_COLLATION_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_CHARSET_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_CHARSET_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_MGR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_MGR_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FREEZE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FREEZE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PX_WORKER_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PX_WORKER_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_AUTO_INCREMENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_AUTO_INCREMENT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SEQUENCE_VALUE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SEQUENCE_VALUE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_STORE_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_STORE_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DDL_OPERATION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DDL_OPERATION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OUTLINE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OUTLINE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OUTLINE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OUTLINE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYNONYM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYNONYM_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYNONYM_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYNONYM_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATABASE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATABASE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATABASE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATABASE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLEGROUP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLEGROUP_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLEGROUP_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLEGROUP_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PART_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PART_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PART_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PART_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PART_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PART_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PART_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PART_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DEF_SUB_PART_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DEF_SUB_PART_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DEF_SUB_PART_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DEF_SUB_PART_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SUB_PART_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SUB_PART_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SUB_PART_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SUB_PART_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CONSTRAINT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CONSTRAINT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CONSTRAINT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CONSTRAINT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FOREIGN_KEY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FOREIGN_KEY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FOREIGN_KEY_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FOREIGN_KEY_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TEMP_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TEMP_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ORI_SCHEMA_VERSION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ORI_SCHEMA_VERSION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYS_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYS_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_USER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_USER_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYS_VARIABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYS_VARIABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYS_VARIABLE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYS_VARIABLE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FUNC_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FUNC_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FUNC_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FUNC_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PACKAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PACKAGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PACKAGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PACKAGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ROUTINE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ROUTINE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_PARAM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ROUTINE_PARAM_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_PARAM_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ROUTINE_PARAM_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TYPE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_ATTR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TYPE_ATTR_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_ATTR_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TYPE_ATTR_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLL_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLL_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLL_TYPE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLL_TYPE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RECYCLEBIN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RECYCLEBIN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SEQUENCE_OBJECT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SEQUENCE_OBJECT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RAID_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RAID_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DTL_CHANNEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DTL_CHANNEL_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DTL_MEMORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DTL_MEMORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBLINK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DBLINK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBLINK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DBLINK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_KEYSTORE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_KEYSTORE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_KEYSTORE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_KEYSTORE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_POLICY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_POLICY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_POLICY_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_POLICY_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_LABEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_LABEL_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_LABEL_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_LABEL_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TABLESPACE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_TABLESPACE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TABLESPACE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_TABLESPACE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_INFORMATION_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_INFORMATION_COLUMNS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_USER_FAILED_LOGIN_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_USER_FAILED_LOGIN_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_PROFILE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_PROFILE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_PROFILE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_PROFILE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SECURITY_AUDIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SECURITY_AUDIT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRIGGER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRIGGER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRIGGER_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRIGGER_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PS_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PS_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PS_ITEM_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PS_ITEM_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYSAUTH_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYSAUTH_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYSAUTH_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYSAUTH_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OBJAUTH_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OBJAUTH_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OBJAUTH_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OBJAUTH_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ERROR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ERROR_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ID_SERVICE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ID_SERVICE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OBJECT_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OBJECT_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_12185_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OPEN_CURSOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OPEN_CURSOR_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TIME_ZONE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TIME_ZONE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TIME_ZONE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TIME_ZONE_NAME_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TIME_ZONE_TRANSITION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TIME_ZONE_TRANSITION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TIME_ZONE_TRANSITION_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TIME_ZONE_TRANSITION_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FILES_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DEPENDENCY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DEPENDENCY_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_OBJECT_DEFINITION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DDL_TASK_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DDL_TASK_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_USAGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_CTX_MEMORY_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_CTX_MEMORY_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_JOB_LOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_JOB_LOG_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_DIRECTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_DIRECTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_DIRECTORY_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_DIRECTORY_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_HISTOGRAM_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_HISTOGRAM_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PX_TARGET_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PX_TARGET_MONITOR_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MONITOR_MODIFIED_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MONITOR_MODIFIED_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OPTSTAT_GLOBAL_PREFS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OPTSTAT_GLOBAL_PREFS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OPTSTAT_USER_PREFS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OPTSTAT_USER_PREFS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBLINK_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DBLINK_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_META_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_META_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_META_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_META_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_TO_LS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_TO_LS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOAD_DATA_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOAD_DATA_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DAM_LAST_ARCH_TS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DAM_LAST_ARCH_TS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DAM_CLEANUP_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DAM_CLEANUP_JOBS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_LS_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_LS_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_LS_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_LS_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_LS_TASK_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_LS_TASK_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_SKIPPED_TABLET_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_SKIPPED_TABLET_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_SKIPPED_TABLET_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_SKIPPED_TABLET_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_SCHEDULE_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_SCHEDULE_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_TO_TABLE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_TO_TABLE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_RECOVERY_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_RECOVERY_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_LS_TASK_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_LS_TASK_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_REPLICA_CHECKSUM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_REPLICA_CHECKSUM_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DDL_CHECKSUM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DDL_CHECKSUM_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DDL_ERROR_MESSAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DDL_ERROR_MESSAGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_REPLICA_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_REPLICA_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PENDING_TRANSACTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PENDING_TRANSACTION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_CONTEXT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_CONTEXT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_CONTEXT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_CONTEXT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_UNIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_UNIT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ARCHIVE_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ARCHIVE_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_APPLY_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_APPLY_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_REPLAY_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_REPLAY_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_ROUTINE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_ROUTINE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OBJ_LOCK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OBJ_LOCK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ZONE_MERGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ZONE_MERGE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MERGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MERGE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TX_DATA_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TX_DATA_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANSACTION_FREEZE_CHECKPOINT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANSACTION_FREEZE_CHECKPOINT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANSACTION_CHECKPOINT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANSACTION_CHECKPOINT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CHECKPOINT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CHECKPOINT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_SET_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_SET_FILES_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_BASELINE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLAN_BASELINE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SPM_CONFIG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SPM_CONFIG_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ASH_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ASH_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ASH_ALL_VIRTUAL_ASH_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_12302_ALL_VIRTUAL_ASH_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DML_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DML_STATS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_PARAMETER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_PARAMETER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RESTORE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RESTORE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RESTORE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RESTORE_PROGRESS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_DDL_KV_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_DDL_KV_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_POINTER_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_POINTER_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_STORAGE_META_MEMORY_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_STORAGE_META_MEMORY_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_KVCACHE_STORE_MEMBLOCK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_KVCACHE_STORE_MEMBLOCK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_QUERY_RESPONSE_TIME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_QUERY_RESPONSE_TIME_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_KV_TTL_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_KV_TTL_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_KV_TTL_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_KV_TTL_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_CHECKSUM_ERROR_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_CHECKSUM_ERROR_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_COMPACTION_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_COMPACTION_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SCHEMA_MEMORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SCHEMA_MEMORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SCHEMA_SLOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SCHEMA_SLOT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MINOR_FREEZE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MINOR_FREEZE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SHOW_TRACE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SHOW_TRACE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_HA_DIAGNOSE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_HA_DIAGNOSE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANSFER_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANSFER_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANSFER_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANSFER_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BALANCE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BALANCE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BALANCE_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BALANCE_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_POLICY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_POLICY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_POLICY_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_POLICY_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_GROUP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_GROUP_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_GROUP_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_GROUP_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_CONTEXT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_CONTEXT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_CONTEXT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_CONTEXT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_ATTRIBUTE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_ATTRIBUTE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_ATTRIBUTE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_ATTRIBUTE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_MYSQL_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_MYSQL_SYS_AGENT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_PLAN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CORE_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CORE_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MALLOC_SAMPLE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MALLOC_SAMPLE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IO_SCHEDULER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IO_SCHEDULER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_EXTERNAL_TABLE_FILE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_EXTERNAL_TABLE_FILE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MDS_NODE_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MDS_NODE_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MDS_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MDS_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DUP_LS_LEASE_MGR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DUP_LS_LEASE_MGR_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DUP_LS_TABLET_SET_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DUP_LS_TABLET_SET_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DUP_LS_TABLETS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DUP_LS_TABLETS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TX_DATA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TX_DATA_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_THREAD_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_THREAD_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVER_STORAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVER_STORAGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SNAPSHOT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_STATNAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_STATNAME_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_KV_CONNECTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_KV_CONNECTION_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_VIRTUAL_LONG_OPS_STATUS_MYSQL_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_VIRTUAL_LONG_OPS_STATUS_MYSQL_SYS_AGENT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RESOURCE_POOL_MYSQL_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RESOURCE_POOL_MYSQL_SYS_AGENT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PX_P2P_DATAHUB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PX_P2P_DATAHUB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_GROUP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_GROUP_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_STORAGE_LEAK_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_STORAGE_LEAK_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_PARAMETER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_PARAMETER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SNAPSHOT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_BUFFER_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_BUFFER_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MLOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MLOG_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_CONTROL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_CONTROL_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BALANCE_TASK_HELPER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BALANCE_TASK_HELPER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BALANCE_GROUP_LS_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BALANCE_GROUP_LS_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CGROUP_CONFIG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CGROUP_CONFIG_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FLT_CONFIG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FLT_CONFIG_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_CLASS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_CLASS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATA_ACTIVITY_METRICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATA_ACTIVITY_METRICS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_GROUP_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_GROUP_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_STORAGE_HA_ERROR_DIAGNOSE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_STORAGE_HA_ERROR_DIAGNOSE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_STORAGE_HA_PERF_DIAGNOSE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_STORAGE_HA_PERF_DIAGNOSE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CLONE_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CLONE_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CLONE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CLONE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_MEMTABLE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_MEMTABLE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_CHECKPOINT_UNIT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_CHECKPOINT_UNIT_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SYSTEM_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SYSTEM_EVENT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_EVENT_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_EVENT_NAME_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_RUNNING_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SCHEDULER_RUNNING_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ROUTINE_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ROUTINE_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQLSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQLSTAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SQLSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SQLSTAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_AUX_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_AUX_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DETECT_LOCK_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DETECT_LOCK_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CLIENT_TO_SERVER_SESSION_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CLIENT_TO_SERVER_SESSION_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SNAPSHOT_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SQLTEXT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SQLTEXT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SHARED_STORAGE_COMPACTION_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SHARED_STORAGE_COMPACTION_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_SNAPSHOT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_INDEX_USAGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_INDEX_USAGE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_AUDIT_LOG_FILTER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_AUDIT_LOG_FILTER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_AUDIT_LOG_USER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_AUDIT_LOG_USER_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SHARED_STORAGE_QUOTA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SHARED_STORAGE_QUOTA_SCHEMA_VERSION); break;
    case OB_ENABLED_ROLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ENABLED_ROLES_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_REPLICA_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_REPLICA_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_PS_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSION_PS_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRACEPOINT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRACEPOINT_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_CHECKSUM_ERROR_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_CHECKSUM_ERROR_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COMPATIBILITY_CONTROL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COMPATIBILITY_CONTROL_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_PROXY_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_USER_PROXY_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_PROXY_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_USER_PROXY_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_REORGANIZE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_REORGANIZE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVICE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVICE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_GROUP_IO_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_GROUP_IO_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_STORAGE_IO_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_STORAGE_IO_USAGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ZONE_STORAGE_MYSQL_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ZONE_STORAGE_MYSQL_SYS_AGENT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_NIC_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_NIC_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SPATIAL_REFERENCE_SYSTEMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SPATIAL_REFERENCE_SYSTEMS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_TRANSPORT_DEST_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_TRANSPORT_DEST_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SS_LOCAL_CACHE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SS_LOCAL_CACHE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_KV_GROUP_COMMIT_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_KV_GROUP_COMMIT_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SPM_EVO_RESULT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SPM_EVO_RESULT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_VECTOR_INDEX_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_VECTOR_INDEX_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PKG_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_TYPE_ATTR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PKG_TYPE_ATTR_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_COLL_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PKG_COLL_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_KV_CLIENT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_KV_CLIENT_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SQL_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SQL_PLAN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_RES_MGR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_RES_MGR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_KV_REDIS_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_KV_REDIS_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FUNCTION_IO_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FUNCTION_IO_STAT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TEMP_FILE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TEMP_FILE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_NCOMP_DLL_V2_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_NCOMP_DLL_V2_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CS_REPLICA_TABLET_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CS_REPLICA_TABLET_STATS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_12514_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_I1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLUGIN_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLUGIN_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CATALOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CATALOG_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CATALOG_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CATALOG_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PL_RECOMPILE_OBJINFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PL_RECOMPILE_OBJINFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_VECTOR_INDEX_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_VECTOR_INDEX_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_VECTOR_INDEX_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_VECTOR_INDEX_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_CATALOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_CATALOG_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CATALOG_DATABASES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CATALOG_DATABASES_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_STORAGE_CACHE_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_STORAGE_CACHE_TASK_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_LOCAL_CACHE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_LOCAL_CACHE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CCL_RULE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CCL_RULE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CCL_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CCL_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_RUNNING_JOB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_RUNNING_JOB_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DYNAMIC_PARTITION_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DYNAMIC_PARTITION_TABLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CCL_RULE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CCL_RULE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_VECTOR_MEM_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_VECTOR_MEM_INFO_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SENSITIVE_RULE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SENSITIVE_RULE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SENSITIVE_RULE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SENSITIVE_RULE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SENSITIVE_COLUMN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SENSITIVE_COLUMN_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SENSITIVE_COLUMN_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SENSITIVE_COLUMN_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SENSITIVE_RULE_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SENSITIVE_RULE_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SENSITIVE_RULE_PRIVILEGE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SENSITIVE_RULE_PRIVILEGE_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBA_SOURCE_V1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DBA_SOURCE_V1_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_AUDIT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_AUDIT_ORA_ALL_VIRTUAL_SQL_AUDIT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15009_ALL_VIRTUAL_SQL_AUDIT_I1_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLAN_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_OUTLINE_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_OUTLINE_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PRIVILEGE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PRIVILEGE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_CHARSET_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_CHARSET_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_COLLATION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_COLLATION_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVER_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVER_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15034_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROCESSLIST_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROCESSLIST_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_WAIT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSION_WAIT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_WAIT_ORA_ALL_VIRTUAL_SESSION_WAIT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15036_ALL_VIRTUAL_SESSION_WAIT_I1_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15037_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MEMORY_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MEMORY_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MEMSTORE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MEMSTORE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSTAT_ORA_ALL_VIRTUAL_SESSTAT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15042_ALL_VIRTUAL_SESSTAT_I1_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYSSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYSSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYSSTAT_ORA_ALL_VIRTUAL_SYSSTAT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15043_ALL_VIRTUAL_SYSSTAT_I1_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYSTEM_EVENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15044_ALL_VIRTUAL_SYSTEM_EVENT_I1_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SESSION_VARIABLE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SESSION_VARIABLE_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_TABLE_COLUMN_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRACE_SPAN_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRACE_SPAN_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATA_TYPE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATA_TYPE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_AUDIT_OPERATION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_AUDIT_OPERATION_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_AUDIT_ACTION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_AUDIT_ACTION_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PX_WORKER_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PX_WORKER_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PS_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PS_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PS_ITEM_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PS_ITEM_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_MGR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_MGR_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15100_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOCK_WAIT_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOCK_WAIT_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OPEN_CURSOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OPEN_CURSOR_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_OBJECT_DEFINITION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_OBJECT_DEFINITION_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TYPE_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLL_TYPE_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLL_TYPE_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ROUTINE_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_AUTO_INCREMENT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_AUTO_INCREMENT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PART_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_USER_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_CONSTRAINT_COLUMN_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_CONSTRAINT_COLUMN_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_TIME_ZONE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_USAGE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_USAGE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_JOB_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_JOB_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_JOB_LOG_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_JOB_LOG_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_STAT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_STAT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PX_TARGET_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PX_TARGET_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MONITOR_MODIFIED_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MONITOR_MODIFIED_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OPTSTAT_GLOBAL_PREFS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OPTSTAT_GLOBAL_PREFS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OPTSTAT_USER_PREFS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OPTSTAT_USER_PREFS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBLINK_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DBLINK_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DAM_LAST_ARCH_TS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DAM_LAST_ARCH_TS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DAM_CLEANUP_JOBS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DAM_CLEANUP_JOBS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CONTEXT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CONTEXT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANS_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANS_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_UNIT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_UNIT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_KVCACHE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_KVCACHE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_META_TABLE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_META_TABLE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_META_TABLE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_META_TABLE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CORE_ALL_TABLE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CORE_ALL_TABLE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_SCHEMA_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_SCHEMA_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_PARTITION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_PARTITION_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ZONE_MERGE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ZONE_MERGE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MERGE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MERGE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_JOB_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_JOB_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_TASK_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_TASK_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_SET_FILES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_SET_FILES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_BASELINE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLAN_BASELINE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SPM_CONFIG_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SPM_CONFIG_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_EVENT_NAME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_EVENT_NAME_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ASH_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ASH_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ASH_ORA_ALL_VIRTUAL_ASH_I1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15236_ALL_VIRTUAL_ASH_I1_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DML_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DML_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_PARAMETER_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_PARAMETER_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RESTORE_JOB_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RESTORE_JOB_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RESTORE_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RESTORE_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OUTLINE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OUTLINE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OUTLINE_HISTORY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OUTLINE_HISTORY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_REPLAY_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_REPLAY_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_APPLY_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_APPLY_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ARCHIVE_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ARCHIVE_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_RECOVERY_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_RECOVERY_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FREEZE_INFO_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FREEZE_INFO_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_REPLICA_TASK_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_REPLICA_TASK_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SHOW_TRACE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SHOW_TRACE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_GROUP_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_GROUP_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_ATTRIBUTE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RLS_ATTRIBUTE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_REWRITE_RULES_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_REWRITE_RULES_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQL_PLAN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQL_PLAN_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANS_SCHEDULER_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANS_SCHEDULER_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_EXTERNAL_TABLE_FILE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_EXTERNAL_TABLE_FILE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LONG_OPS_STATUS_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LONG_OPS_STATUS_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_THREAD_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_THREAD_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SNAPSHOT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SNAPSHOT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_STATNAME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_STATNAME_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SYSSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SYSSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_OBJ_LOCK_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_OBJ_LOCK_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BALANCE_JOB_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BALANCE_JOB_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BALANCE_TASK_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BALANCE_TASK_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANSFER_TASK_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANSFER_TASK_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANSFER_TASK_HISTORY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANSFER_TASK_HISTORY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RESOURCE_POOL_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RESOURCE_POOL_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PX_P2P_DATAHUB_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PX_P2P_DATAHUB_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_PARAMETER_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_PARAMETER_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MLOG_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MLOG_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_CONTROL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_CONTROL_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FLT_CONFIG_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FLT_CONFIG_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSION_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_CLASS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_CLASS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CGROUP_CONFIG_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CGROUP_CONFIG_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SYSTEM_EVENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SYSTEM_EVENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_EVENT_NAME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_EVENT_NAME_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SQLSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SQLSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SQLSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SQLSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_STATNAME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_STATNAME_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_AUX_STAT_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_AUX_STAT_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYS_VARIABLE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYS_VARIABLE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_HISTORY_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_HISTORY_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SQLTEXT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SQLTEXT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_SNAPSHOT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_SNAPSHOT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_INDEX_USAGE_INFO_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_INDEX_USAGE_INFO_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SHARED_STORAGE_QUOTA_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SHARED_STORAGE_QUOTA_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LS_REPLICA_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LS_REPLICA_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SESSION_PS_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SESSION_PS_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TRACEPOINT_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TRACEPOINT_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SERVICE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SERVICE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_GROUP_IO_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_GROUP_IO_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_STORAGE_IO_USAGE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_STORAGE_IO_USAGE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ZONE_STORAGE_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_ZONE_STORAGE_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_NIC_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_NIC_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_QUERY_RESPONSE_TIME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_QUERY_RESPONSE_TIME_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SPATIAL_REFERENCE_SYSTEMS_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SPATIAL_REFERENCE_SYSTEMS_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_LOG_TRANSPORT_DEST_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_LOG_TRANSPORT_DEST_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SS_LOCAL_CACHE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SS_LOCAL_CACHE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SPM_EVO_RESULT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_SPM_EVO_RESULT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_VECTOR_INDEX_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_VECTOR_INDEX_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_TYPE_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PKG_TYPE_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_TYPE_ATTR_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PKG_TYPE_ATTR_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_COLL_TYPE_SYS_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PKG_COLL_TYPE_SYS_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_TYPE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PKG_TYPE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_TYPE_ATTR_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PKG_TYPE_ATTR_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_COLL_TYPE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PKG_COLL_TYPE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SQL_PLAN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SQL_PLAN_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RES_MGR_SYSSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_RES_MGR_SYSSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_RES_MGR_SYSSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_RES_MGR_SYSSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FUNCTION_IO_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_FUNCTION_IO_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TEMP_FILE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TEMP_FILE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_NCOMP_DLL_V2_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_NCOMP_DLL_V2_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_POINTER_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_TABLET_POINTER_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CS_REPLICA_TABLET_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CS_REPLICA_TABLET_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CATALOG_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CATALOG_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PL_RECOMPILE_OBJINFO_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_PL_RECOMPILE_OBJINFO_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_TENANT_VIRTUAL_SHOW_CREATE_CATALOG_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_VIRTUAL_SHOW_CREATE_CATALOG_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CCL_RULE_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CCL_RULE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CCL_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_CCL_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_RUNNING_JOB_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_RUNNING_JOB_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_MVIEW_DEP_REAL_AGENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_MVIEW_DEP_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DYNAMIC_PARTITION_TABLE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DYNAMIC_PARTITION_TABLE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBA_SOURCE_V1_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIRTUAL_DBA_SOURCE_V1_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PLAN_CACHE_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PLAN_CACHE_STAT_SCHEMA_VERSION); break;
    case OB_GV_OB_PLAN_CACHE_PLAN_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PLAN_CACHE_PLAN_STAT_SCHEMA_VERSION); break;
    case OB_SCHEMATA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_SCHEMATA_SCHEMA_VERSION); break;
    case OB_CHARACTER_SETS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CHARACTER_SETS_SCHEMA_VERSION); break;
    case OB_GLOBAL_VARIABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GLOBAL_VARIABLES_SCHEMA_VERSION); break;
    case OB_STATISTICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_STATISTICS_SCHEMA_VERSION); break;
    case OB_VIEWS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_VIEWS_SCHEMA_VERSION); break;
    case OB_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TABLES_SCHEMA_VERSION); break;
    case OB_COLLATIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_COLLATIONS_SCHEMA_VERSION); break;
    case OB_COLLATION_CHARACTER_SET_APPLICABILITY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_COLLATION_CHARACTER_SET_APPLICABILITY_SCHEMA_VERSION); break;
    case OB_PROCESSLIST_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_PROCESSLIST_SCHEMA_VERSION); break;
    case OB_KEY_COLUMN_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_KEY_COLUMN_USAGE_SCHEMA_VERSION); break;
    case OB_ENGINES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ENGINES_SCHEMA_VERSION); break;
    case OB_ROUTINES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ROUTINES_SCHEMA_VERSION); break;
    case OB_PROFILING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_PROFILING_SCHEMA_VERSION); break;
    case OB_OPTIMIZER_TRACE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_OPTIMIZER_TRACE_SCHEMA_VERSION); break;
    case OB_PLUGINS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_PLUGINS_SCHEMA_VERSION); break;
    case OB_INNODB_SYS_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_SYS_COLUMNS_SCHEMA_VERSION); break;
    case OB_INNODB_FT_BEING_DELETED_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_FT_BEING_DELETED_SCHEMA_VERSION); break;
    case OB_INNODB_FT_CONFIG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_FT_CONFIG_SCHEMA_VERSION); break;
    case OB_INNODB_FT_DELETED_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_FT_DELETED_SCHEMA_VERSION); break;
    case OB_INNODB_FT_INDEX_CACHE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_FT_INDEX_CACHE_SCHEMA_VERSION); break;
    case OB_GV_SESSION_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SESSION_EVENT_SCHEMA_VERSION); break;
    case OB_GV_SESSION_WAIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SESSION_WAIT_SCHEMA_VERSION); break;
    case OB_GV_SESSION_WAIT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SESSION_WAIT_HISTORY_SCHEMA_VERSION); break;
    case OB_GV_SYSTEM_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SYSTEM_EVENT_SCHEMA_VERSION); break;
    case OB_GV_SESSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SESSTAT_SCHEMA_VERSION); break;
    case OB_GV_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SYSSTAT_SCHEMA_VERSION); break;
    case OB_V_STATNAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_STATNAME_SCHEMA_VERSION); break;
    case OB_V_EVENT_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_EVENT_NAME_SCHEMA_VERSION); break;
    case OB_V_SESSION_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SESSION_EVENT_SCHEMA_VERSION); break;
    case OB_V_SESSION_WAIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SESSION_WAIT_SCHEMA_VERSION); break;
    case OB_V_SESSION_WAIT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SESSION_WAIT_HISTORY_SCHEMA_VERSION); break;
    case OB_V_SESSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SESSTAT_SCHEMA_VERSION); break;
    case OB_V_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SYSSTAT_SCHEMA_VERSION); break;
    case OB_V_SYSTEM_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SYSTEM_EVENT_SCHEMA_VERSION); break;
    case OB_GV_OB_SQL_AUDIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SQL_AUDIT_SCHEMA_VERSION); break;
    case OB_GV_LATCH_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_LATCH_SCHEMA_VERSION); break;
    case OB_GV_OB_MEMORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_MEMORY_SCHEMA_VERSION); break;
    case OB_V_OB_MEMORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_MEMORY_SCHEMA_VERSION); break;
    case OB_GV_OB_MEMSTORE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_MEMSTORE_SCHEMA_VERSION); break;
    case OB_V_OB_MEMSTORE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_MEMSTORE_SCHEMA_VERSION); break;
    case OB_GV_OB_MEMSTORE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_MEMSTORE_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_MEMSTORE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_MEMSTORE_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_PLAN_CACHE_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PLAN_CACHE_STAT_SCHEMA_VERSION); break;
    case OB_V_OB_PLAN_CACHE_PLAN_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PLAN_CACHE_PLAN_STAT_SCHEMA_VERSION); break;
    case OB_GV_OB_PLAN_CACHE_PLAN_EXPLAIN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PLAN_CACHE_PLAN_EXPLAIN_SCHEMA_VERSION); break;
    case OB_V_OB_PLAN_CACHE_PLAN_EXPLAIN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PLAN_CACHE_PLAN_EXPLAIN_SCHEMA_VERSION); break;
    case OB_V_OB_SQL_AUDIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SQL_AUDIT_SCHEMA_VERSION); break;
    case OB_V_LATCH_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_LATCH_SCHEMA_VERSION); break;
    case OB_GV_OB_RPC_OUTGOING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_RPC_OUTGOING_SCHEMA_VERSION); break;
    case OB_V_OB_RPC_OUTGOING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_RPC_OUTGOING_SCHEMA_VERSION); break;
    case OB_GV_OB_RPC_INCOMING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_RPC_INCOMING_SCHEMA_VERSION); break;
    case OB_V_OB_RPC_INCOMING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_RPC_INCOMING_SCHEMA_VERSION); break;
    case OB_GV_SQL_PLAN_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SQL_PLAN_MONITOR_SCHEMA_VERSION); break;
    case OB_V_SQL_PLAN_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_PLAN_MONITOR_SCHEMA_VERSION); break;
    case OB_DBA_RECYCLEBIN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_RECYCLEBIN_SCHEMA_VERSION); break;
    case OB_TIME_ZONE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TIME_ZONE_SCHEMA_VERSION); break;
    case OB_TIME_ZONE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TIME_ZONE_NAME_SCHEMA_VERSION); break;
    case OB_TIME_ZONE_TRANSITION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TIME_ZONE_TRANSITION_SCHEMA_VERSION); break;
    case OB_TIME_ZONE_TRANSITION_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TIME_ZONE_TRANSITION_TYPE_SCHEMA_VERSION); break;
    case OB_GV_SESSION_LONGOPS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SESSION_LONGOPS_SCHEMA_VERSION); break;
    case OB_V_SESSION_LONGOPS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SESSION_LONGOPS_SCHEMA_VERSION); break;
    case OB_DBA_OB_SEQUENCE_OBJECTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SEQUENCE_OBJECTS_SCHEMA_VERSION); break;
    case OB_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_COLUMNS_SCHEMA_VERSION); break;
    case OB_GV_OB_PX_WORKER_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PX_WORKER_STAT_SCHEMA_VERSION); break;
    case OB_V_OB_PX_WORKER_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PX_WORKER_STAT_SCHEMA_VERSION); break;
    case OB_GV_OB_PS_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PS_STAT_SCHEMA_VERSION); break;
    case OB_V_OB_PS_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PS_STAT_SCHEMA_VERSION); break;
    case OB_GV_OB_PS_ITEM_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PS_ITEM_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_PS_ITEM_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PS_ITEM_INFO_SCHEMA_VERSION); break;
    case OB_GV_SQL_WORKAREA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SQL_WORKAREA_SCHEMA_VERSION); break;
    case OB_V_SQL_WORKAREA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_WORKAREA_SCHEMA_VERSION); break;
    case OB_GV_SQL_WORKAREA_ACTIVE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SQL_WORKAREA_ACTIVE_SCHEMA_VERSION); break;
    case OB_V_SQL_WORKAREA_ACTIVE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_WORKAREA_ACTIVE_SCHEMA_VERSION); break;
    case OB_GV_SQL_WORKAREA_HISTOGRAM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SQL_WORKAREA_HISTOGRAM_SCHEMA_VERSION); break;
    case OB_V_SQL_WORKAREA_HISTOGRAM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_WORKAREA_HISTOGRAM_SCHEMA_VERSION); break;
    case OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_SQL_WORKAREA_MEMORY_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SQL_WORKAREA_MEMORY_INFO_SCHEMA_VERSION); break;
    case OB_GV_OB_PLAN_CACHE_REFERENCE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PLAN_CACHE_REFERENCE_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_PLAN_CACHE_REFERENCE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PLAN_CACHE_REFERENCE_INFO_SCHEMA_VERSION); break;
    case OB_GV_OB_SSTABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SSTABLES_SCHEMA_VERSION); break;
    case OB_V_OB_SSTABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SSTABLES_SCHEMA_VERSION); break;
    case OB_CDB_OB_RESTORE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_RESTORE_PROGRESS_SCHEMA_VERSION); break;
    case OB_CDB_OB_RESTORE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_RESTORE_HISTORY_SCHEMA_VERSION); break;
    case OB_GV_OB_SERVER_SCHEMA_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SERVER_SCHEMA_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_SERVER_SCHEMA_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SERVER_SCHEMA_INFO_SCHEMA_VERSION); break;
    case OB_V_SQL_MONITOR_STATNAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_MONITOR_STATNAME_SCHEMA_VERSION); break;
    case OB_GV_OB_MERGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_MERGE_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_MERGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_MERGE_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_ENCRYPTED_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_ENCRYPTED_TABLES_SCHEMA_VERSION); break;
    case OB_V_ENCRYPTED_TABLESPACES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_ENCRYPTED_TABLESPACES_SCHEMA_VERSION); break;
    case OB_CDB_OB_ARCHIVELOG_PIECE_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_ARCHIVELOG_PIECE_FILES_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_SET_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_SET_FILES_SCHEMA_VERSION); break;
    case OB_CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS_SCHEMA_VERSION); break;
    case OB_GV_OB_TENANT_MEMORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TENANT_MEMORY_SCHEMA_VERSION); break;
    case OB_V_OB_TENANT_MEMORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TENANT_MEMORY_SCHEMA_VERSION); break;
    case OB_GV_OB_PX_TARGET_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PX_TARGET_MONITOR_SCHEMA_VERSION); break;
    case OB_V_OB_PX_TARGET_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PX_TARGET_MONITOR_SCHEMA_VERSION); break;
    case OB_COLUMN_PRIVILEGES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_COLUMN_PRIVILEGES_SCHEMA_VERSION); break;
    case OB_VIEW_TABLE_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_VIEW_TABLE_USAGE_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_JOBS_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_TASKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FILES_SCHEMA_VERSION); break;
    case OB_DBA_OB_TENANTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TENANTS_SCHEMA_VERSION); break;
    case OB_DBA_OB_UNITS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_UNITS_SCHEMA_VERSION); break;
    case OB_DBA_OB_UNIT_CONFIGS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_UNIT_CONFIGS_SCHEMA_VERSION); break;
    case OB_DBA_OB_RESOURCE_POOLS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RESOURCE_POOLS_SCHEMA_VERSION); break;
    case OB_DBA_OB_SERVERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SERVERS_SCHEMA_VERSION); break;
    case OB_DBA_OB_ZONES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ZONES_SCHEMA_VERSION); break;
    case OB_DBA_OB_ROOTSERVICE_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ROOTSERVICE_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_TENANT_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TENANT_JOBS_SCHEMA_VERSION); break;
    case OB_DBA_OB_UNIT_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_UNIT_JOBS_SCHEMA_VERSION); break;
    case OB_DBA_OB_SERVER_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SERVER_JOBS_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_LOCATIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_LOCATIONS_SCHEMA_VERSION); break;
    case OB_CDB_OB_LS_LOCATIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_LS_LOCATIONS_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLET_TO_LS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLET_TO_LS_SCHEMA_VERSION); break;
    case OB_CDB_OB_TABLET_TO_LS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TABLET_TO_LS_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLET_REPLICAS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLET_REPLICAS_SCHEMA_VERSION); break;
    case OB_CDB_OB_TABLET_REPLICAS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TABLET_REPLICAS_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLEGROUPS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLEGROUPS_SCHEMA_VERSION); break;
    case OB_CDB_OB_TABLEGROUPS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TABLEGROUPS_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLEGROUP_PARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLEGROUP_PARTITIONS_SCHEMA_VERSION); break;
    case OB_CDB_OB_TABLEGROUP_PARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TABLEGROUP_PARTITIONS_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_SCHEMA_VERSION); break;
    case OB_CDB_OB_TABLEGROUP_SUBPARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TABLEGROUP_SUBPARTITIONS_SCHEMA_VERSION); break;
    case OB_DBA_OB_DATABASES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_DATABASES_SCHEMA_VERSION); break;
    case OB_CDB_OB_DATABASES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_DATABASES_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLEGROUP_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLEGROUP_TABLES_SCHEMA_VERSION); break;
    case OB_CDB_OB_TABLEGROUP_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TABLEGROUP_TABLES_SCHEMA_VERSION); break;
    case OB_DBA_OB_ZONE_MAJOR_COMPACTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ZONE_MAJOR_COMPACTION_SCHEMA_VERSION); break;
    case OB_CDB_OB_ZONE_MAJOR_COMPACTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_ZONE_MAJOR_COMPACTION_SCHEMA_VERSION); break;
    case OB_DBA_OB_MAJOR_COMPACTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_MAJOR_COMPACTION_SCHEMA_VERSION); break;
    case OB_CDB_OB_MAJOR_COMPACTION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_MAJOR_COMPACTION_SCHEMA_VERSION); break;
    case OB_CDB_OBJECTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OBJECTS_SCHEMA_VERSION); break;
    case OB_CDB_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_TABLES_SCHEMA_VERSION); break;
    case OB_CDB_TAB_COLS_V_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_TAB_COLS_V_SCHEMA_VERSION); break;
    case OB_CDB_TAB_COLS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_TAB_COLS_SCHEMA_VERSION); break;
    case OB_CDB_INDEXES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_INDEXES_SCHEMA_VERSION); break;
    case OB_CDB_IND_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_IND_COLUMNS_SCHEMA_VERSION); break;
    case OB_CDB_PART_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_PART_TABLES_SCHEMA_VERSION); break;
    case OB_CDB_TAB_PARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_TAB_PARTITIONS_SCHEMA_VERSION); break;
    case OB_CDB_TAB_SUBPARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_TAB_SUBPARTITIONS_SCHEMA_VERSION); break;
    case OB_CDB_SUBPARTITION_TEMPLATES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_SUBPARTITION_TEMPLATES_SCHEMA_VERSION); break;
    case OB_CDB_PART_KEY_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_PART_KEY_COLUMNS_SCHEMA_VERSION); break;
    case OB_CDB_SUBPART_KEY_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_SUBPART_KEY_COLUMNS_SCHEMA_VERSION); break;
    case OB_CDB_PART_INDEXES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_PART_INDEXES_SCHEMA_VERSION); break;
    case OB_CDB_IND_PARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_IND_PARTITIONS_SCHEMA_VERSION); break;
    case OB_CDB_IND_SUBPARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_IND_SUBPARTITIONS_SCHEMA_VERSION); break;
    case OB_CDB_TAB_COL_STATISTICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_TAB_COL_STATISTICS_SCHEMA_VERSION); break;
    case OB_DBA_OBJECTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OBJECTS_SCHEMA_VERSION); break;
    case OB_DBA_PART_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PART_TABLES_SCHEMA_VERSION); break;
    case OB_DBA_PART_KEY_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PART_KEY_COLUMNS_SCHEMA_VERSION); break;
    case OB_DBA_SUBPART_KEY_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SUBPART_KEY_COLUMNS_SCHEMA_VERSION); break;
    case OB_DBA_TAB_PARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_PARTITIONS_SCHEMA_VERSION); break;
    case OB_DBA_TAB_SUBPARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_SUBPARTITIONS_SCHEMA_VERSION); break;
    case OB_DBA_SUBPARTITION_TEMPLATES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SUBPARTITION_TEMPLATES_SCHEMA_VERSION); break;
    case OB_DBA_PART_INDEXES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PART_INDEXES_SCHEMA_VERSION); break;
    case OB_DBA_IND_PARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_IND_PARTITIONS_SCHEMA_VERSION); break;
    case OB_DBA_IND_SUBPARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_IND_SUBPARTITIONS_SCHEMA_VERSION); break;
    case OB_GV_OB_SERVERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SERVERS_SCHEMA_VERSION); break;
    case OB_V_OB_SERVERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SERVERS_SCHEMA_VERSION); break;
    case OB_GV_OB_UNITS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_UNITS_SCHEMA_VERSION); break;
    case OB_V_OB_UNITS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_UNITS_SCHEMA_VERSION); break;
    case OB_GV_OB_PARAMETERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PARAMETERS_SCHEMA_VERSION); break;
    case OB_V_OB_PARAMETERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PARAMETERS_SCHEMA_VERSION); break;
    case OB_GV_OB_PROCESSLIST_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PROCESSLIST_SCHEMA_VERSION); break;
    case OB_V_OB_PROCESSLIST_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PROCESSLIST_SCHEMA_VERSION); break;
    case OB_GV_OB_KVCACHE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_KVCACHE_SCHEMA_VERSION); break;
    case OB_V_OB_KVCACHE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_KVCACHE_SCHEMA_VERSION); break;
    case OB_GV_OB_TRANSACTION_PARTICIPANTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TRANSACTION_PARTICIPANTS_SCHEMA_VERSION); break;
    case OB_V_OB_TRANSACTION_PARTICIPANTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TRANSACTION_PARTICIPANTS_SCHEMA_VERSION); break;
    case OB_GV_OB_COMPACTION_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_COMPACTION_PROGRESS_SCHEMA_VERSION); break;
    case OB_V_OB_COMPACTION_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_COMPACTION_PROGRESS_SCHEMA_VERSION); break;
    case OB_GV_OB_TABLET_COMPACTION_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TABLET_COMPACTION_PROGRESS_SCHEMA_VERSION); break;
    case OB_V_OB_TABLET_COMPACTION_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TABLET_COMPACTION_PROGRESS_SCHEMA_VERSION); break;
    case OB_GV_OB_TABLET_COMPACTION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TABLET_COMPACTION_HISTORY_SCHEMA_VERSION); break;
    case OB_V_OB_TABLET_COMPACTION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TABLET_COMPACTION_HISTORY_SCHEMA_VERSION); break;
    case OB_GV_OB_COMPACTION_DIAGNOSE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_COMPACTION_DIAGNOSE_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_COMPACTION_DIAGNOSE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_COMPACTION_DIAGNOSE_INFO_SCHEMA_VERSION); break;
    case OB_GV_OB_COMPACTION_SUGGESTIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_COMPACTION_SUGGESTIONS_SCHEMA_VERSION); break;
    case OB_V_OB_COMPACTION_SUGGESTIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_COMPACTION_SUGGESTIONS_SCHEMA_VERSION); break;
    case OB_GV_OB_DTL_INTERM_RESULT_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_DTL_INTERM_RESULT_MONITOR_SCHEMA_VERSION); break;
    case OB_V_OB_DTL_INTERM_RESULT_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_DTL_INTERM_RESULT_MONITOR_SCHEMA_VERSION); break;
    case OB_GV_OB_IO_CALIBRATION_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_IO_CALIBRATION_STATUS_SCHEMA_VERSION); break;
    case OB_V_OB_IO_CALIBRATION_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_IO_CALIBRATION_STATUS_SCHEMA_VERSION); break;
    case OB_GV_OB_IO_BENCHMARK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_IO_BENCHMARK_SCHEMA_VERSION); break;
    case OB_V_OB_IO_BENCHMARK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_IO_BENCHMARK_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_DELETE_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_DELETE_JOBS_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_DELETE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_DELETE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_DELETE_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_DELETE_TASKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_DELETE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_DELETE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_DELETE_POLICY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_DELETE_POLICY_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_STORAGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_STORAGE_INFO_SCHEMA_VERSION); break;
    case OB_DBA_TAB_STATISTICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_STATISTICS_SCHEMA_VERSION); break;
    case OB_DBA_TAB_COL_STATISTICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_COL_STATISTICS_SCHEMA_VERSION); break;
    case OB_DBA_PART_COL_STATISTICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PART_COL_STATISTICS_SCHEMA_VERSION); break;
    case OB_DBA_SUBPART_COL_STATISTICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SUBPART_COL_STATISTICS_SCHEMA_VERSION); break;
    case OB_DBA_TAB_HISTOGRAMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_HISTOGRAMS_SCHEMA_VERSION); break;
    case OB_DBA_PART_HISTOGRAMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PART_HISTOGRAMS_SCHEMA_VERSION); break;
    case OB_DBA_SUBPART_HISTOGRAMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SUBPART_HISTOGRAMS_SCHEMA_VERSION); break;
    case OB_DBA_TAB_STATS_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_STATS_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_IND_STATISTICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_IND_STATISTICS_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_JOBS_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_TASKS_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_SET_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_SET_FILES_SCHEMA_VERSION); break;
    case OB_DBA_SQL_PLAN_BASELINES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SQL_PLAN_BASELINES_SCHEMA_VERSION); break;
    case OB_DBA_SQL_MANAGEMENT_CONFIG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SQL_MANAGEMENT_CONFIG_SCHEMA_VERSION); break;
    case OB_GV_ACTIVE_SESSION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_ACTIVE_SESSION_HISTORY_SCHEMA_VERSION); break;
    case OB_V_ACTIVE_SESSION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_ACTIVE_SESSION_HISTORY_SCHEMA_VERSION); break;
    case OB_GV_DML_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_DML_STATS_SCHEMA_VERSION); break;
    case OB_V_DML_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_DML_STATS_SCHEMA_VERSION); break;
    case OB_DBA_TAB_MODIFICATIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_MODIFICATIONS_SCHEMA_VERSION); break;
    case OB_DBA_SCHEDULER_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SCHEDULER_JOBS_SCHEMA_VERSION); break;
    case OB_DBA_OB_OUTLINE_CONCURRENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_OUTLINE_CONCURRENT_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_STORAGE_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_STORAGE_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_STORAGE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_STORAGE_INFO_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_DELETE_POLICY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_DELETE_POLICY_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_DELETE_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_DELETE_JOBS_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_DELETE_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_DELETE_TASKS_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_OUTLINES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_OUTLINES_SCHEMA_VERSION); break;
    case OB_DBA_OB_CONCURRENT_LIMIT_SQL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_CONCURRENT_LIMIT_SQL_SCHEMA_VERSION); break;
    case OB_DBA_OB_RESTORE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RESTORE_PROGRESS_SCHEMA_VERSION); break;
    case OB_DBA_OB_RESTORE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RESTORE_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_ARCHIVE_DEST_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ARCHIVE_DEST_SCHEMA_VERSION); break;
    case OB_DBA_OB_ARCHIVELOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ARCHIVELOG_SCHEMA_VERSION); break;
    case OB_DBA_OB_ARCHIVELOG_SUMMARY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ARCHIVELOG_SUMMARY_SCHEMA_VERSION); break;
    case OB_DBA_OB_ARCHIVELOG_PIECE_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ARCHIVELOG_PIECE_FILES_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_PARAMETER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_PARAMETER_SCHEMA_VERSION); break;
    case OB_CDB_OB_ARCHIVE_DEST_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_ARCHIVE_DEST_SCHEMA_VERSION); break;
    case OB_CDB_OB_ARCHIVELOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_ARCHIVELOG_SCHEMA_VERSION); break;
    case OB_CDB_OB_ARCHIVELOG_SUMMARY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_ARCHIVELOG_SUMMARY_SCHEMA_VERSION); break;
    case OB_CDB_OB_BACKUP_PARAMETER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BACKUP_PARAMETER_SCHEMA_VERSION); break;
    case OB_DBA_OB_DEADLOCK_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_DEADLOCK_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_DEADLOCK_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_DEADLOCK_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_SYS_VARIABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_SYS_VARIABLES_SCHEMA_VERSION); break;
    case OB_DBA_OB_KV_TTL_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_KV_TTL_TASKS_SCHEMA_VERSION); break;
    case OB_DBA_OB_KV_TTL_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_KV_TTL_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_GV_OB_LOG_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_LOG_STAT_SCHEMA_VERSION); break;
    case OB_V_OB_LOG_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LOG_STAT_SCHEMA_VERSION); break;
    case OB_ST_GEOMETRY_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ST_GEOMETRY_COLUMNS_SCHEMA_VERSION); break;
    case OB_ST_SPATIAL_REFERENCE_SYSTEMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ST_SPATIAL_REFERENCE_SYSTEMS_SCHEMA_VERSION); break;
    case OB_QUERY_RESPONSE_TIME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_QUERY_RESPONSE_TIME_SCHEMA_VERSION); break;
    case OB_CDB_OB_KV_TTL_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_KV_TTL_TASKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_KV_TTL_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_KV_TTL_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_RSRC_PLANS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_RSRC_PLANS_SCHEMA_VERSION); break;
    case OB_DBA_RSRC_PLAN_DIRECTIVES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_RSRC_PLAN_DIRECTIVES_SCHEMA_VERSION); break;
    case OB_DBA_RSRC_GROUP_MAPPINGS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_RSRC_GROUP_MAPPINGS_SCHEMA_VERSION); break;
    case OB_DBA_RSRC_CONSUMER_GROUPS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_RSRC_CONSUMER_GROUPS_SCHEMA_VERSION); break;
    case OB_V_RSRC_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_RSRC_PLAN_SCHEMA_VERSION); break;
    case OB_CDB_OB_COLUMN_CHECKSUM_ERROR_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_COLUMN_CHECKSUM_ERROR_INFO_SCHEMA_VERSION); break;
    case OB_CDB_OB_TABLET_CHECKSUM_ERROR_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TABLET_CHECKSUM_ERROR_INFO_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_SCHEMA_VERSION); break;
    case OB_CDB_OB_LS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_LS_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLE_LOCATIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLE_LOCATIONS_SCHEMA_VERSION); break;
    case OB_CDB_OB_TABLE_LOCATIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TABLE_LOCATIONS_SCHEMA_VERSION); break;
    case OB_DBA_OB_SERVER_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SERVER_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_FREEZE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_FREEZE_INFO_SCHEMA_VERSION); break;
    case OB_DBA_OB_FREEZE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_FREEZE_INFO_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_REPLICA_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_REPLICA_TASKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_LS_REPLICA_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_LS_REPLICA_TASKS_SCHEMA_VERSION); break;
    case OB_V_OB_LS_REPLICA_TASK_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LS_REPLICA_TASK_PLAN_SCHEMA_VERSION); break;
    case OB_DBA_OB_AUTO_INCREMENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_AUTO_INCREMENT_SCHEMA_VERSION); break;
    case OB_CDB_OB_AUTO_INCREMENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_AUTO_INCREMENT_SCHEMA_VERSION); break;
    case OB_DBA_SEQUENCES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SEQUENCES_SCHEMA_VERSION); break;
    case OB_DBA_SCHEDULER_WINDOWS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SCHEDULER_WINDOWS_SCHEMA_VERSION); break;
    case OB_DBA_OB_USERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_USERS_SCHEMA_VERSION); break;
    case OB_CDB_OB_USERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_USERS_SCHEMA_VERSION); break;
    case OB_DBA_OB_DATABASE_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_DATABASE_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_CDB_OB_DATABASE_PRIVILEGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_DATABASE_PRIVILEGE_SCHEMA_VERSION); break;
    case OB_DBA_OB_USER_DEFINED_RULES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_USER_DEFINED_RULES_SCHEMA_VERSION); break;
    case OB_GV_OB_SQL_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SQL_PLAN_SCHEMA_VERSION); break;
    case OB_V_OB_SQL_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SQL_PLAN_SCHEMA_VERSION); break;
    case OB_DBA_OB_CLUSTER_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_CLUSTER_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_PARAMETERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_PARAMETERS_SCHEMA_VERSION); break;
    case OB_TABLE_PRIVILEGES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TABLE_PRIVILEGES_SCHEMA_VERSION); break;
    case OB_USER_PRIVILEGES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PRIVILEGES_SCHEMA_VERSION); break;
    case OB_SCHEMA_PRIVILEGES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_SCHEMA_PRIVILEGES_SCHEMA_VERSION); break;
    case OB_CHECK_CONSTRAINTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CHECK_CONSTRAINTS_SCHEMA_VERSION); break;
    case OB_REFERENTIAL_CONSTRAINTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_REFERENTIAL_CONSTRAINTS_SCHEMA_VERSION); break;
    case OB_TABLE_CONSTRAINTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TABLE_CONSTRAINTS_SCHEMA_VERSION); break;
    case OB_GV_OB_TRANSACTION_SCHEDULERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TRANSACTION_SCHEDULERS_SCHEMA_VERSION); break;
    case OB_V_OB_TRANSACTION_SCHEDULERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TRANSACTION_SCHEDULERS_SCHEMA_VERSION); break;
    case OB_TRIGGERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TRIGGERS_SCHEMA_VERSION); break;
    case OB_PARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_PARTITIONS_SCHEMA_VERSION); break;
    case OB_DBA_OB_ARBITRATION_SERVICE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ARBITRATION_SERVICE_SCHEMA_VERSION); break;
    case OB_CDB_OB_LS_ARB_REPLICA_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_LS_ARB_REPLICA_TASKS_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_ARB_REPLICA_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_ARB_REPLICA_TASKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_LS_ARB_REPLICA_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_LS_ARB_REPLICA_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_V_OB_ARCHIVE_DEST_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_ARCHIVE_DEST_STATUS_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_SCHEMA_VERSION); break;
    case OB_CDB_OB_LS_LOG_ARCHIVE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_LS_LOG_ARCHIVE_PROGRESS_SCHEMA_VERSION); break;
    case OB_DBA_OB_RSRC_IO_DIRECTIVES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RSRC_IO_DIRECTIVES_SCHEMA_VERSION); break;
    case OB_GV_OB_TABLET_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TABLET_STATS_SCHEMA_VERSION); break;
    case OB_V_OB_TABLET_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TABLET_STATS_SCHEMA_VERSION); break;
    case OB_DBA_OB_ACCESS_POINT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ACCESS_POINT_SCHEMA_VERSION); break;
    case OB_CDB_OB_ACCESS_POINT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_ACCESS_POINT_SCHEMA_VERSION); break;
    case OB_CDB_OB_DATA_DICTIONARY_IN_LOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_DATA_DICTIONARY_IN_LOG_SCHEMA_VERSION); break;
    case OB_DBA_OB_DATA_DICTIONARY_IN_LOG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_DATA_DICTIONARY_IN_LOG_SCHEMA_VERSION); break;
    case OB_GV_OB_OPT_STAT_GATHER_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_OPT_STAT_GATHER_MONITOR_SCHEMA_VERSION); break;
    case OB_V_OB_OPT_STAT_GATHER_MONITOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_OPT_STAT_GATHER_MONITOR_SCHEMA_VERSION); break;
    case OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_SCHEMA_VERSION); break;
    case OB_GV_OB_THREAD_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_THREAD_SCHEMA_VERSION); break;
    case OB_V_OB_THREAD_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_THREAD_SCHEMA_VERSION); break;
    case OB_GV_OB_ARBITRATION_MEMBER_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_ARBITRATION_MEMBER_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_ARBITRATION_MEMBER_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_ARBITRATION_MEMBER_INFO_SCHEMA_VERSION); break;
    case OB_DBA_OB_ZONE_STORAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ZONE_STORAGE_SCHEMA_VERSION); break;
    case OB_GV_OB_SERVER_STORAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SERVER_STORAGE_SCHEMA_VERSION); break;
    case OB_V_OB_SERVER_STORAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SERVER_STORAGE_SCHEMA_VERSION); break;
    case OB_GV_OB_ARBITRATION_SERVICE_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_ARBITRATION_SERVICE_STATUS_SCHEMA_VERSION); break;
    case OB_V_OB_ARBITRATION_SERVICE_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_ARBITRATION_SERVICE_STATUS_SCHEMA_VERSION); break;
    case OB_DBA_WR_ACTIVE_SESSION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_ACTIVE_SESSION_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_WR_ACTIVE_SESSION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_ACTIVE_SESSION_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_WR_SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SNAPSHOT_SCHEMA_VERSION); break;
    case OB_CDB_WR_SNAPSHOT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_SNAPSHOT_SCHEMA_VERSION); break;
    case OB_DBA_WR_STATNAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_STATNAME_SCHEMA_VERSION); break;
    case OB_CDB_WR_STATNAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_STATNAME_SCHEMA_VERSION); break;
    case OB_DBA_WR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_CDB_WR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_GV_OB_KV_CONNECTIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_KV_CONNECTIONS_SCHEMA_VERSION); break;
    case OB_V_OB_KV_CONNECTIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_KV_CONNECTIONS_SCHEMA_VERSION); break;
    case OB_GV_OB_LOCKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_LOCKS_SCHEMA_VERSION); break;
    case OB_V_OB_LOCKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LOCKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_LOG_RESTORE_SOURCE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_LOG_RESTORE_SOURCE_SCHEMA_VERSION); break;
    case OB_DBA_OB_LOG_RESTORE_SOURCE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LOG_RESTORE_SOURCE_SCHEMA_VERSION); break;
    case OB_V_OB_TIMESTAMP_SERVICE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TIMESTAMP_SERVICE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BALANCE_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BALANCE_JOBS_SCHEMA_VERSION); break;
    case OB_CDB_OB_BALANCE_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BALANCE_JOBS_SCHEMA_VERSION); break;
    case OB_DBA_OB_BALANCE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BALANCE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_BALANCE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BALANCE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_BALANCE_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BALANCE_TASKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_BALANCE_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BALANCE_TASKS_SCHEMA_VERSION); break;
    case OB_DBA_OB_BALANCE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BALANCE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_BALANCE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_BALANCE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_TRANSFER_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TRANSFER_TASKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_TRANSFER_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TRANSFER_TASKS_SCHEMA_VERSION); break;
    case OB_DBA_OB_TRANSFER_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TRANSFER_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_TRANSFER_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TRANSFER_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_EXTERNAL_TABLE_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_EXTERNAL_TABLE_FILES_SCHEMA_VERSION); break;
    case OB_ALL_OB_EXTERNAL_TABLE_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OB_EXTERNAL_TABLE_FILES_SCHEMA_VERSION); break;
    case OB_GV_OB_PX_P2P_DATAHUB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PX_P2P_DATAHUB_SCHEMA_VERSION); break;
    case OB_V_OB_PX_P2P_DATAHUB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PX_P2P_DATAHUB_SCHEMA_VERSION); break;
    case OB_GV_SQL_JOIN_FILTER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SQL_JOIN_FILTER_SCHEMA_VERSION); break;
    case OB_V_SQL_JOIN_FILTER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_JOIN_FILTER_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLE_STAT_STALE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLE_STAT_STALE_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_LS_LOG_RESTORE_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LS_LOG_RESTORE_STATUS_SCHEMA_VERSION); break;
    case OB_CDB_OB_EXTERNAL_TABLE_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_EXTERNAL_TABLE_FILES_SCHEMA_VERSION); break;
    case OB_DBA_DB_LINKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_DB_LINKS_SCHEMA_VERSION); break;
    case OB_DBA_WR_CONTROL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_CONTROL_SCHEMA_VERSION); break;
    case OB_CDB_WR_CONTROL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_CONTROL_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_LS_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_LS_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_TENANT_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TENANT_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_TENANT_EVENT_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TENANT_EVENT_HISTORY_SCHEMA_VERSION); break;
    case OB_GV_OB_FLT_TRACE_CONFIG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_FLT_TRACE_CONFIG_SCHEMA_VERSION); break;
    case OB_GV_OB_SESSION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SESSION_SCHEMA_VERSION); break;
    case OB_V_OB_SESSION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SESSION_SCHEMA_VERSION); break;
    case OB_GV_OB_PL_CACHE_OBJECT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PL_CACHE_OBJECT_SCHEMA_VERSION); break;
    case OB_V_OB_PL_CACHE_OBJECT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PL_CACHE_OBJECT_SCHEMA_VERSION); break;
    case OB_CDB_OB_RECOVER_TABLE_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_RECOVER_TABLE_JOBS_SCHEMA_VERSION); break;
    case OB_DBA_OB_RECOVER_TABLE_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RECOVER_TABLE_JOBS_SCHEMA_VERSION); break;
    case OB_CDB_OB_RECOVER_TABLE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_RECOVER_TABLE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_RECOVER_TABLE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RECOVER_TABLE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_IMPORT_TABLE_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_IMPORT_TABLE_JOBS_SCHEMA_VERSION); break;
    case OB_DBA_OB_IMPORT_TABLE_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_IMPORT_TABLE_JOBS_SCHEMA_VERSION); break;
    case OB_CDB_OB_IMPORT_TABLE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_IMPORT_TABLE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_IMPORT_TABLE_JOB_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_IMPORT_TABLE_JOB_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_IMPORT_TABLE_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_IMPORT_TABLE_TASKS_SCHEMA_VERSION); break;
    case OB_DBA_OB_IMPORT_TABLE_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_IMPORT_TABLE_TASKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_IMPORT_TABLE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_IMPORT_TABLE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_IMPORT_TABLE_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_IMPORT_TABLE_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_GV_OB_TENANT_RUNTIME_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TENANT_RUNTIME_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_TENANT_RUNTIME_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TENANT_RUNTIME_INFO_SCHEMA_VERSION); break;
    case OB_GV_OB_CGROUP_CONFIG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_CGROUP_CONFIG_SCHEMA_VERSION); break;
    case OB_V_OB_CGROUP_CONFIG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_CGROUP_CONFIG_SCHEMA_VERSION); break;
    case OB_DBA_WR_SYSTEM_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SYSTEM_EVENT_SCHEMA_VERSION); break;
    case OB_CDB_WR_SYSTEM_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_SYSTEM_EVENT_SCHEMA_VERSION); break;
    case OB_DBA_WR_EVENT_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_EVENT_NAME_SCHEMA_VERSION); break;
    case OB_CDB_WR_EVENT_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_EVENT_NAME_SCHEMA_VERSION); break;
    case OB_DBA_OB_FORMAT_OUTLINES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_FORMAT_OUTLINES_SCHEMA_VERSION); break;
    case OB_PROCS_PRIV_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_PROCS_PRIV_SCHEMA_VERSION); break;
    case OB_GV_OB_SQLSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SQLSTAT_SCHEMA_VERSION); break;
    case OB_V_OB_SQLSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SQLSTAT_SCHEMA_VERSION); break;
    case OB_DBA_WR_SQLSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SQLSTAT_SCHEMA_VERSION); break;
    case OB_CDB_WR_SQLSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_SQLSTAT_SCHEMA_VERSION); break;
    case OB_GV_OB_SESS_TIME_MODEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SESS_TIME_MODEL_SCHEMA_VERSION); break;
    case OB_V_OB_SESS_TIME_MODEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SESS_TIME_MODEL_SCHEMA_VERSION); break;
    case OB_GV_OB_SYS_TIME_MODEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SYS_TIME_MODEL_SCHEMA_VERSION); break;
    case OB_V_OB_SYS_TIME_MODEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SYS_TIME_MODEL_SCHEMA_VERSION); break;
    case OB_DBA_WR_SYS_TIME_MODEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SYS_TIME_MODEL_SCHEMA_VERSION); break;
    case OB_CDB_WR_SYS_TIME_MODEL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_SYS_TIME_MODEL_SCHEMA_VERSION); break;
    case OB_DBA_OB_AUX_STATISTICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_AUX_STATISTICS_SCHEMA_VERSION); break;
    case OB_CDB_OB_AUX_STATISTICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_AUX_STATISTICS_SCHEMA_VERSION); break;
    case OB_DBA_INDEX_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_INDEX_USAGE_SCHEMA_VERSION); break;
    case OB_DBA_OB_SYS_VARIABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SYS_VARIABLES_SCHEMA_VERSION); break;
    case OB_DBA_OB_TRANSFER_PARTITION_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TRANSFER_PARTITION_TASKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_TRANSFER_PARTITION_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TRANSFER_PARTITION_TASKS_SCHEMA_VERSION); break;
    case OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_TRANSFER_PARTITION_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TRANSFER_PARTITION_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_WR_SQLTEXT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SQLTEXT_SCHEMA_VERSION); break;
    case OB_CDB_WR_SQLTEXT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_SQLTEXT_SCHEMA_VERSION); break;
    case OB_GV_OB_ACTIVE_SESSION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_ACTIVE_SESSION_HISTORY_SCHEMA_VERSION); break;
    case OB_V_OB_ACTIVE_SESSION_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_ACTIVE_SESSION_HISTORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_TRUSTED_ROOT_CERTIFICATE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TRUSTED_ROOT_CERTIFICATE_SCHEMA_VERSION); break;
    case OB_DBA_OB_CLONE_PROGRESS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_CLONE_PROGRESS_SCHEMA_VERSION); break;
    case OB_ROLE_EDGES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ROLE_EDGES_SCHEMA_VERSION); break;
    case OB_DEFAULT_ROLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DEFAULT_ROLES_SCHEMA_VERSION); break;
    case OB_CDB_INDEX_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_INDEX_USAGE_SCHEMA_VERSION); break;
    case OB_AUDIT_LOG_FILTER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_AUDIT_LOG_FILTER_SCHEMA_VERSION); break;
    case OB_AUDIT_LOG_USER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_AUDIT_LOG_USER_SCHEMA_VERSION); break;
    case OB_COLUMNS_PRIV_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_COLUMNS_PRIV_SCHEMA_VERSION); break;
    case OB_GV_OB_LS_SNAPSHOTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_LS_SNAPSHOTS_SCHEMA_VERSION); break;
    case OB_V_OB_LS_SNAPSHOTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LS_SNAPSHOTS_SCHEMA_VERSION); break;
    case OB_DBA_OB_CLONE_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_CLONE_HISTORY_SCHEMA_VERSION); break;
    case OB_GV_OB_SHARED_STORAGE_QUOTA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SHARED_STORAGE_QUOTA_SCHEMA_VERSION); break;
    case OB_V_OB_SHARED_STORAGE_QUOTA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SHARED_STORAGE_QUOTA_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_REPLICA_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_REPLICA_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_LS_REPLICA_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_LS_REPLICA_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_MVIEW_LOGS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_MVIEW_LOGS_SCHEMA_VERSION); break;
    case OB_DBA_MVIEW_LOGS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVIEW_LOGS_SCHEMA_VERSION); break;
    case OB_CDB_MVIEWS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_MVIEWS_SCHEMA_VERSION); break;
    case OB_DBA_MVIEWS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVIEWS_SCHEMA_VERSION); break;
    case OB_CDB_MVREF_STATS_SYS_DEFAULTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_MVREF_STATS_SYS_DEFAULTS_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_STATS_SYS_DEFAULTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_STATS_SYS_DEFAULTS_SCHEMA_VERSION); break;
    case OB_CDB_MVREF_STATS_PARAMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_MVREF_STATS_PARAMS_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_STATS_PARAMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_STATS_PARAMS_SCHEMA_VERSION); break;
    case OB_CDB_MVREF_RUN_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_MVREF_RUN_STATS_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_RUN_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_RUN_STATS_SCHEMA_VERSION); break;
    case OB_CDB_MVREF_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_MVREF_STATS_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_STATS_SCHEMA_VERSION); break;
    case OB_CDB_MVREF_CHANGE_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_MVREF_CHANGE_STATS_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_CHANGE_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_CHANGE_STATS_SCHEMA_VERSION); break;
    case OB_CDB_MVREF_STMT_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_MVREF_STMT_STATS_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_STMT_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_STMT_STATS_SCHEMA_VERSION); break;
    case OB_GV_OB_SESSION_PS_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SESSION_PS_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_SESSION_PS_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SESSION_PS_INFO_SCHEMA_VERSION); break;
    case OB_GV_OB_TRACEPOINT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TRACEPOINT_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_TRACEPOINT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TRACEPOINT_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_COMPATIBILITY_CONTROL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_COMPATIBILITY_CONTROL_SCHEMA_VERSION); break;
    case OB_DBA_OB_RSRC_DIRECTIVES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RSRC_DIRECTIVES_SCHEMA_VERSION); break;
    case OB_CDB_OB_RSRC_DIRECTIVES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_RSRC_DIRECTIVES_SCHEMA_VERSION); break;
    case OB_DBA_OB_SERVICES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SERVICES_SCHEMA_VERSION); break;
    case OB_CDB_OB_SERVICES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_SERVICES_SCHEMA_VERSION); break;
    case OB_GV_OB_TENANT_RESOURCE_LIMIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TENANT_RESOURCE_LIMIT_SCHEMA_VERSION); break;
    case OB_V_OB_TENANT_RESOURCE_LIMIT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TENANT_RESOURCE_LIMIT_SCHEMA_VERSION); break;
    case OB_GV_OB_TENANT_RESOURCE_LIMIT_DETAIL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TENANT_RESOURCE_LIMIT_DETAIL_SCHEMA_VERSION); break;
    case OB_V_OB_TENANT_RESOURCE_LIMIT_DETAIL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TENANT_RESOURCE_LIMIT_DETAIL_SCHEMA_VERSION); break;
    case OB_INNODB_LOCK_WAITS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_LOCK_WAITS_SCHEMA_VERSION); break;
    case OB_INNODB_LOCKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_LOCKS_SCHEMA_VERSION); break;
    case OB_INNODB_TRX_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_TRX_SCHEMA_VERSION); break;
    case OB_NDB_TRANSID_MYSQL_CONNECTION_MAP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_NDB_TRANSID_MYSQL_CONNECTION_MAP_SCHEMA_VERSION); break;
    case OB_V_OB_GROUP_IO_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_GROUP_IO_STAT_SCHEMA_VERSION); break;
    case OB_GV_OB_GROUP_IO_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_GROUP_IO_STAT_SCHEMA_VERSION); break;
    case OB_DBA_OB_STORAGE_IO_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_STORAGE_IO_USAGE_SCHEMA_VERSION); break;
    case OB_CDB_OB_STORAGE_IO_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_STORAGE_IO_USAGE_SCHEMA_VERSION); break;
    case OB_TABLESPACES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TABLESPACES_SCHEMA_VERSION); break;
    case OB_INNODB_BUFFER_PAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_BUFFER_PAGE_SCHEMA_VERSION); break;
    case OB_INNODB_BUFFER_PAGE_LRU_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_BUFFER_PAGE_LRU_SCHEMA_VERSION); break;
    case OB_INNODB_BUFFER_POOL_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_BUFFER_POOL_STATS_SCHEMA_VERSION); break;
    case OB_INNODB_CMP_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_CMP_SCHEMA_VERSION); break;
    case OB_INNODB_CMP_PER_INDEX_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_CMP_PER_INDEX_SCHEMA_VERSION); break;
    case OB_INNODB_CMP_PER_INDEX_RESET_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_CMP_PER_INDEX_RESET_SCHEMA_VERSION); break;
    case OB_INNODB_CMP_RESET_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_CMP_RESET_SCHEMA_VERSION); break;
    case OB_INNODB_CMPMEM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_CMPMEM_SCHEMA_VERSION); break;
    case OB_INNODB_CMPMEM_RESET_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_CMPMEM_RESET_SCHEMA_VERSION); break;
    case OB_INNODB_SYS_DATAFILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_SYS_DATAFILES_SCHEMA_VERSION); break;
    case OB_INNODB_SYS_INDEXES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_SYS_INDEXES_SCHEMA_VERSION); break;
    case OB_INNODB_SYS_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_SYS_TABLES_SCHEMA_VERSION); break;
    case OB_INNODB_SYS_TABLESPACES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_SYS_TABLESPACES_SCHEMA_VERSION); break;
    case OB_INNODB_SYS_TABLESTATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_SYS_TABLESTATS_SCHEMA_VERSION); break;
    case OB_INNODB_SYS_VIRTUAL_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_SYS_VIRTUAL_SCHEMA_VERSION); break;
    case OB_INNODB_TEMP_TABLE_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_TEMP_TABLE_INFO_SCHEMA_VERSION); break;
    case OB_INNODB_METRICS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_METRICS_SCHEMA_VERSION); break;
    case OB_EVENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_EVENTS_SCHEMA_VERSION); break;
    case OB_V_OB_NIC_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_NIC_INFO_SCHEMA_VERSION); break;
    case OB_ROLE_TABLE_GRANTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ROLE_TABLE_GRANTS_SCHEMA_VERSION); break;
    case OB_ROLE_COLUMN_GRANTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ROLE_COLUMN_GRANTS_SCHEMA_VERSION); break;
    case OB_ROLE_ROUTINE_GRANTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ROLE_ROUTINE_GRANTS_SCHEMA_VERSION); break;
    case OB_FUNC_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FUNC_SCHEMA_VERSION); break;
    case OB_GV_OB_NIC_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_NIC_INFO_SCHEMA_VERSION); break;
    case OB_GV_OB_QUERY_RESPONSE_TIME_HISTOGRAM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_QUERY_RESPONSE_TIME_HISTOGRAM_SCHEMA_VERSION); break;
    case OB_V_OB_QUERY_RESPONSE_TIME_HISTOGRAM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_QUERY_RESPONSE_TIME_HISTOGRAM_SCHEMA_VERSION); break;
    case OB_DBA_SCHEDULER_JOB_RUN_DETAILS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SCHEDULER_JOB_RUN_DETAILS_SCHEMA_VERSION); break;
    case OB_CDB_SCHEDULER_JOB_RUN_DETAILS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_SCHEDULER_JOB_RUN_DETAILS_SCHEMA_VERSION); break;
    case OB_CDB_OB_SERVER_SPACE_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_SERVER_SPACE_USAGE_SCHEMA_VERSION); break;
    case OB_CDB_OB_SPACE_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_SPACE_USAGE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLE_SPACE_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLE_SPACE_USAGE_SCHEMA_VERSION); break;
    case OB_CDB_OB_TABLE_SPACE_USAGE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TABLE_SPACE_USAGE_SCHEMA_VERSION); break;
    case OB_GV_OB_LOG_TRANSPORT_DEST_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_LOG_TRANSPORT_DEST_STAT_SCHEMA_VERSION); break;
    case OB_V_OB_LOG_TRANSPORT_DEST_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LOG_TRANSPORT_DEST_STAT_SCHEMA_VERSION); break;
    case OB_GV_OB_SS_LOCAL_CACHE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SS_LOCAL_CACHE_SCHEMA_VERSION); break;
    case OB_V_OB_SS_LOCAL_CACHE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SS_LOCAL_CACHE_SCHEMA_VERSION); break;
    case OB_GV_OB_KV_GROUP_COMMIT_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_KV_GROUP_COMMIT_STATUS_SCHEMA_VERSION); break;
    case OB_V_OB_KV_GROUP_COMMIT_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_KV_GROUP_COMMIT_STATUS_SCHEMA_VERSION); break;
    case OB_INNODB_SYS_FIELDS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_SYS_FIELDS_SCHEMA_VERSION); break;
    case OB_INNODB_SYS_FOREIGN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_SYS_FOREIGN_SCHEMA_VERSION); break;
    case OB_INNODB_SYS_FOREIGN_COLS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_INNODB_SYS_FOREIGN_COLS_SCHEMA_VERSION); break;
    case OB_GV_OB_KV_CLIENT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_KV_CLIENT_INFO_SCHEMA_VERSION); break;
    case OB_V_OB_KV_CLIENT_INFO_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_KV_CLIENT_INFO_SCHEMA_VERSION); break;
    case OB_GV_OB_RES_MGR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_RES_MGR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_V_OB_RES_MGR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_RES_MGR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_DBA_WR_SQL_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SQL_PLAN_SCHEMA_VERSION); break;
    case OB_CDB_WR_SQL_PLAN_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_SQL_PLAN_SCHEMA_VERSION); break;
    case OB_DBA_WR_RES_MGR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_RES_MGR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_CDB_WR_RES_MGR_SYSSTAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_WR_RES_MGR_SYSSTAT_SCHEMA_VERSION); break;
    case OB_DBA_OB_SPM_EVO_RESULT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SPM_EVO_RESULT_SCHEMA_VERSION); break;
    case OB_CDB_OB_SPM_EVO_RESULT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_SPM_EVO_RESULT_SCHEMA_VERSION); break;
    case OB_DBA_OB_KV_REDIS_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_KV_REDIS_TABLE_SCHEMA_VERSION); break;
    case OB_CDB_OB_KV_REDIS_TABLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_KV_REDIS_TABLE_SCHEMA_VERSION); break;
    case OB_GV_OB_FUNCTION_IO_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_FUNCTION_IO_STAT_SCHEMA_VERSION); break;
    case OB_V_OB_FUNCTION_IO_STAT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_FUNCTION_IO_STAT_SCHEMA_VERSION); break;
    case OB_DBA_OB_TEMP_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TEMP_FILES_SCHEMA_VERSION); break;
    case OB_CDB_OB_TEMP_FILES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_TEMP_FILES_SCHEMA_VERSION); break;
    case OB_PROC_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_PROC_SCHEMA_VERSION); break;
    case OB_DBA_OB_CS_REPLICA_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_CS_REPLICA_STATS_SCHEMA_VERSION); break;
    case OB_CDB_OB_CS_REPLICA_STATS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_CS_REPLICA_STATS_SCHEMA_VERSION); break;
    case OB_GV_OB_PLUGINS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PLUGINS_SCHEMA_VERSION); break;
    case OB_V_OB_PLUGINS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PLUGINS_SCHEMA_VERSION); break;
    case OB_DBA_OB_LICENSE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LICENSE_SCHEMA_VERSION); break;
    case OB_DBA_OB_VECTOR_INDEX_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_VECTOR_INDEX_TASKS_SCHEMA_VERSION); break;
    case OB_CDB_OB_VECTOR_INDEX_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_VECTOR_INDEX_TASKS_SCHEMA_VERSION); break;
    case OB_DBA_OB_VECTOR_INDEX_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_VECTOR_INDEX_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_CDB_OB_VECTOR_INDEX_TASK_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_VECTOR_INDEX_TASK_HISTORY_SCHEMA_VERSION); break;
    case OB_GV_OB_STORAGE_CACHE_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_STORAGE_CACHE_TASKS_SCHEMA_VERSION); break;
    case OB_V_OB_STORAGE_CACHE_TASKS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_STORAGE_CACHE_TASKS_SCHEMA_VERSION); break;
    case OB_GV_OB_TABLET_LOCAL_CACHE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TABLET_LOCAL_CACHE_SCHEMA_VERSION); break;
    case OB_V_OB_TABLET_LOCAL_CACHE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TABLET_LOCAL_CACHE_SCHEMA_VERSION); break;
    case OB_DBA_OB_CCL_RULES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_CCL_RULES_SCHEMA_VERSION); break;
    case OB_CDB_OB_CCL_RULES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_CCL_RULES_SCHEMA_VERSION); break;
    case OB_GV_OB_SQL_CCL_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SQL_CCL_STATUS_SCHEMA_VERSION); break;
    case OB_V_OB_SQL_CCL_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SQL_CCL_STATUS_SCHEMA_VERSION); break;
    case OB_DBA_MVIEW_RUNNING_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVIEW_RUNNING_JOBS_SCHEMA_VERSION); break;
    case OB_CDB_MVIEW_RUNNING_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_MVIEW_RUNNING_JOBS_SCHEMA_VERSION); break;
    case OB_DBA_MVIEW_DEPS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVIEW_DEPS_SCHEMA_VERSION); break;
    case OB_DBA_OB_DYNAMIC_PARTITION_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_DYNAMIC_PARTITION_TABLES_SCHEMA_VERSION); break;
    case OB_CDB_OB_DYNAMIC_PARTITION_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_DYNAMIC_PARTITION_TABLES_SCHEMA_VERSION); break;
    case OB_V_OB_DYNAMIC_PARTITION_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_DYNAMIC_PARTITION_TABLES_SCHEMA_VERSION); break;
    case OB_GV_OB_VECTOR_MEMORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_VECTOR_MEMORY_SCHEMA_VERSION); break;
    case OB_V_OB_VECTOR_MEMORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_VECTOR_MEMORY_SCHEMA_VERSION); break;
    case OB_DBA_OB_SENSITIVE_RULES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SENSITIVE_RULES_SCHEMA_VERSION); break;
    case OB_CDB_OB_SENSITIVE_RULES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_SENSITIVE_RULES_SCHEMA_VERSION); break;
    case OB_DBA_OB_SENSITIVE_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SENSITIVE_COLUMNS_SCHEMA_VERSION); break;
    case OB_CDB_OB_SENSITIVE_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_SENSITIVE_COLUMNS_SCHEMA_VERSION); break;
    case OB_DBA_OB_SENSITIVE_RULE_PLAINACCESS_USERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SENSITIVE_RULE_PLAINACCESS_USERS_SCHEMA_VERSION); break;
    case OB_CDB_OB_SENSITIVE_RULE_PLAINACCESS_USERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_CDB_OB_SENSITIVE_RULE_PLAINACCESS_USERS_SCHEMA_VERSION); break;
    case OB_DBA_SYNONYMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SYNONYMS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OBJECTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OBJECTS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_OBJECTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OBJECTS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_OBJECTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_OBJECTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SEQUENCES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SEQUENCES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEQUENCES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SEQUENCES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SEQUENCES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_USERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_USERS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_USERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USERS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SYNONYMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYNONYMS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SYNONYMS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SYNONYMS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_IND_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_IND_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_IND_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IND_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_IND_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_IND_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_CONSTRAINTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_CONSTRAINTS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_CONSTRAINTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONSTRAINTS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_CONSTRAINTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_CONSTRAINTS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_COLS_V_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_COLS_V_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_COLS_V_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_COLS_V_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_COLS_V_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_COLS_V_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_COLS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_COLS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_COLS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_COLS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_COLS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_COLS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TABLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_COMMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_COMMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_COMMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_COMMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_COMMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_COMMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_COL_COMMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_COL_COMMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_COL_COMMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COL_COMMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_COL_COMMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_COL_COMMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_INDEXES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_INDEXES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_INDEXES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_INDEXES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_INDEXES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_INDEXES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_CONS_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_CONS_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_CONS_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONS_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_CONS_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_CONS_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SEGMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SEGMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SEGMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SEGMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TYPE_ATTRS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TYPE_ATTRS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_ATTRS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_ATTRS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TYPE_ATTRS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TYPE_ATTRS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_COLL_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_COLL_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_COLL_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLL_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_COLL_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_COLL_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_PROCEDURES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PROCEDURES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_ARGUMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_ARGUMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SOURCE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SOURCE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_PROCEDURES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PROCEDURES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_ARGUMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ARGUMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SOURCE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SOURCE_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_PROCEDURES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PROCEDURES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_ARGUMENTS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_ARGUMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SOURCE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SOURCE_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_PART_KEY_COLUMNS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PART_KEY_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_PART_KEY_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_KEY_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_PART_KEY_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PART_KEY_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SUBPART_KEY_COLUMNS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SUBPART_KEY_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SUBPART_KEY_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SUBPART_KEY_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SUBPART_KEY_COLUMNS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SUBPART_KEY_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_VIEWS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_VIEWS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIEWS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VIEWS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_VIEWS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_VIEWS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_PARTITIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_PARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_SUBPARTITIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_SUBPARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_PART_TABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_PART_TABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PART_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_PART_TABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PART_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_PARTITIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_PARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_PARTITIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_PARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_SUBPARTITIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_SUBPARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_SUBPARTITIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_SUBPARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SUBPARTITION_TEMPLATES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SUBPARTITION_TEMPLATES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SUBPARTITION_TEMPLATES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SUBPARTITION_TEMPLATES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SUBPARTITION_TEMPLATES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SUBPARTITION_TEMPLATES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_PART_INDEXES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PART_INDEXES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_PART_INDEXES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_INDEXES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_PART_INDEXES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PART_INDEXES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_ALL_TABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ALL_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_ALL_TABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_ALL_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_ALL_TABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_ALL_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_PROFILES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PROFILES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_PROFILES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PROFILES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_PROFILES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PROFILES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_COMMENTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_COMMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_MVIEW_COMMENTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_MVIEW_COMMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVIEW_COMMENTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVIEW_COMMENTS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SCHEDULER_PROGRAM_ARGS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SCHEDULER_PROGRAM_ARGS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SCHEDULER_PROGRAM_ARGS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SCHEDULER_PROGRAM_ARGS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SCHEDULER_PROGRAM_ARGS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SCHEDULER_PROGRAM_ARGS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SCHEDULER_JOB_ARGS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SCHEDULER_JOB_ARGS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SCHEDULER_JOB_ARGS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SCHEDULER_JOB_ARGS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SCHEDULER_JOB_ARGS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SCHEDULER_JOB_ARGS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_ERRORS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ERRORS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_ERRORS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_ERRORS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_ERRORS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_ERRORS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_METHODS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_METHODS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TYPE_METHODS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TYPE_METHODS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TYPE_METHODS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TYPE_METHODS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_METHOD_PARAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_METHOD_PARAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_METHOD_PARAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_METHOD_PARAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_METHOD_PARAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_METHOD_PARAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TABLESPACES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TABLESPACES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TABLESPACES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TABLESPACES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_IND_EXPRESSIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_IND_EXPRESSIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_IND_EXPRESSIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_IND_EXPRESSIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_IND_EXPRESSIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IND_EXPRESSIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_IND_PARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IND_PARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_IND_PARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_IND_PARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_IND_PARTITIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_IND_PARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_IND_SUBPARTITIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_IND_SUBPARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_IND_SUBPARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IND_SUBPARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_IND_SUBPARTITIONS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_IND_SUBPARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_ROLES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_ROLES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_ROLE_PRIVS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_ROLE_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_ROLE_PRIVS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_ROLE_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_PRIVS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_PRIVS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_PRIVS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SYS_PRIVS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SYS_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SYS_PRIVS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SYS_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_AUDIT_ACTIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_AUDIT_ACTIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_STMT_AUDIT_OPTION_MAP_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_STMT_AUDIT_OPTION_MAP_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_DEF_AUDIT_OPTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEF_AUDIT_OPTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_STMT_AUDIT_OPTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_STMT_AUDIT_OPTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OBJ_AUDIT_OPTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OBJ_AUDIT_OPTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_AUDIT_TRAIL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_AUDIT_TRAIL_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_AUDIT_TRAIL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_AUDIT_TRAIL_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_AUDIT_EXISTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_AUDIT_EXISTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_AUDIT_SESSION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_AUDIT_SESSION_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_AUDIT_SESSION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_AUDIT_SESSION_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_AUDIT_STATEMENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_AUDIT_STATEMENT_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_AUDIT_STATEMENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_AUDIT_STATEMENT_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_AUDIT_OBJECT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_AUDIT_OBJECT_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_AUDIT_OBJECT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_AUDIT_OBJECT_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_COL_PRIVS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_COL_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_COL_PRIVS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_COL_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_COL_PRIVS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COL_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_ROLE_TAB_PRIVS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ROLE_TAB_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_ROLE_SYS_PRIVS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ROLE_SYS_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_ROLE_ROLE_PRIVS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ROLE_ROLE_PRIVS_ORACLE_SCHEMA_VERSION); break;
    case OB_DICTIONARY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DICTIONARY_ORACLE_SCHEMA_VERSION); break;
    case OB_DICT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DICT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TRIGGERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRIGGERS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TRIGGERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TRIGGERS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TRIGGERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TRIGGERS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_DEPENDENCIES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEPENDENCIES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_DEPENDENCIES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_DEPENDENCIES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_DEPENDENCIES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_DEPENDENCIES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_RSRC_PLANS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_RSRC_PLANS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_RSRC_PLAN_DIRECTIVES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_RSRC_PLAN_DIRECTIVES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_RSRC_GROUP_MAPPINGS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_RSRC_GROUP_MAPPINGS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_RECYCLEBIN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_RECYCLEBIN_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_RECYCLEBIN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_RECYCLEBIN_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_RSRC_CONSUMER_GROUPS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_RSRC_CONSUMER_GROUPS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_LOCATIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_LOCATIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLET_TO_LS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLET_TO_LS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLET_REPLICAS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLET_REPLICAS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLEGROUPS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLEGROUPS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLEGROUP_PARTITIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLEGROUP_PARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_DATABASES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_DATABASES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLEGROUP_TABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLEGROUP_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_ZONE_MAJOR_COMPACTION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ZONE_MAJOR_COMPACTION_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_MAJOR_COMPACTION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_MAJOR_COMPACTION_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_IND_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IND_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_IND_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_IND_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_IND_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_IND_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_JOBS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_JOBS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_JOB_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_JOB_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_TASKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_TASKS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_SET_FILES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_SET_FILES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_MODIFICATIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_MODIFICATIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_MODIFICATIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_MODIFICATIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_MODIFICATIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_MODIFICATIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_STORAGE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_STORAGE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_DELETE_POLICY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_DELETE_POLICY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_DELETE_JOBS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_DELETE_JOBS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_DELETE_TASKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_DELETE_TASKS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_RESTORE_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RESTORE_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_RESTORE_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RESTORE_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_ARCHIVE_DEST_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ARCHIVE_DEST_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_ARCHIVELOG_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ARCHIVELOG_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_ARCHIVELOG_SUMMARY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ARCHIVELOG_SUMMARY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_ARCHIVELOG_PIECE_FILES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ARCHIVELOG_PIECE_FILES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BACKUP_PARAMETER_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BACKUP_PARAMETER_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_FREEZE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_FREEZE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_REPLICA_TASKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_REPLICA_TASKS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_LS_REPLICA_TASK_PLAN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LS_REPLICA_TASK_PLAN_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SCHEDULER_WINDOWS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SCHEDULER_WINDOWS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SCHEDULER_WINDOWS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SCHEDULER_WINDOWS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_DATABASE_PRIVILEGE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_DATABASE_PRIVILEGE_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TENANTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TENANTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_POLICIES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_POLICIES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_POLICIES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_POLICIES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_POLICIES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_POLICIES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_POLICY_GROUPS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_POLICY_GROUPS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_POLICY_GROUPS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_POLICY_GROUPS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_POLICY_GROUPS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_POLICY_GROUPS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_POLICY_CONTEXTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_POLICY_CONTEXTS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_POLICY_CONTEXTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_POLICY_CONTEXTS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_POLICY_CONTEXTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_POLICY_CONTEXTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SEC_RELEVANT_COLS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SEC_RELEVANT_COLS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SEC_RELEVANT_COLS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEC_RELEVANT_COLS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SEC_RELEVANT_COLS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SEC_RELEVANT_COLS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_ARB_REPLICA_TASKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_ARB_REPLICA_TASKS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_RSRC_IO_DIRECTIVES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RSRC_IO_DIRECTIVES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_DB_LINKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DB_LINKS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_DB_LINKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_DB_LINKS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_DB_LINKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_DB_LINKS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_ACTIVE_SESSION_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_ACTIVE_SESSION_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_SNAPSHOT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SNAPSHOT_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_STATNAME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_STATNAME_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_SYSSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SYSSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_LOG_RESTORE_SOURCE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LOG_RESTORE_SOURCE_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_EXTERNAL_TABLE_FILES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_EXTERNAL_TABLE_FILES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_OB_EXTERNAL_TABLE_FILES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OB_EXTERNAL_TABLE_FILES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BALANCE_JOBS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BALANCE_JOBS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BALANCE_JOB_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BALANCE_JOB_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BALANCE_TASKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BALANCE_TASKS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_BALANCE_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_BALANCE_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TRANSFER_TASKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TRANSFER_TASKS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TRANSFER_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TRANSFER_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PX_P2P_DATAHUB_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PX_P2P_DATAHUB_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PX_P2P_DATAHUB_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PX_P2P_DATAHUB_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SQL_JOIN_FILTER_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SQL_JOIN_FILTER_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SQL_JOIN_FILTER_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_JOIN_FILTER_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLE_STAT_STALE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLE_STAT_STALE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_DBMS_LOCK_ALLOCATED_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBMS_LOCK_ALLOCATED_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_CONTROL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_CONTROL_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TENANT_EVENT_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TENANT_EVENT_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SCHEDULER_JOB_RUN_DETAILS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SCHEDULER_JOB_RUN_DETAILS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SCHEDULER_JOB_CLASSES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SCHEDULER_JOB_CLASSES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_RECOVER_TABLE_JOBS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RECOVER_TABLE_JOBS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_RECOVER_TABLE_JOB_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RECOVER_TABLE_JOB_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_IMPORT_TABLE_JOBS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_IMPORT_TABLE_JOBS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_IMPORT_TABLE_JOB_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_IMPORT_TABLE_JOB_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_IMPORT_TABLE_TASKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_IMPORT_TABLE_TASKS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_IMPORT_TABLE_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_IMPORT_TABLE_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_SYSTEM_EVENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SYSTEM_EVENT_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_EVENT_NAME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_EVENT_NAME_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_FORMAT_OUTLINES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_FORMAT_OUTLINES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_SQLSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SQLSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_SYS_TIME_MODEL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SYS_TIME_MODEL_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TRANSFER_PARTITION_TASKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TRANSFER_PARTITION_TASKS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_SQLTEXT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SQLTEXT_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_USERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_USERS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_REPLICA_TASK_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_REPLICA_TASK_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVIEW_LOGS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVIEW_LOGS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_LOGS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_LOGS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_MVIEW_LOGS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_MVIEW_LOGS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVIEWS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVIEWS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEWS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEWS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_MVIEWS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_MVIEWS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_STATS_SYS_DEFAULTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_STATS_SYS_DEFAULTS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_MVREF_STATS_SYS_DEFAULTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_MVREF_STATS_SYS_DEFAULTS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_STATS_PARAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_STATS_PARAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_MVREF_STATS_PARAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_MVREF_STATS_PARAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_RUN_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_RUN_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_MVREF_RUN_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_MVREF_RUN_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_MVREF_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_MVREF_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_CHANGE_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_CHANGE_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_MVREF_CHANGE_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_MVREF_CHANGE_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVREF_STMT_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVREF_STMT_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_MVREF_STMT_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_MVREF_STMT_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_PROXY_USERS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_PROXY_USERS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_SERVICES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SERVICES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_STORAGE_IO_USAGE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_STORAGE_IO_USAGE_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SCHEDULER_JOBS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SCHEDULER_JOBS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_CCL_RULES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_CCL_RULES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVIEW_RUNNING_JOBS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVIEW_RUNNING_JOBS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_MVIEW_DEPS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_MVIEW_DEPS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_DYNAMIC_PARTITION_TABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_DYNAMIC_PARTITION_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SOURCE_V1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SOURCE_V1_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SOURCE_V1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SOURCE_V1_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SOURCE_V1_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SOURCE_V1_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SQL_AUDIT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SQL_AUDIT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SQL_AUDIT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SQL_AUDIT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_INSTANCE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_INSTANCE_ORACLE_SCHEMA_VERSION); break;
    case OB_V_INSTANCE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_INSTANCE_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PLAN_CACHE_PLAN_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PLAN_CACHE_PLAN_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PLAN_CACHE_PLAN_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PLAN_CACHE_PLAN_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PLAN_CACHE_PLAN_EXPLAIN_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PLAN_CACHE_PLAN_EXPLAIN_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SESSION_WAIT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SESSION_WAIT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SESSION_WAIT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SESSION_WAIT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SESSION_WAIT_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SESSION_WAIT_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SESSION_WAIT_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SESSION_WAIT_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_MEMORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_MEMORY_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_MEMORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_MEMORY_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_MEMSTORE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_MEMSTORE_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_MEMSTORE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_MEMSTORE_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_MEMSTORE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_MEMSTORE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_MEMSTORE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_MEMSTORE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SESSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SESSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SESSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SESSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SYSSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SYSSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SYSSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SYSSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SYSTEM_EVENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SYSTEM_EVENT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SYSTEM_EVENT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SYSTEM_EVENT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PLAN_CACHE_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PLAN_CACHE_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PLAN_CACHE_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PLAN_CACHE_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_NLS_SESSION_PARAMETERS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_NLS_SESSION_PARAMETERS_ORACLE_SCHEMA_VERSION); break;
    case OB_NLS_INSTANCE_PARAMETERS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_NLS_INSTANCE_PARAMETERS_ORACLE_SCHEMA_VERSION); break;
    case OB_NLS_DATABASE_PARAMETERS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_NLS_DATABASE_PARAMETERS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_NLS_PARAMETERS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_NLS_PARAMETERS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_VERSION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_VERSION_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PX_WORKER_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PX_WORKER_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PX_WORKER_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PX_WORKER_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PS_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PS_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PS_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PS_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PS_ITEM_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PS_ITEM_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PS_ITEM_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PS_ITEM_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SQL_WORKAREA_ACTIVE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SQL_WORKAREA_ACTIVE_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SQL_WORKAREA_ACTIVE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_WORKAREA_ACTIVE_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SQL_WORKAREA_HISTOGRAM_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SQL_WORKAREA_HISTOGRAM_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SQL_WORKAREA_HISTOGRAM_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_WORKAREA_HISTOGRAM_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SQL_WORKAREA_MEMORY_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PLAN_CACHE_REFERENCE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PLAN_CACHE_REFERENCE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PLAN_CACHE_REFERENCE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PLAN_CACHE_REFERENCE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SQL_WORKAREA_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SQL_WORKAREA_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SQL_WORKAREA_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_WORKAREA_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SSTABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SSTABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SSTABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SSTABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SERVER_SCHEMA_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SERVER_SCHEMA_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SERVER_SCHEMA_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SERVER_SCHEMA_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SQL_PLAN_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SQL_PLAN_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SQL_PLAN_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_PLAN_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SQL_MONITOR_STATNAME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SQL_MONITOR_STATNAME_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OPEN_CURSOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OPEN_CURSOR_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OPEN_CURSOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OPEN_CURSOR_ORACLE_SCHEMA_VERSION); break;
    case OB_V_TIMEZONE_NAMES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_TIMEZONE_NAMES_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_GLOBAL_TRANSACTION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_GLOBAL_TRANSACTION_ORACLE_SCHEMA_VERSION); break;
    case OB_V_GLOBAL_TRANSACTION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_GLOBAL_TRANSACTION_ORACLE_SCHEMA_VERSION); break;
    case OB_V_RSRC_PLAN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_RSRC_PLAN_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_ENCRYPTED_TABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_ENCRYPTED_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_V_ENCRYPTED_TABLESPACES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_ENCRYPTED_TABLESPACES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_COL_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_COL_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_COL_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_COL_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_COL_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_COL_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_PART_COL_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_COL_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_PART_COL_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PART_COL_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_PART_COL_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PART_COL_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SUBPART_COL_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SUBPART_COL_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SUBPART_COL_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SUBPART_COL_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SUBPART_COL_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SUBPART_COL_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_HISTOGRAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_HISTOGRAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_HISTOGRAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_HISTOGRAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_HISTOGRAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_HISTOGRAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_PART_HISTOGRAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_HISTOGRAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_PART_HISTOGRAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PART_HISTOGRAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_PART_HISTOGRAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PART_HISTOGRAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_SUBPART_HISTOGRAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SUBPART_HISTOGRAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SUBPART_HISTOGRAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SUBPART_HISTOGRAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_SUBPART_HISTOGRAMS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_SUBPART_HISTOGRAMS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_JOBS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_JOBS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_JOBS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_JOBS_RUNNING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_JOBS_RUNNING_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_DIRECTORIES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DIRECTORIES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_DIRECTORIES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_DIRECTORIES_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_TENANT_MEMORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TENANT_MEMORY_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_TENANT_MEMORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TENANT_MEMORY_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PX_TARGET_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PX_TARGET_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PX_TARGET_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PX_TARGET_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TAB_STATS_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TAB_STATS_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TAB_STATS_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TAB_STATS_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TAB_STATS_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TAB_STATS_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_DBLINK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_DBLINK_ORACLE_SCHEMA_VERSION); break;
    case OB_V_DBLINK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_DBLINK_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SCHEDULER_JOBS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SCHEDULER_JOBS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SCHEDULER_PROGRAM_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SCHEDULER_PROGRAM_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_CONTEXT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_CONTEXT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_GLOBALCONTEXT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_GLOBALCONTEXT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_UNITS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_UNITS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_UNITS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_UNITS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PARAMETERS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PARAMETERS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PARAMETERS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PARAMETERS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PROCESSLIST_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PROCESSLIST_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PROCESSLIST_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PROCESSLIST_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_KVCACHE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_KVCACHE_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_KVCACHE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_KVCACHE_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_TRANSACTION_PARTICIPANTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TRANSACTION_PARTICIPANTS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_TRANSACTION_PARTICIPANTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TRANSACTION_PARTICIPANTS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_COMPACTION_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_COMPACTION_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_COMPACTION_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_COMPACTION_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_TABLET_COMPACTION_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TABLET_COMPACTION_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_TABLET_COMPACTION_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TABLET_COMPACTION_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_TABLET_COMPACTION_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TABLET_COMPACTION_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_TABLET_COMPACTION_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TABLET_COMPACTION_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_COMPACTION_DIAGNOSE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_COMPACTION_DIAGNOSE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_COMPACTION_DIAGNOSE_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_COMPACTION_DIAGNOSE_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_COMPACTION_SUGGESTIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_COMPACTION_SUGGESTIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_COMPACTION_SUGGESTIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_COMPACTION_SUGGESTIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_DTL_INTERM_RESULT_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_DTL_INTERM_RESULT_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_DTL_INTERM_RESULT_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_DTL_INTERM_RESULT_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SQL_PLAN_BASELINES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SQL_PLAN_BASELINES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_SQL_MANAGEMENT_CONFIG_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_SQL_MANAGEMENT_CONFIG_ORACLE_SCHEMA_VERSION); break;
    case OB_V_EVENT_NAME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_EVENT_NAME_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_ACTIVE_SESSION_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_ACTIVE_SESSION_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_V_ACTIVE_SESSION_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_ACTIVE_SESSION_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_DML_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_DML_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_DML_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_DML_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_OUTLINE_CONCURRENT_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_OUTLINE_CONCURRENT_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_OUTLINES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_OUTLINES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_CONCURRENT_LIMIT_SQL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_CONCURRENT_LIMIT_SQL_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_DEADLOCK_EVENT_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_DEADLOCK_EVENT_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_LOG_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_LOG_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_LOG_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LOG_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_GLOBAL_TRANSACTION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_GLOBAL_TRANSACTION_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_GLOBAL_TRANSACTION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_GLOBAL_TRANSACTION_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLE_LOCATIONS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLE_LOCATIONS_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TRIGGER_ORDERING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRIGGER_ORDERING_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_TRIGGER_ORDERING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_TRIGGER_ORDERING_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_TRIGGER_ORDERING_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_TRIGGER_ORDERING_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_TRANSACTION_SCHEDULERS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TRANSACTION_SCHEDULERS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_TRANSACTION_SCHEDULERS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TRANSACTION_SCHEDULERS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_USER_DEFINED_RULES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_USER_DEFINED_RULES_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SQL_PLAN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SQL_PLAN_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SQL_PLAN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SQL_PLAN_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_ARCHIVE_DEST_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_ARCHIVE_DEST_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_LOCKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_LOCKS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_LOCKS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LOCKS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_ACCESS_POINT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_ACCESS_POINT_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_DATA_DICTIONARY_IN_LOG_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_DATA_DICTIONARY_IN_LOG_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_OPT_STAT_GATHER_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_OPT_STAT_GATHER_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_OPT_STAT_GATHER_MONITOR_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_OPT_STAT_GATHER_MONITOR_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_SESSION_LONGOPS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_SESSION_LONGOPS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_SESSION_LONGOPS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_SESSION_LONGOPS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_THREAD_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_THREAD_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_THREAD_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_THREAD_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_ARBITRATION_MEMBER_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_ARBITRATION_MEMBER_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_ARBITRATION_MEMBER_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_ARBITRATION_MEMBER_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_ARBITRATION_SERVICE_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_ARBITRATION_SERVICE_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_ARBITRATION_SERVICE_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_ARBITRATION_SERVICE_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_TIMESTAMP_SERVICE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TIMESTAMP_SERVICE_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_LS_LOG_RESTORE_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LS_LOG_RESTORE_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_FLT_TRACE_CONFIG_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_FLT_TRACE_CONFIG_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SESSION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SESSION_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SESSION_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SESSION_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_PL_CACHE_OBJECT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_PL_CACHE_OBJECT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_PL_CACHE_OBJECT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_PL_CACHE_OBJECT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_CGROUP_CONFIG_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_CGROUP_CONFIG_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_CGROUP_CONFIG_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_CGROUP_CONFIG_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SQLSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SQLSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SQLSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SQLSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SESS_TIME_MODEL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SESS_TIME_MODEL_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SESS_TIME_MODEL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SESS_TIME_MODEL_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SYS_TIME_MODEL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SYS_TIME_MODEL_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SYS_TIME_MODEL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SYS_TIME_MODEL_ORACLE_SCHEMA_VERSION); break;
    case OB_V_STATNAME_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_STATNAME_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_AUX_STATISTICS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_AUX_STATISTICS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_SYS_VARIABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SYS_VARIABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_ACTIVE_SESSION_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_ACTIVE_SESSION_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_ACTIVE_SESSION_HISTORY_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_ACTIVE_SESSION_HISTORY_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_INDEX_USAGE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_INDEX_USAGE_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_LS_SNAPSHOTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_LS_SNAPSHOTS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_LS_SNAPSHOTS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LS_SNAPSHOTS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SHARED_STORAGE_QUOTA_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SHARED_STORAGE_QUOTA_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SHARED_STORAGE_QUOTA_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SHARED_STORAGE_QUOTA_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SESSION_PS_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SESSION_PS_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SESSION_PS_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SESSION_PS_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_TRACEPOINT_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TRACEPOINT_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_TRACEPOINT_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TRACEPOINT_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_RSRC_DIRECTIVES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_RSRC_DIRECTIVES_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_TENANT_RESOURCE_LIMIT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TENANT_RESOURCE_LIMIT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_TENANT_RESOURCE_LIMIT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TENANT_RESOURCE_LIMIT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_GROUP_IO_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_GROUP_IO_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_GROUP_IO_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_GROUP_IO_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_NIC_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_NIC_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_NIC_INFO_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_NIC_INFO_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_QUERY_RESPONSE_TIME_HISTOGRAM_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_QUERY_RESPONSE_TIME_HISTOGRAM_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_QUERY_RESPONSE_TIME_HISTOGRAM_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_QUERY_RESPONSE_TIME_HISTOGRAM_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_SPATIAL_COLUMNS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SPATIAL_COLUMNS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TABLE_SPACE_USAGE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TABLE_SPACE_USAGE_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_LOG_TRANSPORT_DEST_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_LOG_TRANSPORT_DEST_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_LOG_TRANSPORT_DEST_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_LOG_TRANSPORT_DEST_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SS_LOCAL_CACHE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SS_LOCAL_CACHE_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SS_LOCAL_CACHE_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SS_LOCAL_CACHE_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_PLSQL_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PLSQL_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_PLSQL_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PLSQL_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_PLSQL_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PLSQL_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_PLSQL_COLL_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PLSQL_COLL_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_PLSQL_COLL_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PLSQL_COLL_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_PLSQL_COLL_TYPES_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PLSQL_COLL_TYPES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_PLSQL_TYPE_ATTRS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PLSQL_TYPE_ATTRS_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_PLSQL_TYPE_ATTRS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_PLSQL_TYPE_ATTRS_ORACLE_SCHEMA_VERSION); break;
    case OB_USER_PLSQL_TYPE_ATTRS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_USER_PLSQL_TYPE_ATTRS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_RES_MGR_SYSSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_RES_MGR_SYSSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_RES_MGR_SYSSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_RES_MGR_SYSSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_SQL_PLAN_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_SQL_PLAN_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_WR_RES_MGR_SYSSTAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_WR_RES_MGR_SYSSTAT_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_SPM_EVO_RESULT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_SPM_EVO_RESULT_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_FUNCTION_IO_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_FUNCTION_IO_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_FUNCTION_IO_STAT_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_FUNCTION_IO_STAT_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_TEMP_FILES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_TEMP_FILES_ORACLE_SCHEMA_VERSION); break;
    case OB_DBA_OB_CS_REPLICA_STATS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_DBA_OB_CS_REPLICA_STATS_ORACLE_SCHEMA_VERSION); break;
    case OB_GV_OB_SQL_CCL_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_GV_OB_SQL_CCL_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_SQL_CCL_STATUS_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_SQL_CCL_STATUS_ORACLE_SCHEMA_VERSION); break;
    case OB_V_OB_DYNAMIC_PARTITION_TABLES_ORA_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_V_OB_DYNAMIC_PARTITION_TABLES_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_IDX_DATA_TABLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_3_IDX_DATA_TABLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_IDX_DB_TB_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_3_IDX_DB_TB_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_IDX_TB_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_3_IDX_TB_NAME_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_IDX_TB_COLUMN_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_4_IDX_TB_COLUMN_NAME_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_IDX_COLUMN_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_4_IDX_COLUMN_NAME_SCHEMA_VERSION); break;
    case OB_ALL_DDL_OPERATION_IDX_DDL_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_5_IDX_DDL_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_114_IDX_DATA_TABLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_350_IDX_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_315_IDX_STATUS_SCHEMA_VERSION); break;
    case OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_319_IDX_TASK_KEY_SCHEMA_VERSION); break;
    case OB_ALL_USER_IDX_UR_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_102_IDX_UR_NAME_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_IDX_DB_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_104_IDX_DB_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TABLEGROUP_IDX_TG_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_106_IDX_TG_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_HISTORY_IDX_TENANT_DELETED_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_109_IDX_TENANT_DELETED_SCHEMA_VERSION); break;
    case OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_MODULE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_140_IDX_RS_MODULE_SCHEMA_VERSION); break;
    case OB_ALL_ROOTSERVICE_EVENT_HISTORY_IDX_RS_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_140_IDX_RS_EVENT_SCHEMA_VERSION); break;
    case OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_DB_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_145_IDX_RECYCLEBIN_DB_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_PART_IDX_PART_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_146_IDX_PART_NAME_SCHEMA_VERSION); break;
    case OB_ALL_SUB_PART_IDX_SUB_PART_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_148_IDX_SUB_PART_NAME_SCHEMA_VERSION); break;
    case OB_ALL_DEF_SUB_PART_IDX_DEF_SUB_PART_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_152_IDX_DEF_SUB_PART_NAME_SCHEMA_VERSION); break;
    case OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_MODULE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_154_IDX_SERVER_MODULE_SCHEMA_VERSION); break;
    case OB_ALL_SERVER_EVENT_HISTORY_IDX_SERVER_EVENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_154_IDX_SERVER_EVENT_SCHEMA_VERSION); break;
    case OB_ALL_ROOTSERVICE_JOB_IDX_RS_JOB_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_155_IDX_RS_JOB_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_IDX_FK_CHILD_TID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_166_IDX_FK_CHILD_TID_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_IDX_FK_PARENT_TID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_166_IDX_FK_PARENT_TID_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_IDX_FK_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_166_IDX_FK_NAME_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_CHILD_TID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_167_IDX_FK_HIS_CHILD_TID_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_PARENT_TID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_167_IDX_FK_HIS_PARENT_TID_SCHEMA_VERSION); break;
    case OB_ALL_SYNONYM_IDX_DB_SYNONYM_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_180_IDX_DB_SYNONYM_NAME_SCHEMA_VERSION); break;
    case OB_ALL_SYNONYM_IDX_SYNONYM_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_180_IDX_SYNONYM_NAME_SCHEMA_VERSION); break;
    case OB_ALL_DDL_CHECKSUM_IDX_DDL_CHECKSUM_TASK_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_188_IDX_DDL_CHECKSUM_TASK_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_IDX_DB_ROUTINE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_189_IDX_DB_ROUTINE_NAME_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_IDX_ROUTINE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_189_IDX_ROUTINE_NAME_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_IDX_ROUTINE_PKG_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_189_IDX_ROUTINE_PKG_ID_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PARAM_IDX_ROUTINE_PARAM_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_191_IDX_ROUTINE_PARAM_NAME_SCHEMA_VERSION); break;
    case OB_ALL_PACKAGE_IDX_DB_PKG_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_196_IDX_DB_PKG_NAME_SCHEMA_VERSION); break;
    case OB_ALL_PACKAGE_IDX_PKG_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_196_IDX_PKG_NAME_SCHEMA_VERSION); break;
    case OB_ALL_ACQUIRED_SNAPSHOT_IDX_SNAPSHOT_TABLET_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_202_IDX_SNAPSHOT_TABLET_SCHEMA_VERSION); break;
    case OB_ALL_CONSTRAINT_IDX_CST_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_206_IDX_CST_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_IDX_DB_TYPE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_220_IDX_DB_TYPE_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_IDX_TYPE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_220_IDX_TYPE_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_ATTR_IDX_TYPE_ATTR_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_222_IDX_TYPE_ATTR_NAME_SCHEMA_VERSION); break;
    case OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_224_IDX_COLL_NAME_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_DBLINK_IDX_OWNER_DBLINK_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_232_IDX_OWNER_DBLINK_NAME_SCHEMA_VERSION); break;
    case OB_ALL_DBLINK_IDX_DBLINK_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_232_IDX_DBLINK_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_235_IDX_GRANTEE_ROLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_IDX_GRANTEE_HIS_ROLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_236_IDX_GRANTEE_HIS_ROLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_KEYSTORE_IDX_KEYSTORE_MASTER_KEY_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_237_IDX_KEYSTORE_MASTER_KEY_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_KEYSTORE_HISTORY_IDX_KEYSTORE_HIS_MASTER_KEY_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_238_IDX_KEYSTORE_HIS_MASTER_KEY_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_239_IDX_OLS_POLICY_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_COL_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_239_IDX_OLS_POLICY_COL_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_COMPONENT_IDX_OLS_COM_POLICY_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_241_IDX_OLS_COM_POLICY_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_POLICY_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_243_IDX_OLS_LAB_POLICY_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TAG_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_243_IDX_OLS_LAB_TAG_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_243_IDX_OLS_LAB_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_UID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_245_IDX_OLS_LEVEL_UID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_POLICY_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_245_IDX_OLS_LEVEL_POLICY_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_250_IDX_PROFILE_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SECURITY_AUDIT_IDX_AUDIT_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_252_IDX_AUDIT_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_BASE_OBJ_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_254_IDX_TRIGGER_BASE_OBJ_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TRIGGER_IDX_DB_TRIGGER_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_254_IDX_DB_TRIGGER_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_254_IDX_TRIGGER_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TRIGGER_HISTORY_IDX_TRIGGER_HIS_BASE_OBJ_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_255_IDX_TRIGGER_HIS_BASE_OBJ_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTOR_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_262_IDX_OBJAUTH_GRANTOR_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTEE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_262_IDX_OBJAUTH_GRANTEE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_DB_OBJ_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_283_IDX_OBJ_TYPE_DB_OBJ_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_OBJ_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_283_IDX_OBJ_TYPE_OBJ_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_296_IDX_XA_TRANS_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DEPENDENCY_IDX_DEPENDENCY_REF_OBJ_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_297_IDX_DEPENDENCY_REF_OBJ_SCHEMA_VERSION); break;
    case OB_ALL_DDL_ERROR_MESSAGE_IDX_DDL_ERROR_OBJECT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_308_IDX_DDL_ERROR_OBJECT_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_332_IDX_TABLE_STAT_HIS_SAVTIME_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_STAT_HISTORY_IDX_COLUMN_STAT_HIS_SAVTIME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_333_IDX_COLUMN_STAT_HIS_SAVTIME_SCHEMA_VERSION); break;
    case OB_ALL_HISTOGRAM_STAT_HISTORY_IDX_HISTOGRAM_STAT_HIS_SAVTIME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_334_IDX_HISTOGRAM_STAT_HIS_SAVTIME_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_343_IDX_TABLET_TO_LS_ID_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_343_IDX_TABLET_TO_TABLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_PENDING_TRANSACTION_IDX_PENDING_TX_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_375_IDX_PENDING_TX_ID_SCHEMA_VERSION); break;
    case OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_381_IDX_CTX_NAMESPACE_SCHEMA_VERSION); break;
    case OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_SQL_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_397_IDX_SPM_ITEM_SQL_ID_SCHEMA_VERSION); break;
    case OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_VALUE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_397_IDX_SPM_ITEM_VALUE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DIRECTORY_IDX_DIRECTORY_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_326_IDX_DIRECTORY_NAME_SCHEMA_VERSION); break;
    case OB_ALL_JOB_IDX_JOB_POWNER_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_324_IDX_JOB_POWNER_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_DB_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_213_IDX_SEQ_OBJ_DB_NAME_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_213_IDX_SEQ_OBJ_NAME_SCHEMA_VERSION); break;
    case OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_ORI_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_145_IDX_RECYCLEBIN_ORI_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_DB_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_110_IDX_TB_PRIV_DB_NAME_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_TB_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_110_IDX_TB_PRIV_TB_NAME_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_PRIVILEGE_IDX_DB_PRIV_DB_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_112_IDX_DB_PRIV_DB_NAME_SCHEMA_VERSION); break;
    case OB_ALL_RLS_POLICY_IDX_RLS_POLICY_TABLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_433_IDX_RLS_POLICY_TABLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_RLS_POLICY_IDX_RLS_POLICY_GROUP_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_433_IDX_RLS_POLICY_GROUP_ID_SCHEMA_VERSION); break;
    case OB_ALL_RLS_POLICY_HISTORY_IDX_RLS_POLICY_HIS_TABLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_434_IDX_RLS_POLICY_HIS_TABLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_437_IDX_RLS_GROUP_TABLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_RLS_GROUP_HISTORY_IDX_RLS_GROUP_HIS_TABLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_438_IDX_RLS_GROUP_HIS_TABLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_439_IDX_RLS_CONTEXT_TABLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_RLS_CONTEXT_HISTORY_IDX_RLS_CONTEXT_HIS_TABLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_440_IDX_RLS_CONTEXT_HIS_TABLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_IDX_TENANT_SNAPSHOT_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_460_IDX_TENANT_SNAPSHOT_NAME_SCHEMA_VERSION); break;
    case OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_471_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_SCHEMA_VERSION); break;
    case OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_471_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_REORGANIZE_HISTORY_IDX_TABLET_HIS_TABLE_ID_SRC_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_482_IDX_TABLET_HIS_TABLE_ID_SRC_SCHEMA_VERSION); break;
    case OB_ALL_KV_TTL_TASK_IDX_KV_TTL_TASK_TABLE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_410_IDX_KV_TTL_TASK_TABLE_ID_SCHEMA_VERSION); break;
    case OB_ALL_KV_TTL_TASK_HISTORY_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_411_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_RUN_STATS_IDX_MVIEW_REFRESH_RUN_STATS_NUM_MVS_CURRENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_467_IDX_MVIEW_REFRESH_RUN_STATS_NUM_MVS_CURRENT_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_END_TIME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_468_IDX_MVIEW_REFRESH_STATS_END_TIME_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_MVIEW_END_TIME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_468_IDX_MVIEW_REFRESH_STATS_MVIEW_END_TIME_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_PARTITION_TASK_IDX_TRANSFER_PARTITION_KEY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_498_IDX_TRANSFER_PARTITION_KEY_SCHEMA_VERSION); break;
    case OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_IDX_CLIENT_TO_SERVER_SESSION_INFO_CLIENT_SESSION_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_497_IDX_CLIENT_TO_SERVER_SESSION_INFO_CLIENT_SESSION_ID_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_PRIVILEGE_IDX_COLUMN_PRIVILEGE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_505_IDX_COLUMN_PRIVILEGE_NAME_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_INFO_IDX_USER_PROXY_INFO_PROXY_USER_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_512_IDX_USER_PROXY_INFO_PROXY_USER_ID_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_INFO_HISTORY_IDX_USER_PROXY_INFO_PROXY_USER_ID_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_513_IDX_USER_PROXY_INFO_PROXY_USER_ID_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_IDX_USER_PROXY_ROLE_INFO_PROXY_USER_ID_HISTORY_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_515_IDX_USER_PROXY_ROLE_INFO_PROXY_USER_ID_HISTORY_SCHEMA_VERSION); break;
    case OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_519_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_SCHEMA_VERSION); break;
    case OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_519_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_SCHEMA_VERSION); break;
    case OB_ALL_PKG_TYPE_IDX_PKG_DB_TYPE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_522_IDX_PKG_DB_TYPE_NAME_SCHEMA_VERSION); break;
    case OB_ALL_PKG_TYPE_IDX_PKG_TYPE_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_522_IDX_PKG_TYPE_NAME_SCHEMA_VERSION); break;
    case OB_ALL_PKG_TYPE_ATTR_IDX_PKG_TYPE_ATTR_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_523_IDX_PKG_TYPE_ATTR_NAME_SCHEMA_VERSION); break;
    case OB_ALL_PKG_TYPE_ATTR_IDX_PKG_TYPE_ATTR_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_523_IDX_PKG_TYPE_ATTR_ID_SCHEMA_VERSION); break;
    case OB_ALL_PKG_COLL_TYPE_IDX_PKG_COLL_NAME_TYPE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_524_IDX_PKG_COLL_NAME_TYPE_SCHEMA_VERSION); break;
    case OB_ALL_PKG_COLL_TYPE_IDX_PKG_COLL_NAME_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_524_IDX_PKG_COLL_NAME_ID_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_IDX_CATALOG_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_537_IDX_CATALOG_NAME_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_PRIVILEGE_IDX_CATALOG_PRIV_CATALOG_NAME_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_539_IDX_CATALOG_PRIV_CATALOG_NAME_SCHEMA_VERSION); break;
    case OB_ALL_CCL_RULE_IDX_CCL_RULE_ID_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_547_IDX_CCL_RULE_ID_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_REORGANIZE_HISTORY_IDX_TABLET_HIS_TABLE_ID_DEST_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_482_IDX_TABLET_HIS_TABLE_ID_DEST_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_DATA_TABLE_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15120_IDX_DATA_TABLE_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_DB_TB_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15120_IDX_DB_TB_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_TB_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15120_IDX_TB_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_IDX_TB_COLUMN_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15121_IDX_TB_COLUMN_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_IDX_COLUMN_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15121_IDX_COLUMN_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_IDX_UR_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15129_IDX_UR_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_IDX_DB_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15122_IDX_DB_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_IDX_TG_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15137_IDX_TG_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_IDX_RECYCLEBIN_DB_TYPE_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15135_IDX_RECYCLEBIN_DB_TYPE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_IDX_PART_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15124_IDX_PART_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_IDX_SUB_PART_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15125_IDX_SUB_PART_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_IDX_DEF_SUB_PART_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15163_IDX_DEF_SUB_PART_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_CHILD_TID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15131_IDX_FK_CHILD_TID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_PARENT_TID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15131_IDX_FK_PARENT_TID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15131_IDX_FK_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_IDX_DB_SYNONYM_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15130_IDX_DB_SYNONYM_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_IDX_SYNONYM_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15130_IDX_SYNONYM_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_DB_ROUTINE_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15136_IDX_DB_ROUTINE_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_ROUTINE_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15136_IDX_ROUTINE_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_ROUTINE_PKG_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15136_IDX_ROUTINE_PKG_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_IDX_ROUTINE_PARAM_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15143_IDX_ROUTINE_PARAM_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_IDX_DB_PKG_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15126_IDX_DB_PKG_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_IDX_PKG_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15126_IDX_PKG_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_IDX_CST_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15139_IDX_CST_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_IDX_DB_TYPE_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15140_IDX_DB_TYPE_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_IDX_TYPE_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15140_IDX_TYPE_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_IDX_TYPE_ATTR_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15141_IDX_TYPE_ATTR_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_IDX_COLL_NAME_TYPE_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15142_IDX_COLL_NAME_TYPE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_IDX_OWNER_DBLINK_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15165_IDX_OWNER_DBLINK_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_IDX_DBLINK_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15165_IDX_DBLINK_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_IDX_GRANTEE_ROLE_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15152_IDX_GRANTEE_ROLE_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_IDX_KEYSTORE_MASTER_KEY_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15145_IDX_KEYSTORE_MASTER_KEY_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_IDX_OLS_POLICY_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15146_IDX_OLS_POLICY_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_IDX_OLS_POLICY_COL_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15146_IDX_OLS_POLICY_COL_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_REAL_AGENT_ORA_IDX_OLS_COM_POLICY_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15147_IDX_OLS_COM_POLICY_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_POLICY_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15148_IDX_OLS_LAB_POLICY_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_TAG_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15148_IDX_OLS_LAB_TAG_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15148_IDX_OLS_LAB_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_IDX_OLS_LEVEL_UID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15149_IDX_OLS_LEVEL_UID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_IDX_OLS_LEVEL_POLICY_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15149_IDX_OLS_LEVEL_POLICY_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_IDX_PROFILE_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15151_IDX_PROFILE_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_IDX_AUDIT_TYPE_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15154_IDX_AUDIT_TYPE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_TRIGGER_BASE_OBJ_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15156_IDX_TRIGGER_BASE_OBJ_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_DB_TRIGGER_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15156_IDX_DB_TRIGGER_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_TRIGGER_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15156_IDX_TRIGGER_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_IDX_OBJAUTH_GRANTOR_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15160_IDX_OBJAUTH_GRANTOR_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_IDX_OBJAUTH_GRANTEE_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15160_IDX_OBJAUTH_GRANTEE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_IDX_OBJ_TYPE_DB_OBJ_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15164_IDX_OBJ_TYPE_DB_OBJ_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_IDX_OBJ_TYPE_OBJ_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15164_IDX_OBJ_TYPE_OBJ_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_IDX_DEPENDENCY_REF_OBJ_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15168_IDX_DEPENDENCY_REF_OBJ_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_REAL_AGENT_ORA_IDX_TABLE_STAT_HIS_SAVTIME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15194_IDX_TABLE_STAT_HIS_SAVTIME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_REAL_AGENT_ORA_IDX_COLUMN_STAT_HIS_SAVTIME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15195_IDX_COLUMN_STAT_HIS_SAVTIME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_REAL_AGENT_ORA_IDX_HISTOGRAM_STAT_HIS_SAVTIME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15196_IDX_HISTOGRAM_STAT_HIS_SAVTIME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_IDX_TABLET_TO_LS_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15215_IDX_TABLET_TO_LS_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_IDX_TABLET_TO_TABLE_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15215_IDX_TABLET_TO_TABLE_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CONTEXT_REAL_AGENT_ORA_IDX_CTX_NAMESPACE_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15204_IDX_CTX_NAMESPACE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_IDX_SPM_ITEM_SQL_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15233_IDX_SPM_ITEM_SQL_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_IDX_SPM_ITEM_VALUE_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15233_IDX_SPM_ITEM_VALUE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT_ORA_IDX_DIRECTORY_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15185_IDX_DIRECTORY_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_JOB_REAL_AGENT_ORA_IDX_JOB_POWNER_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15183_IDX_JOB_POWNER_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_IDX_SEQ_OBJ_DB_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15128_IDX_SEQ_OBJ_DB_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_IDX_SEQ_OBJ_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15128_IDX_SEQ_OBJ_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_IDX_RECYCLEBIN_ORI_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15135_IDX_RECYCLEBIN_ORI_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_IDX_TB_PRIV_DB_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15153_IDX_TB_PRIV_DB_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_IDX_TB_PRIV_TB_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15153_IDX_TB_PRIV_TB_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_REAL_AGENT_ORA_IDX_DB_PRIV_DB_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15275_IDX_DB_PRIV_DB_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_IDX_RLS_POLICY_TABLE_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15276_IDX_RLS_POLICY_TABLE_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_IDX_RLS_POLICY_GROUP_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15276_IDX_RLS_POLICY_GROUP_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_GROUP_REAL_AGENT_ORA_IDX_RLS_GROUP_TABLE_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15278_IDX_RLS_GROUP_TABLE_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT_ORA_IDX_RLS_CONTEXT_TABLE_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15279_IDX_RLS_CONTEXT_TABLE_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15397_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15397_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT_ORA_IDX_USER_PROXY_INFO_PROXY_USER_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15446_IDX_USER_PROXY_INFO_PROXY_USER_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT_ORA_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15458_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT_ORA_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15458_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_TYPE_REAL_AGENT_ORA_IDX_PKG_DB_TYPE_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15471_IDX_PKG_DB_TYPE_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_TYPE_REAL_AGENT_ORA_IDX_PKG_TYPE_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15471_IDX_PKG_TYPE_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_TYPE_ATTR_REAL_AGENT_ORA_IDX_PKG_TYPE_ATTR_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15472_IDX_PKG_TYPE_ATTR_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_TYPE_ATTR_REAL_AGENT_ORA_IDX_PKG_TYPE_ATTR_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15472_IDX_PKG_TYPE_ATTR_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_COLL_TYPE_REAL_AGENT_ORA_IDX_PKG_COLL_NAME_TYPE_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15473_IDX_PKG_COLL_NAME_TYPE_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_PKG_COLL_TYPE_REAL_AGENT_ORA_IDX_PKG_COLL_NAME_ID_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15473_IDX_PKG_COLL_NAME_ID_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CATALOG_REAL_AGENT_ORA_IDX_CATALOG_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15494_IDX_CATALOG_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_VIRTUAL_CATALOG_PRIVILEGE_REAL_AGENT_ORA_IDX_CATALOG_PRIV_CATALOG_NAME_REAL_AGENT_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_IDX_15495_IDX_CATALOG_PRIV_CATALOG_NAME_REAL_AGENT_ORACLE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DDL_OPERATION_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_OPERATION_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_USER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_USER_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLEGROUP_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLEGROUP_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLEGROUP_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLEGROUP_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_PRIVILEGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_PRIVILEGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_PRIVILEGE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_PRIVILEGE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_PRIVILEGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_PRIVILEGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_PRIVILEGE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_PRIVILEGE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SERVER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SYS_PARAMETER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_PARAMETER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_TENANT_PARAMETER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_PARAMETER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SYS_VARIABLE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_VARIABLE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SYS_STAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_STAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_UNIT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_UNIT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_UNIT_CONFIG_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_UNIT_CONFIG_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RESOURCE_POOL_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESOURCE_POOL_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CHARSET_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CHARSET_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLLATION_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLLATION_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_HELP_TOPIC_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_TOPIC_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_HELP_CATEGORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_CATEGORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_HELP_KEYWORD_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_KEYWORD_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_HELP_RELATION_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_RELATION_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DUMMY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DUMMY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ROOTSERVICE_EVENT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROOTSERVICE_EVENT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PRIVILEGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PRIVILEGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_OUTLINE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OUTLINE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_OUTLINE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OUTLINE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RECYCLEBIN_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RECYCLEBIN_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PART_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PART_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SUB_PART_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SUB_PART_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SUB_PART_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SUB_PART_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PART_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PART_INFO_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_INFO_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DEF_SUB_PART_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEF_SUB_PART_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DEF_SUB_PART_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEF_SUB_PART_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SERVER_EVENT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVER_EVENT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ROOTSERVICE_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROOTSERVICE_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SYS_VARIABLE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_VARIABLE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DDL_ID_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_ID_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_COLUMN_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_COLUMN_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SYNONYM_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYNONYM_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SYNONYM_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYNONYM_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_AUTO_INCREMENT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUTO_INCREMENT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DDL_CHECKSUM_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_CHECKSUM_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PARAM_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PARAM_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PARAM_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PARAM_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PACKAGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PACKAGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PACKAGE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PACKAGE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ACQUIRED_SNAPSHOT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ACQUIRED_SNAPSHOT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CONSTRAINT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONSTRAINT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CONSTRAINT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONSTRAINT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ORI_SCHEMA_VERSION_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ORI_SCHEMA_VERSION_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_FUNC_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FUNC_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_FUNC_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FUNC_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TEMP_TABLE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TEMP_TABLE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_OBJECT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEQUENCE_OBJECT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_OBJECT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEQUENCE_OBJECT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_VALUE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEQUENCE_VALUE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_FREEZE_SCHEMA_VERSION_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FREEZE_SCHEMA_VERSION_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_ATTR_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_ATTR_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_ATTR_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_ATTR_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLL_TYPE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLL_TYPE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLL_TYPE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLL_TYPE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_WEAK_READ_SERVICE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_WEAK_READ_SERVICE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DBLINK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DBLINK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DBLINK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DBLINK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ROLE_GRANTEE_MAP_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_ROLE_GRANTEE_MAP_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_KEYSTORE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_KEYSTORE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_KEYSTORE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_KEYSTORE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_POLICY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_POLICY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_POLICY_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_POLICY_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_COMPONENT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_COMPONENT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_COMPONENT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_COMPONENT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_LABEL_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_LABEL_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_LABEL_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_LABEL_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_USER_LEVEL_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_USER_LEVEL_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TABLESPACE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TABLESPACE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TABLESPACE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TABLESPACE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_PROFILE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_PROFILE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_PROFILE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_PROFILE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SECURITY_AUDIT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SECURITY_AUDIT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TRIGGER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TRIGGER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TRIGGER_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TRIGGER_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SEED_PARAMETER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEED_PARAMETER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SECURITY_AUDIT_RECORD_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SECURITY_AUDIT_RECORD_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SYSAUTH_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SYSAUTH_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SYSAUTH_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SYSAUTH_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJAUTH_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJAUTH_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJAUTH_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJAUTH_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ERROR_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_ERROR_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_PROGRESS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_PROGRESS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJECT_TYPE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJECT_TYPE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJECT_TYPE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJECT_TYPE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_NAME_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_NAME_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_TRANSITION_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_TRANSITION_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_CONSTRAINT_COLUMN_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_CONSTRAINT_COLUMN_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DEPENDENCY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_DEPENDENCY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_PLAN_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_PLAN_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_DIRECTIVE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_DIRECTIVE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_MAPPING_RULE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_MAPPING_RULE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DDL_ERROR_MESSAGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_ERROR_MESSAGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SPACE_USAGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPACE_USAGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_SET_FILES_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_SET_FILES_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_CONSUMER_GROUP_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_CONSUMER_GROUP_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DDL_TASK_STATUS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_TASK_STATUS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_REGION_NETWORK_BANDWIDTH_LIMIT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_REGION_NETWORK_BANDWIDTH_LIMIT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_USAGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_USAGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_JOB_LOG_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_JOB_LOG_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DIRECTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_DIRECTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DIRECTORY_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_DIRECTORY_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_STAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_STAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_STAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_STAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_HISTOGRAM_STAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_HISTOGRAM_STAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MONITOR_MODIFIED_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MONITOR_MODIFIED_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_STAT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_STAT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_STAT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_STAT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_HISTOGRAM_STAT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_HISTOGRAM_STAT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_OPTSTAT_GLOBAL_PREFS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OPTSTAT_GLOBAL_PREFS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_OPTSTAT_USER_PREFS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OPTSTAT_USER_PREFS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_META_TABLE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_META_TABLE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_TO_LS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_TO_LS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_META_TABLE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_META_TABLE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_STATUS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_STATUS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DAM_LAST_ARCH_TS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DAM_LAST_ARCH_TS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DAM_CLEANUP_JOBS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DAM_CLEANUP_JOBS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_TO_TABLE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_TO_TABLE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_RECOVERY_STAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_RECOVERY_STAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_CHECKSUM_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_CHECKSUM_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_REPLICA_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_REPLICA_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PENDING_TRANSACTION_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PENDING_TRANSACTION_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_PROGRAM_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_PROGRAM_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CONTEXT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONTEXT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CONTEXT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONTEXT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_MERGE_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_MERGE_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MERGE_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MERGE_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_FREEZE_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FREEZE_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DISK_IO_CALIBRATION_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DISK_IO_CALIBRATION_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PLAN_BASELINE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PLAN_BASELINE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PLAN_BASELINE_ITEM_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PLAN_BASELINE_ITEM_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SPM_CONFIG_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPM_CONFIG_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_PARAMETER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_PARAMETER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_KV_TTL_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_KV_TTL_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SERVICE_EPOCH_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVICE_EPOCH_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SPATIAL_REFERENCE_SYSTEMS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPATIAL_REFERENCE_SYSTEMS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_MAPPING_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_MAPPING_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_JOB_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_JOB_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ARBITRATION_SERVICE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ARBITRATION_SERVICE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DATA_DICTIONARY_IN_LOG_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATA_DICTIONARY_IN_LOG_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RLS_POLICY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_POLICY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RLS_POLICY_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_POLICY_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RLS_SECURITY_COLUMN_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_SECURITY_COLUMN_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RLS_SECURITY_COLUMN_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_SECURITY_COLUMN_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RLS_GROUP_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_GROUP_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RLS_GROUP_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_GROUP_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RLS_CONTEXT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_CONTEXT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RLS_CONTEXT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_CONTEXT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RLS_ATTRIBUTE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_ATTRIBUTE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RLS_ATTRIBUTE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_ATTRIBUTE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_REWRITE_RULES_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_REWRITE_RULES_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CLUSTER_EVENT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLUSTER_EVENT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_EXTERNAL_TABLE_FILE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_EXTERNAL_TABLE_FILE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_STORAGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_STORAGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_STORAGE_OPERATION_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_STORAGE_OPERATION_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_SNAPSHOT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SNAPSHOT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_STATNAME_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_STATNAME_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_SYSSTAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SYSSTAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MLOG_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MLOG_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_PARAMS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STATS_PARAMS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_RUN_STATS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_RUN_STATS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STATS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_CHANGE_STATS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_CHANGE_STATS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STMT_STATS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STMT_STATS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DBMS_LOCK_ALLOCATED_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DBMS_LOCK_ALLOCATED_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_CONTROL_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_CONTROL_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_JOB_CLASS_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_JOB_CLASS_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_REORGANIZE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_REORGANIZE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CLONE_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLONE_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_SYSTEM_EVENT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SYSTEM_EVENT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_EVENT_NAME_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_EVENT_NAME_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PRIVILEGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PRIVILEGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PRIVILEGE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PRIVILEGE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_SQLSTAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQLSTAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_NCOMP_DLL_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_NCOMP_DLL_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_AUX_STAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUX_STAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_INDEX_USAGE_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_INDEX_USAGE_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DETECT_LOCK_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DETECT_LOCK_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_PARTITION_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_PARTITION_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_SQLTEXT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQLTEXT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TRUSTED_ROOT_CERTIFICATE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRUSTED_ROOT_CERTIFICATE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_AUDIT_LOG_FILTER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUDIT_LOG_FILTER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_AUDIT_LOG_USER_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUDIT_LOG_USER_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_PRIVILEGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_PRIVILEGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_PRIVILEGE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_PRIVILEGE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LS_REPLICA_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_REPLICA_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_CHECKSUM_ERROR_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_CHECKSUM_ERROR_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_INFO_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_INFO_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_ROLE_INFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_ROLE_INFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SERVICE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVICE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_STORAGE_IO_USAGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_STORAGE_IO_USAGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_DEP_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_DEP_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SPM_EVO_RESULT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPM_EVO_RESULT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_DETECT_LOCK_INFO_V2_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DETECT_LOCK_INFO_V2_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PKG_TYPE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PKG_TYPE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PKG_TYPE_ATTR_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PKG_TYPE_ATTR_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PKG_COLL_TYPE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PKG_COLL_TYPE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_SQL_PLAN_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQL_PLAN_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_RES_MGR_SYSSTAT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_RES_MGR_SYSSTAT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_KV_REDIS_TABLE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_KV_REDIS_TABLE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_NCOMP_DLL_V2_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_NCOMP_DLL_V2_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_FT_DICT_IK_UTF8_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FT_DICT_IK_UTF8_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_FT_STOPWORD_IK_UTF8_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FT_STOPWORD_IK_UTF8_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_FT_QUANTIFIER_IK_UTF8_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FT_QUANTIFIER_IK_UTF8_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_PRIVILEGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_PRIVILEGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_PRIVILEGE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_PRIVILEGE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_LICENSE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LICENSE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_PL_RECOMPILE_OBJINFO_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PL_RECOMPILE_OBJINFO_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_VECTOR_INDEX_TASK_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VECTOR_INDEX_TASK_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_VECTOR_INDEX_TASK_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VECTOR_INDEX_TASK_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CCL_RULE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CCL_RULE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_CCL_RULE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CCL_RULE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_COLUMN_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_COLUMN_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_COLUMN_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_COLUMN_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_PRIVILEGE_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_PRIVILEGE_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_PRIVILEGE_HISTORY_AUX_LOB_META_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_PRIVILEGE_HISTORY_AUX_LOB_META_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DDL_OPERATION_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_OPERATION_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_USER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_USER_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLEGROUP_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLEGROUP_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLEGROUP_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLEGROUP_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_PRIVILEGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_PRIVILEGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_PRIVILEGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_PRIVILEGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DATABASE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATABASE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SERVER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SYS_PARAMETER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_PARAMETER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_TENANT_PARAMETER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_TENANT_PARAMETER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SYS_VARIABLE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_VARIABLE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SYS_STAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_STAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_UNIT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_UNIT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_UNIT_CONFIG_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_UNIT_CONFIG_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RESOURCE_POOL_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESOURCE_POOL_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CHARSET_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CHARSET_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLLATION_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLLATION_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_HELP_TOPIC_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_TOPIC_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_HELP_CATEGORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_CATEGORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_HELP_KEYWORD_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_KEYWORD_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_HELP_RELATION_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_HELP_RELATION_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DUMMY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DUMMY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ROOTSERVICE_EVENT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROOTSERVICE_EVENT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PRIVILEGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PRIVILEGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_OUTLINE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OUTLINE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_OUTLINE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OUTLINE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RECYCLEBIN_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RECYCLEBIN_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PART_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PART_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SUB_PART_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SUB_PART_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SUB_PART_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SUB_PART_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PART_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PART_INFO_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PART_INFO_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DEF_SUB_PART_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEF_SUB_PART_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DEF_SUB_PART_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEF_SUB_PART_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SERVER_EVENT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVER_EVENT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ROOTSERVICE_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROOTSERVICE_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SYS_VARIABLE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYS_VARIABLE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DDL_ID_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_ID_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_COLUMN_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_COLUMN_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SYNONYM_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYNONYM_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SYNONYM_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SYNONYM_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_AUTO_INCREMENT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUTO_INCREMENT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DDL_CHECKSUM_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_CHECKSUM_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PARAM_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PARAM_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PARAM_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PARAM_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PACKAGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PACKAGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PACKAGE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PACKAGE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ACQUIRED_SNAPSHOT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ACQUIRED_SNAPSHOT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CONSTRAINT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONSTRAINT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CONSTRAINT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONSTRAINT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ORI_SCHEMA_VERSION_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ORI_SCHEMA_VERSION_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_FUNC_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FUNC_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_FUNC_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FUNC_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TEMP_TABLE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TEMP_TABLE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_OBJECT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEQUENCE_OBJECT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_OBJECT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEQUENCE_OBJECT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SEQUENCE_VALUE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEQUENCE_VALUE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_FREEZE_SCHEMA_VERSION_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FREEZE_SCHEMA_VERSION_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_ATTR_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_ATTR_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TYPE_ATTR_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TYPE_ATTR_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLL_TYPE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLL_TYPE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLL_TYPE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLL_TYPE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_WEAK_READ_SERVICE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_WEAK_READ_SERVICE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DBLINK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DBLINK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DBLINK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DBLINK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ROLE_GRANTEE_MAP_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_ROLE_GRANTEE_MAP_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_KEYSTORE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_KEYSTORE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_KEYSTORE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_KEYSTORE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_POLICY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_POLICY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_POLICY_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_POLICY_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_COMPONENT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_COMPONENT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_COMPONENT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_COMPONENT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_LABEL_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_LABEL_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_LABEL_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_LABEL_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_USER_LEVEL_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_USER_LEVEL_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TABLESPACE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TABLESPACE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TABLESPACE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TABLESPACE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_PROFILE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_PROFILE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_PROFILE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_PROFILE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SECURITY_AUDIT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SECURITY_AUDIT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TRIGGER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TRIGGER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TRIGGER_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TRIGGER_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SEED_PARAMETER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SEED_PARAMETER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SECURITY_AUDIT_RECORD_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SECURITY_AUDIT_RECORD_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SYSAUTH_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SYSAUTH_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SYSAUTH_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SYSAUTH_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJAUTH_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJAUTH_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJAUTH_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJAUTH_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_ERROR_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_ERROR_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RESTORE_PROGRESS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESTORE_PROGRESS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJECT_TYPE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJECT_TYPE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_OBJECT_TYPE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_OBJECT_TYPE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_NAME_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_NAME_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_TRANSITION_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_TRANSITION_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_CONSTRAINT_COLUMN_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_CONSTRAINT_COLUMN_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DEPENDENCY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_DEPENDENCY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_PLAN_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_PLAN_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_DIRECTIVE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_DIRECTIVE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_MAPPING_RULE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_MAPPING_RULE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DDL_ERROR_MESSAGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_ERROR_MESSAGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SPACE_USAGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPACE_USAGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_SET_FILES_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_SET_FILES_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RES_MGR_CONSUMER_GROUP_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RES_MGR_CONSUMER_GROUP_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DDL_TASK_STATUS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DDL_TASK_STATUS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_REGION_NETWORK_BANDWIDTH_LIMIT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_REGION_NETWORK_BANDWIDTH_LIMIT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_USAGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_USAGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_JOB_LOG_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_JOB_LOG_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DIRECTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_DIRECTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_DIRECTORY_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_DIRECTORY_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_STAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_STAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_STAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_STAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_HISTOGRAM_STAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_HISTOGRAM_STAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MONITOR_MODIFIED_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MONITOR_MODIFIED_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_STAT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_STAT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_STAT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_STAT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_HISTOGRAM_STAT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_HISTOGRAM_STAT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_OPTSTAT_GLOBAL_PREFS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OPTSTAT_GLOBAL_PREFS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_OPTSTAT_USER_PREFS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_OPTSTAT_USER_PREFS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_META_TABLE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_META_TABLE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_TO_LS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_TO_LS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_META_TABLE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_META_TABLE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_STATUS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_STATUS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DAM_LAST_ARCH_TS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DAM_LAST_ARCH_TS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DAM_CLEANUP_JOBS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DAM_CLEANUP_JOBS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_TO_TABLE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_TO_TABLE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_RECOVERY_STAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_RECOVERY_STAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_CHECKSUM_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_CHECKSUM_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_REPLICA_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_REPLICA_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PENDING_TRANSACTION_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PENDING_TRANSACTION_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_PROGRAM_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_PROGRAM_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CONTEXT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONTEXT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CONTEXT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CONTEXT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_MERGE_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_MERGE_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MERGE_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MERGE_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_FREEZE_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_FREEZE_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DISK_IO_CALIBRATION_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DISK_IO_CALIBRATION_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PLAN_BASELINE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PLAN_BASELINE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PLAN_BASELINE_ITEM_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PLAN_BASELINE_ITEM_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SPM_CONFIG_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPM_CONFIG_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_PARAMETER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_PARAMETER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_KV_TTL_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_KV_TTL_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SERVICE_EPOCH_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVICE_EPOCH_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SPATIAL_REFERENCE_SYSTEMS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPATIAL_REFERENCE_SYSTEMS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_MAPPING_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_MAPPING_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_JOB_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_JOB_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ARBITRATION_SERVICE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ARBITRATION_SERVICE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DATA_DICTIONARY_IN_LOG_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DATA_DICTIONARY_IN_LOG_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_POLICY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_POLICY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_POLICY_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_POLICY_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_SECURITY_COLUMN_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_SECURITY_COLUMN_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_SECURITY_COLUMN_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_SECURITY_COLUMN_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_GROUP_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_GROUP_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_GROUP_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_GROUP_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_CONTEXT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_CONTEXT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_CONTEXT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_CONTEXT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_ATTRIBUTE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_ATTRIBUTE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RLS_ATTRIBUTE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RLS_ATTRIBUTE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_REWRITE_RULES_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_REWRITE_RULES_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CLUSTER_EVENT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLUSTER_EVENT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_EXTERNAL_TABLE_FILE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_EXTERNAL_TABLE_FILE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_STORAGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_STORAGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ZONE_STORAGE_OPERATION_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ZONE_STORAGE_OPERATION_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_SNAPSHOT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SNAPSHOT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_STATNAME_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_STATNAME_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_SYSSTAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SYSSTAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MLOG_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MLOG_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_PARAMS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STATS_PARAMS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_RUN_STATS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_RUN_STATS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STATS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STATS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_CHANGE_STATS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_CHANGE_STATS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_REFRESH_STMT_STATS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_REFRESH_STMT_STATS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DBMS_LOCK_ALLOCATED_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DBMS_LOCK_ALLOCATED_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_CONTROL_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_CONTROL_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SCHEDULER_JOB_CLASS_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SCHEDULER_JOB_CLASS_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_REORGANIZE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_REORGANIZE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CLONE_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLONE_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_SYSTEM_EVENT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SYSTEM_EVENT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_EVENT_NAME_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_EVENT_NAME_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PRIVILEGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PRIVILEGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_ROUTINE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_ROUTINE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_SQLSTAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQLSTAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_NCOMP_DLL_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_NCOMP_DLL_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_AUX_STAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUX_STAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_INDEX_USAGE_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_INDEX_USAGE_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DETECT_LOCK_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DETECT_LOCK_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_PARTITION_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_PARTITION_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_SQLTEXT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQLTEXT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TRUSTED_ROOT_CERTIFICATE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TRUSTED_ROOT_CERTIFICATE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_AUDIT_LOG_FILTER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUDIT_LOG_FILTER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_AUDIT_LOG_USER_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_AUDIT_LOG_USER_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_PRIVILEGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_PRIVILEGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_COLUMN_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_COLUMN_PRIVILEGE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LS_REPLICA_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LS_REPLICA_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_TABLET_CHECKSUM_ERROR_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_TABLET_CHECKSUM_ERROR_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_INFO_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_INFO_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_ROLE_INFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_ROLE_INFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SERVICE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SERVICE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_STORAGE_IO_USAGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_STORAGE_IO_USAGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_MVIEW_DEP_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_MVIEW_DEP_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SPM_EVO_RESULT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SPM_EVO_RESULT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_DETECT_LOCK_INFO_V2_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_DETECT_LOCK_INFO_V2_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PKG_TYPE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PKG_TYPE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PKG_TYPE_ATTR_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PKG_TYPE_ATTR_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PKG_COLL_TYPE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PKG_COLL_TYPE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_SQL_PLAN_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQL_PLAN_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_RES_MGR_SYSSTAT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_RES_MGR_SYSSTAT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_KV_REDIS_TABLE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_KV_REDIS_TABLE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_NCOMP_DLL_V2_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_NCOMP_DLL_V2_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_WR_SQL_PLAN_AUX_KEY2SNAPSHOT_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_FT_DICT_IK_UTF8_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FT_DICT_IK_UTF8_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_FT_STOPWORD_IK_UTF8_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FT_STOPWORD_IK_UTF8_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_FT_QUANTIFIER_IK_UTF8_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_FT_QUANTIFIER_IK_UTF8_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_PRIVILEGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_PRIVILEGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CATALOG_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CATALOG_PRIVILEGE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_LICENSE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_LICENSE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_PL_RECOMPILE_OBJINFO_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_PL_RECOMPILE_OBJINFO_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_VECTOR_INDEX_TASK_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VECTOR_INDEX_TASK_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_VECTOR_INDEX_TASK_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_VECTOR_INDEX_TASK_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CCL_RULE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CCL_RULE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_CCL_RULE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_CCL_RULE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_COLUMN_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_COLUMN_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_COLUMN_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_COLUMN_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_PRIVILEGE_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_PRIVILEGE_AUX_LOB_PIECE_SCHEMA_VERSION); break;
    case OB_ALL_SENSITIVE_RULE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID : schema_version = static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::OB_ALL_SENSITIVE_RULE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_SCHEMA_VERSION); break;

#endif // INNER_TABLE_HARD_CODE_SCHEMA_MAPPING_SWITCH
  