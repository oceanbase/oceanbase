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

#define USING_LOG_PREFIX RS
#include "ob_global_index_builder.h"
#include "share/ob_define.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "common/ob_member.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/partition_table/ob_replica_filter.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_index_checksum.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "ob_ddl_service.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_server_manager.h"
#include "ob_zone_manager.h"
#include "ob_root_service.h"
#include "ob_rs_event_history_table_operator.h"
#include "ob_freeze_info_manager.h"
#include "ob_index_builder.h"
namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace common::sqlclient;
using namespace sql;
namespace rootserver {
ObGlobalIndexTask::ObGlobalIndexTask()
    : tenant_id_(OB_INVALID_ID),
      data_table_id_(OB_INVALID_ID),
      index_table_id_(OB_INVALID_ID),
      status_(GIBS_INVALID),
      snapshot_(0),
      major_sstable_exist_reply_ts_(0),
      checksum_snapshot_(0),
      schema_version_(0),
      last_drive_ts_(0),
      build_single_replica_stat_(BSRT_INVALID),
      partition_sstable_stat_array_(),
      partition_col_checksum_stat_array_(),
      partition_unique_stat_array_(),
      lock_(),
      retry_cnt_(0)
{}

ObGlobalIndexTask::~ObGlobalIndexTask()
{}

int ObGlobalIndexTask::get_partition_unique_check_stat(
    const common::ObPartitionKey& pkey, PartitionUniqueStat*& partition_unique_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    partition_unique_stat = NULL;
    bool find = false;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < partition_unique_stat_array_.count(); ++i) {
      PartitionUniqueStat& this_item = partition_unique_stat_array_.at(i);
      if (this_item.pkey_ == pkey) {
        find = true;
        partition_unique_stat = &this_item;
      } else {
        // by pass
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObGlobalIndexTask::get_partition_sstable_build_stat(
    const common::ObPartitionKey& pkey, PartitionSSTableBuildStat*& partition_sstable_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    partition_sstable_stat = NULL;
    bool find = false;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < partition_sstable_stat_array_.count(); ++i) {
      PartitionSSTableBuildStat& this_item = partition_sstable_stat_array_.at(i);
      if (this_item.pkey_ == pkey) {
        find = true;
        partition_sstable_stat = &this_item;
      } else {
        // by pass
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObGlobalIndexTask::get_partition_col_checksum_stat(
    const common::ObPartitionKey& pkey, PartitionColChecksumStat*& partition_col_checksum_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    partition_col_checksum_stat = NULL;
    bool find = false;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < partition_col_checksum_stat_array_.count(); ++i) {
      PartitionColChecksumStat& this_item = partition_col_checksum_stat_array_.at(i);
      if (this_item.pkey_ == pkey) {
        find = true;
        partition_col_checksum_stat = &this_item;
      } else {
        // by pass
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

ObGlobalIndexBuilder::ObGlobalIndexBuilder()
    : inited_(false),
      rpc_proxy_(NULL),
      mysql_proxy_(NULL),
      server_mgr_(NULL),
      schema_service_(NULL),
      pt_operator_(NULL),
      rebalance_task_mgr_(NULL),
      ddl_service_(NULL),
      zone_mgr_(NULL),
      task_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_BUILD_INDEX_SCHEDULER)),
      task_map_(),
      task_map_lock_(),
      idling_(stop_)
{}

ObGlobalIndexBuilder::~ObGlobalIndexBuilder()
{}

const bool ObGlobalIndexBuilder::STATE_SWITCH_ARRAY[GIBS_MAX][GIBS_MAX] = {
    /* INVAILD  |  BSR  |  MRC  |  UICC  |  UIC  |  IBTE  |  FAIL  | FNSH  */
    /* INVALID */ {
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
    },
    /* BSR     */
    {
        false,
        false,
        true,
        false,
        false,
        false,
        true,
        false,
    },
    /* MRC     */
    {
        false,
        true,
        false,
        true,
        false,
        true,
        true,
        false,
    },
    /* UICC    */
    {
        false,
        false,
        false,
        false,
        true,
        false,
        true,
        false,
    },
    /* UIC     */
    {
        false,
        false,
        false,
        false,
        false,
        true,
        true,
        false,
    },
    /* IBTE    */
    {
        false,
        false,
        false,
        false,
        false,
        false,
        true,
        true,
    },
    /* FAIL    */
    {
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        true,
    },
    /* FNSH    */
    {
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
    },
};

int ObGlobalIndexBuilder::init(obrpc::ObSrvRpcProxy* rpc_proxy, common::ObMySQLProxy* mysql_proxy,
    rootserver::ObServerManager* server_mgr, share::ObPartitionTableOperator* pt_operator,
    rootserver::ObRebalanceTaskMgr* rebalance_task_mgr, rootserver::ObDDLService* ddl_service,
    share::schema::ObMultiVersionSchemaService* schema_service, rootserver::ObZoneManager* zone_mgr,
    rootserver::ObFreezeInfoManager* freeze_info_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(NULL == rpc_proxy || NULL == mysql_proxy || NULL == server_mgr || NULL == pt_operator ||
                         NULL == rebalance_task_mgr || NULL == ddl_service || NULL == schema_service ||
                         NULL == zone_mgr || NULL == freeze_info_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        KP(rpc_proxy),
        KP(mysql_proxy),
        KP(rebalance_task_mgr),
        KP(server_mgr),
        KP(pt_operator),
        KP(ddl_service),
        KP(schema_service),
        KP(zone_mgr),
        KP(freeze_info_mgr));
  } else if (OB_FAIL(task_map_.create(TASK_MAP_BUCKET_NUM, ObModIds::OB_BUILD_INDEX_SCHEDULER))) {
    LOG_WARN("fail to create task map", K(ret));
  } else if (OB_FAIL(ObRsReentrantThread::create(THREAD_CNT, "GIdxBuilder"))) {
    LOG_WARN("fail to create global index builder thread", K(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    mysql_proxy_ = mysql_proxy;
    pt_operator_ = pt_operator;
    server_mgr_ = server_mgr;
    schema_service_ = schema_service;
    ddl_service_ = ddl_service;
    zone_mgr_ = zone_mgr;
    rebalance_task_mgr_ = rebalance_task_mgr;
    freeze_info_mgr_ = freeze_info_mgr;
    inited_ = true;
  }
  return ret;
}

int ObGlobalIndexBuilder::submit_build_global_index_task(const share::schema::ObTableSchema* index_schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  const int64_t orig_snapshot = 0;
  int64_t affected_rows = 0;
  ObGlobalIndexTask *task_ptr = NULL;
  bool skip_set_task_map = false;
#ifdef ERRSIM
  ret = E(EventTable::EN_SUBMIT_INDEX_TASK_ERROR_BEFORE_STAT_RECORD) OB_SUCCESS;
#endif
  if (OB_SUCCESS != ret) {
    LOG_INFO("errsim mock push global index task fail", K(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mysql proxy ptr is null", K(ret));
  } else if (OB_UNLIKELY(NULL == (task_ptr = task_allocator_.alloc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(sql_string.assign_fmt("INSERT INTO %s "
                                      "(TENANT_ID, DATA_TABLE_ID, INDEX_TABLE_ID, STATUS, SNAPSHOT, SCHEMA_VERSION) "
                                      "VALUES (%ld, %ld, %ld, %d, %ld, %ld)",
            OB_ALL_INDEX_BUILD_STAT_TNAME,
            index_schema->get_tenant_id(),
            index_schema->get_data_table_id(),
            index_schema->get_table_id(),
            GIBS_BUILD_SINGLE_REPLICA,
            orig_snapshot,
            index_schema->get_schema_version()))) {
      LOG_WARN("fail to assign fmt to sql string",
          K(ret),
          "tenant_id",
          index_schema->get_tenant_id(),
          "data_table_id",
          index_schema->get_data_table_id(),
          "index_table_id",
          index_schema->get_table_id());
    } else if (OB_FAIL(trans.start(mysql_proxy_))) {
      LOG_WARN("fail to start trans", K(ret));
    } else if (OB_FAIL(trans.write(OB_SYS_TENANT_ID, sql_string.ptr(), affected_rows))) {
      ObGlobalIndexTask *tmp_task_ptr = NULL;
      int tmp_ret = OB_SUCCESS;
      // Ghost index: index schema is existed but somehow the build index task is not there.
      // the following logic is for ghost index retry, there could be an index being built in progress
      // while the DDL retry scheduler thinks the index is a ghost. we add a new index task when
      // this happens though it could lead to repeated task. The repeated task can be detected
      // in the task itself.
      if (OB_ERR_PRIMARY_KEY_DUPLICATE != ret) {
        LOG_WARN("fail to execute write sql", K(ret));
      } else {
        ret = OB_SUCCESS;
        // There is a chance that RetryGhostIndexTask finds an unavailable index and sends a build index request,
        // while the index is actually being built and finished before the build index request reach here.
        // In this case, we should not add the task back again.
        skip_set_task_map = true;
        ObSchemaGetterGuard schema_guard;
        const ObTableSchema *latest_index_schema = nullptr;
        const int64_t index_tid = index_schema->get_table_id();
        if (OB_HASH_NOT_EXIST == (tmp_ret = task_map_.get_refactored(index_schema->get_table_id(), tmp_task_ptr))) {
          if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(
                  extract_tenant_id(index_tid), schema_guard))) {
            LOG_WARN("fail to get schema guard", K(ret), K(index_tid));
          } else if (OB_FAIL(schema_guard.get_table_schema(index_tid, latest_index_schema))) {
            LOG_WARN("fail to get table schema", K(ret), K(index_tid));
          } else if (OB_ISNULL(latest_index_schema)) {
            LOG_INFO("index schema is deleted, skip it");
          } else if (latest_index_schema->get_index_status() == INDEX_STATUS_UNAVAILABLE) {
            skip_set_task_map = false;
            LOG_INFO(
                "global index record in __all_index_build_stat, but not in task_map, add it to avoid unexpected miss");
          }
        }
      }
    }
#ifdef ERRSIM
    ret = E(EventTable::EN_SUBMIT_INDEX_TASK_ERROR_AFTER_STAT_RECORD) OB_SUCCESS;
#endif
    if (OB_SUCC(ret)) {
      if (skip_set_task_map) {
        LOG_INFO("task is already in task map, skip");
      } else {
        task_ptr->tenant_id_ = index_schema->get_tenant_id();
        task_ptr->data_table_id_ = index_schema->get_data_table_id();
        task_ptr->index_table_id_ = index_schema->get_table_id();
        task_ptr->snapshot_ = orig_snapshot;
        task_ptr->status_ = GIBS_BUILD_SINGLE_REPLICA;
        task_ptr->schema_version_ = index_schema->get_schema_version();
        task_ptr->last_drive_ts_ = 0;
        // thread safe, no need to acquire lock
        if (OB_FAIL(task_map_.set_refactored(task_ptr->index_table_id_, task_ptr))) {
          LOG_WARN("fail to set refactored", K(ret));
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", K(tmp_ret), "is_commit", OB_SUCC(ret));
        ret = (OB_SUCCESS == ret ? tmp_ret : ret);
      }
    }
    if (OB_FAIL(ret) && NULL != task_ptr) {
      task_allocator_.free(task_ptr);
      task_ptr = NULL;
    }
  }
  idling_.wakeup();
  return ret;
}

int ObGlobalIndexBuilder::check_task_dropped(const ObGlobalIndexTask& task, bool& is_dropped)
{
  int ret = OB_SUCCESS;
  is_dropped = false;
  ObSchemaGetterGuard sys_schema_guard;
  if (OB_FAIL(
          ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(OB_SYS_TENANT_ID, sys_schema_guard))) {
    LOG_WARN("get sys tenant schema guard failed", K(ret));
  } else if (OB_FAIL(sys_schema_guard.check_if_tenant_has_been_dropped(
                 extract_tenant_id(task.data_table_id_), is_dropped))) {
    LOG_WARN("check if tenant has been dropped failed", K(ret), K(task));
  }
  return ret;
}

int ObGlobalIndexBuilder::delete_task_record(const ObGlobalIndexTask& task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  int64_t affected_rows = 0;
  if (OB_FAIL(release_snapshot(&task))) {
    LOG_WARN("release snapshot failed", K(ret), K(task));
  } else if (OB_FAIL(sql_string.assign_fmt(
                 "delete from %s where tenant_id=%ld and data_table_id=%ld and index_table_id=%ld",
                 OB_ALL_INDEX_BUILD_STAT_TNAME,
                 extract_tenant_id(task.data_table_id_),
                 task.data_table_id_,
                 task.index_table_id_))) {
    LOG_WARN("generate sql string failed", K(ret), K(task));
  } else if (OB_FAIL(mysql_proxy_->write(sql_string.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql_string), K(affected_rows));
  } else if (1 == affected_rows) {
    LOG_INFO("delete one index task record success", K(ret), K(task));
  }
  return ret;
}

int ObGlobalIndexBuilder::reload_building_indexes()
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    ObTimeoutCtx ctx;
    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
    } else if (OB_FAIL(reset_run_condition())) {
      LOG_WARN("fail to reset run condition", K(ret));
    } else if (OB_UNLIKELY(NULL == mysql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql proxy ptr is null", K(ret));
    } else if (0 != task_map_.size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task map should be empty before reload", K(ret));
    } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql_string.assign_fmt("SELECT * FROM %s ", OB_ALL_INDEX_BUILD_STAT_TNAME))) {
      LOG_WARN("fail to append sql", K(ret));
    } else if (OB_FAIL(mysql_proxy_->read(res, sql_string.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql_string));
    } else if (OB_UNLIKELY(NULL == (result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret));
    } else {
      ObGlobalIndexTask tmp_task;
      const ObTableSchema* table_schema = nullptr;
      bool need_load_global = false;
      bool is_task_dropped = false;
      ObSchemaGetterGuard tenant_schema_guard;
      SpinWLockGuard guard(task_map_lock_);
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        if (OB_FAIL(fill_global_index_task_result(result, &tmp_task))) {
          LOG_WARN("fail to fill global index task result", K(ret));
        } else if (OB_FAIL(check_task_dropped(tmp_task, is_task_dropped))) {
          LOG_WARN("check task dropped failed", K(ret), K(tmp_task));
        } else if (is_task_dropped) {
          if (OB_FAIL(delete_task_record(tmp_task))) {
            LOG_WARN("delete index task record failed", K(ret), K(tmp_task));
          }
        } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                       extract_tenant_id(tmp_task.data_table_id_), tenant_schema_guard, tmp_task.schema_version_))) {
          LOG_WARN("fail to get tenant schema guard", K(ret), K(tmp_task.data_table_id_));
        } else if (OB_FAIL(tenant_schema_guard.get_table_schema(tmp_task.index_table_id_, table_schema))) {
          LOG_WARN("fail to get table schema", K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table not exist",
              K(ret),
              "table_id",
              tmp_task.data_table_id_,
              "schema_version",
              tmp_task.schema_version_);
        } else {
          need_load_global = table_schema->is_global_index_table();
        }

        if (OB_SUCC(ret) && !is_task_dropped) {
          if (need_load_global) {
            ObGlobalIndexTask* task_ptr = task_allocator_.alloc();
            if (OB_UNLIKELY(NULL == task_ptr)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory", K(ret));
            } else {
              if (OB_FAIL(fill_global_index_task_result(result, task_ptr))) {
                LOG_WARN("fail to fill global index task result", K(ret));
              } else if (OB_FAIL(task_map_.set_refactored(task_ptr->index_table_id_, task_ptr))) {
                if (OB_HASH_EXIST == ret) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("fail to set task map", K(ret));
                }
              } else {
                task_ptr = nullptr;
              }  // no more to do
              if (nullptr != task_ptr) {
                task_allocator_.free(task_ptr);
                task_ptr = NULL;
              }
            }
          } else {
            ObRSBuildIndexTask task;
            if (OB_FAIL(task.init(
                    tmp_task.index_table_id_, tmp_task.data_table_id_, tmp_task.schema_version_, ddl_service_))) {
              LOG_WARN("fail to init build index task", K(ret));
            } else if (OB_FAIL(ObRSBuildIndexScheduler::get_instance().push_task(task))) {
              LOG_WARN("fail to push back create index task", K(ret));
            }
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCCESS == ret) {
        idling_.wakeup();
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::fill_global_index_task_result(ObMySQLResult* result, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == result || NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(result), KP(task));
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", task->tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "data_table_id", task->data_table_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "index_table_id", task->index_table_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "status", task->status_, GlobalIndexBuildStatus);
    EXTRACT_INT_FIELD_MYSQL(*result, "snapshot", task->snapshot_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", task->schema_version_, int64_t);
  }
  return ret;
}

int ObGlobalIndexBuilder::check_and_get_index_schema(share::schema::ObSchemaGetterGuard& schema_guard,
    const uint64_t index_table_id, const share::schema::ObTableSchema*& index_schema, bool& index_schema_exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == index_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index_table_id));
  } else if (OB_FAIL(schema_guard.check_table_exist(index_table_id, index_schema_exist))) {
    LOG_WARN("fail to check table exist", K(ret));
  } else if (!index_schema_exist) {
    // table not exist
  } else if (OB_FAIL(schema_guard.get_table_schema(index_table_id, index_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_UNLIKELY(NULL == index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema ptr is null", K(ret), K(index_table_id));
  } else if (index_schema->is_dropped_schema()) {
    // table delay delete, do not build index
  } else {
  }  // no more to do
  return ret;
}

int ObGlobalIndexBuilder::generate_original_table_partition_leader_array(
    share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema* data_schema,
    common::ObIArray<PartitionServer>& partition_leader_array)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == data_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt operator is null", K(ret));
  } else {
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter partition_key_iter(*data_schema, check_dropped_schema);
    ObPartitionKey pkey;
    ObPartitionKey phy_pkey;
    while (OB_SUCC(ret) && OB_SUCC(partition_key_iter.next_partition_key_v2(pkey))) {
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      ObPartitionInfo info;
      info.set_allocator(&allocator);
      PartitionServer partition_leader;
      const ObPartitionReplica* leader_replica = NULL;
      if (OB_UNLIKELY(!pkey.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey is invalid", K(ret));
      } else if (data_schema->is_binding_table()) {
        if (OB_FAIL(data_schema->get_pg_key(pkey, phy_pkey))) {
          LOG_WARN("fail to get pg key", K(ret), K(pkey));
        }
      } else {
        phy_pkey = pkey;
      }

      if (OB_FAIL(ret)) {
        // failed
      } else if (OB_FAIL(pt_operator_->get(phy_pkey.get_table_id(), phy_pkey.get_partition_id(), info))) {
        LOG_WARN("fail to get partition info", K(phy_pkey));
      } else if (OB_FAIL(info.find_leader_v2(leader_replica))) {
        LOG_WARN("fail to find leader", K(ret));
      } else if (OB_UNLIKELY(NULL == leader_replica)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader replica ptr is null", K(ret));
      } else if (partition_leader.set(
                     leader_replica->server_, pkey.get_table_id(), pkey.get_partition_id(), pkey.get_partition_cnt())) {
        LOG_WARN("fail to set partition leader", K(ret), K(*leader_replica));
      } else if (OB_FAIL(partition_leader_array.push_back(partition_leader))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get leader array", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (partition_leader_array.count() != data_schema->get_all_part_num()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part num and leader cnt not match",
            K(ret),
            "table_id",
            data_schema->get_table_id(),
            "left_cnt",
            partition_leader_array.count(),
            "right_cnt",
            data_schema->get_all_part_num());
      }
    }
  }
  return ret;
}

/* in order to get global index build snapshot, we need to send rpcs to all leader replicas of
 * the data table. we use synchronously waiting as the time consumption for get snapshot is thought low.
 */
int ObGlobalIndexBuilder::get_global_index_build_snapshot(ObGlobalIndexTask* task,
    share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema* index_schema,
    int64_t& snapshot)
{
  int ret = OB_SUCCESS;
  common::ObArray<PartitionServer> partition_leader_array;
  const share::schema::ObTableSchema* data_schema = NULL;
  share::ObSimpleFrozenStatus frozen_status;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == index_schema || NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == rpc_proxy_ || NULL == freeze_info_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy_ ptr is null", K(ret));
  } else if (OB_FAIL(freeze_info_mgr_->get_freeze_info(0 /*means newest*/, frozen_status))) {
    LOG_WARN("fail to get freeze info", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), data_schema))) {
    LOG_WARN("fail to get table schema", K(ret), "table_id", index_schema->get_data_table_id());
  } else if (OB_UNLIKELY(NULL == data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("data schema not exist", K(ret), "table_id", index_schema->get_data_table_id());
  } else if (OB_FAIL(
                 generate_original_table_partition_leader_array(schema_guard, data_schema, partition_leader_array))) {
    LOG_WARN("fail to generate original leader array", K(ret));
  } else {
    ObCheckSchemaVersionElapsedProxy proxy(*rpc_proxy_, &ObSrvRpcProxy::check_schema_version_elapsed);
    ObCheckSchemaVersionElapsedArg arg;
    if (OB_FAIL(do_get_associated_snapshot(
            proxy, arg, task, data_schema->get_all_part_num(), partition_leader_array, snapshot))) {
      LOG_WARN("fail to do get global index build snapshot", K(ret));
    } else {
      snapshot = std::max(snapshot, frozen_status.frozen_timestamp_);
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::init_build_snapshot_ctx(const common::ObIArray<PartitionServer>& partition_leader_array,
    common::ObIArray<int64_t>& invalid_snapshot_id_array, common::ObIArray<int64_t>& snapshot_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(partition_leader_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    invalid_snapshot_id_array.reset();
    snapshot_array.reset();
    const int64_t array_count = partition_leader_array.count();
    for (int64_t index = 0; OB_SUCC(ret) && index < array_count; ++index) {
      if (OB_FAIL(invalid_snapshot_id_array.push_back(index))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(snapshot_array.push_back(0))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

template <typename PROXY, typename ARG>
int ObGlobalIndexBuilder::do_get_associated_snapshot(PROXY& rpc_proxy, ARG& rpc_arg, ObGlobalIndexTask* task,
    const int64_t all_part_num, common::ObIArray<PartitionServer>& partition_leader_array, int64_t& snapshot)
{
  int ret = OB_SUCCESS;
  // an array which is used to record the snapshot of each partition
  common::ObArray<int64_t> snapshot_array;
  // an array which is used to record the partition leader array offset
  // of all the invalid snapshots
  common::ObArray<int64_t> invalid_snapshot_id_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(all_part_num <= 0 || NULL == task || partition_leader_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(all_part_num), "array_cnt", partition_leader_array.count());
  } else if (OB_FAIL(init_build_snapshot_ctx(partition_leader_array, invalid_snapshot_id_array, snapshot_array))) {
    LOG_WARN("fail to init invalid snapshot id array", K(ret));
  } else {
    bool got_snapshot = false;
    common::ObArray<int> ret_code_array;
    int64_t timeout = all_part_num * TIME_INTERVAL_PER_PART_US;
    const int64_t max_timeout = MAX_WAIT_CHECK_SCHEMA_VERSION_INTERVAL_US;
    const int64_t min_timeout = MIN_WAIT_CHECK_SCHEMA_VERSION_INTERVAL_US;
    timeout = std::min(timeout, max_timeout);
    timeout = std::max(timeout, min_timeout);
    int64_t timeout_ts = timeout + ObTimeUtility::current_time();
    while (OB_SUCC(ret) && !got_snapshot && ObTimeUtility::current_time() < timeout_ts) {
      rpc_proxy.reuse();
      ret_code_array.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < invalid_snapshot_id_array.count(); ++i) {
        int64_t index = invalid_snapshot_id_array.at(i);
        rpc_arg.reuse();
        if (index >= partition_leader_array.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index unexpected", K(ret), K(index), "array count", partition_leader_array.count());
        } else if (OB_FAIL(rpc_arg.build(task, partition_leader_array.at(index).pkey_))) {
          LOG_WARN("fail to build rpc arg", K(ret));
        } else if (OB_FAIL(rpc_proxy.call(
                       partition_leader_array.at(index).server_, GET_ASSOCIATED_SNAPSHOT_TIMEOUT, rpc_arg))) {
          LOG_WARN("fail to call rpc", K(ret));
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = rpc_proxy.wait_all(ret_code_array))) {
        LOG_WARN("rpc_proxy wait failed", K(ret), K(tmp_ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      } else if (OB_SUCC(ret)) {  // wait_all SUCC and the above process SUCC
        common::ObArray<int64_t> pre_invalid_snapshot_id_array;
        if (OB_FAIL(pre_invalid_snapshot_id_array.assign(invalid_snapshot_id_array))) {
          LOG_WARN("fail to assign invalid snapshot id array", K(ret));
        } else if (OB_FAIL(update_build_snapshot_ctx(
                       rpc_proxy, ret_code_array, invalid_snapshot_id_array, snapshot_array))) {
          LOG_WARN("fail to update build snapshot ctx", K(ret));
        } else if (invalid_snapshot_id_array.count() <= 0) {
          if (OB_FAIL(pick_build_snapshot(snapshot_array, snapshot))) {
            LOG_WARN("fail to pick snapshot array", K(ret));
          } else {
            LOG_INFO("get snapshot", K(ret));
            got_snapshot = true;
          }
        } else {
          int64_t regular_wait_us = WAIT_US;
          int64_t sleep_us = timeout_ts - ObTimeUtility::current_time();
          sleep_us = std::max(sleep_us, 1L);
          sleep_us = std::min(sleep_us, regular_wait_us);
          usleep(static_cast<uint32_t>(sleep_us));
          if (OB_FAIL(update_partition_leader_array(
                  partition_leader_array, ret_code_array, pre_invalid_snapshot_id_array))) {
            LOG_WARN("fail to update partition leader array", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!got_snapshot) {
      ret = OB_TIMEOUT;
      LOG_WARN("get snapshot timeout", K(ret));
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::switch_state(ObGlobalIndexTask* task, const GlobalIndexBuildStatus next_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy ptr is null", K(ret));
  } else {
    ObMySQLTransaction trans;
    GlobalIndexBuildStatus cur_status = task->status_;
    LOG_INFO("task switch state",
        K(ret),
        K(cur_status),
        K(next_status),
        "tenant_id",
        task->tenant_id_,
        "data_table_id",
        task->data_table_id_,
        "index_id",
        task->index_table_id_,
        "schema_version",
        task->schema_version_,
        "snapshot",
        task->snapshot_);
    if (!STATE_SWITCH_ARRAY[cur_status][next_status]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot do state switch", K(ret), K(cur_status), K(next_status));
    } else if (OB_FAIL(trans.start(mysql_proxy_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else {
      ObSqlString sql_string;
      int64_t affected_rows;
      if (OB_FAIL(sql_string.assign_fmt("UPDATE %s SET STATUS = %d "
                                        "WHERE TENANT_ID = %ld "
                                        "AND DATA_TABLE_ID = %ld "
                                        "AND INDEX_TABLE_ID = %ld ",
              OB_ALL_INDEX_BUILD_STAT_TNAME,
              next_status,
              task->tenant_id_,
              task->data_table_id_,
              task->index_table_id_))) {
        LOG_WARN("fail to format sql", K(ret), K(sql_string));
      } else if (trans.write(sql_string.ptr(), affected_rows)) {
        LOG_WARN("fail to execute sql", K(ret), K(sql_string));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCCESS == ret))) {
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
        LOG_WARN("fail to end trans", K(ret), K(tmp_ret));
      }
      if (OB_SUCC(ret)) {
        ROOTSERVICE_EVENT_ADD("global_index_builder",
            "switch_state",
            "tenant_id",
            task->tenant_id_,
            "data_table_id",
            task->data_table_id_,
            "index_table_id",
            task->index_table_id_,
            "pre_state",
            task->status_,
            "new_state",
            next_status,
            "snapshot_version",
            task->snapshot_);
        task->status_ = next_status;
        // modify the last drive ts in this swich status func to
        // drive the state machine to the next state
        task->last_drive_ts_ = 0;
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::pick_build_snapshot(const common::ObIArray<int64_t>& snapshot_array, int64_t& snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(snapshot_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    snapshot = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < snapshot_array.count(); ++i) {
      const int64_t this_snapshot = snapshot_array.at(i);
      if (this_snapshot > snapshot) {
        snapshot = this_snapshot;
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::update_partition_leader_array(common::ObIArray<PartitionServer>& partition_leader_array,
    const common::ObIArray<int>& ret_code_array, const common::ObIArray<int64_t>& invalid_snapshot_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(ret_code_array.count() != invalid_snapshot_id_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array count not match",
        K(ret),
        "left array count",
        ret_code_array.count(),
        "right array count",
        invalid_snapshot_id_array.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ret_code_array.count(); ++i) {
      int ret_code = ret_code_array.at(i);
      if (OB_SUCCESS == ret_code) {
        // already got snapshot
      } else if (OB_EAGAIN == ret_code || OB_TIMEOUT == ret_code) {
        // transaction on the partition not finish, wait and retry
      } else if (OB_NOT_MASTER == ret_code || OB_PARTITION_NOT_EXIST == ret_code) {
        int64_t part_array_idx = invalid_snapshot_id_array.at(i);
        if (part_array_idx >= partition_leader_array.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part array id unexpected",
              K(ret),
              K(part_array_idx),
              "invalid snapshot id array count",
              partition_leader_array.count());
        } else {
          const ObPartitionKey& pkey = partition_leader_array.at(part_array_idx).pkey_;
          ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
          ObPartitionInfo info;
          const ObPartitionReplica* leader_replica = NULL;
          ObReplicaFilterHolder filter;
          info.set_allocator(&allocator);
          if (OB_FAIL(pt_operator_->get(pkey.get_table_id(), pkey.get_partition_id(), info))) {
            LOG_WARN("fail to get partition info", K(ret), K(pkey));
          } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
            LOG_WARN("fail to set replica status", K(ret));
          } else if (OB_FAIL(filter.set_in_member_list())) {
            LOG_WARN("fail to set in member list", K(ret));
          } else if (OB_FAIL(info.filter(filter))) {
            LOG_WARN("fail to do filter", K(ret));
          } else if (OB_FAIL(info.find_leader_v2(leader_replica))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;  // no leader exists, ignore
            } else {
              LOG_WARN("fail to get leader", K(ret));
            }
          } else if (OB_UNLIKELY(NULL == leader_replica)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("leader replica ptr is null", K(ret));
          } else if (OB_FAIL(partition_leader_array.at(part_array_idx).set_server(leader_replica->server_))) {
            LOG_WARN("fail to set server", K(ret));
          } else {
          }  // no more to do
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ret code", K(ret));
      }
    }
  }
  return ret;
}

template <typename PROXY>
int ObGlobalIndexBuilder::update_build_snapshot_ctx(PROXY& proxy, const common::ObIArray<int>& ret_code_array,
    common::ObIArray<int64_t>& invalid_snapshot_id_array, common::ObIArray<int64_t>& snapshot_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (invalid_snapshot_id_array.count() != ret_code_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array count not match",
        K(ret),
        "left array count",
        invalid_snapshot_id_array.count(),
        "right array count",
        ret_code_array.count());
  } else if (proxy.get_results().count() != ret_code_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array count not match",
        K(ret),
        "left array count",
        proxy.get_results().count(),
        "right array count",
        ret_code_array.count());
  } else {
    common::ObArray<int64_t> tmp_invalid_snapshot_id_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < ret_code_array.count(); ++i) {
      int ret_code = ret_code_array.at(i);
      int64_t snapshot_array_idx = invalid_snapshot_id_array.at(i);
      if (OB_SUCCESS == ret_code) {
        if (snapshot_array_idx >= snapshot_array.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("snapshot array idx unexpected",
              K(ret),
              K(snapshot_array_idx),
              "snapshot_array count",
              snapshot_array.count());
        } else if (NULL == proxy.get_results().at(i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result ptr is null", K(ret));
        } else {
          snapshot_array.at(snapshot_array_idx) = proxy.get_results().at(i)->snapshot_;
        }
      } else if (OB_EAGAIN == ret_code || OB_NOT_MASTER == ret_code || OB_PARTITION_NOT_EXIST == ret_code ||
                 OB_TIMEOUT == ret_code) {
        if (OB_FAIL(tmp_invalid_snapshot_id_array.push_back(snapshot_array_idx))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rpc invoking failed", K(ret), K(ret_code));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      invalid_snapshot_id_array.reset();
      if (OB_FAIL(invalid_snapshot_id_array.assign(tmp_invalid_snapshot_id_array))) {
        LOG_WARN("fail to assign array", K(ret));
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::update_task_global_index_build_snapshot(ObGlobalIndexTask* task, const int64_t snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else if (OB_UNLIKELY(GIBS_BUILD_SINGLE_REPLICA != task->status_ || NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task unexpected", K(ret), K(*task), KP(mysql_proxy_));
  } else {
    ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.start(mysql_proxy_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else {
      ObSqlString sql_string;
      if (OB_FAIL(sql_string.assign_fmt("UPDATE %s SET SNAPSHOT = %ld "
                                        "WHERE TENANT_ID = %ld "
                                        "AND DATA_TABLE_ID = %ld "
                                        "AND INDEX_TABLE_ID = %ld ",
              OB_ALL_INDEX_BUILD_STAT_TNAME,
              snapshot,
              task->tenant_id_,
              task->data_table_id_,
              task->index_table_id_))) {
        LOG_WARN("fail to assign format", K(ret));
      } else if (OB_FAIL(trans.write(sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql_string));
      } else if (1 != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(snapshot), K(*task));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCCESS == ret))) {
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
        LOG_WARN("fail to end trans", K(ret), K(tmp_ret));
      }
      if (OB_SUCC(ret)) {
        task->snapshot_ = snapshot;
        DEBUG_SYNC(AFTER_GLOBAL_INDEX_GET_SNAPSHOT);
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::drive_this_build_single_replica(const share::schema::ObTableSchema* index_schema,
    share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mysql proxy ptr is null", K(ret));
  } else if (BSRT_INVALID == task->build_single_replica_stat_) {
    // go on wait
  } else if (BSRT_SUCCEED == task->build_single_replica_stat_) {
    uint64_t execution_id = OB_INVALID_ID;
    bool is_checksum_equal = false;
    int tmp_ret = sql::ObIndexSSTableBuilder::query_execution_id(
        execution_id, index_schema->get_table_id(), task->snapshot_, *mysql_proxy_);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to query execution id", K(tmp_ret), "index_table_id", index_schema->get_table_id());
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (OB_INVALID_ID == execution_id) {
      LOG_WARN("unexpected execution id", K(ret), "index_table_id", index_schema->get_table_id());
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (OB_FAIL(ObIndexChecksumOperator::check_column_checksum(execution_id,
                   index_schema->get_data_table_id(),
                   index_schema->get_table_id(),
                   is_checksum_equal,
                   *mysql_proxy_))) {
      if (OB_CHECKSUM_ERROR == ret) {
        LOG_WARN("fail to query column checksum", K(ret), "index_table_id", index_schema->get_table_id());
      } else {
        LOG_WARN("fail to query column checksum", K(ret), "index_table_id", index_schema->get_table_id());
      }
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (!is_checksum_equal) {
      LOG_WARN("index table checksum not match",
          "index_table_id",
          index_schema->get_table_id(),
          "data_table_id",
          index_schema->get_data_table_id());
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else {
      (void)switch_state(task, GIBS_MULTI_REPLICA_COPY);
    }
  } else if (BSRT_PRIMARY_KEY_DUPLICATE == task->build_single_replica_stat_) {
    if (OB_FAIL(switch_state(task, GIBS_INDEX_BUILD_FAILED))) {
      LOG_WARN("fail to switch state", K(ret));
    }
  } else if (BSRT_REPLICA_NOT_READABLE == task->build_single_replica_stat_) {
    task->last_drive_ts_ = 0;
  } else if (BSRT_INVALID_SNAPSHOT == task->build_single_replica_stat_) {
    task->last_drive_ts_ = 0;
  } else if (BSRT_FAILED == task->build_single_replica_stat_) {
    (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
  }
  return ret;
}

int ObGlobalIndexBuilder::hold_snapshot(const ObGlobalIndexTask* task, const int64_t snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task || snapshot < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(nullptr == ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_service ptr is null", K(ret));
  } else {
    // hold data table snapshot
    ObSnapshotInfo info1;
    info1.snapshot_type_ = SNAPSHOT_FOR_CREATE_INDEX;
    info1.snapshot_ts_ = snapshot;
    info1.schema_version_ = task->schema_version_;
    info1.tenant_id_ = task->tenant_id_;
    info1.table_id_ = task->data_table_id_;
    // hold index table snapshot
    ObSnapshotInfo info2;
    info2.snapshot_type_ = SNAPSHOT_FOR_CREATE_INDEX;
    info2.snapshot_ts_ = snapshot;
    info2.schema_version_ = task->schema_version_;
    info2.tenant_id_ = task->tenant_id_;
    info2.table_id_ = task->index_table_id_;

    ObMySQLTransaction trans;
    common::ObMySQLProxy& proxy = ddl_service_->get_sql_proxy();
    if (OB_FAIL(trans.start(&proxy))) {
      LOG_WARN("fail to start trans", K(ret));
    } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().acquire_snapshot(trans, info1))) {
      LOG_WARN("fail to acquire snapshot", K(ret));
    } else if (!info2.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(info2));
    } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().set_index_building_snapshot(
                   proxy, info2.table_id_, info2.snapshot_ts_))) {
      LOG_WARN("fail to set index building snapshot", KR(ret), K(info2));
    } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().acquire_snapshot_for_building_index(
                   trans, info2, info2.table_id_))) {
      LOG_WARN("fail to acquire snapshot", K(ret));
    }
    if (trans.is_started()) {
      bool is_commit = (ret == OB_SUCCESS);
      int tmp_ret = trans.end(is_commit);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to end trans", K(ret), K(is_commit));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::launch_new_build_single_replica(const share::schema::ObTableSchema* index_schema,
    share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    int64_t snapshot = 0;
    if (0 != task->snapshot_) {
      snapshot = task->snapshot_;
    } else if (OB_FAIL(get_global_index_build_snapshot(task, schema_guard, index_schema, snapshot))) {
      LOG_WARN("fail to get global index build snapshot", K(ret));
    } else if (OB_FAIL(hold_snapshot(task, snapshot))) {
      LOG_WARN("fail to hold snapshot", K(ret));
    } else if (OB_FAIL(update_task_global_index_build_snapshot(task, snapshot))) {
      LOG_WARN("fail to update global index build snapshot", K(ret));
    } else {
    }  // no more to do

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_build_single_replica(task, index_schema, snapshot))) {
      LOG_WARN("fail to do build single replica", K(ret));
    } else {
      task->build_single_replica_stat_ = BSRT_INVALID;
      task->last_drive_ts_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::do_build_single_replica(
    ObGlobalIndexTask* task, const share::schema::ObTableSchema* index_schema, const int64_t snapshot)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  ObRootService *root_service = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task), KP(index_schema));
  } else if (NULL == (root_service = GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service ptr is null", K(ret));
  } else {
    sql::ObIndexSSTableBuilder::BuildIndexJob job;
    int64_t parallel_server_target = 5;
    int tmp_ret = OB_SUCCESS;
    job.job_id_ = index_schema->get_table_id();
    job.schema_version_ = task->schema_version_;
    job.snapshot_version_ = snapshot;
    job.data_table_id_ = index_schema->get_data_table_id();
    job.index_table_id_ = index_schema->get_table_id();
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ObSchemaUtils::get_tenant_int_variable(
                                       OB_SYS_TENANT_ID, SYS_VAR_PARALLEL_SERVERS_TARGET, parallel_server_target)))) {
      STORAGE_LOG(WARN, "failed to get sys tenant parallel server target", K(tmp_ret));
    }
    job.degree_of_parallelism_ = std::max(10L, parallel_server_target * 2);
    job.degree_of_parallelism_ = std::min(96L, job.degree_of_parallelism_);
    const int64_t timeout = GCONF.global_index_build_single_replica_timeout;
    const int64_t abs_timeout_us = ObTimeUtility::current_time() + timeout;
    if (OB_FAIL(root_service->submit_index_sstable_build_task(job, *this, abs_timeout_us))) {
      LOG_WARN("fail to submit task", K(ret));
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::try_build_single_replica(ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_service_ || NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else {
    SpinWLockGuard item_guard(task->lock_);
    const uint64_t index_table_id = task->index_table_id_;
    const uint64_t tenant_id = extract_tenant_id(index_table_id);
    const ObTableSchema* index_schema = NULL;
    share::schema::ObSchemaGetterGuard schema_guard;
    bool index_schema_exist = false;
    int64_t version_in_inner_table = OB_INVALID_VERSION;
    // strong consistency is used on primary cluster
    // since ObGlobalIndexBuilder is not used on standby cluster
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id;
    int64_t local_schema_version = OB_INVALID_VERSION;
    if (GCTX.is_standby_cluster()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("create global index in slave cluster is not allowed", K(ret), K(index_table_id));
    } else if (GIBS_BUILD_SINGLE_REPLICA != task->status_) {
      // may have been modify by others
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_schema_version))) {
      LOG_WARN("fail to get schema version from guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(
                   *mysql_proxy_, schema_status, version_in_inner_table))) {
      LOG_WARN("fail to get version in inner table", K(ret));
    } else if (version_in_inner_table > local_schema_version) {
      // by pass, this server may not get the newest schema
    } else if (OB_FAIL(check_and_get_index_schema(schema_guard, index_table_id, index_schema, index_schema_exist))) {
      LOG_WARN("fail to get table schema", K(ret), K(index_table_id));
    } else if (!index_schema_exist) {
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (OB_UNLIKELY(NULL == index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema ptr is null", K(ret));
    } else if (is_error_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (is_available_index_status(index_schema->get_index_status())) {
      (void)switch_state(task, GIBS_INDEX_BUILD_FINISH);
    } else if (task->last_drive_ts_ + BUILD_SINGLE_REPLICA_TIMEOUT > ObTimeUtility::current_time()) {
      if (OB_FAIL(drive_this_build_single_replica(index_schema, schema_guard, task))) {
        LOG_WARN("fail to drive this build single replica", K(ret));
      }
    } else {
      if (OB_FAIL(launch_new_build_single_replica(index_schema, schema_guard, task))) {
        LOG_WARN("fail to launch new build single replica", K(ret));
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::check_partition_copy_replica_stat(int64_t& major_sstable_exist_reply_ts,
    share::ObPartitionInfo& partition_info, AllReplicaSSTableStat& all_replica_sstable_stat)
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* leader_replica = NULL;
  ObPartitionKey pkey;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy_ ptr is null", K(ret));
  } else if (OB_FAIL(partition_info.find_leader_v2(leader_replica))) {
    LOG_WARN("fail to find leader", K(ret));
  } else if (OB_UNLIKELY(NULL == leader_replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader replica ptr is null", K(ret));
  } else if (OB_FAIL(
                 pkey.init(leader_replica->table_id_, leader_replica->partition_id_, leader_replica->partition_cnt_))) {
    LOG_WARN("fail to init partition key", K(ret));
  } else {
    int64_t related_count = 0;
    int64_t has_sstable_count = 0;
    int64_t has_sstable_full_replica_count = 0;
    for (int64_t i = 0; i < partition_info.get_replicas_v2().count(); ++i) {
      const ObPartitionReplica& part_replica = partition_info.get_replicas_v2().at(i);
      if ((REPLICA_TYPE_READONLY == part_replica.replica_type_) ||
          (REPLICA_TYPE_FULL == part_replica.replica_type_ && part_replica.is_in_service())) {
        related_count++;
        if (part_replica.data_version_ > 0) {
          has_sstable_count++;
          if (REPLICA_TYPE_FULL == part_replica.replica_type_ && part_replica.is_in_service()) {
            has_sstable_full_replica_count++;
          }
        }
      }
    }
    if (related_count < 0 || related_count < has_sstable_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count", K(ret), K(related_count), K(has_sstable_count));
    } else if (related_count == has_sstable_count) {
      ObCheckAllReplicaMajorSSTableExistResult result;
      ObCheckAllReplicaMajorSSTableExistArg arg;
      const common::ObAddr& dst_server = leader_replica->server_;
      arg.pkey_ = pkey;
      arg.index_id_ = pkey.get_table_id();
      const int64_t timeout = 100 * 1000000;
      int tmp_ret =
          rpc_proxy_->to(dst_server).timeout(timeout).check_all_replica_major_sstable_exist_with_time(arg, result);
      if (OB_EAGAIN == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret || OB_NOT_MASTER == tmp_ret || OB_TIMEOUT == tmp_ret) {
        // a white list of ret code which can be retryed
        all_replica_sstable_stat = ARSS_COPY_MULTI_REPLICA_RETRY;
      } else if (OB_SUCCESS == tmp_ret) {
        if (result.max_timestamp_ > major_sstable_exist_reply_ts) {
          major_sstable_exist_reply_ts = result.max_timestamp_;
        }
        all_replica_sstable_stat = ARSS_ALL_REPLICA_FINISH;
      } else {
        all_replica_sstable_stat = ARSS_INVALID;
        LOG_WARN("check all replica major sstable exist with time", K(ret));
      }
    } else if (has_sstable_full_replica_count > 0) {
      all_replica_sstable_stat = ARSS_COPY_MULTI_REPLICA_RETRY;
    } else {
      all_replica_sstable_stat = ARSS_BUILD_SINGLE_REPLICA_RETRY;
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::build_replica_sstable_copy_task(
    PartitionSSTableBuildStat& part_sstable_build_stat, share::ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (CMRS_IDLE != part_sstable_build_stat.copy_multi_replica_stat_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part sstable build stat unexpected", K(ret));
  } else {
    ObPartitionReplica* src_replica = NULL;
    ObPartitionReplica* dst_replica = NULL;
    for (int64_t i = 0;
         (NULL == src_replica || NULL == dst_replica) && OB_SUCC(ret) && i < partition_info.get_replicas_v2().count();
         ++i) {
      ObPartitionReplica& this_replica = partition_info.get_replicas_v2().at(i);
      if (REPLICA_TYPE_FULL == this_replica.replica_type_ && this_replica.is_in_service() &&
          this_replica.data_version_ > 0 && NULL == src_replica) {
        src_replica = &this_replica;
      }
      if (REPLICA_TYPE_FULL == this_replica.replica_type_ && this_replica.is_in_service() &&
          this_replica.data_version_ <= 0 && NULL == dst_replica) {
        dst_replica = &this_replica;
      }
      if (REPLICA_TYPE_READONLY == this_replica.replica_type_ && this_replica.data_version_ <= 0 &&
          NULL == dst_replica) {
        dst_replica = &this_replica;
      }
    }
    if (NULL == dst_replica || NULL == src_replica) {
      // bypass
    } else {
      ObCopySSTableTaskInfo task_info;
      common::ObArray<ObCopySSTableTaskInfo> task_info_array;
      ObCopySSTableTask task;
      ObReplicaMember data_src = ObReplicaMember(src_replica->server_,
          src_replica->member_time_us_,
          src_replica->replica_type_,
          src_replica->get_memstore_percent());
      int64_t data_size = src_replica->required_size_;
      OnlineReplica dst;
      dst.member_ = ObReplicaMember(dst_replica->server_,
          ObTimeUtility::current_time(),
          dst_replica->replica_type_,
          dst_replica->get_memstore_percent());
      dst.unit_id_ = dst_replica->unit_id_;
      dst.zone_ = dst_replica->zone_;
      common::ObRegion dst_region = DEFAULT_REGION_NAME;
      if (OB_SUCCESS != zone_mgr_->get_region(dst.zone_, dst_region)) {
        dst_region = DEFAULT_REGION_NAME;
      }
      const char* comment = balancer::COPY_GLOBAL_INDEX_SSTABLE;
      const ObCopySSTableType type = OB_COPY_SSTABLE_TYPE_GLOBAL_INDEX;
      if (FALSE_IT(task_info.set_transmit_data_size(data_size))) {
        // will never be here
      } else if (OB_FAIL(task_info.build(part_sstable_build_stat.pkey_, dst, data_src))) {
        LOG_WARN("fail to build copy sstable info", K(ret));
      } else if (FALSE_IT(task_info.set_cluster_id(GCONF.cluster_id))) {
        // will never be here
      } else if (OB_FAIL(task_info_array.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(task.build(task_info_array, type, dst_replica->server_, comment))) {
        LOG_WARN("fail to build copy effect index sstable info", K(ret));
      } else if (OB_UNLIKELY(NULL == rebalance_task_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rebalance task mgr is null", K(ret));
      } else {
        ret = rebalance_task_mgr_->add_task(task);
        if (OB_ENTRY_EXIST == ret) {
          part_sstable_build_stat.copy_multi_replica_stat_ = CMRS_COPY_TASK_EXIST;
          LOG_WARN("same pkey task exist", K(ret), K(task));
        } else if (OB_SUCCESS == ret) {
          part_sstable_build_stat.copy_multi_replica_stat_ = CMRS_COPY_TASK_EXIST;
          LOG_INFO("copy effect index sstable", K(ret), K(task));
        } else {
          LOG_WARN("fail to add task", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::drive_this_copy_multi_replica(const share::schema::ObTableSchema* index_schema,
    share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    bool all_copy_finish = true;
    int64_t major_sstable_exist_reply_ts = 0;
    const bool need_fail_list = false;
    const int64_t cluster_id = OB_INVALID_ID;  // local cluster
    const bool filter_flag_replica = false;
    common::ObArray<PartitionSSTableBuildStat> partition_sstable_stat_array;
    {
      SpinWLockGuard item_guard(task->lock_);
      if (OB_FAIL(partition_sstable_stat_array.assign(task->partition_sstable_stat_array_))) {
        LOG_WARN("fail to assign partition sstable stat", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_sstable_stat_array.count(); ++i) {
      PartitionSSTableBuildStat& item = partition_sstable_stat_array.at(i);
      if (!item.pkey_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey is invalid", K(ret), "pkey", item.pkey_);
      } else if (CMRS_SUCCEED == item.copy_multi_replica_stat_) {
        // by pass, this partition has finish copy
      } else if (CMRS_COPY_TASK_EXIST == item.copy_multi_replica_stat_) {
        // by pass, has copy task exist
        all_copy_finish = false;
      } else if (CMRS_IDLE == item.copy_multi_replica_stat_) {
        all_copy_finish = false;
        AllReplicaSSTableStat all_replica_sstable_stat = ARSS_INVALID;
        const ObPartitionKey& pkey = item.pkey_;
        ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
        ObPartitionInfo info;
        info.set_allocator(&allocator);
        ObReplicaFilterHolder filter;
        if (!pkey.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pkey invalid", K(ret), K(pkey));
        } else if (OB_FAIL(pt_operator_->get(pkey.get_table_id(),
                       pkey.get_partition_id(),
                       info,
                       need_fail_list,
                       cluster_id,
                       filter_flag_replica))) {
          LOG_WARN("fail to get partition info", K(ret), K(pkey));
        } else if (OB_FAIL(filter.set_persistent_replica_status_not_equal(REPLICA_STATUS_OFFLINE))) {
          LOG_WARN("fail to set replica status", K(ret));
        } else if (OB_FAIL(filter.set_filter_log_replica())) {
          LOG_WARN("fail to set filter log replica", K(ret));
        } else if (OB_FAIL(info.filter(filter))) {
          LOG_WARN("fail to do info filter", K(ret));
        } else if (OB_FAIL(check_partition_copy_replica_stat(
                       major_sstable_exist_reply_ts, info, all_replica_sstable_stat))) {
          LOG_WARN("fail to check partition copy replica", K(ret));
        } else if (ARSS_ALL_REPLICA_FINISH == all_replica_sstable_stat) {
          SpinWLockGuard item_guard(task->lock_);
          if (i >= task->partition_sstable_stat_array_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("task partition sstable stat array count unexpected", K(ret));
          } else {
            PartitionSSTableBuildStat& this_item = task->partition_sstable_stat_array_.at(i);
            this_item.copy_multi_replica_stat_ = CMRS_SUCCEED;
          }
        } else if (ARSS_BUILD_SINGLE_REPLICA_RETRY == all_replica_sstable_stat) {
          SpinWLockGuard item_guard(task->lock_);
          (void)switch_state(task, GIBS_BUILD_SINGLE_REPLICA);
          break;
        } else if (ARSS_COPY_MULTI_REPLICA_RETRY == all_replica_sstable_stat) {
          SpinWLockGuard item_guard(task->lock_);
          if (i >= task->partition_sstable_stat_array_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("task partition sstable stat array count unexpected", K(ret));
          } else if (OB_FAIL(build_replica_sstable_copy_task(task->partition_sstable_stat_array_.at(i), info))) {
            LOG_WARN("fail to build replica sstable copy task", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("all replica sstable stat unexpected", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy multi replica stat unexpected", K(ret), "stat", item.copy_multi_replica_stat_);
      }
    }
    if (OB_SUCC(ret)) {
      SpinWLockGuard item_guard(task->lock_);
      if (major_sstable_exist_reply_ts > task->major_sstable_exist_reply_ts_) {
        task->major_sstable_exist_reply_ts_ = major_sstable_exist_reply_ts;
      }
    }
    if (OB_SUCC(ret) && all_copy_finish) {
      SpinWLockGuard item_guard(task->lock_);
      if (index_schema->is_unique_index()) {
        (void)switch_state(task, GIBS_UNIQUE_INDEX_CALC_CHECKSUM);
      } else {
        (void)switch_state(task, GIBS_INDEX_BUILD_TAKE_EFFECT);
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::launch_new_copy_multi_replica(const share::schema::ObTableSchema* index_schema,
    share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard item_guard(task->lock_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == index_schema || NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_schema), KP(task));
  } else if (OB_FAIL(build_task_partition_sstable_stat(schema_guard, index_schema, task))) {
    LOG_WARN("fail to build task partition sstable stat", K(ret));
  } else {
    task->last_drive_ts_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObGlobalIndexBuilder::generate_task_partition_sstable_array(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == index_schema || NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_schema), KP(task));
  } else if (OB_UNLIKELY(NULL == pt_operator_ || NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_operator_ or mysql_proxy_ is null", K(ret), KP(pt_operator_), KP(mysql_proxy_));
  } else {
    task->partition_sstable_stat_array_.reset();
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter partition_key_iter(*index_schema, check_dropped_schema);
    ObPartitionKey pkey;
    while (OB_SUCC(ret) && OB_SUCC(partition_key_iter.next_partition_key_v2(pkey))) {
      if (!pkey.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey is invalid", K(ret), K(pkey), "index_table_id", index_schema->get_table_id());
      } else {
        PartitionSSTableBuildStat item;
        item.pkey_ = pkey;
        item.copy_multi_replica_stat_ = CMRS_IDLE;
        if (OB_FAIL(task->partition_sstable_stat_array_.push_back(item))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more to do
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (task->partition_sstable_stat_array_.count() != index_schema->get_all_part_num()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part num and sstable stat array count not match",
            K(ret),
            "index_id",
            index_schema->get_table_id(),
            "left_cnt",
            task->partition_sstable_stat_array_.count(),
            "right_cnt",
            index_schema->get_all_part_num());
        (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::build_task_partition_sstable_stat(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  common::ObArray<PartitionServer> partition_server_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == index_schema || NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_schema), KP(task));
  } else if (OB_FAIL(generate_task_partition_sstable_array(schema_guard, index_schema, task))) {
    LOG_WARN("fail to generate task partition sstable array", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObGlobalIndexBuilder::try_copy_multi_replica(ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_COPY_GLOBAL_INDEX);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_service_ || NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else {
    uint64_t index_table_id = OB_INVALID_ID;
    uint64_t tenant_id = OB_INVALID_ID;
    int64_t last_drive_ts = -1;
    GlobalIndexBuildStatus build_status = GIBS_INVALID;
    {
      SpinWLockGuard item_guard(task->lock_);
      index_table_id = task->index_table_id_;
      tenant_id = extract_tenant_id(index_table_id);
      build_status = task->status_;
      last_drive_ts = task->last_drive_ts_;
    }
    const ObTableSchema* index_schema = NULL;
    share::schema::ObSchemaGetterGuard schema_guard;
    bool index_schema_exist = false;
    int64_t version_in_inner_table = OB_INVALID_VERSION;
    // strong consistency is used on primary cluster
    // since ObGlobalIndexBuilder is not used on standby cluster
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = extract_tenant_id(index_table_id);
    int64_t local_schema_version = OB_INVALID_VERSION;
    if (GCTX.is_standby_cluster()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("create global index in slave cluster is not allowed", K(ret), K(index_table_id));
    } else if (GIBS_MULTI_REPLICA_COPY != build_status) {
      // may have been modify by others
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_schema_version))) {
      LOG_WARN("fail to get schema version from guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(
                   *mysql_proxy_, schema_status, version_in_inner_table))) {
      LOG_WARN("fail to get version in inner table", K(ret));
    } else if (version_in_inner_table > local_schema_version) {
      // by pass, this server may not get the newest schema
    } else if (OB_FAIL(check_and_get_index_schema(schema_guard, index_table_id, index_schema, index_schema_exist))) {
      LOG_WARN("fail to get table schema", K(ret), K(index_table_id));
    } else if (!index_schema_exist) {
      SpinWLockGuard item_guard(task->lock_);
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (OB_UNLIKELY(NULL == index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema ptr is null", K(ret));
    } else if (is_error_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
      SpinWLockGuard item_guard(task->lock_);
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (is_available_index_status(index_schema->get_index_status())) {
      SpinWLockGuard item_guard(task->lock_);
      (void)switch_state(task, GIBS_INDEX_BUILD_FINISH);
    } else if (last_drive_ts + COPY_MULTI_REPLICA_TIMEOUT > ObTimeUtility::current_time()) {
      if (OB_FAIL(drive_this_copy_multi_replica(index_schema, schema_guard, task))) {
        LOG_WARN("fail to drive this copy multi replica", K(ret));
      }
    } else {
      if (OB_FAIL(launch_new_copy_multi_replica(index_schema, schema_guard, task))) {
        LOG_WARN("fail to launch new copy multi replica", K(ret));
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::send_check_unique_index_rpc(const share::schema::ObTableSchema* index_schema,
    ObGlobalIndexTask* task, const common::ObPartitionKey& pkey, const share::ObPartitionReplica* replica)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == index_schema || NULL == task || !pkey.is_valid() || NULL == replica)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), KP(index_schema), KP(task), KP(replica));
  } else if (NULL == rpc_proxy_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy ptr is null", K(ret));
  } else {
    ObCheckUniqueIndexRequestArg arg;
    arg.pkey_ = pkey;
    arg.index_id_ = index_schema->get_table_id();
    arg.schema_version_ = task->schema_version_;
    if (OB_FAIL(rpc_proxy_->to(replica->server_).check_unique_index_request(arg))) {
      LOG_WARN("fail to check unique index request", K(ret), K(arg));
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::drive_this_unique_index_calc_checksum(const share::schema::ObTableSchema* index_schema,
    const share::schema::ObTableSchema* data_schema, share::schema::ObSchemaGetterGuard& schema_guard,
    ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  UNUSED(data_schema);
  UNUSED(schema_guard);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == pt_operator_ || NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else {
    common::ObArray<int64_t> failed_idx_array;
    common::ObArray<int64_t> succeed_idx_array;
    common::ObArray<int64_t> not_master_id_array;
    int64_t schema_version = -1;
    int64_t checksum_snapshot = -1;
    int64_t snapshot = -1;
    common::ObArray<PartitionColChecksumStat> partition_col_checksum_stat_array;
    {
      SpinRLockGuard item_guard(task->lock_);
      schema_version = task->schema_version_;
      checksum_snapshot = task->checksum_snapshot_;
      snapshot = task->snapshot_;
      if (OB_FAIL(partition_col_checksum_stat_array.assign(task->partition_col_checksum_stat_array_))) {
        LOG_WARN("fail to assign partition col checksum stat", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_col_checksum_stat_array.count(); ++i) {
      const PartitionColChecksumStat& item = partition_col_checksum_stat_array.at(i);
      if (CCS_FAILED == item.col_checksum_stat_) {
        if (OB_FAIL(failed_idx_array.push_back(i))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else if (CCS_SUCCEED == item.col_checksum_stat_) {
        if (OB_FAIL(succeed_idx_array.push_back(i))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else if (CCS_NOT_MASTER == item.col_checksum_stat_) {
        if (OB_FAIL(not_master_id_array.push_back(i))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        // do nothing to UCS_INVALID, do not get a reply
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (failed_idx_array.count() > 0) {
      SpinWLockGuard item_guard(task->lock_);
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (succeed_idx_array.count() >= task->partition_col_checksum_stat_array_.count()) {
      SpinWLockGuard item_guard(task->lock_);
      (void)switch_state(task, GIBS_UNIQUE_INDEX_CHECK);
    } else if (not_master_id_array.count() > 0) {
      uint64_t execution_id = OB_INVALID_ID;
      if (OB_FAIL(sql::ObIndexSSTableBuilder::query_execution_id(
              execution_id, index_schema->get_table_id(), snapshot, *mysql_proxy_))) {
        LOG_WARN("fail to query execution id", K(ret), "index_table_id", index_schema->get_table_id());
        SpinWLockGuard item_guard(task->lock_);
        (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
      } else if (OB_INVALID_ID == execution_id) {
        LOG_WARN("unexpected execution id", K(ret), "index_table_id", index_schema->get_table_id());
        SpinRLockGuard item_guard(task->lock_);
        (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < not_master_id_array.count(); ++i) {
        int64_t idx = not_master_id_array.at(i);
        if (idx >= partition_col_checksum_stat_array.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index unexpected", K(ret), K(idx), "array count", partition_col_checksum_stat_array.count());
        } else {
          const ObPGKey& pgkey = partition_col_checksum_stat_array.at(idx).pgkey_;
          const ObPartitionKey& pkey = partition_col_checksum_stat_array.at(idx).pkey_;
          ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
          ObPartitionInfo info;
          const ObPartitionReplica* leader_replica = NULL;
          ObReplicaFilterHolder filter;
          info.set_allocator(&allocator);
          if (!pkey.is_valid() || !pgkey.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pkey invalid", K(ret), K(pkey), K(pgkey));
          } else if (OB_FAIL(pt_operator_->get(pgkey.get_table_id(), pgkey.get_partition_id(), info))) {
            LOG_WARN("fail to get partition info", K(ret), K(pkey));
          } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
            LOG_WARN("fail to set replica status", K(ret));
          } else if (OB_FAIL(filter.set_in_member_list())) {
            LOG_WARN("fail to do filter", K(ret));
          } else if (OB_FAIL(info.filter(filter))) {
            LOG_WARN("fail to do filter", K(ret));
          } else if (OB_FAIL(info.find_leader_v2(leader_replica))) {
            LOG_WARN("fail to get leader v2", K(ret));
          } else if (OB_FAIL(send_col_checksum_calc_rpc(
                         index_schema, schema_version, checksum_snapshot, execution_id, pkey, leader_replica))) {
            LOG_WARN("fail to send rpc", K(ret));
          }
        }
      }
    } else {
    }  // wait, until all reply
  }
  return ret;
}

int ObGlobalIndexBuilder::launch_new_unique_index_check(const share::schema::ObTableSchema* index_schema,
    share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(build_task_partition_unique_stat(index_schema, task))) {
    LOG_WARN("fail to build task partition item", K(ret));
  } else if (OB_FAIL(send_check_unique_index_request(index_schema, task))) {
    LOG_WARN("fail to send check unique index request", K(ret));
  } else {
    task->last_drive_ts_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObGlobalIndexBuilder::drive_this_unique_index_check(const share::schema::ObTableSchema* index_schema,
    share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == pt_operator_ || NULL == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else {
    common::ObArray<int64_t> illegal_idx_array;
    common::ObArray<int64_t> succeed_idx_array;
    common::ObArray<int64_t> not_master_id_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < task->partition_unique_stat_array_.count(); ++i) {
      const PartitionUniqueStat& item = task->partition_unique_stat_array_.at(i);
      if (UCS_ILLEGAL == item.unique_check_stat_) {
        if (OB_FAIL(illegal_idx_array.push_back(i))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else if (UCS_SUCCEED == item.unique_check_stat_) {
        if (OB_FAIL(succeed_idx_array.push_back(i))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else if (UCS_NOT_MASTER == item.unique_check_stat_) {
        if (OB_FAIL(not_master_id_array.push_back(i))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        // do nothing to UCS_INVALID, do not get a reply
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (illegal_idx_array.count() > 0) {
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (succeed_idx_array.count() >= task->partition_unique_stat_array_.count()) {
      (void)switch_state(task, GIBS_UNIQUE_INDEX_CHECK);
    } else if (not_master_id_array.count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < not_master_id_array.count(); ++i) {
        int64_t idx = not_master_id_array.at(i);
        if (idx >= task->partition_unique_stat_array_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index unexpected", K(ret), K(idx), "array count", task->partition_unique_stat_array_.count());
        } else {
          const ObPartitionKey& pkey = task->partition_unique_stat_array_.at(idx).pkey_;
          ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
          ObPartitionInfo info;
          const ObPartitionReplica* leader_replica = NULL;
          ObReplicaFilterHolder filter;
          info.set_allocator(&allocator);
          if (!pkey.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pkey invalid", K(ret), K(pkey));
          } else if (OB_FAIL(pt_operator_->get(pkey.get_table_id(), pkey.get_partition_id(), info))) {
            LOG_WARN("fail to get partition info", K(ret), K(pkey));
          } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
            LOG_WARN("fail to set replica status", K(ret));
          } else if (OB_FAIL(filter.set_in_member_list())) {
            LOG_WARN("fail to do filter", K(ret));
          } else if (OB_FAIL(info.filter(filter))) {
            LOG_WARN("fail to do filter", K(ret));
          } else if (OB_FAIL(info.find_leader_v2(leader_replica))) {
            LOG_WARN("fail to get leader v2", K(ret));
          } else if (OB_FAIL(send_check_unique_index_rpc(index_schema, task, pkey, leader_replica))) {
            LOG_WARN("fail to send rpc", K(ret));
          }
        }
      }
    } else {
    }  // wait
  }
  return ret;
}

int ObGlobalIndexBuilder::get_checksum_calculation_snapshot(ObGlobalIndexTask* task,
    share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema* index_schema,
    int64_t& checksum_snapshot)
{
  int ret = OB_SUCCESS;
  common::ObArray<PartitionServer> partition_leader_array;
  const share::schema::ObTableSchema *data_schema = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == index_schema || NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (task->major_sstable_exist_reply_ts_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task major sstable reply ts is less than 0", K(ret), "task", *task);
  } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), data_schema))) {
    LOG_WARN("fail to get table schema", K(ret), "table_id", index_schema->get_data_table_id());
  } else if (OB_UNLIKELY(NULL == data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("data schema not exist", K(ret), "table_id", index_schema->get_data_table_id());
  } else if (OB_FAIL(
                 generate_original_table_partition_leader_array(schema_guard, data_schema, partition_leader_array))) {
    LOG_WARN("fail to generate original table partition leader array", K(ret));
  } else {
    ObCheckCtxCreateTimestampElapsedProxy proxy(*rpc_proxy_, &ObSrvRpcProxy::check_ctx_create_timestamp_elapsed);
    ObCheckCtxCreateTimestampElapsedArg arg;
    if (OB_FAIL(do_get_associated_snapshot(
            proxy, arg, task, data_schema->get_all_part_num(), partition_leader_array, checksum_snapshot))) {
      LOG_WARN("fail to do get checksum calculation snapshot", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObGlobalIndexBuilder::build_task_partition_col_checksum_stat(const share::schema::ObTableSchema* index_schema,
    const share::schema::ObTableSchema* data_schema, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard item_guard(task->lock_);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema || NULL == data_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task), KP(index_schema), KP(data_schema));
  } else {
    // build index table column checksum
    common::ObArray<PartitionColChecksumStat> tmp_checksum_stat_array;
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter index_partition_key_iter(*index_schema, check_dropped_schema);
    ObPartitionKey index_pkey;
    while (OB_SUCC(ret) && OB_SUCC(index_partition_key_iter.next_partition_key_v2(index_pkey))) {
      int tmp_ret = OB_SUCCESS;
      PartitionColChecksumStat* part_col_cs_stat = nullptr;
      if (OB_UNLIKELY(!index_pkey.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey is invalid", K(ret));
      } else if (FALSE_IT(tmp_ret = task->get_partition_col_checksum_stat(index_pkey, part_col_cs_stat))) {
        // shall never be here
      } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        PartitionColChecksumStat partition_col_checksum_stat;
        partition_col_checksum_stat.pkey_ = index_pkey;
        partition_col_checksum_stat.pgkey_ = index_pkey;
        partition_col_checksum_stat.col_checksum_stat_ = CCS_INVALID;
        partition_col_checksum_stat.snapshot_ = -1;
        if (OB_FAIL(tmp_checksum_stat_array.push_back(partition_col_checksum_stat))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to get partition col checksum stat", K(ret), "pkey", index_pkey);
      } else if (OB_UNLIKELY(nullptr == part_col_cs_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition col checksum stat ptr is null", K(ret));
      } else {
        if (OB_FAIL(tmp_checksum_stat_array.push_back(*part_col_cs_stat))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }

    // build data table column checksum
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (data_schema->is_binding_table()) {
      bool check_dropped_schema = false;
      ObTablePgKeyIter pg_table_iter(*data_schema, data_schema->get_tablegroup_id(), check_dropped_schema);
      if (OB_FAIL(pg_table_iter.init())) {
        LOG_WARN("fail to init iter", K(ret));
      } else {
        common::ObPartitionKey pkey;
        common::ObPGKey pgkey;
        while (OB_SUCC(ret) && OB_SUCC(pg_table_iter.next(pkey, pgkey))) {
          int tmp_ret = OB_SUCCESS;
          PartitionColChecksumStat* part_col_cs_stat = nullptr;
          if (OB_UNLIKELY(!pkey.is_valid() || !pgkey.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pkey or pgkey invalid", K(ret), K(pkey), K(pgkey));
          } else if (FALSE_IT(tmp_ret = task->get_partition_col_checksum_stat(pkey, part_col_cs_stat))) {
            // shall never be here
          } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
            PartitionColChecksumStat partition_col_checksum_stat;
            partition_col_checksum_stat.pkey_ = pkey;
            partition_col_checksum_stat.pgkey_ = pgkey;
            partition_col_checksum_stat.col_checksum_stat_ = CCS_INVALID;
            partition_col_checksum_stat.snapshot_ = -1;
            if (OB_FAIL(tmp_checksum_stat_array.push_back(partition_col_checksum_stat))) {
              LOG_WARN("fail to push back", K(ret));
            }
          } else if (OB_SUCCESS != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("fail to get partition col checksum stat", K(ret), "pkey", pkey);
          } else if (OB_UNLIKELY(nullptr == part_col_cs_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition col checksum stat ptr is null", K(ret));
          } else {
            if (OB_FAIL(tmp_checksum_stat_array.push_back(*part_col_cs_stat))) {
              LOG_WARN("fail to push back", K(ret));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    } else {
      ObTablePartitionKeyIter data_partition_key_iter(*data_schema, check_dropped_schema);
      ObPartitionKey data_pkey;
      while (OB_SUCC(ret) && OB_SUCC(data_partition_key_iter.next_partition_key_v2(data_pkey))) {
        int tmp_ret = OB_SUCCESS;
        PartitionColChecksumStat* part_col_cs_stat = nullptr;
        if (OB_UNLIKELY(!data_pkey.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data_pkey is invalid", K(ret));
        } else if (FALSE_IT(tmp_ret = task->get_partition_col_checksum_stat(data_pkey, part_col_cs_stat))) {
          // shall never be here
        } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
          PartitionColChecksumStat partition_col_checksum_stat;
          partition_col_checksum_stat.pkey_ = data_pkey;
          partition_col_checksum_stat.pgkey_ = data_pkey;
          partition_col_checksum_stat.col_checksum_stat_ = CCS_INVALID;
          partition_col_checksum_stat.snapshot_ = -1;
          if (OB_FAIL(tmp_checksum_stat_array.push_back(partition_col_checksum_stat))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to get partition col checksum stat", K(ret), "pkey", data_pkey);
        } else if (OB_UNLIKELY(nullptr == part_col_cs_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition col checksum stat ptr is null", K(ret));
        } else {
          if (OB_FAIL(tmp_checksum_stat_array.push_back(*part_col_cs_stat))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      task->partition_col_checksum_stat_array_.reset();
      if (OB_FAIL(task->partition_col_checksum_stat_array_.assign(tmp_checksum_stat_array))) {
        LOG_WARN("fail to assign partition col checksum stat array", K(ret));
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::send_col_checksum_calc_rpc(const share::schema::ObTableSchema* index_schema,
    const int64_t schema_version, const int64_t checksum_snapshot, const uint64_t execution_id,
    const common::ObPartitionKey& pkey, const share::ObPartitionReplica* replica)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == index_schema || schema_version < 0 || checksum_snapshot < 0 || !pkey.is_valid() ||
                         NULL == replica || OB_INVALID_ID == execution_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), KP(replica));
  } else if (NULL == rpc_proxy_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy ptr is null", K(ret));
  } else {
    ObCalcColumnChecksumRequestArg arg;
    arg.pkey_ = pkey;
    arg.index_id_ = index_schema->get_table_id();
    arg.schema_version_ = schema_version;
    arg.execution_id_ = execution_id;
    arg.snapshot_version_ = checksum_snapshot;
    if (OB_FAIL(rpc_proxy_->to(replica->server_).calc_column_checksum_request(arg))) {
      LOG_WARN("fail to calc column checksum request", K(ret));
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::send_checksum_calculation_request(
    const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  uint64_t execution_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == pt_operator_ || NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else {
    int64_t snapshot = -1;
    {
      SpinRLockGuard item_guard(task->lock_);
      snapshot = task->snapshot_;
    }
    if (OB_FAIL(sql::ObIndexSSTableBuilder::query_execution_id(
            execution_id, index_schema->get_table_id(), snapshot, *mysql_proxy_))) {
      LOG_WARN("fail to query execution id", K(ret), "index_table_id", index_schema->get_table_id());
      SpinWLockGuard item_guard(task->lock_);
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (OB_INVALID_ID == execution_id) {
      LOG_WARN("unexpected execution id", K(ret), "index_table_id", index_schema->get_table_id());
      SpinWLockGuard item_guard(task->lock_);
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    }
  }

  if (OB_SUCC(ret)) {
    int64_t schema_version = -1;
    int64_t checksum_snapshot = -1;
    common::ObArray<PartitionColChecksumStat> partition_col_checksum_stat_array;
    {
      SpinRLockGuard item_guard(task->lock_);
      schema_version = task->schema_version_;
      checksum_snapshot = task->checksum_snapshot_;
      if (OB_FAIL(partition_col_checksum_stat_array.assign(task->partition_col_checksum_stat_array_))) {
        LOG_WARN("fail to assign partition col checksum stat", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_col_checksum_stat_array.count(); ++i) {
      const ObPGKey& pgkey = partition_col_checksum_stat_array.at(i).pgkey_;
      const ObPartitionKey& pkey = partition_col_checksum_stat_array.at(i).pkey_;
      PartitionColChecksumStat& item = partition_col_checksum_stat_array.at(i);
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      ObPartitionInfo info;
      const ObPartitionReplica* leader_replica = NULL;
      ObReplicaFilterHolder filter;
      info.set_allocator(&allocator);
      if (!pkey.is_valid() || !pgkey.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey invalid", K(ret), K(pkey), K(pgkey));
      } else if (item.snapshot_ == checksum_snapshot && CCS_SUCCEED == item.col_checksum_stat_) {
        // already done
      } else if (OB_FAIL(pt_operator_->get(pgkey.get_table_id(), pgkey.get_partition_id(), info))) {
        LOG_WARN("fail to get partition info", K(ret), K(pkey));
      } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
        LOG_WARN("fail to set replica status", K(ret));
      } else if (OB_FAIL(filter.set_in_member_list())) {
        LOG_WARN("fail to set in member list", K(ret));
      } else if (OB_FAIL(info.filter(filter))) {
        LOG_WARN("fail to do filter", K(ret));
      } else if (OB_FAIL(info.find_leader_v2(leader_replica))) {
        LOG_WARN("fail to get leader v2", K(ret));
      } else if (OB_FAIL(send_col_checksum_calc_rpc(
                     index_schema, schema_version, checksum_snapshot, execution_id, pkey, leader_replica))) {
        LOG_WARN("fail to send rpc", K(ret));
      } else {
        SpinWLockGuard item_guard(task->lock_);
        if (i >= task->partition_col_checksum_stat_array_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task partition col checksum count unexpected", K(ret));
        } else {
          PartitionColChecksumStat& this_item = task->partition_col_checksum_stat_array_.at(i);
          this_item.snapshot_ = task->checksum_snapshot_;
        }
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::do_checksum_calculation(ObGlobalIndexTask* task,
    const share::schema::ObTableSchema* index_schema, const share::schema::ObTableSchema* data_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema || NULL == data_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret));
  } else if (OB_FAIL(build_task_partition_col_checksum_stat(index_schema, data_schema, task))) {
    LOG_WARN("fail to build task partition column checksum stat", K(ret));
  } else if (OB_FAIL(send_checksum_calculation_request(index_schema, task))) {
    LOG_WARN("fail to send check unique index request", K(ret));
  } else {
    SpinWLockGuard item_guard(task->lock_);
    task->last_drive_ts_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObGlobalIndexBuilder::launch_new_unique_index_calc_checksum(const share::schema::ObTableSchema* index_schema,
    const share::schema::ObTableSchema* data_schema, share::schema::ObSchemaGetterGuard& schema_guard,
    ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema || NULL == data_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    {
      SpinWLockGuard item_guard(task->lock_);
      int64_t checksum_snapshot = 0;
      if (0 != task->checksum_snapshot_) {
        // checksum_snapshot value already exists
      } else if (OB_UNLIKELY(task->major_sstable_exist_reply_ts_ <= 0)) {
        (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
      } else if (OB_FAIL(get_checksum_calculation_snapshot(task, schema_guard, index_schema, checksum_snapshot))) {
        LOG_WARN("fail to get checksum calculation snapshot", K(ret));
      } else {
        task->checksum_snapshot_ = checksum_snapshot;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_checksum_calculation(task, index_schema, data_schema))) {
        LOG_WARN("fail to do checksum calculation", K(ret));
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::try_unique_index_calc_checksum(ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_CHECK_GLOBAL_UNIQUE_INDEX);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_service_ || NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else {
    uint64_t index_table_id = OB_INVALID_ID;
    uint64_t tenant_id = OB_INVALID_ID;
    int64_t last_drive_ts = -1;
    GlobalIndexBuildStatus build_status = GIBS_INVALID;
    {
      SpinWLockGuard item_guard(task->lock_);
      index_table_id = task->index_table_id_;
      tenant_id = extract_tenant_id(index_table_id);
      build_status = task->status_;
      last_drive_ts = task->last_drive_ts_;
    }
    const ObTableSchema* index_schema = NULL;
    const ObTableSchema* data_schema = NULL;
    share::schema::ObSchemaGetterGuard schema_guard;
    bool index_schema_exist = false;
    int64_t version_in_inner_table = OB_INVALID_VERSION;
    // strong consistency is used on primary cluster
    // since ObGlobalIndexBuilder is not used on standby cluster
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = extract_tenant_id(index_table_id);
    int64_t local_schema_version = OB_INVALID_VERSION;
    if (GCTX.is_standby_cluster()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("create global index in slave cluster is not allowed", K(ret), K(index_table_id));
    } else if (GIBS_UNIQUE_INDEX_CALC_CHECKSUM != build_status) {
      // may have been modify by others
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_schema_version))) {
      LOG_WARN("fail to get schema version from guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(
                   *mysql_proxy_, schema_status, version_in_inner_table))) {
      LOG_WARN("fail to get version in inner table", K(ret));
    } else if (version_in_inner_table > local_schema_version) {
      // by pass, this server may not get the newest schema
    } else if (OB_FAIL(check_and_get_index_schema(schema_guard, index_table_id, index_schema, index_schema_exist))) {
      LOG_WARN("fail to get table schema", K(ret), K(index_table_id));
    } else if (!index_schema_exist) {
      SpinWLockGuard item_guard(task->lock_);
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (OB_UNLIKELY(NULL == index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema ptr is null", K(ret));
    } else if (is_error_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
      SpinWLockGuard item_guard(task->lock_);
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (is_available_index_status(index_schema->get_index_status())) {
      SpinWLockGuard item_guard(task->lock_);
      (void)switch_state(task, GIBS_INDEX_BUILD_FINISH);
    } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), data_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (OB_UNLIKELY(NULL == data_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data schema ptr is null", K(ret));
    } else if (last_drive_ts + UNIQUE_INDEX_CALC_CHECKSUM_TIMEOUT > ObTimeUtility::current_time()) {
      if (OB_FAIL(drive_this_unique_index_calc_checksum(index_schema, data_schema, schema_guard, task))) {
        LOG_WARN("fail to drive this unique index calc checksum", K(ret));
      }
    } else {
      if (OB_FAIL(launch_new_unique_index_calc_checksum(index_schema, data_schema, schema_guard, task))) {
        LOG_WARN("fail to launch new unique index calc checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::try_unique_index_check(ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_service_ || NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else {
    SpinWLockGuard item_guard(task->lock_);
    const uint64_t index_table_id = task->index_table_id_;
    const uint64_t tenant_id = extract_tenant_id(index_table_id);
    const ObTableSchema* index_schema = NULL;
    share::schema::ObSchemaGetterGuard schema_guard;
    bool index_schema_exist = false;
    // strong consistency is used on primary cluster
    // since ObGlobalIndexBuilder is not used on standby cluster
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = extract_tenant_id(index_table_id);
    int64_t version_in_inner_table = OB_INVALID_VERSION;
    int64_t local_schema_version = OB_INVALID_VERSION;
    if (GCTX.is_standby_cluster()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("create global index in slave cluster is not allowed", K(ret), K(index_table_id));
    } else if (GIBS_UNIQUE_INDEX_CHECK != task->status_) {
      // may have been modify by others
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(
                   *mysql_proxy_, schema_status, version_in_inner_table))) {
      LOG_WARN("fail to get version in inner table", K(ret));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_schema_version))) {
      LOG_WARN("fail to get schema version from guard", K(ret), K(tenant_id));
    } else if (version_in_inner_table > local_schema_version) {
      // by pass, this server may not get the newest schema
    } else if (OB_FAIL(check_and_get_index_schema(schema_guard, index_table_id, index_schema, index_schema_exist))) {
      LOG_WARN("fail to get table schema", K(ret), K(index_table_id));
    } else if (!index_schema_exist) {
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (OB_UNLIKELY(NULL == index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema ptr is null", K(ret));
    } else if (is_error_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
      (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
    } else if (is_available_index_status(index_schema->get_index_status())) {
      (void)switch_state(task, GIBS_INDEX_BUILD_FINISH);
    } else {
      uint64_t execution_id = OB_INVALID_ID;
      bool is_equal = false;
      if (OB_FAIL(sql::ObIndexSSTableBuilder::query_execution_id(
              execution_id, index_table_id, task->snapshot_, *mysql_proxy_))) {
        LOG_WARN("fail to query execution id", K(ret), K(index_table_id));
        (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
      } else if (OB_INVALID_ID == execution_id) {
        LOG_WARN("unexpected execution id", K(ret), K(index_table_id));
      } else if (OB_FAIL(ObIndexChecksumOperator::check_column_checksum(
                     execution_id, index_schema->get_data_table_id(), index_table_id, is_equal, *mysql_proxy_))) {
        LOG_WARN(
            "fail to check column checksum", K(ret), K(index_table_id), "table_id", index_schema->get_data_table_id());
        (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
      } else if (!is_equal) {
        LOG_WARN("column checksum not equal", K(index_table_id), "table_id", index_schema->get_data_table_id());
        (void)switch_state(task, GIBS_INDEX_BUILD_FAILED);
      } else {
        (void)switch_state(task, GIBS_INDEX_BUILD_TAKE_EFFECT);
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::pick_data_replica(
    const common::ObPartitionKey& pkey, const common::ObIArray<common::ObAddr>& previous, common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema* data_schema = nullptr;
  common::ObPartitionKey phy_part_key;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else if (OB_UNLIKELY(NULL == pt_operator_ || NULL == server_mgr_ || NULL == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt operator ptr is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(pkey.get_table_id(), data_schema))) {
    LOG_WARN("fail to get table schema", K(ret), "table_id", pkey.get_table_id());
  } else if (OB_UNLIKELY(nullptr == data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), "table_id", pkey.get_table_id());
  } else if (data_schema->is_binding_table()) {
    if (OB_FAIL(data_schema->get_pg_key(pkey, phy_part_key))) {
      LOG_WARN("fail to get pg key", K(ret), K(pkey));
    }
  } else {
    phy_part_key = pkey;
  }
  if (OB_SUCC(ret)) {
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    ObPartitionInfo info;
    ObReplicaFilterHolder filter;
    info.set_allocator(&allocator);
    if (OB_FAIL(pt_operator_->get(phy_part_key.get_table_id(), phy_part_key.get_partition_id(), info))) {
      LOG_WARN("fail to get partition info", K(ret), K(phy_part_key));
    } else if (OB_FAIL(filter.set_only_alive_server(*server_mgr_))) {
      LOG_WARN("set filter failed", K(ret));
    } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
      LOG_WARN("fail to set replica status", K(ret));
    } else if (OB_FAIL(filter.set_in_member_list())) {
      LOG_WARN("fail to do filter", K(ret));
    } else if (OB_FAIL(filter.set_filter_log_replica())) {
      LOG_WARN("fail to set filter log replica", K(ret));
    } else if (OB_FAIL(info.filter(filter))) {
      LOG_WARN("fail to do filter", K(ret));
    } else if (info.get_replicas_v2().count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("info replica count zero", K(ret), K(phy_part_key));
    } else if (previous.count() <= 0) {
      const ObPartitionReplica* leader_replica = NULL;
      ret = info.find_leader_v2(leader_replica);
      if (OB_SUCC(ret)) {
        if (NULL == leader_replica) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("leader replica ptr is null", K(ret), K(pkey), K(phy_part_key));
        } else {
          server = leader_replica->server_;
        }
      } else {
        ret = OB_SUCCESS;
        server = info.get_replicas_v2().at(0).server_;
      }
    } else {
      bool find = false;
      for (int64_t i = 0; !find && i < info.get_replicas_v2().count(); ++i) {
        ObPartitionReplica& replica = info.get_replicas_v2().at(i);
        if (has_exist_in_array(previous, replica.server_)) {
          find = true;
          server = replica.server_;
        } else {
        }  // go on next
      }
      if (!find) {
        server = info.get_replicas_v2().at(0).server_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("pick data replica succ", K(pkey), K(server));
  }
  return ret;
}

int ObGlobalIndexBuilder::pick_index_replica(
    const common::ObPartitionKey& pkey, const common::ObIArray<common::ObAddr>& previous, common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else if (OB_UNLIKELY(NULL == pt_operator_ || NULL == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt operator ptr is null", K(ret));
  } else {
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    ObPartitionInfo info;
    ObReplicaFilterHolder filter;
    info.set_allocator(&allocator);
    if (OB_FAIL(pt_operator_->get(pkey.get_table_id(), pkey.get_partition_id(), info))) {
      LOG_WARN("fail to get partition info", K(ret), K(pkey));
    } else if (OB_FAIL(filter.set_only_alive_server(*server_mgr_))) {
      LOG_WARN("set filter failed", K(ret));
    } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
      LOG_WARN("fail to set replica status", K(ret));
    } else if (OB_FAIL(filter.set_in_member_list())) {
      LOG_WARN("fail to do filter", K(ret));
    } else if (OB_FAIL(filter.set_filter_log_replica())) {
      LOG_WARN("fail to set filter log replica", K(ret));
    } else if (OB_FAIL(info.filter(filter))) {
      LOG_WARN("fail to do filter", K(ret));
    } else if (info.get_replicas_v2().count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("info replica count zero", K(ret), K(pkey));
    } else if (previous.count() <= 0) {
      const ObPartitionReplica* leader_replica = NULL;
      ret = info.find_leader_v2(leader_replica);
      if (OB_SUCC(ret)) {
        if (NULL == leader_replica) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("leader replica ptr is null", K(ret), K(pkey));
        } else {
          server = leader_replica->server_;
        }
      } else {
        ret = OB_SUCCESS;
        server = info.get_replicas_v2().at(0).server_;
      }
    } else {
      bool find = false;
      for (int64_t i = 0; !find && i < info.get_replicas_v2().count(); ++i) {
        ObPartitionReplica& replica = info.get_replicas_v2().at(i);
        if (has_exist_in_array(previous, replica.server_)) {
          find = true;
          server = replica.server_;
        } else {
        }  // go on next
      }
      if (!find) {
        server = info.get_replicas_v2().at(0).server_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("pick index replica succ", K(pkey), K(server));
  }
  return ret;
}

static bool is_ob_sql_errno(int err)
{
  return (err > -6000 && err <= -5000);
}

int ObGlobalIndexBuilder::on_build_single_replica_reply(const uint64_t index_table_id, int64_t snapshot, int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(index_table_id == OB_INVALID_ID || snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index_table_id), K(snapshot));
  } else {
    LOG_INFO("on build single replica reply", K(index_table_id), K(ret_code), K(snapshot));
    ObGlobalIndexTask* task = NULL;
    SpinRLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.get_refactored(index_table_id, task))) {
      LOG_WARN("fail to get from map", K(ret), K(index_table_id));
    } else if (OB_UNLIKELY(NULL == task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task ptr is null", K(ret), K(index_table_id));
    } else {
      SpinWLockGuard item_guard(task->lock_);
      LOG_INFO("on build single replica reply", K(index_table_id), K(ret_code), K(snapshot));
      if (GIBS_BUILD_SINGLE_REPLICA != task->status_) {
        // by pass
      } else if (task->snapshot_ != snapshot) {
        LOG_INFO("snapshot not match", K(snapshot), "local_snapshot", task->snapshot_, K(index_table_id));
      } else if (OB_SUCCESS == ret_code) {
        task->build_single_replica_stat_ = BSRT_SUCCEED;
      } else if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret_code) {
        task->build_single_replica_stat_ = BSRT_PRIMARY_KEY_DUPLICATE;
      } else if (is_ob_sql_errno(ret_code)) {
        // error from sql module, for instance:numeric_overflow, do not retry any more
        task->build_single_replica_stat_ = BSRT_FAILED;
        LOG_WARN("Detected sql error, stop retrying", K(ret_code));
      } else if (ObIDDLTask::error_need_retry(ret_code) || OB_ENTRY_EXIST == ret_code ||
                 OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == ret_code) {
        task->build_single_replica_stat_ = BSRT_INVALID_SNAPSHOT;
        LOG_INFO("ddl error need retry", K(ret_code), K(*task));
      } else {
        if (task->retry_cnt_ >= ObGlobalIndexTask::MAX_RETRY_CNT) {
          task->build_single_replica_stat_ = BSRT_FAILED;
          LOG_WARN("retry times exceed limit, stop retrying", K(ret_code), K(*task));
        } else {
          ++task->retry_cnt_;
          task->build_single_replica_stat_ = BSRT_INVALID_SNAPSHOT;
        }
      }
    }
  }
  idling_.wakeup();
  return ret;
}

int ObGlobalIndexBuilder::on_copy_multi_replica_reply(const ObRebalanceTask& rebalance_task)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (ObRebalanceTaskType::COPY_SSTABLE != rebalance_task.get_rebalance_task_type()) {
    // ignore since this task has nothing to do with global index
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rebalance_task.get_sub_task_count(); ++i) {
      const ObRebalanceTaskInfo* task_info = rebalance_task.get_sub_task(i);
      const share::schema::ObSimpleTableSchemaV2* table_schema = NULL;
      bool table_exist = false;
      if (OB_UNLIKELY(nullptr == task_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info ptr is null", K(ret));
      } else if (OB_FAIL(schema_guard.check_table_exist(task_info->get_partition_key().get_table_id(), table_exist))) {
        LOG_WARN("fail to check table exist", K(ret), "pkey", task_info->get_partition_key());
      } else if (!table_exist) {
        // ignore
      } else if (OB_FAIL(schema_guard.get_table_schema(task_info->get_partition_key().get_table_id(), table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), "pkey", task_info->get_partition_key());
      } else if (OB_UNLIKELY(NULL == table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret), "pkey", task_info->get_partition_key());
      } else if (!(table_schema->is_index_table() && table_schema->is_global_index_table())) {
        // by pass
      } else if (share::schema::INDEX_STATUS_UNAVAILABLE != table_schema->get_index_status()) {
        // by pass
      } else {
        ObGlobalIndexTask* task = NULL;
        SpinRLockGuard guard(task_map_lock_);
        const common::ObPartitionKey& pkey = task_info->get_partition_key();
        uint64_t index_table_id = pkey.get_table_id();
        if (OB_FAIL(task_map_.get_refactored(index_table_id, task))) {
          LOG_WARN("fail to get from map", K(ret), K(pkey));
        } else if (OB_UNLIKELY(NULL == task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task ptr is null", K(ret), K(pkey));
        } else {
          SpinWLockGuard item_guard(task->lock_);
          PartitionSSTableBuildStat* partition_sstable_stat = NULL;
          if (GIBS_MULTI_REPLICA_COPY != task->status_) {
            // by pass
          } else if (OB_FAIL(task->get_partition_sstable_build_stat(pkey, partition_sstable_stat))) {
            LOG_WARN("fail to get partition sstable build stat", K(ret), K(pkey));
          } else if (OB_UNLIKELY(NULL == partition_sstable_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition sstable stat", K(ret));
          } else if (CMRS_COPY_TASK_EXIST == partition_sstable_stat->copy_multi_replica_stat_) {
            partition_sstable_stat->copy_multi_replica_stat_ = CMRS_IDLE;
          } else {
          }  // process finish
        }
      }
    }
  }
  idling_.wakeup();
  return ret;
}

int ObGlobalIndexBuilder::on_col_checksum_calculation_reply(
    const uint64_t index_table_id, const common::ObPartitionKey& pkey, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == index_table_id || !pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else {
    LOG_INFO("on col checksum calculation reply", K(index_table_id), K(pkey), K(ret_code));
    ObGlobalIndexTask* task = NULL;
    SpinRLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.get_refactored(index_table_id, task))) {
      LOG_WARN("fail to get from map", K(ret), K(pkey));
    } else if (OB_UNLIKELY(NULL == task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task ptr is null", K(ret), K(pkey), K(index_table_id));
    } else {
      SpinWLockGuard item_guard(task->lock_);
      PartitionColChecksumStat* partition_col_checksum_stat = NULL;
      if (GIBS_UNIQUE_INDEX_CALC_CHECKSUM != task->status_) {
        LOG_INFO("state not match", K(*task), K(ret_code), K(index_table_id), K(pkey));
      } else if (OB_FAIL(task->get_partition_col_checksum_stat(pkey, partition_col_checksum_stat))) {
        LOG_WARN("fail to get partition unique check stat", K(ret), K(pkey), K(index_table_id));
      } else if (OB_UNLIKELY(NULL == partition_col_checksum_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition col checksum stat ptr is null", K(ret));
      } else if (OB_SUCCESS == ret_code) {
        partition_col_checksum_stat->col_checksum_stat_ = CCS_SUCCEED;
      } else if (OB_NOT_MASTER == ret_code || OB_PARTITION_NOT_EXIST == ret_code) {
        partition_col_checksum_stat->col_checksum_stat_ = CCS_NOT_MASTER;
      } else {
        partition_col_checksum_stat->col_checksum_stat_ = CCS_FAILED;
      }
    }
  }
  idling_.wakeup();
  return ret;
}

int ObGlobalIndexBuilder::on_check_unique_index_reply(
    const ObPartitionKey& pkey, const int ret_code, const bool is_unique)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else {
    LOG_INFO("on check unique index reply", K(pkey), K(ret_code), K(is_unique));
    ObGlobalIndexTask* task = NULL;
    SpinRLockGuard guard(task_map_lock_);
    uint64_t index_table_id = pkey.get_table_id();
    if (OB_FAIL(task_map_.get_refactored(index_table_id, task))) {
      LOG_WARN("fail to get from map", K(ret), K(pkey));
    } else if (OB_UNLIKELY(NULL == task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task ptr is null", K(ret), K(pkey));
    } else {
      SpinWLockGuard item_guard(task->lock_);
      PartitionUniqueStat* partition_unique_stat = NULL;
      if (GIBS_UNIQUE_INDEX_CHECK != task->status_) {
        LOG_INFO("state not match", K(*task), K(is_unique), K(ret_code), K(pkey));
      } else if (OB_FAIL(task->get_partition_unique_check_stat(pkey, partition_unique_stat))) {
        LOG_WARN("fail to get partition unique check stat", K(ret), K(pkey));
      } else if (OB_UNLIKELY(NULL == partition_unique_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition unique stat ptr is null", K(ret));
      } else if (OB_SUCCESS == ret_code && is_unique) {
        partition_unique_stat->unique_check_stat_ = UCS_SUCCEED;
      } else if (OB_SUCCESS == ret_code && !is_unique) {
        partition_unique_stat->unique_check_stat_ = UCS_ILLEGAL;
      } else if (OB_NOT_MASTER == ret_code || OB_PARTITION_NOT_EXIST == ret_code) {
        partition_unique_stat->unique_check_stat_ = UCS_NOT_MASTER;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ret code unexpected", K(ret), K(ret_code));
      }
    }
  }
  idling_.wakeup();
  return ret;
}

int ObGlobalIndexBuilder::send_check_unique_index_request(
    const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == pt_operator_ || NULL == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else {
    const common::ObIArray<PartitionUniqueStat>& partition_unique_stat_array = task->partition_unique_stat_array_;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_unique_stat_array.count(); ++i) {
      const ObPartitionKey& pkey = partition_unique_stat_array.at(i).pkey_;
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      ObPartitionInfo info;
      const ObPartitionReplica* leader_replica = NULL;
      ObReplicaFilterHolder filter;
      info.set_allocator(&allocator);
      if (!pkey.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey invalid", K(ret), K(pkey));
      } else if (OB_FAIL(pt_operator_->get(pkey.get_table_id(), pkey.get_partition_id(), info))) {
        LOG_WARN("fail to get partition info", K(ret), K(pkey));
      } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
        LOG_WARN("fail to set replica status", K(ret));
      } else if (OB_FAIL(filter.set_in_member_list())) {
        LOG_WARN("fail to do filter", K(ret));
      } else if (OB_FAIL(info.filter(filter))) {
        LOG_WARN("fail to do filter", K(ret));
      } else if (OB_FAIL(info.find_leader_v2(leader_replica))) {
        LOG_WARN("fail to get leader v2", K(ret));
      } else if (OB_FAIL(send_check_unique_index_rpc(index_schema, task, pkey, leader_replica))) {
        LOG_WARN("fail to send rpc", K(ret));
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::build_task_partition_unique_stat(
    const share::schema::ObTableSchema* schema, ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == schema || NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema), KP(task));
  } else {
    task->partition_unique_stat_array_.reset();
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter partition_key_iter(*schema, check_dropped_schema);
    ObPartitionKey pkey;
    while (OB_SUCC(ret) && OB_SUCC(partition_key_iter.next_partition_key_v2(pkey))) {
      if (!pkey.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pkey is invalid", K(ret));
      } else {
        PartitionUniqueStat partition_unique_stat;
        partition_unique_stat.pkey_ = pkey;
        partition_unique_stat.unique_check_stat_ = UCS_INVALID;
        if (OB_FAIL(task->partition_unique_stat_array_.push_back(partition_unique_stat))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
    } else if (schema->get_all_part_num() != task->partition_unique_stat_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part num not match",
          K(ret),
          "left cnt",
          schema->get_all_part_num(),
          "right cnt",
          task->partition_unique_stat_array_.count());
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::clear_intermediate_result(ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(sql::ObIndexSSTableBuilder::clear_interm_result(*mysql_proxy_, task->index_table_id_))) {
    LOG_WARN("clear interm result failed", K(ret));
  } else if (OB_FAIL(release_snapshot(task))) {
    LOG_WARN("fail to clear snapshot", K(ret));
  }
  return ret;
}

int ObGlobalIndexBuilder::release_snapshot(const ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(nullptr == ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl service ptr is null", K(ret));
  } else if (task->snapshot_ <= 0) {
    // bypass,
  } else {
    // release data table snapshot
    ObSnapshotInfo info1;
    info1.snapshot_type_ = SNAPSHOT_FOR_CREATE_INDEX;
    info1.snapshot_ts_ = task->snapshot_;
    info1.schema_version_ = task->schema_version_;
    info1.tenant_id_ = task->tenant_id_;
    info1.table_id_ = task->data_table_id_;
    // release index table snapshot
    ObSnapshotInfo info2;
    info2.snapshot_type_ = SNAPSHOT_FOR_CREATE_INDEX;
    info2.snapshot_ts_ = task->snapshot_;
    info2.schema_version_ = task->schema_version_;
    info2.tenant_id_ = task->tenant_id_;
    info2.table_id_ = task->index_table_id_;
    ObMySQLTransaction trans;
    common::ObMySQLProxy& proxy = ddl_service_->get_sql_proxy();
    if (OB_FAIL(trans.start(&proxy))) {
      LOG_WARN("fail to start trans", K(ret));
    } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().release_snapshot(trans, info1))) {
      LOG_WARN("fail to release snapshot", K(ret));
    } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().release_snapshot(trans, info2))) {
      LOG_WARN("fail to release snapshot", K(ret));
    }
    if (trans.is_started()) {
      bool is_commit = (ret == OB_SUCCESS);
      int tmp_ret = trans.end(is_commit);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to end trans", K(ret), K(is_commit));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::try_update_index_status_in_schema(
    const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task, const ObIndexStatus new_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task || NULL == index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl service ptr is null", K(ret));
  } else {
    obrpc::ObUpdateIndexStatusArg arg;
    arg.index_table_id_ = index_schema->get_table_id();
    arg.status_ = new_status;
    arg.create_mem_version_ = index_schema->get_create_mem_version();
    arg.exec_tenant_id_ = extract_tenant_id(index_schema->get_table_id());
    DEBUG_SYNC(BEFORE_UPDATE_GLOBAL_INDEX_STATUS);
    if (OB_FAIL(ddl_service_->get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).update_index_status(arg))) {
      LOG_WARN("update index status failed", K(ret));
    } else {
      LOG_INFO("notify index status changed finish", K(new_status), "index_id", index_schema->get_table_id());
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::try_handle_index_build_take_effect(ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_service_ || NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else {
    SpinWLockGuard item_guard(task->lock_);
    if (GIBS_INDEX_BUILD_TAKE_EFFECT != task->status_) {
      // by pass, indeed we shall never be here
    } else {
      const uint64_t index_table_id = task->index_table_id_;
      const uint64_t tenant_id = extract_tenant_id(index_table_id);
      const ObTableSchema* index_schema = NULL;
      share::schema::ObSchemaGetterGuard schema_guard;
      bool index_schema_exist = false;
      // strong consistency is used on primary cluster
      // since ObGlobalIndexBuilder is not used on standby cluster
      ObRefreshSchemaStatus schema_status;
      schema_status.tenant_id_ = extract_tenant_id(index_table_id);
      int64_t version_in_inner_table = OB_INVALID_VERSION;
      int64_t local_schema_version = OB_INVALID_VERSION;
      if (GCTX.is_standby_cluster()) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("create global index in slave cluster is not allowed", K(ret), K(index_table_id));
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret));
      } else if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(
                     *mysql_proxy_, schema_status, version_in_inner_table))) {
        LOG_WARN("fail to get version in inner table", K(ret));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_schema_version))) {
        LOG_WARN("fail to get schema version from guard", K(ret), K(tenant_id));
      } else if (version_in_inner_table > local_schema_version) {
        // by pass, this server may not get the newest schema
      } else if (OB_FAIL(check_and_get_index_schema(schema_guard, index_table_id, index_schema, index_schema_exist))) {
        LOG_WARN("fail to get table schema", K(ret), K(index_table_id));
      } else if (!index_schema_exist) {
        if (OB_FAIL(switch_state(task, GIBS_INDEX_BUILD_FAILED))) {
          LOG_WARN("fail to switch state", K(ret));
        }
      } else if (OB_UNLIKELY(NULL == index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema ptr is null", K(ret));
      } else if (is_error_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
        if (OB_FAIL(switch_state(task, GIBS_INDEX_BUILD_FAILED))) {
          LOG_WARN("fail to switch state", K(ret));
        }
      } else if (is_available_index_status(index_schema->get_index_status())) {
        if (OB_FAIL(switch_state(task, GIBS_INDEX_BUILD_FINISH))) {
          LOG_WARN("fail to switch state", K(ret));
        }
      } else {
        if (INDEX_STATUS_AVAILABLE == index_schema->get_index_status()) {
          if (OB_FAIL(switch_state(task, GIBS_INDEX_BUILD_FINISH))) {
            LOG_WARN("fail to switch state", K(ret));
          }
        } else if (INDEX_STATUS_UNAVAILABLE != index_schema->get_index_status()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index schema status not match", K(ret), K(*index_schema));
        } else if (OB_FAIL(try_update_index_status_in_schema(index_schema, task, INDEX_STATUS_AVAILABLE))) {
          LOG_WARN("fail to try notify index take effect", K(ret));
        } else {
          (void)switch_state(task, GIBS_INDEX_BUILD_FINISH);
        }
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::try_handle_index_build_failed(ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_service_ || NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner variable ptr is null", K(ret));
  } else {
    SpinWLockGuard item_guard(task->lock_);
    if (GIBS_INDEX_BUILD_FAILED != task->status_) {
      // by pass, indeed we shall never be here
    } else {
      const uint64_t index_table_id = task->index_table_id_;
      const uint64_t tenant_id = extract_tenant_id(index_table_id);
      const ObTableSchema* index_schema = NULL;
      share::schema::ObSchemaGetterGuard schema_guard;
      bool index_schema_exist = false;
      // strong consistency is used on primary cluster
      // since ObGlobalIndexBuilder is not used on standby cluster
      ObRefreshSchemaStatus schema_status;
      schema_status.tenant_id_ = extract_tenant_id(index_table_id);
      int64_t version_in_inner_table = OB_INVALID_VERSION;
      int64_t local_schema_version = OB_INVALID_VERSION;
      if (GCTX.is_standby_cluster()) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("create global index in slave cluster is not allowed", K(ret), K(index_table_id));
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret));
      } else if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(
                     *mysql_proxy_, schema_status, version_in_inner_table))) {
        LOG_WARN("fail to get version in inner table", K(ret));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_schema_version))) {
        LOG_WARN("fail to get schema version from guard", K(ret), K(tenant_id));
      } else if (version_in_inner_table > local_schema_version) {
        // by pass, this server may not get the newest schema
      } else if (OB_FAIL(check_and_get_index_schema(schema_guard, index_table_id, index_schema, index_schema_exist))) {
        LOG_WARN("fail to get table schema", K(ret), K(index_table_id));
      } else if (!index_schema_exist) {
        if (OB_FAIL(switch_state(task, GIBS_INDEX_BUILD_FINISH))) {
          LOG_WARN("fail to switch state", K(ret));
        }
      } else if (OB_UNLIKELY(NULL == index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema ptr is null", K(ret));
      } else {
        if (is_final_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
          if (OB_FAIL(switch_state(task, GIBS_INDEX_BUILD_FINISH))) {
            LOG_WARN("fail to switch state", K(ret));
          }
        } else if (INDEX_STATUS_UNAVAILABLE == index_schema->get_index_status()) {
          if (OB_FAIL(try_update_index_status_in_schema(index_schema, task, INDEX_STATUS_INDEX_ERROR))) {
            LOG_WARN("fail to try update index status in schema", K(ret));
          } else if (OB_FAIL(switch_state(task, GIBS_INDEX_BUILD_FINISH))) {
            LOG_WARN("fail to switch state", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index status",
              K(ret),
              "index_id",
              index_schema->get_table_id(),
              "index_status",
              index_schema->get_index_status());
        }
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::try_handle_index_build_finish(ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy ptr is null", K(ret));
  } else {
    uint64_t tenant_id = OB_INVALID_ID;
    uint64_t data_table_id = OB_INVALID_ID;
    uint64_t index_table_id = OB_INVALID_ID;
    {
      SpinWLockGuard item_guard(task->lock_);
      tenant_id = task->tenant_id_;
      data_table_id = task->data_table_id_;
      index_table_id = task->index_table_id_;
      if (GIBS_INDEX_BUILD_FINISH != task->status_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", K(ret), K(*task));
      } else if (OB_FAIL(clear_intermediate_result(task))) {
        LOG_WARN("fail to clear intermediate result", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObMySQLTransaction trans;
      ObSqlString sql_string1;
      ObSqlString sql_string2;
      ObSqlString sql_string3;
      int64_t affected_rows = 0;
      if (OB_FAIL(trans.start(mysql_proxy_))) {
        LOG_WARN("fail to start transaction", K(ret));
      } else {
        if (OB_FAIL(sql_string1.assign_fmt("DELETE FROM %s WHERE TENANT_ID = %ld "
                                           "AND DATA_TABLE_ID = %ld "
                                           "AND INDEX_TABLE_ID = %ld",
                OB_ALL_INDEX_BUILD_STAT_TNAME,
                tenant_id,
                data_table_id,
                index_table_id))) {
          LOG_WARN("fail to assign format", K(ret));
        } else if (OB_FAIL(sql_string2.assign_fmt("DELETE FROM %s WHERE TENANT_ID = %ld "
                                                  "AND INDEX_TABLE_ID = %ld "
                                                  "AND DATA_TABLE_ID = %ld",
                       OB_ALL_GLOBAL_INDEX_DATA_SRC_TNAME,
                       tenant_id,
                       index_table_id,
                       data_table_id))) {
          LOG_WARN("fail to assign format", K(ret));
        } else if (OB_FAIL(sql_string3.assign_fmt("DELETE FROM %s WHERE TENANT_ID = %ld "
                                                  "AND INDEX_TABLE_ID = %ld",
                       OB_ALL_IMMEDIATE_EFFECT_INDEX_SSTABLE_TNAME,
                       tenant_id,
                       index_table_id))) {
          LOG_WARN("fail to assign format", K(ret));
        } else if (OB_FAIL(trans.write(sql_string1.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(ret), K(sql_string1));
        } else if (OB_FAIL(trans.write(sql_string2.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(ret), K(sql_string2));
        } else if (OB_FAIL(trans.write(sql_string3.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(ret), K(sql_string3));
        } else {
        }  // no more to do
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCCESS == ret))) {
          ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
          LOG_WARN("fail to end trans", K(ret), K(tmp_ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // task instance should be free out of task lock
      if (OB_FAIL(task_map_.erase_refactored(index_table_id))) {
        LOG_WARN("fail to erase item from hash map", K(ret), "key", index_table_id);
      } else {
        task_allocator_.free(task);
        task = NULL;
      }
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::try_drive(ObGlobalIndexTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    GlobalIndexBuildStatus build_status = task->status_;
    switch (build_status) {
      case GIBS_BUILD_SINGLE_REPLICA:
        // single replica build state
        if (OB_FAIL(try_build_single_replica(task))) {
          LOG_WARN("fail to try build single replica", K(ret));
        }
        break;
      case GIBS_MULTI_REPLICA_COPY:
        // multiple replica copy state
        if (OB_FAIL(try_copy_multi_replica(task))) {
          LOG_WARN("fail to try copy multi replica", K(ret));
        }
        break;
      case GIBS_UNIQUE_INDEX_CALC_CHECKSUM:
        // unique index column checksum calculation state
        if (OB_FAIL(try_unique_index_calc_checksum(task))) {
          LOG_WARN("fail to try unique index calc checksum", K(ret));
        }
        break;
      case GIBS_UNIQUE_INDEX_CHECK:
        // unique index validation check state
        if (OB_FAIL(try_unique_index_check(task))) {
          LOG_WARN("fail to try unique index check", K(ret));
        }
        break;
      case GIBS_INDEX_BUILD_TAKE_EFFECT:
        if (OB_FAIL(try_handle_index_build_take_effect(task))) {
          LOG_WARN("fail to try handle index build succeed", K(ret));
        }
        break;
      case GIBS_INDEX_BUILD_FAILED:
        // build index task failed state
        if (OB_FAIL(try_handle_index_build_failed(task))) {
          LOG_WARN("fail to clean index build task", K(ret));
        }
        break;
      case GIBS_INDEX_BUILD_FINISH:
        // clean up index build state machine
        if (OB_FAIL(try_handle_index_build_finish(task))) {
          LOG_WARN("fail to try handle index build succeed", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected global index build status", K(ret), K(build_status));
        break;
    }
  }
  return ret;
}

int ObGlobalIndexBuilder::get_task_count_in_lock(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else {
    SpinRLockGuard guard(task_map_lock_);
    task_cnt = task_map_.size();
  }
  return ret;
}

void ObGlobalIndexBuilder::run3()
{
  if (OB_UNLIKELY(!inited_)) {
    int ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalIndexBuilder not init", K(ret));
    idling_.idle(10 * 1000L * 1000L);
  } else {
    LOG_INFO("global index builder start");
    while (!stop_) {
      {
        int64_t task_cnt = 0;
        int tmp_ret = get_task_count_in_lock(task_cnt);
        idling_.idle(1 * 100000);
      };
      ObClusterType cluster_type = ObClusterInfoGetter::get_cluster_type_v2();
      if (PRIMARY_CLUSTER != cluster_type) {
        // nothing todo
      } else {
        {
          SpinRLockGuard guard(task_map_lock_);
          // drive valid task state maching
          if (task_map_.size() > 0 && !stop_) {
            for (task_iterator iter = task_map_.begin(); !stop_ && iter != task_map_.end(); ++iter) {
              int tmp_ret = OB_SUCCESS;
              ObGlobalIndexTask* task = iter->second;
              if (OB_UNLIKELY(NULL == task)) {
                tmp_ret = OB_ERR_UNEXPECTED;
                LOG_WARN("task ptr is null", K(tmp_ret));
              } else if (task->status_ < GIBS_BUILD_SINGLE_REPLICA || task->status_ > GIBS_INDEX_BUILD_FAILED) {
                // do nothing
              } else if (OB_SUCCESS != (tmp_ret = try_drive(task))) {
                LOG_WARN("fail to drive task stat", K(tmp_ret));
              } else {
              }  // no more to do
            }
          }
        };
        {
          SpinWLockGuard guard(task_map_lock_);
          // clean up the invalid task state machine
          if (task_map_.size() > 0 && !stop_) {
            for (task_iterator iter = task_map_.begin(); !stop_ && iter != task_map_.end(); ++iter) {
              int tmp_ret = OB_SUCCESS;
              ObGlobalIndexTask* task = iter->second;
              if (OB_UNLIKELY(NULL == task)) {
                tmp_ret = OB_ERR_UNEXPECTED;
                LOG_WARN("task ptr is null", K(tmp_ret));
              } else if (task->status_ != GIBS_INDEX_BUILD_FINISH) {
                // do nothing
              } else if (OB_SUCCESS != (tmp_ret = try_drive(task))) {
                LOG_WARN("fail to drive task stat", K(tmp_ret));
              }
            }
          }
        };
      }
    }  // end while
    if (OB_SUCCESS != reset_run_condition()) {
      LOG_WARN("fail to reset run condition");
    }
  }
  LOG_INFO("global index builder exit");
}

int ObGlobalIndexBuilder::reset_run_condition()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    SpinWLockGuard guard(task_map_lock_);
    for (task_iterator iter = task_map_.begin(); iter != task_map_.end(); ++iter) {
      ObGlobalIndexTask* task = iter->second;
      if (NULL != task) {
        task_allocator_.free(task);  // shall be freed in destruction func
        task = NULL;
      }
    }
    task_map_.reuse();
    loaded_ = false;
  }
  return ret;
}

void ObGlobalIndexBuilder::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!stop_) {
    ObRsReentrantThread::stop();
    idling_.wakeup();
  }
}

}  // end namespace rootserver
}  // end namespace oceanbase
