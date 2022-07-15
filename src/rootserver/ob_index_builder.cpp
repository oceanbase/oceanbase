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
#include "ob_index_builder.h"

#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_srv_rpc_proxy.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_debug_sync.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_index_trans_status_reporter.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_iter.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_replica_filter.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/ob_index_builder_util.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_service.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "ob_server_manager.h"
#include "ob_zone_manager.h"
#include "ob_ddl_service.h"
#include "ob_global_index_builder.h"
#include "ob_root_service.h"
#include "ob_snapshot_info_manager.h"
#include "share/ob_thread_mgr.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/ob_index_sstable_builder.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/transaction/ob_i_ts_source.h"
#include <map>

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace sql;
namespace rootserver {

ObIndexBuildStatus::PartitionIndexStatus::PartitionIndexStatus()
{
  reset();
}

void ObIndexBuildStatus::PartitionIndexStatus::reset()
{
  partition_id_ = OB_INVALID_INDEX;
  server_.reset();
  index_status_ = INDEX_STATUS_NOT_FOUND;
  ret_code_ = OB_SUCCESS;
}

bool ObIndexBuildStatus::PartitionIndexStatus::is_valid() const
{
  return OB_INVALID_INDEX != partition_id_ && index_status_ > INDEX_STATUS_NOT_FOUND &&
         index_status_ < INDEX_STATUS_MAX;
}

bool ObIndexBuildStatus::PartitionIndexStatus::operator<(const ObIndexBuildStatus::PartitionIndexStatus& o) const
{
  bool less = partition_id_ < o.partition_id_;
  if (!less && partition_id_ == o.partition_id_) {
    less = server_ < o.server_;
  }
  return less;
}

bool ObIndexBuildStatus::PartitionIndexStatus::operator==(const ObIndexBuildStatus::PartitionIndexStatus& other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN("invalid argument", "self", *this, K(other));
  } else {
    equal = partition_id_ == other.partition_id_ && server_ == other.server_;
  }
  return equal;
}

bool ObIndexBuildStatus::PartitionIndexStatusOrder::operator()(
    const PartitionIndexStatus& left, const PartitionIndexStatus& right) const
{
  bool less = false;
  if (OB_SUCCESS != ret_) {
  } else if (!left.is_valid() || !right.is_valid()) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(left), K(right), K_(ret));
  } else if (left.partition_id_ == right.partition_id_) {
    less = left.server_ < right.server_;
  } else {
    less = left.partition_id_ < right.partition_id_;
  }
  return less;
}

int ObIndexBuildStatus::load_all(const uint64_t index_table_id, const int64_t partition_id, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  HEAP_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    // allow load twice
    if (OB_INVALID_ID == index_table_id || partition_id < -1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid index table id", KT(index_table_id), K(ret), K(partition_id));
    } else {
      if (-1 == partition_id) {
        if (OB_FAIL(sql.assign_fmt("SELECT partition_id, svr_ip, svr_port, index_status, ret_code "
                                   "FROM %s WHERE tenant_id = %ld AND index_table_id = %ld",
                OB_ALL_LOCAL_INDEX_STATUS_TNAME,
                extract_tenant_id(index_table_id),
                index_table_id))) {
          LOG_WARN("assign sql failed", K(ret));
        }
      } else {
        if (OB_FAIL(sql.assign_fmt("SELECT partition_id, svr_ip, svr_port, index_status, ret_code "
                                   "FROM %s WHERE tenant_id = %ld AND index_table_id = %ld AND partition_id=%ld",
                OB_ALL_LOCAL_INDEX_STATUS_TNAME,
                extract_tenant_id(index_table_id),
                index_table_id,
                partition_id))) {
          LOG_WARN("assign sql failed", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
      ret = OB_EAGAIN;
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get result", K(ret));
    } else {
      all_status_.reuse();
      PartitionIndexStatus status;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("result next failed", K(ret));
          }
        } else {
          // used to fill the output arg, we need to guarantee that
          // there is no '\0' in this string except the last char
          int64_t tmp_real_str_len = 0;
          char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
          int port = 0;
          status.reset();
          EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", status.partition_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "index_status", status.index_status_, ObIndexStatus);
          EXTRACT_INT_FIELD_MYSQL(*result, "ret_code", status.ret_code_, int);
          EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int);
          EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip, OB_MAX_SERVER_ADDR_SIZE, tmp_real_str_len);
          if (!status.server_.set_ip_addr(ip, port)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, fail to set server addr", K(ret), K(ip), K(port));
          }
          (void)tmp_real_str_len;  // make compiler happy
          if (OB_FAIL(ret)) {
          } else if (!status.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid status", K(status), K(ret));
          } else if (OB_FAIL(all_status_.push_back(status))) {
            LOG_WARN("push back status failed", K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("load local index status failed", K(ret), K(sql));
        }
      }
    }

    if (OB_SUCC(ret)) {
      std::sort(all_status_.begin(), all_status_.end(), PartitionIndexStatusOrder(ret));
      if (OB_FAIL(ret)) {
        LOG_WARN("compare failed", K(ret));
      } else {
        loaded_ = true;
      }
    }
  }
  return ret;
}

int ObIndexBuildStatus::find(const int64_t partition_id, const ObAddr& server, PartitionIndexStatus& status) const
{
  int ret = OB_SUCCESS;
  status.reset();
  if (!loaded_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check inner stat error", K_(loaded), K(ret));
  } else if (OB_INVALID_INDEX == partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_id));
  } else {
    PartitionIndexStatus to_find;
    to_find.partition_id_ = partition_id;
    to_find.server_ = server;
    // otherwise compare will failed because of invalid_argument
    to_find.index_status_ = INDEX_STATUS_UNAVAILABLE;

    ObArray<PartitionIndexStatus>::const_iterator pos =
        std::lower_bound(all_status_.begin(), all_status_.end(), to_find, PartitionIndexStatusOrder(ret));
    if (OB_FAIL(ret)) {
      LOG_WARN("compare failed", K(ret));
    } else if (pos == all_status_.end() || to_find != *pos) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      status = *pos;
    }
    LOG_DEBUG("find index build status", K(ret), K(partition_id), K(server), K(all_status_), K(to_find));
  }
  return ret;
}

ObIndexWaitTransStatus::PartitionWaitTransStatus::PartitionWaitTransStatus()
    : partition_id_(OB_INVALID_ID), trans_status_(OB_SUCCESS), snapshot_version_(0), schema_version_(0)
{}

ObIndexWaitTransStatus::PartitionWaitTransStatus::~PartitionWaitTransStatus()
{}

bool ObIndexWaitTransStatus::PartitionWaitTransStatus::operator<(const PartitionWaitTransStatus& other) const
{
  return partition_id_ < other.partition_id_;
}

bool ObIndexWaitTransStatus::PartitionWaitTransStatus::operator==(const PartitionWaitTransStatus& other) const
{
  return partition_id_ == other.partition_id_;
}

bool ObIndexWaitTransStatus::PartitionWaitTransStatus::operator!=(const PartitionWaitTransStatus& other) const
{
  return !operator==(other);
}

bool ObIndexWaitTransStatus::PartitionWaitTransStatus::is_valid() const
{
  return partition_id_ >= 0;
}

void ObIndexWaitTransStatus::PartitionWaitTransStatus::reset()
{
  partition_id_ = OB_INVALID_ID;
  trans_status_ = OB_SUCCESS;
  snapshot_version_ = 0;
  schema_version_ = 0;
}

bool ObIndexWaitTransStatus::PartitionWaitTransStatusComparator::operator()(
    const ObIndexWaitTransStatus::PartitionWaitTransStatus& lhs,
    const ObIndexWaitTransStatus::PartitionWaitTransStatus& rhs) const
{
  int ret = OB_SUCCESS;
  bool bret = false;
  if (!lhs.is_valid() || !rhs.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(lhs), K(rhs));
  } else {
    bret = lhs.partition_id_ < rhs.partition_id_;
  }
  ret_ = ret;
  return bret;
}

ObIndexWaitTransStatus::ObIndexWaitTransStatus() : loaded_(false), all_wait_trans_status_()
{}

ObIndexWaitTransStatus::~ObIndexWaitTransStatus()
{}

int ObIndexWaitTransStatus::check_wait_trans_end(const uint64_t index_id, ObMySQLProxy& sql_proxy, bool& is_end)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  HEAP_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    int64_t snapshot_version = 0;
    is_end = false;
    if (OB_INVALID_ID == index_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(index_id));
    } else if (OB_FAIL(sql.assign_fmt(
                   "SELECT snapshot_version FROM %s "
                   "WHERE tenant_id = %ld AND index_table_id = %ld AND svr_type = %d AND partition_id = -1",
                   OB_ALL_INDEX_WAIT_TRANSACTION_STATUS_TNAME,
                   extract_tenant_id(index_id),
                   index_id,
                   ObIndexTransStatusReporter::ROOT_SERVICE))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, result must not be NULL", K(ret), KP(result));
    } else if (OB_FAIL(result->next())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", snapshot_version, int64_t);
      is_end = 0 != snapshot_version;
    }
  }
  return ret;
}

int ObIndexWaitTransStatus::get_wait_trans_status(const uint64_t index_id, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  HEAP_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    if (OB_INVALID_ID == index_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(index_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT partition_id, trans_status, snapshot_version, schema_version FROM %s "
                                      "WHERE tenant_id = %ld AND index_table_id = %ld AND svr_type = %d",
                   OB_ALL_INDEX_WAIT_TRANSACTION_STATUS_TNAME,
                   extract_tenant_id(index_id),
                   index_id,
                   ObIndexTransStatusReporter::OB_SERVER))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, result must not be NULL", K(ret));
    } else {
      all_wait_trans_status_.reuse();
      PartitionWaitTransStatus status;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else {
          status.reset();
          EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", status.partition_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "trans_status", status.trans_status_, int);
          EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", status.snapshot_version_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", status.schema_version_, int64_t);
          if (!status.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid wait trans status", K(ret), K(status));
          } else if (OB_FAIL(all_wait_trans_status_.push_back(status))) {
            LOG_WARN("fail to push back wait trans status", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        std::sort(
            all_wait_trans_status_.begin(), all_wait_trans_status_.end(), PartitionWaitTransStatusComparator(ret));
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to sort index wait trans status", K(ret));
        } else {
          loaded_ = true;
        }
      }
    }
  }
  return ret;
}

int ObIndexWaitTransStatus::find_status(const int64_t partition_id, PartitionWaitTransStatus& status) const
{
  int ret = OB_SUCCESS;
  status.reset();
  if (!loaded_) {
    ret = OB_NOT_INIT;
    LOG_WARN("index wait trans status has not been loaded", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_id));
  } else {
    PartitionWaitTransStatus to_find;
    to_find.partition_id_ = partition_id;
    to_find.trans_status_ = OB_SUCCESS;

    ObArray<PartitionWaitTransStatus>::const_iterator pos = std::lower_bound(
        all_wait_trans_status_.begin(), all_wait_trans_status_.end(), to_find, PartitionWaitTransStatusComparator(ret));
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to search partition wait trans status", K(ret), K(to_find));
    } else if (pos == all_wait_trans_status_.end() || to_find != *pos) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      status = *pos;
    }
  }
  return ret;
}

ObRSBuildIndexTask::ObRSBuildIndexTask()
    : ObIDDLTask(DDL_TASK_RS_BUILD_INDEX),
      state_(WAIT_TRANS_END),
      data_table_id_(OB_INVALID_ID),
      index_id_(OB_INVALID_ID),
      schema_version_(0),
      ddl_service_(NULL),
      last_log_timestamp_(0)
{}

ObRSBuildIndexTask::~ObRSBuildIndexTask()
{}

int ObRSBuildIndexTask::init(
    const uint64_t index_id, const uint64_t data_table_id, const int64_t schema_version, ObDDLService* ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRSBuildIndexTask has already been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == index_id || OB_INVALID_ID == data_table_id || schema_version < 0) ||
             OB_ISNULL(ddl_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_id), K(data_table_id), K(schema_version), KP(ddl_service));
  } else {
    index_id_ = index_id;
    data_table_id_ = data_table_id;
    schema_version_ = schema_version;
    ddl_service_ = ddl_service;
    task_id_.init(GCONF.self_addr_);
    is_inited_ = true;
  }
  return ret;
}

int64_t ObRSBuildIndexTask::hash() const
{
  return index_id_;
}

bool ObRSBuildIndexTask::operator==(const ObIDDLTask& other) const
{
  bool is_equal = false;
  if (get_type() == other.get_type()) {
    const ObRSBuildIndexTask& other_task = static_cast<const ObRSBuildIndexTask&>(other);
    is_equal = index_id_ == other_task.index_id_;
  }
  return is_equal;
}

ObRSBuildIndexTask* ObRSBuildIndexTask::deep_copy(char* buf, const int64_t size) const
{
  int ret = OB_SUCCESS;
  ObRSBuildIndexTask* task = NULL;
  if (OB_ISNULL(buf) || size < sizeof(*this)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(size));
  } else {
    task = new (buf) ObRSBuildIndexTask();
    *task = *this;
  }
  return task;
}

int ObRSBuildIndexTask::process()
{
  int ret = OB_SUCCESS;
  bool need_release_snapshot = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRSBuildIndexTask has not been inited", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(
        WARN, "create index online is not supported in old verion", K(ret), "version", GET_MIN_CLUSTER_VERSION());
  } else if (ddl_service_->is_stopped()) {
    STORAGE_LOG(INFO, "rootservice is stopped");
  } else {
    bool is_end = false;
    ObCurTraceId::set(task_id_);
    switch (state_) {
      case WAIT_TRANS_END:
        if (OB_FAIL(wait_trans_end(is_end))) {
          LOG_WARN("fail to wait trans end", K(ret));
        } else if (is_end) {
          state_ = WAIT_BUILD_INDEX_END;
        } else {
          if (OB_FAIL(wait_build_index_end(is_end))) {
            LOG_WARN("fail to wait build index end", K(ret));
          } else if (is_end) {
            state_ = BUILD_INDEX_FINISH;
          }
        }
        break;
      case WAIT_BUILD_INDEX_END:
        if (OB_FAIL(wait_build_index_end(is_end))) {
          LOG_WARN("fail to wait build index end", K(ret));
        } else if (is_end) {
          state_ = BUILD_INDEX_FINISH;
        }
        break;
      default:
        break;
    }
    need_retry_ = (OB_SUCC(ret) && BUILD_INDEX_FINISH != state_);
    if (!need_retry_ && error_need_retry(ret)) {
      need_retry_ = true;
      ret = OB_EAGAIN;
    }
    need_release_snapshot = (OB_FAIL(ret) && !need_retry_) || (BUILD_INDEX_FINISH == state_);
  }
  if (OB_FAIL(ret) && !need_retry_) {
    const ObIndexStatus index_status = INDEX_STATUS_INDEX_ERROR;
    if (OB_FAIL(report_index_status(index_status))) {
      LOG_WARN("fail to report index status", K(ret));
      need_retry_ = true;
      need_release_snapshot = false;
    }
  }
  if (need_release_snapshot) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = release_snapshot())) {
      LOG_WARN("fail to release snapshot", K(ret), K(index_id_));
      need_retry_ = true;
    }
  }
  return ret;
}

bool ObRSBuildIndexTask::need_print_log()
{
  const int64_t now = ObTimeUtility::current_time();
  const bool bret = now - last_log_timestamp_ > PRINT_LOG_INTERVAL;
  last_log_timestamp_ = bret ? now : last_log_timestamp_;
  return bret;
}

int ObRSBuildIndexTask::report_index_status(const ObIndexStatus index_status)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* index_schema = NULL;
  const uint64_t fetch_tenant_id = is_inner_table(index_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(index_id_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRSBuildIndexTask has not been inited", K(ret));
  } else if (OB_FAIL(ddl_service_->get_schema_service().get_tenant_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(fetch_tenant_id), K_(index_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_id_, index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(index_id_));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_SUCCESS;
  } else {
    DEBUG_SYNC(BEFORE_UPDATE_LOCAL_INDEX_STATUS);
    obrpc::ObUpdateIndexStatusArg arg;
    arg.index_table_id_ = index_id_;
    arg.status_ = index_status;
    arg.create_mem_version_ = index_schema->get_create_mem_version();
    arg.exec_tenant_id_ = fetch_tenant_id;
    DEBUG_SYNC(BEFORE_SEND_UPDATE_INDEX_STATUS);
    if (OB_FAIL(ddl_service_->get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).update_index_status(arg))) {
      LOG_WARN("fail to update index status", K(ret), K(arg));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_id_, index_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(index_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_SUCCESS;
      LOG_WARN("can not find this index schema", K(ret), K(index_id_));
    }
  }
  return ret;
}

int ObRSBuildIndexTask::wait_trans_end(bool& is_end)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* index_schema = NULL;
  const ObTableSchema* table_schema = NULL;
  ObIndexWaitTransStatus wait_trans_processor;
  const uint64_t fetch_tenant_id = is_inner_table(index_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(index_id_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRSBuildIndexTask has not been inited", K(ret));
  } else if (OB_FAIL(wait_trans_processor.check_wait_trans_end(index_id_, ddl_service_->get_sql_proxy(), is_end))) {
    LOG_WARN("fail to check wait trans end", K(ret));
  } else if (is_end) {
    // do nothing
    LOG_INFO("check wait trans already end", K(ret), K(index_id_));
  } else if (OB_FAIL(wait_trans_processor.get_wait_trans_status(index_id_, ddl_service_->get_sql_proxy()))) {
    LOG_WARN("fail to get wait trans status", K(ret), K(index_id_));
  } else if (OB_FAIL(ddl_service_->get_schema_service().get_tenant_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(fetch_tenant_id), K_(index_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_id_, index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(index_id_));
  } else if (OB_ISNULL(index_schema) || index_schema->is_dropped_schema()) {
    // index table has been dropped
    ret = OB_SUCCESS;
    is_end = true;
  } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), "table_id", index_schema->get_data_table_id());
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("main table schema not found while index schema exist",
        K(ret),
        K_(index_id),
        "data_table_id",
        index_schema->get_data_table_id());
  } else {
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter part_iter(*table_schema, check_dropped_schema);
    const int64_t part_num = part_iter.get_partition_num();
    int64_t partition_id;
    ObIndexWaitTransStatus::PartitionWaitTransStatus status;
    int trans_status = OB_SUCCESS;
    bool is_end = true;
    int64_t max_commit_version = 0;
    int64_t snapshot_version = 0;
    int64_t schema_version = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
      if (OB_FAIL(part_iter.next_partition_id_v2(partition_id))) {
        LOG_WARN("fail to get next partition id", K(ret));
      } else if (OB_FAIL(wait_trans_processor.find_status(partition_id, status))) {
        if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
          ret = OB_SUCCESS;
          is_end = false;
          if (need_print_log()) {
            LOG_INFO("Index wait trans end not finish", K(index_id_), K(partition_id));
          }
          break;
        } else {
          LOG_WARN("fail to find partition wait trans status", K(ret), K(index_id_), K(partition_id));
        }
      } else if (OB_UNLIKELY(OB_SUCCESS != (trans_status = status.trans_status_))) {
        break;
      }
      max_commit_version = std::max(status.snapshot_version_, max_commit_version);
      if (0 == schema_version) {
        schema_version = status.schema_version_;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(calc_snapshot_version(max_commit_version, snapshot_version))) {
        LOG_WARN("fail to calc snapshot version", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_end) {
      // report wait trans status
      ObMySQLTransaction trans;
      common::ObMySQLProxy &proxy = ddl_service_->get_sql_proxy();
      const int64_t partition_id = -1;
      int64_t frozen_timestamp = 0;
      ObIndexTransStatus report_status;
      report_status.server_ = GCONF.self_addr_;
      report_status.trans_status_ = trans_status;
      report_status.snapshot_version_ = snapshot_version;
      report_status.schema_version_ = index_schema->get_schema_version();
      if (OB_FAIL(ddl_service_->get_zone_mgr().get_frozen_info(report_status.frozen_version_, frozen_timestamp))) {
        LOG_WARN("fail to get frozen info", K(ret));
      } else if (OB_FAIL(trans.start(&proxy))) {
        LOG_WARN("fail to start trans", K(ret));
      } else if (OB_FAIL(acquire_snapshot(report_status.snapshot_version_,
                     index_schema->get_data_table_id(),
                     report_status.schema_version_,
                     trans))) {
        if (OB_SNAPSHOT_DISCARDED == ret) {
          STORAGE_LOG(WARN, "snapshot discard", K(ret), K(index_id_));
          if (OB_FAIL(ObIndexTransStatusReporter::delete_wait_trans_status(index_id_, trans))) {
            LOG_WARN("fail to delete wait trans status", K(ret));
          } else {
            report_status.snapshot_version_ = 0;
            if (OB_FAIL(ObIndexTransStatusReporter::report_wait_trans_status(index_id_,
                    ObIndexTransStatusReporter::ROOT_SERVICE,
                    partition_id,
                    report_status,
                    trans))) {
              LOG_WARN("fail to report wait trans status", K(ret));
            }
          }
        } else {
          LOG_WARN("fail to acquire snapshot", K(ret), K(index_id_));
        }
      } else if (OB_FAIL(ObIndexTransStatusReporter::report_wait_trans_status(index_id_,
                     ObIndexTransStatusReporter::ROOT_SERVICE,
                     partition_id,
                     report_status,
                     trans))) {
        is_end = false;
        LOG_WARN("fail to report wait trans status", K(ret));
      }
      if (trans.is_started()) {
        bool is_commit = (ret == OB_SUCCESS);
        int tmp_ret = trans.end(is_commit);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to end trans", K(ret), K(tmp_ret), K(is_commit));
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
        }
      }
    }
  }
  return ret;
}

int ObRSBuildIndexTask::wait_build_index_end(bool& is_end)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* index_schema = NULL;
  const ObTableSchema* table_schema = NULL;
  ObIndexBuildStatus all_status;
  const bool filter_flag_replica = false;
  ObTablePartitionIterator iter;
  ObReplicaFilterHolder filter;
  int64_t table_id = OB_INVALID_ID;
  const uint64_t fetch_tenant_id = is_inner_table(index_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(index_id_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRSBuildIndexTask has not been inited", K(ret));
  } else if (OB_FAIL(ddl_service_->get_schema_service().get_tenant_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(fetch_tenant_id), K_(index_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_id_, index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(index_id_));
  } else if (OB_ISNULL(index_schema) || index_schema->is_dropped_schema()) {
    ret = OB_SUCCESS;
    is_end = true;
  } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), table_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("fail to find data table while index table exist",
        K(ret),
        "index_id",
        index_schema->get_table_id(),
        "table_id",
        index_schema->get_data_table_id());
  } else if (is_final_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
    LOG_INFO("index table schema is final", K(index_id_), K(ret));
    is_end = true;
  } else if (OB_FAIL(all_status.load_all(index_id_, -1 /*all partitions*/, ddl_service_->get_sql_proxy()))) {
    LOG_WARN("fail to load all build index status", K(ret), K(index_id_));
  } else {
    if (table_schema->is_binding_table()) {
      table_id = table_schema->get_tablegroup_id();
    } else {
      table_id = table_schema->get_table_id();
    }
  }

  if (OB_FAIL(ret) || is_end) {
  } else if (OB_FAIL(iter.init(table_id, schema_guard, ddl_service_->get_pt_operator(), filter_flag_replica))) {
    LOG_WARN("fail to init partition table iterator",
        K(ret),
        "table_id",
        table_schema->get_table_id(),
        K(table_id),
        K(table_schema->is_binding_table()));
  } else if (OB_FAIL(filter.filter_delete_server(ddl_service_->get_server_manager()))) {
    LOG_WARN("fail to set filter", K(ret));
  } else if (OB_FAIL(filter.set_filter_permanent_offline(ddl_service_->get_server_manager()))) {
    LOG_WARN("fail to set permanent offline filter", K(ret));
  } else if (OB_FAIL(filter.set_persistent_replica_status_not_equal(REPLICA_STATUS_OFFLINE))) {
    LOG_WARN("fail to set replica status", K(ret));
  } else if (OB_FAIL(filter.set_filter_log_replica())) {
    LOG_WARN("fail to set filter log replica", K(ret));
  } else {
    bool is_end = true;
    ObPartitionInfo partition;
    int build_index_ret = OB_SUCCESS;
    while (OB_SUCCESS == ret && OB_SUCCESS == build_index_ret) {
      if (OB_FAIL(iter.next(partition))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("iterate partition table failed", K(ret));
        }
        break;
      } else if (OB_FAIL(partition.filter(filter))) {
        LOG_WARN("filter partition failed", K(ret));
      } else if (partition.get_replicas_v2().empty()) {
        ret = OB_EAGAIN;
        LOG_WARN("partition has no alive replica", K(partition), K(ret));
      }

      ObIndexBuildStatus::PartitionIndexStatus status;
      FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCCESS == ret && OB_SUCCESS == build_index_ret)
      {
        status.reset();
        ret = all_status.find(partition.get_partition_id(), r->server_, status);
        if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("find failed", "partition_id", partition.get_partition_id(), "server", r->server_, K(ret));
        } else if (OB_ENTRY_NOT_EXIST == ret) {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_INFO("local index building not finish",
                K(ret),
                "table_schema_id",
                table_schema->get_table_id(),
                KT(index_id_),
                "partition_id",
                partition.get_partition_id(),
                K(status),
                "current_index_status",
                index_schema->get_index_status(),
                "server",
                r->server_,
                "replica",
                *r);
          }
          ret = OB_SUCCESS;
          is_end = false;
        } else if (OB_SUCCESS != status.ret_code_) {
          build_index_ret = status.ret_code_;
          is_end = true;
          LOG_INFO("local index build failed",
              "table_schema_id",
              table_schema->get_table_id(),
              KT(index_id_),
              "partition_id",
              partition.get_partition_id(),
              K(status),
              "current_index_status",
              index_schema->get_index_status(),
              "server",
              r->server_,
              "replica",
              *r,
              K(build_index_ret));
        }
      }
    }
    if (OB_SUCC(ret) && is_end) {
      // report build index status
      ObIndexStatus new_status = index_schema->get_index_status();
      if (OB_SUCCESS != build_index_ret) {
        new_status = INDEX_STATUS_INDEX_ERROR;
      } else if (INDEX_STATUS_UNAVAILABLE == new_status) {
        new_status = INDEX_STATUS_AVAILABLE;
      }
      if (new_status != index_schema->get_index_status()) {
        LOG_INFO("try to update index status", K(new_status), "index_schema", *index_schema);
        if (OB_FAIL(report_index_status(new_status))) {
          LOG_WARN("fail to report new index status", K(ret));
        } else {
          LOG_INFO("update index status success", K(new_status), "index_schema", *index_schema);
        }
      }
    }
  }
  return ret;
}

int ObRSBuildIndexTask::calc_snapshot_version(const int64_t max_commit_version, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  int64_t gc_snapshot_version = 0;
  int64_t freeze_snapshot_version = 0;
  share::ObSimpleFrozenStatus frozen_status;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRSBuildIndexTask has not been inited", K(ret));
  } else if (OB_UNLIKELY(max_commit_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2000 &&
             OB_FAIL(ddl_service_->get_zone_mgr().get_snapshot_gc_ts_in_memory(gc_snapshot_version))) {
    LOG_WARN("fail to get gc snapshot version", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_2000 &&
             OB_FAIL(ddl_service_->get_freeze_info_mgr().get_snapshot_gc_ts_in_memory(gc_snapshot_version))) {
    LOG_WARN("fail to get gc snapshot version", K(ret));
  }
  if (OB_FAIL(ret)) {
    // nothing todo
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS !=
        (tmp_ret = ddl_service_->get_freeze_info_mgr().get_freeze_info(0L /*latest version*/, frozen_status))) {
      LOG_WARN("fail to get freeze info", K(tmp_ret));
    } else {
      freeze_snapshot_version = frozen_status.frozen_timestamp_;
    }
    const int64_t current_time = ObTimeUtility::current_time();
    snapshot_version = std::max(max_commit_version, gc_snapshot_version);
    snapshot_version = std::max(snapshot_version, freeze_snapshot_version);
    // we expected that the snapshot value is not far from the current timestamp
    snapshot_version = std::max(snapshot_version, current_time - INDEX_SNAPSHOT_VERSION_DIFF);
  }
  return ret;
}

int ObRSBuildIndexTask::acquire_snapshot(const int64_t snapshot_version, const int64_t data_table_id,
    const int64_t schema_version, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRSBuildIndexTask has not been inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version < 0 || OB_INVALID_ID == data_table_id || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(snapshot_version), K(data_table_id), K(schema_version));
  } else {
    ObSnapshotInfo info;
    info.snapshot_type_ = SNAPSHOT_FOR_CREATE_INDEX;
    info.snapshot_ts_ = snapshot_version;
    info.schema_version_ = schema_version;
    info.tenant_id_ = extract_tenant_id(index_id_);
    info.table_id_ = data_table_id;
    common::ObMySQLProxy &proxy = ddl_service_->get_sql_proxy();
    if (!info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(info));
    } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().set_index_building_snapshot(
                   proxy, info.table_id_, info.snapshot_ts_))) {
      LOG_WARN("fail to set index building snapshot", KR(ret), K(info));
    } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().acquire_snapshot_for_building_index(trans, info, index_id_))) {
      LOG_WARN("fail to acquire snapshot", K(ret), K(index_id_), K(data_table_id), K(info));
    }
  }
  return ret;
}

int ObRSBuildIndexTask::release_snapshot()
{
  int ret = OB_SUCCESS;
  const int64_t special_partition_idx = -1;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* index_schema = NULL;
  ObIndexTransStatus status;
  const uint64_t fetch_tenant_id = is_inner_table(index_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(index_id_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRSBuildIndexScheduler has not been inited", K(ret));
  } else if (OB_FAIL(ObIndexTransStatusReporter::get_wait_trans_status(index_id_,
                 ObIndexTransStatusReporter::ROOT_SERVICE,
                 special_partition_idx,
                 ddl_service_->get_sql_proxy(),
                 status))) {
    LOG_WARN("fail to get build index version", K(ret), K(index_id_));
  } else if (status.snapshot_version_ <= 0) {
    // snapshot does not exist, skip
  } else if (OB_FAIL(ddl_service_->get_schema_service().get_tenant_schema_guard(
                 fetch_tenant_id, schema_guard, status.schema_version_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(fetch_tenant_id), K_(index_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_id_, index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(index_id_));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("schema error, index schema must not be NULL", K(ret), K(index_id_));
  } else {
    ObSnapshotInfo info;
    info.snapshot_type_ = SNAPSHOT_FOR_CREATE_INDEX;
    info.snapshot_ts_ = status.snapshot_version_;
    info.schema_version_ = status.schema_version_;
    info.tenant_id_ = extract_tenant_id(index_id_);
    info.table_id_ = index_schema->get_data_table_id();
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_BUILD_INDEX_RELEASE_SNAPSHOT_FAILED) OB_SUCCESS;
      LOG_WARN("release snapshot errsim", K(ret));
    }
#endif
    if (OB_SUCC(ret)) {
      ObMySQLTransaction trans;
      common::ObMySQLProxy& proxy = ddl_service_->get_sql_proxy();
      if (OB_FAIL(trans.start(&proxy))) {
      } else if (OB_FAIL(ddl_service_->get_snapshot_mgr().release_snapshot(trans, info))) {
        LOG_WARN("fail to release snapshot", K(ret), K(index_id_));
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
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_index_build_stat_record())) {
      LOG_WARN("fail to remove index build stat record", K(ret));
    }
  }
  return ret;
}

int ObRSBuildIndexTask::generate_index_build_stat_record()
{
  int ret = OB_SUCCESS;
  const int64_t status = 0;
  const int64_t snapshot_version = 0;
  const int64_t schema_version = 0;
  ObSqlString sql_string;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRSBuildIndexScheduler has not been inited", K(ret));
  } else if (OB_FAIL(
                 sql_string.assign_fmt("REPLACE INTO %s "
                                       "(TENANT_ID, DATA_TABLE_ID, INDEX_TABLE_ID, STATUS, SNAPSHOT, SCHEMA_VERSION) "
                                       "VALUES (%ld, %ld, %ld, %ld, %ld, %ld)",
                     OB_ALL_INDEX_BUILD_STAT_TNAME,
                     extract_tenant_id(index_id_),
                     data_table_id_,
                     index_id_,
                     status,
                     snapshot_version,
                     schema_version_))) {
    LOG_WARN("fail to assign sql string", K(ret));
  } else if (OB_FAIL(ddl_service_->get_sql_proxy().write(sql_string.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  }
#ifdef ERRSIM
  ret = E(EventTable::EN_SUBMIT_INDEX_TASK_ERROR_AFTER_STAT_RECORD) OB_SUCCESS;
#endif
  return ret;
}

int ObRSBuildIndexTask::remove_index_build_stat_record()
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRSBuildIndexScheduler has not been inited", K(ret));
  } else if (OB_FAIL(sql_string.assign_fmt("DELETE FROM %s "
                                           "WHERE TENANT_ID='%ld' AND DATA_TABLE_ID='%ld' AND INDEX_TABLE_ID='%ld'",
                 OB_ALL_INDEX_BUILD_STAT_TNAME,
                 extract_tenant_id(index_id_),
                 data_table_id_,
                 index_id_))) {
    LOG_WARN("fail to assign sql string", K(ret));
  } else if (OB_FAIL(ddl_service_->get_sql_proxy().write(sql_string.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  }
  return ret;
}

ObRSBuildIndexScheduler::ObRSBuildIndexScheduler()
    : is_inited_(false), task_executor_(), is_stop_(false), ddl_service_(NULL)
{}

ObRSBuildIndexScheduler::~ObRSBuildIndexScheduler()
{}

int ObRSBuildIndexScheduler::init(ObDDLService* ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRSBuildIndexScheduler has been inited twice", K(ret));
  } else if (OB_ISNULL(ddl_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ddl_service));
  } else if (OB_FAIL(task_executor_.init(DEFAULT_BUCKET_NUM, lib::TGDefIDs::DDLTaskExecutor1))) {
    LOG_WARN("fail to init task executor", K(ret));
  } else {
    is_inited_ = true;
    ddl_service_ = ddl_service;
  }
  return ret;
}

int ObRSBuildIndexScheduler::push_task(ObRSBuildIndexTask& task)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = E(EventTable::EN_SUBMIT_INDEX_TASK_ERROR_BEFORE_STAT_RECORD) OB_SUCCESS;
#endif
  if (OB_SUCCESS != ret) {
    LOG_INFO("errsim mock push local index task fail", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRSBuildIndexScheduler has not been inited", K(ret));
  } else if (is_stop_) {
    // do nothing
  } else if (OB_FAIL(task.generate_index_build_stat_record())) {
    LOG_WARN("fail to generate index build stat record", K(ret));
  } else if (OB_FAIL(task_executor_.push_task(task))) {
    if (OB_LIKELY(OB_ENTRY_EXIST == ret)) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to push back task", K(ret));
    }
  } else {
    LOG_INFO("succeed to push back build index task", K(task));
  }
  return ret;
}

ObRSBuildIndexScheduler& ObRSBuildIndexScheduler::get_instance()
{
  static ObRSBuildIndexScheduler instance;
  return instance;
}

void ObRSBuildIndexScheduler::stop()
{
  is_stop_ = true;
  task_executor_.stop();
}

void ObRSBuildIndexScheduler::wait()
{
  task_executor_.wait();
}

void ObRSBuildIndexScheduler::destroy()
{
  is_inited_ = false;
  stop();
  wait();
  task_executor_.destroy();
}

ObIndexBuilder::ObIndexBuilder(ObDDLService& ddl_service) : ddl_service_(ddl_service)
{}

ObIndexBuilder::~ObIndexBuilder()
{}

int ObIndexBuilder::create_index(const ObCreateIndexArg& arg, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start create index", K(arg), K(frozen_version));
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!arg.is_valid() || frozen_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(frozen_version), K(ret));
  } else if (OB_FAIL(do_create_index(arg, frozen_version))) {
    LOG_WARN("generate_schema failed", K(arg), K(frozen_version), K(ret));
  }
  if (OB_ERR_TABLE_EXIST == ret) {
    if (true == arg.if_not_exist_) {
      ret = OB_SUCCESS;
      LOG_USER_WARN(OB_ERR_KEY_NAME_DUPLICATE, arg.index_name_.length(), arg.index_name_.ptr());
    } else {
      ret = OB_ERR_KEY_NAME_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_KEY_NAME_DUPLICATE, arg.index_name_.length(), arg.index_name_.ptr());
    }
  }
  LOG_INFO("finish create index", K(arg), K(frozen_version), K(ret));
  return ret;
}

int ObIndexBuilder::drop_index(const ObDropIndexArg& arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;

  const bool is_index = false;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const ObTableSchema* table_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  bool to_recyclebin = arg.to_recyclebin();
  bool is_db_in_recyclebin = false;
  schema_guard.set_session_id(arg.session_id_);
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id, arg.database_name_, arg.table_name_, is_index, table_schema))) {
    LOG_WARN("failed to get data table schema", K(arg), K(ret));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(arg.database_name_), to_cstring(arg.table_name_));
    LOG_WARN("table not found", K(arg), K(ret));
  } else if (table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not drop index of table in recyclebin.", K(ret), K(arg));
  } else if (OB_FAIL(schema_guard.check_database_in_recyclebin(table_schema->get_database_id(), is_db_in_recyclebin))) {
    LOG_WARN("check database in recyclebin failed", K(ret));
  } else if (is_db_in_recyclebin) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("Can not drop index of db in recyclebin", K(ret), K(arg));
  }

  if (OB_SUCC(ret)) {
    const uint64_t table_id = table_schema->get_table_id();
    const ObTableSchema* index_table_schema = NULL;
    if (to_recyclebin && table_schema->is_tmp_table()) {
      to_recyclebin = false;
    }

    if (OB_INVALID_ID != arg.index_table_id_) {
      LOG_DEBUG("drop index with index_table_id", K(arg.index_table_id_));
      if (OB_FAIL(schema_guard.get_table_schema(arg.index_table_id_, index_table_schema))) {
        LOG_WARN("fail to get index table schema", K(ret), K(arg.index_table_id_));
      }
    } else {
      ObString index_table_name;
      if (OB_FAIL(ObTableSchema::build_index_table_name(allocator, table_id, arg.index_name_, index_table_name))) {
        LOG_WARN("build_index_table_name failed", K(arg), K(table_id), K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(
                     tenant_id, table_schema->get_database_id(), index_table_name, true, index_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(index_table_schema));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
      LOG_WARN("index table schema should not be null", K(arg.index_name_), K(ret));
      LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, arg.index_name_.length(), arg.index_name_.ptr());
    } else {
      // construct an arg for drop table
      ObTableItem table_item;
      table_item.database_name_ = arg.database_name_;
      table_item.table_name_ = index_table_schema->get_table_name();
      ObDropTableArg drop_table_arg;
      drop_table_arg.tenant_id_ = arg.tenant_id_;
      drop_table_arg.if_exist_ = false;
      drop_table_arg.table_type_ = USER_INDEX;
      drop_table_arg.ddl_stmt_str_ = arg.ddl_stmt_str_;
      if (OB_FAIL(drop_table_arg.tables_.push_back(table_item))) {
        LOG_WARN("failed to add table item!", K(table_item), K(ret));
      } else if (OB_FAIL(ddl_service_.drop_table(drop_table_arg))) {
        if (OB_TABLE_NOT_EXIST == ret) {
          ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
          LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, arg.index_name_.length(), arg.index_name_.ptr());
          LOG_WARN("index not exist, can't drop it", K(arg), K(ret));
        } else {
          LOG_WARN("drop_table failed", K(arg), K(drop_table_arg), K(ret));
        }
      }
    }
  }
  LOG_INFO("finish drop index", K(arg), K(ret));
  return ret;
}

int ObIndexBuilder::do_create_global_index(share::schema::ObSchemaGetterGuard& schema_guard,
    const obrpc::ObCreateIndexArg& arg, const share::schema::ObTableSchema& table_schema, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  ObTableSchema& index_schema = const_cast<ObTableSchema&>(arg.index_schema_);
  ObArray<ObColumnSchemaV2*> gen_columns;
  const bool global_index_without_column_info = false;
  bool gts_on = false;
  int64_t cur_ts_type = 0;
  ObTableSchema new_table_schema;
  if (OB_FAIL(new_table_schema.assign(table_schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else if (!new_table_schema.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to copy table schema", K(ret));
  } else if (OB_FAIL(ObIndexBuilderUtil::adjust_expr_index_args(
                 const_cast<ObCreateIndexArg&>(arg), new_table_schema, gen_columns))) {
    LOG_WARN("fail to adjust expr index args", K(ret));
  } else if (OB_FAIL(generate_schema(
                 arg, frozen_version, new_table_schema, global_index_without_column_info, index_schema))) {
    LOG_WARN("fail to generate schema", K(ret), K(frozen_version), K(arg));
  } else if (OB_FAIL(schema_guard.get_timestamp_service_type(table_schema.get_tenant_id(), cur_ts_type))) {
    LOG_WARN("fail to get cur ts type", K(ret));
  } else {
    gts_on = transaction::is_ts_type_external_consistent(cur_ts_type);
    if (!gts_on) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create global index when GTS is off");
    } else if (gen_columns.empty()) {
      if (OB_FAIL(ddl_service_.create_global_index(arg, new_table_schema, index_schema, frozen_version))) {
        LOG_WARN("fail to create global index", K(ret));
      }
    } else {
      index_schema.set_create_mem_version(frozen_version);
      if (OB_FAIL(ddl_service_.create_global_inner_expr_index(
              arg, table_schema, new_table_schema, gen_columns, index_schema, frozen_version))) {
        LOG_WARN("fail to create global inner expr index", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_CREATE_TABLE_MODE_RESTORE == arg.create_mode_) {
      // skip
    } else if (OB_FAIL(submit_build_global_index_task(index_schema))) {
      LOG_WARN("fail to submit build global index task", K(ret));
    }
  }
  return ret;
}

int ObIndexBuilder::submit_build_global_index_task(const ObTableSchema& index_schema)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = index_schema.get_index_type();
  if (!index_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index_schema is invalid", K(ret), K(index_schema));
  } else if (INDEX_TYPE_NORMAL_GLOBAL != index_type && INDEX_TYPE_UNIQUE_GLOBAL != index_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should be global index_type", K(ret), K(index_type));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema* inner_index_schema = NULL;
    const uint64_t tenant_id = index_schema.get_tenant_id();
    if (OB_UNLIKELY(NULL == GCTX.root_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root service ptr is null", K(ret));
    } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_schema.get_table_id(), inner_index_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (OB_UNLIKELY(NULL == inner_index_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("fail to get index schema", K(ret));
    } else if (OB_FAIL(
                   GCTX.root_service_->get_global_index_builder().submit_build_global_index_task(inner_index_schema))) {
      if (OB_NOT_INIT == ret) {
        ret = OB_EAGAIN;
      }
    }
    // submit retry task if retryable, otherwise report error
    if (OB_EAGAIN == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
      int record_ret = ret;
      if (OB_FAIL(GCTX.ob_service_->submit_retry_ghost_index_task(index_schema.get_table_id()))) {
        LOG_WARN("fail to submit retry ghost index task", K(ret));
        ret = OB_TIMEOUT;
      } else {
        LOG_INFO(
            "submit build global index task fail but fast retryable", K(record_ret), K(index_schema.get_table_id()));
      }
    } else if (OB_FAIL(ret)) {
      LOG_WARN("submit global index task fail, mark it as timeout", K(ret));
      ret = OB_TIMEOUT;
    }
  }
  return ret;
}

int ObIndexBuilder::do_create_local_index(const obrpc::ObCreateIndexArg& create_index_arg,
    const share::schema::ObTableSchema& table_schema, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  ObTableSchema index_schema;
  ObSEArray<ObColumnSchemaV2*, 1> gen_columns;
  ObTableSchema new_table_schema;
  if (OB_FAIL(new_table_schema.assign(table_schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else if (!new_table_schema.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to copy table schema", K(ret));
  } else {
    obrpc::ObCreateIndexArg& my_arg = const_cast<ObCreateIndexArg&>(create_index_arg);
    const bool global_index_without_column_info = true;
    // build a global index with local storage if both the data table and index table are non-partitioned
    if (INDEX_TYPE_NORMAL_GLOBAL == my_arg.index_type_) {
      my_arg.index_type_ = INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE;
      my_arg.index_schema_.set_index_type(INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE);
    } else if (INDEX_TYPE_UNIQUE_GLOBAL == my_arg.index_type_) {
      my_arg.index_type_ = INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE;
      my_arg.index_schema_.set_index_type(INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE);
    }
    if (OB_FAIL(ObIndexBuilderUtil::adjust_expr_index_args(
            const_cast<ObCreateIndexArg&>(my_arg), new_table_schema, gen_columns))) {
      LOG_WARN("fail to adjust expr index args", K(ret));
    } else if (OB_FAIL(generate_schema(
                   my_arg, frozen_version, new_table_schema, global_index_without_column_info, index_schema))) {
      LOG_WARN("fail to generate schema", K(ret), K(frozen_version), K(my_arg));
    } else if (OB_FAIL(new_table_schema.check_create_index_on_hidden_primary_key(index_schema))) {
      LOG_WARN("failed to check create index on table", K(ret), K(index_schema));
    } else if (gen_columns.empty()) {
      if (OB_FAIL(ddl_service_.create_user_table(my_arg, index_schema, frozen_version))) {
        LOG_WARN("fail to create index", K(ret), K(frozen_version), K(index_schema));
      }
    } else {
      index_schema.set_create_mem_version(frozen_version);
      if (OB_FAIL(ddl_service_.create_inner_expr_index(table_schema,
              new_table_schema,
              gen_columns,
              index_schema,
              my_arg.create_mode_,
              frozen_version,
              &my_arg.ddl_stmt_str_))) {
        LOG_WARN("fail to create inner expr index", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_CREATE_TABLE_MODE_RESTORE == create_index_arg.create_mode_) {
      my_arg.index_schema_.set_table_id(index_schema.get_table_id());
      my_arg.index_schema_.set_schema_version(index_schema.get_schema_version());
    } else if (OB_FAIL(submit_build_local_index_task(index_schema))) {
      LOG_WARN("failt to submit build local index task", K(ret));
    } else {
      my_arg.index_schema_.set_table_id(index_schema.get_table_id());
      my_arg.index_schema_.set_schema_version(index_schema.get_schema_version());
    }
  }
  return ret;
}

int ObIndexBuilder::submit_build_local_index_task(const ObTableSchema& index_schema)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = index_schema.get_index_type();
  if (!index_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index_schema is invalid", K(ret), K(index_schema));
  } else if (INDEX_TYPE_IS_NOT == index_type || INDEX_TYPE_NORMAL_GLOBAL == index_type ||
             INDEX_TYPE_UNIQUE_GLOBAL == index_type || INDEX_TYPE_MAX == index_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index_type", K(ret), K(index_type));
  } else {
    ObRSBuildIndexTask task;
    if (OB_FAIL(task.init(index_schema.get_table_id(),
            index_schema.get_data_table_id(),
            index_schema.get_schema_version(),
            &ddl_service_))) {
      LOG_WARN("fail to init ObRSBuildIndexTask", K(ret), "index_id", index_schema.get_table_id());
    } else if (OB_FAIL(ObRSBuildIndexScheduler::get_instance().push_task(task))) {
      LOG_WARN("fail to add task into ObRSBuildIndexScheduler", K(ret));
    }

    // submit retry task if retryable, otherwise report error
    if (OB_EAGAIN == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
      int record_ret = ret;
      if (OB_FAIL(GCTX.ob_service_->submit_retry_ghost_index_task(index_schema.get_table_id()))) {
        LOG_WARN("fail to submit retry ghost index task", K(ret));
        ret = OB_TIMEOUT;
      } else {
        LOG_INFO(
            "submit build local index task fail but fast retryable", K(record_ret), K(index_schema.get_table_id()));
      }
    } else if (OB_FAIL(ret)) {
      obrpc::ObUpdateIndexStatusArg arg;
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema* new_index_schema = NULL;
      arg.index_table_id_ = index_schema.get_table_id();
      arg.status_ = INDEX_STATUS_INDEX_ERROR;
      arg.create_mem_version_ = index_schema.get_create_mem_version();
      const uint64_t fetch_tenant_id =
          is_inner_table(index_schema.get_table_id()) ? OB_SYS_TENANT_ID : index_schema.get_tenant_id();
      arg.exec_tenant_id_ = fetch_tenant_id;
      if (OB_FAIL(ddl_service_.get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).update_index_status(arg))) {
        LOG_ERROR("fail to update index status", K(ret), K(arg));
      } else if (OB_FAIL(ddl_service_.get_schema_service().get_tenant_schema_guard(fetch_tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret), K(fetch_tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schema(arg.index_table_id_, new_index_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(arg.index_table_id_));
      } else if (OB_ISNULL(new_index_schema)) {
        LOG_WARN("can not find this index schema", K(ret), K(arg.index_table_id_));
        ret = OB_SUCCESS;
      } else {
        LOG_INFO("update index status success", LITERAL_K(INDEX_STATUS_INDEX_ERROR), "index_schema", *new_index_schema);
      }
    }
  }
  return ret;
}

// not generate table_id for index, caller will do that
// if we pass the data_schema argument, the create_index_arg can not set database_name
// and table_name, which will used for getting data table schema in generate_schema
int ObIndexBuilder::do_create_index(const ObCreateIndexArg& arg, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const bool is_index = false;
  const ObTableSchema* table_schema = NULL;
  uint64_t table_id = OB_INVALID_ID;
  bool in_tenant_space = true;
  schema_guard.set_session_id(arg.session_id_);
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!arg.is_valid() || frozen_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(frozen_version), K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(arg.tenant_id_, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 arg.tenant_id_, arg.database_name_, arg.table_name_, is_index, table_schema))) {
    LOG_WARN("get_table_schema failed", K(arg), K(ret));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(arg.database_name_), to_cstring(arg.table_name_));
    LOG_WARN("table not exist", K(arg), K(ret));
  } else if (FALSE_IT(table_id = table_schema->get_table_id())) {
  } else if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(table_id, in_tenant_space))) {
    LOG_WARN("fail to check table in tenant space", K(ret), K(table_id));
  } else if (in_tenant_space && (is_sys_table(table_id) || OB_SYS_TENANT_ID != extract_tenant_id(table_id))) {
    // FIXME:
    // 1) do not support index on sys table
    // 2) do not support index on virtual table of user tenant
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("create index on sys table in tenant space not supported", K(ret), K(table_id));
  } else if (!arg.is_inner_ && table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can not add index on table in recyclebin", K(ret), K(arg));
  } else if (table_schema->is_in_splitting()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not create index during splitting", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.check_restore_point_allow(extract_tenant_id(table_id), table_id))) {
    LOG_WARN("failed to check restore point allow.", K(ret), K(extract_tenant_id(table_id)), K(table_id));
  } else if (table_schema->get_index_tid_count() >= OB_MAX_INDEX_PER_TABLE) {
    ret = OB_ERR_TOO_MANY_KEYS;
    LOG_USER_ERROR(OB_ERR_TOO_MANY_KEYS, OB_MAX_INDEX_PER_TABLE);
    int64_t index_count = table_schema->get_index_tid_count();
    LOG_WARN("too many index for table", K(OB_MAX_INDEX_PER_TABLE), K(index_count), K(ret));
  } else if (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_ || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_ ||
             INDEX_TYPE_DOMAIN_CTXCAT == arg.index_type_) {
    if (OB_FAIL(do_create_local_index(arg, *table_schema, frozen_version))) {
      LOG_WARN("fail to do create local index", K(ret), K(arg));
    }
  } else if (INDEX_TYPE_NORMAL_GLOBAL == arg.index_type_ || INDEX_TYPE_UNIQUE_GLOBAL == arg.index_type_) {
    if (!table_schema->is_partitioned_table() && !arg.index_schema_.is_partitioned_table()) {
      // create a global index with local storage when both the data table and index table are non-partitioned
      if (OB_FAIL(do_create_local_index(arg, *table_schema, frozen_version))) {
        LOG_WARN("fail to do create local index", K(ret));
      }
    } else {
      if (OB_FAIL(do_create_global_index(schema_guard, arg, *table_schema, frozen_version))) {
        LOG_WARN("fail to do create global index", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index type unexpected", K(ret), "index_type", arg.index_type_);
  }
  return ret;
}

/* after global index is introducted, the arguments of this interface become complicated
 * if this interface is modified, please update this comments
 * modified
 *   ObIndexBuilder::generate_schema is invoked in thoe following three circumstances:
 *   1. invoked when Create index to build global index: this interface helps to specifiy partition columns,
 *      the column information of index schema is generated during the resolve stage, no need to generate
 *      column information of the index schema any more in this interface,
 *      the argument global_index_without_column_info is false for this situation
 *   2. invoked when Create table with index: this interface helps to specify partition columns,
 *      the column information of index schema is generated during the resolve stage, need to to generate
 *      column information of the index schema any more in this interface,
 *      the argument global_index_without_column_info is false for this situation
 *   3. invoked when Alter table with index, in this situation only non-partition global index can be build,
 *      column information is not filled in the index schema and the global_index_without_column_info is true.
 *   when generate_schema is invoked to build a local index or a global index with local storage,
 *   global_index_without_column_info is false.
 */
int ObIndexBuilder::generate_schema(const ObCreateIndexArg& arg, const int64_t frozen_version,
    ObTableSchema& data_schema, const bool global_index_without_column_info, ObTableSchema& schema)
{
  int ret = OB_SUCCESS;
  // some items in arg may be invalid, don't check arg here(when create table with index, alter
  // table add index)
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (frozen_version <= 0 || !data_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(frozen_version), K(data_schema), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (arg.index_columns_.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index columns can't be empty", "index columns", arg.index_columns_, K(ret));
    } else {
    }

    // do some check
    if (OB_SUCC(ret)) {
      if (ddl_service_.is_sync_primary_ddl()) {
        // this is not used on standby cluster
        // since the ddl of standby cluster is synchronized from primary cluster
      } else if (!GCONF.enable_sys_table_ddl) {
        if (!data_schema.is_user_table() && !data_schema.is_tmp_table()) {
          ret = OB_ERR_WRONG_OBJECT;
          LOG_USER_ERROR(
              OB_ERR_WRONG_OBJECT, to_cstring(arg.database_name_), to_cstring(arg.table_name_), "BASE_TABLE");
          ObTableType table_type = data_schema.get_table_type();
          LOG_WARN("Not support to create index on non-normal table", K(table_type), K(arg), K(ret));
        } else if (OB_INVALID_ID != arg.index_table_id_ || OB_INVALID_ID != arg.data_table_id_) {
          char err_msg[number::ObNumber::MAX_PRINTABLE_SIZE];
          MEMSET(err_msg, 0, sizeof(err_msg));
          // create index specifying index_id can only be used when the configuration is on
          ret = OB_OP_NOT_ALLOW;
          (void)snprintf(err_msg, sizeof(err_msg), "%s", "create index with index_table_id/data_table_id");
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
        }
      }

      if (OB_SUCC(ret) && (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_ || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_ ||
                              INDEX_TYPE_DOMAIN_CTXCAT == arg.index_type_)) {
        if (OB_FAIL(sql::ObResolverUtils::check_unique_index_cover_partition_column(data_schema, arg))) {
          RS_LOG(WARN, "fail to check unique key cover partition column", K(ret));
          if (INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_ && OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF == ret) {
            int tmp_ret = OB_SUCCESS;
            bool allow = false;
            if (OB_SUCCESS !=
                (tmp_ret = ObDDLResolver::check_uniq_allow(data_schema, const_cast<ObCreateIndexArg&>(arg), allow))) {
              RS_LOG(WARN, "fail to check uniq allow", K(ret));
            } else if (allow) {
              RS_LOG(INFO, "uniq index allowd, deduced by constraint", K(ret));
              ret = OB_SUCCESS;
            }
          }
        }
      }
      int64_t index_data_length = 0;
      bool is_ctxcat_added = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
        const ObColumnSchemaV2* data_column = NULL;
        const ObColumnSortItem& sort_item = arg.index_columns_.at(i);
        if (NULL == (data_column = data_schema.get_column_schema(sort_item.column_name_))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(
              OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("get_column_schema failed",
              "tenant_id",
              data_schema.get_tenant_id(),
              "database_id",
              data_schema.get_database_id(),
              "table_name",
              data_schema.get_table_name(),
              "column name",
              sort_item.column_name_,
              K(ret));
        } else if (OB_INVALID_ID != sort_item.get_column_id() &&
                   data_column->get_column_id() != sort_item.get_column_id()) {
          ret = OB_ERR_INVALID_COLUMN_ID;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_ID, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("Column ID specified by create index mismatch with data table Column ID",
              "data_table_column_id",
              data_column->get_column_id(),
              "user_specidifed_column_id",
              sort_item.get_column_id(),
              K(ret));
        } else if (sort_item.prefix_len_ > 0) {
          if ((index_data_length += sort_item.prefix_len_) > OB_MAX_USER_ROW_KEY_LENGTH) {
            ret = OB_ERR_TOO_LONG_KEY_LENGTH;
            LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_USER_ROW_KEY_LENGTH);
            LOG_WARN("index table rowkey length over max_user_row_key_length",
                K(index_data_length),
                LITERAL_K(OB_MAX_USER_ROW_KEY_LENGTH),
                K(ret));
          }
        } else if (ob_is_text_tc(data_column->get_data_type()) && !data_column->is_fulltext_column()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("index created direct on large text column should only be fulltext", K(arg.index_type_), K(ret));
        } else if (ObTimestampTZType == data_column->get_data_type() && arg.is_unique_primary_index()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("TIMESTAMP WITH TIME ZONE column can't be primary/unique key", K(arg.index_type_), K(ret));
        } else if (data_column->get_meta_type().is_blob()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("fulltext index created on blob column is not supported", K(arg.index_type_), K(ret));
        } else if (ob_is_json_tc(data_column->get_data_type())) {
          ret = OB_ERR_JSON_USED_AS_KEY;
          LOG_USER_ERROR(OB_ERR_JSON_USED_AS_KEY, sort_item.column_name_.length(), sort_item.column_name_.ptr());
          LOG_WARN("JSON column cannot be used in key specification.", K(arg.index_type_), K(ret));
        } else if (data_column->is_string_type()) {
          int64_t length = 0;
          if (data_column->is_fulltext_column()) {
            if (!is_ctxcat_added) {
              length = OB_MAX_OBJECT_NAME_LENGTH;
              is_ctxcat_added = true;
            }
          } else if (OB_FAIL(data_column->get_byte_length(length))) {
            LOG_WARN("fail to get byte length of column", K(ret));
          } else if (length <= 0) {
            ret = OB_ERR_WRONG_KEY_COLUMN;
            LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, sort_item.column_name_.length(), sort_item.column_name_.ptr());
            LOG_WARN("byte_length of string type column is less than zero", K(length), K(ret));
          } else { /*do nothing*/
          }

          if (OB_SUCC(ret)) {
            index_data_length += length;
            if (index_data_length > OB_MAX_USER_ROW_KEY_LENGTH) {
              ret = OB_ERR_TOO_LONG_KEY_LENGTH;
              LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, (OB_MAX_USER_ROW_KEY_LENGTH));
              LOG_WARN("index table rowkey length over max_user_row_key_length",
                  K(index_data_length),
                  LITERAL_K(OB_MAX_USER_ROW_KEY_LENGTH),
                  K(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      // column information of the global index is filled during the resolve stage
      const bool is_index_local_storage =
          (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_ || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_ ||
              INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == arg.index_type_ ||
              INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == arg.index_type_ || INDEX_TYPE_DOMAIN_CTXCAT == arg.index_type_);
      const bool need_generate_index_schema_column = (is_index_local_storage || global_index_without_column_info);
      schema.set_table_mode(data_schema.get_table_mode());
      if (OB_FAIL(set_basic_infos(arg, frozen_version, data_schema, schema))) {
        LOG_WARN("set_basic_infos failed", K(arg), K(frozen_version), K(data_schema), K(ret));
      } else if (need_generate_index_schema_column && OB_FAIL(set_index_table_columns(arg, data_schema, schema))) {
        LOG_WARN("set_index_table_columns failed", K(arg), K(data_schema), K(ret));
      } else if (OB_FAIL(set_index_table_options(arg, data_schema, schema))) {
        LOG_WARN("set_index_table_options failed", K(arg), K(data_schema), K(ret));
      } else if (!is_index_local_storage) {
        if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_1440) {
          schema.get_part_option().set_partition_cnt_within_partition_table(0);
        }
      } else {
        LOG_INFO("finish generate index schema", K(schema));
      }
    }
  }
  return ret;
}

int ObIndexBuilder::set_basic_infos(
    const ObCreateIndexArg& arg, const int64_t frozen_version, const ObTableSchema& data_schema, ObTableSchema& schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObDatabaseSchema* database = NULL;
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (OB_FAIL(ddl_service_.get_schema_service().get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema_guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(data_schema.get_database_id(), database))) {
    LOG_WARN("fail to get database_schema", K(ret), "database_id", data_schema.get_database_id());
  } else if (OB_ISNULL(database)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database_schema is null", K(ret), "database_id", data_schema.get_database_id());
  } else if (frozen_version <= 0 || !data_schema.is_valid()) {
    // some items in arg may be invalid, don't check arg
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(frozen_version), K(data_schema), K(ret));
  } else {
    ObString index_table_name = arg.index_name_;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    const uint64_t table_schema_id = data_schema.get_table_id();
    bool use_orig_index_name = false;
    // index building is supported for baseline data restore using the original name
    if (OB_CREATE_TABLE_MODE_RESTORE == arg.create_mode_ &&
        (index_table_name.prefix_match(OB_MYSQL_RECYCLE_PREFIX) ||
            index_table_name.prefix_match(OB_ORACLE_RECYCLE_PREFIX))) {
      use_orig_index_name = true;
    }
    if (table_schema_id != ((OB_INVALID_ID == arg.data_table_id_) ? table_schema_id : arg.data_table_id_)) {
      // need to check if the data table ids are the same when data table id is specified
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid data table id", K(table_schema_id), K(arg.data_table_id_), K(ret));
    } else if (!use_orig_index_name && OB_FAIL(ObTableSchema::build_index_table_name(
                                           allocator, table_schema_id, arg.index_name_, index_table_name))) {
      LOG_WARN("build_index_table_name failed", K(table_schema_id), K(arg), K(ret));
    } else if (OB_FAIL(schema.set_table_name(index_table_name))) {
      LOG_WARN("set_table_name failed", K(index_table_name), K(arg), K(ret));
    } else {
      schema.set_table_id(arg.index_table_id_);
      schema.set_table_type(USER_INDEX);
      schema.set_index_type(arg.index_type_);
      schema.set_index_status(arg.index_option_.index_status_);
      schema.set_data_table_id(table_schema_id);

      // priority same with data table schema
      schema.set_tenant_id(data_schema.get_tenant_id());
      schema.set_database_id(data_schema.get_database_id());
      schema.set_tablegroup_id(OB_INVALID_ID);
      // TODO: may support binding global index to tablegroup
      schema.set_binding(false);
      schema.set_load_type(data_schema.get_load_type());
      schema.set_def_type(data_schema.get_def_type());
      if (INDEX_TYPE_NORMAL_LOCAL == arg.index_type_ || INDEX_TYPE_UNIQUE_LOCAL == arg.index_type_) {
        schema.set_part_level(data_schema.get_part_level());
      } else {
      }  // partition level is filled during resolve stage for global index
      schema.set_charset_type(data_schema.get_charset_type());
      schema.set_collation_type(data_schema.get_collation_type());
      schema.set_row_store_type(data_schema.get_row_store_type());
      schema.set_store_format(data_schema.get_store_format());
      schema.set_storage_format_version(data_schema.get_storage_format_version());
      schema.set_tablespace_id(arg.index_schema_.get_tablespace_id());
      schema.set_encryption_str(arg.index_schema_.get_encryption_str());
      if (data_schema.get_max_used_column_id() > schema.get_max_used_column_id()) {
        schema.set_max_used_column_id(data_schema.get_max_used_column_id());
      }
      schema.set_create_mem_version(frozen_version + 1);
      // index table will not contain auto increment column
      schema.set_autoinc_column_id(0);
      schema.set_progressive_merge_num(data_schema.get_progressive_merge_num());
      schema.set_progressive_merge_round(data_schema.get_progressive_merge_round());
      if (OB_FAIL(schema.set_compress_func_name(data_schema.get_compress_func_name()))) {
        LOG_WARN("set_compress_func_name failed", K(data_schema));
      }
    }
  }
  return ret;
}

int ObIndexBuilder::set_index_table_columns(
    const ObCreateIndexArg& arg, const ObTableSchema& data_schema, ObTableSchema& schema)
{
  int ret = OB_SUCCESS;
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (OB_FAIL(ObIndexBuilderUtil::set_index_table_columns(arg, data_schema, schema))) {
    LOG_WARN("fail to set index table columns", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObIndexBuilder::set_index_table_options(const obrpc::ObCreateIndexArg& arg,
    const share::schema::ObTableSchema& data_schema, share::schema::ObTableSchema& schema)
{
  int ret = OB_SUCCESS;
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (!data_schema.is_valid()) {
    // some items in arg may be invalid, don't check arg
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_schema), K(ret));
  } else {
    schema.set_block_size(arg.index_option_.block_size_);
    schema.set_tablet_size(data_schema.get_tablet_size());
    schema.set_pctfree(data_schema.get_pctfree());
    schema.set_index_attributes_set(arg.index_option_.index_attributes_set_);
    schema.set_is_use_bloomfilter(arg.index_option_.use_bloom_filter_);
    // schema.set_progressive_merge_num(arg.index_option_.progressive_merge_num_);
    schema.set_index_using_type(arg.index_using_type_);
    schema.set_row_store_type(data_schema.get_row_store_type());
    schema.set_store_format(data_schema.get_store_format());
    // set dop for index table
    schema.set_dop(arg.index_schema_.get_dop());
    if (OB_FAIL(schema.set_compress_func_name(data_schema.get_compress_func_name()))) {
      LOG_WARN("set_compress_func_name failed", K(ret), "compress method", data_schema.get_compress_func_name());
    } else if (OB_FAIL(schema.set_comment(arg.index_option_.comment_))) {
      LOG_WARN("set_comment failed", "comment", arg.index_option_.comment_, K(ret));
    } else if (OB_FAIL(schema.set_parser_name(arg.index_option_.parser_name_))) {
      LOG_WARN("set parser name failed", K(ret), "parser_name", arg.index_option_.parser_name_);
    }
  }
  return ret;
}

int ObIndexBuilder::update_local_index_status(const volatile bool& stop, const int64_t merged_version)
{
  int ret = OB_SUCCESS;
  bool need_retry = false;
  ObSchemaGetterGuard schema_guard;
  ObTableIterator iter;
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (merged_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merged version", K(merged_version), K(ret));
  } else if (OB_FAIL(ddl_service_.get_schema_service().get_schema_guard(schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(iter.init(&schema_guard))) {
    LOG_WARN("init table iterator failed", K(ret));
  } else {
    while (!stop && OB_SUCCESS == ret) {
      uint64_t table_id = OB_INVALID_ID;
      if (OB_FAIL(iter.next(table_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("iterator table failed", K(ret));
        }
        break;
      }

      const ObTableSchema* table = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(table_id, table))) {
        LOG_WARN("get table schema failed", K(ret), KT(table_id));
      } else if (NULL == table) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(table), K(table_id));
      } else {
        if (table->is_storage_index_table() && merged_version > table->get_create_mem_version() &&
            !is_final_index_status(table->get_index_status(), table->is_dropped_schema())) {
          if (OB_FAIL(update_local_index_status(stop, merged_version, table_id))) {
            if (OB_EAGAIN == ret) {
              need_retry = true;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("update local index status failed", K(ret), K(merged_version), KT(table_id));
            }
          }
        }
      }
    }
    if (OB_SUCCESS == ret && stop) {
      ret = OB_CANCELED;
    }
  }

  if (OB_SUCCESS == ret && need_retry) {
    LOG_INFO("not all index table status updated, need retry", K(merged_version));
    ret = OB_EAGAIN;
  }

  return ret;
}

bool ObIndexBuilder::is_final_index_status(const ObIndexStatus index_status, const bool is_dropped_schema) const
{
  return (INDEX_STATUS_AVAILABLE == index_status || INDEX_STATUS_UNIQUE_INELIGIBLE == index_status ||
          is_error_index_status(index_status, is_dropped_schema));
}

int ObIndexBuilder::update_local_index_status(
    const volatile bool& stop, const int64_t merged_version, const uint64_t index_table_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* index_schema = NULL;
  const ObTableSchema* table_schema = NULL;
  ObIndexBuildStatus all_status;
  ObTablePartitionIterator iter;
  ObReplicaFilterHolder filter;
  const uint64_t fetch_tenant_id =
      is_inner_table(index_table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(index_table_id);
  if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl_service not init", "ddl_service inited", ddl_service_.is_inited(), K(ret));
  } else if (merged_version <= 0 || OB_INVALID_ID == index_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(merged_version), KT(index_table_id), K(ret));
  } else if (OB_FAIL(ddl_service_.get_schema_service().get_tenant_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("get schema manager failed", K(ret), K(fetch_tenant_id), K(index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_table_id, index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(index_table_id));
  } else if (NULL == index_schema) {
    ret = OB_SUCCESS;  // ignore table not found error
  } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), table_schema))) {
    LOG_WARN(
        "fail to get table schema", KT(index_table_id), "table_schema_id", index_schema->get_data_table_id(), K(ret));
  } else if (NULL == table_schema) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("data table not found while index table exist",
        KT(index_table_id),
        "table_schema_id",
        index_schema->get_data_table_id(),
        K(ret));
  } else if (merged_version <= index_schema->get_create_mem_version() ||
             is_final_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
    LOG_INFO("index table schema changed", K(merged_version), "index_schema", *index_schema);
  } else if (OB_FAIL(all_status.load_all(index_table_id, -1 /*all partitions*/, ddl_service_.get_sql_proxy()))) {
    LOG_WARN("load all index build status failed", KT(index_table_id), K(ret));
  } else if (stop) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(iter.init(table_schema->get_table_id(), schema_guard, ddl_service_.get_pt_operator()))) {
    LOG_WARN("init partition table iterator failed",
        "table_id",
        table_schema->get_table_id(),
        "first_part_num",
        table_schema->get_first_part_num(),
        "part_num",
        table_schema->get_all_part_num(),
        K(ret));
  } else if (OB_FAIL(filter.filter_delete_server(ddl_service_.get_server_manager()))) {
    LOG_WARN("set filter failed", K(ret));
  } else if (OB_FAIL(filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
    LOG_WARN("set filter failed", K(ret));
  } else if (OB_FAIL(filter.set_filter_log_replica())) {
    LOG_WARN("set filter failed", K(ret));
  } else {
    ObPartitionInfo partition;
    int failed_ret = OB_SUCCESS;
    while (!stop && OB_SUCCESS == ret && OB_SUCCESS == failed_ret) {
      if (OB_FAIL(iter.next(partition))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("iterate partition table failed", K(ret));
        }
        break;
      }
      if (OB_FAIL(partition.filter(filter))) {
        LOG_WARN("filter partition failed", K(ret));
      } else if (partition.get_replicas_v2().empty()) {
        ret = OB_EAGAIN;
        LOG_WARN("partition has no alive replica", K(partition), K(ret));
      }

      ObIndexBuildStatus::PartitionIndexStatus status;
      FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCCESS == ret && OB_SUCCESS == failed_ret)
      {
        status.reset();
        ret = all_status.find(partition.get_partition_id(), r->server_, status);
        if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("find failed", "partition_id", partition.get_partition_id(), "server", r->server_, K(ret));
        } else if (OB_ENTRY_NOT_EXIST == ret || (status.index_status_ != index_schema->get_index_status())) {
          ret = OB_EAGAIN;
          LOG_INFO("local index building not finish",
              K(ret),
              "table_schema_id",
              table_schema->get_table_id(),
              KT(index_table_id),
              "partition_id",
              partition.get_partition_id(),
              K(status),
              "current_index_status",
              index_schema->get_index_status(),
              "server",
              r->server_,
              "replica",
              *r);
        } else if (OB_SUCCESS != status.ret_code_) {
          failed_ret = status.ret_code_;
          LOG_INFO("local index build failed",
              "table_schema_id",
              table_schema->get_table_id(),
              KT(index_table_id),
              "partition_id",
              partition.get_partition_id(),
              K(status),
              "current_index_status",
              index_schema->get_index_status(),
              "server",
              r->server_,
              "replica",
              *r,
              K(failed_ret));
        }
      }
    }
    if (OB_SUCCESS == ret && stop) {
      ret = OB_CANCELED;
    }
    if (OB_SUCC(ret)) {
      int64_t create_mem_version = index_schema->get_create_mem_version();
      ObIndexStatus new_status = index_schema->get_index_status();
      if (OB_SUCCESS != failed_ret) {
        new_status = INDEX_STATUS_INDEX_ERROR;
      } else if (INDEX_STATUS_UNAVAILABLE == new_status) {
        if (INDEX_TYPE_UNIQUE_LOCAL == index_schema->get_index_type() ||
            INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_schema->get_index_type()) {
          new_status = INDEX_STATUS_UNIQUE_CHECKING;
          // Update create memory version to avoid checking this index table
          // again in this merge round.
          // Invalid %create_mem_version will be set to frozen_version in DDL thread before executing.
          create_mem_version = -1;
        } else {
          new_status = INDEX_STATUS_AVAILABLE;
        }
      } else if (INDEX_STATUS_UNIQUE_CHECKING == new_status) {
        new_status = INDEX_STATUS_AVAILABLE;
      }

      if (index_schema->get_index_status() != new_status) {
        LOG_INFO("try to update index status", K(new_status), "index_schema", *index_schema);
        // use rpc instead of api to make it executing in DDL thread.
        obrpc::ObUpdateIndexStatusArg arg;
        arg.index_table_id_ = index_table_id;
        arg.status_ = new_status;
        arg.create_mem_version_ = create_mem_version;
        arg.exec_tenant_id_ = fetch_tenant_id;
        DEBUG_SYNC(BEFORE_SEND_UPDATE_INDEX_STATUS);
        if (OB_FAIL(ddl_service_.get_common_rpc()->to(obrpc::ObRpcProxy::myaddr_).update_index_status(arg))) {
          LOG_WARN("update index status failed",
              K(ret),
              "rpc_server",
              obrpc::ObRpcProxy::myaddr_,
              K(new_status),
              "index_schema",
              *index_schema);
        } else if (OB_FAIL(schema_guard.get_table_schema(index_table_id, index_schema))) {
          LOG_WARN("get table schema failed", K(ret), KT(index_table_id));
        } else if (NULL == index_schema) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("not find this table schema: ", K(index_table_id), K(ret));
        } else {
          LOG_INFO("update index status success", K(new_status), "index_schema", *index_schema);
        }
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
