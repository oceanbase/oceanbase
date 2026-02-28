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

#include "ob_load_inner_table_schema_executor.h"

#include "share/inner_table/ob_load_inner_table_schema.h"
#include "deps/oblib/src/lib/utility/utility.h"
#include "share/ob_server_struct.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_global_stat_proxy.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "deps/oblib/src/lib/lock/ob_lock_guard.h"

namespace oceanbase
{
namespace rootserver
{
const share::ObLoadInnerTableSchemaInfo *ObLoadInnerTableSchemaExecutor::ALL_LOAD_SCHEMA_INFO[] = {
    &share::ALL_CORE_TABLE_LOAD_INFO,
    &share::ALL_TABLE_LOAD_INFO,
    &share::ALL_COLUMN_LOAD_INFO,
    &share::ALL_DDL_OPERATION_LOAD_INFO,
    &share::ALL_TABLE_HISTORY_LOAD_INFO,
    &share::ALL_COLUMN_HISTORY_LOAD_INFO,
};
bool ObLoadInnerTableSchemaExecutor::load_schema_hang_enabled_ = false;
int64_t ObLoadInnerTableSchemaExecutor::need_hang_count_ = INT64_MAX;
ObThreadCond ObLoadInnerTableSchemaExecutor::cond_;
void ObLoadInnerTableSchemaExecutor::load_schema_wait()
{
  if (get_load_schema_hang_enabled() && cond_.is_inited()) {
    lib::ObLockGuard<ObThreadCond> guard(cond_);
    FLOG_INFO("begin to wait cond_");
    ATOMIC_DEC(&need_hang_count_);
    cond_.wait();
    FLOG_INFO("wait cond_ finished");
  }
}
void ObLoadInnerTableSchemaExecutor::load_schema_broadcast()
{
  if (get_load_schema_hang_enabled() && cond_.is_inited()) {
    lib::ObLockGuard<ObThreadCond> guard(cond_);
    FLOG_INFO("broadcast cond_");
    cond_.broadcast();
  }
}
void ObLoadInnerTableSchemaExecutor::load_schema_init()
{
  cond_.init(0); // just for test, so we assign 0
}
ERRSIM_POINT_DEF(ERRSIM_LOAD_INNER_TABLE_SCHEMA);
int ObLoadInnerTableSchemaExecutor::load_inner_table_schema(
    const obrpc::ObLoadTenantTableSchemaArg &arg)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(LOAD_INNER_TABLE_SCHEMA);
  load_schema_wait(); // just for test
  const int64_t start_ts = ObTimeUtility::current_time();
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(ERRSIM_LOAD_INNER_TABLE_SCHEMA)) {
    LOG_WARN("ERRSIM_LOAD_INNER_TABLE_SCHEMA", KR(ret));
  }
  bool find = false;
  for (int64_t i = 0; !find && OB_SUCC(ret) && i < ARRAYSIZEOF(ALL_LOAD_SCHEMA_INFO); i++) {
    const share::ObLoadInnerTableSchemaInfo *info = ALL_LOAD_SCHEMA_INFO[i];
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("info is NULL", KR(ret), KP(info), K(i));
    } else if (arg.get_table_id() == info->get_inner_table_id()) {
      find = true;
      if (OB_FAIL(load_inner_table_schema(arg, *info))) {
        LOG_WARN("failed to load inner table schema", KR(ret), K(arg), KPC(info));
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!find)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find match table_id", KR(ret), K(arg));
  }
  LOG_INFO("finish load inner table schema", KR(ret),
      "cost", ObTimeUtility::current_time() - start_ts, K(arg));
  return ret;
}

int ObLoadInnerTableSchemaExecutor::get_extra_header(const obrpc::ObLoadTenantTableSchemaArg &arg,
    ObSqlString &header)
{
  int ret = OB_SUCCESS;
  if (arg.get_table_id() == share::OB_ALL_CORE_TABLE_TID) {
    if (OB_FAIL(header.assign(""))) {
      LOG_WARN("failed to assign header", KR(ret));
    }
  } else if (OB_FAIL(header.assign(", schema_version"))) {
    LOG_WARN("failed to assign header", KR(ret));
  }
  return ret;
}

int ObLoadInnerTableSchemaExecutor::get_extra_value(
    const obrpc::ObLoadTenantTableSchemaArg &arg,
    const uint64_t table_id,
    ObSqlString &value)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  if (arg.get_table_id() == share::OB_ALL_CORE_TABLE_TID) {
    if (OB_FAIL(value.assign(""))) {
      LOG_WARN("failed to assign value", KR(ret));
    }
  } else if (OB_FAIL(share::get_hard_code_schema_version_mapping(
          table_id, schema_version))) {
    LOG_WARN("failed to get_hard_code_schema_version_mapping", KR(ret), K(table_id));
  } else if (OB_INVALID_VERSION == schema_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema version", KR(ret), K(schema_version));
  } else if (OB_FAIL(value.assign_fmt(", %ld", schema_version))) {
    LOG_WARN("failed to assign value", KR(ret));
  }
  return ret;
}

int ObLoadInnerTableSchemaExecutor::load_inner_table_schema(
    const obrpc::ObLoadTenantTableSchemaArg &arg,
    const share::ObLoadInnerTableSchemaInfo &info)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSqlString insert_header;
  ObSqlString extra_header;
  if (!arg.is_valid() || arg.get_table_id() != info.get_inner_table_id() || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(arg), K(info), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(get_extra_header(arg, extra_header))) {
    LOG_WARN("failed to get extra header", KR(ret), K(arg));
  } else if (OB_FAIL(insert_header.append_fmt("INSERT INTO %s(%s%s) VALUES ", info.get_inner_table_name(),
              info.get_inner_table_column_names(), extra_header.ptr()))) {
    LOG_WARN("failed to append insert header", KR(ret), K(info));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, arg.get_tenant_id()))) {
    LOG_WARN("failed to start trans", KR(ret), K(arg));
  } else {
    ObSqlString sql;
    const ObIArray<int64_t> &insert_idx = arg.get_insert_idx();
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_idx.count(); i += LOAD_ROWS_PER_INSERT) {
      int64_t affected_rows = 0;
      int64_t current_row_count = 0;
      uint64_t table_id = 0;
      const char *row = nullptr;
      sql.reuse();
      if (OB_FAIL(sql.append(insert_header.string()))) {
        LOG_WARN("failed to append header", KR(ret), K(insert_header));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < LOAD_ROWS_PER_INSERT && i + j < insert_idx.count(); j++) {
        int64_t idx = insert_idx.at(i + j);
        ObSqlString extra_value;
        if (idx >= info.get_row_count() || idx < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index is out of range", KR(ret), K(i), K(j), K(idx), K(info));
        } else if (OB_FAIL(info.get_row(idx, row, table_id))) {
          LOG_WARN("failed to get row", KR(ret), K(idx));
        } else if (OB_FAIL(get_extra_value(arg, table_id, extra_value))) {
          LOG_WARN("failed to get extra value", KR(ret), K(arg), K(idx));
        } else if (OB_FAIL(sql.append_fmt("%s(%s%s)", j != 0 ? ", " : "",
                row, extra_value.ptr()))) {
          LOG_WARN("failed to append value", KR(ret), K(j), K(idx), K(row));
        } else {
          current_row_count++;
        }
      }
      if (FAILEDx(trans.write(arg.get_tenant_id(), sql.ptr(), affected_rows))) {
        LOG_WARN("failed to write sql", KR(ret), K(sql), K(arg));
      } else if (current_row_count != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert rows not match", KR(ret), K(current_row_count), K(affected_rows), K(sql));
      }
    }
  }
  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
      ret = (OB_SUCC(ret)) ? temp_ret : ret;
    }
  }
  return ret;
}

int ObLoadInnerTableSchemaExecutor::append_arg(const ObIArray<int64_t> &insert_idx,
    const share::ObLoadInnerTableSchemaInfo &info)
{
  int ret = OB_SUCCESS;
  obrpc::ObLoadTenantTableSchemaArg arg;
  if (insert_idx.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx is empty", KR(ret), K(tenant_id_), K(info), K(insert_idx));
  } else if (!is_valid_tenant_id(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_id is invalid", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(arg.init(tenant_id_, info.get_inner_table_id(), insert_idx, DATA_CURRENT_VERSION))) {
    LOG_WARN("failed to init arg", KR(ret), K(tenant_id_), K(info), K(insert_idx));
  } else if (OB_FAIL(args_.push_back(arg))) {
    LOG_WARN("failed to push_back", KR(ret), K(arg));
  }
  return ret;
}

int ObLoadInnerTableSchemaExecutor::init_args_(const ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t rpc_count = 0;
  hash::ObHashSet<uint64_t> all_table_ids;
  ObArray<int64_t> insert_idx;
  if (OB_FAIL(all_table_ids.create(hash::cal_next_prime(table_schemas.count())))) {
    LOG_WARN("failed to create hashset", KR(ret), "count", table_schemas.count());
  } else if (OB_FAIL(insert_idx.reserve(LOAD_ROWS_PER_BATCH))) {
    LOG_WARN("failed to reserve insert_idx", KR(ret));
  }
  FOREACH_CNT_X(table, table_schemas, OB_SUCC(ret)) {
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pointer is null", KP(table));
    } else if (OB_FAIL(all_table_ids.set_refactored(table->get_table_id()))) {
      LOG_WARN("failed to add table_id", KR(ret), K(table->get_table_id()));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(ALL_LOAD_SCHEMA_INFO); i++) {
    const share::ObLoadInnerTableSchemaInfo *info = ALL_LOAD_SCHEMA_INFO[i];
    insert_idx.reuse();
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pointer is null", KR(ret), KP(info), K(i));
    } else {
      const char *row = nullptr;
      uint64_t table_id = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < info->get_row_count(); j++) {
        if (OB_FAIL(info->get_row(j, row, table_id))) {
          LOG_WARN("failed to get row", KR(ret), K(i));
        } else if (FALSE_IT(tmp_ret = all_table_ids.exist_refactored(table_id))) {
        } else if (OB_HASH_EXIST == tmp_ret || share::OB_ALL_CORE_TABLE_TID == table_id) {
          if (OB_FAIL(insert_idx.push_back(j))) {
            LOG_WARN("failed to push_back row_id", KR(ret), K(j));
          } else if (insert_idx.count() == LOAD_ROWS_PER_BATCH) {
            if (OB_FAIL(append_arg(insert_idx, *info))) {
              LOG_WARN("failed to push args to queue", KR(ret), K(insert_idx), KPC(info));
            }
            insert_idx.reuse();
          }
        } else if (OB_HASH_NOT_EXIST == tmp_ret) {
        } else {
          ret = tmp_ret;
          LOG_WARN("failed to check table_id exist", KR(ret), K(table_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (insert_idx.count() > 0 && OB_FAIL(append_arg(insert_idx, *info))) {
        LOG_WARN("failed to push args to queue", KR(ret), K(insert_idx), KPC(info));
      }
    }
  }
  return ret;
}

int ObLoadInnerTableSchemaExecutor::init(const ObIArray<ObTableSchema> &table_schemas,
    const uint64_t tenant_id, const int64_t max_cpu, obrpc::ObSrvRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  if (max_cpu <= 0 || OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parallel count should be positive", KR(ret), K(max_cpu), KP(rpc_proxy));
  } else {
    parallel_count_ = common::max(THREAD_PER_CPU * max_cpu / 2, 1);
    load_rpc_timeout_ = parallel_count_ * GCONF.internal_sql_execute_timeout;
    rpc_proxy_ = rpc_proxy;
  }
  if (FAILEDx(init_args_(table_schemas))) {
    LOG_WARN("failed to init args", KR(ret));
  } else {
    inited_ = true;
  }
  FLOG_INFO("ObLoadInnerTableSchemaExecutor inited", KR(ret), K(tenant_id_),
      K(parallel_count_), K(load_rpc_timeout_));
  return ret;
}

int ObLoadInnerTableSchemaExecutor::call_next_arg_(ObLoadTenantTableSchemaProxy& proxy)
{
  int ret = OB_SUCCESS;
  ObAddr server;
  if (next_arg_index_ >= args_.count()) {
    ret = OB_ITER_END;
  } else if (!is_valid_tenant_id(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id, tenant_id_,
          share::SYS_LS, false/*force_renew*/, server))) {
    LOG_WARN("failed to get tenant sys ls leader", KR(ret), K_(tenant_id));
  } else {
    const obrpc::ObLoadTenantTableSchemaArg &arg = args_[next_arg_index_];
    const int64_t timeout = common::min(THIS_WORKER.get_timeout_remain(), load_rpc_timeout_);
    if (OB_FAIL(proxy.call(server, timeout, GCONF.cluster_id, tenant_id_, arg))) {
      LOG_WARN("failed to call async rpc", KR(ret), K(arg), K(timeout), K(tenant_id_));
    } else {
      next_arg_index_++;
      LOG_INFO("call one rpc in loading table schema", K(timeout),
          "index", next_arg_index_ - 1, K(arg));
    }
  }
  return ret;
}

int ObLoadInnerTableSchemaExecutor::load_schema_version(
    const uint64_t tenant_id,
    common::ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  share::ObGlobalStatProxy proxy(client, tenant_id);
  if (OB_FAIL(proxy.set_core_schema_version(share::INNER_TABLE_CORE_SCHEMA_VERSION))) {
    LOG_WARN("failed to set core_schema_version", KR(ret));
  } else if (OB_FAIL(proxy.set_sys_schema_version(share::INNER_TABLE_SYS_SCHEMA_VERSION))) {
    LOG_WARN("failed to set sys_schema_version", KR(ret));
  }
  return ret;
}

int ObLoadInnerTableSchemaExecutor::execute()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  FLOG_INFO("start to load inner table schema", KR(ret), K_(tenant_id));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(inited));
  } else {
    oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT);
    ObLoadTenantTableSchemaProxy proxy(*rpc_proxy_, &obrpc::ObSrvRpcProxy::load_tenant_table_schema);
    int64_t called_rpc_count = 0;
    bool rpc_has_error = false;
    set_need_hang_count(args_.count()); // just for test
    while (OB_SUCC(ret) && !rpc_has_error) {
      const int64_t finished_rpc_count = proxy.get_response_count();
      if (THIS_WORKER.get_timeout_remain() <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("this worker is timeout", KR(ret), K(THIS_WORKER.get_timeout_remain()));
      } else if (proxy.check_has_error_result()) { // check_has_error_result is not thread safe
        rpc_has_error = true;
      } else if (finished_rpc_count + parallel_count_ > called_rpc_count ||
          OB_UNLIKELY(get_load_schema_hang_enabled())) {
        if (OB_FAIL(call_next_arg_(proxy))) {
          LOG_WARN("failed to call next arg", KR(ret));
        } else {
          called_rpc_count++;
        }
      } else {
        if (REACH_TIME_INTERVAL(10_s)) {
          LOG_INFO("loading tenant schema", KR(ret), K_(tenant_id), K_(next_arg_index),
              K_(load_rpc_timeout), K_(parallel_count));
        }
        ob_usleep(WAIT_THREAD_FREE_TIME);
      }
    }
    if (rpc_has_error) {
      FLOG_INFO("rpc has error when trying to call rpc, check logs below", KR(ret), K(rpc_has_error));
    } else if (OB_ITER_END == ret && called_rpc_count == args_.count()) {
      ret = OB_SUCCESS;
      FLOG_INFO("all rpc are called, begin to wait rpc return", KR(ret), K(called_rpc_count));
    } else {
      FLOG_INFO("failed to call all rpc", KR(ret), K(called_rpc_count), K(args_.count()));
    }
    if (OB_TMP_FAIL(proxy.wait())) {
      LOG_WARN("failed to wait proxy or rpc return error", KR(tmp_ret), KR(ret), K(rpc_has_error));
      ret = OB_FAIL(ret) ? ret : tmp_ret;
    }
    if (rpc_has_error && OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check rpc has error when calling rpc, while proxy not return error", KR(ret), K(rpc_has_error));
    }
  }
  FLOG_INFO("finish load all inner table schema", KR(ret), K_(tenant_id),
      "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

}
}
