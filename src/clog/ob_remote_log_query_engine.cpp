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

#include "common/ob_clock_generator.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_thread_mgr.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "observer/ob_sql_client_decorator.h"
#include "storage/transaction/ob_trans_define.h"
#include "ob_i_log_engine.h"
#include "ob_remote_log_query_engine.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace transaction;
namespace clog {
uint64_t ObPartitionLogInfo::hash() const
{
  uint64_t hash_val = 0;
  hash_val = partition_key_.hash();
  hash_val = murmurhash(&log_id_, sizeof(log_id_), hash_val);
  return hash_val;
}

bool ObPartitionLogInfo::operator==(const ObPartitionLogInfo& other) const
{
  return partition_key_ == other.partition_key_ && log_id_ == other.log_id_;
}

ObRemoteLogQueryEngine::ClearCacheTask::ClearCacheTask() : is_inited_(false), host_(NULL)
{}

int ObRemoteLogQueryEngine::ClearCacheTask::init(ObRemoteLogQueryEngine* host)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ClearCacheTask init twice", K(ret));
  } else if (OB_ISNULL(host)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(host));
  } else {
    host_ = host;
    is_inited_ = true;
  }
  return ret;
}

void ObRemoteLogQueryEngine::ClearCacheTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ClearCacheTask is not inited", K(ret));
  } else if (OB_FAIL(host_->run_clear_cache_task())) {
    CLOG_LOG(WARN, "run_clear_cache_task failed", K(ret));
  } else {
    // do nothing
  }
}

void ObRemoteLogQueryEngine::ClearCacheTask::destroy()
{
  is_inited_ = false;
  host_ = NULL;
}

class ObRemoteLogQueryEngine::RemoveIfFunctor {
public:
  RemoveIfFunctor()
  {}
  ~RemoveIfFunctor()
  {}

public:
  bool operator()(const ObPartitionLogInfo& pl_info, ObTransIDInfo& trans_id_info)
  {
    UNUSED(pl_info);
    return ObClockGenerator::getClock() - trans_id_info.get_ts() > CLEAR_CACHE_INTERVAL;
  }
};

ObRemoteLogQueryEngine::ObRemoteLogQueryEngine()
    : is_inited_(false), is_running_(false), sql_proxy_(NULL), log_engine_(NULL), cache_(), clear_cache_task_(), self_()
{}

ObRemoteLogQueryEngine::~ObRemoteLogQueryEngine()
{
  destroy();
}

int ObRemoteLogQueryEngine::init(common::ObMySQLProxy* sql_proxy, ObILogEngine* log_engine, const common::ObAddr& self)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObRemoteLogQueryEngine init twice");
  } else if (OB_ISNULL(sql_proxy) || OB_ISNULL(log_engine) || !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(sql_proxy), KP(log_engine), K(self));
  } else if (OB_FAIL(cache_.init(ObModIds::OB_CLOG_MGR))) {
    CLOG_LOG(WARN, "cache_ init failed", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::RLogClrCache))) {
    CLOG_LOG(WARN, "timer_ init failed", K(ret));
  } else if (OB_FAIL(clear_cache_task_.init(this))) {
    CLOG_LOG(WARN, "clear_cache_task_ init failed", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(lib::TGDefIDs::RLogQuery, *this))) {
    CLOG_LOG(WARN, "ObSimpleThreadPool init failed", K(ret));
  } else {
    is_running_ = false;
    sql_proxy_ = sql_proxy;
    log_engine_ = log_engine;
    self_ = self;

    is_inited_ = true;
  }

  if (!is_inited_) {
    destroy();
  }
  CLOG_LOG(INFO, "ObRemoteLogQueryEngine init finished", K(ret));
  return ret;
}

int ObRemoteLogQueryEngine::start()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::RLogClrCache, clear_cache_task_, CLEAR_CACHE_INTERVAL, repeat))) {
    CLOG_LOG(WARN, "clear_cache_task_ schedule failed", K(ret));
  } else {
    is_running_ = true;
  }
  return ret;
}

void ObRemoteLogQueryEngine::stop()
{
  is_running_ = false;
  TG_STOP(lib::TGDefIDs::RLogClrCache);
}

void ObRemoteLogQueryEngine::wait()
{
  TG_WAIT(lib::TGDefIDs::RLogClrCache);
}

void ObRemoteLogQueryEngine::destroy()
{
  is_inited_ = false;
  is_running_ = false;
  sql_proxy_ = NULL;
  log_engine_ = NULL;
  cache_.destroy();
  TG_DESTROY(lib::TGDefIDs::RLogClrCache);
  clear_cache_task_.destroy();
  self_.reset();
  TG_DESTROY(lib::TGDefIDs::RLogQuery);
}

int ObRemoteLogQueryEngine::get_log(const common::ObPartitionKey& partition_key, const uint64_t log_id,
    const common::ObMemberList& curr_member_list, transaction::ObTransID& trans_id, int64_t& submit_timestamp)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObPartitionLogInfo pl_info(partition_key, log_id);
  ObTransIDInfo trans_id_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObRemoteLogQueryEngine is not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObRemoteLogQueryEngine is not running", K(ret));
  } else if (!partition_key.is_valid() || OB_INVALID_ID == log_id || !curr_member_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key), K(log_id), K(curr_member_list));
  } else if (OB_FAIL(cache_.get(pl_info, trans_id_info))) {
    ObRemoteLogQETask* task = NULL;
    if (OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(ERROR, "cache_ get failed", K(ret));
    } else if (NULL == (task = op_reclaim_alloc(ObRemoteLogQETask))) {
      CLOG_LOG(ERROR, "op_reclaim_alloc failed");
    } else {
      task->set(partition_key, log_id, curr_member_list);
      if (OB_SUCCESS != (tmp_ret = TG_PUSH_TASK(lib::TGDefIDs::RLogQuery, task))) {
        CLOG_LOG(WARN, "push task failed", K(tmp_ret));
      }
    }
    // rewrite ret value
    ret = OB_EAGAIN;
  } else {
    trans_id = trans_id_info.get_trans_id();
    submit_timestamp = trans_id_info.get_submit_timestamp();
  }

  return ret;
}

void ObRemoteLogQueryEngine::handle(void* task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    CLOG_LOG(WARN, "ObRemoteLogQueryEngine is not inited");
  } else if (!is_running_) {
    CLOG_LOG(WARN, "ObRemoteLogQueryEngine is not running");
  } else if (OB_ISNULL(task)) {
    CLOG_LOG(WARN, "invalid argument", KP(task));
  } else {
    ObRemoteLogQETask* qe_task = static_cast<ObRemoteLogQETask*>(task);
    if (OB_ISNULL(qe_task)) {
      CLOG_LOG(ERROR, "invalid argument", KP(qe_task));
    } else {
      const common::ObPartitionKey& partition_key = qe_task->get_partition_key();
      const uint64_t log_id = qe_task->get_log_id();
      const common::ObMemberList& curr_member_list = qe_task->get_member_list();

      ObAddrArray addr_array1;
      ObAddrArray addr_array2;
      ObTransIDInfo tmp_trans_id_info;
      ObPartitionLogInfo pl_info(partition_key, log_id);
      bool succeed = false;

      if (OB_SUCCESS == (ret = cache_.get(pl_info, tmp_trans_id_info))) {
        CLOG_LOG(INFO, "info_task exists in the cache, skip it", K(ret), K(pl_info));
      } else if (OB_FAIL(transfer_addr_array_(curr_member_list, addr_array1))) {
        CLOG_LOG(ERROR, "transfer_addr_array_ failed", K(ret), K(curr_member_list));
      } else if (OB_FAIL(execute_query_(partition_key, log_id, addr_array1, succeed))) {
        CLOG_LOG(WARN, "execute_query_ failed", K(ret), K(partition_key), K(log_id), K(addr_array1));
      } else if (succeed) {
        CLOG_LOG(INFO, "query succeed, skip it", K(ret), K(partition_key), K(log_id));
      } else if (OB_FAIL(get_addr_array_(partition_key, log_id, addr_array2))) {
        CLOG_LOG(WARN, "get_addr_array_ failed", K(ret), K(partition_key), K(log_id));
      } else if (OB_FAIL(execute_query_(partition_key, log_id, addr_array2, succeed))) {
        CLOG_LOG(WARN, "execute_query_ failed", K(ret), K(partition_key), K(log_id), K(addr_array2));
      }

      op_reclaim_free(qe_task);
      qe_task = NULL;
    }
  }
}

int ObRemoteLogQueryEngine::run_clear_cache_task()
{
  int ret = OB_SUCCESS;
  RemoveIfFunctor fn;
  if (OB_FAIL(cache_.remove_if(fn))) {
    CLOG_LOG(WARN, "remove_if failed", K(ret));
  }
  return ret;
}

int ObRemoteLogQueryEngine::get_addr_array_(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, ObAddrArray& addr_array)
{
  int ret = OB_SUCCESS;

  const bool did_use_weak = false;
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, did_use_weak);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;

    const uint64_t tenant_id = partition_key.get_tenant_id();
    const uint64_t table_id = partition_key.get_table_id();
    const int64_t partition_idx = partition_key.get_partition_id();
    const int32_t partition_cnt = partition_key.get_partition_cnt();

    if (OB_FAIL(sql.append_fmt("SELECT svr_ip, svr_port FROM %s WHERE table_id = %lu "
                               "AND partition_idx = %ld AND partition_cnt = %d "
                               "AND start_log_id <= %lu AND (%lu <= end_log_id OR %lu = end_log_id)",
            OB_ALL_CLOG_HISTORY_INFO_V2_TNAME,
            table_id,
            partition_idx,
            partition_cnt,
            log_id,
            log_id,
            OB_INVALID_ID))) {
      CLOG_LOG(WARN, "append sql failed", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
      CLOG_LOG(WARN, "execute sql failed", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "fail to get reuslt", K(ret));
    } else {
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        int64_t tmp_real_str_len = 0;
        char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
        int port;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip, OB_MAX_SERVER_ADDR_SIZE, tmp_real_str_len);
        EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int);
        (void)tmp_real_str_len;
        ObAddr addr;
        if (!addr.set_ip_addr(ip, port)) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "fail to set ip addr", K(ret), K(ip), K(port));
        } else if (OB_FAIL(addr_array.push_back(addr))) {
          CLOG_LOG(WARN, "addr_array push_back failed", K(ret), K(ip), K(port));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObRemoteLogQueryEngine::execute_query_(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, const ObAddrArray& addr_array, bool& succeed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  succeed = false;
  ObTransID trans_id;
  int64_t submit_timestamp = OB_INVALID_TIMESTAMP;

  for (int64_t index = 0; (!succeed) && index < addr_array.count(); index++) {
    const ObAddr& addr = addr_array[index];
    if (addr == self_) {
      // continue;
    } else if (OB_SUCCESS ==
               (tmp_ret = log_engine_->query_remote_log(addr, partition_key, log_id, trans_id, submit_timestamp))) {
      succeed = true;
    } else {
      // continue;
    }
  }

  if (succeed) {
    ObPartitionLogInfo pl_info(partition_key, log_id);
    ObTransIDInfo trans_id_info(trans_id, submit_timestamp, ObClockGenerator::getClock());

    if (OB_FAIL(cache_.insert(pl_info, trans_id_info))) {
      if (OB_ENTRY_EXIST != ret) {
        CLOG_LOG(WARN, "cache_ insert failed", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObRemoteLogQueryEngine::transfer_addr_array_(const common::ObMemberList& member_list, ObAddrArray& addr_array)
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; OB_SUCC(ret) && index < member_list.get_member_number(); index++) {
    common::ObAddr addr;
    if (OB_FAIL(member_list.get_server_by_index(index, addr))) {
      CLOG_LOG(WARN, "member_list get_server_by_index failed", K(ret), K(index));
    } else if (OB_FAIL(addr_array.push_back(addr))) {
      CLOG_LOG(WARN, "addr_array push_back failed", K(ret), K(addr));
    }
  }
  return ret;
}
}  // namespace clog
}  // namespace oceanbase
