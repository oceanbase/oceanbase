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

#define USING_LOG_PREFIX SQL_EXE

//#include "lib/hash_func/murmur_hash.h"
#include "sql/executor/ob_trans_result_collector.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
namespace sql {

ObReporter::ObReporter() : task_id_(), exec_svr_(), part_keys_(), status_(TS_INVALID), lock_()
{}

ObReporter::ObReporter(const ObTaskID& task_id)
    : task_id_(task_id), exec_svr_(), part_keys_(), status_(TS_INVALID), lock_()
{}

ObReporter::~ObReporter()
{}

bool ObReporter::operator==(const ObReporter& rv) const
{
  return task_id_ == rv.task_id_;
}

int ObReporter::assign(const ObReporter& rv)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &rv)) {
    OX(task_id_ = rv.task_id_);
    OX(exec_svr_ = rv.exec_svr_);
    OZ(part_keys_.assign(rv.part_keys_));
    OX(status_ = rv.status_);
  }
  return ret;
}

void ObReporter::set_task_id(const ObTaskID& task_id)
{
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  task_id_ = task_id;
}

void ObReporter::set_exec_svr(const ObAddr& exec_svr)
{
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  exec_svr_ = exec_svr;
}

int ObReporter::reset_part_key()
{
  int ret = OB_SUCCESS;
  // we can reset part_keys only when the status of reporter is TS_SENT.
  OV(status_ == TS_SENT);
  OX(part_keys_.reset());
  return ret;
}

int ObReporter::add_part_key(const ObPartitionKey& part_key)
{
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  return part_keys_.push_back(part_key);
}

void ObReporter::set_status(ObTaskStatus status)
{
  LOG_TRACE("TRC_set_status", K(task_id_), K(status_), K(status));
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  if (status_ < status || (status_ == TS_RETURNED && status == TS_SENT)) {
    // RETURNED is safe to reset to SENT, because new participants has been merged.
    // this can happen when task level retry.
    status_ = status;
  }
  return;
}

// send_task() should be called before rpc send_task().
int ObTransResultCollector::send_task(const ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("TRC_send_task", "task_id", task_info.get_ob_task_id(), "exec_svr", task_info.get_range_location().server_);
  OZ(set_task_info(task_info, TS_SENT), task_info);
  if (OB_SUCCESS != ret) {
    err_code_ = ret;
  }
  return ret;
}

int ObTransResultCollector::recv_result(const ObTaskID& task_id, const TransResult& trans_result)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("TRC_recv_result", K(task_id));
  OV(OB_NOT_NULL(trans_result_));
  OZ(trans_result_->merge_result(trans_result));
  if (OB_SUCCESS != ret) {
    err_code_ = ret;
  }
  OZ(set_task_status(task_id, TS_RETURNED));
  return ret;
}

int ObTransResultCollector::set_task_status(const ObTaskID& task_id, ObTaskStatus status)
{
  int ret = OB_SUCCESS;
  ObReporter* reporter = NULL;
  OZ(get_reporter(task_id, true, reporter), task_id);
  OV(OB_NOT_NULL(reporter));
  OX(reporter->set_status(status));
  OX(reporter_cond_.signal());
  return ret;
}

// private function, need not set err_code_ if failed.
int ObTransResultCollector::set_task_info(const ObTaskInfo& task_info, ObTaskStatus status)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTaskInfo::ObPartLoc>& part_locs = task_info.get_range_location().part_locs_;
  ObReporter* reporter = NULL;
  OZ(get_reporter(task_info.get_ob_task_id(), true, reporter), task_info);
  OV(OB_NOT_NULL(reporter));
  OX(reporter->set_status(status));
  OX(reporter->set_exec_svr(task_info.get_range_location().server_));
  OZ(reporter->reset_part_key());
  for (int64_t i = 0; OB_SUCC(ret) && i < part_locs.count(); i++) {
    OZ(reporter->add_part_key(part_locs.at(i).partition_key_));
  }
  OX(LOG_TRACE("TRC_part_keys", "part_keys", reporter->get_part_keys()));
  return ret;
}

int ObTransResultCollector::get_reporter(const ObTaskID& task_id, bool allow_exist, ObReporter*& reporter)
{
  int ret = OB_SUCCESS;
  reporter = NULL;
  OV(task_id.is_valid(), OB_ERR_UNEXPECTED, task_id);
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(reporter) && i < reporters_.count(); i++) {
    if (reporters_.at(i).get_task_id() == task_id) {
      OX(reporter = &reporters_.at(i));
    }
  }
  if (OB_ISNULL(reporter)) {
    static const ObReporter EMPTY_REPORTER;
    OZ(reporters_.push_back(EMPTY_REPORTER));
    OV(reporter = &reporters_.at(reporters_.count() - 1));
    OX(reporter->set_task_id(task_id));
    OX(LOG_TRACE("TRC_append_reporter", K(task_id)));
  } else {
    OV(allow_exist, OB_ERR_UNEXPECTED, *reporter);
    OX(LOG_TRACE("TRC_exist_reporter", K(*reporter)));
  }
  return ret;
}

int ObTransResultCollector::ping_reporter(ObReporter& reporter)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TRC_ping_reporter",
      "status",
      reporter.get_task_status(),
      "task_id",
      reporter.get_task_id(),
      "exec_svr",
      reporter.get_exec_svr());
  ObExecutorPingRpcCtx ping_ctx(rpc_tenant_id_, WAIT_ONCE_TIME * 2, dist_task_mgr_, mini_task_mgr_);
  ObPingSqlTask ping_task;
  OV(OB_NOT_NULL(exec_rpc_));
  OX(ping_task.trans_id_ = trans_id_);
  OX(ping_task.sql_no_ = sql_no_);
  OX(ping_task.task_id_ = reporter.get_task_id());
  OX(ping_task.exec_svr_ = reporter.get_exec_svr());
  OZ(ping_task.part_keys_.assign(reporter.get_part_keys()));
  OX(ping_task.cur_status_ = reporter.get_task_status());
  OZ(exec_rpc_->ping_sql_task(ping_ctx, ping_task));
  return ret;
}

void ObTransResultCollector::wait_reporter_event(int64_t wait_timeout)
{
  reporter_cond_.wait(wait_timeout);
}

int ObTransResultCollector::init(ObSQLSessionInfo& session, ObExecutorRpcImpl* exec_rpc,
    ObDistributedSchedulerManager* dist_task_mgr, ObAPMiniTaskMgr* mini_task_mgr)
{
  int ret = OB_SUCCESS;
  OX(reset());
  OV(OB_NOT_NULL(exec_rpc));
  OX(rpc_tenant_id_ = session.get_rpc_tenant_id());
  OX(trans_id_ = session.get_trans_desc().get_trans_id());
  OX(sql_no_ = session.get_trans_desc().get_sql_no());
  OX(exec_rpc_ = exec_rpc);
  OX(dist_task_mgr_ = dist_task_mgr);
  OX(mini_task_mgr_ = mini_task_mgr);
  OX(trans_result_ = &session.get_trans_result());
  return ret;
}

int ObTransResultCollector::wait_all_task(int64_t query_timeout, const bool is_build_index)
{
  UNUSED(is_build_index);
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtility::current_time();
  int64_t next_ping_time = cur_time;
  int64_t max_wait_time = MAX(cur_time + TTL_THRESHOLD * WAIT_ONCE_TIME, query_timeout);
  bool need_wait = true;
  bool need_ping = true;
  while (/*OB_SUCC(ret)*/ need_wait && cur_time < max_wait_time) {
    /* TODO:
     * use hash map or wait list to improve the performance of the whole loop on reporters_
     * to find tasks who still need wait.
     */
    need_wait = false;
    need_ping = (cur_time >= next_ping_time);
    for (int64_t i = 0; /*OB_SUCC(ret)*/ i < reporters_.count(); i++) {
      if (reporters_.at(i).need_wait()) {
        need_wait = true;
        if (need_ping) {
          (void)(ping_reporter(reporters_.at(i)));
        }
      }
    }
    if (need_wait) {
      if (need_ping) {
        next_ping_time += WAIT_ONCE_TIME;
      }
      wait_reporter_event(next_ping_time - cur_time);
    }
    cur_time = ObTimeUtility::current_time();
  }
  if (need_wait || OB_SUCCESS != err_code_) {
    // now we may get many errors, we must log every one, but return any one is OK.
    LOG_WARN("need set incomplete", K(need_wait), K(err_code_));
    OV(OB_NOT_NULL(trans_result_));
    OX(trans_result_->set_incomplete());
    if (need_wait) {
      ret = OB_TIMEOUT;
      for (int64_t i = 0; i < reporters_.count(); i++) {
        if (reporters_.at(i).need_wait()) {
          LOG_WARN("reporter still need wait after timeout", K(reporters_.at(i)));
        }
      }
    }
  }
  trans_result_ = NULL;
  return ret;
}

void ObTransResultCollector::reset()
{
  trans_result_ = NULL;
  err_code_ = OB_SUCCESS;
  trans_id_.reset();
  sql_no_ = 0;
  reporters_.reset();
}
}  // namespace sql
}  // namespace oceanbase
