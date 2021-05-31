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

#include "ob_zone_recovery_task_mgr.h"
#include "ob_zone_server_recovery_machine.h"
#include "lib/lock/ob_mutex.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "rootserver/ob_server_recovery_machine.h"
#include "rootserver/ob_zone_master_storage_util.h"
#include "ob_server_manager.h"
#include "ob_unit_manager.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase {
using namespace common;
using namespace lib;
using namespace obrpc;
namespace rootserver {

bool ObZoneRecoveryTaskKey::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && file_id_ > 0 && server_.is_valid();
}

bool ObZoneRecoveryTaskKey::operator==(const ObZoneRecoveryTaskKey& that) const
{
  return tenant_id_ == that.tenant_id_ && file_id_ == that.file_id_ && server_ == that.server_;
}

ObZoneRecoveryTaskKey& ObZoneRecoveryTaskKey::operator=(const ObZoneRecoveryTaskKey& that)
{
  tenant_id_ = that.tenant_id_;
  file_id_ = that.file_id_;
  server_ = that.server_;
  hash_value_ = that.hash_value_;
  return (*this);
}

uint64_t ObZoneRecoveryTaskKey::hash() const
{
  return hash_value_;
}

int ObZoneRecoveryTaskKey::init(const uint64_t tenant_id, const int64_t file_id, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || file_id <= 0 || !server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(file_id), K(server));
  } else {
    tenant_id_ = tenant_id;
    file_id_ = file_id;
    server_ = server;
    hash_value_ = inner_hash();
  }
  return ret;
}

uint64_t ObZoneRecoveryTaskKey::inner_hash() const
{
  uint64_t hash_val = server_.hash();
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&file_id_, sizeof(file_id_), hash_val);
  return hash_val;
}

int ObZoneRecoveryTask::set_task_id(const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  task_id_.init(addr);
  return ret;
}

int ObZoneRecoveryTask::build(const common::ObAddr& src_server, const int64_t src_svr_seq,
    const common::ObAddr& dest_server, const int64_t dest_svr_seq, const uint64_t tenant_id, const int64_t file_id,
    const uint64_t dest_unit_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src_server.is_valid() || src_svr_seq <= 0 || !dest_server.is_valid() || dest_svr_seq <= 0 ||
                  OB_INVALID_ID == tenant_id || file_id <= 0 || OB_INVALID_ID == dest_unit_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        K(src_server),
        K(src_svr_seq),
        K(dest_server),
        K(dest_svr_seq),
        K(tenant_id),
        K(file_id),
        K(dest_unit_id));
  } else if (OB_FAIL(task_key_.init(tenant_id, file_id, dest_server))) {
    LOG_WARN("fail to init task key", K(ret));
  } else if (OB_FAIL(set_task_id(GCTX.self_addr()))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    src_server_ = src_server;
    src_svr_seq_ = src_svr_seq, dest_server_ = dest_server;
    dest_svr_seq_ = dest_svr_seq;
    tenant_id_ = tenant_id;
    file_id_ = file_id;
    dest_unit_id_ = dest_unit_id;
  }
  return ret;
}

int ObZoneRecoveryTask::build_by_task_result(const obrpc::ObFastRecoveryTaskReplyBatchArg& arg, int& ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (arg.arg_reply_array_.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    const obrpc::ObFastRecoveryTaskReplyArg& this_arg = arg.arg_reply_array_.at(0);
    ret_code = this_arg.ret_code_;
    if (OB_FAIL(task_key_.init(this_arg.tenant_id_, this_arg.file_id_, this_arg.dst_))) {
      LOG_WARN("fail to init task key", K(ret), K(this_arg));
    } else {
      src_server_ = this_arg.src_;
      src_svr_seq_ = this_arg.src_svr_seq_;
      dest_server_ = this_arg.dst_;
      dest_svr_seq_ = this_arg.dst_svr_seq_;
      tenant_id_ = this_arg.tenant_id_;
      file_id_ = this_arg.file_id_;
    }
  }
  return ret;
}

int ObZoneRecoveryTask::set_server_and_tenant_stat(
    ObServerTaskStatMap& server_stat_map, ObTenantTaskStatMap& tenant_stat_map)
{
  int ret = OB_SUCCESS;
  ObTenantTaskStatMap::Item* tenant_stat = nullptr;
  ObServerTaskStatMap::Item* src_server_stat = nullptr;
  ObServerTaskStatMap::Item* dest_server_stat = nullptr;
  // tenant stat map
  if (OB_FAIL(tenant_stat_map.locate(tenant_id_, tenant_stat))) {
    LOG_WARN("fail to get tenant stat item", K(ret));
  } else if (OB_UNLIKELY(nullptr == tenant_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null tenant stat ptr", K(ret), KP(tenant_stat));
  } else {
    tenant_stat_ = tenant_stat;
    tenant_stat_->ref();
    tenant_stat_->v_.low_priority_task_cnt_++;
  }
  // server stat map
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(server_stat_map.locate(src_server_, src_server_stat))) {
    LOG_WARN("fail to locate src server stat", K(ret));
  } else if (OB_FAIL(server_stat_map.locate(dest_server_, dest_server_stat))) {
    LOG_WARN("fail to locate dest server stat", K(ret));
  } else if (OB_UNLIKELY(nullptr == src_server_stat || nullptr == dest_server_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server stat ptr is null", K(ret));
  } else {
    // dest server stat
    dest_server_stat_ = dest_server_stat;
    dest_server_stat_->ref();
    dest_server_stat_->v_.total_in_cnt_++;
    // src server stat
    src_server_stat_ = src_server_stat;
    src_server_stat_->ref();
    src_server_stat_->v_.total_out_cnt_++;
  }
  return ret;
}

void ObZoneRecoveryTask::clean_server_and_tenant_ref(TaskMap& task_map)
{
  UNUSED(task_map);
  if (NULL != src_server_stat_) {
    src_server_stat_->v_.total_out_cnt_--;
    if (in_schedule()) {
      src_server_stat_->v_.in_schedule_out_cnt_--;
    }
    src_server_stat_->unref();
    src_server_stat_ = NULL;
  }
  if (NULL != dest_server_stat_) {
    dest_server_stat_->v_.total_in_cnt_--;
    if (in_schedule()) {
      dest_server_stat_->v_.in_schedule_in_cnt_--;
    }
    dest_server_stat_->unref();
    dest_server_stat_ = NULL;
  }
  if (NULL != tenant_stat_) {
    tenant_stat_->v_.low_priority_task_cnt_--;
    tenant_stat_->unref();
    tenant_stat_ = NULL;
  }
}

void ObZoneRecoveryTask::clean_task_map(TaskMap& task_map)
{
  int tmp_ret = OB_SUCCESS;
  ObZoneRecoveryTask* in_map_task = nullptr;
  if (OB_SUCCESS != (tmp_ret = task_map.get_refactored(task_key_, in_map_task))) {
    // do not exist in task info map, ignore
  } else if (OB_SUCCESS != (tmp_ret = task_map.erase_refactored(task_key_))) {
    LOG_WARN("fail to remove task info from map", K(tmp_ret));
  }
}

void ObZoneRecoveryTask::set_schedule()
{
  if (NULL != src_server_stat_) {
    src_server_stat_->v_.in_schedule_out_cnt_++;
  }
  if (NULL != dest_server_stat_) {
    dest_server_stat_->v_.in_schedule_in_cnt_++;
  }
  schedule_time_ = ObTimeUtility::current_time();
  LOG_INFO("set task to schedule", "task", *this);
}

int ObZoneRecoveryTask::clone(void* input_ptr, ObZoneRecoveryTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObZoneRecoveryTask* my_task = new (input_ptr) ObZoneRecoveryTask();
    my_task->task_id_ = task_id_;
    my_task->task_key_ = task_key_;
    my_task->src_server_stat_ = src_server_stat_;
    my_task->dest_server_stat_ = dest_server_stat_;
    my_task->tenant_stat_ = tenant_stat_;
    // generate_time_ is generated in construct function
    my_task->schedule_time_ = schedule_time_;
    my_task->execute_time_ = execute_time_;
    my_task->src_server_ = src_server_;
    my_task->src_svr_seq_ = src_svr_seq_;
    my_task->dest_server_ = dest_server_;
    my_task->dest_svr_seq_ = dest_svr_seq_;
    my_task->tenant_id_ = tenant_id_;
    my_task->file_id_ = file_id_;
    my_task->dest_unit_id_ = dest_unit_id_;
    // copy ptr to output
    output_task = my_task;
  }
  return ret;
}

int ObZoneRecoveryTask::build_fast_recovery_rpc_batch_arg(obrpc::ObFastRecoveryTaskBatchArg& batch_arg) const
{
  int ret = OB_SUCCESS;
  ObFastRecoveryTaskArg arg;
  arg.task_id_ = task_id_;
  arg.src_ = src_server_;
  arg.src_svr_seq_ = src_svr_seq_;
  arg.dst_ = dest_server_;
  arg.dest_svr_seq_ = dest_svr_seq_;
  arg.tenant_id_ = tenant_id_;
  arg.file_id_ = file_id_;
  arg.dest_unit_id_ = dest_unit_id_;
  if (OB_FAIL(batch_arg.arg_array_.push_back(arg))) {
    LOG_WARN("fail to push back", K(ret));
  }
  return ret;
}

int ObZoneRecoveryTask::execute(obrpc::ObSrvRpcProxy& rpc_proxy) const
{
  int ret = OB_SUCCESS;
  UNUSED(rpc_proxy);
  ObFastRecoveryTaskBatchArg batch_arg;
  if (OB_FAIL(build_fast_recovery_rpc_batch_arg(batch_arg))) {
    LOG_WARN("fail to build fast recovery rpc batch arg", K(ret));
  } else {
    ret = rpc_proxy.to(dest_server_).recover_pg_file(batch_arg);
    if (OB_EAGAIN == ret) {  // special ret code which means this task is executing.
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to execute recovery pg file rpc", K(ret), K(batch_arg));
    } else {
      LOG_INFO("start to recover pg file", K(batch_arg));
    }
  }
  return ret;
}

ObZoneRecoveryTaskQueue::ObZoneRecoveryTaskQueue()
    : is_inited_(false),
      config_(NULL),
      tenant_stat_map_(NULL),
      server_stat_map_(NULL),
      task_alloc_(),
      wait_list_(),
      schedule_list_(),
      task_map_()
{}

ObZoneRecoveryTaskQueue::~ObZoneRecoveryTaskQueue()
{
  if (is_inited_) {
    reuse();
  }
}

int ObZoneRecoveryTaskQueue::init(ObServerConfig& config, ObTenantTaskStatMap& tenant_stat,
    ObServerTaskStatMap& server_stat, const int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (bucket_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(bucket_num));
  } else if (OB_FAIL(task_map_.create(
                 bucket_num, "ZoneRecovTaskQ" /* bucket label */, "ZoneRecovTaskQ" /* node label */))) {
    LOG_WARN("init task map failed", K(ret), K(bucket_num));
  } else if (OB_FAIL(task_alloc_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE))) {
    LOG_WARN("fail to init task allocator", K(ret));
  } else {
    task_alloc_.set_label("ZoneRecovTask");
    config_ = &config;
    tenant_stat_map_ = &tenant_stat;
    server_stat_map_ = &server_stat;
    is_inited_ = true;
  }
  return ret;
}

void ObZoneRecoveryTaskQueue::reuse()
{
  while (!wait_list_.is_empty()) {
    ObZoneRecoveryTask* t = wait_list_.remove_first();
    if (NULL != t) {
      t->clean_server_and_tenant_ref(task_map_);
      t->clean_task_map(task_map_);
      t->~ObZoneRecoveryTask();
      task_alloc_.free(t);
      t = NULL;
    }
  }
  while (!schedule_list_.is_empty()) {
    ObZoneRecoveryTask* t = schedule_list_.remove_first();
    if (NULL != t) {
      t->clean_server_and_tenant_ref(task_map_);
      t->clean_task_map(task_map_);
      t->~ObZoneRecoveryTask();
      task_alloc_.free(t);
      t = NULL;
    }
  }
  task_map_.clear();
}

int ObZoneRecoveryTaskQueue::push_task(ObZoneRecoveryTaskMgr& task_mgr, const ObZoneRecoveryTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == server_stat_map_ || NULL == tenant_stat_map_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server stat map or tenant stat map null", K(ret), KP(server_stat_map_), KP(tenant_stat_map_));
  } else {
    const ObZoneRecoveryTaskKey& task_key = task.get_task_key();
    ObZoneRecoveryTask* task_ptr = NULL;
    if (OB_FAIL(task_map_.get_refactored(task_key, task_ptr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("check task info in task info map failed", K(ret), K(task_key));
      }
    } else if (OB_UNLIKELY(NULL == task_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL task info", K(ret), KP(task_ptr));
    } else {
      ret = OB_ENTRY_EXIST;
      LOG_INFO("task info exist on the same partition", K(task_key), "task", *task_ptr);
    }
    if (OB_FAIL(ret)) {
      // may be ENTRY_EXIST or other error code
    } else if (OB_FAIL(do_push_task(task_mgr, task))) {
      LOG_WARN("fail to push task", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObZoneRecoveryTaskQueue::do_push_task(ObZoneRecoveryTaskMgr& task_mgr, const ObZoneRecoveryTask& task)
{
  int ret = OB_SUCCESS;
  void* raw_ptr = nullptr;
  ObZoneRecoveryTask* new_task = nullptr;
  const int64_t task_deep_copy_size = task.get_deep_copy_size();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == server_stat_map_ || NULL == tenant_stat_map_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), KP(server_stat_map_), KP(tenant_stat_map_));
  } else if (nullptr == (raw_ptr = task_alloc_.alloc(task_deep_copy_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate task", K(ret));
  } else if (OB_FAIL(task.clone(raw_ptr, new_task))) {
    LOG_WARN("fail to clone new task", K(ret));
  } else if (OB_UNLIKELY(nullptr == new_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new task ptr is null", K(ret));
  } else if (OB_FAIL(new_task->set_server_and_tenant_stat(*server_stat_map_, *tenant_stat_map_))) {
    LOG_WARN("fail to set server and tenant stat", K(ret));
  } else if (!wait_list_.add_last(new_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to add task to wait list, should never happen", K(ret));
  } else {
    const int64_t in_cnt_lmt = config_->fast_recovery_concurrency;
    if (OB_FAIL(task_map_.set_refactored(new_task->get_task_key(), new_task))) {
      LOG_WARN("fail to set map", K(ret), "task_key", new_task->get_task_key());
    } else if (task_mgr.get_reach_concurrency_limit()) {
      if ((NULL != new_task->get_dest_server_stat() &&
              new_task->get_dest_server_stat()->v_.in_schedule_in_cnt_ < in_cnt_lmt)) {
        task_mgr.clear_reach_concurrency_limit();
      } else {
      }  // no more to do
    } else {
    }  // no more to check
    // if the above routine fails, remove the task from wait_list_
    if (OB_FAIL(ret)) {
      wait_list_.remove(new_task);
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != new_task) {
      new_task->clean_server_and_tenant_ref(task_map_);
      new_task->clean_task_map(task_map_);
      new_task->~ObZoneRecoveryTask();
      task_alloc_.free(new_task);
      new_task = nullptr;
      raw_ptr = nullptr;
    } else if (nullptr != raw_ptr) {
      task_alloc_.free(raw_ptr);
      raw_ptr = nullptr;
    }
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::set_server_data_in_limit(const ObAddr& addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRebalanceTaskMgr not init", K(ret));
  } else if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else {
    ObServerTaskStatMap::Item* server_stat = NULL;
    if (OB_FAIL(server_stat_.locate(addr, server_stat))) {
      LOG_WARN("fail to locate server stat", K(ret), K(addr));
    } else if (OB_UNLIKELY(NULL == server_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server stat is null", K(ret), KP(server_stat));
    } else {
      server_stat->ref();
      server_stat->v_.data_in_limit_ts_ = ObTimeUtility::current_time();
      LOG_INFO("server copy data in limit", K(addr));
    }
  }
  return ret;
}

void ObZoneRecoveryTaskMgr::try_clear_server_data_in_limit(const ObAddr& addr)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    tmp_ret = OB_NOT_INIT;
    LOG_WARN("ObRebalanceTaskMgr not init", K(tmp_ret));
  } else if (OB_UNLIKELY(!addr.is_valid())) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(tmp_ret), K(addr));
  } else {
    ObServerTaskStatMap::Item* server_stat = NULL;
    if (OB_SUCCESS != (tmp_ret = server_stat_.locate(addr, server_stat))) {
      LOG_WARN("fail to get server stat", K(tmp_ret));
    } else if (OB_UNLIKELY(NULL == server_stat)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server stat is NULL", K(tmp_ret), KP(server_stat));
    } else if (0 == server_stat->v_.data_in_limit_ts_) {
      // no migration restriction, ignore
    } else {
      server_stat->v_.data_in_limit_ts_ = 0;  // reset migration restriction
      server_stat->unref();
      LOG_INFO("clear server data copy in", K(addr));
    }
  }
}

int ObZoneRecoveryTaskQueue::pop_task(ObZoneRecoveryTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else {
    const int64_t in_cnt_lmt = config_->fast_recovery_concurrency;
    const int64_t now = ObTimeUtility::current_time();
    const int64_t interval = ObZoneRecoveryTaskMgr::DATA_IN_CLEAR_INTERVAL;
    DLIST_FOREACH(t, wait_list_)
    {
      if (NULL != t->get_dest_server_stat() && (t->get_dest_server_stat()->v_.in_schedule_in_cnt_ >= in_cnt_lmt ||
                                                   t->get_dest_server_stat()->v_.data_in_limit_ts_ + interval >= now)) {
        // reach in limit
      } else {
        task = t;
        break;
      }
    }

    if (NULL != task) {
      wait_list_.remove(task);
      if (!schedule_list_.add_last(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add task to schedule list failed, this should never happen", K(ret));
      } else {
        task->set_schedule();
      }

      if (OB_FAIL(ret)) {
        // clean the resource if the above routine fails
        task->clean_server_and_tenant_ref(task_map_);
        task->clean_task_map(task_map_);
        task->~ObZoneRecoveryTask();
        task_alloc_.free(task);
        task = NULL;
      }
    }
  }
  return ret;
}

int ObZoneRecoveryTaskQueue::get_discard_in_schedule_task(
    const ObAddr& server, const int64_t start_service_time, ObZoneRecoveryTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid() || start_service_time < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server or start service time", K(ret), K(server), K(start_service_time));
  } else {
    DLIST_FOREACH(t, schedule_list_)
    {
      if (t->get_dest() == server && t->get_generate_time() < start_service_time) {
        task = &(*t);
        break;
      }
    }
  }
  return ret;
}

int ObZoneRecoveryTaskQueue::discard_waiting_task(const common::ObAddr& server, const int64_t start_service_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid() || start_service_time < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server or start service time", K(ret), K(server), K(start_service_time));
  } else {
    DLIST_FOREACH_REMOVESAFE(t, wait_list_)
    {
      if (t->get_dest() == server && t->get_generate_time() < start_service_time) {
        LOG_INFO("discard waiting task", K(server), K(start_service_time), "task", *t);
        wait_list_.remove(t);
        t->clean_server_and_tenant_ref(task_map_);
        t->clean_task_map(task_map_);
        t->~ObZoneRecoveryTask();
        task_alloc_.free(t);
        t = NULL;
      }
    }
  }
  return ret;
}

int ObZoneRecoveryTaskQueue::get_schedule_task(const ObZoneRecoveryTask& input_task, ObZoneRecoveryTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else {
    const ObZoneRecoveryTaskKey& task_key = input_task.get_task_key();
    ObZoneRecoveryTask* sample_task = nullptr;
    if (OB_FAIL(task_map_.get_refactored(task_key, sample_task))) {
      if (OB_HASH_NOT_EXIST == ret) {
        task = NULL;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get task info from map failed", K(ret));
      }
    } else if (OB_UNLIKELY(NULL == sample_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null task info from map", K(ret));
    } else if (!sample_task->in_schedule()) {
      task = NULL;
    } else if (sample_task->get_dest() != input_task.get_dest()) {
      task = NULL;
    } else if (sample_task->get_task_key() != input_task.get_task_key()) {
      task = NULL;
    } else {
      task = sample_task;
    }
  }
  return ret;
}

int ObZoneRecoveryTaskQueue::finish_schedule(ObZoneRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task pointer", K(ret), K(task));
  } else if (!task->in_schedule()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(ret), "task", *task);
  } else {
    // remove from schedule list
    schedule_list_.remove(task);
    // clean the reference both in server and tenant stat maps
    task->clean_server_and_tenant_ref(task_map_);
    task->clean_task_map(task_map_);
    task->~ObZoneRecoveryTask();
    task_alloc_.free(task);
    task = NULL;
  }
  return ret;
}

int ObZoneRecoveryTaskQueue::check_fast_recovery_task_status(
    const common::ObZone& zone, const ObZoneRecoveryTask& task, ZoneFileRecoveryStatus& status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(do_check_datafile_recovery_status(zone,
                 task.get_src(),
                 task.get_src_svr_seq(),
                 task.get_dest(),
                 task.get_dest_svr_seq(),
                 task.get_tenant_id(),
                 task.get_file_id(),
                 status))) {
    LOG_WARN("fail to check datafile recovery status", K(ret), K(task));
  }
  return ret;
}

int ObZoneRecoveryTaskQueue::do_check_datafile_recovery_status(const common::ObZone& zone, const common::ObAddr& server,
    const int64_t svr_seq, const common::ObAddr& dest_server, const int64_t dest_svr_seq, const uint64_t tenant_id,
    const int64_t file_id, ZoneFileRecoveryStatus& status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObZoneMasterStorageUtil::get_datafile_recovery_persistence_status(
          zone, server, svr_seq, dest_server, dest_svr_seq, tenant_id, file_id, status))) {
    LOG_WARN("fail to get datafile recovery persistence status", K(ret));
  }
  return ret;
}

int ObZoneRecoveryTaskQueue::get_not_in_progress_task(obrpc::ObSrvRpcProxy* rpc_proxy, ObServerManager* server_mgr,
    ObZoneRecoveryTask*& task, ZoneFileRecoveryStatus& status)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr || nullptr == rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    DLIST_FOREACH_X(t, schedule_list_, OB_SUCC(ret))
    {
      Bool res = false;
      if (OB_UNLIKELY(NULL == t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task null", K(ret), KP(t));
      } else {
        bool exist = true;
        bool beyond_permanent_offline = true;
        const common::ObAddr& dest = t->get_dest();
        share::ObServerStatus server_status;
        ZoneFileRecoveryStatus my_status = ZoneFileRecoveryStatus::FRS_IN_PROGRESS;
        common::ObZone server_zone;
        if (OB_FAIL(server_mgr->is_server_exist(dest, exist))) {
          LOG_WARN("fail to check server exist", K(ret), "server", dest);
        } else if (!exist) {
          LOG_INFO("dest server not exist", K(dest));
          task = t;
          status = ZoneFileRecoveryStatus::FRS_FAILED;
          break;
        } else if (OB_FAIL(server_mgr->get_server_status(dest, server_status))) {
          LOG_WARN("fail to get server status", K(ret), "server", dest);
        } else if (server_status.is_permanent_offline()) {
          LOG_INFO("dest server permanent offline", K(dest));
          task = t;
          status = ZoneFileRecoveryStatus::FRS_FAILED;
          break;
        } else if (server_status.is_temporary_offline()) {
          LOG_INFO("dest server temporary offline", K(dest));
          task = t;
          status = ZoneFileRecoveryStatus::FRS_FAILED;
          break;
        } else if (OB_FAIL(server_mgr->get_server_zone(dest, server_zone))) {
          LOG_WARN("fail to get server zone", K(ret), K(dest));
        } else {
          ret = check_fast_recovery_task_status(server_zone, *t, my_status);
          if (OB_SUCC(ret)) {
            if (ZoneFileRecoveryStatus::FRS_IN_PROGRESS == my_status || ZoneFileRecoveryStatus::FRS_MAX == my_status) {
              // the file status is not a confirm to reveal if the task is executing.
              // we need to send a recover_pg_file rpc to try to restart this task
              ObFastRecoveryTaskBatchArg batch_arg;
              if (OB_FAIL(t->build_fast_recovery_rpc_batch_arg(batch_arg))) {
                LOG_WARN("fail to build fast recovery rpc batch arg", K(ret));
              } else {
                (void)rpc_proxy->to(dest).recover_pg_file(batch_arg);
              }
            } else {
              task = t;
              status = my_status;
              break;
            }
          } else if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
            ret = OB_SUCCESS;
            task = t;
            status = ZoneFileRecoveryStatus::FRS_FAILED;
            break;
          } else {
            ret = OB_SUCCESS;  // rewrite to succ
            LOG_WARN("fail to check fast recovery task status, but ignore", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// ======================= ObZoneRecoveryTaskMgr ================

ObZoneRecoveryTaskMgr::ObZoneRecoveryTaskMgr()
    : inited_(false),
      reach_concurrency_limited_(false),
      config_(NULL),
      cond_(),
      tenant_stat_(),
      server_stat_(),
      task_queue_(),
      self_(),
      server_mgr_(nullptr),
      unit_mgr_(nullptr),
      rpc_proxy_(nullptr),
      server_recovery_machine_(nullptr)
{}

int ObZoneRecoveryTaskMgr::init(ObServerConfig& config, ObServerManager* server_mgr, ObUnitManager* unit_mgr,
    obrpc::ObSrvRpcProxy* rpc_proxy, ObZoneServerRecoveryMachine* server_recovery_machine)
{
  int ret = OB_SUCCESS;
  const static int64_t zone_recovery_task_mgr_thread_cnt = 1;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr || nullptr == unit_mgr || nullptr == rpc_proxy ||
                         nullptr == server_recovery_machine)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::REBALANCE_TASK_MGR_COND_WAIT))) {
    LOG_WARN("fail to init cond, ", K(ret));
  } else if (OB_FAIL(create(zone_recovery_task_mgr_thread_cnt, "ZoneRecvTaskMgr"))) {
    LOG_WARN("create rebalance task mgr thread failed", K(ret), K(zone_recovery_task_mgr_thread_cnt));
  } else {
    config_ = &config;
    server_mgr_ = server_mgr;
    unit_mgr_ = unit_mgr;
    rpc_proxy_ = rpc_proxy;
    server_recovery_machine_ = server_recovery_machine;

    if (OB_FAIL(tenant_stat_.init(TASK_QUEUE_LIMIT))) {
      LOG_WARN("init tenant stat failed", K(ret), LITERAL_K(TASK_QUEUE_LIMIT));
    } else if (OB_FAIL(server_stat_.init(TASK_QUEUE_LIMIT))) {
      LOG_WARN("init server stat failed", K(ret), LITERAL_K(TASK_QUEUE_LIMIT));
    } else if (OB_FAIL(task_queue_.init(config, tenant_stat_, server_stat_, TASK_QUEUE_LIMIT))) {
      LOG_WARN("init migrate task queue failed", K(ret), LITERAL_K(TASK_QUEUE_LIMIT));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

void ObZoneRecoveryTaskMgr::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObReentrantThread::stop();
    ObThreadCondGuard guard(cond_);
    cond_.broadcast();
  }
}

void ObZoneRecoveryTaskMgr::run3()
{
  int ret = OB_SUCCESS;
  LOG_INFO("zone recovery task mgr start");
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    int64_t last_dump_time = ObTimeUtility::current_time();
    int64_t last_check_task_in_progress_ts = ObTimeUtility::current_time();
    while (!stop_) {
      void* raw_ptr = nullptr;
      ObZoneRecoveryTask* executed_task = nullptr;
      ObZoneRecoveryTask* task = NULL;
      common::ObArenaAllocator allocator;
      int ret = OB_SUCCESS;
      {
        ObThreadCondGuard guard(cond_);
        const int64_t now = ObTimeUtility::current_time();
        if (now > last_dump_time + config_->balancer_log_interval) {
          last_dump_time = now;
          int tmp_ret = dump_statistics();
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("task manager dump statistics failed", K(tmp_ret));
          }
        };
        {
          int64_t wait_cnt = 0;
          int64_t in_schedule_cnt = 0;
          if (OB_FAIL(get_task_cnt(wait_cnt, in_schedule_cnt))) {
            LOG_WARN("get task count failed", K(ret));
          } else if (wait_cnt > 0 && in_schedule_cnt < config_->data_copy_concurrency && !reach_concurrency_limited_) {
            if (OB_FAIL(pop_task(task))) {
              LOG_WARN("pop task for execute failed", K(ret));
            } else {
            }  // no more to do
          } else {
            const int64_t wait_time_ms = 1000;  // 1 second
            cond_.wait(wait_time_ms);
          }
        }
        if (OB_SUCCESS == ret && nullptr != task) {
          const int64_t task_deep_copy_size = task->get_deep_copy_size();
          if (nullptr == (raw_ptr = allocator.alloc(task_deep_copy_size))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate task", K(ret));
          } else if (OB_FAIL(task->clone(raw_ptr, executed_task))) {
            LOG_WARN("fail to clone input task", K(ret));
          } else if (nullptr == executed_task) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("input task ptr is null", K(ret));
          } else {
            task->set_executor_time(ObTimeUtility::current_time());
          }
        }
      }
      if (OB_SUCCESS == ret && nullptr != executed_task) {
        if (OB_FAIL(execute_task(*executed_task))) {
          LOG_WARN("send task to execute failed", K(ret));
        }
        if (nullptr != executed_task) {
          executed_task->~ObZoneRecoveryTask();
          allocator.free(executed_task);
          executed_task = nullptr;
          raw_ptr = nullptr;
        }
      };
      if (nullptr != raw_ptr) {
        allocator.free(raw_ptr);
        raw_ptr = nullptr;
      }
      {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = check_task_in_progress(last_check_task_in_progress_ts))) {
          LOG_WARN("fail to check task in progress", K(tmp_ret));
        }
      }
    }
  }
  LOG_INFO("zone recovery task mgr stop");
}

void ObZoneRecoveryTaskMgr::reuse()
{
  if (inited_) {
    ObThreadCondGuard guard(const_cast<ObThreadCond&>(cond_));
    task_queue_.reuse();
    reach_concurrency_limited_ = false;
    server_stat_.reuse();
    tenant_stat_.reuse();
  }
}

int ObZoneRecoveryTaskMgr::set_self(const ObAddr& self)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!self.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid self address", K(self), K(ret));
  } else {
    self_ = self;
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::pop_task(ObZoneRecoveryTask*& task)
{
  int ret = OB_SUCCESS;
  int64_t wait_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (task_queue_.wait_task_cnt() > 0) {
      wait_cnt += task_queue_.wait_task_cnt();
      if (OB_FAIL(task_queue_.pop_task(task))) {
        LOG_WARN("pop_task failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && nullptr == task && wait_cnt > 0) {
      reach_concurrency_limited_ = true;
    }
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::check_task_in_progress(int64_t& last_check_task_in_progress_ts)
{
  int ret = OB_SUCCESS;
  int64_t wait = 0;
  int64_t schedule = 0;
  {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(get_task_cnt(wait, schedule))) {
      LOG_WARN("fail to get task cnt", K(ret));
    } else if (schedule < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schedule cnt is less than 0", K(ret), K(schedule));
    }
  };
  const int64_t now = ObTimeUtility::current_time();
  if (OB_SUCC(ret) && now > last_check_task_in_progress_ts + schedule * CHECK_IN_PROGRESS_INTERVAL_PER_TASK) {
    last_check_task_in_progress_ts = now;
    if (OB_FAIL(do_check_task_in_progress())) {
      LOG_WARN("fail to do check task in progress", K(ret));
    }
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::do_check_task_in_progress()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObZoneRecoveryTask* task = nullptr;
    do {
      task = nullptr;
      void* raw_ptr = nullptr;
      ObZoneRecoveryTask* input_task = nullptr;
      common::ObArenaAllocator allocator;
      ZoneFileRecoveryStatus recovery_status = ZoneFileRecoveryStatus::FRS_MAX;
      {
        int64_t task_deep_copy_size = -1;
        ObThreadCondGuard guard(cond_);
        if (OB_FAIL(task_queue_.get_not_in_progress_task(rpc_proxy_, server_mgr_, task, recovery_status))) {
          LOG_WARN("fail to get not in progress task", K(ret));
        } else if (nullptr == task) {
          // bypass
        } else if (FALSE_IT(task_deep_copy_size = task->get_deep_copy_size())) {
          // shall never be here
        } else if (nullptr == (raw_ptr = allocator.alloc(task_deep_copy_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate task", K(ret));
        } else if (OB_FAIL(task->clone(raw_ptr, input_task))) {
          LOG_WARN("fail to clone input task", K(ret));
        } else if (ZoneFileRecoveryStatus::FRS_SUCCEED == recovery_status) {
          const int rc = OB_SUCCESS;
          LOG_INFO("task succeed on observer", K(rc), "task_id", task->task_id_);
          if (OB_FAIL(do_execute_over(*task, rc))) {
            LOG_WARN("do execute over failed", K(ret));
          }
        } else if (ZoneFileRecoveryStatus::FRS_FAILED == recovery_status) {
          const int rc = OB_REBALANCE_TASK_NOT_IN_PROGRESS;
          LOG_INFO("task not in progress on observer", K(rc), "task_id", task->task_id_);
          if (OB_FAIL(do_execute_over(*task, rc))) {
            LOG_WARN("do execute over failed", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datafile recovery status unexpected", K(ret), K(recovery_status));
        }
      };
      if (nullptr != input_task) {
        int tmp_ret = OB_SUCCESS;
        const int ret_code =
            (ZoneFileRecoveryStatus::FRS_SUCCEED == recovery_status ? common::OB_SUCCESS
                                                                    : common::OB_REBALANCE_TASK_NOT_IN_PROGRESS);
        if (OB_SUCCESS != (tmp_ret = notify_server_recovery_machine(*input_task, ret_code))) {
          LOG_WARN("fail to notify server recovery machine", K(ret));
        }
      }
      if (nullptr != input_task) {
        input_task->~ObZoneRecoveryTask();
        allocator.free(input_task);
        input_task = nullptr;
        raw_ptr = nullptr;
      } else if (nullptr != raw_ptr) {
        allocator.free(raw_ptr);
        raw_ptr = nullptr;
      }
    } while (OB_SUCC(ret) && nullptr != task);
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::add_task(const ObZoneRecoveryTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebalance task manager not inited", K(ret));
  } else {
    ObThreadCondGuard guard(cond_);
    ObZoneRecoveryTaskQueue& queue = task_queue_;
    int64_t wait_cnt = 0;
    int64_t in_schedule_cnt = 0;
    if (queue.task_cnt() >= TASK_QUEUE_LIMIT) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("task queue full", K(ret), "task_count", queue.task_cnt());
    } else if (OB_FAIL(queue.push_task(*this, task))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_WARN("push back task failed", K(ret));
      }
    } else if (OB_FAIL(get_task_cnt(wait_cnt, in_schedule_cnt))) {
      LOG_WARN("fail to get task cnt", K(ret));
    } else if (!reach_concurrency_limited_ && in_schedule_cnt < config_->data_copy_concurrency) {
      cond_.broadcast();
    }
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::add_task(const ObZoneRecoveryTask& task, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(add_task(task))) {
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_OP_NOT_ALLOW == ret) {
      ret = OB_SUCCESS;  // ignore this error to make the following routine goes
    } else {
      LOG_WARN("fail to add task to task manager", K(ret), K(task));
    }
  } else {
    LOG_INFO("add rebalance task", K(task));
    ++task_cnt;
  }
  return ret;
}

int64_t ObZoneRecoveryTaskMgr::task_cnt()
{
  ObThreadCondGuard guard(const_cast<ObThreadCond&>(cond_));
  return task_queue_.task_cnt();
}

int ObZoneRecoveryTaskMgr::execute_over(const ObZoneRecoveryTask& task, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebalance task manager not inited", K(ret));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(do_execute_over(task, ret_code))) {
      LOG_WARN("do execute over failed", K(ret), K(task), K(ret_code));
    }
  }
  // try to nofity server recovery machine whatever the above routine succeeds or not
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = notify_server_recovery_machine(task, ret_code))) {
    LOG_WARN("fail to notify server recovery machine", K(ret));
  }
  LOG_INFO("execute over", K(ret), "task_cnt", task_cnt());
  return ret;
}

int ObZoneRecoveryTaskMgr::notify_server_recovery_machine(const ObZoneRecoveryTask& task, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_recovery_machine_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server recovery machine ptr is null", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = server_recovery_machine_->on_recover_pg_file_reply(task, ret_code))) {
      LOG_WARN("fail to on recover pg file reply", K(tmp_ret));
    }
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::do_execute_over(
    const ObZoneRecoveryTask& input_task, const int ret_code, const bool need_try_clear_server_data_in_limit)
{
  int ret = OB_SUCCESS;
  ObZoneRecoveryTask* task = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(task_queue_.get_schedule_task(input_task, task))) {
      LOG_WARN("get schedule task failed", K(ret));
    } else if (NULL == task) {
      LOG_INFO("in schedule task not found, "
               "maybe timeouted or scheduled by previous rootserver",
          K(input_task),
          K(ret_code));
    } else {
      LOG_INFO("finish schedule task", "task", (*task));
      if (OB_FAIL(task_queue_.finish_schedule(task))) {
        LOG_WARN("finish schedule failed", K(ret), K(task));
      }
    }
  }

  // reset schedule limited && trigger schedule next task
  reach_concurrency_limited_ = false;
  if (need_try_clear_server_data_in_limit && input_task.get_dest().is_valid()) {
    try_clear_server_data_in_limit(input_task.get_dest());
  }
  cond_.broadcast();
  return ret;
}

int ObZoneRecoveryTaskMgr::get_task_cnt(int64_t& wait_cnt, int64_t& in_schedule_cnt)
{
  int ret = OB_SUCCESS;
  wait_cnt = 0;
  in_schedule_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    wait_cnt = task_queue_.wait_task_cnt();
    in_schedule_cnt = task_queue_.in_schedule_task_cnt();
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::discard_task(const ObAddr& server, const int64_t start_service_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid() || start_service_time <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server or start service time", K(ret), K(server), K(start_service_time));
  } else {
    int64_t safe_start_time = start_service_time - SAFE_DISCARD_TASK_INTERVAL;
    const int ret_code = OB_CANCELED;
    ObZoneRecoveryTask* task = nullptr;
    do {
      task = nullptr;
      void* raw_ptr = nullptr;
      ObZoneRecoveryTask* input_task = nullptr;
      common::ObArenaAllocator allocator;
      {
        int64_t task_deep_copy_size = -1;
        ObThreadCondGuard guard(cond_);
        if (OB_FAIL(task_queue_.get_discard_in_schedule_task(server, safe_start_time, task))) {
          LOG_WARN("fail to get not in progress task", K(ret));
        } else if (nullptr == task) {
          // bypass
        } else if (FALSE_IT(task_deep_copy_size = task->get_deep_copy_size())) {
          // shall never be here
        } else if (nullptr == (raw_ptr = allocator.alloc(task_deep_copy_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate task", K(ret));
        } else if (OB_FAIL(task->clone(raw_ptr, input_task))) {
          LOG_WARN("fail to clone input task", K(ret));
        } else if (OB_FAIL(do_execute_over(*task, ret_code))) {
          LOG_WARN("do execute over failed", K(ret));
        }
      };
      if (nullptr != input_task) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = notify_server_recovery_machine(*input_task, ret_code))) {
          LOG_WARN("fail to notify server recovery machine", K(ret));
        }
      }
      if (nullptr != input_task) {
        input_task->~ObZoneRecoveryTask();
        allocator.free(input_task);
        input_task = nullptr;
        raw_ptr = nullptr;
      } else if (nullptr != raw_ptr) {
        allocator.free(raw_ptr);
        raw_ptr = nullptr;
      }
    } while (OB_SUCC(ret) && nullptr != task);

    if (OB_SUCC(ret)) {
      ObThreadCondGuard guard(cond_);
      if (OB_FAIL(task_queue_.discard_waiting_task(server, safe_start_time))) {
        LOG_WARN("discard waiting task failed", K(ret), K(server), K(safe_start_time));
      }
    }
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::dump_statistics() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("zone recovery task manager statistics",
        "waiting_task_cnt",
        task_queue_.wait_task_cnt(),
        "executing_task_cnt",
        task_queue_.in_schedule_task_cnt());

    ObServerTaskStatMap::HashTable::const_iterator sit = server_stat_.get_hash_table().begin();
    for (; sit != server_stat_.get_hash_table().end(); ++sit) {
      LOG_INFO("server task", "stat", sit->v_);
    }

    ObTenantTaskStatMap::HashTable::const_iterator tit = tenant_stat_.get_hash_table().begin();
    for (; tit != tenant_stat_.get_hash_table().end(); ++tit) {
      LOG_INFO("tenant task", "stat", tit->v_);
    }
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::execute_task(const ObZoneRecoveryTask& task)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(self_);
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  LOG_INFO("execute rebalance task", K(task));
  bool is_active = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_mgr_ ptr is null", K(ret));
  } else if (OB_FAIL(server_mgr_->check_server_active(task.get_dest(), is_active))) {
    LOG_WARN("fail to check server active", K(ret), "server", task.get_dest());
  } else if (!is_active) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("dst server not alive", K(ret), K(task));
  } else if (OB_FAIL(task.execute(*rpc_proxy_))) {
    LOG_WARN("fail to execute rebalance task", K(ret));
  } else {
  }

  ObCurTraceId::init(self_);
  if (OB_FAIL(ret)) {
    int ret_code = ret;
    ObThreadCondGuard guard(cond_);
    int tmp_ret = OB_SUCCESS;
    if (OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT == ret) {
      // server is up to the data copy limit
      (void)set_server_data_in_limit(task.get_dest());
    }
    const bool need_clear_data_in_lmt = (OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT != ret);
    if (OB_SUCCESS != (tmp_ret = do_execute_over(task, ret_code, need_clear_data_in_lmt))) {
      LOG_WARN("do execute over failed", K(tmp_ret), K(ret), K(task));
    } else {
    }  // no more to do
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    int ret_code = ret;
    if (OB_SUCCESS != (tmp_ret = notify_server_recovery_machine(task, ret_code))) {
      LOG_WARN("fail to notify server recovery machine", K(ret));
    }
  }
  return ret;
}

int ObZoneRecoveryTaskMgr::get_all_tasks(common::ObIAllocator& allocator, common::ObIArray<ObZoneRecoveryTask*>& tasks)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObThreadCond& cond = get_cond();
    ObThreadCondGuard guard(cond);
    const ObZoneRecoveryTaskQueue::TaskList* all_task_list[2] = {
        &task_queue_.get_wait_list(), &task_queue_.get_schedule_list()};
    for (int i = 0; OB_SUCC(ret) && i < 2; ++i) {
      DLIST_FOREACH(curr, *all_task_list[i])
      {
        void* raw_ptr = nullptr;
        ObZoneRecoveryTask* new_task = nullptr;
        const int64_t task_deep_copy_size = curr->get_deep_copy_size();
        if (nullptr == (raw_ptr = allocator.alloc(task_deep_copy_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate task", K(ret));
        } else if (OB_FAIL(curr->clone(raw_ptr, new_task))) {
          LOG_WARN("fail to clone new task", K(ret));
        } else if (OB_UNLIKELY(nullptr == new_task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new task ptr is null", K(ret));
        } else if (FALSE_IT(new_task->set_generate_time(curr->get_generate_time()))) {
          // false it, shall never be here
        } else if (OB_FAIL(tasks.push_back(new_task))) {
          LOG_WARN("fail to push back task", K(ret));
        }
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
