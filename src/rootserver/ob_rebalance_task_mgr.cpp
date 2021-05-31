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

#include "ob_rebalance_task_mgr.h"

#include "lib/lock/ob_mutex.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/ob_debug_sync.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "ob_rebalance_task_executor.h"
#include "rootserver/ob_root_balancer.h"
#include "rootserver/restore/ob_restore_table_operator.h"
#include "rootserver/restore/ob_restore_info.h"
#include "ob_rs_event_history_table_operator.h"
#include "ob_server_manager.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "ob_global_index_builder.h"
#include "share/ob_server_status.h"
#include "sql/executor/ob_executor_rpc_proxy.h"

namespace oceanbase {
using namespace common;
using namespace lib;
using namespace obrpc;
namespace rootserver {
ObRebalanceTaskQueue::ObRebalanceTaskQueue()
    : is_inited_(false),
      config_(NULL),
      tenant_stat_map_(NULL),
      server_stat_map_(NULL),
      task_alloc_(),
      wait_list_(),
      schedule_list_(),
      task_info_map_(),
      rpc_proxy_(NULL)
{}

ObRebalanceTaskQueue::~ObRebalanceTaskQueue()
{
  if (is_inited_) {
    reuse();
  }
}

int ObRebalanceTaskQueue::init(ObServerConfig& config, ObTenantTaskStatMap& tenant_stat,
    ObServerTaskStatMap& server_stat, const int64_t bucket_num, obrpc::ObSrvRpcProxy* rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (bucket_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(bucket_num));
  } else if (OB_FAIL(task_info_map_.create(bucket_num, ObModIds::OB_REBALANCE_TASK_MGR))) {
    LOG_WARN("init task map failed", K(ret), K(bucket_num));
  } else if (OB_FAIL(task_alloc_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE))) {
    LOG_WARN("fail to init task allocator", K(ret));
  } else {
    task_alloc_.set_label(ObModIds::OB_REBALANCE_TASK_MGR);
    config_ = &config;
    tenant_stat_map_ = &tenant_stat;
    server_stat_map_ = &server_stat;
    rpc_proxy_ = rpc_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObRebalanceTaskQueue::reuse()
{
  while (!wait_list_.is_empty()) {
    ObRebalanceTask* t = wait_list_.remove_first();
    if (NULL != t) {
      t->clean_server_and_tenant_ref(task_info_map_);
      t->clean_task_info_map(task_info_map_);
      t->notify_cancel();
      t->~ObRebalanceTask();
      task_alloc_.free(t);
      t = NULL;
    }
  }
  while (!schedule_list_.is_empty()) {
    ObRebalanceTask* t = schedule_list_.remove_first();
    if (NULL != t) {
      t->clean_server_and_tenant_ref(task_info_map_);
      t->clean_task_info_map(task_info_map_);
      t->notify_cancel();
      t->~ObRebalanceTask();
      task_alloc_.free(t);
      t = NULL;
    }
  }
  task_info_map_.clear();
}

int ObRebalanceTaskQueue::push_task(ObRebalanceTaskMgr& task_mgr, const ObRebalanceTaskQueue& sibling_queue,
    const ObRebalanceTask& task, bool& has_task_info_in_schedule)
{
  int ret = OB_SUCCESS;
  has_task_info_in_schedule = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == server_stat_map_ || NULL == tenant_stat_map_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server stat map or tenant stat map null", K(ret), KP(server_stat_map_), KP(tenant_stat_map_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task.get_sub_task_count(); ++i) {
      const ObRebalanceTaskInfo* task_info = task.get_sub_task(i);
      if (OB_UNLIKELY(nullptr == task_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info ptr is null", K(ret), K(i));
      } else {
        const ObRebalanceTaskKey& task_key = task_info->get_task_key();
        ObRebalanceTaskInfo* task_info_ptr = NULL;
        if (OB_FAIL(task_info_map_.get_refactored(task_key, task_info_ptr))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("check task info in task info map failed", K(ret), K(task_key));
          }
        } else if (OB_UNLIKELY(NULL == task_info_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL task info", K(ret), KP(task_info_ptr));
        } else {
          ret = OB_ENTRY_EXIST;
          LOG_INFO("task info exist on the same partition", K(task_key), "task_info", *task_info_ptr);
        }
      }
    }
    if (OB_FAIL(ret)) {
      // may be ENTRY_EXIST or other error code
    } else if (OB_FAIL(do_push_task(task_mgr, sibling_queue, task, has_task_info_in_schedule))) {
      LOG_WARN("fail to push task", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObRebalanceTaskQueue::do_push_task(ObRebalanceTaskMgr& task_mgr, const ObRebalanceTaskQueue& sibling_queue,
    const ObRebalanceTask& task, bool& has_task_info_in_schedule)
{
  int ret = OB_SUCCESS;
  void* raw_ptr = nullptr;
  ObRebalanceTask* new_task = nullptr;
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
    has_task_info_in_schedule = false;
    const int64_t out_cnt_lmt = config_->server_data_copy_out_concurrency;
    const int64_t in_cnt_lmt = config_->server_data_copy_in_concurrency;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_task->get_sub_task_count(); ++i) {
      ObRebalanceTaskInfo* task_info = new_task->get_sub_task(i);
      bool in_schedule = false;
      if (OB_UNLIKELY(nullptr == task_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info ptr is null", K(ret));
      } else if (OB_FAIL(sibling_queue.has_in_schedule_task_info(task_info->get_task_key(), in_schedule))) {
        LOG_WARN("fail to check partition has in schedule task info", K(ret), "task_key", task_info->get_task_key());
      } else if (OB_FAIL(task_info->set_host(new_task))) {
        LOG_WARN("fail to set host", K(ret));
      } else if (OB_FAIL(task_info_map_.set_refactored(task_info->get_task_key(), task_info))) {
        LOG_WARN("fail to set map", K(ret), "task_key", task_info->get_task_key());
      } else if (task_mgr.get_reach_concurrency_limit() && !in_schedule) {
        if ((NULL != task_info->get_src_server_stat() &&
                task_info->get_src_server_stat()->v_.in_schedule_out_cnt_ < out_cnt_lmt) ||
            ((NULL != task_info->get_dest_server_stat() && BACKUP_REPLICA != task_info->get_rebalance_task_type() &&
                 VALIDATE_BACKUP != task_info->get_rebalance_task_type() &&
                 task_info->get_dest_server_stat()->v_.in_schedule_in_cnt_ < in_cnt_lmt) ||
                (NULL != task_info->get_dest_server_stat() &&
                    (BACKUP_REPLICA == task_info->get_rebalance_task_type() ||
                        VALIDATE_BACKUP == task_info->get_rebalance_task_type()) &&
                    task_info->get_dest_server_stat()->v_.in_schedule_backup_cnt_ <
                        share::OB_GROUP_BACKUP_CONCURRENCY))) {
          task_mgr.clear_reach_concurrency_limit();
        } else {
        }  // no more to do
      } else {
      }  // no more to check

      if (OB_FAIL(ret)) {
      } else {
        has_task_info_in_schedule = has_task_info_in_schedule || in_schedule;
      }
      if (OB_FAIL(ret)) {
      } else if (!in_schedule) {
      } else if (OB_FAIL(set_sibling_in_schedule(task_info->get_task_key(), in_schedule))) {
        LOG_WARN("fail to set sibling in schedule", K(ret), "task_key", task_info->get_task_key(), K(in_schedule));
      } else {
      }  // no more to do
    }
    if (OB_FAIL(ret)) {  // if the above routine fails, we need to remove the task from wait_list_
      wait_list_.remove(new_task);
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != new_task) {
      new_task->clean_server_and_tenant_ref(task_info_map_);
      new_task->clean_task_info_map(task_info_map_);
      new_task->~ObRebalanceTask();
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

int ObRebalanceTaskQueue::pop_task(ObRebalanceTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else {
    const int64_t out_cnt_lmt = config_->server_data_copy_out_concurrency;
    const int64_t in_cnt_lmt = config_->server_data_copy_in_concurrency;
    DLIST_FOREACH(t, wait_list_)
    {
      bool task_info_can_schedule = true;
      for (int64_t i = 0; OB_SUCC(ret) && task_info_can_schedule && i < t->get_sub_task_count(); ++i) {
        const ObRebalanceTaskInfo* task_info = t->get_sub_task(i);
        // data_in_limit of the server stat is not be cleaned here
        if (OB_UNLIKELY(nullptr == task_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task info ptr is null", K(ret));
        } else if (BACKUP_REPLICA == task_info->get_rebalance_task_type() ||
                   VALIDATE_BACKUP == task_info->get_rebalance_task_type()) {
          if (OB_FAIL(check_backup_task_can_pop(task_info, task_info_can_schedule))) {
            LOG_WARN("failed to check backup task can pop", K(ret), K(*task_info));
          }
        } else {
          if (OB_FAIL(check_balance_task_can_pop(task_info, out_cnt_lmt, in_cnt_lmt, task_info_can_schedule))) {
            LOG_WARN("failed to check balance task can pop", K(ret), K(*task_info));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (task_info_can_schedule) {
          task = t;
          break;
        } else {
        }  // do nothing
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
        // recycle the task resource when failed
        task->clean_server_and_tenant_ref(task_info_map_);
        task->clean_task_info_map(task_info_map_);
        task->notify_cancel();
        task->~ObRebalanceTask();
        task_alloc_.free(task);
        task = NULL;
      }
    }
  }

  return ret;
}

int ObRebalanceTaskQueue::check_balance_task_can_pop(const ObRebalanceTaskInfo* task_info, const int64_t out_cnt_lmt,
    const int64_t in_cnt_lmt, bool& task_info_can_schedule)
{
  int ret = OB_SUCCESS;
  task_info_can_schedule = true;
  int64_t now = ObTimeUtility::current_time();
  int64_t interval = ObRebalanceTaskMgr::DATA_IN_CLEAR_INTERVAL;

  if (OB_ISNULL(task_info) || BACKUP_REPLICA == task_info->get_rebalance_task_type() ||
      VALIDATE_BACKUP == task_info->get_rebalance_task_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check balance task can pop get invalid argument", K(ret), KP(task_info));
  } else if (task_info->is_sibling_in_schedule()) {
    task_info_can_schedule = false;
  } else if (NULL != task_info->get_src_server_stat() &&
             task_info->get_src_server_stat()->v_.in_schedule_out_cnt_ >= out_cnt_lmt) {
    task_info_can_schedule = false;
  } else if (NULL != task_info->get_dest_server_stat() &&
             (task_info->get_dest_server_stat()->v_.in_schedule_in_cnt_ >= in_cnt_lmt ||
                 task_info->get_dest_server_stat()->v_.data_in_limit_ts_ + interval >= now)) {
    task_info_can_schedule = false;
  } else {
  }
  return ret;
}

int ObRebalanceTaskQueue::check_backup_task_can_pop(const ObRebalanceTaskInfo* task_info, bool& task_info_can_schedule)
{
  int ret = OB_SUCCESS;
  task_info_can_schedule = true;
  int64_t now = ObTimeUtility::current_time();
  int64_t interval = ObRebalanceTaskMgr::DATA_IN_CLEAR_INTERVAL;
  ObRebalanceTaskType task_type = task_info->get_rebalance_task_type();

  if (OB_ISNULL(task_info) || (BACKUP_REPLICA != task_type && VALIDATE_BACKUP != task_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check balance task can pop get invalid argument", K(ret), KP(task_info));
  } else if (task_info->is_sibling_in_schedule()) {
    task_info_can_schedule = false;
  } else if (NULL != task_info->get_dest_server_stat() &&
             (task_info->get_dest_server_stat()->v_.in_schedule_backup_cnt_ >= share::OB_GROUP_BACKUP_CONCURRENCY ||
                 task_info->get_dest_server_stat()->v_.data_in_limit_ts_ + interval >= now)) {
    task_info_can_schedule = false;
  } else {
  }
  return ret;
}

int ObRebalanceTaskQueue::get_timeout_task(const int64_t network_bandwidth, ObRebalanceTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (network_bandwidth <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(network_bandwidth));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    DLIST_FOREACH(t, schedule_list_)
    {
      int64_t timeout = 0;
      if (OB_FAIL(t->get_timeout(network_bandwidth,
              std::max(config_->server_data_copy_in_concurrency.get(), config_->server_data_copy_out_concurrency.get()),
              config_->balancer_task_timeout,
              timeout))) {
        LOG_WARN("get timeout failed", K(ret), "task", *t, K(network_bandwidth));
      } else if (t->get_schedule_time() + timeout < now) {
        task = &(*t);
        break;
      }
    }
  }
  return ret;
}

int ObRebalanceTaskQueue::get_not_in_progress_task(ObServerManager* server_mgr, ObRebalanceTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == server_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy null", K(ret), KP(rpc_proxy_));
  } else {
    DLIST_FOREACH_X(t, schedule_list_, OB_SUCC(ret))
    {
      Bool res = false;
      if (OB_UNLIKELY(NULL == t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task null", K(ret), KP(t));
      } else if (ObRebalanceTaskType::FAST_RECOVERY_TASK == t->get_rebalance_task_type()) {
        // do not check sql background task
      } else {
        bool exist = true;
        share::ObServerStatus server_status;
        const common::ObAddr& dest = t->get_dest();
        if (OB_FAIL(server_mgr->is_server_exist(dest, exist))) {
          LOG_WARN("fail to check server exist", K(ret));
        } else if (!exist) {
          task = t;
          break;
        } else if (OB_FAIL(server_mgr->get_server_status(dest, server_status))) {
          LOG_WARN("fail to get server status", K(ret));
        } else if (server_status.is_permanent_offline()) {
          task = t;
          break;
        } else if (server_status.is_temporary_offline()) {
          // bypass
        } else {
          if (ObRebalanceTaskType::SQL_BACKGROUND_DIST_TASK == t->get_rebalance_task_type()) {
            ObRebalanceSqlBKGTask* bkgd_task = static_cast<ObRebalanceSqlBKGTask*>(t);
            bool exist = true;
            if (OB_FAIL(bkgd_task->check_task_exist(exist))) {
              LOG_WARN("check task exist failed", K(ret));
            } else if (!exist) {
              task = t;
              break;
            }
          } else {
            if (OB_FAIL(rpc_proxy_->to(t->dst_).check_migrate_task_exist(t->task_id_, res))) {
              LOG_WARN("fail to check task", K(ret), K(t->task_id_), K(t->dst_));
            } else if (!res) {
              task = t;
              break;
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && NULL != task &&
        ObRebalanceTaskType::SQL_BACKGROUND_DIST_TASK == task->get_rebalance_task_type()) {
      // wakeup sql scheduler here for sql background task.
      task->notify_cancel();
    }
  }
  return ret;
}

int ObRebalanceTaskQueue::get_discard_in_schedule_task(
    const ObAddr& server, const int64_t start_service_time, ObRebalanceTask*& task)
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

int ObRebalanceTaskQueue::discard_waiting_task(const common::ObAddr& server, const int64_t start_service_time)
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
        t->clean_server_and_tenant_ref(task_info_map_);
        t->clean_task_info_map(task_info_map_);
        t->notify_cancel();
        t->~ObRebalanceTask();
        task_alloc_.free(t);
        t = NULL;
      }
    }
  }
  return ret;
}

int ObRebalanceTaskQueue::get_schedule_task(const ObRebalanceTask& input_task, ObRebalanceTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  const ObRebalanceTaskInfo* sample_task_info = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (OB_UNLIKELY(input_task.get_sub_task_count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(ret), "task_info_count", input_task.get_sub_task_count());
  } else if (nullptr == (sample_task_info = input_task.get_sub_task(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sample task info ptr is null", K(ret));
  } else {
    ObRebalanceTaskInfo* task_info_in_map = NULL;
    const ObRebalanceTaskKey& sample_task_key = sample_task_info->get_task_key();
    const ObRebalanceTask* sample_task = NULL;
    if (OB_FAIL(task_info_map_.get_refactored(sample_task_key, task_info_in_map))) {
      if (OB_HASH_NOT_EXIST == ret) {
        task = NULL;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get task info from map failed", K(ret));
      }
    } else if (OB_UNLIKELY(NULL == task_info_in_map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null task info from map", K(ret));
    } else if (NULL == (sample_task = task_info_in_map->get_host())) {
      task = NULL;
    } else if (sample_task->get_sub_task_count() != input_task.get_sub_task_count()) {
      task = NULL;
    } else if (ObRebalanceTaskType::SQL_BACKGROUND_DIST_TASK == sample_task->get_rebalance_task_type()) {
      task = const_cast<ObRebalanceTask*>(sample_task);
    } else if (ObRebalanceTaskType::FAST_RECOVERY_TASK == sample_task->get_rebalance_task_type()) {
      if (((ObFastRecoveryTaskInfo*)(sample_task_info))->get_dst() ==
              ((ObFastRecoveryTaskInfo*)(task_info_in_map))->get_dst() &&
          ((ObFastRecoveryTaskInfo*)(sample_task_info))->get_src() ==
              ((ObFastRecoveryTaskInfo*)(task_info_in_map))->get_src()) {
        task = const_cast<ObRebalanceTask*>(sample_task);
      } else {
        task = NULL;
      }
    } else if (!sample_task->in_schedule()) {
      task = NULL;
    } else if (sample_task->get_dest() != input_task.get_dest()) {
      task = NULL;
    } else if (sample_task->get_rebalance_task_type() != input_task.get_rebalance_task_type()) {
      task = NULL;
    } else {
      task = const_cast<ObRebalanceTask*>(sample_task);
      bool need_check = true;
      for (int64_t i = 0; OB_SUCC(ret) && need_check && i < input_task.get_sub_task_count(); ++i) {
        const ObRebalanceTaskInfo* input_task_info = input_task.get_sub_task(i);
        if (OB_UNLIKELY(nullptr == input_task_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("input task info null", K(ret));
        } else {
          const ObRebalanceTaskKey& task_key = input_task_info->get_task_key();
          ObRebalanceTaskInfo* task_info = NULL;
          int tmp_ret = task_info_map_.get_refactored(task_key, task_info);
          if (OB_HASH_NOT_EXIST == tmp_ret) {
            need_check = false;
            task = NULL;
          } else if (OB_SUCCESS == tmp_ret) {
            if (OB_UNLIKELY(nullptr == task_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null task info from map", K(ret));
            } else if (sample_task != task_info->get_host()) {
              task = NULL;
              need_check = false;
            } else if (task_info->get_partition_key() != input_task_info->get_partition_key()) {
              task = NULL;
              need_check = false;
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get from hash failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRebalanceTaskQueue::get_schedule_task(
    const ObRebalanceTaskInfo& task_info, const ObAddr& dest, ObRebalanceTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  const ObRebalanceTaskInfo* sample_task_info = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (!task_info.get_task_key().is_valid() || !dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get schedule task get invalid argument", K(ret), K(task_info), K(dest));
  } else {
    ObRebalanceTaskInfo* task_info_in_map = NULL;
    const ObRebalanceTaskKey& sample_task_key = task_info.get_task_key();
    const ObRebalanceTask* sample_task = NULL;
    if (OB_FAIL(task_info_map_.get_refactored(sample_task_key, task_info_in_map))) {
      if (OB_HASH_NOT_EXIST == ret) {
        task = NULL;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get task info from map failed", K(ret));
      }
    } else if (OB_UNLIKELY(NULL == task_info_in_map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null task info from map", K(ret));
    } else if (NULL == (sample_task = task_info_in_map->get_host())) {
      task = NULL;
    } else if (ObRebalanceTaskType::SQL_BACKGROUND_DIST_TASK == sample_task->get_rebalance_task_type()) {
      task = const_cast<ObRebalanceTask*>(sample_task);
    } else if (!sample_task->in_schedule()) {
      task = NULL;
    } else if (sample_task->get_dest() != dest) {
      task = NULL;
    } else if (sample_task->get_rebalance_task_type() != task_info.get_rebalance_task_type()) {
      task = NULL;
    } else {
      task = const_cast<ObRebalanceTask*>(sample_task);
    }
  }
  return ret;
}

int ObRebalanceTaskQueue::finish_schedule(ObRebalanceTask* task)
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
    // remove from schedule_list_
    schedule_list_.remove(task);
    // clean up server stat map and tenant stat map
    task->clean_server_and_tenant_ref(task_info_map_);
    task->clean_task_info_map(task_info_map_);
    task->~ObRebalanceTask();
    task_alloc_.free(task);
    task = NULL;
  }
  return ret;
}

int ObRebalanceTaskQueue::flush_wait_task(const uint64_t tenant_id, const ObAdminClearBalanceTaskArg::TaskType& type)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else {
    LOG_WARN("start to flush wait task", K(tenant_id), K(type), "size", wait_list_.get_size());
    DLIST_FOREACH_REMOVESAFE(task, wait_list_)
    {
      bool is_consistency = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_consistency && i < task->get_sub_task_count(); ++i) {
        const ObRebalanceTaskInfo* task_info = task->get_sub_task(i);
        if (OB_UNLIKELY(nullptr == task_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task info ptr is null", K(ret));
        } else {
          bool this_consistency = task_info->is_consistency(tenant_id, type);
          is_consistency = is_consistency && this_consistency;
        }
      }
      if (is_consistency) {
        LOG_INFO("discard waiting task", K(*task), K(tenant_id), K(type));
        wait_list_.remove(task);
        task->clean_server_and_tenant_ref(task_info_map_);
        task->clean_task_info_map(task_info_map_);
        task->notify_cancel();
        task->~ObRebalanceTask();
        task_alloc_.free(task);
        task = NULL;
      }
    }
  }
  return ret;
}

int ObRebalanceTaskQueue::flush_wait_task(const ObZone zone, const ObServerManager* server_mgr,
    const ObAdminClearBalanceTaskArg::TaskType& type, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (NULL == server_mgr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server manage must nut null");
  } else {
    LOG_WARN("start to flush wait task", K(zone), K(type), "size", wait_list_.get_size());
    DLIST_FOREACH_REMOVESAFE(task, wait_list_)
    {
      share::ObServerStatus dest_status;
      if (OB_FAIL(server_mgr->get_server_status(task->get_dest(), dest_status))) {
        LOG_WARN("failed to get server status", K(ret));
      } else if (dest_status.zone_ != zone) {
        continue;
      } else {
        // check if the type and tenant requirements are met
        bool is_consistency = true;
        for (int64_t i = 0; OB_SUCC(ret) && is_consistency && i < task->get_sub_task_count(); ++i) {
          const ObRebalanceTaskInfo* task_info = task->get_sub_task(i);
          if (OB_UNLIKELY(nullptr == task_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("task info ptr is null", K(ret));
          } else {
            bool this_consistency = task_info->is_consistency(tenant_id, type);
            is_consistency = is_consistency && this_consistency;
          }
        }
        if (is_consistency) {
          LOG_INFO("discard waiting task", K(*task), K(tenant_id), K(type));
          wait_list_.remove(task);
          task->clean_server_and_tenant_ref(task_info_map_);
          task->clean_task_info_map(task_info_map_);
          task->notify_cancel();
          task->~ObRebalanceTask();
          task_alloc_.free(task);
          task = NULL;
        }
      }  // end else
    }    // end for find task
  }
  return ret;
}

int ObRebalanceTaskQueue::set_sibling_in_schedule(const ObRebalanceTaskKey& task_key, const bool in_schedule)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not init", K(ret));
  } else {
    ObRebalanceTaskInfo* task_info = NULL;
    if (OB_FAIL(task_info_map_.get_refactored(task_key, task_info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get task info from map failed", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_UNLIKELY(NULL == task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get NULL task info from map", K(ret), KP(task_info));
    } else {
      task_info->set_sibling_in_schedule(in_schedule);
    }
  }
  return ret;
}

int ObRebalanceTaskQueue::set_sibling_in_schedule(const ObRebalanceTask& task, const bool in_schedule)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task.get_sub_task_count(); ++i) {
      const ObRebalanceTaskInfo* this_task_info = task.get_sub_task(i);
      if (nullptr == this_task_info) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this task info", K(ret), KP(this_task_info));
      } else {
        ObRebalanceTaskInfo* task_info = NULL;
        const ObRebalanceTaskKey& task_key = this_task_info->get_task_key();
        if (OB_FAIL(task_info_map_.get_refactored(task_key, task_info))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("fail to get task info from map", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (OB_UNLIKELY(NULL == task_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get NULL task info from map", K(ret), KP(task_info));
        } else {
          task_info->set_sibling_in_schedule(in_schedule);
        }
      }
    }
  }
  return ret;
}

int ObRebalanceTaskQueue::has_in_schedule_task_info(const ObRebalanceTaskKey& task_key, bool& has) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task qeueu not inited", K(ret));
  } else {
    has = false;
    ObRebalanceTaskInfo* task_info = NULL;
    const ObRebalanceTask* task = NULL;
    if (OB_FAIL(task_info_map_.get_refactored(task_key, task_info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get task info from map", K(ret), KP(task_info));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_UNLIKELY(NULL == task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get NULL task info from map", K(ret), KP(task_info));
    } else if (OB_UNLIKELY(NULL == (task = task_info->get_host()))) {
      has = false;
    } else {
      has = task->in_schedule();
    }
  }
  return ret;
}

int ObRebalanceTaskQueue::has_in_migrate_or_transform_task_info(
    const ObPartitionKey& key, const ObAddr& server, bool& has) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("task queue not inited", K(ret));
  } else if (!key.is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition key", K(ret), K(key), K(server));
  } else {
    has = false;
    ObRebalanceTaskInfo* task_info = NULL;
    const ObRebalanceTask* task = NULL;
    ObRebalanceTaskKey task_key;
    if (OB_FAIL(task_key.init(key.get_table_id(),
            key.get_partition_id(),
            0 /*ignore*/,
            OB_INVALID_ID,
            RebalanceKeyType::FORMAL_BALANCE_KEY))) {
      LOG_WARN("fail to init task key", K(ret));
    } else if (OB_FAIL(task_info_map_.get_refactored(task_key, task_info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get task from task map failed", K(ret), K(task_info));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get NULL task from task map", K(ret), K(task_info));
    } else if (OB_UNLIKELY(NULL == (task = task_info->get_host()))) {
      has = false;
    } else if (!task->in_schedule()) {
      has = false;
    } else if (ObRebalanceTaskType::MIGRATE_REPLICA == task_info->get_rebalance_task_type()) {
      ObMigrateTaskInfo* migrate = static_cast<ObMigrateTaskInfo*>(task_info);
      has = (nullptr != migrate && migrate->get_src_member().get_server() == server);
    } else if (ObRebalanceTaskType::TYPE_TRANSFORM == task_info->get_rebalance_task_type()) {
      ObTypeTransformTaskInfo* transform = static_cast<ObTypeTransformTaskInfo*>(task_info);
      has = (nullptr != transform && transform->get_dest().get_server() == server);
    } else {
      has = false;
    }
  }
  return ret;
}

ObRebalanceTaskMgr::ObRebalanceTaskMgr()
    : ObRsReentrantThread(true),
      inited_(false),
      config_(NULL),
      concurrency_limited_ts_(0),
      cond_(),
      tenant_stat_(),
      server_stat_(),
      queues_(),
      high_task_queue_(queues_[0]),
      low_task_queue_(queues_[1]),
      self_(),
      task_executor_(NULL),
      server_mgr_(NULL),
      rpc_proxy_(NULL),
      balancer_(NULL),
      global_index_builder_(NULL)
{}

int ObRebalanceTaskMgr::init(ObServerConfig& config, ObRebalanceTaskExecutor& executor)
{
  return init(config, executor, NULL, NULL, NULL, NULL);
}

int ObRebalanceTaskMgr::init(ObServerConfig& config, ObRebalanceTaskExecutor& executor, ObServerManager* server_mgr,
    obrpc::ObSrvRpcProxy* rpc_proxy, ObRootBalancer* balancer, ObGlobalIndexBuilder* global_index_builder)
{
  int ret = OB_SUCCESS;
  const static int64_t rebalance_task_mgr_thread_cnt = 1;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(NULL == server_mgr || NULL == rpc_proxy || NULL == balancer || NULL == global_index_builder)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::REBALANCE_TASK_MGR_COND_WAIT))) {
    LOG_WARN("fail to init cond, ", K(ret));
  } else if (OB_FAIL(create(rebalance_task_mgr_thread_cnt, "RebTaskMgr"))) {
    LOG_WARN("create rebalance task mgr thread failed", K(ret), K(rebalance_task_mgr_thread_cnt));
  } else {
    config_ = &config;
    task_executor_ = &executor;
    server_mgr_ = server_mgr;
    rpc_proxy_ = rpc_proxy;
    balancer_ = balancer;
    global_index_builder_ = global_index_builder;

    if (OB_FAIL(tenant_stat_.init(TASK_QUEUE_LIMIT))) {
      LOG_WARN("init tenant stat failed", K(ret), LITERAL_K(TASK_QUEUE_LIMIT));
    } else if (OB_FAIL(server_stat_.init(TASK_QUEUE_LIMIT))) {
      LOG_WARN("init server stat failed", K(ret), LITERAL_K(TASK_QUEUE_LIMIT));
    } else if (OB_FAIL(high_task_queue_.init(config, tenant_stat_, server_stat_, TASK_QUEUE_LIMIT, rpc_proxy_))) {
      LOG_WARN("init rebalance task queue failed", K(ret), LITERAL_K(TASK_QUEUE_LIMIT));
    } else if (OB_FAIL(low_task_queue_.init(config, tenant_stat_, server_stat_, TASK_QUEUE_LIMIT, rpc_proxy_))) {
      LOG_WARN("init migrate task queue failed", K(ret), LITERAL_K(TASK_QUEUE_LIMIT));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

void ObRebalanceTaskMgr::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    ObThreadCondGuard guard(cond_);
    cond_.broadcast();
  }
}

void ObRebalanceTaskMgr::run3()
{
  int ret = OB_SUCCESS;
  LOG_INFO("rebalance task executor start");
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    int64_t last_dump_time = ObTimeUtility::current_time();
    int64_t last_check_task_in_progress_ts = ObTimeUtility::current_time();
    while (!stop_) {
      update_last_run_timestamp();
      ObRebalanceTask* task = NULL;
      DEBUG_SYNC(STOP_BALANCE_EXECUTOR);
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
          } else if (wait_cnt > 0 && in_schedule_cnt < config_->data_copy_concurrency && 0 == concurrency_limited_ts_) {
            if (OB_FAIL(pop_task(task))) {
              LOG_WARN("pop task for execute failed", K(ret));
            } else {
            }  // no more to do
          } else {
            int64_t now = ObTimeUtility::current_time();
            cond_.wait(get_schedule_interval() / 1000);
            if (concurrency_limited_ts_ + CONCURRENCY_LIMIT_INTERVAL < now) {
              concurrency_limited_ts_ = 0;
            }
          }
        }
      }  // after this block %task has no lock protect, can not be accessed

      if (OB_SUCCESS == ret && NULL != task) {
        void* raw_ptr = nullptr;
        ObRebalanceTask* input_task = nullptr;
        common::ObArenaAllocator allocator;
        const int64_t task_deep_copy_size = task->get_deep_copy_size();
        if (nullptr == (raw_ptr = allocator.alloc(task_deep_copy_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate task", K(ret));
        } else if (OB_FAIL(task->clone(raw_ptr, input_task))) {
          LOG_WARN("fail to clone input task", K(ret));
        } else if (nullptr == input_task) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("input task ptr is null", K(ret));
        } else if (OB_FAIL(execute_task(*input_task))) {
          LOG_WARN("send task to execute failed", K(ret));
        } else {
          int64_t now = ObTimeUtility::current_time();
          task->set_executor_time(now);
        }  // no more to do
        if (nullptr != input_task) {
          input_task->~ObRebalanceTask();
          allocator.free(input_task);
          input_task = nullptr;
          raw_ptr = nullptr;
        } else if (nullptr != raw_ptr) {
          allocator.free(raw_ptr);
          raw_ptr = nullptr;
        }
      };
      {
        // CHECK TASK IN PROGRESS
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = check_task_in_progress(last_check_task_in_progress_ts))) {
          LOG_WARN("fail to check task in progress", K(tmp_ret));
        }
      }
    }
  }
  LOG_INFO("rebalance task executor stop");
}

int ObRebalanceTaskMgr::check_dest_server_migrate_in_blocked(const ObRebalanceTask& task, bool& is_available)
{
  int ret = OB_SUCCESS;
  const ObAddr& dest = task.get_dest();
  ObServerTaskStatMap::Item* server_stat_ins = NULL;
  if (OB_FAIL(server_stat_.locate(dest, server_stat_ins))) {
    LOG_WARN("get server stat failed", K(ret));
  } else if (NULL == server_stat_ins) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null server stat", K(ret));
  } else if (0 == server_stat_ins->v_.data_in_limit_ts_) {
    // server migrate in restriction not reach
    is_available = true;
  } else if (server_stat_ins->v_.data_in_limit_ts_ + DATA_IN_CLEAR_INTERVAL < ObTimeUtility::current_time()) {
    // restriction timeout, clean up
    is_available = true;
    server_stat_ins->v_.data_in_limit_ts_ = 0;
    server_stat_ins->unref();
    LOG_INFO("clear server data copy in", K(dest));
  } else {
    // server is in migrate in restriction state
    is_available = false;
    LOG_WARN("dest server migrate in delay", K(dest), K(task));
  }
  return ret;
}

int ObRebalanceTaskMgr::check_dest_server_out_of_disk(const ObRebalanceTask& task, bool& is_available)
{
  int ret = OB_SUCCESS;
  const ObAddr& dest = task.get_dest();
  share::ObServerStatus server_status;
  int64_t task_required_size = 0;
  if (NULL == server_mgr_) {
    is_available = true;  // skip check
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebalance task manager not inited", K(ret));
  } else if (OB_FAIL(server_mgr_->get_server_status(dest, server_status))) {
    LOG_WARN("get server status fail", K(dest), K(ret));
  } else if (server_status.resource_info_.partition_cnt_ < 0) {
    // old version server, has no way to check resource, ignore
    is_available = true;
  } else if (OB_FAIL(task.get_execute_transmit_size(task_required_size))) {
    LOG_WARN("fail to get transmit size", K(ret));
  } else {
    int64_t required_size = task_required_size + server_status.resource_info_.disk_in_use_;
    int64_t total_size = server_status.resource_info_.disk_total_;
    int64_t required_percent = (100 * required_size) / total_size;
    int64_t limit_percent = GCONF.data_disk_usage_limit_percentage;
#ifdef ERRSIM
    if (GCONF.skip_balance_disk_limit) {
      required_percent = 0;
    }
#endif
    if (task_required_size <= 0) {
      is_available = true;
    } else if (required_percent >= limit_percent) {
      is_available = false;
      LOG_ERROR("migrate/add/rebuild/transform task fail! server out of disk space.",
          K(dest),
          K(task_required_size),
          K(required_size),
          K(total_size),
          K(required_percent),
          K(limit_percent));
    } else {
      is_available = true;
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::check_dest_server_has_too_many_partitions(const ObRebalanceTask& task, bool& is_available)
{
  int ret = OB_SUCCESS;
  const ObAddr& dest = task.get_dest();
  share::ObServerStatus server_status;
  if (task.get_rebalance_task_type() != ObRebalanceTaskType::MIGRATE_REPLICA ||
      task.get_rebalance_task_type() != ObRebalanceTaskType::ADD_REPLICA) {
    // server partition count not changed
    is_available = true;
  } else if (NULL == server_mgr_) {
    is_available = true;  // skip check
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebalance task manager not inited", K(ret));
  } else if (OB_FAIL(server_mgr_->get_server_status(dest, server_status))) {
    LOG_WARN("get server status fail", K(dest), K(ret));
  } else if (server_status.resource_info_.partition_cnt_ < 0) {
    // old version server, has no way to check resource, ignore
    is_available = true;
  } else {
    // partition number restriction
    const int64_t task_cnt = task.get_sub_task_count();
    int64_t required_cnt = task_cnt + server_status.resource_info_.partition_cnt_;
    if (required_cnt > OB_MAX_PARTITION_NUM_PER_SERVER) {
      is_available = false;
      LOG_ERROR("migrate/add task fail! server has too many partitions.",
          K(dest),
          K(task_cnt),
          K(required_cnt),
          "limit",
          OB_MAX_PARTITION_NUM_PER_SERVER);
    } else {
      is_available = true;
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::get_tenant_task_info_cnt(
    const uint64_t tenant_id, int64_t& high_priority_task_cnt, int64_t& low_priority_task_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebalance task manager not inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    high_priority_task_cnt = 0;
    low_priority_task_cnt = 0;
    ObThreadCondGuard guard(const_cast<ObThreadCond&>(cond_));
    ObTenantTaskStat stat;
    if (OB_FAIL(tenant_stat_.get(tenant_id, stat))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get tenant stat from hash table failed", K(ret), K(tenant_id));
      }
    } else {
      high_priority_task_cnt = stat.high_priority_task_cnt_;
      low_priority_task_cnt = stat.low_priority_task_cnt_;
    }
  }
  return ret;
}

/* this interface is used by LeaderCoordinator, when a task trys to migrate
 * the leader replica or trys to transform a leader into another type, the interface
 * is invoked the calculate the leader switch priority
 */
int ObRebalanceTaskMgr::has_in_migrate_or_transform_task_info(
    const ObPartitionKey& pkey, const ObAddr& server, bool& has) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition key", K(pkey), K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    ObThreadCondGuard guard(const_cast<ObThreadCond&>(cond_));
    has = false;
    for (int64_t i = 0; OB_SUCC(ret) && !has && i < ARRAYSIZEOF(queues_); ++i) {
      if (OB_FAIL(queues_[i].has_in_migrate_or_transform_task_info(pkey, server, has))) {
        LOG_WARN("check has_in_migrate_or_transform_task failed", K(pkey), K(server), K(ret));
      }
    }
  }
  return ret;
}

void ObRebalanceTaskMgr::reuse()
{
  if (inited_) {
    ObThreadCondGuard guard(const_cast<ObThreadCond&>(cond_));
    for (int64_t i = 0; i < ARRAYSIZEOF(queues_); ++i) {
      queues_[i].reuse();
    }
    concurrency_limited_ts_ = 0;
    server_stat_.reuse();
    tenant_stat_.reuse();
  }
}

int ObRebalanceTaskMgr::set_self(const ObAddr& self)
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

int ObRebalanceTaskMgr::set_sibling_in_schedule(const ObRebalanceTask& task, const bool in_schedule)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (OB_FAIL(queues_[i].set_sibling_in_schedule(task, in_schedule))) {
        LOG_WARN("schedule sibling in schedule failed", K(ret), K(i), K(task));
      }
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::pop_task(ObRebalanceTask*& task)
{
  int ret = OB_SUCCESS;
  int64_t wait_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (queues_[i].wait_task_cnt() > 0) {
        wait_cnt += queues_[i].wait_task_cnt();
        if (OB_FAIL(queues_[i].pop_task(task))) {
          LOG_WARN("pop_task failed", K(ret));
        } else if (NULL != task) {
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL == task) {
        if (wait_cnt > 0) {
          concurrency_limited_ts_ = ObTimeUtility::current_time();
        }
      } else {
        const bool in_schedule = true;
        if (OB_FAIL(set_sibling_in_schedule(*task, in_schedule))) {
          LOG_WARN("set sibling in schedule failed", K(ret), "task", *task);
        }
      }
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::check_task_in_progress(int64_t& last_check_task_in_progress_ts)
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

int ObRebalanceTaskMgr::do_check_task_in_progress()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      common::ObArray<int> rc_array;
      ObRebalanceTask* task = nullptr;
      do {
        task = nullptr;
        void* raw_ptr = nullptr;
        ObRebalanceTask* input_task = nullptr;
        common::ObArenaAllocator allocator;
        {
          int64_t task_deep_copy_size = -1;
          ObThreadCondGuard guard(cond_);
          if (OB_FAIL(queues_[i].get_not_in_progress_task(server_mgr_, task))) {
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
          } else {
            const int rc = OB_REBALANCE_TASK_NOT_IN_PROGRESS;
            LOG_INFO("task not in progress on observer", K(rc), "task_id", task->task_id_);
            if (OB_FAIL(task->generate_sub_task_err_code(rc, rc_array))) {
              LOG_WARN("fail to generate sub task err code", K(ret));
            } else if (OB_FAIL(do_execute_over(*task, rc_array))) {
              LOG_WARN("do execute over failed", K(ret));
            }
          }
        };
        if (nullptr != input_task) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = notify_global_index_builder(*input_task))) {
            LOG_WARN("fail to notify global index builder", K(ret));
          }
        }
        if (nullptr != input_task) {
          input_task->~ObRebalanceTask();
          allocator.free(input_task);
          input_task = nullptr;
          raw_ptr = nullptr;
        } else if (nullptr != raw_ptr) {
          allocator.free(raw_ptr);
          raw_ptr = nullptr;
        }
      } while (OB_SUCC(ret) && nullptr != task);
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::add_task(const ObRebalanceTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebalance task manager not inited", K(ret));
  } else if (task.skip_put_to_queue()) {
    int tmp_ret = OB_SUCCESS;
    common::ObArray<int> rc_array;
    const_cast<ObRebalanceTask*>(&task)->set_schedule_time(ObTimeUtility::current_time());
    if (OB_FAIL(task_executor_->execute(task, rc_array))) {
      LOG_WARN("execute task failed", K(ret), K(task));
    }
    if (OB_SUCCESS != (tmp_ret = log_task_result(task, rc_array))) {
      LOG_WARN("fail to log task result", K(ret), K(tmp_ret), K(task));
    }
    if (OB_SUCCESS != (tmp_ret = process_failed_task(task, rc_array))) {
      LOG_WARN("fail to process failed task", K(ret), K(task));
    } else {
    }  // no more to do
  } else {
    ObThreadCondGuard guard(cond_);

    bool has_task_info_in_schedule = false;
    ObRebalanceTaskQueue& queue = task.is_high_priority_task() ? high_task_queue_ : low_task_queue_;
    ObRebalanceTaskQueue& sibling_queue = task.is_high_priority_task() ? low_task_queue_ : high_task_queue_;
    bool is_available = false;
    int64_t wait_cnt = 0;
    int64_t in_schedule_cnt = 0;
    if (OB_FAIL(ret)) {
    } else if (queue.task_info_cnt() >= TASK_QUEUE_LIMIT) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("task queue full", K(ret), "task_count", queue.task_info_cnt());
    } else if (OB_FAIL(task.check_can_execute_on_dest(*this, is_available))) {
      LOG_WARN("fail to check dest server copy in available", K(ret));
    } else if (!is_available) {
      if (ObRebalanceTaskType::COPY_SSTABLE == task.get_rebalance_task_type()) {
        ret = OB_EAGAIN;
      } else {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("dest server is not available to copy data in", "dest", task.get_dest(), K(ret));
      }
    } else if (OB_FAIL(queue.push_task(*this, sibling_queue, task, has_task_info_in_schedule))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_WARN("push back task failed", K(ret));
      } else if (task.is_admin_cmd()) {
        // 'alter system' cmd, return error code the client
        LOG_WARN("partition rebalance task already exists", KR(ret));
        LOG_USER_ERROR(OB_ENTRY_EXIST, "partition rebalance task already exists");
      } else {
      }  // oceanbase internal task, no need to report error msg
    } else if (OB_FAIL(get_task_cnt(wait_cnt, in_schedule_cnt))) {
      LOG_WARN("fail to get task cnt", K(ret));
    } else if (!has_task_info_in_schedule && 0 == concurrency_limited_ts_ &&
               in_schedule_cnt < config_->data_copy_concurrency) {
      cond_.broadcast();
    }
    clear_reach_concurrency_limit();
  }
  if (!task.skip_put_to_queue()) {
    LOG_INFO("root balance add task", K(ret), K(task));
  } else {
    LOG_INFO("root balance add task", K(ret), "sub_task_cnt", task.get_sub_task_count());
  }
  return ret;
}

int ObRebalanceTaskMgr::add_task(const ObRebalanceTask& task, int64_t& task_info_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(add_task(task))) {
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_OP_NOT_ALLOW == ret) {
      // ignore this error code
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to add task to task manager", K(ret), K(task));
    }
  } else {
    LOG_INFO("add rebalance task", K(task));
    task_info_cnt += task.get_sub_task_count();
  }
  return ret;
}

int ObRebalanceTaskMgr::execute_over(const ObRebalanceTask& task, const ObIArray<int>& rc_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebalance task manager not inited", K(ret));
  } else {
    DEBUG_SYNC(REBALANCE_TASK_MGR_BEFORE_EXECUTE_OVER);
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(do_execute_over(task, rc_array))) {
      LOG_WARN("do execute over failed", K(ret), K(task), K(rc_array));
    }
  }
  // try to notify global index builder no matter it succeeds or fails
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = notify_global_index_builder(task))) {
    LOG_WARN("fail to notify global index builder", K(ret));
  }
  LOG_INFO("execute over",
      K(ret),
      "high_prio_task_info_cnt",
      get_high_priority_task_info_cnt(),
      "low_prio_task_info_cnt",
      get_low_priority_task_info_cnt());
  return ret;
}

int ObRebalanceTaskMgr::notify_global_index_builder(const ObRebalanceTask& rebalance_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == global_index_builder_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global index build ptr is null", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = global_index_builder_->on_copy_multi_replica_reply(rebalance_task))) {
      LOG_WARN("fail to on copy multi replica reply", K(ret));
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::do_execute_over(
    const ObRebalanceTask& input_task, const ObIArray<int>& rc_array, const bool need_try_clear_server_data_in_limit)
{
  int ret = OB_SUCCESS;
  ObRebalanceTask* task = NULL;
  ObRebalanceTaskQueue* queue = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (OB_FAIL(queues_[i].get_schedule_task(input_task, task))) {
        LOG_WARN("get schedule task failed", K(ret));
      } else {
        if (NULL != task) {
          queue = &queues_[i];
          break;
        }
      }
    }
  }

  int log_ret = OB_SUCCESS;
  if (NULL != task) {
    // do_execute_over input with a fragment information, no  comments, data_size and so on
    if (OB_SUCCESS != (log_ret = log_task_result(*task, rc_array))) {
      LOG_WARN("log task result failed", K(ret), K(log_ret));
    }
    if (OB_SUCCESS != (log_ret = process_failed_task(*task, rc_array))) {
      LOG_WARN("failed to process task", K(log_ret), K(*task));
    }
  } else {
    if (OB_SUCCESS != (log_ret = log_task_result(input_task, rc_array))) {
      LOG_WARN("log task result failed", K(ret), K(log_ret));
    }
    if (OB_SUCCESS != (log_ret = process_failed_task(input_task, rc_array))) {
      LOG_WARN("failed to process task", K(log_ret), K(input_task));
    }
  }

  if (NULL != balancer_ && balancer_->is_inited() && !balancer_->is_stop()) {
    // try to wakeup the balancer so as to process all tasks effectively
    balancer_->wakeup();
  }

  if (OB_SUCC(ret)) {
    if (NULL == task) {
      LOG_INFO("in schedule task not found, "
               "maybe timeouted or scheduled by previous rootserver",
          K(input_task),
          K(rc_array));
    } else {
      LOG_INFO("finish schedule task", "task", (*task));
      const bool in_schedule = false;
      if (OB_FAIL(set_sibling_in_schedule(*task, in_schedule))) {
        LOG_WARN("set sibling in schedule failed", K(ret), "task", *task);
      } else if (OB_FAIL(queue->finish_schedule(task))) {
        LOG_WARN("finish schedule failed", K(ret), K(task));
      } else {
      }  // no more to do
    }
  }

  // reset schedule limited && trigger schedule next task
  concurrency_limited_ts_ = 0;
  if (need_try_clear_server_data_in_limit && input_task.get_dest().is_valid()) {
    try_clear_server_data_in_limit(input_task.get_dest());
  }
  cond_.broadcast();
  return ret;
}

int ObRebalanceTaskMgr::set_server_data_in_limit(const ObAddr& addr)
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

void ObRebalanceTaskMgr::try_clear_server_data_in_limit(const ObAddr& addr)
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
      // not up to upper limit, ignore
    } else {
      server_stat->v_.data_in_limit_ts_ = 0;  // clean up migration restriction
      server_stat->unref();
      LOG_INFO("clear server data copy in", K(addr));
    }
  }
}

int ObRebalanceTaskMgr::get_task_cnt(int64_t& wait_cnt, int64_t& in_schedule_cnt)
{
  int ret = OB_SUCCESS;
  wait_cnt = 0;
  in_schedule_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(queues_); ++i) {
      wait_cnt += queues_[i].wait_task_cnt();
      in_schedule_cnt += queues_[i].in_schedule_task_cnt();
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::log_task_result(const ObRebalanceTask& task, const ObIArray<int>& rc_array)
{
  int ret = OB_SUCCESS;
  int64_t failed_count = 0;
  if (OB_FAIL(task.log_execute_result(rc_array))) {
    LOG_WARN("fail to log execute result", K(ret));
  }
  // ignore ret code
  for (int i = 0; i < rc_array.count(); ++i) {
    if (OB_SUCCESS != rc_array.at(i)) {
      failed_count++;
    }
  }
  if (failed_count > 0) {
    EVENT_ADD(RS_TASK_FAIL_COUNT, failed_count);
  }
  return ret;
}

int ObRebalanceTaskMgr::process_failed_task(const ObRebalanceTask& task, const ObIArray<int>& rc_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == GCONF.balance_blacklist_failure_threshold) {
    // do nothing ,close black list
  } else if (rc_array.count() != task.get_sub_task_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rc_array and sub task count not match",
        K(ret),
        "rc_array_count",
        rc_array.count(),
        "sub_task_cnt",
        task.get_sub_task_count());
  } else {
    int64_t count = 0;
    common::ObArray<uint64_t> table_ids;
    common::ObArray<int64_t> partition_ids;
    common::ObArray<common::ObAddr> servers;
    share::ObPartitionReplica::FailList fail_msgs;
    for (int64_t i = 0; OB_SUCC(ret) && i < rc_array.count(); ++i) {
      const ObRebalanceTaskInfo* task_info = task.get_sub_task(i);
      if (OB_UNLIKELY(nullptr == task_info)) {
        // bypass
      } else if (GCONF.cluster_id != task_info->get_cluster_id() || !task_info->need_process_failed_task()) {
        // 1. There is no need to record the task into blacklist when the task is copying data from remote
        // 2. The task which does not need to record into blacklist
      } else if (OB_SUCCESS != rc_array.at(i)) {
        share::ObPartitionReplica::FailMsg fail_msg;
        fail_msg.dest_server_ = task.get_dest();
        fail_msg.task_type_ = task.get_rebalance_task_type();
        uint64_t table_id = task_info->get_partition_key().get_table_id();
        int64_t partition_id = task_info->get_partition_key().get_partition_id();
        const ObAddr& server = task_info->get_faillist_anchor();
        if (OB_FAIL(fail_msg.add_failed_timestamp(ObTimeUtility::current_time()))) {
          LOG_WARN("failed to add failed timestamp", K(ret), K(fail_msg));
        } else if (OB_FAIL(table_ids.push_back(table_id))) {
          LOG_WARN("failed to push back table id", K(ret), K(table_id));
        } else if (OB_FAIL(fail_msgs.push_back(fail_msg))) {
          LOG_WARN("failed to push back failmg", K(ret), K(fail_msg));
        } else if (OB_FAIL(partition_ids.push_back(partition_id))) {
          LOG_WARN("failed to push back partition id", K(ret), K(partition_id));
        } else if (OB_FAIL(servers.push_back(server))) {
          LOG_WARN("failed to push back server", K(ret), K(server));
        } else {
          count++;
        }
      }
    }  // end for find replica

    if (OB_SUCC(ret) && count > 0) {
      if (OB_FAIL(balancer_->fill_faillist(count, table_ids, partition_ids, servers, fail_msgs))) {
        LOG_WARN(
            "failed to fill fail list", K(ret), K(count), K(table_ids), K(partition_ids), K(servers), K(fail_msgs));
      }
    }
  }  // end else
  return ret;
}

int ObRebalanceTaskMgr::discard_task(const ObAddr& server, const int64_t start_service_time)
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
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      common::ObArray<int> rc_array;
      ObRebalanceTask* task = nullptr;
      do {
        task = nullptr;
        void* raw_ptr = nullptr;
        ObRebalanceTask* input_task = nullptr;
        common::ObArenaAllocator allocator;
        {
          int64_t task_deep_copy_size = -1;
          ObThreadCondGuard guard(cond_);
          if (OB_FAIL(queues_[i].get_discard_in_schedule_task(server, safe_start_time, task))) {
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
          } else {
            const int rc = OB_CANCELED;
            LOG_INFO("task not in progress on observer", K(rc), "task_id", task->task_id_);
            if (OB_FAIL(task->generate_sub_task_err_code(rc, rc_array))) {
              LOG_WARN("fail to generate sub task err code", K(ret));
            } else if (OB_FAIL(do_execute_over(*task, rc_array))) {
              LOG_WARN("do execute over failed", K(ret));
            }
          }
        };
        if (nullptr != input_task) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = notify_global_index_builder(*input_task))) {
            LOG_WARN("fail to notify global index builder", K(ret));
          }
        }
        if (nullptr != input_task) {
          input_task->~ObRebalanceTask();
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
        if (OB_FAIL(queues_[i].discard_waiting_task(server, safe_start_time))) {
          LOG_WARN("discard waiting task failed", K(ret), K(server), K(safe_start_time));
        }
      }
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::dump_statistics(void) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("Rebalance task manager statistics",
        "waiting_replication_task_cnt",
        high_task_queue_.wait_task_cnt(),
        "executing_replication_task_cnt",
        high_task_queue_.in_schedule_task_cnt(),
        "waiting_migration_task_cnt",
        low_task_queue_.wait_task_cnt(),
        "executing_migration_task_cnt",
        low_task_queue_.in_schedule_task_cnt());

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

int ObRebalanceTaskMgr::execute_task(const ObRebalanceTask& task)
{
  int ret = OB_SUCCESS;
  common::ObArray<int> rc_array;
  if (ObRebalanceTaskType::SQL_BACKGROUND_DIST_TASK == task.get_rebalance_task_type()) {
    if (NULL != ObCurTraceId::get_trace_id()) {
      *ObCurTraceId::get_trace_id() = task.task_id_;
    }
  } else {
    ObCurTraceId::init(self_);
  }
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  LOG_INFO("execute rebalance task", K(task));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_executor_->execute(task, rc_array))) {
    LOG_WARN("fail to execute rebalance task", K(ret));
  } else {
  }  // no more
  ObCurTraceId::init(self_);
  if (OB_FAIL(ret)) {
    ObThreadCondGuard guard(cond_);
    int tmp_ret = OB_SUCCESS;
    rc_array.reset();
    if (OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT == ret) {
      // server end reach the concurrency upper limit
      (void)set_server_data_in_limit(task.get_dest());
    }
    const bool need_clear_data_in_lmt = (OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT != ret);
    if (OB_SUCCESS != (tmp_ret = task.generate_sub_task_err_code(ret, rc_array))) {
      LOG_WARN("fail to generate sub task err code", K(ret));
    } else if (OB_SUCCESS != (tmp_ret = do_execute_over(task, rc_array, need_clear_data_in_lmt))) {
      LOG_WARN("do execute over failed", K(tmp_ret), K(ret), K(task));
    } else {
    }  // no more to do
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = notify_global_index_builder(task))) {
      LOG_WARN("fail to notify global index builder", K(ret));
    }
  }
  return ret;
}

int64_t ObRebalanceTaskMgr::get_high_priority_task_info_cnt() const
{
  ObThreadCondGuard guard(const_cast<ObThreadCond&>(cond_));
  return high_task_queue_.task_info_cnt();
}

int64_t ObRebalanceTaskMgr::get_low_priority_task_info_cnt() const
{
  ObThreadCondGuard guard(const_cast<ObThreadCond&>(cond_));
  return low_task_queue_.task_info_cnt();
}

int ObRebalanceTaskMgr::get_min_out_data_size_server(
    const common::ObIArray<common::ObAddr>& candidate, common::ObIArray<int64_t>& candidate_idxs)
{
  int ret = OB_SUCCESS;
  ObServerTaskStat src_server_stat;
  int64_t min = INT64_MAX;
  {
    ObThreadCondGuard guard(const_cast<ObThreadCond&>(cond_));
    for (int64_t i = 0; OB_SUCC(ret) && i < candidate.count(); ++i) {
      if (OB_FAIL(server_stat_.get(candidate.at(i), src_server_stat))) {
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(candidate_idxs.push_back(i))) {  // overwrite ret
            LOG_WARN("fail push value", K(i), K(ret));
          }
          break;
        } else {
          LOG_WARN("get server stat item failed", K(i), K(ret));
        }
      } else if (src_server_stat.total_out_data_size_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty server stat", K(ret), K(src_server_stat));
      } else {
        if (src_server_stat.total_out_data_size_ < min) {
          min = src_server_stat.total_out_data_size_;
        }
      }
    }
    // all servers with the value of total_out_cnt equals to min are candidates
    if (OB_SUCC(ret) && candidate_idxs.count() <= 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < candidate.count(); ++i) {
        if (OB_SUCC(server_stat_.get(candidate.at(i), src_server_stat))) {
          if (min >= src_server_stat.total_out_data_size_) {
            ret = candidate_idxs.push_back(i);
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (candidate_idxs.count() <= 0) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::clear_task(
    const ObIArray<uint64_t>& tenant_ids, const ObAdminClearBalanceTaskArg::TaskType& type)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  for (int64_t cnt = 0; OB_SUCC(ret) && cnt < tenant_ids.count(); cnt++) {
    uint64_t tenant_id = tenant_ids.at(cnt);
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (OB_FAIL(queues_[i].flush_wait_task(tenant_id, type))) {
        LOG_WARN("fail to flush wait task", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && tenant_ids.count() == 0) {
    uint64_t tenant_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (OB_FAIL(queues_[i].flush_wait_task(tenant_id, type))) {
        LOG_WARN("fail to flush wait task", K(ret));
      }
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::clear_task(
    const common::ObIArray<ObZone>& zone_names, const ObAdminClearBalanceTaskArg::TaskType& type, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  for (int64_t cnt = 0; OB_SUCC(ret) && cnt < zone_names.count(); cnt++) {
    ObZone zone = zone_names.at(cnt);
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); i++) {
      if (OB_FAIL(queues_[i].flush_wait_task(zone, server_mgr_, type, tenant_id))) {
        LOG_WARN("failed to flush wait tash", K(ret), K(zone));
      }
    }
  }

  if (OB_SUCC(ret) && zone_names.count() == 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (OB_FAIL(queues_[i].flush_wait_task(tenant_id, type))) {
        LOG_WARN("fail to flush wait task", K(ret));
      }
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::clear_task(uint64_t tenant_id, const obrpc::ObAdminClearBalanceTaskArg::TaskType type)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    ObThreadCondGuard guard(cond_);
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(queues_); ++i) {
      if (OB_FAIL(queues_[i].flush_wait_task(tenant_id, type))) {
        LOG_WARN("fail to flush wait task", K(ret));
      }
    }
  }
  return ret;
}

int ObRebalanceTaskMgr::get_all_tasks(common::ObIAllocator& allocator, common::ObIArray<ObRebalanceTask*>& tasks)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObThreadCond& cond = get_cond();
    ObThreadCondGuard guard(cond);
    const ObRebalanceTaskQueue::TaskList* all_task_list[4] = {&get_high_priority_queue().get_wait_list(),
        &get_high_priority_queue().get_schedule_list(),
        &get_low_priority_queue().get_wait_list(),
        &get_low_priority_queue().get_schedule_list()};
    for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
      DLIST_FOREACH(curr, *all_task_list[i])
      {
        void* raw_ptr = nullptr;
        ObRebalanceTask* new_task = nullptr;
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

int ObRebalanceTaskMgr::get_all_tasks_count(
    int64_t& high_wait_count, int64_t& high_scheduled_count, int64_t& low_wait_count, int64_t& low_scheduled_count)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObThreadCondGuard guard(get_cond());
    high_wait_count = get_high_priority_queue().get_wait_list().get_size();
    high_scheduled_count = get_high_priority_queue().get_schedule_list().get_size();
    low_wait_count = get_low_priority_queue().get_wait_list().get_size();
    low_scheduled_count = get_low_priority_queue().get_schedule_list().get_size();
  }
  return ret;
}

int ObRebalanceTaskMgr::get_schedule_task(
    const ObRebalanceTaskInfo& task_info, const ObAddr& dest, common::ObIAllocator& allocator, ObRebalanceTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  ObRebalanceTask* tmp_task = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!task_info.get_rebalance_task_key().is_valid() || !dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get schedul task get invalid argument", K(ret), K(task_info), K(dest));
  } else {
    ObThreadCond& cond = get_cond();
    ObThreadCondGuard guard(cond);
    if (OB_FAIL(get_high_priority_queue().get_schedule_task(task_info, dest, tmp_task))) {
      LOG_WARN("failed to get schedule task", K(ret), K(task_info), K(dest));
    } else if (OB_ISNULL(tmp_task) && OB_FAIL(get_low_priority_queue().get_schedule_task(task_info, dest, tmp_task))) {
      LOG_WARN("failed to get schedule task", K(ret), K(task_info), K(dest));
    }
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(tmp_task)) {
        void* raw_ptr = nullptr;
        const int64_t task_deep_copy_size = tmp_task->get_deep_copy_size();
        if (nullptr == (raw_ptr = allocator.alloc(task_deep_copy_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate task", K(ret));
        } else if (OB_FAIL(tmp_task->clone(raw_ptr, task))) {
          LOG_WARN("fail to clone new task", K(ret));
        } else if (OB_UNLIKELY(nullptr == task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new task ptr is null", K(ret));
        } else if (FALSE_IT(task->set_generate_time(tmp_task->get_generate_time()))) {
          // false it, shall never be here
        }
      }
    }
  }
  return ret;
}

int64_t ObRebalanceTaskMgr::get_schedule_interval() const
{
  return 1000LL * 1000LL;  // 1s
}

}  // end namespace rootserver
}  // end namespace oceanbase
