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

#include "ob_rs_gts_task_mgr.h"

#include "lib/lock/ob_mutex.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/wait_event/ob_wait_event.h"
#include "share/ob_srv_rpc_proxy.h"
#include "ob_rs_event_history_table_operator.h"
#include "ob_unit_manager.h"
#include "ob_server_manager.h"
#include "ob_zone_manager.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_server_status.h"

namespace oceanbase {
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace share;

namespace rootserver {

int ObRsGtsTaskQueue::init(const int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(bucket_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(task_map_.create(bucket_num, ObModIds::OB_RS_GTS_MANAGER))) {
    LOG_WARN("fail to init task map", K(ret));
  } else if (OB_FAIL(task_alloc_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE))) {
    LOG_WARN("fail to init task allocator", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObRsGtsTaskQueue::push_task(const ObGtsReplicaTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else {
    const ObGtsReplicaTaskKey& gts_task_key = task->get_task_key();
    ObGtsReplicaTask* this_task = nullptr;
    int tmp_ret = task_map_.get_refactored(gts_task_key, this_task);
    if (OB_SUCCESS == tmp_ret) {
      ret = OB_ENTRY_EXIST;
      LOG_INFO("task from the same gts instance already exist", K(gts_task_key));
    } else if (OB_HASH_NOT_EXIST == tmp_ret) {
      // good, task of this key not exist
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to check task exist", K(ret), K(tmp_ret));
    }
    if (OB_SUCC(ret)) {
      void* raw_ptr = nullptr;
      ObGtsReplicaTask* new_task = nullptr;
      const int64_t task_deep_copy_size = task->get_deep_copy_size();
      if (nullptr == (raw_ptr = task_alloc_.alloc(task_deep_copy_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (OB_FAIL(task->clone_new(raw_ptr, new_task))) {
        LOG_WARN("fail to clone new", K(ret));
      } else if (OB_UNLIKELY(nullptr == new_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new task ptr is null", K(ret));
      } else if (!wait_list_.add_last(new_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to add task to wait list", K(ret));
      } else {
        const ObGtsReplicaTaskKey& task_key = new_task->get_task_key();
        if (OB_FAIL(task_map_.set_refactored(task_key, new_task))) {
          LOG_WARN("fail to set refactored", K(ret), K(task_key));
        }
        if (OB_FAIL(ret)) {
          wait_list_.remove(new_task);
        }
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("add gts task succeed", "task", *task);
      } else if (nullptr != new_task) {  // need to free to avoid memory leak
        new_task->~ObGtsReplicaTask();
        task_alloc_.free(new_task);
        new_task = nullptr;
        raw_ptr = nullptr;
      } else if (nullptr != raw_ptr) {
        task_alloc_.free(raw_ptr);
        raw_ptr = nullptr;
      }
    }
  }
  return ret;
}

int ObRsGtsTaskQueue::pop_task(ObGtsReplicaTask*& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    task = wait_list_.remove_first();
    task_map_.erase_refactored(task->get_task_key());
  }
  return ret;
}

int ObRsGtsTaskQueue::get_task_cnt(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    task_cnt = task_map_.size();
  }
  return ret;
}

int ObRsGtsTaskMgr::init(obrpc::ObSrvRpcProxy* rpc_proxy, common::ObMySQLProxy* sql_proxy)
{
  int ret = OB_SUCCESS;
  static const int64_t rs_gts_task_mgr_thread_cnt = 1;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == rpc_proxy || nullptr == sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(rpc_proxy), KP(sql_proxy));
  } else if (OB_FAIL(rs_gts_task_queue_.init(TASK_QUEUE_LIMIT))) {
    LOG_WARN("fail to init rs gts task queue", K(ret));
  } else if (OB_FAIL(gts_table_operator_.init(sql_proxy))) {
    LOG_WARN("fail to init gts table operator", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::RS_GTS_TASK_MGR_COND_WAIT))) {
    LOG_WARN("fail to init cond", K(ret));
  } else if (OB_FAIL(create(rs_gts_task_mgr_thread_cnt, "RSGTSTaskMgr"))) {
    LOG_WARN("fail to create rs gts task mgr thread", K(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    sql_proxy_ = sql_proxy;
    inited_ = true;
  }
  return ret;
}

int ObRsGtsTaskMgr::push_task(const ObGtsReplicaTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(rs_gts_task_queue_.push_task(task))) {
      LOG_WARN("fail to push task", K(ret));
    } else {
      cond_.broadcast();
    }
  }
  return ret;
}

void ObRsGtsTaskMgr::stop()
{
  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("not init");
  } else {
    ObRsReentrantThread::stop();
    ObThreadCondGuard guard(cond_);
    cond_.broadcast();
  }
}

int ObRsGtsTaskMgr::pop_task(ObGtsReplicaTask*& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(rs_gts_task_queue_.pop_task(task))) {
    LOG_WARN("fail to pop task", K(ret));
  }
  return ret;
}

int ObRsGtsTaskMgr::get_task_cnt(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(rs_gts_task_queue_.get_task_cnt(task_cnt))) {
    LOG_WARN("fail to get task cnt", K(ret));
  }
  return ret;
}

void ObRsGtsTaskMgr::run3()
{
  int ret = OB_SUCCESS;
  LOG_INFO("rs gts task mgr start");
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (!stop_) {
      ObThreadCondGuard guard(cond_);
      // simple implementation here, no concurrency control is performed,
      // all tasks are executed directly
      int64_t task_cnt = 0;
      ObGtsReplicaTask* task = nullptr;
      if (OB_FAIL(get_task_cnt(task_cnt))) {
        LOG_WARN("fail to get task cnt", K(ret));
      } else if (task_cnt <= 0) {
        const int64_t wait_time_ms = 1000;  // 1 second
        cond_.wait(wait_time_ms);
      } else if (OB_FAIL(pop_task(task))) {
        LOG_WARN("fail to pop task", K(ret));
      } else if (OB_UNLIKELY(nullptr == task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task ptr is null", K(ret));
      } else {
        bool can_execute = false;
        ret = task->check_before_execute(server_mgr_, unit_mgr_, gts_table_operator_, can_execute);
        if (OB_SUCC(ret) && can_execute) {
          const ObGtsReplicaTaskKey& task_key = task->get_task_key();
          ret = task->execute(server_mgr_, gts_table_operator_, *rpc_proxy_);
          if (OB_SUCC(ret)) {
            LOG_INFO("execute gts replica task succ", K(ret), K(task_key));
          } else {
            LOG_WARN("execute gts replica task failed", K(ret), K(task_key));
          }
        }
        if (nullptr != task) {
          task->~ObGtsReplicaTask();
          rs_gts_task_queue_.get_task_allocator().free(task);
          task = nullptr;
        }
      }
    }
  }
}
}  // end namespace rootserver
}  // end namespace oceanbase
