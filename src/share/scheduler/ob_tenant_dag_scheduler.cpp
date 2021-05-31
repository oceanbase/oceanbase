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

#define USING_LOG_PREFIX COMMON
#include "share/ob_force_print_log.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_worker_obj_pool.h"
#include "share/scheduler/ob_dag_type.h"
#include "share/rc/ob_context.h"
#include "observer/omt/ob_tenant.h"
#include <algorithm>

namespace oceanbase {
using namespace lib;
using namespace common;
using namespace omt;
namespace share {

/***************************************ObDagSchedulerNew impl********************************************/
ObDagSchedulerNew::ObDagSchedulerNew() : is_inited_(false), is_running_(false), schedule_period_(0)
{}

ObDagSchedulerNew::~ObDagSchedulerNew()
{
  destroy();
}

ObDagSchedulerNew& ObDagSchedulerNew::get_instance()
{
  static ObDagSchedulerNew scheduler;
  return scheduler;
}
const int64_t ObDagSchedulerNew::DEFAULT_BUCKET_NUM = 9;
int ObDagSchedulerNew::init(const int64_t schedule_period /*= SCHEDULER_PERIOD*/,
    const int64_t total_mem_limit /*= TOTAL_LIMIT*/, const int64_t hold_mem_limit /*= HOLD_LIMIT*/,
    const int64_t page_size /*= PAGE_SIZE*/)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "scheduler init twice", K(ret));
  } else if (0 >= total_mem_limit || 0 >= hold_mem_limit || hold_mem_limit > total_mem_limit || 0 >= page_size) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN,
        "init ObDagSchedulerNew with invalid arguments",
        K(ret),
        K(total_mem_limit),
        K(hold_mem_limit),
        K(page_size));
  } else if (OB_FAIL(tenant_thread_pool_map_.create(DEFAULT_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_DAG_MAP))) {
    COMMON_LOG(WARN, "failed to create tenant_thread_pool_map_", K(ret), K(DEFAULT_BUCKET_NUM));
  } else if (OB_FAIL(scheduler_sync_.init(ObWaitEventIds::SCHEDULER_COND_WAIT))) {
    COMMON_LOG(WARN, "failed to init task queue sync", K(ret));
  } else {
    schedule_period_ = schedule_period;
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
    COMMON_LOG(WARN, "failed to init ObDagSchedulerNew", K(ret));
  } else {
    COMMON_LOG(INFO, "ObDagSchedulerNew is inited");
  }
  return ret;
}

void ObDagSchedulerNew::destroy()
{
  if (is_inited_) {
    is_running_ = false;
    stop();
    COMMON_LOG(INFO, "ObDagSchedulerNew starts to destroy");
    notify_();
    wait();
    if (tenant_thread_pool_map_.created()) {
      for (TenantThreadPoolIterator iter = tenant_thread_pool_map_.begin(); iter != tenant_thread_pool_map_.end();
           iter++) {
        (*iter).second->set_stop();
        (*iter).second->destroy();
        ob_delete((*iter).second);
      }
      tenant_thread_pool_map_.destroy();
    }
    scheduler_sync_.destroy();
    is_inited_ = false;
    COMMON_LOG(INFO, "ObDagSchedulerNew destroyed");
  }
}

int ObDagSchedulerNew::add_tenant_thread_pool(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObTenantThreadPool* thread_pool = OB_NEW(ObTenantThreadPool, ObModIds::OB_SCHEDULER, tenant_id);
  ObThreadCondGuard guard(scheduler_sync_);
  if (OB_ISNULL(thread_pool)) {  // allocate failed
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_SUCCESS != (hash_ret = tenant_thread_pool_map_.set_refactored(tenant_id, thread_pool))) {
    if (OB_HASH_EXIST == hash_ret) {
      ret = OB_ENTRY_EXIST;
    } else {
      ret = hash_ret;
      COMMON_LOG(WARN, "failed to add tenant thread pool", K(ret), K(*thread_pool));
    }
  } else {
    if (OB_FAIL(thread_pool->init())) {
      COMMON_LOG(WARN, "tenant thread pool init failed", K(ret));
    } else {
      thread_pool->set_last_schedule_time(ObTimeUtility::current_time());
    }
  }
  if (OB_SUCCESS != ret && OB_NOT_NULL(thread_pool)) {
    ob_delete(thread_pool);  // delete
    thread_pool = NULL;
  }
  return ret;
}

int ObDagSchedulerNew::del_tenant_thread_pool(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantThreadPool* tenant_thread_pool = NULL;
  ObThreadCondGuard guard(scheduler_sync_);
  if (OB_FAIL(get_tenant_thread_pool_by_id_(tenant_id, tenant_thread_pool))) {
    COMMON_LOG(WARN, "failed to get tenant thread pool by id", K(ret), K(tenant_id));
  } else if (tenant_thread_pool->has_set_stop()) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(WARN, "tenant thread pool was deleted", K(ret), K(tenant_id));
  } else {
    tenant_thread_pool->set_stop();
    COMMON_LOG(INFO, "delete tenant thread pool success", K(tenant_id));
  }
  return ret;
}

// call this func with lock
int ObDagSchedulerNew::get_tenant_thread_pool_by_id_(const int64_t tenant_id, ObTenantThreadPool*& tenant_thread_pool)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  tenant_thread_pool = NULL;
  if (OB_SUCCESS != (hash_ret = tenant_thread_pool_map_.get_refactored(tenant_id, tenant_thread_pool)) ||
      NULL == tenant_thread_pool) {
    if (OB_HASH_NOT_EXIST == hash_ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    COMMON_LOG(WARN, "failed to get tenant thread pool by id", K(ret), K(tenant_id));
  }
  return ret;
}

int ObDagSchedulerNew::set_tenant_setting(common::ObIArray<ObTenantSetting>& setting_list)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(scheduler_sync_);
  ObTenantSetting* tenant_setting = NULL;
  ObTenantThreadPool* tenant_thread_pool = NULL;
  for (int i = 0; i < setting_list.count(); ++i) {
    tenant_setting = &(setting_list.at(i));
    if (OB_FAIL(get_tenant_thread_pool_by_id_(tenant_setting->tenant_id_, tenant_thread_pool))) {
      COMMON_LOG(WARN, "failed to get tenant thread pool by id", K(ret), K(tenant_setting->tenant_id_));
    } else if (tenant_thread_pool->has_set_stop()) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(WARN, "tenant thread pool is deleted", K(ret), K(tenant_setting->tenant_id_));
    } else if (!OB_ISNULL(tenant_thread_pool) && OB_SUCC(ret) &&
               OB_FAIL(tenant_thread_pool->set_tenant_setting(*tenant_setting))) {
      COMMON_LOG(WARN, "set tenant type setting failed", K(ret));
    }
  }
  return ret;
}

int ObDagSchedulerNew::get_tenant_settings(common::ObIArray<ObTenantSetting>& setting_list)
{
  int ret = OB_SUCCESS;
  ObTenantThreadPool* thread_pool = NULL;
  ObThreadCondGuard guard(scheduler_sync_);
  for (TenantThreadPoolIterator iter = tenant_thread_pool_map_.begin(); iter != tenant_thread_pool_map_.end(); iter++) {
    thread_pool = (*iter).second;
    if (OB_ISNULL(thread_pool)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "tenant thread pool is NULL");
    } else {
      ObTenantSetting tenant_setting;
      if (OB_FAIL(thread_pool->get_tenant_setting(tenant_setting))) {
        COMMON_LOG(WARN, "get tenant thread pool setting failed", "tenant_id", (*iter).first);
      } else {
        setting_list.push_back(tenant_setting);
      }
    }
  }
  return ret;
}

int ObDagSchedulerNew::get_tenant_setting_by_id(int64_t tenant_id, ObTenantSetting& tenant_setting)
{
  int ret = OB_SUCCESS;
  ObTenantThreadPool* thread_pool = NULL;
  ObThreadCondGuard guard(scheduler_sync_);
  if (OB_FAIL(get_tenant_thread_pool_by_id_(tenant_id, thread_pool))) {
    COMMON_LOG(WARN, "failed to get tenant thread pool", K(tenant_id));
  } else if (OB_FAIL(thread_pool->get_tenant_setting(tenant_setting))) {
    COMMON_LOG(WARN, "failed to get tenant setting", K(tenant_id));
  }
  return ret;
}

int ObDagSchedulerNew::add_dag_in_tenant(int64_t tenant_id, ObIDagNew* dag)
{
  int ret = OB_SUCCESS;
  ObTenantThreadPool* tenant_thread_pool = NULL;
  ObThreadCondGuard guard(scheduler_sync_);
  if (OB_FAIL(get_tenant_thread_pool_by_id_(tenant_id, tenant_thread_pool))) {
    COMMON_LOG(WARN, "failed to get tenant thread pool", K(ret), K(tenant_id), K(dag));
  } else {
    if (tenant_thread_pool->has_set_stop()) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(WARN, "tenant thread is deleted", K(ret), K(tenant_id));
    } else if (OB_FAIL(tenant_thread_pool->add_dag(dag))) {
      COMMON_LOG(WARN, "failed to add dag into tenant thread pool", K(ret), K(this), K(tenant_id), K(dag));
    } else {  // determine if need to wake up scheduler
      COMMON_LOG(INFO, "add dag into tenant thread pool success", K(tenant_id));
      if (tenant_thread_pool->need_schedule(schedule_period_)) {
        notify_();
      }
    }
  }
  return ret;
}

void ObDagSchedulerNew::run1()
{
  while (!has_set_stop()) {
    ObThreadCondGuard guard(scheduler_sync_);
    if (!has_set_stop()) {
      schedule_();  // call schedule func
      scheduler_sync_.wait(schedule_period_);
    }
  }
  COMMON_LOG(INFO, "schduler quit");
}

int ObDagSchedulerNew::start_run()
{
  int ret = OB_SUCCESS;
  {
    ObThreadCondGuard guard(scheduler_sync_);
    if (!is_running_) {
      if (OB_FAIL(start())) {
        COMMON_LOG(WARN, "dag scheduler start failed", K(ret));
      } else {
        is_running_ = true;
        COMMON_LOG(INFO, "dag scheduler start success", K(ret));
      }
    }
  }
  return ret;
}

void ObDagSchedulerNew::notify_()
{
  scheduler_sync_.signal();
}

int64_t ObDagSchedulerNew::get_dag_count_in_tenant(int64_t tenant_id, int64_t type_id)
{
  int64_t dag_cnt = -1;
  ObTenantThreadPool* tenant_thread_pool = NULL;
  int ret = OB_SUCCESS;
  {
    ObThreadCondGuard guard(scheduler_sync_);
    if (type_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid type id", K(ret), K(type_id));
    } else if (OB_FAIL(get_tenant_thread_pool_by_id_(tenant_id, tenant_thread_pool))) {
      COMMON_LOG(WARN, "failed to get tenant thread pool by id", K(ret), K(tenant_id), K(type_id));
    } else {
      if (OB_FAIL(tenant_thread_pool->get_dag_count(type_id, dag_cnt))) {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }  // end of lock
  return dag_cnt;
}

int ObDagSchedulerNew::schedule_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObWorkerObjPool::get_instance().adjust_worker_cnt())) {
    COMMON_LOG(WARN, "adjust worker obj failed", K(ret));
  }
  for (TenantThreadPoolIterator iter = tenant_thread_pool_map_.begin(); iter != tenant_thread_pool_map_.end(); iter++) {
    (*iter).second->dump_dag_status();  // log infomation

    if ((*iter).second->has_set_stop() && (*iter).second->is_empty()) {  // all task finished
      (*iter).second->destroy();                                         // destroy tenant thread pool
    } else if (OB_FAIL((*iter).second->schedule(schedule_period_))) {
      COMMON_LOG(WARN, "failed to schedule", K(ret), K(*iter));
    } else {
      COMMON_LOG(INFO, "schedule finish", K(ret), K(*iter));
    }
  }
  return ret;
}

bool ObDagSchedulerNew::is_empty()
{
  bool bret = true;
  ObThreadCondGuard guard(scheduler_sync_);
  for (TenantThreadPoolIterator iter = tenant_thread_pool_map_.begin(); iter != tenant_thread_pool_map_.end(); iter++) {
    if (!(*iter).second->is_empty()) {  // one tenant is not empty
      bret = false;
      break;
    }
  }
  return bret;
}

#ifndef NDEBUG
void ObDagSchedulerNew::set_sche_info(int64_t tenant_id, GetScheduleInfo* info)
{
  ObTenantThreadPool* thread_pool = NULL;
  if (OB_SUCCESS != get_tenant_thread_pool_by_id_(tenant_id, thread_pool)) {
    COMMON_LOG(WARN, "failed to set_sche_info", K(tenant_id));
  } else {
    thread_pool->set_sche_info(info);
  }
}
GetScheduleInfo::GetScheduleInfo()
    : choose_type_id_list_(NULL, ObModIds::OB_SCHEDULER), running_tasks_cnt_(NULL, ObModIds::OB_SCHEDULER)
{}
GetScheduleInfo::~GetScheduleInfo()
{
  destroy();
}
void GetScheduleInfo::destroy()
{
  choose_type_id_list_.clear();
  running_tasks_cnt_.clear();
}
#endif

}  // namespace share
}  // namespace oceanbase
