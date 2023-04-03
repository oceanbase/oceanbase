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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/px/ob_px_worker_stat.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObPxWorkerStat::ObPxWorkerStat() : session_id_(0), tenant_id_(0), trace_id_(), qc_id_(0), sqc_id_(0), 
    worker_id_(0), dfo_id_(0), start_time_(0), thread_id_(0)
{
}
ObPxWorkerStat::~ObPxWorkerStat()
{
}

int ObPxWorkerStat::init(uint64_t session_id, uint64_t tenant_id, const TraceId& trace_id,
    uint64_t qc_id, int64_t sqc_id, int64_t worker_id, int64_t dfo_id, int64_t start_time, int64_t thread_id)
{
  int ret = OB_SUCCESS;
  session_id_ = session_id;
  tenant_id_ = tenant_id;
  trace_id_ = trace_id;
  qc_id_ = qc_id;
  sqc_id_ = sqc_id;
  worker_id_ = worker_id;
  dfo_id_ = dfo_id;
  start_time_ = start_time;
  thread_id_ = thread_id;
  return ret;
}

//------------------------------------------------------
ObPxWorkerStatList::ObPxWorkerStatList() : lock_(common::ObLatchIds::PX_WORKER_STAT_LOCK)
{
}

ObPxWorkerStatList::~ObPxWorkerStatList()
{
}

ObPxWorkerStatList& ObPxWorkerStatList::instance()
{
  static ObPxWorkerStatList the_px_worker_stat_List;
  return the_px_worker_stat_List;
}

int ObPxWorkerStatList::push(ObPxWorkerStat &stat_value)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if(!worker_stat_list_.add_last(&stat_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add stat", K(ret));
  }
  return ret;
}

int ObPxWorkerStatList::remove(ObPxWorkerStat &stat_value)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if(NULL == worker_stat_list_.remove(&stat_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to move stat", K(ret));
  }
  return ret;
}

int ObPxWorkerStatList::list_to_array(ObArray<ObPxWorkerStat> &stat_array,
    const uint64_t target_tenant_id)
{
  int ret = OB_SUCCESS;
  stat_array.reset();
  ObSpinLockGuard guard(lock_);
  stat_array.reserve(worker_stat_list_.get_size());
  DLIST_FOREACH(cur,worker_stat_list_) {
    if (!is_sys_tenant(target_tenant_id) && cur->get_tenant_id() != target_tenant_id) {
      continue;
    }
    // sys tenant list all tenant stat
    // non-sys tennat list self tenant stat
    if(OB_SUCCESS != stat_array.push_back(*cur)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to change stat_list to array", K(ret));
    }
  }
  return ret;
}



