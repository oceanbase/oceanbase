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

#ifndef __OB_SQL_ENGINE_PX_WORKER_STAT_H__
#define __OB_SQL_ENGINE_PX_WORKER_STAT_H__

#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_array.h"
#include "lib/profile/ob_trace_id.h"

namespace oceanbase
{
namespace sql
{

class ObPxWorkerStat : public common::ObDLinkBase<ObPxWorkerStat>
{
public:
  typedef common::ObCurTraceId::TraceId TraceId;
  ObPxWorkerStat();
  ~ObPxWorkerStat();
  int init(uint64_t session_id, uint64_t tenant_id, const TraceId &trace_id, uint64_t qc_id,
      int64_t sqc_id, int64_t worker_id, int64_t dfo_id, int64_t start_time, int64_t thread_id);
  inline void set_session_id(uint64_t session_id) { session_id_ = session_id; }
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_trace_id(const TraceId& trace_id) { trace_id_ = trace_id; }
  inline void set_qc_id(uint64_t qc_id) { qc_id_ = qc_id; }
  inline void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
  inline void set_worker_id(int64_t worker_id) { worker_id_ = worker_id; }
  inline void set_start_time(int64_t start_time) { start_time_ = start_time; }
  inline void set_dfo_id(int64_t dfo_id) { dfo_id_ = dfo_id; }
  inline uint64_t get_session_id() const { return session_id_; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const TraceId& get_trace_id() const { return trace_id_; }
  inline uint64_t get_qc_id() const { return qc_id_; }
  inline int64_t get_sqc_id() const { return sqc_id_; }
  inline int64_t get_worker_id() const { return worker_id_; }
  inline int64_t get_dfo_id() const { return dfo_id_; }
  inline int64_t get_start_time() const { return start_time_; }
  inline int64_t get_thread_id() const { return thread_id_; }
private:
  uint64_t session_id_;
  uint64_t tenant_id_;
  TraceId trace_id_;
  uint64_t qc_id_;
  int64_t sqc_id_;
  int64_t worker_id_;
  int64_t dfo_id_;
  int64_t start_time_;
  int64_t thread_id_;
};

class ObPxWorkerStatList 
{
public:
  static ObPxWorkerStatList &instance(); 
  int push(ObPxWorkerStat &stat_value);
  int remove(ObPxWorkerStat &stat_value);
  // param[in] tenant_id  if tenant is sys, list all tenant stat, else list target tenant stat
  int list_to_array(common::ObArray<ObPxWorkerStat> &stat_array, const uint64_t tenant_id);
private:
  common::ObDList<ObPxWorkerStat> worker_stat_list_;
  common::ObArenaAllocator allocator_;
  mutable common::ObSpinLock lock_;
private:
  ObPxWorkerStatList();                           
  ~ObPxWorkerStatList();
  DISALLOW_COPY_AND_ASSIGN(ObPxWorkerStatList);                         
};

}
}
#endif

