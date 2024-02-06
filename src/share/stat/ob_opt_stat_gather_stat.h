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

#ifndef __OB_OPT_STAT_GATHER_STAT_H__
#define __OB_OPT_STAT_GATHER_STAT_H__

#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_array.h"
#include "lib/profile/ob_trace_id.h"
#include "share/stat/ob_stat_define.h"

namespace oceanbase
{
namespace common
{
enum ObOptStatGatherType {
  INVALID_GATHER_TYPE = -1,
  MANUAL_GATHER,
  AUTO_GATHER
};

// enum ObOptStatRunningPhase {
//   GATHER_BEGIN = 0,
//   GATHER_SUBPART_STATS,
//   GATHER_PART_STATS,
//   DERIVE_PART_STATS,
//   GATHER_GLOBAL_STATS,
//   DERIVE_GLOBAL_STATS,
//   WRITE_STATS,
//   GATHER_INDEX_STATS,
//   GATHER_END
// };

// static const char *running_phase_name[ObOptStatRunningPhase::GATHER_END + 1] =
// {
//   "GATHER BEGIN",
//   "GATHER SUBPART STATS",
//   "GATHER PART STATS",
//   "DERIVE PART STATS",
//   "GATHER GLOBAL STATS",
//   "DERIVE GLOBAL STATS",
//   "WRITE STATS",
//   "GATHER INDEX STATS",
//   "GATHER END"
// };

struct ObOptStatTaskInfo
{
  ObOptStatTaskInfo() :
    tenant_id_(0),
    session_id_(0),
    trace_id_(),
    task_id_(),
    type_(ObOptStatGatherType::INVALID_GATHER_TYPE),
    task_table_count_(0),
    task_start_time_(0),
    task_end_time_(0),
    ret_code_(0),
    failed_count_(0),
    completed_table_count_(0)
  {}
  int64_t size() const { return trace_id_.length() + task_id_.length(); }
  int init(common::ObIAllocator &allocator,
           uint64_t tenant_id,
           uint64_t session_id,
           const common::ObCurTraceId::TraceId &trace_id,
           ObString &task_id,
           ObOptStatGatherType type,
           uint64_t task_start_time,
           int64_t task_table_cnt);
  int deep_copy(ObOptStatTaskInfo &other, char *buf, int64_t buf_len, int64_t &pos);
  TO_STRING_KV(K(tenant_id_),
               K(session_id_),
               K(trace_id_),
               K(task_id_),
               K(type_),
               K(task_table_count_),
               K(task_start_time_),
               K(task_end_time_),
               K(ret_code_),
               K(failed_count_),
               K(completed_table_count_));
  uint64_t tenant_id_;
  uint64_t session_id_;
  ObString trace_id_;
  ObString task_id_;
  int64_t type_;
  int64_t task_table_count_;
  int64_t task_start_time_;
  int64_t task_end_time_;
  int ret_code_;
  int64_t failed_count_;
  int64_t completed_table_count_;
};

class ObOptStatGatherStat : public common::ObDLinkBase<ObOptStatGatherStat>
{
public:
  ObOptStatGatherStat();
  ObOptStatGatherStat(ObOptStatTaskInfo &task_info);
  ~ObOptStatGatherStat();
  bool is_valid() const { return start_time_ > 0; }
  int assign(const ObOptStatGatherStat &other);
  int64_t size() const;
  int deep_copy(common::ObIAllocator &allocator, ObOptStatGatherStat *&new_stat);
  inline uint64_t get_session_id() const { return task_info_.session_id_; }
  void set_session_id(uint64_t session_id) { task_info_.session_id_ = session_id; }
  inline const ObString &get_trace_id() const { return task_info_.trace_id_; }
  inline void set_trace_id(const char *ptr, int32_t len) { task_info_.trace_id_.assign_ptr(ptr, len); }
  inline uint64_t get_tenant_id() const { return task_info_.tenant_id_; }
  void set_tenant_id(uint64_t tenant_id) { task_info_.tenant_id_ = tenant_id; }
  inline const ObString &get_task_id() const { return task_info_.task_id_; }
  inline int64_t get_type() const { return task_info_.type_; }
  inline void set_type(int64_t type) { task_info_.type_ = type; }
  inline int64_t get_task_start_time() const { return task_info_.task_start_time_; }
  inline void set_task_start_time(int64_t task_start_time) { task_info_.task_start_time_ = task_start_time; }
  inline int64_t get_task_duration_time() const { return ObTimeUtility::current_time() - task_info_.task_start_time_; }
  inline int64_t get_task_end_time() const { return task_info_.task_end_time_; }
  inline void set_task_end_time(int64_t task_end_time) { task_info_.task_end_time_ = task_end_time; }
  inline int get_task_ret_code() const { return task_info_.ret_code_; }
  inline void set_task_ret_code(int ret_code) { task_info_.ret_code_ = ret_code; }
  inline int64_t get_task_table_count() const { return task_info_.task_table_count_; }
  inline void set_task_table_count(int64_t task_table_count) { task_info_.task_table_count_ = task_table_count; }
  inline const ObString &get_database_name() const { return database_name_; }
  inline ObString &get_database_name() { return database_name_; }
  inline void set_database_name(const char *ptr, int32_t len) { database_name_.assign_ptr(ptr, len); }
  inline uint64_t get_table_id() const { return table_id_; }
  void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  inline const ObString &get_table_name() const { return table_name_; }
  inline ObString &get_table_name() { return table_name_; }
  inline void set_table_name(const char *ptr, int32_t len) { table_name_.assign_ptr(ptr, len); }
  inline int get_ret_code() const { return ret_code_; }
  inline void set_ret_code(int ret_code) { ret_code_ = ret_code; }
  inline int64_t get_start_time() const { return start_time_; }
  inline void set_start_time(int64_t start_time) { start_time_ = start_time; }
  inline int64_t get_end_time() const { return end_time_; }
  inline void set_end_time(int64_t end_time) { end_time_ = end_time; }
  inline int64_t get_memory_used() const { return memory_used_; }
  inline void set_memory_used(int64_t size) { memory_used_ = size; }
  inline const ObString &get_stat_refresh_failed_list() const { return stat_refresh_failed_list_; }
  inline ObString &get_stat_refresh_failed_list() { return stat_refresh_failed_list_; }
  inline void set_stat_refresh_failed_list(const char *ptr, int32_t len) { stat_refresh_failed_list_.assign_ptr(ptr, len); }
  inline const ObString &get_properties() const { return properties_; }
  inline void set_properties(const char *ptr, int32_t len) { properties_.assign_ptr(ptr, len); }
  inline int64_t get_running_table_duration_time() const { return ObTimeUtility::current_time() - start_time_; }
  inline int64_t get_completed_table_count() { return task_info_.completed_table_count_; }
  TO_STRING_KV(K(task_info_),
               K(database_name_),
               K(table_id_),
               K(table_name_),
               K(ret_code_),
               K(start_time_),
               K(end_time_),
               K(memory_used_),
               K(stat_refresh_failed_list_),
               K(properties_));

private:
  ObOptStatTaskInfo task_info_;
  ObString database_name_;
  uint64_t table_id_;
  ObString table_name_;
  int ret_code_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t memory_used_;
  ObString stat_refresh_failed_list_;
  ObString properties_;
};

struct ObOptStatRunningMonitor
{
  explicit ObOptStatRunningMonitor(common::ObIAllocator &allocator,
                                   int64_t current_time,
                                   int64_t current_memory_used,
                                   ObOptStatGatherStat &opt_stat_gather_stat) :
    allocator_(allocator),
    last_start_time_(current_time),
    last_memory_used_(current_memory_used),
    opt_stat_gather_stat_(opt_stat_gather_stat)
  {
    opt_stat_gather_stat_.set_start_time(current_time);
  }
  ~ObOptStatRunningMonitor(){}
  void init(int64_t current_time,
            int64_t current_memory_used,
            ObOptStatGatherStat &opt_stat_gather_stat);
  int add_table_info(common::ObTableStatParam &table_param,
                     double stale_percent = -1.0);
  void set_monitor_result(int ret_code,
                          int64_t current_time,
                          int64_t current_memory_used);
  TO_STRING_KV(K(last_start_time_),
               K(last_memory_used_),
               K(opt_stat_gather_stat_));
  common::ObIAllocator &allocator_;
  int64_t last_start_time_;
  int64_t last_memory_used_;
  ObOptStatGatherStat &opt_stat_gather_stat_;
};

class ObOptStatGatherStatList
{
public:
  static ObOptStatGatherStatList &instance();
  int push(ObOptStatGatherStat &stat_value);
  int remove(ObOptStatGatherStat &stat_value);
  void update_gather_stat_info(ObString &db_name,
                                ObString &tab_name,
                                ObString &properties,
                                ObOptStatGatherStat &stat_value);
  void update_gather_stat_refresh_failed_list(ObString &failed_list,
                                              ObOptStatGatherStat &stat_value);
  // param[in] tenant_id  if tenant is sys, list all tenant stat, else list target tenant stat
  int list_to_array(common::ObIAllocator &allocator,
                    const uint64_t target_tenant_id,
                    common::ObIArray<ObOptStatGatherStat> &stat_array);
private:
  common::ObDList<ObOptStatGatherStat> stat_list_;
  mutable common::ObSpinLock lock_;
private:
  ObOptStatGatherStatList();
  ~ObOptStatGatherStatList();
  DISALLOW_COPY_AND_ASSIGN(ObOptStatGatherStatList);
};

} // end of namespace common
} // end of namespace oceanbase

#endif /* __OB_OPT_STAT_GATHER_STAT_H__ */
