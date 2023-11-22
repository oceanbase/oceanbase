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

#define USING_LOG_PREFIX SQL_OPT
#include "share/stat/ob_opt_stat_gather_stat.h"
#include "lib/string/ob_sql_string.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObOptStatTaskInfo::init(common::ObIAllocator &allocator,
                            sql::ObSQLSessionInfo *session,
                            ObString &task_id,
                            ObOptStatGatherType type,
                            uint64_t task_start_time,
                            int64_t task_table_cnt)
{
  int ret = OB_SUCCESS;
  char *trace_id_buf = NULL;
  const int32_t max_trace_id_len = 64;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(session));
  } else if (OB_ISNULL(trace_id_buf = static_cast<char*>(allocator.alloc(max_trace_id_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(trace_id_buf));
  } else {
    tenant_id_ = session->get_effective_tenant_id();
    session_id_ = session->get_sessid();
    int64_t len = session->get_current_trace_id().to_string(trace_id_buf, max_trace_id_len);
    trace_id_.assign_ptr(trace_id_buf, static_cast<int32_t>(len));
    task_id_ = task_id;
    type_ = type;
    task_start_time_ = task_start_time;
    task_table_count_ = task_table_cnt;
    session_ = session;
  }
  return ret;
}

int ObOptStatTaskInfo::deep_copy(ObOptStatTaskInfo &other, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos + other.size() > buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else {
    session_id_ = other.session_id_;
    MEMCPY(buf + pos, other.trace_id_.ptr(), other.trace_id_.length());
    trace_id_.assign_ptr(buf + pos, other.trace_id_.length());
    pos += other.trace_id_.length();
    tenant_id_ = other.tenant_id_;
    MEMCPY(buf + pos, other.task_id_.ptr(), other.task_id_.length());
    task_id_.assign_ptr(buf + pos, other.task_id_.length());
    pos += other.task_id_.length();
    type_ = other.type_;
    task_table_count_ = other.task_table_count_;
    task_start_time_ = other.task_start_time_;
    task_end_time_ = other.task_end_time_;
    ret_code_ = other.ret_code_;
    failed_count_ = other.failed_count_;
    completed_table_count_ = other.completed_table_count_;
  }
  return ret;
}

ObOptStatGatherStat::ObOptStatGatherStat() :
  task_info_(),
  database_name_(),
  table_id_(0),
  table_name_(),
  ret_code_(0),
  start_time_(0),
  end_time_(0),
  memory_used_(0),
  stat_refresh_failed_list_(),
  properties_(),
  table_gather_progress_()
{
}

ObOptStatGatherStat::ObOptStatGatherStat(ObOptStatTaskInfo &task_info) :
  task_info_(task_info),
  database_name_(),
  table_id_(0),
  table_name_(),
  ret_code_(0),
  start_time_(0),
  end_time_(0),
  memory_used_(0),
  stat_refresh_failed_list_(),
  properties_(),
  table_gather_progress_()
{
}

ObOptStatGatherStat::~ObOptStatGatherStat()
{
}

int ObOptStatGatherStat::assign(const ObOptStatGatherStat &other)
{
  int ret = OB_SUCCESS;
  task_info_ = other.task_info_;
  database_name_ = other.database_name_;
  table_id_ = other.table_id_;
  table_name_ = other.table_name_;
  ret_code_ = other.ret_code_;
  start_time_ = other.start_time_;
  end_time_ = other.end_time_;
  memory_used_ = other.memory_used_;
  stat_refresh_failed_list_ = other.stat_refresh_failed_list_;
  properties_ = other.properties_;
  table_gather_progress_ = other.table_gather_progress_;
  return ret;
}

int64_t ObOptStatGatherStat::size() const
{
  int64_t base_size = sizeof(ObOptStatGatherStat);
  base_size += task_info_.size();
  base_size += database_name_.length();
  base_size += table_name_.length();
  base_size += stat_refresh_failed_list_.length();
  base_size += properties_.length();
  base_size += table_gather_progress_.length();
  return base_size;
}

int ObOptStatGatherStat::deep_copy(common::ObIAllocator &allocator, ObOptStatGatherStat *&new_stat)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = size();
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(buf_len));
  } else {
    new_stat = new (buf) ObOptStatGatherStat();
    int64_t pos = sizeof(*this);
    //deep copy task info
    if (OB_FAIL(new_stat->task_info_.deep_copy(task_info_, buf, buf_len, pos))) {
      LOG_WARN("failed to deep copy", K(ret));
    } else {
      //set database_name_
      MEMCPY(buf + pos, database_name_.ptr(), database_name_.length());
      new_stat->set_database_name(buf + pos, database_name_.length());
      pos += database_name_.length();
      //set table_id_
      new_stat->set_table_id(table_id_);
      //set table_name_
      MEMCPY(buf + pos, table_name_.ptr(), table_name_.length());
      new_stat->set_table_name(buf + pos, table_name_.length());
      pos += table_name_.length();
      //set ret_code_
      new_stat->set_ret_code(ret_code_);
      //set start_time_
      new_stat->set_start_time(start_time_);
      //set end_time_
      new_stat->set_end_time(end_time_);
      //set memory_used_
      new_stat->set_memory_used(memory_used_);
      //set stat_refresh_failed_list_
      MEMCPY(buf + pos, stat_refresh_failed_list_.ptr(), stat_refresh_failed_list_.length());
      new_stat->set_stat_refresh_failed_list(buf + pos, stat_refresh_failed_list_.length());
      pos += stat_refresh_failed_list_.length();
      //set properties_
      MEMCPY(buf + pos, properties_.ptr(), properties_.length());
      new_stat->set_properties(buf + pos, properties_.length());
      pos += properties_.length();
      //set table_gather_progress_
      MEMCPY(buf + pos, table_gather_progress_.ptr(), table_gather_progress_.length());
      new_stat->set_table_gather_progress(buf + pos, table_gather_progress_.length());
      pos += table_gather_progress_.length();
      if (OB_UNLIKELY(pos != buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(pos), K(buf_len));
      }
    }
  }
  return ret;
}
//-----------------------------------------------------
int ObOptStatRunningMonitor::add_table_info(common::ObTableStatParam &table_param,
                                            double stale_percent)
{
  int ret = OB_SUCCESS;
  ObString tmp_db_name;
  ObString tmp_tab_name;
  ObString tmp_properties_str;
  if (OB_FAIL(ob_write_string(allocator_,
                              table_param.db_name_,
                              tmp_db_name))) {
    LOG_WARN("failed to write string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_,
                                     table_param.tab_name_,
                                     tmp_tab_name))) {
    LOG_WARN("failed to write string", K(ret));
  } else {
    opt_stat_gather_stat_.set_table_id(table_param.table_id_);
    ObSqlString properties_sql_str;
    char *buf = NULL;
    if (OB_FAIL(properties_sql_str.append_fmt("GRANULARITY:%.*s;METHOD_OPT:%.*s;DEGREE:%ld;ESTIMATE_PERCENT:%lf;BLOCK_SAMPLE:%d;STALE_PERCENT:%lf;",
                                              table_param.granularity_.length(),
                                              table_param.granularity_.ptr(),
                                              table_param.method_opt_.length(),
                                              table_param.method_opt_.ptr(),
                                              table_param.degree_,
                                              table_param.sample_info_.is_sample_ ? table_param.sample_info_.sample_value_ : 100.0,
                                              table_param.sample_info_.is_block_sample_,
                                              stale_percent))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(properties_sql_str.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(properties_sql_str));
    } else {
      MEMCPY(buf, properties_sql_str.ptr(), properties_sql_str.length());
      tmp_properties_str.assign_ptr(buf, static_cast<int32_t>(properties_sql_str.length()));
      ObOptStatGatherStatList::instance().update_gather_stat_info(tmp_db_name, tmp_tab_name,
                                                                  tmp_properties_str, opt_stat_gather_stat_);
    }
  }
  return ret;
}

int ObOptStatRunningMonitor::add_monitor_info(ObOptStatRunningPhase current_phase,
                                              double extra_progress_ratio/*default 0*/)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_str;
  if (OB_FAIL(tmp_str.append_fmt("%.2f%%(%s)", running_progress_ratio[current_phase] + extra_progress_ratio,
                                              running_phase_name[current_phase]))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(tmp_str.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(buf));
    } else {
      ObString tmp_tab_gather_progress;
      MEMCPY(buf , tmp_str.ptr(), tmp_str.length());
      tmp_tab_gather_progress.assign_ptr(buf, tmp_str.length());
      ObOptStatGatherStatList::instance().update_table_gather_progress(tmp_tab_gather_progress,
                                                                       opt_stat_gather_stat_);
    }
  }
  return ret;
}

double ObOptStatRunningMonitor::get_monitor_extra_progress_ratio(ObOptStatRunningPhase current_phase,
                                                                 ObOptStatRunningPhase next_phase,
                                                                 int64_t total_split_cnt)
{
  double res = 0.0;
  if (total_split_cnt > 0) {
    res = (running_progress_ratio[next_phase] - running_progress_ratio[current_phase]) * 1.0 / total_split_cnt;
  }
  return res > 0.0 ? res : 0.0;
}

void ObOptStatRunningMonitor::set_monitor_result(int ret_code,
                                                 int64_t end_time,
                                                 int64_t current_memory_used)
{
  opt_stat_gather_stat_.set_ret_code(ret_code);
  opt_stat_gather_stat_.set_end_time(end_time);
  opt_stat_gather_stat_.set_memory_used(current_memory_used - last_memory_used_);
}

//------------------------------------------------------
ObOptStatGatherStatList::ObOptStatGatherStatList() : lock_(common::ObLatchIds::OPT_STAT_GATHER_STAT_LOCK)
{
}

ObOptStatGatherStatList::~ObOptStatGatherStatList()
{
}

ObOptStatGatherStatList& ObOptStatGatherStatList::instance()
{
  static ObOptStatGatherStatList the_opt_stat_gather_stat_List;
  return the_opt_stat_gather_stat_List;
}

int ObOptStatGatherStatList::push(ObOptStatGatherStat &stat_value)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if(!stat_list_.add_last(&stat_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add stat", K(ret));
  }
  return ret;
}

int ObOptStatGatherStatList::remove(ObOptStatGatherStat &stat_value)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if(NULL == stat_list_.remove(&stat_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to move stat", K(ret));
  }
  return ret;
}

void ObOptStatGatherStatList::update_gather_stat_info(ObString &db_name,
                                                      ObString &tab_name,
                                                      ObString &properties,
                                                      ObOptStatGatherStat &stat_value)
{
  ObSpinLockGuard guard(lock_);
  stat_value.set_database_name(db_name.ptr(), db_name.length());
  stat_value.set_table_name(tab_name.ptr(), tab_name.length());
  stat_value.set_properties(properties.ptr(), properties.length());
}

void ObOptStatGatherStatList::update_table_gather_progress(ObString &table_gather_progress,
                                                           ObOptStatGatherStat &stat_value)
{
  ObSpinLockGuard guard(lock_);
  stat_value.set_table_gather_progress(table_gather_progress.ptr(), table_gather_progress.length());
}

void ObOptStatGatherStatList::update_gather_stat_refresh_failed_list(ObString &failed_list,
                                                                     ObOptStatGatherStat &stat_value)
{
  ObSpinLockGuard guard(lock_);
  stat_value.set_stat_refresh_failed_list(failed_list.ptr(), failed_list.length());
}

int ObOptStatGatherStatList::list_to_array(common::ObIAllocator &allocator,
                                           const uint64_t target_tenant_id,
                                           ObIArray<ObOptStatGatherStat> &stat_array)
{
  int ret = OB_SUCCESS;
  stat_array.reset();
  ObSpinLockGuard guard(lock_);
  stat_array.reserve(stat_list_.get_size());
  DLIST_FOREACH(cur, stat_list_) {
    // sys tenant list all tenant stat
    // non-sys tennat list self tenant stat
    ObOptStatGatherStat *tmp_stat = NULL;
    if (!is_sys_tenant(target_tenant_id) && cur->get_tenant_id() != target_tenant_id) {
      //do nothing
    } else if (cur->deep_copy(allocator, tmp_stat)) {
      LOG_WARN("failed to deep copy", K(ret));
    } else if (OB_ISNULL(tmp_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(tmp_stat));
    } else if (OB_FAIL(stat_array.push_back(*tmp_stat))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObOptStatGatherStatList::cancel_gather_stats(const uint64_t tenant_id,
                                                 const ObString &task_id)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  bool is_cancel = false;
  DLIST_FOREACH(cur, stat_list_) {
    // sys tenant list all tenant stat
    // non-sys tennat list self tenant stat
    ObOptStatGatherStat *tmp_stat = NULL;
    if (!is_sys_tenant(tenant_id) && cur->get_tenant_id() != tenant_id) {
      //do nothing
    } else if (0 != cur->get_task_id().case_compare(task_id) || OB_ISNULL(cur->get_session())) {
      //do nothing
    } else if (OB_FAIL(sql::ObSQLSessionMgr::kill_query(*cur->get_session(), ObSQLSessionState::QUERY_KILLED))) {
      LOG_WARN("kill query failed", K(ret));
    } else {
      is_cancel = true;
    }
  }
  if (OB_SUCC(ret) && !is_cancel) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("The optimizer stats gather task has ended or the task doesn't exist", K(ret));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "The optimizer stats gather task has ended or the task doesn't exist");
  }
  return ret;
}