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

#ifndef OCEANBASE_SHARE_OB_EVENT_HISTORY_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_EVENT_HISTORY_TABLE_OPERATOR_H_

#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/guard/ob_unique_guard.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string_holder.h"
#include "lib/queue/ob_dedup_queue.h"
#include "lib/thread/ob_work_queue.h"
#include "lib/lock/ob_mutex.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_occam_timer.h"
#include "share/ob_task_define.h"
#include <utility>

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
namespace detector
{
class ObDeadLockEventHistoryTableOperator;
}
class ObEventHistoryTableOperator;
class ObEventTableClearTask : public common::ObAsyncTimerTask
{
public:
  ObEventTableClearTask(
    ObEventHistoryTableOperator &rs_event_operator,
    ObEventHistoryTableOperator &server_event_operator,
    ObEventHistoryTableOperator &deadlock_history_operator,
    common::ObWorkQueue &work_queue);
  virtual ~ObEventTableClearTask() {}

  // interface of AsyncTask
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  ObEventHistoryTableOperator &rs_event_operator_;
  ObEventHistoryTableOperator &server_event_operator_;
  ObEventHistoryTableOperator &deadlock_history_operator_;
};

class ObEventHistoryTableOperator
{
public:
  static const int64_t EVENT_TABLE_CLEAR_INTERVAL = 2L * 3600L * 1000L * 1000L; // 2 Hours
  template<bool Truncate, typename T>
  struct ValueConverter {};
  class ObEventTableUpdateTask : public common::IObDedupTask
  {
  public:
    ObEventTableUpdateTask(ObEventHistoryTableOperator &table_operator, const bool is_delete,
        const int64_t create_time);
    virtual ~ObEventTableUpdateTask() {}
    int init(const char *ptr, const int64_t buf_size);
    bool is_valid() const;
    virtual int64_t hash() const;
    virtual bool operator==(const common::IObDedupTask &other) const;
    virtual int64_t get_deep_copy_size() const { return sizeof(*this) + sql_.length(); }
    virtual common::IObDedupTask *deep_copy(char *buf, const int64_t buf_size) const;
    virtual int64_t get_abs_expired_time() const { return 0; }
    virtual int process();
  public:
    void assign_ptr(char *ptr, const int64_t buf_size)
    { sql_.assign_ptr(ptr, static_cast<int32_t>(buf_size));}

    TO_STRING_KV(K_(sql), K_(is_delete), K_(create_time));
  private:
    ObEventHistoryTableOperator &table_operator_;
    common::ObString sql_;
    bool is_delete_;
    int64_t create_time_;

    DISALLOW_COPY_AND_ASSIGN(ObEventTableUpdateTask);
  };

  virtual ~ObEventHistoryTableOperator();
  int init(common::ObMySQLProxy &proxy);
  bool is_inited() const { return inited_; }
  void stop();
  void wait();
  void destroy();
  // number of others should not less than 0, or more than 13
  // if number of others is not 13, should be even, every odd of them are name, every even of them are value
  // If Truncate is true, then too long value will be truncated to fit in the field.
  // Note: Only enable Truncate for rootservice_event_history!
  template <bool Truncate, typename ...Rest>
  int add_event(const char *module, const char *event, Rest &&...others);
  // number of others should not less than 0, or more than 13
  // if number of others is not 13, should be even, every odd of them are name, every even of them are value 
  template <typename ...Rest>
  int sync_add_event(const char *module, const char *event, Rest &&...others);
  // number of others should not less than 0, or more than 13
  // if number of others is not 13, should be even, every odd of them are name, every even of them are value
  template <typename ...Rest>
  int add_event_with_retry(const char *module, const char *event, Rest &&...others);

  virtual int async_delete() = 0;
protected:
  virtual int default_async_delete();
  // recursive begin
  template <int Floor, bool Truncate, typename Name, typename Value, typename ...Rest>
  int add_event_helper_(share::ObDMLSqlSplicer &dml, Name &&name, Value &&value, Rest &&...others);
  // recursive end if there is no extra_info
  template <int Floor, bool Truncate>
  int add_event_helper_(share::ObDMLSqlSplicer &dml);
  // recursive end if there is an extra_info
  template <int Floor, bool Truncate, typename Value>
  int add_event_helper_(share::ObDMLSqlSplicer &dml, Value &&extro_info);
  int add_event_to_timer_(const common::ObSqlString &sql);
  void set_addr(const common::ObAddr self_addr,
                bool is_rs_ev,
                bool is_server_ev)
  {
    self_addr_ = self_addr;
    is_rootservice_event_history_ = is_rs_ev;
    is_server_event_history_ = is_server_ev;
  }
  const common::ObAddr &get_addr() const { return self_addr_; }
  void set_event_table(const char* tname) { event_table_name_ = tname; }
  const char *get_event_table() const { return event_table_name_; }
  int add_task(const common::ObSqlString &sql, const bool is_delete = false,
      const int64_t create_time = OB_INVALID_TIMESTAMP);
  int gen_event_ts(int64_t &event_ts);
protected:
  static constexpr const char * names[7] = {"name1", "name2", "name3", "name4", "name5", "name6", "extra_info"}; // only valid in compile time
  static constexpr const char * values[6] = {"value1", "value2", "value3", "value4", "value5", "value6"}; // only valid in compile time
  static const int64_t TOTAL_LIMIT = 320L * 1024L * 1024L; // 320MB
  static const int64_t HOLD_LIMIT = 160L * 1024L * 1024L;   // 160MB
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t TASK_MAP_SIZE = 20 * 1024;
  static const int64_t TASK_QUEUE_SIZE = 20 *1024;
  static const int64_t MAX_RETRY_COUNT = 12;

  virtual int process_task(const common::ObString &sql, const bool is_delete, const int64_t create_time);
private:
  bool inited_;
  volatile bool stopped_;
  int64_t last_event_ts_;
  lib::ObMutex lock_;
  common::ObMySQLProxy *proxy_;
  common::ObDedupQueue event_queue_;
  const char* event_table_name_;
  common::ObAddr self_addr_;
  bool is_rootservice_event_history_;
  bool is_server_event_history_;
  // timer for add_event_with_retry
  common::ObOccamTimer timer_;
protected:
  ObEventHistoryTableOperator();
private:
  DISALLOW_COPY_AND_ASSIGN(ObEventHistoryTableOperator);
};

template<typename T>
struct ObEventHistoryTableOperator::ValueConverter<true, T>
{
  const ObString & convert(const char *value) {
    truncated_str_.assign_ptr(value, min(static_cast<int64_t>(STRLEN(value)), MAX_ROOTSERVICE_EVENT_VALUE_LENGTH));
    return truncated_str_;
  }

  template<typename U = T, typename std::enable_if<common::__has_to_string__<U>::value, bool>::type = true>
  const ObString & convert(const U &value) {
    int64_t len = value.to_string(buffer_, sizeof(buffer_));
    truncated_str_.assign_ptr(buffer_, len);
    return truncated_str_;
  }

  template<typename U = T, typename std::enable_if<!common::__has_to_string__<U>::value, bool>::type = true>
  inline const U & convert(const U &value) {
    return value;
  }

  char buffer_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH + 1];
  ObString truncated_str_;
};

template<typename T>
struct ObEventHistoryTableOperator::ValueConverter<false, T>
{
  inline const T & convert(const T &value) {
    return value;
  }
};

template <bool Truncate, typename ...Rest>
int ObEventHistoryTableOperator::add_event(const char *module, const char *event, Rest &&...others)
{
  static_assert(sizeof...(others) >= 0 && sizeof...(others) <= 13 &&
                (sizeof...(others) == 13 || (sizeof...(others) % 2 == 0)),
                "max support 6 pair of name-value args and 1 extra info, if number of others is not 13, should be even");
  int ret = common::OB_SUCCESS;
  int64_t event_ts = 0;
  common::ObSqlString sql;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (NULL == module || NULL == event) {
    // name[1-6] can be null, don't need to check them
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "neither module or event can be NULL", KP(module), KP(event), K(ret));
  } else if (OB_FAIL(gen_event_ts(event_ts))) {
    SHARE_LOG(WARN, "gen_event_ts failed", K(ret));
  } else {
    share::ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_gmt_create(event_ts))
        || OB_FAIL(dml.add_column("module", module))
        || OB_FAIL(dml.add_column("event", event))) {
      SHARE_LOG(WARN, "add column failed", K(ret));
    } else if (OB_FAIL((add_event_helper_<0, Truncate>(dml, std::forward<Rest>(others)...)))) {// recursive call
    }
    char ip_buf[common::MAX_IP_ADDR_LENGTH];
    if (common::OB_SUCCESS == ret && self_addr_.is_valid()) {
      if (is_rootservice_event_history_) {
        //Add rs_svr_ip, rs_svr_port to the __all_rootservice_event_history table
        (void)self_addr_.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
        if (OB_FAIL(dml.add_column("rs_svr_ip", ip_buf))
            || OB_FAIL(dml.add_column("rs_svr_port", self_addr_.get_port()))) {
          SHARE_LOG(WARN, "add column failed", K(ret));
        }
      } else {
        // __all_server_event_history has svr_ip and svr_port, but __all_rootservice_event_history hasn't.
        (void)self_addr_.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
        if (OB_FAIL(dml.add_column("svr_ip", ip_buf))
            || OB_FAIL(dml.add_column("svr_port", self_addr_.get_port()))) {
          SHARE_LOG(WARN, "add column failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml.splice_insert_sql(event_table_name_, sql))) {
        SHARE_LOG(WARN, "splice_insert_sql failed", K(ret));
      } else if (OB_FAIL(add_task(sql))) {
        SHARE_LOG(WARN, "add_task failed", K(sql), K(ret));
      }
    }
  }
  ObTaskController::get().allow_next_syslog();
  SHARE_LOG(INFO, "event table add task", K(ret), K_(event_table_name), K(sql));
  return ret;
}

template <typename ...Rest>
int ObEventHistoryTableOperator::sync_add_event(const char *module, const char *event, Rest &&...others)
{
  static_assert(sizeof...(others) >= 0 && sizeof...(others) <= 13 &&
                (sizeof...(others) == 13 || (sizeof...(others) % 2 == 0)),
                "max support 6 pair of name-value args and 1 extra info, if number of others is not 13, should be even");
  int ret = common::OB_SUCCESS;
  int64_t event_ts = 0;
  int64_t affected_rows = 0;
  common::ObSqlString sql;
  share::ObDMLSqlSplicer dml;
  char ip_buf[common::MAX_IP_ADDR_LENGTH];
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (NULL == module || NULL == event) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "neither module or event can be NULL", KP(module), KP(event), K(ret));
  } else if (OB_FAIL(gen_event_ts(event_ts))) {
    SHARE_LOG(WARN, "gen_event_ts failed", K(ret));
  } else if (OB_FAIL(dml.add_gmt_create(event_ts)) ||
             OB_FAIL(dml.add_column("module", module)) ||
             OB_FAIL(dml.add_column("event", event))) {
    SHARE_LOG(WARN, "add column failed", K(ret));
  } else if (OB_FAIL((add_event_helper_<0, false>(dml, std::forward<Rest>(others)...)))) {// recursive call
  } else if (common::OB_SUCCESS == ret && self_addr_.is_valid()) {
    if (is_rootservice_event_history_) {
      //Add rs_svr_ip, rs_svr_port to the __all_rootservice_event_history table
      (void)self_addr_.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
      if (OB_FAIL(dml.add_column("rs_svr_ip", ip_buf))
          || OB_FAIL(dml.add_column("rs_svr_port", self_addr_.get_port()))) {
        SHARE_LOG(WARN, "add column failed", K(ret));
      }
    } else {
      // __all_server_event_history has svr_ip and svr_port, but __all_rootservice_event_history hasn't.
      (void)self_addr_.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
      if (OB_FAIL(dml.add_column("svr_ip", ip_buf))
          || OB_FAIL(dml.add_column("svr_port", self_addr_.get_port()))) {
        SHARE_LOG(WARN, "add column failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dml.splice_insert_sql(event_table_name_, sql))) {
        SHARE_LOG(WARN, "splice_insert_sql failed", K(ret));
    } else if (OB_FAIL(proxy_->write(sql.ptr(), affected_rows))) {
      SHARE_LOG(WARN, "sync execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "affected_rows expected to be one", K(affected_rows), K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      SHARE_LOG(INFO, "event table sync add event success", K(ret), K_(event_table_name), K(sql));
    }
  }
  return ret;
}

template <typename ...Rest>
int ObEventHistoryTableOperator::add_event_with_retry(const char *module, const char *event, Rest &&...others)
{
  static_assert(sizeof...(others) >= 0 && sizeof...(others) <= 13 &&
                (sizeof...(others) == 13 || (sizeof...(others) % 2 == 0)),
                "max support 6 pair of name-value args and 1 extra info, if number of others is not 13, should be even");
  int ret = common::OB_SUCCESS;
  int64_t event_ts = 0;
  common::ObSqlString sql;
  share::ObDMLSqlSplicer dml;
  char ip_buf[common::MAX_IP_ADDR_LENGTH];
  TIMEGUARD_INIT(EVENT_HISTORY, 100_ms, 5_s);                                                                         \
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (NULL == module || NULL == event) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "neither module or event can be NULL", KP(module), KP(event), K(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    SHARE_LOG(WARN, "observer is stopped, cancel add_event", K(sql), K(ret));
  } else if (CLICK_FAIL(gen_event_ts(event_ts))) {
    SHARE_LOG(WARN, "gen_event_ts failed", K(ret));
  } else if (CLICK_FAIL(dml.add_gmt_create(event_ts)) ||
             CLICK_FAIL(dml.add_column("module", module)) ||
             CLICK_FAIL(dml.add_column("event", event))) {
    SHARE_LOG(WARN, "add column failed", K(ret));
  } else if (CLICK_FAIL((add_event_helper_<0, false>(dml, std::forward<Rest>(others)...)))) {// recursive call
  } else if (common::OB_SUCCESS == ret && self_addr_.is_valid()) {
    if (is_rootservice_event_history_) {
      //Add rs_svr_ip, rs_svr_port to the __all_rootservice_event_history table
      (void)self_addr_.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
      if (CLICK_FAIL(dml.add_column("rs_svr_ip", ip_buf))
          || CLICK_FAIL(dml.add_column("rs_svr_port", self_addr_.get_port()))) {
        SHARE_LOG(WARN, "add column failed", K(ret));
      }
    } else {
      // __all_server_event_history has svr_ip and svr_port, but __all_rootservice_event_history hasn't.
      (void)self_addr_.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
      if (CLICK_FAIL(dml.add_column("svr_ip", ip_buf))
          || CLICK_FAIL(dml.add_column("svr_port", self_addr_.get_port()))) {
        SHARE_LOG(WARN, "add column failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (CLICK_FAIL(dml.splice_insert_sql(event_table_name_, sql))) {
      SHARE_LOG(WARN, "splice_insert_sql failed", K(ret));
    } else if (CLICK_FAIL(add_event_to_timer_(sql))) {
      SHARE_LOG(WARN, "add_event_to_timer_ failed", K(ret), K(sql));
    } else {
      SHARE_LOG(INFO, "add_event_with_retry success", K(ret), K(sql));
    }
  }
  return ret;
}
// recursive begin
template <int Floor, bool Truncate, typename Name, typename Value, typename ...Rest>
int ObEventHistoryTableOperator::add_event_helper_(share::ObDMLSqlSplicer &dml, Name &&name, Value &&value, Rest &&...others)
{
  int ret = OB_SUCCESS;
  common::ObSqlString sql;
  ValueConverter<Truncate, Value> converter;
  #if Truncate
  // Truncate can be true only for __all_rootservice_event_history
  if (OB_UNLIKELY(OB_ALL_ROOTSERVICE_EVENT_HISTORY_TNAME != event_table_name_)) {
    ret = OB_NOT_SUPPORTED;
    SHARE_LOG(WARN, "Truncate event only available for __all_rootservice_event_history",
              KR(ret), "actual_table", event_table_name_);
  } else
  #endif
  if (OB_FAIL(dml.add_column(names[Floor], name))) {
    SHARE_LOG(WARN, "add column failed", K(ret), K(Floor));
  } else if (OB_FAIL(dml.add_column(values[Floor], converter.convert(value)))) {
    SHARE_LOG(WARN, "add column failed", K(ret), K(Floor));
  } else if (OB_FAIL((add_event_helper_<Floor + 1, Truncate>(dml, std::forward<Rest>(others)...)))){
  }
  return ret;
}

// recursive end if there is no extra_info
// if not all columns are user-specified, set rest fields empty
template <int Floor, bool Truncate>
int ObEventHistoryTableOperator::add_event_helper_(share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (Floor < 6) {
    for (int64_t idx = Floor; OB_SUCCESS == ret && idx < 6; ++idx) {
      if (OB_FAIL(dml.add_column(values[idx], ""))) {
        SHARE_LOG(WARN, "add column failed", K(ret), K(idx));
      }
    }
  }
  return ret;
}

// recursive end if there is an extra_info
template <int Floor, bool Truncate, typename Value>
int ObEventHistoryTableOperator::add_event_helper_(share::ObDMLSqlSplicer &dml, Value &&extra_info)
{
  static_assert(Floor == 6, "if there is an extra_info column, it must be 13th args in this row, no more, no less");
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_column(names[Floor], extra_info))) {
    SHARE_LOG(WARN, "add column failed", K(ret), K(Floor));
  }
  return ret;
}


}//end namespace rootserver
}//end namespace oceanbase

#endif
