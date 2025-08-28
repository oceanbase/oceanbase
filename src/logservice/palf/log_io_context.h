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

#ifndef OCEANBASE_LOGSERVICE_LOG_IO_CONTEXT_
#define OCEANBASE_LOGSERVICE_LOG_IO_CONTEXT_
#include <cstdint>
#include "lib/utility/ob_print_utils.h"
#include "share/resource_manager/ob_resource_plan_info.h"
#include "log_iterator_info.h"

namespace oceanbase
{
namespace palf
{
enum class LogIOUser {
  DEFAULT = 0,
  REPLAY = 1,
  FETCHLOG = 2,
  ARCHIVE = 3,
  RESTORE = 4,
  CDC = 5,
  STANDBY = 6,
  SHARED_UPLOAD = 7,
  META_INFO = 8,
  RESTART = 9,
  OTHER = 10,
};

inline const char *log_io_user_str(const LogIOUser user_type)
{
  #define USER_TYPE_STR(x) case(LogIOUser::x): return #x
  switch (user_type)
  {
    USER_TYPE_STR(DEFAULT);
    USER_TYPE_STR(REPLAY);
    USER_TYPE_STR(FETCHLOG);
    USER_TYPE_STR(ARCHIVE);
    USER_TYPE_STR(RESTORE);
    USER_TYPE_STR(CDC);
    USER_TYPE_STR(STANDBY);
    USER_TYPE_STR(SHARED_UPLOAD);
    USER_TYPE_STR(META_INFO);
    USER_TYPE_STR(RESTART);
    USER_TYPE_STR(OTHER);
    default:
      return "Invalid";
  }
  #undef USER_TYPE_STR
}

inline share::ObFunctionType log_io_user_prio(const LogIOUser &user_type)
{
  share::ObFunctionType prio = share::ObFunctionType::PRIO_CLOG_LOW;
  if (LogIOUser::REPLAY == user_type ||
      LogIOUser::FETCHLOG == user_type ||
      LogIOUser::SHARED_UPLOAD == user_type ||
      LogIOUser::META_INFO == user_type ||
      LogIOUser::RESTART == user_type) {
    prio = share::ObFunctionType::PRIO_CLOG_HIGH;
  } else if (LogIOUser::CDC == user_type ||
      LogIOUser::ARCHIVE == user_type ||
      LogIOUser::RESTORE == user_type ||
      LogIOUser::STANDBY == user_type) {
    prio = share::ObFunctionType::PRIO_CLOG_MID;
  } else {
    prio = share::ObFunctionType::PRIO_CLOG_MID;
  }
  return prio;
}

class LogIOContext
{
public:
  LogIOContext();
  // do not get group_id
  LogIOContext(const LogIOUser &user);
  LogIOContext(const uint64_t tenant_id, const int64_t palf_id, const LogIOUser &user);
  ~LogIOContext() { destroy(); }
  bool is_valid() const
  {
    bool bool_ret = false;
    if (false == is_enable_fill_cache_user_() && true == iterator_info_.get_allow_filling_cache()) {
      int ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "LogIOContext is invalid!", K(ret), K_(palf_id), K_(user), K_(iterator_info));
    } else {
      bool_ret = true;
    }
    return bool_ret;
  }
  void destroy()
  {
    palf_id_ = 0;
    user_ = LogIOUser::DEFAULT;
    iterator_info_.reset();
  }
  LogIOContext &operator=(const LogIOContext &io_ctx)
  {
    if (&io_ctx != this) {
      this->palf_id_ = io_ctx.palf_id_;
      this->user_ = io_ctx.user_;
      this->iterator_info_ = io_ctx.iterator_info_;
    }
    return *this;
  }
  share::ObFunctionType get_function_type() const { return log_io_user_prio(user_); }
  void set_start_lsn(const LSN &start_lsn) { iterator_info_.set_start_lsn(start_lsn); }
  LogIteratorInfo *get_iterator_info() { return &iterator_info_; }
  void inc_read_io_cnt() 
  { 
    iterator_info_.inc_read_io_cnt();
  }
  void inc_read_io_size(const int64_t read_size) 
  {
    iterator_info_.inc_read_io_size(read_size);
  }
  void inc_read_disk_cost_ts(const int64_t cost_ts)
  {
    iterator_info_.inc_read_disk_cost_ts(cost_ts);
  }
  void inc_hit_cnt(const bool is_cold_cache)
  {
    iterator_info_.inc_hit_cnt(is_cold_cache);
  }
  void inc_miss_cnt(const bool is_cold_cache)
  {
    iterator_info_.inc_miss_cnt(is_cold_cache);
  }
  void inc_cache_read_size(const int64_t read_size, const bool is_cold_cache)
  {
    iterator_info_.inc_cache_read_size(read_size, is_cold_cache);
  }
  TO_STRING_KV("user", log_io_user_str(user_), K_(palf_id), K_(iterator_info));
private:
  bool is_enable_fill_cache_user_() const {
    return (LogIOUser::RESTART == user_ || 
            LogIOUser::FETCHLOG == user_ || 
            LogIOUser::META_INFO == user_ ||
            LogIOUser::REPLAY == user_) ? false : true;
  }
private:
  int64_t palf_id_;
  LogIOUser user_;
  LogIteratorInfo iterator_info_;
};
}
}
#endif
