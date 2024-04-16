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
  FLASHBACK = 10,
  RECOVERY = 11,
  OTHER = 12,
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
    USER_TYPE_STR(FLASHBACK);
    USER_TYPE_STR(RECOVERY);
    USER_TYPE_STR(OTHER);
    default:
      return "Invalid";
  }
  #undef USER_TYPE_STR
}

class LogIOContext
{
public:
  LogIOContext(const LogIOUser &user) : palf_id_(INVALID_PALF_ID), user_(user), iterator_info_() {}
  LogIOContext(const int64_t palf_id, const LogIOUser &user) : palf_id_(palf_id), user_(user), iterator_info_() {}
  ~LogIOContext() { destroy(); }
  void destroy()
  {
    user_ = LogIOUser::DEFAULT;
    iterator_info_.reset();
  }
  void set_user_type(const LogIOUser user) {
    user_ = user;
  }
  void set_palf_id(const int64_t palf_id) { palf_id_ = palf_id; }
  void set_allow_filling_cache(const bool allow_filling_cache) {
    iterator_info_.set_allow_filling_cache(allow_filling_cache);
  }
  void set_start_lsn(const LSN &start_lsn) {
    iterator_info_.set_start_lsn(start_lsn);
  }
  LogIteratorInfo *get_iterator_info() {
    return &iterator_info_;
  }
  TO_STRING_KV("user", log_io_user_str(user_), K(palf_id_), K(iterator_info_));

private:
  int64_t palf_id_;
  LogIOUser user_;
  LogIteratorInfo iterator_info_;
};
}
}
#endif
