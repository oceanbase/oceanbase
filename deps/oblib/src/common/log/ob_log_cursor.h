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

#ifndef OCEANBASE_SHARE_LOG_OB_LOG_CURSOR_
#define OCEANBASE_SHARE_LOG_OB_LOG_CURSOR_
#include "lib/ob_define.h"
#include "lib/utility/serialization.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "common/log/ob_log_entry.h"

namespace oceanbase
{
namespace common
{
struct ObLogCursor
{
  int64_t file_id_;
  int64_t log_id_;
  int64_t offset_;
  ObLogCursor();
  ~ObLogCursor();
  bool is_valid() const;
  int to_start();
  void reset();
  int serialize(char *buf, int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, int64_t len, int64_t &pos) const;
  int64_t get_serialize_size() const;
  char *to_str() const;
  int64_t to_string(char *buf, const int64_t len) const;
  int this_entry(ObLogEntry &entry, const LogCommand cmd, const char *log_data,
                 const int64_t data_len) const;
  int next_entry(ObLogEntry &entry, const LogCommand cmd, const char *log_data,
                 const int64_t data_len) const;
  int advance(const ObLogEntry &entry);
  int advance(LogCommand cmd, int64_t seq, const int64_t data_len);
  int advance(int64_t start_id, int64_t end_id, int64_t len, bool is_file_end);
  bool newer_than(const ObLogCursor &that) const;
  bool equal(const ObLogCursor &that) const;
};

ObLogCursor &set_cursor(ObLogCursor &cursor, const int64_t file_id, const int64_t log_id,
                        const int64_t offset);
class ObAtomicLogCursor
{
public:
  ObAtomicLogCursor() : cursor_lock_(ObLatchIds::DEFAULT_SPIN_RWLOCK) {}
  ~ObAtomicLogCursor() {}
  int get_cursor(ObLogCursor &cursor) const;
  int set_cursor(ObLogCursor &cursor);
private:
  ObLogCursor log_cursor_;
  mutable common::SpinRWLock cursor_lock_;
};
}; // end namespace common
}; // end namespace oceanbase
#endif // OCEANBASE_SHARE_LOG_OB_LOG_CURSOR_
