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

#ifndef OCEANBASE_COMMON_SHARE_OB_LOG_GENERATOR_
#define OCEANBASE_COMMON_SHARE_OB_LOG_GENERATOR_

#include "lib/net/ob_addr.h"
#include "common/log/ob_log_cursor.h"
#include "common/log/ob_log_entry.h"
#include "common/log/ob_log_constants.h"

namespace oceanbase
{
namespace common
{
struct DebugLog
{
  enum { MAGIC = 0xde6a9de6a901 };
  DebugLog(): server_(), ctime_(0), last_ctime_(0) {}
  ~DebugLog() {}
  int advance();
  int64_t to_string(char *buf, const int64_t len) const;
  int serialize(char *buf, int64_t limit, int64_t &pos) const;
  int deserialize(const char *buf, int64_t limit, int64_t &pos);

  ObAddr server_;
  int64_t ctime_;
  int64_t last_ctime_;
};
class ObLogGenerator
{
public:
  ObLogGenerator();
  ~ObLogGenerator();
  int init(int64_t log_buf_size, int64_t log_file_max_size, const ObAddr *id = NULL);
  void destroy();
  int reset();
  bool is_log_start() const;
  int start_log(const ObLogCursor &start_cursor);
  bool check_log_size(const int64_t size) const;
  int update_cursor(const ObLogCursor
                    &log_cursor); // The cursor of log_generator will also be updated when the standby machine writes the log
  int fill_batch(const char *buf, int64_t len);
  int write_log(const LogCommand cmd, const char *log_data, const int64_t data_len);
  template<typename T>
  int write_log(const LogCommand cmd, T &data);
  int get_log(ObLogCursor &start_cursor, ObLogCursor &end_cursor, char *&buf, int64_t &len);
  int commit(const ObLogCursor &end_cursor);
  int switch_log(int64_t &new_file_id);
  int check_point(int64_t &cur_log_file_id);
  int gen_keep_alive();
  bool is_clear() const;
  int64_t to_string(char *buf, const int64_t len) const;
  static bool is_eof(const char *buf, int64_t len);
public:
  int get_start_cursor(ObLogCursor &log_cursor) const;
  int get_end_cursor(ObLogCursor &log_cursor) const;
  int dump_for_debug() const;
protected:
  bool is_inited() const;
  int check_state() const;
  bool has_log() const;
  int do_write_log(const LogCommand cmd, const char *log_data, const int64_t data_len,
                   const int64_t reserved_len);
  int check_log_file_size();
  int switch_log();
  int write_nop(const bool force_write = false);
  int append_eof();
public:
  static char eof_flag_buf_[ObLogConstants::LOG_FILE_ALIGN_SIZE] __attribute__((aligned(ObLogConstants::LOG_FILE_ALIGN_SIZE)));
private:
  bool is_frozen_;
  int64_t log_file_max_size_;
  ObLogCursor start_cursor_;
  ObLogCursor end_cursor_;
  char *log_buf_;
  int64_t log_buf_len_;
  int64_t pos_;
  DebugLog debug_log_;
  char empty_log_[ObLogConstants::LOG_FILE_ALIGN_SIZE * 2];
  char nop_log_[ObLogConstants::LOG_FILE_ALIGN_SIZE * 2];
};

template<typename T>
int generate_log(char *buf, const int64_t len, int64_t &pos, ObLogCursor &cursor,
                 const LogCommand cmd,
                 const T &data)
{
  int ret = OB_SUCCESS;
  ObLogEntry entry;
  int64_t new_pos = pos;
  int64_t data_pos = pos + entry.get_serialize_size();
  int64_t end_pos = data_pos;
  if (OB_ISNULL(buf) || 0 >= len || pos > len || !cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(ERROR, "generate_log(buf=%p, len=%ld, pos=%ld, cursor=%s)=>%d",
            buf, len, pos, cursor.to_str(), ret);
  } else if (OB_FAIL(data.serialize(buf, len, end_pos))) {
    if (entry.get_serialize_size() + data.get_serialize_size() > len) {
      ret = OB_LOG_TOO_LARGE;
      _OB_LOG(WARN, "log too large(size=%ld, limit=%ld)", data.get_serialize_size(), len);
    } else {
      ret = OB_BUF_NOT_ENOUGH;
    }
  } else if (OB_FAIL(cursor.next_entry(entry, cmd, buf + data_pos,
                                       end_pos - data_pos))) {
    _OB_LOG(ERROR, "cursor[%s].next_entry()=>%d", cursor.to_str(), ret);
  } else if (OB_FAIL(entry.serialize(buf, new_pos + entry.get_serialize_size(),
                                     new_pos))) {
    _OB_LOG(ERROR, "serialize_log_entry(buf=%p, len=%ld, entry[id=%ld], data_len=%ld)=>%d",
            buf, len, entry.seq_, end_pos - data_pos, ret);
  } else if (OB_FAIL(cursor.advance(entry))) {
    _OB_LOG(ERROR, "cursor[id=%ld].advance(entry.id=%ld)=>%d", cursor.log_id_, entry.seq_, ret);
  } else {
    pos = end_pos;
  }
  return ret;
}

template<typename T>
int ObLogGenerator::write_log(const LogCommand cmd, T &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else if (is_frozen_) {
    ret = OB_STATE_NOT_MATCH;
    _OB_LOG(ERROR, "log_generator is frozen, cursor=[%s,%s]", to_cstring(start_cursor_),
            to_cstring(end_cursor_));
  } else if (OB_FAIL(generate_log(log_buf_, log_buf_len_ - ObLogConstants::LOG_BUF_RESERVED_SIZE, 
                                  pos_, end_cursor_, cmd, data))
             && OB_BUF_NOT_ENOUGH != ret) {
    _OB_LOG(WARN, "generate_log(pos=%ld)=>%d", pos_, ret);
  }
  return ret;
}
} // end namespace common
} // end namespace oceanbase
#endif //OCEANBASE_COMMON_SHARE_OB_LOG_GENERATOR_
