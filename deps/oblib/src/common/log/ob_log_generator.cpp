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

#include "common/log/ob_log_generator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
int DebugLog::advance()
{
  int ret = OB_SUCCESS;
  last_ctime_ = ctime_;
  ctime_ = ObTimeUtility::current_time();
  return ret;
}

int64_t DebugLog::to_string(char *buf, const int64_t len) const
{
  int64_t pos = 0;
  databuff_printf(buf, len, pos, "DebugLog(%s, ctime=%s[%ld], last_ctime=%s[%ld])",
                  to_cstring(server_),
                  time2str(ctime_), ctime_, time2str(last_ctime_), last_ctime_);
  return pos;
}

int DebugLog::serialize(char *buf, int64_t limit, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || limit < 0 || pos > limit) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, limit, new_pos, MAGIC))
             || OB_FAIL(server_.serialize(buf, limit, new_pos))
             || OB_FAIL(serialization::encode_i64(buf, limit, new_pos, ctime_))
             || OB_FAIL(serialization::encode_i64(buf, limit, new_pos, last_ctime_))) {
    OB_LOG(WARN, "serialize error", K(ret), KP(buf), K(limit), K(pos));
    ret = OB_SERIALIZE_ERROR;
  } else {
    pos = new_pos;
  }
  return ret;
}

int DebugLog::deserialize(const char *buf, int64_t limit, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t magic = 0;
  if (OB_ISNULL(buf) || limit < 0 || pos > limit) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != serialization::decode_i64(buf, limit, new_pos,
                                                     static_cast<int64_t *>(&magic))) {
    ret = OB_DESERIALIZE_ERROR;
  } else if (magic != MAGIC) {
    ret = OB_DESERIALIZE_ERROR;
  } else if (OB_FAIL(server_.deserialize(buf, limit, new_pos))
             || OB_FAIL(serialization::decode_i64(buf, limit, new_pos, static_cast<int64_t *>(&ctime_)))
             || OB_FAIL(serialization::decode_i64(buf, limit, new_pos, static_cast<int64_t *>(&last_ctime_)))) {
    OB_LOG(WARN, "deserialization error", KP(buf), K(limit), K(ctime_), K(last_ctime_));
    ret = OB_DESERIALIZE_ERROR;
  } else {
    pos = new_pos;
  }
  return ret;
}

inline int64_t get_align_padding_size(const int64_t x, const int64_t mask)
{
  return -x & mask;
}

static bool is_aligned(int64_t x, int64_t mask)
{
  return !(x & mask);
}

static int64_t calc_nop_log_len(int64_t pos, int64_t min_log_size)
{
  ObLogEntry entry;
  int64_t header_size = entry.get_serialize_size();
  return get_align_padding_size(pos + header_size + min_log_size,
                                ObLogConstants::LOG_FILE_ALIGN_MASK) + min_log_size;
}

char ObLogGenerator::eof_flag_buf_[ObLogConstants::LOG_FILE_ALIGN_SIZE] __attribute__((aligned(ObLogConstants::LOG_FILE_ALIGN_SIZE)));
static class EOF_FLAG_BUF_CONSTRUCTOR
{
public:
  EOF_FLAG_BUF_CONSTRUCTOR()
  {
    const char *mark_str = "end_of_log_file";
    const int64_t mark_length = static_cast<int64_t>(STRLEN(mark_str));
    const int64_t eof_length = static_cast<int64_t>(sizeof(ObLogGenerator::eof_flag_buf_));
    memset(ObLogGenerator::eof_flag_buf_, 0, sizeof(ObLogGenerator::eof_flag_buf_));
    for (int64_t i = 0; i + mark_length < eof_length; i += mark_length) {
      STRCPY(ObLogGenerator::eof_flag_buf_ + i, mark_str);
    }
  }
  ~EOF_FLAG_BUF_CONSTRUCTOR() {}
} eof_flag_buf_constructor_;

ObLogGenerator::ObLogGenerator(): is_frozen_(false),
                                  log_file_max_size_(1 << 24),
                                  start_cursor_(),
                                  end_cursor_(),
                                  log_buf_(NULL),
                                  log_buf_len_(0),
                                  pos_(0),
                                  debug_log_()
{
  memset(empty_log_, 0, sizeof(empty_log_));
  memset(nop_log_, 0, sizeof(nop_log_));
}

ObLogGenerator:: ~ObLogGenerator()
{
  if (NULL != log_buf_) {
    free(log_buf_);
    log_buf_ = NULL;
  }
}

bool ObLogGenerator::is_inited() const
{
  return NULL != log_buf_ && log_buf_len_ > 0;
}

int ObLogGenerator::dump_for_debug() const
{
  int ret = OB_SUCCESS;
  _OB_LOG(INFO, "[start_cursor[%ld], end_cursor[%ld]]", start_cursor_.log_id_, end_cursor_.log_id_);
  return ret;
}

int ObLogGenerator::check_state() const
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
  }
  return ret;
}

int ObLogGenerator::init(int64_t log_buf_size, int64_t log_file_max_size, const ObAddr *server)
{
  int ret = OB_SUCCESS;
  int sys_ret = 0;
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
  } else if (log_buf_size <= 0 || log_file_max_size <= 0 ||
             log_file_max_size < 2 * ObLogConstants::LOG_FILE_ALIGN_SIZE) {
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != (sys_ret = posix_memalign((void **)&log_buf_, ObLogConstants::LOG_FILE_ALIGN_SIZE,
                                            log_buf_size + ObLogConstants::LOG_FILE_ALIGN_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "posix_memalign", K(ret), "buff size", log_buf_size, KERRNOMSG(sys_ret));
  } else {
    if (NULL != server) {
      debug_log_.server_ = *server;
    }
    log_file_max_size_ = log_file_max_size;
    log_buf_len_ = log_buf_size + ObLogConstants::LOG_FILE_ALIGN_SIZE;
    _OB_LOG(INFO, "log_generator.init(log_buf_size=%ld, log_file_max_size=%ld)", log_buf_size,
            log_file_max_size);
  }
  return ret;
}

void ObLogGenerator::destroy()
{
  if (NULL != log_buf_) {
    free(log_buf_);
  }
  is_frozen_ = false;
  log_file_max_size_ = 1 << 24;
  start_cursor_.reset();
  end_cursor_.reset();
  log_buf_ = NULL;
  log_buf_len_ = 0;
  pos_ = 0;
  memset(empty_log_, 0, sizeof(empty_log_));
  memset(nop_log_, 0, sizeof(nop_log_));
}

bool ObLogGenerator::is_log_start() const
{
  return start_cursor_.is_valid();
}

bool ObLogGenerator::is_clear() const
{
  return 0 == pos_ && false == is_frozen_ && start_cursor_.equal(end_cursor_);
}

int64_t ObLogGenerator::to_string(char *buf, const int64_t len) const
{
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_inited())) {
    databuff_printf(buf, len, pos, "WARN ObLogGenerator not init");
  } else {
    databuff_printf(buf, len, pos, "LogGenerator([%s,%s], len=%ld, frozen=%s)",
                    to_cstring(start_cursor_), to_cstring(end_cursor_), pos_, STR_BOOL(is_frozen_));
  }
  return pos;
}

bool ObLogGenerator::check_log_size(const int64_t size) const
{
  int tmp_ret = OB_SUCCESS;
  bool ret = false;
  if (OB_FAIL(check_state())) {
    tmp_ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObLogGenerator not init", K(ret));
  } else {
    ObLogEntry entry;
    bool ret = (size > 0 && size + ObLogConstants::LOG_BUF_RESERVED_SIZE + entry.get_serialize_size() <= log_buf_len_);
    if (!ret) {
      _OB_LOG(ERROR, "log_size[%ld] + reserved[%ld] + header[%ld] <= log_buf_len[%ld]",
              size, ObLogConstants::LOG_BUF_RESERVED_SIZE, entry.get_serialize_size(), log_buf_len_);
    }
  }
  UNUSED(tmp_ret);
  return ret;
}

int ObLogGenerator::reset()
{
  int ret = OB_SUCCESS;
  if (!is_clear()) {
    ret = OB_LOG_NOT_CLEAR;
    _OB_LOG(ERROR, "log_not_clear, [%s,%s], len=%ld", to_cstring(start_cursor_),
            to_cstring(end_cursor_), pos_);
  } else {
    start_cursor_.reset();
    end_cursor_.reset();
    is_frozen_ = false;
    pos_ = 0;
  }
  return ret;
}

int ObLogGenerator::start_log(const ObLogCursor &log_cursor)
{
  int ret = OB_SUCCESS;
  if (!log_cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(ERROR, "log_cursor.is_valid()=>false");
  } else if (start_cursor_.is_valid()) {
    ret = OB_INIT_TWICE;
    _OB_LOG(ERROR, "cursor=[%ld,%ld] already inited.", start_cursor_.log_id_, end_cursor_.log_id_);
  } else {
    start_cursor_ = log_cursor;
    end_cursor_ = log_cursor;
    _OB_LOG(INFO, "ObLogGenerator::start_log(log_cursor=%s)", to_cstring(log_cursor));
  }
  return ret;
}

int ObLogGenerator:: update_cursor(const ObLogCursor &log_cursor)
{
  int ret = OB_SUCCESS;
  if (!log_cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!is_clear()) {
    ret = OB_LOG_NOT_CLEAR;
    _OB_LOG(ERROR, "log_not_clear, [%s,%s], len=%ld", to_cstring(start_cursor_),
            to_cstring(end_cursor_), pos_);
  } else if (end_cursor_.newer_than(log_cursor)) {
    ret = OB_DISCONTINUOUS_LOG;
    _OB_LOG(ERROR, "end_cursor[%s].newer_than(log_cursor[%s])", to_cstring(end_cursor_),
            to_cstring(log_cursor));
  } else {
    start_cursor_ = log_cursor;
    end_cursor_ = log_cursor;
  }
  return ret;
}

int ObLogGenerator:: get_start_cursor(ObLogCursor &log_cursor) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else {
    log_cursor = start_cursor_;
  }
  return ret;
}

int ObLogGenerator:: get_end_cursor(ObLogCursor &log_cursor) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else {
    log_cursor = end_cursor_;
  }
  return ret;
}

static int serialize_log_entry(char *buf, const int64_t len, int64_t &pos, ObLogEntry &entry,
                               const char *log_data, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || 0 >= len || pos > len || OB_ISNULL(log_data) || 0 >= data_len) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(ERROR, "serialize_log_entry(buf=%p, len=%ld, pos=%ld, log_data=%p, data_len=%ld)=>%d",
            buf, len, pos, log_data, data_len, ret);
  } else if (pos + entry.get_serialize_size() + data_len > len) {
    ret = OB_BUF_NOT_ENOUGH;
    _OB_LOG(DEBUG, "pos[%ld] + entry.serialize_size[%ld] + data_len[%ld] > len[%ld]",
            pos, entry.get_serialize_size(), data_len, len);
  } else if (OB_FAIL(entry.serialize(buf, len, pos))) {
    _OB_LOG(ERROR, "entry.serialize(buf=%p, pos=%ld, capacity=%ld)=>%d",
            buf, len, pos, ret);
  } else {
    MEMCPY(buf + pos, log_data, data_len);
    pos += data_len;
  }
  return ret;
}

static int generate_log(char *buf, const int64_t len, int64_t &pos, ObLogCursor &cursor,
                        const LogCommand cmd,
                        const char *log_data, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  ObLogEntry entry;
  if (OB_ISNULL(buf) || 0 >= len || pos > len || OB_ISNULL(log_data) || 0 >= data_len ||
      !cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(ERROR, "generate_log(buf=%p, len=%ld, pos=%ld, log_data=%p, data_len=%ld, cursor=%s)=>%d",
            buf, len, pos, log_data, data_len, to_cstring(cursor), ret);
  } else if (entry.get_serialize_size() + data_len > len) {
    ret = OB_LOG_TOO_LARGE;
    _OB_LOG(WARN, "header[%ld] + data_len[%ld] > len[%ld]", entry.get_serialize_size(), data_len,
            len);
  } else if (OB_FAIL(cursor.next_entry(entry, cmd, log_data, data_len))) {
    _OB_LOG(ERROR, "cursor[%s].next_entry()=>%d", to_cstring(cursor), ret);
  } else if (OB_FAIL(serialize_log_entry(buf, len, pos, entry, log_data, data_len))) {
    _OB_LOG(DEBUG, "serialize_log_entry(buf=%p, len=%ld, entry[id=%ld], data_len=%ld)=>%d",
            buf, len, entry.seq_, data_len, ret);
  } else if (OB_FAIL(cursor.advance(entry))) {
    _OB_LOG(ERROR, "cursor[id=%ld].advance(entry.id=%ld)=>%d", cursor.log_id_, entry.seq_, ret);
  }
  return ret;
}

int ObLogGenerator:: do_write_log(const LogCommand cmd, const char *log_data,
                                  const int64_t data_len,
                                  const int64_t reserved_len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else if (OB_ISNULL(log_data) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_frozen_) {
    ret = OB_STATE_NOT_MATCH;
    _OB_LOG(ERROR, "log_generator is frozen, cursor=[%s,%s]", to_cstring(start_cursor_),
            to_cstring(end_cursor_));
  } else if (OB_FAIL(generate_log(log_buf_, log_buf_len_ - reserved_len, pos_,
                                  end_cursor_, cmd, log_data, data_len))
             && OB_BUF_NOT_ENOUGH != ret) {
    _OB_LOG(WARN, "generate_log(pos=%ld)=>%d", pos_, ret);
  }
  return ret;
}

static int parse_log_buffer(const char *log_data, int64_t data_len, const ObLogCursor &start_cursor,
                            ObLogCursor &end_cursor, bool check_data_integrity = false)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t tmp_pos = 0;
  int64_t file_id = 0;
  ObLogEntry log_entry;
  end_cursor = start_cursor;
  if (OB_ISNULL(log_data) || data_len <= 0 || !start_cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(ERROR, "invalid argument, log_data=%p, data_len=%ld, start_cursor=%s",
            log_data, data_len, to_cstring(start_cursor));
  }

  while (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(log_entry.deserialize(log_data, data_len, pos))) {
      _OB_LOG(ERROR, "log_entry.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data,
              data_len, pos, ret);
    } else if (pos + log_entry.get_log_data_len() > data_len) {
      ret = OB_LAST_LOG_RUINNED;
      _OB_LOG(ERROR, "last_log broken, cursor=[%s,%s]", to_cstring(start_cursor),
              to_cstring(end_cursor));
    } else if (check_data_integrity &&
               OB_FAIL(log_entry.check_data_integrity(log_data + pos))) {
      _OB_LOG(ERROR, "log_entry.check_data_integrity()=>%d", ret);
    } else {
      tmp_pos = pos;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_LOG_SWITCH_LOG == log_entry.cmd_) {
      if (OB_FAIL(serialization::decode_i64(log_data, data_len, tmp_pos, static_cast<int64_t *>(&file_id)))) {
        _OB_LOG(ERROR, "decode switch_log failed(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data,
                data_len, tmp_pos, ret);
      } else if (start_cursor.log_id_ != file_id) {
        ret = OB_ERR_UNEXPECTED;
        _OB_LOG(ERROR, "decode switch_log failed(log_data=%p, data_len=%ld, pos=%ld, start_cursor.log_id=%ld, file_id=%ld)=>%d", log_data,
                data_len, tmp_pos, start_cursor.log_id_, file_id, ret);
      }
    }

    if (OB_SUCC(ret)) {
      pos += log_entry.get_log_data_len();
      if (OB_FAIL(end_cursor.advance(log_entry))) {
        _OB_LOG(ERROR, "end_cursor[%ld].advance(%ld)=>%d", end_cursor.log_id_, log_entry.seq_, ret);
      }
    }
  }

  if (OB_SUCC(ret) && pos != data_len) {
    ret = OB_ERR_UNEXPECTED;
    _OB_LOG(ERROR, "pos[%ld] != data_len[%ld]", pos, data_len);
  }

  if (OB_FAIL(ret)) {
    hex_dump(log_data, static_cast<int32_t>(data_len), OB_LOG_LEVEL_WARN);
  }
  return ret;
}

int ObLogGenerator:: fill_batch(const char *buf, int64_t len)
{
  int ret = OB_SUCCESS;
  ObLogCursor start_cursor, end_cursor;
  int64_t reserved_len = ObLogConstants::LOG_FILE_ALIGN_SIZE;
  start_cursor = end_cursor_;
  if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else if (OB_ISNULL(buf) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != (len & ObLogConstants::LOG_FILE_ALIGN_MASK)) {
    ret = OB_LOG_NOT_ALIGN;
    _OB_LOG(ERROR, "len[%ld] is not align[mask=%lx], ret=%d", len, ObLogConstants::LOG_FILE_ALIGN_SIZE, ret);
  } else if (is_frozen_) {
    ret = OB_BUF_NOT_ENOUGH;
    _OB_LOG(WARN, "log_buf is frozen, end_cursor=%s, ret=%d", to_cstring(end_cursor_), ret);
  } else if (0 != pos_) {
    ret = OB_LOG_NOT_CLEAR;
    _OB_LOG(ERROR, "fill_batch(pos[%ld] != 0, end_cursor=%s, buf=%p[%ld]), ret=%d",
            pos_, to_cstring(end_cursor_), buf, len, ret);
  } else if (len + reserved_len > log_buf_len_) {
    ret = OB_BUF_NOT_ENOUGH;
    _OB_LOG(ERROR, "len[%ld] + reserved_len[%ld] > log_buf_len[%ld], ret=%d",
            len, reserved_len, log_buf_len_, ret);
  } else if (OB_FAIL(parse_log_buffer(buf, len, start_cursor, end_cursor))) {
    _OB_LOG(ERROR, "parse_log_buffer(buf=%p[%ld], cursor=%s)=>%d", buf, len, to_cstring(end_cursor_),
            ret);
  } else {
    MEMCPY(log_buf_, buf, len);
    pos_ = len;
    end_cursor_ = end_cursor;
    is_frozen_ = true;
  }
  return ret;
}

int ObLogGenerator:: write_log(const LogCommand cmd, const char *log_data, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else if (OB_ISNULL(log_data) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_frozen_) {
    ret = OB_BUF_NOT_ENOUGH;
    _OB_LOG(WARN, "log_buf is frozen, end_cursor=%s", to_cstring(end_cursor_));
  } else if (OB_FAIL(do_write_log(cmd, log_data, data_len, ObLogConstants::LOG_BUF_RESERVED_SIZE))
             && OB_BUF_NOT_ENOUGH != ret) {
    _OB_LOG(WARN, "do_write_log(cmd=%d, pos=%ld, len=%ld)=>%d", cmd, pos_, data_len, ret);
  }

  return ret;
}

int ObLogGenerator::switch_log()
{
  int ret = OB_SUCCESS;
  ObLogEntry entry;
  int64_t header_size = entry.get_serialize_size();
  const int64_t buf_len = ObLogConstants::LOG_FILE_ALIGN_SIZE - header_size;
  char *buf = empty_log_;
  int64_t buf_pos = 0;
  if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, buf_pos,
                                               end_cursor_.file_id_ + 1))) {
    _OB_LOG(ERROR, "encode_i64(file_id_=%ld)=>%d", end_cursor_.file_id_, ret);
  } else if (OB_FAIL(do_write_log(OB_LOG_SWITCH_LOG, buf, buf_len, 0))) {
    _OB_LOG(ERROR, "write(OB_LOG_SWITCH_LOG, len=%ld)=>%d", end_cursor_.file_id_, ret);
  } else {
    _OB_LOG(INFO, "switch_log(file_id=%ld, log_id=%ld)", end_cursor_.file_id_, end_cursor_.log_id_);
  }
  return ret;
}

int ObLogGenerator::check_point(int64_t &cur_log_file_id)
{
  int ret = OB_SUCCESS;
  ObLogEntry entry;
  int64_t header_size = entry.get_serialize_size();
  const int64_t buf_len = ObLogConstants::LOG_FILE_ALIGN_SIZE - header_size;
  char *buf = empty_log_;
  int64_t buf_pos = 0;
  if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, buf_pos,
                                               end_cursor_.file_id_))) {
    _OB_LOG(ERROR, "encode_i64(file_id_=%ld)=>%d", end_cursor_.file_id_, ret);
  } else if (OB_FAIL(do_write_log(OB_LOG_CHECKPOINT, buf, buf_len, 0))) {
    _OB_LOG(ERROR, "write(OB_LOG_SWITCH_LOG, len=%ld)=>%d", end_cursor_.file_id_, ret);
  } else {
    cur_log_file_id = end_cursor_.file_id_;
    _OB_LOG(INFO, "checkpoint(file_id=%ld, log_id=%ld)", end_cursor_.file_id_, end_cursor_.log_id_);
  }
  return ret;
}

int ObLogGenerator::gen_keep_alive()
{
  return write_nop(/*force_write*/true);
}

int ObLogGenerator::append_eof()
{
  int ret = OB_SUCCESS;
  if (pos_ + static_cast<int64_t>(sizeof(eof_flag_buf_)) > log_buf_len_) {
    ret = OB_ERR_UNEXPECTED;
    _OB_LOG(ERROR, "no buf to append eof flag, pos=%ld, log_buf_len=%ld", pos_, log_buf_len_);
  } else {
    MEMCPY(log_buf_ + pos_, eof_flag_buf_, sizeof(eof_flag_buf_));
  }
  return ret;
}

bool ObLogGenerator::is_eof(const char *buf, int64_t len)
{
  return NULL != buf && len >= ObLogConstants::LOG_FILE_ALIGN_SIZE &&
         0 == MEMCMP(buf, eof_flag_buf_, ObLogConstants::LOG_FILE_ALIGN_SIZE);
}

int ObLogGenerator:: check_log_file_size()
{
  int ret = OB_SUCCESS;
  if (end_cursor_.offset_ + log_buf_len_ <= log_file_max_size_)
  {}
  else if (OB_FAIL(switch_log())) {
    _OB_LOG(ERROR, "switch_log()=>%d", ret);
  }
  return ret;
}

int ObLogGenerator:: write_nop(const bool force_write)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  _OB_LOG(TRACE, "try write nop: pos=%ld, force_write=%s", pos_, STR_BOOL(force_write));
  if (is_aligned(pos_, ObLogConstants::LOG_FILE_ALIGN_MASK) && !force_write) {
    _OB_LOG(TRACE, "The log is aligned");
  } else if (OB_FAIL(debug_log_.advance())) {
    _OB_LOG(ERROR, "debug_log.advance()=>%d", ret);
  } else if (OB_FAIL(debug_log_.serialize(nop_log_, sizeof(nop_log_), pos))) {
    _OB_LOG(ERROR, "serialize_nop_log(%s)=>%d", to_cstring(end_cursor_), ret);
  } else if (OB_FAIL(do_write_log(OB_LOG_NOP, nop_log_, calc_nop_log_len(pos_, pos),
                                  0))) {
    _OB_LOG(ERROR, "write_log(OB_LOG_NOP, len=%ld)=>%d", calc_nop_log_len(pos_, pos), ret);
  }
  return ret;
}

int ObLogGenerator:: switch_log(int64_t &new_file_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else if (OB_FAIL(write_nop())) {
    _OB_LOG(ERROR, "write_nop()=>%d", ret);
  } else if (OB_FAIL(switch_log())) {
    _OB_LOG(ERROR, "switch_log()=>%d", ret);
  } else {
    is_frozen_ = true;
    new_file_id = end_cursor_.file_id_;
  }
  return ret;
}

bool ObLogGenerator::has_log() const
{
  return 0 != pos_;
}

int ObLogGenerator:: get_log(ObLogCursor &start_cursor, ObLogCursor &end_cursor, char *&buf,
                             int64_t &len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else if (!is_frozen_ && OB_FAIL(write_nop(has_log()))) {
    _OB_LOG(ERROR, "write_nop()=>%d", ret);
  } else if (!is_frozen_ && OB_FAIL(check_log_file_size())) {
    _OB_LOG(ERROR, "check_log_file_size()=>%d", ret);
  } else if (OB_FAIL(append_eof())) {
    _OB_LOG(ERROR, "write_eof()=>%d", ret);
  } else {
    is_frozen_ = true;
    buf = log_buf_;
    len = pos_;
    end_cursor = end_cursor_;
    start_cursor = start_cursor_;
  }
  return ret;
}

int ObLogGenerator:: commit(const ObLogCursor &end_cursor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_state())) {
    _OB_LOG(ERROR, "check_state()=>%d", ret);
  } else if (!end_cursor.equal(end_cursor_)) {
    ret = OB_ERR_UNEXPECTED;
    _OB_LOG(ERROR, "end_cursor[%ld] != end_cursor_[%ld]", end_cursor.log_id_, end_cursor_.log_id_);
  } else {
    start_cursor_ = end_cursor_;
    pos_ = 0;
    is_frozen_ = false;
  }
  return ret;
}
} // end namespace common
} // end namespace oceanbase
