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

#include "ob_trace.h"

#include <cstdlib>
#include <ctime>
#include <random>

#include "lib/oblog/ob_log_level.h"
#include "lib/time/ob_time_utility.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{

namespace sql
{
ObFLTSpanMgr* __attribute__((weak)) get_flt_span_manager()
{
  return nullptr;
}
int __attribute__((weak))handle_span_record(ObFLTSpanMgr *flt_span_manager, char* tag_buf, int64_t tag_len, ::oceanbase::trace::ObSpanCtx* span)
{
  UNUSED(flt_span_manager);
  return 0;
}
}

namespace trace
{
#define UUID_PATTERN "%8.8lx-%4.4lx-%4.4lx-%4.4lx-%12.12lx"
#define TRACE_PATTERN "{\"trace_id\":\""UUID_PATTERN"\",\"name\":\"%s\",\"id\":\""UUID_PATTERN"\",\"start_ts\":%ld,\"end_ts\":%ld,\"parent_id\":\""UUID_PATTERN"\",\"is_follow\":%s"
#define UUID_TOSTRING(uuid) \
((uuid).high_ >> 32), ((uuid).high_ >> 16 & 0xffff), ((uuid).high_ & 0xffff), \
((uuid).low_ >> 48), ((uuid).low_ & 0xffffffffffff)
#define INIT_SPAN(span) \
if (OB_NOT_NULL(span) && 0 == span->span_id_.high_) { \
  span->span_id_.low_ = UUID::gen_rand(); \
  span->span_id_.high_ = span->start_ts_; \
}
thread_local ObTrace* ObTrace::save_buffer = nullptr;

void flush_trace()
{
  ObTrace& trace = *OBTRACE;
  common::ObDList<ObSpanCtx>& current_span = trace.current_span_;
  if (trace.is_inited() && !current_span.is_empty()) {
    ObSpanCtx* span = current_span.get_first();
    ObSpanCtx* next = nullptr;
    while (current_span.get_header() != span) {
      ObSpanCtx* next = span->get_next();
      if (nullptr != span->tags_ || 0 != span->end_ts_) {
        int64_t pos = 0;
        thread_local char buf[MAX_TRACE_LOG_SIZE];
        int ret = OB_SUCCESS;
        ObTagCtxBase* tag = span->tags_;
        bool first = true;
        char tagstr[] = "\"tags\":[";
        INIT_SPAN(span);
        while (OB_SUCC(ret) && OB_NOT_NULL(tag)) {
          if (pos + sizeof(tagstr) + 1 >= MAX_TRACE_LOG_SIZE) {
            ret = OB_BUF_NOT_ENOUGH;
          } else {
            buf[pos++] = ',';
            if (first) {
              strncpy(buf + pos, tagstr, MAX_TRACE_LOG_SIZE - pos);
              pos += sizeof(tagstr) - 1;
              first = false;
            }
            ret = tag->tostring(buf, MAX_TRACE_LOG_SIZE, pos);
            tag = tag->next_;
          }
        }
        if (0 != pos) {
          if (pos + 1 < MAX_TRACE_LOG_SIZE) {
            buf[pos++] = ']';
            buf[pos++] = 0;
          } else {
            buf[MAX_TRACE_LOG_SIZE - 2] = ']';
            buf[MAX_TRACE_LOG_SIZE - 1] = 0;
          }
        }
        INIT_SPAN(span->source_span_);
        _FLT_LOG(INFO,
                      TRACE_PATTERN "%s}",
                      UUID_TOSTRING(trace.get_trace_id()),
                      __span_type_mapper[span->span_type_],
                      UUID_TOSTRING(span->span_id_),
                      span->start_ts_,
                      span->end_ts_,
                      UUID_TOSTRING(OB_ISNULL(span->source_span_) ? OBTRACE->get_root_span_id() : span->source_span_->span_id_),
                      span->is_follow_ ? "true" : "false",
                      buf);
        buf[0] = '\0';
        IGNORE_RETURN sql::handle_span_record(sql::get_flt_span_manager(), buf, pos, span);
        if (0 != span->end_ts_) {
          current_span.remove(span);
          trace.freed_span_.add_first(span);
        }
        span->tags_ = nullptr;
      }
      span = next;
    }
    trace.offset_ = trace.buffer_size_ / 2;
  }
}
uint64_t UUID::gen_rand()
{
  static thread_local std::random_device dev;
  static thread_local std::mt19937 rng(dev());
  static thread_local std::uniform_int_distribution<uint64_t> dist;
  return dist(rng);
}

UUID UUID::gen()
{
  UUID ret;
  ret.high_ = common::ObClockGenerator::getClock();
  ret.low_ = gen_rand();
  return ret;
}

UUID::UUID(const char* uuid)
{
  high_ = low_ = 0;
  for (int i = 0; i < 18; ++i) {
    if (8 == i || 13 == i) {
      continue;
    } else if (uuid[i] >= '0' && uuid[i] <= '9') {
      high_ = (high_ << 4) + (uuid[i] - '0');
    } else if (uuid[i] >= 'a' && uuid[i] <= 'f') {
      high_ = (high_ << 4) + (uuid[i] - 'a') + 10;
    } else if (uuid[i] >= 'A' && uuid[i] <= 'F') {
      high_ = (high_ << 4) + (uuid[i] - 'A') + 10;
    } else {
      return;
    }
  }
  for (int i = 19; i < 36; ++i) {
    if (23 == i) {
      continue;
    } else if (uuid[i] >= '0' && uuid[i] <= '9') {
      low_ = (low_ << 4) + (uuid[i] - '0');
    } else if (uuid[i] >= 'a' && uuid[i] <= 'f') {
      low_ = (low_ << 4) + (uuid[i] - 'a') + 10;
    } else if (uuid[i] >= 'A' && uuid[i] <= 'F') {
      low_ = (low_ << 4) + (uuid[i] - 'A') + 10;
    } else {
      return;
    }
  }
}

int UUID::tostring(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const char* transfer = "0123456789abcdef";
  if (pos + 37 > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    buf[pos++] = transfer[high_ >> 60 & 0xf];
    buf[pos++] = transfer[high_ >> 56 & 0xf];
    buf[pos++] = transfer[high_ >> 52 & 0xf];
    buf[pos++] = transfer[high_ >> 48 & 0xf];
    buf[pos++] = transfer[high_ >> 44 & 0xf];
    buf[pos++] = transfer[high_ >> 40 & 0xf];
    buf[pos++] = transfer[high_ >> 36 & 0xf];
    buf[pos++] = transfer[high_ >> 32 & 0xf];
    buf[pos++] = '-';
    buf[pos++] = transfer[high_ >> 28 & 0xf];
    buf[pos++] = transfer[high_ >> 24 & 0xf];
    buf[pos++] = transfer[high_ >> 20 & 0xf];
    buf[pos++] = transfer[high_ >> 16 & 0xf];
    buf[pos++] = '-';
    buf[pos++] = transfer[high_ >> 12 & 0xf];
    buf[pos++] = transfer[high_ >>  8 & 0xf];
    buf[pos++] = transfer[high_ >>  4 & 0xf];
    buf[pos++] = transfer[high_ >>  0 & 0xf];
    buf[pos++] = '-';
    buf[pos++] = transfer[ low_ >> 60 & 0xf];
    buf[pos++] = transfer[ low_ >> 56 & 0xf];
    buf[pos++] = transfer[ low_ >> 52 & 0xf];
    buf[pos++] = transfer[ low_ >> 48 & 0xf];
    buf[pos++] = '-';
    buf[pos++] = transfer[ low_ >> 44 & 0xf];
    buf[pos++] = transfer[ low_ >> 40 & 0xf];
    buf[pos++] = transfer[ low_ >> 36 & 0xf];
    buf[pos++] = transfer[ low_ >> 32 & 0xf];
    buf[pos++] = transfer[ low_ >> 28 & 0xf];
    buf[pos++] = transfer[ low_ >> 24 & 0xf];
    buf[pos++] = transfer[ low_ >> 20 & 0xf];
    buf[pos++] = transfer[ low_ >> 16 & 0xf];
    buf[pos++] = transfer[ low_ >> 12 & 0xf];
    buf[pos++] = transfer[ low_ >>  8 & 0xf];
    buf[pos++] = transfer[ low_ >>  4 & 0xf];
    buf[pos++] = transfer[ low_ >>  0 & 0xf];
    buf[pos  ] = '\0';
  }
  return ret;
}

int UUID::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(common::serialization::encode_i64(buf, buf_len, pos, high_))) {
    // LOG_WARN("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(common::serialization::encode_i64(buf, buf_len, pos, low_))) {
    // LOG_WARN("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  }
  return ret;
}

int UUID::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(common::serialization::decode_i64(buf, buf_len, pos, reinterpret_cast<int64_t*>(&high_)))) {
    // LOG_WARN("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(common::serialization::decode_i64(buf, buf_len, pos, reinterpret_cast<int64_t*>(&low_)))) {
    // LOG_WARN("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  }
  return ret;
}

int to_string_and_strip(const char* str, const int64_t length, char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  char from[] = "\"\n\r\\\t";
  const char* to[] = { "\\\"", "\\n", "\\r", "\\\\", " "};
  buf[pos++] = '\"';
  for (int j = 0; j < length && str[j]; ++j) {
    bool conv = false;
    for (int i = 0; i < sizeof(from) - 1; ++i) {
      if (from[i] == str[j]) {
        for (const char* toc = to[i]; *toc; ++toc) {
          if (pos < buf_len) {
            buf[pos++] = *toc;
          } else {
            ret = OB_BUF_NOT_ENOUGH;
          }
        }
        conv = true;
        break;
      }
    }
    if (!conv) {
      if (pos < buf_len) {
        buf[pos++] = str[j];
      } else {
        ret = OB_BUF_NOT_ENOUGH;
      }
    }
  }
  if (OB_FAIL(ret) || pos + 2 >= buf_len) {
    buf[buf_len - 1] = 0;
  } else {
    buf[pos++] = '\"';
    buf[pos++] = '}';
    buf[pos] = 0;
  }
  return ret;
}

template<>
int ObTagCtx<ObString>::tostring(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTagCtxBase::tostring(buf, buf_len, pos))) {
    // do nothing
  } else {
    ret = to_string_and_strip(data_.ptr(), data_.length(), buf, buf_len, pos);
  }
  return ret;
}

template<>
int ObTagCtx<char*>::tostring(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTagCtxBase::tostring(buf, buf_len, pos))) {
    // do nothing
  } else {
    ret = to_string_and_strip(data_, INT64_MAX, buf, buf_len, pos);
  }
  return ret;
}

template<>
int ObTagCtx<void*>::tostring(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTagCtxBase::tostring(buf, buf_len, pos))) {
    // do nothing
  } else {
    ret = to_string_and_strip((char*)&data_, INT64_MAX, buf, buf_len, pos);
  }
  return ret;
}

ObSpanCtx::ObSpanCtx()
  : span_id_(),
    source_span_(nullptr),
    start_ts_(0),
    end_ts_(0),
    tags_(nullptr),
    span_type_(0),
    is_follow_(false)
{
}

ObTrace* ObTrace::get_instance()
{
  if (OB_ISNULL(save_buffer)) {
    thread_local char* default_tsi_buffer = (char*)GET_TSI(ByteBuf<DEFAULT_BUFFER_SIZE>);
    thread_local char default_tls_buffer[MIN_BUFFER_SIZE];
    struct Guard {
      Guard(char* buffer, int64_t size) {
        if (OB_NOT_NULL(buffer)) {
          IGNORE_RETURN new(buffer) ObTrace(size);
        }
      }
    };
    thread_local Guard guard1(default_tsi_buffer, DEFAULT_BUFFER_SIZE);
    thread_local Guard guard2(default_tls_buffer, MIN_BUFFER_SIZE);
    if (OB_ISNULL(default_tsi_buffer)) {
      save_buffer = (ObTrace*)default_tls_buffer;
      LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "tsi was nullptr");
    } else {
      save_buffer = (ObTrace*)default_tsi_buffer;
    }
  }
  return save_buffer;
}

void ObTrace::set_trace_buffer(void* buffer, int64_t buffer_size)
{
  if (OB_ISNULL(buffer) || buffer_size < MIN_BUFFER_SIZE) {
    save_buffer = nullptr;
  } else {
    if (!((ObTrace*)buffer)->check_magic()) {
      IGNORE_RETURN new(buffer) ObTrace(buffer_size);
    }
    save_buffer = (ObTrace*)buffer;
  }
}

ObTrace::ObTrace(int64_t buffer_size)
  : magic_code_(MAGIC_CODE),
    buffer_size_(buffer_size - sizeof(ObTrace)),
    offset_(buffer_size_ / 2),
    current_span_(),
    freed_span_(),
    last_active_span_(nullptr),
    trace_id_(),
    root_span_id_(),
    policy_(0),
    seq_(0)
{
  for (int i = 0; i < (offset_ / sizeof(ObSpanCtx)); ++i) {
    IGNORE_RETURN freed_span_.add_last(new(data_ + i * sizeof(ObSpanCtx)) ObSpanCtx);
  }
}

void ObTrace::init(UUID trace_id, UUID root_span_id, uint8_t policy)
{
  #ifndef NDEBUG
  if (check_magic()) {
    check_leak_span();
  }
  #endif
  reset();
  trace_id_ = trace_id;
  root_span_id_ = root_span_id;
  policy_ = policy;
}

UUID ObTrace::begin()
{
  trace_id_ = UUID::gen();
  return trace_id_;
}

void ObTrace::end()
{
  #ifndef NDEBUG
  check_leak_span();
  #endif
  if (trace_id_.is_inited()) {
    reset();
  }
}

ObSpanCtx* ObTrace::begin_span(uint32_t span_type, uint8_t level, bool is_follow)
{
  ObSpanCtx* new_span = nullptr;
  if (!trace_id_.is_inited() || level > level_) {
    // do nothing
  } else {
    if (freed_span_.is_empty()) {
      FLUSH_TRACE();
    }
    if (freed_span_.is_empty()) {
      check_leak_span();
    } else {
      new_span = freed_span_.remove_last();
      current_span_.add_first(new_span);
      new_span->span_type_ = span_type;
      new_span->span_id_.high_ = 0;
      new_span->span_id_.low_ = ++seq_;
      new_span->span_id_.high_ = 0;
      new_span->source_span_ = last_active_span_;
      new_span->is_follow_ = is_follow;
      new_span->start_ts_ = OB_TSC_TIMESTAMP.current_time();
      new_span->end_ts_ = 0;
      new_span->tags_ = nullptr;
      last_active_span_ = new_span;
    }
  }
  return new_span;
}

// used in ddl task tracing
ObSpanCtx* ObTrace::begin_span_by_id(const uint32_t span_type, const uint8_t level,
                                     const bool is_follow, const UUID span_id, const int64_t start_ts)
{
  ObSpanCtx *span = begin_span(span_type, level, is_follow);
  if (OB_NOT_NULL(span)) {
    span->span_id_ = span_id;
    span->start_ts_ = start_ts;
  }
  return span;
}

void ObTrace::end_span(ObSpanCtx* span)
{
  if (!trace_id_.is_inited() || OB_ISNULL(span) || !span->span_id_.is_inited()) {
    // do nothing
  } else {
    span->end_ts_ = OB_TSC_TIMESTAMP.current_time();
    last_active_span_ = span->source_span_;
  }
}

// used in ddl task tracing
void ObTrace::release_span(ObSpanCtx *&span)
{
  if (!trace_id_.is_inited() || OB_ISNULL(span) || !span->span_id_.is_inited()) {
    // do nothing
  } else {
    end_span(span);
    current_span_.remove(span);
    freed_span_.add_first(span);
    span = nullptr;
  }
}

void ObTrace::reset_span()
{
  #ifndef NDEBUG
  if (!check_magic()) {
    LIB_LOG_RET(ERROR, OB_NOT_INIT, "trace buffer was not inited");
  }
  #endif
  // remove all end span
  if (is_inited() && !current_span_.is_empty()) {
    ObSpanCtx* span = current_span_.get_first();
    while (current_span_.get_header() != span) {
      ObSpanCtx* next = span->get_next();
      if (0 != span->end_ts_) {
        current_span_.remove(span);
        freed_span_.add_first(span);
      }
      span->tags_ = nullptr;
      span = next;
    }
  }
  offset_ = buffer_size_ / 2;
}

int ObTrace::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObSpanCtx* span = const_cast<ObTrace*>(this)->last_active_span_;
  INIT_SPAN(span);
  const UUID& span_id = span == nullptr ? root_span_id_ : span->span_id_;
  if (OB_FAIL(trace_id_.serialize(buf, buf_len, pos))) {
    // LOG_WARN("serialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(span_id.serialize(buf, buf_len, pos))) {
    // LOG_WARN("serialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(common::serialization::encode_i8(buf, buf_len, pos, policy_))) {
    // LOG_WARN("serialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else {
    // do nothing
  }
  return ret;
}

int ObTrace::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(trace_id_.deserialize(buf, buf_len, pos))) {
    // LOG_WARN("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(root_span_id_.deserialize(buf, buf_len, pos))) {
    // LOG_WARN("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(common::serialization::decode_i8(buf, buf_len, pos, reinterpret_cast<int8_t*>(&policy_)))) {
    // LOG_WARN("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else {
    // do nothing
  }
  return ret;
}

void ObTrace::check_leak_span()
{
  ObSpanCtx* span = current_span_.get_first();
  while (current_span_.get_header() != span) {
    if (0 == span->end_ts_) {
      _LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "there were leak span %s", lbt());
      dump_span();
      break;
    }
    span = span->get_next();
  }
}

void ObTrace::reset()
{
  #ifndef NDEBUG
  if (!check_magic()) {
    LIB_LOG_RET(ERROR, OB_NOT_INIT, "trace buffer was not inited");
  }
  #endif
  offset_ = buffer_size_ / 2;
  freed_span_.push_range(current_span_);
  last_active_span_ = nullptr;
  trace_id_ = UUID();
  root_span_id_ = UUID();
  policy_ = 0;
  seq_ = 0;
}

void ObTrace::dump_span()
{
  constexpr int64_t buf_len = 1 << 10;
  char buf[buf_len];
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ObSpanCtx* span = current_span_.get_first();
  IGNORE_RETURN databuff_printf(buf, buf_len, pos, "active_span: ");
  while (OB_SUCC(ret) && current_span_.get_header() != span) {
    if (0 == span->end_ts_) {
      ret = databuff_printf(buf, buf_len, pos, "%s, ", __span_type_mapper[span->span_type_]);
    }
    span = span->get_next();
  }
  _LIB_LOG(WARN, "%s backtrace: %s", buf, lbt());
}

OB_SERIALIZE_MEMBER(FltTransCtx,
                    trace_id_,
                    span_id_,
                    policy_);

}
}
