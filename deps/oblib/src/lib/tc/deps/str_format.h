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

class StrFormat
{
public:
  StrFormat(char* b, int64_t limit): buf_(b), limit_(limit), pos_(0) {
    buf_[0] = 0;
  }
  ~StrFormat() {}
  char* cstr() { return buf_; }
  void append(const char* fmt, ...) __attribute__((format(printf, 2, 3)));
  void clear() { pos_ = 0; }
private:
  char* buf_;
  int64_t limit_;
  int64_t pos_;
};

void StrFormat::append(const char* fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int cnt = vsnprintf(buf_ + pos_,  limit_ - pos_, fmt, ap);
  if (cnt > 0 && pos_ + cnt < limit_) {
    pos_ += cnt;
  }
  va_end(ap);
}

static char* format_bytes(char* buf, int64_t limit, int64_t bytes)
{
  if (bytes >= (INT64_MAX / 16)) {
    snprintf(buf, limit, "x");
  } else if (bytes > 1024 * 1024 * 9) {
    snprintf(buf, limit, "%ldM", bytes/1024/1024);
  } else if (bytes > 1024 * 9) {
    snprintf(buf, limit, "%ldK", bytes/1024);
  } else {
    snprintf(buf, limit, "%ld", bytes);
  }
  return buf;
}
