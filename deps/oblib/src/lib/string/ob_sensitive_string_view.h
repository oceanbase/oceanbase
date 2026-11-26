/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_STRING_OB_SENSITIVE_STRING_VIEW_H_
#define OCEANBASE_LIB_STRING_OB_SENSITIVE_STRING_VIEW_H_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
// Retains ObString's non-owning buffer semantics and wire format, but never prints its contents.
// Use ObSensitiveFixedLengthString when the field must own its buffer.
// Business APIs must explicitly request str()/ptr(); neither belongs in logs.
class ObSensitiveStringView
{
public:
  ObSensitiveStringView() = default;
  explicit ObSensitiveStringView(const ObString &value) : value_(value) {}
  explicit ObSensitiveStringView(const char *value) : value_(value) {}
  ObSensitiveStringView(const int64_t length, const char *ptr) : value_(length, ptr) {}
  ObSensitiveStringView(const int32_t size, const int32_t length, char *ptr) : value_(size, length, ptr) {}
  ObSensitiveStringView &operator=(const ObString &value)
  {
    value_ = value;
    return *this;
  }
  void reset() { value_.reset(); }
  void assign_ptr(const char *bytes, const int64_t length) { value_.assign_ptr(bytes, length); }
  void assign_buffer(char *buffer, const int32_t size) { value_.assign_buffer(buffer, size); }
  bool empty() const { return value_.empty(); }
  bool is_empty() const { return value_.empty(); }
  int32_t length() const { return value_.length(); }
  int32_t size() const { return value_.size(); }
  const char *ptr() const { return value_.ptr(); }
  char *ptr() { return value_.ptr(); }
  ObString &str() { return value_; }
  const ObString &str() const { return value_; }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (nullptr != buf && buf_len > 0) {
      (void)databuff_printf(buf, buf_len, pos, "<redacted>");
    }
    return pos;
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const { return value_.serialize(buf, buf_len, pos); }
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos) { return value_.deserialize(buf, data_len, pos); }
  int64_t get_serialize_size() const { return value_.get_serialize_size(); }

private:
  ObString value_;
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_STRING_OB_SENSITIVE_STRING_VIEW_H_
