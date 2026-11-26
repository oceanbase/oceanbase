/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_STRING_OB_SENSITIVE_FIXED_LENGTH_STRING_H_
#define OCEANBASE_LIB_STRING_OB_SENSITIVE_FIXED_LENGTH_STRING_H_

#include "lib/string/ob_fixed_length_string.h"

namespace oceanbase
{
namespace common
{
// Owns a secret and redacts the whole value when printed, including through K/KP/KPC.
// Raw access is explicit: never pass ptr()/str() to logging APIs.
// Forward serialization without another envelope to preserve ObFixedLengthString's wire format.
template<int64_t N>
class ObSensitiveFixedLengthString
{
public:
  ObSensitiveFixedLengthString() = default;
  ObSensitiveFixedLengthString(const ObSensitiveFixedLengthString &other) { (void)assign(other); }
  explicit ObSensitiveFixedLengthString(const char *str) { (void)assign(str); }
  explicit ObSensitiveFixedLengthString(const ObString &str) { (void)assign(str); }
  ObSensitiveFixedLengthString &operator=(const ObSensitiveFixedLengthString &other)
  {
    if (this != &other) {
      (void)assign(other);
    }
    return *this;
  }

  int assign(const ObSensitiveFixedLengthString &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      ret = value_.assign(other.str());
    }
    return ret;
  }
  int assign(const ObFixedLengthString<N> &other) { return value_.assign(other.str()); }
  int assign(const char *str) { return value_.assign(str); }
  int assign(const ObString &str) { return value_.assign(str); }
  int assign_strive(const char *str) { return value_.assign_strive(str); }
  void reset() { value_.reset(); }
  void reuse() { value_.reuse(); }
  bool is_empty() const { return value_.is_empty(); }
  int64_t size() const { return value_.size(); }
  int64_t capacity() const { return value_.capacity(); }
  const char *ptr() const { return value_.ptr(); }
  char *ptr() { return value_.ptr(); }
  ObString str() const { return value_.str(); }
  bool operator<(const ObSensitiveFixedLengthString &other) const { return value_ < other.value_; }
  bool operator>(const ObSensitiveFixedLengthString &other) const { return value_ > other.value_; }
  bool operator==(const ObSensitiveFixedLengthString &other) const { return value_ == other.value_; }
  bool operator!=(const ObSensitiveFixedLengthString &other) const { return value_ != other.value_; }
  uint64_t hash() const { return value_.hash(); }
  int hash(uint64_t &hash_val, uint64_t seed = 0) const { return value_.hash(hash_val, seed); }

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
  ObFixedLengthString<N> value_;
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_STRING_OB_SENSITIVE_FIXED_LENGTH_STRING_H_
