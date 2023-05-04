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

#ifndef OCEANBASE_LIB_STRING_OB_FIXED_LENGTH_STRING_H_
#define OCEANBASE_LIB_STRING_OB_FIXED_LENGTH_STRING_H_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace common
{
// This class only used to encapsulate raw c_string, can't be inherited
template<int64_t N>
class ObFixedLengthString
{
  OB_UNIS_VERSION(1);

public:
  ObFixedLengthString();
  ObFixedLengthString(const char *str);
  ObFixedLengthString(const ObString &str);
  ObFixedLengthString(const ObFixedLengthString &str);
  ObFixedLengthString &operator =(const ObFixedLengthString &str);
  ~ObFixedLengthString() { }

  int assign(const ObFixedLengthString &other);
  int assign(const char *str);
  int assign_strive(const char *str);
  int assign(const ObString &str);
  void reset() { buf_[0] = '\0'; }
  void reuse() { buf_[0] = '\0'; }
  // compare functions
  bool operator <(const ObFixedLengthString &str) const;
  bool operator >(const ObFixedLengthString &str) const;
  bool operator ==(const ObFixedLengthString &str) const;
  bool operator !=(const ObFixedLengthString &str) const;
  bool is_empty() const;

  const char *ptr() const { return buf_; }
  int64_t size() const { return strlen(buf_); }
  int64_t capacity() const { return N; }
  // dangerous api, invoker assure not to write more than N-1 bytes
  char *ptr() { return buf_; }

  const ObString str() const { return ObString(size(), ptr()); };
  uint64_t hash() const;
  int hash(uint64_t &hash_val, uint64_t seed = 0) const;

  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  char buf_[N];
};

template<int64_t N>
ObFixedLengthString<N>::ObFixedLengthString()
{
  STATIC_ASSERT(N > 0, "N should greater than 0");
  buf_[0] = '\0';
}

template<int64_t N>
ObFixedLengthString<N>::ObFixedLengthString(const char *str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(str))) {
    LIB_LOG(WARN, "assign failed", KCSTRING(str), K(ret));
  }
}

template<int64_t N>
ObFixedLengthString<N>::ObFixedLengthString(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(str))) {
    LIB_LOG(WARN, "assign failed", K(str), K(ret));
  }
}

template<int64_t N>
ObFixedLengthString<N>::ObFixedLengthString(const ObFixedLengthString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(str.buf_))) {
    LIB_LOG(WARN, "assign failed", K(str), K(ret));
  }
}

template<int64_t N>
ObFixedLengthString<N> &ObFixedLengthString<N>::operator =(
    const ObFixedLengthString &str)
{
  int ret = OB_SUCCESS;
  if (this != &str) {
    if (OB_FAIL(assign(str.buf_))) {
      LIB_LOG(WARN, "assign failed", K(str), K(ret));
    }
  }
  return *this;
}

template<int64_t N>
bool ObFixedLengthString<N>::operator <(const ObFixedLengthString &str) const
{
  return (STRCMP(buf_, str.buf_) < 0);
}

template<int64_t N>
bool ObFixedLengthString<N>::operator >(const ObFixedLengthString &str) const
{
  return (STRCMP(buf_, str.buf_) > 0);
}

template<int64_t N>
bool ObFixedLengthString<N>::operator ==(const ObFixedLengthString &str) const
{
  return (0 == STRCMP(buf_, str.buf_));
}

template<int64_t N>
bool ObFixedLengthString<N>::operator !=(const ObFixedLengthString &str) const
{
  return !(*this == str);
}

template<int64_t N>
bool ObFixedLengthString<N>::is_empty() const
{
  return ('\0' == buf_[0]);
}

template<int64_t N>
int ObFixedLengthString<N>::assign(const ObFixedLengthString &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(assign(other.buf_))) {
      LIB_LOG(WARN, "assign failed", K(other), K(ret));
    }
  }
  return ret;
}

template<int64_t N>
int ObFixedLengthString<N>::assign(const char *str)
{
  int ret = OB_SUCCESS;
  if (NULL == str) {
    buf_[0] = '\0';
  } else {
    int64_t str_len = strlen(str);
    if (str_len >= N) {
      ret = OB_BUF_NOT_ENOUGH;
      LIB_LOG(WARN, "buf is not long enough, truncate",
              K(N), "str len", str_len, K(ret));
      MEMCPY(buf_, str, N - 1);
      buf_[N - 1] = '\0';
    } else {
      MEMCPY(buf_, str, str_len);
      buf_[str_len] = '\0';
    }
  }
  return ret;
}

template<int64_t N>
int ObFixedLengthString<N>::assign_strive(const char *str)
{
  int ret = OB_SUCCESS;
  if (NULL == str) {
    buf_[0] = '\0';
  } else {
    STRNCPY(buf_, str, N);
    buf_[N-1] = '\0';
  }
  return ret;
}

template<int64_t N>
int ObFixedLengthString<N>::assign(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (str.length() >= N) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "buf is not long enough, truncate",
        K(N), "str len", str.length(), K(ret));
    MEMCPY(buf_, str.ptr(), N - 1);
    buf_[N - 1] = '\0';
  } else {
    MEMCPY(buf_, str.ptr(), str.length());
    buf_[str.length()] = '\0';
  }
  return ret;
}

template<int64_t N>
uint64_t ObFixedLengthString<N>::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(buf_, static_cast<int32_t>(strlen(buf_)), hash_val);
  return hash_val;
}

template<int64_t N>
int ObFixedLengthString<N>::hash(uint64_t &hash_val, uint64_t seed) const
{
  hash_val = murmurhash(buf_, static_cast<int32_t>(strlen(buf_)), seed);
  return OB_SUCCESS;
}

template<int64_t N>
int64_t ObFixedLengthString<N>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  pos = snprintf(buf, buf_len, "%s", buf_);
  if (pos < 0) {
    pos = 0;
  }
  return pos;
}

OB_SERIALIZE_MEMBER_TEMP(template<int64_t N>, ObFixedLengthString<N>, buf_);

}//end namespace common
}//end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_FIXED_LENGTH_STRING_H_
