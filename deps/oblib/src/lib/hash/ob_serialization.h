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

#ifndef  OCEANBASE_COMMON_HASH_SERIALIZATION_
#define  OCEANBASE_COMMON_HASH_SERIALIZATION_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "lib/hash/ob_hashutils.h"

namespace oceanbase
{
namespace common
{
namespace hash
{
template <class _archive, class _value>
int serialization(_archive &ar, const _value &value)
{
  return (const_cast<_value &>(value)).serialization(ar);
}
template <class _archive, class _value>
int deserialization(_archive &ar, _value &value)
{
  return value.deserialization(ar);
}

class SimpleArchive
{
public:
  SimpleArchive() : fd_(-1) {}
  ~SimpleArchive()
  {
    if (-1 != fd_) {
      destroy();
    }
  };
public:
  int init(const char *filename, int flag)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(filename) || (FILE_OPEN_RFLAG != flag && FILE_OPEN_WFLAG != flag)) {
      HASH_WRITE_LOG(HASH_WARNING, "invalid param filename=%p flag=%x", filename, flag);
      ret = OB_INVALID_ARGUMENT;
    } else if (-1 == (fd_ = open(filename, flag, FILE_OPEN_MODE))) {
      HASH_WRITE_LOG(HASH_WARNING, "open file fail, filename=[%s] flag=%x errno=%u",
          filename, flag, errno);
      ret = OB_ERR_SYS;
    } else {
      // do nothing
    }
    return ret;
  };
  void destroy()
  {
    if (-1 == fd_) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_NOT_INIT, "have not inited");
    } else {
      close(fd_);
    }
  };
  int push(const void *data, int64_t size)
  {
    int ret = OB_SUCCESS;
    ssize_t write_ret = 0;
    if (-1 == fd_) {
      HASH_WRITE_LOG(HASH_WARNING, "have not inited");
      ret = OB_NOT_INIT;
    } else if (OB_ISNULL(data) || 0 == size) {
      HASH_WRITE_LOG(HASH_WARNING, "invalid param data=%p size=%ld", data, size);
      ret = OB_INVALID_ARGUMENT;
    } else if (size != (int64_t)(write_ret = write(fd_, data, size))) {
      HASH_WRITE_LOG(HASH_WARNING, "write fail errno=%u fd_=%d data=%p size=%ld write_ret=%ld",
          errno, fd_, data, size, write_ret);
      ret = OB_ERR_SYS;
    } else {
      // do nothing
    }
    return ret;
  };
  int pop(void *data, int64_t size)
  {
    int ret = OB_SUCCESS;
    ssize_t read_ret = 0;
    if (-1 == fd_) {
      HASH_WRITE_LOG(HASH_WARNING, "have not inited");
      ret = OB_NOT_INIT;
    } else if (OB_ISNULL(data) || 0 == size) {
      HASH_WRITE_LOG(HASH_WARNING, "invalid param data=%p size=%ld", data, size);
      ret = OB_INVALID_ARGUMENT;
    } else if (size != (int64_t)(read_ret = read(fd_, data, size))) {
      HASH_WRITE_LOG(HASH_WARNING, "read fail errno=%u fd_=%d data=%p size=%ld read_ret=%ld",
          errno, fd_, data, size, read_ret);
      ret = OB_ERR_SYS;
    } else {
      // do nothing
    }
    return ret;
  };
private:
  DISALLOW_COPY_AND_ASSIGN(SimpleArchive);
public:
  static const int FILE_OPEN_RFLAG = O_CREAT | O_RDONLY;
  static const int FILE_OPEN_WFLAG = O_CREAT | O_TRUNC | O_WRONLY;
private:
  static const mode_t FILE_OPEN_MODE = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
private:
  int fd_;
};

#define _SERIALIZATION_SPEC(type) \
  template <class _archive> \
  int serialization(_archive &ar, type &value) \
  { \
    return ar.push(&value, sizeof(value)); \
  }
#define _DESERIALIZATION_SPEC(type) \
  template <class _archive> \
  int deserialization(_archive &ar, type &value) \
  { \
    return ar.pop(&value, sizeof(value)); \
  }
_SERIALIZATION_SPEC(int8_t);
_SERIALIZATION_SPEC(uint8_t);
_SERIALIZATION_SPEC(const int8_t);
_SERIALIZATION_SPEC(const uint8_t);
_SERIALIZATION_SPEC(int16_t);
_SERIALIZATION_SPEC(uint16_t);
_SERIALIZATION_SPEC(const int16_t);
_SERIALIZATION_SPEC(const uint16_t);
_SERIALIZATION_SPEC(int32_t);
_SERIALIZATION_SPEC(uint32_t);
_SERIALIZATION_SPEC(const int32_t);
_SERIALIZATION_SPEC(const uint32_t);
_SERIALIZATION_SPEC(int64_t);
_SERIALIZATION_SPEC(uint64_t);
_SERIALIZATION_SPEC(const int64_t);
_SERIALIZATION_SPEC(const uint64_t);
_SERIALIZATION_SPEC(float);
_SERIALIZATION_SPEC(const float);
_SERIALIZATION_SPEC(double);
_SERIALIZATION_SPEC(const double);

_DESERIALIZATION_SPEC(int8_t);
_DESERIALIZATION_SPEC(uint8_t);
_DESERIALIZATION_SPEC(int16_t);
_DESERIALIZATION_SPEC(uint16_t);
_DESERIALIZATION_SPEC(int32_t);
_DESERIALIZATION_SPEC(uint32_t);
_DESERIALIZATION_SPEC(int64_t);
_DESERIALIZATION_SPEC(uint64_t);
_DESERIALIZATION_SPEC(float);
_DESERIALIZATION_SPEC(double);

template <class _archive>
int serialization(_archive &ar, const HashNullObj &value)
{
  UNUSEDx(ar, value);
  return OB_SUCCESS;
}

template <class _archive>
int deserialization(_archive &ar, HashNullObj &value)
{
  UNUSEDx(ar, value);
  return OB_SUCCESS;
}

template <class _archive, typename _T1, typename _T2>
int serialization(_archive &ar, const HashMapPair<_T1, _T2> &pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization(ar, pair.first))
      || OB_FAIL(serialization(ar, pair.second))) {
  }
  return ret;
}

template <class _archive, typename _T1, typename _T2>
int deserialization(_archive &ar, HashMapPair<_T1, _T2> &pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deserialization(ar, pair.first))
      || OB_FAIL(deserialization(ar, pair.second))) {
  }
  return ret;
}
} // hash
} // common
} // ocenabase

#endif // OCEANBASE_COMMON_HASH_SERIALIZATION_
