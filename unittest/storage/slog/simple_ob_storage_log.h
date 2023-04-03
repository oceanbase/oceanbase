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

#ifndef OCEANBASE_SIMPLE_OB_STORAGE_LOG_H
#define OCEANBASE_SIMPLE_OB_STORAGE_LOG_H

#define USING_LOG_PREFIX STORAGE_REDO

#include "lib/list/ob_list.h"
#include "lib/file/file_directory_utils.h"
#include "lib/file/ob_file.h"
#include "lib/random/ob_random.h"
#include "lib/allocator/ob_mod_define.h"
#include "storage/slog/ob_storage_log_struct.h"

namespace oceanbase
{
using namespace common;

namespace storage
{

class SimpleObSlog : public ObIBaseStorageLogEntry
{
public:
  SimpleObSlog();
  SimpleObSlog(const int64_t data_len, const char content);
  ~SimpleObSlog();
  bool operator == (const SimpleObSlog &slog);
  bool is_valid() const;
  void init(const int64_t data_len_, const char content);
  void destroy();
  void set(const int64_t data_len, const char content);

  char *buf_;
  int64_t len_;

  TO_STRING_EMPTY();
  NEED_SERIALIZE_AND_DESERIALIZE;
};

SimpleObSlog::SimpleObSlog()
  : buf_(nullptr), len_(0)
{
}

SimpleObSlog::SimpleObSlog(const int64_t data_len, const char content)
{
  buf_ = new char[data_len];
  MEMSET(buf_, content, data_len);
  len_ = data_len;
}

SimpleObSlog::~SimpleObSlog()
{
  if (nullptr != buf_) {
    delete[] buf_;
    buf_ = nullptr;
  }
}

void SimpleObSlog::set(const int64_t data_len, const char content)
{
  if (buf_ == nullptr && len_ == 0) {
    len_ = data_len;
    buf_ = new char[data_len];
    MEMSET(buf_, content, data_len);
  }
}

bool SimpleObSlog::operator == (const SimpleObSlog &slog)
{
  return 0 == STRCMP(buf_, slog.buf_) && len_ == slog.len_;
}

bool SimpleObSlog::is_valid() const
{
  return true;
}

DEFINE_SERIALIZE(SimpleObSlog)
{
  int ret = OB_SUCCESS;

  if (nullptr == buf || buf_len <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument.", K(buf_len), K(pos));
  } else if (pos + len_ > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_REDO_LOG(WARN, "Buffer is not enough.", K_(len), K(pos), K(buf_len));
  } else {
    MEMCPY(buf + pos, buf_, len_);
    pos += len_;
  }

  return ret;
}

DEFINE_DESERIALIZE(SimpleObSlog)
{
  UNUSEDx(buf, data_len, pos);
  return OB_NOT_SUPPORTED;
}

DEFINE_GET_SERIALIZE_SIZE(SimpleObSlog)
{
  return len_;
}

}
}

#endif