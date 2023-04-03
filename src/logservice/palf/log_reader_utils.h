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

#ifndef OCEANBASE_LOGSERVICE_LOG_READER_UTILS_
#define OCEANBASE_LOGSERVICE_LOG_READER_UTILS_
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace palf
{
struct ReadBuf
{
  ReadBuf();
  ReadBuf(char *buf, const int64_t buf_len);
  ReadBuf(const ReadBuf &rhs);
  bool operator==(const ReadBuf &rhs) const;
  bool operator!=(const ReadBuf &rhs) const;

  ReadBuf &operator=(const ReadBuf &rhs);
  ~ReadBuf();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K(buf_len_), KP(buf_));

  char *buf_;
  int64_t buf_len_;
};

struct ReadBufGuard
{
  ReadBufGuard(const char *label, const int64_t buf_len);
  ~ReadBufGuard();
  ReadBuf read_buf_;
};

int alloc_read_buf(const char *label, const int64_t buf_len, ReadBuf &read_buf);
void free_read_buf(ReadBuf &read_buf);
} // end of logservice
} // end of oceanbase

#endif
