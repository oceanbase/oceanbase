/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  bool is_valid_raw_read_buf();
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

bool is_valid_raw_read_buf(const ReadBuf &raw_read_buf,
                           const int64_t offset,
                           const int64_t nbytes);

} // end of logservice
} // end of oceanbase

#endif
