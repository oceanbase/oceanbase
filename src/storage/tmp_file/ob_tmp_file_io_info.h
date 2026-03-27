/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_IO_INFO_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_IO_INFO_H_

#include "storage/tmp_file/ob_tmp_file_global.h"
#include "share/io/ob_io_define.h"

namespace oceanbase
{
namespace tmp_file
{
struct ObTmpFileIOInfo final
{
  ObTmpFileIOInfo();
  ~ObTmpFileIOInfo();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K(fd_), KP(buf_), K(size_), K(disable_page_cache_),
               K(prefetch_), K(io_timeout_ms_), K(io_desc_));

  int64_t fd_;
  char *buf_;
  int64_t size_;
  bool disable_page_cache_;
  bool prefetch_;
  common::ObIOFlag io_desc_;
  int64_t io_timeout_ms_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_IO_INFO_H_
