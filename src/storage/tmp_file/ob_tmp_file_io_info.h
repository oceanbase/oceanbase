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
  TO_STRING_KV(K(fd_), K(dir_id_), KP(buf_), K(size_), K(disable_page_cache_), K(disable_block_cache_),
               K(prefetch_), K(io_timeout_ms_), K(io_desc_));

  int64_t fd_;
  int64_t dir_id_;
  char *buf_;
  int64_t size_;
  bool disable_page_cache_;
  bool disable_block_cache_;
  bool prefetch_;
  common::ObIOFlag io_desc_;
  int64_t io_timeout_ms_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_IO_INFO_H_
