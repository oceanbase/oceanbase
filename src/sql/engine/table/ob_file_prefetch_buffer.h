/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_FILE_PREFETCH_BUFFER_H
#define OB_FILE_PREFETCH_BUFFER_H

#include "share/ob_i_tablet_scan.h"
#include "lib/file/ob_file.h"
#include "sql/engine/table/ob_external_table_access_service.h"

namespace oceanbase {
namespace sql {
class ObFilePrefetchBuffer
{
public:
  ObFilePrefetchBuffer(ObExternalDataAccessDriver &file_reader) :
    offset_(0), length_(0), buffer_size_(0), buffer_(nullptr), file_reader_(file_reader),
    alloc_(common::ObMemAttr(MTL_ID(), "PrefetchBuffer"))
  {}
  ~ObFilePrefetchBuffer()
  {
    destroy();
  }
  void clear();
  void destroy();
  int prefetch(const int64_t file_offset, const int64_t size);
  bool in_prebuffer_range(const int64_t position, const int64_t nbytes);
  // NOTE: before calling, make sure it is within the buffer range.
  void fetch(const int64_t position, const int64_t nbytes, void *out);

private:
  int64_t offset_;
  int64_t length_;
  int64_t buffer_size_;
  void *buffer_;
  ObExternalDataAccessDriver &file_reader_;
  common::ObMalloc alloc_;
};
}
}

#endif // OB_FILE_PREFETCH_BUFFER_H
