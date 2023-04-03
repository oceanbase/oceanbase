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

#ifndef OCEANBASE_MVCC_OB_ROW_DATA_
#define OCEANBASE_MVCC_OB_ROW_DATA_
#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace memtable
{
struct ObRowData
{
  ObRowData(): data_(NULL), size_(0) {}
  ~ObRowData() {}
  void reset()
  {
    data_ = NULL;
    size_ = 0;
  }
  void set(const char *data, const int32_t size)
  {
    data_ = data;
    size_ = size;
  }
  bool operator==(const ObRowData &that) const
  {
    return this->size_ == that.size_
           && (size_ <= 0
               || (NULL != this->data_ && NULL != that.data_ && 0 == MEMCMP(this->data_, that.data_, size_)));
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos);
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  TO_STRING_KV(KP_(data), K_(size));
  const char *data_;
  int32_t size_;
};

}; // end namespace mvcc
}; // end namespace oceanbase

#endif /* OCEANBASE_MVCC_OB_ROW_DATA_ */
