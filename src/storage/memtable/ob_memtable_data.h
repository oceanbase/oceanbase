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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_DATA_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_DATA_

#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"

#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace memtable
{
// The structure actually written to MvccTransNode::buf_
class ObMemtableDataHeader
{
public:
  ObMemtableDataHeader(blocksstable::ObDmlFlag dml_flag, int64_t buf_len)
      : dml_flag_(dml_flag), buf_len_(buf_len)
  {}
  ~ObMemtableDataHeader() {}
  TO_STRING_KV(K_(dml_flag), K_(buf_len));
  inline int64_t dup_size() const { return (sizeof(ObMemtableDataHeader) + buf_len_); }
  inline int checksum(common::ObBatchChecksum &bc) const
  {
    int ret = common::OB_SUCCESS;
    if (buf_len_ <= 0) {
      ret = common::OB_NOT_INIT;
    } else {
      bc.fill(&dml_flag_, sizeof(dml_flag_));
      bc.fill(&buf_len_, sizeof(buf_len_));
      bc.fill(buf_, buf_len_);
    }
    return ret;
  }

  // the template parameter T supports ObMemtableData and ObMemtableDataHeader,
  // but in practice only ObMemtableDataHeader is involved in building.
  template<class T>
  static int build(ObMemtableDataHeader *new_data, const T *data)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(data->buf_len_ < 0)) {
      ret = OB_NOT_INIT;
    } else if (OB_ISNULL(data->buf_) || 0 == data->buf_len_) {
      // do nothing
    } else if (OB_ISNULL(new(new_data) ObMemtableDataHeader(data->dml_flag_, data->buf_len_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(new_data->buf_, data->buf_, data->buf_len_);
    }
    return ret;
  }
  blocksstable::ObDmlFlag dml_flag_;
  int64_t buf_len_;
  char buf_[0];
};

// only used for more conveniently passing parameters
class ObMemtableData
{
public:
  ObMemtableData(blocksstable::ObDmlFlag dml_flag, int64_t buf_len, const char *buf)
      : dml_flag_(dml_flag), buf_len_(buf_len), buf_(buf)
  {}
  ~ObMemtableData() {}
  TO_STRING_KV(K_(dml_flag), K_(buf_len));
  void set(blocksstable::ObDmlFlag dml_flag, const int64_t data_len, char *buf)
  {
    dml_flag_ = dml_flag;
    buf_len_ = data_len;
    buf_ = buf;
  }
  // must use the size of ObMemtableDataHeader, since the actual structure
  // involved in dup is always ObMemtableDataHeader
  inline int64_t dup_size() const { return (sizeof(ObMemtableDataHeader) + buf_len_); }

  blocksstable::ObDmlFlag dml_flag_;
  int64_t buf_len_;
  const char *buf_;
};

}
}

#endif // OCEANBASE_MEMTABLE_OB_MEMTABLE_DATA_
