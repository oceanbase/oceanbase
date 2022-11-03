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

#ifndef  OCEANBASE_UNITTEST_MEMTABLE_MOCK_ROW_H_
#define  OCEANBASE_UNITTEST_MEMTABLE_MOCK_ROW_H_

#include "storage/memtable/ob_memtable_interface.h"
#include "storage/ob_i_store.h"
#include "storage/access/ob_store_row_iterator.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;
using namespace oceanbase::storage;

class ObMtRowIterator : public ObStoreRowIterator
{
public:
  ObMtRowIterator() : cursor_(0) {}
  ~ObMtRowIterator() {}
  int get_next_row(const ObStoreRow *&row)
  {
    int ret = OB_SUCCESS;
    if (cursor_ < rows_.count()) {
      row = &rows_[cursor_++];
    } else {
      ret = OB_ITER_END;
    }
    return ret;
  }
  void reset() { cursor_ = 0; rows_.reset(); }
  void reset_iter() { cursor_ = 0; }
  void add_row(const ObStoreRow &row)
  {
    rows_.push_back(row);
  }
private:
  int64_t cursor_;
  ObSEArray<ObStoreRow, 64> rows_;
};

}
}

#endif //OCEANBASE_UNITTEST_MEMTABLE_MOCK_ROW_H_


