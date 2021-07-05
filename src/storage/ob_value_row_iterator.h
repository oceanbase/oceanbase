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

#ifndef OCEANBASE_STORAGE_OB_VALUE_ROW_ITERATOR_
#define OCEANBASE_STORAGE_OB_VALUE_ROW_ITERATOR_

#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "common/row/ob_row_iterator.h"
#include "common/rowkey/ob_rowkey.h"
#include "storage/ob_i_store.h"
namespace oceanbase {
namespace storage {
class ObValueRowIterator : public common::ObNewRowIterator {
  static const int64_t DEFAULT_ROW_NUM = 2;
  typedef common::ObSEArray<common::ObNewRow, DEFAULT_ROW_NUM> RowArray;

public:
  ObValueRowIterator();
  virtual ~ObValueRowIterator();
  virtual int init(bool unique);
  virtual int get_next_row(common::ObNewRow*& row);
  virtual int get_next_rows(common::ObNewRow*& rows, int64_t& row_count);
  virtual int add_row(common::ObNewRow& row);
  virtual void reset();

private:
  bool is_inited_;
  bool unique_;
  common::ObArenaAllocator allocator_;
  RowArray rows_;
  int64_t cur_idx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObValueRowIterator);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_VALUE_ROW_ITERATOR_
