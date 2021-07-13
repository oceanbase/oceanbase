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

#ifndef OCEANBASE_STORAGE_OB_INDEX_MERGE_
#define OCEANBASE_STORAGE_OB_INDEX_MERGE_
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "common/row/ob_row_store.h"
#include "storage/ob_i_store.h"
#include "storage/ob_multiple_get_merge.h"
#include "storage/ob_query_iterator_util.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
namespace storage {
class ObTableScanParam;
class ObIndexMerge : public ObQueryRowIterator {
public:
  ObIndexMerge();
  virtual ~ObIndexMerge();
  virtual int init(const ObTableAccessParam& param, const ObTableAccessParam& index_param,
      ObTableAccessContext& context, const ObGetTableParam& get_table_param);
  virtual int get_next_row(ObStoreRow*& row);
  virtual void reset();
  virtual void reuse();
  int open(ObQueryRowIterator& index_iter);

  int save_curr_rowkey();
  int set_tables(const common::ObIArray<ObITable*>& tables);
  int switch_iterator(const int64_t range_array_idx);

private:
  static const int64_t MAX_NUM_PER_BATCH = 1000;
  ObQueryRowIterator* index_iter_;
  ObQueryRowIterator* main_iter_;
  int64_t rowkey_cnt_;
  const ObTableAccessParam* index_param_;
  ObTableAccessContext* access_ctx_;
  ObMultipleGetMerge table_iter_;
  GetRowkeyArray rowkeys_;
  common::ObArenaAllocator rowkey_allocator_;
  ObArray<int64_t> rowkey_range_idx_;
  int64_t index_range_array_cursor_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexMerge);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_INDEX_MERGE_
