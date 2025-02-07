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

#include "access/ob_table_access_param.h"
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "common/row/ob_row_iterator.h"
#include "storage/ob_i_store.h"
#include "storage/access/ob_dml_param.h"
#include "blocksstable/ob_datum_rowkey.h"
#include "blocksstable/ob_datum_row_iterator.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;

class ObValueRowIterator : public blocksstable::ObDatumRowIterator
{
  static const int64_t DEFAULT_ROW_NUM = 2;
  typedef common::ObSEArray<blocksstable::ObDatumRow*, DEFAULT_ROW_NUM> RowArray;
public:
  ObValueRowIterator();
  virtual ~ObValueRowIterator();
  virtual int init(bool unique);
  virtual int get_next_row(blocksstable::ObDatumRow *&row);
  virtual int add_row(blocksstable::ObDatumRow &row, const blocksstable::ObStorageDatumUtils &rowkey_datum_utils);
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

class ObSingleMerge;
class ObSingleRowGetter
{
  typedef common::ObFixedArray<int32_t, common::ObIAllocator> Projector;
  const ObQRIterType ITER_TYPE = T_SINGLE_GET;
public:
  ObSingleRowGetter(common::ObIAllocator &allocator, ObTablet &tablet);
  ~ObSingleRowGetter();

  int init_dml_access_ctx(
      ObStoreCtx &store_ctx,
      bool skip_read_lob = false);
  int init_dml_access_param(
      ObRelativeTable &data_table,
      const common::ObIArray<uint64_t> &out_col_ids,
      const bool skip_read_lob = false);
  int prepare_cached_iter_node(const ObDMLBaseParam &dml_param);
  ObTableAccessParam &get_access_param() { return access_param_; }
  ObTableAccessContext &get_access_ctx() { return access_ctx_; }
  void set_relative_table(ObRelativeTable *relative_table) { relative_table_ = relative_table; }
  int open(const blocksstable::ObDatumRowkey &rowkey, bool use_fuse_row_cache = false);
  int get_next_row(blocksstable::ObDatumRow *&row);
private:
  bool can_use_global_iter_pool(const ObDMLBaseParam &dml_param) const;
  int init_single_merge();
private:
  ObTablet *tablet_;
  ObSingleMerge *single_merge_;
  ObStoreCtx *store_ctx_;
  Projector output_projector_;
  ObTableAccessParam access_param_;
  ObTableAccessContext access_ctx_;
  ObGetTableParam get_table_param_;
  ObRelativeTable *relative_table_;
  common::ObIAllocator &allocator_;
  CachedIteratorNode *cached_iter_node_;
};
} // end namespace storage
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_VALUE_ROW_ITERATOR_
