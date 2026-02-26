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

#ifndef OB_STORAGE_MEMTABLE_OB_MEMTABLE_BLOCK_READER_H_
#define OB_STORAGE_MEMTABLE_OB_MEMTABLE_BLOCK_READER_H_

#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"

namespace oceanbase {
using namespace storage;
using namespace blocksstable;

namespace storage
{
  struct ObFilterResult;
}

namespace memtable {
class ObMemtableSingleRowReader;

// This class provides rows cache and flter pushdown function for memtable.
class ObMemtableBlockReader : public blocksstable::ObIMicroBlockReader {
public:
  static const int64_t MAX_BATCH_SIZE = 225;

public:
  ObMemtableBlockReader(common::ObIAllocator &allocator,
                        ObMemtableSingleRowReader &single_row_reader)
      : has_lob_out_row_(false),
      is_single_version_rows_(true),
      is_delete_insert_(false),
      allocator_(allocator),
      inner_allocator_("MtlBlkReader", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      single_row_reader_(single_row_reader)
  {
    reader_type_ = MemtableReader;
  }
  int init(const bool is_delete_insert);
  void reuse();
  void reset();

  int capacity() const
  { return MAX_BATCH_SIZE; }
  int get_row_count() const
  { return row_count_; }
  int prefetch_rows();
  int get_next_di_row(const ObFilterResult &filter_res,
                      int64_t &current,
                      ObDatumRow &row);
  int filter_pushdown_filter(
      const sql::ObPushdownFilterExecutor *parent,
      sql::ObPushdownFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      common::ObBitmap &result_bitmap) override;

public:
  /********** derived from ObIMicroBlockReader **********/
  virtual int get_row(const int64_t index, ObDatumRow &row) override;

  virtual bool has_lob_out_row() const override { return has_lob_out_row_; }
  bool is_single_version_rows() const { return is_single_version_rows_; }

  /********** derived from ObIMicroBlockReader **********/
  TO_STRING_KV(K_(is_inited), K_(has_lob_out_row), K_(is_single_version_rows), K_(is_delete_insert), K_(reader_type),
               K_(row_count), KPC_(read_info));
private:
  bool has_lob_out_row_;
  bool is_single_version_rows_;
  bool is_delete_insert_;
  ObIAllocator &allocator_;
  common::ObArenaAllocator inner_allocator_;
  ObMemtableSingleRowReader &single_row_reader_;
  blocksstable::ObDatumRow rows_[MAX_BATCH_SIZE];
};

}  // namespace memtable
}  // namespace oceanbase
#endif
