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

#ifndef OB_STORAGE_MEMTABLE_OB_MEMTABLE_SINGLE_ROW_READER_H_
#define OB_STORAGE_MEMTABLE_OB_MEMTABLE_SINGLE_ROW_READER_H_

#include "storage/memtable/mvcc/ob_mvcc_iterator.h"
#include "storage/memtable/ob_nop_bitmap.h"

namespace oceanbase {

namespace memtable {

class ObMemtableSingleRowReader {
public:
  ObMemtableSingleRowReader()
      : is_range_scan_(false),
        row_has_been_gotten_(false),
        param_(nullptr),
        read_info_(nullptr),
        context_(nullptr),
        memtable_(nullptr),
        row_iter_(),
        private_row_(),
        bitmap_() {}

  void reset();
  int init(ObMemtable *memtable, const ObTableIterParam &param, ObTableAccessContext &context);
  int init_a_new_range(const blocksstable::ObDatumRange &new_range_to_scan);
  int get_next_row(const blocksstable::ObDatumRow *&row);
  int fill_in_next_row(blocksstable::ObDatumRow &row);
  int fill_in_next_delete_insert_row(blocksstable::ObDatumRow &latest_row,
                                     blocksstable::ObDatumRow &earliest_row,
                                     int64_t &acquired_row_cnt);
  const ObITableReadInfo *get_read_info() { return read_info_; }
  TO_STRING_KV(K_(is_range_scan), K_(row_has_been_gotten), KPC_(read_info),
               K_(private_row), K_(cur_range));

private:
  int check_is_range_scan_(const blocksstable::ObDatumRange &new_range_to_scan);
  int get_real_range_(const blocksstable::ObDatumRange &range, blocksstable::ObDatumRange &real_range);
  int inner_fill_in_next_row_(blocksstable::ObDatumRow &next_row,
                              blocksstable::ObDatumRow &delete_row,
                              int64_t &acquired_row_cnt);
  int get_next_value_iter_(const ObMemtableKey *&key, ObMvccValueIterator *&value_iter);
  int fill_in_next_row_by_value_iter_(const ObMemtableKey *key,
                                      ObMvccValueIterator *value_iter,
                                      blocksstable::ObDatumRow &next_row,
                                      blocksstable::ObDatumRow &delete_row,
                                      int64_t &acquired_row_cnt);
  int fill_in_row_scn_(const int64_t row_scn, const ObMvccValueIterator *value_iter, blocksstable::ObDatumRow &new_row);

private:
  bool is_range_scan_;
  // used when is_range_scan_ == false to stop get row from memtable directly
  bool row_has_been_gotten_;
  const storage::ObTableIterParam *param_;
  const storage::ObITableReadInfo *read_info_;
  storage::ObTableAccessContext *context_;
  ObMemtable *memtable_;
  ObMvccRowIterator row_iter_;
  blocksstable::ObDatumRow private_row_;
  blocksstable::ObDatumRange cur_range_;
  ObNopBitMap bitmap_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif
