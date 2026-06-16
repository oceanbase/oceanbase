/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BLOCKSSTABLE_OB_COLUMN_CHECKSUM_CALCULATOR_H_
#define OCEANBASE_BLOCKSSTABLE_OB_COLUMN_CHECKSUM_CALCULATOR_H_

#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace compaction
{

class ObColumnChecksumCalculator
{
public:
  ObColumnChecksumCalculator();
  virtual ~ObColumnChecksumCalculator();
  int init(const int64_t column_cnt);
  int calc_column_checksum(
      const ObIArray<share::schema::ObColDesc> &col_descs,
      const blocksstable::ObDatumRow *new_row,
      const blocksstable::ObDatumRow *old_row,
      const bool *is_column_changed);
  int64_t *get_column_checksum() const { return column_checksum_; }
  int64_t get_column_count() const { return column_cnt_; }
  TO_STRING_KV(KP_(column_checksum), K_(column_cnt));
private:
  int calc_column_checksum(
      const ObIArray<share::schema::ObColDesc> &col_descs,
      const blocksstable::ObDatumRow &row,
      const bool new_row,
      const bool *column_changed,
      int64_t *column_checksum);
private:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  int64_t *column_checksum_;
  int64_t column_cnt_;
};

}  // end namespace compaction
}  // end namespace oceanbase

#endif  // OCEANBASE_BLOCKSSTABLE_OB_COLUMN_CHECKSUM_CALCULATOR_H_
