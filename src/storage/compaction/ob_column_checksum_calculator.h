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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_COLUMN_CHECKSUM_CALCULATOR_H_
#define OCEANBASE_BLOCKSSTABLE_OB_COLUMN_CHECKSUM_CALCULATOR_H_

#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTableMeta;
}
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

class ObColumnChecksumAccumulator
{
public:
  ObColumnChecksumAccumulator();
  virtual ~ObColumnChecksumAccumulator();
  int init(const int64_t column_cnt);
  int add_column_checksum(const int64_t column_cnt,
      const int64_t *column_checksum);
  int64_t *get_column_checksum() const { return column_checksum_; }
  int64_t get_column_count() const { return column_cnt_; }
private:
  bool is_inited_;
  int64_t *column_checksum_;
  common::ObArenaAllocator allocator_;
  int64_t column_cnt_;
  lib::ObMutex lock_;
};

class ObSSTableColumnChecksum
{
public:
  ObSSTableColumnChecksum();
  virtual ~ObSSTableColumnChecksum();
  int init(const int64_t concurrent_cnt,
      const share::schema::ObTableSchema &table_schema,
      const bool need_org_checksum,
      const common::ObIArray<const blocksstable::ObSSTableMeta*> &org_sstable_metas);
  int get_checksum_calculator(
      const int64_t idx,
      compaction::ObColumnChecksumCalculator *&checksum_calc);
  int accumulate_task_checksum();
  int64_t *get_column_checksum() const { return accumulator_.get_column_checksum(); }
  int64_t get_column_count() const { return accumulator_.get_column_count(); }
private:
  int init_checksum_calculators(
      const int64_t concurrent_cnt,
      const share::schema::ObTableSchema &table_schema);
  int init_column_checksum(
      const share::schema::ObTableSchema &table_schema,
      const bool need_org_checksum,
      const common::ObIArray<const blocksstable::ObSSTableMeta *> &org_sstable_metas);
  void destroy();
private:
  bool is_inited_;
  common::ObSEArray<compaction::ObColumnChecksumCalculator *, OB_DEFAULT_SE_ARRAY_COUNT> checksums_;
  ObColumnChecksumAccumulator accumulator_;
  common::ObArenaAllocator allocator_;
};

}  // end namespace compaction
}  // end namespace oceanbase

#endif  // OCEANBASE_BLOCKSSTABLE_OB_COLUMN_CHECKSUM_CALCULATOR_H_
