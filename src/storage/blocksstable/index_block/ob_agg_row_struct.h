// Copyright (c) 2021 Ant Group CO., Ltd.
// OceanBase is licensed under Mulan PubL v1.
// You can use this software according to the terms and conditions of the Mulan PubL v1.
// You may obtain a copy of Mulan PubL v1 at:
//             http://license.coscl.org.cn/MulanPubL-1.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v1 for more details.

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_AGG_ROW_STRUCT_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_AGG_ROW_STRUCT_H

#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/encoding/ob_integer_array.h"
#include "storage/blocksstable/index_block/ob_index_block_util.h"
#include "lib/container/ob_fixed_array_iterator.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObAggRowHeader final
{
  // at most 4096 cols, at most 1K agg data
public:
  static const int64_t AGG_COL_TYPE_BITMAP_SIZE = 1; // 1 byte bitmap
  static const int64_t AGG_COL_MAX_OFFSET_SIZE = 2; // total size of agg_data < 1K, at most 64K
  static const int64_t AGG_ROW_HEADER_VERSION = 1;
public:
  ObAggRowHeader();
  ~ObAggRowHeader() = default;
  bool is_valid() const { return version_ == AGG_ROW_HEADER_VERSION && agg_col_cnt_ > 0
      && agg_col_idx_size_ > 0 && agg_col_idx_off_size_ > 0 && bitmap_size_ == AGG_COL_TYPE_BITMAP_SIZE; }
  TO_STRING_KV(K_(version), K_(length), K_(agg_col_cnt), K_(agg_col_idx_size),
      K_(agg_col_idx_off_size), K_(agg_col_off_size), K_(bitmap_size));
public:
  int16_t version_;
  int16_t length_;
  int16_t agg_col_cnt_;
  union
  {
    uint16_t pack_;
    struct
    {
      uint16_t agg_col_idx_size_      : 6;
      uint16_t agg_col_idx_off_size_  : 3;
      uint16_t agg_col_off_size_      : 3;
      uint16_t bitmap_size_           : 4;
    };
  };
};

struct ObAggRowHelper final
{
public:
  ObIntegerArrayGenerator col_idx_gen_;
  ObIntegerArrayGenerator col_idx_off_gen_;
  ObIntegerArrayGenerator col_off_gen_;
  ObIntegerArrayGenerator col_bitmap_gen_;
};

class ObAggRowWriter final
{
public:
  ObAggRowWriter();
  ~ObAggRowWriter();
  int init(const ObIArray<ObSkipIndexColMeta> &agg_col_arr,
           const ObDatumRow &agg_data,
           ObIAllocator &allocator);
  int64_t get_data_size() { return estimate_data_size_ + header_size_; }
  int write_agg_data(char *buf, const int64_t buf_size, int64_t &pos);
  void reset();
private:
  int sort_metas(const ObIArray<ObSkipIndexColMeta> &agg_col_arr, ObIAllocator &allocator);
  int calc_estimate_data_size();
  int write_cell(
      int64_t start,
      int64_t end,
      int64_t nop_count,
      char *buf,
      int64_t &pos);
private:
  static const int64_t DEFAULT_AGG_COL_CNT = 4;
  typedef std::pair<ObSkipIndexColMeta, int64_t> COL_META;
  typedef common::ObFixedArray<COL_META, common::ObIAllocator> ColMetaList;

private:
  bool is_inited_;
  const ObStorageDatum *agg_datums_;
  int64_t column_count_;
  int64_t col_idx_count_;
  int64_t estimate_data_size_;
  ColMetaList col_meta_list_;
  ObAggRowHeader header_;
  int64_t header_size_;
  ObAggRowHelper row_helper_;
  DISALLOW_COPY_AND_ASSIGN(ObAggRowWriter);
};


class ObAggRowReader final
{
public:
  ObAggRowReader();
  ~ObAggRowReader();
  int init(const char *buf, const int64_t buf_size); // init with agg_buf
  int read(const ObSkipIndexColMeta &meta, ObDatum &datum); // use meta get datum
  void reset();
private:
  int inner_init(const char *buf, const int64_t buf_size);
  int binary_search_col(const int64_t col_idx, int64_t &pos);
  int find_col(const int64_t pos, const int64_t type, ObDatum &datum);
  int read_cell(
      const char *cell_buf, const int64_t buf_size, const int64_t type,
      bool &found, int64_t &col_off, int64_t &col_len);

private:
  bool is_inited_;
  const char *buf_;
  int64_t buf_size_;
  int64_t header_size_;
  const ObAggRowHeader *header_;
  ObAggRowHelper row_helper_;
  DISALLOW_COPY_AND_ASSIGN(ObAggRowReader);
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif