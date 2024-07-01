/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OB_STORAGE_COLUMN_STORE_OB_CG_SCANNER_H_
#define OB_STORAGE_COLUMN_STORE_OB_CG_SCANNER_H_
#include "ob_i_cg_iterator.h"
#include "ob_cg_prefetcher.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/column_store/ob_column_oriented_sstable.h"

namespace oceanbase
{
namespace storage
{
class ObCGScanner : public ObICGIterator
{
public:
  ObCGScanner() :
      ObICGIterator(),
      is_inited_(false),
      is_reverse_scan_(false),
      is_new_range_(false),
      sstable_row_cnt_(OB_INVALID_CS_ROW_ID),
      current_(OB_INVALID_CS_ROW_ID),
      query_index_range_(),
      sstable_(nullptr),
      table_wrapper_(),
      iter_param_(nullptr),
      access_ctx_(nullptr),
      prefetcher_(),
      micro_scanner_(nullptr),
      macro_block_reader_()
  {}
  virtual ~ObCGScanner();
  virtual int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override;
  virtual int switch_context(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override;
  virtual void reset() override;
  virtual void reuse() override;
  virtual int locate(
      const ObCSRange &range,
      const ObCGBitmap *bitmap = nullptr) override;
  virtual int apply_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::PushdownFilterInfo &filter_info,
      const int64_t row_count,
      const ObCGBitmap *parent_bitmap,
      ObCGBitmap &result_bitmap) override final;
  virtual int get_next_rows(uint64_t &count, const uint64_t capacity) override
  {
    UNUSEDx(count, capacity);
    return OB_NOT_SUPPORTED;
  }
  virtual ObCGIterType get_type() override
  { return OB_CG_SCANNER; }
  static bool can_skip_filter(const sql::ObPushdownFilterExecutor &parent,
                              const ObCGBitmap &parent_bitmap,
                              const ObCSRange &row_range);
  int get_next_valid_block(sql::ObPushdownFilterExecutor *parent,
                           const ObCGBitmap *parent_bitmap,
                           ObCGBitmap &result_bitmap);
  int build_index_filter(sql::ObPushdownFilterExecutor &filter);
  TO_STRING_KV(K_(is_inited), K_(is_reverse_scan), K_(is_new_range), K_(current),
               K_(query_index_range), K_(prefetcher), K_(sstable_row_cnt));

protected:
  bool start_of_scan() const;
  bool end_of_scan() const;
  int open_cur_data_block();
  int init_micro_scanner();
  void get_data_range(const ObMicroIndexInfo &data_info, ObCSRange &range);

private:
  int inner_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::PushdownFilterInfo &filter_info,
      const ObCGBitmap *parent_bitmap,
      ObCGBitmap &result_bitmap);
  OB_INLINE int64_t row_count_left()
  {
    return is_reverse_scan_ ? (current_ - query_index_range_.start_row_id_ + 1) :
        (query_index_range_.end_row_id_ - current_ + 1);
  }

protected:
  bool is_inited_;
  bool is_reverse_scan_;
  bool is_new_range_;
  uint64_t sstable_row_cnt_;
  ObCSRowId current_;
  ObCSRange query_index_range_;
  ObSSTable *sstable_;
  ObSSTableWrapper table_wrapper_;
  const ObTableIterParam *iter_param_;
  ObTableAccessContext *access_ctx_;
  ObCGPrefetcher prefetcher_;
  ObIMicroBlockRowScanner *micro_scanner_;
  ObMacroBlockReader macro_block_reader_;
};


class ObCGRowScanner : public ObCGScanner
{
public:
  ObCGRowScanner() :
      ObCGScanner(),
      row_ids_(nullptr),
      len_array_(nullptr),
      cell_data_ptrs_(nullptr),
      filter_bitmap_(nullptr),
      read_info_(nullptr)
  {}
  virtual ~ObCGRowScanner();
  virtual void reset() override;
  virtual void reuse() override;
  virtual int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override;
  virtual int get_next_rows(uint64_t &count, const uint64_t capacity) override;
  virtual int locate(
      const ObCSRange &range,
      const ObCGBitmap *bitmap = nullptr) override;
  virtual ObCGIterType get_type() override
  { return OB_CG_ROW_SCANNER; }
  int get_next_rows(uint64_t &count, const uint64_t capacity, const int64_t datum_offset);
  int deep_copy_projected_rows(const int64_t datum_offset, const uint64_t count);
  void set_project_type(const bool project_without_filter)
  { return prefetcher_.set_project_type(project_without_filter); }
  TO_STRING_KV(K_(is_inited), K_(is_reverse_scan), K_(current),
               K_(query_index_range), K_(prefetcher), KP_(filter_bitmap));

private:
  int fetch_rows(const int64_t batch_size, uint64_t &count, const int64_t datum_offset);
  virtual int inner_fetch_rows(const int64_t row_cap, const int64_t datum_offset);

protected:
  int32_t *row_ids_;
  uint32_t *len_array_;
  // for projection in vectorize, need to remove later
  const char **cell_data_ptrs_;
  const ObCGBitmap *filter_bitmap_;
  const ObITableReadInfo *read_info_;
  common::ObFixedArray<ObSqlDatumInfo, common::ObIAllocator> datum_infos_;
  private:
  common::ObFixedArray<const share::schema::ObColumnParam*, common::ObIAllocator> col_params_;
};

class ObCGSingleRowScanner : public ObCGScanner
{
public:
  ObCGSingleRowScanner() = default;
  virtual ~ObCGSingleRowScanner() = default;
  virtual ObCGIterType get_type() override
  { return OB_CG_SINGLE_ROW_SCANNER; }
  virtual int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
private:
  int fetch_row(const blocksstable::ObDatumRow *&datum_row);
  int inner_fetch_row(const blocksstable::ObDatumRow *&datum_row);
};

}
}
#endif
