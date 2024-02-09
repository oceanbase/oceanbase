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
#ifndef OB_STORAGE_COLUMN_STORE_OB_CG_TILE_SCANNER_H_
#define OB_STORAGE_COLUMN_STORE_OB_CG_TILE_SCANNER_H_
#include "ob_i_cg_iterator.h"

namespace oceanbase
{
namespace storage
{
class ObCGScanner;
class ObCGRowScanner;
class ObCGTileScanner : public ObICGIterator
{
public:
  ObCGTileScanner() :
      ObICGIterator(),
      is_inited_(false),
      is_reverse_scan_(false),
      sql_batch_size_(0),
      access_ctx_(nullptr),
      cg_scanners_(),
      datum_row_()
  {}
  virtual ~ObCGTileScanner() { reset(); }
  virtual int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override
  { return OB_NOT_SUPPORTED; }
  int switch_context(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override
  { return OB_NOT_SUPPORTED; }
  int init(
      const ObIArray<ObTableIterParam*> &iter_params,
      const bool project_single_row,
      const bool project_without_filter,
      ObTableAccessContext &access_ctx,
      ObITable *table);
  int switch_context(
      const ObIArray<ObTableIterParam*> &iter_params,
      const bool project_single_row,
      const bool project_without_filter,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const bool col_cnt_changed);
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
      ObCGBitmap &result_bitmap) override;
  virtual int get_next_rows(uint64_t &count, const uint64_t capacity) override;
  virtual int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
  virtual ObCGIterType get_type() override final
  { return OB_CG_TILE_SCANNER; }
  OB_INLINE common::ObIArray<ObICGIterator*> &get_inner_cg_scanners() { return cg_scanners_; }
  TO_STRING_KV(K_(is_inited), K_(is_reverse_scan), K_(sql_batch_size), KP_(access_ctx));

private:
  int get_next_aligned_rows(ObCGRowScanner *cg_scanner, const uint64_t target_row_count);
  int get_next_aggregated_rows(ObCGScanner *cg_scanner);
  bool is_inited_;
  bool is_reverse_scan_;
  int64_t sql_batch_size_;
  ObTableAccessContext* access_ctx_;
  common::ObFixedArray<ObICGIterator*, common::ObIAllocator> cg_scanners_;
  blocksstable::ObDatumRow datum_row_;
};
}
}

#endif
