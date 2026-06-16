/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_BLOCKSSTABLE_ENCODING_OB_IMICRO_BLOCK_DECODER_H_
#define OB_STORAGE_BLOCKSSTABLE_ENCODING_OB_IMICRO_BLOCK_DECODER_H_

#include "lib/container/ob_bitmap.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/ob_i_store.h"
#include "ob_icolumn_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

class ObIMicroBlockDecoder : public ObIMicroBlockReader
{
public:
  ObIMicroBlockDecoder()
    : ObIMicroBlockReader(),
      request_cnt_(0),
      decoder_allocator_(lib::ObLabel("OB_DECODER_CTX"))
  {}
  virtual ~ObIMicroBlockDecoder() {}
  virtual int compare_rowkey(
    const ObDatumRowkey &rowkey, const int64_t index, int32_t &compare_result, const int64_t common_prefix_len = 0) override = 0;
  virtual int compare_rowkey(const ObDatumRange &range, const int64_t index,
    int32_t &start_key_compare_result, int32_t &end_key_compare_result) = 0;

  // Filter interface for filter pushdown
  virtual int filter_pushdown_filter(const sql::ObPushdownFilterExecutor *parent,
    sql::ObPhysicalFilterExecutor &filter, const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap) = 0;
  virtual int filter_pushdown_filter(const sql::ObPushdownFilterExecutor *parent,
    sql::ObWhiteFilterExecutor &filter, const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap) = 0;
  virtual int get_rows(const common::ObIArray<int32_t> &cols,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params, const bool is_padding_mode,
    const int32_t *row_ids, const char **cell_datas, const int64_t row_cap,
    common::ObIArray<ObSqlDatumInfo> &datum_infos, const int64_t datum_offset = 0) = 0;
  virtual bool can_apply_black(const common::ObIArray<int32_t> &col_offsets) const = 0;
  virtual int filter_black_filter_batch(const sql::ObPushdownFilterExecutor *parent,
    sql::ObBlackFilterExecutor &filter, sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap, bool &filter_applied) = 0;
  virtual int find_bound(
      const ObDatumRowkey &key,
      const bool lower_bound,
      const int64_t begin_idx,
      int64_t &row_idx,
      bool &equal) override;

  virtual int get_col_data(const int32_t col_id, ObVectorDecodeCtx &vector_ctx, const bool need_reverse_trans_version = true) = 0;
  virtual int fill_default_for_nop(const int32_t col_id, ObVectorDecodeCtx &vector_ctx)
  {
    UNUSEDx(col_id, vector_ctx);
    return common::OB_SUCCESS;
  }
  virtual int get_rows(
    const common::ObIArrayWrap<uint16_t> *cols,
    const common::ObIArray<blocksstable::ObStorageDatum> *default_datums,
    const int32_t *row_ids,
    const int64_t row_cap,
    const int64_t vec_offset,
    const char **cell_datas,
    uint32_t *len_array,
    common::ObIArray<compaction::ObCompactionVector *> &vectors) override;

protected:
  virtual int find_bound(const ObDatumRange &range, const int64_t begin_idx, int64_t &row_idx,
    bool &equal, int64_t &end_key_begin_idx, int64_t &end_key_end_idx) override;

  // For column store and skip scan
  virtual int find_bound(const ObDatumRowkey &key, const bool lower_bound, const int64_t begin_idx,
    const int64_t end_idx, int64_t &row_idx, bool &equal,
    const int64_t common_prefix_len = 0) override;

  #define FOREACH_ADD_DECODER(col_param) \
  for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; ++i) { \
    if (OB_FAIL(add_decoder(cols_index.at(i),  cols_desc.at(i).col_type_, (col_param), decoders_buf_pos, decoders_[i]))) { \
      LOG_WARN("add_decoder failed", K(ret), "request_idx", i); \
    } \
  }

  #define ADD_DECODERS_BY_READ_INFO(column_count) \
  const ObColumnIndexArray &cols_index = read_info_->get_columns_index(); \
  const ObColDescIArray &cols_desc = read_info_->get_columns_desc(); \
  if (typeid(ObRowkeyReadInfo) == typeid(*read_info_)) { \
    for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; ++i) { \
      const int64_t store_idx = cols_index.at(i); \
      if (store_idx >= column_count) { \
        decoders_[i].decoder_ = &none_exist_column_decoder_; \
        decoders_[i].ctx_ = &none_exist_column_decoder_ctx_; \
      } else if (OB_FAIL(add_decoder(store_idx, cols_desc.at(i).col_type_, nullptr, \
              decoders_buf_pos, decoders_[i]))) { \
        LOG_WARN("add_decoder failed", K(ret), "request_idx", i); \
      } \
    } \
  } else if (typeid(ObCGRowkeyReadInfo) == typeid(*read_info_) || \
      typeid(ObCGReadInfo) == typeid(*read_info_) || \
      read_info_->get_columns()->count() < 1) { \
    FOREACH_ADD_DECODER(nullptr) \
  } else { \
    const ObColumnParamIArray *cols_param = read_info_->get_columns(); \
    FOREACH_ADD_DECODER(cols_param->at(i)) \
  }

protected:
  int64_t request_cnt_;
  ObBlockReaderAllocator decoder_allocator_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif
