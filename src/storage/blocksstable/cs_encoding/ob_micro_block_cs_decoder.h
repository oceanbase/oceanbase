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

#ifndef OB_STORAGE_BLOCKSSTABLE_CS_ENCODING_OB_MICRO_BLOCK_CS_DECODER_H_
#define OB_STORAGE_BLOCKSSTABLE_CS_ENCODING_OB_MICRO_BLOCK_CS_DECODER_H_

#include "storage/blocksstable/encoding/ob_imicro_block_decoder.h"
#include "lib/container/ob_bitmap.h"
#include "ob_cs_encoding_allocator.h"
#include "ob_cs_micro_block_transformer.h"
#include "ob_icolumn_cs_decoder.h"
#include "ob_new_column_cs_decoder.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/cs_encoding/semistruct_encoding/ob_semistruct_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObBlockCachedCSDecoderHeader;
struct ObCSDecoderCtxArray;
class ObIColumnCSDecoder;

struct ObColumnCSDecoder final
{
public:
  ObColumnCSDecoder() : decoder_(nullptr), ctx_(nullptr) {}
  void reset()
  {
    decoder_ = nullptr;
    ctx_ = nullptr;
  }
  // performance critical, do not check parameters
  int decode(const int64_t row_id, ObStorageDatum &datum);

  // used for locate row in micro block rowkey object compare
  // performance critical, do not check parameters
  int quick_compare(const ObStorageDatum &left, const ObStorageDatumCmpFunc &cmp_func,
    const int64_t row_id, int32_t &cmp_ret);

  int batch_decode(const int32_t *row_ids, const int64_t row_cap, common::ObDatum *datums);
  int decode_vector(ObVectorDecodeCtx &vector_ctx);
  int get_row_count(
    const int32_t *row_ids, const int64_t row_cap, const bool contains_null, int64_t &count);
  OB_INLINE int get_distinct_count(int64_t &distinct_cnt) const
  { return decoder_->get_distinct_count(*ctx_, distinct_cnt); }
  OB_INLINE int read_distinct(
      storage::ObGroupByCellBase &group_by_cell) const
  { return decoder_->read_distinct(*ctx_, group_by_cell); }
  OB_INLINE int read_reference(
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCellBase &group_by_cell) const
  { return decoder_->read_reference(*ctx_, row_ids, row_cap, group_by_cell); }

public:
  const ObIColumnCSDecoder *decoder_;
  ObColumnCSDecoderCtx *ctx_;
};

template <class Decoder>
int new_decoder_with_allocated_buf(char *buf, const ObIColumnCSDecoder *&decoder);

class ObSemiStructDecodeCtx
{
public:
  ObSemiStructDecodeCtx():
    allocator_("SemiDec", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    handlers_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("SemiDec", MTL_ID())),
    reserve_memory_(false)
  {}

  ~ObSemiStructDecodeCtx() { reset(); }
  void reuse();
  void reset();
  int build_semistruct_ctx(ObSemiStructColumnDecoderCtx &semistruct_ctx);
  OB_INLINE void set_reserve_memory(bool reserve) { reserve_memory_ = reserve;}

private:
  int build_decode_handler(ObSemiStructColumnDecoderCtx &semistruct_ctx);
  int build_sub_col_decoder(ObSemiStructColumnDecoderCtx &semistruct_ctx);

public:
  common::ObArenaAllocator allocator_;
  ObSEArray<ObSemiStructDecodeHandler*, 10> handlers_;
  bool reserve_memory_;
};

class ObICSEncodeBlockReader
{
public:
  ObICSEncodeBlockReader();
  virtual ~ObICSEncodeBlockReader();
  void reset();
  void reuse();

protected:
  int prepare(const int64_t column_cnt);
  int do_init(const ObMicroBlockData &block_data, const int64_t request_cnt);
  int init_decoders();
  int add_decoder(const int64_t store_idx, const ObObjMeta &obj_meta, int64_t &decoders_buf_pos, ObColumnCSDecoder &dest);
  int acquire(const int64_t store_idx, int64_t &decoders_buf_pos, const ObIColumnCSDecoder *&decoder);
  int alloc_decoders_buf(int64_t &decoders_buf_pos);
  int init_semistruct_decoder(const ObIColumnCSDecoder *decoder, ObColumnCSDecoderCtx &decoder_ctx);

protected:
  static const int64_t DEFAULT_DECODER_CNT = 16;
  ObMicroBlockAddr block_addr_;
  int64_t request_cnt_;  // request column count
  const ObBlockCachedCSDecoderHeader *cached_decoder_;
  ObColumnCSDecoder *decoders_;
  ObCSMicroBlockTransformHelper transform_helper_;
  int32_t column_count_;
  ObColumnCSDecoder default_decoders_[DEFAULT_DECODER_CNT];
  ObColumnCSDecoderCtx *ctxs_;
  common::ObArenaAllocator decoder_allocator_;

  // For highly concurrently get row on wide tables(eg: more then 100 columns),
  // frequently allocate from MTL(ObDecodeResourcePool*), resulting in a bottleneck
  // for atomic operations under ARM, so we allocate mem for decoders in ObEncodeBlockGetReader
  common::ObArenaAllocator buf_allocator_;
  char *allocated_decoders_buf_;
  int64_t allocated_decoders_buf_size_;

  int32_t *store_id_array_;
  common::ObObjMeta *column_type_array_;
  int32_t default_store_ids_[DEFAULT_DECODER_CNT];
  common::ObObjMeta default_column_types_[DEFAULT_DECODER_CNT];
  ObSemiStructDecodeCtx semistruct_decode_ctx_;
  static ObNoneExistColumnCSDecoder none_exist_column_decoder_;
  static ObColumnCSDecoderCtx none_exist_column_decoder_ctx_;
};

class ObCSEncodeBlockGetReader : public ObIMicroBlockGetReader, public ObICSEncodeBlockReader
{
public:
  void reuse();
  ObCSEncodeBlockGetReader() = default;
  virtual ~ObCSEncodeBlockGetReader() = default;
  virtual int get_row(const ObMicroBlockAddr &block_addr, const ObMicroBlockData &block_data,
      const ObDatumRowkey &rowkey, const ObITableReadInfo &read_info, ObDatumRow &row) final;
  virtual int exist_row(const ObMicroBlockData &block_data, const ObDatumRowkey &rowkey,
    const ObITableReadInfo &read_info, bool &exist, bool &found);
  virtual int get_row(
      const ObMicroBlockAddr &block_addr,
      const ObMicroBlockData &block_data,
      const ObITableReadInfo &read_info,
      const uint32_t row_idx,
      ObDatumRow &row) final;
  int get_row_id(
      const ObMicroBlockAddr &block_addr,
      const ObMicroBlockData &block_data,
      const ObDatumRowkey &rowkey,
      const ObITableReadInfo &read_info,
      int64_t &row_id) final;

protected:
  int get_all_columns(const int64_t row_id, ObDatumRow &row);

private:
  int init(const ObMicroBlockAddr &block_addr, const ObMicroBlockData &block_data, const ObITableReadInfo &read_info);
  int init_if_need(const ObMicroBlockAddr &block_addr, const ObMicroBlockData &block_data, const ObITableReadInfo &read_info);
  int init(const ObMicroBlockData &block_data, const int64_t schema_rowkey_cnt,
           const ObColDescIArray &cols_desc, const int64_t request_cnt);
  int locate_row(const ObDatumRowkey &rowkey, const ObStorageDatumUtils &datum_utils,
    int64_t &row_id, bool &found);
};

class ObMicroBlockCSDecoder : public ObIMicroBlockDecoder
{
public:
  static const int64_t MAX_CACHED_DECODER_BUF_SIZE = 1L << 10;

  ObMicroBlockCSDecoder();
  virtual ~ObMicroBlockCSDecoder();

  static int get_decoder_cache_size(const char *block, const int64_t block_size, int64_t &size);
  static int cache_decoders(char *buf, const int64_t size, const char *block, const int64_t block_size);

  virtual void reset();
  virtual int init(const ObMicroBlockData &block_data, const ObITableReadInfo &read_info) override;
  //TODO @fenggu support cs decoder without read info
  virtual int init(const ObMicroBlockData &block_data, const ObStorageDatumUtils *datum_utils) override;
  virtual int get_row(const int64_t index, ObDatumRow &row) override;
  virtual int get_row_header(const int64_t row_idx, const ObRowHeader *&row_header) override;
  virtual int get_row_count(int64_t &row_count) override;
  virtual int get_multi_version_info(const int64_t row_idx, const int64_t schema_rowkey_cnt,
      const ObRowHeader *&row_header,int64_t &trans_version, int64_t &sql_sequence);
  virtual int compare_rowkey(
    const ObDatumRowkey &rowkey, const int64_t index, int32_t &compare_result) override;
  virtual int compare_rowkey(const ObDatumRange &range, const int64_t index,
    int32_t &start_key_compare_result, int32_t &end_key_compare_result) override;

  // Filter interface for filter pushdown
  int filter_pushdown_filter(const sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor *filter, const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap);
  virtual int filter_pushdown_filter(const sql::ObPushdownFilterExecutor *parent,
    sql::ObPhysicalFilterExecutor &filter, const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap) override;
  virtual int filter_pushdown_filter(const sql::ObPushdownFilterExecutor *parent,
    sql::ObWhiteFilterExecutor &filter, const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap) override;
  virtual int filter_black_filter_batch(const sql::ObPushdownFilterExecutor *parent,
    sql::ObBlackFilterExecutor &filter, sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap, bool &filter_applied) override;
  virtual int filter_pushdown_truncate_filter(const sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor &filter, const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap) override;
  virtual int get_rows(
      const common::ObIArray<int32_t> &cols,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const bool is_padding_mode,
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObIArray<ObSqlDatumInfo> &datum_infos,
      const int64_t datum_offset = 0) override;
  virtual int get_row_count(int32_t col_id, const int32_t *row_ids, const int64_t row_cap,
    const bool contains_null, const share::schema::ObColumnParam *col_param, int64_t &count) override final;
  virtual int64_t get_column_count() const override
  {
    return column_count_;
  }
  virtual void reserve_reader_memory(bool reserve) override
  {
    decoder_allocator_.set_reserve_memory(reserve);
    semistruct_decode_ctx_.set_reserve_memory(reserve);
  }
  virtual int get_raw_column_datum(const int32_t col_offset,
      const int64_t row_index,
      ObStorageDatum &datum) override;
  virtual bool can_apply_black(const common::ObIArray<int32_t> &col_offsets) const override
  {
    return 1 == col_offsets.count() &&
        ((ObCSColumnHeader::INT_DICT == decoders_[col_offsets.at(0)].ctx_->type_) || (ObCSColumnHeader::STR_DICT == decoders_[col_offsets.at(0)].ctx_->type_))
            && transform_helper_.get_micro_block_header()->all_lob_in_row_;
  }
  virtual bool can_pushdown_decoder(
      const share::schema::ObColumnParam &col_param,
      const int32_t col_offset,
      const int32_t *row_ids,
      const int64_t row_cap,
      const storage::ObAggCellBase &agg_cell) override;
  virtual int get_aggregate_result(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t col_offset,
      const share::schema::ObColumnParam &col_param,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObAggDatumBuf &agg_datum_buf,
      ObAggCell &agg_cell) override;
  virtual int get_aggregate_result(
      const int32_t col_offset,
      const ObPushdownRowIdCtx &pd_row_id_ctx,
      ObAggCellVec &agg_cell) override;
  virtual int get_distinct_count(const int32_t group_by_col, int64_t &distinct_cnt) const override;
  virtual int read_distinct(const int32_t group_by_col, const char **cell_datas,
    const bool is_padding_mode, storage::ObGroupByCellBase &group_by_cell) const override;
  virtual int read_reference(const int32_t group_by_col, const int32_t *row_ids,
    const int64_t row_cap, storage::ObGroupByCellBase &group_by_cell) const override;
  virtual int get_group_by_aggregate_result(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      storage::ObGroupByCell &group_by_cell) override;
  virtual int get_group_by_aggregate_result(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      const int64_t vec_offset,
      const common::ObIArray<blocksstable::ObStorageDatum> &default_datums,
      uint32_t *len_array,
      sql::ObEvalCtx &eval_ctx,
      storage::ObGroupByCellVec &group_by_cell,
      const bool enable_rich_format) override;
  virtual int get_rows(
      const common::ObIArray<int32_t> &cols,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const common::ObIArray<blocksstable::ObStorageDatum> *default_datums,
      const bool is_padding_mode,
      const int32_t *row_ids,
      const int64_t row_cap,
      const char **cell_datas,
      const int64_t vec_offset,
      uint32_t *len_array,
      sql::ObEvalCtx &eval_ctx,
      sql::ObExprPtrIArray &exprs,
      const bool need_init_vector) override;
  virtual bool has_lob_out_row() const override final
  { return transform_helper_.get_micro_block_header()->has_lob_out_row(); }

private:
  // use inner_reset to reuse the decoder buffer
  // the column count would not change in most cases
  void inner_reset();
  int do_init(const ObMicroBlockData &block_data);
  int init_decoders();
  int add_decoder(
      const int64_t store_idx,
      const ObObjMeta &obj_meta,
      const ObColumnParam *col_param,
      int64_t &decoders_buf_pos,
      ObColumnCSDecoder &dest);
  int init_semistruct_decoder(const ObIColumnCSDecoder *decoder, ObColumnCSDecoderCtx &decoder_ctx);
  int free_decoders();
  int get_stream_data_buf(const int64_t stream_idx, const char *&buf);
  int decode_cells(
    const uint64_t row_id, const int64_t col_begin, const int64_t col_end, ObStorageDatum *datums);
  int get_row_impl(int64_t index, ObDatumRow &row);
  bool can_pushdown_decoder(
      const ObColumnCSDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      const ObAggCell &agg_cell) const;
  OB_INLINE static const ObRowHeader &get_major_store_row_header()
  {
    static ObRowHeader rh = init_major_store_row_header();
    return rh;
  }
  static const ObRowHeader init_major_store_row_header();

  template <typename Allocator>
  static int acquire(
    Allocator &allocatois_row_store_type_with_cs_encodingr, const ObCSColumnHeader::Type type, const ObIColumnCSDecoder *&decoder);

  int acquire(const int64_t store_idx, int64_t &decoders_buf_pos, const ObIColumnCSDecoder *&decoder);
  int alloc_decoders_buf(const bool by_read_info, int64_t &decoders_buf_pos);
  int filter_pushdown_retro(const sql::ObPushdownFilterExecutor *parent,
    sql::ObWhiteFilterExecutor &filter, const sql::PushdownFilterInfo &pd_filter_info,
    const int32_t col_offset, const share::schema::ObColumnParam *col_param,
    ObStorageDatum &decoded_datum, common::ObBitmap &result_bitmap);
  int get_col_datums(int32_t col_id, const int32_t *row_ids, const int64_t row_cap, common::ObDatum *col_datums);
  int get_col_data(int32_t col_id, ObVectorDecodeCtx &ctx);

private:
  int32_t request_cnt_;  // request column count
  const ObBlockCachedCSDecoderHeader *cached_decoder_;
  ObColumnCSDecoder *decoders_;
  ObCSMicroBlockTransformHelper transform_helper_;
  int32_t column_count_;
  ObColumnCSDecoderCtx *ctxs_;
  static ObNoneExistColumnCSDecoder none_exist_column_decoder_;
  static ObColumnCSDecoderCtx none_exist_column_decoder_ctx_;
  static ObNewColumnCSDecoder new_column_decoder_;
  ObBlockReaderAllocator decoder_allocator_;
  common::ObArenaAllocator transform_allocator_;
  common::ObArenaAllocator buf_allocator_;
  char *allocated_decoders_buf_;
  int64_t allocated_decoders_buf_size_;
  ObSemiStructDecodeCtx semistruct_decode_ctx_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif  // OB_STORAGE_BLOCKSSTABLE_ENCODING_OB_MICRO_BLOCK_CS_DECODER_H_
