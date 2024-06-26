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
#include "storage/blocksstable/ob_decode_resource_pool.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObBlockCachedCSDecoderHeader;
struct ObCSDecoderCtxArray;
class ObIColumnCSDecoder;

struct ObColumnCSDecoder
{
public:
  ObColumnCSDecoder() : decoder_(nullptr), ctx_(nullptr) {}
  void reset()
  {
    decoder_ = nullptr;
    ctx_ = nullptr;
  }
  // performance critical, do not check parameters
  int decode(const int64_t row_id, common::ObDatum &datum);

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
      storage::ObGroupByCell &group_by_cell) const
  { return decoder_->read_distinct(*ctx_, group_by_cell); }
  OB_INLINE int read_reference(
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCell &group_by_cell) const
  { return decoder_->read_reference(*ctx_, row_ids, row_cap, group_by_cell); }

public:
  const ObIColumnCSDecoder *decoder_;
  ObColumnCSDecoderCtx *ctx_;
};


class ObCSDecoderCtxArray final
{
public:
  ObCSDecoderCtxArray(): ctxs_(), ctx_blocks_() {};
  ~ObCSDecoderCtxArray()
  {
    reset();
  }

  TO_STRING_KV(K_(ctxs));
  void reset();
  int get_ctx_array(ObColumnCSDecoderCtx **&ctxs, int64_t size);
private:
  ObSEArray<ObColumnCSDecoderCtx *, ObColumnCSDecoderCtxBlock::CS_CTX_NUMS> ctxs_;
  ObSEArray<ObColumnCSDecoderCtxBlock *, 1> ctx_blocks_;

  DISALLOW_COPY_AND_ASSIGN(ObCSDecoderCtxArray);
};

template <class Decoder>
static int acquire_local_decoder(ObCSDecoderPool &local_decoder_pool, const ObIColumnCSDecoder *&decoder);
template <class Decoder>
static int release_local_decoder(ObCSDecoderPool &local_decoder_pool, ObIColumnCSDecoder *decoder);

class ObICSEncodeBlockReader
{
public:
  ObICSEncodeBlockReader();
  virtual ~ObICSEncodeBlockReader();
  void reset();
  void reuse();

protected:
  int prepare(const uint64_t tenant_id, const int64_t column_cnt);
  int do_init(const ObMicroBlockData &block_data, const int64_t request_cnt);
  int init_decoders();
  int add_decoder(const int64_t store_idx, const ObObjMeta &obj_meta, ObColumnCSDecoder &dest);
  int free_decoders();
  int acquire(int64_t store_idx, const ObIColumnCSDecoder *&decoder);

protected:
  static const int64_t DEFAULT_DECODER_CNT = 16;
  static const int64_t DEFAULT_RELEASE_CNT = DEFAULT_DECODER_CNT;

  int64_t request_cnt_;  // request column count
  const ObBlockCachedCSDecoderHeader *cached_decocer_;
  ObColumnCSDecoder *decoders_;
  const ObIColumnCSDecoder **need_release_decoders_;
  int64_t need_release_decoder_cnt_;
  ObCSMicroBlockTransformHelper transform_helper_;
  int32_t column_count_;
  ObColumnCSDecoder default_decoders_[DEFAULT_DECODER_CNT];
  const ObIColumnCSDecoder *default_release_decoders_[DEFAULT_RELEASE_CNT];

  ObCSDecoderPool *local_decoder_pool_;
  ObCSDecoderCtxArray ctx_array_;
  ObColumnCSDecoderCtx **ctxs_;
  common::ObArenaAllocator decoder_allocator_;
  common::ObArenaAllocator buf_allocator_;
  int32_t *store_id_array_;
  common::ObObjMeta *column_type_array_;
  int32_t default_store_ids_[DEFAULT_DECODER_CNT];
  common::ObObjMeta default_column_types_[DEFAULT_DECODER_CNT];
  bool need_cast_;
  static ObNoneExistColumnCSDecoder none_exist_column_decoder_;
  static ObColumnCSDecoderCtx none_exist_column_decoder_ctx_;
};

class ObCSEncodeBlockGetReader : public ObIMicroBlockGetReader, public ObICSEncodeBlockReader
{
public:
  void reuse();
  ObCSEncodeBlockGetReader() = default;
  virtual ~ObCSEncodeBlockGetReader() = default;
  virtual int get_row(const ObMicroBlockData &block_data, const ObDatumRowkey &rowkey,
    const ObITableReadInfo &read_info, ObDatumRow &row) final;
  virtual int exist_row(const ObMicroBlockData &block_data, const ObDatumRowkey &rowkey,
    const ObITableReadInfo &read_info, bool &exist, bool &found);
  virtual int get_row(
      const ObMicroBlockData &block_data,
      const ObITableReadInfo &read_info,
      const uint32_t row_idx,
      ObDatumRow &row) final;
  int get_row_id(
      const ObMicroBlockData &block_data,
      const ObDatumRowkey &rowkey,
      const ObITableReadInfo &read_info,
      int64_t &row_id) final;

protected:
  int get_all_columns(const int64_t row_id, ObDatumRow &row);

private:
  int init(const ObMicroBlockData &block_data, const ObITableReadInfo &read_info);
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

  virtual ObReaderType get_type() override
  {
    return CSDecoder;
  }
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
  virtual int get_rows(
      const common::ObIArray<int32_t> &cols,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
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
  }
  virtual int get_column_datum(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const share::schema::ObColumnParam &col_param,
      const int32_t col_offset,
      const int64_t row_index,
      ObStorageDatum &datum) override;
  virtual bool can_apply_black(const common::ObIArray<int32_t> &col_offsets) const override
  {
    return 1 == col_offsets.count() &&
        ((ObCSColumnHeader::INT_DICT == decoders_[col_offsets.at(0)].ctx_->type_) || (ObCSColumnHeader::STR_DICT == decoders_[col_offsets.at(0)].ctx_->type_))
            && transform_helper_.get_micro_block_header()->all_lob_in_row_;
  }

  virtual int get_aggregate_result(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t col_offset,
      const share::schema::ObColumnParam &col_param,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObAggDatumBuf &agg_datum_buf,
      ObAggCell &agg_cell) override;
  virtual int get_distinct_count(const int32_t group_by_col, int64_t &distinct_cnt) const override;
  virtual int read_distinct(const int32_t group_by_col, const char **cell_datas,
    storage::ObGroupByCell &group_by_cell) const override;
  virtual int read_reference(const int32_t group_by_col, const int32_t *row_ids,
    const int64_t row_cap, storage::ObGroupByCell &group_by_cell) const override;
  virtual int get_group_by_aggregate_result(const int32_t *row_ids, const char **cell_datas,
    const int64_t row_cap, storage::ObGroupByCell &group_by_cell) override;
  virtual int get_rows(
      const common::ObIArray<int32_t> &cols,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const int32_t *row_ids,
      const int64_t row_cap,
      const char **cell_datas,
      const int64_t vec_offset,
      uint32_t *len_array,
      sql::ObEvalCtx &eval_ctx,
      sql::ObExprPtrIArray &exprs) override;
  virtual bool has_lob_out_row() const override final
  { return transform_helper_.get_micro_block_header()->has_lob_out_row(); }

private:
  // use inner_reset to reuse the decoder buffer
  // the column count would not change in most cases
  void inner_reset();
  int do_init(const ObMicroBlockData &block_data);
  int init_decoders();
  int add_decoder(const int64_t store_idx, const ObObjMeta &obj_meta, ObColumnCSDecoder &dest);
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

  int acquire(int64_t store_idx, const ObIColumnCSDecoder *&decoder);
  void release(const ObIColumnCSDecoder *decoder);
  int filter_pushdown_retro(const sql::ObPushdownFilterExecutor *parent,
    sql::ObWhiteFilterExecutor &filter, const sql::PushdownFilterInfo &pd_filter_info,
    const int32_t col_offset, const share::schema::ObColumnParam *col_param,
    ObStorageDatum &decoded_datum, common::ObBitmap &result_bitmap);
  int get_col_datums(int32_t col_id, const int32_t *row_ids, const int64_t row_cap, common::ObDatum *col_datums);
  int get_col_data(int32_t col_id, ObVectorDecodeCtx &ctx);

private:
  int32_t request_cnt_;  // request column count
  const ObBlockCachedCSDecoderHeader *cached_decoder_;
  void *decoder_buf_;
  ObColumnCSDecoder *decoders_;
  ObFixedArray<const ObIColumnCSDecoder *, ObIAllocator> need_release_decoders_;
  int32_t need_release_decoder_cnt_;
  ObCSMicroBlockTransformHelper transform_helper_;
  int32_t column_count_;
  ObCSDecoderPool *local_decoder_pool_;
  ObCSDecoderCtxArray ctx_array_;
  ObColumnCSDecoderCtx **ctxs_;
  static ObNoneExistColumnCSDecoder none_exist_column_decoder_;
  static ObColumnCSDecoderCtx none_exist_column_decoder_ctx_;
  ObBlockReaderAllocator decoder_allocator_;
  common::ObArenaAllocator buf_allocator_;
  common::ObArenaAllocator transform_allocator_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif  // OB_STORAGE_BLOCKSSTABLE_ENCODING_OB_MICRO_BLOCK_CS_DECODER_H_
