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

#ifndef OB_STORAGE_BLOCKSSTABLE_ENCODING_OB_MICRO_BLOCK_DECODER_H_
#define OB_STORAGE_BLOCKSSTABLE_ENCODING_OB_MICRO_BLOCK_DECODER_H_

#include "ob_imicro_block_decoder.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/blocksstable/ob_decode_resource_pool.h"
#include "storage/ob_i_store.h"
#include "ob_integer_array.h"
#include "ob_icolumn_decoder.h"
#include "ob_encoding_allocator.h"
#include "lib/container/ob_bitmap.h"
#include "ob_row_index.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObBlockCachedDecoderHeader;
class ObIColumnDecoder;
struct ObBlockCachedDecoderHeader;
class ObIRowIndex;

struct ObColumnDecoder
{
public:
  ObColumnDecoder() : decoder_(nullptr), ctx_(nullptr) {}
  void reset()
  {
    decoder_ = nullptr;
    ctx_ = nullptr;
  }
  // performance critical, do not check parameters
  int decode(common::ObDatum &datum, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len);

  // used for locate row in micro block rowkey object compare
  // performance critical, do not check parameters
  int quick_compare(const ObStorageDatum &left, const ObStorageDatumCmpFunc &cmp_func, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len, int32_t &cmp_ret);

  int batch_decode(
      const ObIRowIndex *row_index,
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums);

  int decode_vector(
      const ObIRowIndex* row_index,
      ObVectorDecodeCtx &vector_ctx);

  int get_row_count(
      const ObIRowIndex *row_index,
      const int32_t *row_ids,
      const int64_t row_cap,
      const bool contains_null,
      int64_t &count);

  OB_INLINE int get_distinct_count(int64_t &distinct_cnt) const
  { return decoder_->get_distinct_count(distinct_cnt); }
  OB_INLINE int read_distinct(
      const char **cell_datas,
      storage::ObGroupByCell &group_by_cell) const
  { return decoder_->read_distinct(*ctx_, cell_datas, group_by_cell); }
  OB_INLINE int read_reference(
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCell &group_by_cell) const
  { return decoder_->read_reference(*ctx_, row_ids, row_cap, group_by_cell); }
public:
  const ObIColumnDecoder *decoder_;
  ObColumnDecoderCtx *ctx_;
};

class ObDecoderCtxArray final
{
public:
  typedef ObColumnDecoderCtx ObDecoderCtx;
  ObDecoderCtxArray(): ctxs_(), ctx_blocks_()
  {
    ObMemAttr attr(ob_thread_tenant_id(), "TLDecoderCtxArr");
    SET_IGNORE_MEM_VERSION(attr);
    ctxs_.set_attr(attr);
    ctx_blocks_.set_attr(attr);
  };
  ~ObDecoderCtxArray()
  {
    reset();
  }

  TO_STRING_KV(K_(ctxs));
  void reset();
  int get_ctx_array(ObDecoderCtx **&ctxs, int64_t size);
private:
  ObSEArray<ObDecoderCtx *, ObColumnDecoderCtxBlock::CTX_NUMS> ctxs_;
  ObSEArray<ObColumnDecoderCtxBlock *, 1> ctx_blocks_;

  DISALLOW_COPY_AND_ASSIGN(ObDecoderCtxArray);
};

template <class Decoder>
static int acquire_local_decoder(ObDecoderPool &local_decoder_pool,
                           const ObMicroBlockHeader &header,
                           const ObColumnHeader &col_header,
                           const char *meta_data,
                           const ObIColumnDecoder *&decoder);
template <class Decoder>
static int release_local_decoder(ObDecoderPool &local_decoder_pool, ObIColumnDecoder *decoder);
class ObIEncodeBlockReader
{
public:
  ObIEncodeBlockReader();
  virtual ~ObIEncodeBlockReader();
  void reset();
  void reuse();
  static int get_micro_metas(
      const ObMicroBlockHeader *&header,
      const ObColumnHeader *&col_header,
      const char *&meta_data,
      const char *block,
      const int64_t block_size);
protected:
  int prepare(const uint64_t tenant_id, const int64_t column_cnt);
  int do_init(const ObMicroBlockData &block_data, const int64_t request_cnt);
  int init_decoders();
  int add_decoder(const int64_t store_idx, const common::ObObjMeta &obj_meta, ObColumnDecoder &dest);
  int free_decoders();
  int acquire(const int64_t store_idx, const ObIColumnDecoder *&decoder);
  int setup_row(const uint64_t row_id, int64_t &row_len, const char *&row_data);
protected:
  static const int64_t DEFAULT_DECODER_CNT = 16;
  static const int64_t DEFAULT_RELEASE_CNT = (DEFAULT_DECODER_CNT + 1) * 2;

  int64_t request_cnt_; // request column count
  const ObMicroBlockHeader *header_;
  const ObColumnHeader *col_header_;
  const ObBlockCachedDecoderHeader *cached_decocer_;
  const char *meta_data_;
  const char *row_data_;
  ObVarRowIndex var_row_index_;
  ObFixRowIndex fix_row_index_;
  ObIRowIndex *row_index_;
  ObColumnDecoder *decoders_;
  const ObIColumnDecoder **need_release_decoders_;
  int64_t need_release_decoder_cnt_;
  ObColumnDecoder default_decoders_[DEFAULT_DECODER_CNT];
  const ObIColumnDecoder * default_release_decoders_[DEFAULT_RELEASE_CNT];
  ObDecoderPool *local_decoder_pool_;
  ObDecoderCtxArray ctx_array_;
  ObColumnDecoderCtx **ctxs_;
  common::ObArenaAllocator decoder_allocator_;
  common::ObArenaAllocator buf_allocator_;
  int64_t *store_id_array_;
  common::ObObjMeta *column_type_array_;
  int64_t default_store_ids_[DEFAULT_DECODER_CNT];
  common::ObObjMeta default_column_types_[DEFAULT_DECODER_CNT];
  bool need_cast_;
  static ObNoneExistColumnDecoder none_exist_column_decoder_;
  static ObColumnDecoderCtx none_exist_column_decoder_ctx_;
};

class ObEncodeBlockGetReader : public ObIMicroBlockGetReader, public ObIEncodeBlockReader
{
public:
  void reuse();
  ObEncodeBlockGetReader() = default;
  virtual ~ObEncodeBlockGetReader() = default;
  virtual int get_row(
      const ObMicroBlockData &block_data,
      const ObDatumRowkey &rowkey,
      const ObITableReadInfo &read_info,
      ObDatumRow &row) final;
  virtual int exist_row(
      const ObMicroBlockData &block_data,
      const ObDatumRowkey &rowkey,
      const ObITableReadInfo &read_info,
      bool &exist,
      bool &found);
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
  int get_all_columns(
      const char *row_data,
      const int64_t row_len,
      const int64_t row_id,
      ObDatumRow &row);
private:
  int init_by_read_info(
      const ObMicroBlockData &block_data,
      const storage::ObITableReadInfo &read_info);
  int init_by_columns_desc(
      const ObMicroBlockData &block_data,
      const int64_t schema_rowkey_cnt,
      const ObColDescIArray &cols_desc,
      const int64_t request_cnt);
  int locate_row(
      const ObDatumRowkey &rowkey,
      const ObStorageDatumUtils &datum_utils,
      const char *&row_data,
      int64_t &row_len,
      int64_t &row_id,
      bool &found);
};

class ObMicroBlockDecoder : public ObIMicroBlockDecoder
{
public:
  static const int64_t ROW_CACHE_BUF_SIZE = 64 * 1024;
  static const int64_t CPU_CACHE_LINE_SIZE = 64;
  static const int64_t MAX_CACHED_DECODER_BUF_SIZE = 1L << 10;

  ObMicroBlockDecoder();
  virtual ~ObMicroBlockDecoder();

  static int get_decoder_cache_size(
      const char *block,
      const int64_t block_size,
      int64_t &size);
  static int cache_decoders(
      char *buf,
      const int64_t size,
      const char *block,
      const int64_t block_size,
      const ObColDescIArray &full_schema_cols);
  static int update_cached_decoders(char *cache, const int64_t cache_size,
      const char *old_block, const char *cur_block, const int64_t block_size);

  virtual ObReaderType get_type() override { return Decoder; }
  virtual void reset();
  virtual int init(
      const ObMicroBlockData &block_data,
      const storage::ObITableReadInfo &read_info) override;
  //when there is not read_info in input parameters, it indicates reading all columns from all rows
  //when the incoming datum_utils is nullptr, it indicates not calling locate_range or find_bound
  virtual int init(const ObMicroBlockData &block_data, const ObStorageDatumUtils *datum_utils) override;
  virtual int get_row(const int64_t index, ObDatumRow &row) override;
  virtual int get_row_header(
      const int64_t row_idx,
      const ObRowHeader *&row_header) override;
  virtual int get_row_count(int64_t &row_count) override;
  virtual int get_multi_version_info(
      const int64_t row_idx,
      const int64_t schema_rowkey_cnt,
      const ObRowHeader *&row_header,
      int64_t &trans_version,
      int64_t &sql_sequence);
  virtual int compare_rowkey(
      const ObDatumRowkey &rowkey,
      const int64_t index,
      int32_t &compare_result) override;
  virtual int compare_rowkey(
      const ObDatumRange &range,
      const int64_t index,
      int32_t &start_key_compare_result,
      int32_t &end_key_compare_result) override;
  static int cache_decoders(
      char *buf,
      const int64_t size,
      const char *block,
      const int64_t block_size);
  // Filter interface for filter pushdown
  virtual int filter_pushdown_filter(
      const sql::ObPushdownFilterExecutor *parent,
      sql::ObPhysicalFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      common::ObBitmap &result_bitmap) override;
  virtual int filter_pushdown_filter(
      const sql::ObPushdownFilterExecutor *parent,
      sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      common::ObBitmap &result_bitmap) override;
  virtual int filter_black_filter_batch(
      const sql::ObPushdownFilterExecutor *parent,
      sql::ObBlackFilterExecutor &filter,
      sql::PushdownFilterInfo &pd_filter_info,
      common::ObBitmap &result_bitmap,
      bool &filter_applied) override;

  virtual int get_rows(
      const common::ObIArray<int32_t> &cols,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObIArray<ObSqlDatumInfo> &datum_infos,
      const int64_t datum_offset = 0) override;
  virtual int get_row_count(
      int32_t col_id,
      const int32_t *row_ids,
      const int64_t row_cap,
      const bool contains_null,
      const share::schema::ObColumnParam *col_param,
      int64_t &count) override final;
  virtual int get_aggregate_result(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t col_offset,
      const share::schema::ObColumnParam &col_param,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObAggDatumBuf &agg_datum_buf,
      storage::ObAggCell &agg_cell) override;
  virtual int64_t get_column_count() const override
  {
    OB_ASSERT(nullptr != header_);
    return header_->column_count_;
  }
  virtual int get_column_datum(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const share::schema::ObColumnParam &col_param,
      const int32_t col_offset,
      const int64_t row_index,
      ObStorageDatum &datum) override;

  virtual void reserve_reader_memory(bool reserve) override
  {
    decoder_allocator_.set_reserve_memory(reserve);
  }
  virtual bool can_apply_black(const common::ObIArray<int32_t> &col_offsets) const override
  {
    return 1 == col_offsets.count() &&
        ObColumnHeader::DICT == decoders_[col_offsets.at(0)].ctx_->col_header_->type_ &&
        header_->all_lob_in_row_;
  }
  virtual int get_distinct_count(const int32_t group_by_col, int64_t &distinct_cnt) const override;
  virtual int read_distinct(
      const int32_t group_by_col,
      const char **cell_datas,
      storage::ObGroupByCell &group_by_cell) const override;
  virtual int read_reference(
      const int32_t group_by_col,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCell &group_by_cell) const override;
  virtual int get_group_by_aggregate_result(
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      storage::ObGroupByCell &group_by_cell) override;
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
  { return nullptr != header_ && header_->has_lob_out_row(); }

private:
  // use inner_reset to reuse the decoder buffer
  // the column count would not change in most cases
  void inner_reset();
  int do_init(const ObMicroBlockData &block_data);
  int init_decoders();
  int add_decoder(const int64_t store_idx,
                  const common::ObObjMeta &obj_meta,
                  ObColumnDecoder &dest);
  int free_decoders();
  int decode_cells(const uint64_t row_id,
                   const int64_t row_len,
                   const char *row_data,
                   const int64_t col_begin,
                   const int64_t col_end,
                   ObStorageDatum *datums);

  int get_col_datums(int32_t col_id,
                     const int32_t *row_ids,
                     const char **cell_datas,
                     const int64_t row_cap,
                     common::ObDatum *col_datums);
  int get_row_impl(int64_t index, ObDatumRow &row);
  OB_INLINE static const ObRowHeader &get_major_store_row_header()
  {
    static ObRowHeader rh = init_major_store_row_header();
    return rh;
  }
  static const ObRowHeader init_major_store_row_header();
  static int get_micro_metas(
      const ObMicroBlockHeader *&header, const ObColumnHeader *&col_header,
      const char *&meta_data, const char *block, const int64_t block_size);

  template <typename Allocator>
  static int acquire(
      Allocator &allocator,
      const ObMicroBlockHeader &header,
      const ObColumnHeader &col_header,
      const char *meta_data,
      const ObIColumnDecoder *&decoder);
  int acquire(const int64_t store_idx, const ObIColumnDecoder *&decoder);
  void release(const ObIColumnDecoder *decoder);
  int filter_pushdown_retro(
      const sql::ObPushdownFilterExecutor *parent,
      sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      const int32_t col_offset,
      const share::schema::ObColumnParam *col_param,
      ObStorageDatum &decoded_datum,
      common::ObBitmap &result_bitmap);
  int get_col_data(const int32_t col_id, ObVectorDecodeCtx &vector_ctx);

private:
  const ObMicroBlockHeader *header_;
  int64_t request_cnt_; // request column count
  const ObColumnHeader *col_header_;
  const char *meta_data_;
  const char *row_data_;
  ObVarRowIndex var_row_index_;
  ObFixRowIndex fix_row_index_;
  const ObBlockCachedDecoderHeader *cached_decoder_;
  ObIRowIndex *row_index_;
  void *decoder_buf_;
  ObColumnDecoder *decoders_;
  // array size is double of max_column_count, because we may need two decoders for one column,
  // e.g.: column equal encoding, one for column equal decoder and one for referenced column decoder.
  ObFixedArray<const ObIColumnDecoder *, ObIAllocator> need_release_decoders_;
  int64_t need_release_decoder_cnt_;
  ObRowReader flat_row_reader_;
  ObDecoderPool *local_decoder_pool_;
  ObDecoderCtxArray ctx_array_;
  ObColumnDecoderCtx **ctxs_;
  static ObNoneExistColumnDecoder none_exist_column_decoder_;
  static ObColumnDecoderCtx none_exist_column_decoder_ctx_;
  ObBlockReaderAllocator decoder_allocator_;
  common::ObArenaAllocator buf_allocator_;
};

}
}
#endif //OB_STORAGE_BLOCKSSTABLE_ENCODING_OB_MICRO_BLOCK_DECODER_H_
