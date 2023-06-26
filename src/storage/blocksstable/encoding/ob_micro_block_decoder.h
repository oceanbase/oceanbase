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

#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/ob_i_store.h"
#include "ob_integer_array.h"
#include "ob_icolumn_decoder.h"
#include "ob_encoding_allocator.h"
#include "lib/container/ob_bitmap.h"
#include "ob_row_index.h"

namespace oceanbase
{
namespace storage {
struct PushdownFilterInfo;
}
namespace blocksstable
{
struct ObBlockCachedDecoderHeader;
struct ObDecoderCtxArray;
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
  int decode(common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len);

  // used for locate row in micro block rowkey object compare
  // performance critical, do not check parameters
  int quick_compare(const ObStorageDatum &left, const ObStorageDatumCmpFunc &cmp_func, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len, int32_t &cmp_ret);

  int batch_decode(
      const ObIRowIndex *row_index,
      const int64_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums);

  int get_row_count(
      const ObIRowIndex *row_index,
      const int64_t *row_ids,
      const int64_t row_cap,
      const bool contains_null,
      int64_t &count);

public:
  const ObIColumnDecoder *decoder_;
  ObColumnDecoderCtx *ctx_;
};

class ObIEncodeBlockReader
{
  typedef int (*decode_acquire_func)(ObDecoderAllocator &allocator,
                                     const ObMicroBlockHeader &header,
                                     const ObColumnHeader &col_header,
                                     const char *meta_data,
                                     const ObIColumnDecoder *&decoder);
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
  void free_decoders();
  void release(const ObIColumnDecoder *decoder);
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

  ObDecoderAllocator *allocator_;
  ObDecoderCtxArray *ctx_array_;
  ObColumnDecoderCtx **ctxs_;
  lib::MemoryContext decoder_mem_;
  common::ObArenaAllocator *decoder_allocator_;
  int64_t *store_id_array_;
  common::ObObjMeta *column_type_array_;
  int64_t default_store_ids_[DEFAULT_DECODER_CNT];
  common::ObObjMeta default_column_types_[DEFAULT_DECODER_CNT];
  bool need_cast_;
  static ObNoneExistColumnDecoder none_exist_column_decoder_;
  static ObColumnDecoderCtx none_exist_column_decoder_ctx_;
  template <class Decoder>
  static int acquire_decoder(ObDecoderAllocator &allocator,
                            const ObMicroBlockHeader &header,
                            const ObColumnHeader &col_header,
                            const char *meta_data,
                            const ObIColumnDecoder *&decoder);
  static decode_acquire_func acquire_funcs_[ObColumnHeader::MAX_TYPE];
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
      bool &found,
      ObDatumRow &row);
};

class ObMicroBlockDecoder : public ObIMicroBlockReader
{
public:
  static const int64_t ROW_CACHE_BUF_SIZE = 64 * 1024;
  static const int64_t CPU_CACHE_LINE_SIZE = 64;
  static const int64_t MAX_CACHED_DECODER_BUF_SIZE = 1L << 10;

  ObMicroBlockDecoder();
  virtual ~ObMicroBlockDecoder();
  virtual ObReaderType get_type() override { return Decoder; }
  virtual void reset();
  virtual int init(
      const ObMicroBlockData &block_data,
      const storage::ObITableReadInfo &read_info) override;
  //when there is not read_info in input parameters, it indicates reading all columns from all rows
  //when the incoming datum_utils is nullptr, it indicates not calling locate_range or find_bound
  virtual int init(
      const ObMicroBlockData &block_data,
	  const ObStorageDatumUtils *datum_utils) override;
  virtual int get_row(const int64_t index, ObDatumRow &row) override;
  virtual int get_row_header(
      const int64_t row_idx,
      const ObRowHeader *&row_header) override;
  virtual int get_row_count(int64_t &row_count) override;
  virtual int get_multi_version_info(
      const int64_t row_idx,
      const int64_t schema_rowkey_cnt,
      const ObRowHeader *&row_header,
      int64_t &version,
      int64_t &sql_sequence);
  int compare_rowkey(
      const ObDatumRowkey &rowkey,
      const int64_t index,
      int32_t &compare_result);
  int compare_rowkey(
      const ObDatumRange &range,
      const int64_t index,
      int32_t &start_key_compare_result,
      int32_t &end_key_compare_result);
  static int get_decoder_cache_size(
      const char *block,
      const int64_t block_size,
      int64_t &size);
  static int cache_decoders(
      char *buf,
      const int64_t size,
      const char *block,
      const int64_t block_size);
  static int update_cached_decoders(char *cache, const int64_t cache_size,
      const char *old_block, const char *cur_block, const int64_t block_size);
  // Filter interface for filter pushdown
  int filter_pushdown_filter(
      const sql::ObPushdownFilterExecutor *parent,
      sql::ObBlackFilterExecutor &filter,
      const storage::PushdownFilterInfo &pd_filter_info,
      common::ObBitmap &result_bitmap);
  int filter_pushdown_filter(
      const sql::ObPushdownFilterExecutor *parent,
      sql::ObWhiteFilterExecutor &filter,
      const storage::PushdownFilterInfo &pd_filter_info,
      common::ObBitmap &result_bitmap);
  int filter_pushdown_retro(
      const sql::ObPushdownFilterExecutor *parent,
      sql::ObWhiteFilterExecutor &filter,
      const storage::PushdownFilterInfo &pd_filter_info,
      const int32_t col_offset,
      const share::schema::ObColumnParam *col_param,
      common::ObObj &decoded_obj,
      common::ObBitmap &result_bitmap);
  int get_rows(
      const common::ObIArray<int32_t> &cols,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const int64_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObIArray<ObDatum *> &datums);
  virtual int get_row_count(
      int32_t col_id,
      const int64_t *row_ids,
      const int64_t row_cap,
      const bool contains_null,
      int64_t &count) override final;
  int get_min_or_max(
      int32_t col_id,
      const int64_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      ObDatum *datum_buf,
      ObMicroBlockAggInfo<ObDatum> &agg_info);
  virtual int64_t get_column_count() const override
  {
    OB_ASSERT(nullptr != header_);
    return header_->column_count_;
  }

protected:
  virtual int find_bound(const ObDatumRowkey &key,
                         const bool lower_bound,
                         const int64_t begin_idx,
                         int64_t &row_idx,
                         bool &equal) override;
  virtual int find_bound(const ObDatumRange &range,
                         const int64_t begin_idx,
                         int64_t &row_idx,
                         bool &equal,
                         int64_t &end_key_begin_idx,
                         int64_t &end_key_end_idx) override;
private:
  // use inner_reset to reuse the decoder buffer
  // the column count would not change in most cases
  void inner_reset();
  int do_init(const ObMicroBlockData &block_data);
  int init_decoders();
  int add_decoder(const int64_t store_idx,
                  const common::ObObjMeta &obj_meta,
                  ObColumnDecoder &dest);
  void free_decoders();
  int decode_cells(const uint64_t row_id,
                   const int64_t row_len,
                   const char *row_data,
                   const int64_t col_begin,
                   const int64_t col_end,
                   ObStorageDatum *datums);

  int get_col_datums(int32_t col_id,
                     const int64_t *row_ids,
                     const char **cell_datas,
                     const int64_t row_cap,
                     common::ObDatum *col_datums);
  //TODO @hanhui deleted after change rowkey to datum
  int decode_cells(const uint64_t row_id,
                   const int64_t row_len,
                   const char *row_data,
                   const int64_t col_begin,
                   const int64_t col_end,
                   common::ObObj *objs);
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
  ObDecoderAllocator *allocator_;
  ObDecoderCtxArray *ctx_array_;
  ObColumnDecoderCtx **ctxs_;
  static ObNoneExistColumnDecoder none_exist_column_decoder_;
  static ObColumnDecoderCtx none_exist_column_decoder_ctx_;
  common::ObArenaAllocator decoder_allocator_;
  common::ObArenaAllocator buf_allocator_;
};

}
}
#endif //OB_STORAGE_BLOCKSSTABLE_ENCODING_OB_MICRO_BLOCK_DECODER_H_
