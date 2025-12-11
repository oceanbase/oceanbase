/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#include "observer/table_load/backup/ob_table_load_backup_imicro_block_reader.h"
#include "observer/table_load/backup/encoding/ob_icolumn_decoder.h"
#include "observer/table_load/backup/encoding/ob_encoding_allocator.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

struct ObBlockCachedDecoderHeader
{
  struct Col {
    uint16_t offset_;
    int16_t ref_col_idx_;
  };
  uint16_t count_;
  uint16_t col_count_;
  Col col_[0];

  const ObIColumnDecoder &at(const int64_t idx) const
  {
    return *reinterpret_cast<const ObIColumnDecoder *>(
        (reinterpret_cast<const char *>(&col_[count_]) + col_[idx].offset_));
  }
} __attribute__((packed));

struct ObDecoderArrayAllocator
{
  ObDecoderArrayAllocator(char *buf) : buf_(buf), offset_(0) {}
  template <typename T>
  int alloc(T *&t);

  template <typename T>
  void free(const T *)
  {
    // do nothing
  }

private:
  char *buf_;
  int64_t offset_;
};


class ObDecoderCtxArray
{
public:
  typedef ObColumnDecoderCtx ObDecoderCtx;
  ObDecoderCtxArray() {};
  virtual ~ObDecoderCtxArray()
  {
    FOREACH(it, ctxs_) {
      ObDecoderCtx *c = *it;
      OB_DELETE(ObDecoderCtx, tl_decode_attr, c);
    }
    ctxs_.reset();
  }

  TO_STRING_KV(K_(ctxs));

  ObDecoderCtx **get_ctx_array(int64_t size);

private:
  static const int64_t LOCAL_CTX_CNT = 128;
  ObSEArray<ObDecoderCtx *, LOCAL_CTX_CNT> ctxs_;

  DISALLOW_COPY_AND_ASSIGN(ObDecoderCtxArray);
};

class ObTLDecoderCtxArray
{
public:
  ObTLDecoderCtxArray() {}

  virtual ~ObTLDecoderCtxArray()
  {
    FOREACH(it, ctxs_array_) {
      ObDecoderCtxArray *ctxs = *it;
      OB_DELETE(ObDecoderCtxArray, tl_decode_attr, ctxs);
    }
  }

  static ObDecoderCtxArray *alloc();

  static void free(ObDecoderCtxArray *ctxs);

private:
  static ObTLDecoderCtxArray *instance() { return GET_TSI_MULT(ObTLDecoderCtxArray, 1); }

private:
  static const int64_t MAX_ARRAY_CNT = 32;
  ObSEArray<ObDecoderCtxArray *, MAX_ARRAY_CNT> ctxs_array_;

  DISALLOW_COPY_AND_ASSIGN(ObTLDecoderCtxArray);
};

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
  int decode(common::ObObj &cell,
             const int64_t row_id,
             const ObBitStream &bs,
             const char *data,
             const int64_t len);

  // used for locate row in micro block rowkey object compare
  // performance critical, do not check parameters
  int quick_compare(const common::ObObj &left,
                    const int64_t row_id,
                    const ObBitStream &bs,
                    const char *data,
                    const int64_t len,
                    int32_t &cmp_ret);
  int batch_decode(const ObIRowIndex *row_index,
                   const int64_t *row_ids,
                   const char **cell_datas,
                   const int64_t row_cap,
                   common::ObDatum *datums);
  int get_row_count(const ObIRowIndex *row_index,
                    const int64_t *row_ids,
                    const int64_t row_cap,
                    const bool contains_null,
                    int64_t &count);
private:
  int try_convert_number_to_int(const ObObj &original, ObObj &target, bool &is_converted);
public:
  const ObIColumnDecoder *decoder_;
  ObColumnDecoderCtx *ctx_;
};

class ObMicroBlockDecoder : public ObIMicroBlockReader
{
public:
  ObMicroBlockDecoder();
  virtual ~ObMicroBlockDecoder();
  int init(const ObMicroBlockData *micro_block_data,
           const ObTableLoadBackupVersion &backup_version,
           const ObIColumnMap *column_map) override;
  void reset() override;
  int get_row(const int64_t idx, common::ObNewRow &row) override;
private:
  // use inner_reset to reuse the decoder buffer
  // the column count would not change in most cases
  void inner_reset();
  int get_micro_metas(const char *block,
                      const int64_t block_size,
                      const int64_t col_cnt,
                      const ObMicroBlockHeaderV2 *&header,
                      const ObColumnHeader *&col_header,
                      const char *&meta_data);
  int init_decoders();
  int add_decoder(const int64_t store_idx, const common::ObObjMeta &obj_meta, ObColumnDecoder &dest);
  template <typename Allocator>
  static int acquire(Allocator &allocator,
                     const common::ObObjMeta &obj_meta,
                     const ObMicroBlockHeaderV2 &header,
                     const ObColumnHeader &col_header,
                     const char *meta_data,
                     const ObIColumnDecoder *&decoder);
  int acquire(const common::ObObjMeta &obj_meta, int64_t store_idx,
      const ObIColumnDecoder *&decoder);
  void free_decoders();
  void release(const ObIColumnDecoder *decoder);
  int decode_cells(const uint64_t row_id,
                   const int64_t row_len,
                   const char *row_data,
                   const int64_t col_begin,
                   const int64_t col_end,
                   common::ObObj *objs);
private:
  static ObNoneExistColumnDecoder none_exist_column_decoder_;
  static ObColumnDecoderCtx none_exist_column_decoder_ctx_;
  ObDecoderAllocator *allocator_;
  common::ObArenaAllocator buf_allocator_;
  common::ObArenaAllocator cast_allocator_;
  common::ObArenaAllocator decoder_allocator_;
  const ObIColumnMap *column_map_;
  int64_t request_cnt_; // request column count
  int64_t store_cnt_; // store column count
  int64_t rowkey_cnt_; // rowkey column count
  void *decoder_buf_;
  ObColumnDecoder *decoders_;
  const ObMicroBlockHeaderV2 *block_header_;
  const ObColumnHeader *col_header_;
  const char *meta_data_;
  ObDecoderCtxArray *ctx_array_;
  ObColumnDecoderCtx **ctxs_;
  const char *row_data_;
  const ObBlockCachedDecoderHeader *cached_decoder_header_;
  bool need_cast_;
  ObIRowIndex *row_index_;
  ObVarRowIndex var_row_index_;
  ObFixRowIndex fix_row_index_;
  // array size is double of max_column_count, because we may need two decoders for one column,
  // e.g.: column equal encoding, one for column equal decoder and one for referenced column decoder.
  const ObIColumnDecoder *need_release_decoders_[common::OB_ROW_MAX_COLUMNS_COUNT * 2];
  int64_t need_release_decoder_cnt_;
  bool is_inited_;
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
