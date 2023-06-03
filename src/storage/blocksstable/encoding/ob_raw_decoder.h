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

#ifndef OCEANBASE_ENCODING_OB_RAW_DECODER_H_
#define OCEANBASE_ENCODING_OB_RAW_DECODER_H_

#include "ob_icolumn_decoder.h"
#include "ob_encoding_util.h"
#include "ob_encoding_query_util.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "ob_integer_array.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObColumnHeader;
class ObIRowIndex;

typedef void (*raw_fix_batch_decode_func)(
            const int64_t col_len,
            const char *base_data,
            const int64_t *row_ids,
            const int64_t row_cap,
            common::ObDatum *datums);

typedef void (*raw_var_batch_decode_func)(
            const char *base_data,
            const char *row_idx_data,
            const int64_t header_off,
            const int64_t header_len,
            const int64_t header_var_col_cnt,
            const int64_t *row_ids,
            const int64_t row_cap,
            common::ObDatum *datums);

typedef void (*fix_filter_func)(
            const int64_t row_cnt,
            const unsigned char *col_data,
            const uint64_t node_value,
            sql::ObBitVector &res);

class ObRawDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::RAW;

  ObRawDecoder()
    : store_class_(ObExtendSC), integer_mask_(0), meta_data_(nullptr) {}
  virtual ~ObRawDecoder() {}

  OB_INLINE int init(
      const ObMicroBlockHeader &micro_block_header,
      const ObColumnHeader &column_header,
      const char *meta);

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  virtual int decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  void reset() { this->~ObRawDecoder(); new (this) ObRawDecoder(); }
  OB_INLINE void reuse();
  template <typename Header>
  static int locate_cell_data(const char *&cell_data, int64_t &cell_len,
      const char *data, const int64_t len,
      const ObMicroBlockHeader &micro_block_header_, const ObColumnHeader &col_header,
      const Header &header);

  // Batch locate cell data for batch decode
  // To save stack memory usage, @data and @datums contains input parameter of row data and row len,
  // and output would be stored to this tow parameters as cell_data and cell_length
  template <typename Header>
  static int batch_locate_cell_data(
      const char **datas, common::ObDatum *datums, const int64_t row_cap,
      const ObMicroBlockHeader &micro_block_header_, const ObColumnHeader &col_header,
      const Header &header);

  virtual ObColumnHeader::Type get_type() const override { return type_; }

  bool is_inited() const { return NULL != meta_data_; }
  virtual int batch_decode(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int64_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const override;

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      ObBitmap &result_bitmap) const override;

  virtual int get_null_count(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex *row_index,
      const int64_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const override;
private:
  bool fast_decode_valid(const ObColumnDecoderCtx &ctx) const;

  bool fast_filter_valid(
      const ObColumnDecoderCtx &ctx,
      const ObObjType &filter_value_type,
      int32_t &fix_length,
      bool &is_signed_data) const;

  int batch_decode_general(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int64_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const;

  int get_is_null_bitmap(
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const ObIRowIndex* row_index,
      ObBitmap &result_bitmap) const;

  int fast_comparison_operator(
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      int32_t fix_len_tag,
      bool is_signed_data,
      ObBitmap &result_bitmap) const;

  int comparison_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const ObIRowIndex* row_index,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int bt_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const ObIRowIndex* row_index,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int in_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const ObIRowIndex* row_index,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int load_data_to_obj_cell(const ObObjMeta cell_meta, const char *cell_data, int64_t cell_len, ObObj &load_obj) const;

  int traverse_all_data(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const ObIRowIndex* row_index,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap,
      int (*lambda)(
          const ObObj &cur_obj,
          const sql::ObWhiteFilterExecutor &filter,
          bool &result)) const;

private:
  ObObjTypeStoreClass store_class_;
  uint64_t integer_mask_;
  const char *meta_data_;
};

template <typename Header>
int ObRawDecoder::locate_cell_data(const char *&cell_data, int64_t &cell_len,
    const char *data, const int64_t len,
    const ObMicroBlockHeader &micro_block_header,
    const ObColumnHeader &col_header,  const Header &header)
{
  int ret = common::OB_SUCCESS;
  if (NULL == data || len < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(data), K(len));
  } else {
    if (col_header.is_fix_length()) {
      cell_data = data + header.offset_;
      cell_len = header.length_;
    } else {
      const char *var_data = data + header.offset_;
      if (1 == micro_block_header.var_column_count_) {
        cell_data = var_data;
        cell_len = len - header.offset_;
      } else {
        int8_t col_idx_byte = *var_data;
        var_data += sizeof(col_idx_byte);
        ObIntegerArrayGenerator gen;
        if (OB_FAIL(gen.init(var_data, col_idx_byte))) {
          STORAGE_LOG(WARN, "init integer array generator failed", K(ret), K(header));
        } else {
          var_data += (micro_block_header.var_column_count_ - 1) * col_idx_byte;
          int64_t offset = 0;
          if (0 != header.length_) {
            offset = gen.get_array().at(header.length_ - 1);
          }
          if (col_header.is_last_var_field()) {
            cell_len = len - offset - (var_data - data);
          } else {
            cell_len = gen.get_array().at(header.length_) - offset;
          }
          cell_data = var_data + offset;
        }
      }
    }
  }
  return ret;
}

template <typename Header>
int ObRawDecoder::batch_locate_cell_data(
    const char **datas, common::ObDatum *datums, const int64_t row_cap,
    const ObMicroBlockHeader &micro_block_header_, const ObColumnHeader &col_header,
    const Header &header)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(datas) || OB_ISNULL(datums)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(datas), KP(datums));
  } else {
    if (col_header.is_fix_length()) {
      for (int64_t i = 0; i < row_cap; ++i) {
        if (col_header.has_extend_value() && datums[i].is_null()) {
        } else {
          datas[i] += header.offset_;
          datums[i].pack_ = header.length_;
        }
      }
    } else if (1 == micro_block_header_.var_column_count_) {
      for (int64_t i = 0; i < row_cap; ++i) {
        if (col_header.has_extend_value() && datums[i].is_null()) {
        } else {
          datas[i] += header.offset_;
          datums[i].pack_ -= header.offset_;
        }
      }
    } else {
      ObIntegerArrayGenerator gen;
      int8_t col_idx_byte = 0;
      if (col_header.is_last_var_field()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
          if (col_header.has_extend_value() && datums[i].is_null()) {
          } else {
            const char *var_data = datas[i] + header.offset_;
            col_idx_byte = *var_data;
            var_data += sizeof(col_idx_byte);
            if (OB_FAIL(gen.init(var_data - col_idx_byte, col_idx_byte))) {
              STORAGE_LOG(WARN, "init integer array generator failed", K(ret), K(header));
            } else {
              var_data += (micro_block_header_.var_column_count_ - 1) * col_idx_byte;
              // 0 if header.length_ == 0
              int64_t offset = gen.get_array().at(header.length_)
                  & (static_cast<int64_t>(header.length_ == 0) - 1);
              const int64_t datum_offset_in_row = offset + (var_data - datas[i]);
              // datum_offset_in_row is ensured to be included in range of int32
              datums[i].pack_ = datums[i].pack_ - static_cast<const int32_t>(datum_offset_in_row);
              datas[i] = var_data + offset;
            }
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
          if (col_header.has_extend_value() && datums[i].is_null()) {
          } else {
            const char *var_data = datas[i] + header.offset_;
            col_idx_byte = *var_data;
            var_data += sizeof(col_idx_byte);
            if (OB_FAIL(gen.init(var_data - col_idx_byte, col_idx_byte))) {
              STORAGE_LOG(WARN, "init integer array generator failed", K(ret), K(header));
            } else {
              var_data += (micro_block_header_.var_column_count_ - 1) * col_idx_byte;
              // 0 if header.length_ == 0
              int64_t offset = gen.get_array().at(header.length_)
                  & (static_cast<int64_t>(header.length_ == 0) - 1);
              datums[i].pack_ = gen.get_array().at(header.length_ + 1) - offset;
              datas[i] = var_data + offset;
            }
          }
        }
      }
    }
  }
  return ret;
}

OB_INLINE int ObRawDecoder::init(
      const ObMicroBlockHeader &micro_block_header,
      const ObColumnHeader &column_header,
      const char *meta)
{
  UNUSEDx(micro_block_header);
  int ret = common::OB_SUCCESS;
  if (is_inited()) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    const ObObjType store_type = column_header.get_store_obj_type();
    const common::ObObjTypeClass type_class = ob_obj_type_class(store_type);
    store_class_ = get_store_class_map()[type_class];
    if (common::ObIntTC == type_class) {
      int64_t type_store_size = get_type_size_map()[store_type];
      integer_mask_ = ~INTEGER_MASK_TABLE[type_store_size];
    } else {
      integer_mask_ = 0;
    }
    meta_data_ = meta + column_header.offset_;
  }
  return ret;
}

OB_INLINE void ObRawDecoder::reuse()
{
  meta_data_ = NULL;
  /*
  obj_meta_.reset();
  store_class_ = ObExtendSC;
  integer_mask_ = 0;
  micro_block_header_ = NULL;
  header_ = NULL;
  */
}

template <bool IS_SIGNED, int32_t LEN_TAG, int32_t CMP_TYPE>
struct RawFixFilterFunc_T
{
  static void fix_filter_func(
      const int64_t row_cnt,
      const unsigned char *col_data,
      const uint64_t node_value,
      sql::ObBitVector &res)
  {
    typedef typename ObEncodingTypeInference<IS_SIGNED, LEN_TAG>::Type DataType;
    const DataType *values = reinterpret_cast<const DataType *>(col_data);
    DataType right_value = *reinterpret_cast<const DataType *>(&node_value);
    for (int64_t row_id = 0; row_id < row_cnt; ++row_id) {
      if (value_cmp_t<DataType, CMP_TYPE>(values[row_id], right_value)) {
        res.set(row_id);
      }
    }
  }
};

extern ObMultiDimArray_T<fix_filter_func, 2, 4, 6> raw_fix_fast_filter_funcs;
extern bool raw_fix_fast_filter_funcs_inited;


} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_RAW_DECODER_H_
