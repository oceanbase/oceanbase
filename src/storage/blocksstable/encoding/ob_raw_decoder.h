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
            const int32_t *row_ids,
            const int64_t row_cap,
            common::ObDatum *datums);

typedef void (*raw_var_batch_decode_func)(
            const char *base_data,
            const char *row_idx_data,
            const int64_t header_off,
            const int64_t header_len,
            const int64_t header_var_col_cnt,
            const int32_t *row_ids,
            const int64_t row_cap,
            common::ObDatum *datums);

typedef void (*fix_filter_func)(
            const unsigned char *col_data,
            const uint64_t node_value,
            const sql::PushdownFilterInfo &pd_filter_info,
            sql::ObBitVector &res);

typedef void (*raw_compare_function)(
            const unsigned char* raw_data,
            const uint64_t node_value,
            uint8_t* selection,
            uint32_t from,
            uint32_t to);

typedef void (*raw_compare_function_with_null)(
            const unsigned char* raw_data,
            const uint64_t node_value,
            const uint64_t null_node_value,
            uint8_t* selection,
            uint32_t from,
            uint32_t to);

typedef void (*raw_min_max_function)(
            const unsigned char *raw_data,
            uint32_t from,
            uint32_t to,
            uint64_t &res);

typedef void (*raw_min_max_function_with_null)(
            const unsigned char *raw_data,
            const uint64_t null_value,
            uint32_t from,
            uint32_t to,
            uint64_t &res);

typedef void (*raw_min_max_function_with_null_bitmap)(
            const unsigned char* raw_data,
            const uint8_t *null_bitmap,
            uint32_t from,
            uint32_t to,
            uint64_t &res);

class RawCompareFunctionFactory {
public:
  static constexpr uint32_t IS_SIGNED_CNT = 2;
  static constexpr uint32_t FIX_LEN_TAG_CNT = 4;
  static constexpr uint32_t OP_TYPE_CNT = 6;
public:
  static RawCompareFunctionFactory &instance();
  raw_compare_function get_cmp_function(
      const bool is_signed,
      const int32_t fix_len_tag,
      const sql::ObWhiteFilterOperatorType op_type);
  raw_compare_function_with_null get_cs_cmp_function_with_null(
      const int32_t fix_len_tag,
      const sql::ObWhiteFilterOperatorType op_type);
private:
  RawCompareFunctionFactory();
  ~RawCompareFunctionFactory() = default;
  DISALLOW_COPY_AND_ASSIGN(RawCompareFunctionFactory);
private:
  ObMultiDimArray_T<raw_compare_function, IS_SIGNED_CNT, FIX_LEN_TAG_CNT, OP_TYPE_CNT> functions_array_;
  ObMultiDimArray_T<raw_compare_function_with_null, FIX_LEN_TAG_CNT, OP_TYPE_CNT> cs_functions_with_null_array_;
};

class RawAggFunctionFactory
{
public:
  static constexpr uint32_t FIX_LEN_TAG_CNT = 4;
  static constexpr uint32_t MIN_MAX_CNT = 2;
public:
  static RawAggFunctionFactory &instance();
  raw_min_max_function get_min_max_function(
      const int32_t fix_len_tag,
      const bool is_min);
  raw_min_max_function_with_null get_cs_min_max_function_with_null(
      const int32_t fix_len_tag,
      const bool is_min);
  raw_min_max_function_with_null_bitmap get_cs_min_max_function_with_null_bitmap(
      const int32_t fix_len_tag,
      const bool is_min);
private:
  RawAggFunctionFactory();
  ~RawAggFunctionFactory() = default;
  DISALLOW_COPY_AND_ASSIGN(RawAggFunctionFactory);
private:
  ObMultiDimArray_T<raw_min_max_function, FIX_LEN_TAG_CNT, MIN_MAX_CNT> min_max_functions_array_;
  ObMultiDimArray_T<raw_min_max_function_with_null, FIX_LEN_TAG_CNT, MIN_MAX_CNT> cs_min_max_functions_with_null_array_;
  ObMultiDimArray_T<raw_min_max_function_with_null_bitmap, FIX_LEN_TAG_CNT, MIN_MAX_CNT> cs_min_max_functions_with_null_bitmap_array_;
};


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

  virtual int decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
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
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const override;

  virtual int decode_vector(
      const ObColumnDecoderCtx &decoder_ctx,
      const ObIRowIndex* row_index,
      ObVectorDecodeCtx &vector_ctx) const override;

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const override;

  virtual int get_null_count(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex *row_index,
      const int32_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const override;
  virtual bool fast_decode_valid(const ObColumnDecoderCtx &ctx) const override;
private:

  int check_fast_filter_valid(
      const ObColumnDecoderCtx &ctx,
      const sql::ObWhiteFilterExecutor &filter,
      int32_t &fix_length,
      bool &is_signed_data,
      bool &valid) const;

  int batch_decode_general(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const;
  int batch_decode_fast(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums) const;

  int get_is_null_bitmap(
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const ObIRowIndex* row_index,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  // For binary data
  int fast_binary_comparison_operator(
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      int32_t fix_len_tag,
      bool is_signed_data,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  // For structured data
  int fast_datum_comparison_operator(
      const ObColumnDecoderCtx &col_ctx,
      const ObIRowIndex* row_index,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitma) const;

  int comparison_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const ObIRowIndex* row_index,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int bt_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const ObIRowIndex* row_index,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int in_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const ObIRowIndex* row_index,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  template<typename Operator>
  int traverse_all_data(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const ObIRowIndex* row_index,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap,
      Operator const &eval) const;
private:
  class ObRawDecoderFilterCmpFunc
  {
  public:
    ObRawDecoderFilterCmpFunc(
        const ObDatumCmpFuncType &type_cmp_func,
        const ObGetFilterCmpRetFunc &get_cmp_ret)
      : type_cmp_func_(type_cmp_func), get_cmp_ret_(get_cmp_ret) {}
    ~ObRawDecoderFilterCmpFunc() = default;
    int operator()(
        const ObDatum &cur_datum,
        const sql::ObWhiteFilterExecutor &filter,
        bool &result) const;
  private:
    const ObDatumCmpFuncType &type_cmp_func_;
    const ObGetFilterCmpRetFunc &get_cmp_ret_;
  };
  class ObRawDecoderFilterBetweenFunc
  {
  public:
    ObRawDecoderFilterBetweenFunc(const ObDatumCmpFuncType &type_cmp_func) : type_cmp_func_(type_cmp_func)
    {
      get_le_cmp_ret_ = get_filter_cmp_ret_func(sql::WHITE_OP_LE);
      get_ge_cmp_ret_ = get_filter_cmp_ret_func(sql::WHITE_OP_GE);
    }
    ~ObRawDecoderFilterBetweenFunc() = default;
    int operator()(
        const ObDatum &cur_datum,
        const sql::ObWhiteFilterExecutor &filter,
        bool &result) const;
  private:
    const ObDatumCmpFuncType &type_cmp_func_;
    ObGetFilterCmpRetFunc get_le_cmp_ret_;
    ObGetFilterCmpRetFunc get_ge_cmp_ret_;
  };
  class ObRawDecoderFilterInFunc
  {
  public:
    ObRawDecoderFilterInFunc() {}
    ~ObRawDecoderFilterInFunc() = default;
    int operator()(
        const ObDatum &cur_datum,
        const sql::ObWhiteFilterExecutor &filter,
        bool &result) const;
  };

  int decode_vector_bitpacked(
      const ObColumnDecoderCtx &decoder_ctx,
      ObVectorDecodeCtx &vector_ctx) const;
  template<typename VectorType, bool HAS_NULL>
  int decode_vector_bitpacked(
      const ObColumnDecoderCtx &decoder_ctx,
      ObVectorDecodeCtx &vector_ctx) const;

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
      const unsigned char *col_data,
      const uint64_t node_value,
      const sql::PushdownFilterInfo &pd_filter_info,
      sql::ObBitVector &res)
  {
    typedef typename ObEncodingTypeInference<IS_SIGNED, LEN_TAG>::Type DataType;
    const DataType *values = reinterpret_cast<const DataType *>(col_data);
    DataType right_value = *reinterpret_cast<const DataType *>(&node_value);
    int64_t row_id = 0;
    for (int64_t offset = 0; offset < pd_filter_info.count_; ++offset) {
      row_id = offset + pd_filter_info.start_;
      if (value_cmp_t<DataType, CMP_TYPE>(values[row_id], right_value)) {
        res.set(offset);
      }
    }
  }
};

extern ObMultiDimArray_T<fix_filter_func, 2, 4, 6> raw_fix_fast_filter_funcs;
extern bool raw_fix_fast_filter_funcs_inited;


} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_RAW_DECODER_H_
