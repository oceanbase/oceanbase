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

#define USING_LOG_PREFIX STORAGE

#include "ob_cs_encoding_util.h"
#include "lib/wide_integer/ob_wide_integer_cmp_funcs.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

const int64_t ObCSEncodingUtil::ENCODING_ROW_COUNT_THRESHOLD = 4;
// limit by typedef ObPodFix2dArray<ObDatum, 1 << 20, common::OB_MALLOC_NORMAL_BLOCK_SIZE> ObColDatums;
const int64_t ObCSEncodingUtil::MAX_MICRO_BLOCK_ROW_CNT = 1L << 20; // 1M
const int64_t ObCSEncodingUtil::DEFAULT_DATA_BUFFER_SIZE = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
const int64_t ObCSEncodingUtil::MAX_BLOCK_ENCODING_STORE_SIZE = 2 * DEFAULT_DATA_BUFFER_SIZE;
const int64_t ObCSEncodingUtil::MAX_COLUMN_ENCODING_STORE_SIZE = MAX_BLOCK_ENCODING_STORE_SIZE - 64L * 1024;  // reserved for block header

int64_t ObCSEncodingUtil::get_bit_size(const uint64_t v)
{
  int64_t bit_size = 1;
  if (v > 0) {
    bit_size = sizeof(v) * CHAR_BIT - __builtin_clzl(v);
  }
  return bit_size;
}
int ObCSEncodingUtil::build_cs_column_encoding_ctx(ObEncodingHashTable *ht,
  const ObObjTypeStoreClass store_class, const int64_t precision_bytes,
  ObColumnCSEncodingCtx &col_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ht)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ht is null", K(ret));
  } else {
    col_ctx.null_cnt_ = ht->get_null_list().size_;
    col_ctx.nope_cnt_ = ht->get_nope_list().size_;
    col_ctx.ht_ = ht;

    switch (store_class) {
    case ObIntSC: {
      int ret = OB_SUCCESS;
      int64_t int_min = INT64_MAX;
      int64_t int_max = INT64_MIN;
      int64_t value = 0;
      const int64_t row_count = ht->get_node_cnt();
      for (int64_t i = 0; i < row_count; ++i) {
        const ObDatum &datum = *ht->get_node_list()[i].datum_;
        if (!datum.is_null()) {
          value = datum.get_int();
          if (value < int_min) {
            int_min = value;
          }
          if (value > int_max) {
            int_max = value;
          }
        }
      }
      col_ctx.integer_min_ = static_cast<uint64_t>(int_min);
      col_ctx.integer_max_ = static_cast<uint64_t>(int_max);
      break;
    }

    case ObUIntSC: {
      int ret = OB_SUCCESS;
      int64_t uint_min = UINT64_MAX;
      int64_t uint_max = 0;
      uint64_t value = 0;
      const int64_t row_count = ht->get_node_cnt();
      for (int64_t i = 0; i < row_count; ++i) {
        const ObDatum &datum = *ht->get_node_list()[i].datum_;
        if (!datum.is_null()) {
          value = datum.get_uint64();
          if (value < uint_min) {
            uint_min = value;
          }
          if (value > uint_max) {
            uint_max = value;
          }
        }
      }
      col_ctx.integer_min_ = uint_min;
      col_ctx.integer_max_ = uint_max;
      break;
    }

    case ObDecimalIntSC: {
      int ret = OB_SUCCESS;
      int64_t int_min = INT64_MAX;
      int64_t int_max = INT64_MIN;
      int64_t value = 0;
      const int64_t int64_min = INT64_MIN;
      const int64_t int64_max = INT64_MAX;
      col_ctx.fix_data_size_ = -1;
      col_ctx.is_wide_int_ = false;
      decint_cmp_fp cmp = wide::ObDecimalIntCmpSet::get_decint_decint_cmp_func(precision_bytes, sizeof(int64_t));
      const int64_t row_count = ht->get_node_cnt();
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
        const ObDatum &datum = *ht->get_node_list()[i].datum_;
        if (!datum.is_null()) {
          if (OB_UNLIKELY(datum.len_ != precision_bytes)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("datum len is not match with precision bytes",
                K(ret), K(datum), K(precision_bytes), K(i), K(row_count));
          } else if (cmp(datum.get_decimal_int(), (ObDecimalInt*)&int64_min) < 0 || cmp(datum.get_decimal_int(), (ObDecimalInt*)&int64_max) > 0) {
            col_ctx.is_wide_int_ = true;
            break;
          } else { // value range is not over int64_t, store as integer
            int64_t value = 0;
            if (sizeof(int32_t) == precision_bytes) {
              value = datum.get_decimal_int32();
            } else {
              value = datum.get_decimal_int64();
            }
            if (value < int_min) {
              int_min = value;
            }
            if (value > int_max) {
              int_max = value;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        col_ctx.integer_min_ = static_cast<uint64_t>(int_min);
        col_ctx.integer_max_ = static_cast<uint64_t>(int_max);

        if (col_ctx.is_wide_int_) { // store as fixed len string
          col_ctx.fix_data_size_ = precision_bytes;
          FOREACH(l, *ht)
          {
            const int64_t len = l->header_->datum_->len_;
            col_ctx.var_data_size_ += len * l->size_;
            col_ctx.dict_var_data_size_ += len;
          }
        }
      }
      break;
    }

    case ObNumberSC: {
      col_ctx.fix_data_size_ = -1;
      bool var_store = false;
      FOREACH(l, *ht)
      {
        const ObDatum &datum = *l->header_->datum_;
        const int64_t len =
          sizeof(ObNumberDesc) + datum.num_->desc_.len_ * sizeof(datum.num_->digits_[0]);
        col_ctx.var_data_size_ += len * l->size_;
        col_ctx.dict_var_data_size_ += len;
        if (!var_store) {
          if (col_ctx.fix_data_size_ < 0) {
            col_ctx.fix_data_size_ = len;
          } else if (len != col_ctx.fix_data_size_) {
            col_ctx.fix_data_size_ = -1;
            var_store = true;
          }
        }
      }
      break;
    }
    case ObStringSC:
    case ObTextSC:
    case ObJsonSC:
    case ObGeometrySC:
    case ObRoaringBitmapSC: { // geometry, json and text storage class have the same behavior currently
      col_ctx.fix_data_size_ = -1;
      col_ctx.max_string_size_ = -1;
      bool var_store = false;
      FOREACH(l, *ht)
      {
        const int64_t len = l->header_->datum_->len_;
        col_ctx.max_string_size_ = len > col_ctx.max_string_size_ ? len : col_ctx.max_string_size_;
        col_ctx.var_data_size_ += len * l->size_;
        col_ctx.dict_var_data_size_ += len;
        if (!col_ctx.has_zero_length_datum_ && 0 == len) {
          col_ctx.has_zero_length_datum_ = true;
        }
        if (!var_store) {
          if (col_ctx.fix_data_size_ < 0) {
            col_ctx.fix_data_size_ = len;
          } else if (len != col_ctx.fix_data_size_) {
            col_ctx.fix_data_size_ = -1;
            var_store = true;
          }
        }
      }
      break;
    }

    case ObOTimestampSC: {
      col_ctx.fix_data_size_ = -1;
      bool var_store = false;
      FOREACH(l, *ht)
      {
        const int64_t len = l->header_->datum_->len_;
        col_ctx.var_data_size_ += len * l->size_;
        col_ctx.dict_var_data_size_ += len;
        if (!var_store) {
          if (col_ctx.fix_data_size_ < 0) {
            col_ctx.fix_data_size_ = len;
          } else if (len != col_ctx.fix_data_size_) {
            col_ctx.fix_data_size_ = -1;
          var_store = true;
          }
        }
      }
      break;
    }

    case ObIntervalSC: {
      col_ctx.fix_data_size_ = -1;
      bool var_store = false;
      FOREACH(l, *ht)
      {
        const int64_t len = l->header_->datum_->len_;
        col_ctx.var_data_size_ += len * l->size_;
        col_ctx.dict_var_data_size_ += len;
        if (!var_store) {
          if (col_ctx.fix_data_size_ < 0) {
            col_ctx.fix_data_size_ = len;
          } else if (len != col_ctx.fix_data_size_) {
            col_ctx.fix_data_size_ = -1;
            var_store = true;
          }
        }
      }
      break;
    }

    default:
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("not supported store class", K(ret), K(store_class));
    }
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
