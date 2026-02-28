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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_parquet_dict_filter.h"

#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "storage/blocksstable/encoding/ob_encoding_util.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/ob_storage_util.h"

namespace oceanbase
{
namespace sql
{

ObParquetDictFilterPushdown::ObParquetDictFilterPushdown()
    : dict_columns_(), dict_encoding_check_cache_(), decode_exprs_(), processed_dict_op_filters_(),
      failed_dict_filters_(), is_inited_(false)
{
}

ObParquetDictFilterPushdown::~ObParquetDictFilterPushdown()
{
  reset();
  dict_columns_.destroy();
  dict_encoding_check_cache_.destroy();
  column_need_decode_cache_.destroy();
}

int ObParquetDictFilterPushdown::init(int64_t column_count)
{
  int ret = OB_SUCCESS;
  mem_attr_ = ObMemAttr(MTL_ID(), "ParquetDict");
  allocator_.set_attr(mem_attr_);

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(dict_columns_.create(column_count, mem_attr_))) {
    LOG_WARN("fail to create dict columns hash map", K(ret), K(column_count));
  } else if (OB_FAIL(dict_encoding_check_cache_.create(column_count, mem_attr_))) {
    LOG_WARN("fail to create dict encoding check cache", K(ret), K(column_count));
  } else if (OB_FAIL(column_need_decode_cache_.create(column_count, mem_attr_))) {
    LOG_WARN("fail to create column need decode cache", K(ret), K(column_count));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObParquetDictFilterPushdown::init_decode_exprs(const common::ObIArray<ObExpr *> &decode_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("dict filter pushdown not inited", K(ret));
  } else {
    decode_exprs_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < decode_exprs.count(); ++i) {
      if (OB_FAIL(decode_exprs_.push_back(decode_exprs.at(i)))) {
        LOG_WARN("fail to push back expr", K(ret), K(i));
      }
    }
  }
  return ret;
}

void ObParquetDictFilterPushdown::reset()
{
  for (DictColumnsMap::iterator iter = dict_columns_.begin(); iter != dict_columns_.end(); ++iter) {
    if (OB_NOT_NULL(iter->second)) {
      if (OB_NOT_NULL(iter->second->dict_values_)) {
        common::StrDiscVec *str_disc_vec = iter->second->dict_values_;
        str_disc_vec->~StrDiscVec();
        iter->second->dict_values_ = nullptr;
      }
      iter->second->indices_.reset();
      iter->second->~ObParquetDictColumnData();
      iter->second = nullptr;
    }
  }
  dict_columns_.reuse();
  dict_encoding_check_cache_.reuse();
  column_need_decode_cache_.reuse();

  for (int64_t i = 0; i < valid_dict_codes_cache_.count(); ++i) {
    if (OB_NOT_NULL(valid_dict_codes_cache_.at(i).valid_dict_codes_)) {
      valid_dict_codes_cache_.at(i).valid_dict_codes_->~ObBitmap();
      valid_dict_codes_cache_.at(i).valid_dict_codes_ = nullptr;
    }
  }
  valid_dict_codes_cache_.reset();
  dict_filter_executors_.reset();
  processed_dict_op_filters_.reset();
  failed_dict_filters_.reset();
}

int ObParquetDictFilterPushdown::collect_dict_filter_executor(
    ObPushdownFilterExecutor *filter_executor)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_executor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(filter_executor));
  } else if (OB_FAIL(dict_filter_executors_.push_back(filter_executor))) {
    LOG_WARN("fail to push back dict filter executor", K(ret), KP(filter_executor));
  }
  return ret;
}

int ObParquetDictFilterPushdown::save_dict_column_data(int32_t col_idx,
                                                       int32_t *indices,
                                                       int64_t indices_count,
                                                       const void *dict_values,
                                                       int32_t dict_len,
                                                       parquet::Type::type parquet_type,
                                                       bool first_batch,
                                                       int64_t row_count,
                                                       int64_t max_batch_size,
                                                       bool has_null,
                                                       const int16_t *def_levels,
                                                       int16_t max_def_level,
                                                       const ObExpr *file_col_expr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(indices) || row_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(indices), K(row_count));
  } else {
    ObParquetDictColumnData *dict_data = nullptr;
    bool dict_already_saved = false;
    if (OB_FAIL(get_dict_data(col_idx, dict_data, dict_already_saved))) {
      LOG_WARN("fail to get dict data", K(ret), K(col_idx));
    } else if (dict_already_saved) {
      if (OB_ISNULL(dict_data)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dict_data is null but dict_already_saved is true", K(ret), K(col_idx));
      }
    } else {
      dict_data = OB_NEWx(ObParquetDictColumnData, &allocator_, allocator_);
      if (OB_ISNULL(dict_data)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for dict data", K(ret));
      } else if (dict_len <= 0 || OB_ISNULL(dict_values)) {
        // 特殊情况：字典为空或全为 null
        dict_data->dict_len_ = 0;
        dict_data->dict_values_ = nullptr;
        dict_data->parquet_type_ = parquet_type;
        dict_data->has_null_ = true;
        if (OB_FAIL(dict_data->indices_.prepare_allocate(max_batch_size))) {
          LOG_WARN("fail to prepare allocate indices array", K(ret), K(max_batch_size));
        }
      } else if (parquet_type == parquet::Type::BYTE_ARRAY) {
        const parquet::ByteArray *src_dict = static_cast<const parquet::ByteArray *>(dict_values);
        const bool is_large_text = ob_is_large_text(file_col_expr->datum_meta_.type_);

        sql::ObBitVector *nulls = nullptr;
        int32_t *lens = nullptr;
        char **ptrs = nullptr;
        if (OB_ISNULL(nulls = sql::to_bit_vector(
                          allocator_.alloc(sql::ObBitVector::memory_size(dict_len))))
            || OB_ISNULL(lens
                         = static_cast<int32_t *>(allocator_.alloc(sizeof(int32_t) * dict_len)))
            || OB_ISNULL(ptrs
                         = static_cast<char **>(allocator_.alloc(sizeof(char *) * dict_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for StrDiscVec arrays", K(ret), K(dict_len));
        } else {
          nulls->reset(dict_len);
          const int64_t max_length = file_col_expr->max_length_;
          const bool is_oracle_mode = lib::is_oracle_mode();
          const bool is_byte_length
              = is_oracle_byte_length(is_oracle_mode, file_col_expr->datum_meta_.length_semantics_);

          char *string_data = nullptr;
          ObDatum *lob_datums = nullptr;
          if (is_large_text) {
            if (OB_ISNULL(lob_datums = OB_NEW_ARRAY(ObDatum, &allocator_, dict_len))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory for lob datums", K(ret), K(dict_len));
            }
          } else {
            int64_t string_data_size = 0;
            for (int32_t i = 0; i < dict_len; ++i) {
              string_data_size += src_dict[i].len;
            }
            if (OB_ISNULL(string_data = static_cast<char *>(allocator_.alloc(string_data_size)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory for string data", K(ret), K(string_data_size));
            }
          }

          if (OB_SUCC(ret)) {
            char *data_ptr = string_data;
            for (int32_t i = 0; i < dict_len && OB_SUCC(ret); ++i) {
              if (OB_UNLIKELY(is_oracle_mode && 0 == src_dict[i].len)) {
                nulls->set(i);
                has_null = true;
              } else if (OB_UNLIKELY(src_dict[i].len > max_length
                                     && (is_byte_length
                                         || ObCharset::strlen_char(
                                                CS_TYPE_UTF8MB4_BIN,
                                                pointer_cast<const char *>(src_dict[i].ptr),
                                                src_dict[i].len)
                                                > max_length))) {
                ret = OB_ERR_DATA_TOO_LONG;
                LOG_WARN("data too long", K(max_length), K(src_dict[i].len), K(ret));
              } else if (OB_UNLIKELY(is_large_text)) {
                if (OB_FAIL(ObTextStringHelper::string_to_templob_result(
                        file_col_expr->datum_meta_.type_,
                        true, // has_lob_header
                        allocator_,
                        ObString(src_dict[i].len, pointer_cast<const char *>(src_dict[i].ptr)),
                        lob_datums[i]))) {
                  LOG_WARN("fail to convert string to templob", K(ret), K(i));
                } else {
                  ptrs[i] = const_cast<char *>(lob_datums[i].get_string().ptr());
                  lens[i] = lob_datums[i].get_string().length();
                }
              } else {
                MEMCPY(data_ptr, src_dict[i].ptr, src_dict[i].len);
                ptrs[i] = data_ptr;
                lens[i] = src_dict[i].len;
                data_ptr += src_dict[i].len;
              }
            }
          }

          if (OB_SUCC(ret)) {
            common::StrDiscVec *str_disc_vec
                = OB_NEWx(common::StrDiscVec, &allocator_, lens, ptrs, nulls);
            if (OB_ISNULL(str_disc_vec)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory for StrDiscVec", K(ret));
            } else {
              dict_data->dict_len_ = dict_len;
              dict_data->parquet_type_ = parquet_type;
              dict_data->dict_values_ = str_disc_vec;
              dict_data->has_null_ = has_null;
              if (OB_FAIL(dict_data->indices_.prepare_allocate(max_batch_size))) {
                LOG_WARN("fail to prepare allocate indices array", K(ret), K(max_batch_size));
              }
            }
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("only support BYTE_ARRAY type for now", K(ret), K(parquet_type));
      }
    }

    if (OB_SUCC(ret)) {
      if (first_batch) {
        dict_data->indices_count_ = 0;
      }

      const int64_t row_offset = dict_data->indices_count_;
      if (has_null) {
        int j = 0;
        for (int64_t i = 0; i < row_count && OB_SUCC(ret); ++i) {
          if (def_levels[i] < max_def_level) {
            dict_data->indices_[row_offset + i] = dict_len;
          } else {
            dict_data->indices_[row_offset + i] = indices[j++];
          }
        }
        CK(j == indices_count);
      } else {
        // 没有 NULL 的情况，直接 MEMCPY
        int32_t *dest_start = &dict_data->indices_[row_offset];
        MEMCPY(dest_start, indices, sizeof(int32_t) * row_count);
      }
      if (OB_SUCC(ret)) {
        dict_data->indices_count_ += row_count;
      }
    }

    if (OB_SUCC(ret) && !dict_already_saved) {
      if (OB_FAIL(dict_columns_.set_refactored(col_idx, dict_data))) {
        LOG_WARN("fail to save dict column data", K(ret), K(col_idx));
      } else {
        LOG_DEBUG("save dict column data",
                  K(col_idx),
                  K(dict_data->dict_len_),
                  K(row_count),
                  K(has_null));
      }
    }
  }
  return ret;
}

int ObParquetDictFilterPushdown::init_row_group_dict_encoding_check(
    std::shared_ptr<parquet::RowGroupReader> rg_reader,
    const common::ObArrayWrap<int> &column_indexs,
    const common::ObIArray<uint64_t> &eager_columns)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(rg_reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    // 对每个eager列检查是否所有page都是字典编码，并缓存结果
    for (int64_t i = 0; OB_SUCC(ret) && i < eager_columns.count(); ++i) {
      int32_t col_idx = eager_columns.at(i);
      bool all_pages_dict_encoded = false;
      if (OB_FAIL(check_all_pages_dict_encoded(col_idx,
                                               rg_reader,
                                               column_indexs,
                                               all_pages_dict_encoded))) {
        LOG_WARN("fail to check all pages dict encoded", K(ret), K(col_idx));
      } else if (OB_FAIL(dict_encoding_check_cache_.set_refactored(col_idx,
                                                                   all_pages_dict_encoded,
                                                                   1 /*overwrite*/))) {
        LOG_WARN("fail to cache dict encoding check result", K(ret), K(col_idx));
      } else {
        LOG_DEBUG("cached dict encoding check result", K(col_idx), K(all_pages_dict_encoded));
      }
    }
  }

  return ret;
}

// Reference from StarRocks:
// be/src/formats/parquet/scalar_column_reader.cpp RawColumnReader::column_all_pages_dict_encoded()
//
// The Parquet spec allows for column chunks to have mixed encodings
// where some data pages are dictionary-encoded and others are plain
// encoded. For example, a Parquet file writer might start writing
// a column chunk as dictionary encoded, but it will switch to plain
// encoding if the dictionary grows too large.
//
// In order for dictionary filters to skip the entire row group,
// the conjuncts must be evaluated on column chunks that are entirely
// encoded with the dictionary encoding. There are two checks
// available to verify this:
// 1. The encoding_stats field on the column chunk metadata provides
//    information about the number of data pages written in each
//    format. This allows for a specific check of whether all the
//    data pages are dictionary encoded.
// 2. The encodings field on the column chunk metadata lists the
//    encodings used. If this list contains the dictionary encoding
//    and does not include unexpected encodings (i.e. encodings not
//    associated with definition/repetition levels), then it is entirely
//    dictionary encoded.
int ObParquetDictFilterPushdown::check_all_pages_dict_encoded(
    int32_t col_idx,
    std::shared_ptr<parquet::RowGroupReader> rg_reader,
    const common::ObArrayWrap<int> &column_indexs,
    bool &is_all_pages_dict_encoded)
{
  int ret = OB_SUCCESS;
  is_all_pages_dict_encoded = false;
  if (col_idx < 0 || col_idx >= column_indexs.count() || OB_ISNULL(rg_reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(col_idx), K(column_indexs.count()));
  } else {
    arrow::Status status = arrow::Status::OK();
    BEGIN_CATCH_EXCEPTIONS
    std::unique_ptr<parquet::ColumnChunkMetaData> metadata
        = rg_reader->metadata()->ColumnChunk(column_indexs.at(col_idx));

    // Check if there is a dictionary page
    if (!metadata->has_dictionary_page()) {
      is_all_pages_dict_encoded = false;
    } else {
      // Condition #1 above
      const std::vector<parquet::PageEncodingStats> &encoding_stats = metadata->encoding_stats();
      if (!encoding_stats.empty()) {
        // 检查所有Data Page是否都是字典编码
        is_all_pages_dict_encoded = true;
        for (const parquet::PageEncodingStats &enc_stat : encoding_stats) {
          if (enc_stat.page_type == parquet::PageType::DATA_PAGE
              || enc_stat.page_type == parquet::PageType::DATA_PAGE_V2) {
            if (enc_stat.encoding != parquet::Encoding::PLAIN_DICTIONARY
                && enc_stat.encoding != parquet::Encoding::RLE_DICTIONARY) {
              if (enc_stat.count > 0) {
                is_all_pages_dict_encoded = false; // 有非字典编码的Data Page
                break;
              }
            }
          }
        }
      } else {
        // Condition #2 above
        const std::vector<parquet::Encoding::type> &encodings = metadata->encodings();
        bool has_dict_encoding = false;
        bool has_nondict_encoding = false;
        for (const parquet::Encoding::type &encoding : encodings) {
          if (encoding == parquet::Encoding::PLAIN_DICTIONARY
              || encoding == parquet::Encoding::RLE_DICTIONARY) {
            has_dict_encoding = true;
          }

          // RLE and BIT_PACKED are used for repetition/definition levels
          if (encoding != parquet::Encoding::PLAIN_DICTIONARY
              && encoding != parquet::Encoding::RLE_DICTIONARY && encoding != parquet::Encoding::RLE
              && encoding != parquet::Encoding::BIT_PACKED) {
            has_nondict_encoding = true;
            break;
          }
        }

        // Not entirely dictionary encoded if:
        // 1. No dictionary encoding listed
        // OR
        // 2. Some non-dictionary encoding is listed
        is_all_pages_dict_encoded = has_dict_encoding && !has_nondict_encoding;
      }
    }

    END_CATCH_EXCEPTIONS
  }
  return ret;
}

int ObParquetDictFilterPushdown::is_column_all_pages_dict_encoded(
    int32_t col_idx,
    bool &is_all_pages_dict_encoded) const
{
  int ret = OB_SUCCESS;
  is_all_pages_dict_encoded = false;
  if (OB_FAIL(dict_encoding_check_cache_.get_refactored(col_idx, is_all_pages_dict_encoded))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_all_pages_dict_encoded = false;
    } else {
      LOG_WARN("fail to get dict encoding check result", K(ret), K(col_idx));
    }
  }
  return ret;
}

bool ObParquetDictFilterPushdown::is_operator_supported_for_dict_filter(
    ObPushdownFilterExecutor *filter_executor)
{
  bool result = false;
  if (OB_ISNULL(filter_executor)) {
    result = false;
  } else if (filter_executor->is_filter_black_node()) {
    // 黑盒filter：目前支持所有（通过表达式求值）
    result = true;
  } else if (filter_executor->is_filter_white_node()) {
    ObWhiteFilterExecutor *white_filter = static_cast<ObWhiteFilterExecutor *>(filter_executor);
    ObWhiteFilterOperatorType op_type = white_filter->get_op_type();

    // 白盒filter：支持特定的操作符
    switch (op_type) {
      case WHITE_OP_NU:   // IS NULL
      case WHITE_OP_NN:   // IS NOT NULL
      case WHITE_OP_EQ:   // =
      case WHITE_OP_GT:   // >
      case WHITE_OP_GE:   // >=
      case WHITE_OP_LT:   // <
      case WHITE_OP_LE:   // <=
      case WHITE_OP_NE:   // !=
      case WHITE_OP_IN:   // IN
      case WHITE_OP_BT: { // BETWEEN
        result = true;
        break;
      }
      default:
        result = false;
    }
  }

  return result;
}

int ObParquetDictFilterPushdown::get_dict_data(int32_t col_idx,
                                               ObParquetDictColumnData *&dict_data,
                                               bool &has_dict_data)
{
  int ret = OB_SUCCESS;
  has_dict_data = false;
  dict_data = nullptr;
  if (OB_FAIL(dict_columns_.get_refactored(col_idx, dict_data))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get refactored", K(ret), K(col_idx));
    }
  } else if (OB_NOT_NULL(dict_data)) {
    has_dict_data = true;
  }
  return ret;
}

bool ObParquetDictFilterPushdown::is_filter_processed_by_dict(
    const ObPushdownFilterExecutor *filter_executor)
{
  bool result = false;
  if (OB_ISNULL(filter_executor)) {
    result = false;
  } else {
    for (int64_t i = 0; i < dict_filter_executors_.count(); ++i) {
      if (dict_filter_executors_.at(i) == filter_executor) {
        result = true;
        break;
      }
    }
  }
  return result;
}

int ObParquetDictFilterPushdown::apply_single_column_dict_filters(
    ObPushdownFilterExecutor *curr_filter,
    ObPushdownFilterExecutor *parent_filter,
    ObEvalCtx &eval_ctx,
    int64_t row_count,
    const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids,
    const common::ObArrayWrap<int> &column_indexs,
    bool &applied_dict_filter)
{
  int ret = OB_SUCCESS;
  applied_dict_filter = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!has_dict_columns() || OB_ISNULL(curr_filter)) {
    LOG_DEBUG("no dict columns or no filter, skip dict optimization");
  } else {
    common::ObBitmap *result = nullptr;
    // 递归遍历filter tree，对可以字典优化的单列filter应用优化
    if (curr_filter->is_filter_node() && is_filter_processed_by_dict(curr_filter)) {
      const common::ObIArray<uint64_t> *col_ids = nullptr;

      if (curr_filter->is_filter_black_node()) {
        ObBlackFilterExecutor *black_filter = static_cast<ObBlackFilterExecutor *>(curr_filter);
        col_ids = &black_filter->get_col_ids();
      } else if (curr_filter->is_filter_white_node()) {
        ObWhiteFilterExecutor *white_filter = static_cast<ObWhiteFilterExecutor *>(curr_filter);
        col_ids = &white_filter->get_col_ids();
      }

      if (OB_ISNULL(col_ids) || col_ids->count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter has multiple columns", K(ret), K(col_ids->count()));
      } else {
        uint64_t ob_col_id = col_ids->at(0);
        int32_t file_col_idx = -1;
        ObParquetDictColumnData *dict_data = nullptr;
        bool has_dict_data = false;
        // 从 OB 列 ID 映射到 file_column_exprs_ 索引
        for (int64_t i = 0; i < mapping_column_ids.count(); ++i) {
          if (mapping_column_ids.at(i).first == ob_col_id) {
            file_col_idx = i;
            break;
          }
        }

        if (file_col_idx < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("file_col_idx and ob_col_id not one to one mapping", K(ret), K(ob_col_id));
        } else if (OB_FAIL(get_dict_data(file_col_idx, dict_data, has_dict_data))) {
          LOG_WARN("fail to get dict data", K(ret), K(file_col_idx));
        } else if (!has_dict_data) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dict data not found", K(ret), K(file_col_idx));
        } else {
          if (OB_FAIL(curr_filter->init_bitmap(row_count, result))) {
            LOG_WARN("fail to init filter result bitmap", K(ret));
          } else if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("filter result bitmap is null", K(ret));
          } else if (nullptr != parent_filter
                     && OB_FAIL(parent_filter->prepare_skip_filter(false))) {
            LOG_WARN("Failed to check parent skip filter", K(ret));
          } else {
            if (curr_filter->is_filter_black_node()) {
              if (OB_FAIL(apply_dict_filter_on_black_filter(
                      static_cast<ObBlackFilterExecutor *>(curr_filter),
                      eval_ctx,
                      dict_data,
                      *result))) {
                LOG_WARN("fail to apply black dict filter", K(ret), K(ob_col_id), K(file_col_idx));
              }
            } else if (curr_filter->is_filter_white_node()) {
              if (OB_FAIL(apply_dict_filter_on_white_filter(
                      static_cast<ObWhiteFilterExecutor *>(curr_filter),
                      eval_ctx,
                      dict_data,
                      *result))) {
                LOG_WARN("fail to apply white dict filter", K(ret), K(ob_col_id), K(file_col_idx));
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          applied_dict_filter = true;
        } else {
          // 字典过滤应用失败不让整个流程失败，继续使用常规过滤，以保证跟原行为一致
          ret = OB_SUCCESS;
          applied_dict_filter = false;
          if (OB_FAIL(failed_dict_filters_.push_back(curr_filter))) {
            LOG_WARN("fail to add filter to failed list", K(ret));
          }
        }
      }
    } else if (curr_filter->is_logic_op_node()) {
      // 处理逻辑节点（AND/OR）
      // 逻辑节点需要初始化bitmap来合并子filter的结果
      if (OB_FAIL(curr_filter->init_bitmap(row_count, result))) {
        LOG_WARN("Failed to init filter bitmap", K(ret));
      } else {
        sql::ObPushdownFilterExecutor **children = curr_filter->get_childs();

        // AND节点优化：继承父节点的result作为初始值
        if (OB_NOT_NULL(parent_filter) && parent_filter->is_logic_and_node()
            && curr_filter->is_logic_and_node()) {
          MEMCPY(result->get_data(), parent_filter->get_result()->get_data(), row_count);
        }

        // 递归处理所有子filter并合并结果
        bool has_any_child_applied = false;
        bool has_any_child_not_applied = false;

        for (uint32_t i = 0; OB_SUCC(ret) && i < curr_filter->get_child_count(); i++) {
          const common::ObBitmap *child_result = nullptr;
          bool child_applied = false;

          if (OB_FAIL(apply_single_column_dict_filters(children[i],
                                                       curr_filter,
                                                       eval_ctx,
                                                       row_count,
                                                       mapping_column_ids,
                                                       column_indexs,
                                                       child_applied))) {
            LOG_WARN("Failed to filter micro block", K(ret), K(i), KP(children[i]));
          } else if (child_applied) {
            has_any_child_applied = true;
            if (OB_ISNULL(child_result = children[i]->get_result())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unexpected get null filter bitmap", K(ret));
            } else {
              if (curr_filter->is_logic_and_node()) {
                if (OB_FAIL(result->bit_and(*child_result))) {
                  LOG_WARN("Failed to merge result bitmap", K(ret), KP(child_result));
                } else if (result->is_all_false()) {
                  if (OB_FAIL(processed_dict_op_filters_.push_back(curr_filter))) {
                    LOG_WARN("fail to add filter to processed list", K(ret));
                  } else {
                    break;
                  }
                }
              } else {
                if (OB_FAIL(result->bit_or(*child_result))) {
                  LOG_WARN("Failed to merge result bitmap", K(ret), KP(child_result));
                } else if (result->is_all_true()) {
                  if (OB_FAIL(processed_dict_op_filters_.push_back(curr_filter))) {
                    LOG_WARN("fail to add filter to processed list", K(ret));
                  } else {
                    break;
                  }
                }
              }
            }
          } else {
            // 子节点没有应用字典过滤
            has_any_child_not_applied = true;
          }
        }

        if (OB_SUCC(ret)) {
          if (curr_filter->is_logic_and_node() && result->is_all_false()) {
            applied_dict_filter = true;
          } else if (curr_filter->is_logic_or_node() && result->is_all_true()) {
            applied_dict_filter = true;
          } else {
            // 一般情况：只有所有子节点都应用了字典过滤，才能使用结果
            applied_dict_filter = has_any_child_applied && !has_any_child_not_applied;
          }
        }
      }
    }
  }

  return ret;
}

int ObParquetDictFilterPushdown::check_in_non_dict_filter(
    int32_t col_idx,
    ObPushdownFilterExecutor *filter_executor,
    const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids,
    bool &need_decode)
{
  int ret = OB_SUCCESS;

  need_decode = false;

  if (OB_FAIL(column_need_decode_cache_.get_refactored(col_idx, need_decode))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      bool has_non_dict_filter = false;
      if (OB_FAIL(has_non_dict_filter_for_column(filter_executor,
                                                 col_idx,
                                                 mapping_column_ids,
                                                 has_non_dict_filter))) {
        LOG_WARN("fail to check has non dict filter for column", K(ret), K(col_idx));
      } else if (has_non_dict_filter) {
        need_decode = true;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(
                column_need_decode_cache_.set_refactored(col_idx, need_decode, 1 /*overwrite*/))) {
          LOG_WARN("fail to cache need decode result", K(ret), K(col_idx), K(need_decode));
        }
      }
    } else {
      LOG_WARN("fail to get need decode cache result", K(ret), K(col_idx));
    }
  }
  return ret;
}

int ObParquetDictFilterPushdown::need_decode_dict_column(
    int32_t col_idx,
    ObPushdownFilterExecutor *filter_executor,
    const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids,
    const common::ObIArray<bool> &is_dup_project,
    bool is_eager_calc,
    const common::ObIArray<bool> &column_need_conv,
    bool &need_decode)
{
  int ret = OB_SUCCESS;
  need_decode = false;

  if (OB_ISNULL(filter_executor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter executor is null", K(ret));
  }

  // 条件1：列在输出中，且不是eager模式
  if (OB_FAIL(ret)) {
  } else if (is_dup_project.at(col_idx) && !is_eager_calc) {
    need_decode = true;
  }

  // 条件2：列需要类型转换时，必须 decode 以便后续做 conv
  if (OB_FAIL(ret) || need_decode) {
  } else {
    uint64_t column_expr_index = mapping_column_ids.at(col_idx).second;
    if (column_expr_index != OB_INVALID_ID && column_expr_index < column_need_conv.count()
        && !column_need_conv.at(column_expr_index)) {
      need_decode = false;
    } else {
      need_decode = true;
    }
  }

  // 条件3：该列有非字典优化的filter
  if (OB_FAIL(ret) || need_decode) {
  } else if (OB_FAIL(check_in_non_dict_filter(col_idx,
                                              filter_executor,
                                              mapping_column_ids,
                                              need_decode))) {
    LOG_WARN("fail to check need decode dict for column", K(ret), K(col_idx));
  }

  // 条件4：该列有失败的字典优化filter
  if (OB_FAIL(ret) || need_decode) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < failed_dict_filters_.count(); ++i) {
      // 检查这个失败的字典优化filter是否涉及target_col_idx
      ObPushdownFilterExecutor *failed_filter = failed_dict_filters_.at(i);
      const common::ObIArray<uint64_t> *col_ids = nullptr;
      if (failed_filter->is_filter_black_node()) {
        ObBlackFilterExecutor *black_filter = static_cast<ObBlackFilterExecutor *>(failed_filter);
        col_ids = &black_filter->get_col_ids();
      } else if (failed_filter->is_filter_white_node()) {
        ObWhiteFilterExecutor *white_filter = static_cast<ObWhiteFilterExecutor *>(failed_filter);
        col_ids = &white_filter->get_col_ids();
      }

      std::pair<uint64_t, uint64_t> mapping_column_id = mapping_column_ids.at(col_idx);
      uint64_t mapped_col_id = mapping_column_id.first;
      if (OB_ISNULL(col_ids) || col_ids->count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col ids is null or count is not 1", K(ret), K(col_idx));
      } else if (OB_UNLIKELY(mapped_col_id == OB_INVALID_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mapped col id is invalid", K(ret), K(col_idx));
      } else if (col_ids->at(0) == mapped_col_id) {
        need_decode = true;
        break;
      }
    }
  }
  return ret;
}

bool ObParquetDictFilterPushdown::is_failed_dict_filter(ObPushdownFilterExecutor *filter_executor)
{
  bool is_failed = false;
  for (int64_t i = 0; i < failed_dict_filters_.count(); ++i) {
    if (failed_dict_filters_.at(i) == filter_executor) {
      is_failed = true;
      break;
    }
  }
  return is_failed;
}

bool ObParquetDictFilterPushdown::is_processed_dict_op_filter(
    ObPushdownFilterExecutor *filter_executor)
{
  bool is_processed = false;
  for (int64_t i = 0; i < processed_dict_op_filters_.count(); ++i) {
    if (processed_dict_op_filters_.at(i) == filter_executor) {
      is_processed = true;
      break;
    }
  }
  return is_processed;
}

int ObParquetDictFilterPushdown::has_non_dict_filter_for_column(
    ObPushdownFilterExecutor *filter_executor,
    int32_t target_col_idx,
    const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids,
    bool &has_non_dict_filter)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(filter_executor)) {
    has_non_dict_filter = false;
  } else if (filter_executor->is_filter_node()) {
    if (is_filter_processed_by_dict(filter_executor)) {
      has_non_dict_filter = false; // 字典优化的filter不需要解码
    } else {
      // 检查这个非字典优化的filter是否涉及target_col_idx
      const common::ObIArray<uint64_t> *col_ids = nullptr;
      if (filter_executor->is_filter_black_node()) {
        ObBlackFilterExecutor *black_filter = static_cast<ObBlackFilterExecutor *>(filter_executor);
        col_ids = &black_filter->get_col_ids();
      } else if (filter_executor->is_filter_white_node()) {
        ObWhiteFilterExecutor *white_filter = static_cast<ObWhiteFilterExecutor *>(filter_executor);
        col_ids = &white_filter->get_col_ids();
      }

      std::pair<uint64_t, uint64_t> mapping_column_id = mapping_column_ids.at(target_col_idx);
      uint64_t mapped_col_id = mapping_column_id.first;
      if (OB_NOT_NULL(col_ids)) {
        if (mapped_col_id == OB_INVALID_ID) {
          // 不是一一映射，需要解码
          has_non_dict_filter = true;
        } else {
          for (int64_t i = 0; i < col_ids->count(); ++i) {
            if (col_ids->at(i) == mapped_col_id) {
              has_non_dict_filter = true;
              break;
            }
          }
        }
      }
    }
  } else if (filter_executor->is_logic_op_node()) {
    // 递归检查子节点
    sql::ObPushdownFilterExecutor **children = filter_executor->get_childs();
    for (uint32_t i = 0; OB_SUCC(ret) && i < filter_executor->get_child_count(); i++) {
      bool child_has_non_dict_filter = false;
      if (OB_FAIL(has_non_dict_filter_for_column(children[i],
                                                 target_col_idx,
                                                 mapping_column_ids,
                                                 child_has_non_dict_filter))) {
        LOG_WARN("fail to check has non dict filter for column", K(ret), K(i));
      } else if (child_has_non_dict_filter) {
        has_non_dict_filter = true;
        break;
      }
    }
  }

  return ret;
}

int ObParquetDictFilterPushdown::apply_dict_filter_on_black_filter(
    ObBlackFilterExecutor *black_filter,
    ObEvalCtx &eval_ctx,
    const ObParquetDictColumnData *dict_data,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(black_filter) || OB_ISNULL(dict_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(black_filter));
  } else {
    // 阶段1：在字典值上评估filter，得到valid_dict_codes
    const int32_t distinct_ref_cnt = dict_data->get_distinct_ref_cnt();
    ObBitmap *valid_dict_codes = nullptr;

    for (int64_t i = 0; i < valid_dict_codes_cache_.count(); ++i) {
      if (valid_dict_codes_cache_.at(i).filter_executor_ == black_filter) {
        valid_dict_codes = valid_dict_codes_cache_.at(i).valid_dict_codes_;
        LOG_DEBUG("valid dict codes cache hit", KP(black_filter));
        break;
      }
    }

    if (OB_ISNULL(valid_dict_codes)) {
      // 缓存未命中，需要计算并保存到缓存
      if (OB_ISNULL(valid_dict_codes = OB_NEWx(ObBitmap, (&allocator_), allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for bitmap", K(ret));
      } else if (OB_FAIL(valid_dict_codes->init(distinct_ref_cnt))) {
        LOG_WARN("fail to init valid dict codes bitmap",
                 K(ret),
                 K(distinct_ref_cnt),
                 K(dict_data->dict_len_),
                 K(dict_data->has_null_));
      } else if (OB_FAIL(eval_black_filter_on_dict_values(black_filter,
                                                          eval_ctx,
                                                          dict_data,
                                                          *valid_dict_codes))) {
        LOG_WARN("fail to eval filter on dict values", K(ret));
      } else {
        DictFilterCacheEntry entry(black_filter, valid_dict_codes);
        if (OB_FAIL(valid_dict_codes_cache_.push_back(entry))) {
          LOG_WARN("fail to push back to cache array", K(ret));
        } else {
          LOG_DEBUG("valid dict codes cached", KP(black_filter), K(valid_dict_codes->popcnt()));
        }
      }
    }

    // 阶段2：根据valid_dict_codes过滤indices
    if (OB_SUCC(ret)) {
      if (OB_FAIL(filter_by_dict_codes(*valid_dict_codes,
                                       dict_data->indices_,
                                       dict_data->indices_count_,
                                       result_bitmap))) {
        LOG_WARN("fail to filter by dict codes", K(ret));
      } else {
        LOG_DEBUG("black dict filter completed",
                  K(valid_dict_codes->popcnt()),
                  K(result_bitmap.popcnt()),
                  K(dict_data->has_null_));
      }
    }
  }

  return ret;
}

int ObParquetDictFilterPushdown::eval_black_filter_on_dict_values(
    ObBlackFilterExecutor *black_filter,
    ObEvalCtx &eval_ctx,
    const ObParquetDictColumnData *dict_data,
    common::ObBitmap &valid_dict_codes)
{
  int ret = OB_SUCCESS;

  // 获取filter中列表达式的datum buffer指针（数据注入机制）
  const ObPushdownBlackFilterNode &filter_node
      = static_cast<const ObPushdownBlackFilterNode &>(black_filter->get_filter_node());
  const ExprFixedArray &column_exprs = filter_node.column_exprs_;

  if (OB_UNLIKELY(column_exprs.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect single column filter", K(ret), K(column_exprs.count()));
  } else {
    ObExpr *col_expr = column_exprs.at(0);

    const int32_t distinct_ref_cnt = dict_data->get_distinct_ref_cnt();
    const int64_t batch_size = eval_ctx.max_batch_size_;

    if (dict_data->parquet_type_ == parquet::Type::BYTE_ARRAY) {
      ObArenaAllocator temp_allocator;
      common::ObBitmap batch_result(temp_allocator);
      OZ(batch_result.init(batch_size));

      // 批量处理：每次处理batch_size个字典值
      for (int64_t batch_start = 0; OB_SUCC(ret) && batch_start < distinct_ref_cnt;
           batch_start += batch_size) {

        int64_t batch_end = MIN(batch_start + batch_size, static_cast<int64_t>(distinct_ref_cnt));
        int64_t batch_count = batch_end - batch_start;

        OZ(col_expr->init_vector_for_write(eval_ctx, VEC_DISCRETE, batch_size));
        common::StrDiscVec *discrete_vec
            = static_cast<common::StrDiscVec *>(col_expr->get_vector(eval_ctx));
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(discrete_vec)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("buffer is null", K(ret), KP(discrete_vec));
          // 特殊情况：字典为空（全为 null）
        } else if (OB_ISNULL(dict_data->dict_values_) || dict_data->dict_len_ <= 0) {
          discrete_vec->set_null(0);
        } else {
          common::StrDiscVec *dict_values = dict_data->dict_values_;
          char **dict_ptrs = dict_values->get_ptrs();
          int32_t *dict_lens = dict_values->get_lens();

          for (int64_t i = 0; i < batch_count && OB_SUCC(ret); ++i) {
            int32_t dict_idx = static_cast<int32_t>(batch_start + i);

            if (dict_idx < dict_data->dict_len_) {
              if (OB_UNLIKELY(dict_values->is_null(dict_idx))) {
                discrete_vec->set_null(i);
              } else {
                discrete_vec->set_string(i, dict_ptrs[dict_idx], dict_lens[dict_idx]);
              }
            } else {
              discrete_vec->set_null(i);
            }
          }
        }

        if (OB_SUCC(ret)) {
          col_expr->set_evaluated_projected(eval_ctx);
        }

        // 每次迭代重置bitmap
        batch_result.reuse(false);

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(black_filter->filter_batch(nullptr, 0, batch_count, batch_result))) {
          LOG_WARN("fail to filter batch", K(ret), K(batch_start), K(batch_count));
        } else {
          // 清空filter表达式的评估标志
          for (int64_t i = 0; i < black_filter->get_filter_node().filter_exprs_.count(); ++i) {
            ObExpr *filter_expr = black_filter->get_filter_node().filter_exprs_.at(i);
            filter_expr->get_evaluated_flags(eval_ctx).reset(batch_count);
          }

          for (int64_t i = 0; i < batch_count && OB_SUCC(ret); ++i) {
            if (batch_result.test(i)) {
              int32_t dict_idx = static_cast<int32_t>(batch_start + i);
              if (OB_FAIL(valid_dict_codes.set(dict_idx))) {
                LOG_WARN("fail to set valid dict code", K(ret), K(dict_idx));
              }
            }
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("dict opt only supports string type now", K(ret), K(dict_data->parquet_type_));
    }
  }

  return ret;
}

int ObParquetDictFilterPushdown::filter_by_dict_codes(const common::ObBitmap &valid_dict_codes,
                                                      const ObIArray<int32_t> &indices,
                                                      int64_t indices_count,
                                                      common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;

  if (indices_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(indices_count));
  } else {
    if (valid_dict_codes.is_all_true()) {
      if (OB_FAIL(result_bitmap.set_bitmap_batch(0, indices_count, true))) {
        LOG_WARN("fail to set bitmap batch", K(ret), K(indices_count));
      }
    } else if (valid_dict_codes.is_all_false()) {
      // do nothing
    } else if (valid_dict_codes.popcnt() == 1) {
      int64_t offset = 0;
      if (OB_FAIL(valid_dict_codes.next_valid_idx(0, valid_dict_codes.size(), false, offset))) {
        LOG_WARN("fail to get next valid idx", K(ret));
      } else if (OB_UNLIKELY(offset == -1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("next valid idx is -1", K(ret), K(offset));
      } else {
        uint8_t *result_data = result_bitmap.get_data();
        int32_t *indices_data = indices.get_data();
        for (int64_t row = 0; row < indices_count; ++row) {
          result_data[row] = indices_data[row] == offset;
        }
      }
    } else {
      uint8_t *result_data = result_bitmap.get_data();
      uint8_t *valid_dict_codes_data = valid_dict_codes.get_data();
      int32_t *indices_data = indices.get_data();
      for (int64_t row = 0; row < indices_count; ++row) {
        result_data[row] = valid_dict_codes_data[indices_data[row]];
      }
    }
  }

  return ret;
}

int ObParquetDictFilterPushdown::decode_filtered_rows_to_exprs(
    ObPushdownFilterExecutor *filter_executor,
    ObEvalCtx &eval_ctx,
    const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids,
    const common::ObIArray<bool> &is_dup_project,
    bool is_eager_calc,
    const common::ObIArray<bool> &column_need_conv)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // 遍历所有字典列，只解码需要解码的列
    for (DictColumnsMap::iterator iter = dict_columns_.begin();
         OB_SUCC(ret) && iter != dict_columns_.end();
         ++iter) {
      int32_t col_idx = iter->first;

      bool need_decode = false;
      if (OB_FAIL(need_decode_dict_column(col_idx,
                                          filter_executor,
                                          mapping_column_ids,
                                          is_dup_project,
                                          is_eager_calc,
                                          column_need_conv,
                                          need_decode))) {
        LOG_WARN("fail to check need decode dict column", K(ret), K(col_idx));
      } else if (need_decode) {
        if (col_idx < 0 || col_idx >= decode_exprs_.count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(col_idx));
        } else if (OB_FAIL(decode_dict_column_to_expr(iter->second,
                                                      filter_executor,
                                                      decode_exprs_.at(col_idx),
                                                      eval_ctx))) {
          LOG_WARN("fail to decode dict column to expr", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObParquetDictFilterPushdown::decode_dict_column_to_expr(
    const ObParquetDictColumnData *dict_data,
    const ObPushdownFilterExecutor *filter_executor,
    ObExpr *file_col_expr,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dict_data) || OB_ISNULL(file_col_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(file_col_expr));
  } else {
    if (OB_FAIL(file_col_expr->init_vector_for_write(eval_ctx,
                                                     file_col_expr->get_default_res_format(),
                                                     dict_data->indices_count_))) {
      LOG_WARN("fail to init vector for write", K(ret), K(dict_data->indices_count_));
    } else {
      common::StrDiscVec *text_vec
          = static_cast<common::StrDiscVec *>(file_col_expr->get_vector(eval_ctx));

      if (OB_ISNULL(text_vec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("text vector is null", K(ret));
      } else if (VEC_DISCRETE != text_vec->get_format()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", K(ret), "format", text_vec->get_format());
      } else {
        if (dict_data->parquet_type_ == parquet::Type::BYTE_ARRAY) {
          common::StrDiscVec *dict_values = dict_data->dict_values_;

          // 特殊情况：字典为空，所有值都是 null
          if (OB_ISNULL(dict_values) || dict_data->dict_len_ <= 0) {
            for (int64_t row = 0; row < dict_data->indices_count_; ++row) {
              text_vec->set_null(row);
            }
          } else {
            char **ptrs = dict_values->get_ptrs();
            int32_t *lens = dict_values->get_lens();

            const bool is_oracle_mode = lib::is_oracle_mode();
            const bool is_fast_path = !is_oracle_mode && !dict_data->has_null_;

            if (OB_LIKELY(is_fast_path)) {
              for (int64_t row = 0; OB_SUCC(ret) && row < dict_data->indices_count_; ++row) {
                int32_t dict_idx = dict_data->indices_.at(row);
                text_vec->set_string(row, ptrs[dict_idx], lens[dict_idx]);
              }
            } else {
              for (int64_t row = 0; OB_SUCC(ret) && row < dict_data->indices_count_; ++row) {
                int32_t dict_idx = dict_data->indices_.at(row);
                if (dict_idx == dict_data->get_null_ref() || dict_values->is_null(dict_idx)) {
                  text_vec->set_null(row);
                } else if (dict_idx >= 0 && dict_idx < dict_data->dict_len_) {
                  text_vec->set_string(row, ptrs[dict_idx], lens[dict_idx]);
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected dict index", K(ret), K(dict_idx));
                }
              }
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only support string type for now", K(ret), K(dict_data->parquet_type_));
        }
      }
    }

    if (OB_SUCC(ret)) {
      file_col_expr->set_evaluated_projected(eval_ctx);
    }
  }

  return ret;
}

int ObParquetDictFilterPushdown::apply_dict_filter_on_white_filter(
    ObWhiteFilterExecutor *white_filter,
    ObEvalCtx &eval_ctx,
    const ObParquetDictColumnData *dict_data,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(white_filter) || OB_ISNULL(dict_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(white_filter));
  } else {
    ObWhiteFilterOperatorType op_type = white_filter->get_op_type();

    bool is_valid = false;
    if (op_type != WHITE_OP_NU && op_type != WHITE_OP_NN
        && white_filter->get_datums().count() == 0) {
      if (WHITE_OP_IN == op_type) {
        if (OB_FAIL(white_filter->init_in_eval_datums(is_valid))) {
          LOG_WARN("failed to init eval datum", K(ret));
        }
      } else if (OB_FAIL(white_filter->init_compare_eval_datums(is_valid))) {
        LOG_WARN("failed to init eval datum", K(ret));
      }

      if (OB_SUCC(ret) && !is_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter is invalid", K(ret), K(op_type));
      }
    }

    if (OB_SUCC(ret)) {
      // 阶段1：在字典值上评估filter，得到valid_dict_codes
      const int32_t distinct_ref_cnt = dict_data->get_distinct_ref_cnt();
      ObBitmap *valid_dict_codes = nullptr;

      // 在缓存数组中查找
      for (int64_t i = 0; i < valid_dict_codes_cache_.count(); ++i) {
        if (valid_dict_codes_cache_.at(i).filter_executor_ == white_filter) {
          valid_dict_codes = valid_dict_codes_cache_.at(i).valid_dict_codes_;
          LOG_DEBUG("valid dict codes cache hit", KP(white_filter));
          break;
        }
      }

      if (OB_ISNULL(valid_dict_codes)) {
        if (OB_ISNULL(valid_dict_codes = OB_NEWx(ObBitmap, (&allocator_), allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for bitmap", K(ret));
        } else if (OB_FAIL(valid_dict_codes->init(distinct_ref_cnt))) {
          LOG_WARN("fail to init valid dict codes bitmap", K(ret), K(distinct_ref_cnt));
        } else if (OB_FAIL(eval_white_filter_on_dict_values(white_filter,
                                                            dict_data,
                                                            *valid_dict_codes))) {
          LOG_WARN("fail to eval white filter on dict values", K(ret));
        } else {
          DictFilterCacheEntry entry(white_filter, valid_dict_codes);
          if (OB_FAIL(valid_dict_codes_cache_.push_back(entry))) {
            LOG_WARN("fail to push back to cache array", K(ret));
          } else {
            LOG_DEBUG("valid dict codes cached", K(valid_dict_codes->popcnt()));
          }
        }
      }

      // 阶段2：根据valid_dict_codes过滤indices
      if (OB_SUCC(ret)) {
        if (OB_FAIL(filter_by_dict_codes(*valid_dict_codes,
                                         dict_data->indices_,
                                         dict_data->indices_count_,
                                         result_bitmap))) {
          LOG_WARN("fail to filter by dict codes", K(ret));
        } else {
          LOG_DEBUG("white dict filter completed",
                    K(op_type),
                    K(result_bitmap.popcnt()),
                    K(dict_data->has_null_));
        }
      }
    }
  }

  return ret;
}

int ObParquetDictFilterPushdown::eval_white_filter_on_dict_values(
    ObWhiteFilterExecutor *white_filter,
    const ObParquetDictColumnData *dict_data,
    common::ObBitmap &valid_dict_codes)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(white_filter) || OB_ISNULL(dict_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(white_filter));
  } else {
    ObWhiteFilterOperatorType op_type = white_filter->get_op_type();

    if (dict_data->parquet_type_ == parquet::Type::BYTE_ARRAY) {
      if (op_type == WHITE_OP_NU) {
        // IS NULL：只有 null_ref 位置满足
        if (dict_data->has_null_) {
          const int32_t null_ref = dict_data->get_null_ref();
          if (OB_FAIL(valid_dict_codes.set(null_ref, true))) {
            LOG_WARN("fail to set null_ref in valid_dict_codes", K(ret), K(null_ref));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (dict_data->has_null_ && lib::is_oracle_mode()) {
          for (int32_t i = 0; i < dict_data->dict_len_; ++i) {
            if (dict_data->dict_values_->is_null(i) && OB_FAIL(valid_dict_codes.set(i, true))) {
              LOG_WARN("fail to set valid_dict_codes", K(ret), K(i));
            }
          }
        }
      } else if (op_type == WHITE_OP_NN) {
        // IS NOT NULL：所有非 NULL 字典值满足
        for (int32_t i = 0; OB_SUCC(ret) && i < dict_data->dict_len_; ++i) {
          if (OB_FAIL(valid_dict_codes.set(i, true))) {
            LOG_WARN("fail to set valid_dict_codes", K(ret), K(i));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (dict_data->has_null_ && lib::is_oracle_mode()) {
          for (int32_t i = 0; i < dict_data->dict_len_; ++i) {
            if (dict_data->dict_values_->is_null(i) && OB_FAIL(valid_dict_codes.set(i, false))) {
              LOG_WARN("fail to set valid_dict_codes", K(ret), K(i));
            }
          }
        }
      } else {
        // 字典为空（全为 null），跳过比较操作
        if (OB_ISNULL(dict_data->dict_values_) || dict_data->dict_len_ <= 0) {
          // do nothing
        } else if (OB_FAIL(filter_batch_by_format<StrDiscVec>(dict_data->dict_values_,
                                                              white_filter,
                                                              nullptr,
                                                              dict_data->dict_len_,
                                                              &valid_dict_codes))) {
          LOG_WARN("fail to filter batch by format", K(ret));
        }
        // null_ref（dict_len）位置不设置，保持 false（NULL 与任何值比较都是 false）
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support string type for white filter dict optimization",
               K(ret),
               K(dict_data->parquet_type_));
    }
  }

  return ret;
}

template <typename ArgVec>
int ObParquetDictFilterPushdown::filter_batch_by_format(common::ObIVector *col_vector,
                                                        sql::ObWhiteFilterExecutor *white_filter,
                                                        ObPushdownFilterExecutor *parent_filter,
                                                        const int64_t count,
                                                        common::ObBitmap *result)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator temp_allocator;
  ArgVec *arg_vec = static_cast<ArgVec *>(col_vector);
  const sql::ObWhiteFilterOperatorType op_type = white_filter->get_op_type();
  const common::ObIArray<common::ObDatum> &ref_datums = white_filter->get_datums();
  ObDatumCmpFuncType cmp_func = white_filter->cmp_func_;
  int64_t skipped_count = 0;

  const bool is_oracle_mode = lib::is_oracle_mode();

  // 检查是否需要 padding（CHAR 类型）
  const common::ObObjMeta &col_obj_meta = white_filter->get_col_obj_meta();
  bool need_pad = white_filter->is_padding_mode() && col_obj_meta.is_fixed_len_char_type();

  // 如果需要 padding，获取 accuracy 信息
  common::ObAccuracy accuracy;
  if (need_pad) {
    const ExprFixedArray &column_exprs = white_filter->get_filter_node().column_exprs_;
    if (column_exprs.count() > 0 && OB_NOT_NULL(column_exprs.at(0))) {
      ObExpr *col_expr = column_exprs.at(0);
      if (col_expr->max_length_ > 0) {
        accuracy.set_length(col_expr->max_length_);
        accuracy.set_length_semantics(col_expr->datum_meta_.length_semantics_);
      } else {
        need_pad = false;
      }
    } else {
      need_pad = false;
    }
  }

  switch (op_type) {
    case sql::WHITE_OP_EQ:
    case sql::WHITE_OP_NE:
    case sql::WHITE_OP_GT:
    case sql::WHITE_OP_GE:
    case sql::WHITE_OP_LT:
    case sql::WHITE_OP_LE: {
      // 比较操作：使用get_payload统一获取datum
      if (OB_UNLIKELY(ref_datums.count() != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid argument for comparison operator", K(ret), K(ref_datums));
      } else {
        const common::ObDatum &ref_datum = ref_datums.at(0);
        const common::ObCmpOp cmp_op = sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type];
        bool ref_is_null = ref_datum.is_null();

        if (ref_is_null) {
          // ref_datum是NULL，所有行都不通过过滤
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
            if (OB_NOT_NULL(parent_filter) && parent_filter->can_skip_filter(i)) {
              ++skipped_count;
            } else if (arg_vec->is_null(i)) {
              // NULL不通过过滤
            } else {
              const char *payload = nullptr;
              ObLength payload_len = 0;
              arg_vec->get_payload(i, payload, payload_len);
              blocksstable::ObStorageDatum datum;
              datum.set_string(payload, static_cast<int32_t>(payload_len));
              bool cmp_ret = false;
              if (need_pad
                  && OB_FAIL(storage::pad_column(col_obj_meta, accuracy, temp_allocator, datum))) {
                LOG_WARN("fail to pad datum", K(ret), K(i), K(datum));
              } else if (OB_FAIL(blocksstable::compare_datum(datum,
                                                             ref_datum,
                                                             cmp_func,
                                                             cmp_op,
                                                             cmp_ret))) {
                LOG_WARN("Failed to compare datum", K(ret), K(i));
              } else if (cmp_ret) {
                result->set(i, true);
              }
            }
          }
        }
      }
      break;
    }
    case sql::WHITE_OP_BT: {
      if (OB_UNLIKELY(ref_datums.count() != 2)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid argument for between operators", K(ret), K(ref_datums));
      } else {
        const common::ObDatum &ref_datum0 = ref_datums.at(0);
        const common::ObDatum &ref_datum1 = ref_datums.at(1);

        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          if (OB_NOT_NULL(parent_filter) && parent_filter->can_skip_filter(i)) {
            ++skipped_count;
          } else if (arg_vec->is_null(i)) {
            // NULL不通过过滤
          } else {
            const char *payload = nullptr;
            ObLength payload_len = 0;
            arg_vec->get_payload(i, payload, payload_len);
            blocksstable::ObStorageDatum datum;
            datum.set_string(payload, static_cast<int32_t>(payload_len));

            int cmp_ret_0 = 0;
            int cmp_ret_1 = 0;
            if (need_pad
                && OB_FAIL(storage::pad_column(col_obj_meta, accuracy, temp_allocator, datum))) {
              LOG_WARN("fail to pad datum", K(ret), K(i), K(datum));
            } else if (OB_FAIL(cmp_func(datum, ref_datum0, cmp_ret_0))) {
              LOG_WARN("Failed to compare datum", K(ret), K(i));
            } else if (cmp_ret_0 < 0) {
            } else if (OB_FAIL(cmp_func(datum, ref_datum1, cmp_ret_1))) {
              LOG_WARN("Failed to compare datum", K(ret), K(i));
            } else if (cmp_ret_1 <= 0) {
              result->set(i, true);
            }
          }
        }
      }
      break;
    }
    case sql::WHITE_OP_IN: {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        if (OB_NOT_NULL(parent_filter) && parent_filter->can_skip_filter(i)) {
          ++skipped_count;
        } else if (arg_vec->is_null(i)) {
          // NULL不通过过滤
        } else {
          const char *payload = nullptr;
          ObLength payload_len = 0;
          arg_vec->get_payload(i, payload, payload_len);
          blocksstable::ObStorageDatum datum;
          datum.set_string(payload, static_cast<int32_t>(payload_len));
          bool is_existed = false;
          if (need_pad
              && OB_FAIL(storage::pad_column(col_obj_meta, accuracy, temp_allocator, datum))) {
            LOG_WARN("fail to pad datum", K(ret), K(i), K(datum));
          } else if (OB_FAIL(white_filter->exist_in_set(datum, is_existed))) {
            LOG_WARN("Failed to check object in hashset", K(ret), K(i));
          } else if (is_existed) {
            result->set(i, true);
          }
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unexpected filter pushdown operation type", K(ret), K(op_type));
      break;
    }
  }
  LOG_DEBUG("filter batch by format completed", K(ret), K(skipped_count));
  return ret;
}

} // namespace sql
} // namespace oceanbase
