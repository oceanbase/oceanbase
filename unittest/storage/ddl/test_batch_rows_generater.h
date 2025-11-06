/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_STORAGE_OB_TEST_BATCH_ROWS_GENERATER_H_
#define OCEANBASE_STORAGE_OB_TEST_BATCH_ROWS_GENERATER_H_

#define USING_LOG_PREFIX STORAGE

#include "lib/container/ob_array.h"
#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "common/object/ob_obj_type.h"
#include "unittest/storage/blocksstable/cs_encoding/ob_row_vector_converter.h"
#include "sql/engine/expr/ob_array_cast.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace unittest
{
class ObBatchRowsGen
{
public:
  ObBatchRowsGen() :
    is_inited_(false),
    is_storage_layer_row_(true),
    col_obj_types_(nullptr),
    col_count_(0),
    allocator_(ObMemAttr(MTL_ID(), "BRSGEN")),
    char_data_(nullptr),
    cols_desc_(),
    vec_convertor_() { }
  ~ObBatchRowsGen() = default;
  int init(ObObjType *col_obj_types, int64_t col_count, int64_t rowkey_col_count_, bool is_storage_layer_row = true);
  int get_batch_rows(blocksstable::ObBatchDatumRows &batch_rows, int64_t batch_size);
  int set_vector_type_column(common::ObDatum &res_datum);

private:
  OB_INLINE bool is_str_type(const ObObjType obj_type)
  {
    bool is_str_type = false;
    if (ObVarcharType == obj_type || ObCharType == obj_type || ObHexStringType == obj_type ||
        ObNVarchar2Type == obj_type || ObNCharType == obj_type || ObTextType == obj_type) {
      is_str_type = true;
    }
    return is_str_type;
  }
  OB_INLINE bool is_integer_type(const ObObjType obj_type)
  {
    bool is_integer_type = false;
    if (ObTinyIntType == obj_type || ObSmallIntType == obj_type || ObMediumIntType == obj_type ||
        ObInt32Type == obj_type || ObIntType == obj_type || ObUTinyIntType == obj_type ||
        ObUSmallIntType == obj_type || ObUMediumIntType == obj_type || ObUInt32Type == obj_type || ObUInt64Type == obj_type) {
      is_integer_type = true;
    }
    return is_integer_type;
  }
  OB_INLINE bool is_vec_type(const ObObjType obj_type)
  {
    bool is_vec_type = false;
    if (ObCollectionSQLType == obj_type) {
      is_vec_type = true;
    }
    return is_vec_type;
  }
  OB_INLINE bool is_support_type(ObObjType *col_obj_types, int64_t col_count)
  {
    bool is_support = true;
    for (int64_t i = 0; is_support && i < col_count; ++i) {
      ObObjType obj_type = col_obj_types[i];
      if (!(is_integer_type(obj_type) || is_str_type(obj_type) || is_vec_type(obj_type))) {
        is_support = false;
        break;
      }
    }
    return is_support;
  }

private:
  bool is_inited_;
  bool is_storage_layer_row_;
  ObObjType *col_obj_types_;
  int64_t col_count_;
  int64_t rowkey_col_count_;
  ObArenaAllocator allocator_;
  char *char_data_;
  common::ObArray<share::schema::ObColDesc> cols_desc_;
  blocksstable::ObRowVectorConverter vec_convertor_;
};
int ObBatchRowsGen::init(ObObjType *col_obj_types, int64_t col_count, int64_t rowkey_col_count, bool is_storage_layer_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the batch rows gen has been initialized");
  } else if (OB_UNLIKELY(col_count <= 0 || rowkey_col_count <= 0 || col_count < rowkey_col_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the cols desc is empty", K(ret), K(col_count), K(rowkey_col_count));
  } else if (!is_support_type(col_obj_types, col_count)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("there are not support type", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      share::schema::ObColDesc col_desc;
      col_desc.col_id_ = static_cast<uint64_t>(i + OB_APP_MIN_COLUMN_ID);
      col_desc.col_type_.set_type(col_obj_types[i]);
      if (OB_FAIL(cols_desc_.push_back(col_desc))) {
        LOG_WARN("fail to push back col desc", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_storage_layer_row_ = is_storage_layer_row;
      col_obj_types_ = col_obj_types;
      col_count_ = col_count;
      rowkey_col_count_ = rowkey_col_count;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBatchRowsGen::get_batch_rows(blocksstable::ObBatchDatumRows &batch_rows, int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObArray<VectorFormat> vec_formats;
  batch_rows.reset();
  vec_convertor_.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the batch rows gen has not been initialized", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
    if (is_integer_type(col_obj_types_[i])) {
      if (OB_FAIL(vec_formats.push_back(VectorFormat::VEC_FIXED))) {
        LOG_WARN("fail to push back vec format", K(ret));
      }
    } else if (is_str_type(col_obj_types_[i]) || is_vec_type(col_obj_types_[i])) {
      if (OB_FAIL(vec_formats.push_back(VectorFormat::VEC_CONTINUOUS))) {
        LOG_WARN("fail to push back vec format", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t char_data_size = 4096;
    if (nullptr == char_data_) {
      char_data_ = static_cast<char *>(allocator_.alloc(char_data_size));
      if (nullptr == char_data_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else {
        MEMSET(char_data_, 0x7F, char_data_size);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(vec_convertor_.init(cols_desc_, vec_formats, batch_size))) {
      LOG_WARN("fail to initialize vec convertor", K(ret));
    } else {
      bool is_full = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        blocksstable::ObDatumRow *row = OB_NEWx(blocksstable::ObDatumRow, &allocator_);
        if (nullptr == row) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_FAIL(row->init(allocator_, col_count_))) {
          LOG_WARN("fail to initialize datum row", K(ret));
        } else {
          row->row_flag_ = blocksstable::ObDmlFlag::DF_INSERT;
          row->mvcc_row_flag_.set_last_multi_version_row(true);
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < col_count_; ++j) {
          if (is_storage_layer_row_ && rowkey_col_count_ == j) {
            row->storage_datums_[j].set_int(-1);
          } else if (is_storage_layer_row_ && rowkey_col_count_ + 1 == j) {
            row->storage_datums_[j].set_int(0);
          } else if (is_integer_type(col_obj_types_[j])) {
            row->storage_datums_[j].set_int(i);
          } else if (is_str_type(col_obj_types_[j])) {
            row->storage_datums_[j].set_string(char_data_, char_data_size);
          } else if (is_vec_type(col_obj_types_[j])) {
            if (OB_FAIL(set_vector_type_column(row->storage_datums_[j]))) {
              LOG_WARN("fail to set vector type column", K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(vec_convertor_.append_row(*row, is_full))) {
          LOG_WARN("fail to append row", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (!is_full) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the vec convertor is not full", K(ret));
        } else {
          if (OB_FAIL(vec_convertor_.get_batch_datums(batch_rows))) {
            LOG_WARN("fail to get batch rows", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObBatchRowsGen::set_vector_type_column(common::ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObSqlCollectionInfo type_info_parse(allocator_);
  ObString type_name(strlen("ARRAY(FLOAT)"), "ARRAY(FLOAT)");
  ObIArrayType *arr_var = nullptr;
  type_info_parse.set_name(type_name);
  if (OB_FAIL(type_info_parse.parse_type_info())) {
    LOG_WARN("fail to parse type info", K(ret));
  } else {
    ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(type_info_parse.collection_meta_);
    if (OB_FAIL( ObArrayTypeObjFactory::construct(allocator_, *arr_type, arr_var))) {
      LOG_WARN("fail to construct array type", K(ret));
    } else {
      ObString arr_text("[3.14, 3.14, 3.14, 3.14, 3.14]");
      ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
      ObString res_string;
      if (OB_FAIL(sql::ObArrayCastUtils::string_cast(allocator_, arr_text, arr_var, dst_elem_type))) {
        LOG_WARN("fail to cast array", K(ret));
      } else if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(arr_var, arr_var->get_raw_binary_len(), allocator_, res_string))) {
        LOG_WARN("fail to set array res", K(ret));
      } else {
        res_datum.set_string(res_string);
      }
    }
  }
  return ret;
}
} // end namespace unittest
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TEST_BATCH_ROWS_GENERATER_H_