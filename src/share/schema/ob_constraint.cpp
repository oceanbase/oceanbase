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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_constraint.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
using namespace std;
using namespace common;
namespace share
{
namespace schema
{

ObConstraint::ObConstraint()
    : ObSchema()
{
  reset();
}

ObConstraint::ObConstraint(ObIAllocator *allocator)
    : ObSchema(allocator)
{
  reset();
}

ObConstraint::~ObConstraint()
{
}

int ObConstraint::assign(const ObConstraint &src_schema)
{
  int ret = OB_SUCCESS;

  if (this != &src_schema) {
    reset();
    error_ret_ = src_schema.error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    table_id_ = src_schema.table_id_;
    constraint_id_ = src_schema.constraint_id_;
    schema_version_ = src_schema.schema_version_;
    constraint_type_ = src_schema.constraint_type_;
    rely_flag_ = src_schema.rely_flag_;
    enable_flag_ = src_schema.enable_flag_;
    validate_flag_ = src_schema.validate_flag_;
    is_modify_check_expr_ = src_schema.is_modify_check_expr_;
    is_modify_rely_flag_ = src_schema.is_modify_rely_flag_;
    is_modify_enable_flag_ = src_schema.is_modify_enable_flag_;
    is_modify_validate_flag_ = src_schema.is_modify_validate_flag_;
    name_generated_type_ = src_schema.name_generated_type_;
    int ret = OB_SUCCESS;
    if (OB_FAIL(deep_copy_str(src_schema.constraint_name_, constraint_name_))) {
      LOG_WARN("Fail to deep copy constraint_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.check_expr_, check_expr_))) {
      LOG_WARN("Fail to deep copy check_expr", K(ret));
    } else {
      column_cnt_ = src_schema.column_cnt_;
      if (column_cnt_ > 0) {
        column_id_array_ = static_cast<uint64_t*>(alloc(sizeof(uint64_t) * column_cnt_));
        if (NULL == column_id_array_) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for column_id_array_", K(ret));
        } else {
          int64_t i = 0;
          MEMSET(column_id_array_, 0, sizeof(uint64_t) * column_cnt_);
          for (const_cst_col_iterator iter = src_schema.cst_col_begin();
               OB_SUCC(ret) && (iter != src_schema.cst_col_end());
               ++iter) {
            column_id_array_[i++] = *iter;
          }
        }
      }
      need_validate_data_ = src_schema.need_validate_data_;
    }
  }

  return ret;
}

int64_t ObConstraint::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += constraint_name_.length() + 1;
  convert_size += check_expr_.length() + 1;
  convert_size += column_cnt_ * sizeof(uint64_t);
  return convert_size;
}

int ObConstraint::get_not_null_column_name(ObString &cst_col_name) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(CONSTRAINT_TYPE_NOT_NULL != constraint_type_
          || check_expr_.length() <= NOT_NULL_STR_EXTRA_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only not null constraint supported", K(ret));
  } else {
    cst_col_name.assign_ptr(check_expr_.ptr() + 1, check_expr_.length() - NOT_NULL_STR_EXTRA_SIZE);
  }
  return ret;
}

void ObConstraint::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  constraint_id_ = OB_INVALID_ID;
  schema_version_ = 0;
  constraint_name_.reset();
  check_expr_.reset();
  constraint_type_ = CONSTRAINT_TYPE_INVALID;
  rely_flag_ = false;
  enable_flag_ = true;
  validate_flag_ = CST_FK_VALIDATED;
  is_modify_check_expr_ = false;
  is_modify_rely_flag_ = false;
  is_modify_enable_flag_ = false;
  is_modify_validate_flag_ = false;
  column_cnt_ = 0;
  column_id_array_ = NULL;
  need_validate_data_ = true;
  name_generated_type_ = GENERATED_TYPE_UNKNOWN;
  ObSchema::reset();
}

OB_DEF_SERIALIZE(ObConstraint)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              table_id_,
              constraint_id_,
              schema_version_,
              constraint_name_,
              check_expr_,
              constraint_type_,
              rely_flag_,
              enable_flag_,
              validate_flag_,
              is_modify_rely_flag_,
              is_modify_enable_flag_,
              is_modify_validate_flag_,
              is_modify_check_expr_);
  //serialize column count and column ids
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, column_cnt_))) {
      LOG_WARN("Fail to encode column_cnt_", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, column_id_array_[i]))) {
        LOG_WARN("Fail to encode column_ids_", K(ret), K(i), K(column_id_array_[i]));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, need_validate_data_, name_generated_type_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObConstraint)
{
  int ret = OB_SUCCESS;
  ObString constraint_name;
  ObString check_expr;

  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              table_id_,
              constraint_id_,
              schema_version_,
              constraint_name,
              check_expr,
              constraint_type_,
              rely_flag_,
              enable_flag_,
              validate_flag_,
              is_modify_rely_flag_,
              is_modify_enable_flag_,
              is_modify_validate_flag_,
              is_modify_check_expr_);
  //deserialize column count and column_id_array_
  if (OB_SUCC(ret) && pos < data_len) {
    int64_t column_id = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &column_cnt_))) {
      LOG_WARN("Fail to decode index table count", K(ret));
    } else if (column_cnt_ > 0) {
      column_id_array_ = static_cast<uint64_t*>(alloc(sizeof(uint64_t) * column_cnt_));
      if (NULL == column_id_array_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for column_id_array_", K(ret));
      } else {
        MEMSET(column_id_array_, 0, sizeof(uint64_t) * column_cnt_);
        for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
          if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &column_id))) {
            LOG_WARN("Fail to deserialize column id", K(ret));
          } else {
            column_id_array_[i] = column_id;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, need_validate_data_, name_generated_type_);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(constraint_name, constraint_name_))) {
    LOG_WARN("Fail to deep copy constraint_name, ", K(ret), K_(constraint_name));
  } else if (OB_FAIL(deep_copy_str(check_expr, check_expr_))) {
    LOG_WARN("Fail to deep copy check_expr, ", K(ret), K_(check_expr));
  } else {/*do nothing*/}

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObConstraint)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              table_id_,
              constraint_id_,
              schema_version_,
              constraint_name_,
              check_expr_,
              constraint_type_,
              rely_flag_,
              enable_flag_,
              validate_flag_,
              is_modify_rely_flag_,
              is_modify_enable_flag_,
              is_modify_validate_flag_,
              is_modify_check_expr_);
  //get column count and column_id_array_ size
  len += serialization::encoded_length_vi64(column_cnt_);
  for (int64_t i = 0; i < column_cnt_; ++i) {
    len += serialization::encoded_length_vi64(column_id_array_[i]);
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, need_validate_data_, name_generated_type_);
  return len;
}

int64_t ObConstraint::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  J_OBJ_START();
  J_KV(K_(tenant_id),
    K_(table_id),
    K_(constraint_id),
    K_(schema_version),
    K_(constraint_name),
    K_(check_expr),
    K_(constraint_type),
    K_(rely_flag),
    K_(enable_flag),
    K_(validate_flag),
    K_(is_modify_check_expr),
    K_(is_modify_rely_flag),
    K_(is_modify_enable_flag),
    K_(is_modify_validate_flag),
    K_(need_validate_data),
    K_(name_generated_type),
    "column_id_array", ObArrayWrap<uint64_t>(column_id_array_, column_cnt_));
  J_OBJ_END();
  return pos;
}

int ObConstraint::assign_column_ids(const common::ObIArray<uint64_t> &column_ids_array)
{
  int ret = OB_SUCCESS;
  column_cnt_ = 0;
  column_id_array_ = NULL;
  const int64_t column_cnt = column_ids_array.count();
  if (column_cnt > 0) {
    column_id_array_ = static_cast<uint64_t*>(alloc(sizeof(uint64_t) * column_cnt));
    if (NULL == column_id_array_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for column_id_array_", K(ret));
    } else {
      MEMSET(column_id_array_, 0, sizeof(uint64_t) * column_cnt);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids_array.count(); ++i) {
      column_id_array_[column_cnt_++] = column_ids_array.at(i);
    }
  }
  return ret;
}

int ObConstraint::assign_not_null_cst_column_id(const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  column_id_array_ = NULL;
  const int64_t column_cnt = 1;
  column_id_array_ = static_cast<uint64_t*>(alloc(sizeof(uint64_t) * column_cnt));
  if (NULL == column_id_array_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for column_id_array_", K(ret));
  } else {
    MEMSET(column_id_array_, 0, sizeof(uint64_t) * column_cnt);
    column_id_array_[0] = column_id;
    column_cnt_ = 1;
  }
  return ret;
}

bool ObConstraint::is_sys_generated_name(bool check_unknown) const
{
  bool bret = false;
  if (GENERATED_TYPE_SYSTEM == name_generated_type_) {
    bret = true;
  } else if (GENERATED_TYPE_UNKNOWN == name_generated_type_ && check_unknown) {
    const char *cst_type_name = CONSTRAINT_TYPE_PRIMARY_KEY == constraint_type_ ? "_OBPK_" :
                                CONSTRAINT_TYPE_CHECK       == constraint_type_ ? "_OBCHECK_" :
                                CONSTRAINT_TYPE_UNIQUE_KEY  == constraint_type_ ? "_OBUNIQUE_" :
                                CONSTRAINT_TYPE_NOT_NULL    == constraint_type_ ? "_OBNOTNULL_" : nullptr;
    if (OB_NOT_NULL(cst_type_name)) {
      const int64_t cst_type_name_len = static_cast<int64_t>(strlen(cst_type_name));
      bret = (0 != ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_BIN,
                  constraint_name_.ptr(), constraint_name_.length(), cst_type_name, cst_type_name_len));
    }
  } else {
    bret = false;
  }
  return bret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
