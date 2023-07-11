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

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_udt.h"
#include "ob_log_utils.h"

namespace oceanbase
{
namespace libobcdc
{

int ObCDCUdtSchemaInfo::set_main_column(ColumnSchemaInfo *column_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("set null main column info", KR(ret));
  } else {
    main_column_ = column_info;
  }
  return ret;
}

int ObCDCUdtSchemaInfo::get_main_column(ColumnSchemaInfo *&column_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(main_column_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("main_column_ is null", KR(ret));
  } else {
    column_info = main_column_;
  }
  return ret;
}

int ObCDCUdtSchemaInfo::add_hidden_column(ColumnSchemaInfo *column_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("add null hidden column info", KR(ret));
  } else if (OB_FAIL(hidden_columns_.push_back(column_info))) {
    LOG_ERROR("push_back hidden_columns fail", KR(ret));
  }
  return ret;
}

int ObCDCUdtValueMap::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
  } else if (OB_ISNULL(tb_schema_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tb_schema_info is null", KR(ret));
  } else if (OB_FAIL(udt_value_map_.create(4, "UDT"))) {
    LOG_ERROR("create map fail", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObCDCUdtValueMap::add_column_value_to_udt(
    const ColumnSchemaInfo &column_schema_info,
    const bool is_out_row,
    const ObObj *value)
{
  int ret = OB_SUCCESS;
  ColValue *udt_val = nullptr;
  uint64_t udt_set_id = column_schema_info.get_udt_set_id();
  bool is_main_column = column_schema_info.is_udt_main_column();
  if (IS_NOT_INIT && OB_FAIL(init())) {
    LOG_ERROR("init fail", KR(ret), K(udt_set_id));
  } else if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value should not be null", KR(ret), K(udt_set_id));
  } else if (OB_FAIL(get_udt_value_(udt_set_id, udt_val))) {
    LOG_ERROR("get udt value fail", KR(ret), K(udt_set_id));
  } else if (is_main_column) {
    if (OB_FAIL(set_main_column_value_(column_schema_info, *value, *udt_val))) {
      LOG_ERROR("set_udt_main_col_ fail", KR(ret), K(column_schema_info), K(*udt_val));
    }
  } else if (OB_FAIL(add_hidden_column_value_(column_schema_info, is_out_row, *value, *udt_val))) {
    LOG_ERROR("add_hidden_column_value_ fail", KR(ret), K(column_schema_info), K(*udt_val));
  }
  return ret;
}

int ObCDCUdtValueMap::set_main_column_value_(
    const ColumnSchemaInfo &main_column_schema_info,
    const ObObj &value,
    ColValue &udt_val)
{
  int ret = OB_SUCCESS;
  if (main_column_schema_info.is_xmltype()) {
    if (set_xmltype_main_column_value_(value, udt_val)) {
      LOG_WARN("xmltype main column value not correct", KR(ret), K(value));
    }
  } else {
    LOG_ERROR(
        "not supported column type, only support xmltype currently",
        KR(ret),
        K(main_column_schema_info));
  }
  return ret;
}

int ObCDCUdtValueMap::set_xmltype_main_column_value_(const ObObj &value, ColValue &udt_val)
{
  int ret = OB_SUCCESS;
  if (value.is_null() || value.get_string().empty()) {
    // xmltype udt main column value should be null or emtpy
    udt_val.value_ = value;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xmltype main column value not correct", KR(ret), K(value));
  }
  return ret;
}

int ObCDCUdtValueMap::add_hidden_column_value_(
    const ColumnSchemaInfo &column_schema_info,
    const bool is_out_row,
    const ObObj &value,
    ColValue &udt_val)
{
  int ret = OB_SUCCESS;
  ColValue *cv_node = nullptr;
  if (OB_ISNULL(cv_node = static_cast<ColValue *>(allocator_.alloc(sizeof(ColValue))))) {
    LOG_ERROR("allocate memory for ColValue fail", "size", sizeof(ColValue));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    cv_node->reset();
    cv_node->value_ = value;
    cv_node->column_id_ = column_schema_info.get_column_id();
    cv_node->is_out_row_ = is_out_row;
    column_cast(cv_node->value_, column_schema_info);
    if (OB_FAIL(udt_val.add_child(cv_node))) {
      LOG_ERROR("add value to udt fail", KR(ret), K(udt_val), KP(cv_node));
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != cv_node) {
      allocator_.free((void *)cv_node);
      cv_node = NULL;
    }
  }
  return ret;
}

int ObCDCUdtValueMap::get_udt_value_(uint64_t udt_set_id, ColValue *&cv_node)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(udt_value_map_.get_refactored(udt_set_id, cv_node))) {
    // get value success, do nothing
  } else if (OB_HASH_NOT_EXIST == ret) {
    // not found , so create and set
    if (OB_FAIL(create_udt_value_(udt_set_id, cv_node))) {
      LOG_ERROR("create_udt_value_ fail", KR(ret), K(udt_set_id));
    } else if (OB_FAIL(udt_value_map_.set_refactored(udt_set_id, cv_node))) {
      LOG_ERROR("set udt_value_map_ fail", KR(ret), K(udt_set_id));
    }
  } else {
    LOG_ERROR("get_refactored fail", KR(ret), K(udt_set_id));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(cv_node)) {
    allocator_.free(cv_node);
    cv_node = nullptr;
  }
  return ret;
}

int ObCDCUdtValueMap::create_udt_value_(uint64_t udt_set_id, ColValue *&cv_node)
{
  int ret = OB_SUCCESS;
  ColumnSchemaInfo *column_schema_info = nullptr;
  if (OB_FAIL(tb_schema_info_->get_main_column_of_udt(udt_set_id, column_schema_info))) {
    LOG_ERROR("get_main_column_of_udt fail", KR(ret), K(udt_set_id));
  } else if (OB_ISNULL(cv_node = static_cast<ColValue *>(allocator_.alloc(sizeof(ColValue))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for ColValue fail", KR(ret), "size", sizeof(ColValue));
  } else if (OB_FAIL(column_values_.add(cv_node))) {
    LOG_ERROR("add column to column_values fail", KR(ret), K(udt_set_id));
  } else {
    cv_node->reset();
    cv_node->column_id_ = column_schema_info->get_column_id();
    cv_node->is_out_row_ = 0;
    column_cast(cv_node->value_, *column_schema_info);
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(cv_node)) {
    allocator_.free(cv_node);
    cv_node = nullptr;
  }
  return ret;
}

int ObCDCUdtValueBuilder::build(
    const ColumnSchemaInfo &column_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    const bool is_new_value,
    DmlStmtTask &dml_stmt_task,
    ObObj2strHelper &obj2str_helper,
    ObLobDataOutRowCtxList &lob_ctx_cols,
    ColValue &cv)
{
  int ret = OB_SUCCESS;
  if (column_schema_info.is_xmltype()) {
    if (OB_FAIL(build_xmltype(
        column_schema_info,
        tz_info_wrap,
        is_new_value,
        dml_stmt_task,
        obj2str_helper,
        lob_ctx_cols,
        cv))) {
      LOG_ERROR("build xmltype fail", KR(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR(
        "not supported column type, udt value builder only support xmltype currently",
        KR(ret),
        K(column_schema_info));
  }
  return ret;
}

int ObCDCUdtValueBuilder::build_xmltype(
    const ColumnSchemaInfo &column_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    const bool is_new_value,
    DmlStmtTask &dml_stmt_task,
    ObObj2strHelper &obj2str_helper,
    ObLobDataOutRowCtxList &lob_ctx_cols,
    ColValue &cv)
{
  int ret = OB_SUCCESS;
  ColValue *value = cv.children_.head_;
  ObString *col_str = nullptr;
  if (OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("input is null", KR(ret), K(cv));
  } else {
    if (! value->is_out_row_) {
      // ColValue not called obj2str before, so used origin obj value
      // and because xmltype only have one hidden blob cloumn
      // so call ObObj::get_string is fine, but other type hidden column cannot
      if (OB_FAIL(value->value_.get_string(value->string_value_))) {
        LOG_WARN("get_string from col_value", KR(ret), K(*value), K(cv));
      } else {
        col_str =  &value->string_value_;
      }
    } else {
      if (OB_FAIL(lob_ctx_cols.get_lob_column_value(value->column_id_, is_new_value, col_str))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_ERROR("get_lob_column_value fail", KR(ret), K(value->column_id_));
        } else {
          LOG_DEBUG("get_lob_column_value not exist", KR(ret), K(value->column_id_));
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(col_str)) {
      LOG_INFO("col_str is null", K(is_new_value), K(value->is_out_row_), K(value->column_id_));
    } else if (OB_FALSE_IT(cv.value_.set_string(ObUserDefinedSQLType, *col_str))) {
    } else if (OB_FAIL(dml_stmt_task.parse_col(
        dml_stmt_task.get_tenant_id(),
        cv.column_id_,
        column_schema_info,
        tz_info_wrap,
        obj2str_helper,
        cv))) {
      LOG_ERROR("dml_stmt_task parse_col fail", KR(ret), K(dml_stmt_task), K(cv));
    }
  }
  LOG_DEBUG("build_xmltype", KR(ret), K(cv.string_value_), K(value->string_value_));
  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
