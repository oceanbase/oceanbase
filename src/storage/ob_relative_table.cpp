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
#include "ob_relative_table.h"
#include "observer/ob_server_struct.h"
#include "lib/ob_define.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_unique_index_row_transformer.h"
#include "ob_partition_store.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace storage {
// ------ ObRelativeTable ------ //
bool ObRelativeTable::set_schema_param(const ObTableSchemaParam* param)
{
  if (OB_ISNULL(param) || !param->is_valid()) {
    use_schema_param_ = false;
    schema_param_ = NULL;
  } else {
    use_schema_param_ = true;
    schema_param_ = param;
  }
  return use_schema_param_;
}

uint64_t ObRelativeTable::get_table_id() const
{
  return use_schema_param_ ? schema_param_->get_table_id() : schema_->get_table_id();
}

int64_t ObRelativeTable::get_schema_version() const
{
  return use_schema_param_ ? schema_param_->get_schema_version() : schema_->get_schema_version();
}

int ObRelativeTable::get_col_desc(const uint64_t column_id, share::schema::ObColDesc& col_desc) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (!use_schema_param_) {
    const ObColumnSchemaV2* column = NULL;
    if (NULL == (column = schema_->get_column_schema(column_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong column id", K(ret), K(column_id));
    } else {
      col_desc.col_id_ = column_id;
      col_desc.col_type_ = column->get_meta_type();
      col_desc.col_order_ = column->get_order_in_rowkey();
    }
  } else {
    const ObColumnParam* param = NULL;
    if (NULL == (param = schema_param_->get_column(column_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong column id", K(ret), K(column_id), K(*schema_param_));
    } else {
      col_desc.col_id_ = column_id;
      col_desc.col_type_ = param->get_meta_type();
      col_desc.col_order_ = param->get_column_order();
    }
  }
  return ret;
}

int ObRelativeTable::get_col_desc_by_idx(const int64_t idx, share::schema::ObColDesc& col_desc) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    const int64_t column_cnt = get_column_count();
    col_desc.reset();
    if (idx < 0 || idx >= column_cnt) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LOG_WARN("idx out of range", K(ret), K(column_cnt));
    } else if (!use_schema_param_) {
      const ObColumnSchemaV2* column = NULL;
      if (NULL == (column = schema_->get_column_schema_by_idx(idx))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("column is NULL", K(ret), K(idx));
      } else {
        col_desc.col_id_ = column->get_column_id();
        col_desc.col_type_ = column->get_meta_type();
        col_desc.col_order_ = column->get_order_in_rowkey();
      }
    } else {
      const ObColumnParam* param = NULL;
      if (NULL == (param = schema_param_->get_column_by_idx(idx))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong column id", K(ret), K(idx));
      } else {
        col_desc.col_id_ = param->get_column_id();
        col_desc.col_type_ = param->get_meta_type();
        col_desc.col_order_ = param->get_column_order();
      }
    }
  }
  return ret;
}

int ObRelativeTable::get_rowkey_col_desc_by_idx(const int64_t idx, share::schema::ObColDesc& col_desc) const
{
  int ret = OB_SUCCESS;
  int64_t rowkey_size = 0;
  col_desc.reset();
  if (idx < 0 || idx >= (rowkey_size = get_rowkey_column_num())) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("idx out of range", K(ret), K(rowkey_size));
  } else if (!use_schema_param_) {
    const ObRowkeyColumn* column = NULL;
    if (NULL == (column = schema_->get_rowkey_info().get_column(idx))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("rowkey column is NULL", K(ret), K(schema_->get_rowkey_info()));
    } else {
      col_desc.col_id_ = column->column_id_;
      col_desc.col_type_ = column->type_;
      col_desc.col_order_ = column->order_;
    }
  } else {
    const ObColumnParam* param = NULL;
    if (NULL == (param = schema_param_->get_column_by_idx(idx))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong column id", K(ret), K(idx));
    } else {
      col_desc.col_id_ = param->get_column_id();
      col_desc.col_type_ = param->get_meta_type();
      col_desc.col_order_ = param->get_column_order();
    }
  }
  return ret;
}

int ObRelativeTable::get_rowkey_col_id_by_idx(const int64_t idx, uint64_t& col_id) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    const int64_t rowkey_size = get_rowkey_column_num();
    if (idx < 0 || idx >= rowkey_size) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LOG_WARN("idx out of range", K(ret), K(rowkey_size));
    } else if (!use_schema_param_) {
      const ObRowkeyColumn* column = NULL;
      if (NULL == (column = schema_->get_rowkey_info().get_column(idx))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("rowkey column is NULL", K(ret), K(schema_->get_rowkey_info()));
      } else {
        col_id = column->column_id_;
      }
    } else {
      const ObColumnParam* param = NULL;
      if (NULL == (param = schema_param_->get_rowkey_column_by_idx(idx))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong column id", K(ret), K(idx));
      } else {
        col_id = param->get_column_id();
      }
    }
  }
  return ret;
}

int ObRelativeTable::get_rowkey_column_ids(ObIArray<ObColDesc>& column_ids) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    if (!use_schema_param_) {
      if (OB_FAIL(schema_->get_rowkey_column_ids(column_ids))) {
        LOG_WARN("get rowkey column ids from schema fail", K(ret));
      }
    } else {
      if (OB_FAIL(schema_param_->get_rowkey_column_ids(column_ids))) {
        LOG_WARN("get rowkey column ids from param fail", K(ret));
      }
    }
  }
  return ret;
}

int ObRelativeTable::get_column_data_length(const uint64_t column_id, int32_t& len) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    if (!use_schema_param_) {
      const ObColumnSchemaV2* col = NULL;
      if (NULL == (col = schema_->get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema shouldn't be NULL here", K(ret), K(column_id));
      } else {
        len = col->get_data_length();
      }
    } else {
      const ObColumnParam* param = NULL;
      if (NULL == (param = schema_param_->get_column(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column param shouldn't be NULL here", K(ret), K(column_id));
      } else {
        len = param->get_data_length();
      }
    }
  }
  return ret;
}

int ObRelativeTable::is_rowkey_column_id(const uint64_t column_id, bool& is_rowkey) const
{
  int ret = OB_SUCCESS;
  is_rowkey = false;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column id", K(ret), K(column_id));
  } else if (!use_schema_param_) {
    const ObColumnSchemaV2* col = NULL;
    if (nullptr == (col = schema_->get_column_schema(column_id))) {
      is_rowkey = false;
    } else {
      is_rowkey = col->is_rowkey_column();
    }
  } else {
    if (OB_FAIL(schema_param_->is_rowkey_column(column_id, is_rowkey))) {
      LOG_WARN("check is_rowkey fail", K(ret), K(column_id), K(*schema_param_));
    }
  }
  return ret;
}

int ObRelativeTable::is_column_nullable(const uint64_t column_id, bool& is_nullable) const
{
  int ret = OB_SUCCESS;
  is_nullable = false;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column id", K(ret), K(column_id));
  } else if (!use_schema_param_) {
    const ObColumnSchemaV2* col = NULL;
    if (nullptr == (col = schema_->get_column_schema(column_id))) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("column schema is null", K(ret), K(column_id), K(*schema_));
    } else {
      is_nullable = col->is_nullable();
    }
  } else {
    if (OB_FAIL(schema_param_->is_column_nullable(column_id, is_nullable))) {
      LOG_WARN("check is_rowkey fail", K(ret), K(column_id), K(*schema_param_));
    }
  }
  return ret;
}

int64_t ObRelativeTable::get_rowkey_column_num() const
{
  return use_schema_param_ ? schema_param_->get_rowkey_column_num() : schema_->get_rowkey_column_num();
}

int64_t ObRelativeTable::get_shadow_rowkey_column_num() const
{
  return use_schema_param_ ? schema_param_->get_shadow_rowkey_column_num() : schema_->get_shadow_rowkey_column_num();
}

int64_t ObRelativeTable::get_column_count() const
{
  return use_schema_param_ ? schema_param_->get_column_count() : schema_->get_column_count();
}

int ObRelativeTable::get_fulltext_column(uint64_t& column_id) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    if (!use_schema_param_) {
      if (OB_FAIL(schema_->get_index_info().get_fulltext_column(column_id))) {
        LOG_WARN("fail to get fulltext column id", K(ret), K(schema_->get_index_info()));
      }
    } else {
      column_id = schema_param_->get_fulltext_col_id();
    }
  }
  return ret;
}

int ObRelativeTable::get_index_name(ObString& index_name) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    if (!use_schema_param_) {
      if (OB_FAIL(schema_->get_index_name(index_name))) {
        LOG_WARN("get index name from schema fail", K(ret));
      }
    } else {
      if (OB_FAIL(schema_param_->get_index_name(index_name))) {
        LOG_WARN("get index name from param fail", K(ret));
      }
    }
  }
  return ret;
}

int ObRelativeTable::get_primary_key_name(ObString& pk_name) const
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    if (!use_schema_param_) {
      ObString tmp_pk_name;
      if (OB_FAIL(schema_->get_pk_constraint_name(tmp_pk_name))) {
        LOG_WARN("get pk name from param failed", K(ret));
      } else {
        pk_name.assign_ptr(tmp_pk_name.ptr(), tmp_pk_name.length());
      }
    } else {
      pk_name = schema_param_->get_pk_name();
    }
  }

  return ret;
}

bool ObRelativeTable::is_index_table() const
{
  return use_schema_param_ ? schema_param_->is_index_table() : schema_->is_index_table();
}

bool ObRelativeTable::is_storage_index_table() const
{
  return use_schema_param_ ? schema_param_->is_storage_index_table() : schema_->is_storage_index_table();
}

bool ObRelativeTable::can_read_index() const
{
  return use_schema_param_ ? schema_param_->can_read_index() : schema_->can_read_index();
}

bool ObRelativeTable::is_unique_index() const
{
  return use_schema_param_ ? schema_param_->is_unique_index() : schema_->is_unique_index();
}

bool ObRelativeTable::is_domain_index() const
{
  return use_schema_param_ ? schema_param_->is_domain_index() : schema_->is_domain_index();
}

int ObRelativeTable::check_rowkey_in_column_ids(
    const common::ObIArray<uint64_t>& column_ids, const bool has_other_column) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    const int64_t count = get_rowkey_column_num();
    if (count <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema rowkey count should greater than zero", K(ret), K(count));
    } else if ((has_other_column && column_ids.count() < count) || (!has_other_column && column_ids.count() != count)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("rowkey count mismatch", K(has_other_column), K(count), K(column_ids.count()));
    } else {
      if (!use_schema_param_) {
        const ObRowkeyInfo& info = schema_->get_rowkey_info();
        const ObRowkeyColumn* col = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          if (NULL == (col = info.get_column(i))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("wrong rowkey info, can not get column", K(i), K(count), K(info));
          } else if (column_ids.at(i) != col->column_id_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("column id mismatch", K(column_ids.at(i)), K(col->column_id_));
          }
        }
      } else {
        const ObColumnParam* param = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          if (NULL == (param = schema_param_->get_column_by_idx(i))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("wrong column param", K(i), K(count), K(schema_param_));
          } else if (column_ids.at(i) != param->get_column_id()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("column id mismatch", K(column_ids.at(i)), K(param->get_column_id()));
          }
        }
      }
    }
  }
  return ret;
}

int ObRelativeTable::check_column_in_map(const ColumnMap& col_map) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    if (!use_schema_param_) {
      ObTableSchema::const_column_iterator col_iter = schema_->column_begin();
      for (; OB_SUCC(ret) && col_iter != schema_->column_end(); ++col_iter) {
        if (OB_ISNULL(col_iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wrong column iterator", K(ret));
        } else {
          uint64_t col_id = (*col_iter)->get_column_id();
          int32_t idx = -1;
          if (OB_SUCCESS != col_map.get(col_id, idx) || idx < 0) {
            ret = OB_ERR_BAD_FIELD_ERROR;
            STORAGE_LOG(WARN, "column_ids lack some columns", K(ret), K(col_id));
          }
        }
      }
    } else {
      const ObColumnParam* param = NULL;
      const int64_t column_cnt = get_column_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
        if (OB_ISNULL(param = schema_param_->get_column_by_idx(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column param is NULL", K(ret), K(i));
        } else {
          uint64_t col_id = param->get_column_id();
          int32_t idx = 0;
          if (OB_SUCCESS != col_map.get(col_id, idx)) {
            ret = OB_ERR_BAD_FIELD_ERROR;
            STORAGE_LOG(WARN, "column_ids lack some columns", K(ret), K(param));
          }
        }
      }
    }
  }
  return ret;
}

int ObRelativeTable::check_index_column_in_map(const ColumnMap& col_map, const int64_t data_table_rowkey_cnt) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (0 == data_table_rowkey_cnt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rowkey count of data table is zero", K(ret), K(*this));
  } else if (!is_index_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is not index table", K(ret), K(*this));
  } else {
    const int64_t idx_rk_len = get_rowkey_column_num();
    if (!use_schema_param_) {
      const ObRowkeyInfo& rk_info = schema_->get_rowkey_info();
      const ObRowkeyColumn* col = NULL;
      for (int64_t j = 0; OB_SUCC(ret) && j < idx_rk_len - data_table_rowkey_cnt; ++j) {
        col = rk_info.get_column(j);
        int32_t idx = -1;
        if (OB_ISNULL(col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wrong idx table rowkey info, cannot get column", K(ret), K(rk_info), K(j));
        } else if (OB_SUCCESS != col_map.get(col->column_id_, idx) || idx < 0) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          STORAGE_LOG(WARN, "column_ids lack some index columns", K(ret), K(col->column_id_));
        }
      }
    } else {
      const ObColumnParam* param = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < idx_rk_len - data_table_rowkey_cnt; ++i) {
        if (OB_ISNULL(param = schema_param_->get_column_by_idx(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column param is NULL", K(ret), K(i));
        } else {
          uint64_t col_id = param->get_column_id();
          int32_t idx = 0;
          if (OB_SUCCESS != col_map.get(col_id, idx)) {
            ret = OB_ERR_BAD_FIELD_ERROR;
            STORAGE_LOG(WARN, "column_ids lack some columns", K(ret), K(param));
          }
        }
      }
    }
  }
  return ret;
}

int ObRelativeTable::build_table_param(
    const common::ObIArray<uint64_t>& out_col_ids, share::schema::ObTableParam& table_param) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (0 == out_col_ids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("output column ids count is 0", K(ret));
  } else {
    table_param.reset();
    if (!use_schema_param_) {
      if (OB_FAIL(table_param.convert(*schema_, *schema_, out_col_ids, false))) {
        LOG_WARN("build table param from schema fail", K(ret), K(*schema_));
      }
    } else {
      if (OB_FAIL(table_param.convert_schema_param(*schema_param_, out_col_ids))) {
        LOG_WARN("build table param from param fail", K(ret), K(*schema_param_));
      }
    }
  }
  return ret;
}

int ObRelativeTable::build_index_row(const ObNewRow& table_row, const ColumnMap& col_map, const bool only_rowkey,
    ObNewRow& index_row, bool& null_idx_val, ObIArray<ObColDesc>* idx_columns)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (!is_storage_index_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is not storage index table", K(ret), K(*this));
  } else if (table_row.is_invalid() || !col_map.is_inited() || index_row.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "table_row can not be empty", "table_row", to_cstring(table_row), "index_row", to_cstring(index_row));
  } else {
    share::schema::ObColDesc col_desc;
    null_idx_val = false;
    const int64_t rowkey_size = get_rowkey_column_num();
    if (idx_columns != NULL) {
      idx_columns->reset();
    }
    index_row.count_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_size; ++i) {
      if (OB_FAIL(get_rowkey_col_desc_by_idx(i, col_desc))) {
        STORAGE_LOG(WARN, "get rowkey column description fail", K(ret), K(i));
      } else if (OB_FAIL(set_index_value(table_row, col_map, col_desc, rowkey_size, index_row, idx_columns))) {
        STORAGE_LOG(WARN, "failed to set index value");
      }
    }

    if (OB_SUCC(ret) && !only_rowkey) {
      if (!use_schema_param_) {
        ObTableSchema::const_column_iterator b = schema_->column_begin();
        ObTableSchema::const_column_iterator e = schema_->column_end();
        for (; OB_SUCC(ret) && b != e; ++b) {
          if (OB_ISNULL(b)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "wrong column iterator", K(ret));
          } else if ((*b)->is_rowkey_column() || OB_HIDDEN_ROWID_COLUMN_ID == (*b)->get_column_id() ||
                     schema_->is_depend_column((*b)->get_column_id())) {
            // skip following cases:
            // - rowkey column
            // - no need to fill row_id column in memtable
            // - materialized view may contains the columns that do not exist in main table
          } else {
            col_desc.reset();
            col_desc.col_id_ = (*b)->get_column_id();
            col_desc.col_type_ = (*b)->get_meta_type();
            col_desc.col_order_ = (*b)->get_order_in_rowkey();
            if (OB_FAIL(set_index_value(table_row, col_map, col_desc, rowkey_size, index_row, idx_columns))) {
              STORAGE_LOG(WARN, "failed to set index value");
            }
          }
        }
      } else {
        const int64_t column_cnt = get_column_count();
        for (int64_t i = rowkey_size; OB_SUCC(ret) && i < column_cnt; ++i) {
          const ObColumnParam* param = NULL;
          if (NULL == (param = schema_param_->get_column_by_idx(i))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("wrong column id", K(ret), K(i));
          } else if (OB_HIDDEN_ROWID_COLUMN_ID == param->get_column_id() ||
                     schema_param_->is_depend_column(param->get_column_id())) {
            // skip following cases:
            // - no need to fill row_id column in memtable
            // - materialized view may contains the columns that do not exist in main table
          } else {
            col_desc.reset();
            col_desc.col_id_ = param->get_column_id();
            col_desc.col_type_ = param->get_meta_type();
            col_desc.col_order_ = param->get_column_order();
            if (OB_FAIL(set_index_value(table_row, col_map, col_desc, rowkey_size, index_row, idx_columns))) {
              STORAGE_LOG(WARN, "failed to set index value");
            }
          }
        }
      }
    }

    // set shadow columns correctly
    if (OB_SUCC(ret) && is_unique_index()) {
      const int64_t shadow_column_cnt = get_shadow_rowkey_column_num();
      const int64_t unique_key_cnt = get_rowkey_column_num() - shadow_column_cnt;
      if (OB_FAIL(ObUniqueIndexRowTransformer::convert_to_unique_index_row(index_row,
              static_cast<ObCompatibilityMode>(THIS_WORKER.get_compatibility_mode()),
              unique_key_cnt,
              shadow_column_cnt,
              nullptr /*projector*/,
              null_idx_val,
              index_row))) {
        LOG_WARN("fail to convert unique index row", K(ret));
      }
    }
  }
  return ret;
}

int ObRelativeTable::set_index_value(const ObNewRow& table_row, const ColumnMap& col_map, const ObColDesc& col_desc,
    const int64_t rowkey_size, ObNewRow& index_row, ObIArray<share::schema::ObColDesc>* idx_columns)
{
  int ret = OB_SUCCESS;
  int32_t idx = -1;
  uint64_t id =
      col_desc.col_id_ > OB_MIN_SHADOW_COLUMN_ID ? col_desc.col_id_ - OB_MIN_SHADOW_COLUMN_ID : col_desc.col_id_;
  if (table_row.is_invalid() || !col_map.is_inited() || rowkey_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(table_row), K(rowkey_size), K(ret));
  } else {
    if (OB_FAIL(col_map.get(id, idx)) || idx < 0) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "failed to get column index", "column_id", id, "index", idx);
    } else if (idx >= table_row.count_) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "can not get row value", "table_row", to_cstring(table_row), "column_id", id, "index", idx);
    } else {
      *(index_row.cells_ + index_row.count_++) = table_row.cells_[idx];
    }
  }
  if (OB_SUCC(ret)) {
    if (idx_columns) {
      if (OB_FAIL(idx_columns->push_back(col_desc))) {
        STORAGE_LOG(WARN, "failed to add column id", "column_id", col_desc.col_id_);
      }
    }
  }
  return ret;
}

// ------ ObRelativeTables ------ //
bool ObRelativeTables::set_table_param(const ObTableDMLParam* param)
{
  if (OB_ISNULL(param) || !param->is_valid()) {
    use_table_param_ = false;
    table_param_ = NULL;
  } else {
    table_param_ = param;
    use_table_param_ = true;
  }
  return use_table_param_;
}

int ObRelativeTables::get_tenant_schema_version(const uint64_t tenant_id, int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative tables is invalid", K(ret), K(use_table_param_), KP(table_param_), KP(data_table_.schema_));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(tenant_id));
  } else {
    if (!use_table_param_) {
      if (OB_FAIL(schema_guard_.get_schema_version(tenant_id, schema_version))) {
        LOG_WARN("fail to get schema version", K(ret), K(tenant_id));
      }
    } else {
      schema_version = table_param_->get_tenant_schema_version();
    }
  }
  return ret;
}

int ObRelativeTables::prepare_tables(const uint64_t table_id, const int64_t version, const int64_t read_snapshot,
    const int64_t tenant_schema_version, const common::ObIArray<uint64_t>* upd_col_ids,
    share::schema::ObMultiVersionSchemaService* schema_service, ObPartitionStore& store)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
  bool check_formal = extract_pure_id(table_id) > OB_MAX_CORE_TABLE_ID;  // avoid circular reference
  if (OB_INVALID_ID == table_id || version < 0 || OB_ISNULL(schema_service) || !store.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(version), KP(schema_service), K(store.is_inited()));
  } else if (OB_FAIL(check_schema_version(*schema_service, tenant_id, table_id, tenant_schema_version, version))) {
    LOG_WARN("check schema version fail", K(ret), K(tenant_id), K(tenant_schema_version), K(table_id), K(version));
  } else if (!use_table_param_) {
    if (!schema_guard_.is_inited() && OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard_))) {
      LOG_WARN("get schema guard fail", K(ret), K(tenant_id));
    } else if (check_formal && OB_FAIL(schema_guard_.check_formal_guard())) {
      LOG_WARN("schema_guard is not formal", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard_.get_table_schema(table_id, data_table_.schema_)) ||
               OB_ISNULL(data_table_.schema_)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("failed to get schema or schema is null", K(ret), K(table_id), KPC(data_table_.schema_));
    } else if (OB_FAIL(prepare_data_table(read_snapshot, store))) {
      LOG_WARN("fail to get data table", K(ret), K(table_id), K(read_snapshot));
    } else if (OB_FAIL(prepare_index_tables(read_snapshot, upd_col_ids, store))) {
      LOG_WARN("fail to get index tables", K(ret), K(table_id), K(read_snapshot));
    }
  } else {
    if (OB_ISNULL(table_param_) || !table_param_->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param is NULL or invalid", K(ret), KP(table_param_));
      if (OB_NOT_NULL(table_param_)) {
        LOG_WARN("table_param_ is NULL or invalid", K(ret), K(table_param_));
      }
    } else if (version != table_param_->get_data_table().get_schema_version()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("version mismatch", K(ret), K(version), K(table_param_->get_data_table()));
    } else if (!data_table_.set_schema_param(&(table_param_->get_data_table()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set data table schema param fail", K(ret), K(table_param_->get_data_table()));
    } else if (OB_FAIL(prepare_data_table_from_param(read_snapshot, store))) {
      LOG_WARN("fail to get data table from param", K(ret), K(table_param_));
    } else if (OB_FAIL(prepare_index_tables_from_param(read_snapshot, upd_col_ids, store))) {
      LOG_WARN("fail to get index tables from param", K(ret), K(table_param_));
    }
  }
  return ret;
}

int ObRelativeTables::prepare_data_table(const int64_t read_snapshot, ObPartitionStore& store)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_.schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table schema is NULL", K(ret));
  } else {
    const ObTableSchema* schema = data_table_.schema_;
    data_table_.allow_not_ready(
        schema->is_global_index_table() && ObIndexStatus::INDEX_STATUS_UNAVAILABLE == schema->get_index_status());
    max_col_num_ = schema->get_column_count();

    if (OB_FAIL(store.get_read_tables(
            schema->get_table_id(), read_snapshot, data_table_.tables_handle_, data_table_.allow_not_ready()))) {
      LOG_WARN("failed to get data table read tables", K(ret), K(schema->get_table_id()));
    }
  }
  return ret;
}

int ObRelativeTables::prepare_data_table_from_param(const int64_t read_snapshot, ObPartitionStore& store)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_.get_schema_param())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table schema param is NULL", K(ret));
  } else {
    const ObTableSchemaParam& schema = *(data_table_.get_schema_param());
    data_table_.allow_not_ready(
        schema.is_global_index_table() && ObIndexStatus::INDEX_STATUS_UNAVAILABLE == schema.get_index_status());
    max_col_num_ = schema.get_column_count();

    if (schema.is_dropped_schema()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("should not read dropped table", K(ret), K(schema));
    } else if (OB_FAIL(store.get_read_tables(
                   schema.get_table_id(), read_snapshot, data_table_.tables_handle_, data_table_.allow_not_ready()))) {
      LOG_WARN("failed to get data table read tables", K(ret), K(schema.get_table_id()));
    }
  }
  return ret;
}

int ObRelativeTables::prepare_index_tables(
    const int64_t read_snapshot, const common::ObIArray<uint64_t>* upd_col_ids, ObPartitionStore& store)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  bool candidate = true;
  const ObTableSchema* schema = NULL;
  void* buf = NULL;

  if (OB_FAIL(data_table_.schema_->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
    LOG_WARN("get index failed", K(ret), K(*data_table_.schema_));
  } else if (simple_index_infos.count() > OB_MAX_INDEX_PER_TABLE) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("index table cnt too large", K(ret), K(simple_index_infos.count()), K(*data_table_.schema_));
  } else if (simple_index_infos.count() > 0) {
    idx_cnt_ = 0;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObRelativeTable) * simple_index_infos.count()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc relative tables", K(ret), K(simple_index_infos.count()));
    } else {
      index_tables_ = new (buf) ObRelativeTable[simple_index_infos.count()];
      index_tables_buf_count_ = simple_index_infos.count();
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      if (OB_FAIL(schema_guard_.get_table_schema(simple_index_infos.at(i).table_id_, schema))) {
        LOG_WARN("failed to get index schema", K(ret));
        break;
      } else if (OB_ISNULL(schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("failed to get index schema", "index_id", simple_index_infos.at(i).table_id_, K(ret));
        break;
      } else if (OB_UNLIKELY(schema->is_global_index_table())) {
        continue;
      }
      if (OB_SUCC(ret)) {
        if (is_final_invalid_index_status(schema->get_index_status(), schema->is_dropped_schema())) {
          candidate = false;
        } else if (NULL != upd_col_ids) {
          candidate = false;
          for (int64_t j = 0; OB_SUCC(ret) && j < upd_col_ids->count(); ++j) {
            if (NULL != schema->get_column_schema(upd_col_ids->at(j))) {
              candidate = true;
              break;
            }
          }
        } else {
          candidate = true;
        }
      }

      if (OB_SUCC(ret) && candidate && schema->is_valid()) {
        ObRelativeTable& relative_table = index_tables_[idx_cnt_];
        relative_table.allow_not_ready(INDEX_STATUS_UNAVAILABLE == schema->get_index_status());
        relative_table.schema_ = schema;
        if (schema->get_column_count() > max_col_num_) {
          max_col_num_ = schema->get_column_count();
        }

        if (OB_FAIL(store.get_read_tables(schema->get_table_id(),
                read_snapshot,
                relative_table.tables_handle_,
                relative_table.allow_not_ready()))) {
          LOG_WARN("failed to get index read tables", K(ret), K(schema->get_table_id()));
        }
        ++idx_cnt_;
      }
    }
  }
  return ret;
}

int ObRelativeTables::prepare_index_tables_from_param(
    const int64_t read_snapshot, const common::ObIArray<uint64_t>* upd_col_ids, ObPartitionStore& store)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  bool candidate = true;
  const ObTableSchemaParam* schema = NULL;
  void* buf = NULL;

  if (OB_ISNULL(table_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table dml param is NULL", K(ret), KP(table_param_));
  } else if (OB_MAX_INDEX_PER_TABLE < (cnt = table_param_->get_index_tables().count())) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("index table cnt too large", K(ret), K(cnt), K(*table_param_));
  } else if (cnt > 0) {
    idx_cnt_ = 0;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObRelativeTable) * cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc relative tables", K(ret), K(cnt));
    } else {
      index_tables_ = new (buf) ObRelativeTable[cnt];
      index_tables_buf_count_ = cnt;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
      schema = table_param_->get_index_tables().at(i);
      if (OB_ISNULL(schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index schema param is NULL", K(ret), K(i));
        break;
      } else if (OB_UNLIKELY(schema->is_global_index_table()) ||
                 is_final_invalid_index_status(schema->get_index_status(), schema->is_dropped_schema()) ||
                 !schema->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema not correct, check convert_table_dml_param", K(ret), K(*schema));
      }
      if (OB_SUCC(ret)) {
        if (NULL != upd_col_ids) {
          candidate = false;
          for (int64_t j = 0; OB_SUCC(ret) && j < upd_col_ids->count(); ++j) {
            if (NULL != schema->get_column(upd_col_ids->at(j))) {
              candidate = true;
              break;
            }
          }
        } else {
          candidate = true;
        }
      }

      if (OB_SUCC(ret) && candidate) {
        ObRelativeTable& relative_table = index_tables_[idx_cnt_];
        relative_table.allow_not_ready(INDEX_STATUS_UNAVAILABLE == schema->get_index_status());
        if (schema->get_column_count() > max_col_num_) {
          max_col_num_ = schema->get_column_count();
        }
        if (!relative_table.set_schema_param(schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set index table schema param", K(ret), K(*schema));
        } else if (OB_FAIL(store.get_read_tables(schema->get_table_id(),
                       read_snapshot,
                       relative_table.tables_handle_,
                       relative_table.allow_not_ready()))) {
          LOG_WARN("failed to get index read tables", K(ret), K(schema->get_table_id()));
        }
        ++idx_cnt_;
      }
    }
  }
  return ret;
}

int ObRelativeTables::check_schema_version(share::schema::ObMultiVersionSchemaService& schema_service,
    const uint64_t tenant_id, const uint64_t table_id, const int64_t tenant_schema_version, const int64_t table_version)
{
  int ret = OB_SUCCESS;
  int64_t latest_table_version = -1;
  bool check_formal = extract_pure_id(table_id) > OB_MAX_CORE_TABLE_ID;  // avoid circular reference
  int tmp_ret = check_tenant_schema_version(schema_service, tenant_id, table_id, tenant_schema_version);
  if (OB_SUCCESS == tmp_ret) {
    // Check tenant schema first. If not pass, then check table level schema version
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard_))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (check_formal && OB_FAIL(schema_guard_.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard_.get_table_schema_version(table_id, latest_table_version))) {
    LOG_WARN("failed to get table schema version", K(ret), K(table_id));
  } else if (table_version != latest_table_version) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("table version mismatch", K(ret), K(table_id), K(table_version), K(latest_table_version));
  }
  return ret;
}

int ObRelativeTables::check_tenant_schema_version(share::schema::ObMultiVersionSchemaService& schema_service,
    const uint64_t tenant_id, const uint64_t table_id, const int64_t tenant_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t latest_tenant_version = -1;
  if (is_inner_table(table_id)) {
    // sys table can't skip table schema check
    ret = OB_SCHEMA_EAGAIN;
  } else if (tenant_schema_version > 0 &&
             OB_FAIL(schema_service.get_tenant_refreshed_schema_version(tenant_id, latest_tenant_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id), K(tenant_schema_version));
  } else if (tenant_schema_version < 0 || latest_tenant_version < 0) {
    ret = OB_SCHEMA_EAGAIN;
  } else if (!share::schema::ObSchemaService::is_formal_version(latest_tenant_version)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_INFO("local schema_version is not formal, try again",
        K(ret),
        K(tenant_id),
        K(tenant_schema_version),
        K(latest_tenant_version));
  } else if (latest_tenant_version > 0 && tenant_schema_version == latest_tenant_version) {
    // no schema change, do nothing
    ret = OB_SUCCESS;
  } else {
    ret = OB_SCHEMA_EAGAIN;
    LOG_INFO(
        "need check table schema version", K(ret), K(tenant_id), K(tenant_schema_version), K(latest_tenant_version));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
