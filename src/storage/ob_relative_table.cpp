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

#include "storage/ob_relative_table.h"

#include "share/ob_unique_index_row_transformer.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace storage
{
// ------ ObRelativeTable ------ //
ObRelativeTable::~ObRelativeTable()
{
  destroy();
}

bool ObRelativeTable::is_valid() const
{
  return OB_NOT_NULL(schema_param_) && schema_param_->is_valid() && tablet_id_.is_valid();
}

void ObRelativeTable::destroy()
{
  allow_not_ready_ = false;
  schema_param_ = nullptr;
  tablet_id_.reset();
  tablet_iter_.reset();
  is_inited_ = false;
}

int ObRelativeTable::init(
    const share::schema::ObTableSchemaParam *param,
    const ObTabletID &tablet_id,
    const bool allow_not_ready)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRelativeTable has been inited. ", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_UNLIKELY(!tablet_id.is_valid())) {
    LOG_WARN("invalid argument", K(ret), KP(param), K(tablet_id));
  } else {
    schema_param_ = param;
    tablet_id_ = tablet_id;
    allow_not_ready_ = allow_not_ready;
    is_inited_ = true;
  }
  return ret;
}

uint64_t ObRelativeTable::get_table_id() const
{
  return schema_param_->get_table_id();
}

const ObTabletID& ObRelativeTable::get_tablet_id() const
{
  return tablet_id_;
}

int64_t ObRelativeTable::get_schema_version() const
{
  return schema_param_->get_schema_version();
}

int ObRelativeTable::get_col_desc(
    const uint64_t column_id,
    share::schema::ObColDesc &col_desc) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    const ObColumnParam *param = NULL;
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

int ObRelativeTable::get_col_desc_by_idx(
    const int64_t idx,
    share::schema::ObColDesc &col_desc) const
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
    } else {
      const ObColumnParam *param = NULL;
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

int ObRelativeTable::get_rowkey_col_desc_by_idx(
    const int64_t idx,
    share::schema::ObColDesc &col_desc) const
{
  int ret = OB_SUCCESS;
  int64_t rowkey_size = 0;
  col_desc.reset();
  if (idx < 0 || idx >= (rowkey_size = get_rowkey_column_num())) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("idx out of range", K(ret), K(rowkey_size));
  } else {
    const ObColumnParam *param = NULL;
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

int ObRelativeTable::get_rowkey_col_id_by_idx(const int64_t idx, uint64_t &col_id) const
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
    } else {
      const ObColumnParam *param = NULL;
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

int ObRelativeTable::get_rowkey_column_ids(ObIArray<ObColDesc> &column_ids) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (OB_FAIL(schema_param_->get_rowkey_column_ids(column_ids))) {
    LOG_WARN("get rowkey column ids from param fail", K(ret));
  }
  return ret;
}

int ObRelativeTable::get_rowkey_column_ids(ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("relative table is invalid", K(ret), KPC(this));
  } else if (OB_FAIL(schema_param_->get_rowkey_column_ids(column_ids))) {
    LOG_WARN("get rowkey column ids from param fail", K(ret));
  }
  return ret;
}

int ObRelativeTable::get_column_data_length(const uint64_t column_id, int32_t &len) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    const ObColumnParam *param = NULL;
    if (NULL == (param = schema_param_->get_column(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column param shouldn't be NULL here", K(ret), K(column_id));
    } else {
      len = param->get_data_length();
    }
  }
  return ret;
}

int ObRelativeTable::is_rowkey_column_id(const uint64_t column_id, bool &is_rowkey) const
{
  int ret = OB_SUCCESS;
  is_rowkey = false;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column id", K(ret), K(column_id));
  } else if (OB_FAIL(schema_param_->is_rowkey_column(column_id, is_rowkey))) {
    LOG_WARN("check is_rowkey fail", K(ret), K(column_id), K(*schema_param_));
  }
  return ret;
}

int ObRelativeTable::is_column_nullable_for_write(const uint64_t column_id,
                                                  bool &is_nullable_for_write) const
{
  int ret = OB_SUCCESS;
  is_nullable_for_write = false;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column id", K(ret), K(column_id));
  } else if (OB_FAIL(schema_param_->is_column_nullable_for_write(column_id, is_nullable_for_write))) {
    LOG_WARN("check is_rowkey fail", K(ret), K(column_id), K(*schema_param_));
  }
  return ret;
}

int ObRelativeTable::is_column_nullable_for_read(const uint64_t column_id,
                                                 bool &is_nullable_for_read) const
{
  int ret = OB_SUCCESS;
  is_nullable_for_read = false;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column id", K(ret), K(column_id));
  } else {
    const ObColumnParam *col = schema_param_->get_column(column_id);
    if (OB_ISNULL(col)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("column schema is null", K(ret), K(column_id));
    } else {
      is_nullable_for_read = col->is_nullable_for_read();
    }
  }
  return ret;
}

int ObRelativeTable::is_nop_default_value(const uint64_t column_id, bool &is_nop) const
{
  int ret = OB_SUCCESS;
  is_nop = false;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (OB_HIDDEN_ROWID_COLUMN_ID == column_id) {
    //rowid column need to compute on fly
    is_nop = true;
  } else {
    const ObColumnParam *param = NULL;
    if (OB_ISNULL(param = schema_param_->get_column(column_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong column id", K(ret), K(column_id), K(*schema_param_));
    } else if (param->get_cur_default_value().is_nop_value()) {
      is_nop = true;
    }
  }
  return ret;
}

int ObRelativeTable::has_udf_column(bool &has_udf) const
{
  int ret = OB_SUCCESS;
  has_udf = false;
  if (OB_FAIL(schema_param_->has_udf_column(has_udf))) {
    LOG_WARN("check has udf column failed", K(ret));
  }
  return ret;
}

int ObRelativeTable::is_hidden_column(const uint64_t column_id, bool &is_hidden) const
{
  int ret = OB_SUCCESS;
  is_hidden = false;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    const ObColumnParam *col = schema_param_->get_column(column_id);
    if (OB_ISNULL(col)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("column schema is null", K(ret), K(column_id));
    } else {
      is_hidden = col->is_hidden();
    }
  }
  return ret;
}

int ObRelativeTable::is_gen_column(const uint64_t column_id, bool &is_gen_col) const
{
  int ret = OB_SUCCESS;
  is_gen_col = false;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    const ObColumnParam *col = schema_param_->get_column(column_id);
    if (OB_ISNULL(col)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("column schema is null", K(ret), K(column_id));
    } else {
      is_gen_col = col->is_gen_col();
    }
  }
  return ret;
}

int64_t ObRelativeTable::get_rowkey_column_num() const
{
  return schema_param_->get_rowkey_column_num();
}

int64_t ObRelativeTable::get_shadow_rowkey_column_num() const
{
  return schema_param_->get_shadow_rowkey_column_num();
}

int64_t ObRelativeTable::get_column_count() const
{
  return schema_param_->get_column_count();
}

int ObRelativeTable::get_fulltext_column(uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    column_id = schema_param_->get_fulltext_col_id();
  }
  return ret;
}

int ObRelativeTable::get_spatial_geo_col_id(uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    column_id = schema_param_->get_spatial_geo_col_id();
  }
  return ret;
}

int ObRelativeTable::get_spatial_cellid_col_id(uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    column_id = schema_param_->get_spatial_cellid_col_id();
  }
  return ret;
}

int ObRelativeTable::get_spatial_mbr_col_id(uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    column_id = schema_param_->get_spatial_mbr_col_id();
  }
  return ret;
}

int ObRelativeTable::get_index_name(ObString &index_name) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (OB_FAIL(schema_param_->get_index_name(index_name))) {
    LOG_WARN("get index name from param fail", K(ret));
  }
  return ret;
}

int ObRelativeTable::get_primary_key_name(ObString &pk_name) const
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else {
    pk_name = schema_param_->get_pk_name();
  }

  return ret;
}

bool ObRelativeTable::is_index_table() const
{
  return schema_param_->is_index_table();
}

bool ObRelativeTable::is_storage_index_table() const
{
  return schema_param_->is_storage_index_table();
}

bool ObRelativeTable::can_read_index() const
{
  return schema_param_->can_read_index();
}

bool ObRelativeTable::is_unique_index() const
{
  return schema_param_->is_unique_index();
}

bool ObRelativeTable::is_domain_index() const
{
  return schema_param_->is_domain_index();
}

bool ObRelativeTable::is_lob_meta_table() const
{
  return schema_param_->is_lob_meta_table();
}

bool ObRelativeTable::is_spatial_index() const
{
  return schema_param_->is_spatial_index();
}

int ObRelativeTable::check_rowkey_in_column_ids(
    const common::ObIArray<uint64_t> &column_ids,
    const bool has_other_column) const
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
    } else if ((has_other_column && column_ids.count() < count)
                || (!has_other_column && column_ids.count() != count)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("rowkey count mismatch", K(has_other_column), K(count), K(column_ids.count()));
    } else {
      const ObColumnParam * param = NULL;
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
  return ret;
}

int ObRelativeTable::build_index_row(
    const ObNewRow &table_row,
    const share::schema::ColumnMap &col_map,
    const bool only_rowkey,
    ObNewRow &index_row,
    bool &null_idx_val,
    ObIArray<ObColDesc> *idx_columns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRelativeTable hasn't been inited.", K(ret));
  } else if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is invalid", K(ret), K(*this));
  } else if (!is_storage_index_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relative table is not storage index table", K(ret), K(*this));
  } else if (table_row.is_invalid() || !col_map.is_inited() || index_row.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_row can not be empty",
                "table_row", to_cstring(table_row),
                "index_row", to_cstring(index_row));
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
        LOG_WARN("get rowkey column description fail", K(ret), K(i));
      } else if (OB_FAIL(set_index_value(table_row,
                                         col_map,
                                         col_desc,
                                         rowkey_size,
                                         index_row,
                                         idx_columns))) {
        LOG_WARN("failed to set index value");
      }
    }

    if (OB_SUCC(ret) && !only_rowkey) {
      const int64_t column_cnt = get_column_count();
      for (int64_t i = rowkey_size; OB_SUCC(ret) && i < column_cnt; ++i) {
        const ObColumnParam *param = NULL;
        if (NULL == (param = schema_param_->get_column_by_idx(i))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("wrong column id", K(ret), K(i));
        } else if (OB_HIDDEN_ROWID_COLUMN_ID == param->get_column_id()
                    || schema_param_->is_depend_column(param->get_column_id())) {
          // skip following cases:
          // - no need to fill row_id column in memtable
          // - materialized view may contains the columns that do not exist in main table
        } else {
          col_desc.reset();
          col_desc.col_id_ = param->get_column_id();
          col_desc.col_type_ = param->get_meta_type();
          col_desc.col_order_ = param->get_column_order();
          if (OB_FAIL(set_index_value(table_row,
                                      col_map,
                                      col_desc,
                                      rowkey_size,
                                      index_row,
                                      idx_columns))) {
            LOG_WARN("failed to set index value");
          }
        }
      }
    }

    // set shadow columns correctly
    if (OB_SUCC(ret) && is_unique_index()) {
      const int64_t shadow_column_cnt = get_shadow_rowkey_column_num();
      const int64_t unique_key_cnt = get_rowkey_column_num() - shadow_column_cnt;
      if (OB_FAIL(ObUniqueIndexRowTransformerV2<ObNewRow>::convert_to_unique_index_row(
          static_cast<ObCompatibilityMode>(THIS_WORKER.get_compatibility_mode()),
          unique_key_cnt, shadow_column_cnt, nullptr/*projector*/, index_row, null_idx_val))) {
        LOG_WARN("fail to convert unique index row", K(ret));
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObRelativeTable)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(allow_not_ready),
       K_(tablet_id),
       KPC_(schema_param),
       K_(tablet_iter));
  J_OBJ_END();
  return pos;
}

int ObRelativeTable::set_index_value(
    const ObNewRow &table_row,
    const share::schema::ColumnMap &col_map,
    const ObColDesc &col_desc,
    const int64_t rowkey_size,
    ObNewRow &index_row,
    ObIArray<share::schema::ObColDesc> *idx_columns)
{
  int ret = OB_SUCCESS;
  int32_t idx = -1;
  uint64_t id = col_desc.col_id_ > OB_MIN_SHADOW_COLUMN_ID ?
                col_desc.col_id_ - OB_MIN_SHADOW_COLUMN_ID :
                col_desc.col_id_;
  if (table_row.is_invalid() || !col_map.is_inited() || rowkey_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(table_row), K(rowkey_size), K(ret));
  } else {
    if (OB_FAIL(col_map.get(id, idx)) || idx < 0) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to get column index",
                  "column_id", id, "index", idx);
    } else if (idx >= table_row.count_) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not get row value",
                  "table_row", to_cstring(table_row), "column_id", id, "index", idx);
    } else {
      *(index_row.cells_ + index_row.count_++) = table_row.cells_[idx];
    }
  }
  if (OB_SUCC(ret)) {
    if (idx_columns) {
      if (OB_FAIL(idx_columns->push_back(col_desc))) {
        LOG_WARN("failed to add column id",
                    "column_id", col_desc.col_id_);
      }
    }
  }
  return ret;
}

}//namespace oceanbase
}//namespace storage
