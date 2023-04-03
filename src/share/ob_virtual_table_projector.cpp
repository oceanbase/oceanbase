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

#define USING_LOG_PREFIX COMMON
#include "share/ob_virtual_table_projector.h"
#include "share/schema/ob_multi_version_schema_service.h"
namespace oceanbase
{
namespace common
{
int ObVirtualTableProjector::project_row(const ObIArray<Column> &columns, ObNewRow &row, const bool full_columns)
{
  int ret = OB_SUCCESS;
  if (columns.count() < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("columns is empty", K(ret));
  } else if (output_column_ids_.count() > row.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column num not match", KR(ret), K(row), K_(output_column_ids));
  } else if (OB_ISNULL(row.cells_) && row.count_ != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row count is not valid", KR(ret), K(row));
  } else if (OB_ISNULL(row.cells_)) {
    // just skip, won't project row
    // column not specified, eg: count(*)
  } else {
    int64_t cell_idx = 0;
    FOREACH_CNT_X(column_id, output_column_ids_, OB_SUCCESS == ret) {
      bool find = false;
      ObObj column_value = ObObj::make_nop_obj();
      FOREACH_CNT_X(column, columns, !find) {
        if (*column_id == column->column_id_) {
          column_value = column->column_value_;
          find = true;
        }
      }
      if (!find && full_columns) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("column not exist", "column_id", *column_id, K(ret));
      } else {
        row.cells_[cell_idx] = column_value;
        ++cell_idx;
      }
    }
  }
  return ret;
}

int ObVirtualTableProjector::check_column_exist(const share::schema::ObTableSchema *table,
                                                const char* column_name,
                                                bool &exist) const
{
  int ret = OB_SUCCESS;
  const share::schema::ObColumnSchemaV2 *column_schema = NULL;
  exist = false;
  if (OB_ISNULL(table) || OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table), K(column_name));
  } else if (OB_ISNULL(column_schema = table->get_column_schema(column_name))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("fail to get column schema", KR(ret), K(column_name));
  } else {
    uint64_t table_column_id = column_schema->get_column_id();
    FOREACH_CNT_X(column_id, output_column_ids_, OB_SUCCESS == ret) {
      if (*column_id == table_column_id) {
        exist = true;
        break;
      }
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////
ObSimpleVirtualTableIterator::ObSimpleVirtualTableIterator(uint64_t tenant_id, uint64_t table_id)
    :tenant_id_(tenant_id),
     table_id_(table_id),
     schema_guard_(share::schema::ObSchemaMgrItem::MOD_VIRTUAL_TABLE),
     schema_service_(NULL),
     table_schema_(NULL)
{}

int ObSimpleVirtualTableIterator::init(share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (schema_service_ != NULL) {
    ret = OB_INIT_TWICE;
    LOG_WARN("schema_service_ not NULL", K(ret));
  } else if (NULL == schema_service) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_service is NULL", K(ret));
  } else {
    schema_service_ = schema_service;
  }
  return ret;
}

int ObSimpleVirtualTableIterator::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema_service_ not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(get_table_schema(tenant_id_, table_id_))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_FAIL(init_all_data())) {
    LOG_WARN("fail to init all data", K(ret));
  }
  return ret;
}

int ObSimpleVirtualTableIterator::get_table_schema(uint64_t tenant_id, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_guard_.get_table_schema(tenant_id, table_id, table_schema_))) {
    LOG_WARN("fail to get table schema", K(tenant_id), K(table_id), K(ret));
  } else if (NULL == table_schema_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(tenant_id), K(table_id));
  } else {} // no more to do
  return ret;
}

int ObSimpleVirtualTableIterator::inner_get_next_row(
    common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  columns_.reuse();
  if (OB_FAIL(get_next_full_row(table_schema_, columns_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get full row", K(ret));
    }
  } else if (OB_FAIL(project_row(columns_, cur_row_))) {
    LOG_WARN("fail to project row", K(columns_), K(ret));
  } else {
    row = &cur_row_;
  }
  return ret;
}


}//end namespace common
}//end namespace oceanbase
