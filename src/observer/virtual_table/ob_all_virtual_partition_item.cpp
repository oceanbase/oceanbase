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

#include "observer/virtual_table/ob_all_virtual_partition_item.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;

namespace observer {

ObAllVirtualPartitionItem::ObAllVirtualPartitionItem()
    : ObVirtualTableScannerIterator(),
      last_tenant_idx_(-1),
      last_table_idx_(-1),
      part_iter_(),
      last_item_(),
      has_more_(false)
{
  memset(part_high_bound_buf_, 0, BOUND_BUF_LENGTH);
  memset(subpart_high_bound_buf_, 0, BOUND_BUF_LENGTH);
}

ObAllVirtualPartitionItem::~ObAllVirtualPartitionItem()
{
  reset();
}

void ObAllVirtualPartitionItem::reset()
{
  ObVirtualTableScannerIterator::reset();
  has_more_ = false;
  last_tenant_idx_ = -1;
  last_table_idx_ = -1;
  last_item_.reset();
  memset(part_high_bound_buf_, 0, BOUND_BUF_LENGTH);
  memset(subpart_high_bound_buf_, 0, BOUND_BUF_LENGTH);
}

int ObAllVirtualPartitionItem::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or schema_guard_ is NULL", K(ret));
  } else {
    if (!start_to_read_) {
      ObArray<uint64_t> tenant_ids;
      ObArray<uint64_t> table_ids;
      const ObSimpleTableSchemaV2* table_schema = NULL;
      if (OB_FAIL(schema_guard_->get_tenant_ids(tenant_ids))) {
        SERVER_LOG(WARN, "fail to get tenant ids", K(ret));
      } else {
        int64_t i = 0;
        if (last_tenant_idx_ != -1) {
          i = last_tenant_idx_;
          last_tenant_idx_ = -1;
        }
        // all tenants
        for (; OB_SUCC(ret) && i < tenant_ids.count() && !has_more_; ++i) {
          const uint64_t tenant_id = tenant_ids.at(i);
          if (OB_INVALID_ID == tenant_id) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "tenant id is invalid", K(ret));
          } else {
            table_ids.reset();
            if (OB_FAIL(schema_guard_->get_table_ids_in_tenant(tenant_id, table_ids))) {
              SERVER_LOG(WARN, "fail to get table ids in tenant", K(ret), K(tenant_id));
            } else {
              int64_t j = 0;
              if (last_table_idx_ != -1) {
                j = last_table_idx_;
                last_table_idx_ = -1;
              }
              // all tables
              for (; OB_SUCCESS == ret && j < table_ids.count() && !has_more_; ++j) {
                const uint64_t table_id = table_ids.at(j);
                if (OB_FAIL(schema_guard_->get_table_schema(table_id, table_schema))) {
                  SERVER_LOG(WARN, "get table schema failed", K(table_id), K(ret));
                } else if (OB_ISNULL(table_schema)) {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "table_schema is NULL", K(table_id), K(ret));
                } else if (table_schema->is_index_table() || is_virtual_table(table_id) || is_sys_view(table_id)) {
                  continue;  // skip index table, virtual table and system views
                } else {
                  const char* tenant_name = NULL;
                  const ObTenantSchema* tenant_schema = NULL;
                  schema_guard_->get_tenant_info(tenant_id, tenant_schema);
                  if (OB_FAIL(schema_guard_->get_tenant_info(tenant_id, tenant_schema))) {
                    SERVER_LOG(WARN, "failed to get tenant_schema", K(ret));
                  } else if (OB_ISNULL(tenant_schema)) {
                    ret = OB_ERR_UNEXPECTED;
                    SERVER_LOG(WARN, "tenant_schema is null", K(ret));
                  } else if (FALSE_IT(tenant_name = tenant_schema->get_tenant_name())) {
                    // will not reach here
                  } else {
                    ObPartitionItem item;
                    if (last_item_.partition_idx_ != -1) {
                      if (OB_FAIL(fill_row_cells(*table_schema, item, tenant_name))) {
                        SERVER_LOG(WARN, "failed fill row cell");
                      } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
                        SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
                      } else {
                        last_item_.reset();
                      }
                    } else {
                      part_iter_.init(*table_schema);
                    }

                    while (OB_SUCC(ret) && !has_more_ && OB_SUCC(part_iter_.next(item))) {
                      if (OB_FAIL(fill_row_cells(*table_schema, item, tenant_name))) {
                        SERVER_LOG(WARN, "failed fill row cell");
                      } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
                        if (OB_SIZE_OVERFLOW == ret) {
                          SERVER_LOG(INFO, "size over flow to add row", K(table_id), K(ret));
                          last_tenant_idx_ = i;
                          last_table_idx_ = j;
                          last_item_ = item;
                          has_more_ = true;
                          ret = OB_SUCCESS;
                        } else {
                          SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
                        }
                      }
                    }
                    if (OB_ITER_END == ret) {
                      ret = OB_SUCCESS;
                    }
                  }
                }
              }  // table ids
            }
          }
        }  // tenant ids
      }
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }

    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        } else if (has_more_) {
          SERVER_LOG(INFO, "continue to fetch reset rows", K(ret));
          has_more_ = false;
          start_to_read_ = false;
          scanner_.reset();
          ret = inner_get_next_row(row);
        }
      } else {
        row = &cur_row_;
      }
    }
  }

  return ret;
}

int ObAllVirtualPartitionItem::fill_row_cells(
    const ObSimpleTableSchemaV2& table_schema, share::schema::ObPartitionItem& item, const char* tenant_name)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or session_ is NULL", K(ret), K(allocator_), K(session_));
  } else if (OB_ISNULL(tenant_name)) {
    SERVER_LOG(WARN, "pointer of tenant_name is NULL", K(ret));
  } else {
    ObObj* cells = NULL;
    const int64_t col_count = output_column_ids_.count();
    if (OB_ISNULL(cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
    } else if (OB_UNLIKELY(col_count < 1 || col_count > COLUMN_COUNT)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
    } else if (OB_UNLIKELY(col_count > reserved_column_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cells count error", K(ret), K(col_count), K(reserved_column_cnt_));
    } else {
      const ObDataTypeCastParams dtc_params = sql::ObBasicSessionInfo::create_dtc_params(session_);
      ObCastCtx cast_ctx(allocator_, &dtc_params, CM_NONE, ObCharset::get_system_collation());
      ObObj casted_cell;
      ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
      uint64_t cell_idx = 0;
      const int64_t col_count = output_column_ids_.count();
      for (int64_t k = 0; OB_SUCC(ret) && k < col_count; ++k) {
        uint64_t col_id = output_column_ids_.at(k);
        switch (col_id) {
          case TENANT_ID: {
            cur_row_.cells_[cell_idx].set_int(table_schema.get_tenant_id());
            break;
          }
          case TENANT_NAME: {
            ObString tmp_tenant_name(
                OB_MAX_SYS_PARAM_NAME_LENGTH, static_cast<int32_t>(strlen(tenant_name)), tenant_name);
            cur_row_.cells_[cell_idx].set_varchar(tmp_tenant_name);
            cur_row_.cells_[cell_idx].set_collation_type(coll_type);
            break;
          }
          case TABLE_ID: {
            cur_row_.cells_[cell_idx].set_int(table_schema.get_table_id());
            break;
          }
          case TABLE_NAME: {
            cur_row_.cells_[cell_idx].set_varchar(table_schema.get_table_name_str());
            cur_row_.cells_[cell_idx].set_collation_type(coll_type);
            break;
          }
          case PARTITION_LEVEL: {
            cur_row_.cells_[cell_idx].set_int(table_schema.get_part_level());
            break;
          }
          case PARTITION_NUM: {
            cur_row_.cells_[cell_idx].set_int(item.partition_num_);
            break;
          }
          case PARTITION_IDX: {
            cur_row_.cells_[cell_idx].set_int(item.partition_idx_);
            break;
          }
          case PARTITION_ID: {
            cur_row_.cells_[cell_idx].set_int(item.partition_id_);
            break;
          }
          case PART_FUNC_TYPE: {
            ObString func_type;
            if (OB_FAIL(get_part_type_str(table_schema.get_part_option().get_part_func_type(), func_type))) {
              SERVER_LOG(WARN, "failed to get part type string", K(ret));
            } else {
              cur_row_.cells_[cell_idx].set_varchar(func_type);
              cur_row_.cells_[cell_idx].set_collation_type(coll_type);
            }
            break;
          }
          case PART_NUM: {
            cur_row_.cells_[cell_idx].set_int(item.part_num_);
            break;
          }
          case PART_NAME: {
            if (OB_NOT_NULL(item.part_name_)) {
              cur_row_.cells_[cell_idx].set_varchar(*item.part_name_);
              cur_row_.cells_[cell_idx].set_collation_type(coll_type);
            } else {
              cur_row_.cells_[cell_idx].reset();
            }
            break;
          }
          case PART_IDX: {
            cur_row_.cells_[cell_idx].set_int(item.part_idx_);
            break;
          }
          case PART_ID: {
            cur_row_.cells_[cell_idx].set_int(item.part_id_);
            break;
          }
          case PART_HIGH_BOUND: {
            int64_t pos = 0;
            if (table_schema.is_range_part()) {
              if (OB_NOT_NULL(item.part_high_bound_)) {
                if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(*(item.part_high_bound_),
                        part_high_bound_buf_,
                        BOUND_BUF_LENGTH,
                        pos,
                        false,
                        TZ_INFO(session_)))) {
                  SERVER_LOG(WARN, "failed to convert rowkey to sql literal", K(ret));
                } else {
                  ObString part_high_bound_val(
                      static_cast<int32_t>(strlen(part_high_bound_buf_)), part_high_bound_buf_);
                  cur_row_.cells_[cell_idx].set_varchar(part_high_bound_val);
                  cur_row_.cells_[cell_idx].set_collation_type(coll_type);
                }
              } else {
                cur_row_.cells_[cell_idx].reset();
              }
            } else {
              cur_row_.cells_[cell_idx].reset();
            }
            break;
          }
          case SUBPART_FUNC_TYPE: {
            ObString subpart_func_type;
            if (PARTITION_LEVEL_TWO != table_schema.get_part_level()) {
              cur_row_.cells_[cell_idx].reset();
            } else if (OB_FAIL(get_part_type_str(
                           table_schema.get_sub_part_option().get_part_func_type(), subpart_func_type))) {
              SERVER_LOG(WARN, "failed to get subpart type string", K(ret));
            } else {
              cur_row_.cells_[cell_idx].set_varchar(subpart_func_type);
              cur_row_.cells_[cell_idx].set_collation_type(coll_type);
            }
            break;
          }
          case SUBPART_NUM: {
            cur_row_.cells_[cell_idx].set_int(item.subpart_num_);
            break;
          }
          case SUBPART_NAME: {
            if (OB_NOT_NULL(item.subpart_name_)) {
              cur_row_.cells_[cell_idx].set_varchar(*item.subpart_name_);
              cur_row_.cells_[cell_idx].set_collation_type(coll_type);
            } else {
              cur_row_.cells_[cell_idx].reset();
            }
            break;
          }
          case SUBPART_IDX: {
            cur_row_.cells_[cell_idx].set_int(item.subpart_idx_);
            break;
          }
          case SUBPART_ID: {
            cur_row_.cells_[cell_idx].set_int(item.subpart_id_);
            break;
          }
          case SUBPART_HIGH_BOUND: {
            int64_t pos = 0;
            if (table_schema.is_range_subpart()) {
              if (OB_NOT_NULL(item.subpart_high_bound_)) {
                if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(*(item.subpart_high_bound_),
                        subpart_high_bound_buf_,
                        BOUND_BUF_LENGTH,
                        pos,
                        false,
                        TZ_INFO(session_)))) {
                  SERVER_LOG(WARN, "failed to convert rowkey to sql literal", K(ret));
                } else {
                  ObString subpart_high_bound_val(
                      static_cast<int32_t>(strlen(subpart_high_bound_buf_)), subpart_high_bound_buf_);
                  cur_row_.cells_[cell_idx].set_varchar(subpart_high_bound_val);
                  cur_row_.cells_[cell_idx].set_collation_type(coll_type);
                }
              } else {
                cur_row_.cells_[cell_idx].reset();
              }
            } else {
              cur_row_.cells_[cell_idx].reset();
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          ++cell_idx;
        }
      }
    }
  }

  return ret;
}

}  // namespace observer
}  // namespace oceanbase
