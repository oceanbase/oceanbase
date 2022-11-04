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

#include "ob_information_partitions_table.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/schema/ob_part_mgr_util.h" // ObPartitionSchemaIter

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace observer
{

ObInfoSchemaPartitionsTable::ObInfoSchemaPartitionsTable()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID)
{
}

ObInfoSchemaPartitionsTable::~ObInfoSchemaPartitionsTable()
{
  reset();
}

void ObInfoSchemaPartitionsTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}

int ObInfoSchemaPartitionsTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema manager is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "tenant_id is invalid", K(ret), K_(tenant_id));
  }

  if (OB_SUCCESS == ret && !start_to_read_) {
    ObSArray<const ObDatabaseSchema *> database_schemas;
    if (OB_FAIL(schema_guard_->get_database_schemas_in_tenant(tenant_id_,
                                                                database_schemas))) {
      SERVER_LOG(WARN, "failed to get database schema of tenant", K_(tenant_id));
    } else {
      const int64_t col_count = output_column_ids_.count();
      ObObj *cells = NULL;
      if (col_count < 0 || col_count > PARTITION_COLUMN_COUNT) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
      }
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (NULL == (cells = cur_row_.cells_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < database_schemas.count(); ++i) {
          const ObDatabaseSchema* database_schema = database_schemas.at(i);
          if (OB_ISNULL(database_schema)) {
            ret = OB_ERR_BAD_DATABASE;
            SERVER_LOG(WARN, "database not exist", K(ret));
          } else if (database_schema->is_in_recyclebin()
                     || ObString(OB_RECYCLEBIN_SCHEMA_NAME) == database_schema->get_database_name_str()
                     || ObString(OB_PUBLIC_SCHEMA_NAME) == database_schema->get_database_name_str()) {
            continue;
          } else if (OB_FAIL(add_partitions(*database_schema,cells, output_column_ids_.count()))) {
            SERVER_LOG(WARN, "failed to add table constraint of database schema!",
                       K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
      }
    }
  }

  if (OB_SUCCESS == ret && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObInfoSchemaPartitionsTable::add_partitions(const ObDatabaseSchema &database_schema,
                                                ObObj *cells,
                                                const int64_t col_count)
{
  int ret = OB_SUCCESS;
  ObSArray<const ObSimpleTableSchemaV2 *> table_schemas;
  bool priv_passed = true;
  if (OB_ISNULL(schema_guard_) || OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema manager or cells should not be null", K(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schemas_in_database(tenant_id_,
                                                                    database_schema.get_database_id(),
                                                                    table_schemas))) {
    SERVER_LOG(WARN, "failed to get table schema in database", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        SERVER_LOG(WARN, "table schema not exist", K(ret));
      } else if (PARTITION_LEVEL_ZERO == table_schema->get_part_level()) {
        continue;
      } else if (table_schema->is_index_table() || table_schema->is_vir_table() || table_schema->is_aux_lob_table()) {
        continue;
      } else if (OB_FAIL(check_priv("table_acc",
                                 database_schema.get_database_name_str(),
                                 table_schema->get_table_name_str(),
                                 tenant_id_,
                                 priv_passed))) {
        SERVER_LOG(WARN, "failed to check priv", K(ret), K(database_schema.get_database_name_str()),
        K(table_schema->get_table_name_str()));
      } else if (!priv_passed) {
        continue;
      } else if (OB_FAIL(add_partitions(*table_schema,
                                        database_schema.get_database_name_str(),
                                        cells,
                                        col_count))){
        SERVER_LOG(WARN, "failed to add table constraint of table schema",
                   "table_schema", *table_schema, K(ret));
      }
    }
  }
  return ret;
}

int ObInfoSchemaPartitionsTable::add_partitions(const ObSimpleTableSchemaV2 &table_schema,
                                                const ObString &database_name,
                                                ObObj *cells,
                                                const int64_t col_count)
{
  int ret = OB_SUCCESS;
  ObPartitionLevel part_level = table_schema.get_part_level();
  ObPartitionSchemaIter::Info part_info;
  bool is_oracle_mode = false;
  ObPartitionSchemaIter part_iter(table_schema,
                                  ObCheckPartitionMode::CHECK_PARTITION_MODE_NORMAL);
  const ObTablespaceSchema *tablespace = NULL;
  uint64_t tablespace_id = table_schema.get_tablespace_id();
  if (OB_ISNULL(cells) || col_count > reserved_column_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells should not be null", K(ret));
  } else if (PARTITION_LEVEL_MAX == part_level) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "get unexpected part level", K(ret));
  } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    SERVER_LOG(WARN, "fail to check oracle mode", KR(ret), K(table_schema));
  } else if (OB_INVALID_ID != tablespace_id
             && OB_FAIL(schema_guard_->get_tablespace_schema(tenant_id_, tablespace_id, tablespace))) {
    SERVER_LOG(WARN, "fail to get tablespace schema", KR(ret), K_(tenant_id), K(tablespace_id));
  }
  while (OB_SUCC(ret) && OB_SUCC(part_iter.next_partition_info(part_info))) {
    int64_t cell_idx = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
      uint64_t col_id = output_column_ids_.at(j);
      switch(col_id) {
        case TABLE_CATALOG: {
          cells[cell_idx].set_varchar(ObString("def"));
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TABLE_SCHEMA: {
          cells[cell_idx].set_varchar(database_name);
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TABLE_NAME: {
          cells[cell_idx].set_varchar(table_schema.get_table_name_str());
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case PARTITION_NAME: {
          if (PARTITION_LEVEL_ZERO == part_level) {
            cells[cell_idx].set_null();
          } else if (OB_ISNULL(part_info.part_)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "partition is null", KR(ret), K(part_info));
          } else {
            cells[cell_idx].set_varchar(part_info.part_->get_part_name());
            cells[cell_idx].set_collation_type(
                        ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case SUBPARTITION_NAME: {
          if (PARTITION_LEVEL_ZERO == part_level || PARTITION_LEVEL_ONE == part_level) {
            cells[cell_idx].set_null();
          } else if (OB_ISNULL(part_info.partition_)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "partition is null", KR(ret), K(part_info));
          } else {
            cells[cell_idx].set_varchar(part_info.partition_->get_part_name());
            cells[cell_idx].set_collation_type(
                        ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case PARTITION_ORDINAL_POSITION: {
          if (PARTITION_LEVEL_ZERO == part_level) {
            cells[cell_idx].set_null();
          } else {
            cells[cell_idx].set_uint64(part_info.part_idx_ + 1);
          }
          break;
        }
        case SUBPARTITION_ORDINAL_POSITION: {
          if (PARTITION_LEVEL_TWO == part_level) {
            cells[cell_idx].set_uint64(part_info.subpart_idx_ + 1);
          } else {
            cells[cell_idx].set_null();
          }
          break;
        }
        case PARTITION_METHOD: {
          //TODO
          ObPartitionFuncType func_type = table_schema.get_part_option().get_part_func_type();
          switch (func_type){
            case PARTITION_FUNC_TYPE_HASH: {
              cells[cell_idx].set_varchar("HASH");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case PARTITION_FUNC_TYPE_KEY :
            case PARTITION_FUNC_TYPE_KEY_IMPLICIT: {
              cells[cell_idx].set_varchar("KEY");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case PARTITION_FUNC_TYPE_RANGE: {
              cells[cell_idx].set_varchar("RANGE");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case PARTITION_FUNC_TYPE_RANGE_COLUMNS: {
              cells[cell_idx].set_varchar("RANGE COLUMNS");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case PARTITION_FUNC_TYPE_LIST: {
              cells[cell_idx].set_varchar("LIST");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case PARTITION_FUNC_TYPE_LIST_COLUMNS: {
              cells[cell_idx].set_varchar("LIST COLUMNS");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            default: {
              cells[cell_idx].set_varchar("UNKNOWN");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
          }
          break;
        }
        case SUBPARTITION_METHOD: {
          if (ObPartitionLevel::PARTITION_LEVEL_TWO == part_level) {
            ObPartitionFuncType func_type = table_schema.get_sub_part_option().get_part_func_type();
            switch (func_type){
              case PARTITION_FUNC_TYPE_HASH: {
                cells[cell_idx].set_varchar("HASH");
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              case PARTITION_FUNC_TYPE_KEY :
              case PARTITION_FUNC_TYPE_KEY_IMPLICIT: {
                cells[cell_idx].set_varchar("KEY");
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              case PARTITION_FUNC_TYPE_RANGE: {
                cells[cell_idx].set_varchar("RANGE");
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              case PARTITION_FUNC_TYPE_RANGE_COLUMNS: {
                cells[cell_idx].set_varchar("RANGE COLUMNS");
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              case PARTITION_FUNC_TYPE_LIST: {
                cells[cell_idx].set_varchar("LIST");
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              case PARTITION_FUNC_TYPE_LIST_COLUMNS: {
                cells[cell_idx].set_varchar("LIST COLUMNS");
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              default: {
                cells[cell_idx].set_varchar("UNKNOWN");
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
            }
          } else {
            cells[cell_idx].set_null();
          }
          break;
        }
        case PARTITION_EXPRESSION: {
          cells[cell_idx].set_varchar(table_schema.get_part_option().get_part_func_expr_str());
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SUBPARTITION_EXPRESSION: {
          if (PARTITION_LEVEL_TWO == part_level) {
            cells[cell_idx].set_varchar(table_schema.get_sub_part_option().get_part_func_expr_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cells[cell_idx].set_null();
          }
          break;
        }
        case PARTITION_DESCRIPTION: {
          ObString desc;
          if (table_schema.is_range_part()) {
            if (OB_FAIL(gen_high_bound_val_str(is_oracle_mode, part_info.part_, desc))) {
              SERVER_LOG(WARN, "fail to generate PARTITION_DESCRIPTION", K(ret), K(part_info));
            } else {
              cells[cell_idx].set_varchar(desc);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
          } else if (table_schema.is_list_part()) {
            if (OB_FAIL(gen_list_bound_val_str(is_oracle_mode, part_info.part_, desc))) {
              SERVER_LOG(WARN, "fail to generate PARTITION_DESCRIPTION", K(ret), K(part_info));
            } else {
              cells[cell_idx].set_varchar(desc);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
          } else {
            cells[cell_idx].set_null();
          }
          break;
        }
        case TABLE_ROWS: {
          // TODO no row info now
          // cells[cell_idx].set_uint64(0);
          cells[cell_idx].set_null();
          break;
        }
        case AVG_ROW_LENGTH: {
          // TODO no row info now
          // cells[cell_idx].set_uint64(0);
          cells[cell_idx].set_null();
          break;
        }
        case DATA_LENGTH: {
          // TODO no row info now
          // cells[cell_idx].set_uint64(0);
          cells[cell_idx].set_null();
          break;
        }
        case MAX_DATA_LENGTH: {
          // TODO no row info now
          // cells[cell_idx].set_uint64(0);
          cells[cell_idx].set_null();
          break;
        }
        case INDEX_LENGTH: {
          // TODO no row info now
          // cells[cell_idx].set_uint64(0);
          cells[cell_idx].set_null();
          break;
        }
        case DATA_FREE: {
          // TODO no row info now
          // cells[cell_idx].set_uint64(0);
          cells[cell_idx].set_null();
          break;
        }
        case CREATE_TIME: {
          cells[cell_idx].set_null();
          break;
        }
        case UPDATE_TIME: {
          cells[cell_idx].set_null();
          break;
        }
        case CHECK_TIME: {
          cells[cell_idx].set_null();
          break;
        }
        case CHECKSUM: {
          cells[cell_idx].set_null();
          break;
        }
        case PARTITION_COMMENT: {
          cells[cell_idx].set_null();
          break;
        }
        case NODEGROUP: {
          cells[cell_idx].set_varchar("default");
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TABLESPACE_NAME: {
          if (OB_NOT_NULL(tablespace)) {
            cells[cell_idx].set_varchar(tablespace->get_tablespace_name());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cells[cell_idx].set_null();
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                     K(output_column_ids_), K(col_id));
          break;
        }
      }
      if (OB_SUCC(ret)) {
          cell_idx++;
      }
    }
    if (FAILEDx(scanner_.add_row(cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
    }

  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else {
    ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
    SERVER_LOG(WARN, "iter part failed", KR(ret), K(table_schema));
  }
  return ret;
}

int ObInfoSchemaPartitionsTable::gen_high_bound_val_str(
    const bool is_oracle_mode,
    const share::schema::ObBasePartition *part,
    common::ObString &val_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "partition is null", K(ret), KP(part));
  } else {
    char *high_bound_val = NULL;
    int64_t pos = 0;
    if (OB_ISNULL(high_bound_val = static_cast<char *>(allocator_->alloc(OB_MAX_B_HIGH_BOUND_VAL_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "high_bound_val is null", K(ret), K(high_bound_val));
    } else {
      MEMSET(high_bound_val, 0, OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
      ObTimeZoneInfo tz_info;
      tz_info.set_offset(0);
      if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id_, tz_info.get_tz_map_wrap()))) {
        SERVER_LOG(WARN, "get tenant timezone map failed", K(ret), K(tenant_id_));
      } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
          is_oracle_mode, part->get_high_bound_val(), high_bound_val,
          OB_MAX_B_HIGH_BOUND_VAL_LENGTH, pos, false, &tz_info))) {
        SERVER_LOG(WARN, "Failed to convert rowkey to sql text", K(tz_info), K(ret));
      } else {
        val_str.assign_ptr(high_bound_val, pos);
      }
    }
  }
  return ret;
}
int ObInfoSchemaPartitionsTable::gen_list_bound_val_str(
    const bool is_oracle_mode,
    const share::schema::ObBasePartition *part,
    common::ObString &val_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "partition is null", K(ret), KP(part));
  } else {
    char *list_val = NULL;
    int64_t pos = 0;
    if (OB_ISNULL(list_val = static_cast<char *>(allocator_->alloc(OB_MAX_B_HIGH_BOUND_VAL_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "list_val is null", K(ret), K(list_val));
    } else {
      MEMSET(list_val, 0, OB_MAX_B_PARTITION_EXPR_LENGTH);
      ObTimeZoneInfo tz_info;
      tz_info.set_offset(0);
      if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id_, tz_info.get_tz_map_wrap()))) {
        SERVER_LOG(WARN, "get tenant timezone map failed", K(ret), K(tenant_id_));
      } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_sql_literal(
          is_oracle_mode, part->get_list_row_values(), list_val,
          OB_MAX_B_PARTITION_EXPR_LENGTH, pos, false, &tz_info))) {
        SERVER_LOG(WARN, "Failed to convert rowkey to sql text", K(tz_info), K(ret));
      } else {
        val_str.assign_ptr(list_val, pos);
      }
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
