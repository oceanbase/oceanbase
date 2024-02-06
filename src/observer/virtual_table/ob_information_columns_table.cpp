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

#include "observer/virtual_table/ob_information_columns_table.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/virtual_table/ob_table_columns.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log.h"
#include "lib/geo/ob_geo_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/dml/ob_select_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;

namespace observer
{

ObInfoSchemaColumnsTable::ObInfoSchemaColumnsTable() :
    ObVirtualTableScannerIterator(),
    tenant_id_(OB_INVALID_ID),
    last_schema_idx_(-1),
    last_table_idx_(-1),
    last_column_idx_(-1),
    has_more_(false),
    data_type_str_(NULL),
    column_type_str_(NULL),
    column_type_str_len_(-1),
    is_filter_db_(false),
    last_filter_table_idx_(-1),
    last_filter_column_idx_(-1),
    database_schema_array_(),
    filter_table_schema_array_(),
    mem_context_(nullptr),
    iter_cnt_(0)
{
}

ObInfoSchemaColumnsTable::~ObInfoSchemaColumnsTable()
{
  reset();
}

void ObInfoSchemaColumnsTable::reset()
{
  ObVirtualTableScannerIterator::reset();
  tenant_id_ = OB_INVALID_ID;
  if (OB_LIKELY(NULL != mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
  iter_cnt_ = 0;
}

int ObInfoSchemaColumnsTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or schema_guard_ is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "tenant id is invalid_id", K(ret), K(tenant_id_));
  } else if (OB_FAIL(init_mem_context())) {
    SERVER_LOG(WARN, "failed to init mem context", K(ret));
  } else {
    if (!start_to_read_) {
      void *tmp_ptr = NULL;
      // 如果无db filter(is_filter_db_: false)，
      // check_database_table_filter里面最多一次循环: start_key=MIN,MIN, end_key=MAX,MAX
      if (!is_filter_db_ && OB_FAIL(check_database_table_filter())) {
        SERVER_LOG(WARN, "fail to check database and table filter", K(ret));
      // 当指定了db_name后，无需直接遍历租户所有的database_schema_array
      } else if (!is_filter_db_ && OB_FAIL(schema_guard_->get_database_schemas_in_tenant(
          tenant_id_, database_schema_array_))) {
        SERVER_LOG(WARN, "fail to get database schemas in tenant", K(ret),
                   K(tenant_id_));
      } else if (OB_UNLIKELY(NULL == (tmp_ptr = static_cast<char *>(allocator_->alloc(
                             OB_MAX_SYS_PARAM_NAME_LENGTH))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "fail to alloc memory", K(ret));
      } else if (FALSE_IT(data_type_str_ = static_cast<char *>(tmp_ptr))) {
      } else if (OB_UNLIKELY(NULL == (tmp_ptr = static_cast<char *>(allocator_->alloc(
                             OB_MAX_SYS_PARAM_NAME_LENGTH))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "fail to alloc memory", K(ret));
      } else {
        column_type_str_ = static_cast<char *>(tmp_ptr);
        column_type_str_len_ = OB_MAX_SYS_PARAM_NAME_LENGTH;
        view_resolve_alloc_.set_tenant_id(tenant_id_);
      }

      //
      // 分两部分进行遍历:
      // database_schema_array + table_schema_array
      //
      // 1. 扫描database_schema_array
      int64_t i = 0;
      if (last_schema_idx_ != -1) {
        i = last_schema_idx_;
        last_schema_idx_ = -1;
      }
      bool is_filter_table_schema = false;
      for (; OB_SUCC(ret) &&
            i < database_schema_array_.count() && !has_more_; ++i) {
        const ObDatabaseSchema *database_schema = database_schema_array_.at(i);
        if (OB_ISNULL(database_schema)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "database_schema is NULL", K(ret));
        } else if (database_schema->is_in_recyclebin()
          || ObString(OB_RECYCLEBIN_SCHEMA_NAME) == database_schema->get_database_name_str()
          || ObString(OB_PUBLIC_SCHEMA_NAME) == database_schema->get_database_name_str()) {
          continue;
        } else if (OB_FAIL(iterate_table_schema_array(is_filter_table_schema, i))) {
            SERVER_LOG(WARN, "fail to iterate all table schema. ", K(ret));
        }
      } // end for database_schema_array_ loop


      // 2. 扫描table_schema_array
      // 扫描完database_schema_array，继续扫描filter_table_schema_array
      if (OB_SUCC(ret) && database_schema_array_.count() == i) {
        is_filter_table_schema = true;
        if (OB_FAIL(iterate_table_schema_array(is_filter_table_schema, -1))) {
            SERVER_LOG(WARN, "fail to iterate all table schema. ", K(ret));
        }
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

int ObInfoSchemaColumnsTable::iterate_table_schema_array(const bool is_filter_table_schema,
                                                         const int64_t last_db_schema_idx)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *database_schema = NULL;
  ObArray<const ObTableSchema *> table_schema_array;
  int64_t table_schema_array_size = 0;
  // 处理table_schema_array分页
  int64_t i = 0;
  if (!is_filter_table_schema && OB_UNLIKELY(last_db_schema_idx < 0)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "last_db_schema_idx should be greater than or equal to 0", K(ret));
  } else if (is_filter_table_schema) {
    table_schema_array_size = filter_table_schema_array_.count();
    if (last_filter_table_idx_ != -1) {
      i = last_filter_table_idx_;
      last_filter_table_idx_ = -1;
    }
  } else {
    database_schema = database_schema_array_.at(last_db_schema_idx);
    const uint64_t database_id = database_schema->get_database_id();
    //get all tables
    if (OB_FAIL(schema_guard_->get_table_schemas_in_database(
        tenant_id_, database_id, table_schema_array))) {
      SERVER_LOG(WARN, "fail to get table schemas in database", K(ret),
          K(tenant_id_), K(database_id));
    } else {
      table_schema_array_size = table_schema_array.count();
    }
    if (last_table_idx_ != -1) {
      i = last_table_idx_;
      last_table_idx_ = -1;
    }
  }
  for (;
      OB_SUCC(ret) && i < table_schema_array_size && !has_more_;
      ++i) {
    const ObTableSchema *table_schema = NULL;
    if (is_filter_table_schema) {
      table_schema = filter_table_schema_array_.at(i);
    } else {
      table_schema = table_schema_array.at(i);
    }
    ++iter_cnt_;
    if (0 == ++iter_cnt_ % 1024 && OB_FAIL(THIS_WORKER.check_status())) {
      SERVER_LOG(WARN, "failed to check status", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "table_schema should not be NULL", K(ret));
    } else {
      bool is_normal_view = table_schema->is_view_table()&& !table_schema->is_materialized_view();
      //  不显示索引表
      if (table_schema->is_aux_table()
         || table_schema->is_tmp_table()
         || table_schema->is_in_recyclebin()
         || is_ora_sys_view_table(table_schema->get_table_id())) {
        continue;
      } else if (is_filter_table_schema && OB_FAIL(schema_guard_->get_database_schema(tenant_id_,
          table_schema->get_database_id(), database_schema))) {
            SERVER_LOG(WARN, "fail to get database schema", K(ret));
      } else if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "database schema is null", K(ret));
      }
      // for system view, its column info depend on hard code, so its valid by default, but do not have column meta
      // status default value is valid, old version also work whether what status it read because its column count = 0
      bool view_is_invalid = (0 == table_schema->get_object_status() || 0 == table_schema->get_column_count());
      if (OB_FAIL(ret)) {
      } else if (is_normal_view && view_is_invalid) {
        mem_context_->reset_remain_one_page();
        WITH_CONTEXT(mem_context_) {
          ObString view_definition;
          sql::ObSelectStmt *select_stmt = NULL;
          sql::ObSelectStmt *real_stmt = NULL;
          ObStmtFactory stmt_factory(mem_context_->get_arena_allocator());
          ObRawExprFactory expr_factory(mem_context_->get_arena_allocator());
          if (OB_FAIL(ObSQLUtils::generate_view_definition_for_resolve(
                        mem_context_->get_arena_allocator(),
                        session_->get_local_collation_connection(),
                        table_schema->get_view_schema(),
                        view_definition))) {
            SERVER_LOG(WARN, "fail to generate view definition for resolve", K(ret));
          } else if (OB_FAIL(ObTableColumns::resolve_view_definition(&mem_context_->get_arena_allocator(), session_, schema_guard_,
                        *table_schema, select_stmt, expr_factory, stmt_factory, false))) {
            if (OB_ERR_UNKNOWN_TABLE != ret && OB_ERR_VIEW_INVALID != ret) {
              SERVER_LOG(WARN, "failed to resolve view definition", K(view_definition), K(ret), K(table_schema->get_table_id()), K(mem_context_->used()));
            } else {
              ret = OB_SUCCESS;
              continue;
            }
          } else if (OB_UNLIKELY(NULL == select_stmt)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "select_stmt is NULL", K(ret));
          } else if (OB_ISNULL(real_stmt = select_stmt->get_real_stmt())) {
            // case : view definition is set_op
            // Bug :
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "real stmt is NULL", K(ret));
          }
          for (int64_t k = 0; OB_SUCC(ret) && k < real_stmt->get_select_item_size() && !has_more_; ++k) {
            if (OB_FAIL(fill_row_cells(database_schema->get_database_name_str(), table_schema,
                                        real_stmt, real_stmt->get_select_item(k), k + 1/* add for position */))) {
              SERVER_LOG(WARN, "fail to fill row cells", K(ret));
            } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
              SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
              if (OB_SIZE_OVERFLOW == ret) {
                last_schema_idx_ = last_db_schema_idx;
                last_table_idx_ = i;
                last_column_idx_ = k;
                has_more_ = true;
                ret = OB_SUCCESS;
              }
            }
          }
        }
      } else if (OB_FAIL(iterate_column_schema_array(database_schema->get_database_name_str(),
                                                     *table_schema,
                                                     last_db_schema_idx,
                                                     i,
                                                     is_filter_table_schema))) {
        SERVER_LOG(WARN, "fail to iterate all table columns. ", K(ret));
      }
    }
  }
  return ret;
}

int ObInfoSchemaColumnsTable::iterate_column_schema_array(
    const ObString &database_name,
    const share::schema::ObTableSchema &table_schema,
    const int64_t last_db_schema_idx,
    const int64_t last_table_idx,
    const bool is_filter_table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t ordinal_position = 0;
  // 记录column的逻辑顺序
  uint64_t logical_index = 0;
  if (last_column_idx_ != -1) {
    logical_index = last_column_idx_;
    last_column_idx_ = -1;
  }
  ObColumnIterByPrevNextID iter(table_schema);
  const ObColumnSchemaV2 *column_schema = NULL;
  for (int j = 0; OB_SUCC(ret) && j < logical_index && OB_SUCC(iter.next(column_schema)); j++) {
    // do nothing
  }
  while (OB_SUCC(ret) && OB_SUCC(iter.next(column_schema)) && !has_more_) {
    ++logical_index;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "column_schema is NULL", K(ret));
    } else {
      // 不显示隐藏pk
      if (column_schema->is_hidden()) {
        continue;
      }
      // use const_column_iterator, if it's index table
      // so should use the physical position
      if (table_schema.is_index_table()) {
        ordinal_position = column_schema->get_column_id() - 15;
      } else {
        ordinal_position = logical_index;
      }
      if (OB_FAIL(fill_row_cells(database_name, &table_schema,
                                 column_schema, ordinal_position))) {
        SERVER_LOG(WARN, "failed to fill row cells", K(ret));
      } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
        if (OB_SIZE_OVERFLOW == ret) {
          if (is_filter_table_schema) {
            last_filter_table_idx_ = last_table_idx;
            last_filter_column_idx_ = logical_index;
          } else {
            last_schema_idx_ = last_db_schema_idx;
            last_table_idx_ = last_table_idx;
            last_column_idx_ = logical_index;
          }
          has_more_ = true;
          ret = OB_SUCCESS;
        } else {
          SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
      SERVER_LOG(WARN, "fail to iterate all table columns. iter quit. ", K(ret));
    }
  }
  return ret;
}

// 过滤策略:
// 如果key_ranges_抽出来db_name
//   直接遍历当前的database_schema_array,
//   则不再从schema guard中获取租户的所有database_schema_array;
//   不管是否为有效的db_name, is_filter_db_均置为true
// 如果抽出来table_name
//   直接遍历当前的table_schema_array，
//   则不再从database_schema中获取所有的table schema
int ObInfoSchemaColumnsTable::check_database_table_filter()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); ++i) {
    const ObRowkey &start_key = key_ranges_.at(i).start_key_;
    const ObRowkey &end_key = key_ranges_.at(i).end_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
    if (2 != start_key.get_obj_cnt()
        || 2 != end_key.get_obj_cnt()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected # of rowkey columns", K(ret),
        "size of start key", start_key.get_obj_cnt(),
        "size of end key", end_key.get_obj_cnt());
    } else if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
    } else if (start_key_obj_ptr[0].is_varchar_or_char()
               && end_key_obj_ptr[0].is_varchar_or_char()
               && start_key_obj_ptr[0] == end_key_obj_ptr[0]) {
      // 表示至少指定了db_name
      // 包含过滤条件为db_name + table_name
      // 则无需获取租户下所有的database_schema
      is_filter_db_ = true;
      ObString database_name = start_key_obj_ptr[0].get_varchar();
      const ObDatabaseSchema *filter_database_schema = NULL;
      if (database_name.empty()) {
      } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_,
            database_name, filter_database_schema))) {
        SERVER_LOG(WARN, "fail to get database schema", K(ret), K(tenant_id_), K(database_name));
      } else if (NULL == filter_database_schema) {
      } else if (start_key_obj_ptr[1].is_varchar_or_char()
           && end_key_obj_ptr[1].is_varchar_or_char()
           && start_key_obj_ptr[1] == end_key_obj_ptr[1]) {
        // 指定db_name，同时指定了tbl_name
        const ObTableSchema *filter_table_schema = NULL;
        ObString table_name = start_key_obj_ptr[1].get_varchar();
        if (table_name.empty()) {
        } else if (OB_FAIL(schema_guard_->get_table_schema(tenant_id_,
            filter_database_schema->get_database_id(),
            table_name,
            false/*is_index*/,
            filter_table_schema))) {
          SERVER_LOG(WARN, "fail to get table", K(ret), K(tenant_id_), K(database_name), K(table_name));
        } else if (NULL == filter_table_schema) {
        } else if (OB_FAIL(filter_table_schema_array_.push_back(filter_table_schema))) {
          SERVER_LOG(WARN, "push_back failed", K(filter_table_schema->get_table_name()));
        }
      // 此时只指定了db_name，直接将该db push_back进入filter_database_schema_array
      } else if (OB_FAIL(add_var_to_array_no_dup(database_schema_array_, filter_database_schema))) {
        SERVER_LOG(WARN, "push_back failed", K(filter_database_schema->get_database_name()));
      }
    }
  }
  return ret;
}

int ObInfoSchemaColumnsTable::get_type_str(
    const ObObjMeta &obj_meta,
    const ObAccuracy &accuracy,
    const common::ObIArray<ObString> &type_info,
    const int16_t default_length_semantics, int64_t &pos,
    const uint64_t sub_type)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ob_sql_type_str(obj_meta, accuracy, type_info, default_length_semantics,
                              column_type_str_, column_type_str_len_, pos, sub_type))) {
    if (OB_MAX_SYS_PARAM_NAME_LENGTH == column_type_str_len_ && OB_SIZE_OVERFLOW == ret) {
      void *tmp_ptr = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = static_cast<char *>(allocator_->realloc(
                               data_type_str_,
                               OB_MAX_SYS_PARAM_NAME_LENGTH,
                               OB_MAX_EXTENDED_TYPE_INFO_LENGTH))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "fail to alloc memory", K(ret));
      } else if (FALSE_IT(data_type_str_ = static_cast<char *>(tmp_ptr))) {
      } else if (OB_UNLIKELY(NULL == (tmp_ptr = static_cast<char *>(allocator_->realloc(
                               column_type_str_,
                               OB_MAX_SYS_PARAM_NAME_LENGTH,
                               OB_MAX_EXTENDED_TYPE_INFO_LENGTH))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "fail to alloc memory", K(ret));
      } else {
        pos = 0;
        column_type_str_ = static_cast<char *>(tmp_ptr);
        column_type_str_len_ = OB_MAX_EXTENDED_TYPE_INFO_LENGTH;
        ret = ob_sql_type_str(obj_meta, accuracy, type_info, default_length_semantics,
                              column_type_str_, column_type_str_len_, pos, sub_type);
      }
    }
  }
  return ret;
}

int ObInfoSchemaColumnsTable::fill_row_cells(const ObString &database_name,
                                             const ObTableSchema *table_schema,
                                             const ObColumnSchemaV2 *column_schema,
                                             const uint64_t ordinal_position)
{
  int ret = OB_SUCCESS;

  bool is_oracle_mode = false;
  if (OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or session_ is NULL", K(ret), K(allocator_), K(session_));
  } else if (OB_ISNULL(table_schema)
      || OB_ISNULL(column_schema)
      || OB_ISNULL(column_type_str_)
      || OB_ISNULL(data_type_str_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table_schema or column_schema is NULL", K(ret));
  } else if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    SERVER_LOG(WARN, "fail to check oracle mode", KR(ret), KPC(table_schema));
  } else {
    ObObj *cells = NULL;
    const int64_t col_count = output_column_ids_.count();
    if (OB_ISNULL(cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
    } else if (OB_UNLIKELY(col_count < 0 || col_count > COLUMNS_COLUMN_COUNT)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
    } else if (OB_UNLIKELY(col_count > reserved_column_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cells count error", K(ret), K(col_count), K(reserved_column_cnt_), K(database_name));
    } else {
      const ObDataTypeCastParams dtc_params = sql::ObBasicSessionInfo::create_dtc_params(session_);
      ObCastCtx cast_ctx(allocator_, &dtc_params, CM_NONE, ObCharset::get_system_collation());
      ObObj casted_cell;
      uint64_t cell_idx = 0;
      const int64_t col_count = output_column_ids_.count();
      for (int64_t k = 0; OB_SUCC(ret) && k < col_count; ++k) {
        uint64_t col_id = output_column_ids_.at(k);
        switch (col_id) {
        case TABLE_CATALOG: {
            cells[cell_idx].set_varchar(ObString::make_string("def"));
            cells[cell_idx].set_collation_type(ObCharset::
              get_default_collation(ObCharset::get_default_charset()));
            break;
          }
        case TABLE_SCHEMA: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case TABLE_NAME: {
            cells[cell_idx].set_varchar(table_schema->get_table_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case COLUMN_NAME: {
            cells[cell_idx].set_varchar(column_schema->get_column_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case ORDINAL_POSITION: {
            cells[cell_idx].set_uint64(ordinal_position);
            break;
          }
        case COLUMN_DEFAULT: {
            casted_cell.reset();
            const ObObj *res_cell = NULL;
            ObObj def_obj = column_schema->get_cur_default_value();
            ObObjType column_type = ObMaxType;
            const ObColumnSchemaV2 *tmp_column_schema = NULL;
            if (OB_ISNULL(table_schema_) ||
                OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema(col_id))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
            } else if (FALSE_IT(column_type = tmp_column_schema->get_meta_type().get_type())) {
            } else if (IS_DEFAULT_NOW_OBJ(def_obj)) {
              ObObj def_now_obj;
              def_now_obj.set_string(column_type, ObString::make_string(N_UPPERCASE_CUR_TIMESTAMP));
              cells[cell_idx] = def_now_obj;
            } else if (def_obj.is_bit() || ob_is_enum_or_set_type(def_obj.get_type())) {
              char *buf = NULL;
              int64_t buf_len = number::ObNumber::MAX_PRINTABLE_SIZE;
              int64_t pos = 0;
              if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator_->alloc(buf_len))))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                SERVER_LOG(WARN, "fail to allocate memory", K(ret));
              } else if (def_obj.is_bit()) {
                if (OB_FAIL(def_obj.print_varchar_literal(buf, buf_len, pos, TZ_INFO(session_)))) {
                  SERVER_LOG(WARN, "fail to print varchar literal", K(ret), K(def_obj), K(buf_len), K(pos), K(buf));
                } else {
                  cells[cell_idx].set_string(column_type, ObString(static_cast<int32_t>(pos), buf));
                }
              } else {
                if (OB_FAIL(def_obj.print_plain_str_literal(column_schema->get_extended_type_info(), buf, buf_len, pos))) {
                  SERVER_LOG(WARN, "fail to print plain str literal",  KPC(column_schema), K(buf), K(buf_len), K(pos), K(ret));
                } else {
                  cells[cell_idx].set_string(column_type, ObString(static_cast<int32_t>(pos), buf));
                }
              }
            } else {
              if (OB_FAIL(ObObjCaster::to_type(column_type, cast_ctx,
                                               def_obj,
                                               casted_cell, res_cell))) {
                SERVER_LOG(WARN, "failed to cast to object",
                           K(ret), K(def_obj));
              } else if (OB_ISNULL(res_cell)) {
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "succ to cast to object, but res_cell is NULL",
                           K(ret), K(def_obj));
              } else {
                cells[cell_idx] = *res_cell;
              }
            }

            if (OB_SUCC(ret)) {
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
        case IS_NULLABLE: {
            ObString nullable_val = ObString::make_string(
                column_schema->is_nullable() ? "YES" : "NO");
            cells[cell_idx].set_varchar(nullable_val);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case DATA_TYPE: {
            ObObjType column_type = ObMaxType;
            const ObColumnSchemaV2 *tmp_column_schema = NULL;
            if (OB_ISNULL(table_schema_) ||
                OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema(col_id))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
            } else if (FALSE_IT(column_type = tmp_column_schema->get_meta_type().get_type())) {
            } else if (OB_FAIL(ob_sql_type_str(data_type_str_,
                                        column_type_str_len_,
                                        column_schema->get_data_type(),
                                        column_schema->get_collation_type(),
                                        column_schema->get_geo_type()))) {
              SERVER_LOG(WARN,"fail to get data type str",K(ret), K(column_schema->get_data_type()));
            } else {
              ObString type_val(column_type_str_len_,
                                static_cast<int32_t>(strlen(data_type_str_)),data_type_str_);
              cells[cell_idx].set_string(column_type, type_val);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
        case CHARACTER_MAXIMUM_LENGTH: {
            if(ob_is_string_type(column_schema->get_data_type()) ||
               ob_is_json(column_schema->get_data_type()) || ob_is_geometry(column_schema->get_data_type())) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(column_schema->get_data_length()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
        case CHARACTER_OCTET_LENGTH: {
            if(ob_is_string_tc(column_schema->get_data_type())) {
              ObCollationType coll = column_schema->get_collation_type();
              int64_t mbmaxlen = 0;
              if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(coll, mbmaxlen))) {
                SERVER_LOG(WARN, "failed to get mbmaxlen", K(ret), K(coll));
              } else {
                cells[cell_idx].set_uint64(static_cast<uint64_t>(
                        mbmaxlen * column_schema->get_data_length()));
              }
            } else if(ob_is_text_tc(column_schema->get_data_type()) ||
                ob_is_json(column_schema->get_data_type()) || ob_is_geometry(column_schema->get_data_type())) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(column_schema->get_data_length()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
        case NUMERIC_PRECISION: {
            ObPrecision precision = column_schema->get_data_precision();
            //for float(xx), precision==x, scale=-1
            if (!is_oracle_mode && column_schema->get_data_scale() < 0) {
              //mysql float(xx)'s NUMERIC_PRECISION is always 12 from Field.field_length
              //mysql double(xx)'s NUMERIC_PRECISION is always 22  from Field.field_length
              //as ob does not have Field.field_length, we set hard code here for compat
              if (ob_is_float_type(column_schema->get_data_type())) {
                precision = MAX_FLOAT_STR_LENGTH;
              } else if (ob_is_double_type(column_schema->get_data_type())) {
                precision = MAX_DOUBLE_STR_LENGTH;
              }
            }
            if(ob_is_numeric_type(column_schema->get_data_type()) && precision >= 0) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(precision));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
        case NUMERIC_SCALE: {
            ObScale scale = column_schema->get_data_scale();
            if(ob_is_numeric_type(column_schema->get_data_type()) && scale >= 0) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(column_schema->get_data_scale()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
        case DATETIME_PRECISION: {
            if(ob_is_datetime_tc(column_schema->get_data_type())
                || ob_is_time_tc(column_schema->get_data_type())) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(
                      column_schema->get_data_scale()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
        case CHARACTER_SET_NAME: {
              if(ob_is_varchar_char_type(column_schema->get_data_type(),column_schema->get_collation_type()) 
                || ob_is_enum_or_set_type(column_schema->get_data_type())
                || ob_is_text(column_schema->get_data_type(),column_schema->get_collation_type())){
                  cells[cell_idx].set_varchar(common::ObCharset::charset_name(
                    column_schema->get_charset_type()));
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
              } else {
                  cells[cell_idx].reset();                 
              }
              break;
          }
        case COLLATION_NAME: {
              if(ob_is_varchar_char_type(column_schema->get_data_type(),column_schema->get_collation_type()) 
                || ob_is_enum_or_set_type(column_schema->get_data_type())
                || ob_is_text(column_schema->get_data_type(),column_schema->get_collation_type())){
                cells[cell_idx].set_varchar(common::ObCharset::collation_name(
                    column_schema->get_collation_type()));
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                      ObCharset::get_default_charset()));
              } else {
                  cells[cell_idx].reset();                 
              }                                       
              break;
          }
        case COLUMN_TYPE: {
            int64_t pos = 0;
            const ObLengthSemantics default_length_semantics = session_->get_local_nls_length_semantics();
            const uint64_t sub_type = column_schema->is_xmltype() ?
                                      column_schema->get_sub_data_type() : static_cast<uint64_t>(column_schema->get_geo_type());
            ObObjType column_type = ObMaxType;
            const ObColumnSchemaV2 *tmp_column_schema = NULL;
            if (OB_ISNULL(table_schema_) ||
                OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema(col_id))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
            } else if (FALSE_IT(column_type = tmp_column_schema->get_meta_type().get_type())) {
            } else if (OB_FAIL(get_type_str(column_schema->get_meta_type(),
                                     column_schema->get_accuracy(),
                                     column_schema->get_extended_type_info(),
                                     default_length_semantics,
                                     pos, sub_type))) {
              SERVER_LOG(WARN,"fail to get column type str",K(ret), K(column_schema->get_data_type()));
            } else if (column_schema->is_zero_fill()) {
             // zerofill, only for int, float, decimal
              if (OB_FAIL(databuff_printf(column_type_str_, column_type_str_len_,
                                          pos, " zerofill"))) {
                SERVER_LOG(WARN, "fail to print zerofill", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              ObString type_val(column_type_str_len_, static_cast<int32_t>(strlen(column_type_str_)),column_type_str_);
              cells[cell_idx].set_string(column_type, type_val);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
        case COLUMN_KEY: {
            if(column_schema->is_original_rowkey_column()) {
              cells[cell_idx].set_varchar("PRI");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                    ObCharset::get_default_charset()));
            } else {
              // TODO: if (column_schema->is_index_column())
              cells[cell_idx].set_varchar("");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                  ObCharset::get_default_charset()));
            }
            break;
          }
        case EXTRA: {
            //TODO
            ObString extra = ObString::make_string("");
            // auto_increment 和 on update current_timestamp 不会同时出现在同一列上
            if (column_schema->is_autoincrement()) {
              extra = ObString::make_string("auto_increment");
            } else if (column_schema->is_on_update_current_timestamp()) {
              int16_t scale = column_schema->get_data_scale();
              if (0 == scale) {
                extra = ObString::make_string("on update current_timestamp");
              } else {
                char* buf = NULL;
                int64_t buf_len = 32;
                int64_t pos = 0;
                if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator_->alloc(buf_len))))) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  SERVER_LOG(WARN, "fail to allocate memory", K(ret));
                } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "on update current_timestamp(%d)", scale))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print on update current_tiemstamp", K(ret));
                } else {
                  extra = ObString(static_cast<int32_t>(pos), buf);
                }
              }
            } else if (column_schema->is_virtual_generated_column()) {
              extra = ObString::make_string("VIRTUAL GENERATED");
            } else if (column_schema->is_stored_generated_column()) {
              extra = ObString::make_string("STORED GENERATED");
            }
            cells[cell_idx].set_varchar(extra);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case PRIVILEGES: {
            char *buf = NULL;
            int64_t buf_len = 200;
            int64_t pos = 0;
            ObSessionPrivInfo session_priv;
            session_->get_session_priv_info(session_priv);
            if (OB_UNLIKELY(!session_priv.is_valid())) {
              ret = OB_INVALID_ARGUMENT;
              SERVER_LOG(WARN, "session priv is invalid", "tenant_id", session_priv.tenant_id_,
                         "user_id", session_priv.user_id_, K(ret));
            } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              SERVER_LOG(WARN, "fail to allocate memory", K(ret));
            } else {
              ObNeedPriv need_priv(database_name, table_schema->get_table_name(),
                                   OB_PRIV_TABLE_LEVEL, OB_PRIV_SELECT, false);
              if (OB_FAIL(fill_col_privs(session_priv, need_priv, OB_PRIV_SELECT,
                                         "select,", buf, buf_len, pos))) {
                SERVER_LOG(WARN, "fail to fill col priv", K(need_priv), K(ret));
              } else if (OB_FAIL(fill_col_privs(session_priv, need_priv, OB_PRIV_INSERT,
                                                "insert,", buf, buf_len, pos))) {
                SERVER_LOG(WARN, "fail to fill col priv", K(need_priv), K(ret));
              } else if (OB_FAIL(fill_col_privs(session_priv, need_priv, OB_PRIV_UPDATE,
                                                "update,", buf, buf_len, pos))) {
                SERVER_LOG(WARN, "fail to fill col priv", K(need_priv), K(ret));
              } else if (OB_FAIL(fill_col_privs(session_priv, need_priv, OB_PRIV_REFERENCES,
                                                "reference,", buf, buf_len, pos))) {
                SERVER_LOG(WARN, "fail to fill col priv", K(need_priv), K(ret));
              } else {
                if (pos > 0) {
                  cells[cell_idx].set_varchar(ObString(0, pos - 1, buf));
                } else {
                  cells[cell_idx].set_varchar(ObString(""));
                }
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
              }
            }
            break;
          }
        case COLUMN_COMMENT: {
            ObObjType column_type = ObMaxType;
            const ObColumnSchemaV2 *tmp_column_schema = NULL;
            if (OB_ISNULL(table_schema_) ||
                OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema(col_id))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
            } else if (FALSE_IT(column_type = tmp_column_schema->get_meta_type().get_type())) {
            } else {
              cells[cell_idx].set_string(column_type, column_schema->get_comment_str());
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                    ObCharset::get_default_charset()));
            }
            break;
          }
        case GENERATION_EXPRESSION: {
            ObObjType column_type = ObMaxType;
            const ObColumnSchemaV2 *tmp_column_schema = NULL;
            if (OB_ISNULL(table_schema_) ||
                OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema(col_id))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
            } else if (FALSE_IT(column_type = tmp_column_schema->get_meta_type().get_type())) {
            } else if (column_schema->is_generated_column()) {
              cells[cell_idx].set_string(column_type, column_schema->get_orig_default_value().get_string());
            } else {
              cells[cell_idx].set_string(column_type, ObString(""));
            }
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
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
          ++cell_idx;
        }
      }
    }
  }

  return ret;
}

int ObInfoSchemaColumnsTable::fill_col_privs(
    ObSessionPrivInfo &session_priv,
    ObNeedPriv &need_priv, 
    ObPrivSet priv_set, 
    const char *priv_str,
    char* buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;

  need_priv.priv_set_ = priv_set;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "data member is not init", KP(schema_guard_), K(ret));
  } else if (OB_SUCC(schema_guard_->check_single_table_priv(session_priv, need_priv))) {
    ret = databuff_printf(buf, buf_len, pos, "%s", priv_str);
  } else if (OB_ERR_NO_TABLE_PRIVILEGE == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObInfoSchemaColumnsTable::fill_row_cells(const common::ObString &database_name,
                                             const share::schema::ObTableSchema *table_schema,
                                             const sql::ObSelectStmt *select_stmt,
                                             const sql::SelectItem &select_item,
                                             const uint64_t ordinal_position)
{
  int ret = OB_SUCCESS;
  ObTableColumns::ColumnAttributes column_attributes;
  ObObj *cells = NULL;
  const int64_t col_count = output_column_ids_.count();
  bool is_oracle_mode = false;
  if (OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or session_ is NULL", K(ret), K(allocator_), K(session_));
  } else if (OB_ISNULL(table_schema)
      || OB_ISNULL(select_stmt)
      || OB_ISNULL(column_type_str_)
      || OB_ISNULL(data_type_str_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table_schema is NULL", K(ret));
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "data member is not init", K(ret), K(cur_row_.cells_));
  } else if (OB_UNLIKELY(cur_row_.count_ < col_count)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, 
        "cur row cell count is less than output column",
        K(ret),
        K(cur_row_.count_),
        K(col_count));
  } else if (OB_FAIL(ObTableColumns::deduce_column_attributes(is_oracle_mode, select_stmt,
                                                              select_item, schema_guard_,
                                                              session_, column_type_str_,
                                                              column_type_str_len_,
                                                              column_attributes))) {
    SERVER_LOG(WARN, "failed to deduce column attributes",
             K(select_item), K(ret));
  } else {
    const ObDataTypeCastParams dtc_params = sql::ObBasicSessionInfo::create_dtc_params(session_);
    ObCastCtx cast_ctx(allocator_, &dtc_params, CM_NONE, ObCharset::get_system_collation());
    ObObj casted_cell;
    uint64_t cell_idx = 0;

    for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
      uint64_t col_id = output_column_ids_.at(j);
      switch(col_id) {
      case TABLE_CATALOG: {
            cells[cell_idx].set_varchar(ObString::make_string("def"));
            cells[cell_idx].set_collation_type(ObCharset::
              get_default_collation(ObCharset::get_default_charset()));
            break;
          }
      case TABLE_SCHEMA: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
      case TABLE_NAME: {
            cells[cell_idx].set_varchar(table_schema->get_table_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
      case COLUMN_NAME: {
            cells[cell_idx].set_varchar(column_attributes.field_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case ORDINAL_POSITION: {
            cells[cell_idx].set_uint64(ordinal_position);
            break;
          }
        case COLUMN_DEFAULT: {
            casted_cell.reset();
            const ObObj *res_cell = NULL;
            ColumnItem column_item;
            ObObjType column_type = ObMaxType;
            const ObColumnSchemaV2 *tmp_column_schema = NULL;
            if (OB_ISNULL(table_schema_) ||
                OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema(col_id))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
            } else if (FALSE_IT(column_type = tmp_column_schema->get_meta_type().get_type())) {
            } else if (OB_FAIL(ObResolverUtils::resolve_default_value_and_expr_from_select_item(select_item, column_item, select_stmt))) {
              SERVER_LOG(WARN, "failed to resolve default value", K(ret));
            } else if (IS_DEFAULT_NOW_OBJ(column_item.default_value_)) {
              ObObj def_now_obj;
              def_now_obj.set_string(column_type, ObString::make_string(N_UPPERCASE_CUR_TIMESTAMP));
              cells[cell_idx] = def_now_obj;
            } else if (column_item.default_value_.is_bit() || ob_is_enum_or_set_type(column_item.default_value_.get_type())) {
              char *buf = NULL;
              int64_t buf_len = number::ObNumber::MAX_PRINTABLE_SIZE;
              int64_t pos = 0;
              if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator_->alloc(buf_len))))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                SERVER_LOG(WARN, "fail to allocate memory", K(ret));
              } else if (column_item.default_value_.is_bit()) {
                if (OB_FAIL(column_item.default_value_.print_varchar_literal(buf, buf_len, pos, TZ_INFO(session_)))) {
                  SERVER_LOG(WARN, "fail to print varchar literal", K(ret), K(column_item.default_value_), K(buf_len), K(pos), K(buf));
                } else {
                  cells[cell_idx].set_string(column_type, ObString(static_cast<int32_t>(pos), buf));
                }
              } else {
                ObArray<common::ObString> extended_type_info;
                const ObLengthSemantics default_length_semantics = session_->get_local_nls_length_semantics();
                if (OB_FAIL(extended_type_info.assign(select_item.expr_->get_enum_set_values()))) {
                  SERVER_LOG(WARN, "failed to assign enum values", K(ret));
                } else if (OB_FAIL(column_item.default_value_.print_plain_str_literal(extended_type_info, buf, buf_len, pos))) {
                  SERVER_LOG(WARN, "fail to print plain str literal", K(buf), K(buf_len), K(pos), K(ret));
                } else {
                  cells[cell_idx].set_string(column_type, ObString(static_cast<int32_t>(pos), buf));
                }
              }
            } else {
              if (OB_FAIL(ObObjCaster::to_type(column_type, cast_ctx,
                                               column_item.default_value_,
                                               casted_cell, res_cell))) {
                SERVER_LOG(WARN, "failed to cast to object",
                           K(ret), K(column_item.default_value_));
              } else if (OB_ISNULL(res_cell)) {
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "succ to cast to object, but res_cell is NULL",
                           K(ret), K(column_item.default_value_));
              } else {
                cells[cell_idx] = *res_cell;
              }
            }

            if (OB_SUCC(ret)) {
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
        case IS_NULLABLE: {
            ObString nullable_val = ObString::make_string(
                column_attributes.null_ ? "YES" : "NO");
            cells[cell_idx].set_varchar(nullable_val);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case DATA_TYPE: {
            ObGeoType geo_sub_type = ObGeoType::GEOTYPEMAX;
            if (ob_is_geometry(column_attributes.result_type_.get_type())) {
              ObString type_str(strlen(column_type_str_), column_type_str_);
              geo_sub_type = ObGeoTypeUtil::get_geo_type_by_name(type_str);
            }
            ObObjType column_type = ObMaxType;
            const ObColumnSchemaV2 *tmp_column_schema = NULL;
            if (OB_ISNULL(table_schema_) ||
                OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema(col_id))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
            } else if (FALSE_IT(column_type = tmp_column_schema->get_meta_type().get_type())) {
            } else if (OB_FAIL(ob_sql_type_str(data_type_str_,
                                        column_type_str_len_,
                                        column_attributes.result_type_.get_type(),
                                        ObCharset::get_default_collation(ObCharset::get_default_charset()),
                                        geo_sub_type))) {
              SERVER_LOG(WARN,"fail to get data type str",K(ret), K(column_attributes.type_));
            } else {
              ObString type_val(column_type_str_len_,
                                static_cast<int32_t>(strlen(data_type_str_)),data_type_str_);
              cells[cell_idx].set_string(column_type, type_val);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
        case CHARACTER_MAXIMUM_LENGTH: {
            if(ob_is_string_type(column_attributes.result_type_.get_type())) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(column_attributes.get_data_length()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
        case CHARACTER_OCTET_LENGTH: {
            if(ob_is_string_tc(column_attributes.result_type_.get_type())) {
              ObCollationType coll = ObCharset::get_default_collation(ObCharset::get_default_charset());
              int64_t mbmaxlen = 0;
              if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(coll, mbmaxlen))) {
                SERVER_LOG(WARN, "failed to get mbmaxlen", K(ret), K(coll));
              } else {
                cells[cell_idx].set_uint64(static_cast<uint64_t>(
                        mbmaxlen * column_attributes.get_data_length()));
              }
            } else if(ob_is_text_tc(column_attributes.result_type_.get_type())) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(column_attributes.get_data_length()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
        case NUMERIC_PRECISION: {
            ObPrecision precision = column_attributes.result_type_.get_precision();
            //for float(xx), precision==x, scale=-1
            if (!lib::is_oracle_mode() && column_attributes.result_type_.get_scale() < 0) {
              //mysql float(xx)'s NUMERIC_PRECISION is always 12 from Field.field_length
              //mysql double(xx)'s NUMERIC_PRECISION is always 22  from Field.field_length
              //as ob does not have Field.field_length, we set hard code here for compat
              if (ob_is_float_type(column_attributes.result_type_.get_type())) {
                precision = MAX_FLOAT_STR_LENGTH;
              } else if (ob_is_double_type(column_attributes.result_type_.get_type())) {
                precision = MAX_DOUBLE_STR_LENGTH;
              }
            }
            if(ob_is_numeric_type(column_attributes.result_type_.get_type()) && precision >= 0) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(precision));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
        case NUMERIC_SCALE: {
            ObScale scale = column_attributes.result_type_.get_scale();
            if(ob_is_numeric_type(column_attributes.result_type_.get_type()) && scale >= 0) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(scale));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
        case DATETIME_PRECISION: {
            if(ob_is_datetime_tc(column_attributes.result_type_.get_type())
                || ob_is_time_tc(column_attributes.result_type_.get_type())) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(
                      column_attributes.result_type_.get_scale()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
        case CHARACTER_SET_NAME: {
            cells[cell_idx].set_varchar(common::ObCharset::charset_name(
                ObCharset::charset_type_by_coll(column_attributes.result_type_.get_collation_type())));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case COLLATION_NAME: {
            cells[cell_idx].set_varchar(common::ObCharset::collation_name(
                column_attributes.result_type_.get_collation_type()));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case COLUMN_TYPE: {
            ObObjType column_type = ObMaxType;
            const ObColumnSchemaV2 *tmp_column_schema = NULL;
            if (OB_ISNULL(table_schema_) ||
                OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema(col_id))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
            } else if (FALSE_IT(column_type = tmp_column_schema->get_meta_type().get_type())) {
            } else {
              ObString type_val(column_type_str_len_, static_cast<int32_t>(strlen(column_type_str_)),column_type_str_);
              cells[cell_idx].set_string(column_type, type_val);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
        case COLUMN_KEY: {
            cells[cell_idx].set_varchar(column_attributes.key_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case EXTRA: {
            cells[cell_idx].set_varchar(column_attributes.extra_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
            break;
          }
        case PRIVILEGES: {
            char *buf = NULL;
            int64_t buf_len = 200;
            int64_t pos = 0;
            ObSessionPrivInfo session_priv;
            session_->get_session_priv_info(session_priv);
            if (OB_UNLIKELY(!session_priv.is_valid())) {
              ret = OB_INVALID_ARGUMENT;
              SERVER_LOG(WARN, "session priv is invalid", "tenant_id", session_priv.tenant_id_,
                         "user_id", session_priv.user_id_, K(ret));
            } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              SERVER_LOG(WARN, "fail to allocate memory", K(ret));
            } else {
              ObNeedPriv need_priv(database_name, table_schema->get_table_name(),
                                   OB_PRIV_TABLE_LEVEL, OB_PRIV_SELECT, false);
              if (OB_FAIL(fill_col_privs(session_priv, need_priv, OB_PRIV_SELECT,
                                         "select,", buf, buf_len, pos))) {
                SERVER_LOG(WARN, "fail to fill col priv", K(need_priv), K(ret));
              } else if (OB_FAIL(fill_col_privs(session_priv, need_priv, OB_PRIV_INSERT,
                                                "insert,", buf, buf_len, pos))) {
                SERVER_LOG(WARN, "fail to fill col priv", K(need_priv), K(ret));
              } else if (OB_FAIL(fill_col_privs(session_priv, need_priv, OB_PRIV_UPDATE,
                                                "update,", buf, buf_len, pos))) {
                SERVER_LOG(WARN, "fail to fill col priv", K(need_priv), K(ret));
              } else if (OB_FAIL(fill_col_privs(session_priv, need_priv, OB_PRIV_REFERENCES,
                                                "reference,", buf, buf_len, pos))) {
                SERVER_LOG(WARN, "fail to fill col priv", K(need_priv), K(ret));
              } else {
                if (pos > 0) {
                  cells[cell_idx].set_varchar(ObString(0, pos - 1, buf));
                } else {
                  cells[cell_idx].set_varchar(ObString(""));
                }
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                   ObCharset::get_default_charset()));
              }
            }
            break;
          }
        case COLUMN_COMMENT: {
            break;
          }
        case GENERATION_EXPRESSION: {
            break;
        }
      default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                   K(j), K(output_column_ids_), K(col_id));
          break;
        }
      } // end switch
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    } // end for
  }

  return ret;
}

inline int ObInfoSchemaColumnsTable::init_mem_context()
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL == mem_context_)) {
    lib::ContextParam param;
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_mem_attr(tenant_id_, "InfoColCtx", ObCtxIds::DEFAULT_CTX_ID);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      SQL_ENG_LOG(WARN, "mem entity is null", K(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
