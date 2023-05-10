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

#define USING_LOG_PREFIX SERVER
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/virtual_table/ob_table_index.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObTableIndex::ObTableIndex()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID),
      show_table_id_(OB_INVALID_ID),
      database_schemas_(),
      database_schema_idx_(OB_INVALID_ID),
      table_schemas_(),
      table_schema_idx_(OB_INVALID_ID),
      rowkey_info_idx_(OB_INVALID_ID),
      index_tid_array_idx_(OB_INVALID_ID),
      index_column_idx_(OB_INVALID_ID),
      simple_index_infos_(),
      is_rowkey_end_(false),
      is_normal_end_(false),
      ft_dep_col_idx_(OB_INVALID_ID)
{
}

ObTableIndex::~ObTableIndex()
{
}

int ObTableIndex::inner_open()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0;
       OB_SUCC(ret) && OB_INVALID_ID == show_table_id_ && i < key_ranges_.count(); ++i) {
    const ObRowkey &start_key = key_ranges_.at(i).start_key_;
    const ObRowkey &end_key = key_ranges_.at(i).end_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
    if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
      if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
      } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0]
                 && ObIntType == start_key_obj_ptr[0].get_type()) {
        show_table_id_ = start_key_obj_ptr[0].get_int();
      } else {/*do nothing*/}
    }
  }
  return ret;
}
void ObTableIndex::reset()
{
  ObVirtualTableScannerIterator::reset();
  key_ranges_.reset();
  tenant_id_ = OB_INVALID_ID;
  show_table_id_ = OB_INVALID_ID;
  database_schemas_.reset();
  database_schema_idx_ = OB_INVALID_ID;
  table_schemas_.reset();
  table_schema_idx_ = OB_INVALID_ID;
  rowkey_info_idx_ = OB_INVALID_ID;
  index_tid_array_idx_ = OB_INVALID_ID;
  index_column_idx_ = OB_INVALID_ID;
  is_normal_end_ = false;
  is_rowkey_end_ = false;
  simple_index_infos_.reset();
  ft_dep_col_idx_ = OB_INVALID_ID;
}

int ObTableIndex::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_) || OB_ISNULL(cur_row_.cells_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(
        WARN, "data member is not init", K(ret), K(allocator_), K(schema_guard_), K(cur_row_.cells_));
  } else {
    ObObj *cells = cur_row_.cells_;
    const int64_t col_count = output_column_ids_.count();
    bool is_end = false;
    if (OB_INVALID_ID == show_table_id_) {
      if (tenant_id_ == OB_INVALID_ID){
        ret = OB_ITER_END;
      } else {
        if (OB_INVALID_ID == static_cast<uint64_t>(database_schema_idx_)) {//first get next row
          if (OB_FAIL(schema_guard_->get_database_schemas_in_tenant(tenant_id_,
                                                                    database_schemas_))) {
            SERVER_LOG(WARN, "failed to get database schema of tenant", K_(tenant_id));
          } else {
            database_schema_idx_ = 0;
          }
        }
        if (OB_SUCC(ret)) {
          do {
            is_end = false;
            if (OB_UNLIKELY(database_schema_idx_ < 0)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "database_schema_idx_ is invalid", K(ret), K(database_schema_idx_));
            } else if (database_schema_idx_ >= database_schemas_.count()) {
              ret = OB_ITER_END;
              database_schema_idx_ = OB_INVALID_ID;
            } else {
              const ObDatabaseSchema *database_schema = database_schemas_.at(database_schema_idx_);
              if (OB_UNLIKELY(NULL == database_schema)) {
                ret = OB_ERR_BAD_DATABASE;
                SERVER_LOG(WARN, "database not exist", K(ret));
              } else if (database_schema->is_in_recyclebin()) {
                ++database_schema_idx_;
                is_end = true; //ignore this database
              } else if (ObString(OB_RECYCLEBIN_SCHEMA_NAME) == database_schema->get_database_name_str()
                         || ObString(OB_PUBLIC_SCHEMA_NAME) == database_schema->get_database_name_str()) {
                ++database_schema_idx_;
                is_end = true;
              } else if (OB_FAIL(add_database_indexes(*database_schema,
                                                      cells,
                                                      col_count,
                                                      is_end))) {
                SERVER_LOG(WARN, "failed to add table constraint of database schema!",
                           K(ret));
              } else {
                if (is_end) {
                  ++database_schema_idx_;
                }
              }
            }
          } while(OB_SUCC(ret) && is_end);
        }
      }
    } else {
      const ObTableSchema *table_schema = NULL;
      const ObDatabaseSchema *database_schema = NULL;
      if (OB_FAIL(schema_guard_->get_table_schema(tenant_id_, show_table_id_, table_schema))) {
        SERVER_LOG(WARN, "fail to get table schema", K(ret), K(tenant_id_));
      } else if (OB_UNLIKELY(NULL == table_schema)) {
              ret = OB_TABLE_NOT_EXIST;
        SERVER_LOG(WARN, "fail to get table schema", K(ret), K(show_table_id_));
      } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_,
                 table_schema->get_database_id(), database_schema))) {
        SERVER_LOG(WARN, "fail to get database schema", K(ret), K_(tenant_id),
                   "database_id", table_schema->get_database_id());
      } else if (OB_UNLIKELY(NULL == database_schema)) {
        ret = OB_ERR_BAD_DATABASE;
        SERVER_LOG(WARN, "fail to get database schema", K(ret),
                   "database_id", table_schema->get_database_id());
      } else if (OB_FAIL(add_table_indexes(*table_schema,
                                           database_schema->get_database_name_str(),
                                           cells,
                                           col_count,
                                           is_end))){
        SERVER_LOG(WARN, "failed to add table indexes of table schema",
                   "table_schema", *table_schema, K(ret));
      } else {/*do nothing*/}
      if (is_end) {
        ret = OB_ITER_END;
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObTableIndex::add_database_indexes(const ObDatabaseSchema &database_schema,
                                       ObObj *cells,
                                       int64_t col_count,
                                       bool &is_end)
{
  int ret = OB_SUCCESS;
  bool is_sub_end = false;
  if (OB_INVALID_ID == static_cast<uint64_t>(table_schema_idx_)) {
    if (OB_ISNULL(schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "data member is not init", K(ret), K(schema_guard_));
    } else if (OB_FAIL(schema_guard_->get_table_schemas_in_database(tenant_id_,
                                                                    database_schema.get_database_id(),
                                                                    table_schemas_))) {
      SERVER_LOG(WARN, "failed to get table schema in database", K(ret));
    } else {
      table_schema_idx_ = 0;
    }
  }
  if (OB_SUCC(ret)) {
    do {
      is_sub_end = false;
      if (OB_UNLIKELY(table_schema_idx_ < 0)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "table_schema_idx_ is wrong", K(table_schema_idx_));
      } else if (table_schema_idx_ >= table_schemas_.count()) {
        is_end = true;
        table_schema_idx_ = OB_INVALID_ID;
        table_schemas_.reset();
      } else {
        is_end = false;
        const ObTableSchema *table_schema = table_schemas_.at(table_schema_idx_);
        if (OB_UNLIKELY(NULL == table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          SERVER_LOG(WARN, "table schema not exist", K(ret));
        } else if(table_schema->is_index_table() || table_schema->is_aux_lob_table()) {
          is_sub_end = true;
        } else if (OB_FAIL(add_table_indexes(*table_schema,
                                             database_schema.get_database_name_str(),
                                             cells,
                                             col_count,
                                             is_sub_end))){
          SERVER_LOG(WARN, "failed to add table constraint of table schema",
                     "table_schema", *table_schema, K(ret));
        } else {/*do nothing*/}
        if (OB_LIKELY(OB_SUCC(ret) && is_sub_end)) {
            ++table_schema_idx_;
            is_rowkey_end_ = false;
            is_normal_end_ = false;
        }
      }
    } while(OB_SUCC(ret) && is_sub_end);
  }
  return ret;
}

int ObTableIndex::add_table_indexes(const ObTableSchema &table_schema,
                                    const ObString &database_name,
                                    ObObj *cells,
                                    int64_t col_count,
                                    bool &is_end)
{

  int ret = OB_SUCCESS;
  if (!is_rowkey_end_) {
    //add rowkey constraints
    if (OB_FAIL(add_rowkey_indexes(table_schema,
                                   database_name,
                                   cells,
                                   col_count,
                                   is_rowkey_end_))) {
      SERVER_LOG(WARN, "fail to add rowkey indexes", K(ret));
    }
    if (OB_SUCC(ret) && is_rowkey_end_) {
      if (OB_FAIL(add_normal_indexes(table_schema,
                                     database_name,
                                     cells,
                                     col_count,
                                     is_normal_end_))) {
        SERVER_LOG(WARN, "fail to add normal indexes", K(ret));
      }
    }
  } else {
    if (OB_FAIL(add_normal_indexes(table_schema,
                                   database_name,
                                   cells,
                                   col_count,
                                   is_normal_end_))) {
      SERVER_LOG(WARN, "fail to add normal indexes", K(ret));
    }
  }
  is_end = is_rowkey_end_ && is_normal_end_;
  return ret;

}

int ObTableIndex::add_rowkey_indexes(const ObTableSchema &table_schema,
                                     const ObString &database_name,
                                     ObObj *cells,
                                     int64_t col_count,
                                     bool &is_end)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
  if (OB_INVALID_ID == static_cast<uint64_t>(rowkey_info_idx_)) {
    rowkey_info_idx_ = 0;
  }
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "parameter is NULL", K(ret), K(cells));
  } else if (OB_UNLIKELY(cur_row_.count_ < col_count)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN,
                   "cells count is less than output column count",
                   K(ret),
                   K(cur_row_.count_),
                   K(col_count));
  } else if (OB_UNLIKELY(rowkey_info_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "rowkey_info_idx_ is wrong", K(rowkey_info_idx_));
  } else if (rowkey_info_idx_ >= rowkey_info.get_size()) {
    is_end = true;
    rowkey_info_idx_ = OB_INVALID_ID;
  } else {
    const ObColumnSchemaV2 *column_schema = NULL;
    ObRowkeyColumn rowkey_col;
    if (table_schema.is_heap_table()) {
      // don't show hidden pk
      // used for only hidden pk in the RowKey Table_schema
      is_end = true;
      rowkey_info_idx_ = OB_INVALID_ID;
    } else if (OB_FAIL(rowkey_info.get_column(rowkey_info_idx_, rowkey_col))) {
      SERVER_LOG(WARN, "fail to get column", K(ret));
    } else if (OB_UNLIKELY(NULL == (column_schema = table_schema.get_column_schema(rowkey_col.column_id_)))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to get column schema", K(ret), K(rowkey_col.column_id_));
    } else {
      is_end = false;
      uint64_t cell_idx = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
        uint64_t col_id = output_column_ids_.at(j);
          switch(col_id) {
          // table_id
          case OB_APP_MIN_COLUMN_ID: {
            cells[cell_idx].set_int(table_schema.get_table_id());
            break;
          }
          // key_name
          case OB_APP_MIN_COLUMN_ID + 1: {
            cells[cell_idx].set_varchar(ObString("PRIMARY"));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          // seq_in_index
          case OB_APP_MIN_COLUMN_ID + 2: {
            cells[cell_idx].set_int(rowkey_info_idx_ + 1);
            break;
          }
          //table_schema
          case OB_APP_MIN_COLUMN_ID + 3: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          // table
          case OB_APP_MIN_COLUMN_ID + 4: {
            cells[cell_idx].set_varchar(table_schema.get_table_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          // non_unique
          case OB_APP_MIN_COLUMN_ID + 5: {
            cells[cell_idx].set_int(0);
            break;
          }
          //index_schema
          case OB_APP_MIN_COLUMN_ID + 6: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          // column_name
          case OB_APP_MIN_COLUMN_ID + 7: {
            cells[cell_idx].set_varchar(column_schema->get_column_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          // collation
          case OB_APP_MIN_COLUMN_ID + 8: {
            cells[cell_idx].set_varchar(ObString("A")); //FIXME 全部是升序吗？
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          // cardinality
          case OB_APP_MIN_COLUMN_ID + 9: {
            //TODO 索引中唯一值的数目的估计值。通过运行ANALYZE TABLE或myisamchk -a可以更新。
            //基数根据被存储为整数的统计数据来计数，所以即使对于小型表，该值也没有必要是精确的。
            //基数越大，当进行联合时，MySQL使用该索引的机会就越大。
            cells[cell_idx].set_null();
            break;
          }
          // sub_part
          case OB_APP_MIN_COLUMN_ID + 10: {
            //TODO 如果列只是被部分地编入索引，则为被编入索引的字符的数目。如果整列被编入索引，则为NULL。
            cells[cell_idx].set_null();
            break;
          }
          // packed
          case OB_APP_MIN_COLUMN_ID + 11: {
            //TODO 指示关键字如何被压缩。如果没有被压缩，则为NULL。
            cells[cell_idx].set_null();
            break;
          }
          // null
          case OB_APP_MIN_COLUMN_ID + 12: {
            cells[cell_idx].set_varchar(ObString("")); // 主键一定不能为NULL
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          // index_type
          case OB_APP_MIN_COLUMN_ID + 13: {
            cells[cell_idx].set_varchar(ObString("BTREE")); //FIXME 一定是BTREE吗？
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          // comment
          case OB_APP_MIN_COLUMN_ID + 14: {
            //TODO
            cells[cell_idx].set_varchar(ObString("available"));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          // index_comment
          case OB_APP_MIN_COLUMN_ID + 15: {
            //TODO
            cells[cell_idx].set_varchar(ObString(""));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            //is_visible
          case OB_APP_MIN_COLUMN_ID + 16: {
            cells[cell_idx].set_varchar(ObString("YES"));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            //expression
          case OB_APP_MIN_COLUMN_ID + 17: {
            cells[cell_idx].set_null();
            break;
          }

          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                         K(rowkey_info_idx_), K(j), K(output_column_ids_), K(col_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          cell_idx++;
        }
      }
      ++rowkey_info_idx_;
    }
  }
  return ret;
}

int ObTableIndex::add_normal_indexes(const ObTableSchema &table_schema,
                                     const ObString &database_name,
                                     ObObj *cells,
                                     int64_t col_count,
                                     bool &is_end)
{
  int ret = OB_SUCCESS;
  bool is_sub_end = false;
  if (OB_INVALID_ID == static_cast<uint64_t>(index_tid_array_idx_)) {
    simple_index_infos_.reset();
    if (OB_FAIL(table_schema.get_simple_index_infos(
        simple_index_infos_, false))) {
      SERVER_LOG(WARN, "cannot get index list", K(ret));
    } else {
      index_tid_array_idx_ = 0;
    }
  }

  if (OB_SUCC(ret)) {
    do {
      is_sub_end = false;
      if (OB_UNLIKELY(index_tid_array_idx_ < 0)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "index_tid_array_idx_ is wrong", K(index_tid_array_idx_));
      } else if (index_tid_array_idx_ >= simple_index_infos_.count()) {
        is_end = true;
        index_tid_array_idx_ = OB_INVALID_ID;
      } else {
        is_end = false;
        const ObTableSchema *index_schema = NULL;
        if (OB_ISNULL(schema_guard_)) {
          ret = OB_NOT_INIT;
          SERVER_LOG(WARN, "schema guard is not init", K(ret), K(schema_guard_));
        } else if (OB_UNLIKELY(OB_FAIL(schema_guard_->get_table_schema(
                  table_schema.get_tenant_id(),
                  simple_index_infos_.at(index_tid_array_idx_).table_id_,
                  index_schema)))) {
          SERVER_LOG(WARN, "fail to get index table", K(ret),
                     "index_table_id",
                     simple_index_infos_.at(index_tid_array_idx_).table_id_);
        } else if (OB_UNLIKELY(NULL == index_schema)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid index table id", K(ret),
                     "index_table_id",
                     simple_index_infos_.at(index_tid_array_idx_).table_id_);
        } else {
          bool is_ctxcat_fulltext = false;
          ObArray<uint64_t> ft_gen_column_ids;
          ObArray<uint64_t> dep_column_ids;
          if ((INDEX_TYPE_DOMAIN_CTXCAT == index_schema->get_index_type())) {
            if (OB_FAIL(index_schema->get_generated_column_ids(ft_gen_column_ids))) {
              LOG_WARN("get generated column ids failed", K(ret));
            } else if (1 == ft_gen_column_ids.count()) {
              // 对于全文索引表，只有一列生成列, 可以有多个依赖列
              is_ctxcat_fulltext = true;
            }
          }

          if (OB_FAIL(ret)) {
          } else if (is_ctxcat_fulltext) {
            const ObColumnSchemaV2 *gen_column_schema = NULL;
            if (OB_INVALID_ID == static_cast<uint64_t>(ft_dep_col_idx_)) {
              ft_dep_col_idx_ = 0;
            }
            if (OB_ISNULL(gen_column_schema = table_schema.get_column_schema(ft_gen_column_ids[0]))) {
              ret = OB_SCHEMA_ERROR;
              SERVER_LOG(WARN, "fail to get data table column schema", K(ret));
            } else if (OB_FAIL(gen_column_schema->get_cascaded_column_ids(dep_column_ids))) {
              LOG_WARN("get cascaded column ids from column schema failed", K(ret), K(*gen_column_schema));
            } else if (dep_column_ids.count() <= ft_dep_col_idx_) {
              is_sub_end = true;
              ft_dep_col_idx_ = OB_INVALID_ID;
            } else if (OB_FAIL(add_fulltext_index_column(database_name,
                                                  table_schema,
                                                  index_schema,
                                                  cells,
                                                  col_count,
                                                  dep_column_ids[ft_dep_col_idx_]))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "fail to addd normal index column", K(ret), K(col_count), K(ft_dep_col_idx_));
            }
          } else {
            if (OB_FAIL(add_normal_index_column(database_name,
                                                table_schema,
                                                index_schema,
                                                cells,
                                                col_count,
                                                is_sub_end))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "fail to addd normal index column", K(ret), K(col_count), K(is_sub_end));
            }
          }
          if (OB_SUCC(ret)) {
            if (is_sub_end) {
              ++index_tid_array_idx_;
            }
          }
        }
      }
    } while (OB_SUCC(ret) && is_sub_end);
  }
  return ret;
}

int ObTableIndex::add_normal_index_column(const ObString &database_name,
                                          const ObTableSchema &table_schema,
                                          const ObTableSchema *index_schema,
                                          ObObj *cells,
                                          int64_t col_count,
                                          bool &is_end)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == static_cast<uint64_t>(index_column_idx_)) {
    index_column_idx_ = 0;
  }
  if (OB_ISNULL(cells) || OB_ISNULL(index_schema) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(
        WARN, "parameter or data member is NULL", K(ret), K(cells), K(index_schema), K(allocator_));
  } else if (OB_UNLIKELY(cur_row_.count_ < col_count)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells count is less than output column count",
               K(ret), K(cur_row_.count_), K(col_count));
  } else if (OB_UNLIKELY(index_column_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "index_column_idx_ is wrong", K(ret));
  } else if (index_column_idx_ >= index_schema->get_index_info().get_size()) {
    is_end = true;
    index_column_idx_ = OB_INVALID_ID;
  } else {
    const ObIndexInfo &index_info = index_schema->get_index_info();
    const ObRowkeyColumn *rowkey_column = index_info.get_column(index_column_idx_);
    const ObColumnSchemaV2 *column_schema = NULL;
    const ObColumnSchemaV2 *index_column = NULL;
    ObString index_name;
    char *buf = NULL;
    int64_t buf_len = number::ObNumber::MAX_PRINTABLE_SIZE;
    if (OB_UNLIKELY(NULL == rowkey_column)) {
      ret = OB_SCHEMA_ERROR;
      SERVER_LOG(WARN, "fail to get rowkey column", K(ret));
    } else if (index_schema->is_spatial_index()) {
      if (rowkey_column->type_.get_type() == ObVarcharType) {
        is_end = true; // mbr列不需要输出
        index_column_idx_ = OB_INVALID_ID;
      } else { // cellid列,获取主表geo列column_name
        const ObColumnSchemaV2 *cellid_column = NULL;
        if (OB_ISNULL(cellid_column = index_schema->get_column_schema(rowkey_column->column_id_))) {
          ret = OB_SCHEMA_ERROR;
          SERVER_LOG(WARN, "fail to get data table cellid column schema", K(ret), K(rowkey_column->column_id_));
        } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(cellid_column->get_geo_col_id()))) {
          ret = OB_SCHEMA_ERROR;
          SERVER_LOG(WARN, "fail to get data table geo column schema", K(ret), K(cellid_column->get_geo_col_id()));
        }
      }
    } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(rowkey_column->column_id_))) { // 索引表的column_id跟数据表的对应列的column_id是相等的
      ret = OB_SCHEMA_ERROR;
      SERVER_LOG(WARN, "fail to get data table column schema", K(ret), K_(rowkey_column->column_id));
    }

    if (OB_FAIL(ret) || is_end) {
    } else if (OB_ISNULL(index_column = index_schema->get_column_schema(rowkey_column->column_id_))) {
      ret = OB_SCHEMA_ERROR;
      SERVER_LOG(WARN, "get index column schema failed", K_(rowkey_column->column_id));
    } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for print buffer failed", K(ret), K(buf_len));
    } else {
      is_end = false;
      uint64_t cell_idx = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
        uint64_t col_id = output_column_ids_.at(j);
        switch(col_id) {
          // table_id
          case OB_APP_MIN_COLUMN_ID: {
            cells[cell_idx].set_int(table_schema.get_table_id());
            break;
          }
            // key_name
          case OB_APP_MIN_COLUMN_ID + 1: {
            index_name.reset();
            //  get the original short index name
            if (OB_FAIL(ObTableSchema::get_index_name(*allocator_,
                table_schema.get_table_id(), index_schema->get_table_name_str(),
                index_name))) {
              SERVER_LOG(WARN, "error get index table name failed", K(ret));
            } else {
              cells[cell_idx].set_varchar(index_name);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
            // seq_in_index
          case OB_APP_MIN_COLUMN_ID + 2: {
            cells[cell_idx].set_int(index_column_idx_ + 1);
            break;
          }
            //table_schema
          case OB_APP_MIN_COLUMN_ID + 3: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // table
          case OB_APP_MIN_COLUMN_ID + 4: {
            cells[cell_idx].set_varchar(table_schema.get_table_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // non_unique
          case OB_APP_MIN_COLUMN_ID + 5: {
            int64_t non_unique = 0;
            if (index_schema->is_unique_index()) {
              non_unique = 0;
            } else {
              non_unique = 1;
            }
            cells[cell_idx].set_int(non_unique);
            break;
          }
            //index_schema
          case OB_APP_MIN_COLUMN_ID + 6: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // column_name
          case OB_APP_MIN_COLUMN_ID + 7: {
            ObString column_name;
            if (OB_FAIL(get_show_column_name(table_schema, *column_schema, column_name))) {
              LOG_WARN("get show column name failed", K(ret), K(table_schema), KPC(column_schema));
            } else {
              cells[cell_idx].set_varchar(column_name);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
            // collation
          case OB_APP_MIN_COLUMN_ID + 8: {
            cells[cell_idx].set_varchar(ObString("A")); //FIXME 全部是升序吗？
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // cardinality
          case OB_APP_MIN_COLUMN_ID + 9: {
            //TODO 索引中唯一值的数目的估计值。通过运行ANALYZE TABLE或myisamchk -a可以更新。
            //基数根据被存储为整数的统计数据来计数，所以即使对于小型表，该值也没有必要是精确的。
            //基数越大，当进行联合时，MySQL使用该索引的机会就越大。
            cells[cell_idx].set_null();
            break;
          }
            // sub_part
          case OB_APP_MIN_COLUMN_ID + 10: {
            //TODO 如果列只是被部分地编入索引，则为被编入索引的字符的数目。如果整列被编入索引，则为NULL。
            cells[cell_idx].reset(); //清空上一行的结果
            if (column_schema->is_prefix_column()) {
              //打印前缀索引的长度
              int64_t pos = 0;
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%d", column_schema->get_data_length()))) {
                LOG_WARN("print prefix column data length failed", K(ret), KPC(column_schema), K(buf), K(buf_len), K(pos));
              } else {
                cells[cell_idx].set_varchar(ObString(pos, buf));
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              }
            }
            break;
          }
            // packed
          case OB_APP_MIN_COLUMN_ID + 11: {
            //TODO 指示关键字如何被压缩。如果没有被压缩，则为NULL。
            cells[cell_idx].set_null();
            break;
          }
            // null
          case OB_APP_MIN_COLUMN_ID + 12: {
            if (column_schema->is_nullable()) {
              cells[cell_idx].set_varchar(ObString("YES"));
            } else {
              cells[cell_idx].set_varchar(ObString(""));
            }
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // index_type
          case OB_APP_MIN_COLUMN_ID + 13: {
            if (false) {
              cells[cell_idx].set_varchar(ObString("FULLTEXT"));
            } else if (index_schema->is_spatial_index()) {
              cells[cell_idx].set_varchar(ObString("SPATIAL"));
            } else {
              cells[cell_idx].set_varchar(ObString("BTREE")); //FIXME 一定是BTREE吗？
            }
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // comment
          case OB_APP_MIN_COLUMN_ID + 14: {
            //TODO
            cells[cell_idx].set_varchar(ObString(ob_index_status_str(index_schema->get_index_status())));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // index_comment
          case OB_APP_MIN_COLUMN_ID + 15: {
            cells[cell_idx].set_varchar(index_schema->get_comment_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case OB_APP_MIN_COLUMN_ID + 16: {
            const ObString &is_visible = index_schema->is_index_visible() ? "YES" : "NO";
            cells[cell_idx].set_varchar(is_visible);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          //expression
          case OB_APP_MIN_COLUMN_ID + 17: {
            if (column_schema->is_func_idx_column()) {
              ObString col_def;
              if (OB_FAIL(column_schema->get_cur_default_value().get_string(col_def))) {
                LOG_WARN("get generated column definition failed", K(ret), K(*column_schema));
              } else {
                cells[cell_idx].set_varchar(col_def);
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              }
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
          ++cell_idx;
        }
      }
      ++index_column_idx_;
    }
  }
  return ret;
}

int ObTableIndex::add_fulltext_index_column(const ObString &database_name,
                                            const ObTableSchema &table_schema,
                                            const ObTableSchema *index_schema,
                                            ObObj *cells,
                                            int64_t col_count,
                                            const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cells) || OB_ISNULL(index_schema) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "parameter or data member is NULL", K(ret), K(cells), K(index_schema), K(allocator_));
  } else if (OB_UNLIKELY(cur_row_.count_ < col_count)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells count is less than output column count",
               K(ret), K(cur_row_.count_), K(col_count));
  } else if (OB_UNLIKELY(OB_INVALID_ID == ft_dep_col_idx_ || ft_dep_col_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ft_dep_col_idx_ is wrong", K(ret));
  } else {
    const ObColumnSchemaV2 *column_schema = NULL;
    ObString index_name;
    char *buf = NULL;
    int64_t buf_len = number::ObNumber::MAX_PRINTABLE_SIZE;
    if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_id))) {
      ret = OB_SCHEMA_ERROR;
      SERVER_LOG(WARN, "fail to get data table column schema", K(ret), K(column_id));
    } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for print buffer failed", K(ret), K(buf_len));
    } else {
      uint64_t cell_idx = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
        uint64_t col_id = output_column_ids_.at(j);
        switch(col_id) {
          // table_id
          case OB_APP_MIN_COLUMN_ID: {
            cells[cell_idx].set_int(table_schema.get_table_id());
            break;
          }
            // key_name
          case OB_APP_MIN_COLUMN_ID + 1: {
            index_name.reset();
            //  get the original short index name
            if (OB_FAIL(ObTableSchema::get_index_name(*allocator_,
                table_schema.get_table_id(), index_schema->get_table_name_str(),
                index_name))) {
              SERVER_LOG(WARN, "error get index table name failed", K(ret));
            } else {
              cells[cell_idx].set_varchar(index_name);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
            // seq_in_index
          case OB_APP_MIN_COLUMN_ID + 2: {
            cells[cell_idx].set_int(ft_dep_col_idx_ + 1);
            break;
          }
            //table_schema
          case OB_APP_MIN_COLUMN_ID + 3: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // table
          case OB_APP_MIN_COLUMN_ID + 4: {
            cells[cell_idx].set_varchar(table_schema.get_table_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // non_unique
          case OB_APP_MIN_COLUMN_ID + 5: {
            int64_t non_unique = 0;
            if (INDEX_TYPE_UNIQUE_GLOBAL == index_schema->get_index_type()
                || INDEX_TYPE_UNIQUE_LOCAL == index_schema->get_index_type()
                || index_schema->is_spatial_index()) {
              non_unique = 0;
            } else {
              non_unique = 1;
            }
            cells[cell_idx].set_int(non_unique);
            break;
          }
            //index_schema
          case OB_APP_MIN_COLUMN_ID + 6: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // column_name
          case OB_APP_MIN_COLUMN_ID + 7: {
            cells[cell_idx].set_varchar(column_schema->get_column_name());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // collation
          case OB_APP_MIN_COLUMN_ID + 8: {
            cells[cell_idx].set_varchar(ObString("A")); //FIXME 全部是升序吗？
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // cardinality
          case OB_APP_MIN_COLUMN_ID + 9: {
            //TODO 索引中唯一值的数目的估计值。通过运行ANALYZE TABLE或myisamchk -a可以更新。
            //基数根据被存储为整数的统计数据来计数，所以即使对于小型表，该值也没有必要是精确的。
            //基数越大，当进行联合时，MySQL使用该索引的机会就越大。
            cells[cell_idx].set_null();
            break;
          }
            // sub_part
          case OB_APP_MIN_COLUMN_ID + 10: {
            //TODO 如果列只是被部分地编入索引，则为被编入索引的字符的数目。如果整列被编入索引，则为NULL。
            cells[cell_idx].reset(); //清空上一行的结果
            if (column_schema->is_prefix_column()) {
              //打印前缀索引的长度
              int64_t pos = 0;
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%d", column_schema->get_data_length()))) {
                LOG_WARN("print prefix column data length failed", K(ret), KPC(column_schema), K(buf), K(buf_len), K(pos));
              } else {
                cells[cell_idx].set_varchar(ObString(pos, buf));
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              }
            }
            break;
          }
            // packed
          case OB_APP_MIN_COLUMN_ID + 11: {
            //TODO 指示关键字如何被压缩。如果没有被压缩，则为NULL。
            cells[cell_idx].set_null();
            break;
          }
            // null
          case OB_APP_MIN_COLUMN_ID + 12: {
            if (column_schema->is_rowkey_column()) {
              cells[cell_idx].set_varchar(ObString(""));
            } else if (column_schema->is_nullable()) {
              cells[cell_idx].set_varchar(ObString("YES"));
            } else {
              cells[cell_idx].set_varchar(ObString(""));
            }
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // index_type
          case OB_APP_MIN_COLUMN_ID + 13: {
            cells[cell_idx].set_varchar(ObString("FULLTEXT"));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // comment
          case OB_APP_MIN_COLUMN_ID + 14: {
            //TODO
            cells[cell_idx].set_varchar(ObString(ob_index_status_str(index_schema->get_index_status())));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            // index_comment
          case OB_APP_MIN_COLUMN_ID + 15: {
            cells[cell_idx].set_varchar(index_schema->get_comment_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case OB_APP_MIN_COLUMN_ID + 16: {
            const ObString &is_visible = index_schema->is_index_visible() ? "YES" : "NO";
            cells[cell_idx].set_varchar(is_visible);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
            //expression
          case OB_APP_MIN_COLUMN_ID + 17: {
            cells[cell_idx].set_null();
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
      ++ft_dep_col_idx_;
    }
  }
  return ret;
}

int ObTableIndex::get_show_column_name(const ObTableSchema &table_schema,
                                       const ObColumnSchemaV2 &column_schema,
                                       ObString &column_name)
{
  int ret = OB_SUCCESS;
  if (column_schema.is_prefix_column()) {
    //前缀索引生成的列，需要获取到原始列
    ObSEArray<uint64_t, 1> deps_column_ids;
    const ObColumnSchemaV2 *deps_column = NULL;
    if (OB_FAIL(column_schema.get_cascaded_column_ids(deps_column_ids))) {
      LOG_WARN("get cascaded column ids from column schema failed", K(ret), K(column_schema));
    } else if (OB_UNLIKELY(deps_column_ids.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deps column ids is invalid", K(ret), K(deps_column_ids));
    } else if (OB_ISNULL(deps_column = table_schema.get_column_schema(deps_column_ids.at(0)))) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("deps column not found in table schema", K(ret), K(deps_column_ids.at(0)), K(table_schema));
    } else {
      column_name = deps_column->get_column_name_str();
    }
  } else {
    column_name = column_schema.get_column_name_str();
  }
  return ret;
}
}/* ns observer*/
}/* ns oceanbase */
