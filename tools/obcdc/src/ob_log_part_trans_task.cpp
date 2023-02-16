/**
 * Copyright (c) 2022 OceanBase
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

#include "ob_log_part_trans_task.h"

#include "lib/string/ob_string.h"                   // ObString
#include "common/cell/ob_cell_reader.h"             // ObCellReader
#include "share/schema/ob_schema_service.h"         // OB_INVALID_DDL_OP
#include "share/schema/ob_table_schema.h"           // ObTableSchema
#include "share/schema/ob_column_schema.h"          // ObColumnSchemaV2
#include "storage/ob_i_store.h"                     // T_DML_INSERT
#include "storage/memtable/ob_memtable_mutator.h"   // ObMemtableMutatorMeta
#include "storage/memtable/ob_memtable_context.h"   // ObTransRowFlag

#include "ob_log_binlog_record.h"                   // ObLogBR
#include "ob_log_binlog_record_pool.h"              // ObLogBRPool
#include "ob_log_utils.h"                           // obj2str
#include "ob_log_common.h"                          // ALL_DDL_OPERATION_TABLE_DDL_STMT_STR_COLUMN_ID
#include "ob_obj2str_helper.h"                      // ObObj2strHelper
#include "ob_log_instance.h"                        // TCTX
#include "ob_log_part_trans_dispatcher.h"           // PartTransDispatcher
#include "ob_log_config.h"
#include "ob_log_part_trans_parser.h"               // IObLogPartTransParser
#include "ob_log_batch_buffer.h"                    // IObLogBatchBuffer
#include "ob_log_store_task.h"                      // ObLogStoreTask
#include "ob_log_factory.h"                         // ObLogStoreTaskFactory ReadLogBufFactory
#include "ob_log_resource_collector.h"              // IObLogResourceCollector

#define PARSE_INT64(name, obj, val, INVALID_VALUE, check_value) \
    do { \
      if (OB_FAIL(obj.get_int(reinterpret_cast<int64_t &>(val)))) { \
        LOG_ERROR("get_int fail", KR(ret), K(obj), "column", name); \
      } else if (INVALID_VALUE == val && check_value) {\
        LOG_ERROR("invalid value", K(val), "column", name); \
        ret = OB_INVALID_DATA; \
      } \
    } while (0);

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::memtable;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace liboblog
{

void IStmtTask::reset()
{
  hash_value_ = OB_INVALID_ID;
  row_index_ = OB_INVALID_ID;
  next_ = NULL;
}

uint64_t IStmtTask::get_tenant_id() const
{
  return host_.get_tenant_id();
}

////////////////////////////////////////////////////////////////////////////////////////

MutatorRow::MutatorRow(common::ObIAllocator &allocator) :
    ObMemtableMutatorRow(),
    allocator_(allocator),
    deserialized_(false),
    cols_parsed_(false),
    new_cols_(),
    old_cols_(),
    rowkey_cols_()
{}

MutatorRow::~MutatorRow()
{
  reset();
}

int MutatorRow::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;


  if (OB_UNLIKELY(deserialized_)) {
    LOG_ERROR("deserialize twice");
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos > data_len)) {
    LOG_ERROR("invalid argument", K(buf), K(pos), K(data_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObMemtableMutatorRow::deserialize(buf, data_len, pos))) {
    LOG_ERROR("deserialize mutator fail", KR(ret), KP(buf), K(data_len), K(pos));
  } else {
    deserialized_ = true;
  }

  return ret;
}

int MutatorRow::deserialize_first(const char* buf,
    const int64_t buf_len,
    int64_t &pos,
    int32_t &row_size,
    uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  row_size = 0;
  table_id = OB_INVALID_ID;
  int64_t new_pos = pos;

  if (OB_UNLIKELY(deserialized_)) {
    LOG_ERROR("deserialize twice");
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos > buf_len)) {
    LOG_ERROR("invalid argument", K(buf), K(pos), K(buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(decode_i32(buf, buf_len, new_pos, (int32_t *)&row_size_))) {
    LOG_ERROR("deserialize row_size fail", KR(ret), K(new_pos), K(row_size_));
  } else if (pos + row_size_ > buf_len) {
    LOG_ERROR("size overflow", KR(ret), KP(buf), K(buf_len), K(pos), K_(row_size));
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(decode_vi64(buf, buf_len, new_pos, (int64_t *)&table_id_))) {
    LOG_ERROR("deserialize table_id fail", KR(ret), K(new_pos), K(table_id_));
  } else {
    row_size = row_size_;
    table_id = table_id_;
    // The pos indicates the position that has been resolved
    pos = new_pos;
  }

  return ret;
}

int MutatorRow::deserialize_second(const char* buf,
    const int64_t buf_len,
    int64_t &pos,
    int64_t &table_version)
{
  int ret = OB_SUCCESS;
  table_version = 0;
  int64_t new_pos = pos;

  if (OB_UNLIKELY(deserialized_)) {
    LOG_ERROR("deserialize twice");
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos > buf_len)) {
    LOG_ERROR("invalid argument", K(buf), K(pos), K(buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(rowkey_.deserialize(buf, buf_len, new_pos))) {
    LOG_ERROR("deserialize rowkey fail", KR(ret), K(new_pos), K(rowkey_));
  } else if (OB_FAIL(decode_vi64(buf, buf_len, new_pos, &table_version_))) {
    LOG_ERROR("deserialize table_version fail", KR(ret), K(new_pos), K(table_version_));
  } else {
    table_version = table_version_;
    // The pos indicates the position that has been resolved
    pos = new_pos;
  }

  return ret;
}

// If obj2str_helper is empty, then won’t  conversion of obj to string
// also allow table schema to be empty
int MutatorRow::parse_cols(ObObj2strHelper *obj2str_helper /* = NULL */,
    const ObSimpleTableSchemaV2 *simple_table_schema /* = NULL */,
    const TableSchemaInfo *tb_schema_info /* = NULL */,
    const bool enable_output_hidden_primary_key /*  = false */)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(cols_parsed_)) {
    LOG_ERROR("columns has been parsed", K(cols_parsed_));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_UNLIKELY(! deserialized_)) {
    LOG_ERROR("row has not been deserialized");
    ret = OB_STATE_NOT_MATCH;
  } else if (NULL != simple_table_schema
      && OB_UNLIKELY(ObMemtableMutatorRow::table_id_ != simple_table_schema->get_table_id())) {
    LOG_ERROR("invalid table schema", K(table_id_), K(simple_table_schema->get_table_id()));
    ret = OB_INVALID_ARGUMENT;
  }

  // parse value of new column
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(new_row_.data_) || OB_UNLIKELY(new_row_.size_ <= 0)) {
      LOG_WARN("new row data is empty", K(new_row_),
          "mutator_row", (const ObMemtableMutatorRow &)(*this));
      new_cols_.reset();
    } else if (OB_FAIL(parse_columns_(new_cols_, new_row_.data_,
        new_row_.size_, obj2str_helper, simple_table_schema, tb_schema_info, enable_output_hidden_primary_key))) {
      LOG_ERROR("parse new columns fail", KR(ret), K(new_row_), K(obj2str_helper), K(simple_table_schema),
          K(tb_schema_info), K(enable_output_hidden_primary_key));
    } else {
      // succ
    }
  }

  // parse value of old column
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(old_row_.data_) || OB_UNLIKELY(old_row_.size_ <= 0)) {
      // no old cols
      old_cols_.reset();
    } else if (OB_FAIL(parse_columns_(old_cols_, old_row_.data_,
        old_row_.size_, obj2str_helper, simple_table_schema, tb_schema_info, enable_output_hidden_primary_key))) {
      LOG_ERROR("parse old columns fail", KR(ret), K(old_row_), K(obj2str_helper), K(simple_table_schema),
          K(tb_schema_info), K(enable_output_hidden_primary_key));
    } else {
      // succ
    }
  }

  // parse rowkey data
  if (OB_SUCC(ret)) {
    rowkey_cols_.reset();

    if (OB_FAIL(parse_rowkey_(rowkey_cols_, rowkey_, obj2str_helper, simple_table_schema, tb_schema_info,
            enable_output_hidden_primary_key))) {
      LOG_ERROR("parse_rowkey_ fail", KR(ret), K(rowkey_), K(obj2str_helper),
          K(enable_output_hidden_primary_key));
    } else {
      // succ
    }
  }

  if (OB_SUCC(ret)) {
    cols_parsed_ = true;
  }

  return ret;
}

int MutatorRow::parse_columns_(ColValueList &cols,
    const char *col_data,
    const int64_t col_data_size,
    ObObj2strHelper *obj2str_helper,
    const ObSimpleTableSchemaV2 *table_schema,
    const TableSchemaInfo *tb_schema_info,
    const bool enable_output_hidden_primary_key)
{
  int ret = OB_SUCCESS;
  ObCellReader cell_reader;
  ObCompactStoreType store_type = SPARSE;

  // NOTE: Allow obj2str_helper and column_schema to be empty
  if (OB_ISNULL(col_data) || OB_UNLIKELY(col_data_size <= 0)) {
    LOG_ERROR("invalid argument", K(col_data_size), K(col_data));
    ret = OB_INVALID_ARGUMENT;
  }
  // Validate cols values
  else if (OB_UNLIKELY(cols.num_ > 0)) {
    LOG_ERROR("column value list is not reseted", K(cols));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(cell_reader.init(col_data, col_data_size, store_type))) {
    LOG_ERROR("init cell reader fail", KR(ret), KP(col_data), K(col_data_size));
  } else {
    // Iterate through all Cells using Cell Reader
    while (OB_SUCC(ret) && OB_SUCC(cell_reader.next_cell())) {
      uint64_t column_id = OB_INVALID_ID;
      const ObObj *value = NULL;
      bool is_row_finished = false;

      if (OB_FAIL(cell_reader.get_cell(column_id, value, &is_row_finished))) {
        LOG_ERROR("get_cell from cell reader fail", KR(ret), KP(value), K(column_id));
      } else if (is_row_finished) {
        ret = OB_ITER_END;
      } else if (OB_ISNULL(value)) {
        LOG_ERROR("cell data is invalid", K(value));
        ret = OB_INVALID_DATA;
      } else if (OB_INVALID_ID == column_id) {
        // Note: the column_id obtained here may be invalid
        // For example a delete statement with only one cell and an invalid column_id in the cell
        LOG_DEBUG("cell column_id is invalid", "cell", *value,
            K_(dml_type), K_(table_id), K_(rowkey));
      } else {
        bool ignore_column = false;
        ColumnSchemaInfo* column_schema_info = NULL;

        if (NULL != table_schema && NULL != tb_schema_info) {
          ColumnPropertyFlag column_property_flag;

          if (OB_FAIL(tb_schema_info->get_column_schema_info(column_id, enable_output_hidden_primary_key, column_schema_info,
                  column_property_flag))) {
            LOG_ERROR("get_column_schema_info", KR(ret), K_(table_id),
                "table_name", table_schema->get_table_name(),
                K(column_id), K(enable_output_hidden_primary_key),
                K(column_schema_info), K(column_property_flag));
          } else if (column_property_flag.is_non_user()) {
            // ignore non user rowkey columns
            LOG_DEBUG("ignore non user rowkey column", K(column_property_flag),
                "table_id", table_schema->get_table_id(),
                "table_name", table_schema->get_table_name(),
                "table_schame_version", table_schema->get_schema_version(),
                K(column_id));

            ignore_column = true;
          // Column is deleted if Column Schema does not exist
          } else if (column_property_flag.is_delete()) {
            // ignore deleted columns
            LOG_DEBUG("ignore non-existed column", K(column_property_flag),
                "table_id", table_schema->get_table_id(),
                "table_name", table_schema->get_table_name(),
                "table_schame_version", table_schema->get_schema_version(),
                K(column_id));

            ignore_column = true;
          } else if (column_property_flag.is_hidden()) {
            // ignore hidden columns
            LOG_DEBUG("ignore hidden column", K(column_property_flag),
                "table_id", table_schema->get_table_id(),
                "table_name", table_schema->get_table_name(),
                "table_schame_version", table_schema->get_schema_version(),
                K(column_id));

            ignore_column = true;
          } else if (column_property_flag.is_invisible()) {
            // ignore invisible columns
            LOG_DEBUG("ignore invisible column", K(column_property_flag),
                "table_id", table_schema->get_table_id(),
                "table_name", table_schema->get_table_name(),
                "table_schame_version", table_schema->get_schema_version(),
                K(column_id));

            ignore_column = true;
          } else if (OB_ISNULL(column_schema_info)) {
            LOG_ERROR("column_schema_info is null", K(column_schema_info), K(column_property_flag),
                "table_id", table_schema->get_table_id(),
                "table_name", table_schema->get_table_name(),
                "version", table_schema->get_schema_version(),
                K(column_id));
            ret = OB_ERR_UNEXPECTED;
          } else {
            ignore_column = false;
          }
        }

        if (OB_SUCC(ret) && ! ignore_column) {
          if (OB_FAIL(add_column_(cols, column_id, value, obj2str_helper, table_schema,
                  column_schema_info))) {
            LOG_ERROR("add_column_ fail", K(cols), KR(ret), K(column_id), KP(value),
                K(obj2str_helper), K(table_schema), K(column_schema_info));
          }
        }
      }
    }

    ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  }

  return ret;
}

int MutatorRow::add_column_(ColValueList &cols,
    const uint64_t column_id,
    const ObObj *value,
    ObObj2strHelper *obj2str_helper,
    const share::schema::ObSimpleTableSchemaV2 *simple_table_schema,
    const ColumnSchemaInfo *column_schema_info)
{
  int ret = OB_SUCCESS;
  ColValue *cv_node = static_cast<ColValue *>(allocator_.alloc(sizeof(ColValue)));

  // NOTE: Allow obj2str_helper and column_schema to be empty
  if (OB_ISNULL(cv_node)) {
    LOG_ERROR("allocate memory for ColValue fail", "size", sizeof(ColValue));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (NULL != column_schema_info && column_schema_info->is_delete()) {
    LOG_ERROR("column_schema_info is not null, should not be delete column", K(column_id),
        KPC(column_schema_info));
    ret = OB_ERR_UNEXPECTED;
  } else {
    cv_node->reset();
    cv_node->value_ = *value;
    cv_node->column_id_ = column_id;
    common::ObArrayHelper<common::ObString> extended_type_info;
    common::ObAccuracy accuracy;
    common::ObCollationType collation_type = ObCollationType::CS_TYPE_BINARY;

    // Set meta information and scale information if column schema is valid
    if (NULL != column_schema_info) {
      column_cast(cv_node->value_, *column_schema_info);
      column_schema_info->get_extended_type_info(extended_type_info);
      accuracy = column_schema_info->get_accuracy();
      collation_type = column_schema_info->get_collation_type();
    }

    LOG_DEBUG("column_cast: ", K(column_id),
        "old_scale", value->get_scale(), "new_scale", cv_node->value_.get_scale());

    // If the LOB is larger than 2M, do not print the contents, but the address and length, in case of taking too long to print the log
    if (value->is_lob() && value->get_string_len() > 2 * _M_) {
      LOG_DEBUG("column_cast: ", "old_obj_ptr", (void *)value->get_string_ptr(),
          "old_obj_len", value->get_string_len(),
          "new_obj_ptr", (void *)cv_node->value_.get_string_ptr(),
          "new_obj_len", cv_node->value_.get_string_len());
    } else if (value->is_json() && value->get_string_len() > 2 * _M_) { // Json may exceed 2M
      LOG_DEBUG("column_cast: ", "old_obj_ptr", (void *)value->get_string_ptr(),
          "old_obj_len", value->get_string_len(),
          "new_obj_ptr", (void *)cv_node->value_.get_string_ptr(),
          "new_obj_len", cv_node->value_.get_string_len());
    } else {
      LOG_DEBUG("column_cast: ", "old_obj", *value, "new_obj",
          cv_node->value_);
    }

    // convert obj to string if obj2str_helper is valid
    // no deep copy of string required
    // note: currently DML must pass into obj2str_helper and simple_table_schema
    if (OB_NOT_NULL(obj2str_helper) && OB_NOT_NULL(simple_table_schema) && OB_FAIL(obj2str_helper->obj2str(simple_table_schema->get_tenant_id(),
        simple_table_schema->get_table_id(),
        column_id,
        cv_node->value_,
        cv_node->string_value_,
        allocator_,
        false,
        extended_type_info,
        accuracy,
        collation_type))) {
      LOG_ERROR("obj2str fail", KR(ret), "obj", *value, K(obj2str_helper), K(accuracy), K(collation_type));
    } else if (OB_FAIL(cols.add(cv_node))) {
      LOG_ERROR("add column into ColValueList fail", KR(ret), "column_value", *cv_node, K(cols));
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

int MutatorRow::parse_rowkey_(ColValueList &rowkey_cols,
    const common::ObStoreRowkey &rowkey,
    ObObj2strHelper *obj2str_helper,
    const ObSimpleTableSchemaV2 *simple_table_schema,
    const TableSchemaInfo *tb_schema_info,
    const bool enable_output_hidden_primary_key)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_count = rowkey.get_obj_cnt();
  const ObObj *rowkey_objs = rowkey.get_obj_ptr();

  if (OB_UNLIKELY(rowkey_count <= 0) || OB_ISNULL(rowkey_objs)) {
    LOG_ERROR("rowkey is invalid", K(rowkey_count), K(rowkey_objs), K(rowkey));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < rowkey_count; index++) {
      // Column ID is invalid when Table Schema is not provided
      uint64_t column_id = OB_INVALID_ID;
      ColumnSchemaInfo* column_schema_info = NULL;
      bool ignore_column = false;

      if (NULL != simple_table_schema && NULL != tb_schema_info) {
        const ObLogRowkeyInfo &rowkey_info = tb_schema_info->get_rowkey_info();
        ColumnPropertyFlag column_property_flag;

        if (OB_UNLIKELY(rowkey_count != rowkey_info.get_size())) {
          LOG_ERROR("rowkey count does not match schema", K(rowkey_count), K(rowkey_info),
              K(simple_table_schema->get_table_name()), K(simple_table_schema->get_table_id()));
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(rowkey_info.get_column_id(index, column_id))) {
          LOG_ERROR("get column id from rowkey info fail", KR(ret), K(index),
              K(column_id), K(rowkey_info));
        } else if (OB_FAIL(tb_schema_info->get_column_schema_info(column_id, enable_output_hidden_primary_key,
                column_schema_info, column_property_flag))) {
          LOG_ERROR("get_column_schema_info fail", KR(ret), K(index), K(rowkey_count),
              "table_schema_version", simple_table_schema->get_schema_version(),
              "table_id", simple_table_schema->get_table_id(),
              "table_name", simple_table_schema->get_table_name(),
              K(column_id), K(enable_output_hidden_primary_key),
              K(column_schema_info), K(column_property_flag));
        } else if (column_property_flag.is_non_user()) {
          // ignore hidden rowkey column
          LOG_DEBUG("ignore non user rowkey column", K(column_property_flag),
              "table_id", simple_table_schema->get_table_id(),
              "table_name", simple_table_schema->get_table_name(),
              "table_schame_version", simple_table_schema->get_schema_version(),
              K(column_id));

          ignore_column = true;
        } else if (column_property_flag.is_delete()) {
          LOG_ERROR("rowkey column does not exist", K(column_property_flag),
              "table_id", simple_table_schema->get_table_id(),
              "table_name", simple_table_schema->get_table_name(),
              "table_schame_version", simple_table_schema->get_schema_version(),
              K(column_id));
          ret = OB_ERR_UNEXPECTED;
        } else if (column_property_flag.is_hidden()) {
          // ignore hidden rowkey column
          LOG_DEBUG("ignore hidden rowkey column", K(column_property_flag),
              "table_id", simple_table_schema->get_table_id(),
              "table_name", simple_table_schema->get_table_name(),
              "table_schame_version", simple_table_schema->get_schema_version(),
              K(column_id));

          ignore_column = true;
        } else if (column_property_flag.is_invisible()) {
          // ignore invisible column
          LOG_DEBUG("ignore invisible column", K(column_property_flag),
              "table_id", simple_table_schema->get_table_id(),
              "table_name", simple_table_schema->get_table_name(),
              "table_schame_version", simple_table_schema->get_schema_version(),
              K(column_id));

          ignore_column = true;
        } else if (OB_ISNULL(column_schema_info)) {
          LOG_ERROR("column_schema_info is null", K(column_schema_info), K(column_property_flag),
              "table_id", simple_table_schema->get_table_id(),
              "table_name", simple_table_schema->get_table_name(),
              "table_schame_version", simple_table_schema->get_schema_version(),
              K(column_id));
          ret = OB_ERR_UNEXPECTED;
        } else {
          ignore_column = false;
        }
      }

      if (OB_SUCC(ret) && ! ignore_column) {
        if (OB_FAIL(add_column_(rowkey_cols, column_id, rowkey_objs + index,
            obj2str_helper, simple_table_schema, column_schema_info))) {
          LOG_ERROR("add_column_ fail", K(rowkey_cols), KR(ret), K(column_id),
              K(index), K(rowkey_objs[index]), K(obj2str_helper), K(simple_table_schema), K(column_schema_info));
        }
      }
    }
  }

  return ret;
}

void MutatorRow::reset()
{
  deserialized_ = false;
  cols_parsed_ = false;

  // FIXME: All nodes in new_cols_ and all memory used by Obj cast should be destroyed here.
  // but for the time being this memory will be reused in the future, so for the time being it will not be destroyed
  new_cols_.reset();
  old_cols_.reset();
  rowkey_cols_.reset();

  ObMemtableMutatorRow::reset();
}

int MutatorRow::get_cols(ColValueList **rowkey_cols, ColValueList **new_cols, ColValueList **old_cols)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! cols_parsed_)) {
    LOG_ERROR("column has not been parsed");
    ret = OB_STATE_NOT_MATCH;
  } else {
    if (NULL != rowkey_cols) {
      *rowkey_cols = &rowkey_cols_;
    }

    if (NULL != new_cols) {
      *new_cols = &new_cols_;
    }

    if (NULL != old_cols) {
      *old_cols = &old_cols_;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////
int64_t DmlStmtUniqueID::get_dml_unique_id_length() const
{
  int64_t dml_unique_id_length = 0;

  dml_unique_id_length = pkey_and_log_id_str_.length();
  dml_unique_id_length = dml_unique_id_length
    + sizeof(DELIMITER_STR)
    + compute_str_length_base_num(log_offset_)
    + sizeof(DELIMITER_STR)
    + compute_str_length_base_num(row_index_)
    + 1;

  return dml_unique_id_length;
}

int64_t DmlStmtUniqueID::compute_str_length_base_num(uint64_t num)
{
  int64_t str_length = 0;

  if (num < 10) {
    str_length = 1;
  } else if (num < 100) {
    str_length = 2;
  } else if (num < 1000) {
    str_length = 3;
  } else if (num < 10000) {
    str_length = 4;
  } else if (num < 100000) {
    str_length = 5;
  } else if (num < 1000000) {
    str_length = 6;
  } else if (num < 10000000) {
    str_length = 7;
  } else if (num < 100000000) {
    str_length = 8;
  } else if (num < 1000000000) {
    str_length = 9;
  } else if (num < 10000000000) {
    str_length = 10;
  } else {
    str_length = MAX_ROW_INDEX_LENGTH;
  }

  return str_length;
}

int DmlStmtUniqueID::customized_to_string(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    LOG_ERROR("invalid argument", K(buf), K(buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! is_valid())) {
    LOG_ERROR("dml_stmt_unique_id is not valid", K(*this));
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t str_len = pkey_and_log_id_str_.length();

    if (OB_UNLIKELY(buf_len <= str_len)) {
      LOG_ERROR("buf not enough", K(buf_len), K(str_len));
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, pkey_and_log_id_str_.ptr(), str_len);
      pos += str_len;

      if (OB_FAIL(common::databuff_printf(buf, buf_len, pos, DELIMITER_STR"%d", log_offset_))) {
        LOG_ERROR("databuff_printf fail", KR(ret), K(buf), K(buf_len), K(pos), K(row_index_));
      } else if (OB_FAIL(common::databuff_printf(buf, buf_len, pos, DELIMITER_STR"%lu", row_index_))) {
        LOG_ERROR("databuff_printf fail", KR(ret), K(buf), K(buf_len), K(pos), K(row_index_));
      } else {
        // succ
      }
    }
  }

  return ret;
}

DmlStmtTask::DmlStmtTask(PartTransTask &host,
    ObLogEntryTask &redo_log_entry_task,
    MutatorRow &row) :
    IStmtTask(STMT_TYPE_DML, host),
    redo_log_entry_task_(redo_log_entry_task),
    row_(row)
{
  // set hash value
  IStmtTask::set_hash_value(row.rowkey_.murmurhash(host.get_partition().get_table_id() + host.get_partition().get_partition_id()));

}

DmlStmtTask::~DmlStmtTask() { reset(); }

void DmlStmtTask::reset()
{
  IStmtTask::reset();
  row_.reset();
}

int64_t DmlStmtTask::get_global_schema_version() const
{
  return host_.get_global_schema_version();
}

int64_t DmlStmtTask::get_part_id() const
{
  return get_host().get_partition().get_partition_id();
}

int64_t DmlStmtTask::get_row_seq_for_rollback() const
{
  return get_row_sql_no();
}

////////////////////////////////////////////////////////////////////////////////////
int64_t DdlStmtUniqueID::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos,
        "%ld_%ld", cluster_id_, schema_version_);
  }

  return pos;
}

DdlStmtTask::DdlStmtTask(PartTransTask &host, MutatorRow &row, const int64_t cluster_id) :
    IStmtTask(STMT_TYPE_DDL, host),
    row_(row),
    ddl_stmt_str_(),
    ddl_operation_type_(OB_INVALID_DDL_OP),
    ddl_op_schema_version_(0),
    ddl_op_table_id_(OB_INVALID_ID),
    ddl_op_tenant_id_(OB_INVALID_TENANT_ID),
    ddl_op_database_id_(OB_INVALID_ID),
    ddl_op_tablegroup_id_(OB_INVALID_ID),
    ddl_exec_tenant_id_(OB_INVALID_TENANT_ID),
    cluster_id_(cluster_id)
{
  // set hash value
  IStmtTask::set_hash_value(row.rowkey_.murmurhash(host.get_partition().hash()));
  ddl_op_schema_version_str_[0] = '\0';
}

DdlStmtTask::~DdlStmtTask()
{
  reset();
}

int64_t DdlStmtTask::get_row_seq_for_rollback() const
{
  // only has sql_no in obce
  return get_row_sql_no();
}

bool DdlStmtTask::is_recyclebin_database_id(const uint64_t tenant_id, const uint64_t database_id)
{
  return (combine_id(tenant_id, OB_RECYCLEBIN_SCHEMA_ID) == database_id);
}

bool DdlStmtTask::is_drop_table_ddl_(const int64_t ddl_operation_type)
{
  ObSchemaOperationType op_type = static_cast<ObSchemaOperationType>(ddl_operation_type);

  return (OB_DDL_DROP_TABLE == op_type);
}

bool DdlStmtTask::is_drop_tablegroup_ddl_(const int64_t ddl_operation_type)
{
  ObSchemaOperationType op_type = static_cast<ObSchemaOperationType>(ddl_operation_type);

  return (OB_DDL_DEL_TABLEGROUP == op_type);
}

bool DdlStmtTask::is_drop_tenant_ddl_(const int64_t ddl_operation_type)
{
  ObSchemaOperationType op_type = static_cast<ObSchemaOperationType>(ddl_operation_type);

  return (OB_DDL_DEL_TENANT == op_type);
}

bool DdlStmtTask::is_global_index_ddl_(const int64_t ddl_operation_type)
{
  bool bool_ret = false;

  ObSchemaOperationType op_type = static_cast<ObSchemaOperationType>(ddl_operation_type);

  bool_ret = (OB_DDL_CREATE_GLOBAL_INDEX == op_type)
    || (OB_DDL_DROP_GLOBAL_INDEX == op_type);

  return bool_ret;
}

bool DdlStmtTask::is_normal_index_ddl_(const int64_t ddl_operation_type)
{
  bool bool_ret = false;

  ObSchemaOperationType op_type = static_cast<ObSchemaOperationType>(ddl_operation_type);

  bool_ret = (OB_DDL_CREATE_INDEX == op_type)
    || (OB_DDL_DROP_INDEX == op_type);

  return bool_ret;
}

bool DdlStmtTask::is_create_tenant_end_ddl_(const int64_t ddl_operation_type)
{
  bool bool_ret = false;

  ObSchemaOperationType op_type = static_cast<ObSchemaOperationType>(ddl_operation_type);
  bool_ret = (OB_DDL_ADD_TENANT_END == op_type);

  return bool_ret;
}

bool DdlStmtTask::is_finish_schema_split_ddl_(const int64_t ddl_operation_type)
{
  bool bool_ret = false;

  ObSchemaOperationType op_type = static_cast<ObSchemaOperationType>(ddl_operation_type);
  bool_ret = (OB_DDL_FINISH_SCHEMA_SPLIT == op_type);

  return bool_ret;
}

bool DdlStmtTask::is_sub_partition_alter_ddl_(const int64_t ddl_operation_type)
{
  bool bool_ret = false;

  ObSchemaOperationType op_type = static_cast<ObSchemaOperationType>(ddl_operation_type);
  bool_ret = (OB_DDL_ADD_SUB_PARTITION == op_type)
    || (OB_DDL_DROP_SUB_PARTITION == op_type);

  return bool_ret;
}

int DdlStmtTask::parse_ddl_info(ObLogBR *br,
    const uint64_t row_index,
    bool &is_valid_ddl,
    int64_t &update_schema_version,
    uint64_t &exec_tenant_id)
{
  int ret = OB_SUCCESS;
  bool contain_ddl_stmt = false;

  is_valid_ddl = true;

  if (OB_ISNULL(br) || OB_UNLIKELY(OB_INVALID_ID == row_index)) {
    LOG_ERROR("invalid argument", K(br), K(row_index));
    ret = OB_INVALID_ARGUMENT;
  }
  // parses the column data
  // but does not convert the column data to a string
  else if (OB_FAIL(row_.parse_cols())) {
    LOG_ERROR("parse columns fail", KR(ret), K(row_));
  } else if (OB_FAIL(parse_ddl_info_(contain_ddl_stmt, update_schema_version))) {
    if (OB_INVALID_DATA == ret) {
      // If invalid data is encountered, the log is printed but the dirty data is ignored
      LOG_ERROR("fail to parse DDL, __all_ddl_operation table data is invalid",
          "log_id", get_host().get_prepare_log_id(),
          "log_timestamp", get_host().get_timestamp(),
          K(row_));

      // an invalid DDL statement
      is_valid_ddl = false;
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("parse_ddl_info_ fail", KR(ret), K(row_));
    }
  } else {
    is_valid_ddl = true;

    if (! contain_ddl_stmt) {
      // filter if don't contans ddl stmt
      is_valid_ddl = false;

      // 1: If it is a drop table type and the operation is on a table in the recycle bin, then the DDL is retained
      // The reason is that in this case the ddl statement should have been logged, but in the case of drop database it will not be logged, the scenario is as follows.
      // For table A, in the DB, perform the following sequence of operations.
      // 1. delete table A, go to recycle bin
      // 2. delete DB, go to recycle bin
      // 3. directly purge DB, table A in the recycle bin will also be purged, but the database id in the __all_ddl_operation table records
      // the database id of the __recyclebin, not the database id of the DB, resulting in the DB cannot be traced back to table A being deleted,
      // based on the above For the above reason, the DDL of the drop type should be kept here to ensure subsequent committer processing
      //
      // 2: Under oracle tenant, drop user will trigger multiple drop table, drop database DDLs, and none of them have ddl_stmt_str, here keep
      if (is_drop_table_ddl_(ddl_operation_type_)) {
        is_valid_ddl = true;
      }

      if (is_drop_tablegroup_ddl_(ddl_operation_type_)) {
        is_valid_ddl = true;
      }

      if (is_drop_tenant_ddl_(ddl_operation_type_)) {
        is_valid_ddl = true;
      }

      // When create a primary table and specify a global index, the DDL for the primary table and the DDL for the global index will be
      // recorded in the __all_ddl_opertition table, and the ddl_stmt_str will be empty, so make sure the global index DDL is not filtered
      if (is_global_index_ddl_(ddl_operation_type_)) {
        is_valid_ddl = true;
      }

      // When you create the primary table, you also create the unique index table, in this case, in the __all_ddl_opertition table, will
      // record the DDL of the primary table and the DDL of the unique index, at this time, the ddl_stmt_str is empty, you need to ensure that the unique index DDL is not filtered
      // note: neither normal indexes nor unique indexes will be filtered here, you need to determine if it is a unique index based on schema information
      if (is_normal_index_ddl_(ddl_operation_type_)) {
        is_valid_ddl = true;
      }

      // schema split mode, there are two types of tenant creation:
      // 1. OB_DDL_ADD_TENANT_START records ddl_stmt, but does not process it
      // 2. OB_DDL_ADD_TENANT_END does not record ddl_stmt, it cannot be filtered here
      if (is_create_tenant_end_ddl_(ddl_operation_type_)) {
        is_valid_ddl = true;
      }

      // schema split done
      if (is_finish_schema_split_ddl_(ddl_operation_type_)) {
        is_valid_ddl = true;
      }

      // obadmin performs dynamic add/drop operations on secondary partitions with null ddl_stmt_str, but not filtering
      if (is_sub_partition_alter_ddl_(ddl_operation_type_)) {
        is_valid_ddl = true;
      }
    }

    if (OB_SUCCESS == ret && is_valid_ddl) {
      if (OB_FAIL(build_ddl_binlog_record_(br, ddl_stmt_str_, row_index))) {
        LOG_ERROR("build_ddl_binlog_record_ fail", KR(ret), K(br), K(ddl_stmt_str_), "commit_version", get_host().get_global_trans_version(),
            K(row_index));
      } else {
        // set Binlog Record
        set_binlog_record(br);
      }
    }

    if (OB_SUCC(ret)) {
      // 1. If ddl_exec_tenant_id_ is invalid, use ddl_op_tenant_id_, as __all_ddl_operation does not contain an exec_tenant_id column for versions below 1460
      // 2. If ddl_op_tenant_id_ is not valid [e.g. alter system add zone 'z1' etc.], then use the partitioned tenant_id
      if (OB_INVALID_TENANT_ID != ddl_exec_tenant_id_) {
        exec_tenant_id = ddl_exec_tenant_id_;
      } else if (OB_INVALID_TENANT_ID != ddl_op_tenant_id_) {
        exec_tenant_id = ddl_op_tenant_id_;
      } else {
        exec_tenant_id = get_host().get_tenant_id();
      }
    }
  }

  if (OB_SUCCESS == ret) {
    _LOG_INFO("[STAT] [DDL] [PARSE] OP_TYPE=%s(%ld) SCHEMA_VERSION=%ld "
        "VERSION_DELAY=%.3lf(sec) EXEC_TENANT_ID=%ld TABLE_ID=%ld TENANT_ID=%ld DB_ID=%ld "
        "TG_ID=%ld DDL_STMT=[%s] CONTAIN_DDL=%d IS_VALID=%d",
        ObSchemaOperation::type_str((ObSchemaOperationType)ddl_operation_type_),
        ddl_operation_type_, ddl_op_schema_version_, get_delay_sec(ddl_op_schema_version_),
        ddl_exec_tenant_id_, ddl_op_table_id_, ddl_op_tenant_id_,
        ddl_op_database_id_, ddl_op_tablegroup_id_,
        to_cstring(ddl_stmt_str_), contain_ddl_stmt, is_valid_ddl);
  }

  return ret;
}

int DdlStmtTask::parse_ddl_info_(bool &contain_ddl_stmt, int64_t &update_schema_version)
{
  int ret = OB_SUCCESS;
  ColValueList *new_cols = NULL;
  ColValueList *rowkey_cols = NULL;

  if (OB_FAIL(row_.get_cols(&rowkey_cols, &new_cols, NULL))) {
    LOG_ERROR("get_cols fail", KR(ret), K(row_));
  } else if (OB_ISNULL(rowkey_cols) || OB_ISNULL(new_cols)) {
    LOG_ERROR("get_cols fail", K(new_cols), K(rowkey_cols));
    ret = OB_ERR_UNEXPECTED;
  } else {
    contain_ddl_stmt = false;
    update_schema_version = 0;

    ddl_stmt_str_.reset();
    ddl_op_schema_version_ = 0;
    ddl_op_schema_version_str_[0] = '\0';
    ddl_operation_type_ = OB_INVALID_DDL_OP;
    ddl_op_table_id_ = OB_INVALID_ID;
    ddl_op_tenant_id_ = OB_INVALID_TENANT_ID;
    ddl_op_database_id_ = OB_INVALID_ID;
    ddl_op_tablegroup_id_ = OB_INVALID_ID;
    ddl_exec_tenant_id_ = OB_INVALID_TENANT_ID;

    // only parse insert stmt
    if (storage::T_DML_INSERT != row_.dml_type_) {
      LOG_WARN("ignore NON-INSERT statement of table __all_ddl_operation", K(row_));
      contain_ddl_stmt = false;
    } else if (rowkey_cols->num_ != 1) {
      LOG_ERROR("__all_ddl_operation rowkey column num is more than 1. not supported",
          K(*rowkey_cols), K(row_));
      ret = OB_NOT_SUPPORTED;
    } else if (OB_ISNULL(rowkey_cols->head_) || OB_UNLIKELY(rowkey_cols->num_ <= 0)) {
      LOG_ERROR("rowkey column parsed is invalid", KPC(rowkey_cols));
      ret = OB_ERR_UNEXPECTED;
    }
    // parse schema_version from rowkey
    else if (OB_FAIL(parse_schema_version_(rowkey_cols->head_->value_, ddl_op_schema_version_))) {
      LOG_ERROR("parse_schema_version_ fail", KR(ret), K(*(rowkey_cols->head_)));
    } else {
      // update schema version whatever
      update_schema_version = ddl_op_schema_version_;

      // parse normal columns
      if (OB_FAIL(parse_ddl_info_from_normal_columns_(*new_cols))) {
        LOG_ERROR("parse_ddl_info_from_normal_columns_ fail", KR(ret), K(*new_cols));
      } else {
        // verify parse result
        if (ddl_stmt_str_.empty()) {
          // only focus on ddl that not empty
          contain_ddl_stmt = false;
        }
        // ignore ddl which tenant id is invalid
        else if (OB_INVALID_TENANT_ID == ddl_op_tenant_id_) {
          LOG_WARN("ignore DDL which tenant id is invaild",
              K(ddl_op_tenant_id_),
              K(ddl_op_schema_version_),
              "ddl_op_schema_version_str", static_cast<char *>(ddl_op_schema_version_str_),
              K(ddl_operation_type_),
              K(ddl_op_table_id_),
              K(ddl_op_database_id_),
              K(ddl_op_tablegroup_id_),
              K(ddl_stmt_str_));
          contain_ddl_stmt = false;
        }
        // Allow tablegroup id, database id, table id to be invalid
        // but schema version/operation type cannot be invalid, they are treated as invalid DDLs
        else if (ddl_op_schema_version_ <= 0 || OB_INVALID_DDL_OP == ddl_operation_type_) {
          LOG_ERROR("ddl information is incomplete",
              K(ddl_op_tenant_id_),
              K(ddl_op_schema_version_),
              "ddl_op_schema_version_str", static_cast<char *>(ddl_op_schema_version_str_),
              K(ddl_operation_type_),
              K(ddl_op_table_id_),
              K(ddl_op_database_id_),
              K(ddl_op_tablegroup_id_),
              K(ddl_stmt_str_));
          ret = OB_INVALID_DATA;
        } else {
          contain_ddl_stmt = true;
        }
      }
    }
  }

  return ret;
}

int DdlStmtTask::build_ddl_binlog_record_(ObLogBR *br,
    const ObString &ddl_stmt,
    const uint64_t row_index)
{
  int ret = OB_SUCCESS;
  const int64_t global_trans_version = get_host().get_global_trans_version();
  uint64_t cluster_id = get_host().get_cluster_id();
  // DDL tenant_id records the tenant ID of the partition to which it belongs, not the executor tenant ID, to ensure that in schema split
  // scenarios, incremental backup DDLs are not incorrectly distributed to the tenant to which they belong, causing loci to get stuck
  const uint64_t tenant_id = get_host().get_tenant_id();
  ILogRecord *br_data = NULL;

  // no need set trace id, trace_info for ddl
  ObString trace_id;
  ObString trace_info;
  ObString ddl_unique_id;
  const int64_t part_trans_task_count = 1;

  if (OB_ISNULL(br) || OB_UNLIKELY(OB_INVALID_ID == row_index)) {
    LOG_ERROR("invalid argument", K(br), K(ddl_stmt), K(global_trans_version), K(row_index));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(init_ddl_unique_id_(ddl_unique_id))) {
    LOG_ERROR("init_ddl_unique_id_ fail", KR(ret), K(ddl_unique_id));
  } else if (OB_FAIL(br->init_data(EDDL, cluster_id, tenant_id, row_index,
          trace_id, trace_info, ddl_unique_id, ddl_op_schema_version_, global_trans_version,
          part_trans_task_count))) {
    LOG_ERROR("ObLogBR::init_data EDDL fail", KR(ret), K(global_trans_version),
        K(cluster_id), K(tenant_id), K(ddl_op_schema_version_),
        K(trace_id), K(trace_info), K(ddl_unique_id), K(part_trans_task_count));
  } else if (OB_ISNULL(br_data = (br->get_data()))) {
    LOG_ERROR("get binlog record data fail", K(br));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // 2 field for columns
    // ddl_stmt_str and ddl_schema_version
    int col_count = 2;
    int64_t column_array_size = sizeof(BinLogBuf) * col_count;
    BinLogBuf *column_array = static_cast<BinLogBuf *>(get_host().alloc(column_array_size));

    if (OB_ISNULL(column_array)) {
      LOG_ERROR("allocate memory for column array fail", K(column_array_size));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      br_data->setNewColumn(column_array, col_count);
      br_data->setOldColumn(NULL, 0);

      br_data->putNew(ddl_stmt.ptr(), ddl_stmt.length());
      br_data->putNew(ddl_op_schema_version_str_, static_cast<int>(strlen(ddl_op_schema_version_str_)));
    }
  }

  return ret;
}

int DdlStmtTask::init_ddl_unique_id_(common::ObString &ddl_unique_id)
{
  int ret = OB_SUCCESS;
  DdlStmtUniqueID ddl_stmt_unique_id(cluster_id_, ddl_op_schema_version_);

  if (OB_UNLIKELY(! ddl_stmt_unique_id.is_valid())) {
    LOG_ERROR("ddl_stmt_unique_id is not valid", K(ddl_stmt_unique_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    common::ObIAllocator &allocator= this->get_host().get_allocator();
    const int64_t buf_len = MAX_DDL_UNIQUE_ID_LENGTH;
    char *buf = static_cast<char*>(allocator.alloc(buf_len));
    int64_t pos = 0;

    if (OB_ISNULL(buf)) {
      LOG_ERROR("allocate memory for trans id buffer fail", K(buf));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
            "%s", to_cstring(ddl_stmt_unique_id)))) {
      LOG_ERROR("init_ddl_unique_id_ fail", KR(ret), K(buf), K(buf_len), K(pos),
          K(ddl_stmt_unique_id));
    } else {
      ddl_unique_id.assign_ptr(buf, static_cast<int32_t>(pos));
    }
  }

  return ret;
}

int DdlStmtTask::parse_schema_version_(ObObj &value, int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_FAIL(value.get_int(schema_version))) {
    LOG_ERROR("get schema_version fail", KR(ret), K(value));
  } else if (OB_UNLIKELY(schema_version <= 0)) {
    LOG_ERROR("all_ddl_operation schema_version is invalid", K(schema_version), K(value));
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(databuff_printf(ddl_op_schema_version_str_, MAX_DDL_SCHEMA_VERSION_STR_LENGTH,
          pos, "%ld", schema_version))) {
    LOG_ERROR("databuff_printf fail", KR(ret), K(schema_version),
        K(ddl_op_schema_version_str_), K(pos));
  } else {
    // succ
  }

  return ret;
}

int DdlStmtTask::parse_ddl_info_from_normal_columns_(ColValueList &col_value_list)
{
  int ret = OB_SUCCESS;
  const bool is_schema_split_mode = TCTX.is_schema_split_mode_;
  const uint64_t ddl_tenant_id = get_host().get_tenant_id();
  ColValue *cv_node = col_value_list.head_;

  while (NULL != cv_node && OB_SUCCESS == ret) {
    ColValue *next = cv_node->get_next();
    ObObj &value = cv_node->value_;

    switch (cv_node->column_id_) {
      case ALL_DDL_OPERATION_TABLE_EXEC_TENANT_ID_COLUMN_ID: {
        // allow invalid exec_tenant_id
        PARSE_INT64("exec_tenant_id", value, ddl_exec_tenant_id_, OB_INVALID_TENANT_ID, false);
        break;
      }

      case ALL_DDL_OPERATION_TABLE_TENANT_ID_COLUMN_ID: {
        // allow invalid tenant id
        PARSE_INT64("tenant_id", value, ddl_op_tenant_id_, OB_INVALID_TENANT_ID, false);
        // schema split schema, __all_ddl_operation table tenant_id as 0, need to record tenant_id of tenant
        if (is_schema_split_mode) {
          if (OB_INVALID_TENANT_ID == ddl_op_tenant_id_) {
            ddl_op_tenant_id_ = ddl_tenant_id;
          }
        }
        break;
      }

      case ALL_DDL_OPERATION_TABLE_DATABASE_ID_COLUMN_ID: {
        // allow invalid database id
        PARSE_INT64("database_id", value, ddl_op_database_id_, OB_INVALID_ID, false);
        const uint64_t original_ddl_op_database_id = ddl_op_database_id_;
        ddl_op_database_id_ = combine_id_(is_schema_split_mode, ddl_tenant_id, ddl_op_database_id_);
        LOG_DEBUG("[DDL_STMT] [PARSE]", K(ddl_tenant_id), "original_ddl_op_database_id", original_ddl_op_database_id,
            K(ddl_op_database_id_));

        break;
      }

      case ALL_DDL_OPERATION_TABLE_TABLEGROUP_ID_COLUMN_ID: {
        // allow invalid tablegroup id
        PARSE_INT64("tablegroup_id", value, ddl_op_tablegroup_id_, OB_INVALID_ID, false);
        const uint64_t original_ddl_op_tablegroup_id = ddl_op_tablegroup_id_;
        ddl_op_tablegroup_id_ = combine_id_(is_schema_split_mode, ddl_tenant_id, ddl_op_tablegroup_id_);
        LOG_DEBUG("[DDL_STMT] [PARSE]", K(ddl_tenant_id), "original_ddl_op_tablegroup_id", original_ddl_op_tablegroup_id,
            K(ddl_op_tablegroup_id_));

        break;
      }

      case ALL_DDL_OPERATION_TABLE_TABLE_ID_COLUMN_ID: {
        // allow invalid table id
        PARSE_INT64("table_id", value, ddl_op_table_id_, OB_INVALID_ID, false);
        const uint64_t original_ddl_op_table_id = ddl_op_table_id_;
        ddl_op_table_id_ = combine_id_(is_schema_split_mode, ddl_tenant_id, ddl_op_table_id_);
        LOG_DEBUG("[DDL_STMT] [PARSE]", K(ddl_tenant_id), K(original_ddl_op_table_id), K(ddl_op_table_id_));

        break;
      }

      case ALL_DDL_OPERATION_TABLE_OPERATION_TYPE_COLUMN_ID: {
        // check operation type
        PARSE_INT64("operation_type", value, ddl_operation_type_, OB_INVALID_DDL_OP, true);
        break;
      }

      case ALL_DDL_OPERATION_TABLE_DDL_STMT_STR_COLUMN_ID: {
        ddl_stmt_str_ = value.get_varchar();
        break;
      }

      default:
        break;
    }

    cv_node = next;
  }

  return ret;
}

uint64_t DdlStmtTask::combine_id_(const bool is_schema_split_mode,
    const uint64_t tenant_id,
    const uint64_t pure_id)
{
  uint64_t res_id = pure_id;

  if (is_schema_split_mode) {
    if (OB_INVALID_ID != pure_id && pure_id > 0) {
      res_id = combine_id(tenant_id, pure_id);
    }
  }

  return res_id;
}

void DdlStmtTask::reset()
{
  ddl_stmt_str_.reset();
  ddl_op_schema_version_ = 0;
  ddl_op_schema_version_str_[0] = '\0';
  ddl_operation_type_ = OB_INVALID_DDL_OP;
  ddl_op_table_id_ = OB_INVALID_ID;
  ddl_op_tenant_id_ = OB_INVALID_TENANT_ID;
  ddl_op_database_id_ = OB_INVALID_ID;
  ddl_op_tablegroup_id_ = OB_INVALID_ID;
  ddl_exec_tenant_id_ = OB_INVALID_TENANT_ID;
  br_ = NULL;

  row_.reset();
  IStmtTask::reset();
}

////////////////////////////////////////////////////////////////////////////////////

ObLogEntryTask::ObLogEntryTask() :
    host_(NULL),
    participant_(NULL),
    partition_(),
    trans_id_(),
    redo_node_(NULL),
    is_big_row_(false),
    stmt_list_(),
    formatted_stmt_num_(0),
    row_ref_cnt_(0),
    arena_allocator_("LogEntryTask", OB_MALLOC_MIDDLE_BLOCK_SIZE)
{
}

ObLogEntryTask::~ObLogEntryTask()
{
  reset();
}

void ObLogEntryTask::reset()
{
  host_ = NULL;
  participant_ = NULL;
  partition_.reset();
  trans_id_.reset();
  redo_node_ = NULL;
  is_big_row_ = false;
  stmt_list_.reset();
  formatted_stmt_num_ = 0;
  row_ref_cnt_ = 0;

  arena_allocator_.clear();
}

bool ObLogEntryTask::is_valid() const
{
  bool bool_ret = false;

  bool_ret = (NULL != redo_node_)
    && (redo_node_->is_valid());

  return bool_ret;
}

int ObLogEntryTask::init(const common::ObPartitionKey &pkey,
    const char *participant,
    const transaction::ObTransID &trans_id,
    DmlRedoLogNode *redo_node)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node)) {
    LOG_ERROR("invalid argument", K(redo_node));
    ret = OB_INVALID_ARGUMENT;
  } else {
    partition_ = pkey;
    participant_ = participant;
    trans_id_ = trans_id;
    redo_node_ = redo_node;

    LOG_DEBUG("LogEntryTask init", K(this), KPC(this));
  }

  return ret;
}

int ObLogEntryTask::get_status(bool &is_stored)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("invalid redo_node", K(redo_node_));
    ret = OB_INVALID_DATA;
  } else {
    is_stored = redo_node_->is_stored();
  }

  return ret;
}

int ObLogEntryTask::get_storage_key(std::string &key)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("invalid redo_node", K(redo_node_));
    ret = OB_INVALID_DATA;
  } else if (OB_ISNULL(participant_)) {
    LOG_ERROR("invalid argument");
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t store_log_id = redo_node_->get_store_log_id();
    int32_t log_offset = redo_node_->get_log_offset();

    key.append(participant_);
    key.append("_");
    key.append(std::to_string(store_log_id));
    key.append("_");
    key.append(std::to_string(log_offset));
  }

  return ret;
}

int ObLogEntryTask::get_log_id(uint64_t &log_id)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("invalid redo_node", K(redo_node_));
    ret = OB_INVALID_DATA;
  } else {
    log_id = redo_node_->get_store_log_id();
  }

  return ret;
}

int ObLogEntryTask::get_log_offset(int32_t &log_offset)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("invalid redo_node", K(redo_node_));
    ret = OB_INVALID_DATA;
  } else {
    log_offset = redo_node_->get_log_offset();
  }

  return ret;
}

int ObLogEntryTask::get_data_len(int64_t &data_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("invalid redo_node", K(redo_node_));
    ret = OB_INVALID_DATA;
  } else {
    data_len = redo_node_->get_data_len();
  }

  return ret;
}

int ObLogEntryTask::set_data_and_readed_status(bool is_big_row, char *data, int64_t data_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("invalid redo_node", K(redo_node_));
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(redo_node_->set_data_info(data, data_len))) {
    LOG_ERROR("redo_node_ set_data_info failed", KR(ret), K(data), K(data_len));
  } else {
    // Firset set_data_info, then set is_readed status
    redo_node_->set_readed();
    is_big_row_ = is_big_row;
  }

  return ret;
}

void *ObLogEntryTask::alloc(const int64_t size)
{
  void *alloc_ret = NULL;

  if (size > 0) {
    alloc_ret = arena_allocator_.alloc(size);
  }

  return alloc_ret;
}

// NOTE: For ObArenaAllocator: virtual void free(void *ptr) do nothing
void ObLogEntryTask::free(void *ptr)
{
  arena_allocator_.free(ptr);
  ptr = NULL;
}

int ObLogEntryTask::rc_callback()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("invalid redo_node", K(redo_node_));
    ret = OB_INVALID_DATA;
  } else {
    const bool is_stored = redo_node_->is_stored();

    if (is_stored) {
      if (is_big_row_) {
        // big row
        char *data = redo_node_->get_data();

        if (OB_ISNULL(data)) {
          LOG_ERROR("data is NULL");
          ret = OB_ERR_UNEXPECTED;
        } else {
          ob_free(data);
          data = NULL;
        }
      } else {
      }
    } else {
      // data is in memory, do nothing
    }
  }

  return ret;
}

int ObLogEntryTask::add_stmt(const uint64_t row_index, IStmtTask *stmt_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_ID == row_index)
      || OB_ISNULL(stmt_task)) {
    LOG_ERROR("invalid argument", K(row_index), KPC(stmt_task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(stmt_list_.add(stmt_task))) {
    LOG_ERROR("add stmt task into stmt_list fail", KR(ret), K(stmt_list_), KP(stmt_task));
  } else {
    stmt_task->set_row_index(row_index);
  }

  return ret;
}

// The DML STMT is incremented strictly according to SQL_NO/SEQ_NO. We only need to find the first statement greater than SQL_NO/SEQ_NO and rollback that statement and subsequent statements
int ObLogEntryTask::revert_by_rollback_savepoint(const uint64_t current_log_id,
    const uint64_t rollback_log_id,
    const uint64_t row_index,
    const int64_t rollback_no)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(revert_dml_stmt_(current_log_id, rollback_log_id, rollback_no))) {
    LOG_ERROR("revert_dml_stmt_ fail", KR(ret), K(row_index), K(rollback_no));
  }

  return ret;
}

// TODO Later optimization: rollback List supports sorting by LogID to speed up rollback
int ObLogEntryTask::revert_by_rollback_savepoints(const uint64_t current_log_id,
    const RollbackList &rollback_list)
{
  int ret = OB_SUCCESS;
  RollbackNode *rollback_node = rollback_list.head_;

  while (NULL != rollback_node) {
    uint64_t rollback_log_id = rollback_node->get_log_id();
    const common::ObArray<int64_t>& rollback_seqs = rollback_node->get_rollback_seqs();

    if (rollback_log_id <= current_log_id) {
      // rollback_log_id < current_log_id: we can not rollback
      // rollback_log_id == current_log_id: we hava rollbacked based on
      // revert_by_rollback_savepoint(const uint64_t, const int64_t) API
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < rollback_seqs.size(); idx++) {
        const int64_t rollback_no = rollback_seqs[idx];

        if (OB_FAIL(revert_dml_stmt_(current_log_id, rollback_log_id, rollback_no))) {
          LOG_ERROR("revert_dml_stmt_ fail", KR(ret), K(rollback_no));
        }
      }
    }

    if (OB_SUCC(ret)) {
      rollback_node = static_cast<RollbackNode *>(rollback_node->next_);
    }
  }

  return ret;
}

int ObLogEntryTask::revert_dml_stmt_(const uint64_t current_log_id,
    const uint64_t rollback_log_id,
    const int64_t rollback_no)
{
  int ret = OB_SUCCESS;
  const int64_t total_stmt_num = stmt_list_.num_;
  int64_t stmt_num = 0;
  bool found = false;
  IStmtTask *stmt_task = stmt_list_.head_;
  IStmtTask *pre_task = NULL;

  while (OB_SUCC(ret) && NULL != stmt_task && !found) {
    DmlStmtTask *dml_task = dynamic_cast<DmlStmtTask *>(stmt_task);

    if (OB_ISNULL(dml_task)) {
      LOG_ERROR("dynamic cast to DmlStmtTask fail", K(stmt_task), K(dml_task));
      ret = OB_ERR_UNEXPECTED;
    } else {
      const int64_t stmt_no = dml_task->get_row_seq_for_rollback();
      // Statement that SQL_NO/SEQ_NO is less thean or equal to SQL_NO/SEQ_NO by rollback savepoint is not processed
      if (stmt_no <= rollback_no) {
        pre_task = stmt_task;
        stmt_num++;
        stmt_task = stmt_task->get_next();
      } else {
        found = true;
      }
    }
  }

  if (OB_SUCC(ret) && found) {
    if (NULL == pre_task) {
      stmt_list_.head_ = NULL;
      stmt_list_.tail_ = NULL;
    } else {
      pre_task->set_next(NULL);
      stmt_list_.tail_ = pre_task;
    }
    stmt_list_.num_ = stmt_num;

    while (OB_SUCC(ret) && NULL != stmt_task) {
      DmlStmtTask *dml_task = dynamic_cast<DmlStmtTask *>(stmt_task);
      if (OB_ISNULL(dml_task)) {
        LOG_ERROR("dynamic cast to DmlStmtTask fail", K(stmt_task), K(dml_task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        stmt_task = stmt_task->get_next();
        dml_task->~DmlStmtTask();
        free(dml_task);
        dml_task = NULL;
      }
    }
  }

  if (OB_SUCC(ret)) {
    _LOG_INFO("[SAVEPOINT][DML] PART_TRANS=%s/%s CUR_LOG_ID=%ld ROLLBACK[LOG_ID=%ld SEQ=%ld] STMT_CNT=%ld/%ld",
        participant_, to_cstring(trans_id_), current_log_id, rollback_log_id, rollback_no, total_stmt_num, stmt_num);
  }

  return ret;
}

int64_t ObLogEntryTask::inc_formatted_stmt_num()
{
  return ATOMIC_AAF(&formatted_stmt_num_, 1);
}

int ObLogEntryTask::link_row_list(int64_t &row_ref_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("redo_node is NULL", KPC(redo_node_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    DmlStmtTask *stmt_task = static_cast<DmlStmtTask *>(stmt_list_.head_);
    redo_node_->set_valid_row_num(0);

    while (OB_SUCC(ret) && NULL != stmt_task) {
      DmlStmtTask *next_stmt = static_cast<DmlStmtTask *>(stmt_task->get_next());
      ObLogBR *br = stmt_task->get_binlog_record();
      bool need_link = true;

      if (OB_ISNULL(br)) {
        LOG_ERROR("binlog record in statement is invalid", KPC(stmt_task), K(br));
        ret = OB_ERR_UNEXPECTED;
      } else if (! br->is_valid()) {
        // ignore invalid br
        need_link = false;
        // recycle Binlog Record
        LOG_DEBUG("br is not valid", K(*this), "valid_row_num", redo_node_->get_valid_row_num());

        if (OB_FAIL(revert_binlog_record_(br))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("revert binlog record fail", KR(ret), K(br), K(stmt_task), KPC(stmt_task));
          }
        } else {
          br = NULL;
        }
      } else {
        // do nothing
      }

      if (OB_SUCC(ret) && need_link) {
        redo_node_->inc_valid_row_num();
        stmt_task->set_next(NULL);

        if (NULL == redo_node_->get_row_head()) {
          redo_node_->set_row_head(stmt_task);
          redo_node_->set_row_tail(stmt_task);
        } else {
          redo_node_->get_row_tail()->next_ = stmt_task;
          redo_node_->set_row_tail(stmt_task);
        }
      }

      if (OB_SUCC(ret)) {
        stmt_task = next_stmt;
      }
    } // while

    if (OB_SUCC(ret)) {
      // Note: First set ref count before set formatted status, to avoid Sortter has get Dml Stmt
      set_row_ref_cnt(redo_node_->get_valid_row_num());
      row_ref_cnt = row_ref_cnt_;

      if (OB_FAIL(set_redo_log_formatted())) {
        LOG_ERROR("set_redo_log_formatted fail", KR(ret));
      }
    }
  }

  return ret;
}

int ObLogEntryTask::revert_binlog_record_(ObLogBR *br)
{
  int ret = OB_SUCCESS;
  ILogRecord *br_data = NULL;
  IObLogBRPool *br_pool = TCTX.br_pool_;

  if (OB_ISNULL(br_pool)) {
    LOG_ERROR("invalid resource collector", K(br_pool));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(br)) {
    LOG_ERROR("binlog record is invalid", K(br));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(br_data = br->get_data())) {
    LOG_ERROR("binlog record data is invalid", K(br));
    ret = OB_INVALID_ARGUMENT;
  } else {
    br->set_host(NULL);
    br_pool->free(br);
  }

  return ret;
}

int ObLogEntryTask::set_redo_log_parsed()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("redo_node is NULL", KPC(redo_node_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    redo_node_->set_parsed();
  }

  return ret;
}

int ObLogEntryTask::set_redo_log_formatted()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("redo_node is NULL", KPC(redo_node_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    redo_node_->set_formatted();
  }

  return ret;
}

int ObLogEntryTask::get_valid_row_num(int64_t &valid_row_num)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("redo_node_ is NULL", KPC(redo_node_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    valid_row_num = redo_node_->get_valid_row_num();
  }

  return ret;
}

int64_t ObLogEntryTask::dec_row_ref_cnt()
{
  return ATOMIC_AAF(&row_ref_cnt_, -1);
}

void ObLogEntryTask::set_row_ref_cnt(const int64_t row_ref_cnt)
{
  (void)ATOMIC_SET(&row_ref_cnt_, row_ref_cnt);
}

////////////////////////////////////////////////////////////////////////////////////

PartTransTask::PartTransTask() :
    ObLogResourceRecycleTask(ObLogResourceRecycleTask::PART_TRANS_TASK),
    serve_state_(SERVED),
    type_(TASK_TYPE_UNKNOWN),
    exec_tenant_id_(OB_INVALID_TENANT_ID),
    partition_(),
    pkey_str_(NULL),
    timestamp_(OB_INVALID_TIMESTAMP),
    is_trans_id_inited_(false),
    trans_id_(),
    trans_id_str_(),
    prepare_log_id_(OB_INVALID_ID),
    cluster_id_(0),
    cluster_version_(0),
    pkey_and_log_id_str_(),
    row_no_(0),
    sorted_redo_list_(),
    big_row_dml_redo_node_(),
    rollback_list_(),
    global_trans_version_(OB_INVALID_VERSION),
    is_trans_committed_(false),
    is_trans_ready_to_commit_(false),
    checkpoint_seq_(0),
    global_trans_seq_(0),
    global_schema_version_(OB_INVALID_VERSION),
    participants_(NULL),
    participant_count_(0),
    local_schema_version_(OB_INVALID_VERSION),
    stmt_list_(),
    next_task_(NULL),
    ref_cnt_(0),
    data_ready_lock_(),
    is_data_ready_(false),
    wait_formatted_cond_(NULL),
    wait_data_ready_cond_(),
    allocator_(),
    trace_id_(),
    trace_info_(),
    prev_trans_arr_(),
    follow_trans_arr_(),
    reserve_field_(0),
    output_br_count_by_turn_(0)
{
}

PartTransTask::~PartTransTask()
{
  reset();
}

int PartTransTask::set_prepare_log_id(const uint64_t prepare_log_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == prepare_log_id))  {
    LOG_ERROR("invalid argument", K(prepare_log_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    prepare_log_id_ = prepare_log_id;
  }
  return ret;
}

void PartTransTask::set_allocator(const int64_t page_size, ObIAllocator &large_allocator)
{
  allocator_.set_allocator(page_size, large_allocator);
}

void PartTransTask::set_prealloc_page(void *page)
{
  allocator_.set_prealloc_page(page);
}

void PartTransTask::revert_prealloc_page(void *&page)
{
  allocator_.revert_prealloc_page(page);
}

const char *PartTransTask::print_task_type(const TaskType type)
{
  const char *task_type_str = "UNKNOWN";

  switch (type) {
    case TASK_TYPE_UNKNOWN: {
      task_type_str = "UNKNOWN";
      break;
    }
    case TASK_TYPE_DML_TRANS: {
      task_type_str = "DML_TRANS";
      break;
    }
    case TASK_TYPE_DDL_TRANS: {
      task_type_str = "DDL_TRANS";
      break;
    }
    case TASK_TYPE_PART_HEARTBEAT: {
      task_type_str = "PART_HEARTBEAT";
      break;
    }
    case TASK_TYPE_GLOBAL_HEARTBEAT: {
      task_type_str = "GLOBAL_HEARTBEAT";
      break;
    }
    case TASK_TYPE_OFFLINE_PARTITION: {
      task_type_str = "OFFLINE_PARTITION";
      break;
    }
    case TASK_TYPE_NOT_SERVED_TRANS: {
      task_type_str = "NOT_SERVED_TRANS";
      break;
    }

    default: {
      task_type_str = "INVALID";
      break;
    }
  }

  return task_type_str;
}

void PartTransTask::set_pkey_info(const common::ObPartitionKey &partition,
    const char *pkey_str)
{
  partition_ = partition;
  pkey_str_ = pkey_str;
}

void PartTransTask::reset()
{
  destroy_participant_array_();

  serve_state_ = SERVED;
  type_ = TASK_TYPE_UNKNOWN;
  exec_tenant_id_ = OB_INVALID_TENANT_ID;
  partition_.reset();
  pkey_str_ = NULL;
  timestamp_ = OB_INVALID_TIMESTAMP;
  is_trans_id_inited_ = false;
  trans_id_.reset();
  trans_id_str_.reset();
  prepare_log_id_ = OB_INVALID_ID;
  cluster_id_ = 0;
  cluster_version_ = 0;
  pkey_and_log_id_str_.reset();
  row_no_ = 0;
  sorted_redo_list_.reset();
  big_row_dml_redo_node_.reset();
  rollback_list_.reset();
  global_trans_version_ = OB_INVALID_VERSION;
  is_trans_committed_ = false;
  is_trans_ready_to_commit_ = false;
  checkpoint_seq_ = 0;
  global_trans_seq_ = 0;
  global_schema_version_ = OB_INVALID_VERSION;
  participants_ = NULL;
  participant_count_ = 0;

  local_schema_version_ = OB_INVALID_VERSION;

  free_stmt_list();

  next_task_ = NULL;

  ref_cnt_ = 0;
  is_data_ready_ = false;
  wait_formatted_cond_ = NULL;

  // The trace_id memory does not need to be freed separately, the allocator frees it all together
  trace_id_.reset();
  trace_info_.reset();

  prev_trans_arr_.reset();
  follow_trans_arr_.reset();

  // reuse memory
  allocator_.reset();
  reserve_field_ = 0;
  output_br_count_by_turn_ = 0;
}

int PartTransTask::push_redo_log(const common::ObPartitionKey &pkey,
    const transaction::ObTransID &trans_id,
    const int64_t log_no,
    const uint64_t log_id,
    const int32_t log_offset,
    const int64_t tstamp,
    const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObMemtableMutatorMeta meta;
  int64_t pos = 0;
  const bool is_ddl_part = is_ddl_partition(pkey);
  const uint64_t tenant_id = pkey.get_tenant_id();
  const bool need_store_data = need_store_data_();

  if (OB_UNLIKELY(log_no < 0)
      || OB_UNLIKELY(OB_INVALID_ID == log_id)
      || OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len <= 0)) {
    LOG_ERROR("invalid arguments", K(log_no), K(log_id), KP(buf), K(buf_len));
    ret = OB_INVALID_ARGUMENT;
  }
  // deserialize meta
  else if (OB_FAIL(meta.deserialize(buf, buf_len, pos))) {
    LOG_ERROR("deserialize ObMemtableMutatorMeta fail", KR(ret), K(meta), K(log_no),
        K(log_id), K(buf), K(buf_len), K(pos));
  }
  // Check the length of the data recorded in the meta, it can't be smaller than the length of the redo data
  // otherwise the data does not match the one recorded in the meta
  else if (OB_UNLIKELY(meta.get_total_size() < buf_len)) {
    LOG_ERROR("meta total size is little than mutator log data size",
        K(meta), K(buf_len), K(log_id), K(log_no));
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(init_trans_id_info_(pkey, trans_id))) {
    LOG_ERROR("init_trans_id_info_ fail", KR(ret), K(pkey), K(trans_id));
  } else {
    const char *redo_data = buf + pos;
    int64_t redo_data_size = buf_len - pos;
    const uint8_t row_flags = meta.get_flags();

    if (meta.is_row_start()) {
      // If it is the start of a row, a new redo node is generated
      if (OB_FAIL(push_redo_on_row_start_(need_store_data, trans_id, meta, log_no, log_id, log_offset, redo_data, redo_data_size))) {
        if (OB_ENTRY_EXIST == ret) {
          // redo log duplicate
        } else {
          LOG_ERROR("push_redo_on_row_start_ fail", KR(ret), K(trans_id), K(meta), K(log_no), K(log_id), K(log_offset),
              KP(redo_data), K(redo_data_size));
        }
      }
    } else {
      // If it is not the beginning of a row, push data to the previous node
      if (OB_FAIL(push_redo_on_not_row_start_(meta, log_no, log_id, redo_data, redo_data_size))) {
        // If it is the first node, the redo log is incomplete
        if (OB_LOG_MISSING == ret) {
          LOG_WARN("[MISSING_LOG] first redo is not row start, must miss redo log",
              K(pkey), K(log_no), K(log_id), K(tstamp), K(trans_id), K(sorted_redo_list_), K(meta));
        } else {
          LOG_ERROR("push_redo_on_not_row_start_ fail", KR(ret), K(pkey), K(meta), K(log_no),
              K(log_id), K(tstamp), KP(redo_data), K(redo_data_size), K(trans_id));
        }
      }
    }

    // TODO AUTO mode
    if (OB_SUCC(ret)) {
      uint64_t store_log_id = OB_INVALID_ID;
      int32_t store_log_offset = -1;
      const char *data_buf = NULL;
      int64_t data_len = 0;
      bool is_row_completed = true;
      DmlRedoLogNode *big_row_redo = NULL;

      if (! is_ddl_part) {
        if (ObTransRowFlag::is_normal_row(row_flags)) {
          store_log_id = log_id;
          store_log_offset = log_offset;
          data_buf = redo_data;
          data_len = redo_data_size;
        } else if (ObTransRowFlag::is_big_row_end(row_flags)) {
          if (need_store_data) {
            big_row_redo = &big_row_dml_redo_node_;
          } else {
            big_row_redo = static_cast<DmlRedoLogNode*>(sorted_redo_list_.last_push_node_);
          }
          if (OB_ISNULL(big_row_redo)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("big_row_redo is NULL", KR(ret), K(need_store_data), K(big_row_redo), K(sorted_redo_list_));
          } else if (OB_UNLIKELY(! big_row_redo->is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("big_row_redo_ is not valid", KR(ret), KPC(big_row_redo));
          } else {
            store_log_id = big_row_redo->get_store_log_id();
            store_log_offset = big_row_redo->get_log_offset();
            data_buf = big_row_redo->get_data();
            data_len = big_row_redo->get_data_len();
          }
        } else {
          is_row_completed = false;
        }

        if (OB_SUCC(ret) && is_row_completed) {
          if (1 == TCONF.skip_parse_rollback_savepoint) {
          } else if (OB_FAIL(parse_rollback_savepoint_(store_log_id, store_log_offset, data_buf, data_len))) {
            LOG_ERROR("parse_rollback_savepoint_ fail", KR(ret), K(store_log_id), K(store_log_offset));
          } else {}
        } // have_savepoint

        // 1. DML data, a redo data aggregation will be dispatched
        // 2. duplicate redo logs won't dispatch
        if (OB_SUCC(ret) && need_store_data && is_row_completed) {
          if (OB_FAIL(get_and_submit_store_task_(tenant_id, row_flags, store_log_id, store_log_offset,
                  data_buf, data_len))) {
            LOG_ERROR("get_and_submit_store_task_ fail", KR(ret), K(tenant_id), K(row_flags),
                K(store_log_id), K(store_log_offset));
          }
        } // need_store_data
      }
    }
  }

  LOG_DEBUG("push redo log", KR(ret), K(is_ddl_part), K(pkey), K(log_no), K(log_id), K(tstamp), K(buf_len), K(meta),
      K(trans_id), K(sorted_redo_list_));

  return ret;
}

bool PartTransTask::need_store_data_() const
{
  bool bool_ret = false;
  const WorkingMode working_mode = TCTX.working_mode_;

  if (is_memory_working_mode(working_mode)) {
    bool_ret = false;
  } else if (is_storage_working_mode(working_mode)) {
    bool_ret = true;
  } else {
    // TODO AUTO MODE
  }

  return bool_ret;
}

int PartTransTask::free_big_row_()
{
  int ret = OB_SUCCESS;
  char *data = big_row_dml_redo_node_.get_data();

  if (OB_ISNULL(data)) {
    LOG_ERROR("data is NULL", K(big_row_dml_redo_node_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ob_free(data);
    big_row_dml_redo_node_.reset();
  }

  return ret;
}

int PartTransTask::init_trans_id_info_(const common::ObPartitionKey &pkey,
    const transaction::ObTransID &trans_id)
{
  int ret = OB_SUCCESS;

  if (is_trans_id_inited_) {
    // do nothing
  } else {
    trans_id_ = trans_id;

    if (OB_FAIL(to_string_trans_id())) {
      LOG_ERROR("to_string_trans_id fail", KR(ret), K(pkey), K(trans_id));
    } else {
      is_trans_id_inited_ = true;
    }
  }

  return ret;
}

int PartTransTask::push_redo_on_row_start_(const bool need_store_data,
    const transaction::ObTransID &trans_id,
    const ObMemtableMutatorMeta &meta,
    const int64_t log_no,
    const uint64_t log_id,
    const int32_t log_offset,
    const char *redo_data,
    const int64_t redo_data_size)
{
  int ret = OB_SUCCESS;
  // Length of the actual data, minus the meta information
  const int64_t mutator_row_size = meta.get_data_size();
  const bool is_ddl_part = is_ddl_partition(partition_);

  if (is_ddl_part) {
    if (OB_FAIL(push_ddl_redo_on_row_start_(meta, log_no, log_id, redo_data, redo_data_size, mutator_row_size))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_ERROR("push_ddl_redo_on_row_start_ fail", KR(ret), K(trans_id), K(meta), K(log_no), K(log_id),
            KP(redo_data), K(redo_data_size));
      }
    }
  } else {
    if (OB_FAIL(push_dml_redo_on_row_start_(need_store_data, meta, log_no, log_id, log_offset, redo_data, redo_data_size,
            mutator_row_size))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_ERROR("push_dml_redo_on_row_start_ fail", KR(ret), K(trans_id), K(meta), K(log_no), K(log_id),
            KP(redo_data), K(redo_data_size));
      }
    }
  }

  return ret;
}

int PartTransTask::push_ddl_redo_on_row_start_(const ObMemtableMutatorMeta &meta,
    const int64_t log_no,
    const uint64_t log_id,
    const char *redo_data,
    const int64_t redo_data_size,
    const int64_t mutator_row_size)
{
  int ret = OB_SUCCESS;
  DdlRedoLogNode *node = NULL;
  char *mutator_row_data = NULL;

  // alloc a Node
  if (OB_ISNULL(node = static_cast<DdlRedoLogNode *>(allocator_.alloc(sizeof(DdlRedoLogNode))))) {
    LOG_ERROR("allocate memory for DdlRedoLogNode fail", "size", sizeof(DdlRedoLogNode));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  // DDL partitioned transaction task allocates buffer of actual data length
  } else if (OB_ISNULL(mutator_row_data = static_cast<char *>(allocator_.alloc(mutator_row_size)))) {
    LOG_ERROR("allocate memory for mutator row data fail", K(mutator_row_size), K(meta));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    // Fill the data carried in this redo
    (void)MEMCPY(mutator_row_data, redo_data, redo_data_size);

    // reset redo log node
    node->reset(log_no, log_id, mutator_row_data, mutator_row_size, redo_data_size);

    // Push to redo list
    if (OB_FAIL(sorted_redo_list_.push(true/*is_data_in_memory*/, node))) {
      if (OB_ENTRY_EXIST == ret) {
        // redo log duplicate
      } else {
        LOG_ERROR("push node into redo log list fail", KR(ret), K(sorted_redo_list_), KPC(node));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != node) {
      node->reset();
      allocator_.free(node);
      node = NULL;
    }

    if (NULL != mutator_row_data) {
      allocator_.free(mutator_row_data);
      mutator_row_data = NULL;
    }
  }

  return ret;
}

int PartTransTask::push_dml_redo_on_row_start_(const bool need_store_data,
    const memtable::ObMemtableMutatorMeta &meta,
    const int64_t log_no,
    const uint64_t log_id,
    const int32_t log_offset,
    const char *redo_data,
    const int64_t redo_data_size,
    const int64_t mutator_row_size)
{
  int ret = OB_SUCCESS;
  char *mutator_row_data = NULL;
  DmlRedoLogNode *meta_node = NULL;
  const uint8_t row_flags = meta.get_flags();

  if (OB_ISNULL(meta_node = static_cast<DmlRedoLogNode *>(allocator_.alloc(sizeof(DmlRedoLogNode))))) {
    LOG_ERROR("allocate memory for DmlRedoLogNode fail", "size", sizeof(DmlRedoLogNode));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    if (! need_store_data) {
      // The allocator of PartTransTask alloc memory
      if (OB_ISNULL(mutator_row_data = static_cast<char *>(allocator_.alloc(mutator_row_size)))) {
        LOG_ERROR("allocate memory for mutator row data fail", K(mutator_row_size), K(meta));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        // Fill the data carried in this redo log
        (void)MEMCPY(mutator_row_data, redo_data, redo_data_size);

        meta_node->init_for_data_memory(log_no, log_id, log_offset, mutator_row_data, mutator_row_size, redo_data_size);
      }
    } else {
      // need store
      // reset redo log meta node
      meta_node->init_for_data_persistence(log_no, log_id, log_offset, mutator_row_size);

      // For big row, save the memory in temp DmlRedoLogNode
      if (ObTransRowFlag::is_big_row_start(row_flags)) {
        if (OB_ISNULL(mutator_row_data = static_cast<char *>(ob_malloc(mutator_row_size, "CDCBigRowMut")))) {
          LOG_ERROR("allocate memory for mutator row data fail", K(mutator_row_size), K(meta));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          // Fill the data carried in this redo
          (void)MEMCPY(mutator_row_data, redo_data, redo_data_size);

          big_row_dml_redo_node_.init_for_data_memory(log_no, log_id, log_offset,
              mutator_row_data, mutator_row_size, redo_data_size);
        }
      } else {
        // do nothing
      }
    }
  }

  if (OB_SUCC(ret)) {
    // Push to redo list
    if (OB_FAIL(sorted_redo_list_.push(! need_store_data/*is_data_in_memory*/, meta_node))) {
      if (OB_ENTRY_EXIST == ret) {
        // redo log duplicate]
      } else {
        LOG_ERROR("push node into redo log list fail", KR(ret), K(sorted_redo_list_), KPC(meta_node));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != meta_node) {
      meta_node->reset();
      allocator_.free(meta_node);
      meta_node = NULL;
    }

    if (ObTransRowFlag::is_big_row_start(row_flags)) {
      if (NULL != mutator_row_data) {
        ob_free(mutator_row_data);
        mutator_row_data = NULL;
      }
    }
  }

  return ret;
}

int PartTransTask::push_redo_on_not_row_start_(const ObMemtableMutatorMeta &meta,
    const int64_t log_no,
    const uint64_t log_id,
    const char *redo_data,
    const int64_t redo_data_size)
{
  int ret = OB_SUCCESS;
  const bool is_ddl_part = is_ddl_partition(partition_);

  if (is_ddl_part) {
    if (OB_FAIL(push_ddl_redo_on_not_row_start_(meta, log_no, log_id, redo_data, redo_data_size))) {
      if (OB_LOG_MISSING != ret) {
        LOG_ERROR("push_ddl_redo_on_not_row_start_ fail", KR(ret), K(partition_), K(meta), K(log_no),
            K(log_id), KP(redo_data), K(redo_data_size));
      }
    }
  } else {
    if (OB_FAIL(push_dml_redo_on_not_row_start_(meta, log_no, log_id, redo_data, redo_data_size))) {
      if (OB_LOG_MISSING != ret) {
        LOG_ERROR("push_dml_redo_on_not_row_start_ fail", KR(ret), K(partition_), K(meta), K(log_no),
            K(log_id), KP(redo_data), K(redo_data_size));
      }
    }
  }

  return ret;
}

// If it is not the beginning of the line, it must be consecutive to the previous log, i.e. the log number is a successor to the previous push log
//
// 1. if the redo list is empty, the first log is not the starting redo log of the partitioned transaction,
// to facilitate the processing of the LOB log, return OB_LOG_MISSING and ask the log to be ignored externally, and then resume the redo log from the first log
//
// 2. If log_no is not consecutive with the last push log, it means there is a data error, it is a bug of the observer, return OB_DISCONTINUOUS_LOG
int PartTransTask::push_ddl_redo_on_not_row_start_(const ObMemtableMutatorMeta &meta,
    const int64_t log_no,
    const uint64_t log_id,
    const char *redo_data,
    const int64_t redo_data_size)
{
  int ret = OB_SUCCESS;
  DdlRedoLogNode *last_redo = static_cast<DdlRedoLogNode *>(sorted_redo_list_.last_push_node_);

  if (sorted_redo_list_.log_num_ <= 0) {
    ret = OB_LOG_MISSING;
  } else if (OB_ISNULL(last_redo)) {
    LOG_ERROR("last redo node is invalid", K(sorted_redo_list_));
    ret = OB_ERR_UNEXPECTED;
  }
  // Append redo data to the previous node
  else if (OB_FAIL(last_redo->append_redo_log(log_no, log_id, redo_data, redo_data_size))) {
    LOG_ERROR("last redo append data fail", KR(ret), KPC(last_redo), K(log_no), K(log_id),
        K(redo_data_size));
  } else {
    // success
    LOG_DEBUG("LOB data append success", K(meta), K(log_no), K(log_id), K(redo_data_size),
        KPC(last_redo));
  }

  return ret;
}

int PartTransTask::push_dml_redo_on_not_row_start_(const memtable::ObMemtableMutatorMeta &meta,
    const int64_t log_no,
    const uint64_t log_id,
    const char *redo_data,
    const int64_t redo_data_size)
{
  int ret = OB_SUCCESS;
  DmlRedoLogNode *last_redo = static_cast<DmlRedoLogNode *>(sorted_redo_list_.last_push_node_);

  if (sorted_redo_list_.log_num_ <= 0) {
    ret = OB_LOG_MISSING;
  } else if (OB_ISNULL(last_redo)) {
    LOG_ERROR("last redo node is invalid", K(sorted_redo_list_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const bool is_stored = last_redo->is_stored();

    if (! is_stored) {
      if (OB_FAIL(last_redo->append_redo_log(log_no, log_id, redo_data, redo_data_size))) {
        LOG_ERROR("last redo append data fail", KR(ret), KPC(last_redo), K(log_no), K(log_id),
            K(redo_data_size));
      }
    } else {
      if (OB_FAIL(last_redo->update_redo_meta(log_no, log_id))) {
        LOG_ERROR("update_redo_meta fail", KR(ret), K(log_no), K(log_id));
      } else if (OB_UNLIKELY(! big_row_dml_redo_node_.is_valid())) {
        LOG_ERROR("big_row_dml_redo_node_ is not valid", K(big_row_dml_redo_node_));
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(big_row_dml_redo_node_.append_redo_log(log_no, log_id, redo_data, redo_data_size))) {
        LOG_ERROR("big_row_dml_redo_node_ append_redo_log fail", KR(ret), K(log_no), K(log_id),
            K(redo_data_size));
      } else {}
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("LOB data append success", K(meta), K(log_no), K(log_id), K(redo_data_size),
          KPC(last_redo));
    }
  }

  return ret;
}

int PartTransTask::parse_rollback_savepoint_(const uint64_t log_id,
    const int32_t log_offset,
    const char *data_buf,
    const int64_t data_len)
{
  int ret = OB_SUCCESS;
  IObLogPartTransParser *part_trans_parser = TCTX.part_trans_parser_;
  RollbackNode *rollback_node = static_cast<RollbackNode *>(allocator_.alloc(sizeof(RollbackNode)));
  if (OB_ISNULL(part_trans_parser)) {
    LOG_ERROR("part_trans_parser is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(rollback_node)) {
    LOG_ERROR("rollback_node is NULL");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    new(rollback_node) RollbackNode(log_id);
    bool is_rollback_node_valid = false;

    if (OB_FAIL(part_trans_parser->parse_rollback(log_id, log_offset, data_buf, data_len, *this,
            is_rollback_node_valid, *rollback_node))) {
      LOG_ERROR("part_trans_parser parse_rollback fail", KR(ret), K(log_id), K(log_offset));
    } else if (is_rollback_node_valid) {
      if (OB_FAIL(rollback_list_.add(rollback_node))) {
        LOG_ERROR("rollback_list_ add fail", KR(ret), KPC(rollback_node), K(rollback_list_));
      } else {
        LOG_INFO("rollback_list add succ", KPC(rollback_node), K(rollback_list_));
      }
    } else {}
  }

  return ret;
}

int PartTransTask::get_and_submit_store_task_(const uint64_t tenant_id,
    const uint8_t row_flags,
    const uint64_t store_log_id,
    const int32_t store_log_offset,
    const char *data_buf,
    const int64_t data_len)
{
  int ret = OB_SUCCESS;
  ObLogStoreTask *store_task = NULL;

  // Allocate ObLogStoreTask and init
  if (OB_ISNULL(store_task = ObLogStoreTaskFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("store_task is NULL", KR(ret));
  } else if (OB_FAIL(store_task->init(tenant_id, pkey_str_, store_log_id, store_log_offset,
          data_buf, data_len, this))) {
    LOG_ERROR("store_task init fail", KR(ret), K(tenant_id), K(pkey_str_), K(store_log_id), K(store_log_offset),
        K(data_len));
  } else {}

  // Dispatch to BatchBuffer
  if (OB_SUCC(ret)) {
    IObLogBatchBuffer *batch_buffer = TCTX.batch_buffer_;
    LOG_DEBUG("batch_buffer submit", KPC(store_task));

    if (OB_ISNULL(batch_buffer)) {
      LOG_ERROR("batch_buffer is NULL");
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(batch_buffer->submit(store_task))) {
      LOG_ERROR("batch_buffer submit fail", KR(ret));
    } else {}
  }

  if (OB_SUCC(ret)) {
    if (ObTransRowFlag::is_big_row_end(row_flags)) {
      if (OB_FAIL(free_big_row_())) {
        LOG_ERROR("free_big_row_ failed", KR(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    ObLogStoreTaskFactory::free(store_task);
    store_task = NULL;
  }

  return ret;
}

int PartTransTask::prepare(const common::ObPartitionKey &partition,
    const int64_t timestamp,
    const ObTransID &trans_id,
    const uint64_t prepare_log_id,
    const uint64_t cluster_id,
    const ObString &trace_id,
    const ObString &trace_info,
    const transaction::ObElrTransInfoArray &elt_trans_info_array)
{
  int ret = OB_SUCCESS;
  const bool is_ddl_part = is_ddl_table(partition.get_table_id());

  if (OB_UNLIKELY(TASK_TYPE_UNKNOWN != type_)) {
    LOG_ERROR("type is not expected", "type", print_task_type(type_),
        "expected_type", "TASK_TYPE_UNKNOWN");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(! partition.is_valid())
      || OB_UNLIKELY(timestamp <= 0)
      || OB_UNLIKELY(! trans_id.is_valid())
      || OB_UNLIKELY(OB_INVALID_ID == prepare_log_id)) {
    LOG_ERROR("invalid arguemnts", K(partition), K(timestamp), K(trans_id), K(prepare_log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(init_trans_id_info_(partition, trans_id))) {
    LOG_ERROR("init_trans_id_info_ fail", KR(ret), K(partition), K(trans_id));
  } else if (OB_FAIL(init_trace_id_(trace_id))) {
    LOG_ERROR("init_trace_id_ fail", KR(ret), K(trace_id));
  } else if (OB_FAIL(init_trace_info_(trace_info))) {
    LOG_ERROR("init_trace_info_ fail", KR(ret), K(trace_info));
  } else if (OB_FAIL(prev_trans_arr_.assign(elt_trans_info_array))) {
    LOG_ERROR("prev_trans_arr_ assign fail", KR(ret), K(elt_trans_info_array));
  } else if (OB_FAIL(to_string_pkey_and_log_id_(prepare_log_id))) {
    LOG_ERROR("to_string_pkey_and_log_id_ fail", KR(ret), K(prepare_log_id));
  } else {
    // Set type to DDL transaction if it is a DDL table, otherwise it is a DML transaction
    type_ = is_ddl_part ? TASK_TYPE_DDL_TRANS : TASK_TYPE_DML_TRANS;
    timestamp_ = timestamp;
    prepare_log_id_ = prepare_log_id;
    cluster_id_ = cluster_id;
  }

  LOG_DEBUG("PartTransTask::prepare", KR(ret), K(partition), K(type_), K(sorted_redo_list_),
      K(timestamp), K(trans_id), K(prepare_log_id), K(cluster_id), K(trace_id),
      "count", prev_trans_arr_.count(), K(elt_trans_info_array));

  return ret;
}

int PartTransTask::commit(const int64_t global_trans_version,
    const PartitionLogInfoArray &participants,
    const bool is_ready_to_commit,
    const int64_t first_log_ts,
    const transaction::ObTransID &trans_id,
    const TransCommitInfo &trans_commit_info,
    PartTransDispatcher &part_trans_dispatcher)
{
  int ret = OB_SUCCESS;
  const bool is_ddl_part = is_ddl_partition(partition_);

  if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == first_log_ts)) {
    LOG_ERROR("first_log_ts is invalid", K(first_log_ts));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(set_commit_info_(global_trans_version, participants))) {
    LOG_ERROR("set_commit_info_ fail", KR(ret), K(global_trans_version), K(participants));
  }
  // Pre-commit or not
  else if (is_ready_to_commit) {
    // Set up a pre-committed state and wait until the checkpoint advances to update the state or update the state via callbacks/checks in early unlock scenarios
    // see: update_trans_committed_status()
    ATOMIC_STORE(&is_trans_ready_to_commit_, true);

    // Pre-commit status, handling precursor transactions in early unlock scenarios
    if (OB_FAIL(handle_elr_prev_trans_(part_trans_dispatcher, first_log_ts))) {
      LOG_ERROR("handle_elr_prev_trans_ fail", KR(ret), K(partition_), K(prepare_log_id_), K(trans_id_),
          K(prev_trans_arr_), K(follow_trans_arr_));
    } else {
      // succ
    }
  } else {
    ATOMIC_STORE(&is_trans_committed_, true);

    if (OB_FAIL(part_trans_dispatcher.insert_commit_trans(trans_id, trans_commit_info))) {
      LOG_ERROR("trans_commit_map insert fail", KR(ret), K(trans_id), K(trans_commit_info));
    }
    // Transaction commit status. Handling subsequent transactions in early unlock scenarios
    else if (OB_FAIL(handle_elr_follow_trans_(part_trans_dispatcher))) {
      LOG_ERROR("handle_elr_follow_trans_ fail", KR(ret), K(partition_), K(prepare_log_id_), K(trans_id_),
          K(prev_trans_arr_), K(follow_trans_arr_));
    }

    if (! is_ddl_part) {
      set_ref_cnt(sorted_redo_list_.get_node_number() + 1);
    }
  }

  LOG_DEBUG("commit_normal_trans", KR(ret), "pkey", partition_, K(trans_id), K(is_ddl_part),
      K(is_ready_to_commit), K(is_trans_ready_to_commit_),
      K(is_trans_committed_), K(global_trans_version), K(participants),
      "prev_trans_arr_cnt", prev_trans_arr_.count(), K(prev_trans_arr_),
      "follow_trans_arr_cnt", follow_trans_arr_.count(), K(follow_trans_arr_));

  return ret;
}

int PartTransTask::set_commit_info_(const int64_t global_trans_version,
    const PartitionLogInfoArray &participants)
{
  int ret = OB_SUCCESS;

  // A commit transaction may only be a DML transaction or a DDL transaction, all other types are reported as errors
  if (OB_UNLIKELY(TASK_TYPE_DML_TRANS != type_ && TASK_TYPE_DDL_TRANS != type_)) {
    LOG_ERROR("can not commit without prepare", K_(type), K_(timestamp), K_(partition), K_(trans_id));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_UNLIKELY(global_trans_version < 0)) {
    LOG_ERROR("invalid argument", K(global_trans_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(participants.count() <= 0)) {
    LOG_ERROR("participants are empty", K(participants), K(global_trans_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(init_participant_array_(participants))) {
    LOG_ERROR("init participants array fail", KR(ret), K(participants));
  } else {
    global_trans_version_ = global_trans_version;
  }

  return ret;
}

int PartTransTask::handle_elr_prev_trans_(PartTransDispatcher &part_trans_dispatcher,
    const int64_t first_log_ts)
{
  int ret = OB_SUCCESS;
  const int64_t prev_trans_arr_cnt = prev_trans_arr_.count();
  int64_t prev_trans_commit_cnt = 0;
  bool has_done = false;
  const char *commit_reason = "NONE";

  if (prev_trans_arr_cnt > 0) {
    for (int64_t idx = 0; OB_SUCC(ret) && ! has_done && idx < prev_trans_arr_cnt; ++idx) {
      const ObElrTransInfo &elr_trans_info = prev_trans_arr_.at(idx);
      const ObTransID &prev_trans_id = elr_trans_info.get_trans_id();
      const int64_t prev_trans_commit_version = elr_trans_info.get_commit_version();
      const int trans_res = elr_trans_info.get_result();

      if (OB_UNLIKELY(! ObTransResultState::is_valid(trans_res))) {
        LOG_ERROR("elr_trans_info result is not valid", K(trans_res), K(elr_trans_info), K(prev_trans_arr_));
        ret = OB_INVALID_ARGUMENT;
      } else if (ObTransResultState::is_commit(trans_res)) {
        ++prev_trans_commit_cnt;
      } else if (ObTransResultState::is_abort(trans_res)) {
        // The predecessor transaction is in the abort state and is not processed here, essentially the current transaction writes the abort log
        has_done = true;
      } else {
        // The predecessor transaction is in UNKNOWN
        PartTransTask *prev_trans_task = NULL;
        PartTransID prev_part_trans_id(prev_trans_id, partition_);
        bool is_commit_trans = false;

        if (OB_FAIL(part_trans_dispatcher.get_task(prev_part_trans_id, prev_trans_task))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // 1. Find that the predecessor transaction does not exist, indicating that the state of the predecessor transaction must have been determined
            // 2. Determine if the commit version of the predecessor transaction is less than the first log timestamp, no longer processed
            // 3. Find if the predecessor transaction is abort
            ret = OB_SUCCESS;

            if (prev_trans_commit_version < first_log_ts) {
              // The predecessor transaction was not synchronized, and is not handled here
              has_done = true;
            } else {
              if (OB_FAIL(part_trans_dispatcher.find_commit_trans_info(prev_trans_id, is_commit_trans))) {
                LOG_ERROR("part_trans_dispatcher find_commit_trans_info fail", KR(ret),
                    K(is_commit_trans));
              } else if (! is_commit_trans) {
                // The predecessor transaction is in the abort state and is not processed here, essentially the current transaction writes the abort log
                has_done = true;
              } else {
                // The predecessor transaction already commit
                ++prev_trans_commit_cnt;
                commit_reason = "prev_trans_be_commit_trans_map";
              }
            }
          } else {
            LOG_ERROR("part_trans_dispatcher get_task fail", KR(ret), K(prev_part_trans_id), KPC(prev_trans_task));
          }
        } else if (OB_ISNULL(prev_trans_task)) {
          LOG_ERROR("prev_trans_task is NULL", K(prev_part_trans_id), K(prev_trans_task));
          ret = OB_ERR_UNEXPECTED;
        // The predecessor transaction already commit
        } else if (prev_trans_task->is_trans_committed()) {
          ++prev_trans_commit_cnt;
          commit_reason = "prev_trans_be_commited";
        // The predecessor transaction not commit, register a callback
        } else if (OB_FAIL(prev_trans_task->register_elt_follow_trans(trans_id_))) {
          LOG_ERROR("prev_trans_task register_elt_follow_trans fail", KR(ret), K(partition_), K(trans_id_));
        } else {
          // succ
        }
        LOG_DEBUG("elr_prev_trans", K(commit_reason), K(partition_), K(trans_id_), K(prev_trans_arr_), K(elr_trans_info),
            K(is_commit_trans));
      }
    } // for

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_elr_trans_(prev_trans_arr_cnt, prev_trans_commit_cnt, commit_reason, part_trans_dispatcher))) {
        LOG_ERROR("commit_elr_trans_ fail", KR(ret), K(partition_), K(prepare_log_id_), K(trans_id_),
            K(prev_trans_arr_cnt), K(prev_trans_commit_cnt), K(commit_reason));
      } else {
        // succ
      }
    }
  } else {
    // no predecessor transaction
    // do nothing
  }

  return ret;
}

int PartTransTask::commit_elr_trans_(const int64_t prev_trans_arr_cnt,
    const int64_t prev_trans_commit_cnt,
    const char *commit_reason,
    PartTransDispatcher &part_trans_dispatcher)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! ATOMIC_LOAD(&is_trans_ready_to_commit_))) {
    LOG_ERROR("is_trans_ready_to_commit is false, unexcepted trans state", K(partition_), K(trans_id_));
    ret = OB_STATE_NOT_MATCH;
  } else if (prev_trans_arr_cnt == prev_trans_commit_cnt) {
    ATOMIC_STORE(&is_trans_committed_, true);

    LOG_DEBUG("commit_elr_trans", K(commit_reason), K(partition_), K(trans_id_), K(prev_trans_arr_cnt), K(prev_trans_commit_cnt),
        "is_ready_to_commit", ATOMIC_LOAD(&is_trans_ready_to_commit_),
        "is_trans_committed", ATOMIC_LOAD(&is_trans_committed_));

    // Transaction commit status. Handling subsequent transactions in early unlock scenarios
    if (OB_FAIL(handle_elr_follow_trans_(part_trans_dispatcher))) {
      LOG_ERROR("handle_elr_follow_trans_ fail", KR(ret), K(partition_), K(prepare_log_id_), K(trans_id_),
          K(prev_trans_arr_), K(follow_trans_arr_));
    }
  } else {
    // do nothing
  }

  return ret;
}

int PartTransTask::handle_elr_follow_trans_(PartTransDispatcher &part_trans_dispatcher)
{
  int ret = OB_SUCCESS;
  const int64_t follow_trans_arr_cnt = follow_trans_arr_.count();

  for (int64_t idx = 0; OB_SUCC(ret) && idx < follow_trans_arr_cnt; ++idx) {
    const ObTransID &follow_trans_id = follow_trans_arr_.at(idx);
    PartTransTask *follow_task = NULL;
    PartTransID follow_part_trans_id(follow_trans_id, partition_);

    if (OB_FAIL(part_trans_dispatcher.get_task(follow_part_trans_id, follow_task))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // Finding that a successor transaction does not exist means that the status of the successor transaction must have been determined, no special treatment is needed here
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("part_trans_dispatcher get_task fail", KR(ret), K(follow_part_trans_id), KPC(follow_task));
      }
    } else if (OB_ISNULL(follow_task)) {
      LOG_ERROR("follow_task is NULL", K(follow_part_trans_id), K(follow_task));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(follow_task->handle_elt_trans_callback(trans_id_, part_trans_dispatcher))) {
      LOG_ERROR("handle_elt_trans_callback_ fail", KR(ret), K(trans_id_));
    } else {
      // succ
    }
  } // for

  // The list of successor transactions needs to be cleared after processing
  // Three scenarios determine the commit status of a transaction and trigger the processing of the list of successor transactions
  // 1. commit()
  // 2. commit_elr_trans_()
  // 3. update_trans_committed_status
  // Single-unit single-partition/single-unit multi-partition early unlock scenarios are currently supported, so 2 and 3 may be called repeatedly, as the task will not be issued until after the logs have been parsed
  if (OB_SUCC(ret)) {
    follow_trans_arr_.reset();
  }

  return ret;
}

int PartTransTask::handle_elt_trans_callback(const transaction::ObTransID &trans_id,
    PartTransDispatcher &part_trans_dispatcher)
{
  int ret = OB_SUCCESS;
  const int64_t prev_trans_arr_cnt = prev_trans_arr_.count();
  int64_t prev_trans_commit_cnt = 0;
  bool is_prev_trans_exist = false;

  for (int64_t idx = 0; OB_SUCC(ret) && idx < prev_trans_arr_cnt; ++idx) {
    ObElrTransInfo &elr_trans_info = prev_trans_arr_.at(idx);
    const ObTransID &prev_trans_id = elr_trans_info.get_trans_id();
    const int trans_res = elr_trans_info.get_result();

    // Find the corresponding transaction in the precursor array
    if (trans_id == prev_trans_id) {
      if (OB_UNLIKELY(! ObTransResultState::is_unknown(trans_res))) {
        LOG_ERROR("elr_trans_info result is not unknown", K(trans_res), K(elr_trans_info),
            K(prev_trans_arr_));
        ret = OB_STATE_NOT_MATCH;
      } else {
        // Change the status to commit and count the number of committed transactions
        is_prev_trans_exist = true;
        elr_trans_info.set_result(ObTransResultState::COMMIT);
        ++prev_trans_commit_cnt;
      }
    } else if (ObTransResultState::is_commit(trans_res)) {
      ++prev_trans_commit_cnt;
    } else {
      // do nothing
    }
  } // for

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(! is_prev_trans_exist)) {
      LOG_ERROR("unfound prev trans is unexcepted", K(partition_), K(trans_id), K(prev_trans_arr_));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(commit_elr_trans_(prev_trans_arr_cnt, prev_trans_commit_cnt, "follow_trans_callback", part_trans_dispatcher))) {
      LOG_ERROR("commit_elr_trans_ fail", KR(ret), K(partition_), K(prepare_log_id_), K(trans_id_),
          K(prev_trans_arr_cnt), K(prev_trans_commit_cnt));
    } else {
      // succ
    }
  }

  return ret;
}

int PartTransTask::register_elt_follow_trans(const transaction::ObTransID &follow_trans_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(follow_trans_arr_.push_back(follow_trans_id))) {
    LOG_ERROR("follow_trans_arr_ push_back fail", KR(ret), K(partition_), K(trans_id_), K(follow_trans_id));
  }

  return ret;
}

int PartTransTask::update_trans_committed_status(const int64_t checkpoint,
    PartTransDispatcher &part_trans_dispatcher)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_VERSION == checkpoint)) {
    LOG_ERROR("checkpoint is invalid", K(partition_), K(checkpoint));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Single machine multipartition transactions, global_trans_version to determine if they are ready for distribution
    if (ATOMIC_LOAD(&is_trans_ready_to_commit_)) {
      bool bool_ret = (global_trans_version_ <= checkpoint);

      // Atoms set is_trans_committed to true if it can be committed and sent, ensuring that partitioned tasks can be committed and validated
      if (bool_ret) {
        ATOMIC_STORE(&is_trans_committed_, true);

        // Transaction commit status. Handling subsequent transactions in early unlock scenarios
        if (OB_FAIL(handle_elr_follow_trans_(part_trans_dispatcher))) {
          LOG_ERROR("handle_elr_follow_trans_ fail", KR(ret), K(partition_), K(prepare_log_id_), K(trans_id_),
              K(prev_trans_arr_), K(follow_trans_arr_));
        }
      }
    }
  }

  return ret;
}

int PartTransTask::try_to_set_data_ready_status()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! is_dml_trans())) {
    LOG_ERROR("Not a dml trans is unexcepted", KPC(this));
    ret = OB_STATE_NOT_MATCH;
  } else if (is_data_ready()) {
    // do nothing
  } else if (is_contain_empty_redo_log()) {
    set_data_ready();
  } else {
    // Ensure the correctness of concurrent processing of Storager
    // and PartTransDispatcher-try_to_set_data_ready_status
    ObByteLockGuard guard(data_ready_lock_);
    const bool is_part_trans_served = is_served();

    if (OB_UNLIKELY(! is_part_trans_served)) {
      LOG_ERROR("part trans unserved is unexcepted", K(is_part_trans_served), KPC(this));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(check_dml_redo_node_ready_and_handle_())) {
      LOG_ERROR("check_dml_redo_node_ready_and_handle_ fail", KR(ret), KPC(this));
    } else {}
  }

  return ret;
}

int PartTransTask::handle_log_callback()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_ddl_part())) {
    LOG_ERROR("Not a dml part is unexcepted", KPC(this));
    ret = OB_STATE_NOT_MATCH;
  } else {
    ObByteLockGuard guard(data_ready_lock_);
    sorted_redo_list_.inc_ready_node_num();
    const bool is_part_trans_served = is_served();

    if (is_part_trans_served) {
      if (! is_trans_committed()) {
        // do nothing
      } else {
        if (OB_FAIL(check_dml_redo_node_ready_and_handle_())) {
          LOG_ERROR("check_dml_redo_node_ready_and_handle_ fail", KR(ret), KPC(this));
        }
      }
    } else {
      if (OB_FAIL(handle_unserved_trans_())) {
        LOG_ERROR("handle_unserved_trans_ fail", KR(ret), KPC(this));
      }
    }
  }

  return ret;
}

int PartTransTask::check_dml_redo_node_ready_and_handle_()
{
  int ret = OB_SUCCESS;
  bool is_equal = false;

  if (OB_UNLIKELY(is_ddl_part())) {
    LOG_ERROR("Not a dml part is unexcepted", KPC(this));
    ret = OB_STATE_NOT_MATCH;
  } else if (is_data_ready()) {
    // Double check, do nothing when data is ready
  } else if (OB_FAIL(sorted_redo_list_.check_node_num_equality(is_equal))) {
    LOG_ERROR("sorted_redo_list_ check_node_num_equality fail", KR(ret));
  } else if (is_equal) {
    set_data_ready();
  } else {
    // do nothing
    LOG_DEBUG("check_dml_redo_node_ready_and_handle_ not equal", K(is_equal), K_(sorted_redo_list), KPC(this));
  }

  return ret;
}

int PartTransTask::handle_unserved_trans()
{
  int ret = OB_SUCCESS;
  // Ensure the correctness of concurrent processing of Storager and PartTransDispatcher
  ObByteLockGuard guard(data_ready_lock_);

  // set unserved statue
  set_unserved_();

  if (OB_FAIL(handle_unserved_trans_())) {
    LOG_ERROR("handle_unserved_trans_ fail", KR(ret), KPC(this));
  }

  return ret;
}

int PartTransTask::handle_unserved_trans_()
{
  int ret = OB_SUCCESS;
  IObLogResourceCollector *resource_collector = TCTX.resource_collector_;

  if (OB_UNLIKELY(is_ddl_part())) {
    LOG_ERROR("Not a dml part is unexcepted", KPC(this));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_ISNULL(resource_collector)) {
    LOG_ERROR("resource_collector is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (is_data_ready()) {
    LOG_ERROR("data is already ready, not expected", KPC(this));
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_FAIL(check_dml_redo_node_ready_and_handle_())) {
      LOG_ERROR("check_dml_redo_node_ready_and_handle_ fail", KR(ret), KPC(this));
    } else if (is_data_ready()) {
      if (OB_FAIL(resource_collector->revert(this))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("revert PartTransTask fail", KR(ret));
        }
      }
    } else {}
  }

  return ret;
}

int PartTransTask::init_part_heartbeat_info(const common::ObPartitionKey &pkey, const int64_t timestamp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(TASK_TYPE_UNKNOWN != type_)) {
    LOG_ERROR("task has been initialized", K(type_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(timestamp <= 0)) {
    LOG_ERROR("invalid argument", K(timestamp));
    ret = OB_INVALID_ARGUMENT;
  } else {
    type_ = TASK_TYPE_PART_HEARTBEAT;
    partition_ = pkey;
    timestamp_ = timestamp;
  }

  return ret;
}

int PartTransTask::init_global_heartbeat_info(const int64_t timestamp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(TASK_TYPE_UNKNOWN != type_)) {
    LOG_ERROR("task has been initialized", K(type_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(timestamp <= 0)) {
    LOG_ERROR("invalid argument", K(timestamp));
    ret = OB_INVALID_ARGUMENT;
  } else {
    type_ = TASK_TYPE_GLOBAL_HEARTBEAT;
    timestamp_ = timestamp;
  }

  return ret;
}

int PartTransTask::init_offline_partition_task(const common::ObPartitionKey &partition)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(TASK_TYPE_UNKNOWN != type_)) {
    LOG_ERROR("task has been initialized", K(type_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(! partition.is_valid())) {
    LOG_ERROR("invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    type_ = TASK_TYPE_OFFLINE_PARTITION;
    partition_ = partition;
  }

  return ret;
}

// Only conversions from DML/DDL type transactions are supported, direct initialisation is not supported
int PartTransTask::convert_to_not_served_trans()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(TASK_TYPE_DML_TRANS != type_)
      && OB_UNLIKELY(TASK_TYPE_DDL_TRANS != type_)) {
    LOG_ERROR("task is not DML or DDL trans, not support", K(type_));
    ret = OB_NOT_SUPPORTED;
  } else if (OB_UNLIKELY(! is_task_info_valid())) {
    LOG_ERROR("tqsk info is not valid", K(*this));
    ret = OB_INVALID_DATA;
  } else {
    type_ = TASK_TYPE_NOT_SERVED_TRANS;
  }

  return ret;
}

bool PartTransTask::is_base_trans_info_valid_() const
{
  return (is_trans_committed_
      && partition_.is_valid()
      && timestamp_ > 0
      && trans_id_.is_valid()
      && OB_INVALID_ID != prepare_log_id_
      && global_trans_version_ >= 0);
}

bool PartTransTask::is_task_info_valid() const
{
  bool bool_ret = false;

  // All types of transactions require a valid checkpoint seq
  if (checkpoint_seq_ < 0) {
    bool_ret = false;
  } else {
    switch (type_) {
      case TASK_TYPE_PART_HEARTBEAT:
        // Partitioned heartbeats task require both timestamps and partitions to be valid
        bool_ret = (timestamp_ > 0) && (partition_.is_valid());
        break;

      case TASK_TYPE_GLOBAL_HEARTBEAT:
        // Global heartbeat only requires a valid timestamp
        bool_ret = (timestamp_ > 0);
        break;

      case TASK_TYPE_NOT_SERVED_TRANS:
      case TASK_TYPE_DML_TRANS:
        // The basic transaction information is valid and the DML parsing sequence number is valid
        bool_ret = (is_base_trans_info_valid_());
        break;

      case TASK_TYPE_DDL_TRANS:
        // Basic transaction information is valid
        bool_ret = (is_base_trans_info_valid_());
        break;

      case TASK_TYPE_OFFLINE_PARTITION:
        // Offline task only require valid partition
        bool_ret = (partition_.is_valid());
        break;

      default:
        bool_ret = false;
        break;
    }
  }

  if (!bool_ret) {
    LOG_INFO("is_task_info_valid", K(bool_ret), K(*this));
  }
  return bool_ret;
}

void *PartTransTask::alloc(const int64_t size)
{
  void *alloc_ret = NULL;
  if (size > 0) {
    alloc_ret = allocator_.alloc(size);
  }
  return alloc_ret;
}

void PartTransTask::free(void *ptr)
{
  allocator_.free(ptr);
  ptr = NULL;
}

int PartTransTask::add_stmt(const uint64_t row_index, IStmtTask *stmt_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_ID == row_index)
      || OB_ISNULL(stmt_task)) {
    LOG_ERROR("invalid argument", K(row_index), KPC(stmt_task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(stmt_list_.add(stmt_task))) {
    LOG_ERROR("add stmt task into stmt_list fail", KR(ret), K(stmt_list_), KP(stmt_task));
  } else {
    stmt_task->set_row_index(row_index);
  }

  return ret;
}

int PartTransTask::add_ddl_stmt(const uint64_t row_index, DdlStmtTask *ddl_stmt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_ID == row_index)
      || OB_ISNULL(ddl_stmt)
      || OB_UNLIKELY(ddl_stmt->get_op_schema_version() <= 0)
      || OB_ISNULL(ddl_stmt->get_binlog_record())) {
    LOG_ERROR("invalid argument", KPC(ddl_stmt));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ddl_stmt->set_row_index(row_index);
    ddl_stmt->set_next(NULL);

    if (NULL == stmt_list_.head_) {
      stmt_list_.head_ = ddl_stmt;
      stmt_list_.tail_ = ddl_stmt;
    } else {
      IStmtTask *stmt_task = stmt_list_.head_;
      IStmtTask *before = NULL;
      bool located = false;

      // Sort DDL by Schema version incrementally
      while (OB_SUCCESS == ret && ! located && NULL != stmt_task) {
        IStmtTask *next = stmt_task->get_next();
        DdlStmtTask *dt = dynamic_cast<DdlStmtTask *>(stmt_task);

        if (OB_ISNULL(dt)) {
          LOG_ERROR("dynamic cast to DdlStmtTask fail", K(stmt_task), K(*stmt_task));
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        if (dt->get_op_schema_version() > ddl_stmt->get_op_schema_version()) {
          ddl_stmt->set_next(dt);

          if (NULL == before) {
            stmt_list_.head_ = ddl_stmt;
          } else {
            before->set_next(ddl_stmt);
          }

          located = true;
          break;
        } else {
          before = stmt_task;
          stmt_task = next;
        }
      }

      if (OB_SUCCESS == ret && ! located) {
        stmt_list_.tail_->set_next(ddl_stmt);
        stmt_list_.tail_ = ddl_stmt;
        ddl_stmt->set_next(NULL);
      }
    }

    if (OB_SUCC(ret)) {
      stmt_list_.num_++;
    }
  }

  return ret;
}

void PartTransTask::set_formatted()
{
  LOG_DEBUG("[STAT] [TRANS_TASK] SET_FORMATTED", K_(is_data_ready), "task", *this);

  (void)ATOMIC_SET(&is_data_ready_, true);

  // Atomic loading cond variables
  ObCond *cond = ATOMIC_LOAD(&wait_formatted_cond_);

  if (NULL != cond) {
    cond->signal();
  }
}

// Note: requires that the cond variable provided in multiple consecutive calls is the same variable
int PartTransTask::wait_formatted(const int64_t timeout, ObCond &cond)
{
  int ret = OB_SUCCESS;
  int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time() + timeout;

  if (ATOMIC_LOAD(&is_data_ready_)) {
    // The format is already done, nothing needs to be done
  } else {
    // First set the condition variable
    (void)ATOMIC_SET(&wait_formatted_cond_, &cond);

    // Re-check the variable values
    while (OB_SUCCESS == ret && ! ATOMIC_LOAD(&is_data_ready_)) {
      int64_t left_time = end_time - ::oceanbase::common::ObTimeUtility::current_time();

      if (left_time <= 0) {
        ret = OB_TIMEOUT;
        break;
      } else {
        cond.timedwait(left_time);
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("[STAT] [TRANS_TASK] WAIT_FORMATTED", K_(is_data_ready), "task", *this);
  }

  return ret;
}

void PartTransTask::set_data_ready()
{
  LOG_DEBUG("[STAT] [TRANS_TASK] SET_DATA_READY", K_(is_data_ready), "task", *this);
  sorted_redo_list_.init_iterator();
  (void)ATOMIC_SET(&is_data_ready_, true);
  wait_data_ready_cond_.signal();
}

int PartTransTask::wait_data_ready(const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time() + timeout;

  // Re-check the variable values
  while (OB_SUCCESS == ret && ! ATOMIC_LOAD(&is_data_ready_)) {
    int64_t left_time = end_time - ::oceanbase::common::ObTimeUtility::current_time();

    if (left_time <= 0) {
      ret = OB_TIMEOUT;
      break;
    } else {
      wait_data_ready_cond_.timedwait(left_time);
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("[STAT] [TRANS_TASK] WAIT_DATA_READY", K_(is_data_ready), "task", *this);
  }

  return ret;
}

int64_t PartTransTask::dec_ref_cnt()
{
  return ATOMIC_AAF(&ref_cnt_, -1);
}

void PartTransTask::set_ref_cnt(const int64_t value)
{
  (void)ATOMIC_SET(&ref_cnt_, value);
}

int PartTransTask::init_trace_id_(const ObString &trace_id)
{
  int ret = OB_SUCCESS;
  trace_id_.reset();

  if (trace_id.length() > 0) {
    if (OB_ISNULL(trace_id.ptr())) {
      LOG_ERROR("invalid trace id", K(trace_id));
      ret = OB_INVALID_ARGUMENT;
    } else {
      int64_t trace_id_len = trace_id.length();

      // Allocate trace_id memory
      char *buf = static_cast<char*>(allocator_.alloc(trace_id_len));
      if (OB_ISNULL(buf)) {
        LOG_ERROR("allocate memory for trace id buffer fail", K(buf), K(trace_id_len));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        trace_id_.assign_buffer(buf, static_cast<int32_t>(trace_id_len));
        int64_t write_len = trace_id_.write(trace_id.ptr(), trace_id.length());

        if (write_len != trace_id_len) {
          LOG_ERROR("write trace id fail", K(write_len), K(trace_id_len), K(trace_id), K(trace_id_));
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
  }

  return ret;
}

int PartTransTask::init_trace_info_(const ObString &trace_info)
{
  int ret = OB_SUCCESS;
  trace_info_.reset();

  if (trace_info.length() > 0) {
    if (OB_ISNULL(trace_info.ptr())) {
      LOG_ERROR("invalid trace info", K(trace_info));
      ret = OB_INVALID_ARGUMENT;
    } else {
      const int64_t trace_info_len = trace_info.length();
      char *buf = static_cast<char*>(allocator_.alloc(trace_info_len));

      if (OB_ISNULL(buf)) {
        LOG_ERROR("allocate memory for trace id buffer fail", K(buf), K(trace_info_len));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        trace_info_.assign_buffer(buf, static_cast<int32_t>(trace_info_len));
        int64_t write_len = trace_info_.write(trace_info.ptr(), trace_info.length());

        if (write_len != trace_info_len) {
          LOG_ERROR("write trace id fail", K(write_len), K(trace_info_len), K(trace_info), K(trace_info_));
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
  }

  return ret;
}

int PartTransTask::init_participant_array_(const PartitionLogInfoArray &participants)
{
  int ret = OB_SUCCESS;
  int64_t part_count = participants.count();
  transaction::ObPartitionLogInfo *part_array = NULL;

  if (OB_UNLIKELY(NULL != participants_) || OB_UNLIKELY(participant_count_ > 0)) {
    LOG_ERROR("participant has been initialized", K(participants_), K(participant_count_));
    ret = OB_INIT_TWICE;
  } else if (part_count <= 0) {
    // no valid participants
    part_count = 0;
    part_array = NULL;
  } else {
    int64_t alloc_size = part_count * sizeof(transaction::ObPartitionLogInfo);
    part_array = static_cast<transaction::ObPartitionLogInfo *>(allocator_.alloc(alloc_size));

    if (OB_ISNULL(part_array)) {
      LOG_ERROR("allocate memory for participant array fail", K(part_count), K(alloc_size),
          K(participants));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t index = 0; OB_SUCC(ret) && index < participants.count(); index++) {
        new(part_array + index) transaction::ObPartitionLogInfo(participants.at(index));
      }
    }
  }

  if (OB_SUCC(ret)) {
    participants_ = part_array;
    participant_count_ = part_count;
  } else {
    if (NULL != part_array) {
      allocator_.free(part_array);
      part_array = NULL;
    }
  }

  return ret;
}

void PartTransTask::destroy_participant_array_()
{
  if (NULL != participants_ && participant_count_ > 0) {
    for (int64_t index = 0; index < participant_count_; index++) {
      participants_[index].~ObPartitionLogInfo();
    }

    allocator_.free(participants_);
    participants_ = NULL;
    participant_count_ = 0;
  }
}

int PartTransTask::set_participants(const PartitionLogInfoArray &participants)
{
  return init_participant_array_(participants);
}

int PartTransTask::to_string_pkey_and_log_id_(const uint64_t prepare_log_id)
{
  int ret = OB_SUCCESS;
  static const int64_t local_buf_size =
    PrintableSizeGetter<ObPartitionKey, HAS_MEMBER(ObPartitionKey, MAX_PRINTABLE_SIZE)>::value;
  char pkey_local_buf[local_buf_size];
  int64_t pkey_local_buf_pos = 0;

  // Print with a local buffer to avoid memory reuse, so don't call to_cstring(pkey) directly
  pkey_local_buf_pos = partition_.to_string(pkey_local_buf, local_buf_size);

  if (OB_UNLIKELY(pkey_local_buf_pos <= 0 || pkey_local_buf_pos >= local_buf_size)) {
    LOG_ERROR("pkey local buf pos is not valid", K(pkey_local_buf_pos), K(local_buf_size), K(partition_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t buf_len = pkey_local_buf_pos
      + sizeof(DELIMITER_STR)
      + DmlStmtUniqueID::compute_str_length_base_num(prepare_log_id)
      + 1;
    char *buf = static_cast<char*>(allocator_.alloc(buf_len));

    if (OB_ISNULL(buf)) {
      LOG_ERROR("allocate memory for trans id buffer fail", K(buf), K(buf_len));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, pkey_local_buf, pkey_local_buf_pos);
      int64_t pos = pkey_local_buf_pos;

      if (OB_FAIL(common::databuff_printf(buf, buf_len, pos, DELIMITER_STR"%lu", prepare_log_id))) {
        LOG_ERROR("databuff_printf fail", KR(ret), K(buf), K(buf_len), K(pos), K(prepare_log_id));
      } else {
        pkey_and_log_id_str_.assign_ptr(buf, static_cast<int32_t>(pos));
      }
    }
  }

  return ret;
}

int PartTransTask::to_string_trans_id()
{
  int ret = OB_SUCCESS;
  // ObTransID all use the maximum value, 1024 is large enough
  static const int64_t TRANS_ID_BUF_LENGTH = 1024;
  char trans_id_buf[TRANS_ID_BUF_LENGTH];
  int64_t pos = 0;

  if (OB_FAIL(common::databuff_printf(trans_id_buf, TRANS_ID_BUF_LENGTH, pos, "%s", to_cstring(trans_id_)))) {
    LOG_ERROR("databuff_printf fail", K(ret), K(trans_id_), K(trans_id_buf), K(TRANS_ID_BUF_LENGTH), K(pos));
  } else if (OB_UNLIKELY(pos <= 0 || pos >= TRANS_ID_BUF_LENGTH)) {
    LOG_ERROR("local buf pos is not valid", K(pos), K(TRANS_ID_BUF_LENGTH), K(partition_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t buf_len = pos + 1;
    char *buf = static_cast<char*>(allocator_.alloc(buf_len));

    if (OB_ISNULL(buf)) {
      LOG_ERROR("allocator_ alloc for trans id str fail", K(buf), K(buf_len));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, trans_id_buf, pos);
      buf[pos] = '\0';

      trans_id_str_.assign(buf, static_cast<int32_t>(pos));
    }
  }

  return ret;
}

// 1. roll back the local schema version of the task
// 1) dml transactions with statements using the same schema version can be left alone; however, rollback is still done for robustness reasons
// 2) ddl transaction rollback by statement
//
// 2. rollback stmt_list task row.sql_no for stmt greater than the rollback condition
//
int PartTransTask::revert_by_rollback_savepoint(const uint64_t row_index, const int64_t rollback_to_seq)
{
  int ret = OB_SUCCESS;
  const bool is_ddl = is_ddl_trans();

  if (OB_UNLIKELY(OB_INVALID_ID == row_index || OB_UNLIKELY(rollback_to_seq < 0))) {
    LOG_ERROR("invalid argument", K(row_index), K(rollback_to_seq));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! is_ddl)) {
    LOG_ERROR("is not ddl, unexcepted", K(*this));
    ret = OB_ERR_UNEXPECTED;
  // rollabck ddl trans
  } else if (OB_FAIL(revert_ddl_stmt_(rollback_to_seq))) {
    LOG_ERROR("revert ddl stmt by rollback savepoint failed", KR(ret), K(is_ddl), K(rollback_to_seq));
  } else {
    // succ
  }

  return ret;
}

// DDL transactions stmt order is dependent on schema version and cannot be guaranteed to be strictly incremented by sql no
// need to traverse stmt list
int PartTransTask::revert_ddl_stmt_(const int64_t rollback_to_seq)
{
  int ret = OB_SUCCESS;
  uint32_t major_version = 0;
  uint16_t minor_version = 0;
  uint16_t patch_version = 0;

  const int64_t total_stmt_num = stmt_list_.num_;
  int64_t stmt_num = 0;
  IStmtTask *stmt_task = stmt_list_.head_;
  IStmtTask *dummy_task = NULL;
  IStmtTask *pre_task = NULL;

  while (OB_SUCC(ret) && NULL != stmt_task) {
    DdlStmtTask *ddl_task = dynamic_cast<DdlStmtTask *>(stmt_task);
    if (OB_ISNULL(ddl_task)) {
      LOG_ERROR("dynamic cast to DdlStmtTask fail", K(stmt_task), K(ddl_task));
      ret = OB_ERR_UNEXPECTED;
    } else {
      const int64_t stmt_seq_for_rollback = ddl_task->get_row_seq_for_rollback();
      IStmtTask *next = stmt_task->get_next();
      const bool need_not_rollback = stmt_seq_for_rollback <= rollback_to_seq;
      // stmt less than or equal to the sql_no specified by the rollback savepoint is not processed
      if (need_not_rollback) {
        // dummy points to the first matching stmt
        if (NULL == dummy_task) {
          dummy_task = stmt_task;
        }
        pre_task = stmt_task;
        stmt_num++;
        _LOG_DEBUG("[SAVEPOINT][DDL] ddl_stmt need not revert, schema_version=%ld,"
            "rollback_to_seq=%ld, stmt_sql_no=%ld", ddl_task->get_op_schema_version(),
            rollback_to_seq, stmt_seq_for_rollback);
      } else {
        if (NULL != pre_task) {
          pre_task->set_next(stmt_task->get_next());
        }
        ddl_task->~DdlStmtTask();
        free(ddl_task);
        ddl_task = NULL;
      }
      // In all cases, continue traversing the next
      stmt_task = next;
    }
  }

  if (OB_SUCC(ret)) {
    stmt_list_.num_ = stmt_num;
    stmt_list_.head_ = dummy_task;
    stmt_list_.tail_ = pre_task;
  }

  if (OB_SUCC(ret)) {
    _LOG_INFO("[SAVEPOINT][DDL] ROLLBACK_TO_SEQ=%ld STMT_CNT=%ld/%ld",
        rollback_to_seq, total_stmt_num, stmt_num);
  }

  return ret;
}

int PartTransTask::next_redo_to_dispatch(DmlRedoLogNode *&dml_redo_node, bool &is_last_redo)
{
  int ret = OB_SUCCESS;
  RedoLogMetaNode *redo_meta = NULL;

  if (OB_FAIL(sorted_redo_list_.next_dml_redo(redo_meta, is_last_redo))) {
    if (OB_EMPTY_RESULT != ret) {
      LOG_ERROR("failed to get next dml_redo_meta to dispatch", KR(ret), KPC(this));
    }
  } else if (OB_ISNULL(dml_redo_node = static_cast<DmlRedoLogNode*>(redo_meta))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to cast RedoLogMetaNode(get from sorted_redo_list_) to DmlRedoLogNode", KR(ret), KPC(redo_meta));
  } else { /* success */ }

  return ret;
}

int PartTransTask::next_dml_stmt(DmlStmtTask *&dml_stmt_task)
{
  int ret = OB_SUCCESS;
  ObLink *dml_task = NULL;

  if (sorted_redo_list_.is_dml_stmt_iter_end()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(sorted_redo_list_.next_dml_stmt(dml_task))) {
    if (OB_ITER_END != ret && OB_NEED_RETRY != ret) {
      LOG_ERROR("failed to get next dml stmt with valid br", KR(ret), K_(sorted_redo_list));
    }
  } else if (OB_ISNULL(dml_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dml_task get from SortedRedoList should not be null", KR(ret), K(dml_task));
  } else if (OB_ISNULL(dml_stmt_task = static_cast<DmlStmtTask*>(dml_task))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cast ObLink to DmlStmtTask failed", KR(ret), KP(dml_task));
  } else {
    sorted_redo_list_.set_sorted_row_seq_no(dml_stmt_task->get_row_sql_no());
    /* succ */
  }

  return ret;
}

} // namespace liboblog
} // namespace oceanbase
