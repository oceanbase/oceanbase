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
 *
 * Transaction Structures
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_part_trans_task.h"

#include "lib/string/ob_string.h"                   // ObString
#include "share/schema/ob_schema_service.h"         // OB_INVALID_DDL_OP
#include "share/schema/ob_table_schema.h"           // ObTableSchema
#include "share/schema/ob_column_schema.h"          // ObColumnSchemaV2
#include "share/ls/ob_ls_operator.h"                // ObLSAttr
#include "storage/memtable/ob_memtable_mutator.h"   // ObMemtableMutatorMeta
#include "storage/memtable/ob_memtable_context.h"   // ObTransRowFlag
#include "storage/blocksstable/ob_row_reader.h"     // ObRowReader

#include "ob_log_binlog_record.h"                   // ObLogBR
#include "ob_log_binlog_record_pool.h"              // ObLogBRPool
#include "ob_log_utils.h"                           // obj2str
#include "ob_log_common.h"                          // ALL_DDL_OPERATION_TABLE_DDL_STMT_STR_COLUMN_ID
#include "ob_obj2str_helper.h"                      // ObObj2strHelper
#include "ob_log_instance.h"                        // TCTX
#include "ob_log_part_trans_dispatcher.h"           // PartTransDispatcher
#include "storage/tx/ob_clog_encrypt_info.h"
#include "ob_log_config.h"
#include "ob_log_ls_op_processor.h"                 // ObLogLSOpProcessor
#include "ob_log_part_trans_parser.h"               // IObLogPartTransParser
#include "ob_log_batch_buffer.h"                    // IObLogBatchBuffer
#include "ob_log_store_task.h"                      // ObLogStoreTask
#include "ob_log_factory.h"                         // ObLogStoreTaskFactory ReadLogBufFactory
#include "ob_log_resource_collector.h"              // IObLogResourceCollector
#include "ob_cdc_lob_data_merger.h"                 // IObCDCLobDataMerger
#include "ob_log_schema_cache_info.h"               // ColumnSchemaInfo
#include "ob_cdc_udt.h"                             // ObCDCUdtValueMap

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
namespace libobcdc
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

MutatorRow::MutatorRow(
    common::ObIAllocator &allocator) :
    ObMemtableMutatorRow(),
    allocator_(allocator),
    deserialized_(false),
    cols_parsed_(false),
    new_cols_(),
    old_cols_(),
    rowkey_cols_(),
    new_lob_ctx_cols_(allocator)
{}

MutatorRow::~MutatorRow()
{
  reset();
}

int MutatorRow::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  transaction::ObCLogEncryptInfo empty_clog_encrypt_info;
  empty_clog_encrypt_info.init();

  const bool need_extract_encrypt_meta = false;
  share::ObEncryptMeta unused_encrypt_meta;
  share::ObCLogEncryptStatMap unused_encrypt_stat_map;
  ObEncryptRowBuf row_buf;

  if (OB_UNLIKELY(deserialized_)) {
    LOG_ERROR("deserialize twice");
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos > data_len)) {
    LOG_ERROR("invalid argument", K(buf), K(pos), K(data_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObMemtableMutatorRow::deserialize(buf, data_len, pos,
                     row_buf, empty_clog_encrypt_info, need_extract_encrypt_meta,
                     unused_encrypt_meta, unused_encrypt_stat_map))) {
    LOG_ERROR("deserialize mutator fail", KR(ret), KP(buf), K(data_len), K(pos));
  } else {
    deserialized_ = true;
  }

  return ret;
}

int MutatorRow::deserialize_first(
    const char *buf,
    const int64_t buf_len,
    int64_t &pos,
    int32_t &row_size)
{
  int ret = OB_SUCCESS;
  row_size = 0;
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
    // The pos indicates the position that has been resolved
    pos = new_pos;
  }

  return ret;
}

int MutatorRow::deserialize_second(
    const char *buf,
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
int MutatorRow::parse_cols(
    ObObj2strHelper *obj2str_helper /* = NULL */,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const TableSchemaInfo *tb_schema_info /* = NULL */,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    const bool enable_output_hidden_primary_key /*  = false */,
    const ObLogAllDdlOperationSchemaInfo *all_ddl_operation_table_schema_info /* = NULL */)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(cols_parsed_)) {
    LOG_ERROR("columns has been parsed", K(cols_parsed_));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_UNLIKELY(! deserialized_)) {
    LOG_ERROR("row has not been deserialized");
    ret = OB_STATE_NOT_MATCH;
  }

  // parse value of new column
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(new_row_.data_) || OB_UNLIKELY(new_row_.size_ <= 0)) {
      LOG_WARN("new row data is empty", K(new_row_),
          "mutator_row", (const ObMemtableMutatorRow &)(*this));
      new_cols_.reset();
    } else if (OB_FAIL(parse_columns_(true/*is_parse_new_col*/, new_row_.data_, new_row_.size_,
        obj2str_helper, tenant_id, table_id, tb_schema_info, tz_info_wrap, enable_output_hidden_primary_key,
        all_ddl_operation_table_schema_info, new_cols_))) {
      LOG_ERROR("parse new columns fail", KR(ret), K(tenant_id), K(table_id), K(new_row_), K(obj2str_helper),
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
    } else if (OB_FAIL(parse_columns_(false/*is_parse_new_col*/, old_row_.data_, old_row_.size_,
        obj2str_helper, tenant_id, table_id, tb_schema_info, tz_info_wrap, enable_output_hidden_primary_key,
        all_ddl_operation_table_schema_info, old_cols_))) {
      LOG_ERROR("parse old columns fail", KR(ret), K(tenant_id), K(table_id), K(old_row_), K(obj2str_helper),
          K(tb_schema_info), K(enable_output_hidden_primary_key));
    } else {
      // succ
    }
  }

  // parse rowkey data
  if (OB_SUCC(ret)) {
    rowkey_cols_.reset();

    if (OB_FAIL(parse_rowkey_(
        rowkey_cols_,
        rowkey_,
        obj2str_helper,
        tenant_id,
        table_id,
        tb_schema_info,
        tz_info_wrap,
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

template<class CDC_INNER_TABLE_SCHEMA>
int MutatorRow::parse_cols(
    const CDC_INNER_TABLE_SCHEMA &inner_table_schema_info)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema &table_schema = inner_table_schema_info.get_table_schema();

  if (OB_UNLIKELY(cols_parsed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("columns has been parsed", KR(ret), K(cols_parsed_));
  } else if (OB_UNLIKELY(! deserialized_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("row has not been deserialized", KR(ret));
  }

  // parse value of new column
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(new_row_.data_) || OB_UNLIKELY(new_row_.size_ <= 0)) {
      LOG_WARN("new row data is empty", K(new_row_),
          "mutator_row", (const ObMemtableMutatorRow &)(*this));
      new_cols_.reset();
    } else if (OB_FAIL(parse_columns_(true/*is_parse_new_col*/, new_row_.data_,
        new_row_.size_, inner_table_schema_info, new_cols_))) {
      LOG_ERROR("parse new columns fail", KR(ret), K(new_row_), K(table_schema));
    } else {
      // succ
    }
  }

  // parse value of old column
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(old_row_.data_) || OB_UNLIKELY(old_row_.size_ <= 0)) {
      // no old cols
      old_cols_.reset();
    } else if (OB_FAIL(parse_columns_(false/*is_parse_new_col*/, old_row_.data_,
        old_row_.size_, inner_table_schema_info, old_cols_))) {
      LOG_ERROR("parse old columns fail", KR(ret), K(old_row_), K(table_schema));
    } else {
      // succ
    }
  }

  // parse rowkey data
  if (OB_SUCC(ret)) {
    rowkey_cols_.reset();

    if (OB_FAIL(parse_rowkey_(table_schema, rowkey_, rowkey_cols_))) {
      LOG_ERROR("parse_rowkey_ fail", KR(ret), K(rowkey_));
    } else {
      // succ
    }
  }

  if (OB_SUCC(ret)) {
    cols_parsed_ = true;
  }

  return ret;
}

int MutatorRow::parse_columns_(
    const bool is_parse_new_col,
    const char *col_data,
    const int64_t col_data_size,
    ObObj2strHelper *obj2str_helper,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const TableSchemaInfo *tb_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    const bool enable_output_hidden_primary_key,
    const ObLogAllDdlOperationSchemaInfo *all_ddl_operation_table_schema_info,
    ColValueList &cols)
{
  int ret = OB_SUCCESS;
  blocksstable::ObRowReader row_reader;
  blocksstable::ObDatumRow datum_row;
  // warpper cols for udt column values
  ObCDCUdtValueMap udt_value_map(allocator_, tb_schema_info, cols);

  // NOTE: Allow obj2str_helper and column_schema to be empty
  if (OB_ISNULL(col_data) || OB_UNLIKELY(col_data_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(col_data_size), K(col_data));
  }
  // Validate cols values
  else if (OB_UNLIKELY(cols.num_ > 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("column value list is not reseted", KR(ret), K(cols));
  } else if (OB_FAIL(row_reader.read_row(col_data, col_data_size, nullptr, datum_row))) {
    LOG_WARN("Failed to read datum row", KR(ret), K(tenant_id), K(table_id), K(is_parse_new_col));
  } else {
    LOG_DEBUG("prepare to handle datum_row", K(datum_row));
    // Iterate through all Cells using Cell Reader
    for (int64_t column_stored_idx = 0; OB_SUCC(ret) && column_stored_idx < datum_row.get_column_count(); column_stored_idx++) {
      const ObObj *value = NULL;
      uint64_t column_id = OB_INVALID_ID;
      ColumnSchemaInfo *column_schema_info = NULL;
      blocksstable::ObStorageDatum &datum = datum_row.storage_datums_[column_stored_idx];

      if (OB_FAIL(deep_copy_encoded_column_value_(datum))) {
        LOG_ERROR("deep_copy_encoded_column_value_ failed", KR(ret),
            K(tenant_id), K(table_id), K(column_stored_idx), K(datum), K(is_parse_new_col));
      } else if (datum.is_nop()) {
        LOG_DEBUG("ignore nop datum", K(column_stored_idx), K(datum));
      } else if (OB_FAIL(get_column_info_(
            tb_schema_info,
            all_ddl_operation_table_schema_info,
            column_stored_idx,
            false/*is_rowkey_column_idx*/,
            column_id,
            column_schema_info))) {
        LOG_ERROR("get_column_info", KR(ret), K_(table_id), K(column_stored_idx));
      } else {
        bool ignore_column = false;
        if (OB_NOT_NULL(tb_schema_info)) {
          // for normal column which is not belong to some udt, is_usr_column is true and is_udt_column is false
          // for udt column
          // if is main column of group, is_usr_column is true , is_udt_column is also true.
          // if is hidden column of udt, is_usr_column is false, is_udt_column is true.
          if (! (column_schema_info->is_usr_column() || column_schema_info->is_udt_column())) {
            // ignore non user columns
            LOG_DEBUG("ignore non user-required column",
                K(tenant_id), K(table_id), K(column_stored_idx), K(column_schema_info));

            ignore_column = true;
          } else {
            ignore_column = false;
          }
        }

        if (OB_SUCC(ret) && ! ignore_column) {
          ObObjMeta obj_meta;
          ObObj obj;

          if (OB_FAIL(set_obj_propertie_(
              column_id,
              column_stored_idx,
              column_schema_info,
              all_ddl_operation_table_schema_info,
              obj_meta,
              obj))) {
            LOG_ERROR("set_obj_propertie_ failed", K(column_id), K(column_stored_idx),
                KPC(column_schema_info), K(obj_meta), K(obj));
          } else if (OB_FAIL(datum.to_obj_enhance(obj, obj_meta))) {
            LOG_ERROR("transfer datum to obj failed", KR(ret), K(datum), K(obj_meta));
          } else {
            const bool is_lob_storage = obj_meta.is_lob_storage();
            // Default is false
            bool is_out_row = false;

            if (obj.is_null()) {
              // do nothing
            } else if (is_lob_storage) {
              const ObLobCommon &lob_common = datum.get_lob_data();
              is_out_row = ! lob_common.in_row_;
              LOG_DEBUG("handle_lob_v2_data", K(column_stored_idx), K(lob_common), K(obj));

              if (! is_out_row) {
                LOG_DEBUG("is_lob_storage in row", K(column_id), K(is_lob_storage), K(is_parse_new_col), K(lob_common), K(obj));
                obj.set_string(obj.get_type(), lob_common.get_inrow_data_ptr(), lob_common.get_byte_size(datum.len_));
              } else {
                const ObLobData &lob_data = *(reinterpret_cast<const ObLobData *>(lob_common.buffer_));

                const ObLobDataOutRowCtx *lob_data_out_row_ctx =
                  reinterpret_cast<const ObLobDataOutRowCtx *>(lob_data.buffer_);

                LOG_DEBUG("is_lob_storage out row", K(column_id), K(is_lob_storage), K(is_parse_new_col), K(lob_common),
                    K(lob_data), K(obj), KPC(lob_data_out_row_ctx));

                if (is_parse_new_col) {
                  ObLobDataGetCtx *lob_data_get_ctx = static_cast<ObLobDataGetCtx *>(allocator_.alloc(sizeof(ObLobDataGetCtx)));

                  if (OB_ISNULL(lob_data_get_ctx)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_ERROR("allocate memory for ObLobDataGetCtx fail", KR(ret), "size", sizeof(ObLobDataGetCtx));
                  } else {
                    new(lob_data_get_ctx) ObLobDataGetCtx();
                    lob_data_get_ctx->reset((void *)(&new_lob_ctx_cols_), column_id, dml_flag_, &lob_data);

                    new_lob_ctx_cols_.add(lob_data_get_ctx);
                  }
                } else {
                  if (OB_FAIL(new_lob_ctx_cols_.set_old_lob_data(column_id, &lob_data))) {
                    if (OB_ENTRY_NOT_EXIST != ret) {
                      LOG_ERROR("new_lob_ctx_cols_ set_old_lob_data failed", KR(ret), K(column_id), K(lob_data));
                    } else {
                      // Not finding it is a possibility, eg:
                      // LOB old row is out row storage, but new row is in row storage
                      ret = OB_SUCCESS;
                    }
                  }
                }
              }
            } // is_lob_storage

            if (OB_SUCC(ret) && OB_FAIL(add_column_(
                column_id,
                &obj,
                is_out_row,
                tenant_id,
                table_id,
                column_schema_info,
                obj2str_helper,
                tz_info_wrap,
                cols,
                &udt_value_map))) {
              LOG_ERROR("add_column_ fail", KR(ret), K(tenant_id), K(table_id), K(cols), K(column_stored_idx), K(column_id),
                  K(obj), K(obj2str_helper), K(column_schema_info));
            }
          }
        }
      }
    } // for

    ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  }

  return ret;
}

int MutatorRow::set_obj_propertie_(
    const uint64_t column_id,
    const int64_t column_idx_for_datum_row,
    const ColumnSchemaInfo *column_schema_info,
    const ObLogAllDdlOperationSchemaInfo *all_ddl_operation_table_schema_info,
    ObObjMeta &obj_meta,
    ObObj &obj)
{
  int ret = OB_SUCCESS;

  if (nullptr != column_schema_info) {
    obj_meta = column_schema_info->get_meta_type();
    obj_meta.set_scale(column_schema_info->get_accuracy().get_scale());
  }

  if (nullptr != all_ddl_operation_table_schema_info) {
    const share::schema::ObColumnSchemaV2 *column_table_schema = NULL;
    const share::schema::ObTableSchema &ddl_table_schema = all_ddl_operation_table_schema_info->get_table_schema();

    if (OB_ISNULL(column_table_schema = ddl_table_schema.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("column_table_schema is null", KR(ret), K(column_id), K(column_idx_for_datum_row), KPC(column_table_schema));
    } else {
      obj_meta = column_table_schema->get_meta_type();
      obj_meta.set_scale(column_table_schema->get_accuracy().get_scale());

    }
  }

  if (OB_SUCC(ret)) {
    if (obj_meta.is_lob_storage()) {
      obj_meta.set_has_lob_header();
    }
  }

  return ret;
}

int MutatorRow::add_column_(
    const uint64_t column_id,
    const ObObj *value,
    const bool is_out_row,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ColumnSchemaInfo *column_schema_info,
    const ObObj2strHelper *obj2str_helper/*NULL if parse all_ddl_operation_table  columns*/,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    ColValueList &cols,
    ObCDCUdtValueMap *udt_value_map)
{
  int ret = OB_SUCCESS;
  ColValue *cv_node = nullptr;

  // NOTE: Allow obj2str_helper and column_schema to be empty
  if (OB_NOT_NULL(column_schema_info)
      && OB_NOT_NULL(udt_value_map)
      && column_schema_info->is_udt_column()) {
    // for udt column, no need calling obj2str, just add to udt column group
    // then will group columns obj value of same udt to single string value together
    if (OB_FAIL(udt_value_map->add_column_value_to_udt(*column_schema_info, is_out_row, value))) {
      LOG_ERROR("add_column_value_to_udt fail", KR(ret), K(cols));
    }
  } else if (OB_ISNULL(cv_node = static_cast<ColValue *>(allocator_.alloc(sizeof(ColValue))))) {
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
    cv_node->is_out_row_ = is_out_row;
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

    LOG_DEBUG("column_cast: ",
        K(tenant_id),
        K(table_id),
        K(column_id),
        "old_scale", value->get_scale(),
        "new_scale", cv_node->value_.get_scale());

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
    } else if (value->is_geometry() && value->get_string_len() > 2 * _M_) { // geometry may exceed 2M
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
    // note: currently DML must pass into obj2str_helperd
    if (NULL != obj2str_helper && ! is_out_row && OB_FAIL(obj2str_helper->obj2str(tenant_id,
        table_id,
        column_id,
        cv_node->value_,
        cv_node->string_value_,
        allocator_,
        false,
        extended_type_info,
        accuracy,
        collation_type,
        tz_info_wrap))) {
      LOG_ERROR("obj2str fail", KR(ret),
          "obj", *value, K(obj2str_helper), K(accuracy), K(collation_type), K(column_id), K(column_schema_info));
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

int MutatorRow::get_column_info_(
    const TableSchemaInfo *tb_schema_info,
    const ObLogAllDdlOperationSchemaInfo *all_ddl_operation_table_schema_info,
    const int64_t column_idx,
    const bool is_rowkey_column_idx,
    uint64_t &column_id,
    ColumnSchemaInfo *&column_schema_info) const
{
  int ret = OB_SUCCESS;

  if (nullptr != tb_schema_info) {
    const bool is_column_stored_idx = true;

    if (is_rowkey_column_idx) {
      if (OB_FAIL(tb_schema_info->get_column_schema_info_for_rowkey(column_idx, column_schema_info))) {
        LOG_ERROR("get_column_schema_info_with_rowkey_index failed", KR(ret),
            K(column_idx), K(is_rowkey_column_idx), K(tb_schema_info));
      }
    } else if (OB_FAIL(tb_schema_info->get_column_schema_info(column_idx, is_column_stored_idx, column_schema_info))) {
      LOG_ERROR("get_column_schema_info failed", KR(ret), K(column_idx), K(is_rowkey_column_idx), K(tb_schema_info));
    }

    if (OB_SUCC(ret)) {
      column_id = column_schema_info->get_column_id();
    }
  }

  if (nullptr != all_ddl_operation_table_schema_info) {
    const ObArray<share::schema::ObColDesc> &col_des_array = all_ddl_operation_table_schema_info->get_cols_des_array();
    column_id = col_des_array[column_idx].col_id_;
  }

  return ret;
}

int MutatorRow::parse_rowkey_(
    ColValueList &rowkey_cols,
    const common::ObStoreRowkey &rowkey,
    ObObj2strHelper *obj2str_helper,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const TableSchemaInfo *tb_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap,
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
      ColumnSchemaInfo *column_schema_info = NULL;
      bool ignore_column = false;

      if (OB_FAIL(get_column_info_(
          tb_schema_info,
          nullptr/*all_ddl_operation_table_schema_info*/,
          index,
          true/*is_rowkey_column_idx*/,
          column_id,
          column_schema_info))) {
        LOG_ERROR("get_column_info fail", KR(ret), K(index), K(rowkey_count),
            K(column_id), K(enable_output_hidden_primary_key),
            K(column_schema_info));
      } else if (OB_NOT_NULL(tb_schema_info)) {
        const ObLogRowkeyInfo &rowkey_info = tb_schema_info->get_rowkey_info();

        if (OB_UNLIKELY(rowkey_count != rowkey_info.get_size())) {
          ret = OB_INVALID_DATA;
          LOG_ERROR("rowkey count does not match schema", KR(ret),
              K(tenant_id), K(table_id), K(rowkey_count), K(rowkey_info), KPC(tb_schema_info));
        } else if (! column_schema_info->is_usr_column()) {
          // ignore hidden rowkey column
          LOG_DEBUG("ignore non user-required rowkey column", KPC(column_schema_info),
              K(tenant_id), K(table_id), K(column_id));

          ignore_column = true;
        } else {
          ignore_column = false;
        }
      }

      if (OB_SUCC(ret) && ! ignore_column) {
        if (OB_FAIL(add_column_(
            column_id,
            rowkey_objs + index,
            false/*is_out_row*/,
            tenant_id,
            table_id,
            column_schema_info,
            obj2str_helper,
            tz_info_wrap,
            rowkey_cols,
            nullptr/*udt_value_map*/))) {
          LOG_ERROR("add_column_ fail", KR(ret), K(tenant_id), K(table_id), K(rowkey_cols), K(column_id),
              K(index), K(rowkey_objs[index]), K(obj2str_helper), KPC(tb_schema_info), K(column_schema_info));
        }
      }
    }
  }

  return ret;
}

template<class CDC_INNER_TABLE_SCHEMA>
int MutatorRow::parse_columns_(
    const bool is_parse_new_col,
    const char *col_data,
    const int64_t col_data_size,
    const CDC_INNER_TABLE_SCHEMA &inner_table_schema,
    ColValueList &cols)
{
  int ret = OB_SUCCESS;
  blocksstable::ObRowReader row_reader;
  blocksstable::ObDatumRow datum_row;
  const ObArray<share::schema::ObColDesc> &col_des_array = inner_table_schema.get_cols_des_array();
  const share::schema::ObTableSchema &table_schema = inner_table_schema.get_table_schema();

  // NOTE: Allow obj2str_helper and column_schema to be empty
  if (OB_ISNULL(col_data) || OB_UNLIKELY(col_data_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(col_data_size), K(col_data));
  }
  // Validate cols values
  else if (OB_UNLIKELY(cols.num_ > 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("column value list is not reseted", KR(ret), K(cols));
  } else if (OB_FAIL(row_reader.read_row(col_data, col_data_size, nullptr, datum_row))) {
    LOG_ERROR("Failed to read datum row", KR(ret));
  } else {
    LOG_DEBUG("parse_columns_", K(is_parse_new_col), K(datum_row));

    // Iterate through all Cells using Cell Reader
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_row.get_column_count(); i++) {
      uint64_t column_id = OB_INVALID_ID;
      const ObObj *value = NULL;
      blocksstable::ObStorageDatum &datum = datum_row.storage_datums_[i];
      column_id = col_des_array[i].col_id_;

      if (OB_FAIL(deep_copy_encoded_column_value_(datum))) {
        LOG_ERROR("deep_copy_encoded_column_value_ failed", KR(ret), "column_stored_idx", i, K(datum), K(is_parse_new_col));
      } else if (datum.is_nop()) {
        LOG_DEBUG("ignore nop datum", "column_stored_idx", i, K(datum));
      } else if (OB_INVALID_ID == column_id) {
        // Note: the column_id obtained here may be invalid
        // For example a delete statement with only one cell and an invalid column_id in the cell
        LOG_DEBUG("cell column_id is invalid", K(i), K(datum_row), K_(table_id), K_(rowkey));
      } else {
        if (OB_SUCC(ret)) {
          ObObjMeta obj_meta;
          ObObj obj;

          if (OB_FAIL(set_obj_propertie_(column_id, i, table_schema, obj_meta, obj))) {
            LOG_ERROR("set_obj_propertie_ failed", K(column_id), K(i), K(obj_meta), K(obj));
          } else if (OB_FAIL(datum.to_obj_enhance(obj, obj_meta))) {
            LOG_ERROR("transfer datum to obj failed", KR(ret), K(datum), K(obj_meta));
          } else {
            OB_ASSERT(obj.has_lob_header() == false); // debug only
            if (OB_FAIL(add_column_(cols, column_id, &obj))) {
              LOG_ERROR("add_column_ fail", K(cols), KR(ret), K(column_id), K(obj));
            }
          }
        }
      }
    } // for
  }

  return ret;
}

template<class TABLE_SCHEMA>
int MutatorRow::parse_rowkey_(
    const TABLE_SCHEMA &table_schema,
    const common::ObStoreRowkey &rowkey,
    ColValueList &rowkey_cols)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
  int64_t rowkey_count = rowkey.get_obj_cnt();
  const ObObj *rowkey_objs = rowkey.get_obj_ptr();

  if (OB_UNLIKELY(rowkey_count <= 0) || OB_ISNULL(rowkey_objs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("rowkey is invalid", KR(ret), K(rowkey_count), K(rowkey_objs), K(rowkey));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < rowkey_count; index++) {
      ObRowkeyColumn rowkey_col;

      if (OB_FAIL(rowkey_info.get_column(index, rowkey_col))) {
        LOG_ERROR("rowkey_info get_column failed", KR(ret), K(index), K(rowkey_col));
      } else if (OB_FAIL(add_column_(rowkey_cols, rowkey_col.column_id_, rowkey_objs + index))) {
        LOG_ERROR("add_column_ fail", KR(ret), K(rowkey_cols), "column_id", rowkey_col.column_id_,
            K(index), K(rowkey_objs[index]));
      }
    }
  }

  return ret;
}

int MutatorRow::deep_copy_encoded_column_value_(blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;

  if (datum.need_copy_for_encoding_column_with_flat_format(OBJ_DATUM_STRING)) {
    // local buffer will free while allocator_ reset
    char* local_buffer = static_cast<char*>(allocator_.alloc(sizeof(uint64_t)));

    if (OB_ISNULL(local_buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate local buffer to store local encoded column datum", KR(ret));
    } else {
      MEMCPY(local_buffer, datum.ptr_, sizeof(uint64_t));
      datum.set_string(local_buffer, sizeof(uint64_t));
    }
  }

  return ret;
}

int MutatorRow::add_column_(
    ColValueList &cols,
    const uint64_t column_id,
    const ObObj *value)
{
  int ret = OB_SUCCESS;
  ColValue *cv_node = static_cast<ColValue *>(allocator_.alloc(sizeof(ColValue)));

  // NOTE: Allow obj2str_helper and column_schema to be empty
  if (OB_ISNULL(cv_node)) {
    LOG_ERROR("allocate memory for ColValue fail", "size", sizeof(ColValue));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    cv_node->reset();
    cv_node->value_ = *value;
    cv_node->column_id_ = column_id;
    common::ObArrayHelper<common::ObString> extended_type_info;
    common::ObAccuracy accuracy;
    common::ObCollationType collation_type = ObCollationType::CS_TYPE_BINARY;

    LOG_DEBUG("column_cast: ", K(column_id),
        "old_scale", value->get_scale(), "new_scale", cv_node->value_.get_scale());

    if (value->is_lob() && value->get_string_len() > 2 * _M_) {
      LOG_DEBUG("column_cast: ", "old_obj_ptr", (void *)value->get_string_ptr(),
          "old_obj_len", value->get_string_len(),
          "new_obj_ptr", (void *)cv_node->value_.get_string_ptr(),
          "new_obj_len", cv_node->value_.get_string_len());
    } else {
      LOG_DEBUG("column_cast: ", "old_obj", *value, "new_obj",
          cv_node->value_);
    }

    if (OB_FAIL(cols.add(cv_node))) {
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

template<class TABLE_SCHEMA>
int MutatorRow::set_obj_propertie_(
    const uint64_t column_id,
    const int64_t column_idx_for_datum_row,
    const TABLE_SCHEMA &table_schema,
    ObObjMeta &obj_meta,
    ObObj &obj)
{
  int ret = OB_SUCCESS;
  const auto *column_table_schema = table_schema.get_column_schema(column_id);

  if (OB_ISNULL(column_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column_table_schema is null", KR(ret), K(column_id), K(column_idx_for_datum_row), KPC(column_table_schema));
  } else {
    obj_meta = column_table_schema->get_meta_type();
    obj_meta.set_scale(column_table_schema->get_accuracy().get_scale());
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

  new_lob_ctx_cols_.reset();

  ObMemtableMutatorRow::reset();
}

int MutatorRow::get_cols(
    ColValueList **rowkey_cols,
    ColValueList **new_cols,
    ColValueList **old_cols,
    ObLobDataOutRowCtxList **new_lob_ctx_cols)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! cols_parsed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("column has not been parsed", KR(ret));
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

    if (NULL != new_lob_ctx_cols) {
      *new_lob_ctx_cols = &new_lob_ctx_cols_;
    }
  }

  return ret;
}

//////////////////////////////////////// DmlStmtUniqueID ///////////////////////////////////////////////
int64_t DmlStmtUniqueID::get_dml_unique_id_length() const
{
  // 1. tenent_ls_id_str.len(include tenant_id, ls_id, trans_id)
  // 2. redo_log_lsn.val_.len(uint64_t)
  // 3. row_index.len
  // 4. DELIMITER_STR * 5
  // 5. '\0'

  int64_t dml_unique_id_length = part_trans_info_str_.length()
      + sizeof(DELIMITER_STR)
      + compute_str_length_base_num(redo_log_lsn_.val_)
      + sizeof(DELIMITER_STR)
      + compute_str_length_base_num(row_index_)
      + 1;

  return dml_unique_id_length;
}

template<typename NUM> int64_t DmlStmtUniqueID::compute_str_length_base_num(const NUM num_to_compute)
{
  int64_t str_length = 0;
  NUM num = num_to_compute;

  if (num < 0) {
    num = -num;
  }

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

  if (num_to_compute < 0) {
    str_length += 1;
  }

  return str_length;
}

int DmlStmtUniqueID::customized_to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    LOG_ERROR("invalid argument", K(buf), K(buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! is_valid())) {
    LOG_ERROR("dml_stmt_unique_id is not valid", K(*this));
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t str_len = part_trans_info_str_.length();

    if (OB_UNLIKELY(buf_len <= str_len)) {
      LOG_ERROR("buf not enough", K(buf_len), K(str_len), KPC(this));
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, part_trans_info_str_.ptr(), str_len);
      pos += str_len;

      if (OB_FAIL(common::databuff_printf(buf, buf_len, pos, DELIMITER_STR"%lu"DELIMITER_STR"%lu",
          redo_log_lsn_.val_, row_index_))) {
        LOG_ERROR("databuff_printf fail", KR(ret), K(buf), K(buf_len), K(pos), K(str_len), KPC(this));
      } else {
        // succ
      }
    }
  }

  return ret;
}

DmlStmtTask::DmlStmtTask(
    PartTransTask &host,
    ObLogEntryTask &log_entry_task,
    MutatorRow &row) :
    IStmtTask(STMT_TYPE_DML, host),
    is_callback_(0),
    log_entry_task_(log_entry_task),
    table_id_(OB_INVALID_ID),
    row_(row)
{
  // set hash value
  IStmtTask::set_hash_value(row.rowkey_.murmurhash(host.get_tls_id().hash()));

}

DmlStmtTask::~DmlStmtTask() { reset(); }

void DmlStmtTask::reset()
{
  is_callback_ = 0;
  IStmtTask::reset();
  table_id_ = OB_INVALID_ID;
  row_.reset();
}

int DmlStmtTask::parse_cols(
    ObObj2strHelper *obj2str_helper,
    const TableSchemaInfo *tb_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    const bool enable_output_hidden_primary_key)
{
  return row_.parse_cols(
      obj2str_helper,
      host_.get_tenant_id(),
      table_id_,
      tb_schema_info,
      tz_info_wrap,
      enable_output_hidden_primary_key);
}

int DmlStmtTask::parse_col(
    const uint64_t tenant_id,
    const uint64_t column_id,
    const ColumnSchemaInfo &column_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    ObObj2strHelper &obj2str_helper,
    ColValue &cv_node)
{
  int ret = OB_SUCCESS;
  common::ObArrayHelper<common::ObString> extended_type_info;
  common::ObAccuracy accuracy;
  common::ObCollationType collation_type = ObCollationType::CS_TYPE_BINARY;

  column_schema_info.get_extended_type_info(extended_type_info);
  accuracy = column_schema_info.get_accuracy();
  collation_type = column_schema_info.get_collation_type();

  if (OB_FAIL(obj2str_helper.obj2str(tenant_id,
      table_id_,
      column_id,
      cv_node.value_,
      cv_node.string_value_,
      row_.get_allocator(),
      false,
      extended_type_info,
      accuracy,
      collation_type,
      tz_info_wrap))) {
    LOG_ERROR("obj2str fail", KR(ret), K(tenant_id), K(table_id_), K(column_id), K(column_schema_info),
        "obj", cv_node, K(accuracy), K(collation_type));
  }

  return ret;
}

const logservice::TenantLSID &DmlStmtTask::get_tls_id() const
{
  return get_host().get_tls_id();
}

int64_t DmlStmtTask::get_global_schema_version() const
{
  return host_.get_global_schema_version();
}

////////////////////////////////////////////////////////////////////////////////////
int64_t DdlStmtUniqueID::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos,
        "%ld_%lu", tenant_id_, schema_version_);
  }

  return pos;
}

DdlStmtTask::DdlStmtTask(PartTransTask &host, MutatorRow &row) :
    IStmtTask(STMT_TYPE_DDL, host),
    row_(row),
    ddl_stmt_str_(),
    ddl_operation_type_(OB_INVALID_DDL_OP),
    ddl_op_schema_version_(0),
    ddl_op_table_id_(OB_INVALID_ID),
    ddl_op_tenant_id_(OB_INVALID_TENANT_ID),
    ddl_op_database_id_(OB_INVALID_ID),
    ddl_op_tablegroup_id_(OB_INVALID_ID),
    ddl_exec_tenant_id_(OB_INVALID_TENANT_ID)
{
  // set hash value
  IStmtTask::set_hash_value(row.rowkey_.murmurhash(host.get_tls_id().hash()));
  ddl_op_schema_version_str_[0] = '\0';
}

DdlStmtTask::~DdlStmtTask()
{
  reset();
}

bool DdlStmtTask::is_recyclebin_database_id(const uint64_t tenant_id, const uint64_t database_id)
{
  UNUSED(tenant_id);
  return common::is_recyclebin_database_id(database_id);
}

bool DdlStmtTask::is_create_table_ddl_(const int64_t ddl_operation_type)
{
  ObSchemaOperationType op_type = static_cast<ObSchemaOperationType>(ddl_operation_type);

  return (OB_DDL_CREATE_TABLE == op_type);
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

bool DdlStmtTask::is_sub_tls_id_alter_ddl_(const int64_t ddl_operation_type)
{
  bool bool_ret = false;

  ObSchemaOperationType op_type = static_cast<ObSchemaOperationType>(ddl_operation_type);
  bool_ret = (OB_DDL_ADD_SUB_PARTITION == op_type)
    || (OB_DDL_DROP_SUB_PARTITION == op_type);

  return bool_ret;
}

int DdlStmtTask::parse_ddl_info(
    ObLogBR *br,
    const uint64_t row_index,
    const ObLogAllDdlOperationSchemaInfo &all_ddl_operation_table_schema_info,
    bool &is_valid_ddl,
    int64_t &update_schema_version,
    uint64_t &exec_tenant_id,
    volatile bool &stop_flag)
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
   else if (OB_FAIL(row_.parse_cols(
      nullptr,
      host_.get_tenant_id(),
      share::OB_ALL_DDL_OPERATION_TID,
      nullptr,
      nullptr,
      false,
      &all_ddl_operation_table_schema_info))) {
    LOG_ERROR("parse columns fail", KR(ret), K(row_));
  } else if (OB_FAIL(parse_ddl_info_(contain_ddl_stmt, update_schema_version, stop_flag))) {
    if (OB_INVALID_DATA == ret) {
      // If invalid data is encountered, the log is printed but the dirty data is ignored
      LOG_ERROR("fail to parse DDL, __all_ddl_operation table data is invalid",
          "log_lsn", get_host().get_commit_log_lsn(),
          "trans_commit_ts", get_host().get_commit_ts(),
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
      // 3. Offline DDL, should support to add hidden table
      if (is_create_table_ddl_(ddl_operation_type_)) {
        is_valid_ddl = true;
      }

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

      // obadmin performs dynamic add/drop operations on secondary tls_ids with null ddl_stmt_str, but not filtering
      if (is_sub_tls_id_alter_ddl_(ddl_operation_type_)) {
        is_valid_ddl = true;
      }
    }

    if (OB_SUCCESS == ret && is_valid_ddl) {
      if (OB_FAIL(build_ddl_binlog_record_(br, ddl_stmt_str_, row_index))) {
        LOG_ERROR("build_ddl_binlog_record_ fail", KR(ret), K(br), K(ddl_stmt_str_), "commit_version", get_host().get_trans_commit_version(),
            K(row_index));
      } else {
        // set Binlog Record
        set_binlog_record(br);
      }
    }

    if (OB_SUCC(ret)) {
      // 1. If ddl_exec_tenant_id_ is invalid, use ddl_op_tenant_id_, as __all_ddl_operation does not contain an exec_tenant_id column for versions below 1460
      // 2. If ddl_op_tenant_id_ is not valid [e.g. alter system add zone 'z1' etc.], then use the tls_ided tenant_id
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

int DdlStmtTask::parse_ddl_info_(
    bool &contain_ddl_stmt,
    int64_t &update_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ColValueList *new_cols = NULL;
  ColValueList *rowkey_cols = NULL;
  ObLobDataOutRowCtxList *new_lob_ctx_cols = nullptr;

  if (OB_FAIL(row_.get_cols(&rowkey_cols, &new_cols, nullptr, &new_lob_ctx_cols))) {
    LOG_ERROR("get_cols fail", KR(ret), K(row_));
  } else if (OB_ISNULL(rowkey_cols) || OB_ISNULL(new_cols)) {
    LOG_ERROR("get_cols fail", K(new_cols), K(rowkey_cols));
    ret = OB_ERR_UNEXPECTED;
  } else {
    PartTransTask &part_trans_task = get_host();
    if (nullptr != new_lob_ctx_cols && new_lob_ctx_cols->has_out_row_lob()) {
      new_lob_ctx_cols->reset(
          this,
          part_trans_task.get_tenant_id(),
          part_trans_task.get_trans_id(),
          share::OB_ALL_DDL_OPERATION_AUX_LOB_META_TID,
          true/*is_ddl*/);

      IObCDCLobDataMerger *lob_data_merger = TCTX.lob_data_merger_;

      if (OB_ISNULL(lob_data_merger)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("lob_data_merger is nullptr", KR(ret));
      } else if (OB_FAIL(lob_data_merger->push(*new_lob_ctx_cols, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("ObCDCLobDataMerger push failed", KR(ret), KPC(new_lob_ctx_cols), KPC(this));
        }
      } else {}

      while (! new_lob_ctx_cols->is_all_lob_callback_done()) {
        usec_sleep(100);
      }
    }

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

    if (blocksstable::ObDmlFlag::DF_LOCK == row_.dml_flag_) {
      // do nothing
    }
    // only parse insert stmt
    else if (blocksstable::ObDmlFlag::DF_INSERT != row_.dml_flag_) {
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
      if (OB_FAIL(parse_ddl_info_from_normal_columns_(*new_cols, *new_lob_ctx_cols))) {
        LOG_ERROR("parse_ddl_info_from_normal_columns_ fail", KR(ret), K(*new_cols), K(*new_lob_ctx_cols));
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

  if (nullptr != new_lob_ctx_cols && new_lob_ctx_cols->has_out_row_lob()) {
    LOG_INFO("DdlStmtTask", "new_lob_ctx_cols", *new_lob_ctx_cols, KPC(this));
  }

  return ret;
}

int DdlStmtTask::build_ddl_binlog_record_(
    ObLogBR *br,
    const ObString &ddl_stmt,
    const uint64_t row_index)
{
  int ret = OB_SUCCESS;
  const int64_t trans_commit_version = get_host().get_trans_commit_version();
  uint64_t cluster_id = get_host().get_cluster_id();
  // DDL tenant_id records the tenant ID of the tls_id to which it belongs, not the executor tenant ID, to ensure that in schema split
  // scenarios, incremental backup DDLs are not incorrectly distributed to the tenant to which they belong, causing loci to get stuck
  //
  const uint64_t tenant_id = get_host().get_tenant_id();
  IBinlogRecord *br_data = NULL;

  // no need set trace id, trace_info for ddl
  ObString trace_id;
  ObString trace_info;
  ObString ddl_unique_id;
  const int64_t part_trans_task_count = 1;

  if (OB_ISNULL(br) || OB_UNLIKELY(OB_INVALID_ID == row_index)) {
    LOG_ERROR("invalid argument", K(br), K(ddl_stmt), K(trans_commit_version), K(row_index));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(init_ddl_unique_id_(ddl_unique_id))) {
    LOG_ERROR("init_ddl_unique_id_ fail", KR(ret), K(ddl_unique_id));
  } else if (OB_FAIL(br->init_data(EDDL, cluster_id, tenant_id, row_index,
          trace_id, trace_info, ddl_unique_id, ddl_op_schema_version_, trans_commit_version,
          part_trans_task_count))) {
    LOG_ERROR("ObLogBR::init_data EDDL fail", KR(ret), K(trans_commit_version),
        K(cluster_id), K(tenant_id), K(ddl_op_schema_version_),
        K(trace_id), K(trace_info), K(ddl_unique_id), K(part_trans_task_count));
  } else if (OB_ISNULL(br_data = (br->get_data()))) {
    LOG_ERROR("get binlog record data fail", K(br));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // 2 field for columns
    // ddl_stmt_str and ddl_schema_version
    int col_count = 2;
    int64_t column_array_size = sizeof(binlogBuf) * col_count;
    binlogBuf *column_array = static_cast<binlogBuf *>(get_host().alloc(column_array_size));

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
  // TODO currently use ddl_exec_tenant_id_ for unique_id, need check
  DdlStmtUniqueID ddl_stmt_unique_id(ddl_exec_tenant_id_, ddl_op_schema_version_);

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

int DdlStmtTask::parse_ddl_info_from_normal_columns_(
    ColValueList &col_value_list,
    ObLobDataOutRowCtxList &new_lob_ctx_cols)
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
        if (! cv_node->is_out_row_) {
          ddl_stmt_str_ = value.get_varchar();
        } else {
          ObString *new_col_str = nullptr;

          if (OB_FAIL(new_lob_ctx_cols.get_lob_column_value(ALL_DDL_OPERATION_TABLE_DDL_STMT_STR_COLUMN_ID,
                  true/*is_new_col*/, new_col_str))) {
            LOG_ERROR("get_lob_column_value failed", KR(ret), K(ALL_DDL_OPERATION_TABLE_DDL_STMT_STR_COLUMN_ID), K(new_col_str));
          } else {
            ddl_stmt_str_ = *new_col_str;
          }
        }
        break;
      }

      default:
        break;
    }

    cv_node = next;
  }

  return ret;
}

uint64_t DdlStmtTask::combine_id_(
    const bool is_schema_split_mode,
    const uint64_t tenant_id,
    const uint64_t pure_id)
{
  UNUSEDx(is_schema_split_mode, tenant_id);
  return pure_id;
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

ObLogEntryTask::ObLogEntryTask(PartTransTask &host) :
    host_(&host),
    participant_(NULL),
    tls_id_(),
    trans_id_(),
    redo_node_(NULL),
    is_big_row_(false),
    stmt_list_(),
    formatted_stmt_num_(0),
    row_ref_cnt_(0),
    arena_allocator_(host.get_log_entry_task_base_allocator(), "LogEntryTask", host.get_tenant_id())
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
  tls_id_.reset();
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

int ObLogEntryTask::init(
    const logservice::TenantLSID &tls_id,
    const char *participant,
    const transaction::ObTransID &trans_id,
    DmlRedoLogNode *redo_node)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node)) {
    LOG_ERROR("invalid argument", K(redo_node));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tls_id_ = tls_id;
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
    const palf::LSN& store_log_lsn = redo_node_->get_store_log_lsn();

    key.append(participant_);
    key.append("_");
    key.append(std::to_string(store_log_lsn.val_));
  }

  return ret;
}

int ObLogEntryTask::get_log_lsn(palf::LSN &log_lsn)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(redo_node_)) {
    LOG_ERROR("invalid redo_node", K(redo_node_));
    ret = OB_INVALID_DATA;
  } else {
    log_lsn = redo_node_->get_store_log_lsn();
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
      row_ref_cnt = get_row_ref_cnt();

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
  IBinlogRecord *br_data = NULL;
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
    cluster_id_(0),
    type_(TASK_TYPE_UNKNOWN),
    exec_tenant_id_(OB_INVALID_TENANT_ID),
    tls_id_(),
    tls_str_(NULL),
    trans_id_(),
    part_trans_info_str_(),
    is_trans_committed_(false),
    trans_commit_version_(OB_INVALID_VERSION),
    prepare_ts_(OB_INVALID_TIMESTAMP),
    prepare_log_lsn_(),
    commit_ts_(OB_INVALID_TIMESTAMP),
    commit_log_lsn_(),
    trans_type_(transaction::TransType::UNKNOWN_TRANS),
    is_xa_or_dup_(false),
    participants_(),
    trace_id_(),
    trace_info_(),
    sorted_log_entry_info_(),
    sorted_redo_list_(),
    part_tx_fetch_state_(0),
    rollback_list_(),
    ref_cnt_(0),
    multi_data_source_node_arr_(),
    multi_data_source_info_(),
    segment_buf_(),
    checkpoint_seq_(0),
    global_trans_seq_(0),
    global_schema_version_(OB_INVALID_VERSION),
    next_task_(NULL),
    local_schema_version_(OB_INVALID_VERSION),
    stmt_list_(),
    ddl_lob_aux_stmt_list_(),
    data_ready_lock_(),
    is_data_ready_(false),
    wait_data_ready_cond_(),
    wait_formatted_cond_(NULL),
    output_br_count_by_turn_(0),
    allocator_(),
    log_entry_task_base_allocator_()
{
}

PartTransTask::~PartTransTask()
{
  reset();
}

int PartTransTask::set_commit_log_lsn(const palf::LSN &commit_log_lsn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!commit_log_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid commit_log_lsn for PartTransTask", KR(ret), K(commit_log_lsn), KPC(this));
  } else {
    commit_log_lsn_ = commit_log_lsn;
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
  const char *task_type_str = NULL;

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
    case TASK_TYPE_LS_OP_TRANS: {
      task_type_str = "LS_OP";
      break;
    }
    case TASK_TYPE_LS_HEARTBEAT: {
      task_type_str = "LS_HEARTBEAT";
      break;
    }
    case TASK_TYPE_GLOBAL_HEARTBEAT: {
      task_type_str = "GLOBAL_HEARTBEAT";
      break;
    }
    case TASK_TYPE_OFFLINE_LS: {
      task_type_str = "OFFLINE_LS";
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

void PartTransTask::set_task_info(const logservice::TenantLSID &tls_id, const char *info)
{
  tls_id_ = tls_id;
  tls_str_ = info;
}

void PartTransTask::reset()
{
  destroy_participant_array_();

  serve_state_ = SERVED;
  cluster_id_ = 0;
  type_ = TASK_TYPE_UNKNOWN;
  exec_tenant_id_ = OB_INVALID_TENANT_ID;
  tls_id_.reset();
  tls_str_ = NULL;
  trans_id_.reset();
  part_trans_info_str_.reset();
  is_trans_committed_ = false;
  trans_commit_version_ = OB_INVALID_VERSION;
  prepare_ts_ = OB_INVALID_TIMESTAMP;
  prepare_log_lsn_.reset();
  commit_ts_ = OB_INVALID_TIMESTAMP;
  commit_log_lsn_.reset();
  trans_type_ = transaction::TransType::UNKNOWN_TRANS;
  is_xa_or_dup_ = false;
  participants_.reset();
  // The trace_id memory does not need to be freed separately, the allocator frees it all together
  trace_id_.reset();
  trace_info_.reset();
  sorted_log_entry_info_.reset();
  sorted_redo_list_.reset();
  part_tx_fetch_state_ = 0;
  rollback_list_.reset();
  ref_cnt_ = 0;
  multi_data_source_node_arr_.reset();
  multi_data_source_info_.reset();
  segment_buf_.reset();
  checkpoint_seq_ = 0;
  global_trans_seq_ = 0;
  global_schema_version_ = OB_INVALID_VERSION;
  next_task_ = NULL;

  local_schema_version_ = OB_INVALID_VERSION;
  free_stmt_list();
  free_ddl_lob_aux_stmt_list();
  is_data_ready_ = false;
  wait_formatted_cond_ = NULL;
  output_br_count_by_turn_ = 0;
  // reuse memory
  allocator_.reset();
  log_entry_task_base_allocator_.destroy();
}

int PartTransTask::init_log_entry_task_allocator()
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(tls_id_.get_tenant_id(), "LogEntryTaskBas");
  const int64_t cache_block_count = 4; // nway for vslice_alloc

  if (OB_FAIL(log_entry_task_base_allocator_.init(
      OB_MALLOC_MIDDLE_BLOCK_SIZE,
      attr,
      cache_block_count))) {
    LOG_ERROR("init log_entry_task_base_allocator_ failed", KR(ret), KPC(this));
  }

  return ret;
}

int PartTransTask::push_redo_log(
    const transaction::ObTransID &trans_id,
    const palf::LSN &log_lsn,
    const int64_t tstamp,
    const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObMemtableMutatorMeta meta;
  int64_t pos = 0;
  const bool need_store_data = need_store_data_();

  if (OB_UNLIKELY(!log_lsn.is_valid())
      || OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len <= 0)) {
    LOG_ERROR("invalid arguments", K(log_lsn), KP(buf), K(buf_len));
    ret = OB_INVALID_ARGUMENT;
  }
  // deserialize meta
  else if (OB_FAIL(meta.deserialize(buf, buf_len, pos))) {
    LOG_ERROR("deserialize ObMemtableMutatorMeta fail", KR(ret), K(meta),
        K(log_lsn), K(buf), K(buf_len), K(pos));
  }
  // Check the length of the data recorded in the meta, it can't be smaller than the length of the redo data
  // otherwise the data does not match the one recorded in the meta
  else if (OB_UNLIKELY(meta.get_total_size() < buf_len)) {
    LOG_ERROR("meta total size is little than mutator log data size",
        K(meta), K(buf_len), K(log_lsn));
    ret = OB_INVALID_DATA;
  } else {
    const char *redo_data = buf + pos;
    int64_t redo_data_size = buf_len - pos;
    const uint8_t row_flags = meta.get_flags();

    if (meta.is_row_start()) {
      // If it is the start of a row, a new redo node is generated
      if (OB_FAIL(push_redo_on_row_start_(need_store_data, trans_id, meta, log_lsn, redo_data, redo_data_size))) {
        if (OB_ENTRY_EXIST == ret) {
          // redo log duplicate
        } else {
          LOG_ERROR("push_redo_on_row_start_ fail", KR(ret), K(trans_id), K(meta), K(log_lsn),
              KP(redo_data), K(redo_data_size));
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("currently not support big row", KR(ret), K(meta));
    }

    // TODO AUTO mode
    if (OB_SUCC(ret)) {
      palf::LSN store_log_lsn;
      const char *data_buf = NULL;
      int64_t data_len = 0;
      bool is_row_completed = true;

      if ( ! is_sys_ls_part_trans()) {
        if (ObTransRowFlag::is_normal_row(row_flags)) {
          store_log_lsn = log_lsn;
          data_buf = redo_data;
          data_len = redo_data_size;
        } else {
          ret = OB_NOT_SUPPORTED;
        }

        // 1. DML data, a redo data aggregation will be dispatched
        // 2. duplicate redo logs won't dispatch
        if (OB_SUCC(ret) && need_store_data && is_row_completed) {
          if (OB_FAIL(get_and_submit_store_task_(tls_id_.get_tenant_id(), row_flags, store_log_lsn,
                  data_buf, data_len))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("get_and_submit_store_task_ fail", KR(ret), K_(tls_id), K(row_flags));
            }
          }
        } // need_store_data
      }
    }
  }

  LOG_DEBUG("push redo log", KR(ret), K_(tls_id), K(log_lsn), K(tstamp), K(buf_len), K(meta),
      K(trans_id), K_(sorted_redo_list));

  return ret;
}

int PartTransTask::push_rollback_to_info(const palf::LSN &lsn, const ObTxSEQ &rollback_from, const ObTxSEQ &rollback_to)
{
  int ret = OB_SUCCESS;
  RollbackNode *rollback_node = static_cast<RollbackNode*>(allocator_.alloc(sizeof(RollbackNode)));

  if (OB_ISNULL(rollback_node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("rollback node is null", KR(ret), K_(tls_id), K_(trans_id), K(rollback_from), K(rollback_to));
  } else {
    new(rollback_node) RollbackNode(rollback_from, rollback_to);
    if (OB_FAIL(rollback_list_.add(rollback_node))) {
      LOG_ERROR("rollback_list_ add fail", KR(ret), K_(tls_id), K_(trans_id), K_(rollback_list), KPC(rollback_node));
    } else {
      LOG_INFO("rollback_list add succ", K_(tls_id), K_(trans_id), KPC(rollback_node), K_(rollback_list));
    }
  }

  return ret;
}

int PartTransTask::push_fetched_log_entry(const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  LogEntryNode *log_entry_node = NULL;

  if (OB_UNLIKELY(!lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid lsn pushed to fetched_log_entry_arr", KR(ret), K(lsn), KPC(this));
  } else if (OB_FAIL(alloc_log_entry_node_(lsn, log_entry_node))) {
    LOG_ERROR("alloc_log_entry_node_ failed", KR(ret), K(lsn), KPC(this));
  } else if (OB_FAIL(sorted_log_entry_info_.push_fetched_log_entry_node(log_entry_node))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_ERROR("push_fetched_log_entry_node failed", KR(ret), KPC(log_entry_node), KPC(this));
    } else {
      LOG_DEBUG("duplicate log_entry", KR(ret), KPC(log_entry_node), KPC(this));
    }
  }

  return ret;
}

int PartTransTask::push_back_recored_redo_lsn_arr(
    const transaction::ObRedoLSNArray &prev_redo_lsns,
    const palf::LSN &commit_info_lsn,
    const bool has_redo_in_cur_entry)
{
  int ret = OB_SUCCESS;

  for(int i = 0; OB_SUCC(ret) && i < prev_redo_lsns.count(); i++) {
    const palf::LSN &lsn = prev_redo_lsns.at(i);

    if (OB_FAIL(sorted_log_entry_info_.push_recorded_log_entry(lsn))) {
      LOG_ERROR("push_back_recored_redo_lsn_arr failed", KR(ret), K(lsn), K_(sorted_log_entry_info));
    }
  }
  if (OB_SUCC(ret) && has_redo_in_cur_entry
      && OB_FAIL(sorted_log_entry_info_.push_recorded_log_entry(commit_info_lsn))) {
    LOG_ERROR("push_back_recored_redo_lsn_arr failed", KR(ret), K(commit_info_lsn), KPC(this));
  }

  return ret;
}

int PartTransTask::push_multi_data_source_data(
    const palf::LSN &lsn,
    const transaction::ObTxBufferNodeArray &mds_data_arr,
    const bool is_commit_log)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < mds_data_arr.count(); i++) {
    const transaction::ObTxBufferNode &mds_buffer_node = mds_data_arr.at(i);
    const transaction::ObTxDataSourceType &mds_type = mds_buffer_node.get_data_source_type();
    const common::ObString &data = mds_buffer_node.get_data_buf();
    const int64_t data_buf_size = data.length();

    if (OB_UNLIKELY(0 >= data_buf_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("buf of multi_data_source_data is less than 0", KR(ret));
    } else {
      switch (mds_type) {
        case transaction::ObTxDataSourceType::DDL_TRANS:
        {
          if (! is_commit_log) {
            multi_data_source_info_.set_ddl_trans();

            if (OB_FAIL(alloc_and_save_multi_data_source_node_(lsn, mds_buffer_node))) {
              LOG_ERROR("alloc_and_save_multi_data_source_node_ failed", KR(ret), K_(tls_id), K_(trans_id), K(lsn));
            }
          }
          break;
        }
        case transaction::ObTxDataSourceType::CREATE_TABLET_NEW_MDS:
        case transaction::ObTxDataSourceType::DELETE_TABLET_NEW_MDS:
        {
          if (! is_commit_log) {
            if (OB_FAIL(alloc_and_save_multi_data_source_node_(lsn, mds_buffer_node))) {
              LOG_ERROR("alloc_and_save_multi_data_source_node_ failed", KR(ret), K_(tls_id), K_(trans_id), K(lsn));
            }
          }
          break;
        }
        case transaction::ObTxDataSourceType::LS_TABLE:
        {
          // only push_back ls_op in multi_data_source_log, in case of reentrant of commit_log
          // while handling miss_log.
          if (! is_commit_log) {
            int64_t pos = 0;
            share::ObLSAttr ls_attr;;

            if (OB_FAIL(ls_attr.deserialize(data.ptr(), data_buf_size, pos))) {
              LOG_ERROR("deserialize ls_table_op failed", KR(ret), K_(tls_id), K_(trans_id), K(lsn),
                  K(mds_buffer_node), K(ls_attr));
            } else if (OB_FAIL(multi_data_source_info_.push_back_ls_table_op(ls_attr))) {
              LOG_ERROR("push_back ls_table_op info multi_data_source_info failed", KR(ret),
                  K_(tls_id), K_(trans_id), K(lsn), K(mds_buffer_node), K(ls_attr));
            } else {
              LOG_INFO("resolver found ls_attr in multi_data_source log", K_(tls_id), K_(trans_id), K(lsn),
                  K(mds_buffer_node), K(ls_attr));
            }
          }
          break;
        }
        default:
        {
          LOG_INFO("ignore not_supportted multi_data_source type", KR(ret), K_(tls_id), K_(trans_id), K(lsn),
              K(is_commit_log), K(mds_type));
          break;
        }
      }

      LOG_DEBUG("found multi_data_source data", KR(ret), K_(tls_id), K_(trans_id), K(lsn),
          K(is_commit_log), K(mds_buffer_node), K_(multi_data_source_info), K_(multi_data_source_node_arr));
    }

  }

  return ret;
}

int PartTransTask::parse_multi_data_source_data()
{
  int ret = OB_SUCCESS;

  ARRAY_FOREACH_N(multi_data_source_node_arr_, idx, count) {
    MultiDataSourceNode &mds_node = multi_data_source_node_arr_.at(idx);
    ObCDCTabletChangeInfo tablet_change_info;

    if (mds_node.is_tablet_change_node()) {
      if (OB_FAIL(parse_tablet_change_mds_(mds_node, tablet_change_info))) {
        LOG_ERROR("parse_tablet_change_mds_ failed", KR(ret), K_(tls_id), K_(trans_id), K(mds_node), K(idx), KPC(this));
      } else if (OB_FAIL(multi_data_source_info_.push_back_tablet_change_info(tablet_change_info))) {
        LOG_ERROR("push_back_tablet_change_info failed", KR(ret), K_(tls_id), K_(trans_id), K(mds_node), K(tablet_change_info),
            K_(multi_data_source_info), KPC(this));
      } else {
        LOG_DEBUG("[LS_MEMBER_CHANGE_INFO]", K_(tls_id), K_(trans_id), K(tablet_change_info), K(mds_node));
        // success
      }
    }
  }

  return ret;
}

int PartTransTask::parse_multi_data_source_data_for_ddl(
    const char *caller)
{
  int ret = OB_SUCCESS;
  DictTenantArray &tenant_metas = multi_data_source_info_.get_dict_tenant_array();
  DictDatabaseArray &database_metas = multi_data_source_info_.get_dict_database_array();
  DictTableArray &table_metas = multi_data_source_info_.get_dict_table_array();

  ARRAY_FOREACH_N(multi_data_source_node_arr_, idx, count) {
    MultiDataSourceNode &mds_node = multi_data_source_node_arr_.at(idx);

    if (mds_node.is_ddl_trans_node()) {
      const transaction::ObTxBufferNode &tx_buf_node = mds_node.get_tx_buf_node();
      const common::ObString &mds_data = tx_buf_node.get_data_buf();
      int64_t pos = 0;

      if (OB_FAIL(datadict::ObDataDictStorage::parse_dict_metas(allocator_, mds_data.ptr(), mds_data.length(), pos,
            tenant_metas, database_metas, table_metas))) {
        LOG_ERROR("ObDataDictStorage parse_dict_metas failed", KR(ret), K(caller), K(idx), KPC(this));
      } else {
        //TODO DEBUG
        LOG_INFO("parse_dict_metas succ", K(caller), "tenant_id", tls_id_.get_tenant_id(), K_(trans_id),
            K(tenant_metas), K(database_metas), K(table_metas));
      }
    }
  }

  return ret;
}

int PartTransTask::get_tenant_schema_info_with_inc_dict(const uint64_t tenant_id, TenantSchemaInfo &tenant_schema_info)
{
  int ret = OB_SUCCESS;

  if (is_sys_ls_part_trans() && is_ddl_trans()) {
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invald args", KR(ret), K(tenant_id));
    } else if (OB_FAIL(multi_data_source_info_.get_new_tenant_scehma_info(tenant_id, tenant_schema_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // TODO: add check if must get from inc_data_dict
        // try get from BASE DATA_DICT
        ObDictTenantInfoGuard dict_tenant_info_guard;
        ObDictTenantInfo *tenant_info = nullptr;

        if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(
            tenant_id,
            dict_tenant_info_guard))) {
          LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("tenant_info is nullptr", KR(ret), K(tenant_id));
        } else if (OB_FAIL(tenant_info->get_tenant_schema_info(tenant_schema_info))) {
          LOG_ERROR("get_tenant_schema_info from ObDictTenantInfoGuard failed", KR(ret), K(tenant_id), KPC(tenant_info));
        }
      } else {
        LOG_ERROR("get_new_tenant_scehma_info from MultiDataSourceInfo failed", KR(ret), K(tenant_id), KPC(this));
      }
    }
  } else {
    // ONLY used for SYS_LS PartTransTask of DDL_TRANS
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int PartTransTask::get_database_schema_info_with_inc_dict(
    const uint64_t tenant_id,
    const uint64_t db_id,
    DBSchemaInfo &db_schema_info)
{
  int ret = OB_SUCCESS;

  if (is_sys_ls_part_trans() && is_ddl_trans()) {
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_UNLIKELY(OB_INVALID_ID == db_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invald args", KR(ret), K(tenant_id), K(db_id));
    } else if (OB_FAIL(multi_data_source_info_.get_new_database_scehma_info(tenant_id, db_id, db_schema_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // TODO: add check if must get from inc_data_dict
        // try get from BASE DATA_DICT
        ObDictTenantInfoGuard dict_tenant_info_guard;
        ObDictTenantInfo *tenant_info = nullptr;

        if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(
            tenant_id,
            dict_tenant_info_guard))) {
          LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("tenant_info is nullptr", KR(ret), K(tenant_id));
        } else if (OB_FAIL(tenant_info->get_database_schema_info(db_id, db_schema_info))) {
          LOG_ERROR("get_database_schema_info from ObDictTenantInfoGuard failed", KR(ret), K(tenant_id), K(db_id), KPC(tenant_info));
        }
      } else {
        LOG_ERROR("get_new_database_scehma_info from MultiDataSourceInfo failed", KR(ret), K(tenant_id), K(db_id), KPC(this));
      }
    }
  } else {
    // ONLY used for SYS_LS PartTransTask of DDL_TRANS
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int PartTransTask::get_table_meta_with_inc_dict(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const datadict::ObDictTableMeta *&tb_meta)
{
  int ret = OB_SUCCESS;

  if (is_sys_ls_part_trans() && is_ddl_trans()) {
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_UNLIKELY(OB_INVALID_ID == table_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invald args", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(multi_data_source_info_.get_new_table_meta(tenant_id, table_id, tb_meta))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // TODO: add check if must get from inc_data_dict
        // try get from BASE DATA_DICT
        ObDictTenantInfoGuard dict_tenant_info_guard;
        ObDictTenantInfo *tenant_info = nullptr;

        if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(
            tenant_id,
            dict_tenant_info_guard))) {
          LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("tenant_info is nullptr", KR(ret), K(tenant_id));
        } else if (OB_FAIL(tenant_info->get_table_schema(tenant_id, table_id, tb_meta))) {
          LOG_ERROR("get_table_meta from ObDictTenantInfoGuard failed", KR(ret), K(tenant_id), K(table_id), KPC(tenant_info));
        }
      } else {
        LOG_ERROR("get_new_table_meta from MultiDataSourceInfo failed", KR(ret), K(tenant_id), K(table_id), KPC(this));
      }
    } else if (OB_ISNULL(tb_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expect tb_meta", KR(ret), K(tenant_id), K(table_id));
    }
  } else {
    // ONLY used for SYS_LS PartTransTask of DDL_TRANS
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int PartTransTask::check_for_ddl_trans(
    bool &is_not_barrier,
    ObSchemaOperationType &op_type) const
{
  int ret = OB_SUCCESS;
  is_not_barrier = false;
  int64_t other_ddl_count = 0;
  IStmtTask *stmt_task = get_stmt_list().head_;

  while (NULL != stmt_task && OB_SUCC(ret)) {
    DdlStmtTask *ddl_stmt = dynamic_cast<DdlStmtTask *>(stmt_task);

    if (OB_UNLIKELY(! stmt_task->is_ddl_stmt()) || OB_ISNULL(ddl_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid DDL statement", KR(ret), KPC(stmt_task), K(ddl_stmt));
    } else {
      op_type = static_cast<ObSchemaOperationType>(ddl_stmt->get_operation_type());

      // TODO It refer to the create table DDL as a barrer because the Online DDL may cause the incremental
      // data dictionary information containing two tables (such as: the hidden table and original table),
      // resulting in incorrect replay of the incremental data dictionary.
      if (OB_DDL_CREATE_TABLE == op_type) {
        if (get_multi_data_source_info().is_contains_multiple_table_metas()) {
          is_not_barrier = false;
        } else {
          is_not_barrier = true;
        }
      } else {
        ++other_ddl_count;
      }
      stmt_task = stmt_task->get_next();
    }
  } // while

  if (OB_SUCC(ret)) {
    // Normally, a DDL transaction only contains one DDL statement.
    // If there are multiple statements, the DDL transaction is treated as barrier to avoid misjudgments
    if (other_ddl_count > 0) {
      is_not_barrier = false;
    }
  }

  return ret;
}

int PartTransTask::alloc_log_entry_node_(const palf::LSN &lsn, LogEntryNode *&log_entry_node)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid lsn of log_entry", KR(ret), K_(tls_id), K_(trans_id), K(lsn));
  } else if (OB_ISNULL(log_entry_node = static_cast<LogEntryNode*>(allocator_.alloc(sizeof(LogEntryNode))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc log_entry_node failed", KR(ret), K(lsn));
  } else {
    new(log_entry_node) LogEntryNode(lsn);
    if (OB_ISNULL(log_entry_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("construct LogEntryNode failed", KR(ret), K(lsn));
    }
  }

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

int PartTransTask::push_redo_on_row_start_(
    const bool need_store_data,
    const transaction::ObTransID &trans_id,
    const ObMemtableMutatorMeta &meta,
    const palf::LSN &log_lsn,
    const char *redo_data,
    const int64_t redo_data_size)
{
  int ret = OB_SUCCESS;
  // Length of the actual data, minus the meta information
  const int64_t mutator_row_size = meta.get_data_size();

  if (is_sys_ls_part_trans()) {
    if (OB_FAIL(push_ddl_redo_on_row_start_(meta, log_lsn, redo_data, redo_data_size, mutator_row_size))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_ERROR("push_ddl_redo_on_row_start_ fail", KR(ret), K(trans_id), K(meta), K(log_lsn),
            KP(redo_data), K(redo_data_size));
      }
    }
  } else {
    if (OB_FAIL(push_dml_redo_on_row_start_(need_store_data, meta, log_lsn, redo_data, redo_data_size,
            mutator_row_size))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_ERROR("push_dml_redo_on_row_start_ fail", KR(ret), K(trans_id), K(meta), K(log_lsn),
            KP(redo_data), K(redo_data_size));
      }
    }
  }

  return ret;
}

int PartTransTask::push_ddl_redo_on_row_start_(
    const ObMemtableMutatorMeta &meta,
    const palf::LSN &log_lsn,
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
  // DDL tls_ided transaction task allocates buffer of actual data length
  } else if (OB_ISNULL(mutator_row_data = static_cast<char *>(allocator_.alloc(mutator_row_size)))) {
    LOG_ERROR("allocate memory for mutator row data fail", K(mutator_row_size), K(meta));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    // Fill the data carried in this redo
    (void)MEMCPY(mutator_row_data, redo_data, redo_data_size);

    // reset redo log node
    node->reset(log_lsn, mutator_row_data, mutator_row_size, redo_data_size);

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

int PartTransTask::push_dml_redo_on_row_start_(
    const bool need_store_data,
    const memtable::ObMemtableMutatorMeta &meta,
    const palf::LSN &log_lsn,
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

        meta_node->init_for_data_memory(log_lsn, mutator_row_data, mutator_row_size, redo_data_size);
      }
    } else {
      // need store
      // reset redo log meta node
      meta_node->init_for_data_persistence(log_lsn, mutator_row_size);
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
  }

  return ret;
}

int PartTransTask::get_and_submit_store_task_(
    const uint64_t tenant_id,
    const uint8_t row_flags,
    const palf::LSN &log_lsn,
    const char *data_buf,
    const int64_t data_len)
{
  int ret = OB_SUCCESS;
  ObLogStoreTask *store_task = NULL;

  // Allocate ObLogStoreTask and init
  if (OB_ISNULL(store_task = ObLogStoreTaskFactory::alloc())) {
    LOG_ERROR("store_task is NULL");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(store_task->init(tls_id_, log_lsn,
          data_buf, data_len, this))) {
    LOG_ERROR("store_task init fail", KR(ret), K_(tls_id), K(log_lsn),
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
      LOG_ERROR("batch_buffer submit fail", KR(ret), K_(tls_id), K(log_lsn));
    } else {}
  }

  if (OB_SUCC(ret)) {
    if (ObTransRowFlag::is_big_row(row_flags)) {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("big row not support currently", KR(ret), K_(tls_id), K_(trans_id), K(row_flags), K(log_lsn));
    }
  }

  if (OB_FAIL(ret)) {
    ObLogStoreTaskFactory::free(store_task);
    store_task = NULL;
  }

  return ret;
}

int PartTransTask::set_commit_info(
    const ObString &trace_id,
    const ObString &trace_info,
    const bool is_dup_tx,
    const transaction::ObXATransID &xid)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_trace_id_(trace_id))) {
    LOG_ERROR("init_trace_id_ fail", KR(ret), K(trace_id));
  } else if (OB_FAIL(init_trace_info_(trace_info))) {
    LOG_ERROR("init_trace_info_ fail", KR(ret), K(trace_info));
  } else if (is_dup_tx || (! xid.empty() && xid.is_valid())) {
    is_xa_or_dup_ = true;
    LOG_DEBUG("found dup_tx or dist_xa_tx", K(is_dup_tx), K(xid), K_(tls_id), K_(trans_id));
  }

  if (OB_SUCC(ret)) {
    // mark already read commit_info log
    // could change to a strict verify mode if check !has_read_commit_info and
    // set read_commit_info at the end of process commit_info_log(Resolver::handle_commit_info_)
    mark_read_commit_info();
  }

  return ret;
}

int PartTransTask::prepare(
    const palf::LSN &prepare_log_lsn,
    const int64_t prepare_ts,
    PartTransDispatcher &part_trans_dispatcher)
{
  int ret = OB_SUCCESS;
  prepare_ts_ = prepare_ts;
  prepare_log_lsn_ = prepare_log_lsn;

  if (OB_UNLIKELY(has_read_prepare())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("expect read prepare_log only once", KR(ret), KPC(this));
  } else if (OB_FAIL(part_trans_dispatcher.submit_task(*this))) {
    LOG_ERROR("submit part_trans_task to part_trans_dispatcher failed", KR(ret), KPC(this));
  } else {
    mark_read_prepare();
    LOG_DEBUG("preapre_normal_trans", KR(ret), K_(tls_id), K_(trans_id), K(prepare_log_lsn), K(prepare_ts));
  }

  return ret;
}

// expect all redo readed
int PartTransTask::commit(
    const uint64_t cluster_id,
    const transaction::ObTransID &tx_id,
    const int64_t trans_commit_version,
    const transaction::TransType &trans_type,
    const transaction::ObLSLogInfoArray &ls_info_array,
    const palf::LSN &commit_log_lsn,
    const int64_t commit_log_submit_ts,
    const bool is_data_dict_mode)
{
  int ret = OB_SUCCESS;
  trans_type_ = trans_type; // set trans_type first, will be used in following process.

  // 1. decide task type(DDL/DML/LS_TABLE)
  if (tls_id_.is_sys_log_stream() && multi_data_source_info_.is_valid()) {
    if (multi_data_source_info_.is_ddl_trans()) {
      type_ = TASK_TYPE_DDL_TRANS;
    } else if (multi_data_source_info_.has_ls_table_op()) {
      type_ = TASK_TYPE_LS_OP_TRANS;
      int64_t idx = 0;
      int64_t ls_attr_cnt = 0;
      const share::ObLSAttrArray &ls_attr_arr = multi_data_source_info_.get_ls_attr_arr();;

      ARRAY_FOREACH_N(ls_attr_arr, idx, ls_attr_cnt)
      {
        const share::ObLSAttr &ls_attr = ls_attr_arr.at(idx);

        if (OB_UNLIKELY(! ls_attr.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("ls_attr from multi_data_source is not valid", KR(ret), K(idx), K(ls_attr_cnt), K(ls_attr), K(ls_attr_arr), KPC(this));
        } else if (OB_FAIL(ObLogLSOpProcessor::process_ls_op(
            tls_id_.get_tenant_id(),
            commit_log_lsn,
            commit_log_submit_ts,
            ls_attr))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_ERROR("ObLogLSOpProcessor process_ls_op failed", KR(ret), K(tls_id_), K(tx_id), K(commit_log_lsn),
                K(commit_log_submit_ts), K(ls_attr));
          } else {
            if (is_data_dict_mode) {
              // In Data dictionary, it need to fetch the log of the baseline data dict before adding a tenant,
              // and if it encounter a log stream operation in the process of building the data dictionary,
              // it need to ignore it and rely on the incremental replay process of the data dictionary.
              ret = OB_SUCCESS;
              LOG_INFO("ObLogLSOpProcessor process_ls_op when tenant is not exist", KR(ret), K(tls_id_), K(tx_id), K(commit_log_lsn),
                  K(commit_log_submit_ts), K(ls_attr));
            } else {
              LOG_ERROR("ObLogLSOpProcessor process_ls_op failed", KR(ret), K(tls_id_), K(tx_id), K(commit_log_lsn),
                  K(commit_log_submit_ts), K(ls_attr), K(idx), K(ls_attr_arr));
            }
          }
        } else {
          LOG_INFO("ObLogLSOpProcessor process_ls_op succ", K(tls_id_), K(tx_id), K(commit_log_lsn),
              K(commit_log_submit_ts), K(ls_attr), K(idx), K(ls_attr_arr));
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("not supported multi_data_source_data in sys_logstream", KR(ret), K_(tls_id), K(tx_id),
          K(trans_commit_version), K_(multi_data_source_info));
    }
  } else {
    type_ = TASK_TYPE_DML_TRANS;
  }

  if (OB_SUCC(ret)) {
    // 2. decide participants
    if (OB_UNLIKELY(transaction::ObTransVersion::INVALID_TRANS_VERSION == trans_commit_version)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid arguemnts", KR(ret), K(tx_id), K(trans_commit_version), K(ls_info_array), K(cluster_id));
    } else if (OB_UNLIKELY(! has_read_prepare())) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("expect already read prepare log", KR(ret),
          K(cluster_id), K_(tls_id), K(tx_id), K(trans_commit_version), K(trans_type), K(ls_info_array),
          K(commit_log_lsn), K(commit_log_submit_ts), KPC(this));
    } else if (OB_FAIL(init_participant_array_(ls_info_array, commit_log_lsn))) {
      LOG_ERROR("init_participant_array_ fail", KR(ret), K_(tls_id), K(tx_id),
          K(trans_commit_version), K(trans_type), K(ls_info_array), K(commit_log_lsn), KPC(this));
    } else if (OB_FAIL(to_string_part_trans_info_())) {
      LOG_ERROR("to_string_part_trans_info_str failed", KR(ret), K(trans_commit_version), K(cluster_id), K(commit_log_lsn), KPC(this));
    } else {
      // 3. trans_version, cluster_id and commit_log_lsn
      commit_ts_ = commit_log_submit_ts;
      commit_log_lsn_ = commit_log_lsn;

      // data from cluster that should not sync already filtered in PartTransResolver
      cluster_id_ = cluster_id;
      trans_commit_version_ = trans_commit_version;
      ATOMIC_STORE(&is_trans_committed_, true);

      if (! tls_id_.is_sys_log_stream()) {
        set_ref_cnt(sorted_redo_list_.get_node_number() + 1);
      } else if (is_sys_ls_dml_trans()) {
        // set ref for DML in SYS_LS
        set_ref_cnt(1);
      }
    }
  }

  LOG_DEBUG("commit_normal_trans", KR(ret), K_(tls_id), K(tx_id), K(commit_log_lsn), K_(type),
      K_(multi_data_source_info), K_(is_trans_committed), K(trans_commit_version), K(ls_info_array));

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
  } else if (is_sys_ls_dml_trans()) {
    set_data_ready();
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

  if (OB_UNLIKELY(is_sys_ls_part_trans())) {
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

  if (OB_UNLIKELY(is_sys_ls_part_trans())) {
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

  if (OB_UNLIKELY(is_sys_ls_part_trans())) {
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

int PartTransTask::alloc_and_save_multi_data_source_node_(
    const palf::LSN &lsn,
    const transaction::ObTxBufferNode &mds_buffer_node)
{
  int ret = OB_SUCCESS;
  const transaction::ObTxDataSourceType &mds_type = mds_buffer_node.get_data_source_type();
  const common::ObString &data = mds_buffer_node.get_data_buf();
  const int64_t data_buf_size = data.length();
  MultiDataSourceNode node;
  int64_t pos = 0;
  char *buf = static_cast<char*>(allocator_.alloc(data_buf_size));

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("multi_data_source_buf alloc failed", KR(ret), K_(tls_id), K_(trans_id), K(lsn),
        K(mds_buffer_node), K(data), K(data_buf_size));
  } else {
    MEMCPY(buf, data.ptr(), data_buf_size);

    if (OB_FAIL(node.init(lsn, mds_type, buf, data_buf_size))) {
      LOG_ERROR("init multi_data_source_node failed", KR(ret), K_(tls_id), K_(trans_id), K(lsn),
          K(mds_buffer_node));
    } else if (OB_FAIL(multi_data_source_node_arr_.push_back(node))) {
      LOG_ERROR("push_back multi_data_source_node failed", KR(ret), K_(tls_id), K_(trans_id), K(lsn),
          K(mds_buffer_node), K(node), K_(multi_data_source_node_arr));
    } else {
      // success
    }
  }

  return ret;
}

int PartTransTask::parse_tablet_change_mds_(
    const MultiDataSourceNode &multi_data_source_node,
    ObCDCTabletChangeInfo &tablet_change_info)
{
  int ret = OB_SUCCESS;
  const transaction::ObTxBufferNode &tx_buf_node = multi_data_source_node.get_tx_buf_node();

  if (OB_UNLIKELY(! multi_data_source_node.is_valid())
      || OB_UNLIKELY(! multi_data_source_node.is_tablet_change_node())
      || OB_UNLIKELY(! tx_buf_node.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("multi_data_source is not invalid for ls_member_table_mds", KR(ret), K_(tls_id), K_(trans_id),
        K(multi_data_source_node));
  } else {
    const common::ObString &mds_data = tx_buf_node.get_data_buf();

    if (OB_FAIL(tablet_change_info.parse_from_multi_data_source_buf(tls_id_, tx_buf_node))) {
      LOG_ERROR("parse_from_multi_data_source_buf to tablet_change_info failed", KR(ret), K_(tls_id), K_(trans_id),
          K(multi_data_source_node), K(tx_buf_node));
    } else if (OB_UNLIKELY(! tablet_change_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet_change_info deserialize from mulit_data_source_node is invalid", KR(ret), K_(tls_id), K_(trans_id),
          K(multi_data_source_node), K(tx_buf_node), K(tablet_change_info));
    } else {
      LOG_DEBUG("get tablet_change_info", K_(tls_id), K_(trans_id), K(tablet_change_info), K(multi_data_source_node));
    }
  }

  return ret;
}

int PartTransTask::init_ls_heartbeat_info(const logservice::TenantLSID &tls_id, const int64_t timestamp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(TASK_TYPE_UNKNOWN != type_)) {
    LOG_ERROR("task has been initialized", K(type_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(timestamp <= 0)) {
    LOG_ERROR("invalid argument", K(timestamp));
    ret = OB_INVALID_ARGUMENT;
  } else {
    type_ = TASK_TYPE_LS_HEARTBEAT;
    tls_id_ = tls_id;
    prepare_ts_ = timestamp;
    commit_ts_ = timestamp;
    trans_commit_version_ = timestamp;
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
    prepare_ts_ = timestamp;
    commit_ts_ = timestamp;
    trans_commit_version_ = timestamp;
    LOG_DEBUG("generate global heartbeat", K(timestamp), "delay", NTS_TO_DELAY(timestamp));
  }

  return ret;
}

int PartTransTask::init_offline_ls_task(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(TASK_TYPE_UNKNOWN != type_)) {
    LOG_ERROR("task has been initialized", K(type_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(! tls_id.is_valid())) {
    LOG_ERROR("invalid argument", K(tls_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    type_ = TASK_TYPE_OFFLINE_LS;
    tls_id_ = tls_id;
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
      && tls_id_.is_valid()
      && commit_ts_ > 0
      && trans_id_.is_valid()
      && commit_log_lsn_.is_valid()
      && trans_commit_version_ >= 0);
}

bool PartTransTask::is_task_info_valid() const
{
  bool bool_ret = false;

  // All types of transactions require a valid checkpoint seq
  if (checkpoint_seq_ < 0) {
    bool_ret = false;
  } else {
    switch (type_) {
      case TASK_TYPE_LS_HEARTBEAT:
        // Partitioned heartbeats task require both timestamps and tls_ids to be valid
        bool_ret = (commit_ts_ > 0) && (tls_id_.is_valid());
        break;

      case TASK_TYPE_GLOBAL_HEARTBEAT:
        // Global heartbeat only requires a valid timestamp
        bool_ret = (commit_ts_ > 0);
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

      case TASK_TYPE_LS_OP_TRANS:
        // Basic transaction information is valid
        bool_ret = (is_base_trans_info_valid_());
        break;

      case TASK_TYPE_OFFLINE_LS:
        // Offline task only require valid tls_id
        bool_ret = (tls_id_.is_valid());
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

int PartTransTask::add_ddl_lob_aux_stmt(const uint64_t row_index, DmlStmtTask *stmt_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_ID == row_index)
      || OB_ISNULL(stmt_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(row_index), KPC(stmt_task));
  } else if (OB_FAIL(ddl_lob_aux_stmt_list_.add(stmt_task))) {
    LOG_ERROR("add stmt task into ddl_lob_aux_stmt_list_ fail", KR(ret), K(stmt_list_), KP(stmt_task));
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
  if (is_sys_ls_dml_trans()) {
    sorted_redo_list_.mark_sys_ls_dml_trans_dispatched();
  } else {
    sorted_redo_list_.init_iterator();
  }
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
      char *buf = static_cast<char*>(allocator_.alloc(trace_info_len + 1));

      if (OB_ISNULL(buf)) {
        LOG_ERROR("allocate memory for trace id buffer fail", K(buf), K(trace_info_len));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buf, trace_info.ptr(), trace_info_len);
        buf[trace_info_len] = '\0';
        trace_info_.assign_ptr(buf, trace_info_len);
      }
    }
  }

  return ret;
}

int PartTransTask::init_participant_array_(
    const transaction::ObLSLogInfoArray &participants,
    const palf::LSN &commit_log_lsn)
{
  int ret = OB_SUCCESS;
  const int64_t part_count = is_single_ls_trans() ? 1 : participants.count();

  if (OB_UNLIKELY(! tls_id_.is_valid() || ! commit_log_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tls_id or commit_log_lsn", KR(ret), K_(tls_id), K(commit_log_lsn));
  } else if (OB_UNLIKELY(participants_.count() > 0)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("participant has been initialized", KR(ret), K(participants_));
  // participants record prepared ls info, should be empty if is single_ls_trans.
  } else if (OB_UNLIKELY(is_single_ls_trans() && part_count != 1)
      || OB_UNLIKELY(is_dist_trans() && ! is_xa_or_dup_ && part_count <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("trans_type is not consistent with participant_count", KR(ret), K_(tls_id), K_(trans_id),
        K_(trans_type), K(participants));
  } else {
    if (is_single_ls_trans()) {
      if (OB_FAIL(participants_.push_back(transaction::ObLSLogInfo(tls_id_.get_ls_id(), commit_log_lsn)))) {
        LOG_ERROR("participants_ push_back failed", KR(ret), KPC(this));
      } else if (OB_UNLIKELY(! participants_[0].is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexcepted invalid part_array", KR(ret), K(participants_), KPC(this));
      } else {}
    } else {
      for (int64_t index = 0; OB_SUCC(ret) && index < part_count; index++) {
        const transaction::ObLSLogInfo &part_log_info = participants.at(index);

        if (OB_UNLIKELY(! part_log_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("part_log_info recorded in TransCommitLog is invalid", KR(ret),
              K_(tls_id), K_(trans_id), K_(trans_type), K(participants), K(part_log_info));
        } else if (OB_FAIL(participants_.push_back(part_log_info))) {
          LOG_ERROR("participants_ push_back failed", KR(ret), KPC(this));
        } else {}
      }
    }
  }

  return ret;
}

void PartTransTask::destroy_participant_array_()
{
  /*
  if (NULL != participants_ && participant_count_ > 0) {
    for (int64_t index = 0; index < participant_count_; index++) {
      participants_[index].~ObLSLogInfo();
    }

    allocator_.free(participants_);
    participants_ = NULL;
    participant_count_ = 0;
  }
  */
}

int PartTransTask::set_participants(
  const transaction::ObLSLogInfoArray &participants,
  const palf::LSN &commit_log_lsn)
{
  return init_participant_array_(participants, commit_log_lsn);
}

int PartTransTask::to_string_part_trans_info_()
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = strlen(tls_str_)
    + DmlStmtUniqueID::compute_str_length_base_num(trans_id_.get_id())
    + sizeof(DELIMITER_STR)
    + 1; // '\0'
  int64_t pos = 0;

  char *buf = static_cast<char*>(allocator_.alloc(buf_size));

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("part_trans_info_str_buf alloc failed", KR(ret), K_(tls_id), K(buf_size), K(pos));
  } else if (OB_FAIL(common::databuff_printf(buf, buf_size, pos, "%s"DELIMITER_STR"%ld", tls_str_, trans_id_.get_id()))) {
    LOG_ERROR("serialize tls_id_str failed", KR(ret), K(buf), K(buf_size), K(pos), K_(tls_str), K_(trans_id));
  } else {
    part_trans_info_str_.assign_ptr(buf, static_cast<int32_t>(buf_size));
  }

  if (OB_FAIL(ret)) {
    allocator_.free(buf);
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
  } else if (OB_ISNULL(dml_stmt_task = static_cast<DmlStmtTask*>(dml_task))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dml_task get from SortedRedoList should not be null", KR(ret), K(dml_task));
  } else {
    sorted_redo_list_.set_sorted_row_seq_no(dml_stmt_task->get_row_seq_no());
    /* succ */
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
