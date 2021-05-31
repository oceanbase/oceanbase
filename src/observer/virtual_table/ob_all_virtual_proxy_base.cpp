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

#include "ob_all_virtual_proxy_base.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace observer {

ObAllVirtualProxyBaseIterator::ObAllVirtualProxyBaseIterator()
    : ObVirtualTableIterator(), schema_service_(NULL), full_schema_guard_(), calc_buf_(NULL)
{}

ObAllVirtualProxyBaseIterator::~ObAllVirtualProxyBaseIterator()
{}

void ObAllVirtualProxyBaseIterator::set_schema_service(share::schema::ObMultiVersionSchemaService& schema_service)
{
  schema_service_ = &schema_service;
}

int ObAllVirtualProxyBaseIterator::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K_(cur_row), K(ret));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_rowkey_str(const ObRowkey& rowkey, ObObj& out_obj)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  const int64_t buf_len = OB_MAX_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;

  if (OB_ISNULL(calc_buf_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K_(calc_buf), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(calc_buf_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "alloc tmp_buf fail", K(ret));
  } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
                 rowkey, buf, buf_len, pos, false, TZ_INFO(session_)))) {
    SERVER_LOG(WARN, "fail to get rowkey str", K(ret));
  } else if (OB_UNLIKELY(pos >= buf_len) || pos < 0) {
    ret = OB_BUF_NOT_ENOUGH;
    SERVER_LOG(WARN, "fail to get buf", K(pos), K(buf_len), K(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_rowkey_bin_str(const ObRowkey& rowkey, ObObj& out_obj)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  const int64_t buf_len = rowkey.get_serialize_size();
  int64_t pos = 0;
  if (OB_ISNULL(calc_buf_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K_(calc_buf), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(calc_buf_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "alloc tmp_buf fail", K(ret));
  } else if (OB_FAIL(rowkey.serialize(buf, buf_len, pos))) {
    SERVER_LOG(WARN, "fail to serialize", K(ret));
  } else if (OB_UNLIKELY(pos > buf_len) || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to get buf", K(pos), K(buf_len), K(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_rowkey_type_str(const ObRowkey& rowkey, ObObj& out_obj)
{
  int ret = OB_SUCCESS;

  // part range type
  static const int64_t COLUMN_TYPE_STR_LEN = 2 + 1;  // 2 for string format of obj type, 1 for ','
  static const int64_t RANGE_TYPE_BUF_LEN = OB_MAX_PART_COLUMNS * COLUMN_TYPE_STR_LEN;
  char* buf = NULL;
  const int64_t buf_len = RANGE_TYPE_BUF_LEN;
  int64_t pos = 0;
  if (OB_ISNULL(calc_buf_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K_(calc_buf), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(calc_buf_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "alloc tmp_buf fail", K(ret));
  } else {
    const ObObj* objs = rowkey.get_obj_ptr();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
      if (OB_FAIL(BUF_PRINTF("%d", objs[i].get_type()))) {
        SERVER_LOG(WARN, "Failed to print obj type", K(ret));
      } else if (rowkey.get_obj_cnt() - 1 != i) {
        if (OB_FAIL(BUF_PRINTF(","))) {
          SERVER_LOG(WARN, "Failed to add comma", K(ret));
        } else {
          // do nothing
        }
      } else {
        // do nothing
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing here
  } else if (OB_UNLIKELY(pos > buf_len) || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to get buf", K(pos), K(buf_len), K(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_obj_str(const ObObj& obj, ObObj& out_obj)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  const int64_t buf_len = OB_MAX_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;
  if (OB_ISNULL(calc_buf_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K_(calc_buf), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(calc_buf_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "alloc tmp_buf fail", K(ret));
  } else if (OB_FAIL(obj.print_sql_literal(buf, buf_len, pos))) {
    SERVER_LOG(WARN, "fail to print_sql_literal", K(ret));
  } else if (OB_UNLIKELY(pos >= buf_len) || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to get buf", K(pos), K(buf_len), K(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_obj_bin_str(const ObObj& obj, ObObj& out_obj)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  const int64_t buf_len = obj.get_serialize_size();
  int64_t pos = 0;
  if (OB_ISNULL(calc_buf_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K_(calc_buf), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(calc_buf_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "alloc tmp_buf fail", K(ret));
  } else if (OB_FAIL(obj.serialize(buf, buf_len, pos))) {
    SERVER_LOG(WARN, "fail to serialize", K(ret));
  } else if (OB_UNLIKELY(pos > buf_len) || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to get buf", K(pos), K(buf_len), K(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_rows_str(const common::ObIArray<common::ObNewRow>& rows, ObObj& out_obj)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  const int64_t buf_len = OB_MAX_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;
  if (OB_ISNULL(calc_buf_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K_(calc_buf), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(calc_buf_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "alloc tmp_buf fail", K(ret));
  } else if (OB_FAIL(
                 ObPartitionUtils::convert_rows_to_sql_literal(rows, buf, buf_len, pos, false, TZ_INFO(session_)))) {
    SERVER_LOG(WARN, "fail to get rows str", K(ret));
  } else if (OB_UNLIKELY(pos >= buf_len) || pos < 0) {
    ret = OB_BUF_NOT_ENOUGH;
    SERVER_LOG(WARN, "fail to get buf", K(pos), K(buf_len), K(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_rows_bin_str(const common::ObIArray<common::ObNewRow>& rows, ObObj& out_obj)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  const int64_t buf_len = OB_MAX_B_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;
  if (OB_ISNULL(calc_buf_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K_(calc_buf), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(calc_buf_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "alloc tmp_buf fail", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); i++) {
      if (OB_FAIL(rows.at(i).serialize(buf, buf_len, pos))) {
        SERVER_LOG(WARN, "fail to encode row", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(pos > buf_len) || pos < 0) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to get buf", K(pos), K(buf_len), K(ret));
      } else {
        out_obj.set_varchar(buf, static_cast<int32_t>(pos));
      }
    }
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_partition_value_str(
    const ObPartitionFuncType type, const ObBasePartition& partition, ObObj& out_obj)
{
  int ret = OB_SUCCESS;
  if (is_range_part(type)) {
    ret = get_rowkey_str(partition.get_high_bound_val(), out_obj);
  } else if (is_list_part(type)) {
    ret = get_rows_str(partition.get_list_row_values(), out_obj);
  } else if (is_hash_like_part(type)) {
  } else {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid partition type", K(type), K(ret));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_partition_value_bin_str(
    const ObPartitionFuncType type, const ObBasePartition& partition, ObObj& out_obj)
{
  int ret = OB_SUCCESS;
  if (is_range_part(type)) {
    ret = get_rowkey_bin_str(partition.get_high_bound_val(), out_obj);
  } else if (is_list_part(type)) {
    ret = get_rows_bin_str(partition.get_list_row_values(), out_obj);
  } else if (is_hash_like_part(type)) {
  } else {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid partition type", K(type), K(ret));
  }
  return ret;
}

// The SQL for users to check proxy-related virtual tables may be cross-tenant.
// schema_guard is a full schema_guard that includes all tenants.
int ObAllVirtualProxyBaseIterator::get_table_schema(
    ObSchemaGetterGuard& schema_guard, uint64_t table_id, const share::schema::ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  if (OB_INVALID_ID == table_id || OB_INVALID_TENANT_ID == extract_tenant_id(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid table_id", K(ret), K(table_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema service is null", K(ret), K(table_id));
  } else {
    const uint64_t tenant_id = is_sys_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
    if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      SERVER_LOG(WARN, "get table schema failed", K(table_id), K(ret));
    } else if (OB_ISNULL(table_schema)) {
      int64_t received_broadcast_version = OB_INVALID_VERSION;
      int64_t refreshed_version = OB_INVALID_VERSION;
      if (OB_FAIL(schema_service_->get_tenant_received_broadcast_version(tenant_id, received_broadcast_version))) {
        SERVER_LOG(WARN, "fail to get tenant received broadcast version", K(ret), K(table_id));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_version))) {
        SERVER_LOG(WARN, "fail to get schema guard version", K(ret), K(table_id));
      } else if (OB_CORE_SCHEMA_VERSION >= received_broadcast_version ||
                 refreshed_version < received_broadcast_version) {
        ret = OB_SCHEMA_ERROR;
        SERVER_LOG(INFO,
            "schema not exist, need retry",
            K(table_id),
            K(received_broadcast_version),
            K(refreshed_version),
            K(ret));
      } else {
        // do noting
      }
    }
  }
  return ret;
}

}  // end of namespace observer
}  // end of namespace oceanbase
