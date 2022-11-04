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

#include "ob_all_virtual_proxy_base.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace observer
{

ObAllVirtualProxyBaseIterator::ObAllVirtualProxyBaseIterator()
    : ObVirtualTableIterator(),
      input_tenant_name_(),
      schema_service_(NULL),
      tenant_schema_guard_(share::schema::ObSchemaMgrItem::MOD_VIRTUAL_TABLE)
{
}

ObAllVirtualProxyBaseIterator::~ObAllVirtualProxyBaseIterator()
{
}

void ObAllVirtualProxyBaseIterator::set_schema_service(share::schema::ObMultiVersionSchemaService &schema_service)
{
  schema_service_ = &schema_service;
}

int ObAllVirtualProxyBaseIterator::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K_(cur_row), KR(ret));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_rowkey_str(
    const bool is_oracle_mode,
    const ObRowkey &rowkey,
    ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_len = OB_MAX_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;

  if (OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member doesn't init", K_(allocator), KR(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc tmp_buf fail", KR(ret));
  } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
             is_oracle_mode, rowkey, buf, buf_len, pos, false, TZ_INFO(session_)))) {
    LOG_WARN("fail to get rowkey str", KR(ret));
  } else if (OB_UNLIKELY(pos >= buf_len) || pos < 0) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("fail to get buf", K(pos), K(buf_len), KR(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_rowkey_bin_str(const ObRowkey &rowkey, ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_len = rowkey.get_serialize_size();
  int64_t pos = 0;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member doesn't init", K_(allocator), KR(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc tmp_buf fail", KR(ret));
  } else if (OB_FAIL(rowkey.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret));
  } else if (OB_UNLIKELY(pos > buf_len) || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get buf", K(pos), K(buf_len), KR(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_rowkey_type_str(const ObRowkey &rowkey, ObObj &out_obj)
{
  int ret = OB_SUCCESS;

  // part range type
  static const int64_t COLUMN_TYPE_STR_LEN = 2 + 1; // 2 for string format of obj type, 1 for ','
  static const int64_t RANGE_TYPE_BUF_LEN = OB_MAX_PART_COLUMNS * COLUMN_TYPE_STR_LEN;
  char *buf = NULL;
  const int64_t buf_len = RANGE_TYPE_BUF_LEN;
  int64_t pos = 0;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member doesn't init", K_(allocator), KR(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc tmp_buf fail", KR(ret));
  } else {
    const ObObj *objs = rowkey.get_obj_ptr();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
      if (OB_FAIL(BUF_PRINTF("%d", objs[i].get_type()))) {
        LOG_WARN("Failed to print obj type", KR(ret));
      } else if (rowkey.get_obj_cnt() - 1 != i) {
        if (OB_FAIL(BUF_PRINTF(","))) {
          LOG_WARN("Failed to add comma", KR(ret));
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
    LOG_WARN("fail to get buf", K(pos), K(buf_len), KR(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_obj_str(const ObObj &obj, ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_len = OB_MAX_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member doesn't init", K_(allocator), KR(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc tmp_buf fail", KR(ret));
  } else if (OB_FAIL(obj.print_sql_literal(buf, buf_len, pos))) {
    LOG_WARN("fail to print_sql_literal", KR(ret));
  } else if (OB_UNLIKELY(pos >= buf_len) || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get buf", K(pos), K(buf_len), KR(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_obj_bin_str(const ObObj &obj, ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_len = obj.get_serialize_size();
  int64_t pos = 0;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member doesn't init", K_(allocator), KR(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc tmp_buf fail", KR(ret));
  } else if (OB_FAIL(obj.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret));
  } else if (OB_UNLIKELY(pos > buf_len) || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get buf", K(pos), K(buf_len), KR(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_rows_str(
    const bool is_oracle_mode,
    const common::ObIArray<common::ObNewRow>& rows,
    ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_len = OB_MAX_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;
  if (OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member doesn't init", K_(allocator), KR(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc tmp_buf fail", KR(ret));
  } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_sql_literal(
             is_oracle_mode, rows, buf, buf_len, pos, false, TZ_INFO(session_)))) {
    LOG_WARN("fail to get rows str", KR(ret));
  } else if (OB_UNLIKELY(pos >= buf_len) || pos < 0) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("fail to get buf", K(pos), K(buf_len), KR(ret));
  } else {
    out_obj.set_varchar(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_rows_bin_str(const common::ObIArray<common::ObNewRow>& rows, ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_len = OB_MAX_B_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member doesn't init", K_(allocator), KR(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc tmp_buf fail", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); i++) {
      if (OB_FAIL(rows.at(i).serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to encode row", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(pos > buf_len) || pos < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get buf", K(pos), K(buf_len), KR(ret));
      } else {
        out_obj.set_varchar(buf, static_cast<int32_t>(pos));
      }
    }
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_partition_value_str(
    const bool is_oracle_mode,
    const ObPartitionFuncType type,
    const ObBasePartition &partition,
    ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  if (is_range_part(type)) {
    ret = get_rowkey_str(is_oracle_mode, partition.get_high_bound_val(), out_obj);
  } else if (is_list_part(type)) {
    ret = get_rows_str(is_oracle_mode, partition.get_list_row_values(), out_obj);
  } else if (is_hash_like_part(type)) {
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition type", K(type), KR(ret));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::get_partition_value_bin_str(const ObPartitionFuncType type,
                                                               const ObBasePartition &partition,
                                                               ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  if (is_range_part(type)) {
    ret = get_rowkey_bin_str(partition.get_high_bound_val(), out_obj);
  } else if (is_list_part(type)) {
    ret = get_rows_bin_str(partition.get_list_row_values(), out_obj);
  } else if (is_hash_like_part(type)) {
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition type", K(type), KR(ret));
  }
  return ret;
}

int ObAllVirtualProxyBaseIterator::check_schema_version(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t received_broadcast_version = OB_INVALID_VERSION;
  int64_t refreshed_version = OB_INVALID_VERSION;
  if (OB_FAIL(schema_service_->get_tenant_received_broadcast_version(
      tenant_id, received_broadcast_version))) {
    LOG_WARN("fail to get tenant received broadcast version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_version))) {
    LOG_WARN("fail to get schema guard version", KR(ret), K(tenant_id));
  } else if (OB_CORE_SCHEMA_VERSION >= received_broadcast_version
      || refreshed_version < received_broadcast_version) {
    ret = OB_SCHEMA_ERROR;
    LOG_TRACE("schema not exist, need retry", KR(ret), K(tenant_id),
              K(received_broadcast_version), K(refreshed_version));
  } else {
    // do noting
  }
  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase
