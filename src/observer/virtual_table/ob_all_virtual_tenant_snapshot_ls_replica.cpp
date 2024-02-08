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

#include "observer/virtual_table/ob_all_virtual_tenant_snapshot_ls_replica.h"
#include "lib/string/ob_hex_utils_base.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{

ObAllVirtualTenantSnapshotLSReplica::ObAllVirtualTenantSnapshotLSReplica():
    common::ObVirtualTableScannerIterator(),
    ls_meta_buf_(nullptr),
    sql_res_(nullptr),
    result_(nullptr)
{
}

ObAllVirtualTenantSnapshotLSReplica::~ObAllVirtualTenantSnapshotLSReplica()
{
  reset();
}

void ObAllVirtualTenantSnapshotLSReplica::reset()
{
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
  result_ = nullptr;
  if (OB_NOT_NULL(sql_res_)) {
    sql_res_->~ReadResult();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(sql_res_);
    }
    sql_res_ = nullptr;
  }
  if (OB_NOT_NULL(ls_meta_buf_)) {
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(ls_meta_buf_);
    }
    ls_meta_buf_ = nullptr;
  }
}

int ObAllVirtualTenantSnapshotLSReplica::inner_open()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "allocator_ is null", KR(ret));
  } else if (OB_ISNULL(sql_res_ = OB_NEWx(ObMySQLProxy::MySQLResult, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc memory for MySQLResult", KR(ret));
  } else if (OB_ISNULL(ls_meta_buf_ = static_cast<char*>(allocator_->alloc(LS_META_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc memory for ls_meta_buf_", KR(ret), KP(allocator_));
  }
  return ret;
}

int ObAllVirtualTenantSnapshotLSReplica::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute inner_get_next_row", KR(ret));
  }
  return ret;
}

bool ObAllVirtualTenantSnapshotLSReplica::is_need_process(uint64_t tenant_id)
{
  return is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_;
}

int ObAllVirtualTenantSnapshotLSReplica::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(result_)) {
    if (OB_FAIL(get_tenant_snapshot_ls_replica_entries_())) {
      SERVER_LOG(WARN, "fail to get ls_replica result", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
    SERVER_LOG(DEBUG, "fail to get ls_replica result", KR(ret));
  } else if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "sql result is null", KR(ret));
  } else if (OB_SUCC(result_->next())) {
    const ObNewRow *r = result_->get_row();
    if (OB_ISNULL(r)
        || OB_ISNULL(r->cells_)
        || r->get_count() < cur_row_.count_) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "row is null or column count mismatch", KR(ret), KP(r), K(cur_row_.count_));
    } else {
      ObString ls_meta_package_str;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_row_.count_; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        if (LS_META_PACKAGE == col_id) {  // decode ls_meta_package column
          int64_t length = 0;
          HEAP_VAR(ObLSMetaPackage, ls_meta_package) {
            EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result_, "ls_meta_package", ls_meta_package_str);
            if (OB_FAIL(ret)) {
            } else if (ls_meta_package_str.empty()) {
              cur_row_.cells_[i].reset();
            } else if (OB_ISNULL(ls_meta_buf_)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "ls_meta_buf_ is null", KR(ret), KP(ls_meta_buf_), KP(allocator_));
            } else if (OB_FAIL(decode_hex_string_to_package_(ls_meta_package_str, *allocator_, ls_meta_package))){
              SERVER_LOG(WARN, "fail to decode hex string into ls_meta_package", KR(ret));
            } else if ((length = ls_meta_package.to_string(ls_meta_buf_, LS_META_BUFFER_SIZE)) <= 0) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "fail to get ls_meta_package string", KR(ret), K(length));
            } else {
              cur_row_.cells_[i].set_lob_value(ObObjType::ObLongTextType, ls_meta_buf_,
                                              static_cast<int32_t>(length));
              cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                    ObCharset::get_default_charset()));
            }
          }
        } else if (col_id - OB_APP_MIN_COLUMN_ID >= 0 && col_id - OB_APP_MIN_COLUMN_ID < r->count_) {
          // direct copy other columns
          cur_row_.cells_[i] = r->get_cell(col_id - OB_APP_MIN_COLUMN_ID);
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected column id", KR(ret), K(col_id), K(output_column_ids_));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualTenantSnapshotLSReplica::decode_hex_string_to_package_(const ObString& hex_str,
                                                                      ObIAllocator& allocator,
                                                                      ObLSMetaPackage& ls_meta_package)
{
  int ret = OB_SUCCESS;
  char *unhex_buf = nullptr;
  int64_t unhex_buf_len = 0;
  int64_t pos = 0;
  ls_meta_package.reset();
  if (hex_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", KR(ret), K(hex_str));
  } else if (OB_FAIL(ObHexUtilsBase::unhex(hex_str, allocator, unhex_buf, unhex_buf_len))) {
    SERVER_LOG(WARN, "fail to unhex", KR(ret), K(hex_str));
  } else if (OB_FAIL(ls_meta_package.deserialize(unhex_buf, unhex_buf_len, pos))) {
    SERVER_LOG(WARN, "deserialize ls meta package failed", KR(ret), K(hex_str));
  }
  return ret;
}

int ObAllVirtualTenantSnapshotLSReplica::get_tenant_snapshot_ls_replica_entries_()
{
  int ret = OB_SUCCESS;

  ObSqlString sql;
  int64_t user_tenant_id = MTL_ID();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "sql_proxy is null", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT tenant_id, snapshot_id, ls_id, svr_ip, svr_port, "
                                    "gmt_create, gmt_modified, status, zone, unit_id, "
                                    "begin_interval_scn, end_interval_scn, ls_meta_package "
                                    "FROM %s "
                                    "WHERE tenant_id = %lu",
                                    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
                                    user_tenant_id))) {
    SERVER_LOG(WARN, "fail to assign sql", KR(ret), K(sql));
  } else if (OB_ISNULL(sql_res_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "sql_res_ is null", KR(ret));
  } else if (OB_FAIL(GCTX.sql_proxy_->read(*sql_res_, gen_meta_tenant_id(user_tenant_id), sql.ptr()))) {
    SERVER_LOG(WARN, "fail to execute sql", KR(ret), K(sql));
  } else if (OB_ISNULL(result_ = sql_res_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "sql result is null", KR(ret));
  }
  return ret;
}

void ObAllVirtualTenantSnapshotLSReplica::release_last_tenant()
{
  if (OB_NOT_NULL(sql_res_)) {
    sql_res_->reset();
  }
  result_ = nullptr;
}

}  //observer
}  //oceanbase
