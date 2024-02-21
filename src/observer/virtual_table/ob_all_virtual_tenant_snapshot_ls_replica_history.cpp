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

#include "observer/virtual_table/ob_all_virtual_tenant_snapshot_ls_replica_history.h"
#include "lib/string/ob_hex_utils_base.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{

int ObAllVirtualTenantSnapshotLSReplicaHistory::try_convert_row(const ObNewRow *input_row, ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  char* ls_meta_buf = nullptr;
  ObString ls_meta_package_str;
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_row_.count_; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    if (LS_META_PACKAGE == col_id) {  // decode ls_meta_package column
      int64_t length = 0;
      HEAP_VAR(ObLSMetaPackage, ls_meta_package) {
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*inner_sql_res_, "ls_meta_package", ls_meta_package_str);
        if (FAILEDx(FALSE_IT(convert_alloc_.reuse()))) {
        } else if (OB_ISNULL(ls_meta_buf = static_cast<char*>(convert_alloc_.alloc(LS_META_BUFFER_SIZE)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SERVER_LOG(WARN, "fail to alloc memory for ls_meta_buf", KR(ret), K(convert_alloc_));
        } else if (ls_meta_package_str.empty()) {
          cur_row_.cells_[i].reset();
        } else if (OB_FAIL(decode_hex_string_to_package_(ls_meta_package_str, ls_meta_package))){
          SERVER_LOG(WARN, "fail to decode hex string into ls_meta_package", KR(ret), K(ls_meta_package_str));
        } else if ((length = ls_meta_package.to_string(ls_meta_buf, LS_META_BUFFER_SIZE)) <= 0) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to get ls_meta_package string", KR(ret), K(length));
        } else {
          cur_row_.cells_[i].set_lob_value(ObObjType::ObLongTextType, ls_meta_buf,
                                          static_cast<int32_t>(length));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
      }
    } else if (col_id - OB_APP_MIN_COLUMN_ID >= 0 && col_id - OB_APP_MIN_COLUMN_ID < input_row->count_) {
      // direct copy other columns
      cur_row_.cells_[i] = input_row->get_cell(col_id - OB_APP_MIN_COLUMN_ID);
    } else {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected column id", KR(ret), K(col_id), K(output_column_ids_));
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualTenantSnapshotLSReplicaHistory::decode_hex_string_to_package_(const ObString& hex_str,
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
  } else if (OB_FAIL(ObHexUtilsBase::unhex(hex_str, convert_alloc_, unhex_buf, unhex_buf_len))) {
    SERVER_LOG(WARN, "fail to unhex", KR(ret), K(hex_str));
  } else if (OB_FAIL(ls_meta_package.deserialize(unhex_buf, unhex_buf_len, pos))) {
    SERVER_LOG(WARN, "deserialize ls meta package failed", KR(ret), K(hex_str));
  }
  return ret;
}

}  //observer
}  //oceanbase
