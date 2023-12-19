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

#include "observer/virtual_table/ob_all_virtual_ls_snapshot.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_service.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_meta_table.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualLSSnapshot::ObAllVirtualLSSnapshot()
    : ObVirtualTableScannerIterator(),
      addr_(),
      ls_meta_package_buf_(nullptr),
      ls_snapshot_key_arr_(),
      ls_snap_idx_(0)
{
}

ObAllVirtualLSSnapshot::~ObAllVirtualLSSnapshot()
{
  reset();
}

void ObAllVirtualLSSnapshot::reset()
{
  // Multi tenant resources should be released by ObMultiTenantOperator, call ObMultiTenantOperator::reset first
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
  addr_.reset();
  ls_meta_package_buf_ = nullptr;
  ls_snapshot_key_arr_.reset();
  ls_snap_idx_ = 0;
}

int ObAllVirtualLSSnapshot::inner_open()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_ISNULL(allocator_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "allocator_ is null", KR(ret));
  } else if (OB_ISNULL(ls_meta_package_buf_ = static_cast<char*>(allocator_->alloc(LS_META_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc memory for ls_meta_package_buf_", KR(ret), KP(allocator_));
  }
  return ret;
}

int ObAllVirtualLSSnapshot::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute inner_get_next_row", KR(ret));
  }
  return ret;
}

bool ObAllVirtualLSSnapshot::is_need_process(uint64_t tenant_id)
{
  return is_sys_tenant(effective_tenant_id_) || effective_tenant_id_ == tenant_id;
}

void ObAllVirtualLSSnapshot::release_last_tenant()
{
  ls_snapshot_key_arr_.reset();
  ls_snap_idx_ = 0;
}

int ObAllVirtualLSSnapshot::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  ObTenantSnapshotService *tenant_snapshot_service = nullptr;
  if (OB_UNLIKELY(OB_ISNULL(tenant_snapshot_service = MTL(ObTenantSnapshotService*)))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObTenantSnapshotService is null", KR(ret));
  } else if (ls_snapshot_key_arr_.empty()) {
    if (OB_FAIL(tenant_snapshot_service->get_all_ls_snapshot_keys(ls_snapshot_key_arr_))) {
      SERVER_LOG(WARN, "fail to get_all_ls_snapshot_keys", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    HEAP_VAR(ObLSSnapshotVTInfo, ls_snap_info){
      if (OB_FAIL(get_next_ls_snapshot_vt_info_(ls_snap_info))) {
        SERVER_LOG(WARN, "fail to get ls_snapshot info", KR(ret));
      } else if (OB_FAIL(fill_row_(ls_snap_info))){
        SERVER_LOG(WARN, "fail to fill cur_row_", KR(ret), K(ls_snap_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualLSSnapshot::get_next_ls_snapshot_vt_info_(ObLSSnapshotVTInfo &ls_snap_info)
{
  int ret = OB_SUCCESS;

  bool try_next_loop = false;
  do {
    try_next_loop = false;
    ObLSSnapshotMapKey ls_snap_map_key;
    if (ls_snap_idx_ >= ls_snapshot_key_arr_.size()) {
      ret = OB_ITER_END;
      SERVER_LOG(INFO, "iterate current tenant reach end",
          K(ls_snap_idx_), K(ls_snapshot_key_arr_.size()));
    } else if (OB_UNLIKELY(ls_snap_idx_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ls_snap_idx_ out of bound", KR(ret), K(ls_snap_idx_),
          K(ls_snapshot_key_arr_.size()));
    } else {
      ls_snap_map_key = ls_snapshot_key_arr_.at(ls_snap_idx_);
      ObTenantSnapshotService *tenant_snapshot_service = MTL(ObTenantSnapshotService*);
      if (OB_UNLIKELY(OB_ISNULL(tenant_snapshot_service))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "ObTenantSnapshotService is unexpected nullptr", KR(ret));
      } else if (OB_FAIL(tenant_snapshot_service->get_ls_snapshot_vt_info(ls_snap_map_key,
                                                                          ls_snap_info))) {
        if (OB_ENTRY_NOT_EXIST != ret){
          SERVER_LOG(WARN, "fail to get ls_snapshot info", KR(ret), K(ls_snap_map_key));
        } else {
          // ls snapshot may be removed by concurrent thread, skip it
          SERVER_LOG(INFO, "ls snapshot entry not exist, try next",
                     KR(ret), K(ls_snap_map_key), K(ls_snap_idx_), K(ls_snapshot_key_arr_.size()));
          ++ls_snap_idx_;
          try_next_loop = true;
          ret = OB_SUCCESS;
        }
      } else {
        ++ls_snap_idx_;
        try_next_loop = false;
      }
    }
  } while (OB_SUCC(ret) && try_next_loop);

  return ret;
}

int ObAllVirtualLSSnapshot::fill_row_(ObLSSnapshotVTInfo &ls_snap_info)
{
  int ret = OB_SUCCESS;

  ObLSSnapshotBuildCtxVTInfo &build_ctx_info = ls_snap_info.get_build_ctx_info();
  ObTenantSnapshotVTInfo &tsnap_info = ls_snap_info.get_tsnap_info();
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TENANT_ID:
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      case SNAPSHOT_ID:
        cur_row_.cells_[i].set_int(ls_snap_info.get_tenant_snapshot_id().id());
        break;
      case LS_ID:
        cur_row_.cells_[i].set_int(ls_snap_info.get_ls_id().id());
        break;
      case SVR_IP:
        if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to execute ip_to_string", KR(ret), K(addr_));
        }
        break;
      case SVR_PORT:
        cur_row_.cells_[i].set_int(addr_.get_port());
        break;
      case META_EXISTED:
        cur_row_.cells_[i].set_bool(ls_snap_info.get_meta_existed());
        break;
      case BUILD_STATUS:
        if (ls_snap_info.get_has_build_ctx()) {
          cur_row_.cells_[i].set_varchar(build_ctx_info.get_build_status());
        } else { // reset to display NULL
          cur_row_.cells_[i].reset();
        }
        break;
      case REBUILD_SEQ_START:
        if(ls_snap_info.get_has_build_ctx()) {
          cur_row_.cells_[i].set_int(build_ctx_info.get_rebuild_seq_start());
        } else { // reset to display NULL
          cur_row_.cells_[i].reset();
        }
        break;
      case REBUILD_SEQ_END:
        if (ls_snap_info.get_has_build_ctx()) {
          cur_row_.cells_[i].set_int(build_ctx_info.get_rebuild_seq_end());
        } else { // reset to display NULL
          cur_row_.cells_[i].reset();
        }
        break;
      case END_INTERVAL_SCN:
        if (ls_snap_info.get_has_build_ctx()) {
          cur_row_.cells_[i].set_int(build_ctx_info.get_end_interval_scn().get_val_for_inner_table_field());
        } else { // reset to display NULL
          cur_row_.cells_[i].reset();
        }
        break;
      case LS_META_PACKAGE:
        if (ls_snap_info.get_has_build_ctx()) {
          int64_t length = 0;
          if (OB_UNLIKELY(OB_ISNULL(ls_meta_package_buf_))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "ls_meta_package_buf_ is null", KR(ret));
          } else if ((length = build_ctx_info.get_ls_meta_package().to_string(ls_meta_package_buf_,
                                                                          LS_META_BUFFER_SIZE)) <= 0) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to get ls_meta_package str",
                              KR(ret), K(ls_meta_package_buf_), K(build_ctx_info.get_ls_meta_package()));
          } else {
            cur_row_.cells_[i].set_lob_value(ObObjType::ObLongTextType, ls_meta_package_buf_,
                                             static_cast<int32_t>(length));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                  ObCharset::get_default_charset()));
          }
        } else { // reset to display NULL
          cur_row_.cells_[i].reset();
        }
        break;
      case TSNAP_IS_RUNNING:
        if (ls_snap_info.get_has_tsnap_info()){
          cur_row_.cells_[i].set_bool(tsnap_info.get_tsnap_is_running());
        } else { // reset to display NULL
          cur_row_.cells_[i].reset();
        }
        break;
      case TSNAP_HAS_UNFINISHED_CREATE_DAG:
        if (ls_snap_info.get_has_tsnap_info()){
          cur_row_.cells_[i].set_bool(tsnap_info.get_tsnap_has_unfinished_create_dag());
        } else { // reset to display NULL
          cur_row_.cells_[i].reset();
        }
        break;
      case TSNAP_HAS_UNFINISHED_GC_DAG:
        if (ls_snap_info.get_has_tsnap_info()){
          cur_row_.cells_[i].set_bool(tsnap_info.get_tsnap_has_unfinished_gc_dag());
        } else { // reset to display NULL
          cur_row_.cells_[i].reset();
        }
        break;
      case TSNAP_CLONE_REF:
        if (ls_snap_info.get_has_tsnap_info()){
          cur_row_.cells_[i].set_int(tsnap_info.get_tsnap_clone_ref());
        } else { // reset to display NULL
          cur_row_.cells_[i].reset();
        }
        break;
      case TSNAP_META_EXISTED:
        if (ls_snap_info.get_has_tsnap_info()){
          cur_row_.cells_[i].set_bool(tsnap_info.get_tsnap_meta_existed());
        } else { // reset to display NULL
          cur_row_.cells_[i].reset();
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected col id", KR(ret), K(col_id), K(output_column_ids_));
        break;
    }
  }
  return ret;
}

} // observer
} // oceanbase
