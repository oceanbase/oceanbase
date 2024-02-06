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

#include "observer/virtual_table/ob_all_virtual_tablet_ddl_kv_info.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualTabletDDLKVInfo::ObAllVirtualTabletDDLKVInfo()
    : ObVirtualTableScannerIterator(),
      addr_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      ls_iter_guard_(),
      ls_tablet_iter_(ObMDSGetTabletMode::READ_READABLE_COMMITED),
      ddl_kvs_handle_(),
      curr_tablet_id_(),
      ddl_kv_idx_(-1)
{
}

ObAllVirtualTabletDDLKVInfo::~ObAllVirtualTabletDDLKVInfo()
{
  reset();
}

void ObAllVirtualTabletDDLKVInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualTabletDDLKVInfo::release_last_tenant()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ddl_kv_idx_ = -1;
  ddl_kvs_handle_.reset();
  curr_tablet_id_.reset();
  ls_tablet_iter_.reset();
  ls_iter_guard_.reset();
}

int ObAllVirtualTabletDDLKVInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualTabletDDLKVInfo::is_need_process(uint64_t tenant_id)
{
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    return true;
  }
  return false;
}

int ObAllVirtualTabletDDLKVInfo::get_next_ls(ObLS *&ls)
{
  int ret = OB_SUCCESS;
  if (nullptr == ls_iter_guard_.get_ptr() && OB_FAIL(MTL(ObLSService*)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "fail to get ls iter", K(ret));
  } else if (OB_FAIL(ls_iter_guard_->get_next(ls))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next ls", K(ret));
    }
  } else if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "ls is null", K(ret));
  } else {
    ls_id_ = ls->get_ls_id().id();
  }
  return ret;
}

int ObAllVirtualTabletDDLKVInfo::get_next_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ls_tablet_iter_.get_next_ddl_kv_mgr(ddl_kv_mgr_handle))) {
      if (!ls_tablet_iter_.is_valid() || OB_ITER_END == ret) {
        ret = OB_SUCCESS; // continue to next ls
        ObLS *ls = nullptr;
        if (OB_FAIL(get_next_ls(ls))) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "fail to get next ls", K(ret));
          }
        } else if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(ls_tablet_iter_))) {
          SERVER_LOG(WARN, "fail to get tablet iter", K(ret));
        }
      } else {
        SERVER_LOG(WARN, "fail to get next ddl kv mgr", K(ret));
      }
    } else {
      curr_tablet_id_ = ddl_kv_mgr_handle.get_obj()->get_tablet_id();
      break;
    }
  }
  return ret;
}

int ObAllVirtualTabletDDLKVInfo::get_next_ddl_kv(ObDDLKV *&ddl_kv)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  while (OB_SUCC(ret)) {
    if (ddl_kv_idx_ < 0 || ddl_kv_idx_ >= ddl_kvs_handle_.get_count()) {
      ObDDLKvMgrHandle ddl_kv_mgr_handle;
      if (OB_FAIL(get_next_ddl_kv_mgr(ddl_kv_mgr_handle))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "get_next_ddl_kv_mgr failed", K(ret));
        }
      } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(false/*frozen_only*/, ddl_kvs_handle_))) {
        SERVER_LOG(WARN, "fail to get ddl kvs", K(ret));
      } else if (ddl_kvs_handle_.get_count() > 0) {
        ddl_kv_idx_ = 0;
      }
    }

    if (OB_SUCC(ret) && ddl_kv_idx_ >= 0 && ddl_kv_idx_ < ddl_kvs_handle_.get_count()) {
      ddl_kv = static_cast<ObDDLKV *>(ddl_kvs_handle_.get_table(ddl_kv_idx_));
      if (OB_ISNULL(ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to get ddl kv", K(ret), K(ddl_kv_idx_));
      } else {
        ddl_kv_idx_++;
        break;
      }
    }
  }
  return ret;
}

int ObAllVirtualTabletDDLKVInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObDDLKV *cur_kv = nullptr;
  const int64_t col_count = output_column_ids_.count();
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (OB_FAIL(get_next_ddl_kv(cur_kv))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next tablet with ddl kv", K(ret));
    }
  } else if (OB_ISNULL(cur_kv)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "current ddl kv is null", K(ret), KP(cur_kv));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // tenant_id
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // ls_id
          cur_row_.cells_[i].set_int(ls_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // tablet_id
          cur_row_.cells_[i].set_int(curr_tablet_id_.id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // freeze_scn
          cur_row_.cells_[i].set_uint64(cur_kv->get_freeze_scn().get_val_for_inner_table_field());
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // start_scn
          cur_row_.cells_[i].set_uint64(cur_kv->get_ddl_start_scn().get_val_for_inner_table_field());
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // min_scn
          cur_row_.cells_[i].set_uint64(cur_kv->get_min_scn().get_val_for_inner_table_field());
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // macro_block_cnt
          cur_row_.cells_[i].set_int(cur_kv->get_macro_block_cnt());
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // ref_cnt
          cur_row_.cells_[i].set_int(cur_kv->get_ref());
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
