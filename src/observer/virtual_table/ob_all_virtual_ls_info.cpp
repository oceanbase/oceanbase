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

#include "observer/virtual_table/ob_all_virtual_ls_info.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualLSInfo::ObAllVirtualLSInfo()
    : ObVirtualTableScannerIterator(),
      addr_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      ls_iter_guard_()
{
}

ObAllVirtualLSInfo::~ObAllVirtualLSInfo()
{
  reset();
}

void ObAllVirtualLSInfo::reset()
{
  // 注意这里跨租户资源必须由ObMultiTenantOperator释放, 因此必须放在最前面调用
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualLSInfo::release_last_tenant()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_iter_guard_.reset();
}

int ObAllVirtualLSInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualLSInfo::is_need_process(uint64_t tenant_id)
{
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    return true;
  }
  return false;
}

int ObAllVirtualLSInfo::next_ls_info_(ObLSVTInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  do {
    if (OB_FAIL(ls_iter_guard_->get_next(ls))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "get_next_ls failed", K(ret));
      }
    } else if (NULL == ls) {
      SERVER_LOG(WARN, "ls shouldn't NULL here", K(ls));
      // try another ls
      ret = OB_EAGAIN;
    } else if (FALSE_IT(ls_id_ = ls->get_ls_id().id())) {
    } else if (OB_FAIL(ls->get_ls_info(ls_info))) {
      SERVER_LOG(WARN, "get ls info failed", K(ret), KPC(ls));
      // try another ls
      ret = OB_EAGAIN;
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

int ObAllVirtualLSInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObLSVTInfo ls_info;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (ls_iter_guard_.get_ptr() == nullptr && OB_FAIL(MTL(ObLSService*)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "get_ls_iter fail", K(ret));
  } else if (OB_FAIL(next_ls_info_(ls_info))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get next_ls_info failed", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
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
        case OB_APP_MIN_COLUMN_ID + 4: {
          // replica_type
          if (OB_FAIL(replica_type_to_string(ls_info.replica_type_,
                                             replica_type_name_,
                                             sizeof(replica_type_name_)))) {
            SERVER_LOG(WARN, "get replica type name failed", K(ret), K(ls_info.replica_type_));
          } else {
            replica_type_name_[MAX_REPLICA_TYPE_LENGTH - 1] = '\0';
            cur_row_.cells_[i].set_varchar(replica_type_name_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 5: {
          // ls_state
          ObRole role;
          int64_t unused_proposal_id = 0;
          if (OB_FAIL(role_to_string(ls_info.ls_state_,
                                     state_name_,
                                     sizeof(state_name_)))) {
            SERVER_LOG(WARN, "get state role name failed", K(ret), K(role));
          } else {
            state_name_[MAX_LS_STATE_LENGTH - 1] = '\0';
            cur_row_.cells_[i].set_varchar(state_name_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 6:
          // tablet_count
          cur_row_.cells_[i].set_int(ls_info.tablet_count_);
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // weak_read_timestamp
          cur_row_.cells_[i].set_uint64(ls_info.weak_read_scn_.get_val_for_inner_table_field());
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // need_rebuild
          cur_row_.cells_[i].set_varchar(ls_info.need_rebuild_ ? "YES" : "NO");
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // clog_checkpoint_ts
          cur_row_.cells_[i].set_uint64(!ls_info.checkpoint_scn_.is_valid() ? 0 : ls_info.checkpoint_scn_.get_val_for_tx());
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // clog_checkpoint_lsn
          cur_row_.cells_[i].set_uint64(ls_info.checkpoint_lsn_ < 0 ? 0 : ls_info.checkpoint_lsn_);
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // migrate_status
          cur_row_.cells_[i].set_int(ls_info.migrate_status_);
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // rebuild_seq
          cur_row_.cells_[i].set_int(ls_info.rebuild_seq_);
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // tablet_change_checkpoint_scn
          cur_row_.cells_[i].set_uint64(!ls_info.tablet_change_checkpoint_scn_.is_valid() ? 0 : ls_info.tablet_change_checkpoint_scn_.get_val_for_inner_table_field());
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // transfer_scn
          cur_row_.cells_[i].set_uint64(!ls_info.transfer_scn_.is_valid() ? 0 : ls_info.transfer_scn_.get_val_for_inner_table_field());
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          // tx blocked
          cur_row_.cells_[i].set_int(ls_info.tx_blocked_);
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

} // observer
} // oceanbase
