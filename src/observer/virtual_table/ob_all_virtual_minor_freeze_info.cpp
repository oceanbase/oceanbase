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

#include "observer/ob_server.h"
#include "observer/virtual_table/ob_all_virtual_minor_freeze_info.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::common;
using namespace oceanbase::memtable;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualMinorFreezeInfo::ObAllVirtualMinorFreezeInfo()
  : ObVirtualTableScannerIterator(),
    ObMultiTenantOperator(),
    addr_(),
    ls_id_(share::ObLSID::INVALID_LS_ID),
    ls_iter_guard_(),
    diagnose_info_(),
    memtables_info_()
{
}

ObAllVirtualMinorFreezeInfo::~ObAllVirtualMinorFreezeInfo()
{
  reset();
}

void ObAllVirtualMinorFreezeInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_iter_guard_.reset();
  diagnose_info_.reset();
  memtables_info_.reset();
  memset(ip_buf_, 0, common::OB_IP_STR_BUFF);
  memset(memtables_info_string_, 0, OB_MAX_CHAR_LENGTH);
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualMinorFreezeInfo::release_last_tenant()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_iter_guard_.reset();
  diagnose_info_.reset();
  memtables_info_.reset();
  memset(memtables_info_string_, 0, OB_MAX_CHAR_LENGTH);
}

int ObAllVirtualMinorFreezeInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualMinorFreezeInfo::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

int ObAllVirtualMinorFreezeInfo::get_next_ls(ObLS *&ls)
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if (!ls_iter_guard_.get_ptr()
        || OB_FAIL(ls_iter_guard_->get_next(ls))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to switch tenant", K(ret));
      }
      // switch to next tenant
      ret = OB_ITER_END;
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "ls is null", K(ret));
    } else {
      ls_id_ = ls->get_ls_id().id();
      break;
    }
  }

  return ret;
}

int ObAllVirtualMinorFreezeInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObFreezerStat freeze_stat;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (ls_iter_guard_.get_ptr() == nullptr && OB_FAIL(MTL(ObLSService*)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "get_ls_iter fail", K(ret));
  } else if (OB_FAIL(get_next_freeze_stat(freeze_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_freeze_stat failed", K(ret));
    }
  } else {
    int64_t freeze_clock = 0;
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
        case OB_APP_MIN_COLUMN_ID + 4:
          // tablet_id
          cur_row_.cells_[i].set_int(freeze_stat.get_tablet_id().id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // is_force
          cur_row_.cells_[i].set_varchar(freeze_stat.get_is_force() ? "YES" : "NO");
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // freeze_clock
          freeze_clock = freeze_stat.get_freeze_clock();
          cur_row_.cells_[i].set_int(freeze_clock);
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // freeze_snapshot_version
          cur_row_.cells_[i].set_int(freeze_stat.get_freeze_snapshot_version().get_val_for_inner_table_field());
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // start_time
          cur_row_.cells_[i].set_timestamp(freeze_stat.get_start_time());
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // end_time
          cur_row_.cells_[i].set_timestamp(freeze_stat.get_end_time());
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // ret_code
          cur_row_.cells_[i].set_int(freeze_stat.get_ret_code());
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // state
          switch (freeze_stat.get_state()) {
            case ObFreezeState::INVALID:
              cur_row_.cells_[i].set_varchar("INVALID");
              break;
            case ObFreezeState::NOT_SET_FREEZE_FLAG:
              cur_row_.cells_[i].set_varchar("NOT_SET_FREEZE_FLAG");
              break;
            case ObFreezeState::NOT_SUBMIT_LOG:
              cur_row_.cells_[i].set_varchar("NOT_SUBMIT_LOG");
              break;
            case ObFreezeState::WAIT_READY_FOR_FLUSH:
              cur_row_.cells_[i].set_varchar("WAIT_READY_FOR_FLUSH");
              break;
            case ObFreezeState::FINISH:
              cur_row_.cells_[i].set_varchar("FINISH");
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "invalid freeze state", K(ret), K(col_id));
              break;
          }
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // diagnose_info
          if (OB_SUCC(freeze_stat.get_diagnose_info(diagnose_info_))) {
            cur_row_.cells_[i].set_varchar(diagnose_info_.get_ob_string());
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // memtables_info
          if (OB_FAIL(freeze_stat.get_memtables_info(memtables_info_))) {
            TRANS_LOG(WARN, "fail to get_memtables_info", K(ret));
          } else {
            generate_memtables_info();
            cur_row_.cells_[i].set_varchar(memtables_info_string_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
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

int ObAllVirtualMinorFreezeInfo::get_next_freeze_stat(ObFreezerStat &freeze_stat)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObFreezer *freezer = nullptr;

  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_ls(ls))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "get_next_ls failed", K(ret));
      }
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ls shouldn't NULL here", K(ret));
    } else if (FALSE_IT(freezer = ls->get_freezer())) {
    } else if (OB_ISNULL(freezer)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ls shouldn't NULL here", K(ret));
    } else if (OB_FAIL(freezer->get_stat().deep_copy_to(freeze_stat))) {
      SERVER_LOG(WARN, "fail to deep copy", K(ret));
    } else if (!(freeze_stat.is_valid())) {
      SERVER_LOG(WARN, "freeze_stat is invalid", KP(ls), KP(freezer));
    } else {
      // freeze_stat is valid
      break;
    }
  }

  return ret;
}

int ObAllVirtualMinorFreezeInfo::generate_memtables_info()
{
  int ret = OB_SUCCESS;
  memset(memtables_info_string_, 0, OB_MAX_CHAR_LENGTH);
  int memtable_info_count = memtables_info_.count();
  // leave space for '\0'
  int64_t size = OB_MAX_CHAR_LENGTH - 1;

  for (int i = 0; i < memtable_info_count && size > 0; ++i) {
    if (memtables_info_[i].is_valid()) {
      // tablet_id
      append_memtable_info_string(MEMTABLE_INFO_MEMBER[0], to_cstring(memtables_info_[i].tablet_id_.id()), size);
      // start_scn
      append_memtable_info_string(MEMTABLE_INFO_MEMBER[1], to_cstring(memtables_info_[i].start_scn_), size);
      // end_scn
      append_memtable_info_string(MEMTABLE_INFO_MEMBER[2], to_cstring(memtables_info_[i].end_scn_), size);
      // write_ref_cnt
      append_memtable_info_string(MEMTABLE_INFO_MEMBER[3], to_cstring(memtables_info_[i].write_ref_cnt_), size);
      // unsubmitted_cnt
      append_memtable_info_string(MEMTABLE_INFO_MEMBER[4], to_cstring(memtables_info_[i].unsubmitted_cnt_), size);
      // unsynced_cnt
      append_memtable_info_string(MEMTABLE_INFO_MEMBER[5], to_cstring(memtables_info_[i].unsynced_cnt_), size);
      // current_right_boundary
      append_memtable_info_string(MEMTABLE_INFO_MEMBER[6], to_cstring(memtables_info_[i].current_right_boundary_), size);
      // end of the memtable_info
      if (size >= 2) {
        strcat(memtables_info_string_, "; ");
        size = size - 2;
      } else if (size < 0) {
        SERVER_LOG(WARN, "size is invalid", K(size), K(memtable_info_count), K(memtables_info_string_));
      }
    }
  }

  return ret;
}

void ObAllVirtualMinorFreezeInfo::append_memtable_info_string(const char *name, const char *str, int64_t &size)
{
  if (size > 0) {
    int64_t name_len = MIN(size, strlen(name));
    strncat(memtables_info_string_, name, name_len);
    size = size - name_len;

    int64_t mark_len = MIN(size, 1);
    strncat(memtables_info_string_, ":", mark_len);
    size = size - mark_len;

    int64_t str_len = MIN(size, strlen(str));
    strncat(memtables_info_string_, str, str_len);
    size = size - str_len;

    mark_len = MIN(size, 1);
    strncat(memtables_info_string_, " ", mark_len);
    size = size - mark_len;
  }
}

}/* ns observer*/
}/* ns oceanbase */
