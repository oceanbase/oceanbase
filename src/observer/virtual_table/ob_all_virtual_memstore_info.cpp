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
#include "observer/virtual_table/ob_all_virtual_memstore_info.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase::common;
using namespace oceanbase::memtable;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualMemstoreInfo::ObAllVirtualMemstoreInfo()
  : ObVirtualTableScannerIterator(),
    ObMultiTenantOperator(),
    addr_(),
    ls_id_(share::ObLSID::INVALID_LS_ID),
    ls_iter_guard_(),
    ls_tablet_iter_(ObMDSGetTabletMode::READ_READABLE_COMMITED),
    tables_handle_(),
    memtable_array_pos_(0)
{
}

ObAllVirtualMemstoreInfo::~ObAllVirtualMemstoreInfo()
{
  reset();
}

void ObAllVirtualMemstoreInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_tablet_iter_.reset();
  ls_iter_guard_.reset();
  tables_handle_.reset();
  memtable_array_pos_ = 0;
  memset(freeze_time_dist_, 0, OB_MAX_CHAR_LENGTH);
  memset(compaction_info_buf_, 0, sizeof(compaction_info_buf_));
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualMemstoreInfo::release_last_tenant()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_iter_guard_.reset();
  ls_tablet_iter_.reset();
  tables_handle_.reset();
  memtable_array_pos_ = 0;
  memset(freeze_time_dist_, 0, OB_MAX_CHAR_LENGTH);
}

int ObAllVirtualMemstoreInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualMemstoreInfo::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

int ObAllVirtualMemstoreInfo::get_next_ls(ObLS *&ls)
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

int ObAllVirtualMemstoreInfo::get_next_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if (OB_FAIL(ls_tablet_iter_.get_next_tablet(tablet_handle))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next tablet", K(ret));
      }
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
      break;
    }
  }

  return ret;
}

int ObAllVirtualMemstoreInfo::get_next_memtable(memtable::ObMemtable *&mt)
{
  int ret = OB_SUCCESS;

  // memtable_array_pos_ == memtable_array_.count() means all the memtables of this
  // tablet have been disposed or the first time get_next_memtable() is invoked,
  // when get_next_memtable() is invoked the first time, memtable_array_pos_ and
  // memtable_array_.count() are both 0
  while (OB_SUCC(ret)) {
    if (memtable_array_pos_ == tables_handle_.count()) {
      tables_handle_.reset();
      memtable_array_pos_ = 0;
      ObTabletHandle tablet_handle;
      ObIMemtableMgr *memtable_mgr = nullptr;

      if (OB_FAIL(get_next_tablet(tablet_handle))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next tablet", K(ret));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid tablet handle", K(ret), K(tablet_handle));
      } else if (OB_ISNULL(memtable_mgr = tablet_handle.get_obj()->get_memtable_mgr())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "memtable mgr is null", K(ret));
      } else if (OB_FAIL(memtable_mgr->get_all_memtables(tables_handle_))) {
        SERVER_LOG(WARN, "fail to get all memtables for log stream", K(ret));
      }
    } else if (OB_FAIL(tables_handle_.at(memtable_array_pos_++).get_data_memtable(mt))) {
      // get next memtable
      ret = OB_SUCCESS;
    } else if (OB_ISNULL(mt)) {
      ret = OB_ERR_SYS;
      SERVER_LOG(ERROR, "memtable must not null", K(ret), K(mt));
    } else {
      break;
    }
  }

  return ret;
}

void ObAllVirtualMemstoreInfo::get_freeze_time_dist(const memtable::ObMtStat& mt_stat)
{
  memset(freeze_time_dist_, 0, 128);
  int64_t ready_for_flush_cost_time = (mt_stat.ready_for_flush_time_ - mt_stat.frozen_time_) / 1000;
  int64_t flush_cost_time = (mt_stat.release_time_ - mt_stat.ready_for_flush_time_) / 1000;

  if (ready_for_flush_cost_time >= 0) {
    strcat(freeze_time_dist_, to_cstring(ready_for_flush_cost_time));

    if (flush_cost_time >=0) {
      strcat(freeze_time_dist_, ",");
      strcat(freeze_time_dist_, to_cstring(flush_cost_time));
    }
  }
}

int ObAllVirtualMemstoreInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObMemtable *mt = NULL;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (ls_iter_guard_.get_ptr() == nullptr && OB_FAIL(MTL(ObLSService*)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "get_ls_iter fail", K(ret));
  } else if (OB_FAIL(get_next_memtable(mt))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_memtable failed", K(ret));
    }
  } else if (NULL == mt) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "mt shouldn't NULL here", K(ret), K(mt));
  } else {
    memtable::ObMtStat& mt_stat = mt->get_mt_stat();
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
          cur_row_.cells_[i].set_int(mt->get_key().tablet_id_.id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // is_active
          cur_row_.cells_[i].set_varchar(mt->is_active_memtable() ? "YES" : "NO");
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // start_ts
          cur_row_.cells_[i].set_uint64(mt->get_key().scn_range_.start_scn_.get_val_for_inner_table_field());
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // end_ts
          cur_row_.cells_[i].set_uint64(mt->get_key().scn_range_.end_scn_.get_val_for_inner_table_field());
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // logging_blocked
          cur_row_.cells_[i].set_varchar(mt->get_logging_blocked() ? "YES" : "NO");
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // freeze_clock
          cur_row_.cells_[i].set_int(mt->get_freeze_clock());
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // unsubmitted_count
          cur_row_.cells_[i].set_int(mt->get_unsubmitted_cnt());
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // unsynced_count
          cur_row_.cells_[i].set_int(mt->get_unsynced_cnt());
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // write_ref_count
          cur_row_.cells_[i].set_int(mt->get_write_ref());
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // mem_used
          cur_row_.cells_[i].set_int(mt->get_occupied_size());
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // hash_item_count
          cur_row_.cells_[i].set_int(mt->get_hash_item_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          // hash_mem_used
          cur_row_.cells_[i].set_int(mt->get_hash_alloc_memory());
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          // btree_item_count
          cur_row_.cells_[i].set_int(mt->get_btree_item_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          // btree_mem_used
          cur_row_.cells_[i].set_int(mt->get_btree_alloc_memory());
          break;
        case OB_APP_MIN_COLUMN_ID + 18:
          // insert_row_count
          cur_row_.cells_[i].set_int(mt_stat.insert_row_count_);
          break;
        case OB_APP_MIN_COLUMN_ID + 19:
          // update_row_count
          cur_row_.cells_[i].set_int(mt_stat.update_row_count_);
          break;
        case OB_APP_MIN_COLUMN_ID + 20:
          // delete_row_count
          cur_row_.cells_[i].set_int(mt_stat.delete_row_count_);
          break;
        case OB_APP_MIN_COLUMN_ID + 21:
          cur_row_.cells_[i].set_int(mt_stat.frozen_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 22:
          // freeze_state
          switch (mt->get_freeze_state()) {
            case ObMemtableFreezeState::INVALID:
              cur_row_.cells_[i].set_varchar("INVALID");
              break;
            case ObMemtableFreezeState::NOT_READY_FOR_FLUSH:
              cur_row_.cells_[i].set_varchar("NOT_READY_FOR_FLUSH");
              break;
            case ObMemtableFreezeState::READY_FOR_FLUSH:
              cur_row_.cells_[i].set_varchar("READY_FOR_FLUSH");
              break;
            case ObMemtableFreezeState::FLUSHED:
              cur_row_.cells_[i].set_varchar("FLUSHED");
              break;
            case ObMemtableFreezeState::RELEASED:
              cur_row_.cells_[i].set_varchar("RELEASED");
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "invalid freeze state", K(ret), K(col_id));
              break;
          }
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 23:
          // freeze_time_dist
          get_freeze_time_dist(mt_stat);
          cur_row_.cells_[i].set_varchar(freeze_time_dist_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 24: {
          // compaction info list
          cur_row_.cells_[i].set_varchar("-");
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
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
