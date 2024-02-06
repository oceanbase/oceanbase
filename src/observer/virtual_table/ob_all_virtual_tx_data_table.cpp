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

#include "observer/virtual_table/ob_all_virtual_tx_data_table.h"
#include "observer/ob_server.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase::common;
using namespace oceanbase::memtable;
using namespace oceanbase::storage;
namespace oceanbase {
namespace observer {

ObAllVirtualTxDataTable::ObAllVirtualTxDataTable()
    : ObVirtualTableScannerIterator(),
      addr_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      memtable_array_pos_(-1),
      sstable_array_pos_(-1),
      ls_iter_guard_(),
      tablet_handle_(),
      table_store_wrapper_(),
      mgr_handle_(),
      memtable_handles_(),
      sstable_handles_()
{}

ObAllVirtualTxDataTable::~ObAllVirtualTxDataTable()
{
  reset();
}

void ObAllVirtualTxDataTable::reset()
{
  // release tenant resources first
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualTxDataTable::release_last_tenant()
{
  // resources related with tenant must be released by this function
  ls_iter_guard_.reset();
}

bool ObAllVirtualTxDataTable::is_need_process(uint64_t tenant_id)
{
  bool bool_ret = false;
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    bool_ret = true;
  }

  return bool_ret;
}

int ObAllVirtualTxDataTable::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  ObITable *tx_data_table = nullptr;
  RowData row_data;

  if (nullptr == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be nullptr", K(allocator_), KR(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (ls_iter_guard_.get_ptr() == nullptr &&
             OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "get_ls_iter fail", KR(ret));
  } else if (OB_FAIL(get_next_tx_data_table_(tx_data_table))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get next tx data table failed", KR(ret));
    }
  } else if (OB_UNLIKELY(nullptr == tx_data_table)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tx_data_table shouldn't nullptr here", KR(ret), KP(tx_data_table));
  } else if (OB_FAIL(prepare_row_data_(tx_data_table, row_data))) {
    SERVER_LOG(WARN, "prepare_row_data_ fail", KR(ret), KP(tx_data_table));
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP_COL:
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", KR(ret));
          }
          break;
        case SVR_PORT_COL:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case TENANT_ID_COL:
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        case LS_ID_COL:
          cur_row_.cells_[i].set_int(ls_id_);
          break;
        case STATE_COL:
          cur_row_.cells_[i].set_varchar(row_data.state_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case START_SCN_COL: {
          uint64_t v = tx_data_table->get_key().scn_range_.start_scn_.get_val_for_inner_table_field();
          cur_row_.cells_[i].set_uint64(v);
          break;
        }
        case END_SCN_COL: {
          uint64_t v = tx_data_table->get_key().scn_range_.end_scn_.get_val_for_inner_table_field();
          cur_row_.cells_[i].set_uint64(v);
          break;
        }
        case TX_DATA_COUNT_COL:
          cur_row_.cells_[i].set_int(row_data.tx_data_count_);
          break;
        case MIN_TX_SCN_COL:
          cur_row_.cells_[i].set_uint64(row_data.min_tx_scn_.get_val_for_inner_table_field());
          break;
        case MAX_TX_SCN_COL:
          cur_row_.cells_[i].set_uint64(row_data.max_tx_scn_.get_val_for_inner_table_field());
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", KR(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

int ObAllVirtualTxDataTable::get_next_tx_data_table_(ObITable *&tx_data_table)
{
  int ret = OB_SUCCESS;

  // memtable_array_pos_ < 0 && sstable_array_pos_ < 0 means all the tx data memtables of this logstream have been
  // disposed or the first time get_next_tx_data_memtable() is invoked,  when get_next_tx_data_table_ is invoked the
  // first time, memtable_array_pos_ and memtable_array_.count() are both -1
  while (OB_SUCC(ret) && memtable_array_pos_ < 0 && sstable_array_pos_ < 0) {
    ObLS *ls = nullptr;
    ObTablet *tablet = nullptr;
    ObIMemtableMgr *memtable_mgr = nullptr;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    memtable_handles_.reset();
    sstable_handles_.reset();
    tablet_handle_.reset();
    mgr_handle_.reset();
    table_store_wrapper_.reset();

    if (OB_FAIL(ls_iter_guard_->get_next(ls))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next logstream", KR(ret));
      }
    } else if (FALSE_IT(ls_id_ = ls->get_ls_id().id())) {
    } else if (OB_FAIL(ls->get_tablet_svr()->get_tx_data_memtable_mgr(mgr_handle_))) {
      SERVER_LOG(WARN, "fail to get tx data memtable mgr.", KR(ret));
    } else if (FALSE_IT(memtable_mgr = mgr_handle_.get_memtable_mgr())) {
    } else if (OB_FAIL(memtable_mgr->get_all_memtables(memtable_handles_))) {
      SERVER_LOG(WARN, "fail to get all memtables for log stream", KR(ret));
    } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(LS_TX_DATA_TABLET, tablet_handle_))) {
      SERVER_LOG(WARN, "fail to get tx data tablet", KR(ret));
    } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
    } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper_))) {
      SERVER_LOG(WARN, "fail to fetch table store", K(ret));
    } else if (OB_FAIL(table_store_wrapper_.get_member()->get_minor_sstables().get_all_tables(sstable_handles_))) {
      SERVER_LOG(WARN, "fail to get sstable handles", KR(ret));
    } else {
      // iterate from the newest memtable in memtable handles
      memtable_array_pos_ = memtable_handles_.count() - 1;
      // iterate from the newest sstable in sstable handles
      sstable_array_pos_ = sstable_handles_.count() - 1;
      if (memtable_array_pos_ < 0 && sstable_array_pos_ < 0) {
        SERVER_LOG(INFO,
                   "empty logstream. may be offlined",
                   KR(ret),
                   K(ls_id_),
                   K(addr_),
                   K(memtable_array_pos_),
                   K(sstable_array_pos_));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (memtable_array_pos_ >= 0) {
    tx_data_table = memtable_handles_[memtable_array_pos_--].get_table();
  } else if (sstable_array_pos_ >= 0) {
    tx_data_table = sstable_handles_[sstable_array_pos_--];
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObAllVirtualTxDataTable::prepare_row_data_(ObITable *tx_data_table, RowData &row_data)
{
  int ret = OB_SUCCESS;
  if (ObITable::TableType::TX_DATA_MEMTABLE == tx_data_table->get_key().table_type_) {
    ObTxDataMemtable *tx_data_memtable = static_cast<ObTxDataMemtable *>(tx_data_table);
    row_data.state_ = tx_data_memtable->get_state_string();
    row_data.tx_data_count_ = tx_data_memtable->size();
    row_data.min_tx_scn_ = tx_data_memtable->get_min_tx_scn();
    row_data.max_tx_scn_ = tx_data_memtable->get_max_tx_scn();
  } else if (tx_data_table->is_multi_version_minor_sstable()) {
    ObSSTable *tx_data_sstable = static_cast<ObSSTable *>(tx_data_table);
    ObSSTableMetaHandle sstable_meta_hdl;
    if (OB_FAIL(tx_data_sstable->get_meta(sstable_meta_hdl))) {
      STORAGE_LOG(WARN, "fail to get sstable meta handle", K(ret), KPC(tx_data_sstable));
    } else {
      row_data.state_ = ObITable::get_table_type_name(tx_data_table->get_key().table_type_);
      row_data.tx_data_count_ = sstable_meta_hdl.get_sstable_meta().get_row_count();
      row_data.min_tx_scn_ = sstable_meta_hdl.get_sstable_meta().get_filled_tx_scn();
      row_data.max_tx_scn_ = tx_data_sstable->get_key().scn_range_.end_scn_;
    }
  } else {
    STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "Iterate an invalid table while select virtual tx data table.");
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
