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

#include "observer/virtual_table/ob_all_virtual_table_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "observer/ob_server.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/column_store/ob_column_oriented_sstable.h"

using namespace oceanbase;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace observer;

ObAllVirtualTableMgr::ObAllVirtualTableMgr()
    : ObVirtualTableScannerIterator(),
      addr_(),
      tablet_iter_(nullptr),
      tablet_allocator_("VTTable"),
      tablet_handle_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      table_store_iter_(),
      iter_buf_(nullptr)
{
}


ObAllVirtualTableMgr::~ObAllVirtualTableMgr()
{
  reset();
}

void ObAllVirtualTableMgr::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;

  if (OB_NOT_NULL(iter_buf_)) {
    allocator_->free(iter_buf_);
    iter_buf_ = nullptr;
  }

  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTableMgr::init(common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (start_to_read_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), KP(allocator));
  } else if (OB_ISNULL(iter_buf_ = allocator->alloc(sizeof(ObTenantTabletIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc tablet iter buf", K(ret));
  } else {
    allocator_ = allocator;
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualTableMgr::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute", K(ret));
  }
  return ret;
}

void ObAllVirtualTableMgr::release_last_tenant()
{
  table_store_iter_.reset();
  tablet_handle_.reset();
  if (OB_NOT_NULL(tablet_iter_)) {
    tablet_iter_->~ObTenantTabletIterator();
    tablet_iter_ = nullptr;
  }
  tablet_allocator_.reset();
}

bool ObAllVirtualTableMgr::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)){
    return true;
  }
  return false;
}

int ObAllVirtualTableMgr::get_next_tablet()
{
  int ret = OB_SUCCESS;

  tablet_handle_.reset();
  tablet_allocator_.reuse();
  if (nullptr == tablet_iter_) {
    tablet_allocator_.set_tenant_id(MTL_ID());
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    if (OB_ISNULL(tablet_iter_ = new (iter_buf_) ObTenantTabletIterator(*t3m, tablet_allocator_, nullptr/*no op*/))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to new tablet_iter_", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tablet_iter_->get_next_tablet(tablet_handle_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "fail to get tablet iter", K(ret));
    }
  } else if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected invalid tablet", K(ret), K(tablet_handle_));
  } else {
    ls_id_ = tablet_handle_.get_obj()->get_tablet_meta().ls_id_.id();
  }

  return ret;
}

int ObAllVirtualTableMgr::get_next_table(ObITable *&table)
{
  int ret = OB_SUCCESS;
  table = nullptr;
  if (OB_FAIL(table_store_iter_.get_next(table))) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      SERVER_LOG(WARN, "fail to iterate next table", K(ret));
    } else {
      ret = OB_SUCCESS;
      while (OB_SUCC(ret)) {
        table_store_iter_.reset();
        if (OB_FAIL(get_next_tablet())) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "fail to get next tablet", K(ret));
          }
        } else if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected invalid tablet", K(ret), K_(tablet_handle));
        } else if (OB_FAIL(tablet_handle_.get_obj()->get_all_tables(table_store_iter_, true/*unpack_cg_table*/))) {
          SERVER_LOG(WARN, "fail to get all tables", K(ret), K_(tablet_handle), K_(table_store_iter));
        } else if (0 != table_store_iter_.count()) {
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(table_store_iter_.get_next(table))) {
        SERVER_LOG(WARN, "fail to get table after switch tablet", K(ret));
      }
    }
  }
  return ret;
}

int ObAllVirtualTableMgr::process_curr_tenant(common::ObNewRow *&row)
{
  // each get_next_row will switch to required tenant, and released guard later
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  if (OB_UNLIKELY(!start_to_read_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(start_to_read_), K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(get_next_table(table))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_table failed", K(ret));
    }
  } else if (NULL == table) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table shouldn't NULL here", K(ret), K(table));
  } else {
    const int64_t nested_offset = table->is_sstable() ? static_cast<ObSSTable *>(table)->get_macro_offset() : 0;
    const int64_t nested_size = table->is_sstable() ? static_cast<ObSSTable *>(table)->get_macro_read_size() : 0;
    const ObITable::TableKey &table_key = table->get_key();
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP:
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case TENANT_ID:
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        case LS_ID:
          cur_row_.cells_[i].set_int(ls_id_);
          break;
        case TABLET_ID:
          cur_row_.cells_[i].set_int(table_key.tablet_id_.id());
          break;
          //TODO:SCN
        case START_LOG_SCN: {
          uint64_t v = table_key.scn_range_.start_scn_.get_val_for_inner_table_field();
          cur_row_.cells_[i].set_uint64(v);
          break;
        }
        case END_LOG_SCN: {
          uint64_t v = table_key.scn_range_.end_scn_.get_val_for_inner_table_field();
          cur_row_.cells_[i].set_uint64(v);
        }
          break;
        case UPPER_TRANS_VERSION: {
          uint64_t v =  table->get_upper_trans_version() < 0 ? 0 : (uint64_t)table->get_upper_trans_version();
          cur_row_.cells_[i].set_uint64(v);
          break;
        }
        case TABLE_TYPE:
          cur_row_.cells_[i].set_int(table_key.table_type_);
          break;
        case SIZE: {
          int64_t size = 0;
          if (table->is_memtable()) {
            size = static_cast<ObIMemtable *>(table)->get_occupied_size();
          } else if (table->is_sstable()) {
            size = static_cast<blocksstable::ObSSTable *>(table)->get_occupy_size();
          }
          cur_row_.cells_[i].set_int(size);
          break;
        }
        case DATA_BLOCK_CNT: {
          const int64_t blk_cnt = table->is_memtable() ? 0
            : static_cast<ObSSTable *>(table)->get_data_macro_block_count();
          cur_row_.cells_[i].set_int(blk_cnt);
          break;
        }
        case INDEX_BLOCK_CNT: {
          int64_t blk_cnt = 0;
          if (table->is_sstable()) {
            blocksstable::ObSSTable * sstable = static_cast<blocksstable::ObSSTable *>(table);
            blk_cnt = sstable->get_total_macro_block_count() - sstable->get_data_macro_block_count();
          }
          cur_row_.cells_[i].set_int(blk_cnt);
          break;
        }
        case LINKED_BLOCK_CNT: {
          int64_t blk_cnt = 0;
          if (table->is_sstable()) {
            blocksstable::ObSSTableMetaHandle sst_meta_hdl;
            if (OB_FAIL(static_cast<blocksstable::ObSSTable *>(table)->get_meta(sst_meta_hdl))) {
              SERVER_LOG(WARN, "fail to get sstable meta handle", K(ret));
            } else {
              blk_cnt = sst_meta_hdl.get_sstable_meta().get_linked_macro_block_count();
            }
          }
          cur_row_.cells_[i].set_int(blk_cnt);
          break;
        }
        case REF:
          cur_row_.cells_[i].set_int(table->get_ref());
          break;
        case IS_ACTIVE: {
          bool is_active = false;
          if (table->is_memtable()) {
            is_active = static_cast<ObIMemtable *>(table)->is_active_memtable();
          }
          cur_row_.cells_[i].set_varchar(is_active ? "YES" : "NO");
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case CONTAIN_UNCOMMITTED_ROW: {
          bool contain_uncommitted_row = false;
          if (table->is_memtable()) {
            contain_uncommitted_row = true;
          } else if (table->is_sstable()) {
            contain_uncommitted_row = static_cast<blocksstable::ObSSTable *>(table)->contain_uncommitted_row();
          }
          cur_row_.cells_[i].set_varchar(contain_uncommitted_row ? "YES" : "NO");
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case NESTED_OFFSET:
          cur_row_.cells_[i].set_int(nested_offset);
          break;
        case NESTED_SIZE:
          cur_row_.cells_[i].set_int(nested_size);
          break;
        case CG_IDX:
          cur_row_.cells_[i].set_int(table_key.get_column_group_id());
          break;
        case DATA_CHECKSUM: {
          int64_t data_checksum = 0;
          if (table->is_memtable()) {
            // memtable has no data checksum, do nothing
          } else if (table->is_co_sstable()) {
            data_checksum = static_cast<storage::ObCOSSTableV2 *>(table)->get_cs_meta().data_checksum_;
          } else if (table->is_sstable()) {
            data_checksum = static_cast<blocksstable::ObSSTable *>(table)->get_data_checksum();
          }
          cur_row_.cells_[i].set_int(data_checksum);
          break;
        }
        case TABLE_FLAG:
          // TODO(yanfeng): only for place holder purpose, need change when auto_split branch merge
          cur_row_.cells_[i].set_int(0);
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

