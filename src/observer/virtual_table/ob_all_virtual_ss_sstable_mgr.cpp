/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "observer/virtual_table/ob_all_virtual_ss_sstable_mgr.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/ob_shared_meta_service.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualSSSSTableMgr::ObAllVirtualSSSSTableMgr()
    : ObVirtualTableScannerIterator(),
      tenant_id_(0),
      ls_id_(),
      tablet_id_(),
      transfer_scn_(),
      tablet_(nullptr)
{
}

ObAllVirtualSSSSTableMgr::~ObAllVirtualSSSSTableMgr()
{
  reset();
}

void ObAllVirtualSSSSTableMgr::reset()
{
  tenant_id_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  transfer_scn_.reset();
  table_store_iter_.reset();
  tablet_ = nullptr;
  tablet_hdl_.reset();
  tablet_allocator_.reset();
  ObVirtualTableScannerIterator::reset();
  tablet_allocator_.reset();
}

int ObAllVirtualSSSSTableMgr::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

#ifndef OB_BUILD_SHARED_STORAGE
  ret = OB_ITER_END;
#else
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else {
    if (false == start_to_read_) {
      if (OB_FAIL(get_primary_key_())) {
        SERVER_LOG(WARN, "get primary key failed", KR(ret));
      } else {
        start_to_read_ = true;
      }
    }
    if (true == start_to_read_) {
      if (OB_FAIL(generate_virtual_row_(ss_sstable_row_))) {
        if (OB_ITER_END == ret) {
        } else if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_ITER_END;
        } else {
          SERVER_LOG(WARN, "generate virtual tablet meta row failed", KR(ret));
        }
      } else if (OB_FAIL(fill_in_row_(ss_sstable_row_, row))) {
        SERVER_LOG(WARN, "fill in row failed", KR(ret));
      }
    } else {
      ret = OB_ITER_END;
    }
  }
#endif
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObAllVirtualSSSSTableMgr::get_primary_key_()
{
  int ret = OB_SUCCESS;
  if (key_ranges_.count() != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "only support select a single tablet once, multiple tablet select is ");
    SERVER_LOG(WARN, "invalid key ranges", KR(ret));
  } else {
    ObNewRange &key_range = key_ranges_.at(0);
    if (OB_UNLIKELY(key_range.get_start_key().get_obj_cnt() != ROWKEY_COL_COUNT || key_range.get_end_key().get_obj_cnt() != ROWKEY_COL_COUNT)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR,
                 "unexpected key_ranges_ of rowkey columns",
                 KR(ret),
                 "size of start key",
                 key_range.get_start_key().get_obj_cnt(),
                 "size of end key",
                 key_range.get_end_key().get_obj_cnt());
    } else if (OB_FAIL(handle_key_range_(key_range))) {
      SERVER_LOG(WARN, "handle key range faield", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualSSSSTableMgr::handle_key_range_(ObNewRange &key_range)
{
  int ret = OB_SUCCESS;
  ObObj tenant_obj_low = (key_range.get_start_key().get_obj_ptr()[0]);
  ObObj tenant_obj_high = (key_range.get_end_key().get_obj_ptr()[0]);
  ObObj ls_obj_low = (key_range.get_start_key().get_obj_ptr()[1]);
  ObObj ls_obj_high = (key_range.get_end_key().get_obj_ptr()[1]);
  ObObj tablet_obj_low = (key_range.get_start_key().get_obj_ptr()[2]);
  ObObj tablet_obj_high = (key_range.get_end_key().get_obj_ptr()[2]);
  ObObj transfer_obj_low = (key_range.get_start_key().get_obj_ptr()[3]);
  ObObj transfer_obj_high = (key_range.get_end_key().get_obj_ptr()[3]);

  uint64_t tenant_low = tenant_obj_low.is_min_value() ? 0 : tenant_obj_low.get_uint64();
  uint64_t tenant_high = tenant_obj_high.is_max_value() ? UINT64_MAX : tenant_obj_high.get_uint64();
  ObLSID ls_low = ls_obj_low.is_min_value() ? ObLSID(0) : ObLSID(ls_obj_low.get_int());
  ObLSID ls_high = ls_obj_high.is_max_value() ? ObLSID(INT64_MAX) : ObLSID(ls_obj_high.get_int());
  ObTabletID tablet_low = tablet_obj_low.is_min_value() ? ObTabletID(0) : ObTabletID(tablet_obj_low.get_int());
  ObTabletID tablet_high = tablet_obj_high.is_max_value() ? ObTabletID(INT64_MAX) : ObTabletID(tablet_obj_high.get_int());

  SCN transfer_low;
  if (transfer_obj_low.is_min_value()) { transfer_low = SCN::min_scn(); }
  else { transfer_low.convert_for_sql(transfer_obj_low.get_int()); }
  SCN transfer_high;
  if (transfer_obj_high.is_max_value()) { transfer_high = SCN::max_scn(); }
  else { transfer_high.convert_for_sql(transfer_obj_high.get_int()); }

  if (tenant_low != tenant_high
      || ls_low != ls_high
      || tablet_low != tablet_high
      || transfer_low != transfer_high) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant id, ls id, tablet id and transfer scn must be specified. range select is ");
    SERVER_LOG(WARN,
               "only support point select.",
               KR(ret),
               K(tenant_low),
               K(tenant_high),
               K(ls_low),
               K(ls_high),
               K(tablet_low),
               K(tablet_high),
               K(transfer_low),
               K(transfer_high));
  } else {
    tenant_id_ = tenant_low;
    ls_id_ = ls_low;
    tablet_id_ = tablet_low;
    transfer_scn_ = transfer_low;
  }

  return ret;
}

int ObAllVirtualSSSSTableMgr::get_next_tablet_()
{
  int ret = OB_SUCCESS;

  if (tablet_ != nullptr) {
    ret = OB_ITER_END;
  } else {
    tablet_allocator_.reuse();
    ObSSMetaService *meta_service = MTL(ObSSMetaService *);
    ObSSMetaReadParam param;
    ObTablet *tablet = nullptr;
    param.set_tablet_level_param(ObSSLogMetaType::SSLOG_TABLET_META,
                                ls_id_,
                                tablet_id_,
                                transfer_scn_);
    if (OB_UNLIKELY(!param.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(meta_service->get_tablet(param, tablet_allocator_, tablet_hdl_))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_ITER_END;
      } else {
        SERVER_LOG(WARN, "get tablet from meta service failed", KR(ret), K(param));
      }
    } else if (OB_ISNULL(tablet_ = tablet_hdl_.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "get tablet failed ", KR(ret), KP(tablet), K(param));
    }
  }
  return ret;
}

int ObAllVirtualSSSSTableMgr::get_next_table_(ObITable *&table)
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
        if (OB_FAIL(get_next_tablet_())) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "fail to get next tablet", K(ret));
          }
        } else if (OB_FAIL(tablet_->get_all_tables(table_store_iter_, true/*unpack_cg_table*/))) {
          SERVER_LOG(WARN, "fail to get all tables", K(ret), KP_(tablet), K_(table_store_iter));
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

int ObAllVirtualSSSSTableMgr::fill_in_row_(const VirtualSSSSTableRow &row_data, common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
    case TENANT_ID:
      cur_row_.cells_[i].set_int(tenant_id_);
      break;
    case LS_ID:
      cur_row_.cells_[i].set_int(ls_id_.id());
      break;
    case TABLET_ID:
      cur_row_.cells_[i].set_int(tablet_id_.id());
      break;
    case TRANSFER_SCN:
      cur_row_.cells_[i].set_int(transfer_scn_.get_val_for_inner_table_field());
      break;
    case TABLE_TYPE:
      cur_row_.cells_[i].set_int(row_data.table_type_);
      break;
    case START_LOG_SCN:
      cur_row_.cells_[i].set_int(row_data.start_log_scn_);
      break;
    case END_LOG_SCN:
      cur_row_.cells_[i].set_int(row_data.end_log_scn_);
      break;
    case UPPER_TRANS_VERSION: {
      cur_row_.cells_[i].set_int(row_data.upper_trans_version_);
      break;
    }
    case SIZE: {
      cur_row_.cells_[i].set_int(row_data.size_);
      break;
    }
    case DATA_BLOCK_COUNT:
      cur_row_.cells_[i].set_int(row_data.data_block_count_);
      break;
    case INDEX_BLOCK_COUNT: {
      cur_row_.cells_[i].set_int(row_data.index_block_count_);
      break;
    }
    case LINKED_BLOCK_COUNT: {
      cur_row_.cells_[i].set_int(row_data.linked_block_count_);
      break;
    }
    case CONTAIN_UNCOMMITTED_ROW: {
      cur_row_.cells_[i].set_varchar(row_data.contain_uncommitted_row_ ? "YES" : "NO");
      cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    }
    case NESTED_OFFSET:
      cur_row_.cells_[i].set_int(row_data.nested_offset_);
      break;
    case NESTED_SIZE:
      cur_row_.cells_[i].set_int(row_data.nested_size_);
      break;
    case CG_IDX:
      cur_row_.cells_[i].set_int(row_data.cg_idx_);
      break;
    case DATA_CHECKSUM:
      cur_row_.cells_[i].set_int(row_data.data_checksum_);
      break;
    case TABLE_FLAG:
      cur_row_.cells_[i].set_int(row_data.table_flag_);
      break;
    case REC_SCN:
      cur_row_.cells_[i].set_int(row_data.rec_scn_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

int ObAllVirtualSSSSTableMgr::generate_virtual_row_(VirtualSSSSTableRow &row)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id_)
  {
    ObITable *table = nullptr;
    if (OB_FAIL(get_next_table_(table))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "get_next_table failed", K(ret));
      }
    } else if (NULL == table) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "table shouldn't NULL here", K(ret), K(table));
    } else if (!table->is_sstable() && !table->is_co_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "should only be sstable", K(ret), KPC(table));
    } else {
      int64_t blk_cnt = 0;
      blocksstable::ObSSTableMetaHandle sst_meta_hdl;
      const int64_t nested_offset = table->is_sstable() ? static_cast<ObSSTable *>(table)->get_macro_offset() : 0;
      const int64_t nested_size = table->is_sstable() ? static_cast<ObSSTable *>(table)->get_macro_read_size() : 0;
      const ObITable::TableKey &table_key = table->get_key();
      const ObTabletMeta &meta = tablet_->get_tablet_meta();

      row.table_type_ = table_key.table_type_;
      row.start_log_scn_ = table_key.scn_range_.start_scn_.get_val_for_inner_table_field();
      row.end_log_scn_ = table_key.scn_range_.end_scn_.get_val_for_inner_table_field();
      row.upper_trans_version_ = table->get_upper_trans_version() < 0 ? 0 : (uint64_t)table->get_upper_trans_version();
      if (table->is_sstable()) {
        row.size_ = static_cast<blocksstable::ObSSTable *>(table)->get_occupy_size();
      } else {
        row.size_ = 0;
      }
      row.data_block_count_ = static_cast<ObSSTable *>(table)->get_data_macro_block_count();
      if (table->is_sstable()) {
        blocksstable::ObSSTable * sstable = static_cast<blocksstable::ObSSTable *>(table);
        blk_cnt = sstable->get_total_macro_block_count() - sstable->get_data_macro_block_count();
      }
      row.index_block_count_ = blk_cnt;
      blk_cnt = 0;
      if (table->is_sstable()) {
        if (OB_FAIL(static_cast<blocksstable::ObSSTable *>(table)->get_meta(sst_meta_hdl))) {
          SERVER_LOG(WARN, "fail to get sstable meta handle", K(ret));
        } else {
          blk_cnt = sst_meta_hdl.get_sstable_meta().get_linked_macro_block_count();
        }
      }
      row.linked_block_count_ = blk_cnt;
      if (table->is_sstable()) {
        row.contain_uncommitted_row_ = static_cast<blocksstable::ObSSTable *>(table)->contain_uncommitted_row();
      } else {
        row.contain_uncommitted_row_ = false;
      }

      row.nested_offset_ = nested_offset;
      row.nested_size_ = nested_size;
      row.cg_idx_ = table_key.get_column_group_id();
      int64_t data_checksum = 0;
      if (table->is_co_sstable() && !static_cast<const ObCOSSTableV2 *>(table)->is_cgs_empty_co_table()) {
        data_checksum = static_cast<storage::ObCOSSTableV2 *>(table)->get_cs_meta().data_checksum_;
      } else if (table->is_sstable()) {
        data_checksum = static_cast<blocksstable::ObSSTable *>(table)->get_data_checksum();
      }
      row.data_checksum_ = data_checksum;
      ObTableBackupFlag table_backup_flag;
      if (table->is_sstable()) {
        if (OB_FAIL(static_cast<blocksstable::ObSSTable *>(table)->get_meta(sst_meta_hdl))) {
          SERVER_LOG(WARN, "fail to get sstable meta handle", K(ret));
        } else {
          table_backup_flag = sst_meta_hdl.get_sstable_meta().get_table_backup_flag();
        }
      }
      row.table_flag_ = table_backup_flag.flag_;
      row.rec_scn_ = table->get_rec_scn().get_val_for_inner_table_field();
      SERVER_LOG(DEBUG, "generate row succeed", K(row));
    }
  }

  if (OB_FAIL(ret) && OB_TENANT_NOT_IN_SERVER == ret) {
    ret = OB_ITER_END;
  }
  return ret;
}
#endif

} // observer
} // oceanbase
