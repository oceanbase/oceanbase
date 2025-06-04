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

#include "observer/virtual_table/ob_all_virtual_ss_tablet_meta.h"
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

ObAllVirtualSSTabletMeta::ObAllVirtualSSTabletMeta()
    : ObVirtualTableScannerIterator(),
      tenant_id_(0),
      ls_id_(),
      tablet_id_(),
      transfer_scn_()
{
}

ObAllVirtualSSTabletMeta::~ObAllVirtualSSTabletMeta()
{
  reset();
}

void ObAllVirtualSSTabletMeta::reset()
{
  tenant_id_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  transfer_scn_.reset();
}

int ObAllVirtualSSTabletMeta::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

#ifndef OB_BUILD_SHARED_STORAGE
  ret = OB_ITER_END;
#else
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else if (false == start_to_read_) {
    if (OB_FAIL(get_primary_key_())) {
      SERVER_LOG(WARN, "get primary key failed", KR(ret));
    } else if (OB_FAIL(generate_virtual_row_(tablet_meta_row_))) {
      if (OB_ITER_END == ret) {
      } else if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_ITER_END;
      } else {
        SERVER_LOG(WARN, "generate virtual tablet meta row failed", KR(ret));
      }
    } else if (OB_FAIL(fill_in_row_(tablet_meta_row_, row))) {
      SERVER_LOG(WARN, "fill in row failed", KR(ret));
    } else {
      start_to_read_ = true;
    }
  } else {
    ret = OB_ITER_END;
  }
#endif
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObAllVirtualSSTabletMeta::get_primary_key_()
{
  int ret = OB_SUCCESS;
  if (key_ranges_.count() != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "only support select a single row once, multiple range select is ");
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

int ObAllVirtualSSTabletMeta::handle_key_range_(ObNewRange &key_range)
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
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant name, ls id, tablet id and transfer scn must be specified. range select is ");
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

int ObAllVirtualSSTabletMeta::fill_in_row_(const VirtualTabletMetaRow &row_data, common::ObNewRow *&row)
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
    case META_VERSION:
      cur_row_.cells_[i].set_int(row_data.version_.get_val_for_inner_table_field());
      break;
    case DATA_TABLET_ID:
      cur_row_.cells_[i].set_int(row_data.data_tablet_id_.id());
      break;
    case CREATE_SCN: {
      int64_t v = row_data.create_scn_.get_val_for_inner_table_field();
      cur_row_.cells_[i].set_int(v);
      break;
    }
    case START_SCN: {
      int64_t v = row_data.start_scn_.get_val_for_inner_table_field();
      cur_row_.cells_[i].set_int(v);
      break;
    }
    case CREATE_SCHEMA_VERSION:
      cur_row_.cells_[i].set_int(row_data.create_schema_version_);
      break;
    case DATA_CHECKPOINT_SCN: {
      int64_t v = row_data.data_checkpoint_scn_.get_val_for_inner_table_field();
      cur_row_.cells_[i].set_int(v);
      break;
    }
    case MDS_CHECKPOINT_SCN: {
      int64_t v = row_data.mds_checkpoint_scn_.get_val_for_inner_table_field();
      cur_row_.cells_[i].set_int(v);
      break;
    }
    case DDL_CHECKPOINT_SCN: {
      int64_t v = row_data.ddl_checkpoint_scn_.get_val_for_inner_table_field();
      cur_row_.cells_[i].set_int(v);
      break;
    }
    case MULTI_VERSION_START:
      cur_row_.cells_[i].set_int(row_data.multi_version_start_);
      break;
    case TABLET_SNAPSHOT_VERSION:
      cur_row_.cells_[i].set_int(row_data.tablet_snapshot_version_);
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

int ObAllVirtualSSTabletMeta::generate_virtual_row_(VirtualTabletMetaRow &row)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id_)
  {
    common::ObArenaAllocator allocator("TabletMetaVT",
                                       OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
    ObSSMetaService *meta_service = MTL(ObSSMetaService *);
    ObSSMetaReadParam param;
    ObTablet *tablet = nullptr;
    ObTabletHandle tablet_hdl;
    param.set_tablet_level_param(ObSSLogMetaType::SSLOG_TABLET_META,
                                ls_id_,
                                tablet_id_,
                                transfer_scn_);
    if (OB_UNLIKELY(!param.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(meta_service->get_tablet(param, allocator, tablet_hdl))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_ITER_END;
      } else {
        SERVER_LOG(WARN, "get tablet from meta service failed", KR(ret), K(param));
      }
    } else if (OB_ISNULL(tablet = tablet_hdl.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "get tablet failed ", KR(ret), KP(tablet), K(param));
    } else {
      const ObTabletMeta &meta = tablet->get_tablet_meta();
      row.version_ = SCN::min_scn();   // TODO: use the real version
      row.data_tablet_id_ = meta.data_tablet_id_;
      row.create_scn_ = meta.create_scn_;
      row.start_scn_ = meta.start_scn_;
      row.create_schema_version_ = meta.create_schema_version_;
      row.data_checkpoint_scn_ = meta.clog_checkpoint_scn_;
      row.mds_checkpoint_scn_ = meta.mds_checkpoint_scn_;
      row.ddl_checkpoint_scn_ = meta.ddl_checkpoint_scn_;
      row.multi_version_start_ = meta.multi_version_start_;
      row.tablet_snapshot_version_ = meta.snapshot_version_;
      SERVER_LOG(DEBUG, "generate row succeed", K(param), K(row));
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
