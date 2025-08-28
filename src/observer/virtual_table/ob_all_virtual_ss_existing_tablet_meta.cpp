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

#include "observer/virtual_table/ob_all_virtual_ss_existing_tablet_meta.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualSSExistingTabletMeta::ObAllVirtualSSExistingTabletMeta()
    : ObVirtualTableScannerIterator(),
      tenant_id_(0),
      ls_id_(),
      tablet_id_(),
      transfer_scn_(),
      result_()
{
}

ObAllVirtualSSExistingTabletMeta::~ObAllVirtualSSExistingTabletMeta()
{
  reset();
}

void ObAllVirtualSSExistingTabletMeta::reset()
{
  tenant_id_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  transfer_scn_.reset();
  result_.reset();
}

int ObAllVirtualSSExistingTabletMeta::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_SHARED_STORAGE
  ret = OB_ITER_END;
#else
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else if (false == start_to_read_) {
    ObArray<VirtualTabletMetaRow> tablet_meta_rows;
    if (OB_FAIL(get_primary_key_())) {
      SERVER_LOG(WARN, "get primary key failed", KR(ret));
    } else if (OB_FAIL(generate_virtual_rows_(tablet_meta_rows))) {
      if (OB_ITER_END == ret) {
      } else if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_ITER_END;
      } else {
        SERVER_LOG(WARN, "generate virtual tablet meta row failed", KR(ret));
      }
    } else if (OB_FAIL(fill_in_rows_(tablet_meta_rows))) {
      SERVER_LOG(WARN, "fill in row failed", KR(ret));
    } else {
      start_to_read_ = true;
      scanner_it_ = scanner_.begin();
    }
  }
  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
#endif
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObAllVirtualSSExistingTabletMeta::get_primary_key_()
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

int ObAllVirtualSSExistingTabletMeta::handle_key_range_(ObNewRange &key_range)
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

int ObAllVirtualSSExistingTabletMeta::fill_in_rows_(const ObArray<VirtualTabletMetaRow> &row_datas)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  int row_count = row_datas.count();
  ObObj *cells = cur_row_.cells_;
  for (int64_t row_i = 0; OB_SUCC(ret) && row_i < row_count; ++row_i) {
    const VirtualTabletMetaRow &row_data = row_datas[row_i];
    for (int64_t col_i = 0; OB_SUCC(ret) && col_i < col_count; ++col_i) {
      uint64_t col_id = output_column_ids_.at(col_i);
      switch (col_id) {
      case TENANT_ID:
        cells[col_i].set_int(tenant_id_);
        break;
      case LS_ID:
        cells[col_i].set_int(ls_id_.id());
        break;
      case TABLET_ID:
        cells[col_i].set_int(tablet_id_.id());
        break;
      case TRANSFER_SCN:
        cells[col_i].set_int(transfer_scn_.get_val_for_inner_table_field());
        break;
      case META_VERSION:
        cells[col_i].set_int(row_data.version_.get_val_for_inner_table_field());
        break;
      case DATA_TABLET_ID:
        cells[col_i].set_int(row_data.data_tablet_id_.id());
        break;
      case CREATE_SCN: {
        int64_t v = row_data.create_scn_.get_val_for_inner_table_field();
        cells[col_i].set_int(v);
        break;
      }
      case START_SCN: {
        int64_t v = row_data.start_scn_.get_val_for_inner_table_field();
        cells[col_i].set_int(v);
        break;
      }
      case CREATE_SCHEMA_VERSION:
        cells[col_i].set_int(row_data.create_schema_version_);
        break;
      case DATA_CHECKPOINT_SCN: {
        int64_t v = row_data.data_checkpoint_scn_.get_val_for_inner_table_field();
        cells[col_i].set_int(v);
        break;
      }
      case MDS_CHECKPOINT_SCN: {
        int64_t v = row_data.mds_checkpoint_scn_.get_val_for_inner_table_field();
        cells[col_i].set_int(v);
        break;
      }
      case DDL_CHECKPOINT_SCN: {
        int64_t v = row_data.ddl_checkpoint_scn_.get_val_for_inner_table_field();
        cells[col_i].set_int(v);
        break;
      }
      case MULTI_VERSION_START:
        cells[col_i].set_int(row_data.multi_version_start_);
        break;
      case TABLET_SNAPSHOT_VERSION:
        cells[col_i].set_int(row_data.tablet_snapshot_version_);
        break;
      case SSTABLE_OP_ID:
        cells[col_i].set_int(row_data.sstable_op_id_);
        break;
      case UPDATE_REASON:
        if (row_data.update_reason_str_.empty()) {
          cells[col_i].set_varchar(get_meta_update_reason_name(row_data.update_reason_));
        } else {
          cells[col_i].set_varchar(row_data.update_reason_str_);
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))){
      SERVER_LOG(WARN, "fail to add row to scanner", K(ret), K(cur_row_));
    }
  }
  return ret;
}

int ObAllVirtualSSExistingTabletMeta::extract_result_(
    common::sqlclient::ObMySQLResult &res,
    VirtualTabletMetaRow &row)
{
  int ret = OB_SUCCESS;
  int64_t meta_version = 0;
  int64_t data_tablet_id;
  int64_t create_scn = 0;
  int64_t start_scn = 0;
  int64_t data_checkpoint_scn = 0;
  int64_t mds_checkpoint_scn = 0;
  int64_t ddl_checkpoint_scn = 0;

  (void)GET_COL_IGNORE_NULL(res.get_int, "meta_version", meta_version);
  (void)GET_COL_IGNORE_NULL(res.get_int, "data_tablet_id", data_tablet_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "create_scn", create_scn);
  (void)GET_COL_IGNORE_NULL(res.get_int, "start_scn", start_scn);
  (void)GET_COL_IGNORE_NULL(res.get_int, "create_schema_version", row.create_schema_version_);
  (void)GET_COL_IGNORE_NULL(res.get_int, "data_checkpoint_scn", data_checkpoint_scn);
  (void)GET_COL_IGNORE_NULL(res.get_int, "mds_checkpoint_scn", mds_checkpoint_scn);
  (void)GET_COL_IGNORE_NULL(res.get_int, "ddl_checkpoint_scn", ddl_checkpoint_scn);
  (void)GET_COL_IGNORE_NULL(res.get_int, "multi_version_start", row.multi_version_start_);
  (void)GET_COL_IGNORE_NULL(res.get_int, "tablet_snapshot_version", row.tablet_snapshot_version_);
  (void)GET_COL_IGNORE_NULL(res.get_int, "sstable_op_id", row.sstable_op_id_);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "update_reason", row.update_reason_str_);
  if (OB_FAIL(ret)) {
  } else {
    row.data_tablet_id_ = data_tablet_id;
    row.version_.convert_for_sql(meta_version);
    row.create_scn_.convert_for_sql(create_scn);
    row.start_scn_.convert_for_sql(start_scn);
    row.data_checkpoint_scn_.convert_for_sql(data_checkpoint_scn);
    row.mds_checkpoint_scn_.convert_for_sql(mds_checkpoint_scn);
    row.ddl_checkpoint_scn_.convert_for_sql(ddl_checkpoint_scn);
  }
  return ret;
}

int ObAllVirtualSSExistingTabletMeta::get_virtual_rows_remote_(
    common::sqlclient::ObMySQLResult &res,
    ObArray<VirtualTabletMetaRow> &row_datas)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    VirtualTabletMetaRow row;
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN, "get next result failed", KR(ret));
      }
      break;
    } else if (OB_FAIL(extract_result_(res, row))) {
      SERVER_LOG(WARN, "failed to extract result", KR(ret));
    } else if (OB_FAIL(row_datas.push_back(row))) {
      SERVER_LOG(WARN, "failed to push back row_datas. ", K(row_datas));
    }
  } // end while
  return ret;
}

int ObAllVirtualSSExistingTabletMeta::get_virtual_rows_remote_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    common::ObTabletID &tablet_id,
    share::SCN &transfer_scn,
    ObArray<VirtualTabletMetaRow> &row_datas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "operation is not valid", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "lst operator ptr or sql proxy is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
                    "SELECT * FROM %s WHERE tenant_id=%lu and tablet_id=%lu and ls_id=%lu and transfer_scn=%lu",
                    OB_ALL_VIRTUAL_SS_EXISTING_TABLET_META_TNAME, tenant_id, tablet_id.id(),
                    ls_id.id(), transfer_scn.get_val_for_sql()))) {
      SERVER_LOG(WARN, "failed to assign sql", KR(ret), K(sql), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(result_, tenant_id, sql.ptr()))) {
      SERVER_LOG(WARN, "execute sql failed", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result_.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "get mysql result failed", KR(ret), K(sql));
    } else if (OB_FAIL(get_virtual_rows_remote_(*result_.get_result(), row_datas))) {
      SERVER_LOG(WARN, "generate virtual row remote failed", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualSSExistingTabletMeta::generate_virtual_rows_(ObArray<VirtualTabletMetaRow> &row_datas)
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(effective_tenant_id_) && effective_tenant_id_ != tenant_id_) {
    if (OB_FAIL(get_virtual_rows_remote_(tenant_id_, ls_id_, tablet_id_, transfer_scn_, row_datas))) {
      SERVER_LOG(WARN, "generate virtual row remote failed", KR(ret), K_(tenant_id), K_(ls_id));
    }
  } else {
    MTL_SWITCH(tenant_id_)
    {
      common::ObArenaAllocator allocator("TabletMetaVT",
                                        OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
      ObSSMetaService *meta_service = MTL(ObSSMetaService *);
      ObSSMetaReadParam param;
      ObTablet *tablet = nullptr;
      ObSSMetaIterGuard<ObSSTabletIterator> tablet_iter_guard;
      ObSSTabletIterator *tablet_iter = nullptr;
      const SCN start_scn = SCN::min_scn();
      SCN end_scn;
      if (OB_FAIL(meta_service->get_max_committed_meta_scn(ls_id_, end_scn))) {
        SERVER_LOG(WARN, "get max committed meta scn failed", K(ret));
      }
      ObMetaVersionRange range(start_scn, end_scn, false);
      param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                  ObSSMetaReadResultType::READ_WHOLE_ROW,
                                  false, /*try read local*/
                                  ObSSLogMetaType::SSLOG_TABLET_META,
                                  ls_id_,
                                  tablet_id_,
                                  transfer_scn_);
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(!param.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(meta_service->get_tablet(param, range, tablet_iter_guard))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_ITER_END;
        } else {
          SERVER_LOG(WARN, "get tablet from meta service failed", KR(ret), K(param));
        }
      } else if (OB_FAIL(tablet_iter_guard.get_iter(tablet_iter))) {
        SERVER_LOG(WARN, "get tablet_iter from iter_guard failed", KR(ret));
      } else {
        while (OB_SUCC(ret)) {
          ObTabletHandle tablet_handle;
          share::SCN row_scn;
          ObSSMetaUpdateMetaInfo update_meta_info;
          ObAtomicExtraInfo extra_info; // useless for now.
          if (OB_FAIL(tablet_iter->get_next(allocator, tablet_handle, row_scn, update_meta_info, extra_info))) {
            if (OB_UNLIKELY(OB_ITER_END != ret && OB_INVALID_QUERY_TIMESTAMP != ret)) {
              SERVER_LOG(WARN, "get next tablet from tablet_iter failed", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
            ret = OB_INVALID_ARGUMENT;
            SERVER_LOG(WARN, "tablet handle is invalid", KR(ret));
          } else {
            const ObTabletMeta &meta = tablet_handle.get_obj()->get_tablet_meta();
            VirtualTabletMetaRow row;
            ObMetaUpdateReason update_reason;
            share::SCN acquire_scn; // useless for now.
            int64_t sstable_op_id;
            update_meta_info.get(update_reason, acquire_scn, sstable_op_id);
            row.version_ = row_scn;
            row.data_tablet_id_ = meta.data_tablet_id_;
            row.create_scn_ = meta.create_scn_;
            row.start_scn_ = meta.start_scn_;
            row.create_schema_version_ = meta.create_schema_version_;
            row.data_checkpoint_scn_ = meta.clog_checkpoint_scn_;
            row.mds_checkpoint_scn_ = meta.mds_checkpoint_scn_;
            row.ddl_checkpoint_scn_ = meta.ddl_checkpoint_scn_;
            row.multi_version_start_ = meta.multi_version_start_;
            row.tablet_snapshot_version_ = meta.snapshot_version_;
            row.sstable_op_id_ = sstable_op_id;
            row.update_reason_ = update_reason;
            if (OB_FAIL(row_datas.push_back(row))) {
              SERVER_LOG(WARN, "failed to push back", K(row_datas), K(row));
            }
            SERVER_LOG(DEBUG, "generate row succeed", K(param), K(row));
          }

        }
      }
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
