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

#include "observer/virtual_table/ob_all_virtual_ss_ls_meta.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_shared_ls_meta.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualSSLSMeta::ObAllVirtualSSLSMeta()
    : ObVirtualTableScannerIterator(),
      tenant_id_(0),
      ls_id_()
{
}

ObAllVirtualSSLSMeta::~ObAllVirtualSSLSMeta()
{
  reset();
}

void ObAllVirtualSSLSMeta::reset()
{
  tenant_id_ = 0;
  ls_id_.reset();
}

int ObAllVirtualSSLSMeta::inner_get_next_row(common::ObNewRow *&row)
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
int ObAllVirtualSSLSMeta::get_primary_key_()
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

int ObAllVirtualSSLSMeta::handle_key_range_(ObNewRange &key_range)
{
  int ret = OB_SUCCESS;
  ObObj tenant_obj_low = (key_range.get_start_key().get_obj_ptr()[0]);
  ObObj tenant_obj_high = (key_range.get_end_key().get_obj_ptr()[0]);
  ObObj ls_obj_low = (key_range.get_start_key().get_obj_ptr()[1]);
  ObObj ls_obj_high = (key_range.get_end_key().get_obj_ptr()[1]);

  uint64_t tenant_low = tenant_obj_low.is_min_value() ? 0 : tenant_obj_low.get_uint64();
  uint64_t tenant_high = tenant_obj_high.is_max_value() ? UINT64_MAX : tenant_obj_high.get_uint64();
  ObLSID ls_low = ls_obj_low.is_min_value() ? ObLSID(0) : ObLSID(ls_obj_low.get_int());
  ObLSID ls_high = ls_obj_high.is_max_value() ? ObLSID(INT64_MAX) : ObLSID(ls_obj_high.get_int());

  if (tenant_low != tenant_high
      || ls_low != ls_high) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant id, ls id must be specified. range select is ");
    SERVER_LOG(WARN,
               "only support point select.",
               KR(ret),
               K(tenant_low),
               K(tenant_high),
               K(ls_low),
               K(ls_high));
  } else {
    tenant_id_ = tenant_low;
    ls_id_ = ls_low;
  }

  return ret;
}

int ObAllVirtualSSLSMeta::fill_in_row_(const VirtualSSLSMetaRow &row_data, common::ObNewRow *&row)
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
    case META_VERSION:
      cur_row_.cells_[i].set_int(row_data.version_.get_val_for_inner_table_field());
      break;
    case SS_CHECKPOINT_SCN: {
      int64_t v = row_data.ss_checkpoint_scn_.get_val_for_inner_table_field();
      cur_row_.cells_[i].set_int(v);
      break;
    }
    case SS_CHECKPOINT_LSN: {
      int64_t v = row_data.ss_checkpoint_lsn_.val_;
      cur_row_.cells_[i].set_int(v);
      break;
    }
    case SSLOG_CHECKPOINT_SCN: {
      int64_t v = row_data.sslog_checkpoint_scn_.get_val_for_inner_table_field();
      cur_row_.cells_[i].set_int(v);
      break;
    }
    case SS_CLOG_ACCUM_CHECKSUM:
      cur_row_.cells_[i].set_int(row_data.clog_checksum_);
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

int ObAllVirtualSSLSMeta::generate_virtual_row_(VirtualSSLSMetaRow &row)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id_)
  {
    SMART_VAR(ObSSLSMeta, ls_meta) {
      ObSSMetaService *meta_service = MTL(ObSSMetaService *);
      if (OB_UNLIKELY(!ls_id_.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(WARN, "invalid argument", K(ret), K(ls_id_));
      } else if (OB_FAIL(meta_service->get_ls_meta(ls_id_, ls_meta))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_ITER_END;
        } else {
          SERVER_LOG(WARN, "get ls meta from meta service failed", KR(ret), K(ls_id_));
        }
      } else {
        row.version_ = SCN::min_scn();   // TODO: use the real version
        row.ss_checkpoint_scn_ = ls_meta.get_ss_checkpoint_scn();
        row.ss_checkpoint_lsn_ = ls_meta.get_ss_base_lsn();
        palf::PalfBaseInfo palf_meta;
        (void)ls_meta.get_palf_meta(palf_meta);
        row.clog_checksum_ = palf_meta.prev_log_info_.accum_checksum_;
        row.sslog_checkpoint_scn_ = ls_meta.get_sslog_checkpoint_scn();
        SERVER_LOG(DEBUG, "generate row succeed", K(ls_meta), K(row));
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
