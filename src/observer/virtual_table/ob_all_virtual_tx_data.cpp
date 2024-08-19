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
#include "ob_all_virtual_tx_data.h"

#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_multi_tenant.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase {
namespace observer {

using namespace share;
using namespace storage;
using namespace transaction;
using namespace omt;

int ObAllVirtualTxData::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (false == start_to_read_) {
    if (OB_FAIL(get_primary_key_())) {
      SERVER_LOG(WARN, "get primary key failed", KR(ret));
    } else if (OB_FAIL(generate_virtual_tx_data_row_(tx_data_row_))) {
      if (OB_ITER_END == ret) {
      } else if (OB_TRANS_CTX_NOT_EXIST == ret) {
        ret = OB_ITER_END;
      } else {
        SERVER_LOG(WARN, "generate virtual tx data row failed", KR(ret));
      }
    } else if (OB_FAIL(fill_in_row_(tx_data_row_, row))) {
      SERVER_LOG(WARN, "fill in row failed", KR(ret));
    } else {
      start_to_read_ = true;
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAllVirtualTxData::fill_in_row_(const VirtualTxDataRow &row_data, common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TENANT_ID_COL:
        cur_row_.cells_[i].set_int(tenant_id_);
        break;
      case LS_ID_COL:
        cur_row_.cells_[i].set_int(ls_id_.id());
        break;
      case TX_ID_COL:
        cur_row_.cells_[i].set_int(tx_id_.get_id());
        break;
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
      case STATE_COL:
        cur_row_.cells_[i].set_varchar(ObTxCommitData::get_state_string(row_data.state_));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case START_SCN_COL: {
        uint64_t v = row_data.start_scn_.get_val_for_inner_table_field();
        cur_row_.cells_[i].set_uint64(v);
        break;
      }
      case END_SCN_COL: {
        uint64_t v = row_data.end_scn_.get_val_for_inner_table_field();
        cur_row_.cells_[i].set_uint64(v);
        break;
      }
      case COMMIT_VERSION_COL: {
        uint64_t v = row_data.commit_version_.get_val_for_inner_table_field();
        cur_row_.cells_[i].set_uint64(v);
        break;
      }
      case UNDO_STATUS_COL:
        cur_row_.cells_[i].set_varchar(row_data.undo_status_list_str_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case TX_OP_COL:
        cur_row_.cells_[i].set_varchar(row_data.tx_op_str_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
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

int ObAllVirtualTxData::get_primary_key_()
{
  int ret = OB_SUCCESS;
  if (key_ranges_.count() != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "only support select a single tx data once, multiple range select is ");
    SERVER_LOG(WARN, "invalid key ranges", KR(ret));
  } else {
    ObNewRange &key_range = key_ranges_.at(0);
    if (OB_UNLIKELY(key_range.get_start_key().get_obj_cnt() != 3 || key_range.get_end_key().get_obj_cnt() != 3)) {
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

int ObAllVirtualTxData::handle_key_range_(ObNewRange &key_range)
{
  int ret = OB_SUCCESS;
  ObObj tenant_obj_low = (key_range.get_start_key().get_obj_ptr()[0]);
  ObObj tenant_obj_high = (key_range.get_end_key().get_obj_ptr()[0]);
  ObObj ls_obj_low = (key_range.get_start_key().get_obj_ptr()[1]);
  ObObj ls_obj_high = (key_range.get_end_key().get_obj_ptr()[1]);
  ObObj tx_id_obj_low = (key_range.get_start_key().get_obj_ptr()[2]);
  ObObj tx_id_obj_high = (key_range.get_end_key().get_obj_ptr()[2]);

  uint64_t tenant_low = tenant_obj_low.is_min_value() ? 0 : tenant_obj_low.get_uint64();
  uint64_t tenant_high = tenant_obj_high.is_max_value() ? UINT64_MAX : tenant_obj_high.get_uint64();
  ObLSID ls_low = ls_obj_low.is_min_value() ? ObLSID(0) : ObLSID(ls_obj_low.get_int());
  ObLSID ls_high = ls_obj_high.is_max_value() ? ObLSID(INT64_MAX) : ObLSID(ls_obj_high.get_int());
  ObTransID tx_id_low = tx_id_obj_low.is_min_value() ? ObTransID(0) : ObTransID(tx_id_obj_low.get_int());
  ObTransID tx_id_high = tx_id_obj_high.is_min_value() ? ObTransID(INT64_MAX) : ObTransID(tx_id_obj_high.get_int());

  if (tenant_low != tenant_high || ls_low != ls_high || tx_id_low != tx_id_high) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant name, ls id and trans id must be specified. range select is ");
    SERVER_LOG(WARN,
               "only support point select.",
               KR(ret),
               K(tenant_low),
               K(tenant_high),
               K(ls_low),
               K(ls_high),
               K(tx_id_low),
               K(tx_id_high));
  } else {
    tenant_id_ = tenant_low;
    ls_id_ = ls_low;
    tx_id_ = tx_id_low;
  }

  return ret;
}

int ObAllVirtualTxData::generate_virtual_tx_data_row_(VirtualTxDataRow &tx_data_row)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id_)
  {
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    ObLSService *ls_service = MTL(ObLSService *);
    if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      if (OB_LS_NOT_EXIST == ret) {
        ret = OB_ITER_END;
      } else {
        SERVER_LOG(WARN, "get ls from ls service failed", KR(ret), K(ls_id_));
      }
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "get ls failed from ls handle", KR(ret), K(ls_handle), K(tenant_id_), K(ls_id_));
    } else if (OB_FAIL(ls->generate_virtual_tx_data_row(tx_id_, tx_data_row))) {
      SERVER_LOG(WARN, "ls genenrate virtual tx data row failed", KR(ret), K(ls_handle), K(tenant_id_), K(ls_id_));
    } else {
      SERVER_LOG(DEBUG, "generate tx data row succeed", KPC(ls), K(tx_data_row));
    }
  }

  if (OB_FAIL(ret) && OB_TENANT_NOT_IN_SERVER == ret) {
    ret = OB_ITER_END;
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
