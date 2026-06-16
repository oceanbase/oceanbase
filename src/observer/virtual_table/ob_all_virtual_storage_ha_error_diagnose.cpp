/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_all_virtual_storage_ha_error_diagnose.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace share;
namespace observer
{
ObAllVirtualStorageHAErrorDiagnose::ObAllVirtualStorageHAErrorDiagnose()
    : inflight_iter_()
{
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(info_str_, 0, sizeof(info_str_));
  memset(task_id_, 0, sizeof(task_id_));
}

ObAllVirtualStorageHAErrorDiagnose::~ObAllVirtualStorageHAErrorDiagnose()
{
  reset();
}

int ObAllVirtualStorageHAErrorDiagnose::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute", K(ret));
  }
  return ret;
}

void ObAllVirtualStorageHAErrorDiagnose::release_last_tenant()
{
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(info_str_, 0, sizeof(info_str_));
  memset(task_id_, 0, sizeof(task_id_));
  inflight_iter_.reset();
}

bool ObAllVirtualStorageHAErrorDiagnose::is_need_process(uint64_t tenant_id)
{
  bool need_process = false;
  if (!is_virtual_tenant_id(tenant_id)
      && (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    need_process = true;
  }
  return need_process;
}

int ObAllVirtualStorageHAErrorDiagnose::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (!inflight_iter_.is_opened() && OB_FAIL(inflight_iter_.open())) {
    SERVER_LOG(WARN, "failed to open inflight iter", K(ret));
  } else {
    ObLSID ls_id;
    ObHAInflightDiagState state;
    // Skip inflight entries that haven't recorded an error — this virtual table
    // is the error view, and should stay aligned with the history table's
    // semantics (only non-SUCCESS rows are written there).
    while (OB_SUCC(ret)) {
      if (OB_FAIL(inflight_iter_.next(ls_id, state))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "failed to fetch next inflight key", K(ret));
        }
      } else if (OB_SUCCESS == state.last_err_code_) {
        // keep iterating
      } else {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      ObObj *cells = cur_row_.cells_;
      const int64_t col_count = output_column_ids_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
        case TENANT_ID:
          cells[i].set_int(MTL_ID());
          break;
        case LS_ID:
          cells[i].set_int(ls_id.id());
          break;
        case MODULE: {
          cells[i].set_varchar(ObStorageDiagModuleStr[
              static_cast<int64_t>(ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE)]);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TYPE: {
          cells[i].set_varchar(ha_diag_task_type_str(state.cur_phase_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TASK_ID: {
          const int n = snprintf(task_id_, sizeof(task_id_), "%ld", state.task_id_.id());
          if (n < 0 || n >= static_cast<int>(sizeof(task_id_))) {
            ret = OB_BUF_NOT_ENOUGH;
            SERVER_LOG(WARN, "failed to render task id", K(ret), K(n));
          } else {
            cells[i].set_varchar(task_id_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case SVR_IP:
          if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case SVR_PORT:
          cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
          break;
        case RETRY_ID:
          cells[i].set_int(state.retry_count_);
          break;
        case CREATE_TIME:
          cells[i].set_timestamp(state.start_ts_);
          break;
        case RESULT_CODE:
          cells[i].set_int(state.last_err_code_);
          break;
        case RESULT_MSG: {
          const int64_t msg_idx = static_cast<int64_t>(state.last_result_msg_);
          const char *msg = (msg_idx >= 0 && msg_idx < static_cast<int64_t>(ObStorageHACostItemName::MAX_NAME))
              ? ObTransferErrorDiagMsg[msg_idx]
              : "Unstatistical errors";
          cells[i].set_varchar(msg);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case INFO: {
          if (OB_FAIL(ObHAInflightDiag::serialize_info(state, info_str_, sizeof(info_str_)))) {
            SERVER_LOG(WARN, "failed to serialize inflight info", K(ret));
          } else {
            cells[i].set_lob_value(ObLongTextType, info_str_, strlen(info_str_) + 1);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
        }
      }
      if (OB_SUCC(ret)) {
        row = &cur_row_;
      }
    }
  }
  return ret;
}

void ObAllVirtualStorageHAErrorDiagnose::reset()
{
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(info_str_, 0, sizeof(info_str_));
  memset(task_id_, 0, sizeof(task_id_));
  inflight_iter_.reset();
}


} /* namespace observer */
} /* namespace oceanbase */
