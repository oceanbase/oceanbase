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

#include "ob_all_virtual_storage_ha_error_diagnose.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace share;
namespace observer
{
ObAllVirtualStorageHAErrorDiagnose::ObAllVirtualStorageHAErrorDiagnose()
    : info_(nullptr),
      iter_()
{
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(info_str_, 0, sizeof(info_str_));
  memset(task_id_, 0, sizeof(task_id_));
}

ObAllVirtualStorageHAErrorDiagnose::~ObAllVirtualStorageHAErrorDiagnose()
{
  reset();
}

int ObAllVirtualStorageHAErrorDiagnose::get_info_from_type_(ObStorageHADiagInfo *&info, ObTransferErrorDiagInfo &transfer_err_diag)
{
  int ret = OB_SUCCESS;
  info = nullptr;
  ObStorageHADiagTaskKey key;
  if (OB_FAIL(iter_.get_cur_key(key))) {
    if (ret != OB_ITER_END) {
      SERVER_LOG(WARN, "failed to get cur key", K(ret), K(iter_));
    }
  } else {
    switch(key.module_) {
      case ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE: {
        info = &transfer_err_diag;
        break;
      }
      case ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE:
        ret = OB_NOT_SUPPORTED;
        SERVER_LOG(WARN, "not supported perf diagnose", K(ret), K(key.module_));
        break;
      default: {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(WARN, "invalid module", K(ret), K(key.module_));
        break;
      }
    }
  }

  return ret;
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
  info_ = nullptr;
  iter_.reset();
}

bool ObAllVirtualStorageHAErrorDiagnose::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id)
      && (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

int ObAllVirtualStorageHAErrorDiagnose::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  ObTransferErrorDiagInfo transfer_err_diag;
  if (!iter_.is_opened() && OB_FAIL(iter_.open(ObStorageHADiagType::ERROR_DIAGNOSE))) {
    SERVER_LOG(WARN, "Fail to open storage ha diagnose iter", K(ret), K(iter_));
  } else if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(get_info_from_type_(info_, transfer_err_diag))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get info");
    }
  } else if (OB_FAIL(iter_.get_next_info(*info_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next diagnose info", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
      case TENANT_ID:
        cells[i].set_int(MTL_ID());
        break;
      case LS_ID:
        cells[i].set_int(info_->ls_id_.id());
        break;
      case MODULE: {
        cells[i].set_varchar(info_->get_module_str());
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case TYPE: {
        cells[i].set_varchar(info_->get_type_str());
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case TASK_ID: {
        if (OB_FAIL(info_->get_task_id(task_id_, sizeof(task_id_)))) {
          SERVER_LOG(WARN, "failed to get task id", K(ret));
        }
        cells[i].set_varchar(task_id_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SVR_IP:
        //svr_ip
        if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
        }
        break;
      case SVR_PORT:
        //svr_port
        cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
        break;
      case RETRY_ID:
        cells[i].set_int(info_->retry_id_);
        break;
      case CREATE_TIME:
        cells[i].set_timestamp(info_->timestamp_);
        break;
      case RESULT_CODE:
        cells[i].set_int(info_->result_code_);
        break;
      case RESULT_MSG: {
        cells[i].set_varchar(info_->get_transfer_error_diagnose_msg());
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case INFO: {
        if (OB_FAIL(info_->get_info(info_str_, sizeof(info_str_)))) {
          SERVER_LOG(WARN, "failed to get info str", K(ret));
        }
        cells[i].set_lob_value(ObLongTextType, info_str_, strlen(info_str_) + 1);
        cells[i].set_collation_type(ObCharset::get_default_collation(
                                      ObCharset::get_default_charset()));
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
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
  info_ = nullptr;
  iter_.reset();
}


} /* namespace observer */
} /* namespace oceanbase */
