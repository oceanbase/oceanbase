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

#include "observer/virtual_table/ob_all_virtual_checkpoint.h"
#include "storage/checkpoint/ob_checkpoint_executor.h"
#include "logservice/ob_log_base_type.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::logservice;
using namespace oceanbase::storage::checkpoint;
namespace oceanbase
{
namespace observer
{

ObAllVirtualCheckpointInfo::ObAllVirtualCheckpointInfo()
    : ObVirtualTableScannerIterator(),
      addr_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      ls_iter_guard_()
{
}

ObAllVirtualCheckpointInfo::~ObAllVirtualCheckpointInfo()
{
  reset();
}

void ObAllVirtualCheckpointInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualCheckpointInfo::get_next_ls_(ObLS *&ls)
{
  int ret = OB_SUCCESS;

  if (ls_iter_guard_.get_ptr() == nullptr
      && OB_FAIL(MTL(ObLSService*)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "get_ls_iter fail", K(ret));
  } else if (OB_FAIL(ls_iter_guard_->get_next(ls))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_ls failed", K(ret));
    }
  } else {
    ls_id_ = ls->get_ls_id().id();
  }

  return ret;
}

int ObAllVirtualCheckpointInfo::prepare_to_read_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObArray<ObCheckpointVTInfo> infos;
  ob_checkpoint_iter_.reset();
  if (OB_FAIL(get_next_ls_(ls))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_ls failed", K(ret));
    }
  } else if (NULL == ls) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ls shouldn't NULL here", K(ret), K(ls));
  } else if (FALSE_IT(infos.reset())) {
  } else if (OB_FAIL(ls->get_checkpoint_info(infos))) {
    SERVER_LOG(WARN, "get checkpoint info failed", K(ret), KPC(ls));
  } else {
    int64_t idx = 0;
    for (; idx < infos.count() && OB_SUCC(ret); ++idx) {
      if (OB_FAIL(ob_checkpoint_iter_.push(infos.at(idx)))) {
        SERVER_LOG(ERROR, "ob_checkpoint_iter push failed", K(ret), KPC(ls));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(ob_checkpoint_iter_.set_ready())) {
    SERVER_LOG(WARN, "iterate freeze_checkpoint info begin error", K(ret));
  }

  if (OB_FAIL(ret)) {
    ob_checkpoint_iter_.reset();
  }

  return ret;
}

int ObAllVirtualCheckpointInfo::get_next_(ObCheckpointVTInfo &checkpoint)
{
  int ret = OB_SUCCESS;
  // ensure inner_get_next_row can get new data
  bool need_retry = true;
  while (need_retry) {
    if (!ob_checkpoint_iter_.is_ready() && OB_FAIL(prepare_to_read_())) {
      if (OB_ITER_END == ret) {
        SERVER_LOG(DEBUG, "iterate checkpoint info iter end", K(ret));
      } else {
        SERVER_LOG(WARN, "prepare data failed", K(ret));
      }
    } else if (OB_FAIL(ob_checkpoint_iter_.get_next(checkpoint))) {
      if (OB_ITER_END == ret) {
        ob_checkpoint_iter_.reset();
        SERVER_LOG(DEBUG, "iterate checkpoint info iter in the ls end",
                                                      K(ret), K(ls_id_));
        continue;
      } else {
        SERVER_LOG(WARN, "get next checkpoint info error.", K(ret));
      }
    }
    need_retry = false;
  }
  return ret;
}

bool ObAllVirtualCheckpointInfo::is_need_process(uint64_t tenant_id)
{
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    return true;
  }
  return false;
}

void ObAllVirtualCheckpointInfo::release_last_tenant()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_iter_guard_.reset();
  ob_checkpoint_iter_.reset();
}

int ObAllVirtualCheckpointInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

int ObAllVirtualCheckpointInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObCheckpointVTInfo checkpoint;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (OB_FAIL(get_next_(checkpoint))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_checkpoint failed", K(ret));
    }
  } else {
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
        case OB_APP_MIN_COLUMN_ID + 4: {
          if (OB_FAIL(log_base_type_to_string(ObLogBaseType(checkpoint.service_type),
                                              service_type_buf_,
                                              sizeof(service_type_buf_)))) {
            SERVER_LOG(WARN, "get service type buf failed", K(ret), K(checkpoint));
          } else {
            service_type_buf_[MAX_SERVICE_TYPE_BUF_LENGTH - 1] = '\0';
            cur_row_.cells_[i].set_varchar(service_type_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 5: {
          //TODO:SCN
          cur_row_.cells_[i].set_uint64(checkpoint.rec_scn.is_valid() ? checkpoint.rec_scn.get_val_for_inner_table_field() : 0);
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

} // observer
} // oceanbase
