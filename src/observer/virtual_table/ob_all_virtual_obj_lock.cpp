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

#include "observer/virtual_table/ob_all_virtual_obj_lock.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_part_ctx.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualObjLock::ObAllVirtualObjLock()
    : ObVirtualTableScannerIterator(),
      addr_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      ls_(nullptr),
      ls_iter_guard_(),
      ls_tx_ctx_iter_(),
      obj_lock_iter_(),
      lock_op_iter_(),
      is_iter_tx_(true)
{
}

ObAllVirtualObjLock::~ObAllVirtualObjLock()
{
  reset();
}

void ObAllVirtualObjLock::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualObjLock::release_last_tenant()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_ = nullptr;
  is_iter_tx_ = true;
  ls_iter_guard_.reset();
  ls_tx_ctx_iter_.reset();
  obj_lock_iter_.reset();
  lock_op_iter_.reset();
  start_to_read_ = false;
}

int ObAllVirtualObjLock::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualObjLock::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

int ObAllVirtualObjLock::get_next_ls()
{
  int ret = OB_SUCCESS;

  if (!ls_iter_guard_.get_ptr() || OB_FAIL(ls_iter_guard_->get_next(ls_))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to switch tenant", K(ret));
    }
    // switch to next tenant
    ret = OB_ITER_END;
    SERVER_LOG(DEBUG, "finish iterate this tenant, switch to next tenant then", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "ls is null", K(ret));
  } else {
    ls_id_ = ls_->get_ls_id().id();
    is_iter_tx_ = true;  // iterate tx firstly, then iterate lock_memtable
    ls_tx_ctx_iter_.reset();
    obj_lock_iter_.reset();
    lock_op_iter_.reset();
  }

  return ret;
}

int ObAllVirtualObjLock::get_next_tx_ctx(transaction::ObPartTransCtx *&tx_ctx)
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if (!ls_tx_ctx_iter_.is_ready()) {
      if (OB_ISNULL(ls_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "ls is null", K(ret), K(ls_id_));
      } else if (OB_FAIL(ls_->iterate_tx_ctx(ls_tx_ctx_iter_))) {
        SERVER_LOG(WARN, "fail to get ls_tx_ctx_iter", K(ret), K(ls_id_));
      }
    } else if (OB_FAIL(ls_tx_ctx_iter_.get_next_tx_ctx(tx_ctx))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "ls_tx_ctx_iter_.get_next_tx_ctx failed", K(ret));
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObAllVirtualObjLock::get_next_lock_id(ObLockID &lock_id)
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if (!obj_lock_iter_.is_ready()) {
      if (OB_ISNULL(ls_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "ls is null", K(ret), K(ls_id_));
      } else if (OB_FAIL(ls_->get_lock_id_iter(obj_lock_iter_))) {
        SERVER_LOG(WARN, "fail to get obj_lock_iter", K(ret), K(ls_id_));
      }
    } else if (OB_FAIL(obj_lock_iter_.get_next(lock_id))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next lock_id", K(ret), K(ls_id_));
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObAllVirtualObjLock::get_next_lock_op(transaction::tablelock::ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;

  // loop until get lock_op
  while (OB_SUCC(ret)) {
    if (OB_FAIL(lock_op_iter_.get_next(lock_op))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next lock op", K(ret), K(is_iter_tx_), K(ls_id_));
      }
      lock_op_iter_.reset();  // clean lock_op_iter to save memory
      if (OB_FAIL(get_next_lock_op_iter())) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next lock_op_iter", K(ret), K(is_iter_tx_), K(ls_id_));
        }
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObAllVirtualObjLock::get_next_lock_op_iter()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t retry_times = 0;  // use it to avoid unexpected error code, and try to iterate next one

  // loop until get valid lock_op_iter, or has iterated all ls
  while (OB_SUCC(ret)) {
    if (is_iter_tx_) {
      if (OB_TMP_FAIL(get_next_lock_op_iter_from_tx_ctx())) {
        if (OB_ITER_END != tmp_ret) {
          retry_times++;
          SERVER_LOG(WARN, "get next lock_op_iter from tx_ctx failed", K(tmp_ret), K(retry_times), K(ls_id_));
        }
        if (OB_ITER_END == tmp_ret || retry_times >= MAX_RETRY_TIMES) {
          is_iter_tx_ = false;
          ls_tx_ctx_iter_.reset();  // clean lx_tx_ctx_iter to save memory
          SERVER_LOG(DEBUG, "iterate tx finish, iterate lock_memtable then", K(tmp_ret), K(ls_id_));
        }
      }
    } else {
      if (OB_TMP_FAIL(get_next_lock_op_iter_from_lock_memtable())) {
        if (OB_ITER_END != tmp_ret) {
          retry_times++;
          SERVER_LOG(WARN, "get next lock_op_iter from lock_memtable failed", K(tmp_ret), K(retry_times), K(ls_id_));
        }
        if (OB_ITER_END == tmp_ret || retry_times >= MAX_RETRY_TIMES) {
          if (OB_FAIL(get_next_ls())) {
            // has iterated all ls
            if (OB_ITER_END != ret) {
              SERVER_LOG(WARN, "get next ls failed", K(ret));
            }
          }
        }
      }
    }
    // get valid lock_op_iter
    if (OB_SUCCESS == tmp_ret) {
      break;
    }
  }

  return ret;
}

int ObAllVirtualObjLock::get_next_lock_op_iter_from_tx_ctx()
{
  int ret = OB_SUCCESS;
  transaction::ObPartTransCtx *tx_ctx = nullptr;

  if (OB_FAIL(get_next_tx_ctx(tx_ctx))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next tx_ctx", K(ret));
    }
  } else if (OB_ISNULL(tx_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tx_ctx is null", K(ret), K(ls_id_));
  } else {
    lock_op_iter_.reset();
    if (OB_FAIL(tx_ctx->iterate_tx_obj_lock_op(lock_op_iter_))) {
      SERVER_LOG(WARN, "fail to get lock op iter", K(ret), K(ls_id_));
    } else if (OB_FAIL(lock_op_iter_.set_ready())) {
      SERVER_LOG(WARN, "set lock_op_iter ready failed", K(ret), K(ls_id_));
    }
  }
  if (OB_NOT_NULL(tx_ctx)) {
    ls_tx_ctx_iter_.revert_tx_ctx(tx_ctx);
  }
  return ret;
}

int ObAllVirtualObjLock::get_next_lock_op_iter_from_lock_memtable()
{
  int ret = OB_SUCCESS;
  ObLockID lock_id;

  if (OB_FAIL(get_next_lock_id(lock_id))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next lock_id", K(ret), K(ls_id_));
    }
  } else {
    lock_op_iter_.reset();
    if (OB_FAIL(ls_->get_lock_op_iter(lock_id, lock_op_iter_))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        SERVER_LOG(WARN, "fail to get lock op iter, try to get next lock_id", K(ret), K(lock_id));
        ret = OB_SUCCESS;  // continue
      }
      SERVER_LOG(WARN, "fail to get lock op iter", K(ret), K(lock_id));
    }
  }
  return ret;
}

int ObAllVirtualObjLock::prepare_start_to_read()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (OB_ISNULL(ls_iter_guard_.get_ptr())
             && OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "init ls_iter_guard_ failed", K(ret));
  } else if (OB_FAIL(get_next_ls())) {
    SERVER_LOG(WARN, "init ls_ failed", K(ret));
  } else if (OB_FAIL(get_next_lock_op_iter())) {
    SERVER_LOG(WARN, "init lock_op_iter_ failed", K(ret), K(ls_id_));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualObjLock::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  transaction::tablelock::ObTableLockOp lock_op;
  if (!start_to_read_ && OB_FAIL(prepare_start_to_read())) {
    SERVER_LOG(WARN, "prepare start to read failed", K(ret));
    ret = OB_ITER_END;  // to avoid throw error code to client
  } else if (OB_FAIL(get_next_lock_op(lock_op))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_lock_op failed", K(ret));
    }
    ret = OB_ITER_END;  // to avoid throw error code to client
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP: {
          // svr_ip
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        }
        case SVR_PORT:
          // svr_port
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case TENANT_ID:
          // tenant_id
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        case LS_ID:
          // ls_id
          cur_row_.cells_[i].set_int(ls_id_);
          break;
        case LOCK_ID: {
          lock_op.lock_id_.to_string(lock_id_buf_, sizeof(lock_id_buf_));
          lock_id_buf_[MAX_LOCK_ID_BUF_LENGTH - 1] = '\0';
          cur_row_.cells_[i].set_varchar(lock_id_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case LOCK_MODE: {
          if (OB_FAIL(lock_mode_to_string(lock_op.lock_mode_,
                                          lock_mode_buf_,
                                          sizeof(lock_mode_buf_)))) {
            SERVER_LOG(WARN, "get lock mode buf failed", K(ret), K(lock_op));
          } else {
            lock_mode_buf_[MAX_LOCK_MODE_BUF_LENGTH - 1] = '\0';
            cur_row_.cells_[i].set_varchar(lock_mode_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OWNER_ID:
          cur_row_.cells_[i].set_int(lock_op.owner_id_.raw_value());
          break;
        case CREATE_TRANS_ID:
          cur_row_.cells_[i].set_int(lock_op.create_trans_id_.get_id());
          break;
        case OP_TYPE: {
          if (OB_FAIL(lock_op_type_to_string(lock_op.op_type_,
                                             lock_op_type_buf_,
                                             sizeof(lock_op_type_buf_)))) {
            SERVER_LOG(WARN, "get lock op type buf failed", K(ret), K(lock_op));
          } else {
            lock_op_type_buf_[MAX_LOCK_OP_TYPE_BUF_LENGTH - 1] = '\0';
            cur_row_.cells_[i].set_varchar(lock_op_type_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OP_STATUS: {
          if (OB_FAIL(lock_op_status_to_string(lock_op.lock_op_status_,
                                               lock_op_status_buf_,
                                               sizeof(lock_op_status_buf_)))) {
            SERVER_LOG(WARN, "get lock op status buf failed", K(ret), K(lock_op));
          } else {
            lock_op_status_buf_[MAX_LOCK_OP_STATUS_BUF_LENGTH - 1] = '\0';
            cur_row_.cells_[i].set_varchar(lock_op_status_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case TRANS_VERSION: {
          cur_row_.cells_[i].set_uint64(lock_op.commit_version_.get_val_for_inner_table_field());
          break;
        }
        case CREATE_TIMESTAMP: {
          cur_row_.cells_[i].set_int(lock_op.create_timestamp_);
          break;
        }
        case CREATE_SCHEMA_VERSION: {
          cur_row_.cells_[i].set_int(lock_op.create_schema_version_);
          break;
        }
        case EXTRA_INFO:
          snprintf(lock_op_extra_info_, sizeof(lock_op_extra_info_),
                   "count:%ld, position:%s",
                   ((lock_op.op_type_ == IN_TRANS_DML_LOCK && !is_iter_tx_) ? lock_op.lock_seq_no_.cast_to_int() : 0),
                   is_iter_tx_ ? "tx_ctx" : "lock_table");
          lock_op_extra_info_[MAX_LOCK_OP_EXTRA_INFO_LENGTH - 1] = '\0';
          cur_row_.cells_[i].set_varchar(lock_op_extra_info_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case TIME_AFTER_CREATE: {
          cur_row_.cells_[i].set_int(ObTimeUtility::current_time() - lock_op.create_timestamp_);
          break;
        }
        case OBJ_TYPE: {
          if (OB_FAIL(lock_obj_type_to_string(lock_op.lock_id_.obj_type_,
                                              lock_obj_type_buf_,
                                              sizeof(lock_obj_type_buf_)))) {
            SERVER_LOG(WARN, "get lock obj type buf failed", K(ret), K(lock_op));
          } else {
            lock_obj_type_buf_[MAX_LOCK_OBJ_TYPE_BUF_LENGTH - 1] = '\0';
            cur_row_.cells_[i].set_varchar(lock_obj_type_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OBJ_ID: {
          cur_row_.cells_[i].set_int(lock_op.lock_id_.obj_id_);
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

}  // namespace observer
}  // namespace oceanbase
