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

#include "observer/virtual_table/ob_all_virtual_tx_lock_stat.h"
#include "observer/ob_server.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;

namespace observer
{
ObGVTxLockStat::ObGVTxLockStat()
    : ObVirtualTableScannerIterator(),
      ip_buffer_(),
      tx_id_buffer_(),
      proxy_session_id_buffer_(),
      memtable_key_buffer_(),
      ls_iter_guard_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      ls_(nullptr),
      ls_tx_ctx_iter_(),
      tx_lock_stat_iter_() {}
ObGVTxLockStat::~ObGVTxLockStat() { reset(); }

void ObGVTxLockStat::reset()
{
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

bool ObGVTxLockStat::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

void ObGVTxLockStat::release_last_tenant()
{
  ip_buffer_[0] = '\0';
  tx_id_buffer_[0] = '\0';
  proxy_session_id_buffer_[0] = '\0';
  memtable_key_buffer_[0] = '\0';

  ls_iter_guard_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_ = nullptr;
  ls_tx_ctx_iter_.reset();
  tx_lock_stat_iter_.reset();
  start_to_read_ = false;
}

int ObGVTxLockStat::get_next_ls_(ObLS *&ls)
{
  int ret = OB_SUCCESS;

  if (!ls_iter_guard_.get_ptr() || OB_FAIL(ls_iter_guard_->get_next(ls))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to switch tenant", K(ret));
    }
    // switch to next tenant
    ret = OB_ITER_END;
    SERVER_LOG(DEBUG, "finish iterate this tenant, switch to next tenant then", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "ls is null", K(ret));
  } else {
    ls_id_ = ls->get_ls_id().id();
    ls_tx_ctx_iter_.reset();
    tx_lock_stat_iter_.reset();
  }

  return ret;
}

int ObGVTxLockStat::get_next_tx_ctx_(transaction::ObPartTransCtx *&tx_ctx)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;

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
        retry_times++;
        SERVER_LOG(WARN, "ls_tx_ctx_iter_.get_next_tx_ctx failed", K(ret), K(retry_times));
        ret = OB_SUCCESS;  // continue loop
      }
      if (OB_ITER_END == ret || retry_times >= MAX_RETRY_TIMES){
        ls_tx_ctx_iter_.reset();
        SERVER_LOG(DEBUG, "iterate this ls finished, iterate next ls then", K(ret), K(ls_id_));
        if (OB_FAIL(get_next_ls_(ls_))) {
          SERVER_LOG(WARN, "get next ls failed", K(ret));
        }
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObGVTxLockStat::get_next_tx_lock_stat_iter_(transaction::ObTxLockStatIterator &tx_lock_stat_iter)
{
  int ret = OB_SUCCESS;
  transaction::ObPartTransCtx *tx_ctx = nullptr;

  if (OB_FAIL(get_next_tx_ctx_(tx_ctx))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next tx_ctx", K(ret));
    }
  } else if (OB_ISNULL(tx_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tx_ctx is null", K(ret), K(ls_id_));
  } else {
    tx_lock_stat_iter.reset();
    if (OB_FAIL(tx_ctx->iterate_tx_lock_stat(tx_lock_stat_iter))) {
      SERVER_LOG(WARN, "fail to get lock op iter", K(ret), K(ls_id_));
    } else if (OB_FAIL(tx_lock_stat_iter.set_ready())) {
      SERVER_LOG(WARN, "set lock_op_iter_ ready failed", K(ret), K(ls_id_));
    }
  }
  if (OB_NOT_NULL(tx_ctx)) {
    ls_tx_ctx_iter_.revert_tx_ctx(tx_ctx);
  }
  return ret;
}

int ObGVTxLockStat::get_next_tx_lock_stat_(ObTxLockStat &tx_lock_stat)
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if (OB_FAIL(tx_lock_stat_iter_.get_next(tx_lock_stat))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "get next tx_lock_stat failed", K(ret), K(ls_id_));
      }
      if (OB_FAIL(get_next_tx_lock_stat_iter_(tx_lock_stat_iter_))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "get next tx_lock_stat_iter failed", K(ret), K(ls_id_));
        }
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObGVTxLockStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

int ObGVTxLockStat::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (OB_ISNULL(ls_iter_guard_.get_ptr())
             && OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "init ls_iter_guard_ failed", K(ret));
  } else if (OB_FAIL(get_next_ls_(ls_))) {
    SERVER_LOG(WARN, "init ls_ failed", K(ret));
  } else if (OB_FAIL(get_next_tx_lock_stat_iter_(tx_lock_stat_iter_))) {
    SERVER_LOG(WARN, "init tx_lock_stat_iter_ failed", K(ret));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

int ObGVTxLockStat::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTxLockStat tx_lock_stat;

  if (!start_to_read_ && OB_FAIL(prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare_start_to_read_ error", K(ret), K(start_to_read_));
    ret = OB_ITER_END;  // to avoid throw error code to client
  } else if (OB_FAIL(get_next_tx_lock_stat_(tx_lock_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_lock_op failed", K(ret));
    }
    ret = OB_ITER_END;  // to avoid throw error code to client
  } else {
    const int64_t col_count = output_column_ids_.count();
    int ret = OB_SUCCESS;

    for (int64_t i = 0; i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
      case OB_APP_MIN_COLUMN_ID:
        // tenant_id
        cur_row_.cells_[i].set_int(tx_lock_stat.get_tenant_id());
        break;
      case OB_APP_MIN_COLUMN_ID + 1: {
        // trans_id
        // For compatibility, column type should be determined by schema before cluster is in upgrade mode.
        const ObColumnSchemaV2 *tmp_column_schema = nullptr;
        bool type_is_varchar = true;
        if (OB_ISNULL(table_schema_) ||
            OB_ISNULL(tmp_column_schema = table_schema_->get_column_schema(col_id))) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "table or column schema is null", KR(ret), KP(table_schema_), KP(tmp_column_schema));
        } else {
          type_is_varchar = tmp_column_schema->get_meta_type().is_character_type();
        }
        if (OB_SUCC(ret)) {
          // Before version 4.2, the type of trans_id in this table
          // is varchar, e.g. 'tx_id: 10000'. It's incompatible with
          // other virtual tables. So we change it to integer since 4.2.
          if (type_is_varchar) {
            (void)tx_lock_stat.get_tx_id().to_string(tx_id_buffer_,
                                                    OB_MIN_BUFFER_SIZE);
            cur_row_.cells_[i].set_varchar(tx_id_buffer_);
            cur_row_.cells_[i].set_collation_type(
                ObCharset::get_default_collation(
                    ObCharset::get_default_charset()));
          } else {
            cur_row_.cells_[i].set_int(tx_lock_stat.get_tx_id().get_id());
          }
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2:
        // svr_ip
        (void)tx_lock_stat.get_addr().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
        cur_row_.cells_[i].set_varchar(ip_buffer_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 3:
        // svr_port
        cur_row_.cells_[i].set_int(tx_lock_stat.get_addr().get_port());
        break;
      case OB_APP_MIN_COLUMN_ID + 4:
        // ls_id
        cur_row_.cells_[i].set_int(tx_lock_stat.get_ls_id().id());
        break;
      case OB_APP_MIN_COLUMN_ID + 5:
        // table_id
        cur_row_.cells_[i].set_int(0);
        break;
      case OB_APP_MIN_COLUMN_ID + 6:
        // table_id
        cur_row_.cells_[i].set_int(tx_lock_stat.get_memtable_key_info().get_tablet_id().id());
        break;
      case OB_APP_MIN_COLUMN_ID + 7:
        // rowkey
        snprintf(memtable_key_buffer_, OB_MEMTABLE_KEY_BUFFER_SIZE, "%s", tx_lock_stat.get_memtable_key_info().read_buf());
        if ('\0' == memtable_key_buffer_[0]) {
          // if rowkey is empty, we should set it as NULL
          cur_row_.cells_[i].reset();
        } else {
          cur_row_.cells_[i].set_varchar(memtable_key_buffer_);
        }
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 8:
        // session_id
        cur_row_.cells_[i].set_int(tx_lock_stat.get_session_id());
        break;
      case OB_APP_MIN_COLUMN_ID + 9:
        // proxy_session_id
        if (tx_lock_stat.get_proxy_session_id() > 0) {
          ObAddr client_server;
          //解析出来client server的信息
          (void)get_addr_by_proxy_sessid(tx_lock_stat.get_proxy_session_id(), client_server);
          if (client_server.is_valid()) {
            client_server.to_string(proxy_session_id_buffer_, OB_MIN_BUFFER_SIZE);
            cur_row_.cells_[i].set_varchar(proxy_session_id_buffer_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cur_row_.cells_[i].reset();
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
        } else {
          cur_row_.cells_[i].reset();
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 10:
        // tx_ctx_create_time
        cur_row_.cells_[i].set_timestamp(tx_lock_stat.get_tx_ctx_create_time());
        break;
      case OB_APP_MIN_COLUMN_ID + 11:
        // expired_time
        cur_row_.cells_[i].set_timestamp(tx_lock_stat.get_tx_expired_time());
        break;
      case OB_APP_MIN_COLUMN_ID + 12:
        cur_row_.cells_[i].set_int(ObTimeUtility::current_time() - tx_lock_stat.get_tx_ctx_create_time());
        break;
      case OB_APP_MIN_COLUMN_ID + 13:
        //row_lock_addr
        cur_row_.cells_[i].set_uint64(uint64_t(tx_lock_stat.get_memtable_key_info().get_row_lock()));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

}//observer
}//oceanbase
