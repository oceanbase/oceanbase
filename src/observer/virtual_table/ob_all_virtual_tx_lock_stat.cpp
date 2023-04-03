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

namespace oceanbase
{
using namespace common;
using namespace transaction;

namespace observer
{
void ObGVTxLockStat::reset()
{
  ip_buffer_[0] = '\0';
  tx_id_buffer_[0] = '\0';
  proxy_session_id_buffer_[0] = '\0';
  memtable_key_buffer_[0] = '\0';
  ObVirtualTableScannerIterator::reset();
}

void ObGVTxLockStat::destroy()
{
  txs_ = NULL;
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(tx_id_buffer_, 0, OB_MIN_BUFFER_SIZE);
  memset(proxy_session_id_buffer_, 0, OB_MIN_BUFFER_SIZE);
  memset(memtable_key_buffer_, 0, OB_MEMTABLE_KEY_BUFFER_SIZE);

  ObVirtualTableScannerIterator::reset();
}

int ObGVTxLockStat::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_ || NULL == txs_) {
    SERVER_LOG(WARN, "invalid argument, allocator_ or txs_ is null", "allocator",
               OB_P(allocator_), "txs_", OB_P(txs_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = txs_->iterate_ls_id(ls_id_iter_))) {
    TRANS_LOG(WARN, "iterate ls id error", K(ret));
  } else if (!ls_id_iter_.is_ready()) {
    TRANS_LOG(WARN, "ls_id_iter is not ready");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_SUCCESS != (ret = tx_lock_stat_iter_.set_ready())) {
    TRANS_LOG(WARN, "tx_lock_stat_iter_ set ready error", K(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVTxLockStat::get_next_tx_lock_stat_(ObTxLockStat &tx_lock_stat)
{
  int ret = OB_SUCCESS;
  ObTxLockStat cur_tx_lock_stat;
  share::ObLSID ls_id;
  bool next_ls = true;

  while(next_ls && OB_SUCCESS == ret) {
    if (OB_ITER_END == (ret = tx_lock_stat_iter_.get_next(cur_tx_lock_stat))) {
      if (OB_FAIL(ls_id_iter_.get_next(ls_id))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "ls_id_iter get next ls id error", K(ret));
        }
      } else {
        tx_lock_stat_iter_.reset();
        if (OB_SUCCESS != (ret = txs_->iterate_tx_lock_stat(ls_id, tx_lock_stat_iter_))) {
          TRANS_LOG(WARN, "iterate_tx_lock_stat error", K(ret), K(ls_id));
        } else {
          // do nothing
        }
      }
    } else {
      next_ls = false;
    }
  }

  if (OB_SUCC(ret)) {
    tx_lock_stat = cur_tx_lock_stat;
  }
  return ret;
}

int ObGVTxLockStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTxLockStat tx_lock_stat;

  if (!start_to_read_ && OB_FAIL(prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare_start_to_read_ error", K(ret), K(start_to_read_));
  } else if (OB_FAIL(get_next_tx_lock_stat_(tx_lock_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "ObGVTxLockStat iter error", K(ret));
    } else {
      // do nothing
    }
  } else {
    ret = output_row_(tx_lock_stat, row);
  }
  return ret;
}

int ObGVTxLockStat::output_row_(const ObTxLockStat& tx_lock_stat, ObNewRow *&row) {
  const int64_t col_count = output_column_ids_.count();
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < col_count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch(col_id) {
    case OB_APP_MIN_COLUMN_ID:
      // tenant_id
      cur_row_.cells_[i].set_int(tx_lock_stat.get_tenant_id());
      break;
    case OB_APP_MIN_COLUMN_ID + 1:
      // trans_id
      (void)tx_lock_stat.get_tx_id().to_string(tx_id_buffer_, OB_MIN_BUFFER_SIZE);
      cur_row_.cells_[i].set_varchar(tx_id_buffer_);
      cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
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
      //(void)tx_lock_stat.get_memtable_key_info().to_string(memtable_key_buffer_, OB_MEMTABLE_KEY_BUFFER_SIZE);
      snprintf(memtable_key_buffer_, OB_MEMTABLE_KEY_BUFFER_SIZE, "%s", tx_lock_stat.get_memtable_key_info().read_buf());
      cur_row_.cells_[i].set_varchar(memtable_key_buffer_);
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
          cur_row_.cells_[i].set_varchar(ObString("NULL"));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
      } else {
        cur_row_.cells_[i].set_varchar(ObString("NULL"));
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
      //row_lock_addr
      cur_row_.cells_[i].set_uint64(uint64_t(tx_lock_stat.get_memtable_key_info().get_row_lock()));
      break;
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

}//observer
}//oceanbase
