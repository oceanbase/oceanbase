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

#include "observer/virtual_table/ob_all_virtual_tx_ctx_mgr_stat.h"
#include "observer/ob_server.h"
#include "storage/tx/ob_trans_service.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace observer
{
void ObGVTxCtxMgrStat::reset()
{
  ip_buffer_[0] = '\0';
  memstore_version_buffer_[0] = '\0';

  ObVirtualTableScannerIterator::reset();
}

void ObGVTxCtxMgrStat::destroy()
{
  trans_service_ = NULL;
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(memstore_version_buffer_, 0, common::MAX_VERSION_LENGTH);

  ObVirtualTableScannerIterator::reset();
}


int ObGVTxCtxMgrStat::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;

  if (NULL == allocator_ || NULL == trans_service_) {
    SERVER_LOG(WARN, "invalid argument, allocator_ or trans_service_ is null", "allocator",
        OB_P(allocator_), "trans_service", OB_P(trans_service_));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(trans_service_->iterate_tx_ctx_mgr_stat(tx_ctx_mgr_stat_iter_))) {
    SERVER_LOG(WARN, "iterate_tx_ctx_mgr_stat error", K(ret));
    if (OB_NOT_RUNNING == ret || OB_NOT_INIT == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tx_ctx_mgr_stat_iter_.set_ready())) {
    TRANS_LOG(WARN, "tx_ctx_mgr_stat_iter set ready error", KR(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVTxCtxMgrStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObLSTxCtxMgrStat ls_tx_ctx_mgr_stat;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare_start_to_read_ error", K(ret), K(start_to_read_));
  } else if (OB_SUCCESS != (ret = tx_ctx_mgr_stat_iter_.get_next(ls_tx_ctx_mgr_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "tx_ctx_mgr_stat_iter_ get next error", K(ret));
    } else {
      SERVER_LOG(DEBUG, "tx_ctx_mgr_stat_iter_ end success");
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          (void)ls_tx_ctx_mgr_stat.get_addr().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(ls_tx_ctx_mgr_stat.get_addr().get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // table_id
          cur_row_.cells_[i].set_int(ls_tx_ctx_mgr_stat.get_ls_id().id());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
           // is_master_
          cur_row_.cells_[i].set_int(ls_tx_ctx_mgr_stat.is_master()? 1 : 0);
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // is_stopped_
          cur_row_.cells_[i].set_int(ls_tx_ctx_mgr_stat.is_stopped()? 1 : 0);
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // state_
          cur_row_.cells_[i].set_int(ls_tx_ctx_mgr_stat.get_state());
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // state_str
          cur_row_.cells_[i].set_varchar(ls_tx_ctx_mgr_stat.get_state_str());
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // total_tx_ctx_count
          cur_row_.cells_[i].set_int(ls_tx_ctx_mgr_stat.get_total_tx_ctx_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          cur_row_.cells_[i].set_int(ls_tx_ctx_mgr_stat.get_mgr_addr());
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
