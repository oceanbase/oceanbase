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

#include "observer/virtual_table/ob_all_virtual_tx_stat.h"

#include "observer/ob_server.h"
#include "storage/tx/ob_trans_service.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace observer
{
void ObGVTxStat::reset()
{
  ip_buffer_[0] = '\0';
  participants_buffer_[0] = '\0';
  scheduler_buffer_[0] = '\0';
  ctx_addr_buffer_[0] = '\0';

  ObVirtualTableScannerIterator::reset();
  all_tenants_.reset();
  xid_.reset();
  init_ = false;
}

void ObGVTxStat::destroy()
{
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(participants_buffer_, 0, OB_MAX_BUFFER_SIZE);
  memset(scheduler_buffer_, 0, MAX_IP_PORT_LENGTH + 8);
  memset(ctx_addr_buffer_, 0, CTX_ADDR_BUFFER_SIZE);

  ObVirtualTableScannerIterator::reset();
  all_tenants_.reset();
  xid_.reset();
  init_ = false;
}

int ObGVTxStat::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  tx_stat_iter_.reset();
  if (NULL == allocator_) {
    SERVER_LOG(WARN, "invalid argument, allocator_ or txs_ is null", "allocator",
        OB_P(allocator_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = fill_tenant_ids_())) {
    SERVER_LOG(WARN, "fail to fill tenant ids", K(ret));
  } else {
    for (int i = 0; i < all_tenants_.count() && OB_SUCC(ret); i++) {
      int64_t cur_tenant_id = all_tenants_.at(i);
      MTL_SWITCH(cur_tenant_id) {
        transaction::ObTransService *txs = MTL(transaction::ObTransService*);
        if (OB_SUCCESS != (ret = txs->iterate_all_observer_tx_stat(tx_stat_iter_))) {
          SERVER_LOG(WARN, "iterate transaction stat error", K(ret), K(cur_tenant_id));
          // when interate tenant failed, show all info collected, not need return error code
          if (OB_NOT_RUNNING == ret || OB_NOT_INIT == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }
  }
  if (OB_SUCCESS != ret) {
    // SERVER_LOG(WARN, "fail to prepare start to read", K(ret));
  } else if (OB_SUCCESS != (ret = tx_stat_iter_.set_ready())) { // set ready for the first count
    SERVER_LOG(WARN, "ObTransStatIterator set ready error", K(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVTxStat::init()
{
  int ret = OB_SUCCESS;
  if (init_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else {
    init_ = true;
  }
  return ret;
}


int ObGVTxStat::get_next_tx_info_(ObTxStat &tx_stat)
{
  ObTxStat tmp_tx_stat;

  int ret = tx_stat_iter_.get_next(tmp_tx_stat);

  if (OB_SUCC(ret)) {
    tx_stat = tmp_tx_stat;
  }

  return ret;
}

int ObGVTxStat::fill_tenant_ids_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!init_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == effective_tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid tenant id", KR(ret), K_(effective_tenant_id));
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to get multi tenant from GCTX", K(ret));
  } else {
    omt::TenantIdList tmp_all_tenants;
    tmp_all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
    GCTX.omt_->get_tenant_ids(tmp_all_tenants);
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_all_tenants.size(); ++i) {
      uint64_t tenant_id = tmp_all_tenants[i];
      if (!is_virtual_tenant_id(tenant_id) && // skip virtual tenant
          (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
        if (OB_FAIL(all_tenants_.push_back(tenant_id))) {
          SERVER_LOG(WARN, "fail to push back tenant id", KR(ret), K(tenant_id));
        }
      }
    }
    SERVER_LOG(INFO, "succeed to get tenant ids", K(all_tenants_));
  }

  return ret;
}

int ObGVTxStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTxStat tx_stat;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare start to read error", K(ret), K(start_to_read_));
  } else if (OB_SUCCESS != (ret = get_next_tx_info_(tx_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "ObGVTxStat iter error", K(ret));
    } else {
      SERVER_LOG(DEBUG, "ObGVTxStat iter end success");
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    xid_ = tx_stat.xid_;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID:
          cur_row_.cells_[i].set_int(tx_stat.tenant_id_);
          break;
        case SVR_IP:
          (void)tx_stat.addr_.ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(tx_stat.addr_.get_port());
          break;
        case TX_TYPE:
          cur_row_.cells_[i].set_int(tx_stat.tx_type_);
          break;
        case TX_ID:
          cur_row_.cells_[i].set_int(tx_stat.tx_id_.get_id());
          break;
        case SESSION_ID:
          cur_row_.cells_[i].set_int(tx_stat.session_id_);
          break;
        case SCHEDULER_ADDR:
          (void)tx_stat.scheduler_addr_.to_string(scheduler_buffer_, MAX_IP_PORT_LENGTH + 8);
          cur_row_.cells_[i].set_varchar(scheduler_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case IS_DECIDED:
          cur_row_.cells_[i].set_bool(tx_stat.has_decided_);
          break;
        case LS_ID:
          cur_row_.cells_[i].set_int(tx_stat.ls_id_.id());
          break;
        case PARTICIPANTS:
          // if participants' count is equal to 0, then its value is NULL
          if (0 < tx_stat.participants_.count()) {
            (void)tx_stat.participants_.to_string(participants_buffer_, OB_MAX_BUFFER_SIZE);
            cur_row_.cells_[i].set_varchar(participants_buffer_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cur_row_.cells_[i].reset();
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case TX_CTX_CREATE_TIME:
          if (is_valid_timestamp_(tx_stat.tx_ctx_create_time_)) {
            cur_row_.cells_[i].set_timestamp(tx_stat.tx_ctx_create_time_);
          } else {
            // if invalid timestamp, display NULL
            cur_row_.cells_[i].reset();
          }
          break;
        case TX_EXPIRED_TIME:
          if (is_valid_timestamp_(tx_stat.tx_expired_time_)) {
            cur_row_.cells_[i].set_timestamp(tx_stat.tx_expired_time_);
          } else {
            // if invalid timestamp, display NULL
            cur_row_.cells_[i].reset();
          }
          break;
        case REF_CNT:
          cur_row_.cells_[i].set_int(tx_stat.ref_cnt_);
          break;
        case LAST_OP_SN:
          cur_row_.cells_[i].set_int(tx_stat.last_op_sn_);
          break;
        case PENDING_WRITE:
          cur_row_.cells_[i].set_int(tx_stat.pending_write_);
          break;
        case STATE:
          cur_row_.cells_[i].set_int(tx_stat.state_);
          break;
        case PART_TX_ACTION:
          cur_row_.cells_[i].set_int(tx_stat.part_tx_action_);
          break;
        case TX_CTX_ADDR:
          ctx_addr_buffer_[0] = 0;
          snprintf(ctx_addr_buffer_, 18, "0x%lx", (uint64_t)tx_stat.tx_ctx_addr_);
          cur_row_.cells_[i].set_varchar(ctx_addr_buffer_);
          cur_row_.cells_[i].set_default_collation_type();
          break;
        case MEM_CTX_ID:
          //TODO shanyan.g removed schema
          cur_row_.cells_[i].set_int(-1);
          break;
        case PENDING_LOG_SIZE:
          cur_row_.cells_[i].set_int(tx_stat.pending_log_size_);
          break;
        case FLUSHED_LOG_SIZE:
          cur_row_.cells_[i].set_int(tx_stat.flushed_log_size_);
          break;
        case ROLE_STATE:
          cur_row_.cells_[i].set_int(tx_stat.role_state_);
          break;
        case IS_EXITING:
          cur_row_.cells_[i].set_int(tx_stat.is_exiting_);
          break;
        case COORD:
          cur_row_.cells_[i].set_int(tx_stat.coord_.id());
          break;
        case LAST_REQUEST_TS:
          cur_row_.cells_[i].set_timestamp(tx_stat.last_request_ts_);
          break;
        case GTRID:
          if (!xid_.empty()) {
            cur_row_.cells_[i].set_varchar(xid_.get_gtrid_str());
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            // use default value NULL
            cur_row_.cells_[i].reset();
          }
          break;
        case BQUAL:
          if (!xid_.empty()) {
            cur_row_.cells_[i].set_varchar(xid_.get_bqual_str());
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            // use default value NULL
            cur_row_.cells_[i].reset();
          }
          break;
        case FORMAT_ID:
          if (!xid_.empty()) {
            cur_row_.cells_[i].set_int(xid_.get_format_id());
          } else {
            cur_row_.cells_[i].set_int(-1);
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid coloum_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

bool ObGVTxStat::is_valid_timestamp_(const int64_t timestamp) const
{
  bool ret_bool = true;
  if (INT64_MAX == timestamp || 0 > timestamp) {
    ret_bool = false;
  }
  return ret_bool;
}

}/* ns observer*/
}/* ns oceanbase */
