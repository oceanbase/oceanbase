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

#include "observer/virtual_table/ob_all_virtual_tx_scheduler_stat.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace observer
{

ObGVTxSchedulerStat::ObGVTxSchedulerStat()
    : ObVirtualTableScannerIterator(),
      xid_(),
      tx_scheduler_stat_iter_()
{
}

ObGVTxSchedulerStat::~ObGVTxSchedulerStat()
{
  reset();
}

void ObGVTxSchedulerStat::reset()
{
  // release tenant resources first
  omt::ObMultiTenantOperator::reset();
  ip_buffer_[0] = '\0';
  parts_buffer_[0] = '\0';
  tx_desc_addr_buffer_[0] = '\0';
  savepoints_buffer_[0] = '\0';
  xid_.reset();
  ObVirtualTableScannerIterator::reset();
}

void ObGVTxSchedulerStat::release_last_tenant()
{
  // resources related with tenant must be released by this function
  tx_scheduler_stat_iter_.reset();
}

bool ObGVTxSchedulerStat::is_need_process(uint64_t tenant_id)
{
  bool bool_ret = false;
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    bool_ret = true;
  }

  return bool_ret;
}

int ObGVTxSchedulerStat::get_next_tx_info_(ObTxSchedulerStat &tx_scheduler_stat)
{
  ObTxSchedulerStat tmp_tx_scheduler_stat;

  int ret = tx_scheduler_stat_iter_.get_next(tmp_tx_scheduler_stat);

  if (OB_SUCC(ret)) {
    tx_scheduler_stat = tmp_tx_scheduler_stat;
  }

  return ret;

}

int ObGVTxSchedulerStat::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTxSchedulerStat tx_scheduler_stat;

  if (nullptr == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be nullptr", K(allocator_), KR(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (!tx_scheduler_stat_iter_.is_ready()) {
    if (OB_FAIL(MTL(ObTransService*)->iterate_tx_scheduler_stat(tx_scheduler_stat_iter_))) {
      SERVER_LOG(WARN, "iterate transaction scheduler error", KR(ret));
      if (OB_NOT_RUNNING == ret || OB_NOT_INIT == ret) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tx_scheduler_stat_iter_.set_ready())) {
      SERVER_LOG(WARN, "ObTransSchedulerIterator set ready error", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_next_tx_info_(tx_scheduler_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "ObGVTxSchedulerStat iter error", KR(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    xid_ = tx_scheduler_stat.xid_;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.tenant_id_);
          break;
        case SVR_IP:
          (void)tx_scheduler_stat.addr_.ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_default_collation_type();
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.addr_.get_port());
          break;
        case SESSION_ID:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.sess_id_);
          break;
        case TX_ID:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.tx_id_.get_id());
          break;
        case STATE:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.state_);
          break;
        case CLUSTER_ID:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.cluster_id_);
          break;
        case COORDINATOR:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.coord_id_.id());
          break;
        case PARTICIPANTS:
          if (0 < tx_scheduler_stat.parts_.count()) {
            tx_scheduler_stat.get_parts_str(parts_buffer_, OB_MAX_BUFFER_SIZE);
            cur_row_.cells_[i].set_varchar(parts_buffer_);
            cur_row_.cells_[i].set_default_collation_type();
          } else {
            cur_row_.cells_[i].reset();
          }
          break;
        case ISOLATION_LEVEL:
          cur_row_.cells_[i].set_int((int)tx_scheduler_stat.isolation_);
          break;
        case SNAPSHOT_VERSION:
          if (tx_scheduler_stat.snapshot_version_.get_val_for_inner_table_field() != OB_INVALID_SCN_VAL) {
            cur_row_.cells_[i].set_uint64(tx_scheduler_stat.snapshot_version_.get_val_for_inner_table_field());
          } else {
            cur_row_.cells_[i].reset();
          }
          break;
        case ACCESS_MODE:
          cur_row_.cells_[i].set_int((int)tx_scheduler_stat.access_mode_);
          break;
        case TX_OP_SN:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.op_sn_);
          break;
        case FLAG:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.flag_);
          break;
        case ACTIVE_TS:
          if (is_valid_timestamp_(tx_scheduler_stat.active_ts_)) {
            cur_row_.cells_[i].set_timestamp(tx_scheduler_stat.active_ts_);
          } else {
            cur_row_.cells_[i].reset();
          }
          break;
        case EXPIRE_TS:
          if (is_valid_timestamp_(tx_scheduler_stat.expire_ts_)) {
            cur_row_.cells_[i].set_timestamp(tx_scheduler_stat.expire_ts_);
          } else {
            cur_row_.cells_[i].reset();
          }
          break;
        case TIMEOUT_US:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.timeout_us_);
          break;
        case REF_CNT:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.ref_cnt_);
          break;
        case TX_DESC_ADDR:
          tx_desc_addr_buffer_[0] = 0;
          snprintf(tx_desc_addr_buffer_, 18, "0x%lx", (uint64_t)tx_scheduler_stat.tx_desc_addr_);
          cur_row_.cells_[i].set_varchar(tx_desc_addr_buffer_);
          cur_row_.cells_[i].set_default_collation_type();
          break;
        case SAVEPOINTS:
          if (0 < tx_scheduler_stat.savepoints_.count()) {
            (void)tx_scheduler_stat.savepoints_.to_string(savepoints_buffer_, OB_MAX_BUFFER_SIZE);
            cur_row_.cells_[i].set_varchar(savepoints_buffer_);
            cur_row_.cells_[i].set_default_collation_type();
          } else {
            cur_row_.cells_[i].reset();
          }
          break;
        case SAVEPOINTS_TOTAL_CNT:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.savepoints_.count());
          break;
        case INTERNAL_ABORT_CAUSE:
          cur_row_.cells_[i].set_int(tx_scheduler_stat.abort_cause_);
          break;
        case CAN_EARLY_LOCK_RELEASE:
          cur_row_.cells_[i].set_bool(tx_scheduler_stat.can_elr_);
          break;
        case GTRID:
          if (!xid_.empty()) {
            cur_row_.cells_[i].set_varchar(xid_.get_gtrid_str());
            cur_row_.cells_[i].set_default_collation_type();
          } else {
            cur_row_.cells_[i].reset();
          }
          break;
        case BQUAL:
          if (!xid_.empty()) {
            cur_row_.cells_[i].set_varchar(xid_.get_bqual_str());
            cur_row_.cells_[i].set_default_collation_type();
          } else {
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
          SERVER_LOG(WARN, "invalid coloum_id", KR(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

bool ObGVTxSchedulerStat::is_valid_timestamp_(const int64_t timestamp) const
{
  bool ret_bool = true;
  if (INT64_MAX == timestamp || 0 > timestamp) {
    ret_bool = false;
  }
  return ret_bool;
}

}
}
