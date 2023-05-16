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

#include "observer/virtual_table/ob_all_virtual_dup_ls_lease_mgr.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace observer
{
void ObAllVirtualDupLSLeaseMgr::reset()
{
  memset(ip_buffer_, 0, sizeof(ip_buffer_));
  memset(follower_ip_buffer_, 0, sizeof(follower_ip_buffer_));

  ObVirtualTableScannerIterator::reset();
  dup_ls_lease_mgr_stat_iter_.reset();
  all_tenants_.reset();
  self_addr_.reset();
  init_ = false;
}

void ObAllVirtualDupLSLeaseMgr::destroy()
{
  reset();
}

int ObAllVirtualDupLSLeaseMgr::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  dup_ls_lease_mgr_stat_iter_.reset();
  if (OB_ISNULL(allocator_)) {
    SERVER_LOG(WARN, "invalid argument, allocator_ is null", KP(allocator_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(fill_tenant_ids_())) {
    SERVER_LOG(WARN, "fail to fill tenant ids", K(ret));
  } else {
    for (int i = 0; i < all_tenants_.count() && OB_SUCC(ret); i++) {
      int64_t cur_tenant_id = all_tenants_.at(i);
      MTL_SWITCH(cur_tenant_id) {
        transaction::ObTransService *txs = MTL(transaction::ObTransService*);
        if (OB_ISNULL(txs)) {
          ret = OB_INVALID_ARGUMENT;
          SERVER_LOG(WARN, "invalid argument, txs is null", KP(txs));
        } else if (OB_FAIL(txs->get_dup_table_loop_worker().
                    iterate_dup_ls(dup_ls_lease_mgr_stat_iter_))) {
          if (OB_NOT_INIT == ret ) {
            ret = OB_SUCCESS;
          } else {
            SERVER_LOG(WARN, "collect dup ls lease mgr failed", K(ret), K(cur_tenant_id));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(dup_ls_lease_mgr_stat_iter_.set_ready())) { // set ready for the first count
    SERVER_LOG(WARN, "dup_ls_iter set ready error", K(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObAllVirtualDupLSLeaseMgr::init(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;

  if (init_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else {
    init_ = true;
    self_addr_ = addr;

    if (false == self_addr_.ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
    }
  }

  return ret;
}

int ObAllVirtualDupLSLeaseMgr::fill_tenant_ids_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!init_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == effective_tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid tenant id", K(ret), K_(effective_tenant_id));
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
          SERVER_LOG(WARN, "fail to push back tenant id", K(ret), K(tenant_id));
        }
      }
    }
    SERVER_LOG(INFO, "succeed to get tenant ids", K(all_tenants_));
  }

  return ret;
}

int ObAllVirtualDupLSLeaseMgr::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObDupTableLSLeaseMgrStat lease_mgr_stat;

  if (!start_to_read_ && OB_FAIL(prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare start to read error", K(ret), K(start_to_read_));
  } else if (OB_FAIL(dup_ls_lease_mgr_stat_iter_.get_next(lease_mgr_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "ObAllVirtualDupLSLeaseMgr iter end", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID:
          cur_row_.cells_[i].set_int(lease_mgr_stat.get_tenant_id());
          break;
        case LS_ID:
          cur_row_.cells_[i].set_int(lease_mgr_stat.get_ls_id().id());
          break;
        case SVR_IP:
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(self_addr_.get_port());
          break;
        case FOLLOWER_IP:
          if (lease_mgr_stat.get_follower_addr().ip_to_string(follower_ip_buffer_, common::OB_IP_STR_BUFF)) {
            cur_row_.cells_[i].set_varchar(follower_ip_buffer_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case FOLLOWER_PORT:
          cur_row_.cells_[i].set_int(lease_mgr_stat.get_follower_addr().get_port());
          break;
        case GRANT_TIMESTAMP:
          if (is_valid_timestamp_(lease_mgr_stat.get_grant_ts())) {
            cur_row_.cells_[i].set_timestamp(lease_mgr_stat.get_grant_ts());
          }
          break;
        case EXPIRED_TIMESTAMP:
          if (is_valid_timestamp_(lease_mgr_stat.get_expired_ts())) {
            cur_row_.cells_[i].set_timestamp(lease_mgr_stat.get_expired_ts());
          }
          break;
        case REMAIN_US:
          cur_row_.cells_[i].set_int(lease_mgr_stat.get_remain_us());
          break;
        case GRANT_REQ_TS:
          cur_row_.cells_[i].set_int(lease_mgr_stat.get_grant_req_ts());
          break;
        case CACHED_REQ_TS:
          cur_row_.cells_[i].set_int(lease_mgr_stat.get_cached_req_ts());
          break;
        case LEASE_INTERVAL_US:
          cur_row_.cells_[i].set_int(lease_mgr_stat.get_lease_interval());
          break;
        case MAX_REPLAYED_LOG_SCN:
          cur_row_.cells_[i].set_int(lease_mgr_stat.get_max_replayed_scn().convert_to_ts(true /* ignore invalid */));
          break;
        case MAX_READ_VERSION:
          cur_row_.cells_[i].set_int(lease_mgr_stat.get_max_read_version());
          break;
        case MAX_COMMIT_VERSION:
          cur_row_.cells_[i].set_int(lease_mgr_stat.get_max_commit_version());
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

bool ObAllVirtualDupLSLeaseMgr::is_valid_timestamp_(const int64_t timestamp) const
{
  bool ret_bool = true;
  if (INT64_MAX == timestamp || 0 > timestamp) {
    ret_bool = false;
  }
  return ret_bool;
}

}/* ns observer*/
}/* ns oceanbase */
