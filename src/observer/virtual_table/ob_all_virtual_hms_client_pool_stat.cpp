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

#include "observer/virtual_table/ob_all_virtual_hms_client_pool_stat.h"
#include "share/catalog/hive/ob_hms_client_pool.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{

namespace observer
{
int ObHMSClientPoolGetter::operator()(
    common::hash::HashMapPair<ObHMSClientPoolKey, share::ObHMSClientPool *> &entry)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  share::ObHMSClientPool *hms_client_pool = entry.second;
  int64_t total_clients = 0, in_use_clients = 0, idle_clients = 0;
  hms_client_pool->get_pool_stats(total_clients, in_use_clients, idle_clients);
  SERVER_LOG(TRACE, "client pool stats", K(ret), K(total_clients), K(in_use_clients), K(idle_clients));
  for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < output_column_ids_.count(); ++cell_idx) {
    uint64_t col_id = output_column_ids_.at(cell_idx);
    switch (col_id) {
      case ObAllVirtualHMSClientPoolStat::COLUMNS::SVR_IP: {
        cells[cell_idx].set_varchar(svr_ip_);
        cells[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case ObAllVirtualHMSClientPoolStat::COLUMNS::SVR_PORT: {
        cells[cell_idx].set_int(port_);
        break;
      }
      case ObAllVirtualHMSClientPoolStat::COLUMNS::TENANT_ID: {
        cells[cell_idx].set_int(hms_client_pool->get_tenant_id());
        break;
      }
      case ObAllVirtualHMSClientPoolStat::COLUMNS::CATALOG_ID: {
        cells[cell_idx].set_int(hms_client_pool->get_catalog_id());
        break;
      }
      case ObAllVirtualHMSClientPoolStat::COLUMNS::TOTAL_CLIENTS: {
        cells[cell_idx].set_int(total_clients);
        break;
      }
      case ObAllVirtualHMSClientPoolStat::COLUMNS::IN_USE_CLIENTS: {
        cells[cell_idx].set_int(in_use_clients);
        break;
      }
      case ObAllVirtualHMSClientPoolStat::COLUMNS::IDLE_CLIENTS: {
        cells[cell_idx].set_int(idle_clients);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid coloum_id", K(ret), K(col_id));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_.add_row(cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
    } else { /*do nothing*/ }
  }
  return ret;
}

ObAllVirtualHMSClientPoolStat::ObAllVirtualHMSClientPoolStat()
    : port_(0), tenant_ids_(), tenant_idx_(0)
{
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
}

ObAllVirtualHMSClientPoolStat::~ObAllVirtualHMSClientPoolStat()
{
  reset();
}

void ObAllVirtualHMSClientPoolStat::destroy()
{
  reset();
}

void ObAllVirtualHMSClientPoolStat::reset()
{
  ObVirtualTableScannerIterator::reset();
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
  port_ = 0;
  tenant_ids_.reset();
  tenant_idx_ = 0;
}

int ObAllVirtualHMSClientPoolStat::inner_open()
{
  int ret = OB_SUCCESS;
  // sys tenant show all tenant infos
  if (is_sys_tenant(effective_tenant_id_)) {
    if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids_))) {
      SERVER_LOG(WARN, "failed to add tenant id", K(ret));
    }
  } else {
    // user tenant show self tenant infos
    if (OB_FAIL(tenant_ids_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN,
                 "failed to push back tenant id",
                 KR(ret),
                 K(effective_tenant_id_),
                 K(tenant_ids_));
    }
  }
  return ret;
}

int ObAllVirtualHMSClientPoolStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (tenant_idx_ >= tenant_ids_.count()) {
    ret = OB_ITER_END;
  } else if (!start_to_read_) {
    if (OB_FAIL(fill_scanner(tenant_ids_.at(tenant_idx_)))) {
      SERVER_LOG(WARN, "fill scanner failed", K(ret));
    } else {
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      } else if (++tenant_idx_ < tenant_ids_.count()) { // load a new tenant info
        ret = OB_SUCCESS;
        start_to_read_ = false;
        if (OB_FAIL(SMART_CALL(inner_get_next_row(row)))) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "failed to inner get next row", K(ret));
          }
        }
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualHMSClientPoolStat::fill_scanner(const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  const common::ObAddr &addr = GCTX.self_addr();
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else if (!addr.ip_to_string(svr_ip_, sizeof(svr_ip_))) {
    SERVER_LOG(ERROR, "ip to string failed", K(ret), K(addr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    port_ = addr.get_port();
    scanner_.reuse();
    MTL_SWITCH(tenant_id)
    {
      ObHMSClientPoolGetter getter(scanner_,
                                   output_column_ids_,
                                   svr_ip_,
                                   port_,
                                   cur_row_,
                                   tenant_id);
      ObHMSClientPoolMgr *hms_client_pool_mgr = MTL(ObHMSClientPoolMgr *);
      if (OB_ISNULL(hms_client_pool_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN,
                   "hms client pool mgr is NULL",
                   K(ret),
                   K(tenant_id),
                   K(effective_tenant_id_));
      } else if (OB_FAIL(hms_client_pool_mgr->generate_pool_stat_monitoring_info_rows(getter))) {
        SERVER_LOG(WARN,
                   "failed to generate pool stat monitoring info rows",
                   K(ret),
                   K(tenant_id),
                   K(effective_tenant_id_));
      } else {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase