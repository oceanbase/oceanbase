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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_all_virtual_sql_workarea_histogram.h"
#include "lib/allocator/ob_mod_define.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_context.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
using namespace oceanbase::share;

ObSqlWorkareaHistogramIterator::ObSqlWorkareaHistogramIterator() :
  wa_histograms_(), tenant_ids_(), cur_nth_wa_hist_(0), cur_nth_tenant_(0)
{}

void ObSqlWorkareaHistogramIterator::destroy()
{
  reset();
}

void ObSqlWorkareaHistogramIterator::reset()
{
  wa_histograms_.reset();
  tenant_ids_.reset();
  cur_nth_wa_hist_ = 0;
  cur_nth_tenant_ = 0;
}

int ObSqlWorkareaHistogramIterator::init(const uint64_t effective_tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null of omt", KR(ret));
  } else if (is_sys_tenant(effective_tenant_id)) {
    if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids_))) {
      LOG_WARN("failed to get_mtl_tenant_ids", KR(ret));
    }
  } else if (OB_FAIL(tenant_ids_.push_back(effective_tenant_id))) {
    LOG_WARN("failed to push back tenant_id", KR(ret), K(effective_tenant_id));
  }
  return ret;
}

int ObSqlWorkareaHistogramIterator::get_next_batch_wa_histograms()
{
  int ret = OB_SUCCESS;
  cur_nth_wa_hist_ = 0;
  wa_histograms_.reset();
  if (cur_nth_tenant_ >= tenant_ids_.count()) {
    ret = OB_ITER_END;
  } else {
    uint64_t tenant_id = tenant_ids_.at(cur_nth_tenant_);
    MTL_SWITCH(tenant_id) {
      ObTenantSqlMemoryManager *sql_mem_mgr = nullptr;
      sql_mem_mgr = MTL(ObTenantSqlMemoryManager*);
      if (nullptr != sql_mem_mgr && OB_FAIL(sql_mem_mgr->get_workarea_histogram(wa_histograms_))) {
        LOG_WARN("failed to get workarea stat", K(ret));
      }
    }
    ++cur_nth_tenant_;
    LOG_TRACE("trace get histogram for next tenant", K(tenant_id), K(wa_histograms_.count()));
  }
  return ret;
}

int ObSqlWorkareaHistogramIterator::get_next_wa_histogram(
  ObWorkareaHistogram *&wa_histogram,
  uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  if (0 > cur_nth_wa_hist_ || cur_nth_wa_hist_ > wa_histograms_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: current wa exceeds total wa stats", K(ret));
  } else {
    while (OB_SUCC(ret) && cur_nth_wa_hist_ >= wa_histograms_.count()) {
      if (OB_FAIL(get_next_batch_wa_histograms())) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    wa_histogram = &wa_histograms_.at(cur_nth_wa_hist_);
    tenant_id = tenant_ids_.at(cur_nth_tenant_ - 1);
    ++cur_nth_wa_hist_;
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////
ObSqlWorkareaHistogram::ObSqlWorkareaHistogram() :
  ipstr_(), port_(0)
{}

void ObSqlWorkareaHistogram::destroy()
{
  ipstr_.reset();
  iter_.destroy();
}

void ObSqlWorkareaHistogram::reset()
{
  port_ = 0;
  ipstr_.reset();
  iter_.reset();
  start_to_read_ = false;
}

int ObSqlWorkareaHistogram::get_server_ip_and_port()
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  const common::ObAddr &addr = GCTX.self_addr();
  if (!addr.ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      LOG_WARN("failed to write string", K(ret));
    }
    port_ = addr.get_port();
  }
  return ret;
}

int ObSqlWorkareaHistogram::fill_row(
  uint64_t tenant_id,
  ObWorkareaHistogram &wa_histogram,
  common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < output_column_ids_.count(); ++cell_idx) {
    uint64_t col_id = output_column_ids_.at(cell_idx);
    switch(col_id) {
      case SVR_IP: {
        cells[cell_idx].set_varchar(ipstr_);
        cells[cell_idx].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SVR_PORT: {
        cells[cell_idx].set_int(port_);
        break;
      }
      case LOW_OPTIMAL_SIZE: {
        cells[cell_idx].set_int(wa_histogram.get_low_optimal_size());
        break;
      }
      case HIGH_OPTIMAL_SIZE: {
        cells[cell_idx].set_int(wa_histogram.get_high_optimal_size());
        break;
      }
      case OPTIMAL_EXECUTIONS: {
        cells[cell_idx].set_int(wa_histogram.get_optimal_executions());
        break;
      }
      case ONEPASS_EXECUTIONS: {
        cells[cell_idx].set_int(wa_histogram.get_onepass_executions());
        break;
      }
      case MULTIPASSES_EXECUTIONS: {
        cells[cell_idx].set_int(wa_histogram.get_multipass_executions());
        break;
      }
      case TOTAL_EXECUTIONS: {
        cells[cell_idx].set_int(wa_histogram.get_total_executions());
        break;
      }
      case TENAND_ID: {
        cells[cell_idx].set_int(tenant_id);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column id", K(col_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

int ObSqlWorkareaHistogram::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(iter_.init(effective_tenant_id_))) {
      LOG_WARN("failed to init iterator", K(ret));
    } else {
      start_to_read_ = true;
      if (OB_FAIL(get_server_ip_and_port())) {
        LOG_WARN("failed to get server ip and port", K(ret));
      }
    }
  }
  ObWorkareaHistogram *wa_histogram = nullptr;
  uint64_t tenant_id = 0;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(iter_.get_next_wa_histogram(wa_histogram, tenant_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next channel", K(ret));
    }
  } else if (OB_FAIL(fill_row(tenant_id, *wa_histogram, row))) {
    LOG_WARN("failed to get row from channel info", K(ret));
  }
  return ret;
}
