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

#include "ob_all_latch.h"
#include "observer/ob_server.h"
#include "observer/ob_server_utils.h"
#include "share/ash/ob_di_util.h"

namespace oceanbase
{
using namespace lib;
using namespace common;

namespace observer
{

int ObAllLatch::inner_open()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObAllLatch::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_ = NULL;
  latch_iter_ = 0;
  tenant_di_.reset();
  ObVirtualTableScannerIterator::reset();
}


int ObAllLatch::get_the_diag_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObDiagnosticInfoUtil::get_the_diag_info(tenant_id, tenant_di_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      SERVER_LOG(WARN, "Fail to get tenant latch stat", KR(ret), K(tenant_id));
    }
  }
  return ret;
}


int ObAllLatch::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

int ObAllLatch::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(addr_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "Some variable is null", K_(allocator), K_(addr), K(ret));
  } else {
    if (0 == latch_iter_) {
      if (OB_FAIL(get_the_diag_info(MTL_ID()))) {
        SERVER_LOG(WARN, "Fail to get tenant status", K(ret));
      }
    }
    if (latch_iter_ >= ObLatchIds::LATCH_END) {
      ret = OB_ITER_END;
    }

    if (OB_SUCC(ret)) {
      ObObj *cells = cur_row_.cells_;
      ObString ipstr;
      if (OB_ISNULL(cells)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
      } else if (latch_iter_ >= ObLatchIds::LATCH_END) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "The latch iter exceed", K_(latch_iter), K(ret));
      } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
        SERVER_LOG(ERROR, "get server ip failed", K(ret));
      }
      ObLatchStat *p_latch_stat = tenant_di_.get_latch_stats().get_item(latch_iter_);
      while(OB_SUCC(ret) && OB_ISNULL(p_latch_stat)) {
        latch_iter_++;
        if (latch_iter_ >= ObLatchIds::LATCH_END) {
          ret = OB_ITER_END;
        } else {
          p_latch_stat = tenant_di_.get_latch_stats().get_item(latch_iter_);
        }
      }

      for (int64_t cell_idx = 0;
          OB_SUCC(ret) &&  OB_NOT_NULL(p_latch_stat) && cell_idx < output_column_ids_.count();
          ++cell_idx) {
        const ObLatchStat& latch_stat = *p_latch_stat;
        const uint64_t column_id = output_column_ids_.at(cell_idx);
        switch(column_id) {
        case TENANT_ID: {
            cells[cell_idx].set_int(MTL_ID());
            break;
          }
        case SVR_IP: {
            cells[cell_idx].set_varchar(ipstr);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
        case SVR_PORT: {
            cells[cell_idx].set_int(addr_->get_port());
            break;
          }
        case LATCH_ID: {
            cells[cell_idx].set_int(OB_LATCHES[latch_iter_].latch_id_);
            break;
          }
        case NAME: {
            cells[cell_idx].set_varchar(OB_LATCHES[latch_iter_].latch_name_);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
        case ADDR: {
            cells[cell_idx].set_null();
            break;
          }
        case LEVEL: {
            cells[cell_idx].set_int(0);
            break;
          }
        case HASH: {
            cells[cell_idx].set_int(0);
            break;
          }
        case GETS: {
            cells[cell_idx].set_int(latch_stat.gets_);
            break;
          }
        case MISSES: {
            cells[cell_idx].set_int(latch_stat.misses_);
            break;
          }
        case SLEEPS: {
            cells[cell_idx].set_int(latch_stat.sleeps_);
            break;
          }
        case IMMEDIATE_GETS: {
            cells[cell_idx].set_int(latch_stat.immediate_gets_);
            break;
          }
        case IMMEDIATE_MISSES: {
            cells[cell_idx].set_int(latch_stat.immediate_misses_);
            break;
          }
        case SPIN_GETS: {
            cells[cell_idx].set_int(latch_stat.spin_gets_);
            break;
          }
        case WAIT_TIME: {
            cells[cell_idx].set_int(latch_stat.wait_time_);
            break;
          }
        default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(cell_idx), K_(output_column_ids), K(ret));
            break;
          }
        }
      }

      if (OB_SUCC(ret)) {
        row = &cur_row_;
      }
    }
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase
