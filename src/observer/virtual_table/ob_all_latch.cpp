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
#include "lib/stat/ob_session_stat.h"
#include "observer/ob_server.h"
#include "observer/ob_server_utils.h"
#include "share/ob_tenant_mgr.h"

namespace oceanbase
{
using namespace lib;
using namespace common;

namespace observer
{
ObAllLatch::ObAllLatch()
    : ObVirtualTableIterator(),
      addr_(NULL),
      iter_(0),
      latch_iter_(0),
      tenant_dis_()
{
}

ObAllLatch::~ObAllLatch()
{
  reset();
}

int ObAllLatch::inner_open()
{
  int ret = OB_SUCCESS;
  tenant_dis_.reset();
  return ret;
}

void ObAllLatch::reset()
{
  addr_ = NULL;
  iter_ = 0;
  latch_iter_ = 0;
  tenant_dis_.reset();
}

int ObAllLatch::get_all_diag_info()
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(effective_tenant_id_)) {
    if (OB_FAIL(common::ObDIGlobalTenantCache::get_instance().get_all_latch_stat(*allocator_, tenant_dis_))) {
      SERVER_LOG(WARN, "Fail to get tenant latch stat", KR(ret));
    }
  } else if (OB_FAIL(get_the_diag_info(effective_tenant_id_))) {
    SERVER_LOG(WARN, "Fail to get tenant latch stat", KR(ret), K_(effective_tenant_id));
  }
  return ret;
}

int ObAllLatch::get_the_diag_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "allocator is null", KR(ret));
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(common::ObDiagnoseTenantInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "Fail to alloc buf", KR(ret));
  } else {
    std::pair<uint64_t, common::ObDiagnoseTenantInfo*> pair;
    pair.first = tenant_id;
    pair.second = new (buf) common::ObDiagnoseTenantInfo(allocator_);
    if (OB_FAIL(common::ObDIGlobalTenantCache::get_instance().get_the_diag_info(tenant_id, *(pair.second)))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN, "Fail to get tenant latch stat", KR(ret), K(tenant_id));
      }
    } else if (OB_FAIL(tenant_dis_.push_back(pair))) {
      SERVER_LOG(WARN, "Fail to push diag info value to array", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObAllLatch::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(addr_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "Some variable is null", K_(allocator), K_(addr), K(ret));
  } else {
    if (0 == iter_ && 0 == latch_iter_) {
      ret = get_all_diag_info();
      if (OB_FAIL(ret)) {
        SERVER_LOG(WARN, "Fail to get tenant status", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (iter_ >= tenant_dis_.count()) {
        ret = OB_ITER_END;
      }
    }

    if (OB_SUCC(ret)) {
      ObObj *cells = cur_row_.cells_;
      std::pair<uint64_t, common::ObDiagnoseTenantInfo*> dipair;
      ObString ipstr;
      if (OB_ISNULL(cells)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
      } else if (OB_FAIL(tenant_dis_.at(iter_, dipair))) {
        SERVER_LOG(WARN, "Fail to get tenant dis", K_(iter), K(ret));
      } else if (latch_iter_ >= ObLatchIds::LATCH_END) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "The latch iter exceed", K_(latch_iter), K(ret));
      } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
        SERVER_LOG(ERROR, "get server ip failed", K(ret));
      }

      for (int64_t cell_idx = 0;
          OB_SUCC(ret) && cell_idx < output_column_ids_.count();
          ++cell_idx) {
        const uint64_t column_id = output_column_ids_.at(cell_idx);
        ObLatchStat *p_latch_stat = dipair.second->get_latch_stats().get_item(latch_iter_);
        if (OB_ISNULL(p_latch_stat)) continue;
        const ObLatchStat& latch_stat = *p_latch_stat;
        switch(column_id) {
        case TENANT_ID: {
            cells[cell_idx].set_int(dipair.first);
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
        if (++latch_iter_ >= ObLatchIds::LATCH_END) {
          latch_iter_ = 0;
          iter_++;
        }
      }
    }
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase
