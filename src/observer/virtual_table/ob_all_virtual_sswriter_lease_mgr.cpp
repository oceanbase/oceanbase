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

#include "observer/virtual_table/ob_all_virtual_sswriter_lease_mgr.h"
#include "observer/ob_server.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/ob_sswriter_service.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{
void ObAllVirtualSSWriterLeaseMgr::reset()
{
  omt::ObMultiTenantOperator::reset();
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(target_ip_buffer_, 0, common::OB_IP_STR_BUFF);
#ifdef OB_BUILD_SHARED_STORAGE
  stat_iter_.reset();
#endif
  region_.reset();
  is_ready_ = false;
  ObVirtualTableScannerIterator::reset();
  is_inited_ = false;
}

void ObAllVirtualSSWriterLeaseMgr::destroy()
{
  reset();
}

bool ObAllVirtualSSWriterLeaseMgr::is_need_process(uint64_t tenant_id)
{
  bool bool_ret = false;
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    bool_ret = true;
  }
  return bool_ret;
}

void ObAllVirtualSSWriterLeaseMgr::release_last_tenant()
{
  // resources related with tenant must be released by this function
#ifdef OB_BUILD_SHARED_STORAGE
  stat_iter_.reset();
#endif
  is_ready_ = false;
}

int ObAllVirtualSSWriterLeaseMgr::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_SHARED_STORAGE
  ret = OB_ITER_END;
#else
  if (nullptr == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator shouldn't be nullptr", K(allocator_), KR(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (!is_ready_) {
    stat_iter_.reset();
    if (GCTX.is_shared_storage_mode()) {
      // only for sharead storage mode
      if (OB_FAIL(MTL(ObSSWriterService*)->iterate_sswriter_mgr_stat(stat_iter_))) {
        if (OB_NOT_RUNNING == ret || OB_NOT_INIT == ret) {
          ret = OB_SUCCESS;
        } else {
          SERVER_LOG(WARN, "iterate sswriter mgr stat failed", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stat_iter_.set_ready())) {
        SERVER_LOG(WARN, "iterator set ready failed", KR(ret));
      } else {
        is_ready_ = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSSWriterMgrStat sswriter_mgr_stat;
    if (OB_FAIL(get_next_sswriter_mgr_stat_(sswriter_mgr_stat))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "get next sswriter mgr stat failed", KR(ret), K(sswriter_mgr_stat));
      } else {
        SERVER_LOG(INFO, "iterate sswriter mgr stat end");
      }
    } else {
      const int64_t col_count = output_column_ids_.count();
      ObAddr self_addr = GCONF.self_addr_;
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case TENANT_ID: {
            cur_row_.cells_[i].set_int(MTL_ID());
            break;
          }
          case SVR_IP: {
            MEMSET(ip_buffer_, '\0', OB_IP_STR_BUFF);
            (void)self_addr.ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
            cur_row_.cells_[i].set_varchar(ip_buffer_);
            cur_row_.cells_[i].set_default_collation_type();
            break;
          }
          case SVR_PORT: {
            cur_row_.cells_[i].set_int(self_addr.get_port());
            break;
          }
          case LS_ID: {
            cur_row_.cells_[i].set_int(sswriter_mgr_stat.get_ls_id().id());
            break;
          }
          case TYPE: {
            cur_row_.cells_[i].set_varchar(
                sswriter_type_to_str(sswriter_mgr_stat.get_sswriter_type()));
            cur_row_.cells_[i].set_default_collation_type();
            break;
          }
          case GROUP_ID: {
            cur_row_.cells_[i].set_int(sswriter_mgr_stat.get_group_id());
            break;
          }
          case REGION: {
            region_ = sswriter_mgr_stat.get_region();
            cur_row_.cells_[i].set_varchar(region_.str());
            cur_row_.cells_[i].set_default_collation_type();
            break;
          }
          case STATE: {
            cur_row_.cells_[i].set_varchar(
                sswriter_group_state_to_str(sswriter_mgr_stat.get_sswriter_group_state()));
            cur_row_.cells_[i].set_default_collation_type();
            break;
          }
          case TARGET_SVR_IP: {
            MEMSET(target_ip_buffer_, '\0', OB_IP_STR_BUFF);
            (void)sswriter_mgr_stat.get_target_server().ip_to_string(
                target_ip_buffer_, common::OB_IP_STR_BUFF);
            cur_row_.cells_[i].set_varchar(target_ip_buffer_);
            cur_row_.cells_[i].set_default_collation_type();
            break;
          }
          case TARGET_SVR_PORT: {
            cur_row_.cells_[i].set_int(sswriter_mgr_stat.get_target_server().get_port());
            break;
          }
          case CURRENT_TS: {
            cur_row_.cells_[i].set_int(sswriter_mgr_stat.get_current_ts());
            break;
          }
          case LAST_UPDATE_STATE_TS: {
            cur_row_.cells_[i].set_int(sswriter_mgr_stat.get_last_update_state_ts());
            break;
          }
          case LEASE_EXPIRE_TS: {
            cur_row_.cells_[i].set_int(sswriter_mgr_stat.get_lease_expire_ts());
            break;
          }
          case EPOCH: {
            cur_row_.cells_[i].set_int(sswriter_mgr_stat.get_epoch());
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column_id", KR(ret), K(col_id));
            break;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
#endif
  return ret;
}

int ObAllVirtualSSWriterLeaseMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObAllVirtualSSWriterLeaseMgr::get_next_sswriter_mgr_stat_(
    ObSSWriterMgrStat &stat)
{
  return stat_iter_.get_next(stat);
}
#endif

}/* namespace observer*/
}/* namespace oceanbase */
