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

#define USING_LOG_PREFIX SERVER

#include "ob_all_virtual_px_target_monitor.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace observer
{

ObAllVirtualPxTargetMonitor::ObAllVirtualPxTargetMonitor()
    : tenand_array_(),
      tenant_idx_(0),
      target_info_array_(),
      target_usage_idx_(0)
{
  svr_ip_buff_[0] = '\0';
  peer_ip_buff_[0] = '\0';
}

int ObAllVirtualPxTargetMonitor::init()
{
  int ret = OB_SUCCESS;
  tenand_array_.reset();
  tenant_idx_ = 0;
  target_info_array_.reset();
  target_usage_idx_ = 0;
  return ret;
}

int ObAllVirtualPxTargetMonitor::inner_open()
{
  int ret = OB_SUCCESS;
  // sys tenant show all tenant infos
  if (is_sys_tenant(effective_tenant_id_)) {
    if (OB_FAIL(OB_PX_TARGET_MGR.get_all_tenant(tenand_array_))) {
      LOG_WARN("get all tenant failed", K(ret));
    }
  } else {
    // user tenant show self tenant info
    if (OB_FAIL(tenand_array_.push_back(effective_tenant_id_))) {
      LOG_WARN("push back tenant array fail", KR(ret), K(effective_tenant_id_));
    }
  }
  return ret;
}

int ObAllVirtualPxTargetMonitor::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  ObPxTargetInfo target_info;
  if (!start_to_read_ && OB_FAIL(prepare_start_to_read())) {
    LOG_WARN("prepare_get_px_target fail", K(ret));
  } else if (OB_FAIL(get_next_target_info(target_info))) {
    if (ret == OB_ITER_END) {
      LOG_INFO("get_px_target finish", K(ret));
    } else {
      LOG_WARN("get_px_target failed", K(ret));
    }
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP: {
          memset(svr_ip_buff_, 0, common::OB_IP_STR_BUFF);
          if (!target_info.server_.ip_to_string(svr_ip_buff_, common::OB_IP_PORT_STR_BUFF)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("get server_ip failed", K(ret), K(target_info));
          } else {
            cur_row_.cells_[i].set_varchar(ObString::make_string(svr_ip_buff_));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                      ObCharset::get_default_charset()));
          }
          break;
        }
        case SVR_PORT: {
          cur_row_.cells_[i].set_int(static_cast<int64_t>(target_info.server_.get_port()));
          break;
        }
        case TENANT_ID: {
          cur_row_.cells_[i].set_int(target_info.tenant_id_);
          break;
        }
        case IS_LEADER: {
          cur_row_.cells_[i].set_bool(target_info.is_leader_);
          break;
        }
        case VERSION: {
          cur_row_.cells_[i].set_uint64(target_info.version_);
          break;
        }
        case PEER_IP: {
          memset(peer_ip_buff_, 0, common::OB_IP_STR_BUFF);
          if (!target_info.peer_server_.ip_to_string(peer_ip_buff_, common::OB_IP_PORT_STR_BUFF)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("get server_ip failed", K(ret), K(target_info));
          } else {
            cur_row_.cells_[i].set_varchar(ObString::make_string(peer_ip_buff_));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                      ObCharset::get_default_charset()));
          }
          break;
        }
        case PEER_PORT: {
          cur_row_.cells_[i].set_int(static_cast<int64_t>(target_info.peer_server_.get_port()));
          break;
        }
        case PEER_TARGET: {
          cur_row_.cells_[i].set_int(target_info.parallel_servers_target_);
          break;
        }
        case PEER_TARGET_USED: {
          cur_row_.cells_[i].set_int(target_info.peer_target_used_);
          break;
        }
        case LOCAL_TARGET_USED: {
          cur_row_.cells_[i].set_int(target_info.local_target_used_);
          break;
        }
        case LOCAL_PARALLEL_SESSION_COUNT: {
          cur_row_.cells_[i].set_int(target_info.local_parallel_session_count_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column_id", K(ret), K(col_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualPxTargetMonitor::get_next_target_info(ObPxTargetInfo &target_info)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && target_usage_idx_ >= target_info_array_.count()) {
    if (tenant_idx_ == tenand_array_.count()) {
      ret = OB_ITER_END;
    } else {
      uint64_t tenant_id = tenand_array_.at(tenant_idx_++);
      target_info_array_.reset();
      target_usage_idx_ = 0;
      if (OB_FAIL(OB_PX_TARGET_MGR.get_all_target_info(tenant_id, target_info_array_))) {
        LOG_WARN("get all target_info failed", K(ret), K(tenant_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    target_info = target_info_array_.at(target_usage_idx_++);
  }
  return ret;
}

int ObAllVirtualPxTargetMonitor::inner_close()
{
  int ret = OB_SUCCESS;
  tenand_array_.destroy();
  target_info_array_.destroy();
  return ret;
}

int ObAllVirtualPxTargetMonitor::prepare_start_to_read()
{
  int ret = OB_SUCCESS;
  start_to_read_ = true;
  return ret;
}

}//namespace observer
}//namespace oceanbase
