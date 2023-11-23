/**
 * Copyright (c) 2023 OceanBase
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
#include "observer/virtual_table/ob_all_virtual_ddl_sim_point_stat.h"
#include "share/ob_ddl_sim_point.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace observer
{


int ObAllVirtualDDLSimPoint::inner_get_next_row(common::ObNewRow *&row)
{
#ifdef ERRSIM
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  ObDDLSimPoint sim_point;
  while (OB_SUCC(ret)) {
    if (point_idx_ >= MAX_DDL_SIM_POINT_ID) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(ObDDLSimPointMgr::get_instance().get_sim_point(point_idx_++, sim_point))) {
      LOG_WARN("get ddl sim point failed", K(ret));
    } else if (sim_point.is_valid()) {
      break;
    }
  }
  if (OB_SUCC(ret) && sim_point.is_valid()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case SIM_POINT_ID: {
          cells[i].set_int(sim_point.id_);
          break;
        }
        case SIM_POINT_NAME: {
          cells[i].set_varchar(sim_point.name_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SIM_POINT_DESC: {
          cells[i].set_varchar(sim_point.desc_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SIM_POINT_ACTION: {
          memset(action_str_, 0, sizeof(action_str_));
          sim_point.action_->to_string(action_str_, sizeof(action_str_));
          action_str_[sizeof(action_str_) - 1] = 0;
          cells[i].set_varchar(action_str_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
#else
  return OB_ITER_END;
#endif
}

int ObAllVirtualDDLSimPointStat::init(const common::ObAddr &addr)
{
#ifdef ERRSIM
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY((!addr.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else {
    addr_ = addr;
    MEMSET(ip_buf_, 0, sizeof(ip_buf_));
    if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ip to string failed", K(ret), K(addr_));
    } else if (OB_FAIL(ObDDLSimPointMgr::get_instance().get_sim_stat(task_sim_points_, sim_counts_))) {
      LOG_WARN("get ddl sim stat failed", K(ret));
    } else if (OB_UNLIKELY(task_sim_points_.count() != sim_counts_.count())) {
      ret = OB_ERR_SYS;
      LOG_WARN("the stat count not match", K(ret), K(task_sim_points_.count()), K(sim_counts_.count()));
    } else {
      idx_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
#else
  return OB_SUCCESS;
#endif
}

int ObAllVirtualDDLSimPointStat::inner_get_next_row(common::ObNewRow *&row)
{
#ifdef ERRSIM
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (idx_ >= task_sim_points_.count()) {
    ret = OB_ITER_END;
  } else {
    const ObDDLSimPointMgr::TaskSimPoint &task_sim_point = task_sim_points_.at(idx_);
    const int64_t sim_count = sim_counts_.at(idx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case SVR_IP: {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(addr_.get_port());
          break;
        }
        case TENANT_ID: {
          cells[i].set_int(task_sim_point.tenant_id_);
          break;
        }
        case DDL_TASK_ID: {
          cells[i].set_int(task_sim_point.task_id_);
          break;
        }
        case SIM_POINT_ID: {
          cells[i].set_int(task_sim_point.point_id_);
          break;
        }
        case TRIGGER_COUNT: {
          cells[i].set_int(sim_count);
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
      ++idx_;
    }
  }
  return ret;
#else
  return OB_ITER_END;
#endif
}

}
}
