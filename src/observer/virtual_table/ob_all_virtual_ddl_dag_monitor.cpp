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

#include "ob_all_virtual_ddl_dag_monitor.h"
#include "lib/oblog/ob_log_module.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"

#define USING_LOG_PREFIX SERVER

namespace oceanbase
{
namespace observer
{

using namespace common;
using namespace storage;

ObAllVirtualDDLDagMonitor::ObAllVirtualDDLDagMonitor()
    : addr_(),
      is_inited_(false),
      tenant_ids_(),
      tenant_idx_(-1),
      nodes_(),
      node_idx_(0),
      task_infos_(),
      info_idx_(0),
      entry_()
{
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
}

ObAllVirtualDDLDagMonitor::~ObAllVirtualDDLDagMonitor()
{
  reset();
}

void ObAllVirtualDDLDagMonitor::reset()
{
  common::ObVirtualTableScannerIterator::reset();
  addr_.reset();
  is_inited_ = false;
  tenant_ids_.reset();
  tenant_idx_ = -1;
  for (int64_t i = 0; i < nodes_.count(); ++i) {
    if (OB_NOT_NULL(nodes_.at(i))) {
      nodes_.at(i)->dec_ref(); // paired with ObDDLDagMonitorMgr::get_all_nodes() which inc_ref()'ed
    }
  }
  nodes_.reset();
  node_idx_ = 0;
  task_infos_.reset();
  info_idx_ = 0;
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
}

int ObAllVirtualDDLDagMonitor::init(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllVirtualDDLDagMonitor already inited", K(ret));
  } else if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(ret), K(addr));
  } else if (FALSE_IT(addr_ = addr)) {
  } else if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ip to string failed", K(ret), K(addr_));
  } else {
    tenant_ids_.reset();
    tenant_idx_ = -1;
    const uint64_t tenant_id = MTL_ID();
    if (OB_INVALID_TENANT_ID == tenant_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      if (OB_ISNULL(GCTX.omt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("omt is null", K(ret));
      } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids_))) {
        LOG_WARN("failed to get mtl tenant ids", K(ret));
      }
    } else {
      if (OB_FAIL(tenant_ids_.push_back(tenant_id))) {
        LOG_WARN("failed to add tenant id", K(ret), K(tenant_id));
      }
    }

    if (OB_SUCC(ret) && tenant_ids_.count() > 0) {
      if (OB_FAIL(advance_to_next_tenant())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("no tenant nodes loaded", K(ret), K(tenant_ids_));
        } else {
          LOG_WARN("failed to load first tenant nodes", K(ret), K(tenant_ids_));
        }
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObAllVirtualDDLDagMonitor::advance_to_next_tenant()
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  while (OB_SUCC(ret) && ++tenant_idx_ < tenant_ids_.count()) {
    const uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
    for (int64_t i = 0; i < nodes_.count(); ++i) {
      if (OB_NOT_NULL(nodes_.at(i))) {
        nodes_.at(i)->dec_ref(); // paired with ObDDLDagMonitorMgr::get_all_nodes() which inc_ref()'ed
      }
    }
    nodes_.reset();
    node_idx_ = -1;
    task_infos_.reset();
    info_idx_ = 0;
    if (OB_FAIL(guard.switch_to(tenant_id))) {
      LOG_WARN("switch to tenant failed", K(ret), K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      ObDDLDagMonitorMgr *mgr = MTL(ObDDLDagMonitorMgr*);
      if (OB_ISNULL(mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant ddl dag monitor mgr is null", K(tenant_id));
      } else if (OB_FAIL(mgr->get_all_nodes(nodes_))) {
        LOG_WARN("failed to get all nodes", K(ret), K(tenant_id));
      } else {
        LOG_DEBUG("advance to tenant", K(tenant_id), "node_count", nodes_.count());
        break;
      }
    }
  }
  if (OB_SUCC(ret) && tenant_idx_ >= tenant_ids_.count()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAllVirtualDDLDagMonitor::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualDDLDagMonitor not inited", K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  }
  while (OB_SUCC(ret) && OB_ISNULL(row)) {
    // Nested iteration: node -> infos
    if (node_idx_ >= 0 && node_idx_ < nodes_.count()
        && info_idx_ >= 0 && info_idx_ < task_infos_.count()) {
      ObDDLDagMonitorNode *node = nodes_.at(node_idx_);
      ObDDLDagMonitorInfo *task_info = task_infos_.at(info_idx_);
      if (OB_FAIL(fill_cells(node, task_info))) {
        LOG_WARN("failed to fill cells", K(ret), K(node_idx_), K(info_idx_), KPC(node), KPC(task_info));
      } else {
        row = &cur_row_;
        ++info_idx_;
      }
    } else {
      while (OB_SUCC(ret) && ++node_idx_ < nodes_.count()) {
        ObDDLDagMonitorNode *node = nodes_.at(node_idx_);
        if (OB_ISNULL(node)) {
          // skip
        } else if (OB_FAIL(node->get_all_infos(task_infos_))) {
          LOG_WARN("failed to get node infos", K(ret), K(node_idx_));
        } else if (task_infos_.count() <= 0) {
          // skip
        } else if (OB_FAIL(fill_cells(node, task_infos_.at(0)))) {
          LOG_WARN("failed to fill cells", K(ret), K(node_idx_), "info_idx", 0);
        } else {
          row = &cur_row_;
          info_idx_ = 1;
          break;
        }
      }
      if (OB_SUCC(ret) && node_idx_ >= nodes_.count()) {
        if (OB_FAIL(advance_to_next_tenant())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to advance to next tenant", K(ret), K(tenant_ids_), K(tenant_idx_));
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDDLDagMonitor::fill_cells(storage::ObDDLDagMonitorNode *node,
                                                 storage::ObDDLDagMonitorInfo *task_info)
{
  int ret = OB_SUCCESS;
  entry_.reuse();
  if (OB_ISNULL(node) || OB_ISNULL(task_info) || OB_ISNULL(cur_row_.cells_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(node), KP(task_info), KP(cur_row_.cells_));
  } else if (cur_row_.count_ < output_column_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cell count is not enough", K(ret), "cell_count", cur_row_.count_, "output_col_cnt", output_column_ids_.count());
  } else if (OB_FAIL(task_info->convert_to_monitor_entry(entry_))) {
    LOG_WARN("convert to monitor entry failed", K(ret), KPC(node), KPC(task_info));
  } else {
    // Node-level fields should be consistent for all task rows.
    IGNORE_RETURN entry_.set_dag_id(node->get_dag_ptr());
    // If task-side didn't set trace_id, fill them from node.
    if (entry_.get_trace_id().empty()) {
      IGNORE_RETURN entry_.set_trace_id(node->get_trace_id());
    }

    const int64_t col_count = output_column_ids_.count();
    ObObj *cells = cur_row_.cells_;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      const uint64_t col_id = output_column_ids_.at(i);
      ObObj &cell = cells[i];
      switch (col_id) {
        case SVR_IP: {
          cell.set_varchar(ip_buf_);
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cell.set_int(addr_.get_port());
          break;
        }
        case TENANT_ID: {
          const uint64_t tenant_id = (tenant_idx_ >= 0 && tenant_idx_ < tenant_ids_.count()) ? tenant_ids_.at(tenant_idx_) : OB_INVALID_TENANT_ID;
          cell.set_int(tenant_id);
          break;
        }
        case DAG_ID: {
          cell.set_varchar(entry_.get_dag_id());
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case DAG_INFO: {
          cell.set_varchar(entry_.get_dag_info());
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TASK_ID: {
          cell.set_varchar(entry_.get_task_id());
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TASK_INFO: {
          cell.set_varchar(entry_.get_task_info());
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case FORMAT_VERSION: {
          cell.set_int(entry_.get_format_version());
          break;
        }
        case TRACE_ID: {
          cell.set_varchar(entry_.get_trace_id());
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case CREATE_TIME: {
          cell.set_timestamp(entry_.get_create_time());
          break;
        }
        case FINISH_TIME: {
          cell.set_timestamp(entry_.get_finish_time());
          break;
        }
        case MESSAGE: {
          // Column `message` is defined as LONGTEXT, so we must output a lob value here.
          const ObString &msg = entry_.get_message();
          if (msg.empty()) {
            cell.set_lob_value(ObLongTextType, "", 0);
          } else {
            cell.set_lob_value(ObLongTextType, msg.ptr(), static_cast<int32_t>(msg.length()));
          }
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(col_id));
          break;
        }
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
