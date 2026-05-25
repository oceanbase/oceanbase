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

#include "observer/virtual_table/ob_all_virtual_sandbox_process.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace observer
{

namespace
{
const char *sandbox_state_to_str(const SandboxState state)
{
  const char *state_str = "UNKNOWN";
  switch (state) {
    case SandboxState::STATE_UNKNOWN:
      state_str = "UNKNOWN";
      break;
    case SandboxState::STATE_IDLE:
      state_str = "IDLE";
      break;
    case SandboxState::STATE_RUNNING:
      state_str = "RUNNING";
      break;
    case SandboxState::STATE_EXITED:
      state_str = "EXITED";
      break;
    default:
      state_str = "UNKNOWN";
      break;
  }
  return state_str;
}
} // namespace

ObAllVirtualSandboxProcess::ObAllVirtualSandboxProcess()
  : ObVirtualTableScannerIterator(),
    is_inited_(false),
    next_idx_(0),
    snapshots_(),
    ip_buf_(),
    process_state_buf_()
{
}

ObAllVirtualSandboxProcess::~ObAllVirtualSandboxProcess()
{
  reset();
}

int ObAllVirtualSandboxProcess::inner_open()
{
  int ret = OB_SUCCESS;
  if (!ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip to string failed", K(ret));
  }
  return ret;
}

void ObAllVirtualSandboxProcess::reset()
{
  is_inited_ = false;
  next_idx_ = 0;
  snapshots_.reset();
  ip_buf_[0] = '\0';
  process_state_buf_[0] = '\0';
  process_state_buf_[1] = '\0';
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSandboxProcess::fill_row_(const ObSandboxProcessSnapshot &snapshot, common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  common::ObCollationType default_collation =
      common::ObCharset::get_default_collation(common::ObCharset::get_default_charset());
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    const uint64_t col_id = output_column_ids_.at(i);
    common::ObObj *cells = cur_row_.cells_;
    switch (col_id) {
      case SVR_IP: {
        cells[i].set_varchar(ip_buf_);
        cells[i].set_collation_type(default_collation);
        break;
      }
      case SVR_PORT: {
        cells[i].set_int(GCONF.self_addr_.get_port());
        break;
      }
      case TENANT_ID: {
        cells[i].set_int(snapshot.get_tenant_id());
        break;
      }
      case PID: {
        cells[i].set_int(snapshot.get_pid());
        break;
      }
      case SANDBOX_STATE: {
        cells[i].set_varchar(sandbox_state_to_str(snapshot.get_state()));
        cells[i].set_collation_type(default_collation);
        break;
      }
      case PROCESS_STATE: {
        const char state = snapshot.get_process_state();
        if ('\0' == state) {
          process_state_buf_[0] = '\0';
        } else {
          process_state_buf_[0] = state;
        }
        process_state_buf_[1] = '\0';
        cells[i].set_varchar(process_state_buf_);
        cells[i].set_collation_type(default_collation);
        break;
      }
      case PROCESS_NAME: {
        cells[i].set_varchar(snapshot.get_process_name());
        cells[i].set_collation_type(default_collation);
        break;
      }
      case CPU_USAGE: {
        cells[i].set_double(snapshot.get_cpu_usage());
        break;
      }
      case CPU_TIME: {
        cells[i].set_int(snapshot.get_cpu_time());
        break;
      }
      case MEMORY_USAGE: {
        cells[i].set_int(snapshot.get_memory_usage());
        break;
      }
      case START_TIME: {
        cells[i].set_timestamp(snapshot.get_start_time());
        break;
      }
      case EXECUTE_PATH: {
        cells[i].set_varchar(snapshot.get_execute_path());
        cells[i].set_collation_type(default_collation);
        break;
      }
      case ROOT_PATH: {
        cells[i].set_varchar(snapshot.get_root_path());
        cells[i].set_collation_type(default_collation);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected column id", K(ret), K(col_id), K(i));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualSandboxProcess::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (!is_inited_) {
    if (OB_FAIL(ObSandboxManager::get_instance().get_sandbox_process_snapshots(snapshots_))) {
      SERVER_LOG(WARN, "get sandbox process snapshots failed", K(ret));
    } else {
      is_inited_ = true;
      next_idx_ = 0;
    }
  }
  if (OB_SUCC(ret)) {
    if (next_idx_ >= snapshots_.count()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(fill_row_(snapshots_.at(next_idx_), row))) {
      SERVER_LOG(WARN, "fill sandbox process row failed", K(ret), K(next_idx_));
    } else {
      ++next_idx_;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
