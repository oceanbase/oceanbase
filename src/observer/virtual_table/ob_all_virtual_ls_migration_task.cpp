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

#include "observer/virtual_table/ob_all_virtual_ls_migration_task.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualLSMigrationTask::ObAllVirtualLSMigrationTask()
    : ObVirtualTableScannerIterator(),
      addr_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      ls_iter_guard_(),
      ip_buf_{0},
      task_id_buf_{0},
      config_version_buf_{0},
      src_ip_port_buf_{0},
      dest_ip_port_buf_{0},
      data_src_ip_port_buf_{0}
{
}

ObAllVirtualLSMigrationTask::~ObAllVirtualLSMigrationTask()
{
  reset();
}

void ObAllVirtualLSMigrationTask::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_iter_guard_.reset();
  MEMSET(ip_buf_, 0, common::OB_IP_STR_BUFF);
  MEMSET(task_id_buf_, 0, common::OB_TRACE_STAT_BUFFER_SIZE);
  MEMSET(config_version_buf_, 0, palf::LogConfigVersion::CONFIG_VERSION_LEN);
  MEMSET(src_ip_port_buf_, 0, common::MAX_IP_PORT_LENGTH);
  MEMSET(dest_ip_port_buf_, 0, common::MAX_IP_PORT_LENGTH);
  MEMSET(data_src_ip_port_buf_, 0, common::MAX_IP_PORT_LENGTH);
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualLSMigrationTask::release_last_tenant()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_iter_guard_.reset();
}

int ObAllVirtualLSMigrationTask::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "execute fail", KR(ret));
    }
  }
  return ret;
}

bool ObAllVirtualLSMigrationTask::is_need_process(uint64_t tenant_id)
{
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    return true;
  }
  return false;
}

int ObAllVirtualLSMigrationTask::next_migration_task_(
    ObLSMigrationTask &task,
    ObLSMigrationHandlerStatus &status)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSMigrationHandler *migration_handler = nullptr;
  do {
    if (OB_FAIL(ls_iter_guard_->get_next(ls))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "get_next_ls failed", K(ret));
      }
    } else if (NULL == ls) {
      SERVER_LOG(WARN, "ls shouldn't NULL here", K(ls));
      // try another ls
      ret = OB_EAGAIN;
    } else if (FALSE_IT(ls_id_ = ls->get_ls_id().id())) {
    } else if (FALSE_IT(migration_handler = ls->get_ls_migration_handler())) {
    } else if (NULL == migration_handler) {
      SERVER_LOG(WARN, "migration_handler shouldn't NULL here", K(ls));
      // try another ls
      ret = OB_EAGAIN;
    } else if (OB_FAIL(migration_handler->get_migration_task_and_handler_status(task, status))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        SERVER_LOG(WARN, "get_migration_task_and_handler_status failed", K(ret), K(ls));
      } else {
        // try another ls
        ret = OB_EAGAIN;
      }
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

int ObAllVirtualLSMigrationTask::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObLSMigrationTask task;
  ObLSMigrationHandlerStatus status = ObLSMigrationHandlerStatus::MAX_STATUS;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (ls_iter_guard_.get_ptr() == nullptr && OB_FAIL(MTL(ObLSService*)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "get_ls_iter fail", K(ret));
  } else if (OB_FAIL(next_migration_task_(task, status))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get next_migration_task_ failed", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // tenant_id
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // ls_id
          cur_row_.cells_[i].set_int(ls_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 4: {
          // type
          cur_row_.cells_[i].set_varchar(ObMigrationOpType::get_str(task.arg_.type_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 5:
          // status
          cur_row_.cells_[i].set_varchar(ObLSMigrationHandlerStatusHelper::get_status_str(status));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 6: {
          // task_id
          task.task_id_.to_string(task_id_buf_, OB_TRACE_STAT_BUFFER_SIZE);
          cur_row_.cells_[i].set_varchar(task_id_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 7:
          // priority
          cur_row_.cells_[i].set_int(task.arg_.priority_);
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // config_version
          task.arg_.member_list_config_version_.to_string(config_version_buf_, 128);
          cur_row_.cells_[i].set_varchar(config_version_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // src
          if (OB_FAIL(task.arg_.src_.get_server().ip_port_to_string(src_ip_port_buf_, MAX_IP_PORT_LENGTH))) {
            SERVER_LOG(WARN, "failed to print ip port", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(src_ip_port_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // dst
          if (OB_FAIL(task.arg_.dst_.get_server().ip_port_to_string(dest_ip_port_buf_, MAX_IP_PORT_LENGTH))) {
            SERVER_LOG(WARN, "failed to print ip port", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(dest_ip_port_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // data_src
          if (OB_FAIL(task.arg_.data_src_.get_server().ip_port_to_string(data_src_ip_port_buf_, MAX_IP_PORT_LENGTH))) {
            SERVER_LOG(WARN, "failed to print ip port", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(data_src_ip_port_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // paxos_replica_number
          cur_row_.cells_[i].set_int(task.arg_.paxos_replica_number_);
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // prioritize_same_zone_src
          cur_row_.cells_[i].set_varchar(task.arg_.prioritize_same_zone_src_ ? "YES" : "NO");
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

} // observer
} // oceanbase
