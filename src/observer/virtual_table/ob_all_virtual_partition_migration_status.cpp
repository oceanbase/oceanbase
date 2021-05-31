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

#include "ob_all_virtual_partition_migration_status.h"
#include "observer/ob_server.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
using namespace common;
using namespace storage;

namespace observer {
ObAllVirtualPartitionMigrationStatus::ObAllVirtualPartitionMigrationStatus()
    : iter_(),
      task_id_(),
      svr_ip_(),
      parent_svr_ip_(),
      src_svr_ip_(),
      dest_svr_ip_(),
      replica_state_(),
      local_versions_(),
      target_(),
      comment_()
{}

ObAllVirtualPartitionMigrationStatus::~ObAllVirtualPartitionMigrationStatus()
{}

int ObAllVirtualPartitionMigrationStatus::init(ObPartitionMigrationStatusMgr& status_mgr)
{
  int ret = OB_SUCCESS;

  if (start_to_read_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_FAIL(status_mgr.get_iter(iter_))) {
    SERVER_LOG(WARN, "failed to get iter", K(ret));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualPartitionMigrationStatus::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObPartitionMigrationStatus status;
  row = NULL;

  if (!start_to_read_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(ret));
  } else if (!iter_.is_ready()) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "iter not ready", K(ret));
  } else if (OB_FAIL(iter_.get_next(status))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "failed to get next status", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    ObCollationType collcation_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TASK_ID: {
          // task_id
          int64_t n = status.task_id_.to_string(task_id_, sizeof(task_id_));
          if (n < 0 || n >= sizeof(task_id_)) {
            ret = OB_BUF_NOT_ENOUGH;
            SERVER_LOG(WARN, "buffer not enough", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(task_id_);
            cur_row_.cells_[i].set_collation_type(collcation_type);
          }
          break;
        }
        case TENANT_ID: {
          // tenant_id
          cur_row_.cells_[i].set_int(status.pkey_.get_tenant_id());
          break;
        }
        case TABLE_ID: {
          // table_id
          cur_row_.cells_[i].set_int(status.pkey_.get_table_id());
          break;
        }
        case PARTITION_IDX: {
          // partition_idx
          cur_row_.cells_[i].set_int(status.pkey_.get_partition_id());
          break;
        }
        case SVR_IP: {
          // svr_ip
          if (!OBSERVER.get_self().ip_to_string(svr_ip_, sizeof(svr_ip_))) {
            ret = OB_BUF_NOT_ENOUGH;
            SERVER_LOG(WARN, "buffer not enough", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(svr_ip_);
            cur_row_.cells_[i].set_collation_type(collcation_type);
          }
          break;
        }
        case SVR_PORT: {
          // svr_port
          cur_row_.cells_[i].set_int(OBSERVER.get_self().get_port());
          break;
        }
        case MIGRATE_TYPE: {
          cur_row_.cells_[i].set_varchar(status.migrate_type_);
          cur_row_.cells_[i].set_collation_type(collcation_type);
          break;
        }
        case PARENT_IP: {
          // parent_svr_ip
          if (!status.clog_parent_.ip_to_string(parent_svr_ip_, sizeof(parent_svr_ip_))) {
            ret = OB_BUF_NOT_ENOUGH;
            SERVER_LOG(WARN, "buffer not enough", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(parent_svr_ip_);
            cur_row_.cells_[i].set_collation_type(collcation_type);
          }
          break;
        }
        case PARENT_PORT: {
          // parent_svr_port
          cur_row_.cells_[i].set_int(status.clog_parent_.get_port());
          break;
        }
        case SRC_IP: {
          // src_svr_ip
          if (!status.src_.ip_to_string(src_svr_ip_, sizeof(src_svr_ip_))) {
            ret = OB_BUF_NOT_ENOUGH;
            SERVER_LOG(WARN, "buffer not enough", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(src_svr_ip_);
            cur_row_.cells_[i].set_collation_type(collcation_type);
          }
          break;
        }
        case SRC_PORT: {
          // src_svr_port
          cur_row_.cells_[i].set_int(status.src_.get_port());
          break;
        }
        case DEST_IP: {
          // dest_svr_ip
          if (!status.dest_.ip_to_string(dest_svr_ip_, sizeof(dest_svr_ip_))) {
            ret = OB_BUF_NOT_ENOUGH;
            SERVER_LOG(WARN, "buffer not enough", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(dest_svr_ip_);
            cur_row_.cells_[i].set_collation_type(collcation_type);
          }
          break;
        }
        case DEST_PORT: {
          // dest_svr_port
          cur_row_.cells_[i].set_int(status.dest_.get_port());
          break;
        }
        case RESULT: {
          // result
          cur_row_.cells_[i].set_int(status.result_);
          break;
        }
        case START_TIME: {
          // start_time
          cur_row_.cells_[i].set_timestamp(status.start_time_);
          break;
        }
        case FINISH_TIME: {
          // finish_time
          cur_row_.cells_[i].set_timestamp(status.finish_time_);
          break;
        }
        case ACTION: {
          // action
          const char* action = ObMigrateCtx::trans_action_to_str(status.action_);
          cur_row_.cells_[i].set_varchar(action);
          cur_row_.cells_[i].set_collation_type(collcation_type);
          break;
        }
        case REPLICA_STATE: {
          // replica_state
          const char* state = partition_replica_state_to_str(status.replica_state_);
          cur_row_.cells_[i].set_varchar(state);
          cur_row_.cells_[i].set_collation_type(collcation_type);
          break;
        }
        case REBUILD_COUNT: {
          // rebuild_count
          cur_row_.cells_[i].set_int(status.rebuild_count_);
          break;
        }
        case TOTAL_MACRO_BLOCK: {
          // total_macro_block
          cur_row_.cells_[i].set_int(status.data_statics_.total_macro_block_);
          break;
        }
        case DONE_MACRO_BLOCK: {
          // done_macro_block
          cur_row_.cells_[i].set_int(status.data_statics_.ready_macro_block_);
          break;
        }
        case MAJOR_COUNT: {
          // major_size
          cur_row_.cells_[i].set_int(status.data_statics_.major_count_);
          break;
        }
        case MINI_MINOR_COUNT: {
          // mini_minor_size
          cur_row_.cells_[i].set_int(status.data_statics_.mini_minor_count_);
          break;
        }
        case NORMAL_MINOR_COUNT: {
          // minor_size
          cur_row_.cells_[i].set_int(status.data_statics_.normal_minor_count_);
          break;
        }
        case BUF_MINOR_COUNT: {
          // buf_minor_size
          cur_row_.cells_[i].set_int(status.data_statics_.buf_minor_count_);
          break;
        }
        case REUSE_COUNT: {
          // copy_size
          cur_row_.cells_[i].set_int(status.data_statics_.reuse_count_);
          break;
        }
        case COMMENT: {
          // comment, ignore ret
          snprintf(comment_, sizeof(comment_), "%s", status.comment_);
          cur_row_.cells_[i].set_varchar(comment_);
          cur_row_.cells_[i].set_collation_type(collcation_type);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "invalid coloum_id", K(ret), K(col_id));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

void ObAllVirtualPartitionMigrationStatus::reset()
{
  iter_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualPartitionMigrationStatus::convert_version_source_string(const common::ObIArray<ObVersion>& version_list,
    const common::ObIArray<ObAddr>& src_list, char* buf, int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (version_list.count() != src_list.count()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(
        WARN, "count mismatch", K(ret), "versions count", version_list.count(), "sources count", src_list.count());
  } else {
    int64_t count = version_list.count();
    J_ARRAY_START();
    for (int64_t index = 0; index < count; ++index) {
      if (pos + MAX_IP_PORT_LENGTH + OB_MAX_VERSION_LENGTH + 10 >= buf_len - 1) {
        BUF_PRINTF(OB_LOG_ELLIPSIS);
        break;
      }
      BUF_PRINTO(version_list.at(index));
      if (src_list.at(index).is_valid()) {
        BUF_PRINTF("@");
        BUF_PRINTO(src_list.at(index));
      }

      if (index != count - 1) {
        J_COMMA();
      }
    }
    J_ARRAY_END();
  }

  return ret;
}

}  // namespace observer
}  // namespace oceanbase
