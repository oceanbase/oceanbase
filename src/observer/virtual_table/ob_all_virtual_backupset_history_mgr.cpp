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

#include "observer/virtual_table/ob_all_virtual_backupset_history_mgr.h"
#include "observer/ob_server.h"
#include "share/backup/ob_tenant_backup_task_updater.h"

using namespace oceanbase;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace observer;

ObAllVirtualBackupSetHistoryMgr::ObAllVirtualBackupSetHistoryMgr()
    : ObVirtualTableScannerIterator(),
      is_inited_(false),
      res_(),
      result_(NULL),
      backup_recovery_window_(0),
      tenant_backup_task_info_()
{
  MEMSET(backup_dest_str_, 0, sizeof(backup_dest_str_));
  MEMSET(cluster_version_display_, 0, sizeof(cluster_version_display_));
}

ObAllVirtualBackupSetHistoryMgr::~ObAllVirtualBackupSetHistoryMgr()
{
  reset();
}

void ObAllVirtualBackupSetHistoryMgr::reset()
{
  ObVirtualTableScannerIterator::reset();
  is_inited_ = false;
  res_.reset();
  result_ = NULL;
  backup_recovery_window_ = 0;
}

int ObAllVirtualBackupSetHistoryMgr::init(common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const int64_t tenant_id = OB_SYS_TENANT_ID;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s ", OB_ALL_BACKUP_TASK_HISTORY_TNAME))) {
    SERVER_LOG(WARN, "fail to assign sql", K(ret));
  } else if (OB_FAIL(sql_proxy.read(res_, tenant_id, sql.ptr()))) {
    SERVER_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result_ = res_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "error unexpected, query result must not be NULL", K(ret));
  } else {
    backup_recovery_window_ = GCONF.backup_recovery_window;
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualBackupSetHistoryMgr::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObObj* cells = cur_row_.cells_;
  tenant_backup_task_info_.reset();

  MEMSET(backup_dest_str_, 0, sizeof(backup_dest_str_));
  MEMSET(cluster_version_display_, 0, sizeof(cluster_version_display_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(result_->next())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "failed to get next result", K(ret));
    }
  } else if (OB_FAIL(ObITenantBackupTaskOperator::extract_tenant_backup_task(result_, tenant_backup_task_info_))) {
    SERVER_LOG(WARN, "failed to extract tenant backup task", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID: {
          cells[i].set_int(tenant_backup_task_info_.tenant_id_);
          break;
        }
        case INCARNATION: {
          cells[i].set_int(tenant_backup_task_info_.incarnation_);
          break;
        }
        case BACKUP_SET_ID: {
          cells[i].set_int(tenant_backup_task_info_.backup_set_id_);
          break;
        }
        case BACKUP_TYPE: {
          cells[i].set_varchar(tenant_backup_task_info_.backup_type_.get_backup_type_str());
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case DEVICE_TYPE: {
          cells[i].set_varchar(get_storage_type_str(tenant_backup_task_info_.device_type_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SNAPSHOT_VERSION: {
          cells[i].set_int(tenant_backup_task_info_.snapshot_version_);
          break;
        }
        case PREV_FULL_BACKUP_SET_ID: {
          cells[i].set_int(tenant_backup_task_info_.prev_full_backup_set_id_);
          break;
        }
        case PREV_INC_BACKUP_SET_ID: {
          cells[i].set_int(tenant_backup_task_info_.prev_inc_backup_set_id_);
          break;
        }
        case PREV_BACKUP_DATA_VERSION: {
          cells[i].set_int(tenant_backup_task_info_.prev_backup_data_version_);
          break;
        }
        case PG_COUNT: {
          cells[i].set_int(tenant_backup_task_info_.pg_count_);
          break;
        }
        case MACRO_BLOCK_COUNT: {
          cells[i].set_int(tenant_backup_task_info_.macro_block_count_);
          break;
        }
        case FINISH_PG_COUNT: {
          cells[i].set_int(tenant_backup_task_info_.finish_pg_count_);
          break;
        }
        case FINISH_MACRO_BLOCK_COUNT: {
          cells[i].set_int(tenant_backup_task_info_.finish_macro_block_count_);
          break;
        }
        case INPUT_BYTES: {
          cells[i].set_int(tenant_backup_task_info_.input_bytes_);
          break;
        }
        case OUTPUT_BYTES: {
          cells[i].set_int(tenant_backup_task_info_.output_bytes_);
          break;
        }
        case START_TIME: {
          cells[i].set_timestamp(tenant_backup_task_info_.start_time_);
          break;
        }
        case END_TIME: {
          cells[i].set_timestamp(tenant_backup_task_info_.end_time_);
          break;
        }
        case COMPATIBLE: {
          cells[i].set_int(tenant_backup_task_info_.compatible_);
          break;
        }
        case CLUSTER_VERSION: {
          cells[i].set_int(tenant_backup_task_info_.cluster_version_);
          break;
        }
        case STATUS: {
          cells[i].set_varchar(tenant_backup_task_info_.get_backup_task_status_str());
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case RESULT: {
          cells[i].set_int(tenant_backup_task_info_.result_);
          break;
        }
        case CLUSTER_ID: {
          cells[i].set_int(tenant_backup_task_info_.cluster_id_);
          break;
        }
        case BACKUP_DEST: {
          if (OB_FAIL(tenant_backup_task_info_.backup_dest_.get_backup_dest_str(
                  backup_dest_str_, share::OB_MAX_BACKUP_DEST_LENGTH))) {
            SERVER_LOG(WARN, "failed to get backup dest str", K(ret), K(tenant_backup_task_info_));
          } else {
            cells[i].set_varchar(backup_dest_str_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case BACKUP_DATA_VERSION: {
          cells[i].set_int(tenant_backup_task_info_.backup_data_version_);
          break;
        }
        case BACKUP_SCHMEA_VERSION: {
          cells[i].set_int(tenant_backup_task_info_.backup_schema_version_);
          break;
        }
        case CLUSTER_VERSION_DISPLAY: {
          const int64_t pos = ObClusterVersion::get_instance().print_vsn(cluster_version_display_,
              OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH,
              tenant_backup_task_info_.cluster_version_);
          if (pos <= 0) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "failed to print vsn", K(ret), K(pos), K(tenant_backup_task_info_));
          } else {
            cells[i].set_varchar(cluster_version_display_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case PARTITION_COUNT: {
          cells[i].set_int(tenant_backup_task_info_.partition_count_);
          break;
        }
        case FINISH_PARTITION_COUNT: {
          cells[i].set_int(tenant_backup_task_info_.finish_partition_count_);
          break;
        }
        case IS_MARK_DELETED: {
          cells[i].set_bool(tenant_backup_task_info_.is_mark_deleted_);
          break;
        }
        case ENCRYPTION_MODE: {
          cells[i].set_varchar(ObBackupEncryptionMode::to_str(tenant_backup_task_info_.encryption_mode_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case PASSWD: {
          cells[i].set_varchar(tenant_backup_task_info_.passwd_.ptr());
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case BACKUP_RECOVERY_WINDOW: {
          cells[i].set_int(backup_recovery_window_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
        }  // default
      }    // switch
    }      // for
  }        // else
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}
