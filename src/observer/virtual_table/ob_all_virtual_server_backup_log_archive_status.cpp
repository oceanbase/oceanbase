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

#include "ob_all_virtual_server_backup_log_archive_status.h"
#include "storage/ob_partition_service.h"  // ObPartitionService
#include "clog/ob_log_define.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

namespace oceanbase {
namespace observer {
ObAllVirtualServerBackupLogArchiveStatus::ObAllVirtualServerBackupLogArchiveStatus()
    : is_inited_(false),
      has_reported_(false),
      ps_(NULL),
      iter_(NULL),
      addr_(),
      incarnation_(0),
      round_(0),
      pg_count_(0),
      start_archive_time_(OB_INVALID_TIMESTAMP),
      max_archive_progress_(OB_INVALID_TIMESTAMP)
{
  memset(ip_buf_, 0, common::OB_IP_STR_BUFF);
}

ObAllVirtualServerBackupLogArchiveStatus::~ObAllVirtualServerBackupLogArchiveStatus()
{
  finish_read_();
}

int ObAllVirtualServerBackupLogArchiveStatus::init(storage::ObPartitionService* partition_service, common::ObAddr& addr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualServerBackupLogArchiveStatus has been inited", K(ret));
  } else {
    ps_ = partition_service;
    addr_ = addr;
    (void)addr_.ip_to_string(ip_buf_, sizeof(ip_buf_));
    is_inited_ = true;
  }

  return ret;
}

void ObAllVirtualServerBackupLogArchiveStatus::finish_read_()
{
  if (NULL != iter_) {
    if (NULL == ps_) {
      SERVER_LOG(ERROR, "partition_service_ is null");
    } else {
      ps_->revert_pg_iter(iter_);
      iter_ = NULL;
    }
  }
  is_inited_ = false;
  addr_.reset();
  ps_ = NULL;
  memset(ip_buf_, 0, common::OB_IP_STR_BUFF);
}

int ObAllVirtualServerBackupLogArchiveStatus::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionGroupIterator* iter = NULL;
  int64_t start_archive_time = OB_INVALID_TIMESTAMP;
  int64_t max_archive_progress = OB_INVALID_TIMESTAMP;
  // two different incarnation or round should not exist

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualServerBackupLogArchiveStatus not init", K(ret));
  } else if (has_reported_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(prepare_to_read_())) {
    SERVER_LOG(WARN, "prepare_to_read_", K(ret));
  } else if (OB_FAIL(fill_rows_())) {
    SERVER_LOG(WARN, "fill_rows_ fail", K(ret));
  } else {
    row = &cur_row_;
    has_reported_ = true;
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

int ObAllVirtualServerBackupLogArchiveStatus::prepare_to_read_()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  clog::ObPGLogArchiveStatus log_archive_status;

  if (OB_ISNULL(ps_) || OB_ISNULL(cur_row_.cells_)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid arguments", K(ret), K(ps_), K(cur_row_.cells_));
  } else if (OB_ISNULL(iter_ = ps_->alloc_pg_iter())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "scan next partition failed", K(ret));
    }
  } else {
    bool archive_progress_init = false;
    while (OB_SUCC(ret)) {
      clog::ObPGLogArchiveStatus log_archive_status;
      partition = NULL;
      if (OB_FAIL(iter_->get_next(partition))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "scan next partition failed", K(ret));
        }
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "get partition failed", K(ret));
      } else if (OB_SYS_TENANT_ID == partition->get_partition_key().get_tenant_id()) {
        // skip sys tenant
      } else if (OB_FAIL(partition->get_log_archive_status(log_archive_status))) {
        SERVER_LOG(WARN, "get_log_archive_status fail", K(ret));
      } else {
        start_archive_time_ = std::max(start_archive_time_, log_archive_status.round_start_ts_);
        pg_count_++;

        if (!archive_progress_init) {
          max_archive_progress_ = log_archive_status.last_archived_checkpoint_ts_;
          archive_progress_init = true;
        } else {
          max_archive_progress_ = std::min(max_archive_progress_, log_archive_status.last_archived_checkpoint_ts_);
        }

        if (0 != incarnation_ && log_archive_status.archive_incarnation_ != incarnation_) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "exist different incarnation in log archive tasks", K(incarnation_), K(log_archive_status));
        } else if (log_archive_status.archive_incarnation_ != incarnation_) {
          incarnation_ = log_archive_status.archive_incarnation_;
        }

        if (0 != round_ && log_archive_status.log_archive_round_ != round_) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "exist different round in log archive tasks", K(round_), K(log_archive_status));
        } else {
          round_ = log_archive_status.log_archive_round_;
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObAllVirtualServerBackupLogArchiveStatus::fill_rows_()
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();

  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case SVR_IP:
        cur_row_.cells_[i].set_varchar(ip_buf_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case SVR_PORT:
        cur_row_.cells_[i].set_int(addr_.get_port());
        break;
      case INCARNATION:
        cur_row_.cells_[i].set_int(incarnation_);
        break;
      case LOG_ARCHIVE_ROUND:
        cur_row_.cells_[i].set_int(round_);
        break;
      case START_TS:
        cur_row_.cells_[i].set_int(start_archive_time_);
        break;
      case CUR_LOG_ARCHIVE_PROGRESS:
        cur_row_.cells_[i].set_int(max_archive_progress_);
        break;
      case PG_COUNT:
        cur_row_.cells_[i].set_int(pg_count_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "invalid column id", K(ret), K(col_id));
        break;
    }
  }

  return ret;
}

}  // namespace observer
}  // namespace oceanbase
