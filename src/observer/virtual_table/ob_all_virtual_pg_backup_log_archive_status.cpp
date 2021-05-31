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

#include "ob_all_virtual_pg_backup_log_archive_status.h"
#include "storage/ob_partition_service.h"  // ObPartitionService
#include "clog/ob_log_define.h"
#include "storage/ob_partition_group.h"  // ObIPartitionGroup

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

namespace oceanbase {
namespace observer {
ObAllVirtualPGBackupLogArchiveStatus::ObAllVirtualPGBackupLogArchiveStatus()
    : ObVirtualTableIterator(), is_inited_(false), ps_(NULL), addr_(), ptt_iter_(NULL)
{}

ObAllVirtualPGBackupLogArchiveStatus::~ObAllVirtualPGBackupLogArchiveStatus()
{
  reset();
}

void ObAllVirtualPGBackupLogArchiveStatus::reset()
{
  if (NULL != ptt_iter_) {
    if (NULL == ps_) {
      SERVER_LOG(ERROR, "partition_service_ is null");
    } else {
      ps_->revert_pg_iter(ptt_iter_);
      ptt_iter_ = NULL;
    }
  }
  is_inited_ = false;
  addr_.reset();
  ps_ = NULL;
  ip_buf_[0] = '\0';
}

int ObAllVirtualPGBackupLogArchiveStatus::init(storage::ObPartitionService* partition_service, common::ObAddr& addr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualPGBackupLogArchiveStatus has been inited", K(ret));
  } else {
    ps_ = partition_service;
    addr_ = addr;
    (void)addr_.ip_to_string(ip_buf_, sizeof(ip_buf_));
    is_inited_ = true;
  }

  return ret;
}

int ObAllVirtualPGBackupLogArchiveStatus::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  while (OB_FAIL(inner_get_next_row_(row)) && OB_EAGAIN == ret) {
    // continue
  }
  return ret;
}

int ObAllVirtualPGBackupLogArchiveStatus::inner_get_next_row_(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  clog::ObPGLogArchiveStatus log_archive_status;

  if (!is_inited_ || NULL == allocator_ || NULL == ps_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualPGBackupLogArchiveStatus not init", K(is_inited_), K(allocator_), K(ps_), K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells of cur row is NULL", K(ret));
  } else if (NULL == ptt_iter_ && NULL == (ptt_iter_ = ps_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "fail to alloc partition iter", K(ret));
  } else if (OB_FAIL(ptt_iter_->get_next(partition))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "scan next partition failed", K(ret));
    }
  } else if (NULL == partition) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "get partition failed", K(ret));
  } else if (OB_SYS_TENANT_ID == partition->get_partition_key().get_tenant_id()) {
    // skip sys tenant
    ret = OB_EAGAIN;
  } else if (OB_FAIL(partition->get_log_archive_status(log_archive_status))) {
    ARCHIVE_LOG(WARN, "get_log_archive_status fail", K(ret));
    ret = OB_ITER_END;
    // TODO: rewrite ret
  } else {
    const int64_t col_count = output_column_ids_.count();
    const ObPGKey pkey = partition->get_partition_key();
    uint64_t max_log_id = OB_INVALID_ID;
    int64_t max_log_ts = OB_INVALID_TIMESTAMP;
    partition->get_log_service()->get_last_replay_log(max_log_id, max_log_ts);

    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP:
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case TENANT_ID:
          cur_row_.cells_[i].set_int(extract_tenant_id(pkey.table_id_));
          break;
        case TABLE_ID:
          cur_row_.cells_[i].set_int(pkey.table_id_);
          break;
        case PARTITION_ID:
          cur_row_.cells_[i].set_int(pkey.get_partition_id());
          break;
        case INCARNATION:
          cur_row_.cells_[i].set_int(log_archive_status.archive_incarnation_);
          break;
        case LOG_ARCHIVE_ROUND:
          cur_row_.cells_[i].set_int(log_archive_status.log_archive_round_);
          break;
        case START_TS:
          cur_row_.cells_[i].set_int(log_archive_status.round_start_ts_);
          break;
        case STATUS:
          cur_row_.cells_[i].set_int((int64_t)log_archive_status.status_);
          break;
        case LAST_ARCHIVED_LOG_ID:
          cur_row_.cells_[i].set_int(log_archive_status.last_archived_log_id_);
          break;
        case LAST_ARCHIVED_LOG_TS:
          cur_row_.cells_[i].set_int(log_archive_status.last_archived_checkpoint_ts_);
          break;
        case MAX_LOG_ID:
          cur_row_.cells_[i].set_int(max_log_id);
          break;
        case MAX_LOG_TS:
          cur_row_.cells_[i].set_int(max_log_ts);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  } else if (OB_EAGAIN != ret) {
    // revert ptt_iter_ no matter ret is OB_ITER_END or other errors
    if (NULL != ps_ && NULL != ptt_iter_) {
      ps_->revert_pg_iter(ptt_iter_);
      ptt_iter_ = NULL;
    }
  }

  return ret;
}

const char* ObAllVirtualPGBackupLogArchiveStatus::get_log_archive_status_str_(ObLogArchiveStatus::STATUS status)
{
  const char* status_str = "MAX";

  switch (status) {
    case ObLogArchiveStatus::STATUS::STOP:
      status_str = "STOP";
      break;
    case ObLogArchiveStatus::STATUS::BEGINNING:
      status_str = "BEGINNING";
      break;
    case ObLogArchiveStatus::STATUS::DOING:
      status_str = "DOING";
      break;
    case ObLogArchiveStatus::STATUS::STOPPING:
      status_str = "STOPPING";
      break;
    case ObLogArchiveStatus::STATUS::INTERRUPTED:
      status_str = "INTERRUPTED";
      break;
    case ObLogArchiveStatus::STATUS::MIXED:
      status_str = "MIXED";
      break;
    default:
      ARCHIVE_LOG(WARN, "unkown status type");
  }

  return status_str;
}

}  // namespace observer
}  // namespace oceanbase
