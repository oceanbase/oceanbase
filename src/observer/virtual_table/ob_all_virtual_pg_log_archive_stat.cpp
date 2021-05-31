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

#include "ob_all_virtual_pg_log_archive_stat.h"
#include "storage/ob_partition_service.h"  // ObPartitionService
#include "clog/ob_log_define.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::archive;

namespace oceanbase {
namespace observer {
void PGLogArchiveStat::reset()
{
  pg_key_.reset();
  incarnation_ = -1;
  round_ = -1;
  epoch_ = -1;
  been_deleted_ = false;
  is_first_record_finish_ = false;
  has_encount_error_ = false;
  current_ilog_id_ = OB_INVALID_FILE_ID;
  max_log_id_ = OB_INVALID_ID;
  round_start_log_id_ = OB_INVALID_ID;
  round_start_log_ts_ = OB_INVALID_TIMESTAMP;
  round_snapshot_version_ = OB_INVALID_TIMESTAMP;
  cur_start_log_id_ = OB_INVALID_ID;
  fetcher_max_split_log_id_ = OB_INVALID_ID;
  clog_max_split_log_id_ = OB_INVALID_ID;
  clog_max_split_log_ts_ = OB_INVALID_TIMESTAMP;
  clog_split_checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  max_archived_log_id_ = OB_INVALID_ID;
  max_archived_log_ts_ = OB_INVALID_TIMESTAMP;
  max_archived_checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  archived_clog_epoch_ = OB_INVALID_TIMESTAMP;
  archived_accum_checksum_ = 0;
  cur_index_file_id_ = 0;
  index_file_offset_ = -1;
  cur_data_file_id_ = 0;
  data_file_offset_ = -1;
}

ObAllVirtualPGLogArchiveStat::ObAllVirtualPGLogArchiveStat()
    : ObVirtualTableIterator(), is_inited_(false), ps_(NULL), addr_(), iter_(NULL)
{}

ObAllVirtualPGLogArchiveStat::~ObAllVirtualPGLogArchiveStat()
{
  reset();
}

void ObAllVirtualPGLogArchiveStat::reset()
{
  if (NULL != iter_) {
    iter_->destroy();
    ob_free(iter_);
    iter_ = NULL;
  }
  is_inited_ = false;
  addr_.reset();
  ps_ = NULL;
  ip_buf_[0] = '\0';
}

int ObAllVirtualPGLogArchiveStat::init(storage::ObPartitionService* partition_service, common::ObAddr& addr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualPGBackupLogArchiveStatus has been inited", K(ret));
  } else if (OB_ISNULL(partition_service)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "partition_service is NULL", K(partition_service));
  } else {
    ps_ = partition_service;
    addr_ = addr;
    if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cannot ip to string", K(ret), K(addr_));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObAllVirtualPGLogArchiveStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  PGLogArchiveStat stat;

  if (!is_inited_ || NULL == allocator_ || NULL == ps_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualPGLogArchiveStat not init", K(is_inited_), K(allocator_), K(ps_));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells of cur row is NULL", K(ret));
  } else if (NULL == iter_ && OB_FAIL(get_iter_())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "fail to get_iter_", K(ret));
  } else if (OB_FAIL(get_log_archive_stat_(stat))) {
    SERVER_LOG(WARN, "get_log_archive_stat_ fail", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    const ObPGKey pkey = stat.pg_key_;
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
          cur_row_.cells_[i].set_int(stat.incarnation_);
          break;
        case LOG_ARCHIVE_ROUND:
          cur_row_.cells_[i].set_int(stat.round_);
          break;
        case EPOCH:
          cur_row_.cells_[i].set_int(stat.epoch_);
          break;
        case BEEN_DELETE_FLAG:
          cur_row_.cells_[i].set_bool(stat.been_deleted_);
          break;
        case FIRST_RECORD_FINISH_FLAG:
          cur_row_.cells_[i].set_bool(stat.is_first_record_finish_);
          break;
        case ENCOUNTER_ERROR_FLAG:
          cur_row_.cells_[i].set_bool(stat.has_encount_error_);
          break;
        case CUR_ILOG_ID:
          cur_row_.cells_[i].set_int(stat.current_ilog_id_);
          break;
        case MAX_LOG_ID:
          cur_row_.cells_[i].set_int(stat.max_log_id_);
          break;
        case ROUND_START_LOG_ID:
          cur_row_.cells_[i].set_int(stat.round_start_log_id_);
          break;
        case ROUND_START_LOG_TS:
          cur_row_.cells_[i].set_int(stat.round_start_log_ts_);
          break;
        case ROUND_SNAPSHOT_VERSION:
          cur_row_.cells_[i].set_int(stat.round_snapshot_version_);
          break;
        case CUR_EPOCH_START_LOG_ID:
          cur_row_.cells_[i].set_int(stat.cur_start_log_id_);
          break;
        case FETCHER_MAX_SPLIT_LOG_ID:
          cur_row_.cells_[i].set_int(stat.fetcher_max_split_log_id_);
          break;
        case CLOG_MAX_SPLIT_LOG_ID:
          cur_row_.cells_[i].set_int(stat.clog_max_split_log_id_);
          break;
        case CLOG_MAX_SPLIT_LOG_TS:
          cur_row_.cells_[i].set_int(stat.clog_max_split_log_ts_);
          break;
        case CLOG_SPLIT_CHECKPOINT_TS:
          cur_row_.cells_[i].set_int(stat.clog_split_checkpoint_ts_);
          break;
        case MAX_ARCHIVED_LOG_ID:
          cur_row_.cells_[i].set_int(stat.max_archived_log_id_);
          break;
        case MAX_ARCHIVED_LOG_TS:
          cur_row_.cells_[i].set_int(stat.max_archived_log_ts_);
          break;
        case MAX_ARCHIVED_CHECKPOINT_TS:
          cur_row_.cells_[i].set_int(stat.max_archived_checkpoint_ts_);
          break;
        case ARCHIVED_CLOG_EPOCH:
          cur_row_.cells_[i].set_int(stat.archived_clog_epoch_);
          break;
        case ARCHIVED_ACCUM_CHECKSUM:
          cur_row_.cells_[i].set_int(stat.archived_accum_checksum_);
          break;
        case CUR_INDEX_FILE_ID:
          cur_row_.cells_[i].set_int(stat.cur_index_file_id_);
          break;
        case CUR_INDEX_FILE_OFFSET:
          cur_row_.cells_[i].set_int(stat.index_file_offset_);
          break;
        case CUR_DATA_FILE_ID:
          cur_row_.cells_[i].set_int(stat.cur_data_file_id_);
          break;
        case CUR_DATA_FILE_OFFSET:
          cur_row_.cells_[i].set_int(stat.data_file_offset_);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  } else if (NULL != iter_) {
    iter_->destroy();
    ob_free(iter_);
    iter_ = NULL;
  }

  return ret;
}

int ObAllVirtualPGLogArchiveStat::get_log_archive_stat_(PGLogArchiveStat& stat)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTask* pg_archive_task = NULL;

  if (NULL == iter_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "fail to get_iter_", K(ret));
  } else if (OB_ISNULL(pg_archive_task = iter_->next(pg_archive_task))) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(pg_archive_task->get_log_archive_stat(stat))) {
    SERVER_LOG(WARN, "get_log_archive_stat fail", K(ret), KPC(pg_archive_task));
  }

  if (NULL != pg_archive_task && NULL != iter_) {
    iter_->revert(pg_archive_task);
  }

  return ret;
}

int ObAllVirtualPGLogArchiveStat::get_iter_()
{
  int ret = OB_SUCCESS;
  PGArchiveMap* pg_map = NULL;
  void* buf = NULL;

  if (OB_ISNULL(ps_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ps_ is NULL", K(ret), K(ps_));
  } else if (NULL == (buf = ob_malloc(sizeof(archive::PGArchiveMap::Iterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "ob_malloc fail", K(ret));
  } else if (OB_FAIL(ps_->get_archive_pg_map(pg_map))) {
    SERVER_LOG(WARN, "get_archive_pg_map fail", K(ret));
  } else {
    iter_ = new (buf) PGArchiveMap::Iterator(*pg_map);
  }

  if (OB_FAIL(ret) && NULL != buf) {
    ob_free(buf);
    buf = NULL;
  }

  return ret;
}
}  // namespace observer
}  // namespace oceanbase
