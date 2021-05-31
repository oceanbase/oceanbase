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

#include "ob_archive_log_wrapper.h"
#include "ob_archive_mgr.h"
#include "clog/ob_i_log_engine.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_saved_storage_info_v2.h"

namespace oceanbase {
using namespace storage;
namespace archive {
int ObArchiveLogWrapper::init(ObPartitionService* partition_service, ObILogEngine* log_engine)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ARCHIVE_LOG(WARN, "ObArchiveLogWrapper has inited");
    ret = OB_INIT_TWICE;
  } else {
    partition_service_ = partition_service;
    log_engine_ = log_engine;

    inited_ = true;
  }

  ARCHIVE_LOG(INFO, "ObArchiveLogWrapper init succ");

  return ret;
}

void ObArchiveLogWrapper::refresh_next_ilog_file_id_()
{
  int ret = OB_SUCCESS;
  file_id_t next_ilog_file_id = OB_INVALID_FILE_ID;

  if (OB_ISNULL(log_engine_)) {
    ARCHIVE_LOG(ERROR, "log_engine_ is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log_engine_->query_next_ilog_file_id(next_ilog_file_id))) {
    ARCHIVE_LOG(WARN, "query_next_ilog_file_id fail");
  } else {
    ATOMIC_STORE(&next_ilog_file_id_, next_ilog_file_id);
  }

  if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
    ARCHIVE_LOG(INFO, "refresh_next_ilog_file_id_", K(next_ilog_file_id_));
  }
}

int ObArchiveLogWrapper::query_max_ilog_file_id(file_id_t& max_ilog_id)
{
  int ret = OB_SUCCESS;

  refresh_next_ilog_file_id_();

  if (OB_UNLIKELY(OB_INVALID_FILE_ID == next_ilog_file_id_)) {
    ARCHIVE_LOG(WARN, "next_ilog_file_id_ is invalid", K(next_ilog_file_id_));
  } else {
    max_ilog_id = next_ilog_file_id_ - 1;
  }

  return ret;
}

int ObArchiveLogWrapper::locate_ilog_by_log_id(
    const ObPGKey& pg_key, const uint64_t start_log_id, uint64_t& end_log_id, bool& ilog_file_exist, file_id_t& ilog_id)
{
  int ret = OB_SUCCESS;
  const int64_t max_retry_times = MAX_LOCATE_RETRY_TIMES;
  uint64_t retry_time = 0;
  bool locate = false;
  ilog_file_exist = false;

  if (OB_ISNULL(log_engine_)) {
    ARCHIVE_LOG(ERROR, "log_engine_ is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid pg_key", KR(ret), K(pg_key));
  } else {
    refresh_next_ilog_file_id_();
    while (retry_time < max_retry_times && (OB_NEED_RETRY == ret || OB_SUCCESS == ret) && !locate) {
      ret = OB_SUCCESS;
      locate = true;
      if (OB_FAIL(log_engine_->locate_ilog_file_by_log_id(pg_key, start_log_id, end_log_id, ilog_id))) {
        // locate file in FileIdCache
        if (OB_NEED_RETRY == ret) {
          locate = false;
          ARCHIVE_LOG(WARN,
              "locate_ilog_file_by_log_id need retry",
              KR(ret),
              K(pg_key),
              K(start_log_id),
              K(retry_time),
              K(max_retry_times));
        } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          ARCHIVE_LOG(WARN,
              "start_log_id ilog smaller than minimum ilog file exists",
              KR(ret),
              K(pg_key),
              K(start_log_id),
              K(retry_time),
              K(max_retry_times));
        } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret || OB_PARTITION_NOT_EXIST == ret) {
          // If ilog is not exist in clog file or pg is not exist, pg start ilog id shoule be set next_ilog_file_id_.
          // If pg is already dropped, other work thread can clean it.
          ilog_id = ATOMIC_LOAD(&next_ilog_file_id_);
          ret = OB_SUCCESS;
          ARCHIVE_LOG(DEBUG,
              "start_log_id ilog bigger than maximum ilog file exists",
              KR(ret),
              K(pg_key),
              K(start_log_id),
              K(retry_time),
              K(max_retry_times));
        } else {
          ARCHIVE_LOG(WARN,
              "locate_ilog_by_log_id fail",
              KR(ret),
              K(pg_key),
              K(start_log_id),
              K(retry_time),
              K(max_retry_times));
        }
      } else if (OB_UNLIKELY(OB_INVALID_FILE_ID == ilog_id)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(WARN, "invalid file id", KR(ret), K(pg_key), K(start_log_id), K(ilog_id));
      } else {
        ilog_file_exist = true;
        ARCHIVE_LOG(DEBUG, "locate_ilog_by_log_id succ", K(pg_key), K(start_log_id), K(ilog_id));
      }

      retry_time++;
    }
  }

  if (OB_FAIL(ret)) {
    ARCHIVE_LOG(WARN, "locate_ilog_by_log_id fail", KR(ret), K(retry_time++));
  }

  return ret;
}

int ObArchiveLogWrapper::check_is_from_restore(const ObPGKey& pg_key, bool& is_from_restore)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;

  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "partition_service_ is NULL", KR(ret), K(pg_key));
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid pg_key", KR(ret), K(pg_key));
  } else if (OB_FAIL(partition_service_->get_partition(pg_key, guard))) {
    ARCHIVE_LOG(WARN, "get_partition fail", KR(ret), K(pg_key));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "got partition group is NULL", KR(ret), K(pg_key));
  } else if (OB_FAIL(guard.get_partition_group()->check_is_from_restore(is_from_restore))) {
    ARCHIVE_LOG(WARN, "failed to check_is_from_restore", KR(ret), K(pg_key));
  }
  return ret;
}

// get pg minor merge info
// pg start archive log id = minor + 1
int ObArchiveLogWrapper::get_all_saved_info(const ObPGKey& pg_key, ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;

  if (OB_ISNULL(partition_service_)) {
    ARCHIVE_LOG(ERROR, "partition_service_ is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid pg_key", KR(ret), K(pg_key));
  } else if (OB_FAIL(partition_service_->get_partition(pg_key, guard))) {
    ARCHIVE_LOG(WARN, "get_partition fail", KR(ret), K(pg_key));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "got partition group is NULL", KR(ret), K(pg_key));
  } else if (OB_FAIL(guard.get_partition_group()->get_all_saved_info(info))) {
    ARCHIVE_LOG(WARN, "get_all_saved_info fail", KR(ret), K(pg_key));
  }

  return ret;
}

int ObArchiveLogWrapper::get_pg_max_log_id(const ObPGKey& pg_key, uint64_t& ret_max_log_id)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  clog::ObIPartitionLogService* pls = NULL;

  if (OB_ISNULL(log_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "log_engine_ is NULL", KR(ret), K(log_engine_));
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid pg_key", KR(ret), K(pg_key));
  } else if (OB_FAIL(partition_service_->get_partition(pg_key, guard))) {
    if (OB_PARTITION_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_partition fail", KR(ret), K(pg_key));
    } else {
      ARCHIVE_LOG(WARN, "partition not exist", KR(ret), K(pg_key));
      ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "get_partition_group is NULL", KR(ret), K(pg_key));
  } else if (OB_ISNULL(pls = guard.get_partition_group()->get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ObIPartitionLogService is NULL", KR(ret), K(pg_key));
  } else {
    ret_max_log_id = pls->get_next_index_log_id() - 1;
  }

  return ret;
}

int ObArchiveLogWrapper::get_pg_log_archive_status(const ObPGKey& pg_key, ObPGLogArchiveStatus& status)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;

  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "partition_service_ is NULL", KR(ret), K(pg_key));
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_key));
  } else if (OB_FAIL(partition_service_->get_partition(pg_key, guard))) {
    ARCHIVE_LOG(WARN, "get_partition fail", KR(ret), K(pg_key));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "get_partition_group fail", KR(ret), K(pg_key));
  } else if (OB_FAIL(partition->get_log_archive_status(status))) {
    ARCHIVE_LOG(WARN, "get_log_archive_status fail", KR(ret), K(pg_key));
  }

  return ret;
}

int ObArchiveLogWrapper::get_pg_first_log_submit_ts(const ObPGKey& pg_key, int64_t& submit_ts)
{
  int ret = OB_SUCCESS;
  uint64_t log_id = 1;
  ObLogCursorExt cursor;

  if (OB_ISNULL(log_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "log_engine_ is NULL", KR(ret), K(log_engine_));
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid pg_key", KR(ret), K(pg_key));
  } else if (OB_FAIL(log_engine_->get_cursor(pg_key, log_id, cursor))) {
    ARCHIVE_LOG(WARN, "get_cursor_batch fail", KR(ret), K(pg_key), K(log_id));
  } else {
    submit_ts = cursor.get_submit_timestamp();
  }

  return ret;
}
}  // namespace archive
}  // namespace oceanbase
