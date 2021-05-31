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

#define USING_LOG_PREFIX STORAGE
#include "storage/ob_backup_archive_log.h"
#include "archive/ob_archive_io.h"
#include "archive/ob_archive_path.h"
#include "archive/ob_archive_util.h"
#include "archive/ob_archive_file_utils.h"
#include "lib/queue/ob_link.h"
#include "lib/restore/ob_storage.h"
#include "observer/ob_server_struct.h"
#include "share/ob_rs_mgr.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/backup/ob_backup_path.h"
#include "lib/allocator/ob_allocator.h"
#include "storage/ob_partition_migrator.h"

using namespace oceanbase::archive;
using namespace oceanbase::clog;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;

namespace oceanbase {
namespace storage {

ObBackupArchiveLogPGCtx::ObBackupArchiveLogPGCtx()
    : is_opened_(false),
      pg_key_(),
      log_archive_round_(-1),
      checkpoint_ts_(-1),
      rs_checkpoint_ts_(-1),
      src_backup_dest_(),
      src_storage_info_(),
      dst_backup_dest_(),
      dst_storage_info_(),
      mig_ctx_(NULL),
      throttle_(NULL)
{}

ObBackupArchiveLogPGCtx::~ObBackupArchiveLogPGCtx()
{}

int ObBackupArchiveLogPGCtx::open(
    ObMigrateCtx& mig_ctx, const common::ObPGKey& pg_key, common::ObInOutBandwidthThrottle& throttle)
{
  int ret = OB_SUCCESS;
  const share::ObBackupArchiveLogArg& arg = mig_ctx.replica_op_arg_.backup_archive_log_arg_;
  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pg_key));
  } else if (OB_FAIL(databuff_printf(src_backup_dest_, OB_MAX_BACKUP_PATH_LENGTH, "%s", arg.src_backup_dest_))) {
    LOG_WARN("failed to databuff printf", KR(ret), K(arg));
  } else if (OB_FAIL(databuff_printf(src_storage_info_, OB_MAX_BACKUP_PATH_LENGTH, "%s", arg.src_storage_info_))) {
    LOG_WARN("failed to databuff printf", KR(ret), K(arg));
  } else if (OB_FAIL(databuff_printf(dst_backup_dest_, OB_MAX_BACKUP_PATH_LENGTH, "%s", arg.dst_backup_dest_))) {
    LOG_WARN("failed to databuff printf", KR(ret), K(arg));
  } else if (OB_FAIL(databuff_printf(dst_storage_info_, OB_MAX_BACKUP_PATH_LENGTH, "%s", arg.dst_storage_info_))) {
    LOG_WARN("failed to databuff printf", KR(ret), K(arg));
  } else {
    pg_key_ = pg_key;
    log_archive_round_ = arg.log_archive_round_;
    rs_checkpoint_ts_ = arg.rs_checkpoint_ts_;
    mig_ctx_ = &mig_ctx;
    throttle_ = &throttle;
    is_opened_ = true;
  }
  return ret;
}

int ObBackupArchiveLogPGCtx::close()
{
  int ret = OB_SUCCESS;
  is_opened_ = false;
  return ret;
}

ObBackupArchiveLogPGTask::ObBackupArchiveLogPGTask()
    : ObITask(TASK_TYPE_BACKUP_ARCHIVELOG), is_inited_(false), pg_key_(), mig_ctx_(NULL), pg_ctx_(NULL), throttle_(NULL)
{}

ObBackupArchiveLogPGTask::~ObBackupArchiveLogPGTask()
{}

int ObBackupArchiveLogPGTask::init(ObMigrateCtx& mig_ctx, ObBackupArchiveLogPGCtx& pg_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup backupset task is inited", KR(ret));
  } else {
    mig_ctx_ = &mig_ctx;
    pg_ctx_ = &pg_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupArchiveLogPGTask::process()
{
  int ret = OB_SUCCESS;
  bool is_mounted = true;
  int64_t round = 0;
  FileRange index_delta_range;
  FileRange data_delta_range;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_ISNULL(pg_ctx_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("pg ctx should not be null", KR(ret), K(pg_ctx_));
  } else if (FALSE_IT(round = pg_ctx_->log_archive_round_)) {
    // assign
  } else if (OB_FAIL(check_nfs_mounted_if_nfs(*pg_ctx_, is_mounted))) {
    LOG_WARN("failed to check nfs mounted", KR(ret));
  } else if (!is_mounted) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_ERROR("nfs is not mounted", KR(ret));
  } else if (OB_FAIL(try_touch_archive_key(*pg_ctx_, OB_START_INCARNATION, round))) {
    LOG_WARN("failed to try touch archive key", KR(ret));
  } else if (OB_FAIL(converge_task(LOG_ARCHIVE_FILE_TYPE_INDEX, *pg_ctx_, index_delta_range))) {
    LOG_WARN("failed to converge index task", K(ret));
  } else if (OB_FAIL(converge_task(LOG_ARCHIVE_FILE_TYPE_DATA, *pg_ctx_, data_delta_range))) {
    LOG_WARN("failed to converge data task", K(ret));
  } else if (OB_FAIL(extract_last_log_in_data_file(*pg_ctx_, data_delta_range.max_file_id_))) {
    LOG_WARN("failed to extract last log in data file", K(ret), K(data_delta_range));
  } else if (OB_FAIL(catchup_archive_log(LOG_ARCHIVE_FILE_TYPE_INDEX, *pg_ctx_, index_delta_range))) {
    LOG_WARN("failed to catch up index archive log", K(ret));
  } else if (OB_FAIL(catchup_archive_log(LOG_ARCHIVE_FILE_TYPE_DATA, *pg_ctx_, data_delta_range))) {
    LOG_WARN("failed to catch up data archive log", K(ret));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::check_nfs_mounted_if_nfs(const ObBackupArchiveLogPGCtx& pg_ctx, bool& is_mounted)
{
  int ret = OB_SUCCESS;
  is_mounted = true;
  bool exist = false;
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_dest;
  ObBackupPath path;
  ObStorageUtil util(true /*need retry*/);
  ObStorageType storage_type;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset task do not init", KR(ret));
  } else if (OB_FAIL(get_storage_type_from_path(pg_ctx.dst_backup_dest_, storage_type))) {
    LOG_WARN("failed to get storage type from path", KR(ret), K(pg_ctx));
  } else if (OB_STORAGE_FILE != storage_type) {
    LOG_INFO("storage info is not nfs", K(storage_type));
  } else if (OB_FAIL(backup_dest.set(pg_ctx.dst_backup_dest_, pg_ctx.dst_storage_info_))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(pg_ctx));
  } else if (OB_FAIL(cluster_dest.set(backup_dest, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_name_info_path(cluster_dest, path))) {
    LOG_WARN("failed to get cluster clog backup info path", KR(ret), K(cluster_dest));
  } else {
    ObString uri(path.get_obstr());
    ObString storage_info(pg_ctx.dst_storage_info_);
    if (OB_FAIL(util.is_exist(uri, storage_info, exist))) {
      LOG_WARN("failed to check file exist", KR(ret), K(uri), K(storage_info));
    } else {
      if (!exist) {
        is_mounted = false;
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogPGTask::converge_task(
    const archive::LogArchiveFileType file_type, const ObBackupArchiveLogPGCtx& pg_ctx, FileRange& delta_range)
{
  int ret = OB_SUCCESS;
  FileRange src_file_range;
  FileRange dst_file_range;
  const ObPGKey& pg_key = pg_ctx.pg_key_;
  ObString src_storage_info(pg_ctx.src_storage_info_);
  ObString dst_storage_info(pg_ctx.dst_storage_info_);
  const int64_t log_archive_round = pg_ctx.log_archive_round_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(get_file_range(
                 pg_ctx.src_backup_dest_, src_storage_info, file_type, log_archive_round, pg_key, src_file_range))) {
    LOG_WARN("failed to get src data file range", K(ret), K(pg_ctx));
  } else if (OB_FAIL(get_file_range(
                 pg_ctx.dst_backup_dest_, dst_storage_info, file_type, log_archive_round, pg_key, dst_file_range))) {
    LOG_WARN("failed to get src data file range", K(ret), K(pg_ctx));
  } else if (OB_FAIL(
                 cal_data_file_range_delta(pg_key, log_archive_round, src_file_range, dst_file_range, delta_range))) {
    LOG_WARN("failed to calculate data file range delta", K(ret), K(src_file_range), K(dst_file_range));
  } else {
    LOG_DEBUG("converge task info", K(delta_range));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::catchup_archive_log(
    const archive::LogArchiveFileType file_type, const ObBackupArchiveLogPGCtx& task, const FileRange& delta_range)
{
  int ret = OB_SUCCESS;
  const common::ObPGKey& pg_key = task.pg_key_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(check_and_mkdir(file_type, task))) {
    LOG_WARN("failed to mkdir", KR(ret), K(file_type), K(task));
  } else if (0 == delta_range.max_file_id_) {
    // do nothing
  } else {
    int64_t start = 0 == delta_range.min_file_id_ ? 1 : delta_range.min_file_id_;
    int64_t end = delta_range.max_file_id_;
    const int64_t round = task.log_archive_round_;
    for (int64_t i = start; OB_SUCC(ret) && i <= end; ++i) {
      FileInfo src_info;
      FileInfo dst_info;
      bool file_exist = false;
      if (OB_FAIL(get_file_path_info(task.src_backup_dest_,
              task.src_storage_info_,
              pg_key,
              file_type,
              OB_START_INCARNATION,
              round,
              i,
              src_info))) {
        LOG_WARN("failed to get file path info", K(ret), K(pg_key));
      } else if (OB_FAIL(get_file_path_info(task.dst_backup_dest_,
                     task.dst_storage_info_,
                     pg_key,
                     file_type,
                     OB_START_INCARNATION,
                     round,
                     i,
                     dst_info))) {
        LOG_WARN("failed to get file path info", K(ret), K(pg_key));
      } else if (OB_FAIL(check_file_exist(dst_info, file_exist))) {
        LOG_WARN("failed to check file exist", K(ret), K(dst_info));
      } else if (OB_FAIL(do_file_transfer(file_type, task, src_info, dst_info, file_exist))) {
        LOG_WARN("failed to do file transfer", K(ret), K(src_info), K(dst_info), K(file_exist));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogPGTask::get_file_range(const char* backup_dest, const common::ObString& storage_info,
    const archive::LogArchiveFileType file_type, const int64_t log_archive_round, const common::ObPGKey& pg_key,
    FileRange& file_range)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  uint64_t min_file_id = 0;
  uint64_t max_file_id = 0;
  char path[archive::OB_MAX_ARCHIVE_PATH_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(build_archive_file_prefix(
                 backup_dest, pg_key, file_type, OB_START_INCARNATION, log_archive_round, path, pos))) {
    LOG_WARN("failed to build archive file prefix", KR(ret), K(pg_key));
  } else {
    ObString uri(path);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.get_file_range(uri, storage_info, min_file_id, max_file_id))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_DEBUG("data file do not exist", K(ret), K(backup_dest), K(pg_key));
        ret = OB_SUCCESS;
        min_file_id = 0;
        max_file_id = 0;
      } else {
        ARCHIVE_LOG(WARN, "get_file_range fail", K(ret), K(pg_key), K(file_type));
      }
    }
  }
  if (OB_SUCC(ret)) {
    file_range.min_file_id_ = min_file_id;
    file_range.max_file_id_ = max_file_id;
  }
  return ret;
}

int ObBackupArchiveLogPGTask::init_archive_file_store_(
    ObBackupArchiveLogPGCtx& task, const int64_t incarnation, const int64_t round, ObArchiveLogFileStore& file_store)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = task.pg_key_.get_tenant_id();
  const char* cluster_name = GCTX.config_->cluster;
  const int64_t cluster_id = GCTX.config_->cluster_id;
  if (OB_FAIL(file_store.init(
          task.src_backup_dest_, task.src_storage_info_, cluster_name, cluster_id, tenant_id, incarnation, round))) {
    LOG_WARN("archive file store init fail", KR(ret), K(task), K(incarnation), K(round));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::get_file_path_info(const char* backup_dest, const char* storage_info,
    const common::ObPGKey& pg_key, const archive::LogArchiveFileType file_type, const int64_t incarnation,
    const int64_t round, const int64_t file_id, FileInfo& file_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(build_archive_file_path(
                 backup_dest, pg_key, file_type, incarnation, round, file_id, file_info.dest_path_))) {
    LOG_WARN("failed to build archive file path", K(ret));
  } else if (OB_FAIL(databuff_printf(file_info.storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH, "%s", storage_info))) {
    LOG_WARN("failed to data buff printf", KR(ret), K(storage_info));
  } else {
    file_info.uri_ = ObString::make_string(file_info.dest_path_);
    file_info.info_ = ObString::make_string(file_info.storage_info_);
  }
  return ret;
}

int ObBackupArchiveLogPGTask::extract_last_log_in_data_file(ObBackupArchiveLogPGCtx& task, const int64_t file_id)
{
  int ret = OB_SUCCESS;
  ObArchiveFileUtils util;
  ObArchiveLogFileStore file_store;
  FileInfo file_info;
  LogArchiveFileType file_type = LOG_ARCHIVE_FILE_TYPE_DATA;
  archive::ObArchiveBlockMeta block_meta;
  clog::ObLogEntry log_entry;
  ObLogArchiveInnerLog inner_log;
  const int64_t log_archive_round = task.log_archive_round_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(get_file_path_info(task.src_backup_dest_,
                 task.src_storage_info_,
                 task.pg_key_,
                 file_type,
                 OB_START_INCARNATION,
                 log_archive_round,
                 file_id,
                 file_info))) {
    LOG_WARN("failed to get file path info", K(ret));
  } else if (OB_FAIL(init_archive_file_store_(task, OB_START_INCARNATION, log_archive_round, file_store))) {
    LOG_WARN("failed to get archive file store", K(ret));
  } else if (OB_FAIL(util.extract_last_log_in_data_file(task.pg_key_,
                 file_id,
                 &file_store,
                 file_info.uri_,
                 file_info.storage_info_,
                 block_meta,
                 log_entry,
                 inner_log))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("failed to extract last log in data file", K(ret), K(file_info));
      ret = OB_SUCCESS;
    }
  } else {
    task.checkpoint_ts_ = inner_log.get_checkpoint_ts();
  }
  return ret;
}

int ObBackupArchiveLogPGTask::cal_data_file_range_delta(const common::ObPGKey& pkey, const int64_t log_archive_round,
    const FileRange& src_file_range, const FileRange& dst_file_range, FileRange& delta_file_range)
{
  int ret = OB_SUCCESS;
  bool interrupted = false;
  delta_file_range.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (log_archive_round < 0 || !src_file_range.is_valid() || !dst_file_range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cal data file range delta get invalid argument",
        K(ret),
        K(log_archive_round),
        K(src_file_range),
        K(dst_file_range));
  } else if (OB_FAIL(check_archive_log_interrupted(log_archive_round, src_file_range, dst_file_range, interrupted))) {
    LOG_WARN("failed to check archive log interrupted", KR(ret), K(pkey), K(log_archive_round));
  } else if (interrupted) {
    ret = OB_LOG_ARCHIVE_INTERRUPTED;
    FLOG_WARN("log archive interrupted, missing files",
        KR(ret),
        K(pkey),
        K(log_archive_round),
        K(src_file_range),
        K(dst_file_range));
  } else {
    delta_file_range.min_file_id_ = dst_file_range.max_file_id_;
    delta_file_range.max_file_id_ = src_file_range.max_file_id_;
  }
  return ret;
}

// o indicates not exist, x indicates exist
// case-1: there is not any file in whether src or dst
//      1 2 3 4 5
//  src o o o o o
//  dst o o o o o no cutoff
// case 0: exist in src, not exist in dst, src start from 1
//      1 2 3 4 5
//  src x x x x x
//  dst o o o o o no cutoff
// case 1: 4,5 exist in src; 1,2,3 exist in dest
//      1 2 3 4 5
//  src o o o x x
//  dst x x x o o no cutoff
// TODO : need handle 3 is incomplete
// case 2: 4,5 exist in src; 1,2 exist in dst
//      1 2 3 4 5
//  src o o o x x
//  dst x x o o o cutoff
// case 3: 3,4,5 exist in src; 1,2,3 in dst
//      1 2 3 4 5
//  src o o x x x
//  dst x x x o o no cutoff
// case 4: 3,4,5 exist in src; nothing in dst
//      1 2 3 4 5
//  src o o x x x
//  dst o o o o o cutoff
int ObBackupArchiveLogPGTask::check_archive_log_interrupted(const int64_t log_archive_round,
    const FileRange& src_file_range, const FileRange& dst_file_range, bool& interrupted)
{
  int ret = OB_SUCCESS;
  interrupted = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (log_archive_round < 0 || !src_file_range.is_valid() || !dst_file_range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cal data file range delta get invalid argument",
        K(ret),
        K(log_archive_round),
        K(src_file_range),
        K(dst_file_range));
  } else if (0 == src_file_range.min_file_id_ || 1 == src_file_range.min_file_id_) {
    // do nothing
  } else if (src_file_range.min_file_id_ - dst_file_range.max_file_id_ > 1) {
    interrupted = true;
  }
#ifdef ERRSIM
  if (1 == log_archive_round) {
    ret = E(EventTable::EN_BACKUP_BACKUP_LOG_ARCHIVE_INTERRUPTED) OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("cal data file range delta err sim", KR(ret));
  }
#endif
  return ret;
}

int ObBackupArchiveLogPGTask::check_file_exist(const FileInfo& file_info, bool& file_exist)
{
  int ret = OB_SUCCESS;
  file_exist = false;
  ObStorageUtil util(false);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(util.is_exist(file_info.uri_, file_info.storage_info_, file_exist))) {
    LOG_WARN("failed to check file exist", K(ret), K(file_info));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::get_file_length(const FileInfo& file_info, int64_t& file_length)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(util.get_file_length(file_info.uri_, file_info.storage_info_, file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", K(ret), K(file_info));
    }
  }
  return ret;
}

int ObBackupArchiveLogPGTask::check_and_mkdir(
    const archive::LogArchiveFileType file_type, const ObBackupArchiveLogPGCtx& pg_task)
{
  int ret = OB_SUCCESS;
  // TODO
  int64_t pos = 0;
  const int64_t archive_round = pg_task.log_archive_round_;
  // if uri prefix matches OB_OSS_PREFIX
  char file_prefix_path[OB_MAX_ARCHIVE_PATH_LENGTH] = "";
  ObStorageUtil util(false);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(build_archive_file_prefix(pg_task.dst_backup_dest_,
                 pg_task.pg_key_,
                 file_type,
                 OB_START_INCARNATION,
                 archive_round,
                 file_prefix_path,
                 pos))) {
    LOG_WARN("failed to build archive file prefix", K(ret), K(pg_task));
  } else {
    ObString uri(file_prefix_path);
    ObString storage_info(pg_task.dst_storage_info_);
    if (OB_FAIL(util.mkdir(uri, storage_info))) {
      LOG_WARN("failed to mkdir", K(ret), K(uri), K(storage_info));
    } else {
      LOG_DEBUG("mkdir succ", K(pg_task), K(uri));
    }
  }
  return ret;
}

int ObBackupArchiveLogPGTask::do_file_transfer(const archive::LogArchiveFileType file_type,
    const ObBackupArchiveLogPGCtx& pg_task, const FileInfo& src_info, const FileInfo& dst_info, const bool dst_exist)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else {
    if (dst_exist) {
      if (OB_FAIL(do_part_file_transfer(src_info, dst_info))) {
        LOG_WARN("failed to do part file transfer", K(ret), K(src_info), K(dst_info));
      }
    } else {
      if (OB_FAIL(do_single_file_transfer(file_type, pg_task, src_info, dst_info))) {
        LOG_WARN("failed to do single file transfer", K(ret), K(pg_task), K(src_info), K(dst_info));
      }
    }
  }
  return ret;
}

// TODO : log file verification
int ObBackupArchiveLogPGTask::do_single_file_transfer(const archive::LogArchiveFileType file_type,
    const ObBackupArchiveLogPGCtx& pg_task, const FileInfo& src_info, const FileInfo& dst_info)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false);
  clog::ObReadBuf rbuf;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(get_file_length(src_info, rbuf.buf_len_))) {
    LOG_WARN("failed to get file length", K(ret), K(src_info));
  } else if (OB_ISNULL(rbuf.buf_ = static_cast<char*>(ob_archive_malloc(rbuf.buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(read_single_file(src_info, rbuf))) {
    LOG_WARN("failed to read single file", K(ret), K(src_info), K(rbuf));
  } else if (OB_FAIL(check_and_mkdir(file_type, pg_task))) {
    LOG_WARN("failed to mkdir for pg key", K(ret), K(pg_task));
  } else if (OB_FAIL(write_part_file(dst_info, rbuf))) {
    LOG_WARN("failed to write single file", K(ret), K(src_info), K(rbuf));
  }

  if (NULL != rbuf.buf_) {
    ob_archive_free(rbuf.buf_);
    rbuf.reset();
  }
  return ret;
}

int ObBackupArchiveLogPGTask::do_part_file_transfer(const FileInfo& src_info, const FileInfo& dst_info)
{
  int ret = OB_SUCCESS;
  int64_t src_file_len = 0;
  int64_t dst_file_len = 0;
  clog::ObReadBuf rbuf;
  ObStorageAppender dst_appender(CREATE_OPEN_NOLOCK);
  // TODO: not support backup backup to cos, will support by yanfeng later.
  ObStorageAppender::AppenderParam param;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(dst_appender.open(dst_info.uri_, dst_info.storage_info_, param))) {
    LOG_WARN("failed to open dst storage appender", KR(ret), K(dst_info));
  } else if (OB_FAIL(get_file_length(src_info, src_file_len))) {
    LOG_WARN("failed to get src file len", K(ret), K(src_info));
  } else if (OB_FAIL(get_file_length(dst_info, dst_file_len))) {
    LOG_WARN("failed to get dst file len", K(ret), K(dst_info));
  } else if (dst_file_len == src_file_len) {
    LOG_INFO("no need to do file transfer if size match", K(dst_file_len), K(src_file_len));
  } else if (dst_file_len > src_file_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dst file len should not greater than src file len", K(ret), K(src_file_len), K(dst_file_len));
  } else if (FALSE_IT(rbuf.buf_len_ = src_file_len - dst_file_len)) {
  } else if (OB_ISNULL(rbuf.buf_ = static_cast<char*>(ob_archive_malloc(rbuf.buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(rbuf));
  } else if (OB_FAIL(read_part_file(src_info, dst_file_len, rbuf))) {
    LOG_WARN("failed to read part file", K(ret), K(src_info));
  } else if (OB_FAIL(write_part_file(dst_info, rbuf))) {
    LOG_WARN("failed to write part file", K(ret), K(dst_info));
  }
  if (NULL != rbuf.buf_) {
    ob_archive_free(rbuf.buf_);
    rbuf.reset();
  }
  return ret;
}

int ObBackupArchiveLogPGTask::read_single_file(const FileInfo& file, clog::ObReadBuf& rbuf)
{
  int ret = OB_SUCCESS;
  int64_t read_len = 0;
  ObStorageUtil util(false);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(util.read_single_file(file.uri_, file.storage_info_, rbuf.buf_, rbuf.buf_len_, read_len))) {
    LOG_WARN("failed to read single file", K(ret), K(file));
  } else if (read_len != rbuf.buf_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read len not expected", K(ret), K(read_len), K(rbuf));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::write_single_file(const FileInfo& file, const clog::ObReadBuf& buf)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (util.write_single_file(file.uri_, file.storage_info_, buf.buf_, buf.buf_len_)) {
    LOG_WARN("failed to write single file", K(ret), K(file));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::read_part_file(const FileInfo& file_info, const int64_t offset, clog::ObReadBuf& rbuf)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false);
  int64_t read_len = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(util.read_part_file(
                 file_info.uri_, file_info.storage_info_, rbuf.buf_, rbuf.buf_len_, offset, read_len))) {
    LOG_WARN("failed to read part file", K(ret), K(file_info), K(offset));
  } else if (read_len != rbuf.buf_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read len not expected", K(ret), K(read_len), K(rbuf));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::write_part_file(const FileInfo& file_info, const clog::ObReadBuf& buf)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool write_succ = true;
  ObStorageAppender appender;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(appender.open(file_info.uri_, file_info.storage_info_))) {
    LOG_WARN("failed to open storage appender", K(ret), K(file_info));
  } else if (OB_FAIL(appender.write(buf.buf_, buf.buf_len_))) {
    write_succ = false;
    LOG_WARN("failed to write to storage appender", K(ret), K(buf));
  } else if (OB_FAIL(appender.close())) {
    LOG_WARN("failed to close storage appender", K(ret), K(buf));
  }
  if (!write_succ) {
    if (OB_SUCCESS != (tmp_ret = appender.close())) {
      LOG_WARN("failed to close storage appender", KR(tmp_ret));
    }
  }
  return ret;
}

int ObBackupArchiveLogPGTask::build_archive_file_prefix(const char* backup_dest, const common::ObPGKey& pg_key,
    const LogArchiveFileType file_type, const int64_t incarnation, const int64_t round, char* dest_path, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t cluster_id = GCTX.config_->cluster_id;
  const char* cluster_name = GCTX.config_->cluster;
  const uint64_t tenant_id = pg_key.get_tenant_id();
  char base_path[OB_MAX_BACKUP_PATH_LENGTH] = "";
  ObArchivePathUtil util;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(util.build_base_path(backup_dest,
                 cluster_name,
                 cluster_id,
                 tenant_id,
                 incarnation,
                 round,
                 OB_MAX_BACKUP_PATH_LENGTH,
                 base_path))) {
    LOG_WARN("failed to build_base_path", K(ret), K(pg_key));
  } else if (OB_FAIL(build_file_prefix(pg_key, base_path, file_type, dest_path, pos))) {
    LOG_WARN("failed to build file prefix", K(ret), K(pg_key));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::build_archive_file_path(const char* backup_dest, const common::ObPGKey& pg_key,
    const LogArchiveFileType file_type, const int64_t incarnation, const int64_t round, const int64_t file_id,
    char* dest_path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(build_archive_file_prefix(backup_dest, pg_key, file_type, incarnation, round, dest_path, pos))) {
    LOG_WARN("failed to build archive file prefix", KR(ret), K(pg_key), K(file_type));
  } else if (OB_FAIL(databuff_printf(dest_path, OB_MAX_BACKUP_PATH_LENGTH, pos, "/%lu", file_id))) {
    LOG_WARN("failed to data buff printf", K(ret));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::build_file_prefix(
    const ObPGKey& pg_key, const char* base_path, const LogArchiveFileType file_type, char* dest_path_buf, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = pg_key.get_table_id();
  const int64_t partition_id = pg_key.get_partition_id();
  int64_t path_buf_len = OB_MAX_ARCHIVE_PATH_LENGTH;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(dest_path_buf,
                 path_buf_len,
                 pos,
                 "%s/%s/%lu/%lu",
                 base_path,
                 get_file_prefix_with_type(file_type),
                 table_id,
                 partition_id))) {
    LOG_WARN("failed to data buff printf", K(ret));
  }
  return ret;
}

const char* ObBackupArchiveLogPGTask::get_file_prefix_with_type(const LogArchiveFileType file_type)
{
  const char* prefix = "invalid";
  switch (file_type) {
    case LOG_ARCHIVE_FILE_TYPE_INDEX: {
      prefix = "index";
      break;
    }
    case LOG_ARCHIVE_FILE_TYPE_DATA: {
      prefix = "data";
      break;
    }
    default:
      prefix = "invalid";
      ARCHIVE_LOG(ERROR, "invalid file type", K(file_type));
  }
  return prefix;
}

int ObBackupArchiveLogPGTask::try_touch_archive_key(
    const ObBackupArchiveLogPGCtx& task, const int64_t incarnation, const int64_t round)
{
  int ret = OB_SUCCESS;
  const ObPGKey& pg_key = task.pg_key_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", KR(ret));
  } else if (OB_FAIL(build_archive_key_prefix(task, pg_key, incarnation, round))) {
    LOG_WARN("build_archive_key_prefix fail", KR(ret), K(incarnation), K(round), K(pg_key));
  } else if (OB_FAIL(touch_archive_key_file(task, incarnation, round, pg_key))) {
    LOG_WARN("touch_archive_key_file fail", KR(ret), K(incarnation), K(round), K(pg_key));
  } else {
    LOG_DEBUG("try_touch_archive_key_ succ", KR(ret), K(incarnation), K(round), K(pg_key));
  }

  return ret;
}

int ObBackupArchiveLogPGTask::build_archive_key_prefix(
    const ObBackupArchiveLogPGCtx& task, const ObPGKey& pg_key, const int64_t incarnation, const int64_t round)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_dest;
  ObBackupPath archive_key_prefix_path;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", KR(ret));
  } else if (OB_FAIL(backup_dest.set(task.dst_backup_dest_, task.dst_storage_info_))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(task));
  } else if (OB_FAIL(cluster_dest.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_prefix(
                 cluster_dest, pg_key.get_tenant_id(), round, archive_key_prefix_path))) {
    LOG_WARN("failed to get clog archive key prefix path", KR(ret), K(cluster_dest), K(pg_key), K(round));
  } else {
    ObString uri(archive_key_prefix_path.get_obstr());
    ObString storage_info(task.dst_storage_info_);
    ObStorageUtil storage_util(false /*need retry*/);
    if (OB_FAIL(storage_util.mkdir(uri, storage_info))) {
      LOG_WARN("make archive_key dir fail", KR(ret), K(pg_key), K(uri), K(storage_info));
    }
  }
  return ret;
}

int ObBackupArchiveLogPGTask::touch_archive_key_file(
    const ObBackupArchiveLogPGCtx& task, const int64_t incarnation, const int64_t round, const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObBackupPath archive_key_path;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", KR(ret));
  } else if (OB_FAIL(build_archive_key_path(task, incarnation, round, pg_key, archive_key_path))) {
    LOG_WARN("failed to build archive key path", KR(ret), K(pg_key));
  } else {
    ObString uri(archive_key_path.get_obstr());
    ObString storage_info(task.dst_storage_info_);
    ObArchiveFileUtils file_utils;
    if (OB_FAIL(file_utils.create_file(uri, storage_info))) {
      LOG_WARN("create_file fail", KR(ret), K(uri), K(storage_info));
    }
  }
  return ret;
}

int ObBackupArchiveLogPGTask::build_archive_key_path(const ObBackupArchiveLogPGCtx& task, const int64_t incarnation,
    const int64_t round, const ObPGKey& pg_key, ObBackupPath& archive_key_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", KR(ret));
  } else if (OB_FAIL(backup_dest.set(task.dst_backup_dest_, task.dst_storage_info_))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(task));
  } else if (OB_FAIL(cluster_dest.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_path(
                 cluster_dest, pg_key.get_tenant_id(), round, pg_key, archive_key_path))) {
    LOG_WARN("failed to get clog archive key path", KR(ret), K(cluster_dest), K(pg_key), K(round));
  }
  return ret;
}

ObBackupArchiveLogFinishTask::ObBackupArchiveLogFinishTask()
    : ObITask(TASK_TYPE_BACKUP_ARCHIVELOG), is_inited_(false), mig_ctx_(NULL)
{}

ObBackupArchiveLogFinishTask::~ObBackupArchiveLogFinishTask()
{}

int ObBackupArchiveLogFinishTask::init(ObMigrateCtx& mig_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup archive log finish task init twice", KR(ret));
  } else {
    mig_ctx_ = &mig_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupArchiveLogFinishTask::process()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_FINISH_BACKUP_ARCHIVELOG_TASK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log finish task do not init", KR(ret));
  } else if (OB_FAIL(pg_ctx_.close())) {
    LOG_WARN("failed to close backup archive log pg ctx", KR(ret));
  } else {
    LOG_INFO("backup archive log finish", KR(ret));
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase
