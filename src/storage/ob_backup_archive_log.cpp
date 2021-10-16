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
      piece_triple_(),
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
    piece_triple_.round_id_ = arg.log_archive_round_;
    piece_triple_.piece_id_ = arg.piece_id_;
    piece_triple_.create_date_ = arg.create_date_;
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
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_BACKUP_BACKUPPIECE_FILE_TASK) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("errsim transfer file", KR(ret));
    }
  }
#endif
  DEBUG_SYNC(BEFORE_START_BACKUP_ARCHIVELOG_TASK);
  if (OB_FAIL(ret)) {
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(do_task_with_retry())) {
    LOG_WARN("failed to do task with retry", KR(ret));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::do_task_with_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t retry_times = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else {
    while (retry_times < MAX_RETRY_TIMES) {
      if (OB_SUCCESS != (tmp_ret = do_task())) {
        LOG_WARN("failed to do task", KR(tmp_ret));
      }

      if (OB_SUCCESS == tmp_ret) {
        break;
      } else {
        ++retry_times;
        usleep(MAX_RETRY_TIME_INTERVAL);
      }
    }
    // use the last tmp_ret if failed
    ret = tmp_ret;
  }
  return ret;
}

int ObBackupArchiveLogPGTask::do_task()
{
  int ret = OB_SUCCESS;
  bool is_mounted = true;
  FileRange index_delta_range;
  FileRange data_delta_range;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_ISNULL(pg_ctx_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("pg ctx should not be null", KR(ret), K(pg_ctx_));
  } else if (OB_FAIL(check_nfs_mounted_if_nfs(*pg_ctx_, is_mounted))) {
    LOG_WARN("failed to check nfs mounted", KR(ret));
  } else if (!is_mounted) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_ERROR("nfs is not mounted", KR(ret));
  } else if (OB_FAIL(try_touch_archive_key(*pg_ctx_, OB_START_INCARNATION, pg_ctx_->piece_triple_))) {
    LOG_WARN("failed to try touch archive key", KR(ret));
  } else if (OB_FAIL(converge_task(LOG_ARCHIVE_FILE_TYPE_INDEX, *pg_ctx_, index_delta_range))) {
    LOG_WARN("failed to converge index task", K(ret));
  } else if (OB_FAIL(converge_task(LOG_ARCHIVE_FILE_TYPE_DATA, *pg_ctx_, data_delta_range))) {
    LOG_WARN("failed to converge data task", K(ret));
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
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_mount_file_path(
                 cluster_dest, OB_SYS_TENANT_ID, pg_ctx.piece_triple_.round_id_, path))) {
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(get_file_range(
                 pg_ctx.src_backup_dest_, src_storage_info, file_type, pg_ctx.piece_triple_, pg_key, src_file_range))) {
    LOG_WARN("failed to get src data file range", K(ret), K(pg_ctx));
  } else if (OB_FAIL(get_file_range(
                 pg_ctx.dst_backup_dest_, dst_storage_info, file_type, pg_ctx.piece_triple_, pg_key, dst_file_range))) {
    LOG_WARN("failed to get src data file range", K(ret), K(pg_ctx));
  } else if (OB_FAIL(cal_data_file_range_delta(
                 pg_key, pg_ctx.piece_triple_, src_file_range, dst_file_range, delta_range))) {
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
    for (int64_t i = start; OB_SUCC(ret) && i <= end; ++i) {
      FileInfo src_info;
      FileInfo dst_info;
      bool src_file_exist = false;
      if (OB_FAIL(get_file_path_info(task.src_backup_dest_,
              task.src_storage_info_,
              pg_key,
              file_type,
              OB_START_INCARNATION,
              task.piece_triple_,
              i,
              src_info))) {
        LOG_WARN("failed to get file path info", K(ret), K(pg_key));
      } else if (OB_FAIL(get_file_path_info(task.dst_backup_dest_,
                     task.dst_storage_info_,
                     pg_key,
                     file_type,
                     OB_START_INCARNATION,
                     task.piece_triple_,
                     i,
                     dst_info))) {
        LOG_WARN("failed to get file path info", K(ret), K(pg_key));
      } else if (OB_FAIL(check_file_exist(src_info, src_file_exist))) {
        LOG_WARN("failed to check file exist", KR(ret), K(src_info));
      } else if (!src_file_exist) {
        // do nothing
      } else if (OB_FAIL(do_file_transfer(src_info, dst_info))) {
        LOG_WARN("failed to do file transfer", K(ret), K(src_info), K(dst_info));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogPGTask::get_file_range(const char* backup_dest, const common::ObString& storage_info,
    const archive::LogArchiveFileType file_type, const ObBackupPieceTriple& piece_triple, const common::ObPGKey& pg_key,
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
                 backup_dest, storage_info.ptr(), pg_key, file_type, OB_START_INCARNATION, piece_triple, path, pos))) {
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
  const int64_t piece_id = task.piece_triple_.piece_id_;
  const int64_t piece_create_date = task.piece_triple_.create_date_;
  if (OB_FAIL(file_store.init(task.src_backup_dest_,
          task.src_storage_info_,
          cluster_name,
          cluster_id,
          tenant_id,
          incarnation,
          round,
          piece_id,
          piece_create_date))) {
    LOG_WARN("archive file store init fail", KR(ret), K(task), K(incarnation), K(round));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::get_file_path_info(const char* backup_dest, const char* storage_info,
    const common::ObPGKey& pg_key, const archive::LogArchiveFileType file_type, const int64_t incarnation,
    const ObBackupPieceTriple& triple, const int64_t file_id, FileInfo& file_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(build_archive_file_path(
                 backup_dest, storage_info, pg_key, file_type, incarnation, triple, file_id, file_info.dest_path_))) {
    LOG_WARN("failed to build archive file path", K(ret));
  } else if (OB_FAIL(databuff_printf(file_info.storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH, "%s", storage_info))) {
    LOG_WARN("failed to data buff printf", KR(ret), K(storage_info));
  } else {
    file_info.uri_ = ObString::make_string(file_info.dest_path_);
    file_info.info_ = ObString::make_string(file_info.storage_info_);
  }
  return ret;
}

// extract last log may fail
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(get_file_path_info(task.src_backup_dest_,
                 task.src_storage_info_,
                 task.pg_key_,
                 file_type,
                 OB_START_INCARNATION,
                 task.piece_triple_,
                 file_id,
                 file_info))) {
    LOG_WARN("failed to get file path info", K(ret));
  } else if (OB_FAIL(init_archive_file_store_(task, OB_START_INCARNATION, task.piece_triple_.round_id_, file_store))) {
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

int ObBackupArchiveLogPGTask::cal_data_file_range_delta(const common::ObPGKey& pkey,
    const ObBackupPieceTriple& piece_triple, const FileRange& src_file_range, const FileRange& dst_file_range,
    FileRange& delta_file_range)
{
  int ret = OB_SUCCESS;
  bool interrupted = false;
  delta_file_range.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (!piece_triple.is_valid() || !src_file_range.is_valid() || !dst_file_range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cal data file range delta get invalid argument",
        K(ret),
        K(piece_triple),
        K(src_file_range),
        K(dst_file_range));
  } else if (OB_FAIL(check_archive_log_interrupted(piece_triple, src_file_range, dst_file_range, interrupted))) {
    LOG_WARN("failed to check archive log interrupted", KR(ret), K(pkey), K(piece_triple));
  } else if (interrupted) {
    ret = OB_LOG_ARCHIVE_INTERRUPTED;
    FLOG_WARN("log archive interrupted, missing files",
        KR(ret),
        K(pkey),
        K(piece_triple),
        K(src_file_range),
        K(dst_file_range));
  } else {
    if (0 == piece_triple.piece_id_) {
      delta_file_range.min_file_id_ = dst_file_range.max_file_id_;
      delta_file_range.max_file_id_ = src_file_range.max_file_id_;
    } else {
      delta_file_range.min_file_id_ = src_file_range.min_file_id_;
      delta_file_range.max_file_id_ = src_file_range.max_file_id_;
    }
  }
  return ret;
}

// o indicates not exist, x indicates exist
// // case-1: there is not any file in whether src or dst
// //      1 2 3 4 5
// //  src o o o o o
// //  dst o o o o o no cutoff
// // case 0: exist in src, not exist in dst, src start from 1
// //      1 2 3 4 5
// //  src x x x x x
// //  dst o o o o o no cutoff
// // case 1: 4,5 exist in src; 1,2,3 exist in dest
// //      1 2 3 4 5
// //  src o o o x x
// //  dst x x x o o no cutoff
// // case 2: 4,5 exist in src; 1,2 exist in dst
// //      1 2 3 4 5
// //  src o o o x x
// //  dst x x o o o cutoff
// // case 3: 3,4,5 exist in src; 1,2,3 in dst
// //      1 2 3 4 5
// //  src o o x x x
// //  dst x x x o o no cutoff
// // case 4: 3,4,5 exist in src; nothing in dst
// //      1 2 3 4 5
// //  src o o x x x
// //  dst o o o o o cutoff
int ObBackupArchiveLogPGTask::check_archive_log_interrupted(const ObBackupPieceTriple& piece_triple,
    const FileRange& src_file_range, const FileRange& dst_file_range, bool& interrupted)
{
  int ret = OB_SUCCESS;
  UNUSEDx(piece_triple, src_file_range, dst_file_range);
  interrupted = false;
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
  file_length = 0;
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
  // if uri prefix matches OB_OSS_PREFIX
  char file_prefix_path[OB_MAX_ARCHIVE_PATH_LENGTH] = "";
  ObStorageUtil util(false);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(build_archive_file_prefix(pg_task.dst_backup_dest_,
                 pg_task.dst_storage_info_,
                 pg_task.pg_key_,
                 file_type,
                 OB_START_INCARNATION,
                 pg_task.piece_triple_,
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

int ObBackupArchiveLogPGTask::do_file_transfer(const FileInfo& src_info, const FileInfo& dst_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
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
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      src_file_len = 0;
    } else {
      LOG_WARN("failed to get src file len", K(ret), K(src_info));
    }
  } else if (OB_FAIL(get_file_length(dst_info, dst_file_len))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      dst_file_len = 0;
    } else {
      LOG_WARN("failed to get dst file len", K(ret), K(dst_info));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (0 == src_file_len) {
    // do nothing if src file is empty
  } else if (dst_file_len == src_file_len) {
    LOG_DEBUG("no need to do file transfer if size match", K(dst_file_len), K(src_file_len));
  } else if (dst_file_len > src_file_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dst file len should not greater than src file len", K(ret), K(src_file_len), K(dst_file_len));
  } else if (FALSE_IT(rbuf.buf_len_ = src_file_len - dst_file_len)) {
  } else if (OB_ISNULL(rbuf.buf_ = static_cast<char*>(bb_malloc(rbuf.buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(rbuf));
  } else if (OB_FAIL(read_part_file(src_info, dst_file_len, rbuf))) {
    LOG_WARN("failed to read part file", K(ret), K(src_info));
  } else if (OB_FAIL(dst_appender.pwrite(rbuf.buf_, rbuf.buf_len_, dst_file_len))) {
    LOG_WARN("failed to write to storage appender", K(ret), K(rbuf));
  }
  if (NULL != rbuf.buf_) {
    bb_free(rbuf.buf_);
    rbuf.reset();
  }
  if (OB_SUCCESS != (tmp_ret = dst_appender.close())) {
    LOG_WARN("failed to close storage appender", KR(ret), KR(tmp_ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
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

int ObBackupArchiveLogPGTask::build_archive_file_prefix(const char* root_path, const char* storage_info,
    const common::ObPGKey& pg_key, const LogArchiveFileType file_type, const int64_t incarnation,
    const ObBackupPieceTriple& triple, char* dest_path, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t cluster_id = GCTX.config_->cluster_id;
  const char* cluster_name = GCTX.config_->cluster;
  const uint64_t tenant_id = pg_key.get_tenant_id();
  char base_path[OB_MAX_BACKUP_PATH_LENGTH] = "";
  ObArchivePathUtil util;
  ObBackupDest backup_dest;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(backup_dest.set(root_path, storage_info))) {
    LOG_WARN("failed to set backup dest", K(ret), K(backup_dest), K(storage_info));
  } else if (OB_FAIL(util.build_base_path(backup_dest,
                 cluster_name,
                 cluster_id,
                 tenant_id,
                 incarnation,
                 triple.round_id_,
                 triple.piece_id_,
                 triple.create_date_,
                 OB_MAX_BACKUP_PATH_LENGTH,
                 base_path))) {
    LOG_WARN("failed to build_base_path", K(ret), K(pg_key));
  } else if (OB_FAIL(build_file_prefix(pg_key, base_path, file_type, dest_path, pos))) {
    LOG_WARN("failed to build file prefix", K(ret), K(pg_key));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::build_archive_file_path(const char* backup_dest, const char* storage_info,
    const common::ObPGKey& pg_key, const LogArchiveFileType file_type, const int64_t incarnation,
    const ObBackupPieceTriple& triple, const int64_t file_id, char* dest_path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(build_archive_file_prefix(
                 backup_dest, storage_info, pg_key, file_type, incarnation, triple, dest_path, pos))) {
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
    const ObBackupArchiveLogPGCtx& task, const int64_t incarnation, const ObBackupPieceTriple& triple)
{
  int ret = OB_SUCCESS;
  const ObPGKey& pg_key = task.pg_key_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", KR(ret));
  } else if (OB_FAIL(build_archive_key_prefix(task, pg_key, incarnation, triple))) {
    LOG_WARN("build_archive_key_prefix fail", KR(ret), K(incarnation), K(triple), K(pg_key));
  } else {
    if (0 == triple.piece_id_) {
      if (OB_FAIL(touch_archive_key_file(task, incarnation, triple, pg_key))) {
        LOG_WARN("touch_archive_key_file fail", KR(ret), K(incarnation), K(triple), K(pg_key));
      }
    } else {
      if (OB_FAIL(copy_archive_key_file(task, incarnation, triple, pg_key))) {
        LOG_WARN("copy_archive_key file fail", KR(ret), K(incarnation), K(triple), K(pg_key));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogPGTask::build_archive_key_prefix(const ObBackupArchiveLogPGCtx& task, const ObPGKey& pg_key,
    const int64_t incarnation, const ObBackupPieceTriple& triple)
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
  } else if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_prefix(cluster_dest,
                 pg_key.get_tenant_id(),
                 triple.round_id_,
                 triple.piece_id_,
                 triple.create_date_,
                 archive_key_prefix_path))) {
    LOG_WARN("failed to get clog archive key prefix path", KR(ret), K(cluster_dest), K(pg_key), K(triple));
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

int ObBackupArchiveLogPGTask::touch_archive_key_file(const ObBackupArchiveLogPGCtx& task, const int64_t incarnation,
    const ObBackupPieceTriple& triple, const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObBackupPath archive_key_path;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", KR(ret));
  } else if (OB_FAIL(build_archive_key_path(task, incarnation, triple, pg_key, false /*is_src*/, archive_key_path))) {
    LOG_WARN("failed to build archive key path", KR(ret), K(triple), K(pg_key));
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

int ObBackupArchiveLogPGTask::copy_archive_key_file(const ObBackupArchiveLogPGCtx& pg_ctx, const int64_t incarnation,
    const ObBackupPieceTriple& triple, const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObBackupPath src_path, dst_path;
  FileInfo src_file_info, dst_file_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", KR(ret));
  } else if (OB_FAIL(build_archive_key_path(pg_ctx, incarnation, triple, pg_key, true /*is_src*/, src_path))) {
    LOG_WARN("failed to build archive key path", KR(ret));
  } else if (OB_FAIL(build_archive_key_path(pg_ctx, incarnation, triple, pg_key, false /*is_src*/, dst_path))) {
    LOG_WARN("failed to build archive key path", KR(ret));
  } else if (OB_FAIL(get_archive_key_file_info(pg_ctx, src_path, true /*is_src*/, src_file_info))) {
    LOG_WARN("failed to get archive key file info", KR(ret), K(src_path));
  } else if (OB_FAIL(get_archive_key_file_info(pg_ctx, dst_path, false /*is_src*/, dst_file_info))) {
    LOG_WARN("failed to get archive key file info", KR(ret), K(src_path));
  } else if (OB_FAIL(do_file_transfer(src_file_info, dst_file_info))) {
    LOG_WARN("failed to do file transfer", KR(ret), K(src_file_info), K(dst_file_info));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::build_archive_key_path(const ObBackupArchiveLogPGCtx& task, const int64_t incarnation,
    const ObBackupPieceTriple& triple, const ObPGKey& pg_key, const bool is_src, ObBackupPath& archive_key_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", KR(ret));
  } else if (is_src && OB_FAIL(backup_dest.set(task.src_backup_dest_, task.src_storage_info_))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(task));
  } else if (!is_src && OB_FAIL(backup_dest.set(task.dst_backup_dest_, task.dst_storage_info_))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(task));
  } else if (OB_FAIL(cluster_dest.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_path(cluster_dest,
                 pg_key.get_tenant_id(),
                 triple.round_id_,
                 triple.piece_id_,
                 triple.create_date_,
                 pg_key,
                 archive_key_path))) {
    LOG_WARN("failed to get clog archive key path", KR(ret), K(cluster_dest), K(pg_key), K(triple));
  }
  return ret;
}

int ObBackupArchiveLogPGTask::get_archive_key_file_info(const ObBackupArchiveLogPGCtx& pg_ctx,
    const share::ObBackupPath& archive_key_path, const bool is_src, FileInfo& file_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup archive log pipeline do not init", K(ret));
  } else if (OB_FAIL(
                 databuff_printf(file_info.dest_path_, OB_MAX_BACKUP_PATH_LENGTH, "%s", archive_key_path.get_ptr()))) {
    LOG_WARN("failed to data buff printf", KR(ret), K(pg_ctx));
  } else if (is_src &&
             OB_FAIL(databuff_printf(
                 file_info.storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH, "%s", pg_ctx.src_storage_info_))) {
    LOG_WARN("failed to data buff printf", KR(ret), K(pg_ctx));
  } else if (!is_src &&
             OB_FAIL(databuff_printf(
                 file_info.storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH, "%s", pg_ctx.dst_storage_info_))) {
    LOG_WARN("failed to data buff printf", KR(ret), K(pg_ctx));
  } else {
    file_info.uri_ = ObString::make_string(file_info.dest_path_);
    file_info.info_ = ObString::make_string(file_info.storage_info_);
  }
  return ret;
}

void* ObBackupArchiveLogPGTask::bb_malloc(const int64_t nbyte)
{
  ObMemAttr memattr;
  memattr.label_ = "BB_CLOG_FILE";
  return ob_malloc(nbyte, memattr);
}

void ObBackupArchiveLogPGTask::bb_free(void* ptr)
{
  ob_free(ptr);
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
