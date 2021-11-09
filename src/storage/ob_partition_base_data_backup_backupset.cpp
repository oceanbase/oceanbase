// Copyright 2020 Alibaba Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>
// Normalizer:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#define USING_LOG_PREFIX STORAGE
#include "storage/ob_partition_base_data_backup_backupset.h"
#include "storage/ob_partition_base_data_physical_restore.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_backupset_operator.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/ob_partition_migrator.h"

#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {

/* ObBackupBackupsetPGCtx::SubTask */

ObBackupBackupsetPGCtx::SubTask::SubTask() : macro_block_cnt_(0), macro_index_list_(NULL)
{}

ObBackupBackupsetPGCtx::SubTask::~SubTask()
{
  reset();
}

void ObBackupBackupsetPGCtx::SubTask::reset()
{
  macro_index_list_ = NULL;
  macro_block_cnt_ = 0;
}

/* ObBackupBackupsetPGCtx */

ObBackupBackupsetPGCtx::ObBackupBackupsetPGCtx()
    : is_opened_(false),
      all_macro_block_need_reuse_(false),
      retry_cnt_(0),
      result_(OB_SUCCESS),
      sub_tasks_(NULL),
      sub_task_cnt_(0),
      mig_ctx_(NULL),
      pg_key_(),
      allocator_(ObModIds::BACKUP),
      throttle_(NULL),
      backup_backupset_arg_(),
      macro_index_appender_(NULL),
      reuse_macro_index_store_(),
      partition_cnt_(0),
      total_macro_block_cnt_(0),
      finish_macro_block_cnt_(0),
      cond_(),
      sub_task_idx_()
{}

ObBackupBackupsetPGCtx::~ObBackupBackupsetPGCtx()
{}

int ObBackupBackupsetPGCtx::open(
    ObMigrateCtx& mig_ctx, const common::ObPartitionKey& pg_key, common::ObInOutBandwidthThrottle& throttle)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pg_key));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::RESTORE_READER_COND_WAIT))) {
    LOG_WARN("failed to init thread cond", KR(ret));
  } else {
    mig_ctx_ = &mig_ctx;
    pg_key_ = pg_key;
    throttle_ = &throttle;
    backup_backupset_arg_ = mig_ctx_->replica_op_arg_.backup_backupset_arg_;

    if (OB_FAIL(init_macro_index_appender())) {
      LOG_WARN("failed to init macro index appender", KR(ret));
    } else if (OB_FAIL(backup_backupset_arg_.get_dst_backup_base_data_info(path_info))) {
      LOG_WARN("failed to get dst backup base data info", KR(ret));
    } else {
      if (path_info.inc_backup_set_id_ != path_info.full_backup_set_id_) {
        path_info.inc_backup_set_id_ = backup_backupset_arg_.prev_inc_backup_set_id_;
      }
      if (OB_FAIL(reuse_macro_index_store_.init(path_info, pg_key_))) {
        LOG_WARN("failed to init reuse macro index store", KR(ret), K(path_info), K(pg_key));
      } else {
        is_opened_ = true;
      }
    }
  }
  return ret;
}

int ObBackupBackupsetPGCtx::close()
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", KR(ret));
  } else if (OB_FAIL(macro_index_appender_->close())) {
    LOG_WARN("failed to close macro index appender", KR(ret));
  } else {
    allocator_.reset();
    is_opened_ = false;
  }
  return ret;
}

int ObBackupBackupsetPGCtx::wait_for_task(int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened yet", KR(ret));
  } else if (task_idx >= sub_task_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task_idx is greater than sub task count", KR(ret), K(task_idx), K(sub_task_cnt_));
  } else {
    while (OB_SUCC(ret) && task_idx != ATOMIC_LOAD(&sub_task_idx_)) {
      if (OB_FAIL(get_result())) {
        LOG_WARN("backup backupset task already failed", KR(ret), K(task_idx));
        break;
      }
      ObThreadCondGuard guard(cond_);
      if (OB_FAIL(cond_.wait_us(DEFAULT_WAIT_TIME))) {
        if (OB_TIMEOUT == ret) {
          LOG_WARN("build macro index is too slow", KR(ret), K(task_idx), K(sub_task_idx_));
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupsetPGCtx::finish_task(int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened yet", KR(ret));
  } else if (OB_UNLIKELY(task_idx != ATOMIC_LOAD(&sub_task_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "backup data, build index order unexpected", KR(ret), K(task_idx), K(sub_task_idx_));
  } else {
    ObThreadCondGuard guard(cond_);
    ATOMIC_INC(&sub_task_idx_);
    if (OB_FAIL(cond_.broadcast())) {
      STORAGE_LOG(ERROR, "failed to broadcast other condition, ", KR(ret));
    }
  }
  return ret;
}

int ObBackupBackupsetPGCtx::init_macro_index_appender()
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBackupFileAppender)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for backup file appender", KR(ret));
  } else {
    macro_index_appender_ = new (buf) ObBackupFileAppender;
    buf = NULL;

    ObPhysicalBackupArg backup_arg;
    ObBackupBaseDataPathInfo path_info;
    ObBackupPath path;
    const ObBackupFileType type = ObBackupFileType::BACKUP_MACRO_DATA_INDEX;
    if (OB_FAIL(convert_to_backup_arg(backup_backupset_arg_, backup_arg))) {
      LOG_WARN("failed to convert to backup arg", KR(ret));
    } else if (OB_FAIL(backup_arg.get_backup_base_data_info(path_info))) {  // TODO
      LOG_WARN("failed to get backup base data info", KR(ret));
    } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_index_path(
                   path_info, pg_key_.get_table_id(), pg_key_.get_partition_id(), retry_cnt_, path))) {
      LOG_WARN("failed to get macro index path", KR(ret), K(path_info), K(pg_key_));
    } else if (OB_FAIL(macro_index_appender_->open(*throttle_, backup_arg, path.get_obstr(), type))) {
      LOG_WARN("failed to init macro index appender", KR(ret), K(path_info), K(pg_key_));
    }
  }
  return ret;
}

int ObBackupBackupsetPGCtx::convert_to_backup_arg(
    const share::ObBackupBackupsetArg& in_arg, share::ObPhysicalBackupArg& out_arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(out_arg.uri_header_, OB_MAX_URI_HEADER_LENGTH, "%s", in_arg.dst_uri_header_))) {
    LOG_WARN("failed to databuff printf", KR(ret), K(in_arg));
  } else if (OB_FAIL(databuff_printf(out_arg.storage_info_, OB_MAX_URI_LENGTH, "%s", in_arg.dst_storage_info_))) {
    LOG_WARN("failed to databuff printf", KR(ret), K(in_arg));
  } else {
    out_arg.incarnation_ = in_arg.incarnation_;
    out_arg.tenant_id_ = in_arg.tenant_id_;
    out_arg.backup_set_id_ = in_arg.backup_set_id_;
    out_arg.backup_data_version_ = in_arg.backup_data_version_;
    out_arg.backup_schema_version_ = in_arg.backup_schema_version_;
    out_arg.prev_full_backup_set_id_ = in_arg.prev_full_backup_set_id_;
    out_arg.prev_inc_backup_set_id_ = in_arg.prev_inc_backup_set_id_;
    out_arg.prev_data_version_ = 0;              // TODO
    out_arg.task_id_ = 0;                        // TODO
    out_arg.backup_type_ = in_arg.backup_type_;  // TODO
    out_arg.compatible_ = in_arg.compatible_;
  }
  return ret;
}

/* ObBackupBackupsetPGFileTask */

ObBackupBackupsetPGFileCtx::ObBackupBackupsetPGFileCtx()
    : backup_backupset_arg_(),
      major_files_(),
      minor_files_(),
      allocator_(),
      pg_key_(),
      minor_task_id_(),
      is_opened_(false),
      result_(OB_SUCCESS)
{}

ObBackupBackupsetPGFileCtx::~ObBackupBackupsetPGFileCtx()
{}

int ObBackupBackupsetPGFileCtx::open(const share::ObBackupBackupsetArg& arg, const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_OPEN_TWICE;
    LOG_WARN("open twice", KR(ret));
  } else if (!arg.is_valid() && !pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg file ctx get invalid argument", KR(ret), K(arg), K(pg_key));
  } else {
    backup_backupset_arg_ = arg;
    pg_key_ = pg_key;
    is_opened_ = true;
    result_ = OB_SUCCESS;
  }
  return ret;
}

int ObBackupBackupsetPGFileCtx::close()
{
  int ret = OB_SUCCESS;
  is_opened_ = false;
  return ret;
}

/* ObBackupBackupsetFileTask */

ObBackupBackupsetFileTask::ObBackupBackupsetFileTask()
    : ObITask(TASK_TYPE_BACKUP_BACKUPSET), is_inited_(false), task_idx_(-1), mig_ctx_(NULL), pg_ctx_(NULL)
{}

ObBackupBackupsetFileTask::~ObBackupBackupsetFileTask()
{}

int ObBackupBackupsetFileTask::init(int task_idx, ObMigrateCtx& mig_ctx, ObBackupBackupsetPGFileCtx& pg_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup backupset task is inited", KR(ret));
  } else if (task_idx < 0 || task_idx >= pg_ctx.major_files_.count() + pg_ctx.minor_files_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        KR(ret),
        K(task_idx),
        "file_count",
        pg_ctx.major_files_.count() + pg_ctx.minor_files_.count());
  } else {
    task_idx_ = task_idx;
    mig_ctx_ = &mig_ctx;
    pg_ctx_ = &pg_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupBackupsetFileTask::generate_next_task(share::ObITask*& next_task)
{
  int ret = OB_SUCCESS;
  ObBackupBackupsetFileTask* tmp_next_task = NULL;
  const int64_t next_task_idx = task_idx_ + 1;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset task do not init", KR(ret));
  } else if (OB_FAIL(pg_ctx_->get_result())) {
    LOG_WARN("backup backupset task already failed", KR(ret));
  } else if (next_task_idx == pg_ctx_->major_files_.count() + pg_ctx_->minor_files_.count()) {
    ret = OB_ITER_END;
    LOG_INFO("task ended", KR(ret), K(next_task_idx));
  } else if (OB_FAIL(dag_->alloc_task(tmp_next_task))) {
    LOG_WARN("failed to allocate task", KR(ret));
  } else if (OB_FAIL(tmp_next_task->init(next_task_idx, *mig_ctx_, *pg_ctx_))) {
    LOG_WARN("failed to init next task", KR(ret));
  } else {
    next_task = tmp_next_task;
    tmp_next_task = NULL;
  }

  return ret;
}

int ObBackupBackupsetFileTask::process()
{
  int ret = OB_SUCCESS;
  const ObPGKey& pg_key = pg_ctx_->pg_key_;
  ObString file_name;
  bool is_minor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset task do not init", KR(ret));
  } else if (OB_ISNULL(pg_ctx_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("pg ctx cannot be null", KR(ret), KP(pg_ctx_));
  } else if (OB_FAIL(get_next_file(file_name, is_minor))) {
    LOG_WARN("failed to get next file", KR(ret));
  } else if (OB_FAIL(transfer_file_with_retry(pg_key, file_name, is_minor))) {
    LOG_WARN("failed to do transfer file with retry", KR(ret), K(pg_key), K(file_name));
  }

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_BACKUPSET_FILE_TASK) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    LOG_WARN("errsim backup backupset file task", KR(ret));
  }
#endif
  return ret;
}

int ObBackupBackupsetFileTask::get_next_file(common::ObString& file_name, bool& is_minor)
{
  int ret = OB_SUCCESS;
  const int64_t major_file_count = pg_ctx_->major_files_.count();
  const int64_t minor_file_count = pg_ctx_->minor_files_.count();
  const int64_t total_file_count = major_file_count + minor_file_count;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset task do not init", KR(ret));
  } else if (task_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx should not be less than 0", KR(ret), K(task_idx_));
  } else if (task_idx_ <= major_file_count - 1) {
    file_name = pg_ctx_->major_files_.at(task_idx_);
    is_minor = false;
  } else if (task_idx_ >= major_file_count && task_idx_ <= total_file_count - 1) {
    file_name = pg_ctx_->minor_files_.at(task_idx_ - major_file_count);
    is_minor = true;
  }
  return ret;
}

int ObBackupBackupsetFileTask::check_and_mkdir(const common::ObPGKey& pg_key, const bool is_minor)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false);
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  const share::ObBackupBackupsetArg& bb_arg = pg_ctx_->backup_backupset_arg_;
  ObString dst_storage_info(bb_arg.dst_storage_info_);
  const int64_t minor_task_id = pg_ctx_->minor_task_id_;
  const int64_t table_id = pg_key.get_table_id();
  const int64_t partition_id = pg_key.get_partition_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset task do not init", KR(ret));
  } else if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg key is not valid", KR(ret), K(pg_key));
  } else if (OB_FAIL(bb_arg.get_dst_backup_base_data_info(path_info))) {
    LOG_WARN("failed to get dst backup base data info", KR(ret), K(bb_arg));
  } else {
    if (is_minor) {
      if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_minor_data_path(
              path_info, table_id, partition_id, minor_task_id, path))) {
        LOG_WARN("failed to get tenant pg minor data path", KR(ret), K(path_info), K(pg_key), K(minor_task_id));
      }
    } else {
      if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_major_data_path(path_info, table_id, partition_id, path))) {
        LOG_WARN("failed to get tenant pg major data path", KR(ret), K(path_info), K(pg_key));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(util.mkdir(path.get_obstr(), dst_storage_info))) {
        LOG_WARN("failed to mkdir", KR(ret), K(pg_key), K(path));
      }
    }
  }
  return ret;
}

int ObBackupBackupsetFileTask::transfer_file_with_retry(
    const common::ObPGKey& pg_key, const common::ObString& file_name, const bool is_minor)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset task do not init", KR(ret));
  } else {
    while (retry_times < RETRY_TIME) {
      if (OB_FAIL(check_and_mkdir(pg_key, is_minor))) {
        LOG_WARN("failed to check and mkdir", KR(ret), K(pg_key), K(is_minor));
      } else if (OB_FAIL(transfer_file(pg_key, file_name, is_minor))) {
        LOG_WARN("failed to relay major blocks", KR(ret), K(pg_key), K(file_name), K(is_minor), K(retry_times));
      }

      if (OB_SUCC(ret)) {
        break;
      }

      if (OB_FAIL(ret)) {
        ++retry_times;
        usleep(RETRY_SLEEP_INTERVAL);
      }
    }
  }
  return ret;
}

int ObBackupBackupsetFileTask::transfer_file(
    const common::ObPGKey& pg_key, const common::ObString& file, const bool is_minor)
{
  int ret = OB_SUCCESS;
  int64_t transfer_len = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset task do not init", KR(ret));
  } else if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg key is not valid", KR(ret), K(pg_key));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(inner_transfer_file(pg_key, file, is_minor, transfer_len))) {
        LOG_WARN("failed to inner transfer file ", KR(ret), K(pg_key), K(file), K(is_minor), K(transfer_len));
      }
      if (0 == transfer_len) {
        LOG_INFO("transfer ended", KR(ret), K(pg_key), K(file));
        break;
      }
    }
  }
  return ret;
}

int ObBackupBackupsetFileTask::inner_transfer_file(
    const common::ObPGKey& pg_key, const common::ObString& file_name, const bool is_minor, int64_t& transfer_len)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  transfer_len = 0;
  ObStorageAppender appender(CREATE_OPEN_NOLOCK);
  // TODO: not support backup backup to cos, will support by yanfeng later.
  ObStorageAppender::AppenderParam param;
  ObBackupPath src_path, dst_path;
  ObString src_storage_info, dst_storage_info;
  int64_t src_len = 0;
  int64_t dst_len = 0;
  char* buf = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset task do not init", KR(ret));
  } else if (OB_ISNULL(pg_ctx_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("pg ctx must not be null", KR(ret), KP(pg_ctx_));
  } else if (FALSE_IT(src_storage_info.assign_ptr(pg_ctx_->backup_backupset_arg_.src_storage_info_,
                 strlen(pg_ctx_->backup_backupset_arg_.src_storage_info_)))) {
    // assign ptr
  } else if (FALSE_IT(dst_storage_info.assign_ptr(pg_ctx_->backup_backupset_arg_.dst_storage_info_,
                 strlen(pg_ctx_->backup_backupset_arg_.dst_storage_info_)))) {
    // assign ptr
  } else if (OB_FAIL(get_file_backup_path(pg_key, file_name, true /*is_src*/, is_minor, src_path))) {
    LOG_WARN("failed to get src file path info", KR(ret), K(pg_key), K(file_name), K(is_minor));
  } else if (OB_FAIL(get_file_backup_path(pg_key, file_name, false /*is_src*/, is_minor, dst_path))) {
    LOG_WARN("failed to get dst file path info", KR(ret), K(pg_key), K(file_name), K(is_minor));
  } else if (OB_FAIL(appender.open(dst_path.get_obstr(), dst_storage_info, param))) {
    LOG_WARN("failed to open dst storage appender", KR(ret), K(dst_path), K(dst_storage_info));
  } else if (OB_FAIL(get_file_length(src_path.get_obstr(), src_storage_info, src_len))) {
    LOG_WARN("failed to get src file length", KR(ret), K(src_path), K(src_storage_info));
  } else if (OB_FAIL(get_file_length(dst_path.get_obstr(), dst_storage_info, dst_len))) {
    LOG_WARN("failed to get dst file length", KR(ret), K(dst_path), K(dst_storage_info));
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      dst_len = 0;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (dst_len == src_len) {
    // do nothing
  } else if (dst_len > src_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dst len should not be greater then src len", KR(ret), K(pg_key), K(file_name), K(src_len), K(dst_len));
  } else if (OB_FAIL(get_transfer_len(src_len - dst_len, transfer_len))) {
    LOG_WARN("failed to get transfer len", KR(ret), K(src_len), K(dst_len));
  } else if (OB_ISNULL(buf = static_cast<char*>(bb_malloc(transfer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(transfer_len));
  } else if (OB_FAIL(read_part_file(src_path.get_obstr(), src_storage_info, dst_len, transfer_len, buf))) {
    LOG_WARN("failed to read part file", KR(ret), K(src_path), K(src_storage_info));
  } else if (OB_FAIL(appender.pwrite(buf, transfer_len, dst_len))) {
    LOG_WARN("failed to write appender file", KR(ret));
  }

  if (NULL != buf) {
    bb_free(buf);
  }
  if (OB_SUCCESS != (tmp_ret = appender.close())) {
    LOG_WARN("failed to close storage appender", KR(ret), KR(tmp_ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }

  return ret;
}

int ObBackupBackupsetFileTask::get_file_backup_path(const common::ObPGKey& pg_key, const common::ObString& file_name,
    const bool is_src, const bool is_minor, ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  path.reset();
  ObBackupBaseDataPathInfo path_info;
  const share::ObBackupBackupsetArg& bb_arg = pg_ctx_->backup_backupset_arg_;
  const int64_t minor_task_id = pg_ctx_->minor_task_id_;
  const int64_t table_id = pg_key.get_table_id();
  const int64_t partition_id = pg_key.get_partition_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset task do not init", KR(ret));
  } else if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg key is not valid", KR(ret), K(pg_key));
  } else if (is_src && OB_FAIL(bb_arg.get_src_backup_base_data_info(path_info))) {
    LOG_WARN("failed to get src backup base data info", KR(ret), K(bb_arg));
  } else if (!is_src && OB_FAIL(bb_arg.get_dst_backup_base_data_info(path_info))) {
    LOG_WARN("failed to get src backup base data info", KR(ret), K(bb_arg));
  } else {
    if (is_minor) {
      if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_minor_data_path(
              path_info, table_id, partition_id, minor_task_id, path))) {
        LOG_WARN("failed to get tenant pg minor data path", KR(ret), K(path_info), K(pg_key), K(minor_task_id));
      }
    } else {
      if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_major_data_path(path_info, table_id, partition_id, path))) {
        LOG_WARN("failed to get tenant pg major data path", KR(ret), K(path_info), K(pg_key));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(path.join(file_name))) {
        LOG_WARN("failed to join path", KR(ret), K(path), K(file_name));
      }
    }
  }
  return ret;
}

int ObBackupBackupsetFileTask::get_file_length(
    const common::ObString& file_path, const common::ObString& storage_info, int64_t& length)
{
  int ret = OB_SUCCESS;
  length = 0;
  ObStorageUtil util(false);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset file task do not init", K(ret));
  } else if (OB_FAIL(util.get_file_length(file_path, storage_info, length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", KR(ret), K(file_path));
    }
  }
  return ret;
}

int ObBackupBackupsetFileTask::read_part_file(
    const ObString& file_path, const ObString& storage_info, const int64_t offset, const int64_t len, char*& buf)
{
  int ret = OB_SUCCESS;
  int64_t read_len = 0;
  ObStorageUtil util(false /*need retry*/);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset file task do not init", KR(ret));
  } else if (OB_FAIL(util.read_part_file(file_path, storage_info, buf, len, offset, read_len))) {
    LOG_WARN("failed to read part file", KR(ret), K(file_path), K(storage_info));
  } else if (read_len != len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read len not expected", K(ret), K(read_len), K(len));
  }
  return ret;
}

int ObBackupBackupsetFileTask::get_transfer_len(const int64_t delta_len, int64_t& transfer_len)
{
  int ret = OB_SUCCESS;
  transfer_len = 0;
  const int64_t MAX_PART_FILE_SIZE = 2 * 1024 * 1024;  // 2MB
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset task do not init", KR(ret));
  } else if (delta_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get transfer len get invalid argument", KR(ret), K(delta_len));
  } else {
    transfer_len = std::min(delta_len, MAX_PART_FILE_SIZE);
  }
  return ret;
}

void* ObBackupBackupsetFileTask::bb_malloc(const int64_t nbyte)
{
  ObMemAttr memattr;
  memattr.label_ = "BB_BASE_DATA";
  return ob_malloc(nbyte, memattr);
}

void ObBackupBackupsetFileTask::bb_free(void* ptr)
{
  ob_free(ptr);
}

/* ObBackupBackupsetFinishTask */

ObBackupBackupsetFinishTask::ObBackupBackupsetFinishTask()
    : ObITask(TASK_TYPE_BACKUP_BACKUPSET), is_inited_(false), mig_ctx_(NULL), pg_ctx_()
{}

ObBackupBackupsetFinishTask::~ObBackupBackupsetFinishTask()
{}

int ObBackupBackupsetFinishTask::init(ObMigrateCtx& mig_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup backupset finish task init twice", KR(ret));
  } else {
    mig_ctx_ = &mig_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupBackupsetFinishTask::process()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_REPORT_BACKUP_BACKUPSET_TASK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset finish task do not init", KR(ret));
  } else if (OB_FAIL(pg_file_ctx_.close())) {
    LOG_WARN("failed to close backup backupset pg ctx", KR(ret));
  } else {
    LOG_INFO("backup backupset finish", KR(ret));
  }
  return ret;
}

int ObBackupBackupsetFinishTask::report_pg_task_stat()
{
  int ret = OB_SUCCESS;
  ObPGBackupBackupsetTaskRowKey row_key;
  ObPGBackupBackupsetTaskStat stat;
  const share::ObBackupBackupsetArg& arg = pg_ctx_.backup_backupset_arg_;
  const bool dropped_tenant = arg.tenant_dropped_;
  SimpleBackupBackupsetTenant tenant;
  tenant.is_dropped_ = dropped_tenant;
  tenant.tenant_id_ = pg_ctx_.pg_key_.get_tenant_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset finish task do not init", KR(ret));
  } else if (OB_FAIL(row_key.set(arg.tenant_id_,
                 arg.job_id_,
                 OB_START_INCARNATION,
                 arg.backup_set_id_,
                 arg.copy_id_,
                 arg.pg_key_.get_table_id(),
                 arg.pg_key_.get_partition_id()))) {
    LOG_WARN("failed to set row key", KR(ret), K(arg));
  } else if (OB_FAIL(stat.set(pg_ctx_.partition_cnt_,
                 pg_ctx_.total_macro_block_cnt_,
                 pg_ctx_.partition_cnt_,
                 pg_ctx_.finish_macro_block_cnt_))) {
    LOG_WARN("failed to set task stat", KR(ret), K(stat));
  } else if (!row_key.is_valid() || !stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("report pg task get invalid argument", KR(ret));
  } else if (OB_FAIL(ObPGBackupBackupsetOperator::update_task_stat(tenant, row_key, stat, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to update task status", KR(ret), K(tenant), K(row_key), K(stat));
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase
