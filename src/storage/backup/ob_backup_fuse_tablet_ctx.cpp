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
#include "storage/backup/ob_backup_fuse_tablet_ctx.h"

namespace oceanbase
{
namespace backup
{

/******************ObInitBackupTabletFuseParam*********************/
ObInitBackupTabletFuseParam::ObInitBackupTabletFuseParam()
  : tenant_id_(OB_INVALID_ID),
    job_desc_(),
    backup_dest_(),
    dest_id_(0),
    backup_set_desc_(),
    ls_id_(),
    turn_id_(),
    retry_id_()
{
}

ObInitBackupTabletFuseParam::~ObInitBackupTabletFuseParam()
{
}

void ObInitBackupTabletFuseParam::reset()
{
  tenant_id_ = OB_INVALID_ID;
  backup_dest_.reset();
  dest_id_ = 0;
  backup_set_desc_.reset();
  ls_id_.reset();
  turn_id_ = 0;
  retry_id_ = -1;
}

// TODO(yanfeng): abstract common parameter later
int ObInitBackupTabletFuseParam::init(const ObLSBackupDagNetInitParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(param));
  } else if (OB_FAIL(backup_dest_.deep_copy(param.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(param));
  } else {
    tenant_id_ = param.tenant_id_;
    job_desc_ = param.job_desc_;
    dest_id_ = param.dest_id_;
    backup_set_desc_ = param.backup_set_desc_;
    ls_id_ = param.ls_id_;
    turn_id_ = param.turn_id_;
    retry_id_ = param.retry_id_;
  }
  return ret;
}

int ObInitBackupTabletFuseParam::assign(const ObInitBackupTabletFuseParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(param));
  } else if (OB_FAIL(backup_dest_.deep_copy(param.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(param));
  } else {
    tenant_id_ = param.tenant_id_;
    job_desc_ = param.job_desc_;
    dest_id_ = param.dest_id_;
    backup_set_desc_ = param.backup_set_desc_;
    ls_id_ = param.ls_id_;
    turn_id_ = param.turn_id_;
    retry_id_ = param.retry_id_;
  }
  return ret;
}

bool ObInitBackupTabletFuseParam::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && job_desc_.is_valid()
      && backup_dest_.is_valid()
      && dest_id_ > 0
      && backup_set_desc_.is_valid()
      && ls_id_.is_valid()
      && turn_id_ > 0
      && retry_id_ >= 0;
}

bool ObInitBackupTabletFuseParam::operator==(const ObInitBackupTabletFuseParam &other) const
{
  return tenant_id_ == other.tenant_id_
      && job_desc_ == other.job_desc_
      && backup_dest_ == other.backup_dest_
      && dest_id_ == other.dest_id_
      && backup_set_desc_ == other.backup_set_desc_
      && ls_id_ == other.ls_id_
      && turn_id_ == other.turn_id_
      && retry_id_ == other.retry_id_;
}

/******************ObBackupTabletGroupFuseCtx*********************/
ObBackupTabletGroupFuseCtx::ObBackupTabletGroupFuseCtx()
  : is_inited_(false),
    start_ts_(0),
    finish_ts_(0),
    task_id_(),
    param_(),
    mutex_(common::ObLatchIds::BACKUP_LOCK),
    fuser_(),
    extern_tablet_meta_writer_(),
    result_mgr_(),
    report_ctx_()
{
}

ObBackupTabletGroupFuseCtx::~ObBackupTabletGroupFuseCtx()
{
}

bool ObBackupTabletGroupFuseCtx::is_valid() const
{
  return param_.is_valid();
}

void ObBackupTabletGroupFuseCtx::reset()
{
  is_inited_ = false;
  start_ts_ = 0;
  finish_ts_ = 0;
  task_id_.reset();
  param_.reset();
}

void ObBackupTabletGroupFuseCtx::reuse()
{
  reset();
}

int ObBackupTabletGroupFuseCtx::init()
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_set_dest;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tablet group fuse ctx init twice", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K_(param));
  } else if (OB_FAIL(ObBackupPathUtil::construct_backup_set_dest(
      param_.backup_dest_, param_.backup_set_desc_, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest", K(ret), K_(param));
  } else if (OB_FAIL(fuser_.init(param_.tenant_id_, param_.backup_dest_,
      param_.backup_set_desc_, param_.ls_id_, param_.turn_id_))) {
    LOG_WARN("failed to init fuser", K(ret), K_(param));
  } else if (OB_FAIL(extern_tablet_meta_writer_.init(backup_set_dest,
                                                     param_.ls_id_,
                                                     param_.turn_id_,
                                                     param_.retry_id_,
                                                     param_.dest_id_,
                                                     true/*is_final_fuse*/,
                                                     *GCTX.bandwidth_throttle_))) {
    LOG_WARN("failed to init extern tablet meta writer", K(ret), K_(param));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupTabletGroupFuseCtx::do_fuse()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet group fuse ctx init twice", K(ret));
  } else if (OB_FAIL(fuser_.do_fuse())) {
    LOG_WARN("failed to do fuse", K(ret));
  } else {
    LOG_INFO("backup tablet group do fuse", K_(param));
  }
  return ret;
}

int ObBackupTabletGroupFuseCtx::get_next_tablet_item(
    ObBackupTabletFuseItem &tablet_item)
{
  int ret = OB_SUCCESS;
  tablet_item.reset();
  lib::ObMutexGuard guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("group fuse ctx do not init", K(ret));
  } else if (OB_FAIL(fuser_.get_next_tablet_item(tablet_item))) {
    if (OB_ITER_END == ret) {
      LOG_INFO("no next tablet item", K(ret));
    } else {
      LOG_WARN("failed to get next tablet id", K(ret));
    }
  }
  return ret;
}

int ObBackupTabletGroupFuseCtx::write_tablet_meta(const ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  blocksstable::ObSelfBufferWriter buffer_writer("BackupWriteTabletMeta");
  blocksstable::ObBufferReader buffer_reader;
  const int64_t serialize_size = param.get_serialize_size();
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet meta is invalid", K(ret), K(param));
  } else if (MAX_BACKUP_TABLET_META_SERIALIZE_SIZE < serialize_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta is too large.", K(ret), K(serialize_size), K(param));
  } else if (OB_FAIL(buffer_writer.ensure_space(backup::OB_BACKUP_READ_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(buffer_writer.write_serialize(param))) {
    LOG_WARN("failed to writer", K(ret), K(param));
  } else {
    buffer_reader.assign(buffer_writer.data(), buffer_writer.length(), buffer_writer.length());
    if (OB_FAIL(extern_tablet_meta_writer_.write_meta_data(buffer_reader, param.tablet_id_))) {
      LOG_WARN("failed to write meta data", K(ret), K(param));
    }
  }
  return ret;
}

int ObBackupTabletGroupFuseCtx::close_extern_writer()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_FAIL(extern_tablet_meta_writer_.close())) {
    LOG_WARN("failed to close extern tablet meta writer", K(ret));
  }
  return ret;
}

int ObBackupTabletGroupFuseCtx::set_result(
    const int32_t result,
    const bool need_retry,
    const enum share::ObDagType::ObDagTypeEnum type)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_FAIL(result_mgr_.set_result(result, need_retry, type))) {
    LOG_WARN("failed to set result", K(ret), K(result), K(*this));
  }
  return ret;
}

bool ObBackupTabletGroupFuseCtx::is_failed() const
{
  return result_mgr_.is_failed();
}

int ObBackupTabletGroupFuseCtx::check_allow_retry(bool &allow_retry)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  allow_retry = false;
  if (OB_FAIL(result_mgr_.check_allow_retry(allow_retry))) {
    LOG_WARN("failed to check need retry", K(ret), K(*this));
  }
  return ret;
}

int ObBackupTabletGroupFuseCtx::get_result(int32_t &result)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  result = OB_SUCCESS;
  if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("failed to get result", K(ret), K(*this));
  }
  return ret;
}

/******************ObBackupTabletFuseCtx*********************/
ObBackupTabletFuseCtx::ObBackupTabletFuseCtx()
  : param_(),
    fuse_item_(),
    group_ctx_(NULL)
{
}

ObBackupTabletFuseCtx::~ObBackupTabletFuseCtx()
{
}

bool ObBackupTabletFuseCtx::is_valid() const
{
  return param_.is_valid()
      && fuse_item_.is_valid()
      && OB_NOT_NULL(group_ctx_);
}

void ObBackupTabletFuseCtx::reset()
{
  param_.reset();
  fuse_item_.reset();
  group_ctx_ = NULL;
}

}
}
