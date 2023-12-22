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

#include "storage/ls/ob_ls.h"
#include "storage/slog_ckpt/ob_tenant_meta_snapshot_handler.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_defs.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_meta_table.h"

namespace oceanbase
{
namespace storage
{

int ObLSSnapshot::init(const ObTenantSnapshotID& tenant_snapshot_id,
                       const ObLSID& ls_id,
                       common::ObConcurrentFIFOAllocator* build_ctx_allocator,
                       ObTenantMetaSnapshotHandler* meta_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSSnapshot init twice", KR(ret), KPC(this));
  } else if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is not valid", KR(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is not valid", KR(ret));
  } else if (OB_ISNULL(build_ctx_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build_ctx_allocator is unexpected nullptr", KR(ret));
  } else if (OB_ISNULL(meta_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_handler is unexpected nullptr", KR(ret));
  } else {
    tenant_snapshot_id_ = tenant_snapshot_id;
    ls_id_ = ls_id;
    build_ctx_allocator_ = build_ctx_allocator;
    meta_handler_ = meta_handler;
    is_inited_ = true;
  }
  return ret;
}

void ObLSSnapshot::destroy()
{
  if (IS_INIT) {
    LOG_INFO("ls snapshot destroy", KPC(this));
    reset();
  }
}

int ObLSSnapshot::build_ls_snapshot(ObLS* ls)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLSSnapshot is not init", KR(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ls is nullptr", KR(ret), KPC(this));
  } else {
    FLOG_INFO("build_ls_snapshot start", KPC(this));

    if (is_build_ctx_lost_()) {
      LOG_INFO("ls_snapshot build ctx lost, maybe observer restart before snapshot result reported",
          KPC(this));
      if (OB_FAIL(clear_meta_snapshot_())) {
        LOG_WARN("fail to clear_meta_snapshot_", KR(ret), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      SCN max_sstable_range_scn;
      if (OB_FAIL(try_alloc_build_ctx_())) {
        LOG_WARN("fail to try_alloc_build_ctx_", KR(ret), KPC(this));
      } else if (OB_ISNULL(build_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("build_ctx_ is unexpected nullptr", KR(ret), KPC(this));
      } else if (FALSE_IT(build_ctx_->set_rebuild_seq_start(ls->get_ls_meta().get_rebuild_seq()))) {
      } else if (OB_FAIL(ls->get_ls_meta().check_valid_for_backup())) {
        LOG_WARN("fail to check valid for backup", KR(ret), "ls_meta", ls->get_ls_meta());
      } else if (OB_FAIL(build_ctx_->set_ls_meta_package(ls))) {
        LOG_WARN("fail to set ls meta package", KR(ret), K(tenant_snapshot_id_), K(ls_id_));
      } else if (OB_FAIL(build_meta_snapshot_(max_sstable_range_scn))) {
        LOG_WARN("fail to build_meta_snapshot_", KR(ret), K(tenant_snapshot_id_), K(ls_id_));
      } else if (FALSE_IT(build_ctx_->set_end_interval_scn(
              MAX(max_sstable_range_scn, build_ctx_->get_begin_interval_scn())))) {
      } else if (OB_FAIL(ls->get_ls_meta().check_valid_for_backup())) {
        LOG_WARN("fail to check valid for backup", KR(ret), "ls_meta", ls->get_ls_meta());
      } else if (FALSE_IT(build_ctx_->set_rebuild_seq_end(ls->get_ls_meta().get_rebuild_seq()))) {
        LOG_WARN("fail to get end scn", KR(ret), KPC(this), "ls_meta", ls->get_ls_meta());
      } else {
        if (meta_existed_) {
          build_ctx_->determine_final_rlt();
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("ls snapshot meta is not existed", KR(ret), KPC(this), KPC(ls));
          build_ctx_->set_failed();
        }
      }
    }
  }

  FLOG_INFO("build_ls_snapshot finished", KR(ret), KPC(this));
  return ret;
}

int ObLSSnapshot::gc_ls_snapshot()
{
  int ret = OB_SUCCESS;

  try_free_build_ctx_();

  if (OB_FAIL(clear_meta_snapshot_())) {
    LOG_WARN("fail to clear_meta_snapshot_", KR(ret), KPC(this));
  }

  LOG_INFO("gc ls snapshot finished", KR(ret), KPC(this));
  return ret;
}

void ObLSSnapshot::notify_tenant_gc()
{
  LOG_INFO("notify tenant gc", KPC(this));

  try_free_build_ctx_();
  meta_existed_ = false;
}

int ObLSSnapshot::get_report_info(ObLSSnapshotReportInfo& info)
{
  int ret = OB_SUCCESS;

  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObLSSnapshotReportInfo is not valid", KR(ret), K(info), KPC(this));
  } else if (!is_valid_for_reporting_succ()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ObLSSnapshot state is not valid for reporting", KR(ret), KPC(this));
  } else {
    info.to_success(build_ctx_->get_begin_interval_scn(),
                    build_ctx_->get_end_interval_scn(),
                    build_ctx_->get_ls_meta_package_const_ptr());
  }
  return ret;
}

int ObLSSnapshot::get_ls_snapshot_vt_info(ObLSSnapshotVTInfo &info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLSSnapshot is not init", KR(ret), KPC(this));
  } else {
    info.set_ls_id(ls_id_);
    info.set_tenant_snapshot_id(tenant_snapshot_id_);
    info.set_meta_existed(meta_existed_);
    // build_ctx_ might be deleted if create ls_snapshot successfully,
    // it's ok that we leave it be
    lib::ObMutexGuard build_ctx_guard(build_ctx_mutex_);
    if (OB_NOT_NULL(build_ctx_)) {
      info.set_has_build_ctx(true);
      ObLSSnapshotBuildCtxVTInfo &build_ctx_info = info.get_build_ctx_info();
      build_ctx_info.set_build_status(
        ObString::make_string(ObLSSnapshotBuildCtx::status_to_str(build_ctx_->get_build_status())));
      build_ctx_info.set_rebuild_seq_start(build_ctx_->get_rebuild_seq_start());
      build_ctx_info.set_rebuild_seq_end(build_ctx_->get_rebuild_seq_end());
      build_ctx_->get_ls_meta_package(build_ctx_info.get_ls_meta_package());
      build_ctx_info.set_end_interval_scn(build_ctx_->get_end_interval_scn());
    } else {
      info.set_has_build_ctx(false);
    }
  }
  return ret;
}

void ObLSSnapshot::try_free_build_ctx()
{
  try_free_build_ctx_();
}

void ObLSSnapshot::try_set_failed()
{
  if (OB_NOT_NULL(build_ctx_)) {
    build_ctx_->set_failed();
  }

  LOG_INFO("ObLSSnapshot exec try_set_failed", KPC(this));
}

bool ObLSSnapshot::is_build_finished() const
{
  return build_ctx_ != nullptr && build_ctx_->is_finished();
}

bool ObLSSnapshot::is_build_ctx_lost_() const
{
  return meta_existed_ && nullptr == build_ctx_;
}

bool ObLSSnapshot::is_valid_for_reporting_succ() const
{
  return meta_existed_ && build_ctx_ != nullptr && build_ctx_->is_succ();
}

int ObLSSnapshot::load()
{
  int ret = OB_SUCCESS;

  meta_existed_ = true;
  LOG_INFO("ls snapshot load succ", KPC(this));
  return ret;
}

int ObLSSnapshot::build_meta_snapshot_(SCN& max_sstable_range_scn)
{
  int ret = OB_SUCCESS;

  SCN tmp_clog_max_scn;
  if (meta_existed_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls snapshot meta is already existed", KR(ret), KPC(this));
  } else if (OB_FAIL(meta_handler_->create_single_ls_snapshot(tenant_snapshot_id_,
                                                              ls_id_,
                                                              tmp_clog_max_scn))) {
    LOG_ERROR("fail to create_single_ls_snapshot", KR(ret), KPC(this));
  } else {
    meta_existed_ = true;
    max_sstable_range_scn = tmp_clog_max_scn;
  }

  LOG_INFO("build ls meta snapshot finished", KR(ret), KPC(this), K(tmp_clog_max_scn));
  return ret;
}

int ObLSSnapshot::clear_meta_snapshot_()
{
  int ret = OB_SUCCESS;

  if (meta_existed_) {
    if (OB_FAIL(meta_handler_->delete_single_ls_snapshot(tenant_snapshot_id_, ls_id_))) {
      LOG_ERROR("fail to delete_single_ls_snapshot", KR(ret), KPC(this));
    } else {
      meta_existed_ = false;
    }
  }

  LOG_INFO("clear ls meta snapshot finished", KR(ret), KPC(this));
  return ret;
}

int ObLSSnapshot::try_alloc_build_ctx_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObMemAttr memattr(tenant_id, "LSSnapBuildCtx");

  lib::ObMutexGuard build_ctx_guard(build_ctx_mutex_);
  if (nullptr == build_ctx_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = build_ctx_allocator_->alloc(sizeof(ObLSSnapshotBuildCtx), memattr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObLSSnapshotBuildCtx", KR(ret), KPC(this));
    } else {
      build_ctx_ = new (buf) ObLSSnapshotBuildCtx();
    }
  }
  return ret;
}

void ObLSSnapshot::try_free_build_ctx_()
{
  lib::ObMutexGuard build_ctx_guard(build_ctx_mutex_);
  if (build_ctx_ != nullptr) {
    build_ctx_->~ObLSSnapshotBuildCtx();
    build_ctx_allocator_->free(build_ctx_);
    build_ctx_ = nullptr;
  }
}

int ObLSSnapshot::get_tablet_meta_entry(blocksstable::MacroBlockId &tablet_meta_entry)
{
  int ret = OB_SUCCESS;

  if (!meta_existed_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ObLSSnapshot's meta not existed", KR(ret), KPC(this));
  } else if (OB_FAIL(meta_handler_->get_ls_snapshot(tenant_snapshot_id_,
                                                    ls_id_,
                                                    tablet_meta_entry))) {
    LOG_WARN("fail to get ls snapshot tablet meta entry", KR(ret), KPC(this));
  }
  return ret;
}

const char* ObLSSnapshot::ObLSSnapshotBuildCtx::LS_SNAPSHOT_BUILD_STATUS_ARRAY[] =
{
  "BUILDING",
  "FAILED",
  "SUCCESSFUL"
};

const char* ObLSSnapshot::ObLSSnapshotBuildCtx::status_to_str(ObLSSnapshotBuildCtx::BuildStatus status)
{
  STATIC_ASSERT(ARRAYSIZEOF(LS_SNAPSHOT_BUILD_STATUS_ARRAY) == static_cast<int64_t>(ObLSSnapshotBuildCtx::BuildStatus::MAX),
                "type string array size mismatch with enum ls snapshot build status count");
  const char* str = "INVALID";
  if (status >= 0 && status < ObLSSnapshotBuildCtx::BuildStatus::MAX) {
    str = LS_SNAPSHOT_BUILD_STATUS_ARRAY[static_cast<int64_t>(status)];
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid ls snapshot build status", K(status));
  }
  return str;
}

bool ObLSSnapshot::ObLSSnapshotBuildCtx::is_valid_for_reporting_succ() const
{
  int ret = OB_SUCCESS;

  bool bret = true;
  if (bret) {
    bret = rebuild_seq_start_ >= 0 && rebuild_seq_end_ >= 0 && rebuild_seq_start_ == rebuild_seq_end_;
    if (!bret) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("rebuild seq is not valid for reporting succ", KR(ret), KPC(this));
    }
  }

  if (bret) {
    bret = ls_meta_package_.is_valid();
    if (!bret) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("ls_meta_package is not valid for reporting succ", KR(ret), KPC(this));
    }
  }

  if (bret) {
    bret = end_interval_scn_.is_valid() &&
           ls_meta_package_.ls_meta_.get_clog_checkpoint_scn() <= end_interval_scn_;
    if (!bret) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("clog interval is not valid for reporting succ", KR(ret), KPC(this));
    }
  }

  return bret;
}

bool ObLSSnapshot::ObLSSnapshotBuildCtx::is_finished() const
{
  return FAILED == build_status_ || SUCCESSFUL == build_status_;
}

bool ObLSSnapshot::ObLSSnapshotBuildCtx::is_succ() const
{
  return SUCCESSFUL == build_status_;
}

bool ObLSSnapshot::ObLSSnapshotBuildCtx::is_failed() const
{
  return FAILED == build_status_;
}

void ObLSSnapshot::ObLSSnapshotBuildCtx::set_failed()
{
  build_status_ = FAILED;
}

void ObLSSnapshot::ObLSSnapshotBuildCtx::determine_final_rlt()
{
  int ret = OB_SUCCESS;

  if (is_finished()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ObLSSnapshotBuildCtx is not in building status", KR(ret), KPC(this));
  } else if (!is_valid_for_reporting_succ()) {
    ret = OB_INVALID_ERROR;
    build_status_ = FAILED;
    LOG_WARN("ObLSSnapshotBuildCtx is not valid for reporting succ, switch state to failed",
        KR(ret), KPC(this));
  } else {
    build_status_ = SUCCESSFUL;
    LOG_INFO("ObLSSnapshotBuildCtx switch state to succ", KPC(this));
  }
}

SCN ObLSSnapshot::ObLSSnapshotBuildCtx::get_begin_interval_scn() const
{
  return ls_meta_package_.ls_meta_.get_clog_checkpoint_scn();
}

int ObLSSnapshot::ObLSSnapshotBuildCtx::set_ls_meta_package(ObLS* ls)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(ls_meta_package_mutex_);

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ls is nullptr", KR(ret), KPC(this));
  } else if (OB_FAIL(ls->get_ls_meta_package(false, ls_meta_package_))) {
    LOG_WARN("fail to get ls meta package",
        KR(ret), KPC(this), KPC(ls));
  }

  return ret;
}

void ObLSSnapshot::ObLSSnapshotBuildCtx::get_ls_meta_package(ObLSMetaPackage& out)
{
  lib::ObMutexGuard guard(ls_meta_package_mutex_);
  out = ls_meta_package_;
}

}
}
