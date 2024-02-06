/**log_engine.cpp
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

#define USING_LOG_PREFIX PALF
#include "log_engine.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"                 // For UNUSED
#include "lib/oblog/ob_log_module.h"                    // PALF_LOG
#include "share/ob_errno.h"                             // ERRNO
#include "share/rc/ob_tenant_base.h"                    // mtl_malloc
#include "share/allocator/ob_tenant_mutil_allocator.h"  // ObILogAllocator
#include "log_io_task_cb_utils.h"                       // LogFlushCbCtx...
#include "log_io_task.h"                                // LogIOTask
#include "log_io_worker.h"                              // LogIOWorker
#include "log_reader_utils.h"                           // ReadBuf
#include "log_writer_utils.h"                           // LogWriteBuf
#include "lsn.h"                                        // LSN
#include "log_meta_entry.h"                             // LogMetaEntry
#include "log_group_entry_header.h"                     // LogGroupEntryHeader
#include "log_define.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{

// ===================== LogEngine start =======================
LogEngine::LogEngine() :
    block_gc_lock_(common::ObLatchIds::PALF_LOG_ENGINE_LOCK),
    min_block_max_scn_(),
    min_block_id_(LOG_INVALID_BLOCK_ID),
    base_lsn_for_block_gc_(PALF_INITIAL_LSN_VAL),
    log_meta_lock_(common::ObLatchIds::PALF_LOG_ENGINE_LOCK),
    log_meta_(),
    log_meta_storage_(),
    log_storage_(),
    log_net_service_(),
    alloc_mgr_(NULL),
    log_io_worker_(NULL),
    plugins_(NULL),
    palf_id_(INVALID_PALF_ID),
    palf_epoch_(-1),
    last_purge_throttling_ts_(OB_INVALID_TIMESTAMP),
    is_inited_(false)
{}

LogEngine::~LogEngine() { destroy(); }


int LogEngine::append_log_meta_(const LogMeta &log_meta)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = NULL;
  const int64_t buf_len = MAX_META_ENTRY_SIZE;
  if (NULL == (buf = reinterpret_cast<char *>(mtl_malloc(buf_len,
            "LogEngine")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "allocate memory failed", K(ret), K_(palf_id), K_(is_inited));
  } else if (OB_FAIL(serialize_log_meta_(log_meta, buf, buf_len))) {
    PALF_LOG(ERROR, "serialize_log_meta_ failed", K(ret), K_(palf_id), K_(is_inited), K(log_meta));
  } else if (OB_FAIL(log_meta_storage_.append_meta(buf, buf_len))) {
    PALF_LOG(ERROR, "log_meta_storage_ append failed", K(ret), K_(palf_id), K_(is_inited));
  }
  if (NULL != buf) {
    mtl_free(buf);
  }
  return ret;
}

int LogEngine::init(const int64_t palf_id,
                    const char *base_dir,
                    const LogMeta &log_meta,
                    ObILogAllocator *alloc_mgr,
                    ILogBlockPool *log_block_pool,
                    LogHotCache *hot_cache,
                    LogRpc *log_rpc,
                    LogIOWorker *log_io_worker,
                    LogPlugins *plugins,
                    const int64_t palf_epoch,
                    const int64_t log_storage_block_size,
                    const int64_t log_meta_storage_block_ize)
{
  int ret = OB_SUCCESS;
  auto log_meta_storage_update_manifest_cb = [](const block_id_t max_block_id, const bool in_restart) {
    // do nothing
    return OB_SUCCESS;
  };
  auto log_storage_update_manifest_cb = [this](const block_id_t max_block_id, const bool in_restart) {
    return this->update_manifest(max_block_id);
  };
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogEngine has inited!!!", K(ret), K(palf_id));
  } else if (false == is_valid_palf_id(palf_id) || OB_ISNULL(base_dir) || OB_ISNULL(alloc_mgr)
             || OB_ISNULL(log_rpc) || OB_ISNULL(log_io_worker) || OB_ISNULL(plugins)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR,
             "Invalid argument!!!",
             K(ret),
             K(palf_id),
             K(base_dir),
             K(log_meta),
             K(hot_cache),
             K(alloc_mgr),
             K(log_io_worker),
             K(plugins));
    // NB: Nowday, LSN is strongly dependent on physical block,
  } else if (OB_FAIL(log_meta_storage_.init(base_dir,
                                            "meta",
                                            LSN(PALF_INITIAL_LSN_VAL),
                                            palf_id,
                                            log_meta_storage_block_ize,
                                            LOG_DIO_ALIGN_SIZE,
                                            LOG_DIO_ALIGNED_BUF_SIZE_META,
                                            log_meta_storage_update_manifest_cb,
                                            log_block_pool,
                                            plugins,
                                            NULL /*set hot_cache to NULL for meta storage*/))) {
    PALF_LOG(ERROR, "LogMetaStorage init failed", K(ret), K(palf_id), K(base_dir));
  } else if(0 != log_storage_block_size
      && OB_FAIL(log_storage_.init(base_dir,
                                   "log",
                                   log_meta.get_log_snapshot_meta().base_lsn_,
                                   palf_id,
                                   log_storage_block_size,
                                   LOG_DIO_ALIGN_SIZE,
                                   LOG_DIO_ALIGNED_BUF_SIZE_REDO,
                                   log_storage_update_manifest_cb,
                                   log_block_pool,
                                   plugins,
                                   hot_cache))) {
    PALF_LOG(ERROR, "LogStorage init failed!!!", K(ret), K(palf_id), K(base_dir), K(log_meta));
  } else if (OB_FAIL(log_net_service_.init(palf_id, log_rpc))) {
    PALF_LOG(ERROR, "LogNetService init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(append_log_meta_(log_meta))) {
    PALF_LOG(ERROR, "append_log_meta_ failed", K(ret));
  } else {
    palf_id_ = palf_id;
    log_meta_ = log_meta;
    alloc_mgr_ = alloc_mgr;
    log_io_worker_ = log_io_worker;
    plugins_ = plugins;
    palf_epoch_ = palf_epoch;
    base_lsn_for_block_gc_ = log_meta.get_log_snapshot_meta().base_lsn_;
    is_inited_ = true;
    PALF_LOG(INFO, "LogEngine init success", K(ret), K(palf_id), K(base_dir), K(palf_epoch));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void LogEngine::destroy()
{
  if (IS_INIT) {
    PALF_LOG(INFO, "LogEngine destroy", K_(palf_id), K_(is_inited));
    is_inited_ = false;
    palf_id_ = INVALID_PALF_ID;
    log_io_worker_ = NULL;
    alloc_mgr_ = NULL;
    log_net_service_.destroy();
    log_meta_storage_.destroy();
    log_meta_.reset();
    log_storage_.destroy();
    base_lsn_for_block_gc_.reset();
    min_block_id_ = LOG_INVALID_BLOCK_ID;
    min_block_max_scn_.reset();
    last_purge_throttling_ts_ = OB_INVALID_TIMESTAMP;
  }
}

int LogEngine::load(const int64_t palf_id,
                    const char *base_dir,
                    common::ObILogAllocator *alloc_mgr,
                    ILogBlockPool *log_block_pool,
                    LogHotCache *hot_cache,
                    LogRpc *log_rpc,
                    LogIOWorker *log_io_worker,
                    LogPlugins *plugins,
                    LogGroupEntryHeader &entry_header,
                    const int64_t palf_epoch,
                    bool &is_integrity,
                    const int64_t log_storage_block_size,
                    const int64_t log_meta_storage_block_ize)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("load", 0);
  block_id_t expected_next_block_id = LOG_INVALID_BLOCK_ID;
  auto log_meta_storage_update_manifest_cb = [](const block_id_t new_expected_next_block_id, const bool in_restart) {
    // do nothing
    return OB_SUCCESS;
  };
  auto log_storage_update_manifest_cb = [&expected_next_block_id, this](const block_id_t new_expected_next_block_id, const bool in_restart) {
    int ret = OB_SUCCESS;
    if (in_restart) {
      if (new_expected_next_block_id == expected_next_block_id + 1) {
        PALF_LOG(INFO, "need update_manifest in restart", K(in_restart), K(new_expected_next_block_id), K(expected_next_block_id));
        ret = this->update_manifest(new_expected_next_block_id);
      } else {
        PALF_LOG(INFO, "no need update_manifest in restart", K(in_restart), K(new_expected_next_block_id), K(expected_next_block_id));
      }
    } else {
      ret = this->update_manifest(new_expected_next_block_id);
    }
    return ret;
  };
  LSN last_group_entry_header_lsn;
  LSN last_meta_entry_start_lsn;
  LogMetaEntryHeader unused_meta_entry_header;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogEngine has initted!!!", K(ret), K(palf_id));
  } else if (false == is_valid_palf_id(palf_id) || OB_ISNULL(base_dir)
      || OB_ISNULL(alloc_mgr) || OB_ISNULL(log_rpc) || OB_ISNULL(log_io_worker)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(base_dir), K(hot_cache), K(alloc_mgr),
             K(log_io_worker));
  } else if (OB_FAIL(log_meta_storage_.load(base_dir,
                                            "meta",
                                            LSN(PALF_INITIAL_LSN_VAL),
                                            palf_id,
                                            log_meta_storage_block_ize,
                                            LOG_DIO_ALIGN_SIZE,
                                            LOG_DIO_ALIGNED_BUF_SIZE_META,
                                            log_meta_storage_update_manifest_cb,
                                            log_block_pool,
                                            plugins,
                                            NULL, /*set hot_cache to NULL for meta storage*/
                                            unused_meta_entry_header,
                                            last_meta_entry_start_lsn))) {
    PALF_LOG(ERROR, "LogMetaStorage load failed", K(ret), K(palf_id));
  } else if (OB_FAIL(construct_log_meta_(last_meta_entry_start_lsn, expected_next_block_id))) {
    PALF_LOG(ERROR, "construct_log_meta_ failed", K(ret));
  } else if (FALSE_IT(guard.click("load log_meta_storage"))
             || (0 != log_storage_block_size
                && OB_FAIL(log_storage_.load(base_dir, "log",
                                          log_meta_.get_log_snapshot_meta().base_lsn_, palf_id,
                                          log_storage_block_size, LOG_DIO_ALIGN_SIZE,
                                          LOG_DIO_ALIGNED_BUF_SIZE_REDO,
                                          log_storage_update_manifest_cb, log_block_pool, plugins,
                                          hot_cache, entry_header, last_group_entry_header_lsn)))) {
    PALF_LOG(ERROR, "LogStorage load failed", K(ret), K(palf_id), K(base_dir));
  } else if (FALSE_IT(guard.click("load log_storage"))
             || (0 != log_storage_block_size
                && OB_FAIL(try_clear_up_holes_and_check_storage_integrity_(
             last_group_entry_header_lsn, expected_next_block_id, entry_header)))) {
    PALF_LOG(ERROR, "the last block may be deleted by human, restart failed!!!", K(ret),
        K_(palf_id), K_(is_inited));
  } else if (OB_FAIL(integrity_verify_(last_meta_entry_start_lsn, last_group_entry_header_lsn, is_integrity))) {
    PALF_LOG(ERROR, "integrity_verify_ failed, unexpected error", K(ret), KPC(this));
  } else if (OB_FAIL(log_net_service_.init(palf_id, log_rpc))) {
    PALF_LOG(ERROR, "LogNetService init failed", K(ret), K(palf_id));
  } else {
    palf_id_ = palf_id;
    palf_epoch_ = palf_epoch;
    alloc_mgr_ = alloc_mgr;
    log_io_worker_ = log_io_worker;
    base_lsn_for_block_gc_ = log_meta_.get_log_snapshot_meta().base_lsn_;
    is_inited_ = true;
    PALF_LOG(INFO,
             "LogEngine load success",
             K_(palf_id), K_(is_inited),
             K(entry_header),
             K(log_storage_),
             K(log_meta_storage_),
             K(guard),
             K_(palf_epoch));
  }

  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int LogEngine::integrity_verify_(const LSN &last_meta_entry_start_lsn,
                                 const LSN &last_group_entry_header_lsn,
                                 bool &is_integrity)
{
  int ret = OB_SUCCESS;
  is_integrity = true;
  bool meta_is_valid = last_meta_entry_start_lsn.is_valid();
  bool redo_is_valid = last_group_entry_header_lsn.is_valid();
  // 1. meta is empty, but redo is not empty, unexpected error
  // 2. meta and redo are both empty, move this directory to another place, and remove this from PalfEnvImpl
  // 3. meta is not empty, but redo is empty, normal case
  // 4. meta and redo are not empty, normal case
  // In case2, we will load LogEngine success, and remove PalfHandleImpl in PalfEnvImpl.
  if (false == meta_is_valid && true == redo_is_valid) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "meta dir is empty, but redo is not empty, unexpected error", K(ret), KPC(this));
  } else if (false == meta_is_valid && false == redo_is_valid) {
    is_integrity = false;
    PALF_LOG(WARN, "log engine is not integrity", KPC(this));
  } else {
  }
  return ret;
}

int LogEngine::submit_flush_log_task(const FlushLogCbCtx &flush_log_cb_ctx,
                                     const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;
  LogIOFlushLogTask *flush_log_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret));
  } else if (false == flush_log_cb_ctx.is_valid() || false == write_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(flush_log_cb_ctx), K(write_buf));
  } else if (OB_FAIL(generate_flush_log_task_(flush_log_cb_ctx, write_buf, flush_log_task))) {
    PALF_LOG(ERROR, "generate_flush_log_task failed", K(ret), K(flush_log_cb_ctx));
  } else if (OB_FAIL(log_io_worker_->submit_io_task(flush_log_task))) {
    PALF_LOG(ERROR, "submit_io_task failed", K(ret));
  } else {
    PALF_LOG(TRACE, "submit_flush_log_task success", K(ret), K(flush_log_cb_ctx), K(write_buf));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(flush_log_task)) {
    alloc_mgr_->free_log_io_flush_log_task(flush_log_task);
    flush_log_task = NULL;
  }
  return ret;
}

int LogEngine::submit_flush_prepare_meta_task(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                              const LogPrepareMeta &prepare_meta)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(log_meta_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret));
  } else if (false == flush_meta_cb_ctx.is_valid() || false == prepare_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(flush_meta_cb_ctx), K(prepare_meta));
  } else if (OB_FAIL(log_meta_.update_log_prepare_meta(prepare_meta))) {
    PALF_LOG(ERROR, "LogMeta update_log_prepare_meta failed", K(ret), K_(palf_id), K_(is_inited));
  } else if (OB_FAIL(submit_flush_meta_task_(flush_meta_cb_ctx, log_meta_))) {
    PALF_LOG(ERROR, "submit_flush_meta_task_ failed", K(ret));
  } else {
    PALF_LOG(INFO, "submit_flush_prepare_meta_task success", K(ret), K(flush_meta_cb_ctx), K(prepare_meta));
  }
  return ret;
}

int LogEngine::submit_flush_change_config_meta_task(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                                    const LogConfigMeta &config_meta)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(log_meta_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEnginenot inited!!!", K(ret));
  } else if (false == flush_meta_cb_ctx.is_valid() || false == config_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(flush_meta_cb_ctx), K(config_meta));
  } else if (OB_FAIL(log_meta_.update_log_config_meta(config_meta))) {
    PALF_LOG(ERROR, "LogMeta update_log_config_meta failed", K(ret), K_(palf_id), K_(is_inited));
  } else if (OB_FAIL(submit_flush_meta_task_(flush_meta_cb_ctx, log_meta_))) {
    PALF_LOG(ERROR, "submit_flush_meta_task_ failed", K(ret));
  } else {
    PALF_LOG(INFO, "submit_flush_change_config_meta_task success", K(ret), K(flush_meta_cb_ctx), K(config_meta));
  }
  return ret;
}

int LogEngine::submit_flush_mode_meta_task(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                           const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(log_meta_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEnginenot inited!!!", K(ret));
  } else if (false == flush_meta_cb_ctx.is_valid() || false == mode_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(flush_meta_cb_ctx), K(mode_meta));
  } else if (OB_FAIL(log_meta_.update_log_mode_meta(mode_meta))) {
    PALF_LOG(ERROR, "LogMeta update_log_mode_meta failed", K(ret), K_(palf_id), K_(is_inited));
  } else if (OB_FAIL(submit_flush_meta_task_(flush_meta_cb_ctx, log_meta_))) {
    PALF_LOG(ERROR, "submit_flush_meta_task_ failed", K(ret));
  } else {
    PALF_LOG(INFO, "submit_flush_mode_meta_task success", K(ret), K(flush_meta_cb_ctx), K(mode_meta));
  }
  return ret;
}

int LogEngine::submit_flush_snapshot_meta_task(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                               const LogSnapshotMeta &log_snapshot_meta)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(log_meta_lock_);
  const LSN &curr_base_lsn = log_meta_.get_log_snapshot_meta().base_lsn_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret));
  } else if (false == flush_meta_cb_ctx.is_valid() || false == log_snapshot_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(flush_meta_cb_ctx), K(log_snapshot_meta));
  } else if (log_snapshot_meta.base_lsn_ != curr_base_lsn &&
      OB_FAIL(log_meta_.update_log_snapshot_meta(log_snapshot_meta))) {
    PALF_LOG(WARN, "update_log_snapshot_meta failed", K(log_snapshot_meta));
  } else if (OB_FAIL(submit_flush_meta_task_(flush_meta_cb_ctx, log_meta_))) {
    PALF_LOG(
        WARN, "submit_flush_snapshot_meta_task_ failed", K(ret), K(flush_meta_cb_ctx), K_(palf_id), K_(is_inited));
  } else {
    PALF_LOG(TRACE, "submit_flush_snapshot_meta_task success", K(ret), K(flush_meta_cb_ctx));
  }
  return ret;
}

int LogEngine::submit_flush_replica_property_meta_task(
    const FlushMetaCbCtx &flush_meta_cb_ctx,
    const LogReplicaPropertyMeta &log_replica_property_meta)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(log_meta_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret));
  } else if (false == flush_meta_cb_ctx.is_valid()
             || false == log_replica_property_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(flush_meta_cb_ctx),
        K(log_replica_property_meta));
  } else if (OB_FAIL(log_meta_.update_log_replica_property_meta(log_replica_property_meta))) {
    PALF_LOG(WARN, "update_log_replica_property_meta failed", K(log_replica_property_meta));
  } else if (OB_FAIL(submit_flush_meta_task_(flush_meta_cb_ctx, log_meta_))) {
    PALF_LOG(WARN, "submit_flush_replica_property_meta_task_ failed", K(ret), K(flush_meta_cb_ctx), K_(palf_id), K_(is_inited));
  } else {
    PALF_LOG(TRACE, "submit_flush_replica_property_meta_task_ success", K(ret), K(flush_meta_cb_ctx));
  }
  return ret;
}

int LogEngine::submit_truncate_log_task(const TruncateLogCbCtx &truncate_log_cb_ctx)
{
  int ret = OB_SUCCESS;
  LogIOTruncateLogTask *truncate_log_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret));
  } else if (false == truncate_log_cb_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(truncate_log_cb_ctx));
  } else if (OB_FAIL(generate_truncate_log_task_(truncate_log_cb_ctx, truncate_log_task))) {
    PALF_LOG(ERROR, "generate_truncate_log_task_ failed", K(ret), K(truncate_log_cb_ctx));
  } else if (OB_FAIL(log_io_worker_->submit_io_task(truncate_log_task))) {
    PALF_LOG(ERROR, "submit_io_task failed", K(ret));
  } else {
    PALF_LOG(INFO, "submit_truncate_log_task success", K(ret), K(truncate_log_cb_ctx));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(truncate_log_task)) {
    alloc_mgr_->free_log_io_truncate_log_task(truncate_log_task);
    truncate_log_task = NULL;
  }
  return ret;
}

int LogEngine::submit_truncate_prefix_blocks_task(
    const TruncatePrefixBlocksCbCtx &truncate_prefix_blocks_ctx)
{
  int ret = OB_SUCCESS;
  LogIOTruncatePrefixBlocksTask *truncate_prefix_blocks_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret));
  } else if (false == truncate_prefix_blocks_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(truncate_prefix_blocks_ctx));
  } else if (OB_FAIL(generate_truncate_prefix_blocks_task_(truncate_prefix_blocks_ctx,
                                                           truncate_prefix_blocks_task))) {
    PALF_LOG(ERROR, "generate_truncate_log_task_ failed", K(ret), K(truncate_prefix_blocks_ctx));
  } else if (OB_FAIL(log_io_worker_->submit_io_task(truncate_prefix_blocks_task))) {
    PALF_LOG(ERROR, "submit_io_task failed", K(ret));
  } else {
    PALF_LOG(
        INFO, "submit_truncate_prefix_blocks_task success", K(ret), K(truncate_prefix_blocks_ctx));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(truncate_prefix_blocks_task)) {
    alloc_mgr_->free_log_io_truncate_prefix_blocks_task(truncate_prefix_blocks_task);
    truncate_prefix_blocks_task = NULL;
  }
  return ret;
}

int LogEngine::submit_flashback_task(const FlashbackCbCtx &flashback_cb_ctx)
{
  int ret = OB_SUCCESS;
  LogIOFlashbackTask *flashback_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret));
  } else if (false == flashback_cb_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(flashback_cb_ctx));
  } else if (OB_FAIL(generate_flashback_task_(flashback_cb_ctx, flashback_task))) {
    PALF_LOG(ERROR, "generate_flashback_task_ failed", K(ret), K(flashback_cb_ctx));
  } else if (OB_FAIL(log_io_worker_->submit_io_task(flashback_task))) {
    PALF_LOG(ERROR, "submit_io_task failed", K(ret));
  } else {
    PALF_LOG(INFO, "submit_flashback_task success", K(ret), K(flashback_cb_ctx));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(flashback_task)) {
    alloc_mgr_->free_log_io_flashback_task(flashback_task);
    flashback_task = NULL;
  }
  return ret;
}
int LogEngine::submit_purge_throttling_task(const PurgeThrottlingType purge_type)
{
  int ret = OB_SUCCESS;
  PurgeThrottlingCbCtx purge_cb_ctx(purge_type);
  LogIOPurgeThrottlingTask *purge_task = NULL;
  const int64_t cur_ts = ObClockGenerator::getClock();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret));
  } else if (OB_UNLIKELY(!purge_cb_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(purge_cb_ctx));
  } else if ((!need_force_purge(purge_type)) &&
             (cur_ts - last_purge_throttling_ts_ <= PURGE_THROTTLING_INTERVAL)) {
    PALF_LOG(INFO, "no need to purge throttling according to PURGE_THROTTLING_INTERVAL",
    K(purge_cb_ctx), K(last_purge_throttling_ts_), K(cur_ts), K(PURGE_THROTTLING_INTERVAL));
  } else if (OB_FAIL(generate_purge_throttling_task_(purge_cb_ctx, purge_task))) {
    PALF_LOG(ERROR, "generate_purge_throttling_ failed", K(purge_cb_ctx));
  } else if (OB_FAIL(log_io_worker_->submit_io_task(purge_task))) {
    PALF_LOG(ERROR, "submit_io_task failed", K(purge_cb_ctx));
  } else {
    last_purge_throttling_ts_ = cur_ts;
    PALF_LOG(INFO, "submit_purge_throttling success", K(last_purge_throttling_ts_), "purge_type",
             get_purge_throttling_type_str(purge_type));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(purge_task)) {
    alloc_mgr_->free_log_io_purge_throttling_task(purge_task);
    purge_task = NULL;
  }
  return ret;
}

// ====================== LogStorage start =====================
int LogEngine::append_log(const LSN &lsn, const LogWriteBuf &write_buf, const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret), K_(palf_id), K_(is_inited));
  } else if (false == lsn.is_valid() || false == write_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K_(palf_id), K_(is_inited), K(lsn), K(write_buf));
  } else if (OB_FAIL(log_storage_.writev(lsn, write_buf, scn))) {
    PALF_LOG(ERROR, "LogStorage append_log failed", K(ret), K_(palf_id), K_(is_inited));
  } else {
    PALF_LOG(
        TRACE, "LogEngine append_log success", K(ret), K_(palf_id), K_(is_inited), K(lsn), K(write_buf), K(scn));
  }
  return ret;
}

int LogEngine::append_log(const LSNArray &lsn_array, const LogWriteBufArray &write_buf_array,
                          const SCNArray &scn_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret), K_(palf_id), K_(is_inited));
  } else if (OB_FAIL(log_storage_.writev(lsn_array, write_buf_array, scn_array))) {
    PALF_LOG(ERROR, "LogStorage writev failed", K(ret), K_(palf_id), K_(is_inited));
  } else {
  }
  return ret;
}

int LogEngine::read_log(const LSN &lsn,
                        const int64_t in_read_size,
                        ReadBuf &read_buf,
                        int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret), K_(palf_id), K_(is_inited));
  } else if (false == lsn.is_valid() || 0 >= in_read_size || false == read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K_(palf_id), K_(is_inited), K(lsn), K(in_read_size), K(read_buf));
  } else if (OB_FAIL(log_storage_.pread(lsn, in_read_size, read_buf, out_read_size))) {
    PALF_LOG(ERROR, "LogEngine read_log failed", K(ret), K(lsn), K(in_read_size), K(read_buf));
  } else {
    PALF_LOG(TRACE, "LogEngine read_log success", K(ret), K(lsn), K(read_buf), K(out_read_size));
  }
  return ret;
}

int LogEngine::read_group_entry_header(const LSN &lsn, LogGroupEntryHeader &log_group_entry_header)
{
  int ret = OB_SUCCESS;
  // 4K is enough to read log_group_entry_header;
  const int64_t in_read_size = MAX_LOG_HEADER_SIZE;
  ReadBufGuard read_buf_guard("LogEngine", in_read_size);
  ReadBuf &read_buf = read_buf_guard.read_buf_;
  int64_t out_read_size = 0;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!read_buf.is_valid()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "allocate memory failed", KPC(this), K(lsn));
  } else if (OB_FAIL(log_storage_.pread_without_block_header(lsn, in_read_size, read_buf, out_read_size))) {
    PALF_LOG(WARN, "LogStorage pread failed", K(ret));
  } else if (OB_FAIL(log_group_entry_header.deserialize(read_buf.buf_, in_read_size, pos))) {
    PALF_LOG(WARN,
             "deserialize log_group_entry_header failed",
             K(ret),
             K(read_buf),
             K(in_read_size),
             K(pos),
             K(out_read_size));
  } else if (false == log_group_entry_header.check_header_integrity()) {
    ret = OB_INVALID_DATA;
    PALF_LOG(ERROR, "the data has been corrupted!!!", K(ret), K(lsn), K(log_group_entry_header));
  } else {
    PALF_LOG(TRACE, "read_group_entry_header success", K(ret), K(lsn), K(log_group_entry_header));
  }
  return ret;
}

int LogEngine::truncate(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  LogGroupEntryHeader log_group_entry_header;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret), K_(palf_id), K_(is_inited));
  } else if (false == lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K_(palf_id), K_(is_inited), K(lsn));
  } else if (OB_FAIL(read_group_entry_header(lsn, log_group_entry_header))
      && OB_ERR_OUT_OF_UPPER_BOUND != ret) {
    PALF_LOG(ERROR, "read_group_entry_header failed, unexpected error, lsn must be the start position"
        "of one LogGroupEntry", K(ret), K(lsn), K_(palf_id), K_(is_inited));
  } else if (OB_FAIL(log_storage_.truncate(lsn))) {
    PALF_LOG(ERROR, "LogStorage truncate failed", K(ret), K(lsn));
  } else {
    PALF_LOG(INFO, "truncate success", K(lsn));
  }
  return ret;
}

int LogEngine::truncate_prefix_blocks(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret), K_(palf_id), K_(is_inited));
  } else if (false == lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K_(palf_id), K_(is_inited), K(lsn));
  } else if (OB_FAIL(log_storage_.truncate_prefix_blocks(lsn))) {
    PALF_LOG(WARN, "truncate_prefix_blocks failed", K(ret), K_(palf_id), K_(is_inited), K(lsn));
  } else {
    PALF_LOG(INFO, "truncate_prefix_blocks success", K(ret), K_(palf_id), K_(is_inited), K(lsn)); }
  return ret;
}

int LogEngine::begin_flashback(const LSN &start_lsn_of_block)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_storage_.begin_flashback(start_lsn_of_block))) {
    PALF_LOG(ERROR, "LogStorage begin_flashback failed", K(ret), KPC(this), K(start_lsn_of_block));
  } else {
    PALF_LOG(INFO, "LogEngine begin_flashback success", KPC(this), K(start_lsn_of_block));
  }
  return ret;
}

int LogEngine::end_flashback(const LSN &start_lsn_of_block)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_storage_.end_flashback(start_lsn_of_block))) {
    PALF_LOG(ERROR, "LogStorege end_flashback failed", K(ret), KPC(this), K(start_lsn_of_block));
  } else {
    PALF_LOG(INFO, "LogEngine end_flashback success", KPC(this), K(start_lsn_of_block));
  }
  return ret;
}

// NB: delete_block only called by GC
//
// Nowdays, may be concurrently executed with 'truncate_prefix_blocks', need handle follow cases:
// 1. delete block may be failed;
// 2. get_block_min_scn may be failed, and then, we need reset 'min_block_scn' which used by GC.
int LogEngine::delete_block(const block_id_t &block_id)
{
  int ret = OB_SUCCESS;
  block_id_t next_block_id = block_id + 1;
  SCN next_block_max_scn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret), K_(palf_id), K_(is_inited));
  } else if (false == is_valid_block_id(block_id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K_(palf_id), K_(is_inited), K(block_id));
  } else if (OB_FAIL(log_storage_.delete_block(block_id)) && OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
    PALF_LOG(ERROR, "LogStorage delete block failed, unexpected error", K(ret), K(block_id));
  } else if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
    PALF_LOG(INFO, "file not exist, may be concurrently delete by others module", K(ret), K(block_id));
    ret = OB_SUCCESS;
  } else {
    PALF_LOG(INFO, "delete success", K(block_id), K_(palf_id), K_(is_inited));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCC(ret) && OB_SUCCESS != (tmp_ret = get_block_min_scn(next_block_id + 1, next_block_max_scn))) {
    PALF_LOG(WARN, "get the max ts of next block failed", K(tmp_ret), K(next_block_id));
  }
  // If 'delete_block' or 'get_block_min_scn' failed, need reset 'min_block_scn_' to be invalid.
  reset_min_block_info_guarded_by_lock_(next_block_id, next_block_max_scn);
  return ret;
}

int LogEngine::get_block_min_scn(const block_id_t &block_id, SCN &min_scn) const
{
  return log_storage_.get_block_min_scn(block_id, min_scn);
}

const LSN LogEngine::get_begin_lsn() const { return log_storage_.get_begin_lsn(); }

int LogEngine::get_block_id_range(block_id_t &min_block_id, block_id_t &max_block_id) const
{
  return log_storage_.get_block_id_range(min_block_id, max_block_id);
}

int LogEngine::get_min_block_info_for_gc(block_id_t &block_id, SCN &max_scn)
{
  int ret = OB_SUCCESS;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  SCN min_block_max_scn;

  do {
    ObSpinLockGuard guard(block_gc_lock_);
    min_block_id = min_block_id_;
    min_block_max_scn = min_block_max_scn_;
  } while (0);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogEngine is not inited", K(ret), KPC(this));
  } else if (min_block_max_scn.is_valid()) {
    block_id = min_block_id;
    max_scn = min_block_max_scn;
  } else if (OB_FAIL(get_block_id_range(min_block_id, max_block_id))) {
    PALF_LOG(WARN, "get_block_id_range failed", K(ret));
    // NB: used next block min_block_ts as the max_scn of current block
  } else if (OB_FAIL(get_block_min_scn(min_block_id+1, min_block_max_scn))) {
    PALF_LOG(TRACE, "get_block_min_scn failed", K(ret));
  } else {
    reset_min_block_info_guarded_by_lock_(min_block_id, min_block_max_scn);
    block_id = min_block_id;
    max_scn = min_block_max_scn;
  }
  return ret;
}

int LogEngine::get_min_block_info(block_id_t &min_block_id, SCN &min_block_scn) const
{
  int ret = OB_SUCCESS;
  block_id_t max_block_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogEngine is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(get_block_id_range(min_block_id, max_block_id))) {
    PALF_LOG(WARN, "get_block_id_range failed", K(ret), KPC(this));
  } else if (OB_FAIL(get_block_min_scn(min_block_id, min_block_scn))) {
    PALF_LOG(WARN, "get_block_min_scn failed", K(ret), KPC(this));
  } else {
  }
  return ret;
}

int LogEngine::get_total_used_disk_space(int64_t &total_used_size_byte,
                                         int64_t &unrecyclable_disk_space) const
{
  int ret = OB_SUCCESS;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  int64_t log_storage_used = 0;
  int64_t meta_storage_used = 0;
  int64_t log_storage_logical_block_size = 0;
  int64_t meta_storage_logical_block_size = 0;
  // calc log storage used
  if (OB_FAIL(get_block_id_range(min_block_id, max_block_id))
      && OB_ENTRY_NOT_EXIST != ret) {
    PALF_LOG(WARN, "get_block_id_range failed", K(ret), KPC(this));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    log_storage_used = 0;
    ret = OB_SUCCESS;
  } else if (OB_FAIL(log_storage_.get_logical_block_size(log_storage_logical_block_size))) {
    PALF_LOG(WARN, "LogStorage get_logical_block_size failed", KPC(this));
  } else if (OB_FAIL(log_meta_storage_.get_logical_block_size(meta_storage_logical_block_size))) {
    PALF_LOG(WARN, "MetaStorage get_logical_block_size failed", KPC(this));
  } else {
    //usage calculation should be precise to avoid stopping writing when actually no need
    log_storage_used = (max_block_id - min_block_id) * (log_storage_logical_block_size + MAX_INFO_BLOCK_SIZE)
      + lsn_2_offset(log_storage_.get_end_lsn(), log_storage_logical_block_size) + MAX_INFO_BLOCK_SIZE;
    PALF_LOG(TRACE, "log_storage_used size", K(min_block_id), K(max_block_id), K(log_storage_used));
  }
  // calc meta storage used
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(log_meta_storage_.get_block_id_range(min_block_id, max_block_id))) {
    PALF_LOG(WARN, "get_block_id_range failed", K(ret), KPC(this));
  } else {
    meta_storage_used = meta_storage_logical_block_size + MAX_INFO_BLOCK_SIZE;
    total_used_size_byte = log_storage_used + meta_storage_used;

    const int64_t unrecyclable_meta_size = meta_storage_used;
    unrecyclable_disk_space = log_storage_.get_end_lsn() - get_base_lsn_used_for_block_gc() + unrecyclable_meta_size;
    PALF_LOG(TRACE, "get_total_used_disk_space", K(meta_storage_used), K(log_storage_used), K(total_used_size_byte));
  }
  return ret;
}

// ====================== LogStorage end =======================

// ===================== MetaStorage start =====================
// NB: need update snapshot_meta inc.
int LogEngine::update_base_lsn_used_for_gc(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(log_meta_lock_);
  if (true == base_lsn_for_block_gc_.is_valid() && lsn < base_lsn_for_block_gc_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN,
             "lsn is smaller than base_lsn_for_block_gc_",
             K(ret),
             K(lsn),
             K(base_lsn_for_block_gc_));
  } else {
    base_lsn_for_block_gc_ = lsn;
    PALF_LOG(INFO, "update_base_lsn_used_for_gc success", K(ret), K(lsn), K_(palf_id), K_(is_inited));
  }
  return ret;
}

int LogEngine::update_manifest(const block_id_t block_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_block_id(block_id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument!!!", KPC(this), K(block_id));
  } else if (OB_FAIL(log_meta_storage_.update_manifest_used_for_meta_storage(block_id))) {
    PALF_LOG(WARN, "update_manifest_used_for_meta_storage failed", K(ret), K_(palf_id), K_(is_inited));
  } else {
    PALF_LOG(INFO,
             "update_max_block_id_for_switch_block_cb success",
             K(ret),
             K_(palf_id), K_(is_inited),
             K(block_id));
  }
  return ret;
}

int LogEngine::append_meta(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not inited!!!", K(ret), K_(palf_id), K_(is_inited));
  } else if (OB_UNLIKELY(NULL == buf || 0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argumnt!!!", K(ret), K_(palf_id), K_(is_inited), K(buf), K(buf_len));
  } else if (OB_FAIL(log_meta_storage_.append_meta(buf, buf_len))) {
    PALF_LOG(ERROR, "LogMetaStorage pwrite failed", K(ret), K(buf), K(buf_len));
  } else {
    PALF_LOG(TRACE, "LogEngine append_meta success", K(ret), K(buf), K(buf_len));
  }
  return ret;
}

int LogEngine::update_log_snapshot_meta_for_flashback(const LogInfo &log_info)
{
  int ret = OB_SUCCESS;
  LogSnapshotMeta snapshot_meta = log_meta_.get_log_snapshot_meta();
  snapshot_meta.prev_log_info_ = log_info;
  if (OB_FAIL(log_meta_.update_log_snapshot_meta(snapshot_meta))) {
    PALF_LOG(WARN, "update_log_snapshot_meta failed", K(ret), KPC(this), K(log_info), K(snapshot_meta));
  } else if (OB_FAIL(append_log_meta_(log_meta_))) {
    PALF_LOG(WARN, "append_log_meta failed", K(ret), KPC(this), K(log_info), K(snapshot_meta));
  } else {
    PALF_LOG(INFO, "update_log_snapshot_meta_for_flashback success", K(ret), KPC(this), K(log_info),
        K(snapshot_meta));
  }
  return ret;
}

// ===================== MetaStorage end =======================

// ===================== NetService start ======================
int LogEngine::submit_push_log_req(const common::ObAddr &server,
                                   const PushLogType &push_log_type,
                                   const int64_t &msg_proposal_id,
                                   const int64_t &prev_log_proposal_id,
                                   const LSN &prev_lsn,
                                   const LSN &curr_lsn,
                                   const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not init", K(ret), K_(palf_id), K_(is_inited));
  } else if (OB_FAIL(log_net_service_.submit_push_log_req(server,
                                                          push_log_type,
                                                          msg_proposal_id,
                                                          prev_log_proposal_id,
                                                          prev_lsn,
                                                          curr_lsn,
                                                          write_buf))) {
    PALF_LOG(ERROR,
             "LogNetService submit_group_entry_to_server failed",
             K(ret),
             K_(palf_id), K_(is_inited),
             K(server),
             K(prev_log_proposal_id),
             K(prev_lsn),
             K(curr_lsn),
             K(write_buf));
  } else {
    PALF_LOG(TRACE,
             "submit_group_entry_to_server success",
             K(ret),
             K_(palf_id), K_(is_inited),
             K(server),
             K(prev_log_proposal_id),
             K(prev_lsn),
             K(curr_lsn));
  }
  return ret;
}

int LogEngine::submit_push_log_resp(const ObAddr &server,
                                    const int64_t &msg_proposal_id,
                                    const LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_push_log_resp(server, msg_proposal_id, lsn);
    PALF_LOG(TRACE, "submit_push_log_resp success", K(ret), K(server));
  }
  return ret;
}

int LogEngine::submit_prepare_meta_resp(const common::ObAddr &server,
                                        const int64_t &msg_proposal_id,
                                        const bool vote_granted,
                                        const int64_t &log_proposal_id,
                                        const LSN &max_flushed_lsn,
                                        const LSN &committed_end_lsn,
                                        const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_prepare_meta_resp(
        server, msg_proposal_id, vote_granted, log_proposal_id, max_flushed_lsn, committed_end_lsn, mode_meta);
  }
  return ret;
}

int LogEngine::submit_change_config_meta_resp(const common::ObAddr &server,
                                              const int64_t msg_proposal_id,
                                              const LogConfigVersion &config_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_change_config_meta_resp(server, msg_proposal_id, config_version);
  }
  return ret;
}

int LogEngine::submit_change_mode_meta_resp(const common::ObAddr &server,
                                 const int64_t &msg_proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_change_mode_meta_resp(server, msg_proposal_id);
  }
  return ret;
}

int LogEngine::submit_config_change_pre_check_req(const common::ObAddr &server,
                                                  const LogConfigVersion &config_version,
                                                  const bool need_purge_throttling,
                                                  const int64_t timeout_us,
                                                  LogGetMCStResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_config_change_pre_check_req(
        server, config_version, need_purge_throttling, timeout_us, resp);
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int LogEngine::sync_get_arb_member_info(const common::ObAddr &server,
                                        const int64_t timeout_us,
                                        LogGetArbMemberInfoResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_get_arb_member_info_req(
        server, timeout_us, resp);
  }
  return ret;
}
#endif

int LogEngine::submit_fetch_log_req(const ObAddr &server,
                                    const FetchLogType fetch_type,
                                    const int64_t msg_proposal_id,
                                    const LSN &prev_lsn,
                                    const LSN &lsn,
                                    const int64_t fetch_log_size,
                                    const int64_t fetch_log_count,
                                    const int64_t accepted_mode_pid)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_fetch_log_req(
        server, fetch_type, msg_proposal_id, prev_lsn, lsn,
        fetch_log_size, fetch_log_count, accepted_mode_pid);
  }
  return ret;
}

int LogEngine::submit_batch_fetch_log_resp(const common::ObAddr &server,
                                           const int64_t msg_proposal_id,
                                           const int64_t prev_log_proposal_id,
                                           const LSN &prev_lsn,
                                           const LSN &curr_lsn,
                                           const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_batch_fetch_log_resp(
        server, msg_proposal_id, prev_log_proposal_id,
        prev_lsn, curr_lsn, write_buf);
  }
  return ret;
}

int LogEngine::submit_notify_rebuild_req(const ObAddr &server,
                                         const LSN &base_lsn,
                                         const LogInfo &base_prev_log_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_notify_rebuild_req(server, base_lsn, base_prev_log_info);
  }
  return ret;
}

int LogEngine::submit_notify_fetch_log_req(const ObMemberList &dst_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_notify_fetch_log_req(dst_list);
  }
  return ret;
}

int LogEngine::submit_register_parent_req(const common::ObAddr &server,
                                          const LogLearner &child_itself,
                                          const bool is_to_leader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_register_parent_req(server, child_itself, is_to_leader);
  }
  return ret;
}

int LogEngine::submit_register_parent_resp(const common::ObAddr &server,
                                           const LogLearner &parent_itself,
                                           const LogCandidateList &candidate_list,
                                           const RegisterReturn reg_ret)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_register_parent_resp(
        server, parent_itself, candidate_list, reg_ret);
  }
  return ret;
}

int LogEngine::submit_retire_parent_req(const common::ObAddr &server,
                                        const LogLearner &child_itself)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_retire_parent_req(server, child_itself);
  }
  return ret;
}

int LogEngine::submit_retire_child_req(const common::ObAddr &server,
                                       const LogLearner &parent_itself)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_retire_child_req(server, parent_itself);
  }
  return ret;
}

int LogEngine::submit_learner_keepalive_req(const common::ObAddr &server,
                                            const LogLearner &sender_itself)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_learner_keepalive_req(server, sender_itself);
  }
  return ret;
}

int LogEngine::submit_learner_keepalive_resp(const common::ObAddr &server,
                                             const LogLearner &sender_itself)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_learner_keepalive_resp(server, sender_itself);
  }
  return ret;
}

int LogEngine::submit_committed_info_req(
      const common::ObAddr &server,
      const int64_t &msg_proposal_id,
      const int64_t prev_log_id,
      const int64_t &prev_log_proposal_id,
      const LSN &committed_end_lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogEngine not init", K(ret), KPC(this));
  } else if (OB_FAIL(log_net_service_.submit_committed_info_req(
        server, msg_proposal_id,
        prev_log_id, prev_log_proposal_id, committed_end_lsn))) {
    PALF_LOG(ERROR, "LogNetService submit_committed_info_req failed", K(ret),
        KPC(this), K(server),
        K(prev_log_id), K(prev_log_proposal_id), K(committed_end_lsn));
  } else {
    PALF_LOG(TRACE, "submit_committed_info_req success", K(ret), KPC(this),
        K(server), K(msg_proposal_id), K(prev_log_id),
        K(prev_log_proposal_id), K(committed_end_lsn));
  }
  return ret;
}

int LogEngine::submit_get_stat_req(const common::ObAddr &server,
                                   const int64_t timeout_us,
                                   const LogGetStatReq &req,
                                   LogGetStatResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_net_service_.submit_get_stat_req(server, timeout_us, req, resp);
  }
  return ret;
}

LogMeta LogEngine::get_log_meta() const
{
  ObSpinLockGuard guard(log_meta_lock_);
  return log_meta_;
}

const LSN &LogEngine::get_base_lsn_used_for_block_gc() const
{
  ObSpinLockGuard guard(log_meta_lock_);
  return base_lsn_for_block_gc_;
}

int LogEngine::submit_flush_meta_task_(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                       const LogMeta &log_meta)
{
  int ret = OB_SUCCESS;
  LogIOFlushMetaTask *flush_meta_task = NULL;
  if (OB_FAIL(generate_flush_meta_task_(flush_meta_cb_ctx, log_meta, flush_meta_task))) {
    PALF_LOG(ERROR, "generate_flush_meta_task_ failed", K(ret), K(flush_meta_cb_ctx), K(log_meta_));
  } else if (OB_FAIL(log_io_worker_->submit_io_task(flush_meta_task))) {
    PALF_LOG(ERROR, "submit_io_task failed", K(ret));
  } else {
    PALF_LOG(INFO, "submit_flush_meta_task_ success", K(flush_meta_cb_ctx), K(log_meta));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(flush_meta_task)) {
    alloc_mgr_->free_log_io_flush_meta_task(flush_meta_task);
    flush_meta_task = NULL;
  }
  return ret;
}

int LogEngine::construct_log_meta_(const LSN &lsn, block_id_t &expected_next_block_id)
{
  int ret = OB_SUCCESS;
  int64_t out_read_size = 0;
  int64_t pos = 0;
  const int64_t buf_len = MAX_META_ENTRY_SIZE;
  ReadBufGuard guard("LogEngine", buf_len);
  ReadBuf &read_buf = guard.read_buf_;
  LogMetaEntry meta_entry;
  if (false == lsn.is_valid()) {
    PALF_LOG(INFO, "there is no meta entry, maybe create palf failed", K(ret), K_(palf_id), K_(is_inited));
  } else if (!read_buf.is_valid()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "allocate memory failed", KPC(this), K(lsn));
  } else if (OB_FAIL(log_meta_storage_.pread(lsn, buf_len, read_buf, out_read_size))) {
    PALF_LOG(WARN, "ObLogMetaStorage pread failed", K(ret), K_(palf_id), K_(is_inited));
    // NB: when lsn is invalid, means there is no data on disk.
  } else if (OB_FAIL(meta_entry.deserialize(read_buf.buf_, buf_len, pos))) {
    PALF_LOG(WARN, "LogMetaEntry deserialize failed", K(ret), K(pos));
  } else if (false == meta_entry.check_integrity()) {
    ret = OB_INVALID_DATA;
    PALF_LOG(ERROR,
             "the data of LogMeta has been corrupted, unexpected error!!!",
             K(ret),
             K(meta_entry),
             K_(palf_id), K_(is_inited));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(log_meta_.deserialize(meta_entry.get_buf(), meta_entry.get_data_len(), pos))) {
    PALF_LOG(WARN, "LogMeta deserialize failed");
  } else if (OB_FAIL(log_meta_storage_.load_manifest_for_meta_storage(expected_next_block_id))) {
    PALF_LOG(ERROR, "load_manifest_for_meta_storage failed", K(ret), KPC(this));
  } else {
    PALF_LOG(INFO, "construct_log_meta_ success", K(ret), K(log_meta_), K(meta_entry));
  }
  return ret;
}

int LogEngine::generate_flush_log_task_(const FlushLogCbCtx &flush_log_cb_ctx,
                                        const LogWriteBuf &write_buf,
                                        LogIOFlushLogTask *&flush_log_task)
{
  int ret = OB_SUCCESS;
  // Be careful to handle the duration of this pointer
  flush_log_task = NULL;
  if (false == flush_log_cb_ctx.is_valid() || false == write_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (flush_log_task = alloc_mgr_->alloc_log_io_flush_log_task(palf_id_, palf_epoch_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "alloc_log_io_flush_log_task failed", K(ret));
  } else if (OB_FAIL(flush_log_task->init(flush_log_cb_ctx, write_buf))) {
    PALF_LOG(ERROR, "init LogIOFlushLogTask failed", K(ret));
  } else {/*do nothing*/}
  if (OB_FAIL(ret) && NULL != flush_log_task) {
    alloc_mgr_->free_log_io_flush_log_task(flush_log_task);
    flush_log_task = NULL;
  }
  return ret;
}

int LogEngine::generate_truncate_log_task_(const TruncateLogCbCtx &truncate_log_cb_ctx,
                                           LogIOTruncateLogTask *&truncate_log_task)
{
  int ret = OB_SUCCESS;
  truncate_log_task = NULL;
  if (NULL == (truncate_log_task = alloc_mgr_->alloc_log_io_truncate_log_task(palf_id_, palf_epoch_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "alloc_log_io_truncate_log_task failed", K(ret));
  } else if (OB_FAIL(truncate_log_task->init(truncate_log_cb_ctx))) {
    PALF_LOG(ERROR, "init LogIOTruncateLogTask failed", K(ret), K_(palf_id), K_(is_inited));
  } else {
    PALF_LOG(TRACE, "generate_truncate_log_task_ success", K(ret), K_(palf_id), K_(is_inited));
  }
  if (OB_FAIL(ret) && NULL != truncate_log_task) {
    alloc_mgr_->free_log_io_truncate_log_task(truncate_log_task);
    truncate_log_task = NULL;
  }
  return ret;
}

int LogEngine::generate_truncate_prefix_blocks_task_(
    const TruncatePrefixBlocksCbCtx &truncate_prefix_blocks_ctx,
    LogIOTruncatePrefixBlocksTask *&truncate_prefix_blocks_task)
{
  int ret = OB_SUCCESS;
  if (NULL
      == (truncate_prefix_blocks_task = alloc_mgr_->alloc_log_io_truncate_prefix_blocks_task(palf_id_, palf_epoch_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "alloc_log_io_truncate_prefix_blocks_task failed", K(ret));
  } else if (OB_FAIL(truncate_prefix_blocks_task->init(truncate_prefix_blocks_ctx))) {
    PALF_LOG(ERROR, "init LogIOTruncatePrefixBlocksTask failed", K(ret), K_(palf_id), K_(is_inited));
  } else {/*do nothing*/}
  if (OB_FAIL(ret) && NULL != truncate_prefix_blocks_task) {
    alloc_mgr_->free_log_io_truncate_prefix_blocks_task(truncate_prefix_blocks_task);
    truncate_prefix_blocks_task = NULL;
  }
  return ret;
}

int LogEngine::generate_flush_meta_task_(const FlushMetaCbCtx &flush_meta_cb_ctx,
                                         const LogMeta &log_meta,
                                         LogIOFlushMetaTask *&flush_meta_task)
{

  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_len = MAX_META_ENTRY_SIZE;
  flush_meta_task = NULL;
  if (false == flush_meta_cb_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (flush_meta_task = alloc_mgr_->alloc_log_io_flush_meta_task(palf_id_, palf_epoch_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "alloc_log_io_flush_meta_task failed", K(ret));
  } else if (NULL == (buf = reinterpret_cast<char *>(mtl_malloc(buf_len,
            "LogEngine")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "allocate memory failed", K(ret), K_(palf_id), K_(is_inited));
  } else if (OB_FAIL(serialize_log_meta_(log_meta, buf, buf_len))) {
    PALF_LOG(ERROR, "serialize_log_meta_ failed", K(ret), K_(palf_id), K_(is_inited), K(log_meta));
  } else if (OB_FAIL(flush_meta_task->init(flush_meta_cb_ctx, buf, buf_len))) {
    PALF_LOG(ERROR, "init LogIOFlushMetaTask failed", K(ret));
  } else {
    PALF_LOG(TRACE, "generate_flush_meta_task_ success", K(ret), K_(palf_id), K_(is_inited));
  }

  if (OB_FAIL(ret)) {
    //free memory
    if (NULL != buf) {
      mtl_free(buf);
    }
    if (NULL != flush_meta_task) {
      alloc_mgr_->free_log_io_flush_meta_task(flush_meta_task);
      flush_meta_task = NULL;
    }
  }

  return ret;
}

int LogEngine::generate_flashback_task_(const FlashbackCbCtx &flashback_cb_ctx,
                                        LogIOFlashbackTask *&flashback_task)
{

  int ret = OB_SUCCESS;
  flashback_task = NULL;
  if (false == flashback_cb_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (flashback_task = alloc_mgr_->alloc_log_io_flashback_task(palf_id_, palf_epoch_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "alloc_log_io_flush_log_task failed", K(ret));
  } else if (OB_FAIL(flashback_task->init(flashback_cb_ctx,
                                          palf_id_))) {
    PALF_LOG(ERROR, "init LogIOFlashbackTask failed", K(ret));
  } else {
    PALF_LOG(TRACE, "generate_flashback_task_ hsuccess", K(ret), KPC(this));
  }
  if (OB_FAIL(ret) && NULL != flashback_task) {
    alloc_mgr_->free_log_io_flashback_task(flashback_task);
    flashback_task = NULL;
  }
  return ret;
}

int LogEngine::generate_purge_throttling_task_(const PurgeThrottlingCbCtx &purge_cb_ctx,
    LogIOPurgeThrottlingTask *&purge_task) {

  int ret = OB_SUCCESS;
  purge_task = NULL;
  if (!purge_cb_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (purge_task = alloc_mgr_->alloc_log_io_purge_throttling_task(palf_id_, palf_epoch_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "alloc_log_io_purge_throttling_task failed", KPC(this));
  } else if (OB_FAIL(purge_task->init(purge_cb_ctx))) {
    PALF_LOG(ERROR, "init LogIOPurgeThrottlingTask failed", KPC(this));
  } else {
    PALF_LOG(TRACE, "generate_purge_throttling_task_ hsuccess", KPC(this));
  }
  if (OB_FAIL(ret) && NULL != purge_task) {
    alloc_mgr_->free_log_io_purge_throttling_task(purge_task);
    purge_task = NULL;
  }
  return ret;
}

int LogEngine::serialize_log_meta_(const LogMeta& log_meta, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  LogMetaEntry log_meta_entry;
  LogMetaEntryHeader log_meta_entry_header;
  const int64_t log_meta_entry_header_len = log_meta_entry_header.get_serialize_size();
  // ASSUME the real length of meta is smaller than 4K
  const int64_t log_meta_entry_body_len = buf_len - log_meta_entry_header_len;
  const int64_t log_meta_entry_len = log_meta_entry_header_len + log_meta_entry_body_len;
  constexpr int64_t log_meta_body_serialize_buf_len = MAX_META_ENTRY_SIZE;
  int64_t pos = 0;
  char log_meta_body_serialize_buf[log_meta_body_serialize_buf_len] = {'\0'};
  if (MAX_META_ENTRY_SIZE != buf_len) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "log_meta_entry_len must be smaller than or equal to LOG_DIO_ALIGN_SIZE",
             K(ret), K_(palf_id), K_(is_inited), K(log_meta_entry_header_len), K(log_meta_entry_body_len), K(log_meta_entry_len),
             K(buf_len));
  } else if (FALSE_IT(memset(buf, '\0', buf_len))) {
    // serialize log_meta_entry_body
  } else if (OB_FAIL(log_meta.serialize(log_meta_body_serialize_buf, log_meta_body_serialize_buf_len, pos))) {
    PALF_LOG(ERROR, "log_meta serialize failed", K(ret), K_(palf_id), K_(is_inited));
  } else if (OB_FAIL(log_meta_entry_header.generate(log_meta_body_serialize_buf, log_meta_entry_body_len))) {
    PALF_LOG(ERROR, "generate LogMetaEntryHeader failed", K(ret), K(buf), K(log_meta_entry_body_len));
  } else if (OB_FAIL(log_meta_entry.generate(log_meta_entry_header, log_meta_body_serialize_buf))) {
    PALF_LOG(ERROR, "generate LogMetaEntry failed", K(ret), K(log_meta_entry_header));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(log_meta_entry.serialize(buf, buf_len, pos))) {
    PALF_LOG(ERROR, "LogMetaEntry serialize failed", K(ret), K(buf), K(pos),
             K(log_meta_entry_body_len), K(log_meta_entry_header_len), K(log_meta_entry_len));
  } else {
    PALF_LOG(INFO, "serialize_log_meta_ success", K(ret), K(log_meta),
             K(log_meta_entry), K(log_meta_entry_header_len));
  }
  return ret;
}

int LogEngine::update_config_meta_guarded_by_lock_(const LogConfigMeta &config_meta,
                                                   LogMeta &log_meta)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(log_meta_lock_);
  if (OB_FAIL(log_meta_.update_log_config_meta(config_meta))) {
    PALF_LOG(WARN, "update_log_config_meta failed", K(ret), K_(palf_id), K_(is_inited), K(config_meta));
  } else {
    log_meta = log_meta_;
  }
  return ret;
}

// Background:
int LogEngine::try_clear_up_holes_and_check_storage_integrity_(
    const LSN &last_entry_begin_lsn,
    const block_id_t &expected_next_block_id,
    LogGroupEntryHeader &last_group_entry_header)
{
  int ret = OB_SUCCESS;
  const LSN base_lsn = log_meta_.get_log_snapshot_meta().base_lsn_;
  const LogInfo prev_log_info = log_meta_.get_log_snapshot_meta().prev_log_info_;
  const bool prev_log_info_is_valid = prev_log_info.is_valid();
  // 'expected_next_block_id': ethier it's the empty block or not exist.
  block_id_t base_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  int64_t logical_block_size = 0;
  const LSN log_storage_tail = log_storage_.get_end_lsn();
  if (OB_FAIL(log_storage_.get_logical_block_size(logical_block_size))) {
    PALF_LOG(WARN, "get_logical_block_size failed", K(ret), K_(palf_id), K_(is_inited));
  } else if (FALSE_IT(base_block_id = lsn_2_block(base_lsn, logical_block_size))) {
  } else if (OB_FAIL(log_storage_.get_block_id_range(min_block_id, max_block_id))
      && OB_ENTRY_NOT_EXIST != ret) {
    PALF_LOG(ERROR, "get_block_id_range failed", K(ret), K_(palf_id), K_(is_inited));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // For empty directory, ethier 'base_lsn' is initial lsn or 'prev_log_info(rebuild)' is valid,
    // meanwhile, 'expected_next_block_id' must be smaller than or eaul to 'base_block_id'.
    if (true == is_valid_block_id(expected_next_block_id)
        && expected_next_block_id > base_block_id) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR,
               "the directory of log is empty, but expected_next_block_id is greater than "
               "base_block_id, unexpected error!!!",
               K(ret), K_(palf_id), K_(is_inited));
    } else {
      ret = OB_SUCCESS;
    }
    // If log_storage_ is not empty but last_group_entry_header is invalid, unexpected error.
    // For rebuild, the base_lsn may be greater than the log_tail of LogStorage because we
    // update LogSnapshotMeta firstly.
  } else if (log_storage_.get_end_lsn() != base_lsn && !last_group_entry_header.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unexpected error, LogStorage are not empty bus last log entry is invalid",
        K(last_entry_begin_lsn), K(expected_next_block_id), K(last_group_entry_header));
  } else {
    // NB: assume before 'truncate_prefix_blocks' finish, there is not any new write opt!!!
    //
    // For non-empty directory:
    // 1. If 'base_block_id' is greater than 'expected_next_block_id', we need delete all blocks
    //    between ['min_block_id', 'base_block_id');
    // 2. otherwise, check 'expected_next_block_id' whether is integrity.
    //
    // Ensure that:
    // 1. check LogStorage integrity only when 'expected_next_block_id' is greater than
    // 'base_block_id', consider rebuild, all blocks on disk may be deleted.;
    // 2. 'min_block_id' must be smaller than or equal to 'base_block_id';
    // 3. the last block is integral, means that 'max_block_id' is ethier equal to
    // 'expected_next_block_id'(last block is
    //    empty) or 'expected_next_block_id' is equal to 'max_block_id' + 1.
    LSN log_storage_tail = last_entry_begin_lsn + last_group_entry_header.get_serialize_size()
                           + last_group_entry_header.get_data_len();
    if (false == is_valid_block_id(expected_next_block_id)) {
      PALF_LOG(INFO,
               "expected_next_block_id is invalid, no need check_last_block_whether_is_integrity_",
               K(ret), KPC(this), K(expected_next_block_id));
    } else if (expected_next_block_id <= base_block_id) {
      // do nothing
    } else if (min_block_id > base_block_id
               || false
                      == check_last_block_whether_is_integrity_(expected_next_block_id,
                                                                max_block_id, log_storage_tail)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR,
               "directory is not empty, but there are something unexpected!!!(may be min block is "
               "less than base block "
               "or last block is incorrect)",
               K(min_block_id), K(base_block_id), K(expected_next_block_id), K(max_block_id));
      // NB: If just advance 'base_lsn' in rebuild, we need handle holes for 'get_block_id_range',
      // an easy way to do this is that, after advance 'base_lsn', the 'min_block_id_' of
      // 'LogBlockMgr' is lsn_2_block('base_lsn', PALF_BLOCK_SIZE), meanwhile, we need make
      // 'BlockGCTimerTask' can see the really 'min_block_id_' and 'max_block_id_'.
    } else {
    }

    // Deleting holes before 'base_block_id'
    // NB: deleting holes only when 'prev_log_info' is valid. 'prev_log_info' is valid means that
    // the rebuild option has occured, and we can't known previous log which before 'base_lsn'
    // whether has been confirmed, therefore, delete these blocks.
    // if the tail of last entry (log_storage_tail) is smaller than or equal to base lsn, need
    // reset last_group_entry_header, PalfHandleImpl will be inited with the prev log info saved
    // snapshot meta.
    if (OB_SUCC(ret) && true == prev_log_info_is_valid) {
      if (OB_FAIL(log_storage_.truncate_prefix_blocks(base_lsn))) {
        PALF_LOG(ERROR, "clear_up_holes_ failed", K(ret), K(min_block_id), K(max_block_id),
            K(base_block_id), K_(palf_id), K_(is_inited));
      } else if (base_lsn >= last_group_entry_header.get_committed_end_lsn()) {
        PALF_LOG(WARN, "the max committed end lsn is smaller than or equal to base_lsn,"
            " there is a rebuild operation before restart, and we will use prev_log_info"
            " to construct PalfBaseInfo", K_(palf_id), K(base_lsn), K(last_group_entry_header),
            K(prev_log_info));
        last_group_entry_header.reset();
      }
    }
  }
  PALF_LOG(INFO, "try_clear_up_holes_and_check_storage_integrity_ finish", K(ret), K_(palf_id), K(min_block_id),
           K(max_block_id), K(base_block_id), K(expected_next_block_id), K(prev_log_info));
  return ret;
}

bool LogEngine::check_last_block_whether_is_integrity_(const block_id_t expected_next_block_id,
                                                       const block_id_t max_block_id,
                                                       const LSN &log_storage_tail)
{
  // NB:
  // 1. 'expected_next_block_id' == 'max_block_id' + 1, normal case
  // 2. 'expected_next_block_id' <= 'max_block_id', means:
  //    1. a 'truncate' or 'flashback' opt before stop palf, we need update manifest first,
  //       and stop palf before delete blocks on disk, 'expected_next_block_id' is smaller
  //       than or equal to 'max_block_id'.
  //    2. a switch block opt before stop palf, and just create new block on disk success,
  //       expected_next_block_id is equal to 'max_block_id', and 'max_block_id' is empty,
  //       we need update manifest to max_block_id + 1 in process of restart.
  return expected_next_block_id == max_block_id + 1
         || expected_next_block_id <= max_block_id;
}

void LogEngine::reset_min_block_info_guarded_by_lock_(const block_id_t min_block_id, const SCN &min_block_max_scn)
{
  ObSpinLockGuard guard(block_gc_lock_);
  min_block_id_ = min_block_id;
  min_block_max_scn_ = min_block_max_scn;
}

LogNetService& LogEngine::get_net_service()
{
  return log_net_service_;
}
} // end namespace palf
} // end namespace oceanbase
