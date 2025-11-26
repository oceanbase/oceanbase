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

#include "ob_tenant_slog_checkpoint_workflow.h"

#include "observer/omt/ob_tenant.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/compaction/ob_compaction_schedule_util.h" // for MERGE_SCHEDULER_PTR
#include "observer/ob_server_event_history_table_operator.h" // for SERVER_EVENT_ADD

namespace oceanbase
{
namespace storage
{
using DiskedTabletFilterOp = ObTenantSlogCkptUtil::DiskedTabletFilterOp;
using TabletDfgtPicker = ObTenantSlogCkptUtil::TabletDefragmentPicker;
// =================================
//  ObTenantSlogCheckpointWorkflow
// =================================
ObTenantSlogCheckpointWorkflow::Context::Context(
  lib::ObMutex &supper_block_mutex,
  ObTenantSlogCkptUtil::MetaBlockListApplier &mbl_applier,
  ObTenantSlogCheckpointInfo &ckpt_info)
  : supper_block_mutex_(supper_block_mutex),
    mbl_applier_(mbl_applier),
    ckpt_info_(ckpt_info),
    t3m_(*MTL(ObTenantMetaMemMgr*)),
    tenant_(*static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant())),
    ls_service_(*MTL(ObLSService*)),
    server_smeta_svr_(SERVER_STORAGE_META_SERVICE),
    tenant_smeta_svr_(*MTL(ObTenantStorageMetaService*))
{
}

bool ObTenantSlogCheckpointWorkflow::Context::is_valid() const
{
  return mbl_applier_.is_valid() &&
         ckpt_info_.is_valid();
}

ObMemAttr ObTenantSlogCheckpointWorkflow::Context::get_mem_attr() const
{
  OB_ASSERT(is_valid());
  return ObMemAttr(MTL_ID(), ObModIds::OB_CHECKPOINT);
}

bool ObTenantSlogCheckpointWorkflow::Context::ignore_this_block(const MacroBlockId &block_id) const
{
  OB_ASSERT(is_valid());
  MacroBlockId cur_block_id;
  tenant_smeta_svr_.get_shared_object_raw_reader_writer().get_cur_shared_block(cur_block_id);
  return block_id == cur_block_id;
}

DEF_TO_STRING(ObTenantSlogCheckpointWorkflow::Context)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_service),
       K_(ckpt_info));
  J_OBJ_END();
  return pos;
}

int ObTenantSlogCheckpointWorkflow::execute(
    const Type type,
    ObTenantCheckpointSlogHandler &ckpt_handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ckpt_handler.is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ckpt_handler is not inited", K(ret));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_SLOG_CKPT_ERROR) ret;
    LOG_WARN("[ERRSIM] tenant slog checkpoint workflow", K(ret));
  }
#endif
  if (OB_SUCC(ret)) {
    ObTenantSlogCkptUtil::MetaBlockListApplier mbl_applier(
        &ckpt_handler.lock_,
        &ckpt_handler.ls_block_handle_,
        &ckpt_handler.tablet_block_handle_,
        &ckpt_handler.wait_gc_tablet_block_handle_);
    Context context(ckpt_handler.super_block_mutex_, mbl_applier, ckpt_handler.ckpt_info_);
    ObTenantSlogCheckpointWorkflow workflow(type, context);
    if (OB_FAIL(workflow.inner_execute_())) {
      STORAGE_LOG(WARN, "failed to execute tenant slog checkpoint workflow", K(ret), K(workflow));
    }
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::inner_execute_()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(DEBUG, "tenant slog checkpoint workflow start", KPC(this));

  if (OB_UNLIKELY(!is_valid_())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid slog checkpoint workflow", K(ret));
  } else if (OB_ISNULL(MERGE_SCHEDULER_PTR)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null merge scheduler ptr", K(ret));
  } else {
    Tracer tracer;
    tracer.start();

    /* ---step1: tablet defragment(if need) --- */
    if (!skip_defragment_()) {
      const bool parallel = parallel_enabled_();
      const bool pick_all_tablets = pick_all_tablets_();

      TabletDefragmentHelper defragment_helper(context_, tracer);
      if (parallel &&
          OB_FAIL(defragment_helper.do_defragment_parallel())) {
        STORAGE_LOG(WARN, "failed to do parallel tablet defragment", K(ret), KPC(this));
      } else if (!parallel &&
                 OB_FAIL(defragment_helper.do_defragment(pick_all_tablets))) {
        STORAGE_LOG(WARN, "failed to do tablet defragment", K(ret), KPC(this));
      }
    }

    /**
     * NOTE: Tablet defragment and slog truncate should be orthogonal(except COMPAT_UPGRADE mode)
     * the result of tablet defragment should not affect slog truncate (if can_ignore_errcode
     * returns TRUE). So do log and reset @c ret to OB_SUCCESS if some error occurs.
     */
    if (OB_FAIL(ret) && can_ignore_errcode_(ret)) {
      ret = OB_SUCCESS;
    }

    /* --- step2: slog truncate --- */
    if (OB_SUCC(ret)) {
      SlogTruncateHelper slog_truncate_helper(context_, tracer);
      if (OB_FAIL(slog_truncate_helper.do_truncate(force_truncate_slog_()))) {
        STORAGE_LOG(WARN, "failed to do slog truncate", K(ret), KPC(this));
      }
    }

    tracer.log_ckpt_finish(*this);
  }
  return ret;
}

bool ObTenantSlogCheckpointWorkflow::skip_defragment_() const
{
  OB_ASSERT(nullptr != MERGE_SCHEDULER_PTR);
  bool skip = false;
  if (type_ == Type::NORMAL_SS ||
      /* temporary disabled dfgt in SS */
      (GCTX.is_shared_storage_mode() && type_ == Type::FORCE)) {
    skip = true;
  } else if (type_ == Type::NORMAL_SN) {
    int64_t broadcast_version = MERGE_SCHEDULER_PTR->get_frozen_version();
    int64_t frozen_version = MERGE_SCHEDULER_PTR->get_inner_table_merged_scn();
    if (broadcast_version != frozen_version) {
      skip = true;
      STORAGE_LOG(INFO, "major is doing while trying to process tablet defragment, skip this time",
        K(broadcast_version), K(frozen_version));
    }
  }
  return skip;
}

DEF_TO_STRING(ObTenantSlogCheckpointWorkflow)
{
  int64_t pos = 0;
  ObString type;
  switch(type_) {
  case Type::NORMAL_SN:
    type = ObString("NORMAL_SN");
    break;
  case Type::NORMAL_SS:
    type = ObString("NORMAL_SS");
    break;
  case Type::COMPAT_UPGRADE:
    type = ObString("COMPAT_UPGRADE");
    break;
  case Type::FORCE:
    type = ObString("FORCE");
  }
  bool pick_all_tablets = pick_all_tablets_();
  bool skip_defragment = skip_defragment_();
  //bool write_slog_after_defragment = write_slog_after_defragment_();
  bool parallel_enabled = parallel_enabled_();
  bool force_truncate_slog = force_truncate_slog_();
  J_OBJ_START();
  J_KV(K(type),
       K(pick_all_tablets),
       K(skip_defragment),
       //K(write_slog_after_defragment),
       K(parallel_enabled),
       K(force_truncate_slog),
       K_(context));
  J_OBJ_END();
  return pos;
}

// ============================
//    TabletDefragmentHelper
// ============================
 void ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::ProgressPrinter::increase()
 {
    ++current_;
    if (current_ % print_interval_ == 0 || current_ == total_) {
        float percentage = current_ * 1. / total_;
        snprintf(print_buf_, sizeof(print_buf_), "tablet defragment process procedure: %.2f%%, current: %ld, total: %ld",
            percentage * 100, current_, total_);
        FLOG_INFO(print_buf_);
    }
}

ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::WriteTask::WriteTask(PSTHdl &hdl):
  PITask(hdl),
  ctx_(nullptr),
  allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "ParallelDfgt", ObCtxIds::DEFAULT_CTX_ID))
{
}

int ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::WriteTask::init(
    const ObTabletStorageParam &param,
    const Context *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid storage param", K(ret), K(param));
  } else if (OB_UNLIKELY(nullptr == ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null ctx", K(ret));
  } else {
    storage_param_ = param;
    ctx_ = ctx;
    on_init_succeed_();
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::WriteTask::execute()
{
  int ret = OB_SUCCESS;
  bool skipped = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet write task has not been inited yet", K(ret));
  } else {
    OB_ASSERT(nullptr != ctx_);
    // set tenant context
    ObTenantSwitchGuard guard(&ctx_->tenant_);
    if (OB_FAIL(ObTenantSlogCkptUtil::write_and_apply_tablet(storage_param_, ctx_->t3m_,
                                                             ctx_->ls_service_, ctx_->tenant_smeta_svr_,
                                                             allocator_, skipped))) {
      STORAGE_LOG(WARN, "failed to write tablet", K(ret), K_(storage_param));
    } else if (skipped) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected skipped tablet", K(ret), K_(storage_param));
    }
  }

  if (OB_SUCC(ret)) {
    on_exec_succeed_();
  } else {
    on_exec_error_(ret);
  }
  return ret;
}

int64_t ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::WriteTask::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(storage_param),
       KP_(ctx));
  J_OBJ_END();
  return pos;
}

ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::TabletDefragmentHelper(
  Context &ctx,
  Tracer &tracer)
    : ctx_(ctx),
      tablet_storage_params_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(ctx.get_mem_attr())),
      start_time_(ObTimeUtility::current_time()),
      tracer_(tracer)
{
  OB_ASSERT(ctx.is_valid());
}

int ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::pick_tablets_(const bool force_pick_all_tablets)
{
  OB_ASSERT(ctx_.is_valid());
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time_ms();
  if (force_pick_all_tablets && OB_FAIL(pick_all_tablets_())) {
    STORAGE_LOG(WARN, "failed to pick all tablets", K(ret), K(force_pick_all_tablets));
  } else if (!force_pick_all_tablets) {
    // then pick tablets by size amp
    bool need_defragment = false;
    if (OB_FAIL(check_if_required_(need_defragment))) {
      STORAGE_LOG(WARN, "failed to check_if_required", K(ret));
    } else if (need_defragment && OB_FAIL(pick_tablets_by_size_amp_())) {
      STORAGE_LOG(WARN, "failed to pick tablets by size amp", K(ret));
    }
  }
  const int64_t duration_ms = ObTimeUtility::current_time_ms() - start_time;
  tracer_.record_pick_tablets_cost(duration_ms);
  LOG_TRACE("pick tablets finished", K(ret), K(duration_ms));
  return ret;
}

int ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::check_if_required_(bool &need_defragment) const
{
  OB_ASSERT(ctx_.is_valid());
  int ret = OB_SUCCESS;
  need_defragment = false;
  int64_t interval = INTERVAL_FACTOR * ctx_.ckpt_info_.last_defragment_cost_us_;
  if (interval > MAX_DEFRAGMENT_INTERVAL) {
    interval = MAX_DEFRAGMENT_INTERVAL;
  } else if (interval < MIN_DEFRAGMENT_INTERVAL) {
    interval = MIN_DEFRAGMENT_INTERVAL;
  }
  if (ctx_.ckpt_info_.last_defragment_time_us_ + interval > start_time_) {
    LOG_TRACE("interval duration since the last checkpoint hasn't been reached yet", K_(ctx_.ckpt_info), K(interval), KTIME(start_time_));
  } else {
    need_defragment = true;
    LOG_TRACE("interval duration since the last checkpoint has been reached", K_(ctx_.ckpt_info), K(interval), KTIME(start_time_));
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::pick_tablets_by_size_amp_()
{
  int ret = OB_SUCCESS;

  if (!ctx_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is invalid", K(ret), K(ctx_));
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "pick tablets by size amplification is not supported in SS mode",  K(ret));
  } else {
    int64_t offset = 0, size = 0;
    int64_t empty_shell_size = 0;
    SMART_VAR(TabletDfgtPicker, picker, ctx_.get_mem_attr()) {
      /// NOTE: choose a reasonable value of bucket_num
      const int64_t total_tablet_cnt = ctx_.t3m_.get_total_tablet_cnt();
      const int64_t bucket_num = std::min(static_cast<int64_t>(std::ceil(total_tablet_cnt / 32.0)), 10000L);
      OB_ASSERT(0 == total_tablet_cnt || bucket_num > 0);
      if (OB_UNLIKELY(0 == total_tablet_cnt)) {
        // do nothing
        STORAGE_LOG(INFO, "total tablet cnt of t3m is 0, nothing to do", K(total_tablet_cnt));
      } else if (OB_FAIL(picker.create(bucket_num))) { // initialize buckets of tablet picker
        STORAGE_LOG(WARN, "failed to create tablet defragment picker", K(ret), K(bucket_num), K(total_tablet_cnt));
      } else {
        // initialize tablet defragment picker by t3m.
        {
          ObT3mTabletStorageParamIterator iter(ctx_.t3m_);
          ObTabletStorageParam param;
          MacroBlockId param_block;
          while (OB_SUCC(ret)) {
            param.reset();
            param_block.reset();
            if (OB_FAIL(iter.get_next(param))) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                STORAGE_LOG(WARN, "failed to get storage param of next tablet", K(ret));
              }
            } else if (param.original_addr_.is_file() &&
              OB_FAIL(tablet_storage_params_.push_back(param))) {
              /// NOTE: in order to transform empty shell into block,
              /// add empty shell into process queue.
              STORAGE_LOG(WARN, "failed to add empty shell tablet into process queue", K(ret), K(param));
            } else if (param.original_addr_.is_file()) {
              empty_shell_size += param.get_size(false);
            } else if (OB_FAIL(param.original_addr_.get_block_addr(param_block, offset, size))) {
              STORAGE_LOG(WARN, "tablet is not in block", K(ret), K(param));
              ret = OB_SUCCESS; // it's fine to continue
            } else if (ctx_.ignore_this_block(param_block)) {
              LOG_TRACE("skip tablet which has been written currently", K(param));
            } else if (OB_FAIL(picker.add_tablet(param))) {
              STORAGE_LOG(WARN, "failed to build tablet param defragment picker", K(ret), K(param));
            }
          }
        }

        // pick tablets if everything is ok.
        if (OB_SUCC(ret)) {
          const double total_size_amp = ObTenantSlogCkptUtil::cal_size_amplification(picker.block_num(),
            picker.total_tablet_size());

          tracer_.record_size_amp_before_dfgt(total_size_amp);
          int64_t total_picked_tablet_size = empty_shell_size;
          if (total_size_amp < SIZE_AMPLIFICATION_THRESHOLD) {
            // nothing to do if total_size_amp is less than threshold
            LOG_TRACE("total size amplification hasn't been reach the threshold yet", K(total_size_amp), K(SIZE_AMPLIFICATION_THRESHOLD));
          } else {
            LOG_TRACE("total size amplification reach the threshold", K(total_size_amp), K(SIZE_AMPLIFICATION_THRESHOLD));
            int64_t picked_tablet_size = 0;
            if (OB_FAIL(picker.pick_tablets_for_defragment(
                tablet_storage_params_,
                MIN_SIZE_AMPLIFICATION,
                MAX_PICKED_TABLET_CNT,
                MAX_PICKED_TABLET_SIZE,
                picked_tablet_size))) {
              STORAGE_LOG(WARN, "failed to pick tablets for defragment", K(ret));
            } else {
              total_picked_tablet_size += picked_tablet_size;
            }
          }
          if (OB_SUCC(ret) && total_picked_tablet_size > 0) {
            // record pick result
            if (OB_UNLIKELY(tablet_storage_params_.empty())) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "total_picked_tablet_size is greater than 0 while none tablet was picked", K(ret), K(total_picked_tablet_size), K(tablet_storage_params_.count()));
            } else {
              tracer_.record_dfgt_tablets_size(total_picked_tablet_size);
              tracer_.record_dfgt_tablets_cnt(tablet_storage_params_.count());
            }
          }
        }
      }
    }
  }
  return ret;
}

/// NOTE: make sure that all member in @c ctx_ is initialized before ObServerStorageMetaService::start() got called.
int ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::pick_all_tablets_()
{
  int ret = OB_SUCCESS;

  if (!ctx_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is invalid", K(ret), K(ctx_));
  } else {
    int64_t picked_tablet_size = 0;
    ObT3mTabletStorageParamIterator iter(ctx_.t3m_);
    ObTabletStorageParam param;
    if (OB_FAIL(tablet_storage_params_.reserve(ctx_.t3m_.get_total_tablet_cnt()))) {
      STORAGE_LOG(WARN, "failed to prealloc mem for tablet storage params", K(ret), K(ctx_.t3m_.get_total_tablet_cnt()));
    } else {
      while (OB_SUCC(ret)) {
        param.reset();
        if (OB_FAIL(iter.get_next(param))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            STORAGE_LOG(WARN, "failed to get storage param of next tablet", K(ret));
          }
        } else if (OB_FAIL(!param.original_addr_.is_disked())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tablet is not in disk", K(ret), K(param));
        } else if (OB_FAIL(tablet_storage_params_.push_back(param))) {
          STORAGE_LOG(WARN, "failed to build tablet param defragment picker", K(ret), K(param));
        } else {
          picked_tablet_size += param.get_size(param.original_addr_.is_block());
        }
      }

      if (OB_SUCC(ret)) {
        tracer_.record_dfgt_tablets_size(picked_tablet_size);
        tracer_.record_dfgt_tablets_cnt(tablet_storage_params_.count());
      }
    }
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::do_defragment(
    const bool force_pick_all_tablets)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid workflow ctx", K(ret), K(ctx_));
  } else if (OB_FAIL(pick_tablets_(force_pick_all_tablets))) {
    STORAGE_LOG(WARN, "failed to pick tablets", K(ret), K(force_pick_all_tablets));
  } else if (tablet_storage_params_.empty()) {
    LOG_TRACE("succeed to pick defragment tablets(nothing to do)", K(tablet_storage_params_.count()));
  } else {
    LOG_TRACE("succeed to pick defragment tablets", K(ret), K(tablet_storage_params_.count()));

    const int64_t total_tablet_cnt = tablet_storage_params_.count();
    ProgressPrinter progress_printer(total_tablet_cnt);
    ObArenaAllocator allocator(ctx_.get_mem_attr());
    bool skipped = false;
    int64_t total_skipped_cnt = 0;

    for (int64_t tsp_idx = 0; tsp_idx < total_tablet_cnt; ++tsp_idx) {
      allocator.reset();
      const ObTabletStorageParam &storage_param = tablet_storage_params_.at(tsp_idx);

      if (OB_FAIL(ObTenantSlogCkptUtil::write_and_apply_tablet(storage_param, ctx_.t3m_,
                                                               ctx_.ls_service_, ctx_.tenant_smeta_svr_,
                                                               allocator, skipped))) {
          STORAGE_LOG(WARN, "failed to write and install tablet, give up handling defragment", K(ret), K(storage_param), K(progress_printer));
          break;
      } else {
        total_skipped_cnt += skipped ? 1 : 0;
      }
      progress_printer.increase();
    }
    tracer_.record_skipped_tablet_cnt(total_skipped_cnt);
    tracer_.record_defragment_end(ret);
    if (FAILEDx(update_ckpt_info_())) {
        STORAGE_LOG(WARN, "failed to update defragment info", K(ret));
    }
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::do_defragment_parallel()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid workflow ctx", K(ret), K(ctx_));
  } else if (OB_FAIL(pick_tablets_(true))) {
    STORAGE_LOG(WARN, "failed to pick tablets", K(ret));
  } else if (tablet_storage_params_.empty()) {
    LOG_TRACE("succeed to pick defragment tablets(nothing to do)", K(tablet_storage_params_.count()));
  } else {
    PSTHdl parallel_startup_task_hdl;
    const int64_t total_tablet_cnt = tablet_storage_params_.count();
    const int64_t thread_cnt = parallel_startup_task_hdl.get_thread_cnt();
    LOG_TRACE("succeed to pick defragment tablets", K(ret), K(total_tablet_cnt));

    for (int64_t tsp_idx = 0; OB_SUCC(ret) && tsp_idx < total_tablet_cnt; ++tsp_idx) {
      const ObTabletStorageParam &storage_param = tablet_storage_params_.at(tsp_idx);
      if (OB_FAIL(parallel_startup_task_hdl.add_task<WriteTask>(storage_param, &ctx_))) {
        STORAGE_LOG(WARN, "failed to add task to parallel startup task handler", K(ret), K(storage_param));
      }
    }
    // do wait anyway
    if (OB_FAIL(ret)) {
      // whatever, we need to let thread finish, but ignore wait result
      parallel_startup_task_hdl.wait();
    } else if (OB_FAIL(parallel_startup_task_hdl.wait())) {
      STORAGE_LOG(WARN, "failed to wait parallel startup task handler", K(ret));
    }
    tracer_.record_defragment_end(ret);
    FLOG_INFO("parallel tablets defragment finished", K(ret), K(total_tablet_cnt), K(thread_cnt));
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::update_ckpt_info_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx_.ckpt_info_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ckpt_info", K(ret), K(ctx_.ckpt_info_));
  } else {
    int64_t end_time = ObTimeUtility::current_time();
    OB_ASSERT(end_time > start_time_);
    ctx_.ckpt_info_.last_defragment_time_us_ = end_time;
    ctx_.ckpt_info_.last_defragment_cost_us_ = end_time - start_time_;
  }
  return ret;
}

// ============================
//    SlogTruncateHelper
// ============================
ObTenantSlogCheckpointWorkflow::SlogTruncateHelper::SlogTruncateHelper(
  Context &ctx,
  Tracer &tracer)
  : ctx_(ctx),
    start_time_(ObTimeUtility::current_time()),
    tracer_(tracer)
{
}

int ObTenantSlogCheckpointWorkflow::SlogTruncateHelper::do_truncate(const bool is_force)
{
  int ret = OB_SUCCESS;
  bool need_truncate = is_force;
  common::ObLogCursor ckpt_cursor;
  if (OB_UNLIKELY(!ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is invalid", K(ret), K(ctx_));
  } else {
    HEAP_VAR(ObTenantSuperBlock, last_super_block, ctx_.tenant_.get_super_block()) {
      if (OB_UNLIKELY(!last_super_block.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to get tenant super block", K(ret), K(last_super_block));
      } else if (OB_FAIL(get_slog_ckpt_cursor_(ckpt_cursor))) {
        STORAGE_LOG(WARN, "failed to get slog ckpt cursor", K(ret));
      } else if (!is_force && OB_FAIL(check_if_required_(ckpt_cursor, last_super_block, need_truncate))) {
        STORAGE_LOG(WARN, "failed to do check_if_required", K(ret), K(ckpt_cursor));
      } else if (need_truncate) {
        common::ObSharedGuard<ObLSIterator> ls_iter;
        ObSlogCheckpointFdDispenser fd_dispenser;
        ObLS *ls = nullptr;

        tracer_.record_super_block_before_truncate(last_super_block);

        ls_item_writer_.reset();
        tablet_item_writer_.reset();
        ObMemAttr mem_attr = ctx_.get_mem_attr();

        ObSlogCheckpointFdDispenser *fd_dispenser_ptr = GCTX.is_shared_storage_mode() ? &fd_dispenser : nullptr;
        if (OB_NOT_NULL(fd_dispenser_ptr)) {
          fd_dispenser_ptr->set_cur_max_file_id(last_super_block.max_file_id_);
        }

        if (OB_FAIL(ctx_.ls_service_.get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
          STORAGE_LOG(WARN, "failed to get log stream iterator", K(ret));
        } else if (OB_FAIL(ls_item_writer_.init_for_slog_ckpt(MTL_ID(), MTL_EPOCH_ID(), mem_attr, fd_dispenser_ptr))) {
          STORAGE_LOG(WARN, "failed to init log stream item writer", K(ret));
        } else {
          // iterate and record all log streams.
          while (OB_SUCC(ret)) {
            ls = nullptr;
            if (OB_FAIL(ls_iter->get_next(ls))) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                STORAGE_LOG(WARN, "failed to get next log stream", K(ret));
              }
            }

            if (OB_ISNULL(ls)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "unexpected null ls", K(ret), KP(ls));
              break;
            }
            // record this ls meta
            if (OB_SUCC(ret) && OB_FAIL(record_single_ls_meta_(*ls, fd_dispenser_ptr))) {
              STORAGE_LOG(WARN, "failed to do record log stream meta", K(ret), KPC(ls));
            }
          }

          MacroBlockId ls_meta_entry;
          MacroBlockId wait_gc_tablet_entry = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(ls_item_writer_.close())) {
            STORAGE_LOG(WARN, "failed to close ls item writer", K(ret));
          } else if (OB_FAIL(ls_item_writer_.get_entry_block(ls_meta_entry))) {
            STORAGE_LOG(WARN, "failed to get ls entry block", K(ret));
          } else if (OB_FAIL(ObTenantSlogCkptUtil::record_wait_gc_tablet(ctx_.tenant_,
                                                                        ctx_.tenant_smeta_svr_,
                                                                        wait_gc_tablet_item_writer_ /*out*/,
                                                                        wait_gc_tablet_entry /*out*/,
                                                                        fd_dispenser_ptr,
                                                                        ctx_.get_mem_attr()))) {
            STORAGE_LOG(WARN, "failed to record wait gc tablet", K(ret));
          } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.fsync_block())) { // sync macro block link before update tenant's super block
            STORAGE_LOG(WARN, "failed to fsync block", K(ret));
          } else if (OB_FAIL(apply_truncate_result_(ckpt_cursor,
                                                    ls_meta_entry,
                                                    wait_gc_tablet_entry,
                                                    fd_dispenser))) { // apply truncate result if everything is ok.
            STORAGE_LOG(WARN, "failed to apply truncate result",
              K(ret), K(ckpt_cursor), K(ls_meta_entry), K(wait_gc_tablet_entry), K(fd_dispenser));
          }
        }
        tracer_.record_truncate_end(ret);

        if (FAILEDx(update_ckpt_info_())) {
          STORAGE_LOG(WARN, "failed to update slog truncate info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::SlogTruncateHelper::update_ckpt_info_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx_.ckpt_info_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ckpt_info", K(ret), K_(ctx_.ckpt_info));
  } else {
    ctx_.ckpt_info_.last_truncate_time_us_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::SlogTruncateHelper::get_slog_ckpt_cursor_(common::ObLogCursor &ckpt_cursor)
{
  OB_ASSERT(ctx_.is_valid());
  int ret = OB_SUCCESS;
  storage::ObStorageLogger *slogger = nullptr;
  if (FALSE_IT(slogger = &ctx_.tenant_smeta_svr_.get_slogger())) {
  } else if (OB_FAIL(slogger->get_active_cursor(ckpt_cursor))) {
    STORAGE_LOG(WARN, "failed to get current cursor", K(ret));
  } else if (OB_UNLIKELY(!ckpt_cursor.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid current cursor", K(ret));
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::SlogTruncateHelper::check_if_required_(
    const common::ObLogCursor &ckpt_cursor,
    const ObTenantSuperBlock &last_super_block,
    bool &need_truncate) const
{
  OB_ASSERT(ctx_.is_valid());
  int ret = OB_SUCCESS;
  int64_t truncate_interval = MAX_TRUNCATE_INTERVAL;
  need_truncate = false;

  if (OB_UNLIKELY(!ckpt_cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ckpt cursor is invalid", K(ret), K(ckpt_cursor));
  } else if (OB_UNLIKELY(!ctx_.ckpt_info_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null ckpt info", K(ret));
  } else {
    if (OB_UNLIKELY(!last_super_block.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected invalid super block", K(ret), K(last_super_block), K(last_super_block));
    } else if (last_super_block.is_old_version()) {
      need_truncate = true;
      LOG_TRACE("tenant super block is old version", K(ret), K(need_truncate));
    } else if (start_time_ > ctx_.ckpt_info_.last_truncate_time_us_ + truncate_interval && // reach interval
               ckpt_cursor.newer_than(last_super_block.replay_start_point_) && // slog is long
               ckpt_cursor.log_id_ - last_super_block.replay_start_point_.log_id_ >= MIN_WRITE_CHECKPOINT_LOG_CNT) {
      need_truncate = true;
      LOG_TRACE("the length of slog reached the threshold", K(ret), K(need_truncate),
        K(ckpt_cursor), K(last_super_block.replay_start_point_));
    }
  }
  return ret;
}

/// see ObTenantStorageSnapshotWriter::write_item
int ObTenantSlogCheckpointWorkflow::SlogTruncateHelper::write_ls_item_(const ObLSCkptMember &ls_ckpt_member)
{
  OB_ASSERT(ctx_.is_valid());
  int ret = OB_SUCCESS;
  int64_t buf_len = ls_ckpt_member.get_serialize_size();
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_UNLIKELY(!ls_ckpt_member.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ls_ckpt_member", K(ret), K(ls_ckpt_member));
  } else if (OB_ISNULL(buf = static_cast<char*>(common::ob_malloc(buf_len, ctx_.get_mem_attr())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to allocate memory", K(ret), K(buf_len));
  } else if (OB_FAIL(ls_ckpt_member.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "failed to serialize ls_ckpt_member", K(ret), KP(buf), K(buf_len), K(pos), K(ls_ckpt_member));
  } else if (OB_FAIL(ls_item_writer_.write_item(buf, buf_len, /*item idx*/nullptr))) {
    STORAGE_LOG(WARN, "failed to write ls_ckpt_member", K(ret), KP(buf), K(buf_len), K(ls_ckpt_member));
  }

  if (OB_LIKELY(nullptr != buf)) {
    common::ob_free(buf);
  }
  return ret;
}

/// see ObTenantStorageSnapshotWriter::do_record_ls_meta
int ObTenantSlogCheckpointWorkflow::SlogTruncateHelper::record_single_ls_meta_(
    ObLS &ls,
    ObSlogCheckpointFdDispenser *fd_dispenser)
{
  OB_ASSERT(ctx_.is_valid());
  int ret = OB_SUCCESS;

  ObLSCkptMember ls_ckpt_member;
  {
    ObLSLockGuard lock_ls(&ls);
    if (OB_FAIL(ls.get_ls_meta(ls_ckpt_member.ls_meta_))) {
      STORAGE_LOG(WARN, "failed to get ls meta", K(ret));
    } else if (OB_FAIL(ls.get_dup_table_ls_meta(ls_ckpt_member.dup_ls_meta_))) {
      STORAGE_LOG(WARN, "failed to get dup ls meta", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(record_ls_tablets_(ls, ls_ckpt_member.tablet_meta_entry_/*out*/, fd_dispenser))) {
    STORAGE_LOG(WARN, "failed to write checkpoint for this ls", K(ret), K(ls));
  } else if (OB_FAIL(write_ls_item_(ls_ckpt_member))) {
    STORAGE_LOG(WARN, "failed to write ls item", K(ret), K(ls_ckpt_member));
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::SlogTruncateHelper::record_ls_tablets_(
    ObLS &ls,
    MacroBlockId &tablet_meta_entry,
    ObSlogCheckpointFdDispenser *fd_dispenser)
{
  OB_ASSERT(ctx_.is_valid());
  int ret = OB_SUCCESS;

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;

  char slog_buf[sizeof(ObUpdateTabletLog)];

  DiskedTabletFilterOp iter_op; // skip in mem tablet
  /// @c mode_ seems unused in ObLSTabletFastIter(only for validity check)
  ObLSTabletFastIter tablet_iter(iter_op, ObMDSGetTabletMode::READ_WITHOUT_CHECK);

  tablet_item_writer_.reuse_for_next_round();
  ObMemAttr mem_attr = ctx_.get_mem_attr();
  if (OB_FAIL(tablet_item_writer_.init_for_slog_ckpt(MTL_ID(), MTL_EPOCH_ID(), mem_attr, fd_dispenser))) {
    STORAGE_LOG(WARN, "failed to init tablet item writer", K(ret));
  } else if (OB_FAIL(ls.get_tablet_svr()->build_tablet_iter_with_lock_hold(tablet_iter))) {
    STORAGE_LOG(WARN, "failed to build tablet addr iterator", K(ret), K(ls));
  }

  while (OB_SUCC(ret)) {
    tablet_handle.reset();
    tablet = nullptr;

    if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        STORAGE_LOG(WARN, "failed to get next tablet", K(ret));
      }
    } else if (OB_FAIL(record_single_tablet_(tablet_handle, ls.get_ls_epoch(), slog_buf))) {
      STORAGE_LOG(WARN, "failed to record single tablet", K(ret), K(tablet_handle));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tablet_item_writer_.close())) {
    STORAGE_LOG(WARN, "failed to close tablet item writer", K(ret));
  } else if (OB_FAIL(tablet_item_writer_.get_entry_block(tablet_meta_entry))) {
    STORAGE_LOG(WARN, "failed to get tablet meta entry", K(ret));
  }

  STORAGE_LOG(INFO, "record ls tablets finish", K(ret), K(tablet_meta_entry));
  return ret;
}

int ObTenantSlogCheckpointWorkflow::SlogTruncateHelper::record_single_tablet_(
    ObTabletHandle &tablet_handle,
    const int64_t ls_epoch,
    char (&slog_buf)[sizeof(ObUpdateTabletLog)])
{
  OB_ASSERT(ctx_.is_valid());
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  int64_t slog_buf_pos = 0;
  ObMetaDiskAddr old_addr;

  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid tablet handle", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null tablet", K(ret));
  } else if (FALSE_IT(old_addr = tablet->get_tablet_addr()))  {
  } else if (OB_UNLIKELY(!old_addr.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid tablet addr", K(ret), K(old_addr));
  } else if (OB_UNLIKELY(!old_addr.is_disked())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected non disked tablet", K(tablet->get_tablet_addr()));
  } else if (old_addr.is_block()) {
    /**
     * NOTE: CAN'T USE ObTablet::is_empty_shell() to judge whether a tablet is empty shell here,
     * because empty shell would be transform into BLOCK after defragment. If an empty shell is block
     * type, we should also record it into tablet's macro block list.
     */
    // write tablet item
    ObUpdateTabletPointerParam update_param;
    ObUpdateTabletLog slog;
    if (OB_FAIL(tablet->get_updating_tablet_pointer_param(update_param))) {
      STORAGE_LOG(WARN, "failed to get updating tablet pointer param", K(ret), K(update_param));
    } else if (FALSE_IT(slog = ObUpdateTabletLog(tablet->get_ls_id(),
                                                 tablet->get_tablet_id(),
                                                 update_param,
                                                 ls_epoch))) {
    } else if (OB_UNLIKELY(!slog.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid slog entry", K(ret), K(slog), K(tablet->get_ls_id()), K(tablet->get_tablet_id()), K(ls_epoch), K(update_param));
    } else if (OB_FAIL(slog.serialize(slog_buf, sizeof(ObUpdateTabletLog), slog_buf_pos))) {
      STORAGE_LOG(WARN, "failed to serialize update tablet slog", K(ret), K(slog_buf_pos));
    } else if (OB_FAIL(tablet_item_writer_.write_item(slog_buf, slog.get_serialize_size()))) {
      STORAGE_LOG(WARN, "failed to write update tablet slog into ckpt", K(ret));
    }
  } else if (old_addr.is_file()) {
    OB_ASSERT(!GCTX.is_shared_storage_mode()); // IMPOSSIBLE
    const ObTabletMapKey tablet_key = ObTabletMapKey(tablet->get_ls_id(), tablet->get_tablet_id());

    tablet = nullptr;
    tablet_handle.reset(); // release tablet before refresh

    ObLSHandle ls_handle;
    if (OB_FAIL(ctx_.ls_service_.get_ls(tablet_key.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      STORAGE_LOG(WARN, "failed to get ls", K(ret), K(tablet_key));
    } else if (OB_FAIL(ls_handle.get_ls()->refresh_empty_shell_for_slog_ckpt(ctx_.t3m_, tablet_key, old_addr))) {
      STORAGE_LOG(WARN, "failed to apply defragment tablet", K(ret), K(tablet_key), K(old_addr));
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
  } else {
    // UNREACHABLE
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unsupported tablet addr type in record_single_tablet!!!", K(ret), K(old_addr));
  }
  return ret;
}

int ObTenantSlogCheckpointWorkflow::SlogTruncateHelper::apply_truncate_result_(
    const common::ObLogCursor &ckpt_cursor,
    const MacroBlockId &ls_meta_entry,
    const MacroBlockId &wait_gc_tablet_entry,
    const ObSlogCheckpointFdDispenser &fd_dispenser)
{
  OB_ASSERT(ctx_.is_valid());
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ckpt_cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ckpt_cursor", K(ret), K(ckpt_cursor));
  } else if (OB_UNLIKELY(!ls_meta_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ls_meta_entry", K(ret), K(ls_meta_entry));
  } else if (OB_UNLIKELY(!wait_gc_tablet_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid wait_gc_tablet_entry", K(ret), K(wait_gc_tablet_entry));
  } else {
    // step1: update tenant super block
    HEAP_VAR(ObTenantSuperBlock, super_block) {
      /// HELD SUPER BLOCK MUTEX OF @c ObTenantCheckpointSlogHandler
      lib::ObMutexGuard guard(ctx_.supper_block_mutex_);
      super_block = ctx_.tenant_.get_super_block();
      super_block.replay_start_point_ = ckpt_cursor;
      super_block.ls_meta_entry_ = ls_meta_entry;
      super_block.wait_gc_tablet_entry_ = wait_gc_tablet_entry;
      super_block.copy_snapshots_from(ctx_.tenant_.get_super_block());
      if (OB_FAIL(ctx_.server_smeta_svr_.update_tenant_super_block(0, super_block))) {
        STORAGE_LOG(WARN, "failed to update tenant super block", K(ret), K(super_block));
      } else {
        super_block.min_file_id_ = fd_dispenser.get_min_file_id();
        super_block.max_file_id_ = fd_dispenser.get_max_file_id();
        ctx_.tenant_.set_tenant_super_block(super_block);
      }
      tracer_.record_super_block_after_truncate(super_block);
    }

    OB_ASSERT(ctx_.mbl_applier_.is_valid());

    const ObIArray<MacroBlockId> &ls_block_list = ls_item_writer_.get_meta_block_list();
    const ObIArray<MacroBlockId> &tablet_block_list = tablet_item_writer_.get_meta_block_list();
    const ObIArray<MacroBlockId> &wait_gc_tablet_block_list = wait_gc_tablet_item_writer_.get_meta_block_list();

    /** step2: apply block list and remove useless slog file if step1 is successful.
     * NOTE: apply block list aims for hold the macro block's ref cnt, and we should ABORT
     * if failed to apply block list because once tenant super block has been updated,
     * it cannot be rollback.
     */
    if (OB_FAIL(ret)) {
      // do nothing
    }  else if (OB_FAIL(ctx_.mbl_applier_.apply_from(ls_block_list, tablet_block_list, wait_gc_tablet_block_list))) {
      STORAGE_LOG(WARN, "failed to apply macro block lists", K(ret));
      // abort if failed.
      ob_usleep(1000 * 1000);
      ob_abort();
    } else if (OB_FAIL(ctx_.tenant_smeta_svr_.get_slogger().remove_useless_log_file(ckpt_cursor.file_id_, MTL_ID()))) {
      STORAGE_LOG(WARN, "failed to remove useless slog file", K(ret), K(ctx_.tenant_.get_super_block()));
    }
  }
  return ret;
}

// ============
//    Tracer
// ============
ObTenantSlogCheckpointWorkflow::Tracer::Tracer()
  : start_time_(INT_MAX),
    defragment_end_time_(INT_MAX),
    defragment_ret_(INT_MAX),
    size_amp_before_defragment_(DBL_MAX),
    pick_tablets_cost_ms_(INT_MAX),
    defragment_tablets_size_(INT_MAX),
    defragment_tablets_cnt_(INT_MAX),
    skipped_tablet_cnt_(INT_MAX),
    truncate_end_time_(-1),
    truncate_ret_(INT_MAX),
    super_block_before_truncate_(),
    super_block_after_truncate_()
{
}

void ObTenantSlogCheckpointWorkflow::Tracer::log_defragment_only_(const int64_t total_cost_time, const ObTenantSlogCheckpointWorkflow &workflow) const
{
  int ret = OB_SUCCESS;
  FLOG_INFO("tenant slog checkpoint finished(only did defragment)",
        K(workflow),
        KTIME(start_time_.get()),
        K_(defragment_ret), // defragment status
        KTIME(defragment_end_time_.get()),
        K_(size_amp_before_defragment),
        K_(pick_tablets_cost_ms),
        K_(defragment_tablets_size),
        K_(defragment_tablets_cnt),
        K_(skipped_tablet_cnt),
        K(total_cost_time));
}

void ObTenantSlogCheckpointWorkflow::Tracer::log_truncate_only_(const int64_t total_cost_time, const ObTenantSlogCheckpointWorkflow &workflow) const
{
  // only do slog truncate(NORMAL_SS)
  int ret = OB_SUCCESS;
  FLOG_INFO("tenant slog checkpoint finished（only did slog truncate)",
    K(workflow),
    KTIME(start_time_.get()),
    K_(size_amp_before_defragment),
    K_(truncate_ret), // truncate status
    KTIME(truncate_end_time_.get()),
    K_(super_block_before_truncate),
    K_(super_block_after_truncate),
    K(total_cost_time));
}

void ObTenantSlogCheckpointWorkflow::Tracer::log_both_(const int64_t total_cost_time, const ObTenantSlogCheckpointWorkflow &workflow) const
{
  int ret = OB_SUCCESS;
  FLOG_INFO("tenant slog checkpoint finished",
        K(workflow),
        KTIME(start_time_.get()),
        K_(defragment_ret), // defragment status
        KTIME(defragment_end_time_.get()),
        K_(size_amp_before_defragment),
        K_(pick_tablets_cost_ms),
        K_(defragment_tablets_size),
        K_(defragment_tablets_cnt),
        K_(skipped_tablet_cnt),
        K_(truncate_ret), // truncate status
        KTIME(truncate_end_time_.get()),
        K_(super_block_before_truncate),
        K_(super_block_after_truncate),
        K(total_cost_time));
}

void ObTenantSlogCheckpointWorkflow::Tracer::log_ckpt_finish(const ObTenantSlogCheckpointWorkflow &workflow) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!start_time_.setted())) {
    FLOG_WARN("failed to trace tenant slog checkpoint, nothing to log");
  } else if (!defragment_ret_.setted() && !truncate_ret_.setted()) {
    // nothing to log
  } else {
    uint64_t tenant_id = MTL_ID();
    int64_t total_cost_time =  ObTimeUtility::current_time() - start_time_.get();
    if (truncate_ret_.setted() && !defragment_ret_.setted()) {
      log_truncate_only_(total_cost_time, workflow);
    } else if (defragment_ret_.setted() && !truncate_ret_.setted()) {
      log_defragment_only_(total_cost_time, workflow);
    } else {
      log_both_(total_cost_time, workflow);
    }
    SERVER_EVENT_ADD(
      "storage", "tenant slog checkpoint",
      "tenant_id", tenant_id,
      "size_amp_before_defragment", size_amp_before_defragment_,
      "defragment_ret", defragment_ret_,
      "super_block_after_truncate", super_block_after_truncate_,
      "truncate_ret", truncate_ret_,
      "cost_time(us)", total_cost_time);
  }
}

} // end namespace storage
} // end namespace oceanbase

