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

#include "storage/tx_storage/ob_checkpoint_service.h"
#include "logservice/ob_log_service.h"
#include "logservice/archiveservice/ob_archive_service.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/share/ob_shared_ls_meta.h"
#include "storage/incremental/ob_shared_meta_service.h"
#endif

namespace oceanbase
{
using namespace share;
using namespace palf;
namespace storage
{
namespace checkpoint
{

int64_t ObCheckPointService::CHECK_CLOG_USAGE_INTERVAL = 2000 * 1000L;
int64_t ObCheckPointService::CHECKPOINT_INTERVAL = 5000 * 1000L;
int64_t ObCheckPointService::TRAVERSAL_FLUSH_INTERVAL = 5000 * 1000L;

#ifdef OB_BUILD_SHARED_STORAGE
// Update ss checkpoint scn each 10 seconds
int64_t ObCheckPointService::SS_UPDATE_CKPT_INTERVAL = 10LL * 1000LL * 1000LL;

// Flush all CLOG module each 10 minutes
int64_t ObCheckPointService::SS_ADVANCE_CKPT_INTERVAL = 10LL * 60LL * 1000LL * 1000LL;

// try schedule full/emergency upload each second
int64_t ObCheckPointService::SS_TRY_SCHEDULE_UPLOAD_INTERVAL = 1LL * 1000LL * 1000LL;
#endif

int ObCheckPointService::mtl_init(ObCheckPointService* &m)
{
  return m->init(MTL_ID());
}

int ObCheckPointService::init(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCheckPointService init twice.", K(ret));
  } else if (OB_FAIL(freeze_thread_.init(tenant_id, lib::TGDefIDs::LSFreeze))) {
    LOG_WARN("fail to initialize freeze thread", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

#define INIT_TRACE_GUARD           \
  ObCurTraceId::TraceId trace_id;  \
  trace_id.init(GCONF.self_addr_); \
  ObTraceIdGuard trace_id_guard(trace_id);

#define INIT_AND_START_TIMER_TASK(TIMER, TASK, NAME, LABEL, INTERVAL, REPEAT) \
  if (OB_FAIL(ret)) {                                                         \
  } else if (OB_FAIL(TIMER.set_run_wrapper_with_ret(MTL_CTX()))) {            \
    STORAGE_LOG(ERROR, "fail to set checkpoint_timer's run wrapper", K(ret)); \
  } else if (OB_FAIL(TIMER.init(NAME, ObMemAttr(MTL_ID(), LABEL)))) {         \
    STORAGE_LOG(ERROR, "fail to init checkpoint_timer", K(ret));              \
  } else if (OB_FAIL(TIMER.schedule(TASK, INTERVAL, REPEAT))) {               \
    STORAGE_LOG(ERROR, "fail to schedule checkpoint task", K(ret));           \
  } else {                                                                    \
    STORAGE_LOG(INFO, "finish schedule timer task", K(NAME), K(INTERVAL));    \
  }

int ObCheckPointService::start()
{
  int ret = OB_SUCCESS;
  INIT_AND_START_TIMER_TASK(checkpoint_timer_,
                            checkpoint_task_,
                            "TxCkpt",
                            "CheckPointTimer",
                            CHECKPOINT_INTERVAL,
                            true/* repeat */);
  INIT_AND_START_TIMER_TASK(traversal_flush_timer_,
                            traversal_flush_task_,
                            "Flush",
                            "FLushTimer",
                            TRAVERSAL_FLUSH_INTERVAL,
                            true/* repeat */);
  INIT_AND_START_TIMER_TASK(check_clog_disk_usage_timer_,
                            check_clog_disk_usage_task_,
                            "CKCClogDisk",
                            "DiskUsageTimer",
                            CHECK_CLOG_USAGE_INTERVAL,
                            true/* repeat */);

#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    INIT_AND_START_TIMER_TASK(ss_update_ckpt_scn_timer_,
                              ss_update_ckpt_scn_task_,
                              "SSUpCkptSCN",
                              "SSUpSCNTimer",
                              SS_UPDATE_CKPT_INTERVAL,
                              true/* repeat */);
    INIT_AND_START_TIMER_TASK(ss_update_ckpt_lsn_timer_,
                              ss_update_ckpt_lsn_task_,
                              "SSUpCkptLSN",
                              "SSUpLSNTimer",
                              SS_UPDATE_CKPT_INTERVAL,
                              true/* repeat */);
    INIT_AND_START_TIMER_TASK(ss_advance_ckpt_timer_,
                              ss_advance_ckpt_task_,
                              "SSAdvanceCKPT",
                              "SSAdvanceTimer",
                              SS_ADVANCE_CKPT_INTERVAL,
                              true/* repeat */);
    INIT_AND_START_TIMER_TASK(ss_schedule_upload_timer_,
                              ss_schedule_upload_task_,
                              "SSUploader",
                              "SSUploaderTimer",
                              SS_TRY_SCHEDULE_UPLOAD_INTERVAL,
                              true/* repeat */);
  }
#endif // OB_BUILD_SHARED_STORAGE
  return ret;
}
#undef INIT_AND_START_TIMER_TASK

int ObCheckPointService::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckPointService is not initialized", K(ret));
  } else {
    TG_STOP(freeze_thread_.get_tg_id());
    LOG_INFO("ObCheckPointService stoped");
  }
  checkpoint_timer_.stop();
  traversal_flush_timer_.stop();
  check_clog_disk_usage_timer_.stop();

#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    ss_update_ckpt_scn_timer_.stop();
    ss_update_ckpt_lsn_timer_.stop();
    ss_advance_ckpt_timer_.stop();
    ss_schedule_upload_timer_.stop();
  }
#endif

  return ret;
}

void ObCheckPointService::wait()
{
  checkpoint_timer_.wait();
  traversal_flush_timer_.wait();
  check_clog_disk_usage_timer_.wait();

#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    ss_update_ckpt_scn_timer_.wait();
    ss_update_ckpt_lsn_timer_.wait();
    ss_advance_ckpt_timer_.wait();
    ss_schedule_upload_timer_.wait();
  }
#endif
  TG_WAIT(freeze_thread_.get_tg_id());
}

int ObCheckPointService::add_ls_freeze_task(
    ObDataCheckpoint *data_checkpoint,
    SCN rec_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(freeze_thread_.add_task(data_checkpoint, rec_scn))) {
    STORAGE_LOG(WARN, "logstream freeze task failed", K(ret));
  }
  return ret;
}

void ObCheckPointService::destroy()
{
  TG_DESTROY(freeze_thread_.get_tg_id());
  is_inited_ = false;
  checkpoint_timer_.destroy();
  traversal_flush_timer_.destroy();
  check_clog_disk_usage_timer_.destroy();

#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    ss_update_ckpt_scn_timer_.destroy();
    ss_update_ckpt_lsn_timer_.destroy();
    ss_advance_ckpt_timer_.destroy();
    ss_schedule_upload_timer_.destroy();
  }
#endif
}

struct CheckpointFunctorForLS {
public:
  int operator()(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    const ObLSID ls_id = ls.get_ls_id();
    ObCheckpointExecutor *checkpoint_executor = nullptr;
    ObDataCheckpoint *data_checkpoint = nullptr;
    if (OB_ISNULL(data_checkpoint = ls.get_data_checkpoint())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "data_checkpoint should not be null", K(ret), K(ls_id));
    } else if (OB_FAIL(data_checkpoint->check_can_move_to_active_in_newcreate())) {
      STORAGE_LOG(WARN, "check can move to active failed", K(ret), K(ls_id));
    } else if (OB_ISNULL(checkpoint_executor = ls.get_checkpoint_executor())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "checkpoint_executor should not be null", K(ls_id));
    } else if (OB_FAIL(checkpoint_executor->update_clog_checkpoint())) {
      STORAGE_LOG(WARN, "update_clog_checkpoint failed", K(ret), K(ls_id));
    } else if (GCONF.enable_logservice) {
      succ_ls_cnt_++;
      STORAGE_LOG(DEBUG, "skip advance_local_clog_base_lsn_ in logservice mode", K(ls_id));
    } else if (OB_FAIL(advance_local_clog_base_lsn_(ls))) {
      STORAGE_LOG(WARN, "advance_local_clog_base_lsn_ failed", K(ret), K(ls_id));
    }

    iterated_ls_cnt_++;
    // returen OB_SUCCESS to iterate all logstreams
    return OB_SUCCESS;
  }

  TO_STRING_KV(K(iterated_ls_cnt_), K(succ_ls_cnt_));

private:
  int advance_local_clog_base_lsn_(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    const ObLSID ls_id = ls.get_ls_id();

    palf::LSN archive_lsn;
    SCN unused_archive_scn;
    bool archive_force_wait = false;
    bool archive_ignore = false;
    if (OB_FAIL(MTL(archive::ObArchiveService *)
                    ->get_ls_archive_progress(
                        ls_id, archive_lsn, unused_archive_scn, archive_force_wait, archive_ignore))) {
      STORAGE_LOG(WARN, "get ls archive progress failed", K(ret), K(ls_id));
    } else {
      palf::LSN checkpoint_lsn = ls.get_clog_base_lsn();
      if (!archive_force_wait || archive_ignore || archive_lsn >= checkpoint_lsn) {
        // do nothing
      } else {
        STORAGE_LOG(TRACE,
                    "archive_lsn small than checkpoint_lsn, set base_lsn with archive_lsn",
                    K(archive_lsn),
                    K(checkpoint_lsn),
                    K(ls));
        checkpoint_lsn = archive_lsn;
      }
      if (OB_FAIL(ls.get_log_handler()->advance_base_lsn(checkpoint_lsn))) {
        STORAGE_LOG(WARN, "advance base lsn failed", K(ret), K(checkpoint_lsn));
      } else {
        succ_ls_cnt_++;
        FLOG_INFO("[CHECKPOINT] advance palf base lsn successfully", K(checkpoint_lsn), K(ls_id));
      }
    }
    return ret;
  }

public:
  int64_t iterated_ls_cnt_;
  int64_t succ_ls_cnt_;
};

void ObCheckPointService::ObCheckpointTask::runTimerTask()
{
  STORAGE_LOG(INFO, "====== checkpoint timer task ======");

  int ret = OB_SUCCESS;
  INIT_TRACE_GUARD;
  CheckpointFunctorForLS checkpoint_func;
  if (OB_FAIL(MTL(ObLSService *)->foreach_ls(checkpoint_func))) {
    STORAGE_LOG(WARN, "for each ls functor failed", KR(ret));
  } else {
    STORAGE_LOG(INFO, "checkpoint ls finish", K(checkpoint_func));
  }
}

struct TraverseFlushFunctorForLS {
public:
  int operator()(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    ObCheckpointExecutor *checkpoint_executor = nullptr;
    if (OB_ISNULL(checkpoint_executor = ls.get_checkpoint_executor())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "checkpoint_executor should not be null", K(ls.get_ls_id()));
    } else if (OB_FAIL(checkpoint_executor->traversal_flush())) {
      STORAGE_LOG(WARN, "traversal_flush failed", K(ret), K(ls.get_ls_id()));
    }

    // returen OB_SUCCESS to iterate all logstreams
    return OB_SUCCESS;
  }
};

void ObCheckPointService::ObTraversalFlushTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "====== traversal_flush timer task ======");
  INIT_TRACE_GUARD;
  TraverseFlushFunctorForLS traverse_flush_func;
  if (OB_FAIL(MTL(ObLSService *)->foreach_ls(traverse_flush_func))) {
    STORAGE_LOG(WARN, "for each ls functor failed", KR(ret));
  }
}

struct FlushRecycleClogFunctorForLS {
public:
  FlushRecycleClogFunctorForLS() : iterated_ls_cnt_(0), succ_ls_cnt_(0) {}
  int operator()(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    ObCheckpointExecutor *checkpoint_executor = ls.get_checkpoint_executor();
    ObDataCheckpoint *data_checkpoint = ls.get_data_checkpoint();
    if (OB_ISNULL(checkpoint_executor) || OB_ISNULL(data_checkpoint)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,
                  "checkpoint_executor or data_checkpoint should not be null",
                  KP(checkpoint_executor),
                  KP(data_checkpoint));
    } else if (data_checkpoint->is_flushing()) {
      STORAGE_LOG(TRACE, "data_checkpoint is flushing");
    } else if (OB_FAIL(checkpoint_executor->update_clog_checkpoint())) {
      STORAGE_LOG(WARN, "update_clog_checkpoint failed", KP(checkpoint_executor), KP(data_checkpoint));
    } else if (OB_FAIL(ls.flush_to_recycle_clog())) {
      STORAGE_LOG(WARN, "flush ls to recycle clog failed", K(ls.get_ls_id()));
    } else {
      ++succ_ls_cnt_;
    }

    iterated_ls_cnt_++;
    // returen OB_SUCCESS to iterate all logstreams
    return OB_SUCCESS;
  }

  TO_STRING_KV(K(iterated_ls_cnt_), K(succ_ls_cnt_));

public:
  int64_t iterated_ls_cnt_;
  int64_t succ_ls_cnt_;
};

void ObCheckPointService::ObCheckClogDiskUsageTask::runTimerTask()
{
  STORAGE_LOG(INFO, "====== check clog disk timer task ======");
  int ret = OB_SUCCESS;
  bool need_flush = false;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected error, ObLogService is nullptr", KP(log_service));
  } else if (GCONF.enable_logservice) {
    if (REACH_TIME_INTERVAL(10_s)) {
      STORAGE_LOG(TRACE, "Independent LogService does not need check disk usage");
    }
  } else if (OB_FAIL(log_service->check_need_do_checkpoint(need_flush))) {
    STORAGE_LOG(WARN, "check_need_do_checkpoint failed", KP(log_service));
  } else if (need_flush) {
    INIT_TRACE_GUARD;
    FlushRecycleClogFunctorForLS flush_to_recycle_clog_func;
    if (OB_FAIL(MTL(ObLSService *)->foreach_ls(flush_to_recycle_clog_func))) {
      STORAGE_LOG(WARN, "for each ls functor failed", KR(ret));
    } else {
      STORAGE_LOG(INFO, "finish flush to recycle clog", K(flush_to_recycle_clog_func));
    }
  }
}

#ifdef OB_BUILD_SHARED_STORAGE
class UpdateSSCkptFunctorForMetaSvr : public ObIMetaFunction {
  OB_UNIS_VERSION(1);
public:
  UpdateSSCkptFunctorForMetaSvr(ObIMetaFunction::MetaFuncType func_type, const ObLSID ls_id)
      : ObIMetaFunction(func_type), ls_id_(ls_id) {}
  virtual int operator()(ObSSLSMeta &ss_ls_meta) override
  {
    int ret = OB_SUCCESS;

    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      STORAGE_LOG(WARN, "get log stream failed", K(ret), K(ls_id_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "log stream not exist", K(ret), K(ls_id_));
    } else {
      SCN ss_checkpoint_scn(ObScnRange::MAX_SCN);
      SCN sslog_max_committed_scn(ObScnRange::MAX_SCN);
      LSN ss_checkpoint_lsn;
      PalfBaseInfo palf_meta;

      // NOTICE!!! : must get ss_rec_scn before get max_committed_meta_scn
      ObSSCheckpointExecutor &ss_ckpt_executor = ls->get_ss_checkpoint_executor();
      if (OB_FAIL(ss_ckpt_executor.get_ss_rec_scn(ss_checkpoint_scn))) {
        STORAGE_LOG(WARN, "get ss rec scn failed", KR(ret), KPC(ls));
      } else if (OB_FAIL(ls->get_log_handler()->locate_by_scn_coarsely(ss_checkpoint_scn, ss_checkpoint_lsn))) {
        STORAGE_LOG(WARN, "locate ss_checkpoint_lsn failed", KR(ret), KPC(ls));
      } else if (OB_FAIL(MTL(ObSSMetaService *)->get_max_committed_meta_scn(ls_id_, sslog_max_committed_scn))) {
        STORAGE_LOG(WARN, "get max comiitted meta scn failed", KR(ret), KPC(ls));
      } else if (OB_FAIL(ls->get_palf_base_info(ss_checkpoint_lsn, palf_meta))) {
        STORAGE_LOG(WARN, "get palf base info failed", KR(ret), KPC(ls));
      } else if (OB_FAIL(ss_ls_meta.set_ss_ckpt_with_sslog_snapshot(
                     ss_checkpoint_scn, ss_checkpoint_lsn, sslog_max_committed_scn))) {
        STORAGE_LOG(WARN, "set ss checkpoint failed", KR(ret), KPC(ls));
      } else if (OB_FAIL(ss_ls_meta.set_palf_meta(palf_meta))) {
        STORAGE_LOG(WARN, "set palf meta failed", KR(ret), KPC(ls));
      } else {
        FLOG_INFO("finish update ss_checkpoint_scn",
                  K(ls_id_),
                  K(ss_checkpoint_scn),
                  K(sslog_max_committed_scn),
                  K(ss_checkpoint_lsn),
                  K(palf_meta));
      }

    }
    return ret;
  }

private:
  ObLSID ls_id_;
};

OB_SERIALIZE_MEMBER_INHERIT(UpdateSSCkptFunctorForMetaSvr, ObIMetaFunction, ls_id_)

struct UpdateSSCkptFunctorForLS {
public:
  int operator()(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    const ObLSID ls_id = ls.get_ls_id();
    ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
    UpdateSSCkptFunctorForMetaSvr func(ObIMetaFunction::MetaFuncType::UPDATE_SS_CKPT_FUNC, ls_id);

    ObSSWriterKey sswriter_key(ObSSWriterType::META, ls_id);
    bool is_sswriter = false;
    int64_t unused_epoch = 0;
    if (OB_ISNULL(meta_svr)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "mtl MetaService should not be null", K(ret), K(MTL_ID()), KP(meta_svr));
    } else if (OB_FAIL(MTL(ObSSWriterService *)->check_lease(sswriter_key, is_sswriter, unused_epoch))) {
      STORAGE_LOG(WARN, "check lease fail", KR(ret), K(sswriter_key));
    } else if (!is_sswriter) {
      if (REACH_TIME_INTERVAL(10LL * 1000LL * 1000LL)) {
        STORAGE_LOG(INFO, "this replica is not meta sswriter", K(is_sswriter));
      }
    } else if (OB_FAIL(meta_svr->update_ls_meta(ls_id, ObMetaUpdateReason::UPDATE_LS_SS_CHECKPOINT_SCN, func))) {
      STORAGE_LOG_RET(WARN, 0, "update ls meta failed", KR(ret), K(ls));
    } else {
      // finish update ls meta
    }

    // returen OB_SUCCESS to iterate all logstreams
    return OB_SUCCESS;
  }
};

void ObCheckPointService::ObSSUpdateCkptSCNTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "====== SS Update Checkpoint SCN Task ======");
  INIT_TRACE_GUARD;
  UpdateSSCkptFunctorForLS up_ckpt_func_for_ls;
  if (OB_FAIL(MTL(ObLSService *)->foreach_ls(up_ckpt_func_for_ls))) {
    STORAGE_LOG(WARN, "for each ls functor failed", KR(ret));
  }
}

struct UpdateSSLSNFunctorForLS {
public:
  // TODO : @gengli.wzy
  // When there are multiple regions, collect the checkpoint LSN values from each and use min{ checkpoint_lsn }
  int operator()(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    ObSSLSMeta ss_ls_meta;
    if (OB_FAIL(MTL(ObSSMetaService *)->get_ls_meta(ls.get_ls_id(), ss_ls_meta))) {
      STORAGE_LOG(ERROR, "get ls meta failed", KR(ret), K(ss_ls_meta));
    } else {
      palf::LSN checkpoint_lsn = ss_ls_meta.get_ss_base_lsn();
      if (OB_FAIL(ls.get_log_handler()->advance_base_lsn(checkpoint_lsn))) {
        STORAGE_LOG(WARN, "advance base lsn failed", K(ret), K(checkpoint_lsn));
      } else {
        FLOG_INFO("[CHECKPOINT] advance palf base lsn successfully", K(checkpoint_lsn), K(ls.get_ls_id()));
      }
    }

    // returen OB_SUCCESS to iterate all logstreams
    return OB_SUCCESS;
  }
};

void ObCheckPointService::ObSSUpdateCkptLSNTask::runTimerTask()
{
  if (!GCONF.enable_logservice) {
    // skip if logservice is not enabled. No need to update checkpoint lsn
    return;
  }
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "====== SS Update LogService LSN Task ======");
  INIT_TRACE_GUARD;
  UpdateSSLSNFunctorForLS up_lsn_func_for_ls;
  if (OB_FAIL(MTL(ObLSService *)->foreach_ls(up_lsn_func_for_ls))) {
    STORAGE_LOG(WARN, "for each ls functor failed", KR(ret));
  }
}

struct SSAdvanceCkptFunctorForLS {
public:
  int operator()(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ls.advance_checkpoint_by_flush(
            SCN::max_scn(), INT64_MAX /*timeout*/, false /*is_tenant_freeze*/, ObFreezeSourceFlag::CLOG_CHECKPOINT))) {
      STORAGE_LOG(WARN, "flush ls to recycle clog failed", KR(ret), K(ls));
    }

    // returen OB_SUCCESS to iterate all logstreams
    return OB_SUCCESS;
  }
};

void ObCheckPointService::ObSSAdvanceCkptTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "====== SS Advance Checkpoint Task ======");
  INIT_TRACE_GUARD;
  SSAdvanceCkptFunctorForLS advance_ckpt_func_for_ls;
  if (OB_FAIL(MTL(ObLSService *)->foreach_ls(advance_ckpt_func_for_ls))) {
    STORAGE_LOG(WARN, "for each ls functor failed", KR(ret));
  }
}

struct ScheduleUploadFunctorForLS {
public:
  int operator()(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ls.get_inc_sstable_upload_handler().try_schedule_upload_task())) {
      STORAGE_LOG(WARN, "process ls fail", K(ret), K(ls.get_ls_id()));
    }

    // returen OB_SUCCESS to iterate all logstreams
    return OB_SUCCESS;
  }
};

void ObCheckPointService::ObSSScheduleIncUploadTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ScheduleUploadFunctorForLS schedule_upload_func;
  if (OB_FAIL(MTL(ObLSService *)->foreach_ls(schedule_upload_func))) {
    STORAGE_LOG(WARN, "for each ls functor failed", KR(ret));
  }
}

#endif

} // checkpoint
} // storage
} // oceanbase
