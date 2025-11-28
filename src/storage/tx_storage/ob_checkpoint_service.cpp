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

#include "rootserver/ob_ls_service_helper.h" // ObLSServiceHelper
#include "rootserver/ob_tenant_info_loader.h" // ObTenantInfoLoader
#include "storage/tx_storage/ob_checkpoint_service.h"
#include "logservice/ob_log_service.h"
#include "logservice/archiveservice/ob_archive_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "observer/ob_srv_network_frame.h"
#include "storage/incremental/share/ob_shared_ls_meta.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ob_ss_checkpoint_rpc.h"
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

// Check if need flush all CLOG module each 1 minute
int64_t ObCheckPointService::SS_TRY_ADVANCE_CKPT_INTERVAL = 60LL * 1000LL * 1000LL;

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
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(ss_sche_upload_thread_pool_.init())) {
    LOG_WARN("fail to init ss sche upload thread pool", KR(ret));
#endif
  } else {
    is_inited_ = true;
#ifdef OB_BUILD_SHARED_STORAGE
    prev_ss_advance_ckpt_task_ts_ = 0;
#endif
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
    if (OB_FAIL(ss_sche_upload_thread_pool_.start())) {
      LOG_WARN("fail to start ss sche upload thread pool", KR(ret));
    } else {
      INIT_AND_START_TIMER_TASK(ss_update_ckpt_scn_timer_,
                                ss_update_ckpt_scn_task_,
                                "SSUpCkptSCN",
                                "SSUpSCNTimer",
                                SS_UPDATE_CKPT_INTERVAL,
                                true /* repeat */);
      INIT_AND_START_TIMER_TASK(ss_update_ckpt_lsn_timer_,
                                ss_update_ckpt_lsn_task_,
                                "SSUpCkptLSN",
                                "SSUpLSNTimer",
                                SS_UPDATE_CKPT_INTERVAL,
                                true /* repeat */);
      INIT_AND_START_TIMER_TASK(ss_advance_ckpt_timer_,
                                ss_advance_ckpt_task_,
                                "SSAdvanceCKPT",
                                "SSAdvanceTimer",
                                SS_TRY_ADVANCE_CKPT_INTERVAL,
                                true /* repeat */);
      INIT_AND_START_TIMER_TASK(ss_schedule_upload_timer_,
                                ss_schedule_upload_task_,
                                "SSUploader",
                                "SSUploaderTimer",
                                SS_TRY_SCHEDULE_UPLOAD_INTERVAL,
                                true /* repeat */);
    }
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

    // stop ss_schedule_upload_timer_ before ss_sche_upload_thread_pool_
    ss_schedule_upload_timer_.stop();
    ss_sche_upload_thread_pool_.stop();
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

    // wait ss_schedule_upload_timer_ before ss_sche_upload_thread_pool_
    ss_schedule_upload_timer_.wait();
    ss_sche_upload_thread_pool_.wait();
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

    // destroy ss_schedule_upload_timer_ before ss_sche_upload_thread_pool_
    ss_schedule_upload_timer_.destroy();
    ss_sche_upload_thread_pool_.destroy();
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

    if (ObReplicaTypeCheck::is_log_replica(ls.get_replica_type())) {
      rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
      share::SCN checkpoint_scn;
      palf::LSN checkpoint_lsn;
      // When clog recycle mode is strict (which is default value), we set clog recycle
      // point at pure_readable_scn. But pure_readable_scn can not promoted when
      // majority of F-replicas are down, thus making clog can not be recycled
      // In this case, we can set clog strict recycle mode to false.
      // When clog recycle mode is not strict, we set clog recycle point at sync_scn
      // sync_scn can promoted when majority of F-replicas are down
      bool is_strict_clog_recycle_mode = ObServerConfig::get_instance()._ob_enable_log_replica_strict_recycle_mode;
      if (OB_ISNULL(tenant_info_loader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_info_loader is null", KR(ret), KP(tenant_info_loader));
      } else if (!is_strict_clog_recycle_mode) {
        // not strict recycle mode, recycle clog based on sync_scn
        if (OB_FAIL(tenant_info_loader->get_sync_scn(checkpoint_scn))) {
          LOG_WARN("fail to get tenant sync scn", KR(ret));
        }
      } else {
        // is strict recycle mode, recycle clog based on pure_readable_scn
        if (OB_FAIL(tenant_info_loader->get_pure_readable_scn(checkpoint_scn))) {
          LOG_WARN("fail to get tenant readable scn", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ls.get_log_handler()->locate_by_scn_coarsely(checkpoint_scn, checkpoint_lsn))) {
        LOG_WARN("fail to convert scn to lsn", KR(ret), K(checkpoint_scn));
      } else if (OB_FAIL(ls.set_clog_checkpoint(checkpoint_lsn, checkpoint_scn, true/*write_slog*/))) {
        LOG_WARN("fail to set clog checkpoint", KR(ret), K(checkpoint_lsn), K(checkpoint_scn));
      } else if (OB_FAIL(ls.get_log_handler()->advance_base_lsn(checkpoint_lsn))) {
        LOG_WARN("fail to advance base lsn", KR(ret), K(checkpoint_lsn));
      } else {
        succ_ls_cnt_++;
      }
      FLOG_INFO("[CHECKPOINT] log replica update checkpoint", KR(ret), "tenant_id", MTL_ID(), K(ls.get_ls_id()),
                K(checkpoint_lsn), K(checkpoint_scn), K(is_strict_clog_recycle_mode));
    } else {
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

    if (ObReplicaTypeCheck::is_log_replica(ls.get_replica_type())) {
        // do nothing
    } else if (OB_ISNULL(checkpoint_executor = ls.get_checkpoint_executor())) {
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
    if (ObReplicaTypeCheck::is_log_replica(ls.get_replica_type())) {
      // do nothing
      ++succ_ls_cnt_;
    } else if (OB_ISNULL(checkpoint_executor) || OB_ISNULL(data_checkpoint)) {
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

void ScheLSUploadTask::run()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_handle_.get_ls()->get_inc_sstable_uploader().try_schedule_upload_task())) {
    STORAGE_LOG(WARN, "process ls fail", K(ret), K(ls_handle_.get_ls()->get_ls_id()));
  }
}

int ObSSScheUploadThreadPool::push_task(ScheLSUploadTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
    LOG_WARN("fail to push task", K(ret));
  }
  return ret;
}

void ObSSScheUploadThreadPool::handle(void *task)
{
  STORAGE_LOG(DEBUG, "====== ss sche upload thread pool handle ======");
  if (OB_ISNULL(task)) {
    STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "task is null", KP(task));
  } else {
    ScheLSUploadTask *sche_ls_upload_task = static_cast<ScheLSUploadTask *>(task);
    (void)sche_ls_upload_task->run();
    MTL_DELETE(ScheLSUploadTask, "ScheLSUploadTask", sche_ls_upload_task);
  }
}

int ObSSScheUploadThreadPool::init()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "====== ss sche upload thread pool init ======");
  if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSScheduleUpload, tg_id_))) {
    LOG_WARN("create tg failed", K(ret));
  }
  return ret;
}

int ObSSScheUploadThreadPool::start()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "====== ss sche upload thread pool start ======");
  if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("fail to set handler and start", K(ret));
  }
  return ret;
}

void ObSSScheUploadThreadPool::stop()
{
  STORAGE_LOG(INFO, "====== ss sche upload thread pool stop ======");
  TG_STOP(tg_id_);
}

void ObSSScheUploadThreadPool::wait()
{
  STORAGE_LOG(INFO, "====== ss sche upload thread pool wait ======");
  TG_WAIT(tg_id_);
}


void ObSSScheUploadThreadPool::destroy()
{
  STORAGE_LOG(INFO, "====== ss sche upload thread pool destroy ======");
  TG_DESTROY(tg_id_);
}

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
    } else if (ObReplicaTypeCheck::is_log_replica(ls->get_replica_type())) {
      // do nothing
    } else {
      SCN prev_ss_checkpoint_scn(ObScnRange::MAX_SCN);
      LSN prev_ss_checkpoint_lsn;
      LSN sn_checkpoint_lsn;

      ObSSCheckpointExecutor &ss_ckpt_executor = ls->get_ss_checkpoint_executor();
      sn_checkpoint_lsn = ls->get_clog_base_lsn();
      if (OB_FAIL(MTL(ObSSMetaService *)->get_ls_meta(ls_id_, ss_ls_meta))) {
        STORAGE_LOG(WARN, "get ls meta failed", KR(ret), K(ss_ls_meta));
      } else if (FALSE_IT(prev_ss_checkpoint_scn = ss_ls_meta.get_ss_checkpoint_scn())) {
      } else if (FALSE_IT(prev_ss_checkpoint_lsn = ss_ls_meta.get_ss_base_lsn())) {
      } else if (sn_checkpoint_lsn < prev_ss_checkpoint_lsn) {
        FLOG_INFO("SSWriter changed. Current Replica is not catching up the latest clog.",
                  K(sn_checkpoint_lsn),
                  K(prev_ss_checkpoint_lsn));
      } else {
        SCN ss_checkpoint_scn(ObScnRange::MAX_SCN);
        LSN ss_checkpoint_lsn;
        PalfBaseInfo palf_meta;
        SCN transfer_scn;
        ObLSTransferMetaInfo transfer_meta_info;
        if (OB_FAIL(ss_ckpt_executor.get_ss_rec_scn(ss_checkpoint_scn))) {
          STORAGE_LOG(WARN, "get ss rec scn failed", KR(ret), KPC(ls));
        } else if (prev_ss_checkpoint_scn >= ss_checkpoint_scn) {
          FLOG_INFO("finish update ss_checkpoint_scn: no need update because pre_ss_checkpoint_scn is larger or equal",
                    K(ls_id_),
                    K(prev_ss_checkpoint_scn),
                    K(ss_checkpoint_scn));
          ret = OB_SUCCESS;
        } else if (OB_FAIL(ls->get_log_handler()->locate_by_scn_coarsely(ss_checkpoint_scn, ss_checkpoint_lsn))) {
          STORAGE_LOG(WARN, "locate ss_checkpoint_lsn failed", KR(ret), KPC(ls));
        } else if (OB_FAIL(ls->get_palf_base_info(ss_checkpoint_lsn, palf_meta))) {
          STORAGE_LOG(WARN, "get palf base info failed", KR(ret), KPC(ls));
        } else if (OB_FAIL(ls->get_transfer_meta(transfer_scn, transfer_meta_info))) {
          STORAGE_LOG(WARN, "get transfer meta failed", KR(ret), KPC(ls));
        } else if (OB_FAIL(ss_ls_meta.set_ss_checkpoint(ss_checkpoint_scn, ss_checkpoint_lsn))) {
          STORAGE_LOG(WARN, "set ss checkpoint failed", KR(ret), KPC(ls));
        } else if (OB_FAIL(ss_ls_meta.set_palf_meta(palf_meta))) {
          STORAGE_LOG(WARN, "set palf meta failed", KR(ret), KPC(ls));
        } else if (OB_FAIL(ss_ls_meta.set_transfer_meta(transfer_scn, transfer_meta_info))) {
          STORAGE_LOG(WARN, "set transfer meta failed", KR(ret), KPC(ls));
        } else {
          FLOG_INFO("finish update ss_checkpoint_scn",
                    K(ls_id_),
                    K(ss_checkpoint_scn),
                    K(ss_checkpoint_lsn),
                    K(palf_meta),
                    K(transfer_scn),
                    K(transfer_meta_info));
        }
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
    ObSSLSMeta ss_ls_meta;
    LSN ss_checkpoint_lsn;
    LSN sn_checkpoint_lsn = ls.get_clog_base_lsn();

    if (OB_FAIL(MTL(ObSSMetaService *)->get_ls_meta(ls_id, ss_ls_meta))) {
      STORAGE_LOG(WARN, "get ls meta failed", KR(ret), K(ss_ls_meta));
    } else if (FALSE_IT(ss_checkpoint_lsn = ss_ls_meta.get_ss_base_lsn())) {
    } else if (FALSE_IT(throttle_flush_if_needed(
                   ss_ls_meta, sn_checkpoint_lsn, ss_checkpoint_lsn, ls))) {
    } else if (sn_checkpoint_lsn < ss_checkpoint_lsn) {
      FLOG_INFO("local ckpt less than ss ckpt", K(ls_id), K(sn_checkpoint_lsn), K(ss_checkpoint_lsn));
    } else {
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
    }
    // returen OB_SUCCESS to iterate all logstreams
    return OB_SUCCESS;
  }

private:
  bool is_leader_(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    // set default value as leader because leader do not skip throttle
    bool is_leader = true;
    ObRole role;
    int64_t proposal_id = 0;
    if (OB_FAIL(ls.get_log_handler()->get_role(role, proposal_id))) {
      LOG_WARN("get ls role failed", KR(ret), K(ls.get_ls_id()));
    } else if (common::is_strong_leader(role)) {
      is_leader = true;
    } else {
      is_leader = false;
    }
    return is_leader;
  }

#define PRINT_CKPT_INFO_WRAPPER                                                                   \
  "ls_id", ls.get_ls_id(), K(sn_ckpt_lsn), K(ss_ckpt_lsn), K(sn_ckpt_scn), K(ss_ckpt_scn),        \
      KTIME(sn_ckpt_scn.convert_to_ts()), KTIME(ss_ckpt_scn.convert_to_ts()), "SCN GAP(seconds)", \
      scn_ts_gap / 1000 / 1000, "MAX LSN GAP(MB)", max_lsn_gap / 1024 / 1024
  void throttle_flush_if_needed(const ObSSLSMeta &ss_ls_meta, LSN sn_ckpt_lsn, LSN ss_ckpt_lsn, ObLS &ls)
  {
    if (is_leader_(ls)) {
      bool ckpt_lag_too_large = false;
      // use 16GB as default lsn gap
      int64_t max_lsn_gap = 16LL * 1024LL * 1024LL * 1024LL;
      (void)MTL(ObTenantFreezer *)->get_tenant_memstore_limit(max_lsn_gap);
      const int64_t config_trigger = ObTenantFreezer::get_freeze_trigger_percentage();
      const int64_t freeze_trigger = config_trigger > 0 ? config_trigger : 20;
      // use 1/8 freeze_trigger as lsn_gap
      max_lsn_gap = max_lsn_gap * freeze_trigger / 100 / 8;

      // get advance_checkpoint interval
      int64_t advance_ckpt_interval = 10LL * 60LL * 1000LL * 1000LL; // default 10 minutes
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        advance_ckpt_interval = tenant_config->_ss_advance_checkpoint_interval;
      }

      const SCN sn_ckpt_scn = ls.get_clog_checkpoint_scn();
      const SCN ss_ckpt_scn = ss_ls_meta.get_ss_checkpoint_scn();
      const int64_t scn_ts_gap = (sn_ckpt_scn.convert_to_ts() - ss_ckpt_scn.convert_to_ts());

      if (!ls.get_inc_sstable_uploader().finish_reloading()) {
        (void)ls.disable_flush();
        FLOG_INFO("disable ls flush to wait uploader reloading", PRINT_CKPT_INFO_WRAPPER);
      } else if (ls.get_inc_sstable_uploader().task_overflowed()) {
        (void)ls.disable_flush();
        FLOG_INFO("disable ls flush to handle current tasks", PRINT_CKPT_INFO_WRAPPER);
      } else if ((sn_ckpt_lsn > ss_ckpt_lsn) && (sn_ckpt_lsn - ss_ckpt_lsn > max_lsn_gap) &&
                 (scn_ts_gap > 2 * advance_ckpt_interval)) {
        (void)ls.disable_flush();
        ckpt_lag_too_large = true;
        FLOG_INFO("disable ls flush to throttle writes and allow ss checkpoint to catch up with sn checkpoint",
                  PRINT_CKPT_INFO_WRAPPER);
      } else if (ls.flush_is_disabled()) {
        (void)ls.enable_flush();
        FLOG_INFO("enable ls flush", PRINT_CKPT_INFO_WRAPPER);
      }
    } else {
      // this ls replica is not leader
      if (ls.flush_is_disabled()) {
        (void)ls.enable_flush();
        FLOG_INFO("enable ls flush because this ls is not leader", K(ls.get_ls_id()));
      }
    }
  }
#undef  PRINT_CKPT_INFO_WRAPPER
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
    SCN min_checkpoint_scn_from_cn;
    const ObLSID ls_id = ls.get_ls_id();
    if (!is_ls_leader_(ls)) {
      STORAGE_LOG(INFO, "current replica is not ls leader, no need advance base lsn", K(ls_id));
    } else if (OB_FAIL(MTL(ObSSMetaService *)->get_ls_meta(ls_id, ss_ls_meta))) {
      STORAGE_LOG(WARN, "get ls meta failed", KR(ret), K(ls_id),K(ss_ls_meta));
    } else if (OB_FAIL(get_min_clog_checkpoint_scn_from_compute_node_(ls, min_checkpoint_scn_from_cn))) {
      STORAGE_LOG(WARN, "get min clog checkpoint scn from compute nodes failed", KR(ret), K(ls_id));
    } else {
      SCN delay_recycle_checkpoint_scn;
      LSN ss_clog_recycle_lsn;
      // Delay clog recycling to avoid rebuilding
      int64_t clog_recycle_delay_time_us = 10LL * 60LL * 1000LL * 1000LL; // default 10 minutes
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        clog_recycle_delay_time_us = tenant_config->_ss_clog_retention_period;
      }

      SCN base_scn = MIN(ss_ls_meta.get_ss_checkpoint_scn(), min_checkpoint_scn_from_cn);
      int64_t delay_recycle_ts_us = base_scn.convert_to_ts() - clog_recycle_delay_time_us;
      if (delay_recycle_ts_us < 0) {
        delay_recycle_ts_us = 0;
      }

      delay_recycle_checkpoint_scn.convert_from_ts(delay_recycle_ts_us);
      if (OB_FAIL(ls.get_log_handler()->locate_by_scn_coarsely(delay_recycle_checkpoint_scn, ss_clog_recycle_lsn))) {
        STORAGE_LOG(WARN, "locate lsn failed", K(ret), KTIME(delay_recycle_ts_us), K(ss_clog_recycle_lsn));
      } else if (OB_FAIL(ls.get_log_handler()->advance_base_lsn(ss_clog_recycle_lsn))) {
        STORAGE_LOG(WARN, "advance base lsn failed", K(ret), K(ss_clog_recycle_lsn));
      } else {
        FLOG_INFO("[CHECKPOINT] advance palf base lsn successfully",
                  K(base_scn),
                  KTIME(delay_recycle_ts_us),
                  K(delay_recycle_checkpoint_scn),
                  K(ss_clog_recycle_lsn),
                  K(ls_id));
      }
    }

    if (OB_FAIL(ret)) {
      FLOG_INFO("[CHECKPOINT] advance palf base lsn fail", KR(ret), K(ls_id));
    }

    // returen OB_SUCCESS to iterate all logstreams
    return OB_SUCCESS;
  }
private:
  bool is_ls_leader_(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    // set default value as leader because leader do not skip throttle
    bool is_leader = true;
    ObRole role;
    int64_t proposal_id = 0;
    if (OB_FAIL(ls.get_log_handler()->get_role(role, proposal_id))) {
      LOG_WARN("get ls role failed", KR(ret), K(ls.get_ls_id()));
    } else if (common::is_strong_leader(role)) {
      is_leader = true;
    } else {
      is_leader = false;
    }
    return is_leader;
  }


  int get_min_clog_checkpoint_scn_from_compute_node_(ObLS &ls, SCN &min_clog_checkpoint_scn)
  {
    int ret = OB_SUCCESS;
    min_clog_checkpoint_scn.set_max();
    const int64_t expire_renew_time = INT64_MAX;
    share::ObLSLocation location;
    bool is_cache_hit = false;
    const uint64_t tenant_id = MTL_ID();
    const ObLSID ls_id = ls.get_ls_id();

    obrpc::ObSSCkptRpcProxy ss_ckpt_rpc_proxy;
    ObSSRpcCollectCNClogCkptArg arg(tenant_id, ls_id);
    if (OB_FAIL(GCTX.location_service_->get(
            GCONF.cluster_id, tenant_id, ls_id, expire_renew_time, is_cache_hit, location))) {
      LOG_WARN("get ls location failed", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(GCTX.net_frame_->get_proxy(ss_ckpt_rpc_proxy))) {
      STORAGE_LOG(WARN, "get proxy failed", KR(ret), K(ls_id), K(arg));
    } else {
      const ObIArray<ObLSReplicaLocation> &ls_locations = location.get_replica_locations();
      for (int i = 0; i < ls_locations.count() && OB_SUCC(ret); ++i) {
        const ObAddr &server = ls_locations.at(i).get_server();
        ObSSRpcCollectCNClogCkptResult result;
        if (OB_FAIL(ss_ckpt_rpc_proxy.to(server).by(tenant_id).collect_cn_clog_ckpt(arg, result))) {
          STORAGE_LOG(WARN, "post rpc collect cn clog ckpt failed", KR(ret), K(ls_id), K(arg));
        } else if (OB_FAIL(result.ret_code_)) {
          STORAGE_LOG(WARN, "collect cn clog ckpt failed", KR(ret), K(ls_id), K(arg), K(result));
        } else {
          min_clog_checkpoint_scn = MIN(min_clog_checkpoint_scn, result.clog_checkpoint_scn_);
          FLOG_INFO("finish get clog_checkpoint_scn from one compute node",
                    K(ls_id),
                    K(server),
                    K(result),
                    K(min_clog_checkpoint_scn));
        }
      }
    }

    return ret;
  }

private:
  obrpc::ObSSCkptRpcProxy ss_ckpt_rpc_proxy_;
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
    if (ls.flush_is_disabled()) {
      FLOG_INFO("memtable flush is disabled, skip advance checkpoint once", K(ls.get_ls_id()));
    } else if (OB_FAIL(ls.advance_checkpoint_by_flush(
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
  INIT_TRACE_GUARD;

  // set 10 minutes as defualt value
  int64_t advance_checkpoint_interval = 10LL * 60LL * 1000LL * 1000LL;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    // use config value if config is valid
    advance_checkpoint_interval = tenant_config->_ss_advance_checkpoint_interval;
  }

  const int64_t current_ts = ObClockGenerator::getClock();
  if (current_ts - MTL(ObCheckPointService *)->prev_ss_advance_ckpt_task_ts() > advance_checkpoint_interval) {
    STORAGE_LOG(INFO, "====== SS Advance Checkpoint Task ======");
    SSAdvanceCkptFunctorForLS advance_ckpt_func_for_ls;
    if (OB_FAIL(MTL(ObLSService *)->foreach_ls(advance_ckpt_func_for_ls))) {
      STORAGE_LOG(WARN, "for each ls functor failed", KR(ret));
    } else {
      MTL(ObCheckPointService *)->set_prev_ss_advance_ckpt_task_ts(current_ts);
    }
  }
}

struct ScheduleParallelUploadFunctor {
public:
  ScheduleParallelUploadFunctor(ObCheckPointService &host_ckpt_sv) : host_ckpt_sv_(host_ckpt_sv) {}
  int operator()(ObLS &ls)
  {
    int ret = OB_SUCCESS;
    void *task_buffer = nullptr;
    if (ls.get_inc_sstable_uploader().is_processing()) {
      STORAGE_LOG(INFO, "skip push new upload task because uploader is processing", K(ls.get_ls_id()));
    } else if (OB_ISNULL(task_buffer = mtl_malloc(sizeof(ScheLSUploadTask), "ScheLSUpload"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate sche_ls_upload_task failed", K(ret), K(ls.get_ls_id()));
    } else {
      ScheLSUploadTask *sche_ls_upload_task = new (task_buffer) ScheLSUploadTask();
      // hold ls handle to avoid ls being destroyed
      if (OB_FAIL(MTL(ObLSService *)->get_ls(ls.get_ls_id(), sche_ls_upload_task->ls_handle(), ObLSGetMod::STORAGE_MOD))) {
        STORAGE_LOG(WARN, "get ls failed", K(ret), K(ls.get_ls_id()));
      } else if (OB_FAIL(host_ckpt_sv_.ss_sche_upload_thread_pool().push_task(sche_ls_upload_task))) {
        STORAGE_LOG(WARN, "push sche_ls_upload_task failed", K(ret), K(ls.get_ls_id()));
      }

      if (OB_FAIL(ret)) {
        mtl_free(sche_ls_upload_task);
        sche_ls_upload_task = nullptr;
      }
    }

    // returen OB_SUCCESS to iterate all logstreams
    return OB_SUCCESS;
  }

public:
  ObCheckPointService &host_ckpt_sv_;
};

void ObCheckPointService::ObSSScheduleIncUploadTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ScheduleParallelUploadFunctor schedule_upload_func(host_ckpt_sv_);
  if (OB_FAIL(MTL(ObLSService *)->foreach_ls(schedule_upload_func))) {
    STORAGE_LOG(WARN, "for each ls functor failed", KR(ret));
  }
}

#endif

} // checkpoint
} // storage
} // oceanbase
