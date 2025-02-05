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

#include "storage/backup/ob_backup_task.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/backup/ob_backup_operator.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "observer/omt/ob_tenant.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/backup/ob_backup_meta_cache.h"

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::archive;

namespace oceanbase {
namespace backup {

#ifndef REPORT_TASK_RESULT
#define REPORT_TASK_RESULT(dag_id, result)\
    if (OB_SUCCESS != (tmp_ret = ObBackupUtils::report_task_result(param_.job_desc_.job_id_, \
                              param_.job_desc_.task_id_, \
                              param_.tenant_id_, \
                              param_.ls_id_, \
                              param_.turn_id_, \
                              param_.retry_id_, \
                              param_.job_desc_.trace_id_, \
                              (dag_id), \
                              (result), \
                              report_ctx_))) { \
      LOG_WARN("failed to report task result", K(tmp_ret)); \
    }
#endif
ERRSIM_POINT_DEF(EN_LS_BACKUP_FAILED);
ERRSIM_POINT_DEF(EN_BACKUP_DATA_TASK_FAILED);

static int get_ls_handle(const uint64_t tenant_id, const share::ObLSID &ls_id, storage::ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ls_handle.reset();
  ObLSService *ls_service = NULL;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream service is NULL", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  }
  return ret;
}

static int advance_checkpoint_by_flush(const uint64_t tenant_id, const share::ObLSID &ls_id,
  const SCN &start_scn, storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  static const int64_t CHECK_TIME_INTERVAL = 1_s;
  static const int64_t MAX_ADVANCE_TIME_INTERVAL = 60_s;
  const int64_t advance_checkpoint_timeout = GCONF._advance_checkpoint_timeout;
  LOG_INFO("backup advance checkpoint timeout", K(tenant_id), K(advance_checkpoint_timeout));
  if (start_scn < SCN::min_scn()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(start_scn));
  } else {
    ObLSMeta ls_meta;
    int64_t i = 0;
    const bool check_archive = false;
    const int64_t start_ts = ObTimeUtility::current_time();
    int64_t last_advance_checkpoint_ts = ObTimeUtility::current_time();
    do {
      const int64_t cur_ts = ObTimeUtility::current_time();
      const int64_t advance_checkpoint_interval = MIN(std::pow(2, (2 * i + 1)) * 1000 * 1000, MAX_ADVANCE_TIME_INTERVAL);
      const bool need_advance_checkpoint = (0 == i) || (cur_ts - last_advance_checkpoint_ts >= advance_checkpoint_interval);
      if (cur_ts - start_ts > advance_checkpoint_timeout) {
        ret = OB_BACKUP_ADVANCE_CHECKPOINT_TIMEOUT;
        LOG_WARN("backup advance checkpoint by flush timeout", K(ret), K(tenant_id), K(ls_id), K(start_scn));
      } else if (need_advance_checkpoint) {
        if (OB_FAIL(ls->advance_checkpoint_by_flush(start_scn,
                                                    INT64_MAX, /*timeout*/
                                                    false,     /*is_tenant_freeze*/
                                                    ObFreezeSourceFlag::BACKUP))) {
          if (OB_NO_NEED_UPDATE == ret) {
            // clog checkpoint ts has passed start log ts
            ret = OB_SUCCESS;
            break;
          } else if (OB_EAGAIN == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to advance checkpoint by flush", K(ret), K(tenant_id), K(ls_id));
          }
        } else {
          last_advance_checkpoint_ts = ObTimeUtility::current_time();
          i++;
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(ls->get_ls_meta(ls_meta))) {
        LOG_WARN("failed to get ls meta", K(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(ls_meta.check_valid_for_backup())) {
        LOG_WARN("failed to check valid for backup", K(ret), K(ls_meta));
      } else {
        const SCN clog_checkpoint_scn = ls_meta.get_clog_checkpoint_scn();
        if (clog_checkpoint_scn >= start_scn) {
          LOG_INFO("clog checkpoint scn has passed start scn",
              K(i),
              K(tenant_id),
              K(ls_id),
              K(clog_checkpoint_scn),
              K(start_scn));
          break;
        } else {
          LOG_WARN("clog checkpoint scn has not passed start scn, retry again",
              K(i),
              K(tenant_id),
              K(ls_id),
              K(clog_checkpoint_scn),
              K(start_scn),
              K(need_advance_checkpoint),
              K(advance_checkpoint_interval),
              K(cur_ts),
              K(last_advance_checkpoint_ts));
        }
        ob_usleep(CHECK_TIME_INTERVAL);
        if (OB_FAIL(share::dag_yield())) {
          LOG_WARN("fail to yield dag", KR(ret));
        }
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

/* ObLSBackupDagNetInitParam */

ObLSBackupDagNetInitParam::ObLSBackupDagNetInitParam()
    : job_desc_(),
      backup_dest_(),
      tenant_id_(OB_INVALID_ID),
      dest_id_(0),
      backup_set_desc_(),
      ls_id_(-1),
      turn_id_(-1),
      retry_id_(-1),
      report_ctx_(),
      start_scn_(),
      backup_data_type_(),
      compl_start_scn_(),
      compl_end_scn_(),
      is_only_calc_stat_(false)
{}

ObLSBackupDagNetInitParam::~ObLSBackupDagNetInitParam()
{}

bool ObLSBackupDagNetInitParam::is_valid() const
{
  return job_desc_.is_valid() && backup_dest_.is_valid() && OB_INVALID_ID != tenant_id_ && dest_id_ > 0 &&
         backup_set_desc_.is_valid() && ls_id_.is_valid() && turn_id_ > 0 && retry_id_ >= 0 && report_ctx_.is_valid();
}

int ObLSBackupDagNetInitParam::assign(const ObLSBackupDagNetInitParam &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(other));
  } else if (OB_FAIL(backup_dest_.deep_copy(other.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret));
  } else {
    job_desc_ = other.job_desc_;
    tenant_id_ = other.tenant_id_;
    dest_id_ = other.dest_id_;
    backup_set_desc_ = other.backup_set_desc_;
    ls_id_ = other.ls_id_;
    turn_id_ = other.turn_id_;
    retry_id_ = other.retry_id_;
    report_ctx_ = other.report_ctx_;
    start_scn_ = other.start_scn_;
    backup_data_type_ = other.backup_data_type_;
    compl_start_scn_ = other.compl_start_scn_;
    compl_end_scn_ = other.compl_end_scn_;
    is_only_calc_stat_ = other.is_only_calc_stat_;
  }
  return ret;
}

int ObLSBackupDagNetInitParam::convert_to(ObLSBackupParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(*this));
  } else if (OB_FAIL(param.backup_dest_.deep_copy(backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K_(backup_dest));
  } else {
    param.job_id_ = job_desc_.job_id_;
    param.task_id_ = job_desc_.task_id_;
    param.tenant_id_ = tenant_id_;
    param.backup_set_desc_ = backup_set_desc_;
    param.ls_id_ = ls_id_;
    param.turn_id_ = turn_id_;
    param.retry_id_ = retry_id_;
    param.dest_id_ = dest_id_;
  }
  return ret;
}

int ObLSBackupDagNetInitParam::convert_to(ObLSBackupDagInitParam &init_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(*this));
  } else if (OB_FAIL(init_param.backup_dest_.deep_copy(backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K_(backup_dest));
  } else {
    init_param.job_desc_ = job_desc_;
    init_param.tenant_id_ = tenant_id_;
    init_param.backup_set_desc_ = backup_set_desc_;
    init_param.ls_id_ = ls_id_;
    init_param.turn_id_ = turn_id_;
    init_param.retry_id_ = retry_id_;
    init_param.dest_id_ = dest_id_;
  }
  return ret;
}

bool ObLSBackupDagNetInitParam::operator==(const ObLSBackupDagNetInitParam &other) const
{
  return job_desc_ == other.job_desc_ && backup_dest_ == other.backup_dest_ && tenant_id_ == other.tenant_id_
      && backup_set_desc_ == other.backup_set_desc_ && ls_id_ == other.ls_id_
      && turn_id_ == other.turn_id_ && retry_id_ == other.retry_id_ && dest_id_ == other.dest_id_
      && is_only_calc_stat_ == other.is_only_calc_stat_;
}

/* ObLSBackupDagInitParam */

ObLSBackupDagInitParam::ObLSBackupDagInitParam()
    : job_desc_(),
      backup_dest_(),
      tenant_id_(OB_INVALID_ID),
      backup_set_desc_(),
      ls_id_(),
      turn_id_(),
      retry_id_(),
      backup_stage_(LOG_STREAM_BACKUP_MAX),
      dest_id_(0)
{}

ObLSBackupDagInitParam::~ObLSBackupDagInitParam()
{}

bool ObLSBackupDagInitParam::is_valid() const
{
  return job_desc_.is_valid() && backup_dest_.is_valid() && OB_INVALID_ID != tenant_id_ && backup_set_desc_.is_valid() && dest_id_ > 0;
}

bool ObLSBackupDagInitParam::operator==(const ObLSBackupDagInitParam &other) const
{
  return job_desc_ == other.job_desc_ && backup_dest_ == other.backup_dest_ && tenant_id_ == other.tenant_id_ &&
         backup_set_desc_ == other.backup_set_desc_ && ls_id_ == other.ls_id_ && turn_id_ == other.turn_id_ &&
         retry_id_ == other.retry_id_ && backup_stage_ == other.backup_stage_ && dest_id_ == other.dest_id_;
}

int ObLSBackupDagInitParam::convert_to(const share::ObBackupDataType &backup_data_type, ObLSBackupDataParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(*this));
  } else if (OB_FAIL(param.backup_dest_.deep_copy(backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K_(backup_dest));
  } else {
    param.job_desc_ = job_desc_;
    param.tenant_id_ = tenant_id_;
    param.backup_set_desc_ = backup_set_desc_;
    param.ls_id_ = ls_id_;
    param.backup_data_type_ = backup_data_type;
    param.turn_id_ = turn_id_;
    param.retry_id_ = retry_id_;
    param.dest_id_ = dest_id_;
  }
  return ret;
}

/* ObBackupDagNet */

ObBackupDagNet::ObBackupDagNet(const ObBackupDagNetSubType &sub_type)
  : ObIDagNet(share::ObDagNetType::DAG_NET_TYPE_BACKUP),
    sub_type_(sub_type)
{
}

ObBackupDagNet::~ObBackupDagNet()
{
}

/* ObLSBackupMetaDagNet */

ObLSBackupMetaDagNet::ObLSBackupMetaDagNet()
  : ObLSBackupDataDagNet()
{
  sub_type_ = ObBackupDagNetSubType::LOG_STREAM_BACKUP_META_DAG_NET;
}

ObLSBackupMetaDagNet::~ObLSBackupMetaDagNet()
{}

int ObLSBackupMetaDagNet::start_running()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSBackupDagInitParam init_param;
  ObLSBackupMetaDag *backup_meta_dag = NULL;
  ObLSBackupPrepareDag *prepare_dag = NULL;
  ObLSBackupFinishDag *finish_dag = NULL;
  ObTenantDagScheduler *dag_scheduler = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net not init", K(ret));
  } else if (OB_FAIL(guard.switch_to(param_.tenant_id_))) {
    LOG_WARN("failed to switch to tenant", K(ret), K_(param));
  } else if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler must not be NULL", K(ret));
  } else if (OB_FAIL(param_.convert_to(init_param))) {
    LOG_WARN("failed to convert to init param", K(ret));
  } else if (OB_FALSE_IT(init_param.backup_stage_ = start_stage_)) {
  } else if (OB_FAIL(inner_init_before_run_())) {
    LOG_WARN("failed to inner init before run", K(ret));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(backup_meta_dag))) {
    LOG_WARN("failed to alloc backup meta dag", K(ret));
  } else if (OB_FAIL(backup_meta_dag->init(param_.start_scn_, init_param, report_ctx_, ls_backup_ctx_))) {
    LOG_WARN("failed to init backup meta dag", K(ret), K_(param));
  } else if (OB_FAIL(backup_meta_dag->create_first_task())) {
    LOG_WARN("failed to create first task for child dag", K(ret), KPC(backup_meta_dag));
  } else if (OB_FAIL(add_dag_into_dag_net(*backup_meta_dag))) {
    LOG_WARN("failed to add dag into dag net", K(ret), KPC(backup_meta_dag));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(prepare_dag))) {
    LOG_WARN("failed to alloc dag", K(ret));
  } else if (OB_FAIL(prepare_dag->init(init_param,
                                       backup_data_type_,
                                       report_ctx_,
                                       ls_backup_ctx_,
                                       *provider_,
                                       task_mgr_,
                                       *index_kv_cache_))) {
    LOG_WARN("failed to init backup dag", K(ret), K(init_param));
  } else if (OB_FAIL(prepare_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(backup_meta_dag->add_child(*prepare_dag))) {
    LOG_WARN("failed to add dag into dag_net", K(ret), KPC(prepare_dag));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(finish_dag))) {
    LOG_WARN("failed to create dag", K(ret));
  } else if (OB_FAIL(finish_dag->init(init_param, report_ctx_, ls_backup_ctx_, *index_kv_cache_))) {
    LOG_WARN("failed to init finish dag", K(ret), K(init_param));
  } else if (OB_FAIL(finish_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(prepare_dag->add_child(*finish_dag))) {
    LOG_WARN("failed to add child", K(ret), KPC(prepare_dag), KPC(finish_dag));
  } else {
#ifdef ERRSIM
    ret = OB_E(EventTable::EN_ADD_BACKUP_META_DAG_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("backup_errsim", "add_backup_meta_dag_failed");
    }
#endif
    if (FAILEDx(dag_scheduler->add_dag(finish_dag))) {
      LOG_WARN("failed to add dag", K(ret), KP(finish_dag));
    } else if (OB_FAIL(dag_scheduler->add_dag(prepare_dag))) {
      LOG_WARN("failed to add dag", K(ret), KP(prepare_dag));
      if (OB_TMP_FAIL(dag_scheduler->cancel_dag(finish_dag))) {
        LOG_ERROR("failed to cancel backup dag", K(tmp_ret), KP(dag_scheduler), KP(finish_dag));
      } else {
        finish_dag = nullptr;
      }
    } else if (OB_FAIL(dag_scheduler->add_dag(backup_meta_dag))) {
      LOG_WARN("failed to add dag", K(ret), KP(prepare_dag));
      if (OB_TMP_FAIL(dag_scheduler->cancel_dag(finish_dag))) {
        LOG_ERROR("failed to cancel backup dag", K(tmp_ret), KP(dag_scheduler), KP(finish_dag));
      } else {
        finish_dag = nullptr;
      }
      if (OB_TMP_FAIL(dag_scheduler->cancel_dag(prepare_dag))) {
        LOG_ERROR("failed to cancel backup dag", K(tmp_ret), KP(dag_scheduler), KP(prepare_dag));
      } else {
        prepare_dag = nullptr;
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(backup_meta_dag)) {
    dag_scheduler->free_dag(*backup_meta_dag);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(prepare_dag)) {
    dag_scheduler->free_dag(*prepare_dag);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(finish_dag)) {
    dag_scheduler->free_dag(*finish_dag);
  }

  if (OB_FAIL(ret)) {
    REPORT_TASK_RESULT(this->get_dag_id(), ret);
  }
  return ret;
}

int ObLSBackupDagInitParam::assign(const ObLSBackupDagInitParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_dest_.deep_copy(param.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret));
  } else {
    job_desc_ = param.job_desc_;
    tenant_id_ = param.tenant_id_;
    backup_set_desc_ = param.backup_set_desc_;
    ls_id_ = param.ls_id_;
    turn_id_ = param.turn_id_;
    retry_id_ = param.retry_id_;
    backup_stage_ = param.backup_stage_;
    dest_id_ = param.dest_id_;
  }
  return ret;
}

/* ObLSBackupDagNet */

bool ObLSBackupMetaDagNet::operator==(const share::ObIDagNet &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObBackupDagNet &backup_dag_net = static_cast<const ObBackupDagNet &>(other);
    if (backup_dag_net.get_sub_type() != get_sub_type()) {
      bret = false;
    } else {
      const ObLSBackupMetaDagNet &dag = static_cast<const ObLSBackupMetaDagNet &>(other);
      bret = dag.param_ == param_;
    }
  }
  return bret;
}

int ObLSBackupMetaDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 "[BACKUP_META_DAG_NET]: tenant_id=%lu, backup_set_id=%ld, ls_id=%ld",
                 param_.tenant_id_,
                 param_.backup_set_desc_.backup_set_id_,
                 param_.ls_id_.id()))) {
    LOG_WARN("failed to fill comment", K(ret), K(param_));
  };
  return ret;
}

int ObLSBackupMetaDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf,
          buf_len,
          "tenant_id=%lu, backup_set_id=%ld, ls_id=%ld",
          param_.tenant_id_,
          param_.backup_set_desc_.backup_set_id_,
          param_.ls_id_.id()))) {
    LOG_WARN("failed to fill dag net key", K(ret), K(param_));
  };
  return ret;
}

/* ObLSBackupDataDagNet */

ObLSBackupDataDagNet::ObLSBackupDataDagNet()
    : ObBackupDagNet(ObBackupDagNetSubType::LOG_STREAM_BACKUP_DAG_DAG_NET),
      is_inited_(false),
      start_stage_(),
      param_(),
      ls_backup_ctx_(),
      provider_(NULL),
      task_mgr_(),
      index_kv_cache_(NULL),
      report_ctx_()
{}

ObLSBackupDataDagNet::~ObLSBackupDataDagNet()
{
  if (OB_NOT_NULL(provider_)) {
    ObLSBackupFactory::free(provider_);
  }
}

int ObLSBackupDataDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dag net init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid() || !OB_BACKUP_INDEX_CACHE.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(param));
  } else if (OB_FAIL(param_.assign(*(static_cast<const ObLSBackupDagNetInitParam *>(param))))) {
    LOG_WARN("failed to assign param", K(ret));
  } else if (OB_FAIL(this->set_dag_id(param_.job_desc_.trace_id_))) {
    LOG_WARN("failed to set dag id", K(ret), K_(param));
  } else {
    is_inited_ = true;
    index_kv_cache_ = &OB_BACKUP_INDEX_CACHE;
    backup_data_type_ = param_.backup_data_type_;
    report_ctx_ = param_.report_ctx_;
  }
  return ret;
}

int ObLSBackupDataDagNet::inner_init_before_run_()
{
  int ret = OB_SUCCESS;
  ObLSBackupParam backup_param;
  int64_t batch_size = 0;
  if (OB_FAIL(param_.convert_to(backup_param))) {
    LOG_WARN("failed to convert param", K(param_));
  } else if (OB_FAIL(ls_backup_ctx_.open(backup_param, backup_data_type_, *param_.report_ctx_.sql_proxy_, *GCTX.bandwidth_throttle_))) {
    LOG_WARN("failed to open log stream backup ctx", K(ret), K(backup_param));
  } else if (OB_FAIL(prepare_backup_tablet_provider_(backup_param, backup_data_type_, ls_backup_ctx_,
      OB_BACKUP_INDEX_CACHE, *param_.report_ctx_.sql_proxy_, provider_))) {
    LOG_WARN("failed to prepare backup tablet provider", K(ret), K(backup_param), K_(backup_data_type));
  } else if (OB_FAIL(get_batch_size_(batch_size))) {
    LOG_WARN("failed to get batch size", K(ret));
  } else if (OB_FAIL(task_mgr_.init(backup_data_type_, batch_size, ls_backup_ctx_))) {
    LOG_WARN("failed to init task mgr", K(ret), K(batch_size));
  } else {
    ls_backup_ctx_.stat_mgr_.mark_begin(backup_data_type_);
  }
  return ret;
}

int ObLSBackupDataDagNet::start_running()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSBackupDagInitParam init_param;
  ObLSBackupPrepareDag *prepare_dag = NULL;
  ObLSBackupFinishDag *finish_dag = NULL;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  // create dag and connections
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net do not init", K(ret));
  } else if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null MTL scheduler", K(ret), KP(scheduler));
  } else if (OB_FAIL(inner_init_before_run_())) {
    LOG_WARN("failed to inner init before run", K(ret));
  } else if (OB_FAIL(param_.convert_to(init_param))) {
    LOG_WARN("failed to convert to param", K(ret), K_(param));
  } else if (FALSE_IT(init_param.backup_stage_ = start_stage_)) {
  } else if (OB_FAIL(scheduler->alloc_dag(prepare_dag))) {
    LOG_WARN("failed to alloc dag", K(ret));
  } else if (OB_FAIL(prepare_dag->init(init_param,
                 backup_data_type_,
                 report_ctx_,
                 ls_backup_ctx_,
                 *provider_,
                 task_mgr_,
                 *index_kv_cache_))) {
    LOG_WARN("failed to init backup dag", K(ret), K(init_param));
  } else if (OB_FAIL(prepare_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*prepare_dag))) {
    LOG_WARN("failed to add dag into dag_net", K(ret), KPC(prepare_dag));
  } else if (OB_FAIL(scheduler->alloc_dag(finish_dag))) {
    LOG_WARN("failed to create dag", K(ret));
  } else if (OB_FAIL(finish_dag->init(init_param, report_ctx_, ls_backup_ctx_, *index_kv_cache_))) {
    LOG_WARN("failed to init finish dag", K(ret), K(init_param));
  } else if (OB_FAIL(finish_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(prepare_dag->add_child(*finish_dag))) {
    LOG_WARN("failed to add child", K(ret), KPC(prepare_dag), KPC(finish_dag));
  } else {
    bool add_finish_dag_success = false;
    bool add_prepare_dag_success = false;
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_ADD_BACKUP_FINISH_DAG_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "add_backup_finish_dag_failed");
      }
    }
#endif
    if (FAILEDx(scheduler->add_dag(finish_dag))) {
      LOG_WARN("failed to add dag into dag_scheduler", K(ret), KP(finish_dag));
    } else {
      add_finish_dag_success = true;
      LOG_INFO("success to add finish dag into dag_net", K(ret), K(init_param), KP(finish_dag));
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_ADD_BACKUP_PREPARE_DAG_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "add_backup_prepare_dag_failed");
      }
    }
#endif
    if (FAILEDx(scheduler->add_dag(prepare_dag))) {
      LOG_WARN("failed to add dag into dag_scheduler", K(ret), KP(prepare_dag));
    } else {
      add_prepare_dag_success = true;
      LOG_INFO("success to add prepare dag into dag_net", K(ret), K(init_param), KP(prepare_dag));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(scheduler) && OB_NOT_NULL(finish_dag)) {
      // add finish dag success and add prepare dag failed, need cancel finish dag
      if (add_finish_dag_success && !add_prepare_dag_success) {
        if (OB_TMP_FAIL(scheduler->cancel_dag(finish_dag))) {
          LOG_ERROR("failed to cancel backup dag", K(tmp_ret), KP(scheduler), KP(finish_dag));
        } else {
          finish_dag = NULL;
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(scheduler) && OB_NOT_NULL(prepare_dag)) {
    scheduler->free_dag(*prepare_dag);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(scheduler) && OB_NOT_NULL(finish_dag)) {
    scheduler->free_dag(*finish_dag);
  }

  if (OB_FAIL(ret)) {
    REPORT_TASK_RESULT(this->get_dag_id(), ret);
  }
  return ret;
}

bool ObLSBackupDataDagNet::operator==(const ObIDagNet &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObBackupDagNet &backup_dag_net = static_cast<const ObBackupDagNet &>(other);
    if (backup_dag_net.get_sub_type() != get_sub_type()) {
      bret = false;
    } else {
      const ObLSBackupDataDagNet &dag = static_cast<const ObLSBackupDataDagNet &>(other);
      bret = dag.param_ == param_ && dag.backup_data_type_ == backup_data_type_;
    }
  }
  return bret;
}

bool ObLSBackupDataDagNet::is_valid() const
{
  return param_.is_valid();
}

int64_t ObLSBackupDataDagNet::hash() const
{
  int64_t hash_value = 0;
  const int64_t type = ObBackupDagNetSubType::LOG_STREAM_BACKUP_DAG_DAG_NET;
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value = common::murmurhash(&param_, sizeof(param_), hash_value);
  hash_value = common::murmurhash(&backup_data_type_, sizeof(backup_data_type_), hash_value);
  return hash_value;
}

int ObLSBackupDataDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 "[BACKUP_DATA_DAG_NET]: tenant_id=%lu, backup_set_id=%ld, ls_id=%ld",
                 param_.tenant_id_,
                 param_.backup_set_desc_.backup_set_id_,
                 param_.ls_id_.id()))) {
    LOG_WARN("failed to fill comment", K(ret), K(param_));
  };
  return ret;
}

int ObLSBackupDataDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf,
          buf_len,
          "tenant_id=%lu, backup_set_id=%ld, ls_id=%ld",
          param_.tenant_id_,
          param_.backup_set_desc_.backup_set_id_,
          param_.ls_id_.id()))) {
    LOG_WARN("failed to fill dag net key", K(ret), K(param_));
  };
  return ret;
}

int ObLSBackupDataDagNet::get_batch_size_(int64_t &batch_size)
{
  int ret = OB_SUCCESS;
  int64_t data_file_size = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (!tenant_config.is_valid()) {
    data_file_size = DEFAULT_BACKUP_DATA_FILE_SIZE;
   } else {
    data_file_size = tenant_config->backup_data_file_size;
  }
  if (0 == data_file_size) {
    batch_size = OB_DEFAULT_BACKUP_BATCH_COUNT;
  } else {
    batch_size = data_file_size / OB_DEFAULT_MACRO_BLOCK_SIZE;
  }
#ifdef ERRSIM
  if (1 != param_.ls_id_.id()) {
    const int64_t max_block_per_backup_task = GCONF._max_block_per_backup_task;
    if (0 != max_block_per_backup_task) {
      batch_size = max_block_per_backup_task;
    }
  }
#endif
  LOG_INFO("get batch size", K(data_file_size), K(batch_size));
  return ret;
}

int ObLSBackupDataDagNet::prepare_backup_tablet_provider_(const ObLSBackupParam &param,
    const share::ObBackupDataType &backup_data_type, ObLSBackupCtx &ls_backup_ctx,
    ObBackupIndexKVCache &index_kv_cache, common::ObMySQLProxy &sql_proxy,
    ObIBackupTabletProvider *&provider)
{
  int ret = OB_SUCCESS;
  ObBackupTabletProvider *tmp_provider = NULL;
  const ObBackupTabletProviderType type = BACKUP_TABLET_PROVIDER;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param));
  }  else if (OB_ISNULL(tmp_provider = static_cast<ObBackupTabletProvider *>(ObLSBackupFactory::get_backup_tablet_provider(type, param.tenant_id_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate provider", K(ret), K(param));
  } else if (OB_FAIL(tmp_provider->init(param, backup_data_type, ls_backup_ctx, index_kv_cache, sql_proxy))) {
    LOG_WARN("failed to init provider", K(ret), K(param), K(backup_data_type));
  } else {
    provider = tmp_provider;
    tmp_provider = NULL;
  }
  if (OB_NOT_NULL(tmp_provider)) {
    ObLSBackupFactory::free(tmp_provider);
  }
  return ret;
}

/* ObBackupBuildTenantIndexDagNet */

ObBackupBuildTenantIndexDagNet::ObBackupBuildTenantIndexDagNet()
    : ObBackupDagNet(ObBackupDagNetSubType::LOG_STREAM_BACKUP_BUILD_INDEX_DAG_NET),
      is_inited_(false),
      param_(),
      backup_data_type_(),
      report_ctx_()
{}

ObBackupBuildTenantIndexDagNet::~ObBackupBuildTenantIndexDagNet()
{}

// TODO(yangyi.yyy): check if param can be casted in 4.1
int ObBackupBuildTenantIndexDagNet::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  ObLSBackupDagNetInitParam init_param;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dag net init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(param));
  } else if (OB_FAIL(init_param.assign(*(static_cast<const ObLSBackupDagNetInitParam *>(param))))) {
    LOG_WARN("failed to assign param", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param.job_desc_.trace_id_))) {
    LOG_WARN("failed to set dag id", K(ret), K(init_param));
  } else if (OB_FAIL(param_.backup_dest_.deep_copy(init_param.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(init_param));
  } else {
    param_.job_desc_ = init_param.job_desc_;
    param_.tenant_id_ = init_param.tenant_id_;
    param_.backup_set_desc_ = init_param.backup_set_desc_;
    param_.ls_id_ = ObLSID(0);  // 0 means build all ls backup index
    param_.turn_id_ = init_param.turn_id_;
    param_.retry_id_ = init_param.retry_id_;
    param_.dest_id_ = init_param.dest_id_;
    backup_data_type_ = init_param.backup_data_type_;
    report_ctx_ = init_param.report_ctx_;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupBuildTenantIndexDagNet::start_running()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSBackupIndexRebuildDag *rebuild_dag = NULL;
  const ObBackupIndexLevel index_level = BACKUP_INDEX_LEVEL_TENANT;
  ObTenantDagScheduler *dag_scheduler = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net not init", K(ret));
  } else if (OB_FAIL(guard.switch_to(param_.tenant_id_))) {
    LOG_WARN("failed to switch to tenant", K(ret), K_(param));
  } else if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler must not be NULL", K(ret));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(rebuild_dag))) {
    LOG_WARN("failed to alloc rebuild index dag", K(ret));
  } else if (OB_FAIL(rebuild_dag->init(param_,
                 backup_data_type_,
                 index_level,
                 report_ctx_,
                 NULL /*task_mgr*/,
                 NULL /*provider*/,
                 NULL /*index_kv_cache*/,
                 NULL /*ctx*/))) {
    LOG_WARN("failed to init child dag", K(ret), K_(param));
  } else if (OB_FAIL(rebuild_dag->create_first_task())) {
    LOG_WARN("failed to create first task for child dag", K(ret), KPC(rebuild_dag));
  } else if (OB_FAIL(add_dag_into_dag_net(*rebuild_dag))) {
    LOG_WARN("failed to add dag into dag_net", K(ret), KPC(rebuild_dag));
  } else if (OB_FAIL(dag_scheduler->add_dag(rebuild_dag))) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("failed to add dag", K(ret), KPC(rebuild_dag));
    } else {
      LOG_WARN("may exist same dag", K(ret), KPC(rebuild_dag));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(rebuild_dag)) {
    dag_scheduler->free_dag(*rebuild_dag);
  }

  if (OB_FAIL(ret)) {
    REPORT_TASK_RESULT(this->get_dag_id(), ret);
  }
  return ret;
}

bool ObBackupBuildTenantIndexDagNet::operator==(const share::ObIDagNet &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObBackupDagNet &backup_dag_net = static_cast<const ObBackupDagNet &>(other);
    if (backup_dag_net.get_sub_type() != get_sub_type()) {
      bret = false;
    } else {
      const ObBackupBuildTenantIndexDagNet &dag = static_cast<const ObBackupBuildTenantIndexDagNet &>(other);
      bret = dag.param_ == param_ && dag.backup_data_type_ == backup_data_type_;
    }
  }
  return bret;
}

bool ObBackupBuildTenantIndexDagNet::is_valid() const
{
  return param_.is_valid();
}

int64_t ObBackupBuildTenantIndexDagNet::hash() const
{
  int64_t hash_value = 0;
  const int64_t type = ObBackupDagNetSubType::LOG_STREAM_BACKUP_BUILD_INDEX_DAG_NET;
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value = common::murmurhash(&param_, sizeof(param_), hash_value);
  hash_value = common::murmurhash(&backup_data_type_, sizeof(backup_data_type_), hash_value);
  return hash_value;
}

int ObBackupBuildTenantIndexDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 "[BACKUP_BUILD_INDEX_DAG_NET]: tenant_id=%lu, backup_set_id=%ld, ls_id=%ld",
                 param_.tenant_id_,
                 param_.backup_set_desc_.backup_set_id_,
                 param_.ls_id_.id()))) {
    LOG_WARN("failed to fill comment", K(ret), K(param_));
  };
  return ret;
}

int ObBackupBuildTenantIndexDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf,
          buf_len,
          "tenant_id=%lu, backup_set_id=%ld, ls_id=%ld",
          param_.tenant_id_,
          param_.backup_set_desc_.backup_set_id_,
          param_.ls_id_.id()))) {
    LOG_WARN("failed to fill dag net key", K(ret), K(param_));
  };
  return ret;
}

/* ObLSBackupMetaDag */

ObLSBackupMetaDag::ObLSBackupMetaDag()
    : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_META), is_inited_(false), start_scn_(), param_(), report_ctx_(),
      ls_backup_ctx_(nullptr), compat_mode_(lib::Worker::CompatMode::INVALID)
{}

ObLSBackupMetaDag::~ObLSBackupMetaDag()
{}

int ObLSBackupMetaDag::init(
    const SCN &start_scn, const ObLSBackupDagInitParam &param, const ObBackupReportCtx &report_ctx,
    ObLSBackupCtx &ls_backup_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls backup meta dag init twice", K(ret));
  } else if (!start_scn.is_valid() || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(start_scn), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(param.tenant_id_, compat_mode_))) {
    LOG_WARN("failed to get_compat_mode", K(ret), K(param));
  } else {
    start_scn_ = start_scn;
    report_ctx_ = report_ctx;
    ls_backup_ctx_ = &ls_backup_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupMetaDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObLSBackupMetaTask *task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(start_scn_, param_, report_ctx_, *ls_backup_ctx_))) {
    LOG_WARN("failed to init task", K(ret), K_(start_scn), K_(param));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("success to add backup meta task", K(ret), KPC(this), KPC(task));
  }
  return ret;
}

bool ObLSBackupMetaDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObLSBackupMetaDag &other_dag = static_cast<const ObLSBackupMetaDag &>(other);
    bret = param_ == other_dag.param_;
  }

  return bret;
}

int ObLSBackupMetaDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup meta dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(), param_.ls_id_.id()))) {
    LOG_WARN("failed to add dag warning info param", K(ret));
  }
  return ret;
}

int ObLSBackupMetaDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ret = databuff_printf(buf, buf_len, pos, "ls_id=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, param_.ls_id_);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(param));
  }
  return ret;
}

int64_t ObLSBackupMetaDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}

/* ObLSBackupPrepareDag */

ObLSBackupPrepareDag::ObLSBackupPrepareDag()
    : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_PREPARE),
      is_inited_(false),
      param_(),
      backup_data_type_(),
      ls_backup_ctx_(NULL),
      provider_(NULL),
      task_mgr_(NULL),
      index_kv_cache_(NULL),
      report_ctx_(),
      compat_mode_(lib::Worker::CompatMode::INVALID)
{}

ObLSBackupPrepareDag::~ObLSBackupPrepareDag()
{}

int ObLSBackupPrepareDag::init(const ObLSBackupDagInitParam &param, const share::ObBackupDataType &backup_data_type,
    const ObBackupReportCtx &report_ctx, ObLSBackupCtx &ls_backup_ctx, ObIBackupTabletProvider &provider,
    ObBackupMacroBlockTaskMgr &task_mgr, ObBackupIndexKVCache &index_kv_cache)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup dag init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(param.tenant_id_, compat_mode_))) {
    LOG_WARN("failed to get_compat_mode", K(ret), K(param));
  } else {
    backup_data_type_ = backup_data_type;
    ls_backup_ctx_ = &ls_backup_ctx;
    provider_ = &provider;
    task_mgr_ = &task_mgr;
    index_kv_cache_ = &index_kv_cache;
    report_ctx_ = report_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupPrepareDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObLSBackupPrepareTask *task = NULL;
  int64_t concurrency = 0;
  if (OB_FAIL(get_concurrency_count_(backup_data_type_, concurrency))) {
    LOG_WARN("failed to get concurrency count", K(ret), K_(backup_data_type));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(concurrency,
                 param_,
                 backup_data_type_,
                 *ls_backup_ctx_,
                 *provider_,
                 *task_mgr_,
                 *index_kv_cache_,
                 report_ctx_))) {
    LOG_WARN("failed to init task", K(ret), K_(param), K_(backup_data_type));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("success to add prepare task", K(ret), KPC(this), KPC(task));
  }
  return ret;
}

bool ObLSBackupPrepareDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObLSBackupPrepareDag &other_dag = static_cast<const ObLSBackupPrepareDag &>(other);
    bret = param_ == other_dag.param_ && backup_data_type_ == other_dag.backup_data_type_;
  }
  return bret;
}

int ObLSBackupPrepareDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup prepare dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  static_cast<int64_t>(param_.tenant_id_), param_.backup_set_desc_.backup_set_id_,
                                  param_.ls_id_.id(), param_.turn_id_, param_.retry_id_))) {
    LOG_WARN("failed to add dag warning info param", K(ret));
  }
  return ret;
}

int ObLSBackupPrepareDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ret = databuff_printf(buf, buf_len, pos, "ls_id=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, param_.ls_id_);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(param));
  }
  return ret;
}

int64_t ObLSBackupPrepareDag::hash() const
{
  int64_t hash_value = 0;
  const int64_t type = get_type();
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value = common::murmurhash(&param_, sizeof(param_), hash_value);
  hash_value = common::murmurhash(&backup_data_type_, sizeof(backup_data_type_), hash_value);
  return hash_value;
}

int ObLSBackupPrepareDag::get_concurrency_count_(const share::ObBackupDataType &backup_data_type, int64_t &concurrency)
{
  int ret = OB_SUCCESS;
  const int64_t sys_backup_concurrency = 1;
  int64_t data_backup_concurrency = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    data_backup_concurrency = tenant_config->ha_low_thread_score;
  }
  if (0 == data_backup_concurrency) {
    data_backup_concurrency = OB_DEFAULT_BACKUP_CONCURRENCY;
  } else if (data_backup_concurrency > OB_MAX_BACKUP_CONCURRENCY) {
    data_backup_concurrency = OB_MAX_BACKUP_CONCURRENCY;
  }
  switch (backup_data_type.type_) {
    case ObBackupDataType::BACKUP_SYS: {
      concurrency = sys_backup_concurrency;
      break;
    }
    case ObBackupDataType::BACKUP_USER: {
      concurrency = data_backup_concurrency;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid type", K(ret), K(backup_data_type));
      break;
    }
  }
  LOG_INFO("get concurrency count", K(MTL_ID()), K(backup_data_type), K(concurrency));
  return ret;
}

/* ObLSBackupFinishDag */

ObLSBackupFinishDag::ObLSBackupFinishDag()
    : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_FINISH), is_inited_(false), report_ctx_(), ls_backup_ctx_(NULL), index_kv_cache_(NULL),
      compat_mode_(lib::Worker::CompatMode::INVALID)
{}

ObLSBackupFinishDag::~ObLSBackupFinishDag()
{}

int ObLSBackupFinishDag::init(const ObLSBackupDagInitParam &param, const ObBackupReportCtx &report_ctx,
    ObLSBackupCtx &ls_backup_ctx, ObBackupIndexKVCache &index_kv_cache)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup dag init twice", K(ret));
  } else if (!param.is_valid() || !report_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(report_ctx));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(param.tenant_id_, compat_mode_))) {
    LOG_WARN("failed to get_compat_mode", K(ret), K(param));
  } else {
    report_ctx_ = report_ctx;
    ls_backup_ctx_ = &ls_backup_ctx;
    index_kv_cache_ = &index_kv_cache;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupFinishDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObLSBackupFinishTask *task = NULL;
  ObLSBackupDataParam param;
  ObBackupDataType backup_data_type;
  backup_data_type.set_major_data_backup();
  if (OB_FAIL(param_.convert_to(backup_data_type, param))) {
    LOG_WARN("failed to convert param", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(param, report_ctx_, *ls_backup_ctx_, *index_kv_cache_))) {
    LOG_WARN("failed to init task", K(ret), K(param));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("success to add finish task", K(ret), KPC(this), KPC(task));
  }
  return ret;
}

bool ObLSBackupFinishDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObLSBackupFinishDag &other_dag = static_cast<const ObLSBackupFinishDag &>(other);
    bret = param_ == other_dag.param_;
  }
  return bret;
}

int ObLSBackupFinishDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup finish dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  static_cast<int64_t>(param_.tenant_id_),
                                  param_.backup_set_desc_.backup_set_id_, param_.ls_id_.id()))) {
    LOG_WARN("failed to add dag warning info param", K(ret));
  }
  return ret;
}

int ObLSBackupFinishDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ret = databuff_printf(buf, buf_len, pos, "ls_id=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, param_.ls_id_);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(param));
  }
  return ret;
}

int64_t ObLSBackupFinishDag::hash() const
{
  int64_t hash_value = 0;
  const int64_t type = get_type();
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value = common::murmurhash(&param_, sizeof(param_), hash_value);
  return hash_value;
}

bool ObLSBackupFinishDag::check_can_schedule()
{
  return ls_backup_ctx_->is_finished();
}

/* ObLSBackupDataDag */

ObLSBackupDataDag::ObLSBackupDataDag()
    : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_DATA),
      is_inited_(false),
      task_id_(0),
      param_(),
      backup_data_type_(),
      ls_backup_ctx_(NULL),
      provider_(NULL),
      task_mgr_(NULL),
      index_kv_cache_(NULL),
      report_ctx_(),
      index_rebuild_dag_(NULL),
      compat_mode_(lib::Worker::CompatMode::INVALID)
{}

ObLSBackupDataDag::ObLSBackupDataDag(const share::ObDagType::ObDagTypeEnum type)
    : share::ObIDag(type),
      is_inited_(false),
      task_id_(0),
      param_(),
      backup_data_type_(),
      ls_backup_ctx_(NULL),
      provider_(NULL),
      task_mgr_(NULL),
      index_kv_cache_(NULL),
      report_ctx_(),
      index_rebuild_dag_(NULL)
{}

ObLSBackupDataDag::~ObLSBackupDataDag()
{}

int ObLSBackupDataDag::init(const int64_t task_id, const ObLSBackupDagInitParam &param,
    const share::ObBackupDataType &backup_data_type, const ObBackupReportCtx &report_ctx, ObLSBackupCtx &ls_backup_ctx,
    ObIBackupTabletProvider &provider, ObBackupMacroBlockTaskMgr &task_mgr, ObBackupIndexKVCache &index_kv_cache,
    share::ObIDag *index_rebuild_dag)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup dag init twice", K(ret));
  } else if (!param.is_valid() || !report_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(report_ctx));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(param.tenant_id_, compat_mode_))) {
    LOG_WARN("failed to get_compat_mode", K(ret), K(param));
  } else {
    task_id_ = task_id;
    backup_data_type_ = backup_data_type;
    ls_backup_ctx_ = &ls_backup_ctx;
    provider_ = &provider;
    task_mgr_ = &task_mgr;
    index_kv_cache_ = &index_kv_cache;
    report_ctx_ = report_ctx;
    index_rebuild_dag_ = index_rebuild_dag;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupDataDag::provide(const common::ObIArray<ObBackupProviderItem> &items)
{
  return append(backup_items_, items);
}

int ObLSBackupDataDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObLSBackupDataTask *backup_task = NULL;
  ObLSBackupDataParam param;
  if (OB_FAIL(param_.convert_to(backup_data_type_, param))) {
    LOG_WARN("failed to convert param", K(ret));
  } else if (OB_FAIL(alloc_task(backup_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(backup_task->init(task_id_,
                 backup_data_type_,
                 backup_items_,
                 param,
                 report_ctx_,
                 *ls_backup_ctx_,
                 *provider_,
                 *task_mgr_,
                 *index_kv_cache_,
                 index_rebuild_dag_))) {
    LOG_WARN("failed to init backup task", K(ret), K_(task_id));
  } else if (OB_FAIL(add_task(*backup_task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("generate backup data task", K_(param), K_(backup_data_type));
  }
  return ret;
}

bool ObLSBackupDataDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObLSBackupDataDag &other_dag = static_cast<const ObLSBackupDataDag &>(other);
    bret = param_ == other_dag.param_ && task_id_ == other_dag.task_id_ && backup_data_type_ == other_dag.backup_data_type_;
  }
  return bret;
}

int64_t ObLSBackupDataDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}

int ObLSBackupDataDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup data dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  static_cast<int64_t>(param_.tenant_id_), param_.backup_set_desc_.backup_set_id_,
                                  static_cast<int64_t>(backup_data_type_.type_), param_.ls_id_.id(),
                                  param_.turn_id_, param_.retry_id_, task_id_))) {
    LOG_WARN("failed to add dag warning info param", K(ret));
  }
  return ret;
}

int ObLSBackupDataDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ret = databuff_printf(buf, buf_len, pos, "ls_id=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, param_.ls_id_);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(param));
  }
  return ret;
}

/* ObPrefetchBackupInfoDag */

ObPrefetchBackupInfoDag::ObPrefetchBackupInfoDag()
  : ObLSBackupDataDag(ObDagType::DAG_TYPE_PREFETCH_BACKUP_INFO)
{}

ObPrefetchBackupInfoDag::~ObPrefetchBackupInfoDag()
{}

bool ObPrefetchBackupInfoDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObPrefetchBackupInfoDag &other_dag = static_cast<const ObPrefetchBackupInfoDag &>(other);
    bret = param_ == other_dag.param_ && task_id_ == other_dag.task_id_ && backup_data_type_ == other_dag.backup_data_type_;
  }
  return bret;
}

int64_t ObPrefetchBackupInfoDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}

int ObPrefetchBackupInfoDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObPrefetchBackupInfoTask *task = NULL;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(param_,
                 backup_data_type_,
                 report_ctx_,
                 *ls_backup_ctx_,
                 *provider_,
                 *task_mgr_,
                 *index_kv_cache_,
                 index_rebuild_dag_))) {
    LOG_WARN("failed to init task", K(ret), K(param_), K_(backup_data_type));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("success to add prefetch task", K(ret), K_(param), K_(backup_data_type), KPC(task));
  }
  return ret;
}

int ObPrefetchBackupInfoDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ret = databuff_printf(buf, buf_len, pos, "ls_id=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, param_.ls_id_);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(param));
  }
  return ret;
}

/* ObLSBackupIndexRebuildDag */

const ObCompressorType ObLSBackupIndexRebuildDag::DEFAULT_COMPRESSOR_TYPE = OB_DEFAULT_BACKUP_INDEX_COMPRESSOR_TYPE;

ObLSBackupIndexRebuildDag::ObLSBackupIndexRebuildDag()
    : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_INDEX_REBUILD),
      is_inited_(false),
      report_ctx_(),
      param_(),
      index_level_(),
      backup_data_type_(),
      ls_backup_ctx_(NULL),
      task_mgr_(NULL),
      provider_(NULL),
      index_kv_cache_(NULL),
      compat_mode_(lib::Worker::CompatMode::INVALID)
{}

ObLSBackupIndexRebuildDag::~ObLSBackupIndexRebuildDag()
{}

int ObLSBackupIndexRebuildDag::init(const ObLSBackupDagInitParam &param,
    const share::ObBackupDataType &backup_data_type, const ObBackupIndexLevel &index_level,
    const ObBackupReportCtx &report_ctx, ObBackupMacroBlockTaskMgr *task_mgr, ObIBackupTabletProvider *provider,
    ObBackupIndexKVCache *index_kv_cache, ObLSBackupCtx *ls_backup_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task init twice", K(ret));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(param.tenant_id_, compat_mode_))) {
    LOG_WARN("failed to get_compat_mode", K(ret), K(param));
  } else {
    backup_data_type_ = backup_data_type;
    index_level_ = index_level;
    report_ctx_ = report_ctx;
    task_mgr_ = task_mgr;
    provider_ = provider;
    index_kv_cache_ = index_kv_cache;
    ls_backup_ctx_ = ls_backup_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupIndexRebuildDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupIndexRebuildTask *task = NULL;
  ObLSBackupDataParam param;
  const ObCompressorType &compressor_type = DEFAULT_COMPRESSOR_TYPE;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(ret));
  } else if (OB_FAIL(param_.convert_to(backup_data_type_, param))) {
    LOG_WARN("failed to convert param", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(param,
                 index_level_,
                 ls_backup_ctx_,
                 provider_,
                 task_mgr_,
                 index_kv_cache_,
                 report_ctx_,
                 compressor_type))) {
    LOG_WARN("failed to init task", K(ret), K(param), K_(index_level));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("success to add index rebuild dag", K(ret), KPC(this), KPC(task));
  }
  return ret;
}

int ObLSBackupIndexRebuildDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup index rebuild dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  static_cast<int64_t>(param_.tenant_id_), param_.backup_set_desc_.backup_set_id_,
                                  static_cast<int64_t>(backup_data_type_.type_), param_.ls_id_.id(),
                                  param_.turn_id_, param_.retry_id_))) {
    LOG_WARN("failed to add dag warning info param", K(ret));
  }
  return ret;
}

int ObLSBackupIndexRebuildDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ret = databuff_printf(buf, buf_len, pos, "ls_id=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, param_.ls_id_);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(param));
  }
  return ret;
}

bool ObLSBackupIndexRebuildDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObLSBackupIndexRebuildDag &dag = static_cast<const ObLSBackupIndexRebuildDag &>(other);
    bret = dag.param_ == param_ && dag.index_level_ == index_level_ && dag.backup_data_type_ == backup_data_type_;
  }
  return bret;
}

int64_t ObLSBackupIndexRebuildDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}

int ObLSBackupIndexRebuildDag::get_file_id_list_(common::ObIArray<int64_t> &file_id_list)
{
  int ret = OB_SUCCESS;
  file_id_list.reset();
  ObBackupIoAdapter util;
  ObBackupPath backup_path;
  ObBackupDataFileRangeOp file_range_op(OB_STR_BACKUP_MACRO_BLOCK_DATA);
  if (OB_FAIL(ObBackupPathUtil::get_ls_backup_data_dir_path(param_.backup_dest_,
          param_.ls_id_,
          backup_data_type_,
          param_.turn_id_,
          param_.retry_id_,
          backup_path))) {
    LOG_WARN("failed to get log stream backup data dir path", K(ret), K_(param), K_(backup_data_type));
  } else if (OB_FAIL(util.list_files(backup_path.get_obstr(), param_.backup_dest_.get_storage_info(), file_range_op))) {
    LOG_WARN("list_files fail", K(ret), K(backup_path), K_(param));
  } else if (OB_FAIL(file_range_op.get_file_list(file_id_list))) {
    LOG_WARN("failed to get file list", K(ret));
  } else {
    LOG_INFO("get file id list", K(backup_path), K(file_id_list));
  }
  return ret;
}

/* ObPrefetchBackupInfoTask */

ObPrefetchBackupInfoTask::ObPrefetchBackupInfoTask()
    : ObITask(TASK_TYPE_MIGRATE_COPY_PHYSICAL),
      is_inited_(false),
      param_(),
      report_ctx_(),
      backup_data_type_(),
      ls_backup_ctx_(NULL),
      provider_(NULL),
      task_mgr_(NULL),
      index_kv_cache_(NULL),
      macro_index_store_for_inc_(),
      macro_index_store_for_turn_(),
      index_rebuild_dag_(NULL)
{}

ObPrefetchBackupInfoTask::~ObPrefetchBackupInfoTask()
{}

int ObPrefetchBackupInfoTask::init(const ObLSBackupDagInitParam &param, const share::ObBackupDataType &backup_data_type,
    const ObBackupReportCtx &report_ctx, ObLSBackupCtx &ls_backup_ctx, ObIBackupTabletProvider &provider,
    ObBackupMacroBlockTaskMgr &task_mgr, ObBackupIndexKVCache &index_kv_cache, share::ObIDag *index_rebuild_dag)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task init twice", K(ret));
  } else if (!param.is_valid() || !report_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(report_ctx));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    report_ctx_ = report_ctx;
    backup_data_type_ = backup_data_type;
    ls_backup_ctx_ = &ls_backup_ctx;
    provider_ = &provider;
    task_mgr_ = &task_mgr;
    index_kv_cache_ = &index_kv_cache;
    index_rebuild_dag_ = index_rebuild_dag;
    if (OB_FAIL(inner_init_macro_index_store_for_inc_(param, backup_data_type, report_ctx))) {
      LOG_WARN("failed to inner init macro index store for inc", K(ret));
    } else if (OB_FAIL(inner_init_macro_index_store_for_turn_(param, backup_data_type, report_ctx))) {
      LOG_WARN("failed to inner init macro index store for turn", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPrefetchBackupInfoTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_report_error = false;
#ifdef ERRSIM
  if (backup_data_type_.is_user_backup() && 1002 == param_.ls_id_.id() && 1 == param_.turn_id_ && 1 == param_.retry_id_) {
    SERVER_EVENT_SYNC_ADD("backup_errsim", "before_backup_prefetch_task");
    DEBUG_SYNC(BEFORE_BACKUP_PREFETCH_TASK);
  }
#endif

  if (backup_data_type_.is_user_backup() && !param_.ls_id_.is_sys_ls()) {
    SERVER_EVENT_SYNC_ADD("backup", "before_backup_prefetch_task",
                          "tenant_id", MTL_ID(),
                          "ls_id", param_.ls_id_.id(),
                          "turn_id", param_.turn_id_,
                          "retry_id", param_.retry_id_);
    DEBUG_SYNC(BEFORE_PREFETCH_BACKUP_INFO_TASK);
  }

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("prefetch backup info task is not inited", K(ret));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
    LOG_INFO("backup already failed, do nothing");
  } else if (OB_FAIL(inner_process_())) {
    LOG_WARN("failed to process", K(ret), K_(param));
  } else {
    LOG_INFO("prefetch backup info process", K_(backup_data_type));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_PREFETCH_BACKUP_INFO_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      if (0 != param_.retry_id_) {
        ret = OB_SUCCESS;  // only errsim first retry
      } else if (backup_data_type_.is_sys_backup()) {
        ret = OB_SUCCESS;  // only errsim data tablet
      } else {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_prefetch_info");
        LOG_WARN("errsim backup prefetch backup info failed", K(ret));
      }
    }
  }
#endif
  if (OB_FAIL(ret) && OB_NOT_NULL(ls_backup_ctx_)) {
    bool is_set = false;
    ls_backup_ctx_->set_result_code(ret, is_set);
    need_report_error = is_set;
  }
  if (OB_NOT_NULL(ls_backup_ctx_) && need_report_error) {
    REPORT_TASK_RESULT(this->get_dag()->get_dag_id(), ls_backup_ctx_->get_result_code());
  }
  return ret;
}

int ObPrefetchBackupInfoTask::setup_macro_index_store_(const ObLSBackupDagInitParam &param, const share::ObBackupDataType &backup_data_type,
    const ObBackupSetDesc &backup_set_desc, const ObBackupReportCtx &report_ctx, ObBackupIndexStoreParam &index_store_param,
    ObBackupOrderedMacroBlockIndexStore &index_store)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid() || !backup_data_type.is_valid() || !backup_set_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(backup_data_type), K(backup_set_desc));
  } else {
    ObBackupRestoreMode mode = BACKUP_MODE;
    ObBackupIndexLevel index_level = BACKUP_INDEX_LEVEL_TENANT;
    index_store_param.index_level_ = index_level;
    index_store_param.tenant_id_ = param.tenant_id_;
    index_store_param.backup_set_id_ = param.backup_set_desc_.backup_set_id_;
    index_store_param.ls_id_ = param.ls_id_;
    index_store_param.is_tenant_level_ = true;
    index_store_param.backup_data_type_ = backup_data_type;
    index_store_param.dest_id_ = param.dest_id_;
     if (OB_FAIL(index_store.init(mode, index_store_param, param.backup_dest_,
            backup_set_desc, *index_kv_cache_))) {
      LOG_WARN("failed to init macro index store", K(ret), K(param), K(index_store_param), K(backup_set_desc), K(backup_data_type));
    }
  }
  return ret;
}

int ObPrefetchBackupInfoTask::inner_init_macro_index_store_for_inc_(const ObLSBackupDagInitParam &param,
    const share::ObBackupDataType &backup_data_type, const ObBackupReportCtx &report_ctx)
{
  int ret = OB_SUCCESS;
  if (param.backup_set_desc_.backup_type_.is_inc_backup() && backup_data_type.is_user_backup()) {
    share::ObBackupSetFileDesc prev_backup_set_info;
    ObBackupSetDesc prev_backup_set_desc;
    ObBackupIndexStoreParam index_store_param;
    if (OB_FAIL(get_prev_backup_set_desc_(param.tenant_id_, param.dest_id_, param.backup_set_desc_, prev_backup_set_info))) {
      LOG_WARN("failed to get prev backup set desc", K(ret), K(param));
    } else if (OB_FALSE_IT(prev_backup_set_desc.backup_set_id_ = prev_backup_set_info.backup_set_id_)) {
    } else if (OB_FALSE_IT(prev_backup_set_desc.backup_type_ = prev_backup_set_info.backup_type_)) {
    } else if (OB_FALSE_IT(index_store_param.turn_id_ = prev_backup_set_info.major_turn_id_)) {
    } else if (OB_FAIL(get_tenant_macro_index_retry_id_(param.backup_dest_,
        prev_backup_set_desc, backup_data_type, prev_backup_set_info.major_turn_id_, index_store_param.retry_id_))) {
      LOG_WARN("failed to get retry id", K(ret), K(param), K(prev_backup_set_desc), K(backup_data_type));
    } else if (OB_FAIL(setup_macro_index_store_(param, backup_data_type, prev_backup_set_desc, report_ctx, index_store_param, macro_index_store_for_inc_))) {
      LOG_WARN("failed to setup macro index store", K(ret));
    } else {
      LOG_INFO("inner init macro index store for incremental backup", K(prev_backup_set_info), K(index_store_param), K(param));
    }
  }
  return ret;
}

int ObPrefetchBackupInfoTask::inner_init_macro_index_store_for_turn_(const ObLSBackupDagInitParam &param,
    const share::ObBackupDataType &backup_data_type, const ObBackupReportCtx &report_ctx)
{
  int ret = OB_SUCCESS;
  if (backup_data_type.is_user_backup()) {
    const ObBackupSetDesc &backup_set_desc = param.backup_set_desc_;
    ObBackupIndexStoreParam index_store_param;
    const int64_t cur_turn_id = param.turn_id_;
    index_store_param.turn_id_ = get_prev_turn_id_(cur_turn_id);
    if (0 == index_store_param.turn_id_) {
      // no need init
    } else if (OB_FAIL(get_tenant_macro_index_retry_id_(param.backup_dest_,
        backup_set_desc, backup_data_type, index_store_param.turn_id_, index_store_param.retry_id_))) {
      LOG_WARN("failed to get retry id", K(ret), K(param), K(backup_data_type));
    } else if (OB_FAIL(setup_macro_index_store_(param, backup_data_type, backup_set_desc, report_ctx, index_store_param, macro_index_store_for_turn_))) {
      LOG_WARN("failed to setup macro index store", K(ret));
    } else {
      LOG_INFO("inner init macro index store for change turn", K(cur_turn_id), K(index_store_param), K(param));
    }
  }
  return ret;
}

int64_t ObPrefetchBackupInfoTask::get_prev_turn_id_(const int64_t cur_turn_id)
{
  const int64_t prev_turn_id = cur_turn_id - 1;
  LOG_INFO("get prev turn id", K(cur_turn_id), K(prev_turn_id));
  return prev_turn_id;
}

int ObPrefetchBackupInfoTask::inner_process_()
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  ObArray<ObBackupProviderItem> sorted_items;
  ObArray<ObBackupProviderItem> need_copy_item_list;
  ObArray<ObBackupProviderItem> no_need_copy_item_list;
  ObArray<ObBackupDeviceMacroBlockId> no_need_copy_macro_index_list;
  if (OB_ISNULL(provider_) || OB_ISNULL(task_mgr_) || OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task mgr can not be null", K(ret), KP_(provider), KP_(task_mgr), KP_(ls_backup_ctx));
  } else {
    const bool is_run_out = provider_->is_run_out();
    if (!is_run_out) {
      if (OB_FAIL(provider_->get_next_batch_items(sorted_items, task_id))) {
        if (OB_ITER_END == ret) {
          LOG_INFO("provider reach end", K(ret));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next batch item", K(ret));
        }
      } else if (OB_FAIL(get_need_copy_item_list_(
                     sorted_items, need_copy_item_list, no_need_copy_item_list, no_need_copy_macro_index_list))) {
        LOG_WARN("failed to check need reuse", K(ret), K(sorted_items));
      } else if (OB_FAIL(ls_backup_ctx_->tablet_stat_.mark_items_pending(backup_data_type_, need_copy_item_list))) {
        LOG_WARN("failed to mark items pending", K(ret), K_(backup_data_type), K(need_copy_item_list));
      } else if (OB_FAIL(ls_backup_ctx_->tablet_stat_.mark_items_reused(
                     backup_data_type_, no_need_copy_item_list, no_need_copy_macro_index_list))) {
        LOG_WARN("failed to mark items reused",
            K(ret),
            K_(backup_data_type),
            K(no_need_copy_item_list),
            K(no_need_copy_macro_index_list));
      } else if (OB_FAIL(task_mgr_->receive(task_id, sorted_items))) {
        LOG_WARN("failed to receive items", K(ret), K(task_id), K(need_copy_item_list));
      } else {
        LOG_INFO("receive backup items", K(task_id), K_(backup_data_type), "need_copy_count", need_copy_item_list.count(),
                "no_need_copy_count", no_need_copy_item_list.count(), K_(param));
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<ObBackupProviderItem> items;
      int64_t file_id = 0;
      if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
        LOG_INFO("backup task already failed", "result_code", ls_backup_ctx_->get_result_code());
      } else if (OB_FAIL(task_mgr_->deliver(items, file_id))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
          if (!items.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_INFO("item list is not empty", K(items), K_(backup_data_type));
          } else {
            if (!is_run_out) {
              if (OB_FAIL(generate_next_prefetch_dag_())) {
                LOG_WARN("failed to generate prefetch dag", K(ret));
              } else {
                LOG_INFO("generate next prefetch dag", K(items), K_(backup_data_type));
              }
            } else {
              LOG_INFO("run out", K_(param), K_(backup_data_type), K(items));
            }
          }
        } else {
          LOG_WARN("failed to receive output", K(ret));
        }
      } else if (OB_FAIL(check_backup_items_valid_(items))) {
        LOG_WARN("failed to check backup items valid", K(ret), K(items));
      } else {
        if (OB_FAIL(ls_backup_ctx_->set_max_file_id(file_id))) {
          LOG_WARN("failed to set max file id", K(ret));
        } else if (OB_FAIL(generate_backup_dag_(file_id, items))) {
          LOG_WARN("failed to generate backup dag", K(ret), K(file_id));
        } else {
          LOG_INFO("generate backup dag", K(task_id),  K(file_id),
              K_(backup_data_type), "ls_id", param_.ls_id_, "turn_id", param_.turn_id_,
              "retry_id", param_.retry_id_, K(items));
        }
      }
    }
  }
  return ret;
}

int ObPrefetchBackupInfoTask::check_backup_items_valid_(const common::ObIArray<ObBackupProviderItem> &items)
{
  int ret = OB_SUCCESS;
  if (items.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("items should not be empty", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      const ObBackupProviderItem &item = items.at(i);
      if (!item.is_valid()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("item is not valid", K(ret), K(item));
      }
    }
  }
  return ret;
}

int ObPrefetchBackupInfoTask::get_prev_backup_set_desc_(
    const uint64_t tenant_id, const int64_t dest_id, const share::ObBackupSetDesc &cur_backup_set_desc, share::ObBackupSetFileDesc &prev_backup_set_info)
{
  int ret = OB_SUCCESS;
  if (!cur_backup_set_desc.backup_type_.is_inc_backup()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("full backup set has no prev", K(ret), K(cur_backup_set_desc));
  } else if (OB_FAIL(ObLSBackupOperator::get_prev_backup_set_desc(
                 tenant_id, cur_backup_set_desc.backup_set_id_, dest_id, prev_backup_set_info, *report_ctx_.sql_proxy_))) {
    LOG_WARN("failed to get prev backup set desc", K(ret), K(tenant_id), K(cur_backup_set_desc));
  } else {
    LOG_INFO("get prev backup set desc", K(tenant_id), K(dest_id), K(cur_backup_set_desc), K(prev_backup_set_info));
  }
  return ret;
}

int ObPrefetchBackupInfoTask::get_tenant_macro_index_retry_id_(const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
    const share::ObBackupDataType &backup_data_type, const int64_t turn_id, int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  retry_id = -1;
  ObBackupTenantIndexRetryIDGetter retry_id_getter;
  const bool is_restore = false;
  const bool is_macro_index = true;
  if (!backup_dest.is_valid() || !backup_set_desc.is_valid() || !backup_data_type.is_valid() || turn_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(backup_set_desc), K(backup_data_type), K(turn_id));
  } else if (OB_FAIL(retry_id_getter.init(backup_dest, backup_set_desc, backup_data_type,
      turn_id, is_restore, is_macro_index, false/*is_sec_meta*/))) {
    LOG_WARN("failed to init retry id getter", K(ret), K_(param));
  } else if (OB_FAIL(retry_id_getter.get_max_retry_id(retry_id))) {
    LOG_WARN("failed to get max retry id", K(ret));
  }
  return ret;
}

int ObPrefetchBackupInfoTask::get_need_copy_item_list_(common::ObIArray<ObBackupProviderItem> &list,
    common::ObIArray<ObBackupProviderItem> &need_copy_list, common::ObIArray<ObBackupProviderItem> &no_need_copy_list,
    common::ObIArray<ObBackupDeviceMacroBlockId> &no_need_copy_macro_index_list)
{
  int ret = OB_SUCCESS;
  need_copy_list.reset();
  no_need_copy_list.reset();
  no_need_copy_macro_index_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    bool need_copy = false;
    const ObBackupProviderItem &item = list.at(i);
    ObBackupMacroBlockIndex macro_index;
    // item type
    if (!item.is_valid()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("backup items is not valid", K(ret), K(item));
    } else if (OB_FAIL(check_backup_item_need_copy_(item, need_copy, macro_index))) {
      LOG_WARN("failed to check macro block need reuse", K(ret), K_(param), K(item));
    } else {
      if (need_copy) {
        if (OB_FAIL(need_copy_list.push_back(item))) {
          LOG_WARN("failed to push back", K(ret), K(item));
        }
      } else {
        ObBackupDeviceMacroBlockId physical_id;
        if (OB_FAIL(no_need_copy_list.push_back(item))) {
          LOG_WARN("failed to push back", K(ret), K(item));
        } else if (OB_FAIL(macro_index.get_backup_physical_id(backup_data_type_, physical_id))) {
          LOG_WARN("failed to get backup physical id", K(ret), K(physical_id));
        } else if (OB_FAIL(no_need_copy_macro_index_list.push_back(physical_id))) {
          LOG_WARN("failed to push back", K(ret), K(macro_index), K(physical_id));
        } else {
          list.at(i).set_no_need_copy();
          list.at(i).set_macro_index(macro_index);
        }
      }
    }
  }
  return ret;
}

int ObPrefetchBackupInfoTask::check_backup_item_need_copy_(
    const ObBackupProviderItem &item, bool &need_copy, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  need_copy = false;
  macro_index.reset();
  const ObBackupDataType &backup_data_type = item.get_backup_data_type();
  if (item.get_item_type() != PROVIDER_ITEM_MACRO_ID) {
    need_copy = true;
  } else if (!backup_data_type.is_major_backup()) {
    need_copy = true;
  } else {
    switch (param_.backup_set_desc_.backup_type_.type_) {
      case share::ObBackupType::FULL_BACKUP:
        need_copy = true;
        break;
      case share::ObBackupType::INCREMENTAL_BACKUP: {
        bool is_exist = false;
        if (!macro_index_store_for_inc_.is_inited()) {
          ret = OB_NOT_INIT;
          LOG_WARN("index store is not init", K(ret));
        } else if (OB_FAIL(macro_index_store_for_inc_.get_macro_block_index(item.get_logic_id(), macro_index))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            need_copy = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get macro block index", K(ret), K(item));
          }
        } else {
          LOG_DEBUG("macro block was reused", K(macro_index), K_(param));
          need_copy = false;
#ifdef ERRSIM
          SERVER_EVENT_SYNC_ADD("backup_data", "reuse_macro_block",
                                "tenant_id", param_.tenant_id_,
                                "backup_set_id", param_.backup_set_desc_.backup_set_id_,
                                "reused_macro_block_index", macro_index);
#endif
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown backup type", K(ret), K(param_));
    }
    if (OB_SUCC(ret) && need_copy) {
      if (OB_FAIL(inner_check_backup_item_need_copy_when_change_retry_(item, need_copy, macro_index))) {
        LOG_WARN("failed to inner check backup item need copy when change retry", K(ret));
      } else if (!need_copy) {
        LOG_INFO("recover when change retry", "turn_id", param_.turn_id_,
                            "retry_id", param_.retry_id_, K(item));
      } else if (OB_FAIL(inner_check_backup_item_need_copy_when_change_turn_(item, need_copy, macro_index))) {
        LOG_WARN("failed to inner check backup item need copy when change turn", K(ret));
      }
    }
  }
  return ret;
}

struct ObCompareBackupMacroBlockIdPair
{
  bool operator()(const ObBackupMacroBlockIDPair &pair, const ObLogicMacroBlockId &logic_id) const
  {
    return pair.logic_id_ < logic_id;
  }
};

int ObPrefetchBackupInfoTask::inner_check_backup_item_need_copy_when_change_retry_(
    const ObBackupProviderItem &item, bool &need_copy, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  need_copy = false;
  macro_index.reset();
  ObCompareBackupMacroBlockIdPair compare;
  const ObBackupDataType &backup_data_type = item.get_backup_data_type();
  if (!backup_data_type.is_major_backup()) {
    need_copy = true;
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (ls_backup_ctx_->backup_retry_ctx_.reused_pair_list_.empty()) {
    need_copy = true;
  } else {
    typedef common::ObArray<ObBackupMacroBlockIDPair>::iterator Iter;
    const ObLogicMacroBlockId &logic_id = item.get_logic_id();
    Iter iter = std::lower_bound(ls_backup_ctx_->backup_retry_ctx_.reused_pair_list_.begin(),
                                 ls_backup_ctx_->backup_retry_ctx_.reused_pair_list_.end(),
                                 logic_id,
                                 compare);
    if (iter != ls_backup_ctx_->backup_retry_ctx_.reused_pair_list_.end()) {
      if (iter->logic_id_ != logic_id) {
        need_copy = true;
        LOG_DEBUG("do not find, need copy", K(logic_id));
      } else {
        LOG_DEBUG("find, no need copy", K(logic_id));
        need_copy = false;
        if (OB_FAIL(iter->physical_id_.get_backup_macro_block_index(iter->logic_id_, macro_index))) {
          LOG_WARN("failed to get backup macro block index", K(ret), K_(backup_data_type));
        }
      }
    } else {
      need_copy = true;
    }
  }
  return ret;
}

int ObPrefetchBackupInfoTask::inner_check_backup_item_need_copy_when_change_turn_(
    const ObBackupProviderItem &item, bool &need_copy, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  need_copy = true;
  if (!macro_index_store_for_turn_.is_inited()) {
    need_copy = true;
    LOG_DEBUG("macro index store for turn is not inited", K(ret));
  } else if (OB_FAIL(macro_index_store_for_turn_.get_macro_block_index(item.get_logic_id(), macro_index))) {
    LOG_WARN("inner check backup item need copy when change turn", K(ret), K(item), K(need_copy), K(macro_index));
    if (OB_ENTRY_NOT_EXIST == ret) {
      need_copy = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get macro block index", K(ret), K(item));
    }
  } else {
    need_copy = false;
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("backup_errsim", "reuse_macro_block",
                          "tablet_id", item.get_tablet_id(),
                          "table_key", item.get_table_key(),
                          "turn_id", param_.turn_id_,
                          "retry_id", param_.retry_id_,
                          "logic_id", item.get_logic_id(),
                          "macro_index", macro_index);
#endif
    LOG_INFO("inner check backup item need copy when change turn", K(item), K(need_copy), K(macro_index));
  }
  return ret;
}

int ObPrefetchBackupInfoTask::generate_next_prefetch_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPrefetchBackupInfoDag *child_dag = NULL;
  int64_t prefetch_task_id = 0;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObIDagNet *dag_net = NULL;
  if (OB_ISNULL(scheduler) || OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null MTL scheduler", K(ret), KP(scheduler), KP_(ls_backup_ctx));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), K(*this));
  } else if (OB_FAIL(scheduler->alloc_dag(child_dag))) {
    LOG_WARN("failed to alloc child dag", K(ret));
  } else if (OB_FAIL(ls_backup_ctx_->get_prefetch_task_id(prefetch_task_id))) {
    LOG_WARN("failed to get prefetch task id", K(ret));
  } else if (OB_FAIL(child_dag->init(prefetch_task_id,
                 param_,
                 backup_data_type_,
                 report_ctx_,
                 *ls_backup_ctx_,
                 *provider_,
                 *task_mgr_,
                 *index_kv_cache_,
                 index_rebuild_dag_))) {
    LOG_WARN("failed to init child dag", K(ret), K_(param));
  } else if (OB_FAIL(child_dag->create_first_task())) {
    LOG_WARN("failed to create first task for child dag", K(ret), KPC(child_dag));
  } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*child_dag))) {
    LOG_WARN("failed to add dag into dag net", K(ret), KPC(child_dag));
  } else if (OB_FAIL(dag_->add_child_without_inheritance(*child_dag))) {
    LOG_WARN("failed to alloc dependency dag", K(ret), KPC(dag_), KPC(child_dag));
  } else if (OB_FAIL(child_dag->add_child_without_inheritance(*index_rebuild_dag_))) {
    LOG_WARN("failed to alloc dependency dag", K(ret), KPC(child_dag), KPC_(index_rebuild_dag));
  } else if (OB_FAIL(scheduler->add_dag(child_dag))) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("failed to add dag", K(ret), KPC(dag_), KPC(child_dag));
    } else {
      LOG_WARN("may exist same dag", K(ret));
    }
  } else {
    LOG_INFO("success to alloc next prefetch dag", K(ret), K(prefetch_task_id), K_(param));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(scheduler) && OB_NOT_NULL(child_dag)) {
    scheduler->free_dag(*child_dag);
  }
  return ret;
}

int ObPrefetchBackupInfoTask::generate_backup_dag_(
    const int64_t task_id, const common::ObIArray<ObBackupProviderItem> &items)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSBackupDataDag *child_dag = NULL;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObIDagNet *dag_net = NULL;
  if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null MTL scheduler", K(ret), KP(scheduler));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), K(*this));
  } else if (OB_FAIL(scheduler->alloc_dag(child_dag))) {
    LOG_WARN("failed to alloc child dag", K(ret));
  } else if (OB_FAIL(child_dag->init(task_id,
                 param_,
                 backup_data_type_,
                 report_ctx_,
                 *ls_backup_ctx_,
                 *provider_,
                 *task_mgr_,
                 *index_kv_cache_,
                 index_rebuild_dag_))) {
    LOG_WARN("failed to init child dag", K(ret), K(task_id), K_(param));
  } else if (OB_FAIL(child_dag->provide(items))) {
    LOG_WARN("failed to provide items", K(ret), K(items));
  } else if (OB_FAIL(child_dag->create_first_task())) {
    LOG_WARN("failed to create first task for child dag", K(ret), KPC(child_dag));
  } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*child_dag))) {
    LOG_WARN("failed to add dag into dag net", K(ret), KPC(child_dag));
  } else if (OB_FAIL(dag_->add_child_without_inheritance(*child_dag))) {
    LOG_WARN("failed to alloc dependency dag", K(ret), KPC(dag_), KPC(child_dag));
  } else if (OB_FAIL(child_dag->add_child_without_inheritance(*index_rebuild_dag_))) {
    LOG_WARN("failed to add child without inheritance", K(ret), KPC_(index_rebuild_dag));
  } else {
#ifdef ERRSIM
    ret = OB_E(EventTable::EN_ADD_BACKUP_DATA_DAG_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("backup_errsim", "add_backup_data_dag_failed");
    }
#endif
    if (FAILEDx(scheduler->add_dag(child_dag))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to add dag", K(ret), KPC(dag_), KPC(child_dag));
      } else {
        LOG_WARN("may exist same dag", K(ret));
      }
    } else {
      LOG_INFO("success to alloc backup dag", K(ret), K(task_id), K_(backup_data_type), K(items));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(scheduler) && OB_NOT_NULL(child_dag)) {
    scheduler->free_dag(*child_dag);
  }
  return ret;
}

/* ObLSBackupDataTask */

ObLSBackupDataTask::ObLSBackupDataTask()
    : ObITask(ObITaskType::TASK_TYPE_MIGRATE_COPY_PHYSICAL),
      is_inited_(false),
      task_id_(-1),
      backup_stat_(),
      param_(),
      report_ctx_(),
      backup_data_type_(),
      backup_data_ctx_(),
      ls_backup_ctx_(NULL),
      provider_(NULL),
      task_mgr_(NULL),
      index_kv_cache_(NULL),
      index_builder_mgr_(NULL),
      disk_checker_(),
      allocator_(),
      backup_items_(),
      finished_tablet_list_(),
      index_rebuild_dag_(NULL),
      index_tree_io_fd_(),
      meta_tree_io_fd_(),
      rebuilder_mgr_()
{}

ObLSBackupDataTask::~ObLSBackupDataTask()
{}

// TODO(yangyi.yyy): extract parameters later in 4.1
int ObLSBackupDataTask::init(const int64_t task_id, const share::ObBackupDataType &backup_data_type,
    const common::ObIArray<ObBackupProviderItem> &backup_items, const ObLSBackupDataParam &param,
    const ObBackupReportCtx &report_ctx, ObLSBackupCtx &ls_backup_ctx, ObIBackupTabletProvider &provider,
    ObBackupMacroBlockTaskMgr &task_mgr, ObBackupIndexKVCache &kv_cache, share::ObIDag *index_rebuild_dag)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (task_id < 0 || !param.is_valid() || !report_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(task_id), K(param), K(report_ctx));
  } else if (OB_FAIL(backup_items_.assign(backup_items))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(backup_data_ctx_.open(param, backup_data_type, task_id, *ls_backup_ctx.bandwidth_throttle_))) {
    LOG_WARN("failed to open backup data ctx", K(ret), K(param), K(backup_data_type), KP(ls_backup_ctx.bandwidth_throttle_));
  } else if (OB_FAIL(disk_checker_.init(ls_backup_ctx.get_tablet_holder()))) {
    LOG_WARN("failed to init disk checker", K(ret));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    task_id_ = task_id;
    backup_stat_.ls_id_ = param.ls_id_;
    backup_stat_.backup_set_id_ = param.backup_set_desc_.backup_set_id_;
    backup_stat_.file_id_ = task_id_;
    report_ctx_ = report_ctx;
    ls_backup_ctx_ = &ls_backup_ctx;
    backup_data_type_ = backup_data_type;
    provider_ = &provider;
    task_mgr_ = &task_mgr;
    index_kv_cache_ = &kv_cache;
    index_rebuild_dag_ = index_rebuild_dag;
    index_builder_mgr_ = &ls_backup_ctx.index_builder_mgr_;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupDataTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  bool need_report_error = false;
  LOG_INFO("start backup data", K_(param), K_(task_id), K_(backup_data_type), K_(backup_items));
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    // case: 1011_same_sstable_data_on_different_retry.test
    int64_t failed_task_id = 0;
    if (0 == param_.retry_id_) {
      failed_task_id = 15;
    } else if (1 == param_.retry_id_) {
      failed_task_id = 16;
    } else if (2 == param_.retry_id_) {
      failed_task_id = 17;
    } else {
      // do nothing
    }
    if (!param_.ls_id_.is_sys_ls() && backup_data_type_.is_user_backup() && param_.retry_id_ <= 2 && task_id_ >= failed_task_id) {
      ret = OB_E(EventTable::EN_BACKUP_MULTIPLE_MACRO_BLOCK) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_macro_block_errsim",
                              "ls_id", param_.ls_id_.id(),
                              "tenant_id", param_.tenant_id_,
                              "turn_id", param_.turn_id_,
                              "retry_id", param_.retry_id_,
                              "task_id", task_id_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (param_.ls_id_.is_sys_ls()) {
      SERVER_EVENT_SYNC_ADD("backup_errsim", "before_backup_sys_tablets",
                            "ls_id", param_.ls_id_.id());
      DEBUG_SYNC(BEFORE_BACKUP_SYS_TABLETS);
    }
  }
#endif

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (backup_data_type_.is_user_backup() && 1002 == param_.ls_id_.id() && 1 == param_.turn_id_ && 0 == param_.retry_id_ && 1 == task_id_) {
      ret = EN_BACKUP_DATA_TASK_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "before_backup_data_task");
        DEBUG_SYNC(BEFORE_BACKUP_DATA_TASK);
      }
    }
  }
#endif

  ObArray<ObIODevice *> device_handle_array;
  ObBackupIntermediateTreeType index_tree_type = ObBackupIntermediateTreeType::BACKUP_INDEX_TREE;
  ObBackupWrapperIODevice *index_tree_device_handle = NULL;
  ObBackupIntermediateTreeType meta_tree_type = ObBackupIntermediateTreeType::BACKUP_META_TREE;
  ObBackupWrapperIODevice *meta_tree_device_handle = NULL;

  ObStorageIdMod mod;
  mod.storage_id_ = param_.dest_id_;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;

  if (OB_FAIL(ret)) {
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("log stream backup data task not init", K(ret));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
    ret = OB_CANCELED;
    LOG_INFO("already failed, no need backup again", K(ret), "result", ls_backup_ctx_->get_result_code());
  } else if (OB_FAIL(ObBackupUtils::check_ls_validity(param_.tenant_id_, param_.ls_id_))) {
    LOG_WARN("failed to check ls validity", K(ret), K_(param));
  } else if (OB_FAIL(ObBackupUtils::check_ls_valid_for_backup(
      param_.tenant_id_, param_.ls_id_, ls_backup_ctx_->rebuild_seq_))) {
    LOG_WARN("failed to check ls valid for backup", K(ret), K_(param));
  } else if (OB_FAIL(do_write_file_header_())) {
    LOG_WARN("failed to do write file header", K(ret));
  } else if (OB_FAIL(prepare_companion_index_file_handle_(
      task_id_, index_tree_type, mod, index_tree_io_fd_, index_tree_device_handle))) {
    LOG_WARN("failed to prepare companion index file handle for data file", K(ret), K_(task_id), K(index_tree_type));
  } else if (OB_FAIL(device_handle_array.push_back(index_tree_device_handle))) {
    LOG_WARN("failed to push back device handle", K(ret), KP(index_tree_device_handle));
  } else if (OB_FAIL(prepare_companion_index_file_handle_(
      task_id_, meta_tree_type, mod, meta_tree_io_fd_, meta_tree_device_handle))) {
    LOG_WARN("failed to prepare companion index file handle for data file", K(ret), K_(task_id), K(meta_tree_type));
  } else if (OB_FAIL(device_handle_array.push_back(meta_tree_device_handle))) {
    LOG_WARN("failed to push back device handle", K(ret), KP(meta_tree_device_handle));
  } else if (OB_FAIL(do_iterate_backup_items_(device_handle_array))) {
    LOG_WARN("failed to do iterate backup items", K(ret));
  } else if (OB_FAIL(ObBackupUtils::check_ls_valid_for_backup(
      param_.tenant_id_, param_.ls_id_, ls_backup_ctx_->rebuild_seq_))) {
    LOG_WARN("failed to check ls valid for backup", K(ret), K_(param));
  } else if (OB_FAIL(close_tree_device_handle_(index_tree_device_handle, meta_tree_device_handle))) {
    LOG_WARN("failed to close tree device handle", K(ret), KP(index_tree_device_handle), KP(meta_tree_device_handle));
  } else if (OB_FAIL(finish_task_in_order_())) {
    LOG_WARN("failed to finish task in order", K(ret));
  } else if (OB_FAIL(do_generate_next_task_())) {
    LOG_WARN("failed to do generate next task", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(index_tree_device_handle)) {
      if (OB_TMP_FAIL(index_tree_device_handle->abort(index_tree_io_fd_))) {
        LOG_WARN("fail to abort multipart upload", K(ret), K(tmp_ret), KP(index_tree_device_handle), K(index_tree_io_fd_));
      }

      if (OB_TMP_FAIL(ObBackupDeviceHelper::close_device_and_fd(index_tree_device_handle, index_tree_io_fd_))) {
        LOG_WARN("failed to close index tree device and fd", K(tmp_ret), K(ret));
      }
    }

    if (OB_NOT_NULL(meta_tree_device_handle)) {
      if (OB_TMP_FAIL(meta_tree_device_handle->abort(meta_tree_io_fd_))) {
        LOG_WARN("fail to abort multipart upload", K(ret), K(tmp_ret), KP(meta_tree_device_handle), K(meta_tree_io_fd_));
      }

      if (OB_TMP_FAIL(ObBackupDeviceHelper::close_device_and_fd(meta_tree_device_handle, meta_tree_io_fd_))) {
        LOG_WARN("failed to close meta tree device and fd", K(tmp_ret), K(ret));
      }
    }
  }
#ifdef ERRSIM
  if (FAILEDx(may_inject_simulated_error_())) {
    LOG_WARN("may inject simulated error", K(ret));
  }
#endif
  if (OB_FAIL(ret) && OB_NOT_NULL(ls_backup_ctx_)) {
    bool is_set = false;
    ls_backup_ctx_->set_result_code(ret, is_set);
    need_report_error = is_set;
  }
  if (OB_NOT_NULL(ls_backup_ctx_) && need_report_error) {
    const int64_t result = ls_backup_ctx_->get_result_code();
    if (OB_TMP_FAIL(ObLSBackupOperator::mark_ls_task_info_final(param_.job_desc_.task_id_,
                  param_.tenant_id_,
                  param_.ls_id_,
                  param_.turn_id_,
                  param_.retry_id_,
                  param_.backup_data_type_,
                  *report_ctx_.sql_proxy_))) {
      LOG_WARN("failed to mark ls task info final", K(ret), K_(param));
    } else {
      LOG_INFO("mark ls task info final", K(ret), K_(param), K_(task_id));
    }
    REPORT_TASK_RESULT(this->get_dag()->get_dag_id(), result);
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_ts;
  record_server_event_(cost_us);
  return ret;
}

int ObLSBackupDataTask::may_inject_simulated_error_()
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  if (0 == param_.retry_id_) {
    if (backup_data_type_.is_sys_backup()) {
      ret = OB_E(EventTable::EN_BACKUP_SYS_TABLET_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_sys_tablet");
        LOG_WARN("errsim backup sys tablet data task failed", K(ret));
      }
    } else if (backup_data_type_.is_user_backup()) {
      ret = OB_E(EventTable::EN_BACKUP_DATA_TABLET_MAJOR_SSTABLE_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_major_sstable");
        LOG_WARN("errsim backup data tablet major sstable task failed", K(ret));
      }
    }
  }
#endif
  return ret;
}

int ObLSBackupDataTask::build_backup_file_header_(ObBackupFileHeader &file_header)
{
  int ret = OB_SUCCESS;
  ObBackupFileMagic file_magic;
  const ObBackupFileType file_type = BACKUP_DATA_FILE;
  if (OB_FAIL(convert_backup_file_type_to_magic(file_type, file_magic))) {
    LOG_WARN("failed to convert backup file type to magic", K(ret), K(file_type));
  } else {
    file_header.magic_ = file_magic;
    file_header.version_ = BACKUP_DATA_VERSION_V1;
    file_header.file_type_ = file_type;
    file_header.reserved_ = 0;
  }
  return ret;
}

int ObLSBackupDataTask::do_write_file_header_()
{
  int ret = OB_SUCCESS;
  ObBackupFileHeader file_header;
  if (OB_FAIL(build_backup_file_header_(file_header))) {
    LOG_WARN("failed to build backup file header", K(ret));
  } else if (OB_FAIL(backup_data_ctx_.write_backup_file_header(file_header))) {
    LOG_WARN("failed to write backup file header", K(ret), K(file_header));
  }
  return ret;
}

// TODO(yanfeng): consider wrapping device_handle_array into a structure
int ObLSBackupDataTask::prepare_macro_block_readers_(ObMultiMacroBlockBackupReader *&macro_reader,
                                                     ObMultiMacroBlockBackupReader *&ddl_macro_reader,
                                                     common::ObIArray<ObIODevice *> &device_handle_array)
{
  int ret = OB_SUCCESS;
  macro_reader = NULL;
  ddl_macro_reader = NULL;
  ObArray<ObBackupMacroBlockId> ddl_macro_list;
  ObArray<ObBackupMacroBlockId> macro_list;
  ObArray<ObBackupProviderItem> item_list;
  if (OB_FAIL(get_ddl_block_id_list_(ddl_macro_list))) {
    LOG_WARN("failed to get macro block id list", K(ret));
  } else if (OB_UNLIKELY(ddl_macro_list.empty())) {
    LOG_INFO("no macro list need to backup");
  } else if (OB_FAIL(prepare_macro_block_reader_(param_.tenant_id_, ddl_macro_list, ddl_macro_reader))) {
    LOG_WARN("failed to prepare macro block reader", K(ret), K_(param), K(ddl_macro_list));
  }
  if (FAILEDx(get_macro_block_id_list_(macro_list, item_list))) {
    LOG_WARN("failed to get macro block id list", K(ret));
  } else if (OB_UNLIKELY(macro_list.empty())) {
    LOG_INFO("no macro list need to backup");
  } else if (OB_FAIL(prepare_macro_block_reader_(param_.tenant_id_, macro_list, macro_reader))) {
    LOG_WARN("failed to prepare macro block reader", K(ret), K_(param), K(macro_list));
  } else if (OB_FAIL(rebuilder_mgr_.init(item_list, index_builder_mgr_, device_handle_array))) {
    LOG_WARN("failed to init rebuilder mg", K(ret), K(item_list));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(macro_reader)) {
      ObLSBackupFactory::free(macro_reader);
    }
    if (OB_NOT_NULL(ddl_macro_reader)) {
      ObLSBackupFactory::free(ddl_macro_reader);
    }
  }
  return ret;
}

int ObLSBackupDataTask::do_iterate_backup_items_(common::ObIArray<ObIODevice *> &device_handle_array)
{
  int ret = OB_SUCCESS;
  ObMultiMacroBlockBackupReader *macro_reader = NULL;
  ObMultiMacroBlockBackupReader *ddl_macro_reader = NULL;
  if (OB_FAIL(prepare_macro_block_readers_(macro_reader, ddl_macro_reader, device_handle_array))) {
    LOG_WARN("failed to prepare readers", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_items_.count(); ++i) {
    const ObBackupProviderItem &item = backup_items_.at(i);
    if (REACH_TIME_INTERVAL(CHECK_DISK_SPACE_INTERVAL)) {
      if (OB_FAIL(check_disk_space_())) {
        LOG_WARN("failed to check disk space", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      switch (item.get_item_type()) {
        case PROVIDER_ITEM_TABLET_SSTABLE_INDEX_BUILDER_PREPARE: {
          if (OB_FAIL(do_prepare_sstable_builders_(item))) {
            LOG_WARN("failed to do prepare sstable builders", K(ret), K(item));
          }
          break;
        }
        case PROVIDER_ITEM_MACRO_ID: {
          if (OB_FAIL(do_backup_single_macro_block_data_(macro_reader, item, device_handle_array))) {
            LOG_WARN("failed to do backup single macro block data", K(ret), K(item));
          }
          break;
        }
        case PROVIDER_ITEM_DDL_OTHER_BLOCK_ID: {
          if (OB_FAIL(do_backup_single_ddl_other_block_(ddl_macro_reader, item))) {
            LOG_WARN("failed to do backup single ddl other block", K(ret), K(item));
          }
          break;
        }
        case PROVIDER_ITEM_TABLET_AND_SSTABLE_META: {
          const ObTabletID &tablet_id = item.get_tablet_id();
          const storage::ObITable::TableKey &table_key = item.get_table_key();
          if (OB_FAIL(deal_with_sstable_other_block_root_blocks_(tablet_id, table_key))) {
            LOG_WARN("failed to deal with ddl sstable root blocks", K(ret), K(tablet_id), K(table_key));
          } else if (OB_FAIL(do_backup_single_meta_data_(item, device_handle_array.at(0)))) {
            LOG_WARN("failed to do backup single meta data", K(ret), K(item));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("item type is not valid", K(ret), K(item));
          break;
        }
      }
    }
  }
  if (OB_NOT_NULL(macro_reader)) {
    ObLSBackupFactory::free(macro_reader);
  }
  if (OB_NOT_NULL(ddl_macro_reader)) {
    ObLSBackupFactory::free(ddl_macro_reader);
  }
  return ret;
}

int ObLSBackupDataTask::do_prepare_sstable_builders_(const ObBackupProviderItem &item)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = item.get_tablet_id();
  ObBackupTabletHandleRef *tablet_ref = NULL;
  ObArray<storage::ObSSTableWrapper> sstable_array;
  bool is_major_compaction_mview_dep_tablet = false;
  share::SCN mview_dep_scn;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(item));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(get_tablet_handle_(tablet_id, tablet_ref))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_ref->tablet_handle_.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret), K(tablet_id));
  } else if (OB_FAIL(ls_backup_ctx_->check_is_major_compaction_mview_dep_tablet(tablet_id, mview_dep_scn, is_major_compaction_mview_dep_tablet))) {
    LOG_WARN("failed to check is mview dep tablet", K(ret), K(tablet_id));
  } else if (OB_FAIL(ObBackupUtils::get_sstables_by_data_type(tablet_ref->tablet_handle_, backup_data_type_,
      *table_store_wrapper.get_member(), is_major_compaction_mview_dep_tablet, mview_dep_scn, sstable_array))) {
    LOG_WARN("failed to get sstables by data type", K(ret), KPC(tablet_ref), K_(backup_data_type));
  } else if (OB_FAIL(prepare_tablet_sstable_index_builders_(tablet_id, is_major_compaction_mview_dep_tablet, sstable_array))) {
    LOG_WARN("failed to prepare tablet sstable index builders", K(ret), K_(param), K(tablet_id), K(sstable_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      storage::ObSSTableWrapper &sstable_wrapper = sstable_array.at(i);
      ObSSTable *sstable_ptr = NULL;
      if (OB_ISNULL(sstable_ptr = sstable_wrapper.get_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret));
      } else {
        const ObITable::TableKey &table_key = sstable_ptr->get_key();
        if (GCTX.is_shared_storage_mode() && table_key.is_ddl_dump_sstable()) {
          // do nothing
        } else if (OB_FAIL(open_tablet_sstable_index_builder_(tablet_id, tablet_ref->tablet_handle_, table_key, sstable_ptr))) {
          LOG_WARN("failed to open tablet sstable index builder", K(ret), K(tablet_id), KPC(tablet_ref), K(table_key));
        }
      }
    }
  }
  return ret;
}

int ObLSBackupDataTask::prepare_tablet_sstable_index_builders_(const common::ObTabletID &tablet_id,
    const bool is_major_compaction_mview_dep_tablet, common::ObIArray<storage::ObSSTableWrapper> &sstable_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_builder_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index builder mgr should not be null", K(ret));
  } else {
    ObArray<storage::ObITable::TableKey> table_key_array;
    ARRAY_FOREACH_X(sstable_array, idx, cnt, OB_SUCC(ret)) {
      storage::ObSSTableWrapper &sstable_wrapper = sstable_array.at(idx);
      ObSSTable *sstable_ptr = NULL;
      if (OB_ISNULL(sstable_ptr = sstable_wrapper.get_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret));
      } else if (OB_FAIL(table_key_array.push_back(sstable_ptr->get_key()))) {
        LOG_WARN("failed to push back", K(ret), KPC(sstable_ptr));
      }
    }
    if (FAILEDx(index_builder_mgr_->prepare_sstable_index_builders(tablet_id, table_key_array, is_major_compaction_mview_dep_tablet))) {
      LOG_WARN("failed to open sstable index builder", K(ret), K(tablet_id), K(table_key_array), K(is_major_compaction_mview_dep_tablet));
    }
  }
  return ret;
}

int ObLSBackupDataTask::open_tablet_sstable_index_builder_(
    const common::ObTabletID &tablet_id, const storage::ObTabletHandle &tablet_handle,
    const storage::ObITable::TableKey &table_key, blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_builder_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index builder mgr should not be null", K(ret));
  } else if (OB_FAIL(index_builder_mgr_->open_sstable_index_builder(tablet_id, tablet_handle, table_key, sstable))) {
    LOG_WARN("failed to open sstable index builder", K(ret), K(tablet_id), K(tablet_handle), K(table_key), KPC(sstable));
  }
  return ret;
}

int ObLSBackupDataTask::do_wait_index_builder_ready_(const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  ObBackupTabletSSTableIndexBuilderMgr *mgr = NULL;
  bool exist = false;
  static const int64_t DEFAULT_SLEEP_US = 10_ms;
  while (OB_SUCC(ret)) {
    if (GCTX.is_shared_storage_mode() && table_key.is_ddl_dump_sstable()) {
      // ddl sstable in shared storage mode has no index builder
      break;
    } else if (OB_ISNULL(ls_backup_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls back ctx should not be null", K(ret));
    } else if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
      ret = OB_EAGAIN;
      LOG_WARN("ctx already failed", K(ret));
    } else if (OB_ISNULL(index_builder_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mgr should not be null", K(ret));
    } else if (OB_FAIL(index_builder_mgr_->check_sstable_index_builder_mgr_exist(tablet_id, table_key, exist))) {
      LOG_WARN("failed to check sstable index builder mgr exist", K(ret), K(tablet_id), K(table_key));
    } else if (!exist) {
      LOG_INFO("index builder mgr still not exist", K(ret), K(tablet_id), K(table_key));
      usleep(DEFAULT_SLEEP_US);
    } else {
      break;
    }
  }
  return ret;
}

int ObLSBackupDataTask::do_backup_single_macro_block_data_(ObMultiMacroBlockBackupReader *macro_reader,
    const ObBackupProviderItem &backup_item, common::ObIArray<ObIODevice *> &device_handle)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator io_allocator("BackupMacroBlk", OB_MALLOC_NORMAL_BLOCK_SIZE, param_.tenant_id_);
  ObBufferReader buffer_reader;
  common::ObTabletID tablet_id;
  ObITable::TableKey table_key;
  ObLogicMacroBlockId logic_id;
  MacroBlockId macro_id;
  ObBackupMacroBlockIndex macro_index;
  ObBackupDeviceMacroBlockId physical_id;
  bool need_copy = true; // when the macro block is neither in inc backup, or backed up in previous turn or retry, we backup it.
  bool need_reuse_for_mv = false; // when two major sstable share one macro block, we reuse it
#ifdef ERRSIM
  bool has_need_copy = false;
  ObLogicMacroBlockId first_logic_id;
#endif
  if (OB_ISNULL(macro_reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro reader should not be null", K(ret));
  } else if (OB_FAIL(do_wait_index_builder_ready_(backup_item.get_tablet_id(), backup_item.get_table_key()))) {
    LOG_WARN("failed to do wait index builder ready", K(ret), K(backup_item));
  } else if (OB_FAIL(get_next_macro_block_data_(macro_reader, buffer_reader, table_key, logic_id, macro_id, &io_allocator))) {
    if (OB_ITER_END == ret) {
      LOG_INFO("iterator meet end", K(logic_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next macro block data", K(ret));
    }
  } else if (FALSE_IT(tablet_id = table_key.tablet_id_)) {
  } else if (FALSE_IT(need_copy = backup_item.get_need_copy())) {
  } else if (OB_FAIL(check_need_reuse_sstable_macro_block_for_mv_(tablet_id, table_key, logic_id, need_reuse_for_mv))) {
    LOG_WARN("failed to check need reuse across sstable", K(ret));
  } else if (OB_FAIL(check_macro_block_data_(buffer_reader))) {
    LOG_WARN("failed to check macro block data", K(ret), K(buffer_reader));
  } else {
    if (need_reuse_for_mv) {
      if (OB_FAIL(wait_reuse_other_block_ready_(tablet_id, logic_id, macro_index))) {
        LOG_WARN("failed to wait reuse other block ready", K(ret), K(tablet_id), K(logic_id));
      } else {
        LOG_INFO("wait reuse other block ready", K(tablet_id), K(table_key), K(logic_id), K(macro_index));
      }
    } else {
      if (need_copy) {
        if (OB_FAIL(write_macro_block_data_(buffer_reader, table_key, logic_id, macro_index))) {
          LOG_WARN("failed to write macro block data", K(ret), K(buffer_reader), K(table_key), K(logic_id));
        } else {
          LOG_INFO("write macro block", K(tablet_id), K(table_key), K(logic_id), K(macro_index));
        }
      } else {
        macro_index = backup_item.get_macro_index();
      }
    }
    if (FAILEDx(macro_index.get_backup_physical_id(backup_data_type_, physical_id))) {
      LOG_WARN("failed to get backup physical id", K(ret), K_(backup_data_type), K(macro_index));
    } else if (OB_FAIL(prepare_index_block_rebuilder_if_need_(backup_item, &task_id_))) {
      LOG_WARN("failed to prepare index block rebuilder if need", K(ret));
    } else if (OB_FAIL(append_macro_row_to_rebuilder_(backup_item, buffer_reader, physical_id))) {
      LOG_WARN("failed to append macro row to rebuilder", K(ret), K(backup_item));
    } else if (OB_FAIL(update_logic_id_to_macro_index_(tablet_id, table_key, logic_id, macro_index))) {
      LOG_WARN("failed to update logic id to macro index", K(ret), K(logic_id), K(table_key), K(macro_index));
    } else if (OB_FAIL(close_index_block_rebuilder_if_need_(backup_item))) {
      LOG_WARN("failed to close index block rebuilder if need", K(ret));
    } else if (OB_FAIL(mark_backup_item_finished_(backup_item, physical_id))) {
      LOG_WARN("failed to mark backup item finished", K(ret), K(backup_item), K(macro_index), K(physical_id));
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      bool is_last = false;
      const int64_t ERRSIM_TASK_ID = GCONF.errsim_backup_task_id;
      const int64_t ERRSIM_TABLET_ID = GCONF.errsim_backup_tablet_id;
      if (task_id_ != ERRSIM_TASK_ID && backup_item.get_tablet_id().id() != ERRSIM_TABLET_ID && !backup_item.get_table_key().is_major_sstable()) {
        // do nothing
      } else if (OB_FAIL(rebuilder_mgr_.check_is_last_item_for_table_key(backup_item, is_last))) {
        LOG_WARN("failed to check is last item for table key", K(ret));
      } else if (is_last) {
        SERVER_EVENT_SYNC_ADD("backup", "BEFORE_CLOSE_BACKUP_INDEX_BUILDER",
                              "turn_id", param_.turn_id_,
                              "retry_id", param_.retry_id_,
                              "tablet_id", backup_item.get_tablet_id(),
                              "table_key", backup_item.get_table_key(),
                              "task_id", task_id_);
        DEBUG_SYNC(BEFORE_CLOSE_BACKUP_INDEX_BUILDER);
      }
    }
#endif

    if (FAILEDx(ls_backup_ctx_->stat_mgr_.add_macro_block(backup_item.get_backup_data_type(), logic_id, backup_item.get_need_copy()))) {
      LOG_WARN("failed to add macro block", K(ret), K(backup_item));
    } else if (OB_FAIL(ls_backup_ctx_->stat_mgr_.add_bytes(backup_data_type_, macro_index.length_))) {
      LOG_WARN("failed to add bytes", K(ret));
    } else {
#ifdef ERRSIM
        if (need_copy && table_key.is_major_sstable()) {
          if (!has_need_copy) {
            has_need_copy = true;
            first_logic_id = logic_id;
          }
        }
#endif
      backup_stat_.input_bytes_ += OB_DEFAULT_MACRO_BLOCK_SIZE;
      backup_stat_.finish_macro_block_count_ += 1;
    }
  }
#ifdef ERRSIM
  if (has_need_copy) {
    SERVER_EVENT_SYNC_ADD("backup_data", "first_need_copy_logic_id",
                          "tenant_id", param_.tenant_id_,
                          "backup_set_id", param_.backup_set_desc_.backup_set_id_,
                          "ls_id", param_.ls_id_.id(),
                          "turn_id", param_.turn_id_,
                          "retry_id", param_.retry_id_,
                          "first_logic_id", first_logic_id,
                          to_cstring(task_id_));
  }
#endif
  return ret;
}

int ObLSBackupDataTask::do_backup_single_ddl_other_block_(
    ObMultiMacroBlockBackupReader *reader, const ObBackupProviderItem &item)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator io_allocator("BackupDDLBlock", OB_MALLOC_NORMAL_BLOCK_SIZE, param_.tenant_id_);
  ObBufferReader buffer_reader;
  ObITable::TableKey table_key;
  ObLogicMacroBlockId unused_logic_id;
  MacroBlockId macro_id;
  ObBackupLinkedBlockAddr physical_id;
  ObTabletID tablet_id;
  ObBackupOtherBlocksMgr *other_block_mgr = NULL;
  ObBackupLinkedBlockItemWriter *linked_writer = NULL;

  if (OB_FAIL(get_next_macro_block_data_(reader,
                                         buffer_reader,
                                         table_key,
                                         unused_logic_id,
                                         macro_id,
                                         &io_allocator))) {
    if (OB_ITER_END == ret) {
      LOG_INFO("iterator meet end");
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next macro block data", K(ret));
    }
  } else if (OB_FAIL(check_macro_block_data_(buffer_reader))) {
    LOG_WARN("failed to check macro block data", K(ret), K(buffer_reader));
  } else if (OB_FAIL(get_tablet_id_for_macro_id_(macro_id, tablet_id))) {
    LOG_WARN("failed to get tablet id for macro id", K(ret), K(macro_id));
  } else if (OB_FAIL(get_other_block_mgr_for_tablet_(tablet_id, other_block_mgr, linked_writer))) {
    LOG_WARN("failed to get other block mgr for tablet", K(ret), K(tablet_id));
  } else if (OB_FAIL(write_ddl_other_block_(buffer_reader, physical_id))) {
    LOG_WARN("failed to write ddl other blocks", K(ret));
  } else if (OB_FAIL(add_item_to_other_block_mgr_(macro_id, physical_id, other_block_mgr))) {
    LOG_WARN("failed to add item to other block mgr", K(ret), K(macro_id), K(physical_id));
  } else {
    backup_stat_.input_bytes_ += OB_DEFAULT_MACRO_BLOCK_SIZE;
    backup_stat_.finish_macro_block_count_ += 1;
  }
  return ret;
}

int ObLSBackupDataTask::get_ddl_block_id_list_(common::ObIArray<ObBackupMacroBlockId> &list)
{
  int ret = OB_SUCCESS;
  list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_items_.count(); ++i) {
    const ObBackupProviderItem &item = backup_items_.at(i);
    ObBackupMacroBlockId macro_id;
    macro_id.macro_block_id_ = item.get_macro_block_id();
    macro_id.nested_offset_ = item.get_nested_offset();
    macro_id.nested_size_ = item.get_nested_size();
    macro_id.is_ss_ddl_other_block_ = true;
    if (PROVIDER_ITEM_DDL_OTHER_BLOCK_ID == item.get_item_type()) {
      if (OB_FAIL(list.push_back(macro_id))) {
        LOG_WARN("failed to push back", K(ret), K(macro_id));
      }
    }
  }
  return ret;
}

int ObLSBackupDataTask::get_other_block_mgr_for_tablet_(const common::ObTabletID &tablet_id,
    ObBackupOtherBlocksMgr *&other_block_mgr, ObBackupLinkedBlockItemWriter *&linked_writer)
{
  int ret = OB_SUCCESS;
  other_block_mgr = NULL;
  ObBackupTabletStat *tablet_stat = NULL;
  ObBackupTabletCtx *tablet_ctx = NULL;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (FALSE_IT(tablet_stat = &ls_backup_ctx_->tablet_stat_)) {
  } else if (OB_FAIL(tablet_stat->get_tablet_stat(tablet_id, tablet_ctx))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  } else {
    other_block_mgr = &tablet_ctx->other_block_mgr_;
    linked_writer = &tablet_ctx->linked_writer_;
  }
  return ret;
}

int ObLSBackupDataTask::write_ddl_other_block_(const blocksstable::ObBufferReader &buffer_reader, ObBackupLinkedBlockAddr &physical_id)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  int64_t length = 0;
  if (OB_FAIL(backup_data_ctx_.write_other_block(buffer_reader, offset, length))) {
    LOG_WARN("failed to write ddl other block", K(ret), K(buffer_reader));
  } else {
    ObBackupDataType backup_data_type;
    backup_data_type.set_user_data_backup();
    physical_id.backup_set_id_ = param_.backup_set_desc_.backup_set_id_;
    physical_id.ls_id_ = param_.ls_id_.id();
    physical_id.turn_id_ = param_.turn_id_;
    physical_id.retry_id_ = param_.retry_id_;
    physical_id.file_id_ = task_id_;
    physical_id.data_type_ = backup_data_type.type_;
    physical_id.offset_ = offset / DIO_READ_ALIGN_SIZE;
    physical_id.length_ = length / DIO_READ_ALIGN_SIZE;
    physical_id.block_type_ = ObBackupDeviceMacroBlockId::DATA_BLOCK;
    physical_id.id_mode_ = static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_BACKUP);
    physical_id.version_ = ObBackupDeviceMacroBlockId::BACKUP_MACRO_BLOCK_ID_VERSION;
  }
  return ret;
}

int ObLSBackupDataTask::add_item_to_other_block_mgr_(const blocksstable::MacroBlockId &macro_id,
    const ObBackupLinkedBlockAddr &physical_id, ObBackupOtherBlocksMgr *other_block_mgr)
{
  int ret = OB_SUCCESS;
  ObBackupLinkedItem linked_item;
  linked_item.macro_id_ = macro_id;
  linked_item.backup_id_ = physical_id;
  if (!linked_item.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("linked item is not valid", K(ret));
  } else if (OB_ISNULL(other_block_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("other block mgr should not be null", K(ret), KP(other_block_mgr));
  } else if (OB_FAIL(other_block_mgr->add_item(linked_item))) {
    LOG_WARN("failed to add item", K(ret), K(linked_item));
  } else {
    LOG_INFO("add item to other block mgr", K(linked_item));
  }
  return ret;
}

int ObLSBackupDataTask::deal_with_sstable_other_block_root_blocks_(
    const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  ObBackupOtherBlocksMgr *other_block_mgr = NULL;
  ObBackupLinkedBlockItemWriter *linked_writer = NULL;
  if (!GCTX.is_shared_storage_mode()) {
    // do nothing
  } else if (!table_key.is_ddl_dump_sstable()) {
    // do nothing
  } else if (OB_FAIL(get_other_block_mgr_for_tablet_(tablet_id, other_block_mgr, linked_writer))) {
    LOG_WARN("failed to get other block mgr for tablet", K(ret), K(tablet_id), K(table_key));
  } else if (OB_FAIL(linked_writer->init(param_, tablet_id, table_key, task_id_,
      backup_data_ctx_.file_write_ctx_, backup_data_ctx_.file_offset_))) {
    LOG_WARN("failed to init backup linked block item writer", K(ret), K(tablet_id), K(table_key));
  } else if (OB_FAIL(other_block_mgr->wait(ls_backup_ctx_))) {
    LOG_WARN("failed to wait other block mgr", K(ret), KP_(ls_backup_ctx));
  } else {
    ObBackupLinkedItem link_item;
    while (OB_SUCC(ret)) {
      link_item.reset();
      if (OB_FAIL(other_block_mgr->get_next_item(link_item))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next item", K(ret));
        }
      } else if (OB_FAIL(linked_writer->write(link_item))) {
        LOG_WARN("failed to write link item", K(ret));
      } else {
        LOG_INFO("write link item", K(tablet_id), K(table_key), K(link_item));
      }
    }
    if (FAILEDx(linked_writer->close())) {
      LOG_WARN("failed to close item writer", K(ret));
    }
    LOG_INFO("deal with ddl sstable root blocks", K(tablet_id), K(table_key));
  }
  return ret;
}

int ObLSBackupDataTask::get_sstable_meta_item_list_(common::ObIArray<ObBackupProviderItem> &list)
{
  int ret = OB_SUCCESS;
  list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_items_.count(); ++i) {
    const ObBackupProviderItem &item = backup_items_.at(i);
    if (PROVIDER_ITEM_TABLET_AND_SSTABLE_META == item.get_item_type()) {
      if (OB_FAIL(list.push_back(item))) {
        LOG_WARN("failed to push back", K(ret), K(item));
      }
    }
  }
  return ret;
}

int ObLSBackupDataTask::get_tablet_id_for_macro_id_(const blocksstable::MacroBlockId &macro_id, common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ARRAY_FOREACH_X(backup_items_, idx, cnt, OB_SUCC(ret)) {
    const ObBackupProviderItem &item = backup_items_.at(idx);
    if (item.get_macro_block_id() == macro_id) {
      tablet_id = item.get_tablet_id();
      found = true;
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup item should exist", K_(backup_items), K(macro_id));
  }
  return ret;
}

int ObLSBackupDataTask::do_backup_single_meta_data_(const ObBackupProviderItem &item, ObIODevice *device_handle)
{
  int ret = OB_SUCCESS;
  ObBackupTabletStat *tablet_stat = NULL;
  const common::ObTabletID &tablet_id = item.get_tablet_id();
  const share::ObBackupDataType &backup_data_type = item.get_backup_data_type();
  ObBackupTabletHandleRef *tablet_ref = NULL;
  bool can_release = false;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (FALSE_IT(tablet_stat = &ls_backup_ctx_->tablet_stat_)) {
  } else if (OB_FAIL(get_tablet_handle_(tablet_id, tablet_ref))) {
    LOG_WARN("failed to get tablet handle", K(ret), K_(task_id), K(tablet_id), K(item));
  } else if (OB_FAIL(do_wait_sstable_index_builder_ready_(tablet_ref->tablet_handle_))) {
    LOG_WARN("failed to wait sstable index builder ready", K(ret), K(item));
  } else if (OB_FAIL(do_backup_tablet_meta_(SSTABLE_META_READER, BACKUP_SSTABLE_META,
      backup_data_type, tablet_id, tablet_ref->tablet_handle_, device_handle))) {
    LOG_WARN("failed to backup sstable meta", K(ret), K(backup_data_type), K(tablet_id), KPC(tablet_ref));
  } else if (OB_FAIL(do_backup_tablet_meta_(TABLET_META_READER, BACKUP_TABLET_META,
      backup_data_type, tablet_id, tablet_ref->tablet_handle_, device_handle))) {
    LOG_WARN("failed to backup tablet meta", K(ret), K(backup_data_type), K(tablet_id), KPC(tablet_ref));
  } else if (OB_FAIL(tablet_stat->add_finished_tablet_meta_count(tablet_id))) {
    LOG_WARN("failed to add finished tablt meta count", K(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_stat->check_can_release_tablet(tablet_id, can_release))) {
    LOG_WARN("failed to check can release tablet", K(ret), K(tablet_id));
  } else if (!can_release) {
    // do nothing
  } else if (OB_FAIL(tablet_stat->free_tablet_stat(tablet_id))) {
    LOG_WARN("failed to free tablet stat", K(ret), K_(finished_tablet_list), K(tablet_id));
  } else if (OB_FAIL(release_tablet_handle_(tablet_id))) {
    LOG_WARN("failed to release tablet handle", K(ret), K(tablet_id));
  } else if (OB_FAIL(remove_sstable_index_builder_(tablet_id))) {
    LOG_WARN("failed to remove sstable index builder", K(ret), K(tablet_id));
  }
  return ret;
}

int ObLSBackupDataTask::do_wait_sstable_index_builder_ready_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  bool is_major_compaction_mview_dep = false;
  share::SCN mview_dep_scn;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  common::ObArray<storage::ObSSTableWrapper> sstable_array;
  const ObTabletTableStore *table_store = nullptr;
  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is nullptr.", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("failed to get table store", K(ret), KPC(tablet));
  } else if (OB_FAIL(ls_backup_ctx_->check_is_major_compaction_mview_dep_tablet(tablet->get_tablet_id(), mview_dep_scn, is_major_compaction_mview_dep))) {
    LOG_WARN("failed to check is mview dep tablet", K(ret), KPC(tablet));
  } else if (OB_FAIL(ObBackupUtils::get_sstables_by_data_type(
      tablet_handle, backup_data_type_, *table_store_wrapper.get_member(), is_major_compaction_mview_dep, mview_dep_scn, sstable_array))) {
    LOG_WARN("failed to get sstables by data type", K(ret), K(tablet_handle));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      storage::ObSSTableWrapper &sstable_wrapper = sstable_array.at(i);
      ObSSTable *sstable_ptr = NULL;
      if (OB_ISNULL(sstable_ptr = sstable_wrapper.get_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret));
      } else {
        if (OB_FAIL(do_wait_index_builder_ready_(tablet->get_tablet_id(), sstable_ptr->get_key()))) {
          LOG_WARN("failed to do wait index builder ready", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLSBackupDataTask::do_backup_tablet_meta_(const ObTabletMetaReaderType reader_type,
    const ObBackupMetaType meta_type, const share::ObBackupDataType &backup_data_type,
    const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle, ObIODevice *device_handle)
{
  int ret = OB_SUCCESS;
  ObITabletMetaBackupReader *reader = NULL;
  ObBufferReader buffer_reader;
  ObBackupMetaIndex meta_index;
  const ObBackupDeviceMacroBlockId physical_id = ObBackupDeviceMacroBlockId::get_default();
  if (OB_FAIL(prepare_tablet_meta_reader_(tablet_id, reader_type, backup_data_type, tablet_handle, device_handle, reader))) {
    LOG_WARN("failed to prepare tablet meta reader", K(tablet_id), K(reader_type));
  } else if (OB_FAIL(reader->get_meta_data(buffer_reader))) {
    LOG_WARN("failed to get meta data", K(ret), K(tablet_id));
  } else if (OB_FAIL(write_backup_meta_(buffer_reader, tablet_id, meta_type, meta_index))) {
    LOG_WARN("failed to write backup meta", K(ret), K(buffer_reader), K(tablet_id), K(meta_type));
  } else {
    backup_stat_.input_bytes_ += buffer_reader.capacity();
    if (BACKUP_TABLET_META == meta_type) {
      backup_stat_.finish_tablet_count_ += 1;
      ls_backup_ctx_->stat_mgr_.add_tablet_meta(backup_data_type_, tablet_id);
      ls_backup_ctx_->stat_mgr_.add_bytes(backup_data_type_, meta_index.length_);
#ifdef ERRSIM
      SERVER_EVENT_SYNC_ADD("backup", "backup_tablet_meta",
                            "ls_id", param_.ls_id_.id(),
                            "tablet_id", tablet_id.id());
#endif
    } else if (BACKUP_SSTABLE_META == meta_type) {
      backup_stat_.finish_sstable_count_ += 1;
      ls_backup_ctx_->stat_mgr_.add_sstable_meta(backup_data_type_, tablet_id);
      ls_backup_ctx_->stat_mgr_.add_bytes(backup_data_type_, meta_index.length_);
#ifdef ERRSIM
      SERVER_EVENT_SYNC_ADD("backup", "backup_sstable_meta",
                            "ls_id", param_.ls_id_.id(),
                            "tablet_id", tablet_id.id());
#endif
    }
  }
  if (OB_NOT_NULL(reader)) {
    ObLSBackupFactory::free(reader);
  }
  return ret;
}

int ObLSBackupDataTask::finish_task_in_order_()
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  SERVER_EVENT_ADD("backup_data", "finish_task_in_order",
                   "task_id", task_id_);
  DEBUG_SYNC(BEFORE_BACKUP_TASK_FINISH);
#endif
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(ls_backup_ctx_->wait_task(task_id_))) {
    LOG_WARN("failed to wait task", K(ret), K_(task_id));
  } else if (OB_FAIL(backup_data_ctx_.close())) {
    LOG_WARN("failed to close backup data ctx", K(ret), K_(param));
  } else if (OB_FALSE_IT(backup_stat_.output_bytes_ = backup_data_ctx_.get_file_size())) {
  } else if (OB_FAIL(report_ls_backup_task_info_(backup_stat_))) {
    LOG_WARN("failed to report ls backup task info", K(ret));
  } else if (OB_FAIL(ls_backup_ctx_->finish_task(task_id_))) {
    LOG_WARN("failed to finish task", K(ret), K_(task_id));
  } else {
    FLOG_INFO("finish task in order", K_(task_id), K_(param));
  }
  return ret;
}

int ObLSBackupDataTask::report_ls_backup_task_info_(const ObLSBackupStat &stat)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t max_file_id = 0;
  const bool for_update = true;
  if (OB_FAIL(trans.start(report_ctx_.sql_proxy_, gen_meta_tenant_id(param_.tenant_id_)))) {
    LOG_WARN("failed to start transaction", K(ret), K(param_));
  } else {
    const int64_t job_id = param_.job_desc_.job_id_;
    const int64_t task_id = param_.job_desc_.task_id_;
    const uint64_t tenant_id = param_.tenant_id_;
    const share::ObLSID &ls_id = param_.ls_id_;
    const int64_t turn_id = param_.turn_id_;
    const int64_t retry_id = param_.retry_id_;
    const share::ObBackupDataType &backup_data_type = param_.backup_data_type_;
    share::ObBackupSetTaskAttr old_set_task_attr;
    share::ObBackupLSTaskAttr old_ls_task_attr;
    ObBackupLSTaskInfo old_ls_task_info;
    ObLSBackupStat new_ls_task_info_stat;
    share::ObBackupStats new_ls_task_stat;
    share::ObBackupStats new_backup_set_stats;
    if (OB_FAIL(ObBackupTaskOperator::get_backup_task(trans, job_id, tenant_id, for_update, old_set_task_attr))) {
      LOG_WARN("failed to get backup task", K(ret), K_(param));
    } else if (OB_FAIL(ObBackupLSTaskOperator::get_ls_task(trans, for_update,
              task_id, tenant_id, ls_id, old_ls_task_attr))) {
      LOG_WARN("failed to get ls task", K(ret), K_(param));
    } else if (OB_FAIL(ObLSBackupOperator::get_backup_ls_task_info(tenant_id,
        task_id, ls_id, turn_id, retry_id, backup_data_type, for_update, old_ls_task_info, trans))) {
      LOG_WARN("failed to get backup ls task info", K(ret), K(param_));
    } else if (old_ls_task_info.is_final_) {
      ret = OB_CANCELED;
      LOG_INFO("can not update if final", K(old_ls_task_info), K(stat));
    } else if (old_ls_task_info.max_file_id_ + 1 != stat.file_id_) {
      LOG_INFO("can not update if file id is not consecutive", K(old_ls_task_info), K(stat));
    } else if (OB_FAIL(update_task_stat_(old_set_task_attr.stats_, stat, new_backup_set_stats))) {
      LOG_WARN("failed to update task stat", K(ret));
    } else if (OB_FAIL(update_ls_task_stat_(old_ls_task_attr.stats_, stat, new_ls_task_stat))) {
      LOG_WARN("failed to update ls task stat", K(ret), K(old_ls_task_attr));
    } else if (OB_FAIL(update_ls_task_info_stat_(old_ls_task_info, stat, new_ls_task_info_stat))) {
      LOG_WARN("failed to update ls task info stat", K(ret), K(old_ls_task_info), K(stat));
    } else if (OB_FAIL(ObBackupLSTaskOperator::update_stats(trans, task_id, tenant_id, ls_id, new_ls_task_stat))) {
      LOG_WARN("failed to update stat", K(ret), K(param_));
    } else if (OB_FAIL(ObLSBackupOperator::report_ls_backup_task_info(tenant_id,
        task_id, turn_id, retry_id, backup_data_type, new_ls_task_info_stat, trans))) {
      LOG_WARN("failed to report single task info", K_(param), K_(backup_data_type), K(new_ls_task_info_stat));
    } else if (OB_FAIL(ObBackupTaskOperator::update_stats(trans, task_id, tenant_id, new_backup_set_stats))) {
      LOG_WARN("failed to update stats", K(ret), K(task_id), K(tenant_id));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        LOG_WARN("failed to commit", K(ret));
      }
    } else {
      if (OB_TMP_FAIL(trans.end(false /* commit*/))) {
        LOG_WARN("failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLSBackupDataTask::update_task_stat_(const share::ObBackupStats &old_backup_stat, const ObLSBackupStat &ls_stat,
    share::ObBackupStats &new_backup_stat)
{
  int ret = OB_SUCCESS;
  new_backup_stat.input_bytes_ = old_backup_stat.input_bytes_ + ls_stat.input_bytes_;
  new_backup_stat.output_bytes_ = old_backup_stat.output_bytes_ + ls_stat.output_bytes_;
  new_backup_stat.macro_block_count_ = old_backup_stat.macro_block_count_;
  new_backup_stat.tablet_count_ = old_backup_stat.tablet_count_;
  new_backup_stat.finish_macro_block_count_ = old_backup_stat.finish_macro_block_count_ + ls_stat.finish_macro_block_count_;
  new_backup_stat.finish_tablet_count_ = old_backup_stat.finish_tablet_count_ + ls_stat.finish_tablet_count_;
  // calibrate tablet_count and macro_block_count
  if (new_backup_stat.finish_macro_block_count_ > new_backup_stat.macro_block_count_) {
    LOG_INFO("calibrate macro block count", "old_macro_block_count", new_backup_stat.macro_block_count_,
                                           "new_macro_block_count", new_backup_stat.finish_macro_block_count_);
    new_backup_stat.macro_block_count_ = new_backup_stat.finish_macro_block_count_;
  }
  if (new_backup_stat.finish_tablet_count_ > new_backup_stat.tablet_count_) {
    LOG_INFO("calibrate tablet count", "old_tablet_count", new_backup_stat.tablet_count_,
                                       "new_tablet_count", new_backup_stat.finish_tablet_count_);
    new_backup_stat.tablet_count_ = new_backup_stat.finish_tablet_count_;
  }
  return ret;
}

int ObLSBackupDataTask::update_ls_task_stat_(const share::ObBackupStats &old_backup_stat, const ObLSBackupStat &ls_stat,
    share::ObBackupStats &new_backup_stat)
{
  int ret = OB_SUCCESS;
  new_backup_stat.input_bytes_ = old_backup_stat.input_bytes_ + ls_stat.input_bytes_;
  new_backup_stat.output_bytes_ = old_backup_stat.output_bytes_ + ls_stat.output_bytes_;
  new_backup_stat.tablet_count_ = old_backup_stat.tablet_count_;
  new_backup_stat.macro_block_count_ = old_backup_stat.macro_block_count_;
  new_backup_stat.finish_macro_block_count_ = old_backup_stat.finish_macro_block_count_ + ls_stat.finish_macro_block_count_;
  new_backup_stat.finish_tablet_count_ = old_backup_stat.finish_tablet_count_ + ls_stat.finish_tablet_count_;
  // calibrate tablet_count and macro_block_count
  if (new_backup_stat.finish_macro_block_count_ > new_backup_stat.macro_block_count_) {
    LOG_INFO("calibrate macro block count", "old_macro_block_count", new_backup_stat.macro_block_count_,
                                            "new_macro_block_count", new_backup_stat.finish_macro_block_count_);
    new_backup_stat.macro_block_count_ = new_backup_stat.finish_macro_block_count_;
  }
  return ret;
}

int ObLSBackupDataTask::update_ls_task_info_stat_(
    const ObBackupLSTaskInfo &task_info, const ObLSBackupStat &stat, ObLSBackupStat &new_stat)
{
  int ret = OB_SUCCESS;
  new_stat.ls_id_ = task_info.ls_id_;
  new_stat.backup_set_id_ = task_info.backup_set_id_;
  new_stat.file_id_ = std::max(task_info.max_file_id_, stat.file_id_);
  new_stat.input_bytes_ = task_info.input_bytes_ + stat.input_bytes_;
  new_stat.output_bytes_ = task_info.output_bytes_ + stat.output_bytes_;
  new_stat.finish_macro_block_count_ = task_info.finish_macro_block_count_ + stat.finish_macro_block_count_;
  new_stat.finish_tablet_count_ = task_info.finish_tablet_count_ + stat.finish_tablet_count_;
  return ret;
}

int ObLSBackupDataTask::do_generate_next_task_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_finished = false;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
    // do nothing
  } else if (OB_FAIL(check_backup_finish_(is_finished))) {
    LOG_WARN("failed to check backup finish", K(ret));
  } else if (is_finished) {
    // do nothing
  } else if (OB_FAIL(do_generate_next_backup_dag_())) {
    LOG_WARN("failed to generate backup dag", K(ret));
  } else {
    LOG_INFO("do generate next backup dag", K_(task_id), K_(backup_data_type), K_(param));
  }
  return ret;
}

int ObLSBackupDataTask::check_disk_space_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(disk_checker_.check_disk_space())) {
    LOG_WARN("failed to check disk space", K(ret));
  } else {
    LOG_INFO("check disk space");
  }
  return ret;
}

int ObLSBackupDataTask::get_macro_block_id_list_(common::ObIArray<ObBackupMacroBlockId> &macro_list,
    common::ObIArray<ObBackupProviderItem> &item_list)
{
  int ret = OB_SUCCESS;
  macro_list.reset();
  item_list.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < backup_items_.count(); ++i) {
    const ObBackupProviderItem &item = backup_items_.at(i);
    ObBackupMacroBlockId macro_id;
    macro_id.table_key_ = item.get_table_key();
    macro_id.logic_id_ = item.get_logic_id();
    macro_id.macro_block_id_ = item.get_macro_block_id();
    macro_id.nested_offset_ = item.get_nested_offset();
    macro_id.nested_size_ = item.get_nested_size();
    macro_id.absolute_row_offset_ = item.get_absolute_row_offset();
    macro_id.is_ss_ddl_other_block_ = false;
    if (PROVIDER_ITEM_MACRO_ID == item.get_item_type()) {
      if (OB_FAIL(macro_list.push_back(macro_id))) {
        LOG_WARN("failed to push back", K(ret), K(macro_id));
      } else if (OB_FAIL(item_list.push_back(item))) {
        LOG_WARN("failed to push back", K(ret), K(item));
      }
    }
  }
  return ret;
}

int ObLSBackupDataTask::get_need_copy_macro_block_id_list_(common::ObIArray<ObBackupMacroBlockId> &list)
{
  int ret = OB_SUCCESS;
  list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_items_.count(); ++i) {
    const ObBackupProviderItem &item = backup_items_.at(i);
    ObBackupMacroBlockId macro_id;
    macro_id.table_key_ = item.get_table_key();
    macro_id.logic_id_ = item.get_logic_id();
    macro_id.macro_block_id_ = item.get_macro_block_id();
    macro_id.nested_offset_ = item.get_nested_offset();
    macro_id.nested_size_ = item.get_nested_size();
    macro_id.absolute_row_offset_ = item.get_absolute_row_offset();
    if ((PROVIDER_ITEM_MACRO_ID == item.get_item_type())
        && item.get_need_copy()) {
      if (OB_FAIL(list.push_back(macro_id))) {
        LOG_WARN("failed to push back", K(ret), K(macro_id));
      }
    }
  }
  return ret;
}

int ObLSBackupDataTask::get_meta_item_list_(common::ObIArray<ObBackupProviderItem> &list)
{
  int ret = OB_SUCCESS;
  list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_items_.count(); ++i) {
    const ObBackupProviderItem &item = backup_items_.at(i);
    if (PROVIDER_ITEM_TABLET_AND_SSTABLE_META == item.get_item_type()) {
      if (OB_FAIL(list.push_back(item))) {
        LOG_WARN("failed to push back", K(ret), K(item));
      }
    }
  }
  return ret;
}

int ObLSBackupDataTask::prepare_macro_block_reader_(const uint64_t tenant_id,
    const common::ObIArray<ObBackupMacroBlockId> &list, ObMultiMacroBlockBackupReader *&reader)
{
  int ret = OB_SUCCESS;
  ObMultiMacroBlockBackupReader *tmp_reader = NULL;
  const ObMacroBlockReaderType reader_type = LOCAL_MACRO_BLOCK_READER;
  if (OB_UNLIKELY(list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret));
  } else if (OB_ISNULL(tmp_reader = ObLSBackupFactory::get_multi_macro_block_backup_reader(param_.tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc iterator", K(ret));
  } else if (OB_FAIL(tmp_reader->init(tenant_id, reader_type, list))) {
    LOG_WARN("failed to init", K(ret), K(list));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
    LOG_INFO("prepare macro block reader", K(list));
  }
  if (OB_NOT_NULL(tmp_reader)) {
    ObLSBackupFactory::free(tmp_reader);
  }
  return ret;
}

int ObLSBackupDataTask::prepare_tablet_meta_reader_(const common::ObTabletID &tablet_id,
    const ObTabletMetaReaderType &reader_type, const share::ObBackupDataType &backup_data_type,
    storage::ObTabletHandle &tablet_handle, ObIODevice *device_handle, ObITabletMetaBackupReader *&reader)
{
  int ret = OB_SUCCESS;
  ObITabletMetaBackupReader *tmp_reader = NULL;
  ObBackupOtherBlocksMgr *other_block_mgr = NULL;
  ObBackupLinkedBlockItemWriter *linked_writer = NULL;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tmp_reader = ObLSBackupFactory::get_tablet_meta_backup_reader(reader_type, param_.tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc iterator", K(ret), K(reader_type));
  } else if (OB_FAIL(get_other_block_mgr_for_tablet_(tablet_id, other_block_mgr, linked_writer))) {
    LOG_WARN("failed to get other block mgr for tablet", K(ret), K(tablet_id));
  } else if (OB_FAIL(tmp_reader->init(tablet_id, backup_data_type,
      tablet_handle, *index_builder_mgr_, *ls_backup_ctx_, device_handle, linked_writer))) {
    LOG_WARN("failed to init", K(ret), K(tablet_id), K_(backup_data_type));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
    LOG_INFO("prepare tablet meta reader", K(tablet_id), K(reader_type));
  }
  if (OB_NOT_NULL(tmp_reader)) {
    ObLSBackupFactory::free(tmp_reader);
  }
  return ret;
}

int ObLSBackupDataTask::get_next_macro_block_data_(ObMultiMacroBlockBackupReader *reader, ObBufferReader &buffer_reader,
    storage::ObITable::TableKey &table_key, blocksstable::ObLogicMacroBlockId &logic_id, blocksstable::MacroBlockId &macro_id, ObIAllocator *io_allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret));
  } else if (OB_FAIL(reader->get_next_macro_block(buffer_reader, table_key, logic_id, macro_id, io_allocator))) {
    if (OB_ITER_END == ret) {
      // do nothing
    } else {
      LOG_WARN("failed to get next macro block", K(ret));
    }
  } else {
    LOG_INFO("get next macro block data", K(buffer_reader), K(logic_id), K(macro_id));
  }
  return ret;
}

int ObLSBackupDataTask::check_macro_block_data_(const ObBufferReader &buffer)
{
  int ret = OB_SUCCESS;
  ObSSTableMacroBlockChecker checker;
  if (!buffer.is_valid()) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("macro block data is not valid", K(ret));
  } else if (OB_FAIL(checker.check(buffer.data(), buffer.length()))) {
    LOG_WARN("failed to check macro block", K(ret), K(buffer));
  }
  return ret;
}

int ObLSBackupDataTask::write_macro_block_data_(const ObBufferReader &data, const storage::ObITable::TableKey &table_key,
    const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;

  if (!data.is_valid() || !logic_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(data), K(logic_id));
  } else if (OB_FAIL(backup_data_ctx_.write_macro_block_data(data, table_key, logic_id, macro_index))) {
    LOG_WARN("failed to write macro block data", K(ret), K(data), K(table_key), K(logic_id));
  } else {
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_macro_block",
                          "tenant_id", param_.tenant_id_,
                          "ls_id", param_.ls_id_,
                          "tablet_id", logic_id.tablet_id_,
                          "logic_id", logic_id,
                          "macro_index", macro_index,
                          "backup_data_type", backup_data_type_);
#endif
    LOG_INFO("write macro block data", K(data), K(logic_id), K(macro_index));
  }
  return ret;
}

int ObLSBackupDataTask::write_backup_meta_(const ObBufferReader &data, const common::ObTabletID &tablet_id,
    const ObBackupMetaType &meta_type, ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  if (0 == data.length()) {
    // do nothing
  } else if (!data.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(data), K(tablet_id));
  } else if (OB_FAIL(backup_data_ctx_.write_meta_data(data, tablet_id, meta_type, meta_index))) {
    LOG_WARN("failed to write macro block data", K(ret), K(data), K(tablet_id), K(meta_type));
  } else {
    LOG_DEBUG("write meta data", K(tablet_id), K(meta_type), K(meta_index));
  }
  return ret;
}

int ObLSBackupDataTask::get_tablet_handle_(const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *&tablet_handle)
{
  int ret = OB_SUCCESS;
  tablet_handle = NULL;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(ls_backup_ctx_->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to acquire tablet", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle should not be null", K(ret), K(tablet_id));
  } else {
    LOG_DEBUG("get tablet handle", K(tablet_id));
  }
  return ret;
}

int ObLSBackupDataTask::release_tablet_handle_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(ls_backup_ctx_->release_tablet(tablet_id))) {
    LOG_WARN("failed to acquire tablet", K(ret), K(tablet_id));
  } else {
    LOG_DEBUG("release tablet handle", K(tablet_id));
  }
  return ret;
}

int ObLSBackupDataTask::check_backup_finish_(bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = false;
  if (OB_ISNULL(provider_) || OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("provider should not be null", K(ret));
  } else if (task_id_ < ls_backup_ctx_->max_file_id_) {
    LOG_INFO("backup is not finished", K_(task_id), K(ls_backup_ctx_->max_file_id_));
  } else {
    const bool is_run_out = provider_->is_run_out();
    const bool has_remain = task_mgr_->has_remain();
    is_finished = is_run_out && !has_remain;
    LOG_INFO("run out no need generate next dag", K(ret), K(is_finished), K(is_run_out), K(has_remain));
  }
  return ret;
}

int ObLSBackupDataTask::do_generate_next_backup_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPrefetchBackupInfoDag *next_dag = NULL;
  ObLSBackupDagInitParam dag_param;
  ObLSBackupStage stage = LOG_STREAM_BACKUP_MAJOR;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObIDagNet *dag_net = NULL;
  int64_t prefetch_task_id = 0;
  if (OB_ISNULL(scheduler) || OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null MTL scheduler", K(ret), KP(scheduler), KP_(ls_backup_ctx));
  } else if (OB_FAIL(param_.convert_to(stage, dag_param))) {
    LOG_WARN("failed to convert to param", K(ret), K(stage));
  } else if (OB_FAIL(scheduler->alloc_dag(next_dag))) {
    LOG_WARN("failed to alloc child dag", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), K(*this));
  } else if (OB_FAIL(ls_backup_ctx_->get_prefetch_task_id(prefetch_task_id))) {
    LOG_WARN("failed to get prefetch task id", K(ret));
  } else if (OB_FAIL(next_dag->init(prefetch_task_id,
                 dag_param,
                 backup_data_type_,
                 report_ctx_,
                 *ls_backup_ctx_,
                 *provider_,
                 *task_mgr_,
                 *index_kv_cache_,
                 index_rebuild_dag_))) {
    LOG_WARN("failed to init child dag", K(ret), K(param_));
  } else if (OB_FAIL(next_dag->create_first_task())) {
    LOG_WARN("failed to create first task for child dag", K(ret), KPC(next_dag));
  } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*next_dag))) {
    LOG_WARN("failed to add dag into dag net", K(ret), KPC(next_dag));
  } else if (OB_FAIL(dag_->add_child_without_inheritance(*next_dag))) {
    LOG_WARN("failed to alloc dependency dag", K(ret), KPC(dag_), KPC(next_dag));
  } else if (OB_FAIL(next_dag->add_child_without_inheritance(*index_rebuild_dag_))) {
    LOG_WARN("failed to add child without inheritance", K(ret), KPC_(index_rebuild_dag));
  } else {
    if (FAILEDx(scheduler->add_dag(next_dag))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to add dag", K(ret), KPC(dag_), KPC(next_dag));
      } else {
        LOG_WARN("may exist same dag", K(ret));
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(scheduler) && OB_NOT_NULL(next_dag)) {
    scheduler->free_dag(*next_dag);
  }
  return ret;
}

int ObLSBackupDataTask::get_max_file_id_(int64_t &max_file_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(ls_backup_ctx_->get_max_file_id(max_file_id))) {
    LOG_WARN("failed to get max file id", K(ret));
  }
  return ret;
}

bool ObLSBackupDataTask::is_change_turn_error_(const int64_t error_code) const
{
  return error_code == OB_LS_NOT_EXIST;
}

void ObLSBackupDataTask::record_server_event_(const int64_t cost_us) const
{
  const char *backup_data_event = NULL;
  if (backup_data_type_.is_sys_backup()) {
    backup_data_event = "backup_sys_data";
  } else if (backup_data_type_.is_user_backup()) {
    backup_data_event = "backup_user_data";
  }
  SERVER_EVENT_ADD("backup",
      backup_data_event,
      "tenant_id",
      param_.tenant_id_,
      "backup_set_id",
      param_.backup_set_desc_.backup_set_id_,
      "ls_id",
      param_.ls_id_.id(),
      "retry_id",
      param_.retry_id_,
      "file_id",
      task_id_,
      "cost_us",
      cost_us);
}

int ObLSBackupDataTask::get_backup_item_(const storage::ObITable::TableKey &table_key,
    const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupProviderItem &item)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_items_.count(); ++i) {
    const ObBackupProviderItem &tmp_item = backup_items_.at(i);
    if (PROVIDER_ITEM_MACRO_ID == tmp_item.get_item_type()
        && table_key == tmp_item.get_table_key()
        && logic_id == tmp_item.get_logic_id()) {
      item = tmp_item;
      found = true;
      break;
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("do not found", K(logic_id), K_(backup_items));
  }
  return ret;
}

int ObLSBackupDataTask::mark_backup_item_finished_(
    const ObBackupProviderItem &item, const ObBackupDeviceMacroBlockId &physical_id)
{
  int ret = OB_SUCCESS;
  ObBackupTabletStat *tablet_stat = NULL;
  const ObTabletID &tablet_id = item.get_tablet_id();
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (FALSE_IT(tablet_stat = &ls_backup_ctx_->tablet_stat_)) {
  } else if (OB_FAIL(tablet_stat->mark_item_finished(backup_data_type_, item, physical_id))) {
    LOG_WARN("failed to mark item finished", K(ret), K(item), K_(backup_data_type));
  }
  return ret;
}

int ObLSBackupDataTask::get_companion_index_file_path_(
    const ObBackupIntermediateTreeType &tree_type,
    const int64_t task_id, ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  const ObBackupDest &backup_dest = param_.backup_dest_;
  const ObBackupSetDesc &backup_set_desc = param_.backup_set_desc_;
  const share::ObLSID &ls_id = param_.ls_id_;
  const share::ObBackupDataType backup_data_type = backup_data_type_;
  const int64_t turn_id = param_.turn_id_;
  const int64_t retry_id = param_.retry_id_;
  if (task_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(task_id));
  } else if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_intermediate_layer_index_backup_path(
      backup_dest, backup_set_desc, ls_id, backup_data_type, turn_id, retry_id, task_id, tree_type, backup_path))) {
    LOG_WARN("failed to get intermediate layer index backup path", K(ret), K_(param), K(task_id), K(tree_type));
  } else {
    LOG_INFO("get intermediate layer index backup path", K_(param), K(backup_path));
  }
  return ret;
}

int ObLSBackupDataTask::prepare_companion_index_file_handle_(const int64_t task_id, const ObBackupIntermediateTreeType &tree_type,
    const ObStorageIdMod &mod, common::ObIOFd &io_fd, ObBackupWrapperIODevice *&device_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  device_handle = NULL;
  ObBackupWrapperIODevice *device = NULL;
  void *buf = NULL;
  ObBackupPath backup_path;
  ObIODOpts io_d_opts;
  ObIODOpt opts[BACKUP_WRAPPER_DEVICE_OPT_NUM];
  io_d_opts.opts_ = opts;
  io_d_opts.opt_cnt_ = BACKUP_WRAPPER_DEVICE_OPT_NUM;
  const int flag = -1;
  const mode_t mode = 0;
  char storage_info_buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  if (OB_FAIL(ObBackupDeviceHelper::alloc_backup_device(param_.tenant_id_, device))) {
    LOG_WARN("failed to alloc backup device", K(ret));
  } else if (OB_FAIL(get_companion_index_file_path_(tree_type, task_id, backup_path))) {
    LOG_WARN("failed to get companion index file path", K(ret), K(tree_type), K(task_id));
  } else if (OB_FAIL(setup_io_storage_info_(param_.backup_dest_, storage_info_buf, sizeof(storage_info_buf), &io_d_opts))) {
    LOG_WARN("failed to setup io storage info", K(ret), K_(param));
  } else if (OB_FAIL(setup_io_device_opts_(task_id, tree_type, &io_d_opts))) {
    LOG_WARN("failed to setup io device opts", K(ret), K(task_id), K(tree_type), K(mod));
  } else if (OB_FAIL(device->open(backup_path.get_ptr(),
                                  flag,
                                  mode,
                                  io_fd,
                                  &io_d_opts))) {
    LOG_WARN("failed to open device", K(ret), K_(param), K(backup_path));
  } else if (FALSE_IT(device->set_storage_id_mod(mod))) {
  } else {
    device_handle = device;
    device = NULL;
  }

  if (OB_NOT_NULL(device)) {
    ObBackupDeviceHelper::release_backup_device(device);
  }

  return ret;
}

int ObLSBackupDataTask::setup_io_storage_info_(
    const share::ObBackupDest &backup_dest, char *buf, const int64_t len, common::ObIODOpts *iod_opts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupWrapperIODevice::setup_io_storage_info(backup_dest, buf, len, iod_opts))) {
    LOG_WARN("failed to setup io storage info", K(ret), K(backup_dest));
  }
  return ret;
}

int ObLSBackupDataTask::setup_io_device_opts_(const int64_t task_id,
    const ObBackupIntermediateTreeType &tree_type, common::ObIODOpts *io_d_opts)
{
  int ret = OB_SUCCESS;
  const ObStorageAccessType access_type = OB_STORAGE_ACCESS_MULTIPART_WRITER;
  if (OB_FAIL(ObBackupWrapperIODevice::setup_io_opts_for_backup_device(param_.backup_set_desc_.backup_set_id_,
                                                                       param_.ls_id_,
                                                                       backup_data_type_,
                                                                       param_.turn_id_,
                                                                       param_.retry_id_,
                                                                       task_id,
                                                                       ObBackupIntermediateTreeType::BACKUP_META_TREE == tree_type ?
                                                                         ObBackupDeviceMacroBlockId::META_TREE_BLOCK : ObBackupDeviceMacroBlockId::INDEX_TREE_BLOCK,
                                                                       access_type,
                                                                       io_d_opts))) {
    LOG_WARN("failed to setup io opts for backup device", K(ret), K_(param), K(backup_data_type_), K(task_id));
  } else {
    LOG_INFO("set io device opts", K_(param), K_(backup_data_type), K(task_id));
  }
  return ret;
}

int ObLSBackupDataTask::get_index_block_rebuilder_ptr_(const common::ObTabletID &tablet_id,
    const storage::ObITable::TableKey &table_key, ObIndexBlockRebuilder *&index_block_rebuilder)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rebuilder_mgr_.get_index_block_rebuilder(table_key, index_block_rebuilder))) {
    LOG_WARN("failed to get index block rebuilder", K(ret), K(table_key));
  }
  return ret;
}

int ObLSBackupDataTask::prepare_index_block_rebuilder_if_need_(
    const ObBackupProviderItem &item, const int64_t *task_idx)
{
  int ret = OB_SUCCESS;
  bool is_opened = false;
  ObBackupTabletStat *tablet_stat = NULL;
  const ObTabletID &tablet_id = item.get_tablet_id();
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(item));
  } else if (OB_FAIL(rebuilder_mgr_.prepare_index_block_rebuilder_if_need(item, task_idx, is_opened))) {
    LOG_WARN("failed to prepare index block rebuilder", K(ret), K(item));
  } else if (!is_opened) {
    // do nothing
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (FALSE_IT(tablet_stat = &ls_backup_ctx_->tablet_stat_)) {
  } else if (OB_FAIL(tablet_stat->add_opened_rebuilder_count(tablet_id))) {
    LOG_WARN("failed to add opened rebuilder count", K(ret), K(tablet_id));
  } else {
    LOG_INFO("add opened rebuilder count", K(tablet_id), "table_key", item.get_table_key(), K(item));
  }
  return ret;
}

int ObLSBackupDataTask::append_macro_row_to_rebuilder_(const ObBackupProviderItem &item,
    const blocksstable::ObBufferReader &buffer_reader, const ObBackupDeviceMacroBlockId &physical_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObIndexBlockRebuilder *rebuilder = NULL;
  const common::ObTabletID &tablet_id = item.get_tablet_id();
  const storage::ObITable::TableKey &table_key = item.get_table_key();
  const blocksstable::ObLogicMacroBlockId &logic_id = item.get_logic_id();
  const int64_t absolute_row_offset = item.get_absolute_row_offset();
  MacroBlockId macro_block_id;
  if (OB_FAIL(get_index_block_rebuilder_ptr_(tablet_id, table_key, rebuilder))) {
    LOG_WARN("failed to get index block rebuilder ptr", K(ret), K(tablet_id), K(table_key));
  } else if (OB_FAIL(convert_macro_block_id_(physical_id, macro_block_id))) {
    LOG_WARN("failed to convert macro block id", K(ret), K(physical_id));
  } else if (OB_FAIL(rebuilder->append_macro_row(buffer_reader.data(),
                                                 buffer_reader.length(),
                                                 macro_block_id,
                                                 absolute_row_offset))) {
    LOG_WARN("failed to append macro row", K(ret), K(buffer_reader), K(macro_block_id));
  } else {
    LOG_DEBUG("append macro row to rebuilder", K(item), K(physical_id));
  }
  return ret;
}

int ObLSBackupDataTask::close_index_block_rebuilder_if_need_(const ObBackupProviderItem &item)
{
  int ret = OB_SUCCESS;
  bool is_closed = false;
  ObBackupTabletStat *tablet_stat = NULL;
  const ObTabletID &tablet_id = item.get_tablet_id();
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(item));
  } else if (OB_FAIL(rebuilder_mgr_.close_index_block_rebuilder_if_need(item, is_closed))) {
    LOG_WARN("failed to close index block rebuilder", K(ret));
  } else if (!is_closed) {
    // do nothing
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (FALSE_IT(tablet_stat = &ls_backup_ctx_->tablet_stat_)) {
  } else if (OB_FAIL(tablet_stat->add_closed_rebuilder_count(tablet_id))) {
    LOG_WARN("failed to add closed rebuilder count", K(ret), K(tablet_id));
  } else {
    LOG_INFO("close rebuilder count", K(tablet_id), "table_key", item.get_table_key(), K(item));
  }
  return ret;
}

int ObLSBackupDataTask::convert_macro_block_id_(
    const ObBackupDeviceMacroBlockId &physical_id, MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (!physical_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(physical_id));
  } else {
    macro_id = MacroBlockId(physical_id.first_id(),
                            physical_id.second_id(),
                            physical_id.third_id(),
                            0/*forth_id*/);
  }
  return ret;
}

int ObLSBackupDataTask::remove_sstable_index_builder_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_builder_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index builder mgr should not be null", K(ret));
  } else if (OB_FAIL(index_builder_mgr_->remove_sstable_index_builder(tablet_id))) {
    LOG_WARN("failed to remove sstable index builder", K(ret), K(tablet_id));
  }
  return ret;
}

int ObLSBackupDataTask::close_tree_device_handle_(
    ObBackupWrapperIODevice *&index_tree_device_handle, ObBackupWrapperIODevice *&meta_tree_device_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(index_tree_device_handle) || OB_ISNULL(meta_tree_device_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index tree or meta tree device handle should not be null", K(ret), KP(index_tree_device_handle), KP(meta_tree_device_handle));
  } else {
    if (OB_FAIL(index_tree_device_handle->complete(index_tree_io_fd_))) {
      LOG_WARN("fail to complete multipart upload", K(ret), KP(index_tree_device_handle), K(index_tree_io_fd_));
    } else if (OB_FAIL(meta_tree_device_handle->complete(meta_tree_io_fd_))) {
      LOG_WARN("fail to complete multipart upload", K(ret), KP(meta_tree_device_handle), K(meta_tree_io_fd_));
    }

    if (OB_TMP_FAIL(ObBackupDeviceHelper::close_device_and_fd(index_tree_device_handle, index_tree_io_fd_))) {
      LOG_WARN("failed to close index tree device and fd", K(tmp_ret), K(ret));
      ret = COVER_SUCC(tmp_ret);
    }

    if (OB_TMP_FAIL(ObBackupDeviceHelper::close_device_and_fd(meta_tree_device_handle, meta_tree_io_fd_))) {
      LOG_WARN("failed to close meta tree device and fd", K(tmp_ret), K(ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObLSBackupDataTask::update_logic_id_to_macro_index_(const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key,
    const blocksstable::ObLogicMacroBlockId &logic_id, const ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  ObBackupTabletSSTableIndexBuilderMgr *mgr = NULL;
  if (OB_ISNULL(index_builder_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mgr should not be null", K(ret));
  } else if (!logic_id.is_valid() || !macro_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(logic_id), K(macro_index));
  } else if (!table_key.is_major_sstable()) {
    // do nothing
  } else if (OB_FAIL(index_builder_mgr_->get_sstable_index_builder_mgr(tablet_id, mgr))) {
    LOG_WARN("failed to get sstable index builder mgr", K(ret), K(tablet_id));
  } else if (!mgr->is_major_compaction_mview_dep_tablet()) {
    // do nothing
  } else if (OB_FAIL(mgr->update_logic_id_to_macro_index(logic_id, macro_index))) {
    LOG_WARN("failed to update logic id to macro index", K(ret), K(logic_id));
  }
  return ret;
}

int ObLSBackupDataTask::wait_reuse_other_block_ready_(
    const common::ObTabletID &tablet_id, const blocksstable::ObLogicMacroBlockId &logic_id,
    ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  macro_index.reset();
  bool is_ready = false;
  static const int64_t DEFAULT_SLEEP_US = 10_ms;
  int64_t start_time = ObTimeUtility::current_time();
  while (OB_SUCC(ret)) {
    is_ready = false;
    if (OB_ISNULL(ls_backup_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls backup ctx should not be null", K(ret));
    } else if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
      ret = OB_EAGAIN;
      LOG_WARN("ctx already failed", K(ret));
    } else if (OB_FAIL(inner_check_reuse_block_ready_(tablet_id, logic_id, macro_index, is_ready))) {
      LOG_WARN("failed to inner check reuse block ready", K(ret), K(tablet_id), K(logic_id));
    } else if (is_ready) {
      LOG_INFO("reuse macro block is ready", K(tablet_id), K(logic_id), K(macro_index));
#ifdef ERRSIM
      SERVER_EVENT_SYNC_ADD("backup_data", "reuse_macro_block",
                            "tablet_id", tablet_id.id(),
                            "logic_id", logic_id,
                            "macro_index", macro_index);
#endif
      break;
    } else {
      usleep(DEFAULT_SLEEP_US);
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(ls_backup_ctx_)) {
    ls_backup_ctx_->add_wait_reuse_across_sstable_time(ObTimeUtility::current_time() - start_time);
  }
  return ret;
}

int ObLSBackupDataTask::inner_check_reuse_block_ready_(
    const common::ObTabletID &tablet_id, const blocksstable::ObLogicMacroBlockId &logic_id,
    ObBackupMacroBlockIndex &macro_index, bool &is_ready)
{
  int ret = OB_SUCCESS;
  macro_index.reset();
  is_ready = false;
  ObBackupTabletSSTableIndexBuilderMgr *mgr = NULL;
  if (OB_ISNULL(index_builder_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mgr should not be null", K(ret));
  } else if (!tablet_id.is_valid() || !logic_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id), K(logic_id));
  } else if (OB_FAIL(index_builder_mgr_->get_sstable_index_builder_mgr(tablet_id, mgr))) {
    LOG_WARN("failed to get sstable index builder mgr", K(ret), K(tablet_id));
  } else if (OB_FAIL(mgr->check_real_macro_index_exist(logic_id, is_ready, macro_index)))  {
    LOG_WARN("failed to check macro index exist", K(ret), K(logic_id));
  }
  return ret;
}

int ObLSBackupDataTask::check_need_reuse_sstable_macro_block_for_mv_(const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key,
    const blocksstable::ObLogicMacroBlockId &logic_id, bool &need_reuse_for_mv)
{
  int ret = OB_SUCCESS;
  need_reuse_for_mv = false;
  ObBackupTabletIndexBlockBuilderMgr *mgr = index_builder_mgr_;
  ObBackupTabletSSTableIndexBuilderMgr *sstable_mgr = NULL;
  bool macro_index_exist = false;
  if (!table_key.is_major_sstable()) {
    need_reuse_for_mv = false;
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else {
    ObMutexGuard guard(ls_backup_ctx_->mv_mutex_);
    if (OB_FAIL(mgr->get_sstable_index_builder_mgr(tablet_id, sstable_mgr))) {
      LOG_WARN("failed to get sstable index builder mgr", K(ret), K(tablet_id));
    } else if (!sstable_mgr->is_major_compaction_mview_dep_tablet()) {
      need_reuse_for_mv = false;
    } else if (OB_FAIL(sstable_mgr->check_place_holder_macro_index_exist(logic_id, macro_index_exist))) {
      LOG_WARN("failed to check place holder macro index exist", K(ret), K(logic_id));
    } else if (macro_index_exist) {
      need_reuse_for_mv = true;
      LOG_INFO("macro index exist, reuse local", K(tablet_id), K(table_key), K(logic_id));
    } else if (OB_FAIL(sstable_mgr->insert_place_holder_macro_index(logic_id))) {
      LOG_WARN("failed to insert empty macro index", K(ret), K(logic_id));
    } else {
      need_reuse_for_mv = false;
    }
  }
  return ret;
}

/* ObLSBackupMetaTask */

ObLSBackupMetaTask::ObLSBackupMetaTask()
    : ObITask(ObITaskType::TASK_TYPE_MIGRATE_COPY_PHYSICAL),
      is_inited_(false),
      start_scn_(),
      param_(),
      report_ctx_(),
      ls_backup_ctx_(nullptr)
{}

ObLSBackupMetaTask::~ObLSBackupMetaTask()
{}

int ObLSBackupMetaTask::init(
    const SCN &start_scn, const ObLSBackupDagInitParam &param, const ObBackupReportCtx &report_ctx,
    ObLSBackupCtx &ls_backup_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls backup meta task init twice", K(ret));
  } else if (!start_scn.is_valid() || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(start_scn), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    start_scn_ = start_scn;
    report_ctx_ = report_ctx;
    ls_backup_ctx_ = &ls_backup_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupMetaTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  SERVER_EVENT_SYNC_ADD("backup_data", "before_backup_meta");
  DEBUG_SYNC(BEFORE_BACKUP_META);
  const SCN start_scn = start_scn_;
  const int64_t task_id = param_.job_desc_.task_id_;
  const uint64_t tenant_id = param_.tenant_id_;
  const share::ObLSID ls_id = param_.ls_id_;
  const int64_t retry_id = param_.retry_id_;
  storage::ObLS *ls = NULL;
#ifdef ERRSIM
  if (ls_id == ObLSID(1001)) {
    DEBUG_SYNC(BEFORE_BACKUP_1001_META);
  }
#endif
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (ls_id.is_sys_ls()) {
      ret = OB_E(EventTable::EN_BACKUP_SYS_META_TASK_FAILED) OB_SUCCESS;
    } else {
      ret = OB_E(EventTable::EN_BACKUP_USER_META_TASK_FAILED) OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_meta",
                            "tenant_id", param_.tenant_id_,
                            "task_id", param_.job_desc_.task_id_,
                            "ls_id", param_.ls_id_.id(),
                            "turn_id", param_.turn_id_,
                            "retry_id", param_.retry_id_);
      LOG_WARN("errsim backup meta task failed", K(ret));
    }
  }
#endif
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup meta task do not init", K(ret));
  } else {
    MTL_SWITCH(tenant_id) {
      if (OB_FAIL(guard.switch_to(tenant_id))) {
        LOG_WARN("failed to switch tenant", K(ret), K(tenant_id));
      } else if (OB_FAIL(ObBackupUtils::check_ls_validity(tenant_id, ls_id))) {
        LOG_WARN("failed to check ls validity", K(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(advance_checkpoint_by_flush_(tenant_id, ls_id, start_scn))) {
        LOG_WARN("failed to advance checkpoint by flush", K(ret), K(tenant_id), K(ls_id), K(start_scn));
      } else if (OB_FAIL(backup_ls_meta_and_tablet_metas_(tenant_id, ls_id))) {
        LOG_WARN("failed to get backup meta ctx", K(ret), K(tenant_id), K(ls_id));
      }
    }
  }

  if (OB_FAIL(ret)) {
    bool is_set = false;
    ls_backup_ctx_->set_result_code(ret, is_set);
    ls_backup_ctx_->set_finished();
    REPORT_TASK_RESULT(this->get_dag()->get_dag_id(), ret);
  }
  return ret;
}

int ObLSBackupMetaTask::advance_checkpoint_by_flush_(
    const uint64_t tenant_id, const share::ObLSID &ls_id, const SCN &start_scn)
{
  int ret = OB_SUCCESS;
  checkpoint::ObCheckpointExecutor *checkpoint_executor = NULL;
  storage::ObLSHandle ls_handle;
  storage::ObLS *ls = NULL;
  if (OB_FAIL(get_ls_handle(tenant_id, ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream not exist", K(ret), K(ls_id));
  } else if (OB_FAIL(advance_checkpoint_by_flush(tenant_id, ls_id, start_scn, ls))) {
    LOG_WARN("failed to advance checkpoint by flush", K(ret), K(tenant_id), K(ls_id), K(start_scn));
  }
  return ret;
}

int ObLSBackupMetaTask::backup_ls_meta_and_tablet_metas_(const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  storage::ObLSHandle ls_handle;
  storage::ObLS *ls = NULL;
  ObTenantDagScheduler *scheduler = NULL;
  share::ObTenantBase *tenant_base = MTL_CTX();
  omt::ObTenant *tenant = NULL;
  ObBackupLSMetaInfo ls_meta_info;
  ObExternTabletMetaWriter writer;
  ObBackupDest backup_set_dest;
  int64_t backup_tablet_count = 0;
  int64_t backup_macro_block_count = 0;
  int64_t calc_macro_block_count_time = 0;

  // save max tablet checkpoint scn of all the tablets belong to the same ls.
  SCN max_tablet_checkpoint_scn;
  max_tablet_checkpoint_scn.set_min();

  // save ls meta
  auto save_ls_meta_f = [&ls_meta_info, &max_tablet_checkpoint_scn](const ObLSMetaPackage &meta_package)->int {
    int ret = OB_SUCCESS;
    if (!meta_package.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ls meta package is invalid", K(ret), K(meta_package));
    } else {
      ls_meta_info.ls_meta_package_ = meta_package;
      max_tablet_checkpoint_scn = MAX(max_tablet_checkpoint_scn, meta_package.ls_meta_.get_clog_checkpoint_scn());
    }
    return ret;
  };

  // persist tablet meta
  auto backup_tablet_meta_f = [&writer, &backup_tablet_count, &max_tablet_checkpoint_scn, &backup_macro_block_count, &calc_macro_block_count_time]
      (const obrpc::ObCopyTabletInfo &tablet_info, const ObTabletHandle &tablet_handle)->int {
    int ret = OB_SUCCESS;
    blocksstable::ObSelfBufferWriter buffer_writer("LSBackupMetaTask");
    blocksstable::ObBufferReader buffer_reader;
    int64_t macro_block_count = 0;
    int64_t start_time = 0;
    const int64_t serialize_size = tablet_info.param_.get_serialize_size();
    if (!tablet_info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tablet meta is invalid", K(ret), K(tablet_info));
    } else if (MAX_BACKUP_TABLET_META_SERIALIZE_SIZE < serialize_size) {
      // In case of the tablet meta is too large.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet meta is too large.", K(ret), K(serialize_size), K(tablet_info));
    } else if (OB_FAIL(buffer_writer.ensure_space(backup::OB_BACKUP_READ_BLOCK_SIZE))) {
      LOG_WARN("failed to ensure space");
    } else if (OB_FAIL(buffer_writer.write_serialize(tablet_info.param_))) {
      LOG_WARN("failed to writer", K(tablet_info));
    } else if (FALSE_IT(start_time = ObTimeUtility::current_time())) {
    } else if (OB_FAIL(ObStorageHAUtils::calc_tablet_sstable_macro_block_cnt(tablet_handle, macro_block_count))) {
      LOG_WARN("failed to calc tablet sstable macro block count", K(ret), K(tablet_handle));
    } else if (FALSE_IT(calc_macro_block_count_time += ObTimeUtility::current_time() - start_time)) {
    } else {
      buffer_reader.assign(buffer_writer.data(), buffer_writer.length(), buffer_writer.length());
      if (OB_FAIL(writer.write_meta_data(buffer_reader, tablet_info.param_.tablet_id_))) {
        LOG_WARN("failed to write meta data", K(ret), K(tablet_info));
      } else {
        max_tablet_checkpoint_scn = MAX(max_tablet_checkpoint_scn, tablet_info.param_.get_max_tablet_checkpoint_scn());
        backup_macro_block_count += macro_block_count;
        LOG_INFO("succeed backup tablet meta", "meta", tablet_info.param_);
      }
    }

    ++backup_tablet_count;
    return ret;
  };

  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(tenant_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant base should not be NULL", K(ret), KP(tenant_base));
  } else if (FALSE_IT(tenant = static_cast<omt::ObTenant *>(tenant_base))) {
  } else if (OB_FAIL(get_ls_handle(tenant_id, ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream not exist", K(ret), K(ls_id));
  } else if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(param_.backup_dest_, param_.backup_set_desc_, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest", K(ret), K(param_));
  } else if (OB_FAIL(writer.init(backup_set_dest, param_.ls_id_, param_.turn_id_, param_.retry_id_, param_.dest_id_, false/*is_final_fuse*/, *ls_backup_ctx_->bandwidth_throttle_))) {
    LOG_WARN("failed to init tablet info writer", K(ret));
  } else {
    const int64_t WAIT_GC_LOCK_TIMEOUT = 30 * 60 * 1000 * 1000; // 30 min TODO(zeyong) optimization timeout later 4.3
    const int64_t CHECK_GC_LOCK_INTERVAL = 1000000; // 1s
    const int64_t wait_gc_lock_start_ts = ObTimeUtility::current_time();
    int64_t cost_ts = 0;
    do {
      if (ls->is_stopped()) {
        ret = OB_NOT_RUNNING;
        LOG_WARN("ls is not running, stop backup", K(ret), KPC(ls));
      } else if (scheduler->has_set_stop()) {
        ret = OB_SERVER_IS_STOPPING;
        LOG_WARN("tenant dag scheduler has set stop, stop backup", K(ret), KPC(ls));
      } else if (tenant->has_stopped()) {
        ret = OB_TENANT_HAS_BEEN_DROPPED;
        LOG_WARN("tenant has been stopped, stop backup", K(ret), KPC(ls));
      } else if (OB_FAIL(ls->get_ls_meta_package_and_tablet_metas(
                false/* check_archive */,
                save_ls_meta_f,
                true/*need_sorted_tablet_id*/,
                backup_tablet_meta_f))) {
        if (OB_TABLET_GC_LOCK_CONFLICT != ret) {
          LOG_WARN("failed to get ls meta package and tablet meta", K(ret), KPC(ls));
        } else {
          cost_ts = ObTimeUtility::current_time() - wait_gc_lock_start_ts;
          if (WAIT_GC_LOCK_TIMEOUT <= cost_ts) {
            ret = OB_EAGAIN;
            LOG_WARN("get ls meta package and tablet meta timeout, need try again.", K(ret), K(ls_id));
          } else {
            ob_usleep(CHECK_GC_LOCK_INTERVAL);
          }
        }
      } else {
        cost_ts = ObTimeUtility::current_time() - wait_gc_lock_start_ts;
        LOG_INFO("succeed to get ls meta package and tablet meta", K(ls_id), K(cost_ts), K(backup_macro_block_count),
                                                                   K(calc_macro_block_count_time),
                                                                   "calc_macro_block_time_ratio", calc_macro_block_count_time * 100.0 / cost_ts);
      }
    } while (OB_TABLET_GC_LOCK_CONFLICT == ret);


    if (FAILEDx(backup_ls_meta_package_(ls_meta_info))) {
      LOG_WARN("failed to backup ls meta package", K(ret), K(ls_meta_info));
    } else if (OB_FAIL(ObBackupLSTaskOperator::update_max_tablet_checkpoint_scn(
                       *report_ctx_.sql_proxy_,
                       param_.job_desc_.task_id_,
                       param_.tenant_id_,
                       param_.ls_id_,
                       max_tablet_checkpoint_scn))) {
      LOG_WARN("failed to update max tablet checkpoint scn", K(ret), K(param_), K(max_tablet_checkpoint_scn));
    } else if (OB_FAIL(report_backup_stat_(backup_tablet_count, backup_macro_block_count))) {
      LOG_WARN("failed to report backup stat", K(ret));
    } else {
      LOG_INFO("succeed backup ls meta and all tablet metas", K(ls_id), K(ls_meta_info), K(backup_tablet_count), K(max_tablet_checkpoint_scn));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(writer.close())) {
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
      LOG_WARN("failed to close writer", K(ret), K(tmp_ret), K(ls_id));
    }
  }

  return ret;
}

int ObLSBackupMetaTask::backup_ls_meta_package_(const ObBackupLSMetaInfo &ls_meta_info)
{
  int ret = OB_SUCCESS;
  ObExternLSMetaMgr extern_mgr;
  const ObBackupDest &backup_dest = param_.backup_dest_;
  const ObBackupSetDesc &backup_set_desc = param_.backup_set_desc_;
  const share::ObLSID &ls_id = param_.ls_id_;
  const int64_t turn_id = param_.turn_id_;
  const int64_t retry_id = param_.retry_id_;
  const int64_t dest_id = param_.dest_id_;
  if (OB_FAIL(extern_mgr.init(backup_dest, backup_set_desc, ls_id, turn_id, retry_id, dest_id))) {
    LOG_WARN("failed to init extern mgr", K(ret), K_(param));
  } else if (OB_FAIL(extern_mgr.write_ls_meta_info(ls_meta_info))) {
    LOG_WARN("failed to write ls meta info", K(ret), K(ls_meta_info));
  } else {
    LOG_INFO("backup ls meta package", K(ret), K(ls_meta_info));
  }
  return ret;
}

int ObLSBackupMetaTask::report_backup_stat_(const int64_t tablet_count, const int64_t macro_block_count)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(report_ctx_.sql_proxy_, gen_meta_tenant_id(param_.tenant_id_)))) {
    LOG_WARN("failed to start transaction", K(ret), K(param_));
  } else {
    const int64_t job_id = param_.job_desc_.job_id_;
    const int64_t task_id = param_.job_desc_.task_id_;
    const uint64_t tenant_id = param_.tenant_id_;
    const share::ObLSID &ls_id = param_.ls_id_;
    const bool for_update = true;
    share::ObBackupSetTaskAttr old_set_task_attr;
    share::ObBackupLSTaskAttr old_ls_task_attr;
    share::ObBackupStats new_backup_task_stats;
    share::ObBackupStats new_ls_task_stats;
    if (OB_FAIL(ObBackupTaskOperator::get_backup_task(trans, job_id, tenant_id, for_update, old_set_task_attr))) {
      LOG_WARN("failed to get backup task", K(ret), K_(param));
    } else if (OB_FAIL(ObBackupLSTaskOperator::get_ls_task(trans, for_update, task_id, tenant_id, ls_id, old_ls_task_attr))) {
      LOG_WARN("failed to get ls task", K(ret), K_(param));
    } else if (OB_FAIL(calc_backup_stat_(old_set_task_attr, tablet_count, macro_block_count, new_backup_task_stats))) {
      LOG_WARN("failed to calc backup stat", K(ret), K(old_set_task_attr), K(tablet_count), K(macro_block_count));
    } else if (OB_FAIL(calc_ls_backup_stat_(old_ls_task_attr.stats_, tablet_count, macro_block_count, new_ls_task_stats))) {
      LOG_WARN("failed to calc ls backup stat", K(ret), K_(param));
    } else if (OB_FAIL(ObBackupTaskOperator::update_stats(trans, task_id, tenant_id, new_backup_task_stats))) {
      LOG_WARN("failed to update stats", K(ret), K_(param));
    } else if (OB_FAIL(ObBackupLSTaskOperator::update_stats(trans, task_id, tenant_id, ls_id, new_ls_task_stats))) {
      LOG_WARN("failed to update stat", K(ret), K_(param));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        LOG_WARN("failed to commit", K(ret));
      }
    } else {
      if (OB_TMP_FAIL(trans.end(false /* commit*/))) {
        LOG_WARN("failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLSBackupMetaTask::calc_backup_stat_(const ObBackupSetTaskAttr &set_task_attr,
    const int64_t tablet_count, const int64_t macro_block_count, ObBackupStats &backup_stats)
{
  int ret = OB_SUCCESS;
  const ObBackupStats &orig_stats = set_task_attr.stats_;
  if (OB_FAIL(backup_stats.assign(orig_stats))) {
    LOG_WARN("failed to assign backup stats", K(ret), K(orig_stats));
  } else {
    backup_stats.tablet_count_ += tablet_count;
    backup_stats.macro_block_count_ += macro_block_count;
  }
  return ret;
}

int ObLSBackupMetaTask::calc_ls_backup_stat_(const share::ObBackupStats &old_backup_stat, const int64_t tablet_count,
    const int64_t macro_block_count, ObBackupStats &backup_stats)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_stats.assign(old_backup_stat))) {
    LOG_WARN("failed to assign backup stats", K(ret), K(old_backup_stat));
  } else {
    backup_stats.tablet_count_ = tablet_count;
    backup_stats.macro_block_count_ = macro_block_count;
  }
  return ret;
}

/* ObLSBackupPrepareTask */

ObLSBackupPrepareTask::ObLSBackupPrepareTask()
    : ObITask(ObITaskType::TASK_TYPE_MIGRATE_COPY_PHYSICAL),
      is_inited_(false),
      concurrency_(-1),
      param_(),
      backup_data_type_(),
      ls_backup_ctx_(NULL),
      provider_(NULL),
      task_mgr_(NULL),
      index_kv_cache_(NULL),
      report_ctx_()
{}

ObLSBackupPrepareTask::~ObLSBackupPrepareTask()
{}

int ObLSBackupPrepareTask::init(const int64_t concurrency, const ObLSBackupDagInitParam &param,
    const share::ObBackupDataType &backup_data_type, ObLSBackupCtx &ls_backup_ctx, ObIBackupTabletProvider &provider,
    ObBackupMacroBlockTaskMgr &task_mgr, ObBackupIndexKVCache &index_kv_cache, const ObBackupReportCtx &report_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("log stream backup prepare task init twice", K(ret));
  } else if (concurrency <= 0 || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(concurrency), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    concurrency_ = concurrency;
    backup_data_type_ = backup_data_type;
    ls_backup_ctx_ = &ls_backup_ctx;
    provider_ = &provider;
    task_mgr_ = &task_mgr;
    index_kv_cache_ = &index_kv_cache;
    report_ctx_ = report_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupPrepareTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObIDagNet *dag_net = NULL;
  ObLSBackupIndexRebuildDag *rebuild_dag = NULL;
  const ObBackupIndexLevel index_level = BACKUP_INDEX_LEVEL_LOG_STREAM;
  SERVER_EVENT_SYNC_ADD("backup_data", "before_backup_prepare_task");
  DEBUG_SYNC(BEFORE_BACKUP_PREPARE_TASK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("prepare task do not init", K(ret));
  } else if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
    ret = ls_backup_ctx_->get_result_code();
    LOG_WARN("backup already failed, do nothing", K(ret));
  } else if (OB_FAIL(may_need_advance_checkpoint_())) {
    LOG_WARN("may need advance checkpoint failed", K(ret), K_(param));
  } else if (OB_FAIL(prepare_backup_tx_table_filled_tx_scn_())) {
    LOG_WARN("failed to check tx data can explain user data", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(rebuild_dag))) {
    LOG_WARN("failed to alloc child dag", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), K(*this));
  } else if (OB_FAIL(rebuild_dag->init(param_,
                 backup_data_type_,
                 index_level,
                 report_ctx_,
                 task_mgr_,
                 provider_,
                 index_kv_cache_,
                 ls_backup_ctx_))) {
    LOG_WARN("failed to init child dag", K(ret), K(param_));
  } else if (OB_FAIL(rebuild_dag->create_first_task())) {
    LOG_WARN("failed to create first task for child dag", K(ret), KPC(rebuild_dag));
  } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*rebuild_dag))) {
    LOG_WARN("failed to add dag into dag net", K(ret), KPC(rebuild_dag));
  } else if (OB_FAIL(dag_->add_child_without_inheritance(*rebuild_dag))) {
    LOG_WARN("failed to alloc dependency dag", K(ret), KPC(dag_), KPC(rebuild_dag));
  }
  ObArray<ObPrefetchBackupInfoDag *> success_add_prefetch_dags;
  for (int i = 0; OB_SUCC(ret) && i < concurrency_; ++i) {
    ObPrefetchBackupInfoDag *child_dag = NULL;
    int64_t prefetch_task_id = 0;
    if (OB_ISNULL(scheduler) || OB_ISNULL(ls_backup_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null MTL scheduler", K(ret), KP(scheduler), KP_(ls_backup_ctx));
    } else if (OB_FAIL(scheduler->alloc_dag(child_dag))) {
      LOG_WARN("failed to alloc child dag", K(ret));
    } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag net should not be NULL", K(ret), K(*this));
    } else if (OB_FAIL(ls_backup_ctx_->get_prefetch_task_id(prefetch_task_id))) {
      LOG_WARN("failed to get prefetch task id", K(ret));
    } else if (OB_FAIL(child_dag->init(prefetch_task_id,
                   param_,
                   backup_data_type_,
                   report_ctx_,
                   *ls_backup_ctx_,
                   *provider_,
                   *task_mgr_,
                   *index_kv_cache_,
                   rebuild_dag))) {
      LOG_WARN("failed to init child dag", K(ret), K(param_));
    } else if (OB_FAIL(child_dag->create_first_task())) {
      LOG_WARN("failed to create first task for child dag", K(ret), KPC(child_dag));
    } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*child_dag))) {
      LOG_WARN("failed to add dag into dag net", K(ret), KPC(child_dag));
    } else if (OB_FAIL(dag_->add_child_without_inheritance(*child_dag))) {
      LOG_WARN("failed to alloc dependency dag", K(ret), KPC(dag_), KPC(child_dag));
    } else if (OB_FAIL(child_dag->add_child_without_inheritance(*rebuild_dag))) {
      LOG_WARN("failed to add child without inheritance", K(ret), KPC(rebuild_dag));
    } else {
      bool add_dag_success = false;
#ifdef ERRSIM
      if (OB_SUCC(ret) && 0 != i) {
        ret = OB_E(EventTable::EN_ADD_BACKUP_PREFETCH_DAG_FAILED) OB_SUCCESS;
        if (OB_FAIL(ret)) {
          FLOG_INFO("add_backup_build_prefetch_dag_failed");
        }
      }
#endif
      if (FAILEDx(scheduler->add_dag(child_dag))) {
        if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
          LOG_WARN("failed to add dag", K(ret), KPC(dag_), KPC(child_dag));
        } else {
          LOG_WARN("may exist same dag", K(ret), K(i));
        }
      } else if (OB_FAIL(success_add_prefetch_dags.push_back(child_dag))) {
        LOG_WARN("failed to push back", K(ret), KP(child_dag));
      } else {
        LOG_INFO("success to alloc prefetch backup info dag", K(ret), K(i));
        add_dag_success = true;
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(scheduler) && OB_NOT_NULL(child_dag)) {
        if (!add_dag_success) {
          scheduler->free_dag(*child_dag);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
#ifdef ERRSIM
    ret = OB_E(EventTable::EN_ADD_BACKUP_BUILD_INDEX_DAG_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      FLOG_INFO("add_backup_build_index_dag_failed");
    }
#endif
    if (FAILEDx(scheduler->add_dag(rebuild_dag))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to add dag", K(ret), KPC(dag_), KPC(rebuild_dag));
      } else {
        LOG_WARN("may exist same dag", K(ret));
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(scheduler)) {
    for (int64_t i = 0; i < success_add_prefetch_dags.count(); ++i) {
      ObPrefetchBackupInfoDag *prefetch_dag = success_add_prefetch_dags.at(i);
      if (OB_ISNULL(prefetch_dag)) {
        // do nothing
      } else if (OB_TMP_FAIL(scheduler->cancel_dag(prefetch_dag))) {
        LOG_ERROR("failed to cancel dag", K(tmp_ret), KP(prefetch_dag), KP(dag_));
      } else {
        success_add_prefetch_dags.at(i) = NULL;
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(scheduler) && OB_NOT_NULL(rebuild_dag)) {
    scheduler->free_dag(*rebuild_dag);
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_PREPARE_TASK_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_prepare_task", "result", ret);
      LOG_WARN("errsim backup prepare task failed", K(ret));
    }
  }
#endif

  bool need_report_error = false;
  if (OB_FAIL(ret) && OB_NOT_NULL(ls_backup_ctx_)) {
    bool is_set = false;
    ls_backup_ctx_->set_result_code(ret, is_set);
    need_report_error = is_set;
  }
  if (OB_NOT_NULL(ls_backup_ctx_) && need_report_error) {
    REPORT_TASK_RESULT(this->get_dag()->get_dag_id(), ls_backup_ctx_->get_result_code());
  }
  return ret;
}

int ObLSBackupPrepareTask::get_consistent_scn_(share::SCN &consistent_scn) const
{
  int ret = OB_SUCCESS;
  ObBackupSetFileDesc backup_set_file;
  if (backup_data_type_.is_sys_backup()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need not get consistent scn during backup data of inner tablets", K(ret), K_(param), K_(backup_data_type));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(
                     *report_ctx_.sql_proxy_,
                     false/*for update*/,
                     param_.backup_set_desc_.backup_set_id_,
                     OB_START_INCARNATION,
                     param_.tenant_id_,
                     param_.dest_id_,
                     backup_set_file))) {
    LOG_WARN("failed to get backup set", K(ret), K_(param), K_(backup_data_type));
  } else if (OB_FALSE_IT(consistent_scn = backup_set_file.consistent_scn_)) {
  } else if (!consistent_scn.is_valid_and_not_min()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("consistent scn is not valid", K(ret), K_(param), K_(backup_data_type), K(backup_set_file));
  }

  return ret;
}

int ObLSBackupPrepareTask::may_need_advance_checkpoint_()
{
  int ret = OB_SUCCESS;
  int64_t rebuild_seq = 0;
  SCN backup_clog_checkpoint_scn;
  const bool check_archive = false;
  SCN consistent_scn;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(fetch_cur_ls_rebuild_seq_(rebuild_seq))) {
    LOG_WARN("failed to fetch cur ls rebuild seq", K(ret), K_(param));
  } else if (FALSE_IT(ls_backup_ctx_->rebuild_seq_ = rebuild_seq)) {
    // assign rebuild seq
  } else if (ls_backup_ctx_->backup_data_type_.is_sys_backup()) {
  } else if (OB_FAIL(fetch_backup_ls_meta_(backup_clog_checkpoint_scn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // this ls may be created after backup ls meta. and some tablets transfer in this ls.
      // we need to backup the tablets. but we didn't backup the ls meta. so we no need to advance checkpoint.
      ret = OB_SUCCESS;
      LOG_WARN("new created ls", K(ret), K_(param));
    } else {
      LOG_WARN("failed to fetch backup ls meta checkpoint scn", K(ret), K_(param));
    }
  } else if (!backup_data_type_.is_sys_backup() && OB_FAIL(get_consistent_scn_(consistent_scn))) {
    LOG_WARN("failed to get consistent scn", K(ret), K_(param), K_(backup_data_type));
  } else {
    const SCN clog_checkpoint_scn = backup_data_type_.is_sys_backup() ? backup_clog_checkpoint_scn : consistent_scn;
    const uint64_t tenant_id = param_.tenant_id_;
    const share::ObLSID &ls_id = param_.ls_id_;
    MTL_SWITCH(tenant_id) {
      storage::ObLSHandle ls_handle;
      storage::ObLS *ls = NULL;
      ObLSMeta cur_ls_meta;
      if (OB_FAIL(get_ls_handle(tenant_id, ls_id, ls_handle))) {
        LOG_WARN("failed to get ls handle", K(ret), K(tenant_id), K(ls_id));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log stream not exist", K(ret), K(ls_id));
      } else if (OB_FAIL(ls->get_ls_meta(cur_ls_meta))) {
        LOG_WARN("failed to get ls meta", K(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(cur_ls_meta.check_valid_for_backup())) {
        LOG_WARN("failed to check valid for backup", K(ret), K(cur_ls_meta));
      } else if (clog_checkpoint_scn <= cur_ls_meta.get_clog_checkpoint_scn()) {
        LOG_INFO("no need advance checkpoint", K_(param), K_(backup_data_type), K(clog_checkpoint_scn));
      } else if (OB_FAIL(advance_checkpoint_by_flush(tenant_id, ls_id, clog_checkpoint_scn, ls))) {
        LOG_WARN("failed to advance checkpoint by flush", K(ret), K(ls_id), K_(backup_data_type), K(clog_checkpoint_scn));
      } else {
        LOG_INFO("advance checkpint by flush", K_(param), K_(backup_data_type), K(clog_checkpoint_scn));
      }
    }
  }
  return ret;
}

int ObLSBackupPrepareTask::fetch_cur_ls_rebuild_seq_(int64_t &rebuild_seq)
{
  int ret = OB_SUCCESS;
  rebuild_seq = -1;
  storage::ObLSHandle ls_handle;
  storage::ObLS *ls = NULL;
  const uint64_t tenant_id = param_.tenant_id_;
  const share::ObLSID &ls_id = param_.ls_id_;
  const bool check_archive = true;
  MTL_SWITCH(tenant_id) {
    ObLSMeta cur_ls_meta;
    if (OB_FAIL(get_ls_handle(tenant_id, ls_id, ls_handle))) {
      LOG_WARN("failed to get ls handle", K(ret), K(tenant_id), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream not exist", K(ret), K(ls_id));
    } else if (OB_FAIL(ls->get_ls_meta(cur_ls_meta))) {
      LOG_WARN("failed to get ls meta", K(ret), K(tenant_id), K(ls_id));
    } else {
      rebuild_seq = cur_ls_meta.get_rebuild_seq();
    }
  }
  return ret;
}

int ObLSBackupPrepareTask::fetch_backup_ls_meta_(share::SCN &clog_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  clog_checkpoint_scn.reset();
  ObLSMetaPackage ls_meta_package;
  storage::ObBackupDataStore store;

  if (OB_FAIL(store.init(param_.backup_dest_, param_.backup_set_desc_))) {
    LOG_WARN("fail to init backup data store", K(ret));
  } else if (OB_FAIL(store.read_ls_meta_infos(param_.ls_id_, ls_meta_package))) {
    LOG_WARN("fail to red ls meta infos", K(ret), "ls_id", param_.ls_id_);
  } else if (!ls_meta_package.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta package", K(ret), K(ls_meta_package));
  } else {
    clog_checkpoint_scn = ls_meta_package.ls_meta_.get_clog_checkpoint_scn();
  }
  return ret;
}

int ObLSBackupPrepareTask::prepare_backup_tx_table_filled_tx_scn_()
{
  int ret = OB_SUCCESS;
  bool created_after_backup = false;
  if (!backup_data_type_.is_user_backup()) {
    // do nothing
  } else if (OB_FAIL(check_ls_created_after_backup_start_(param_.ls_id_, created_after_backup))) {
    LOG_WARN("failed to check ls created after backup start", K(ret), K_(param));
  } else if (created_after_backup) {
    LOG_INFO("ls is created after backup start and does not backup ls meta and inner tablet. no need to check tx data can explain user data", K(param_));
    SERVER_EVENT_ADD("backup_data", "ls_created_after_backup_start",
                     "tenant_id", param_.tenant_id_,
                     "job_id", param_.job_desc_.job_id_,
                     "ls_id", param_.ls_id_);
  } else if (OB_FAIL(get_backup_tx_data_table_filled_tx_scn_(ls_backup_ctx_->backup_tx_table_filled_tx_scn_))) {
    LOG_WARN("failed to get backup tx data table filled tx scn", K(ret));
  }
  return ret;
}

int ObLSBackupPrepareTask::get_backup_tx_data_table_filled_tx_scn_(SCN &filled_tx_scn)
{
  int ret = OB_SUCCESS;
  filled_tx_scn = SCN::max_scn();
  const common::ObTabletID &tx_data_tablet_id = LS_TX_DATA_TABLET;
  const ObBackupMetaType meta_type = ObBackupMetaType::BACKUP_SSTABLE_META;
  ObBackupDataType sys_backup_data_type;
  sys_backup_data_type.set_sys_data_backup();
  ObBackupMetaIndex meta_index;
  ObBackupPath backup_path;
  ObArray<ObBackupSSTableMeta> meta_array;
  ObBackupMetaIndexStore meta_index_store;
  ObStorageIdMod mod;
  mod.storage_id_ = param_.dest_id_;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  if (OB_FAIL(prepare_meta_index_store_(meta_index_store))) {
    LOG_WARN("failed to prepare meta index store", K(ret));
  } else if (OB_FAIL(meta_index_store.get_backup_meta_index(tx_data_tablet_id, meta_type, meta_index))) {
    LOG_WARN("failed to get backup meta index", K(ret), K(tx_data_tablet_id), K(meta_type));
  } else if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(param_.backup_dest_,
      param_.backup_set_desc_, param_.ls_id_, sys_backup_data_type, meta_index.turn_id_,
      meta_index.retry_id_, meta_index.file_id_, backup_path))) {
    LOG_WARN("failed to get ls meta index backup path", K(ret), K_(param), K(sys_backup_data_type), K(meta_index));
  } else if (OB_FAIL(ObLSBackupRestoreUtil::read_sstable_metas(
      backup_path.get_obstr(), param_.backup_dest_.get_storage_info(), mod, meta_index, &OB_BACKUP_META_CACHE, meta_array))) {
    LOG_WARN("failed to read sstable metas", K(ret), K(backup_path), K(meta_index));
  } else if (meta_array.empty()) {
    filled_tx_scn = SCN::min_scn();
    LOG_INFO("the log stream do not have tx data sstable", K(ret));
  } else {
    filled_tx_scn = meta_array.at(0).sstable_meta_.basic_meta_.filled_tx_scn_;
    ARRAY_FOREACH_X(meta_array, idx, cnt, OB_SUCC(ret)) {
      const ObBackupSSTableMeta &sstable_meta = meta_array.at(idx);
      const storage::ObITable::TableKey &table_key = sstable_meta.sstable_meta_.table_key_;
      if (ObITable::TableType::MINOR_SSTABLE == table_key.table_type_
          && sstable_meta.sstable_meta_.basic_meta_.filled_tx_scn_ > table_key.get_start_scn()) {
        filled_tx_scn = MAX(filled_tx_scn, sstable_meta.sstable_meta_.basic_meta_.filled_tx_scn_);
      }
    }
  }
  return ret;
}

int ObLSBackupPrepareTask::get_sys_ls_turn_and_retry_id_(int64_t &turn_id, int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  retry_id = -1;
  ObBackupDataStore store;
  ObBackupPath backup_path;
  ObBackupSetTaskAttr task_attr;
  turn_id = 0;
  if (OB_FAIL(ObBackupTaskOperator::get_backup_task(
    *report_ctx_.sql_proxy_, param_.job_desc_.job_id_, param_.tenant_id_, false, task_attr))) {
    LOG_WARN("failed to get backup task", K(ret), K_(param));
  } else if (OB_FALSE_IT(turn_id = param_.ls_id_.is_sys_ls() ? 1 : task_attr.meta_turn_id_)) {
  } else if (OB_FAIL(store.init(param_.backup_dest_))) {
    LOG_WARN("failed to init backup data store", K(ret), K_(param));
  } else if (OB_FAIL(ObBackupPathUtil::get_ls_backup_dir_path(
      param_.backup_dest_, param_.backup_set_desc_, param_.ls_id_, backup_path))) {
    LOG_WARN("failed to get ls backup dir path", K(ret), K_(param));
  } else if (OB_FAIL(store.get_max_sys_ls_retry_id(backup_path, param_.ls_id_, turn_id, retry_id))) {
    LOG_WARN("failed to get max sys retry id", K(ret), K(backup_path), K(param_));
  }
  return ret;
}

int ObLSBackupPrepareTask::check_ls_created_after_backup_start_(const ObLSID &ls_id, bool &created_after_backup)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  ObBackupDataLSAttrDesc ls_info;
  created_after_backup = true;
  ObBackupSetTaskAttr task_attr;
  common::ObMySQLProxy *sql_proxy = nullptr;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql prxoy must not be null", K(ret));
  } else if (OB_FAIL(ObBackupTaskOperator::get_backup_task(
        *sql_proxy, param_.job_desc_.job_id_, param_.tenant_id_, false, task_attr))) {
    LOG_WARN("failed to get backup task", K(ret), K_(param));
  } else if (OB_FAIL(store.init(param_.backup_dest_, param_.backup_set_desc_))) {
    LOG_WARN("failed to init backup data source", K(ret), K_(param));
  } else if (OB_FAIL(store.read_ls_attr_info(task_attr.meta_turn_id_, ls_info))) {
    LOG_WARN("failed to readd ls attr info", K(ret));
  } else {
    ARRAY_FOREACH(ls_info.ls_attr_array_, i) {
      if (ls_info.ls_attr_array_.at(i).get_ls_id() == ls_id) {
        created_after_backup = false;
      }
    }
  }
  return ret;
}

int ObLSBackupPrepareTask::prepare_meta_index_store_(ObBackupMetaIndexStore &meta_index_store)
{
  int ret = OB_SUCCESS;
  ObBackupRestoreMode mode = ObBackupRestoreMode::BACKUP_MODE;
  ObBackupIndexStoreParam index_store_param;
  int64_t sys_retry_id = -1;
  int64_t sys_turn_id = 0;
  if (OB_FAIL(get_sys_ls_turn_and_retry_id_(sys_turn_id, sys_retry_id))) {
    LOG_WARN("failed to get sys ls retry id", K(ret));
  } else if (OB_FAIL(prepare_meta_index_store_param_(sys_turn_id, sys_retry_id, index_store_param))) {
    LOG_WARN("failed to preparep meta index store param", K(ret), K(sys_retry_id));
  } else if (OB_FAIL(meta_index_store.init(mode, index_store_param, param_.backup_dest_,
     param_.backup_set_desc_, false/*is_sec_meta*/, *index_kv_cache_))) {
    LOG_WARN("failed to init meta index store", K(ret), K(mode), K(index_store_param));
  }
  return ret;
}

int ObLSBackupPrepareTask::prepare_meta_index_store_param_(
    const int64_t turn_id, const int64_t retry_id, ObBackupIndexStoreParam &index_param)
{
  int ret = OB_SUCCESS;
  if (retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("retry id is not valid", K(ret), K(retry_id));
  } else {
    index_param.index_level_ = ObBackupIndexLevel::BACKUP_INDEX_LEVEL_LOG_STREAM;
    index_param.tenant_id_ = param_.tenant_id_;
    index_param.backup_set_id_ = param_.backup_set_desc_.backup_set_id_;
    index_param.ls_id_ = param_.ls_id_;
    index_param.is_tenant_level_ = false;
    index_param.backup_data_type_.set_sys_data_backup();
    index_param.turn_id_ = turn_id;
    index_param.retry_id_ = retry_id;
    index_param.dest_id_ = param_.dest_id_;
  }
  return ret;
}

int ObLSBackupPrepareTask::get_cur_ls_min_filled_tx_scn_(SCN &min_filled_tx_scn)
{
  int ret = OB_SUCCESS;
  min_filled_tx_scn = SCN::max_scn();
  ObLSTabletIterator iterator(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  storage::ObLSHandle ls_handle;
  storage::ObLS *ls = NULL;
  ObLSTabletService *ls_tablet_svr = NULL;
  ObTabletHandle tablet_handle;
  const uint64_t tenant_id = param_.tenant_id_;
  const share::ObLSID &ls_id = param_.ls_id_;
  if (OB_FAIL(get_ls_handle(tenant_id, ls_id, ls_handle))) {
    LOG_WARN("failed to get ls handle", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream not exist", K(ret), K(ls_id));
  } else if (FALSE_IT(ls_tablet_svr = ls->get_tablet_svr())) {
  } else if (OB_FAIL(ls_tablet_svr->build_tablet_iter(iterator))) {
    STORAGE_LOG(WARN, "build ls table iter failed.", KR(ret));
  } else {
    while (OB_SUCC(iterator.get_next_tablet(tablet_handle))) {
      SCN tmp_filled_tx_scn = SCN::max_scn();
      bool has_minor_sstable = false;
      if (OB_FAIL(get_tablet_min_filled_tx_scn_(tablet_handle, tmp_filled_tx_scn, has_minor_sstable))) {
        STORAGE_LOG(WARN, "get min end_log_ts from a single tablet failed.", KR(ret));
      } else if (!has_minor_sstable) {
        continue;
      } else if (tmp_filled_tx_scn < min_filled_tx_scn) {
        min_filled_tx_scn = tmp_filled_tx_scn;
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLSBackupPrepareTask::get_tablet_min_filled_tx_scn_(
    ObTabletHandle &tablet_handle, SCN &min_filled_tx_scn, bool &has_minor_sstable)
{
  int ret = OB_SUCCESS;
  has_minor_sstable = false;
  min_filled_tx_scn = SCN::max_scn();
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is nullptr.", K(ret), K(tablet_handle));
  } else if (tablet->get_tablet_meta().tablet_id_.is_ls_inner_tablet()) {
    // skip inner tablet
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &sstable_array = table_store_wrapper.get_member()->get_minor_sstables();
    has_minor_sstable = !sstable_array.empty();
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      ObITable *table_ptr = sstable_array[i];
      ObSSTable *sstable = NULL;
      ObSSTableMetaHandle sst_meta_hdl;
      if (OB_ISNULL(table_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table ptr should not be null", K(ret));
      } else if (!table_ptr->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table ptr type not expectedd", K(ret));
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(table_ptr))) {
      } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
        LOG_WARN("fail to get sstable meta", K(ret));
      } else {
        min_filled_tx_scn = std::min(
          std::max(sst_meta_hdl.get_sstable_meta().get_filled_tx_scn(), sstable->get_end_scn()), min_filled_tx_scn);
      }
    }
  }
  return ret;
}

/* ObBackupIndexRebuildTask */

ObBackupIndexRebuildTask::ObBackupIndexRebuildTask()
    : ObITask(ObITaskType::TASK_TYPE_MIGRATE_COPY_PHYSICAL),
      is_inited_(false),
      param_(),
      index_level_(),
      ls_backup_ctx_(NULL),
      provider_(NULL),
      task_mgr_(NULL),
      index_kv_cache_(NULL),
      report_ctx_(),
      compressor_type_()
{}

ObBackupIndexRebuildTask::~ObBackupIndexRebuildTask()
{}

int ObBackupIndexRebuildTask::init(const ObLSBackupDataParam &param, const ObBackupIndexLevel &index_level,
    ObLSBackupCtx *ls_backup_ctx, ObIBackupTabletProvider *provider, ObBackupMacroBlockTaskMgr *task_mgr,
    ObBackupIndexKVCache *index_kv_cache, const ObBackupReportCtx &report_ctx, const ObCompressorType &compressor_type)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup dag init twice", K(ret));
  } else if (!param.is_valid() || index_level < BACKUP_INDEX_LEVEL_LOG_STREAM ||
             index_level >= MAX_BACKUP_INDEX_LEVEL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(param), K(index_level));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    report_ctx_ = report_ctx;
    index_level_ = index_level;
    ls_backup_ctx_ = ls_backup_ctx;
    provider_ = provider;
    task_mgr_ = task_mgr;
    index_kv_cache_ = index_kv_cache;
    compressor_type_ = compressor_type;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupIndexRebuildTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_report_error = false;
  DEBUG_SYNC(BEFORE_BACKUP_BUILD_INDEX);
  const int64_t start_ts = ObTimeUtility::current_time();
  LOG_INFO("start backup index rebuild", K_(index_level), K_(param));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild task do not init", K(ret));
  } else if (OB_NOT_NULL(ls_backup_ctx_) && OB_SUCCESS != ls_backup_ctx_->result_code_) {
    // do nothing
  } else if (OB_FAIL(check_all_tablet_released_())) {
    LOG_WARN("failed to check all tablet released", K(ret));
  } else if (OB_FAIL(mark_ls_task_final_())) {
    LOG_WARN("failed to mark ls task final", K(ret));
  } else if (OB_FAIL(merge_macro_index_())) {
    LOG_WARN("failed to merge macro index", K(ret), K_(param));
  } else if (OB_FAIL(merge_meta_index_())) {
    LOG_WARN("failed to merge meta index", K(ret), K_(param));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (0 == param_.ls_id_.id() && !param_.backup_data_type_.is_sys_backup()) {
      ret = OB_E(EventTable::EN_BACKUP_BUILD_TENANT_LEVEL_INDEX_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_build_tenant_level_index");
        LOG_WARN("errsim backup build tenant level index task failed", K(ret));
      }
    } else {
      ret = OB_E(EventTable::EN_BACKUP_BUILD_LS_LEVEL_INDEX_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_build_ls_level_index");
        LOG_WARN("errsim backup build ls level index task failed", K(ret));
      }
    }
  }
#endif
  if (OB_TMP_FAIL(report_check_tablet_info_event_())) {
    LOG_WARN("failed to report check tablet info event", K(tmp_ret));
  }
  if (OB_NOT_NULL(ls_backup_ctx_)) {
    ls_backup_ctx_->set_finished();
    bool is_set = false;
    ls_backup_ctx_->set_result_code(ret, is_set);
    need_report_error = is_set;
  }
  if (0 == param_.ls_id_.id() || need_report_error) {
    REPORT_TASK_RESULT(this->get_dag()->get_dag_id(), ret);
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_ts;
  record_server_event_(cost_us);
  return ret;
}

int ObBackupIndexRebuildTask::check_all_tablet_released_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (0 == param_.ls_id_.id()) {
    LOG_INFO("do nothing if tenant level build");
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else {
    bool all_released = ls_backup_ctx_->tablet_holder_.is_empty();
    if (!all_released) {
      LOG_WARN("tablet handle not released", K(ret));
      if (OB_SUCCESS != (tmp_ret = ls_backup_ctx_->tablet_stat_.print_tablet_stat())) {
        LOG_WARN("failed to print tablet stat", K(ret), K(tmp_ret));
      }
      ls_backup_ctx_->tablet_stat_.reuse();
    }
  }
  return ret;
}

int ObBackupIndexRebuildTask::mark_ls_task_final_()
{
  int ret = OB_SUCCESS;
  if (0 == param_.ls_id_.id()) {
    // do nothing
  } else if (OB_FAIL(ObLSBackupOperator::mark_ls_task_info_final(param_.job_desc_.task_id_,
                 param_.tenant_id_,
                 param_.ls_id_,
                 param_.turn_id_,
                 param_.retry_id_,
                 param_.backup_data_type_,
                 *report_ctx_.sql_proxy_))) {
    LOG_WARN("failed to mark ls task info final", K(ret), K_(param));
  }
  return ret;
}

bool ObBackupIndexRebuildTask::need_build_index_(const bool is_build_macro_index) const
{
  bool bret = true;
  if (0 == param_.ls_id_.id() && is_build_macro_index) {
    if (is_build_macro_index) {
      bret = param_.backup_data_type_.is_user_backup();
    } else {
      bret = !param_.backup_data_type_.is_sys_backup();
    }
  }
  return bret;
}

int ObBackupIndexRebuildTask::merge_macro_index_()
{
  int ret = OB_SUCCESS;
  if (need_build_index_(true)) {
    SMART_VARS_2((ObBackupUnorderdMacroBlockIndexMerger, macro_index_merger),
                 (ObBackupIndexMergeParam, merge_param)) {
      if (OB_FAIL(param_.convert_to(index_level_, compressor_type_, merge_param))) {
        LOG_WARN("failed to convert param", K(ret), K_(param), K_(index_level));
      } else if (OB_FAIL(macro_index_merger.init(merge_param, *report_ctx_.sql_proxy_, *GCTX.bandwidth_throttle_))) {
        LOG_WARN("failed to init index merger", K(ret), K(merge_param));
      } else if (OB_FAIL(macro_index_merger.merge_index())) {
        LOG_WARN("failed to merge macro index", K(ret), K_(param));
      } else {
        LOG_INFO("do merge macro block index", K(merge_param));
      }
    }
  }
  return ret;
}

int ObBackupIndexRebuildTask::merge_meta_index_()
{
  int ret = OB_SUCCESS;
  if (need_build_index_(false)) {
    ObBackupIndexMergeParam merge_param;
    ObBackupMetaIndexMerger meta_index_merger;
    if (OB_FAIL(param_.convert_to(index_level_, compressor_type_, merge_param))) {
      LOG_WARN("failed to convert param", K(ret), K_(param), K_(index_level));
    } else if (OB_FALSE_IT(merge_param.backup_data_type_ = param_.backup_data_type_)) {
    } else if (OB_FAIL(meta_index_merger.init(merge_param, *report_ctx_.sql_proxy_, *GCTX.bandwidth_throttle_))) {
      LOG_WARN("failed to init index merger", K(ret), K(merge_param));
    } else if (OB_FAIL(meta_index_merger.merge_index())) {
      LOG_WARN("failed to merge meta index", K(ret), K_(param));
    } else {
      LOG_INFO("do merge meta index", K(merge_param));
    }
  }
  return ret;
}

void ObBackupIndexRebuildTask::record_server_event_(const int64_t cost_us) const
{
  SERVER_EVENT_ADD("backup",
      "build_index",
      "tenant_id",
      param_.tenant_id_,
      "backup_set_id",
      param_.backup_set_desc_.backup_set_id_,
      "ls_id",
      param_.ls_id_.id(),
      "retry_id",
      param_.retry_id_,
      "cost_us",
      cost_us);
  FLOG_INFO("build backup index finish", K_(param), K(cost_us));
}

int ObBackupIndexRebuildTask::report_check_tablet_info_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (BACKUP_INDEX_LEVEL_TENANT == index_level_) {
    // do nothing
  } else if (!param_.backup_data_type_.is_user_backup()) {
    // do nothing
  } else {
    const int64_t total_cost_time = ls_backup_ctx_->check_tablet_info_cost_time_;
    const int64_t total_tablet_count = ls_backup_ctx_->data_tablet_id_list_.count();
    int64_t avg_cost_time = 0;
    if (0 != total_tablet_count) {
      avg_cost_time = total_cost_time / total_tablet_count;
    }
    SERVER_EVENT_ADD("backup_data", "check_tablet_info",
                     "tenant_id", ls_backup_ctx_->param_.tenant_id_,
                     "ls_id", ls_backup_ctx_->param_.ls_id_.id(),
                     "total_cost_time_us", total_cost_time,
                     "total_tablet_count", total_tablet_count,
                     "avg_cost_time_us", avg_cost_time);
  }
  return ret;
}

/* ObLSBackupFinishTask */

ObLSBackupFinishTask::ObLSBackupFinishTask()
    : ObITask(TASK_TYPE_MIGRATE_COPY_PHYSICAL),
      is_inited_(false),
      param_(),
      report_ctx_(),
      ls_backup_ctx_(NULL),
      index_kv_cache_(NULL)
{}

ObLSBackupFinishTask::~ObLSBackupFinishTask()
{}

int ObLSBackupFinishTask::init(const ObLSBackupDataParam &param, const ObBackupReportCtx &report_ctx,
    ObLSBackupCtx &ls_backup_ctx, ObBackupIndexKVCache &index_kv_cache)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup dag init twice", K(ret));
  } else if (!param.is_valid() || !report_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(report_ctx));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    report_ctx_ = report_ctx;
    ls_backup_ctx_ = &ls_backup_ctx;
    index_kv_cache_ = &index_kv_cache;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_BACKUP_FINISH);
  int64_t result = OB_SUCCESS;
  const uint64_t tenant_id = param_.tenant_id_;
  const int64_t task_id = param_.job_desc_.task_id_;
  const share::ObLSID &ls_id = param_.ls_id_;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_UNLIKELY(!ls_backup_ctx_->is_finished())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should be finished", K(ret));
  } else if (FALSE_IT(ls_backup_ctx_->stat_mgr_.mark_end(ls_backup_ctx_->backup_data_type_))) {
  } else if (FALSE_IT(result = ls_backup_ctx_->get_result_code())) {
  } else {
    ls_backup_ctx_->stat_mgr_.print_stat();
    SERVER_EVENT_ADD("backup_data", "report_wait_reuse_across_sstable_time",
                     "tenant_id", tenant_id,
                     "task_id", task_id,
                     "ls_id", ls_id.id(),
                     "wait_reuse_time", ls_backup_ctx_->wait_reuse_across_sstable_time_);
  }
#ifdef ERRSIM
  if (ls_backup_ctx_->param_.ls_id_.id() == GCONF.errsim_backup_ls_id) {
    ret = EN_LS_BACKUP_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      result = ret;
      SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_macro_block_failed", "ret", ret);
      ret = OB_SUCCESS;
    }
  }
#endif
  REPORT_TASK_RESULT(this->get_dag()->get_dag_id(), result);

  return ret;
}

}  // namespace backup
}  // namespace oceanbase
