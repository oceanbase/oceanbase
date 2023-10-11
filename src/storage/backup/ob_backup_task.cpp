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
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/backup/ob_archive_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_rs_mgr.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/backup/ob_backup_iterator.h"
#include "storage/backup/ob_backup_operator.h"
#include "storage/backup/ob_backup_utils.h"
#include "storage/backup/ob_backup_restore_util.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_macro_block_checker.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/checkpoint/ob_checkpoint_executor.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "logservice/archiveservice/ob_archive_file_utils.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/ob_storage_rpc.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_archive_store.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant.h"
#include <algorithm>

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
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
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
        if (OB_FAIL(ls->advance_checkpoint_by_flush(start_scn))) {
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
        share::dag_yield();
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
      compl_end_scn_()
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
  return job_desc_ == other.job_desc_ && backup_dest_ == other.backup_dest_ && tenant_id_ == other.tenant_id_ &&
         backup_set_desc_ == other.backup_set_desc_ && ls_id_ == other.ls_id_ &&
         turn_id_ == other.turn_id_ && retry_id_ == other.retry_id_ && dest_id_ == other.dest_id_;
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
      if (OB_TMP_FAIL(dag_scheduler->cancel_dag(finish_dag, prepare_dag))) {
        LOG_ERROR("failed to cancel backup dag", K(tmp_ret), KP(dag_scheduler), KP(finish_dag));
      } else {
        finish_dag = nullptr;
      }
    } else if (OB_FAIL(dag_scheduler->add_dag(backup_meta_dag))) {
      LOG_WARN("failed to add dag", K(ret), KP(prepare_dag));
      if (OB_TMP_FAIL(dag_scheduler->cancel_dag(finish_dag, prepare_dag))) {
        LOG_ERROR("failed to cancel backup dag", K(tmp_ret), KP(dag_scheduler), KP(finish_dag));
      } else {
        finish_dag = nullptr;
      }
      if (OB_TMP_FAIL(dag_scheduler->cancel_dag(prepare_dag, backup_meta_dag))) {
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
  } else if (OB_FAIL(ls_backup_ctx_.open(backup_param, backup_data_type_, *param_.report_ctx_.sql_proxy_))) {
    LOG_WARN("failed to open log stream backup ctx", K(ret), K(backup_param));
  } else if (OB_FAIL(prepare_backup_tablet_provider_(backup_param, backup_data_type_, ls_backup_ctx_,
      OB_BACKUP_INDEX_CACHE, *param_.report_ctx_.sql_proxy_, provider_))) {
    LOG_WARN("failed to prepare backup tablet provider", K(ret), K(backup_param), K_(backup_data_type));
  } else if (OB_FAIL(get_batch_size_(batch_size))) {
    LOG_WARN("failed to get batch size", K(ret));
  } else if (OB_FAIL(task_mgr_.init(backup_data_type_, batch_size))) {
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
  const int64_t data_file_size = GCONF.backup_data_file_size;
  if (0 == data_file_size) {
    batch_size = OB_DEFAULT_BACKUP_BATCH_COUNT;
  } else {
    batch_size = data_file_size / OB_DEFAULT_MACRO_BLOCK_SIZE;
  }
#ifdef ERRSIM
  const int64_t max_block_per_backup_task = GCONF._max_block_per_backup_task;
  if (0 != max_block_per_backup_task) {
    batch_size = max_block_per_backup_task;
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

/* ObLSBackupComplementLogDagNet */

ObLSBackupComplementLogDagNet::ObLSBackupComplementLogDagNet()
    : ObBackupDagNet(ObBackupDagNetSubType::LOG_STREAM_BACKUP_COMPLEMENT_LOG_DAG_NET),
      is_inited_(false),
      param_(),
      report_ctx_(),
      compl_start_scn_(),
      compl_end_scn_()
{}

ObLSBackupComplementLogDagNet::~ObLSBackupComplementLogDagNet()
{}

int ObLSBackupComplementLogDagNet::init_by_param(const share::ObIDagInitParam *param)
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
    param_.ls_id_ = init_param.ls_id_;
    param_.turn_id_ = init_param.turn_id_;
    param_.retry_id_ = init_param.retry_id_;
    param_.dest_id_ = init_param.dest_id_;
    report_ctx_ = init_param.report_ctx_;
    compl_start_scn_ = init_param.compl_start_scn_;
    compl_end_scn_ = init_param.compl_end_scn_;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupComplementLogDagNet::start_running()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSBackupComplementLogDag *complement_dag = NULL;
  ObTenantDagScheduler *dag_scheduler = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net not init", K(ret));
  } else if (OB_FAIL(guard.switch_to(param_.tenant_id_))) {
    LOG_WARN("failed to switch to tenant", K(ret), K_(param));
  } else if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler must not be NULL", K(ret));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(complement_dag))) {
    LOG_WARN("failed to alloc rebuild index dag", K(ret));
  } else if (OB_FAIL(complement_dag->init(param_.job_desc_,
                 param_.backup_dest_,
                 param_.tenant_id_,
                 param_.dest_id_,
                 param_.backup_set_desc_,
                 param_.ls_id_,
                 param_.turn_id_,
                 param_.retry_id_,
                 compl_start_scn_,
                 compl_end_scn_,
                 report_ctx_))) {
    LOG_WARN("failed to init child dag", K(ret), K_(param), K_(compl_start_scn), K_(compl_end_scn));
  } else if (OB_FAIL(complement_dag->create_first_task())) {
    LOG_WARN("failed to create first task for child dag", K(ret), KPC(complement_dag));
  } else if (OB_FAIL(add_dag_into_dag_net(*complement_dag))) {
    LOG_WARN("failed to add dag into dag_net", K(ret), KPC(complement_dag));
  } else if (OB_FAIL(dag_scheduler->add_dag(complement_dag))) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("failed to add dag", K(ret), KPC(complement_dag));
    } else {
      LOG_WARN("may exist same dag", K(ret), KPC(complement_dag));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(complement_dag)) {
    dag_scheduler->free_dag(*complement_dag);
  }

  if (OB_FAIL(ret)) {
    REPORT_TASK_RESULT(this->get_dag_id(), ret);
  }
  return ret;
}

bool ObLSBackupComplementLogDagNet::operator==(const share::ObIDagNet &other) const
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
      const ObLSBackupComplementLogDagNet &dag = static_cast<const ObLSBackupComplementLogDagNet &>(other);
      bret = dag.param_ == param_;
    }
  }
  return bret;
}

bool ObLSBackupComplementLogDagNet::is_valid() const
{
  return param_.is_valid() && compl_start_scn_.is_valid() && compl_end_scn_.is_valid();
}

int64_t ObLSBackupComplementLogDagNet::hash() const
{
  int64_t hash_value = 0;
  const int64_t type = ObBackupDagNetSubType::LOG_STREAM_BACKUP_COMPLEMENT_LOG_DAG_NET;
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value = common::murmurhash(&param_, sizeof(param_), hash_value);
  return hash_value;
}

int ObLSBackupComplementLogDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 "[BACKUP_COMPLEMENT_LOG_DAG_NET]: tenant_id=%lu, backup_set_id=%ld, ls_id=%ld",
                 param_.tenant_id_,
                 param_.backup_set_desc_.backup_set_id_,
                 param_.ls_id_.id()))) {
    LOG_WARN("failed to fill comment", K(ret), K(param_));
  };
  return ret;
}

int ObLSBackupComplementLogDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
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
      ls_backup_ctx_(nullptr)
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
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%s", to_cstring(param_.ls_id_)))) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(param));
  }
  return ret;
}

int64_t ObLSBackupMetaDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}

lib::Worker::CompatMode ObLSBackupMetaDag::get_compat_mode() const
{
  return lib::Worker::CompatMode::MYSQL;
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
      report_ctx_()
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
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%s", to_cstring(param_.ls_id_)))) {
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

lib::Worker::CompatMode ObLSBackupPrepareDag::get_compat_mode() const
{
  return lib::Worker::CompatMode::MYSQL;
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
    case ObBackupDataType::BACKUP_MINOR:
    case ObBackupDataType::BACKUP_MAJOR: {
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
    : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_FINISH), is_inited_(false), report_ctx_(), ls_backup_ctx_(NULL), index_kv_cache_(NULL)
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
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%s", to_cstring(param_.ls_id_)))) {
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

lib::Worker::CompatMode ObLSBackupFinishDag::get_compat_mode() const
{
  return lib::Worker::CompatMode::MYSQL;
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
      index_rebuild_dag_(NULL)
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
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%s", to_cstring(param_.ls_id_)))) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(param));
  }
  return ret;
}

lib::Worker::CompatMode ObLSBackupDataDag::get_compat_mode() const
{
  return lib::Worker::CompatMode::MYSQL;
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
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%s", to_cstring(param_.ls_id_)))) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(param));
  }
  return ret;
}

/* ObLSBackupIndexRebuildDag */

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
      index_kv_cache_(NULL)
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
                 report_ctx_))) {
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
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%s", to_cstring(param_.ls_id_)))) {
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

lib::Worker::CompatMode ObLSBackupIndexRebuildDag::get_compat_mode() const
{
  return lib::Worker::CompatMode::MYSQL;
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

/* ObLSBackupComplementLogDag */

ObLSBackupComplementLogDag::ObLSBackupComplementLogDag()
    : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_COMPLEMENT_LOG),
      is_inited_(false),
      job_desc_(),
      backup_dest_(),
      tenant_id_(OB_INVALID_ID),
      dest_id_(),
      backup_set_desc_(),
      ls_id_(),
      turn_id_(0),
      retry_id_(-1),
      compl_start_scn_(),
      compl_end_scn_(),
      report_ctx_()
{}

ObLSBackupComplementLogDag::~ObLSBackupComplementLogDag()
{}

int ObLSBackupComplementLogDag::init(const ObBackupJobDesc &job_desc, const ObBackupDest &backup_dest,
    const uint64_t tenant_id, const int64_t dest_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
    const int64_t turn_id, const int64_t retry_id, const SCN &start_scn, const SCN &end_scn,
    const ObBackupReportCtx &report_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls backup complement log task init twice", K(ret));
  } else if (!job_desc.is_valid() || !backup_dest.is_valid() || OB_INVALID_ID == tenant_id ||
             !backup_set_desc.is_valid() || !ls_id.is_valid() || !start_scn.is_valid() || !end_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args",
        K(ret),
        K(job_desc),
        K(backup_dest),
        K(tenant_id),
        K(backup_set_desc),
        K(ls_id),
        K(start_scn),
        K(end_scn));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    job_desc_ = job_desc;
    tenant_id_ = tenant_id;
    dest_id_ = dest_id;
    backup_set_desc_ = backup_set_desc;
    ls_id_ = ls_id;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    compl_start_scn_ = start_scn;
    compl_end_scn_ = end_scn;
    report_ctx_ = report_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupComplementLogDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObLSBackupComplementLogTask *task = NULL;
  if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(job_desc_, backup_dest_, tenant_id_, dest_id_, backup_set_desc_, ls_id_, compl_start_scn_,
      compl_end_scn_, turn_id_, retry_id_, report_ctx_))) {
    LOG_WARN("failed to init task", K(ret), K_(tenant_id), K_(backup_set_desc), K_(ls_id), K_(compl_start_scn), K_(compl_end_scn));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("success to add complement log task", K(ret), KPC(this), KPC(task));
  }
  return ret;
}

int ObLSBackupComplementLogDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup complement log dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  static_cast<int64_t>(tenant_id_), backup_set_desc_.backup_set_id_,
                                  ls_id_.id()))){
    LOG_WARN("failed to add dag warning info param", K(ret));
  }
  return ret;
}

int ObLSBackupComplementLogDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%s", to_cstring(ls_id_)))) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(ls_id));
  }
  return ret;
}

bool ObLSBackupComplementLogDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
      const ObLSBackupComplementLogDag &other_dag = static_cast<const ObLSBackupComplementLogDag &>(other);
      bret = job_desc_ == other_dag.job_desc_ && backup_dest_ == other_dag.backup_dest_ &&
                tenant_id_ == other_dag.tenant_id_ && dest_id_ == other_dag.dest_id_ && backup_set_desc_ == other_dag.backup_set_desc_ &&
                ls_id_ == other_dag.ls_id_;
  }
  return bret;
}

int64_t ObLSBackupComplementLogDag::hash() const
{
  int64_t hash_value = 0;
  const int64_t type = get_type();
  hash_value = common::murmurhash(&job_desc_, sizeof(job_desc_), hash_value);
  hash_value = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  hash_value = common::murmurhash(&backup_set_desc_, sizeof(backup_set_desc_), hash_value);
  hash_value = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_value);
  return hash_value;
}

lib::Worker::CompatMode ObLSBackupComplementLogDag::get_compat_mode() const
{
  return lib::Worker::CompatMode::MYSQL;
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
    ObBackupMacroBlockIndexStore &index_store)
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
     if (OB_FAIL(index_store.init(mode, index_store_param, param.backup_dest_,
            backup_set_desc, *index_kv_cache_, *report_ctx.sql_proxy_))) {
      LOG_WARN("failed to init macro index store", K(ret), K(param), K(index_store_param), K(backup_set_desc), K(backup_data_type));
    }
  }
  return ret;
}

int ObPrefetchBackupInfoTask::inner_init_macro_index_store_for_inc_(const ObLSBackupDagInitParam &param,
    const share::ObBackupDataType &backup_data_type, const ObBackupReportCtx &report_ctx)
{
  int ret = OB_SUCCESS;
  if (param.backup_set_desc_.backup_type_.is_inc_backup() && backup_data_type.is_major_backup()) {
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
  if (backup_data_type.is_major_backup()) {
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
  ObArray<ObBackupPhysicalID> no_need_copy_macro_index_list;
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
      } else if (OB_FAIL(task_mgr_->receive(task_id, need_copy_item_list))) {
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
          if (!is_run_out) {
            if (OB_FAIL(generate_next_prefetch_dag_())) {
              LOG_WARN("failed to generate prefetch dag", K(ret));
            } else {
              LOG_INFO("generate next prefetch dag", K(items), K_(backup_data_type));
            }
          } else {
            LOG_INFO("run out", K_(param), K_(backup_data_type), K(items));
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
          LOG_INFO("generate backup dag", K(task_id), K(file_id));
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
  const bool is_sec_meta = false;
  if (!backup_dest.is_valid() || !backup_set_desc.is_valid() || !backup_data_type.is_valid() || turn_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(backup_set_desc), K(backup_data_type), K(turn_id));
  } else if (OB_FAIL(retry_id_getter.init(backup_dest, backup_set_desc, backup_data_type,
      turn_id, is_restore, is_macro_index, is_sec_meta))) {
    LOG_WARN("failed to init retry id getter", K(ret), K_(param));
  } else if (OB_FAIL(retry_id_getter.get_max_retry_id(retry_id))) {
    LOG_WARN("failed to get max retry id", K(ret));
  }
  return ret;
}

int ObPrefetchBackupInfoTask::get_need_copy_item_list_(const common::ObIArray<ObBackupProviderItem> &list,
    common::ObIArray<ObBackupProviderItem> &need_copy_list, common::ObIArray<ObBackupProviderItem> &no_need_copy_list,
    common::ObIArray<ObBackupPhysicalID> &no_need_copy_macro_index_list)
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
        ObBackupPhysicalID physical_id;
        if (OB_FAIL(no_need_copy_list.push_back(item))) {
          LOG_WARN("failed to push back", K(ret), K(item));
        } else if (OB_FAIL(macro_index.get_backup_physical_id(physical_id))) {
          LOG_WARN("failed to get backup physical id", K(ret), K(physical_id));
        } else if (OB_FAIL(no_need_copy_macro_index_list.push_back(physical_id))) {
          LOG_WARN("failed to push back", K(ret), K(macro_index), K(physical_id));
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
  if (item.get_item_type() != PROVIDER_ITEM_MACRO_ID) {
    need_copy = true;
  } else if (!backup_data_type_.is_major_backup()) {
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
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown backup type", K(ret), K(param_));
    }
  }
  if (OB_SUCC(ret) && need_copy) {
    if (OB_FAIL(inner_check_backup_item_need_copy_when_change_turn_(item, need_copy, macro_index))) {
      LOG_WARN("failed to inner check backup item need copy when change turn", K(ret));
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
    LOG_INFO("macro index store for turn is not inited", K(ret));
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
      disk_checker_(),
      allocator_(),
      backup_items_(),
      finished_tablet_list_(),
      index_rebuild_dag_(NULL)
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
  } else if (OB_FAIL(backup_data_ctx_.open(param, backup_data_type, task_id))) {
    LOG_WARN("failed to open backup data ctx", K(ret), K(param), K(backup_data_type));
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
    if (!param_.ls_id_.is_sys_ls() && backup_data_type_.is_major_backup() && param_.retry_id_ <= 2 && task_id_ >= 2) {
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
  if (OB_FAIL(ret)) {
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("log stream backup data task not init", K(ret));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
    LOG_INFO("already failed, no need backup again");
  } else if (OB_FAIL(ObBackupUtils::check_ls_validity(param_.tenant_id_, param_.ls_id_))) {
    LOG_WARN("failed to check ls validity", K(ret), K_(param));
  } else if (OB_FAIL(ObBackupUtils::check_ls_valid_for_backup(
      param_.tenant_id_, param_.ls_id_, ls_backup_ctx_->rebuild_seq_))) {
    LOG_WARN("failed to check ls valid for backup", K(ret), K_(param));
  } else if (OB_FAIL(do_write_file_header_())) {
    LOG_WARN("failed to do write file header", K(ret));
  } else if (OB_FAIL(do_backup_macro_block_data_())) {
    LOG_WARN("failed to do backup macro block data", K(ret));
  } else if (OB_FAIL(do_backup_meta_data_())) {
    LOG_WARN("failed to do backup meta data", K(ret));
  } else if (OB_FAIL(ObBackupUtils::check_ls_valid_for_backup(
      param_.tenant_id_, param_.ls_id_, ls_backup_ctx_->rebuild_seq_))) {
    LOG_WARN("failed to check ls valid for backup", K(ret), K_(param));
  } else if (OB_FAIL(finish_backup_items_())) {
    LOG_WARN("failed to finish backup items", K(ret));
  } else if (OB_FAIL(finish_task_in_order_())) {
    LOG_WARN("failed to finish task in order", K(ret));
  } else if (OB_FAIL(do_generate_next_task_())) {
    LOG_WARN("failed to do generate next task", K(ret));
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
    } else if (backup_data_type_.is_minor_backup()) {
      ret = OB_E(EventTable::EN_BACKUP_DATA_TABLET_MINOR_SSTABLE_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_minor_sstable");
        LOG_WARN("errsim backup data tablet minor sstable task failed", K(ret));
      }
    } else if (backup_data_type_.is_major_backup()) {
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

int ObLSBackupDataTask::do_backup_macro_block_data_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMacroBlockId> macro_list;
  ObMultiMacroBlockBackupReader *reader = NULL;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(get_macro_block_id_list_(macro_list))) {
    LOG_WARN("failed to get macro block id list", K(ret));
  } else if (OB_UNLIKELY(macro_list.empty())) {
    LOG_INFO("no macro list need to backup");
  } else if (OB_FAIL(prepare_macro_block_reader_(param_.tenant_id_, macro_list, reader))) {
    LOG_WARN("failed to prepare macro block reader", K(ret), K(macro_list));
  } else {
    LOG_INFO("start backup macro block data", K(macro_list));
    while (OB_SUCC(ret)) {
      ObBufferReader buffer_reader;
      ObLogicMacroBlockId logic_id;
      ObBackupMacroBlockIndex macro_index;
      ObBackupProviderItem backup_item;
      ObBackupPhysicalID physical_id;
      if (REACH_TIME_INTERVAL(CHECK_DISK_SPACE_INTERVAL)) {
        if (OB_FAIL(check_disk_space_())) {
          LOG_WARN("failed to check disk space", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_next_macro_block_data_(reader, buffer_reader, logic_id))) {
        if (OB_ITER_END == ret) {
          LOG_INFO("iterator meet end", K(logic_id));
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next macro block data", K(ret));
        }
      } else if (OB_FAIL(check_macro_block_data_(buffer_reader))) {
        LOG_WARN("failed to check macro block data", K(ret), K(buffer_reader));
      } else if (OB_FAIL(write_macro_block_data_(buffer_reader, logic_id, macro_index))) {
        LOG_WARN("failed to write macro block data", K(ret), K(buffer_reader), K(logic_id));
      } else if (OB_FAIL(macro_index.get_backup_physical_id(physical_id))) {
        LOG_WARN("failed to get backup physical id", K(ret), K(macro_index));
      } else if (OB_FAIL(get_backup_item_(logic_id, backup_item))) {
        LOG_WARN("failed to get backup item", K(ret), K(logic_id));
      } else if (OB_FAIL(mark_backup_item_finished_(backup_item, physical_id))) {
        LOG_WARN("failed to mark backup item finished", K(ret), K(backup_item), K(macro_index), K(physical_id));
      } else if (OB_FAIL(ls_backup_ctx_->stat_mgr_.add_macro_block(backup_data_type_, logic_id))) {
        LOG_WARN("failed to add macro block", K(ret));
      } else if (OB_FAIL(ls_backup_ctx_->stat_mgr_.add_bytes(backup_data_type_, macro_index.length_))) {
        LOG_WARN("failed to add bytes", K(ret));
      } else {
        backup_stat_.input_bytes_ += OB_DEFAULT_MACRO_BLOCK_SIZE;
        backup_stat_.finish_macro_block_count_ += 1;
      }
    }
  }
  if (OB_NOT_NULL(reader)) {
    ObLSBackupFactory::free(reader);
  }
  return ret;
}

int ObLSBackupDataTask::do_backup_meta_data_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObBackupProviderItem> item_list;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(get_meta_item_list_(item_list))) {
    LOG_WARN("failed to get meta item list", K(ret));
  } else if (item_list.empty()) {
    LOG_INFO("no meta need to back up");
  } else {
    LOG_INFO("start backup meta data", K(item_list));
    for (int64_t i = 0; OB_SUCC(ret) && i < item_list.count(); ++i) {
      const ObBackupProviderItem &item = item_list.at(i);
      const common::ObTabletID &tablet_id = item.get_tablet_id();
      ObITabletMetaBackupReader *reader = NULL;
      ObBufferReader buffer_reader;
      ObTabletHandle tablet_handle;
      ObTabletMetaReaderType reader_type;
      ObBackupMetaType meta_type;
      ObBackupMetaIndex meta_index;
      const ObBackupPhysicalID physical_id = ObBackupPhysicalID::get_default();
      if (OB_FAIL(get_tablet_meta_info_(item, reader_type, meta_type))) {
        LOG_WARN("failed to get tablet meta info", K(ret), K(item));
      } else if (OB_FAIL(get_tablet_handle_(tablet_id, tablet_handle))) {
        LOG_WARN("failed to get tablet handle", K(ret), K_(task_id), K(tablet_id), K(item), K(i), K(item_list));
      } else if (OB_FAIL(prepare_tablet_meta_reader_(tablet_id, reader_type, tablet_handle, reader))) {
        LOG_WARN("failed to prepare tablet meta reader", K(tablet_id), K(reader_type));
      } else if (OB_FAIL(reader->get_meta_data(buffer_reader))) {
        LOG_WARN("failed to get meta data", K(ret), K(tablet_id));
      } else if (OB_FAIL(write_backup_meta_(buffer_reader, tablet_id, meta_type, meta_index))) {
        LOG_WARN("failed to write backup meta", K(ret), K(buffer_reader), K(tablet_id), K(meta_type));
      } else if (OB_FAIL(mark_backup_item_finished_(item, physical_id))) {
        LOG_WARN("failed to mark backup item finished", K(ret), K(item), K(physical_id));
      } else {
        backup_stat_.input_bytes_ += buffer_reader.capacity();
        if (PROVIDER_ITEM_TABLET_META == item.get_item_type()) {
          backup_stat_.finish_tablet_count_ += 1;
          ls_backup_ctx_->stat_mgr_.add_tablet_meta(backup_data_type_, item.get_tablet_id());
          ls_backup_ctx_->stat_mgr_.add_bytes(backup_data_type_, meta_index.length_);
        } else if (PROVIDER_ITEM_SSTABLE_META == item.get_item_type()) {
          backup_stat_.finish_sstable_count_ += 1;
          ls_backup_ctx_->stat_mgr_.add_sstable_meta(backup_data_type_, item.get_table_key());
          ls_backup_ctx_->stat_mgr_.add_bytes(backup_data_type_, meta_index.length_);
        }
      }
      if (OB_NOT_NULL(reader)) {
        ObLSBackupFactory::free(reader);
      }
    }
  }
  return ret;
}

int ObLSBackupDataTask::get_tablet_meta_info_(
    const ObBackupProviderItem &item, ObTabletMetaReaderType &reader_type, ObBackupMetaType &meta_type)
{
  int ret = OB_SUCCESS;
  switch (item.get_item_type()) {
    case PROVIDER_ITEM_SSTABLE_META: {
      reader_type = SSTABLE_META_READER;
      meta_type = BACKUP_SSTABLE_META;
      break;
    }
    case PROVIDER_ITEM_TABLET_META: {
      reader_type = TABLET_META_READER;
      meta_type = BACKUP_TABLET_META;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid type", K(ret), K(item));
      break;
    }
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
  ObBackupLSTaskInfo ls_task_info;
  if (OB_FAIL(trans.start(report_ctx_.sql_proxy_, gen_meta_tenant_id(param_.tenant_id_)))) {
    LOG_WARN("failed to start transaction", K(ret), K(param_));
  } else {
    ObLSBackupStat new_stat;
    share::ObBackupStats new_task_stat;
    share::ObBackupLSTaskAttr ls_task_attr;
    if (OB_FAIL(share::ObBackupLSTaskOperator::get_ls_task(trans, for_update,
              param_.job_desc_.task_id_, param_.tenant_id_, param_.ls_id_, ls_task_attr))) {
        LOG_WARN("failed to get ls task", K(ret), K_(param));
    } else if (OB_FAIL(ObLSBackupOperator::get_backup_ls_task_info(param_.tenant_id_,
            param_.job_desc_.task_id_,
            param_.ls_id_,
            param_.turn_id_,
            param_.retry_id_,
            param_.backup_data_type_,
            for_update,
            ls_task_info,
            trans))) {
      LOG_WARN("failed to get backup ls task info", K(ret), K(param_));
    } else if (ls_task_info.is_final_) {
      LOG_INFO("can not update if final", K(ls_task_info), K(stat));
    } else if (ls_task_info.max_file_id_ + 1 != stat.file_id_) {
      LOG_INFO("can not update if file id is not consecutive", K(ls_task_info), K(stat));
    } else if (OB_FAIL(update_task_stat_(ls_task_attr.stats_, stat, new_task_stat))) {
      LOG_WARN("failed to update task stat", K(ret), K(ls_task_attr));
    } else if (OB_FAIL(update_task_info_stat_(ls_task_info, stat, new_stat))) {
      LOG_WARN("failed to update task info stat", K(ret), K(ls_task_info), K(stat));
    } else if (OB_FAIL(share::ObBackupLSTaskOperator::update_stats_(trans, param_.job_desc_.task_id_,
        param_.tenant_id_, param_.ls_id_, new_task_stat))) {
      LOG_WARN("failed to update stat", K(ret), K(param_));
    } else if (OB_FAIL(ObLSBackupOperator::report_ls_backup_task_info(param_.tenant_id_,
                   param_.job_desc_.task_id_,
                   param_.turn_id_,
                   param_.retry_id_,
                   backup_data_type_,
                   new_stat,
                   trans))) {
      LOG_WARN("failed to report single task info", K_(param), K_(backup_data_type), K(new_stat));
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

// TODO(yangyi.yyy): make tablet count accurate
int ObLSBackupDataTask::update_task_stat_(const share::ObBackupStats &old_backup_stat, const ObLSBackupStat &ls_stat,
    share::ObBackupStats &new_backup_stat)
{
  int ret = OB_SUCCESS;
  new_backup_stat.input_bytes_ = old_backup_stat.input_bytes_ + ls_stat.input_bytes_;
  new_backup_stat.output_bytes_ = old_backup_stat.output_bytes_ + ls_stat.output_bytes_;
  new_backup_stat.macro_block_count_ = old_backup_stat.macro_block_count_ + ls_stat.finish_macro_block_count_;
  new_backup_stat.finish_macro_block_count_ = old_backup_stat.finish_macro_block_count_ + ls_stat.finish_macro_block_count_;
  if (backup_data_type_.is_minor_backup() || backup_data_type_.is_sys_backup()) {
    new_backup_stat.tablet_count_ = old_backup_stat.tablet_count_ + ls_stat.finish_tablet_count_;
    new_backup_stat.finish_tablet_count_ = old_backup_stat.finish_tablet_count_ + ls_stat.finish_tablet_count_;
  } else {
    new_backup_stat.tablet_count_ = old_backup_stat.tablet_count_;
    new_backup_stat.finish_tablet_count_ = old_backup_stat.finish_tablet_count_;
  }
  return ret;
}

int ObLSBackupDataTask::update_task_info_stat_(
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

int ObLSBackupDataTask::get_macro_block_id_list_(common::ObIArray<ObBackupMacroBlockId> &list)
{
  int ret = OB_SUCCESS;
  list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_items_.count(); ++i) {
    const ObBackupProviderItem &item = backup_items_.at(i);
    ObBackupMacroBlockId macro_id;
    macro_id.logic_id_ = item.get_logic_id();
    macro_id.macro_block_id_ = item.get_macro_block_id();
    macro_id.nested_offset_ = item.get_nested_offset();
    macro_id.nested_size_ = item.get_nested_size();
    if (PROVIDER_ITEM_MACRO_ID == item.get_item_type()) {
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
    if (PROVIDER_ITEM_MACRO_ID != item.get_item_type()) {
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
    const ObTabletMetaReaderType &reader_type, storage::ObTabletHandle &tablet_handle,
    ObITabletMetaBackupReader *&reader)
{
  int ret = OB_SUCCESS;
  ObITabletMetaBackupReader *tmp_reader = NULL;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tmp_reader = ObLSBackupFactory::get_tablet_meta_backup_reader(reader_type, param_.tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc iterator", K(ret), K(reader_type));
  } else if (OB_FAIL(tmp_reader->init(tablet_id, backup_data_type_, tablet_handle))) {
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

int ObLSBackupDataTask::get_next_macro_block_data_(
    ObMultiMacroBlockBackupReader *reader, ObBufferReader &buffer_reader, blocksstable::ObLogicMacroBlockId &logic_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret));
  } else if (OB_FAIL(reader->get_next_macro_block(buffer_reader, logic_id))) {
    if (OB_ITER_END == ret) {
      // do nothing
    } else {
      LOG_WARN("failed to get next macro block", K(ret));
    }
  } else {
    LOG_INFO("get next macro block data", K(buffer_reader), K(logic_id));
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

int ObLSBackupDataTask::write_macro_block_data_(
    const ObBufferReader &data, const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  if (!data.is_valid() || !logic_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(data), K(logic_id));
  } else if (OB_FAIL(backup_data_ctx_.write_macro_block_data(data, logic_id, macro_index))) {
    LOG_WARN("failed to write macro block data", K(ret), K(data), K(logic_id));
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
  if (!data.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(data), K(tablet_id));
  } else if (OB_FAIL(backup_data_ctx_.write_meta_data(data, tablet_id, meta_type, meta_index))) {
    LOG_WARN("failed to write macro block data", K(ret), K(data), K(tablet_id), K(meta_type));
  } else {
    LOG_INFO("write meta data", K(tablet_id), K(meta_type));
  }
  return ret;
}

int ObLSBackupDataTask::get_tablet_handle_(const common::ObTabletID &tablet_id, storage::ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(ls_backup_ctx_->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to acquire tablet", K(ret), K(tablet_id));
  } else {
    LOG_INFO("get tablet handle", K(tablet_id));
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
    LOG_INFO("release tablet handle", K(tablet_id));
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
  } else if (backup_data_type_.is_minor_backup()) {
    backup_data_event = "backup_minor_data";
  } else if (backup_data_type_.is_major_backup()) {
    backup_data_event = "backup_major_data";
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

int ObLSBackupDataTask::finish_backup_items_()
{
  int ret = OB_SUCCESS;
  ObBackupTabletStat *tablet_stat = NULL;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (FALSE_IT(tablet_stat = &ls_backup_ctx_->tablet_stat_)) {
  } else if (OB_FAIL(backup_secondary_metas_(tablet_stat))) {
    LOG_WARN("failed to backup secondary metas", K(ret));
  } else if (OB_FAIL(release_tablet_handles_(tablet_stat))) {
    LOG_WARN("failed to release tablet handles", K(ret));
  } else {
    LOG_INFO("finish backup items", K_(finished_tablet_list), K_(backup_items));
  }
  return ret;
}

int ObLSBackupDataTask::get_backup_item_(const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupProviderItem &item)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_items_.count(); ++i) {
    const ObBackupProviderItem &tmp_item = backup_items_.at(i);
    if (PROVIDER_ITEM_MACRO_ID == tmp_item.get_item_type() && logic_id == tmp_item.get_logic_id()) {
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
    const ObBackupProviderItem &item, const ObBackupPhysicalID &physical_id)
{
  int ret = OB_SUCCESS;
  bool is_all_finished = false;
  ObBackupTabletStat *tablet_stat = NULL;
  const ObTabletID &tablet_id = item.get_tablet_id();
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (FALSE_IT(tablet_stat = &ls_backup_ctx_->tablet_stat_)) {
  } else if (OB_FAIL(tablet_stat->mark_item_finished(backup_data_type_, item, physical_id, is_all_finished))) {
    LOG_WARN("failed to mark item finished", K(ret), K(item), K_(backup_data_type));
  } else if (!is_all_finished) {
    // do nothing
  } else if (OB_FAIL(finished_tablet_list_.push_back(tablet_id))) {
    LOG_WARN("failed to push back", K(ret), K(tablet_id));
  } else {
    LOG_INFO("mark tablet finished", K(tablet_id));
  }
  return ret;
}

int ObLSBackupDataTask::backup_secondary_metas_(ObBackupTabletStat *tablet_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_backup_ctx_) || OB_ISNULL(tablet_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  }
  std::sort(finished_tablet_list_.begin(), finished_tablet_list_.end());
  for (int64_t i = 0; OB_SUCC(ret) && i < finished_tablet_list_.count(); ++i) {
    const common::ObTabletID &tablet_id = finished_tablet_list_.at(i);
    ObBackupTabletCtx *ctx = NULL;
    ObTabletPhysicalIDMetaBackupReader reader;
    ObBufferReader buffer_reader;
    const ObBackupMetaType &meta_type = BACKUP_MACRO_BLOCK_ID_MAPPING_META;
    ObBackupMetaIndex meta_index;

    if (OB_FAIL(tablet_stat->get_tablet_stat(tablet_id, ctx))) {
      LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
    } else if (OB_FAIL(may_fill_reused_backup_items_(tablet_id, tablet_stat))) {
      LOG_WARN("failed to may fill reused backup items", K(ret), K(tablet_id));
    } else if (OB_FAIL(reader.init(tablet_id, ctx))) {
      LOG_WARN("failed to init reader", K(ret), K(tablet_id), K(ctx));
    } else if (OB_FAIL(reader.get_meta_data(buffer_reader))) {
      LOG_WARN("failed to get meta data", K(ret));
    } else if (OB_FAIL(write_backup_meta_(buffer_reader, tablet_id, meta_type, meta_index))) {
      LOG_WARN("failed to write backup meta", K(ret), K(tablet_id), K(meta_type));
    } else {
      LOG_INFO("do backup sec meta", K(tablet_id), K(meta_index), K(buffer_reader));
    }
  }
  return ret;
}

int ObLSBackupDataTask::may_fill_reused_backup_items_(
    const common::ObTabletID &tablet_id, ObBackupTabletStat *tablet_stat)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObBackupDataType backup_data_type;
  backup_data_type.set_major_data_backup();
  ObArray<ObITable *> sstable_array;
  ObITable* table_ptr = NULL;
  ObSSTable *sstable_ptr = NULL;
  ObArray<ObLogicMacroBlockId> logic_id_list;
  if (OB_ISNULL(ls_backup_ctx_) || OB_ISNULL(tablet_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (!ls_backup_ctx_->backup_data_type_.is_major_backup()) {
    // do nothing
  } else if (tablet_id.id() != ls_backup_ctx_->backup_retry_ctx_.need_skip_logic_id_.tablet_id_) {
    // do nothing
  } else if (OB_FAIL(get_tablet_handle_(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(ObBackupUtils::get_sstables_by_data_type(tablet_handle,
      backup_data_type, *table_store_wrapper.get_member(), sstable_array))) {
    LOG_WARN("failed to get sstable by data type", K(ret), K(tablet_handle));
  } else if (sstable_array.empty()) {
    // do nothing
  } else if (1 != sstable_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable array count not 1", K(ret), K(sstable_array));
  } else if (OB_ISNULL(table_ptr = sstable_array.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be null", K(ret), K(sstable_array));
  } else if (!table_ptr->is_major_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get incorrect table type", K(ret), KPC(table_ptr));
  } else if (FALSE_IT(sstable_ptr = static_cast<ObSSTable *>(table_ptr))) {
  } else if (OB_FAIL(ObBackupUtils::fetch_macro_block_logic_id_list(
      tablet_handle, *sstable_ptr, logic_id_list))) {
    LOG_WARN("failed to fetch macro block logic id list", K(ret), K(tablet_handle), KPC(sstable_ptr));
  } else {
    std::sort(logic_id_list.begin(), logic_id_list.end());
    const ObITable::TableKey &table_key = table_ptr->get_key();
    const ObArray<ObBackupMacroBlockIDPair> &id_array = ls_backup_ctx_->backup_retry_ctx_.reused_pair_list_;
    for (int64_t i = 0; OB_SUCC(ret) && i < id_array.count(); ++i) {
      const ObBackupMacroBlockIDPair &pair = id_array.at(i);
      bool need_reuse = true;
      if (OB_FAIL(check_macro_block_need_reuse_when_recover_(pair.logic_id_, logic_id_list, need_reuse))) {
        LOG_WARN("failed to check macro block need reuse when recover", K(ret), K(pair), K(logic_id_list));
      } else if (!need_reuse) {
        // do nothing
      } else if (OB_FAIL(tablet_stat->mark_item_reused(backup_data_type_, table_key, pair))) {
        LOG_WARN("failed to mark item finished", K(ret), K_(backup_data_type), K(table_key));
      } else {
        LOG_INFO("mark item reuse", K(table_key), K(pair));
      }
    }
  }
  return ret;
}

int ObLSBackupDataTask::check_macro_block_need_reuse_when_recover_(const blocksstable::ObLogicMacroBlockId &logic_id,
    const common::ObArray<blocksstable::ObLogicMacroBlockId> &logic_id_list, bool &need_reuse)
{
  int ret = OB_SUCCESS;
  need_reuse = false;
  typedef ObArray<ObLogicMacroBlockId>::const_iterator Iterator;
  Iterator iter = std::lower_bound(logic_id_list.begin(), logic_id_list.end(), logic_id);
  if (iter != logic_id_list.end()) {
    need_reuse = *iter == logic_id;
  }
  return ret;
}

int ObLSBackupDataTask::release_tablet_handles_(ObBackupTabletStat *tablet_stat)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < finished_tablet_list_.count(); ++i) {
    const common::ObTabletID &tablet_id = finished_tablet_list_.at(i);
    if (OB_FAIL(tablet_stat->free_tablet_stat(tablet_id))) {
      LOG_WARN("failed to free tablet stat", K(ret), K(tablet_id));
    } else if (OB_FAIL(release_tablet_handle_(tablet_id))) {
      LOG_WARN("failed to release tablet handle", K(ret), K(tablet_id));
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
  if (IS_NOT_INIT) {
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

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (ls_id.is_sys_ls()) {
      ret = OB_E(EventTable::EN_BACKUP_SYS_META_TASK_FAILED) OB_SUCCESS;
    } else {
      ret = OB_E(EventTable::EN_BACKUP_USER_META_TASK_FAILED) OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_meta");
      LOG_WARN("errsim backup meta task failed", K(ret));
    }
  }
#endif
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
  auto backup_tablet_meta_f = [&writer, &backup_tablet_count, &max_tablet_checkpoint_scn](const obrpc::ObCopyTabletInfo &tablet_info)->int {
    int ret = OB_SUCCESS;
    blocksstable::ObSelfBufferWriter buffer_writer("LSBackupMetaTask");
    blocksstable::ObBufferReader buffer_reader;
    if (!tablet_info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tablet meta is invalid", K(ret), K(tablet_info));
    } else if (OB_FAIL(buffer_writer.ensure_space(backup::OB_BACKUP_READ_BLOCK_SIZE))) {
      LOG_WARN("failed to ensure space");
    } else if (OB_FAIL(buffer_writer.write_serialize(tablet_info.param_))) {
      LOG_WARN("failed to writer", K(tablet_info));
    } else {
      buffer_reader.assign(buffer_writer.data(), buffer_writer.length(), buffer_writer.length());
      if (OB_FAIL(writer.write_meta_data(buffer_reader, tablet_info.param_.tablet_id_))) {
        LOG_WARN("failed to write meta data", K(ret), K(tablet_info));
      } else {
        max_tablet_checkpoint_scn = MAX(max_tablet_checkpoint_scn, tablet_info.param_.get_max_tablet_checkpoint_scn());
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
  } else if (OB_FAIL(writer.init(backup_set_dest, param_.ls_id_, param_.turn_id_, param_.retry_id_))) {
    LOG_WARN("failed to init tablet info writer", K(ret));
  } else {
    const int64_t WAIT_GC_LOCK_TIMEOUT = 30 * 60 * 1000 * 1000; // 30 min TODO(chongrong.th) optimization timeout later 4.3
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
        LOG_INFO("succeed to get ls meta package and tablet meta", K(ls_id), K(cost_ts));
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
  if (OB_FAIL(extern_mgr.init(backup_dest, backup_set_desc, ls_id, turn_id, retry_id))) {
    LOG_WARN("failed to init extern mgr", K(ret), K_(param));
  } else if (OB_FAIL(extern_mgr.write_ls_meta_info(ls_meta_info))) {
    LOG_WARN("failed to write ls meta info", K(ret), K(ls_meta_info));
  } else {
    LOG_INFO("backup ls meta package", K(ret), K(ls_meta_info));
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
          scheduler->free_dag(*child_dag, dag_);
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
      } else if (OB_TMP_FAIL(scheduler->cancel_dag(prefetch_dag, dag_))) {
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
  if (!backup_data_type_.is_minor_backup()) {
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
  if (OB_FAIL(prepare_meta_index_store_(meta_index_store))) {
    LOG_WARN("failed to prepare meta index store", K(ret));
  } else if (OB_FAIL(meta_index_store.get_backup_meta_index(tx_data_tablet_id, meta_type, meta_index))) {
    LOG_WARN("failed to get backup meta index", K(ret), K(tx_data_tablet_id), K(meta_type));
  } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_backup_path(param_.backup_dest_,
      param_.backup_set_desc_, param_.ls_id_, sys_backup_data_type, meta_index.turn_id_,
      meta_index.retry_id_, meta_index.file_id_, backup_path))) {
    LOG_WARN("failed to get ls meta index backup path", K(ret), K_(param), K(sys_backup_data_type), K(meta_index));
  } else if (OB_FAIL(ObLSBackupRestoreUtil::read_sstable_metas(
      backup_path.get_obstr(), param_.backup_dest_.get_storage_info(), meta_index, meta_array))) {
    LOG_WARN("failed to read sstable metas", K(ret), K(backup_path), K(meta_index));
  } else if (meta_array.empty()) {
    filled_tx_scn = SCN::min_scn();
    LOG_INFO("the log stream do not have tx data sstable", K(ret));
  } else {
    filled_tx_scn = meta_array.at(0).sstable_meta_.basic_meta_.filled_tx_scn_;
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
  const bool is_sec_meta = false;
  int64_t sys_retry_id = -1;
  int64_t sys_turn_id = 0;
  if (OB_FAIL(get_sys_ls_turn_and_retry_id_(sys_turn_id, sys_retry_id))) {
    LOG_WARN("failed to get sys ls retry id", K(ret));
  } else if (OB_FAIL(prepare_meta_index_store_param_(sys_turn_id, sys_retry_id, index_store_param))) {
    LOG_WARN("failed to preparep meta index store param", K(ret), K(sys_retry_id));
  } else if (OB_FAIL(meta_index_store.init(mode, index_store_param, param_.backup_dest_,
     param_.backup_set_desc_, is_sec_meta, *index_kv_cache_))) {
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
      report_ctx_()
{}

ObBackupIndexRebuildTask::~ObBackupIndexRebuildTask()
{}

int ObBackupIndexRebuildTask::init(const ObLSBackupDataParam &param, const ObBackupIndexLevel &index_level,
    ObLSBackupCtx *ls_backup_ctx, ObIBackupTabletProvider *provider, ObBackupMacroBlockTaskMgr *task_mgr,
    ObBackupIndexKVCache *index_kv_cache, const ObBackupReportCtx &report_ctx)
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
  } else if (OB_FAIL(merge_meta_index_(false /*is_sec_meta*/))) {
    LOG_WARN("failed to merge meta index", K(ret), K_(param));
  } else if (OB_FAIL(merge_meta_index_(true /*is_sec_meta*/))) {
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

bool ObBackupIndexRebuildTask::need_build_index_() const
{
  bool bret = true;
  if (0 == param_.ls_id_.id()) {
    bret = !param_.backup_data_type_.is_sys_backup();
  }
  return bret;
}

int ObBackupIndexRebuildTask::merge_macro_index_()
{
  int ret = OB_SUCCESS;
  if (need_build_index_()) {
    ObBackupIndexMergeParam merge_param;
    ObBackupMacroBlockIndexMerger macro_index_merger;
    if (OB_FAIL(param_.convert_to(index_level_, merge_param))) {
      LOG_WARN("failed to convert param", K(ret), K_(param), K_(index_level));
    } else if (OB_FAIL(macro_index_merger.init(merge_param, *report_ctx_.sql_proxy_))) {
      LOG_WARN("failed to init index merger", K(ret), K(merge_param));
    } else if (OB_FAIL(macro_index_merger.merge_index())) {
      LOG_WARN("failed to merge macro index", K(ret), K_(param));
    }
  }
  return ret;
}

int ObBackupIndexRebuildTask::merge_meta_index_(const bool is_sec_meta)
{
  int ret = OB_SUCCESS;
  if (need_build_index_()) {
    ObBackupIndexMergeParam merge_param;
    ObBackupMetaIndexMerger meta_index_merger;
    if (OB_FAIL(param_.convert_to(index_level_, merge_param))) {
      LOG_WARN("failed to convert param", K(ret), K_(param), K_(index_level));
    } else if (OB_FAIL(meta_index_merger.init(merge_param, is_sec_meta, *report_ctx_.sql_proxy_))) {
      LOG_WARN("failed to init index merger", K(ret), K(merge_param));
    } else if (OB_FAIL(meta_index_merger.merge_index())) {
      LOG_WARN("failed to merge meta index", K(ret), K_(param));
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
  } else if (!param_.backup_data_type_.is_major_backup()) {
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

/* ObLSBackupComplementLogTask */

ObLSBackupComplementLogTask::BackupPieceFile::BackupPieceFile()
    : dest_id_(-1), round_id_(-1), piece_id_(-1), ls_id_(), file_id_(-1), start_scn_(), path_()
{}

void ObLSBackupComplementLogTask::BackupPieceFile::reset()
{
  dest_id_ = -1;
  round_id_ = -1;
  piece_id_ = -1;
  ls_id_.reset();
  file_id_ = -1;
  start_scn_.reset();
  path_.reset();
}

int ObLSBackupComplementLogTask::BackupPieceFile::set(const int64_t dest_id, const int64_t round_id,
    const int64_t piece_id, const share::ObLSID &ls_id, const int64_t file_id, const share::SCN &start_scn, const ObBackupPathString &path)
{
  int ret = OB_SUCCESS;
  if (dest_id <= 0 || round_id <= 0 || piece_id <= 0 || !ls_id.is_valid() || file_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(dest_id), K(round_id), K(piece_id), K(ls_id), K(file_id));
  } else if (OB_FAIL(path_.assign(path))) {
    LOG_WARN("failed to assign path", K(ret), K(path));
  } else {
    dest_id_ = dest_id;
    round_id_ = round_id;
    piece_id_ = piece_id;
    ls_id_ = ls_id;
    file_id_ = file_id;
    start_scn_ = start_scn;
  }
  return ret;
}

ObLSBackupComplementLogTask::BackupPieceOp::BackupPieceOp() : file_id_list_()
{}

int ObLSBackupComplementLogTask::BackupPieceOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char file_name[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  int64_t file_id = -1;
  int32_t len = 0;
  ObString entry_suffix;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", K(ret));
  } else if (FALSE_IT(len = strlen(entry->d_name) - strlen(OB_ARCHIVE_SUFFIX))) {
  } else if (len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file name without a unified suffix", K(ret), K(entry->d_name), K(OB_ARCHIVE_SUFFIX));
  } else if (FALSE_IT(entry_suffix.assign_ptr(entry->d_name + len, strlen(OB_ARCHIVE_SUFFIX)))) {
  } else if (!entry_suffix.prefix_match(OB_ARCHIVE_SUFFIX)) {
    // not ended with archive suffix
    ObString file_name_str(entry->d_name);
    LOG_INFO("skip file which is not archive file", K(file_name_str), K(entry_suffix));
  } else if (OB_FAIL(databuff_printf(file_name, sizeof(file_name), "%.*s", len, entry->d_name))) {
    LOG_WARN("fail to save tmp file name", K(ret), K(file_name));
  } else if (0 == ObString::make_string(file_name).case_compare(OB_STR_LS_FILE_INFO)) {
    LOG_INFO("skip ls file info");
  } else if (OB_FAIL(ob_atoll(file_name, file_id))) {
    LOG_WARN("failed to change string to number", K(ret), K(file_name));
  } else if (OB_FAIL(file_id_list_.push_back(file_id))) {
    LOG_WARN("failed to push back", K(ret), K(file_id));
  }
  return ret;
}

int ObLSBackupComplementLogTask::BackupPieceOp::get_file_id_list(common::ObIArray<int64_t> &files) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(files.assign(file_id_list_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

bool ObLSBackupComplementLogTask::CompareArchivePiece::operator()(
    const ObTenantArchivePieceAttr &lhs, const ObTenantArchivePieceAttr &rhs) const
{
  return lhs.key_.piece_id_ < rhs.key_.piece_id_;
}

ObLSBackupComplementLogTask::ObLSBackupComplementLogTask()
    : ObITask(TASK_TYPE_MIGRATE_COPY_PHYSICAL),
      is_inited_(false),
      job_desc_(),
      backup_dest_(),
      tenant_id_(OB_INVALID_ID),
      dest_id_(0),
      backup_set_desc_(),
      ls_id_(),
      compl_start_scn_(),
      compl_end_scn_(),
      turn_id_(0),
      retry_id_(-1),
      archive_dest_(),
      report_ctx_()
{}

ObLSBackupComplementLogTask::~ObLSBackupComplementLogTask()
{}

int ObLSBackupComplementLogTask::init(const ObBackupJobDesc &job_desc, const ObBackupDest &backup_dest,
    const uint64_t tenant_id, const int64_t dest_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
    const SCN &start_scn, const SCN &end_scn, const int64_t turn_id, const int64_t retry_id,
    const ObBackupReportCtx &report_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls backup complement log task init twice", K(ret));
  } else if (!job_desc.is_valid() || !backup_dest.is_valid() || OB_INVALID_ID == tenant_id ||
             !backup_set_desc.is_valid() || !ls_id.is_valid() || !start_scn.is_valid() || !end_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args",
        K(ret),
        K(job_desc),
        K(backup_dest),
        K(tenant_id),
        K(backup_set_desc),
        K(ls_id),
        K(start_scn),
        K(end_scn));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    job_desc_ = job_desc;
    tenant_id_ = tenant_id;
    dest_id_ = dest_id;
    backup_set_desc_ = backup_set_desc;
    ls_id_ = ls_id;
    compl_start_scn_ = start_scn;
    compl_end_scn_ = end_scn;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    report_ctx_ = report_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupComplementLogTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_BACKUP_COMPLEMENT_LOG);
  const uint64_t tenant_id = tenant_id_;
  int64_t archive_dest_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup complement log task do not init", K(ret));
  } else if (OB_FAIL(get_active_round_dest_id_(tenant_id, archive_dest_id))) {
    LOG_WARN("failed to get active round dest id", K(ret), K(tenant_id));
  } else {
    ObArray<ObLSID> need_process_ls_ids;
    if (OB_FAIL(need_process_ls_ids.push_back(ls_id_))) {
      LOG_WARN("failed to push back ls id", K(ret));
    } else if (!ls_id_.is_sys_ls()) {
    } else if (OB_FAIL(get_newly_created_ls_in_piece_(archive_dest_id, tenant_id,
        compl_start_scn_, compl_end_scn_, need_process_ls_ids))) {
      LOG_WARN("failed to get newly created ls in piece", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < need_process_ls_ids.count(); ++i) {
      const ObLSID &ls_id = need_process_ls_ids.at(i);
      if (OB_FAIL(inner_process_(archive_dest_id, ls_id))) {
        LOG_WARN("failed to do inner process", K(ret), K(archive_dest_id), K(ls_id));
      } else {
        LOG_INFO("backup complement log", K(ls_id));
      }
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_COMPLEMENT_LOG_TASK_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_complement_log");
      LOG_WARN("errsim complement log task failed", K(ret));
    }
  }
#endif
  if (OB_SUCCESS != (tmp_ret = ObBackupUtils::report_task_result(
                         job_desc_.job_id_, job_desc_.task_id_, tenant_id_, ls_id_, turn_id_, retry_id_,
                         job_desc_.trace_id_, this->get_dag()->get_dag_id(), ret, report_ctx_))) {
    LOG_WARN("failed to report task result", K(tmp_ret), K_(job_desc), K_(tenant_id), K_(ls_id), K(ret));
  } else {
    FLOG_INFO("report backup complement log task", K_(job_desc), K_(tenant_id), K_(ls_id));
  }
  return ret;
}

// TODO(yangyi.yyy): move this logic to RS later
int ObLSBackupComplementLogTask::get_newly_created_ls_in_piece_(const int64_t dest_id, const uint64_t tenant_id,
    const share::SCN &start_scn, const share::SCN &end_scn, common::ObIArray<share::ObLSID> &ls_array)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSID> backup_ls_ids;
  ObArray<ObLSID> archive_ls_ids;
  const int64_t task_id = job_desc_.task_id_;
  if (OB_FAIL(ObLSBackupOperator::get_all_backup_ls_id(
      tenant_id, task_id, backup_ls_ids, *report_ctx_.sql_proxy_))) {
    LOG_WARN("failed to get all backup ls ids", K(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(ObLSBackupOperator::get_all_archive_ls_id(
      tenant_id, dest_id, start_scn, end_scn, archive_ls_ids, *report_ctx_.sql_proxy_))) {
    LOG_WARN("failed to get all backup ls ids", K(ret), K(tenant_id), K(task_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < archive_ls_ids.count(); ++i) {
      const ObLSID &archive_ls_id = archive_ls_ids.at(i);
      bool exist = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < backup_ls_ids.count(); ++j) {
        const ObLSID &backup_ls_id = backup_ls_ids.at(j);
        if (backup_ls_id == archive_ls_id) {
          exist = true;
          break;
        }
      }
      if (OB_SUCC(ret) && !exist) {
        if (OB_FAIL(ls_array.push_back(archive_ls_id))) {
          LOG_WARN("failed to push back ls id", K(ret), K(archive_ls_id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get newly created ls in piece", K(dest_id), K(tenant_id), K(start_scn), K(end_scn),
        K(backup_ls_ids), K(archive_ls_ids), K(ls_array));
  }
  return ret;
}

int ObLSBackupComplementLogTask::inner_process_(
    const int64_t archive_dest_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObBackupPath backup_path;
  ObBackupIoAdapter util;
  ObArray<BackupPieceFile> file_list;
  const uint64_t tenant_id = tenant_id_;
  const int64_t task_id = job_desc_.task_id_;
  if (OB_FAIL(get_complement_log_dir_path_(backup_path))) {
    LOG_WARN("failed to get backup file path", K(ret));
  } else if (OB_FAIL(util.mkdir(backup_path.get_obstr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to make parent dir", K(ret), K(backup_path), K(backup_dest_));
  } else if (OB_FAIL(calc_backup_file_range_(archive_dest_id, ls_id, file_list))) {
    LOG_WARN("failed to calc backup file range", K(ret), K(archive_dest_id), K(ls_id));
  } else if (OB_FAIL(backup_complement_log_(file_list))) {
    LOG_WARN("failed to backup complement log", K(ret), K(file_list));
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_complement_log_dir_path_(ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupPathUtil::get_complement_log_dir_path(backup_dest_, backup_set_desc_, backup_path))) {
    LOG_WARN("failed to get backup file path", K(ret), K_(backup_dest), K_(tenant_id), K_(backup_set_desc), K_(ls_id));
  }
  return ret;
}

// TODO(yangyi.yyy): depend on @duotian implementing restore using real piece list
int ObLSBackupComplementLogTask::write_format_file_()
{
  int ret = OB_SUCCESS;
  share::ObBackupStore store;
  share::ObBackupFormatDesc format_desc;
  bool is_exist = false;
  ObBackupDest new_backup_dest;
  if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(
      backup_dest_, backup_set_desc_, new_backup_dest))) {
    LOG_WARN("failed to set archive dest", K(ret), K_(backup_dest));
  } else if (OB_FAIL(store.init(new_backup_dest))) {
    LOG_WARN("fail to init store", K(ret), K(new_backup_dest));
  } else if (OB_FAIL(store.is_format_file_exist(is_exist))) {
    LOG_WARN("fail to check format file exist", K(ret), K_(backup_dest));
  } else if (is_exist) {
    // do not recreate the format file
  } else if (OB_FAIL(generate_format_desc_(format_desc))) {
    LOG_WARN("fail to generate format desc", K(ret));
  } else if (OB_FAIL(store.write_format_file(format_desc))) {
    LOG_WARN("fail to write format file", K(ret), K(format_desc));
  }
  return ret;
}

int ObLSBackupComplementLogTask::generate_format_desc_(share::ObBackupFormatDesc &format_desc)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  share::ObBackupPathString root_path;
  const schema::ObTenantSchema *tenant_schema = nullptr;
  ObBackupDest new_backup_dest;
  const ObBackupDestType::TYPE &dest_type = ObBackupDestType::DEST_TYPE_ARCHIVE_LOG;
  if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(
      backup_dest_, backup_set_desc_, new_backup_dest))) {
    LOG_WARN("failed to set archive dest", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant schema", K(ret), K_(tenant_id));
  } else if (OB_FAIL(format_desc.cluster_name_.assign(GCONF.cluster.str()))) {
    LOG_WARN("failed to assign cluster name", K(ret), K_(tenant_id));
  } else if (OB_FAIL(format_desc.tenant_name_.assign(tenant_schema->get_tenant_name()))) {
    LOG_WARN("failed to assign tenant name", K(ret), K_(tenant_id));
  } else if (OB_FAIL(new_backup_dest.get_backup_path_str(format_desc.path_.ptr(), format_desc.path_.capacity()))) {
    LOG_WARN("failed to get backup path", K(ret), K(backup_dest_));
  } else {
    format_desc.tenant_id_ = tenant_id_;
    format_desc.incarnation_ = OB_START_INCARNATION;
    format_desc.dest_id_ = dest_id_;
    format_desc.dest_type_ = dest_type;
    format_desc.cluster_id_ = GCONF.cluster_id;
  }
  return ret;
}

int ObLSBackupComplementLogTask::calc_backup_file_range_(const int64_t dest_id, const share::ObLSID &ls_id,
    common::ObIArray<BackupPieceFile> &file_list)
{
  int ret = OB_SUCCESS;
  file_list.reset();
  const uint64_t tenant_id = tenant_id_;
  const SCN start_scn = compl_start_scn_;
  const SCN end_scn = compl_end_scn_;
  int64_t start_piece_id = 0;
  int64_t end_piece_id = 0;
  ObArray<ObTenantArchivePieceAttr> piece_list;
  if (OB_FAIL(get_piece_id_by_scn_(tenant_id, dest_id, start_scn, start_piece_id))) {
    LOG_WARN("failed to get piece id by ts", K(ret), K(tenant_id), K(start_scn));
  } else if (OB_FAIL(get_piece_id_by_scn_(tenant_id, dest_id, end_scn, end_piece_id))) {
    LOG_WARN("failed to get piece id by ts", K(ret), K(tenant_id), K(end_scn));
  } else if (OB_FAIL(get_all_pieces_(tenant_id, dest_id, start_piece_id, end_piece_id, piece_list))) {
    LOG_WARN("failed to get all round pieces", K(ret), K(tenant_id), K(start_piece_id), K(end_piece_id));
  } else if (OB_FAIL(wait_pieces_frozen_(piece_list))) {
    LOG_WARN("failed to wait pieces frozen", K(ret), K(piece_list));
  } else if (OB_FAIL(get_all_piece_file_list_(tenant_id, ls_id, piece_list, start_scn, end_scn, file_list))) {
    LOG_WARN("failed to get all piece file list", K(ret), K(tenant_id), K(ls_id), K(start_piece_id), K(end_piece_id));
  } else {
    LOG_INFO("get all piece file list", K(tenant_id), K(start_piece_id), K(end_piece_id), K(piece_list), K(file_list));
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_active_round_dest_id_(const uint64_t tenant_id, int64_t &dest_id)
{
  int ret = OB_SUCCESS;
  ObArchivePersistHelper helper;
  ObArray<ObTenantArchiveRoundAttr> rounds;
  if (OB_FAIL(helper.init(tenant_id))) {
    LOG_WARN("failed to init archive persist helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_all_active_rounds(*report_ctx_.sql_proxy_, rounds))) {
    LOG_WARN("failed to get all active rounds", K(ret), K(tenant_id), K(dest_id));
  } else if (1 != rounds.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("round count is not one", K(ret), K(tenant_id));
  } else {
    dest_id = rounds.at(0).dest_id_;
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_piece_id_by_scn_(const uint64_t tenant_id,
    const int64_t dest_id, const SCN &scn, int64_t &piece_id)
{
  int ret = OB_SUCCESS;
  ObArchivePersistHelper helper;
  ObTenantArchivePieceAttr piece;
  if (OB_FAIL(helper.init(tenant_id))) {
    LOG_WARN("failed to init archive persist helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_piece_by_scn(*report_ctx_.sql_proxy_, dest_id, scn, piece))) {
    LOG_WARN("failed to get pieces by range", K(ret), K(tenant_id), K(dest_id), K(scn));
  } else {
    piece_id = piece.key_.piece_id_;
    LOG_INFO("get piece id by scn", K(tenant_id), K(scn), K(piece_id));
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_all_pieces_(const uint64_t tenant_id, const int64_t dest_id,
    const int64_t start_piece_id, const int64_t end_piece_id, common::ObArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  piece_list.reset();
  ObArchivePersistHelper helper;
  if (OB_FAIL(helper.init(tenant_id))) {
    LOG_WARN("failed to init archive persist helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_pieces_by_range(*report_ctx_.sql_proxy_, dest_id, start_piece_id, end_piece_id, piece_list))) {
    LOG_WARN("failed to get pieces by range", K(ret), K(tenant_id), K(dest_id), K(start_piece_id), K(end_piece_id));
  } else {
    LOG_INFO("get pieces by range", K(tenant_id), K(start_piece_id), K(end_piece_id), K(piece_list));
  }
  return ret;
}

int ObLSBackupComplementLogTask::wait_pieces_frozen_(const common::ObArray<share::ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  if (0 == piece_list.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("piece list should not be empty", K(ret));
  } else if (1 == piece_list.count()) {
    LOG_INFO("only one piece, no need wait frozen", K(piece_list));
  } else {
    int64_t start_idx = piece_list.count() - 2;
    for (int64_t i = start_idx; OB_SUCC(ret) && i >= 0; i--) {
      const ObTenantArchivePieceAttr &piece = piece_list.at(i);
      if (OB_FAIL(wait_piece_frozen_(piece))) {
        LOG_WARN("failed to wait piece frozen", K(ret));
      }
    }
  }
  return ret;
}

int ObLSBackupComplementLogTask::wait_piece_frozen_(const share::ObTenantArchivePieceAttr &piece)
{
  int ret = OB_SUCCESS;
  static const int64_t CHECK_TIME_INTERVAL = 1_s;
  static const int64_t MAX_CHECK_INTERVAL = 60_s;
  const int64_t start_ts = ObTimeUtility::current_time();
  do {
    bool is_frozen = false;
    const int64_t cur_ts = ObTimeUtility::current_time();
    if (cur_ts - start_ts > MAX_CHECK_INTERVAL) {
      ret = OB_TIMEOUT;
      LOG_WARN("backup advance checkpoint by flush timeout", K(ret), K(piece));
    } else if (OB_FAIL(check_piece_frozen_(piece, is_frozen))) {
      LOG_WARN("failed to get ls meta", K(ret), K(piece));
    } else if (is_frozen) {
      LOG_INFO("piece already frozen", K(piece));
      break;
    } else {
      LOG_INFO("wait piece frozen", K(piece));
      ob_usleep(CHECK_TIME_INTERVAL);
      share::dag_yield();
    }
  } while (OB_SUCC(ret));
  return ret;
}

int ObLSBackupComplementLogTask::check_piece_frozen_(const share::ObTenantArchivePieceAttr &piece, bool &is_frozen)
{
  int ret = OB_SUCCESS;
  is_frozen = false;
  ObArchivePersistHelper helper;
  ObTenantArchivePieceAttr cur_piece;
  const uint64_t tenant_id = tenant_id_;
  const int64_t dest_id = piece.key_.dest_id_;
  const int64_t round_id = piece.key_.round_id_;
  const int64_t piece_id = piece.key_.piece_id_;
  if (OB_FAIL(helper.init(tenant_id))) {
    LOG_WARN("failed to init archive persist helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_piece(*report_ctx_.sql_proxy_, dest_id, round_id, piece_id, false/*need_lock*/, cur_piece))) {
    LOG_WARN("failed to get pieces by range", K(ret), K(piece));
  } else {
    is_frozen = cur_piece.status_.is_frozen();
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_all_piece_file_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObIArray<ObTenantArchivePieceAttr> &piece_list, const SCN &start_scn, const SCN &end_scn,
    common::ObIArray<BackupPieceFile> &piece_file_list)
{
  int ret = OB_SUCCESS;
  piece_file_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < piece_list.count(); ++i) {
    const ObTenantArchivePieceAttr &round_piece = piece_list.at(i);
    ObArray<BackupPieceFile> tmp_file_list;
    if (OB_FAIL(inner_get_piece_file_list_(ls_id, round_piece, tmp_file_list))) {
      LOG_WARN("failed to inner get piece file list", K(ret), K(ls_id), K(round_piece));
    } else {
      if (1 == piece_list.count()) {
        int64_t start_file_id = 0;
        int64_t end_file_id = 0;
        if (OB_FAIL(locate_archive_file_id_by_scn_(round_piece, ls_id, start_scn, start_file_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_INFO("path may not exist", K(ret), K(round_piece), K(ls_id), K(start_scn));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to locate archive file id by scn", K(ret), K(round_piece), K(ls_id), K(start_scn));
          }
        } else if (OB_FAIL(locate_archive_file_id_by_scn_(round_piece, ls_id, end_scn, end_file_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_INFO("path may not exist", K(ret), K(round_piece), K(ls_id), K(end_scn));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to locate archive file id by scn", K(ret), K(round_piece), K(ls_id), K(end_scn));
          }
        } else if (start_file_id > end_file_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("start file id should not greater than end file id", K(ret), K(tenant_id), K(ls_id), K(round_piece),
              K(start_scn), K(end_scn), K(start_file_id), K(end_file_id));
        } else if (OB_FAIL(get_file_in_between_(start_file_id, end_file_id, tmp_file_list))) {
          LOG_WARN("failed to get file in between", K(ret), K(start_file_id), K(end_file_id));
        }
      } else if (0 == i) {
        int64_t file_id = 0;
        if (OB_FAIL(locate_archive_file_id_by_scn_(round_piece, ls_id, start_scn, file_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_INFO("path may not exist", K(ret), K(round_piece), K(ls_id), K(end_scn));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to locate archive file id by scn", K(ret), K(round_piece), K(ls_id), K(end_scn));
          }
        } else if (OB_FAIL(filter_file_id_smaller_than_(file_id, tmp_file_list))) {
          LOG_WARN("failed to filter file id smaller than", K(ret), K(file_id));
        }
      } else if (piece_list.count() - 1 == i) {
        int64_t file_id = 0;
        if (OB_FAIL(locate_archive_file_id_by_scn_(round_piece, ls_id, end_scn, file_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_INFO("path may not exist", K(ret), K(round_piece), K(ls_id), K(end_scn));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to locate archive file id by scn", K(ret), K(round_piece), K(ls_id), K(end_scn));
          }
        } else if (OB_FAIL(filter_file_id_larger_than_(file_id, tmp_file_list))) {
          LOG_WARN("failed to filter file id smaller than", K(ret), K(file_id));
        }
      } else {
      }  // do nothing
    }
    if (FAILEDx(append(piece_file_list, tmp_file_list))) {
      LOG_WARN("failed to append list", K(ret));
    }
  }
  return ret;
}

int ObLSBackupComplementLogTask::inner_get_piece_file_list_(const share::ObLSID &ls_id,
    const ObTenantArchivePieceAttr &piece_attr, common::ObIArray<BackupPieceFile> &file_list)
{
  int ret = OB_SUCCESS;
  BackupPieceFile piece_file;
  ObBackupIoAdapter util;
  ObBackupPath src_piece_dir_path;
  BackupPieceOp op;
  ObArray<int64_t> file_id_list;
  const int64_t dest_id = piece_attr.key_.dest_id_;
  const int64_t round_id = piece_attr.key_.round_id_;
  const int64_t piece_id = piece_attr.key_.piece_id_;
  const share::SCN &start_scn = piece_attr.start_scn_;
  if (OB_FAIL(get_src_backup_piece_dir_(ls_id, piece_attr, src_piece_dir_path))) {
    LOG_WARN("failed to get src backup piece dir", K(ret), K(round_id), K(piece_id), K(ls_id), K(piece_attr));
  } else if (OB_FAIL(util.list_files(src_piece_dir_path.get_obstr(), archive_dest_.get_storage_info(), op))) {
    LOG_WARN("failed to list files", K(ret), K(src_piece_dir_path));
  } else if (OB_FAIL(op.get_file_id_list(file_id_list))) {
    LOG_WARN("failed to get files", K(ret));
  } else {
    BackupPieceFile piece_file;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_id_list.count(); ++i) {
      const int64_t file_id = file_id_list.at(i);
      piece_file.reset();
      if (OB_FAIL(piece_file.set(dest_id, round_id, piece_id, ls_id, file_id, start_scn, piece_attr.path_))) {
        LOG_WARN("failed to set piece files", K(ret), K(tenant_id_), K(round_id), K(piece_id));
      } else if (OB_FAIL(file_list.push_back(piece_file))) {
        LOG_WARN("failed to push back", K(ret), K(piece_file));
      }
    }
  }
  return ret;
}

int ObLSBackupComplementLogTask::locate_archive_file_id_by_scn_(
    const ObTenantArchivePieceAttr &piece_attr, const share::ObLSID &ls_id, const SCN &scn, int64_t &file_id)
{
  int ret = OB_SUCCESS;
  ObBackupDest archive_dest;
  const int64_t dest_id = piece_attr.key_.dest_id_;
  const int64_t round_id = piece_attr.key_.round_id_;
  const int64_t piece_id = piece_attr.key_.piece_id_;
  if (OB_FAIL(get_archive_backup_dest_(piece_attr.path_, archive_dest))) {
    LOG_WARN("failed to get archive backup dest", K(ret), K(piece_attr));
  } else if (OB_FAIL(ObArchiveFileUtils::locate_file_by_scn(archive_dest,
      dest_id, round_id, piece_id, ls_id, scn, file_id))) {
    LOG_WARN("failed to locate file by scn", K(ret), K(archive_dest), K(ls_id), K(scn), K(piece_attr));
  } else {
    LOG_INFO("locate archive file id by scn", K(piece_attr), K(ls_id), K(scn), K(file_id));
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_file_in_between_(
    const int64_t start_file_id, const int64_t end_file_id, common::ObIArray<BackupPieceFile> &list)
{
  int ret = OB_SUCCESS;
  ObArray<BackupPieceFile> tmp_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    const BackupPieceFile &file = list.at(i);
    if (file.file_id_ >= start_file_id && file.file_id_ <= end_file_id) {
      if (OB_FAIL(tmp_list.push_back(file))) {
        LOG_WARN("failed to push back", K(ret), K(file));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(list.assign(tmp_list))) {
      LOG_WARN("failed to assign list", K(ret), K(tmp_list));
    }
  }
  return ret;

}

int ObLSBackupComplementLogTask::filter_file_id_smaller_than_(
    const int64_t file_id, common::ObIArray<BackupPieceFile> &list)
{
  int ret = OB_SUCCESS;
  ObArray<BackupPieceFile> tmp_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    const BackupPieceFile &file = list.at(i);
    if (file.file_id_ >= file_id) {
      if (OB_FAIL(tmp_list.push_back(file))) {
        LOG_WARN("failed to push back", K(ret), K(file));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(list.assign(tmp_list))) {
      LOG_WARN("failed to assign list", K(ret), K(tmp_list));
    }
  }
  return ret;
}

int ObLSBackupComplementLogTask::filter_file_id_larger_than_(
    const int64_t file_id, common::ObIArray<BackupPieceFile> &list)
{
  int ret = OB_SUCCESS;
  ObArray<BackupPieceFile> tmp_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    const BackupPieceFile &file = list.at(i);
    if (file.file_id_ <= file_id) {
      if (OB_FAIL(tmp_list.push_back(file))) {
        LOG_WARN("failed to push back", K(ret), K(file));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(list.assign(tmp_list))) {
      LOG_WARN("failed to assign list", K(ret), K(tmp_list));
    }
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_src_backup_piece_dir_(const share::ObLSID &ls_id,
    const ObTenantArchivePieceAttr &piece_attr, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest archive_dest;
  if (OB_FAIL(get_archive_backup_dest_(piece_attr.path_, archive_dest))) {
    LOG_WARN("failed to get archive backup dest", K(ret), K(piece_attr));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_ls_log_dir_path(archive_dest,
          piece_attr.key_.dest_id_,
          piece_attr.key_.round_id_,
          piece_attr.key_.piece_id_,
          ls_id,
          backup_path))) {
    LOG_WARN("failed to get ls log archive path", K(ret), K_(backup_dest));
  }

  return ret;
}

int ObLSBackupComplementLogTask::get_src_backup_file_path_(
    const BackupPieceFile &piece_file, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest archive_dest;
  if (OB_FAIL(get_archive_backup_dest_(piece_file.path_, archive_dest))) {
    LOG_WARN("failed to get archive backup dest", K(ret), K(piece_file));
  } else if (OB_FAIL(ObArchivePathUtil::get_ls_archive_file_path(archive_dest,
          piece_file.dest_id_,
          piece_file.round_id_,
          piece_file.piece_id_,
          piece_file.ls_id_,
          piece_file.file_id_,
          backup_path))) {
    LOG_WARN("failed to get ls log archive path", K(ret), K_(backup_dest));
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_dst_backup_file_path_(
    const BackupPieceFile &piece_file, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest new_backup_dest;
  if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(
      backup_dest_, backup_set_desc_, new_backup_dest))) {
    LOG_WARN("failed to set archive dest", K(ret), K(piece_file));
  } else if (OB_FAIL(ObArchivePathUtil::get_ls_archive_file_path(new_backup_dest,
          dest_id_,
          piece_file.round_id_,
          piece_file.piece_id_,
          piece_file.ls_id_,
          piece_file.file_id_,
          backup_path))) {
    LOG_WARN("failed to get ls log archive path", K(ret), K_(backup_dest));
  }
  return ret;
}

int ObLSBackupComplementLogTask::backup_complement_log_(const common::ObIArray<BackupPieceFile> &src_file_list)
{
  int ret = OB_SUCCESS;
  ObBackupPath src_path;
  ObBackupPath dst_path;
  ObBackupIoAdapter util;
  if (!src_file_list.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_file_list.count(); ++i) {
      const BackupPieceFile &piece_file = src_file_list.at(i);
      if (OB_FAIL(transform_and_copy_meta_file_(piece_file))) {
        LOG_WARN("failed to transform and copy meta file", K(ret), K(piece_file));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src_file_list.count(); ++i) {
    const BackupPieceFile &piece_file = src_file_list.at(i);
    ObBackupDest src, dest;
    const share::ObLSID &ls_id = piece_file.ls_id_;
    if (!ls_id.is_sys_ls()) {
      // do nothing
    } else if (OB_FAIL(get_copy_src_and_dest_(piece_file, src, dest))) {
      LOG_WARN("failed to get copy src and dest", K(ret), K(piece_file));
    } else if (OB_FAIL(copy_piece_start_file(piece_file, src, dest))) {
      LOG_WARN("failed to copy piece start file", K(ret), K(piece_file));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src_file_list.count(); ++i) {
    const BackupPieceFile &piece_file = src_file_list.at(i);
    if (OB_FAIL(get_src_backup_file_path_(piece_file, src_path))) {
      LOG_WARN("failed to get src backup file path", K(ret), K(piece_file));
    } else if (OB_FAIL(get_dst_backup_file_path_(piece_file, dst_path))) {
      LOG_WARN("failed to get dst backup file path", K(ret), K(piece_file));
    } else if (OB_FAIL(util.mk_parent_dir(dst_path.get_obstr(), backup_dest_.get_storage_info()))) {
      LOG_WARN("failed to mk parent dir", K(ret), K(dst_path));
    } else if (OB_FAIL(inner_backup_complement_log_(src_path, dst_path))) {
      LOG_WARN("failed to inner backup complement log", K(ret), K(src_path), K(dst_path));
    }else {
      SERVER_EVENT_ADD("backup", "backup_complement_log_file",
                      "tenant_id", tenant_id_,
                      "backup_set_id", backup_set_desc_.backup_set_id_,
                      "dest_id", piece_file.dest_id_,
                      "round_id", piece_file.round_id_,
                      "piece_id", piece_file.piece_id_,
                      "file_id", piece_file.file_id_);
      LOG_INFO("inner backup complement log", K(piece_file), K(src_path), K(dst_path));
    }
  }
  return ret;
}

int ObLSBackupComplementLogTask::inner_backup_complement_log_(
    const ObBackupPath &src_path, const ObBackupPath &dst_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transfer_clog_file_(src_path, dst_path))) {
    LOG_WARN("failed to transfer clog file", K(ret), K(src_path), K(dst_path));
  } else {
    LOG_INFO("backup complement log", K(src_path), K(dst_path));
  }
  return ret;
}

// TODO(yangyi.yyy): refine this method later in 4.2: add validation while copy clog file
int ObLSBackupComplementLogTask::transfer_clog_file_(const ObBackupPath &src_path, const ObBackupPath &dst_path)
{
  int ret = OB_SUCCESS;
  int64_t transfer_len = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(inner_transfer_clog_file_(src_path, dst_path, transfer_len))) {
      LOG_WARN("failed to inner transfer clog file", K(ret), K(src_path), K(dst_path));
    }
    if (0 == transfer_len) {
      LOG_INFO("transfer ended", K(ret), K(src_path), K(dst_path));
      break;
    }
  }
  return ret;
}

int ObLSBackupComplementLogTask::inner_transfer_clog_file_(
    const ObBackupPath &src_path, const ObBackupPath &dst_path, int64_t &transfer_len)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  transfer_len = 0;
  ObIOFd fd;
  ObBackupIoAdapter util;
  ObIODevice *device_handle = NULL;
  int64_t write_size = -1;
  ObArenaAllocator allocator;
  int64_t src_len = 0;
  int64_t dst_len = 0;
  char *buf = NULL;
  int64_t read_len = 0;
  if (OB_FAIL(util.open_with_access_type(
          device_handle, fd, backup_dest_.get_storage_info(), dst_path.get_obstr(), OB_STORAGE_ACCESS_RANDOMWRITER))) {
    LOG_WARN("failed to open with access type", K(ret));
  } else if (OB_FAIL(get_file_length_(src_path.get_obstr(), archive_dest_.get_storage_info(), src_len))) {
    LOG_WARN("failed to get file length", K(ret), K(src_path));
  } else if (OB_FAIL(get_file_length_(dst_path.get_obstr(), backup_dest_.get_storage_info(), dst_len))) {
    LOG_WARN("failed to get file length", K(ret), K(dst_path));
  } else if (dst_len == src_len) {
    transfer_len = 0;
  } else if (dst_len > src_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dst len should not be greater then src len", K(ret), K(src_len), K(dst_len));
  } else if (OB_FAIL(get_transfer_length_(src_len - dst_len, transfer_len))) {
    LOG_WARN("failed to get transfer len", K(ret), K(src_len), K(dst_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(transfer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(transfer_len));
  } else if (OB_FAIL(util.read_part_file(src_path.get_obstr(), archive_dest_.get_storage_info(), buf, transfer_len, dst_len, read_len))) {
    LOG_WARN("failed to read part file", K(ret), K(src_path));
  } else if (read_len != transfer_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read len not expected", K(ret), K(read_len), K(transfer_len));
  } else if (OB_FAIL(device_handle->pwrite(fd, dst_len, transfer_len, buf, write_size))) {
    LOG_WARN("failed to write appender file", K(ret));
  }
  if (OB_SUCCESS != (tmp_ret = util.close_device_and_fd(device_handle, fd))) {
    LOG_WARN("failed to close storage appender", K(ret), KR(tmp_ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_transfer_length_(const int64_t delta_len, int64_t &transfer_len)
{
  int ret = OB_SUCCESS;
  transfer_len = 0;
  const int64_t MAX_PART_FILE_SIZE = 2 * 1024 * 1024;  // 2MB
  if (delta_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(delta_len));
  } else {
    transfer_len = std::min(delta_len, MAX_PART_FILE_SIZE);
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_file_length_(
    const common::ObString &file_path, const share::ObBackupStorageInfo *storage_info, int64_t &length)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  if (OB_FAIL(util.get_file_length(file_path, storage_info, length))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      length = 0;
    } else {
      LOG_WARN("failed to get file length", K(ret), K(file_path));
    }
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_copy_src_and_dest_(
    const BackupPieceFile &piece_file, share::ObBackupDest &src, share::ObBackupDest &dest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_archive_backup_dest_(piece_file.path_, src))) {
    LOG_WARN("failed to set archive dest", K(ret), K(piece_file));
  } else if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(
      backup_dest_, backup_set_desc_, dest))) {
    LOG_WARN("failed to set archive dest", K(ret), K(piece_file));
  }
  return ret;
}

int ObLSBackupComplementLogTask::transform_and_copy_meta_file_(const BackupPieceFile &piece_file)
{
  int ret = OB_SUCCESS;
  ObBackupDest src;
  ObBackupDest dest;
  ObArchiveStore src_store;
  ObArchiveStore dest_store;
  const share::ObLSID &ls_id = piece_file.ls_id_;
  if (OB_FAIL(get_copy_src_and_dest_(piece_file, src, dest))) {
    LOG_WARN("failed to get copy src and dest", K(ret), K(piece_file));
  } else if (OB_FAIL(src_store.init(src))) {
    LOG_WARN("failed to init src store", K(ret), K(src));
  } else if (OB_FAIL(dest_store.init(dest))) {
    LOG_WARN("failed to init dest store", K(ret), K(dest));
  } else if (OB_FAIL(copy_ls_file_info_(piece_file, src_store, dest_store))) {
    LOG_WARN("failed to copy ls file info", K(ret), K(src), K(dest), K(piece_file));
  } else if (!ls_id.is_sys_ls()) {
    // do nothing
  } else if (OB_FAIL(copy_piece_file_info_(piece_file, src_store, dest_store))) {
    LOG_WARN("failed to copy piece file info", K(ret), K(src), K(dest), K(piece_file));
  } else if (OB_FAIL(copy_single_piece_info_(piece_file, src_store, dest_store))) {
    LOG_WARN("failed to copy single piece info", K(ret), K(src), K(dest), K(piece_file));
  } else if (OB_FAIL(copy_tenant_archive_piece_infos(piece_file, src_store, dest_store))) {
    LOG_WARN("failed to copy tenant archive piece infos", K(ret), K(src), K(dest), K(piece_file));
  } else if (OB_FAIL(copy_checkpoint_info(piece_file, src_store, dest_store))) {
    LOG_WARN("failed to copy checkpoint info", K(ret), K(src), K(dest), K(piece_file));
  } else if (OB_FAIL(copy_round_start_file(piece_file, src_store, dest_store))) {
    LOG_WARN("failed to copy round start file", K(ret), K(src), K(dest), K(piece_file));
  }
  return ret;
}

int ObLSBackupComplementLogTask::copy_ls_file_info_(
    const BackupPieceFile &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.dest_id_;
  const int64_t dest_dest_id = dest_id_;
  const int64_t round_id = piece_file.round_id_;
  const int64_t piece_id = piece_file.piece_id_;
  const share::ObLSID &ls_id = piece_file.ls_id_;
  ObSingleLSInfoDesc desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_single_ls_info_file_exist(src_dest_id, round_id, piece_id, ls_id, is_exist))) {
    LOG_WARN("failed to check is single ls info file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_single_ls_info(src_dest_id, round_id, piece_id, ls_id, desc))) {
    LOG_WARN("failed to read single ls info", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_single_ls_info(dest_dest_id, round_id, piece_id, ls_id, desc))) {
    LOG_WARN("failed to write single ls info", K(ret), K(piece_file));
  }
  return ret;
}

int ObLSBackupComplementLogTask::copy_piece_file_info_(
    const BackupPieceFile &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.dest_id_;
  const int64_t dest_dest_id = dest_id_;
  const int64_t round_id = piece_file.round_id_;
  const int64_t piece_id = piece_file.piece_id_;
  ObPieceInfoDesc piece_info_desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_piece_info_file_exist(src_dest_id, round_id, piece_id, is_exist))) {
    LOG_WARN("failed to check is piece info file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_piece_info(src_dest_id, round_id, piece_id, piece_info_desc))) {
    LOG_WARN("failed to read piece info", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_piece_info(dest_dest_id, round_id, piece_id, piece_info_desc))) {
    LOG_WARN("failed to write piece info", K(ret), K(piece_file));
  }
  return ret;
}

int ObLSBackupComplementLogTask::copy_single_piece_info_(
    const BackupPieceFile &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.dest_id_;
  const int64_t dest_dest_id = dest_id_;
  const int64_t round_id = piece_file.round_id_;
  const int64_t piece_id = piece_file.piece_id_;
  ObSinglePieceDesc single_piece_desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_single_piece_file_exist(src_dest_id, round_id, piece_id, is_exist))) {
    LOG_WARN("failed to check is single piece file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_single_piece(src_dest_id, round_id, piece_id, single_piece_desc))) {
    LOG_WARN("failed to read piece info", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_single_piece(dest_dest_id, round_id, piece_id, single_piece_desc))) {
    LOG_WARN("failed to write piece info", K(ret), K(piece_file));
  }
  return ret;
}

int ObLSBackupComplementLogTask::copy_tenant_archive_piece_infos(
    const BackupPieceFile &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.dest_id_;
  const int64_t dest_dest_id = dest_id_;
  const int64_t round_id = piece_file.round_id_;
  const int64_t piece_id = piece_file.piece_id_;
  ObTenantArchivePieceInfosDesc desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_tenant_archive_piece_infos_file_exist(src_dest_id, round_id, piece_id, is_exist))) {
    LOG_WARN("failed to check is tenant archive piece infos file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_tenant_archive_piece_infos(src_dest_id, round_id, piece_id, desc))) {
    LOG_WARN("failed to read piece info", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_tenant_archive_piece_infos(dest_dest_id, round_id, piece_id, desc))) {
    LOG_WARN("failed to write piece info", K(ret), K(piece_file));
  }
  return ret;
}

int ObLSBackupComplementLogTask::copy_checkpoint_info(const BackupPieceFile &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.dest_id_;
  const int64_t dest_dest_id = dest_id_;
  const int64_t round_id = piece_file.round_id_;
  const int64_t piece_id = piece_file.piece_id_;
  ObPieceCheckpointDesc desc;
  bool is_exist = false;
  const int64_t file_id = 0;
  if (OB_FAIL(src_store.is_piece_checkpoint_file_exist(src_dest_id, round_id, piece_id, file_id, is_exist))) {
    LOG_WARN("failed to check is piece checkpoint file file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_piece_checkpoint(src_dest_id, round_id, piece_id, file_id, desc))) {
    LOG_WARN("failed to read piece checkpoint", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_piece_checkpoint(dest_dest_id, round_id, piece_id, file_id, desc))) {
    LOG_WARN("failed to write piece checkpoint", K(ret), K(piece_file));
  }
  return ret;
}

int ObLSBackupComplementLogTask::copy_round_start_file(const BackupPieceFile &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.dest_id_;
  const int64_t dest_dest_id = dest_id_;
  const int64_t round_id = piece_file.round_id_;
  const int64_t piece_id = piece_file.piece_id_;
  ObRoundStartDesc desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_round_start_file_exist(src_dest_id, round_id, is_exist))) {
    LOG_WARN("failed to check is round start file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_round_start(src_dest_id, round_id, desc))) {
    LOG_WARN("failed to read round start", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_round_start(dest_dest_id, round_id, desc))) {
    LOG_WARN("failed to write round start", K(ret), K(piece_file));
  }
  return ret;
}

int ObLSBackupComplementLogTask::copy_piece_start_file(const BackupPieceFile &piece_file, const share::ObBackupDest &src, const share::ObBackupDest &dest)
{
  int ret = OB_SUCCESS;
  ObArchiveStore src_store;
  ObArchiveStore dest_store;
  const int64_t src_dest_id = piece_file.dest_id_;
  const int64_t dest_dest_id = dest_id_;
  const int64_t round_id = piece_file.round_id_;
  const int64_t piece_id = piece_file.piece_id_;
  const share::SCN &create_scn = piece_file.start_scn_;
  ObPieceStartDesc desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.init(src))) {
    LOG_WARN("failed to init src store", K(ret), K(src));
  } else if (OB_FAIL(dest_store.init(dest))) {
    LOG_WARN("failed to init dest store", K(ret), K(dest));
  } else if (OB_FAIL(src_store.is_piece_start_file_exist(src_dest_id, round_id, piece_id, create_scn, is_exist))) {
    LOG_WARN("failed to check is piece start file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_piece_start(src_dest_id, round_id, piece_id, create_scn, desc))) {
    LOG_WARN("failed to read round start", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_piece_start(dest_dest_id, round_id, piece_id, create_scn, desc))) {
    LOG_WARN("failed to write round start", K(ret), K(piece_file));
  }
  return ret;
}

int ObLSBackupComplementLogTask::get_archive_backup_dest_(
    const ObBackupPathString &path, share::ObBackupDest &archive_dest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(
      *report_ctx_.sql_proxy_, tenant_id_, path, archive_dest))) {
    LOG_WARN("failed to get archive dest", K(ret), K(tenant_id_), K(path));
  } else if (OB_FAIL(archive_dest_.deep_copy(archive_dest))) {
    LOG_WARN("failed to deep copy archive dest", K(ret));
  } else {
    LOG_INFO("succ get backup dest", K(tenant_id_), K(path));
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
