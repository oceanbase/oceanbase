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
#include "ob_ls_complete_migration.h"
#include "observer/ob_server.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "logservice/ob_log_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "ob_storage_ha_utils.h"
#include "storage/tablet/ob_tablet.h"
#include "ob_storage_ha_utils.h"
#include "storage/high_availability/ob_transfer_service.h"
#include "storage/high_availability/ob_rebuild_service.h"
#include "observer/omt/ob_tenant.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

ERRSIM_POINT_DEF(WAIT_CLOG_SYNC_FAILED);
ERRSIM_POINT_DEF(SERVER_STOP_BEFORE_UPDATE_MIGRATION_STATUS);
/******************ObLSCompleteMigrationCtx*********************/
ObLSCompleteMigrationCtx::ObLSCompleteMigrationCtx()
  : ObIHADagNetCtx(),
    tenant_id_(OB_INVALID_ID),
    arg_(),
    task_id_(),
    start_ts_(0),
    finish_ts_(0),
    rebuild_seq_(0)
{
}

ObLSCompleteMigrationCtx::~ObLSCompleteMigrationCtx()
{
}

bool ObLSCompleteMigrationCtx::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid()
      && tenant_id_ != 0 && tenant_id_ != OB_INVALID_ID;
}

void ObLSCompleteMigrationCtx::reset()
{
  tenant_id_ = OB_INVALID_ID;
  arg_.reset();
  task_id_.reset();
  start_ts_ = 0;
  finish_ts_ = 0;
  ObIHADagNetCtx::reset();
}

int ObLSCompleteMigrationCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls complete migration ctx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ls complete migration : task_id = %s, "
      "tenant_id = %s, ls_id = %s, src = %s, dest = %s",
      to_cstring(task_id_), to_cstring(tenant_id_), to_cstring(arg_.ls_id_), to_cstring(arg_.src_.get_server()),
      to_cstring(arg_.dst_.get_server())))) {
    LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
  }
  return ret;
}

void ObLSCompleteMigrationCtx::reuse()
{
  ObIHADagNetCtx::reuse();
}

/******************ObLSCompleteMigrationDagNet*********************/
ObLSCompleteMigrationParam::ObLSCompleteMigrationParam()
  : arg_(),
    task_id_(),
    result_(OB_SUCCESS),
    rebuild_seq_(0)
{
}

bool ObLSCompleteMigrationParam::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid() && rebuild_seq_ >= 0;
}

void ObLSCompleteMigrationParam::reset()
{
  arg_.reset();
  task_id_.reset();
  result_ = OB_SUCCESS;
  rebuild_seq_ = 0;
}


ObLSCompleteMigrationDagNet::ObLSCompleteMigrationDagNet()
    : ObIDagNet(ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION),
      is_inited_(false),
      ctx_()

{
}

ObLSCompleteMigrationDagNet::~ObLSCompleteMigrationDagNet()
{
}

int ObLSCompleteMigrationDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObLSCompleteMigrationParam* init_param = static_cast<const ObLSCompleteMigrationParam*>(param);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls complete migration dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(this->set_dag_id(init_param->task_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else {
    ctx_.tenant_id_ = MTL_ID();
    ctx_.arg_ = init_param->arg_;
    ctx_.task_id_ = init_param->task_id_;
    ctx_.rebuild_seq_ = init_param->rebuild_seq_;
    if (OB_SUCCESS != init_param->result_) {
      if (OB_FAIL(ctx_.set_result(init_param->result_, false /*allow_retry*/))) {
        LOG_WARN("failed to set result", K(ret), KPC(init_param));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObLSCompleteMigrationDagNet::is_valid() const
{
  return ctx_.is_valid();
}

int ObLSCompleteMigrationDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls complete migration dag net do not init", K(ret));
  } else if (OB_FAIL(start_running_for_migration_())) {
    LOG_WARN("failed to start running for migration", K(ret));
  }
  return ret;
}

int ObLSCompleteMigrationDagNet::start_running_for_migration_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialCompleteMigrationDag *initial_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls complete migration dag net do not init", K(ret));
  } else if (FALSE_IT(ctx_.start_ts_ = ObTimeUtil::current_time())) {
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(ObMigrationUtils::get_dag_priority(ctx_.arg_.type_, prio))) {
    LOG_WARN("failed to get dag priority", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, initial_dag))) {
    LOG_WARN("failed to alloc initial dag ", K(ret), K(prio));
  } else if (OB_FAIL(initial_dag->init(this))) {
    LOG_WARN("failed to init initial dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*initial_dag))) {
    LOG_WARN("failed to add initial dag into dag net", K(ret));
  } else if (OB_FAIL(initial_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(initial_dag))) {
    LOG_WARN("failed to add initial dag", K(ret), K(*initial_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    initial_dag = nullptr;
  }

  if (OB_NOT_NULL(initial_dag) && OB_NOT_NULL(scheduler)) {
    if (OB_SUCCESS != (tmp_ret = (erase_dag_from_dag_net(*initial_dag)))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(initial_dag));
    }
    scheduler->free_dag(*initial_dag); // contain reset_children
    initial_dag = nullptr;
  }
  return ret;
}

bool ObLSCompleteMigrationDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObLSCompleteMigrationDagNet &other_dag_net = static_cast<const ObLSCompleteMigrationDagNet &>(other);
    if (!is_valid() || !other_dag_net.is_valid()) {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ls complete migration dag net is invalid", K(*this), K(other));
      is_same = false;
    } else if (ctx_.arg_.ls_id_ != other_dag_net.get_ls_id()) {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObLSCompleteMigrationDagNet::hash() const
{
  int64_t hash_value = 0;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    tmp_ret = OB_NOT_INIT;
    LOG_ERROR_RET(tmp_ret, "ls complete migration ctx is NULL", K(tmp_ret), K(ctx_));
  } else {
    hash_value = common::murmurhash(&ctx_.arg_.ls_id_, sizeof(ctx_.arg_.ls_id_), hash_value);
  }
  return hash_value;
}

int ObLSCompleteMigrationDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls complete migration dag net do not init ", K(ret));
  } else if (OB_UNLIKELY(0 > ctx_.task_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to get trace id string", K(ret), K(ctx_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
          "ObLSCompleteMigrationDagNet: tenant_id=%s, ls_id=%s, migration_type=%d, trace_id=%s",
          to_cstring(ctx_.tenant_id_), to_cstring(ctx_.arg_.ls_id_), ctx_.arg_.type_, task_id_str))) {
    LOG_WARN("failed to fill comment", K(ret), K(ctx_));
  }
  return ret;
}

int ObLSCompleteMigrationDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls complete migration dag net do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "ObLSCompleteMigrationDagNet: ls_id = %s, migration_type = %s",
      to_cstring(ctx_.arg_.ls_id_), ObMigrationOpType::get_str(ctx_.arg_.type_)))) {
    LOG_WARN("failed to fill comment", K(ret), K(ctx_));
  }
  return ret;
}

int ObLSCompleteMigrationDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  int32_t result = OB_SUCCESS;
  ObLSMigrationHandler *ls_migration_handler = nullptr;
  ObLSHandle ls_handle;
  LOG_INFO("start clear dag net ctx", K(ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls complete migration dag net do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_.arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("ls should not be NULL", K(ret), K(ctx_));
  } else {
    if (OB_SUCCESS != (tmp_ret = update_migration_status_(ls))) {
      LOG_WARN("failed to update migration status", K(tmp_ret), K(ret), K(ctx_));
    }

    if (OB_SUCCESS != (tmp_ret = report_ls_meta_table_(ls))) {
      LOG_WARN("failed to report ls meta table", K(tmp_ret), K(ret), K(ctx_));
    }

    if (OB_ISNULL(ls_migration_handler = ls->get_ls_migration_handler())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls migration handler should not be NULL", K(tmp_ret), K(ctx_));
    } else if (OB_FAIL(ctx_.get_result(result))) {
      LOG_WARN("failed to get ls complate migration ctx result", K(ret), K(ctx_));
    } else if (OB_FAIL(ls_migration_handler->switch_next_stage(result))) {
      LOG_WARN("failed to report result", K(ret), K(result), K(ctx_));
    }

    ctx_.finish_ts_ = ObTimeUtil::current_time();
    const int64_t cost_ts = ctx_.finish_ts_ - ctx_.start_ts_;
    FLOG_INFO("finish ls complete migration dag net", "ls id", ctx_.arg_.ls_id_, "type", ctx_.arg_.type_, K(cost_ts));
  }
  return ret;
}

int ObLSCompleteMigrationDagNet::trans_rebuild_fail_status_(
    ObLS &ls,
    const ObMigrationStatus &current_migration_status,
    ObMigrationStatus &new_migration_status)
{
  int ret = OB_SUCCESS;
  bool is_valid_member = true;
  bool is_ls_deleted = false;
  new_migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  bool is_tenant_dropped = false;
  if (!ObMigrationStatusHelper::is_valid(current_migration_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("current migration status is invalid", K(ret), K(current_migration_status));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD != current_migration_status
      && ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_WAIT != current_migration_status
      && ObMigrationStatus::OB_MIGRATION_STATUS_NONE != current_migration_status) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration status is unexpected", K(ret), K(current_migration_status), K(ctx_));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObStorageHADagUtils::check_self_is_valid_member(ls.get_ls_id(), is_valid_member))) {
      is_valid_member = true; // reset value if fail
      LOG_WARN("failed to check self is valid member", K(ret), K(tmp_ret), K(ctx_));
    }
    if (OB_TMP_FAIL(ObStorageHAUtils::check_ls_deleted(ls.get_ls_id(), is_ls_deleted))) {
      is_ls_deleted = false; // reset value if fail
      LOG_WARN("failed to get ls status from inner table", K(ret), K(tmp_ret), K(ls));
    }
    if (OB_TMP_FAIL(check_tenant_is_dropped_(is_tenant_dropped))) {
      is_tenant_dropped = false;
      LOG_WARN("failed to check tenant is droppping or dropped", K(ret), K(tmp_ret), K(ctx_));
    }
    if (FAILEDx(ObMigrationStatusHelper::trans_rebuild_fail_status(
        current_migration_status, is_valid_member, is_ls_deleted, is_tenant_dropped, new_migration_status))) {
      LOG_WARN("failed to trans rebuild fail status", K(ret), K(ctx_));
    }
  }
  return ret;
}

int ObLSCompleteMigrationDagNet::update_migration_status_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  static const int64_t UPDATE_MIGRATION_STATUS_INTERVAL_MS = 100 * 1000; //100ms
  ObTenantDagScheduler *scheduler = nullptr;
  int32_t result = OB_SUCCESS;
  bool is_tenant_deleted = false;

  DEBUG_SYNC(BEFORE_COMPLETE_MIGRATION_UPDATE_STATUS);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration dag net do not init", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update migration status get invalid argument", K(ret), KP(ls));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    while (!is_finish) {
      ObMigrationStatus current_migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
      ObMigrationStatus new_migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
      bool need_update_status = true;

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = SERVER_STOP_BEFORE_UPDATE_MIGRATION_STATUS ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake SERVER_STOP_BEFORE_UPDATE_MIGRATION_STATUS", K(ret));
        break;
      }
    }
#endif

      if (ls->is_stopped()) {
        ret = OB_NOT_RUNNING;
        LOG_WARN("ls is not running, stop migration dag net", K(ret), K(ctx_));
        break;
      } else if (scheduler->has_set_stop()) {
        ret = OB_SERVER_IS_STOPPING;
        LOG_WARN("tenant dag scheduler has set stop, stop migration dag net", K(ret), K(ctx_));
        break;
      } else {
        bool in_final_state = false;
        if (OB_FAIL(ls->get_migration_status(current_migration_status))) {
          LOG_WARN("failed to get migration status", K(ret), K(ctx_));
        } else if (OB_FAIL(ctx_.get_result(result))) {
          LOG_WARN("failed to get result", K(ret), K(ctx_));
        } else if (OB_FAIL(ObMigrationStatusHelper::check_migration_in_final_state(current_migration_status, in_final_state))) {
          LOG_WARN("failed to check migration in final state", K(ret), K(ctx_));
        } else if (in_final_state) {
          need_update_status = false;
          if (ObMigrationOpType::REBUILD_LS_OP == ctx_.arg_.type_
              && (ObMigrationStatus::OB_MIGRATION_STATUS_NONE == current_migration_status
                  || ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_FAIL == current_migration_status
                  || ObMigrationStatus::OB_MIGRATION_STATUS_GC == current_migration_status)) {
            LOG_INFO("current migration status is none, no need update migration status", K(current_migration_status), K(ctx_));
          } else {
            if (!ctx_.is_failed()) {
              result = OB_ERR_UNEXPECTED;
              if (OB_FAIL(ctx_.set_result(result, false/*need_retry*/))) {
                LOG_WARN("failed to set result", K(ret), K(result), K(ctx_));
              }
            }
            LOG_ERROR("current migration status is final state and unexpected", K(OB_ERR_UNEXPECTED), K(current_migration_status), K(ctx_));
          }
        } else if (ctx_.is_failed()) {
          if (ObMigrationOpType::REBUILD_LS_OP == ctx_.arg_.type_) {
            if (OB_FAIL(trans_rebuild_fail_status_(*ls, current_migration_status, new_migration_status))) {
              LOG_WARN("failed check rebuild status", K(ret), K(current_migration_status), K(new_migration_status));
            }
          } else if (OB_FAIL(ObMigrationStatusHelper::trans_fail_status(current_migration_status, new_migration_status))) {
            LOG_WARN("failed to trans fail status", K(ret), K(current_migration_status), K(new_migration_status));
          }
        } else {
          new_migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
        }

        if (OB_FAIL(ret)) {
        } else if (!need_update_status) {
          is_finish = true;
        } else if (ObMigrationOpType::REBUILD_LS_OP == ctx_.arg_.type_ && ObMigrationStatus::OB_MIGRATION_STATUS_NONE == new_migration_status
            && OB_FAIL(ls->clear_saved_info())) {
          LOG_WARN("failed to clear ls saved info", K(ret), KPC(ls));
        } else if (OB_FAIL(ls->set_migration_status(new_migration_status, ctx_.rebuild_seq_))) {
          LOG_WARN("failed to set migration status", K(ret), K(current_migration_status), K(new_migration_status), K(ctx_));
        } else {
          is_finish = true;
        }
      }

      if (OB_FAIL(ret)) {
        ob_usleep(UPDATE_MIGRATION_STATUS_INTERVAL_MS);
      }
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const bool need_retry = false;
    if (OB_SUCCESS != (tmp_ret = ctx_.set_result(ret, need_retry))) {
      LOG_ERROR("failed to set result", K(ret), K(ret), K(tmp_ret), K(ctx_));
    }
  }
  return ret;
}

int ObLSCompleteMigrationDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls complete migration dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_.set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

int ObLSCompleteMigrationDagNet::report_ls_meta_table_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObMigrationStatus status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  const int64_t MAX_RETRY_NUM = 3;
  const int64_t REPORT_INTERVAL = 200_ms;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls complete migration dag net do not init", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update migration status get invalid argument", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_migration_status(status))) {
    LOG_WARN("failed to get migration status", K(ret), KPC(ls));
  } else {
    for (int64_t i = 0; i < MAX_RETRY_NUM; ++i) {
      //overwrite ret
      if (OB_FAIL(ObStorageHAUtils::report_ls_meta_table(ctx_.tenant_id_, ctx_.arg_.ls_id_, status))) {
        LOG_WARN("failed to report ls meta table", K(ret), K(ctx_));
      } else {
        break;
      }
      if (OB_FAIL(ret)) {
        ob_usleep(REPORT_INTERVAL);
      }
    }

    if (OB_SUCC(ret)) {
      //do nothing
    } else if (OB_FAIL(GCTX.ob_service_->submit_ls_update_task(ctx_.tenant_id_, ctx_.arg_.ls_id_))) {
      //overwrite ret
      LOG_WARN("failed to submit ls update task", K(ret), K(ctx_));
    }
  }
  return ret;
}

int ObLSCompleteMigrationDagNet::check_tenant_is_dropped_(
    bool &is_tenant_dropped)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  is_tenant_dropped = false;
  const ObTenantSchema *tenant_schema = nullptr;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "schema_service is nullptr", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(ctx_));
  } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(ctx_.tenant_id_, is_tenant_dropped))) {
    LOG_WARN("fail to check if tenant has been dropped", KR(ret), K(ctx_));
  } else if (is_tenant_dropped) {
    LOG_INFO("tenant is already dropped", K(ctx_), K(is_tenant_dropped));
  }
  return ret;
}

/******************ObCompleteMigrationDag*********************/
ObCompleteMigrationDag::ObCompleteMigrationDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObStorageHADag(dag_type)
{
}

ObCompleteMigrationDag::~ObCompleteMigrationDag()
{
}

bool ObCompleteMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObStorageHADag &ha_dag = static_cast<const ObStorageHADag&>(other);
    if (OB_ISNULL(ha_dag_net_ctx_) || OB_ISNULL(ha_dag.get_ha_dag_net_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "complete migration ctx should not be NULL", KP(ha_dag_net_ctx_), KP(ha_dag.get_ha_dag_net_ctx()));
    } else if (ha_dag_net_ctx_->get_dag_net_ctx_type() != ha_dag.get_ha_dag_net_ctx()->get_dag_net_ctx_type()) {
      is_same = false;
    } else {
      ObLSCompleteMigrationCtx *self_ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_);
      ObLSCompleteMigrationCtx *other_ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag.get_ha_dag_net_ctx());
      if (self_ctx->arg_.ls_id_ != other_ctx->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObCompleteMigrationDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("complete migration ctx should not be NULL", KP(ha_dag_net_ctx_));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else {
    ObLSCompleteMigrationCtx *self_ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_);
    hash_value = common::murmurhash(
        &self_ctx->arg_.ls_id_, sizeof(self_ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObCompleteMigrationDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *ctx = nullptr;

  if (FALSE_IT(ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(ctx->tenant_id_), ctx->arg_.ls_id_.id(),
                                static_cast<int64_t>(ctx->arg_.type_),
                                "dag_net_task_id", to_cstring(ctx->task_id_),
                                "src", to_cstring(ctx->arg_.src_.get_server()),
                                "dest", to_cstring(ctx->arg_.dst_.get_server())))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObCompleteMigrationDag::prepare_ctx(share::ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationDagNet *complete_dag_net = nullptr;
  ObLSCompleteMigrationCtx *self_ctx = nullptr;

  if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(complete_dag_net = static_cast<ObLSCompleteMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(self_ctx = complete_dag_net->get_ctx())) {
  } else if (OB_ISNULL(self_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("complete migration ctx should not be NULL", K(ret), KP(self_ctx));
  } else {
    ha_dag_net_ctx_ = self_ctx;
  }
  return ret;
}

/******************ObInitialCompleteMigrationDag*********************/
ObInitialCompleteMigrationDag::ObInitialCompleteMigrationDag()
  : ObCompleteMigrationDag(share::ObDagType::DAG_TYPE_INITIAL_COMPLETE_MIGRATION),
    is_inited_(false)
{
}

ObInitialCompleteMigrationDag::~ObInitialCompleteMigrationDag()
{
}

int ObInitialCompleteMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *self_ctx = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial prepare migration dag do not init", K(ret));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
         "ObInitialCompleteMigrationDag: ls_id = %s, migration_type = %s, dag_prio = %s",
         to_cstring(self_ctx->arg_.ls_id_), ObMigrationOpType::get_str(self_ctx->arg_.type_),
         ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), K(*self_ctx));
  }
  return ret;
}

int ObInitialCompleteMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial complete migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init initial complete migration dag get invalid argument", K(ret), KP(dag_net));
  } else if (OB_FAIL(ObCompleteMigrationDag::prepare_ctx(dag_net))) {
    LOG_WARN("failed to prepare ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObInitialCompleteMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObInitialCompleteMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial complete migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init initial complete migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObInitialCompleteMigrationTask*********************/
ObInitialCompleteMigrationTask::ObInitialCompleteMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObInitialCompleteMigrationTask::~ObInitialCompleteMigrationTask()
{
}

int ObInitialCompleteMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSCompleteMigrationDagNet *prepare_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial prepare migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(prepare_dag_net = static_cast<ObLSCompleteMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = prepare_dag_net->get_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init initial complete migration task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObInitialCompleteMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial complete migration task do not init", K(ret));
  } else if (OB_FAIL(generate_migration_dags_())) {
    LOG_WARN("failed to generate migration dags", K(ret), K(*ctx_));
  }
  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  return ret;
}

int ObInitialCompleteMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("storage_ha", "initial_complete_migration_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id",ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_,
        "is_failed", ctx_->is_failed(),
        ObMigrationOpType::get_str(ctx_->arg_.type_));
  }
  return ret;
}

int ObInitialCompleteMigrationTask::generate_migration_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStartCompleteMigrationDag *start_complete_dag = nullptr;
  ObFinishCompleteMigrationDag *finish_complete_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObInitialCompleteMigrationDag *initial_complete_migration_dag = nullptr;
  ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial prepare migration task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(initial_complete_migration_dag = static_cast<ObInitialCompleteMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial complete migration dag should not be NULL", K(ret), KP(initial_complete_migration_dag));
  } else if (OB_FAIL(ObMigrationUtils::get_dag_priority(ctx_->arg_.type_, prio))) {
    LOG_WARN("failed to get dag priority", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, start_complete_dag))) {
      LOG_WARN("failed to alloc start complete migration dag ", K(ret));
    } else if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, finish_complete_dag))) {
      LOG_WARN("failed to alloc finish complete migration dag", K(ret));
    } else if (OB_FAIL(start_complete_dag->init(dag_net_))) {
      LOG_WARN("failed to init start complete migration dag", K(ret));
    } else if (OB_FAIL(finish_complete_dag->init(dag_net_))) {
      LOG_WARN("failed to init finish complete migration dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*start_complete_dag))) {
      LOG_WARN("failed to add start complete dag", K(ret), KPC(start_complete_dag));
    } else if (OB_FAIL(start_complete_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(start_complete_dag->add_child(*finish_complete_dag))) {
      LOG_WARN("failed to add finish complete migration dag as child", K(ret));
    } else if (OB_FAIL(finish_complete_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(finish_complete_dag))) {
      LOG_WARN("failed to add finish complete migration dag", K(ret), K(*finish_complete_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else if (OB_FAIL(scheduler->add_dag(start_complete_dag))) {
      LOG_WARN("failed to add dag", K(ret), K(*start_complete_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }

      if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(finish_complete_dag))) {
        LOG_WARN("failed to cancel ha dag", K(tmp_ret), KPC(initial_complete_migration_dag));
      } else {
        finish_complete_dag = nullptr;
      }
      finish_complete_dag = nullptr;
    } else {
      LOG_INFO("succeed to schedule start complete migration dag", K(*start_complete_dag));
      start_complete_dag = nullptr;
      finish_complete_dag = nullptr;
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(finish_complete_dag)) {
        scheduler->free_dag(*finish_complete_dag);
        finish_complete_dag = nullptr;
      }

      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(start_complete_dag)) {
        scheduler->free_dag(*start_complete_dag);
        start_complete_dag = nullptr;
      }

      if (OB_SUCCESS != (tmp_ret = ctx_->set_result(ret, true /*allow_retry*/, this->get_dag()->get_type()))) {
        LOG_WARN("failed to set complete migration result", K(ret), K(tmp_ret), K(*ctx_));
      }
    }
  }
  return ret;
}

/******************ObStartCompleteMigrationDag*********************/
ObStartCompleteMigrationDag::ObStartCompleteMigrationDag()
  : ObCompleteMigrationDag(share::ObDagType::DAG_TYPE_START_COMPLETE_MIGRATION),
    is_inited_(false)
{
}

ObStartCompleteMigrationDag::~ObStartCompleteMigrationDag()
{
}

int ObStartCompleteMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *self_ctx = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial prepare migration dag do not init", K(ret));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
         "ObStartPrepareMigrationDag: ls_id = %s, migration_type = %s, dag_prio = %s",
         to_cstring(self_ctx->arg_.ls_id_), ObMigrationOpType::get_str(self_ctx->arg_.type_),
         ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), K(*self_ctx));
  }
  return ret;
}

int ObStartCompleteMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start complete migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init start complete migration dag get invalid argument", K(ret), KP(dag_net));
  } else if (OB_FAIL(prepare_ctx(dag_net))) {
    LOG_WARN("failed to prepare ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStartCompleteMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObStartCompleteMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init start complete migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObStartCompleteMigrationTask*********************/
ObStartCompleteMigrationTask::ObStartCompleteMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ls_handle_(),
    ctx_(nullptr),
    log_sync_lsn_(0),
    max_minor_end_scn_(SCN::min_scn())
{
}

ObStartCompleteMigrationTask::~ObStartCompleteMigrationTask()
{
}

int ObStartCompleteMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSCompleteMigrationDagNet *complete_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start complete migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (OB_ISNULL(complete_dag_net = static_cast<ObLSCompleteMigrationDagNet*>(dag_net))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("complete dag net should not be NULL", K(ret), KP(complete_dag_net));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(complete_dag_net->get_ctx()->arg_.ls_id_, ls_handle_))) {
    LOG_WARN("failed to get ls", K(ret), KPC(dag_net));
  } else {
    ctx_ = complete_dag_net->get_ctx();
    is_inited_ = true;
    LOG_INFO("succeed init start complete migration task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObStartCompleteMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(update_ls_migration_status_wait_())) {
    LOG_WARN("failed to update ls migration wait", K(ret), KPC(ctx_));
  } else if (OB_FAIL(wait_log_sync_())) {
    LOG_WARN("failed wait log sync", K(ret), KPC(ctx_));
  } else if (OB_FAIL(wait_log_replay_sync_())) {
    LOG_WARN("failed to wait log replay sync", K(ret), KPC(ctx_));
  } else if (OB_FAIL(wait_transfer_table_replace_())) {
    LOG_WARN("failed to wait transfer table replace", K(ret), KPC(ctx_));
  } else if (OB_FAIL(check_all_tablet_ready_())) {
    LOG_WARN("failed to check all tablet ready", K(ret), KPC(ctx_));
  } else if (OB_FAIL(wait_log_replay_to_max_minor_end_scn_())) {
    LOG_WARN("failed to wait log replay to max minor end scn", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ObStorageHAUtils::check_disk_space())) {
    LOG_WARN("failed to check disk space", K(ret), KPC(ctx_));
  } else if (OB_FAIL(update_ls_migration_status_hold_())) {
    LOG_WARN("failed to update ls migration status hold", K(ret), KPC(ctx_));
  } else if (OB_FAIL(change_member_list_with_retry_())) {
    LOG_WARN("failed to change member list", K(ret), KPC(ctx_));
  }
  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObStartCompleteMigrationTask::get_wait_timeout_(int64_t &timeout)
{
  int ret = OB_SUCCESS;
  timeout = 10_min;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    timeout = tenant_config->_ls_migration_wait_completing_timeout;
  }
  return ret;
}

int ObStartCompleteMigrationTask::wait_log_sync_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  bool is_log_sync = false;
  bool is_need_rebuild = false;
  palf::LSN last_end_lsn(0);
  palf::LSN current_end_lsn(0);
  bool need_wait = true;
  ObTimeoutCtx timeout_ctx;
  int64_t timeout = 10_min;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration finish task do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), KPC(ctx_));
  } else if (OB_FAIL(check_need_wait_(ls, need_wait))) {
    LOG_WARN("failed to check need wait log sync", K(ret), KPC(ctx_));
  } else if (!need_wait) {
    FLOG_INFO("no need wait log sync", KPC(ctx_));
  } else if (OB_FAIL(get_wait_timeout_(timeout))) {
    LOG_WARN("failed to get wait timeout", K(ret));
  } else if (OB_FAIL(init_timeout_ctx_(timeout, timeout_ctx))) {
    LOG_WARN("failed to init timeout ctx", K(ret));
  } else {
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("storage_ha", "wait_log_sync",
                          "tenant_id", ctx_->tenant_id_,
                          "ls_id", ls->get_ls_id().id());
#endif
    DEBUG_SYNC(BEFORE_WAIT_LOG_SYNC);
    const int64_t wait_replay_start_ts = ObTimeUtility::current_time();
    int64_t current_ts = 0;
    int64_t last_wait_replay_ts = ObTimeUtility::current_time();
    while (OB_SUCC(ret) && !is_log_sync) {
      if (timeout_ctx.is_timeouted()) {
        ret = OB_LOG_NOT_SYNC;
        LOG_WARN("already timeout", K(ret), KPC(ctx_));
      } else if (OB_FAIL(check_ls_and_task_status_(ls))) {
        LOG_WARN("failed to check ls and task status", K(ret), KPC(ctx_));
      } else if (OB_FAIL(ls->is_in_sync(is_log_sync, is_need_rebuild))) {
        LOG_WARN("failed to check is in sync", K(ret), KPC(ctx_));
      }

      if (OB_FAIL(ret)) {
      } else if (is_log_sync) {
        if (OB_FAIL(ls->get_end_lsn(log_sync_lsn_))) {
          LOG_WARN("failed to get end lsn", K(ret), KPC(ctx_));
        } else {
          const int64_t cost_ts = ObTimeUtility::current_time() - wait_replay_start_ts;
          LOG_INFO("log is sync, stop wait_log_sync", "arg", ctx_->arg_, K(cost_ts));
        }
      } else if (is_need_rebuild) {
        const int64_t cost_ts = ObTimeUtility::current_time() - wait_replay_start_ts;
        ret = OB_LOG_NOT_SYNC;
        LOG_WARN("log is not sync", K(ret), KPC(ctx_), K(cost_ts));
      } else if (OB_FAIL(ls->get_end_lsn(current_end_lsn))) {
        LOG_WARN("failed to get end scn", K(ret), KPC(ctx_));
      } else {
        bool is_timeout = false;
        if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("log is not sync, retry next loop", "arg", ctx_->arg_);
        }

        if (current_end_lsn == last_end_lsn) {
          const int64_t current_ts = ObTimeUtility::current_time();
          if ((current_ts - last_wait_replay_ts) > timeout) {
            is_timeout = true;
          }

          if (is_timeout) {
            if (OB_FAIL(ctx_->set_result(OB_LOG_NOT_SYNC, true /*allow_retry*/, this->get_dag()->get_type()))) {
                LOG_WARN("failed to set result", K(ret), KPC(ctx_));
            } else {
              ret = OB_LOG_NOT_SYNC;
              STORAGE_LOG(WARN, "failed to check log replay sync. timeout, stop migration task",
                  K(ret), K(*ctx_), K(timeout), K(wait_replay_start_ts),
                  K(current_ts), K(current_end_lsn));
            }
          }
        } else if (last_end_lsn > current_end_lsn) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("last end log ts should not smaller than current end log ts", K(ret), K(last_end_lsn), K(current_end_lsn));
        } else {
          last_end_lsn = current_end_lsn;
          last_wait_replay_ts = ObTimeUtility::current_time();
        }

        if (OB_SUCC(ret)) {
          ob_usleep(CHECK_CONDITION_INTERVAL);
        }
      }
    }
    if (OB_SUCC(ret) && !is_log_sync) {
      const int64_t cost_ts = ObTimeUtility::current_time() - wait_replay_start_ts;
      ret = OB_LOG_NOT_SYNC;
      LOG_WARN("log is not sync", K(ret), KPC(ctx_), K(cost_ts));
    }
  }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = WAIT_CLOG_SYNC_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake WAIT_CLOG_SYNC_FAILED", K(ret));
      }
    }
#endif
  return ret;
}

int ObStartCompleteMigrationTask::wait_log_replay_sync_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  logservice::ObLogService *log_service = nullptr;
  logservice::ObLogReplayService *log_replay_service = nullptr;
  ObRebuildService *rebuild_service = nullptr;
  bool wait_log_replay_success = false;
  SCN current_replay_scn;
  SCN last_replay_scn;
  bool need_wait = false;
  bool is_done = false;
  const bool is_primay_tenant = MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID();
  share::SCN readable_scn;
  ObTimeoutCtx timeout_ctx;
  int64_t timeout = 10_min;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (OB_ISNULL(log_service = (MTL(logservice::ObLogService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log service should not be NULL", K(ret), KP(log_replay_service));
  } else if (OB_ISNULL(rebuild_service = (MTL(ObRebuildService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild service should not be NULL", K(ret), KP(rebuild_service));
  } else if (OB_ISNULL(log_replay_service = log_service->get_log_replay_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls replay service should not be NULL", K(ret), KP(log_replay_service));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), KPC(ctx_));
  } else if (OB_FAIL(check_need_wait_(ls, need_wait))) {
    LOG_WARN("failed to check need wait log replay", K(ret), KPC(ctx_));
  } else if (!need_wait) {
    FLOG_INFO("no need wait replay log sync", KPC(ctx_));
  } else if (!is_primay_tenant && OB_FAIL(ObStorageHAUtils::get_readable_scn_with_retry(readable_scn))) {
    LOG_WARN("failed to get readable scn", K(ret), KPC(ctx_));
  } else if (OB_FAIL(get_wait_timeout_(timeout))) {
    LOG_WARN("failed to get wait timeout", K(ret));
  } else if (OB_FAIL(init_timeout_ctx_(timeout, timeout_ctx))) {
    LOG_WARN("failed to init timeout ctx", K(ret));
  } else {
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("storage_ha", "wait_log_replay_sync",
                          "tenant_id", ctx_->tenant_id_,
                          "ls_id", ls->get_ls_id().id());
#endif
    DEBUG_SYNC(BEFORE_WAIT_LOG_REPLAY_SYNC);
    const int64_t wait_replay_start_ts = ObTimeUtility::current_time();
    int64_t last_replay_ts = 0;
    int64_t current_ts = 0;
    bool need_rebuild = false;
    while (OB_SUCC(ret) && !wait_log_replay_success) {
      if (timeout_ctx.is_timeouted()) {
        ret = OB_WAIT_REPLAY_TIMEOUT;
        LOG_WARN("already timeout", K(ret), KPC(ctx_));
      } else if (OB_FAIL(check_ls_and_task_status_(ls))) {
        LOG_WARN("failed to check ls and task status", K(ret), KPC(ctx_));
      } else if (OB_FAIL(rebuild_service->check_ls_need_rebuild(ls->get_ls_id(), need_rebuild))) {
        LOG_WARN("failed to check ls need rebuild", K(ret), KPC(ls));
      } else if (need_rebuild) {
        if (OB_FAIL(ctx_->set_result(OB_LS_NEED_REBUILD, false /*allow_retry*/))) {
          LOG_WARN("failed to set result", K(ret), KPC(ctx_));
        } else {
          ret = OB_LS_NEED_REBUILD;
          LOG_WARN("ls need rebuild", K(ret), KPC(ls));
        }
      } else if (OB_FAIL(log_replay_service->is_replay_done(ls->get_ls_id(), log_sync_lsn_, is_done))) {
        LOG_WARN("failed to check is replay done", K(ret), KPC(ls), K(log_sync_lsn_));
      } else if (is_done) {
        wait_log_replay_success = true;
        const int64_t cost_ts = ObTimeUtility::current_time() - wait_replay_start_ts;
        LOG_INFO("wait replay log ts ns success, stop wait", "arg", ctx_->arg_, K(cost_ts));
      } else if (OB_FAIL(ls->get_max_decided_scn(current_replay_scn))) {
        LOG_WARN("failed to get current replay log ts", K(ret), KPC(ctx_));
      } else if (!is_primay_tenant && current_replay_scn >= readable_scn) {
        wait_log_replay_success = true;
        const int64_t cost_ts = ObTimeUtility::current_time() - wait_replay_start_ts;
        LOG_INFO("wait replay log ts ns success, stop wait", "arg", ctx_->arg_, K(cost_ts),
            K(is_primay_tenant), K(current_replay_scn), K(readable_scn));
      }

      if (OB_SUCC(ret) && !wait_log_replay_success) {
        current_ts = ObTimeUtility::current_time();
        bool is_timeout = false;
        if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("replay log is not sync, retry next loop", "arg", ctx_->arg_,
              "current_replay_scn", current_replay_scn,
              "log_sync_lsn", log_sync_lsn_);
        }

        if (current_replay_scn == last_replay_scn) {
          if (current_ts - last_replay_ts > timeout) {
            is_timeout = true;
          }
          if (is_timeout) {
            if (OB_FAIL(ctx_->set_result(OB_WAIT_REPLAY_TIMEOUT, true /*allow_retry*/, this->get_dag()->get_type()))) {
                LOG_WARN("failed to set result", K(ret), KPC(ctx_));
            } else {
              ret = OB_WAIT_REPLAY_TIMEOUT;
              STORAGE_LOG(WARN, "failed to check log replay sync. timeout, stop migration task",
                  K(ret), K(*ctx_), K(timeout), K(wait_replay_start_ts),
                  K(current_ts), K(current_replay_scn));
            }
          }
        } else if (last_replay_scn > current_replay_scn) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("last end log ts should not smaller than current end log ts", K(ret),
              K(last_replay_scn), K(current_replay_scn));
        } else {
          last_replay_scn = current_replay_scn;
          last_replay_ts = current_ts;
        }

        if (OB_SUCC(ret)) {
          ob_usleep(CHECK_CONDITION_INTERVAL);
        }
      }
    }

    if (OB_SUCC(ret) && !wait_log_replay_success) {
      const int64_t cost_ts = ObTimeUtility::current_time() - wait_replay_start_ts;
      ret = OB_LOG_NOT_SYNC;
      LOG_WARN("log is not sync", K(ret), KPC(ctx_), K(cost_ts));
    }
  }
  return ret;
}

int ObStartCompleteMigrationTask::wait_transfer_table_replace_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  const int64_t check_all_tablet_start_ts = ObTimeUtility::current_time();
  const bool need_initial_state = false;
  bool need_wait_transfer_table_replace = false;
  ObTimeoutCtx timeout_ctx;
  int64_t timeout = 10_min;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to change member list", K(ret), KP(ls));
  } else if (OB_FAIL(check_need_wait_transfer_table_replace_(ls, need_wait_transfer_table_replace))) {
    LOG_WARN("failed to check need wait transfer table replace", K(ret), KPC(ctx_));
  } else if (!need_wait_transfer_table_replace) {
    LOG_INFO("no need wait transfer table replace", KPC(ls));
  } else if (OB_FAIL(get_wait_timeout_(timeout))) {
    LOG_WARN("failed to get wait timeout", K(ret));
  } else if (OB_FAIL(init_timeout_ctx_(timeout, timeout_ctx))) {
    LOG_WARN("failed to init timeout ctx", K(ret));
  } else {
    SERVER_EVENT_ADD("storage_ha", "wait_transfer_table_replace",
                  "tenant_id", ctx_->tenant_id_,
                  "ls_id", ls->get_ls_id().id());
    ObHALSTabletIDIterator iter(ls->get_ls_id(), need_initial_state);
    ObTabletID tablet_id;
    if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(iter))) {
      LOG_WARN("failed to build tablet iter", K(ret), KPC(ctx_));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter.get_next_tablet_id(tablet_id))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get tablet id", K(ret));
          }
        } else if (OB_FAIL(check_tablet_transfer_table_ready_(tablet_id, ls, timeout))) {
          LOG_WARN("failed to check tablet ready", K(ret), K(tablet_id), KPC(ls));
        }
      }
      LOG_INFO("check all tablet transfer table ready finish", K(ret), "ls_id", ctx_->arg_.ls_id_,
          "cost ts", ObTimeUtility::current_time() - check_all_tablet_start_ts);
    }
  }
  return ret;
}

int ObStartCompleteMigrationTask::change_member_list_with_retry_()
{
  //change to HOLD status do not allow failed
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  static const int64_t CHANGE_MEMBER_LIST_RETRY_INTERVAL = 2_s;
  int64_t retry_times = 0;
  bool is_valid_member = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else {
    while (retry_times < OB_MAX_RETRY_TIMES) {
      if (retry_times >= 1) {
        LOG_WARN("retry change member list", K(retry_times));
      }

      if (OB_FAIL(change_member_list_())) {
        LOG_WARN("failed to change member list", K(ret), K(retry_times));
      } else {
        break;
      }

      if (OB_FAIL(ret)) {
        //overwrite ret
        if (OB_FAIL(ObStorageHADagUtils::check_self_is_valid_member(ctx_->arg_.ls_id_, is_valid_member))) {
          LOG_WARN("failed to check self is valid member", K(ret), KPC(ctx_));
        } else if (is_valid_member) {
          break;
        } else {
          ret = OB_EAGAIN;
          LOG_WARN("self ls is not valid member, need retry", K(ret), "ls_id", ctx_->arg_.ls_id_);
        }
      }

      if (OB_FAIL(ret)) {
        retry_times++;
        ob_usleep(CHANGE_MEMBER_LIST_RETRY_INTERVAL);
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = ctx_->set_result(OB_MEMBER_CHANGE_FAILED, false /*need retry*/, this->get_dag()->get_type()))) {
        LOG_WARN("failed to set result", K(tmp_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObStartCompleteMigrationTask::change_member_list_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  DEBUG_SYNC(MEMBERLIST_CHANGE_MEMBER);
  const int64_t start_ts = ObTimeUtility::current_time();
  common::ObAddr leader_addr;
  share::SCN ls_transfer_scn;
  uint64_t cluster_version = 0;
  palf::LogConfigVersion fake_config_version;
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_SET_MEMBER_LIST_FAIL) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to change member list", K(ret), KP(ls));
  } else if (ObMigrationOpType::ADD_LS_OP != ctx_->arg_.type_
      && ObMigrationOpType::MIGRATE_LS_OP != ctx_->arg_.type_) {
    //do nothing
  } else if (FALSE_IT(cluster_version = GET_MIN_CLUSTER_VERSION())) {
  } else if (OB_FAIL(ObStorageHAUtils::get_ls_leader(ctx_->tenant_id_, ctx_->arg_.ls_id_, leader_addr))) {
    LOG_WARN("failed to get ls leader", K(ret), KPC(ctx_));
  } else if (OB_FAIL(fake_config_version.generate(0, 0))) {
    LOG_WARN("failed to generate config version", K(ret));
  } else if (cluster_version < CLUSTER_VERSION_4_2_0_0) {
    const int64_t change_member_list_timeout_us = GCONF.sys_bkgd_migration_change_member_list_timeout;
    if (ObMigrationOpType::ADD_LS_OP == ctx_->arg_.type_) {
      if (REPLICA_TYPE_FULL == ctx_->arg_.dst_.get_replica_type()) {
        if (OB_FAIL(ls->get_log_handler()->add_member(ctx_->arg_.dst_, ctx_->arg_.paxos_replica_number_,
            fake_config_version, change_member_list_timeout_us))) {
          LOG_WARN("failed to add member", K(ret), KPC(ctx_));
        }
      }
    } else if (ObMigrationOpType::MIGRATE_LS_OP == ctx_->arg_.type_) {
      if (REPLICA_TYPE_FULL == ctx_->arg_.dst_.get_replica_type()) {
        if (OB_FAIL(ls->get_log_handler()->replace_member(ctx_->arg_.dst_, ctx_->arg_.src_,
            fake_config_version, change_member_list_timeout_us))) {
          LOG_WARN("failed to repalce member", K(ret), KPC(ctx_));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("change member list get invalid type", K(ret), KPC(ctx_));
    }
  } else {
    const int64_t change_member_list_timeout_us = GCONF.sys_bkgd_migration_change_member_list_timeout;
    if (ObMigrationOpType::ADD_LS_OP == ctx_->arg_.type_) {
      if (REPLICA_TYPE_FULL == ctx_->arg_.dst_.get_replica_type()) {
        if (OB_FAIL(switch_learner_to_acceptor_(ls))) {
          LOG_WARN("failed to switch learner to acceptor", K(ret), K(leader_addr), K(ls_transfer_scn));
        }
      } else {
        // R-replica
        if (OB_FAIL(replace_learners_for_add_(ls))) {
          LOG_WARN("failed to replace learners for add", K(ret), K(leader_addr), K(ls_transfer_scn));
        }
      }
    } else if (ObMigrationOpType::MIGRATE_LS_OP == ctx_->arg_.type_) {
      if (REPLICA_TYPE_FULL == ctx_->arg_.dst_.get_replica_type()) {
        if (OB_FAIL(replace_member_with_learner_(ls))) {
          LOG_WARN("failed to replace member with learner", K(ret), K(leader_addr), K(ls_transfer_scn));
        }
      } else {
        // R-replica
        if (OB_FAIL(replace_learners_for_migration_(ls))) {
          LOG_WARN("failed to replace learners for migration", K(ret), K(leader_addr), K(ls_transfer_scn));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("change member list get invalid type", K(ret), KPC(ctx_));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    LOG_INFO("succeed change member list", "cost", cost_ts, "tenant_id", ctx_->tenant_id_, "ls_id", ctx_->arg_.ls_id_);
  }

  DEBUG_SYNC(AFTER_MEMBERLIST_CHANGED);
  return ret;
}

int ObStartCompleteMigrationTask::get_ls_transfer_scn_(ObLS *ls, share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret));
  } else if (OB_FAIL(ls->get_transfer_scn(transfer_scn))) {
    LOG_WARN("failed to get transfer scn", K(ret), KP(ls));
  }
  return ret;
}

int ObStartCompleteMigrationTask::switch_learner_to_acceptor_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
  ObMember dst = ctx_->arg_.dst_;
  dst.set_migrating();
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_FAIL(ls->switch_learner_to_acceptor(dst, ctx_->arg_.paxos_replica_number_, timeout))) {
    LOG_WARN("failed to switch_learner_to_acceptor", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObStartCompleteMigrationTask::replace_member_with_learner_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
  ObMember dst = ctx_->arg_.dst_;
  dst.set_migrating();
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_FAIL(ls->replace_member_with_learner(dst, ctx_->arg_.src_, timeout))) {
    LOG_WARN("failed to replace_member_with_learner", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObStartCompleteMigrationTask::replace_learners_for_add_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObMemberList added_learners, removed_learners;
  ObMember new_dst = ctx_->arg_.dst_;
  new_dst.reset_migrating();
  ObMember old_dst = ctx_->arg_.dst_;
  old_dst.set_migrating();
  const int64_t change_member_list_timeout_us = GCONF.sys_bkgd_migration_change_member_list_timeout;
  if (OB_FAIL(added_learners.add_member(new_dst))) {
    LOG_WARN("failed to add member", K(ret), KPC(ctx_));
  } else if (OB_FAIL(removed_learners.add_member(old_dst))) {
    LOG_WARN("failed to add member", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ls->replace_learners(added_learners, removed_learners, change_member_list_timeout_us))) {
    LOG_WARN("failed to replace learners", K(ret), KPC(ctx_));
  } else {
    LOG_INFO("replace learners for add", K(added_learners), K(removed_learners));
  }
  return ret;
}

int ObStartCompleteMigrationTask::replace_learners_for_migration_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObMemberList added_learners, removed_learners;
  const int64_t change_member_list_timeout_us = GCONF.sys_bkgd_migration_change_member_list_timeout;
  ObMember new_dst = ctx_->arg_.dst_;
  new_dst.reset_migrating();
  ObMember old_dst = ctx_->arg_.dst_;
  old_dst.set_migrating();
  ObMember src = ctx_->arg_.src_;
  src.reset_migrating();
  if (OB_FAIL(added_learners.add_member(new_dst))) {
    LOG_WARN("failed to add member", K(ret), KPC(ctx_));
  } else if (OB_FAIL(removed_learners.add_member(old_dst))) {
    LOG_WARN("failed to add member", K(ret), KPC(ctx_));
  } else if (OB_FAIL(removed_learners.add_member(src))) {
    LOG_WARN("failed to add member", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ls->replace_learners(added_learners, removed_learners, change_member_list_timeout_us))) {
    LOG_WARN("failed to replace learners", K(ret), KPC(ctx_));
  } else {
    LOG_INFO("replace members for migration", K(added_learners), K(removed_learners));
  }
  return ret;
}

int ObStartCompleteMigrationTask::check_need_wait_(
    ObLS *ls,
    bool &need_wait)
{
  int ret = OB_SUCCESS;
  need_wait = true;
  ObLSRestoreStatus ls_restore_status;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check need wait log sync get invalid argument", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_restore_status(ls_restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), KPC(ctx_));
  } else if (ls_restore_status.is_in_restore()) {
    need_wait = false;
  } else if (ObMigrationOpType::REBUILD_LS_OP == ctx_->arg_.type_) {
    need_wait = false;
  } else if (ObMigrationOpType::ADD_LS_OP == ctx_->arg_.type_
      || ObMigrationOpType::MIGRATE_LS_OP == ctx_->arg_.type_) {
    need_wait = true;
  } else if (ObMigrationOpType::CHANGE_LS_OP == ctx_->arg_.type_) {
    // TODO: make sure this is right
    if (!ObReplicaTypeCheck::is_replica_with_ssstore(REPLICA_TYPE_FULL)
        && ObReplicaTypeCheck::is_full_replica(ctx_->arg_.dst_.get_replica_type())) {
      need_wait = true;
    }
  }
  return ret;
}

int ObStartCompleteMigrationTask::check_need_wait_transfer_table_replace_(
    ObLS *ls,
    bool &need_wait)
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus ls_restore_status;
  need_wait = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check need wait log sync get invalid argument", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_restore_status(ls_restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), KPC(ctx_));
  } else if (ls_restore_status.is_before_restore_to_consistent_scn()) {
    need_wait = false;
  }
  return ret;
}

int ObStartCompleteMigrationTask::update_ls_migration_status_wait_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObMigrationStatus wait_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (ObMigrationOpType::MIGRATE_LS_OP != ctx_->arg_.type_
      && ObMigrationOpType::ADD_LS_OP != ctx_->arg_.type_
      && ObMigrationOpType::REBUILD_LS_OP != ctx_->arg_.type_) {
    ret = OB_ERR_SYS;
    LOG_WARN("no other type should update migration status wait", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ObMigrationOpType::get_ls_wait_status(ctx_->arg_.type_, wait_status))) {
    LOG_WARN("failed to get ls wait status", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to change member list", K(ret), KP(ls));
  } else if (OB_FAIL(ls->set_migration_status(wait_status, ctx_->rebuild_seq_))) {
    LOG_WARN("failed to set migration status", K(ret), KPC(ls));
  } else {
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("storage_ha", "update_ls_migration_status_wait",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_);
#endif
  }
  return ret;
}

int ObStartCompleteMigrationTask::update_ls_migration_status_hold_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  const ObMigrationStatus hold_status = ObMigrationStatus::OB_MIGRATION_STATUS_HOLD;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (ObMigrationOpType::REBUILD_LS_OP == ctx_->arg_.type_) {
    //do nothing
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to change member list", K(ret), KP(ls));
  } else if (OB_FAIL(ls->set_migration_status(hold_status, ctx_->rebuild_seq_))) {
    LOG_WARN("failed to set migration status", K(ret), KPC(ls));
  } else {
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("storage_ha", "update_ls_migration_status_hold",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_);
#endif
    DEBUG_SYNC(AFTER_CHANGE_MIGRATION_STATUS_HOLD);
  }
  return ret;
}

int ObStartCompleteMigrationTask::check_all_tablet_ready_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  const int64_t check_all_tablet_start_ts = ObTimeUtility::current_time();
  const bool need_initial_state = false;
  ObTimeoutCtx timeout_ctx;
  int64_t timeout = 10_min;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to change member list", K(ret), KP(ls));
  } else if (OB_FAIL(get_wait_timeout_(timeout))) {
    LOG_WARN("failed to get wait timeout", K(ret));
  } else if (OB_FAIL(init_timeout_ctx_(timeout, timeout_ctx))) {
    LOG_WARN("failed to init timeout ctx", K(ret));
  } else {
    ObHALSTabletIDIterator iter(ls->get_ls_id(), need_initial_state);
    ObTabletID tablet_id;
    if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(iter))) {
      LOG_WARN("failed to build tablet iter", K(ret), KPC(ctx_));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter.get_next_tablet_id(tablet_id))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get tablet id", K(ret));
          }
        } else if (OB_FAIL(check_tablet_ready_(tablet_id, ls, timeout))) {
          LOG_WARN("failed to check tablet ready", K(ret), K(tablet_id), KPC(ls));
        }
      }
      LOG_INFO("check all tablet ready finish", K(ret), "ls_id", ctx_->arg_.ls_id_,
          "cost ts", ObTimeUtility::current_time() - check_all_tablet_start_ts);
    }
  }
  return ret;
}

int ObStartCompleteMigrationTask::check_tablet_ready_(
    const common::ObTabletID &tablet_id,
    ObLS *ls,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const ObMDSGetTabletMode read_mode = ObMDSGetTabletMode::READ_WITHOUT_CHECK;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (!tablet_id.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check tablet ready get invalid argument", K(ret), K(tablet_id), KP(ls));
  } else {
    const int64_t wait_tablet_start_ts = ObTimeUtility::current_time();

    while (OB_SUCC(ret)) {
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;

      if (OB_FAIL(check_ls_and_task_status_(ls))) {
        LOG_WARN("failed to check ls and task status", K(ret), KPC(ctx_));
      } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle, 0, read_mode))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_handle), K(tablet_id));
      } else if (tablet->is_empty_shell()) {
        max_minor_end_scn_ = MAX(max_minor_end_scn_, tablet->get_tablet_meta().get_max_replayed_scn());
        break;
      } else if (tablet->get_tablet_meta().ha_status_.is_data_status_complete()) {
        ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
        if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
          LOG_WARN("fail to fetch table store", K(ret));
        } else {
          const ObSSTableArray &minor_sstables = table_store_wrapper.get_member()->get_minor_sstables();
          if (minor_sstables.empty()) {
            max_minor_end_scn_ = MAX(max_minor_end_scn_, tablet->get_tablet_meta().get_max_replayed_scn());
          } else {
            max_minor_end_scn_ = MAX3(max_minor_end_scn_,
                                     minor_sstables.get_boundary_table(true)->get_end_scn(),
                                     tablet->get_tablet_meta().get_max_replayed_scn());
          }
        }
        break;
      } else {
        const int64_t current_ts = ObTimeUtility::current_time();
        if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("tablet not ready, retry next loop", "arg", ctx_->arg_, "tablet_id", tablet_id,
              "ha_status", tablet->get_tablet_meta().ha_status_,
              "wait_tablet_start_ts", wait_tablet_start_ts,
              "current_ts", current_ts);
        }

        if (current_ts - wait_tablet_start_ts < timeout) {
        } else {
          if (OB_FAIL(ctx_->set_result(OB_WAIT_TABLET_READY_TIMEOUT, true /*allow_retry*/,
              this->get_dag()->get_type()))) {
            LOG_WARN("failed to set result", K(ret), KPC(ctx_));
          } else {
            ret = OB_WAIT_TABLET_READY_TIMEOUT;
            STORAGE_LOG(WARN, "failed to check tablet ready, timeout, stop migration task",
                K(ret), K(*ctx_), KPC(tablet), K(current_ts),
                K(wait_tablet_start_ts));
          }
        }

        if (OB_SUCC(ret)) {
          ob_usleep(CHECK_CONDITION_INTERVAL);
        }
      }
    }
  }
  return ret;
}

int ObStartCompleteMigrationTask::check_tablet_transfer_table_ready_(
    const common::ObTabletID &tablet_id,
    ObLS *ls,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObTransferService *transfer_service = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (!tablet_id.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check tablet ready get invalid argument", K(ret), K(tablet_id), KP(ls));
  } else if (OB_ISNULL(transfer_service = MTL(ObTransferService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer service should not be NULL", K(ret), KP(transfer_service));
  } else {
    const int64_t wait_tablet_start_ts = ObTimeUtility::current_time();
    bool need_check_again = false;

    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_ls_and_task_status_(ls))) {
        LOG_WARN("failed to check ls and task status", K(ret), KPC(ctx_));
      } else if (OB_FAIL(inner_check_tablet_transfer_table_ready_(tablet_id, ls, need_check_again))) {
        LOG_WARN("failed to inner check tablet transfer table ready", K(ret), K(tablet_id), KP(ls));
      } else if (!need_check_again) {
        break;
      } else {
        LOG_INFO("tablet has transfer table", K(ret), K(tablet_id), "ls_id", ls->get_ls_id());
        const int64_t current_ts = ObTimeUtility::current_time();
        transfer_service->wakeup();
        if (current_ts - wait_tablet_start_ts < timeout) {
        } else {
          if (OB_FAIL(ctx_->set_result(OB_WAIT_TABLET_READY_TIMEOUT, true /*allow_retry*/,
              this->get_dag()->get_type()))) {
            LOG_WARN("failed to set result", K(ret), KPC(ctx_));
          } else {
            ret = OB_WAIT_TABLET_READY_TIMEOUT;
            STORAGE_LOG(WARN, "failed to check tablet ready, timeout, stop migration task",
                K(ret), K(*ctx_), K(tablet_id), KPC(ls), K(current_ts), K(wait_tablet_start_ts));
          }
        }

        if (OB_SUCC(ret)) {
          ob_usleep(CHECK_CONDITION_INTERVAL);
        }
      }
    }
  }
  return ret;
}

int ObStartCompleteMigrationTask::inner_check_tablet_transfer_table_ready_(
    const common::ObTabletID &tablet_id,
    ObLS *ls,
    bool &need_check_again)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  need_check_again = true;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  const ObMDSGetTabletMode read_mode = ObMDSGetTabletMode::READ_WITHOUT_CHECK;
  bool can_skip = true;
  ObTabletCreateDeleteMdsUserData user_data;
  ObLSService *ls_service = nullptr;
  ObLSHandle src_ls_handle;
  ObLS *src_ls = nullptr;
  SCN scn;
  ObTabletHandle src_tablet_handle;

  if (!tablet_id.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id), KP(ls));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      need_check_again = false;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_handle), K(tablet_id));
  } else if (tablet->is_empty_shell()) {
    need_check_again = false;
  } else if (!tablet->get_tablet_meta().has_transfer_table()) {
    LOG_INFO("tablet do not has transfer table", K(ret), K(tablet_id), "ls_id", ls->get_ls_id());
    need_check_again = false;
  } else if (OB_FAIL(ls_service->get_ls(tablet->get_tablet_meta().transfer_info_.ls_id_, src_ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get transfer src ls", K(ret), KPC(tablet));
    if (OB_LS_NOT_EXIST == ret) {
      if (OB_SUCCESS != (tmp_ret = ctx_->set_result(OB_TRANSFER_SRC_LS_NOT_EXIST, true /*allow_retry*/))) {
        LOG_WARN("failed to set result", K(ret), K(tmp_ret), KPC(ctx_));
      }
    }
  } else if (OB_ISNULL(src_ls = src_ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src ls should not be NULL", K(ret), KPC(tablet), KP(src_ls));
  } else if (OB_FAIL(src_ls->get_max_decided_scn(scn))) {
    if (OB_STATE_NOT_MATCH == ret) {
      //src ls not ready, need check again
      need_check_again = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get max decided scn", K(ret), KPC(src_ls));
    }
  } else if (scn <= tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
    need_check_again = true;
    if (REACH_TENANT_TIME_INTERVAL(5 * 1000 * 1000/*5s*/)) {
      LOG_INFO("need to wait for the max_decided_scn to be greater than transfer_start_scn", K(tablet_id), K(scn),
          "transfer_start_scn", tablet->get_tablet_meta().transfer_info_.transfer_start_scn_,
          "ls_id", tablet->get_tablet_meta().transfer_info_.ls_id_);
    }
  } else if (OB_FAIL(src_ls->ha_get_tablet(tablet_id, src_tablet_handle))) {
    LOG_WARN("failed to get transfer src tablet", K(ret), K(tablet_id));
    if (OB_TABLET_NOT_EXIST == ret) {
      if (OB_SUCCESS != (tmp_ret = ctx_->set_result(OB_TRANSFER_SRC_TABLET_NOT_EXIST, true /*allow_retry*/))) {
        LOG_WARN("failed to set result", K(ret), K(tmp_ret), KPC(ctx_));
      }
    }
  } else {
    need_check_again = true;
#ifdef ERRSIM
    if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000)) {
      SERVER_EVENT_ADD("TRANSFER", "still_has_transfer_table",
                      "ls_id", ls->get_ls_id(),
                      "tablet_id", tablet_id.id(),
                      "has_transfer_table", tablet->get_tablet_meta().has_transfer_table());
    }
#endif
  }
  return ret;
}

int ObStartCompleteMigrationTask::wait_log_replay_to_max_minor_end_scn_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  bool need_wait = true;
  SCN current_replay_scn = share::SCN::min_scn();
  ObTimeoutCtx timeout_ctx;
  int64_t timeout = 10_min;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start complete migration task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), KPC(ctx_));
  } else if (OB_FAIL(check_need_wait_(ls, need_wait))) {
    LOG_WARN("failed to check need replay to max minor end scn", K(ret), KPC(ls), KPC(ctx_));
  } else if (!need_wait) {
    LOG_INFO("no need to wait ls checkpoint ts push", K(ret), KPC(ctx_));
  } else if (OB_FAIL(get_wait_timeout_(timeout))) {
    LOG_WARN("failed to get wait timeout", K(ret));
  } else if (OB_FAIL(init_timeout_ctx_(timeout, timeout_ctx))) {
    LOG_WARN("failed to init timeout ctx", K(ret));
  } else {
    const int64_t wait_replay_start_ts = ObTimeUtility::current_time();
    while (OB_SUCC(ret)) {
      if (timeout_ctx.is_timeouted()) {
        ret = OB_WAIT_REPLAY_TIMEOUT;
        LOG_WARN("already timeout", K(ret), KPC(ctx_));
        break;
      } else if (OB_FAIL(check_ls_and_task_status_(ls))) {
        LOG_WARN("failed to check ls and task status", K(ret), KPC(ctx_));
      } else if (OB_FAIL(ls->get_max_decided_scn(current_replay_scn))) {
        LOG_WARN("failed to get current replay log ts", K(ret), KPC(ctx_));
      } else if (current_replay_scn >= max_minor_end_scn_) {
        const int64_t cost_ts = ObTimeUtility::current_time() - wait_replay_start_ts;
        LOG_INFO("wait replay log ts push to max minor end scn success, stop wait", "arg", ctx_->arg_,
            K(cost_ts), K(max_minor_end_scn_), K(current_replay_scn));
        break;
      } else {
        const int64_t current_ts = ObTimeUtility::current_time();
        if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("ls wait replay to max minor sstable end log ts, retry next loop", "arg", ctx_->arg_,
              "wait_replay_start_ts", wait_replay_start_ts,
              "current_ts", current_ts,
              "max_minor_end_scn", max_minor_end_scn_,
              "current_replay_scn", current_replay_scn);
        }

        if (current_ts - wait_replay_start_ts < timeout) {
        } else {
          if (OB_FAIL(ctx_->set_result(OB_WAIT_REPLAY_TIMEOUT, true /*allow_retry*/,
              this->get_dag()->get_type()))) {
            LOG_WARN("failed to set result", K(ret), KPC(ctx_));
          } else {
            ret = OB_WAIT_REPLAY_TIMEOUT;
            STORAGE_LOG(WARN, "failed to wait replay to max minor end scn, timeout, stop migration task",
                K(ret), K(*ctx_), K(current_ts), K(wait_replay_start_ts));
          }
        }

        if (OB_SUCC(ret)) {
          ob_usleep(CHECK_CONDITION_INTERVAL);
        }
      }
    }
  }
  return ret;
}

int ObStartCompleteMigrationTask::check_ls_and_task_status_(
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  bool is_cancel = false;
  bool is_ls_deleted = true;
  int32_t result = OB_SUCCESS;

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check ls and task status get invalid argument", K(ret), KP(ls));
  } else if (ctx_->is_failed()) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "ls migration task is failed", K(ret), KPC(ctx_));
  } else if (ls->is_stopped()) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls is not running, stop migration dag net", K(ret), KPC(ctx_));
  } else if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(get_dag()->get_dag_id(), is_cancel))) {
    STORAGE_LOG(ERROR, "failed to check is task canceled", K(ret), K(*this));
  } else if (is_cancel) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "task is cancelled", K(ret), K(*this));
  } else if (OB_FAIL(ObStorageHAUtils::check_ls_deleted(ls->get_ls_id(), is_ls_deleted))) {
    LOG_WARN("failed to get ls status from inner table", K(ret));
  } else if (is_ls_deleted) {
    ret = OB_CANCELED;
    LOG_WARN("ls will be removed, no need run migration", K(ret), KPC(ls), K(is_ls_deleted));
  } else if (OB_FAIL(ObStorageHAUtils::check_log_status(ls->get_tenant_id(), ls->get_ls_id(), result))) {
    LOG_WARN("failed to check log status", K(ret), KPC(ls));
  } else if (OB_SUCCESS != result) {
    LOG_INFO("can not replay log, it will retry", K(result));
    if (OB_FAIL(ctx_->set_result(result/*result*/, false/*need_retry*/, this->get_dag()->get_type()))) {
      LOG_WARN("failed to set result", K(ret), KPC(ctx_));
    } else {
      ret = result;
      LOG_WARN("log sync or replay error, need retry", K(ret), KPC(ls));
    }
  }
  return ret;
}

int ObStartCompleteMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("storage_ha", "start_complete_migration_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_,
        "is_failed", ctx_->is_failed(),
        ObMigrationOpType::get_str(ctx_->arg_.type_));
  }
  return ret;
}

int ObStartCompleteMigrationTask::init_timeout_ctx_(
    const int64_t timeout,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  if (timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(timeout))) {
    LOG_WARN("failed to set timeout", K(ret), K(timeout));
  }
  return ret;
}

/******************ObFinishCompleteMigrationDag*********************/
ObFinishCompleteMigrationDag::ObFinishCompleteMigrationDag()
  : ObCompleteMigrationDag(share::ObDagType::DAG_TYPE_FINISH_COMPLETE_MIGRATION),
    is_inited_(false)
{
}

ObFinishCompleteMigrationDag::~ObFinishCompleteMigrationDag()
{
}

int ObFinishCompleteMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *self_ctx = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial prepare migration dag do not init", K(ret));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
         "ObFinishCompleteMigrationDag: ls_id = %s, migration_type = %s, dag_prio = %s",
         to_cstring(self_ctx->arg_.ls_id_), ObMigrationOpType::get_str(self_ctx->arg_.type_),
         ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), K(*self_ctx));
  }
  return ret;
}

int ObFinishCompleteMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish complete migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init finish complete migration dag get invalid argument", K(ret), KP(dag_net));
  } else if (OB_FAIL(prepare_ctx(dag_net))) {
    LOG_WARN("failed to prepare ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObFinishCompleteMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObFinishCompleteMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish complete migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init finish complete migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObFinishCompleteMigrationTask*********************/
ObFinishCompleteMigrationTask::ObFinishCompleteMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObFinishCompleteMigrationTask::~ObFinishCompleteMigrationTask()
{
}

int ObFinishCompleteMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSCompleteMigrationDagNet *complete_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish complete migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(complete_dag_net = static_cast<ObLSCompleteMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = complete_dag_net->get_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init finish complete migration task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObFinishCompleteMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("start do finish complete migration task", KPC(ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish complete migration task do not init", K(ret));
  } else {
    if (ctx_->is_failed()) {
      bool allow_retry = false;
      if (OB_FAIL(ctx_->check_allow_retry(allow_retry))) {
        LOG_ERROR("failed to check need retry", K(ret), K(*ctx_));
      } else if (allow_retry) {
        ctx_->reuse();
        if (OB_FAIL(generate_prepare_initial_dag_())) {
          LOG_WARN("failed to generate prepare initial dag", K(ret), KPC(ctx_));
        }
      }
    }
    if (OB_FAIL(ret)) {
      const bool need_retry = false;
      if (OB_SUCCESS != (tmp_ret = ctx_->set_result(ret, need_retry,
          this->get_dag()->get_type()))) {
        LOG_WARN("failed to set result", K(ret), K(ret), K(tmp_ret), KPC(ctx_));
      }
    }
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObFinishCompleteMigrationTask::generate_prepare_initial_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialCompleteMigrationDag *initial_complete_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObFinishCompleteMigrationDag *finish_complete_migration_dag = nullptr;
  ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish complete migration task do not init", K(ret));
  } else if (OB_ISNULL(finish_complete_migration_dag = static_cast<ObFinishCompleteMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish complete migration dag should not be NULL", K(ret), KP(finish_complete_migration_dag));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(ObMigrationUtils::get_dag_priority(ctx_->arg_.type_, prio))) {
    LOG_WARN("failed to get dag priority", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, initial_complete_dag))) {
      LOG_WARN("failed to alloc initial complete migration dag ", K(ret));
    } else if (OB_FAIL(initial_complete_dag->init(dag_net_))) {
      LOG_WARN("failed to init initial complete migration dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*initial_complete_dag))) {
      LOG_WARN("failed to add initial complete dag as child", K(ret), KPC(initial_complete_dag));
    } else if (OB_FAIL(initial_complete_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(initial_complete_dag))) {
      LOG_WARN("failed to add initial complete migration dag", K(ret), K(*initial_complete_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("start create initial complete migration dag", K(ret), K(*ctx_));
      initial_complete_dag = nullptr;
    }

    if (OB_NOT_NULL(initial_complete_dag) && OB_NOT_NULL(scheduler)) {
      scheduler->free_dag(*initial_complete_dag);
      initial_complete_dag = nullptr;
    }
  }
  return ret;
}

int ObFinishCompleteMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("storage_ha", "finish_complete_migration_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_,
        "is_failed", ctx_->is_failed(),
        ObMigrationOpType::get_str(ctx_->arg_.type_));
  }
  return ret;
}

