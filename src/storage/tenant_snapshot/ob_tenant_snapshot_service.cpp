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
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_defs.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_defs.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_service.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_task.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_meta_table.h"
#include "lib/lock/mutex.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ObTenantSnapshotService::ObTenantSnapshotService()
  : is_inited_(false),
    is_running_(false),
    meta_loaded_(false),
    unit_is_deleting_(false),
    tenant_snapshot_mgr_(),
    ls_snapshot_mgr_(),
    meta_handler_(),
    cond_(),
    tg_id_(INT32_MAX),
    running_mode_(RUNNING_MODE::INVALID),
    clone_service_()
{}

ObTenantSnapshotService::~ObTenantSnapshotService() { }

int ObTenantSnapshotService::mtl_init(ObTenantSnapshotService* &service)
{
  return service->init();
}

int ObTenantSnapshotService::init()
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = MTL_ID();

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantSnapshotService is already inited", KR(ret), KPC(this));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("virtual tenant do not need ObTenantSnapshotService");
  } else if (OB_FAIL(clone_service_.init(&meta_handler_))) {
    LOG_WARN("fail to init clone service", KR(ret), KPC(this));
  } else if (OB_FAIL(ls_snapshot_mgr_.init(&meta_handler_))) {
    LOG_WARN("fail to init ls snapshot mgr", KR(ret), KPC(this));
  } else if (OB_FAIL(tenant_snapshot_mgr_.init(&ls_snapshot_mgr_, &meta_handler_))) {
    LOG_WARN("fail to init tenant snapshot mgr", KR(ret), KPC(this));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::TENANT_SNAPSHOT_SERVICE_COND_WAIT))) {
    LOG_WARN("fail to init TENANT_SNAPSHOT_SERVICE_COND_WAIT", KR(ret), KPC(this));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TSnapSvc, tg_id_))) {
    LOG_ERROR("fail to create tenant snapshot service thread", KR(ret), KPC(this));
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    LOG_ERROR("fail to set tenant snapshot service thread runnable", KR(ret), KPC(this));
  } else {
    meta_loaded_ = false;
    running_mode_ = RUNNING_MODE::INVALID;
    is_inited_ = true;
    unit_is_deleting_ = false;
  }

  return ret;
}

void ObTenantSnapshotService::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    LOG_INFO("ObTenantSnapshotService::destroy start");

    if (is_running_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ObTenantSnapshotService is still running when destroy function called",
          KR(ret), KPC(this));
    }
    if (OB_SUCC(ret)) {
      TG_DESTROY(tg_id_);
      tg_id_ = INT32_MAX;
      clone_service_.destroy();
      tenant_snapshot_mgr_.destroy();
      ls_snapshot_mgr_.destroy();
      meta_loaded_ = false;
      unit_is_deleting_ = false;
      cond_.destroy();
      running_mode_ = RUNNING_MODE::INVALID;
      is_inited_ = false;
    }
    LOG_INFO("ObTenantSnapshotService::destroy end", KR(ret));
  }
}

void ObTenantSnapshotService::stop()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObTenantSnapshotService not inited, cannot stop", KR(ret), KPC(this));
  } else if (!ATOMIC_LOAD(&is_running_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObTenantSnapshotService already has stopped", KR(ret), KPC(this));
  } else {
    TG_STOP(tg_id_);
    tenant_snapshot_mgr_.stop();
    is_running_ = false;
    {
      ObThreadCondGuard guard(cond_);
      cond_.signal();
    }
    LOG_INFO("ObTenantSnapshotService stopped", KR(ret), KPC(this));
  }
}

void ObTenantSnapshotService::wait()
{
  int ret = OB_SUCCESS;
  while(OB_FAIL(wait_())) {
    usleep(100000);
  }
}

int ObTenantSnapshotService::wait_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    LOG_INFO("ObTenantSnapshotService not inited", KPC(this));
  } else if (ATOMIC_LOAD(&is_running_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObTenantSnapshotService is running when wait function is called", KR(ret), KPC(this));
    stop();
  } else {
    TG_WAIT(tg_id_);
  }
  return ret;
}

int ObTenantSnapshotService::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObTenantSnapshotService is not inited, cannot start", KR(ret), KPC(this));
  } else if (ATOMIC_LOAD(&is_running_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantSnapshotService is already running", KR(ret), KPC(this));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_ERROR("fail to start ObTenantSnapshotService thread", KR(ret), KPC(this));
  } else {
    is_running_ = true;
    LOG_INFO("ObTenantSnapshotService start successfully", KPC(this));
  }
  return ret;
}

int ObTenantSnapshotService::load_()
{
  int ret = OB_SUCCESS;

  ObArray<ObTenantSnapshotID> tsnap_id_arr;
  if (OB_FAIL(meta_handler_.get_all_tenant_snapshot(tsnap_id_arr))) {
    LOG_WARN("fail to get_all_tenant_snapshot", KR(ret), KPC(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tsnap_id_arr.count(); ++i) {
      const ObTenantSnapshotID tenant_snapshot_id = tsnap_id_arr.at(i);
      ObTenantSnapshot *tenant_snapshot = nullptr;
      if (OB_FAIL(tenant_snapshot_mgr_.acquire_tenant_snapshot(tenant_snapshot_id,
                                                               tenant_snapshot))) {
        LOG_WARN("fail to acquire tenant snapshot", KR(ret), K(tenant_snapshot_id));
      } else {
        if (OB_FAIL(tenant_snapshot->load())) {
          LOG_WARN("fail to load tenant snapshot", KR(ret), K(tenant_snapshot_id));
        } else {
          LOG_INFO("tenant snapshot load succ", K(tenant_snapshot_id), KPC(tenant_snapshot));
        }
        tenant_snapshot_mgr_.revert_tenant_snapshot(tenant_snapshot);
      }
    }
  }

  LOG_INFO("all tenant snapshot load finished", KR(ret));
  return ret;
}

int ObTenantSnapshotService::get_tenant_status_(ObTenantStatus& tenant_status)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *tenant_schema = NULL;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is unexpected nullptr", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(MTL_ID(), tenant_schema))) {
    LOG_WARN("fail to get tenant info", KR(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret));
  } else {
    tenant_status = tenant_schema->get_status();
  }
  return ret;
}

int ObTenantSnapshotService::get_tenant_role_(ObTenantRole& tenant_role)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(MTL_ID(),
                                                     GCTX.sql_proxy_,
                                                     false /*for_update*/,
                                                     tenant_info))) {
    LOG_WARN("fail to load tenant info", KR(ret));
  } else {
    tenant_role = tenant_info.get_tenant_role();
  }

  return ret;
}

int ObTenantSnapshotService::common_env_check_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotService has not been inited", KR(ret), KPC(this));
  } else if (!ATOMIC_LOAD(&is_running_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObTenantSnapshotService is not running", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!ObServerCheckpointSlogHandler::get_instance().is_started())) {
    ret = OB_NOT_RUNNING;
    LOG_INFO("ObTenantSnapshotService does not work before server slog replay finished",
        KR(ret), KPC(this));
  }

  return ret;
}

int ObTenantSnapshotService::normal_running_env_check_()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  if (ATOMIC_LOAD(&running_mode_) != NORMAL) {
    ret = OB_STATE_NOT_MATCH;
    LOG_INFO("the running mode is not NORMAL", KR(ret), KPC(this));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get_min_data_version failed", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_INFO("ObTenantSnapshotService does not work before data version upgrade to 4_3_0_0",
        KR(ret), KPC(this), K(data_version));
  } else {
    LOG_INFO("normal_running_env_check_ succ", KPC(this));
  }

  return ret;
}

int ObTenantSnapshotService::clone_running_env_check_()
{
  int ret = OB_SUCCESS;

  if (ATOMIC_LOAD(&running_mode_) != CLONE) {
    ret = OB_STATE_NOT_MATCH;
    LOG_INFO("the running mode is not CLONE", KR(ret), KPC(this));
  } else {
    LOG_INFO("clone_running_env_check_ succ", KPC(this));
  }
  return ret;
}

int ObTenantSnapshotService::check_if_tenant_has_been_dropped_(bool &has_dropped)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();

  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  has_dropped = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(tenant_id, has_dropped))) {
    LOG_WARN("fail to check if tenant has been dropped", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantSnapshotService::decide_running_mode_(enum RUNNING_MODE& running_mode)
{
  int ret = OB_SUCCESS;

  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *tenant_schema = NULL;
  ObTenantStatus tenant_status = TENANT_STATUS_MAX;

  bool has_dropped = false;
  if (unit_is_deleting_) {
    running_mode = GC;
  } else if (OB_FAIL(check_if_tenant_has_been_dropped_(has_dropped))) {
    LOG_WARN("fail to check_if_tenant_has_been_dropped_", KR(ret));
  } else if (has_dropped) {
    running_mode = GC;
  } else if (OB_FAIL(get_tenant_status_(tenant_status))) {
    LOG_WARN("fail to get_tenant_status_", KR(ret));
  } else if (TENANT_STATUS_NORMAL == tenant_status) {
    running_mode = NORMAL;
  } else if (TENANT_STATUS_RESTORE == tenant_status) {
    ObTenantRole tenant_role;
    if (OB_FAIL(get_tenant_role_(tenant_role))) {
      LOG_WARN("fail to get_tenant_role", KR(ret));
    } else if (tenant_role.is_clone()) {
      running_mode = CLONE;
    } else if (tenant_role.is_restore()) {
      running_mode = RESTORE;
    } else {
      ret = OB_NOT_RUNNING;
      LOG_WARN("ObTenantSnapshotService does not work", KR(ret), K(tenant_status), K(tenant_role));
    }
  } else {
    ret = OB_NOT_RUNNING;
    LOG_INFO("ObTenantSnapshotService does not work in current tenant status",
        KR(ret), K(tenant_status));
  }
  return ret;
}

int ObTenantSnapshotService::create_tenant_snapshot(const obrpc::ObInnerCreateTenantSnapshotArg &arg)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotID tenant_snapshot_id;

  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObInnerCreateTenantSnapshotArg is not valid", KR(ret), K(arg));
  } else if (FALSE_IT(tenant_snapshot_id = arg.get_tenant_snapshot_id())) {
  } else if (OB_FAIL(common_env_check_())) {
    LOG_WARN("fail to common_env_check_", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(normal_running_env_check_())) {
    LOG_WARN("fail to normal_running_env_check_", KR(ret), K(tenant_snapshot_id));
  } else if (!ATOMIC_LOAD(&meta_loaded_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("storage node cannot process create tenant snapshot request before meta loaded",
        KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(try_create_tenant_snapshot_(tenant_snapshot_id))) {
    LOG_WARN("fail to try_create_tenant_snapshot_", KR(ret), K(tenant_snapshot_id));
  }

  LOG_INFO("execute create_tenant_snapshot finished", KR(ret), K(arg));
  return ret;
}

int ObTenantSnapshotService::try_create_tenant_snapshot_(const ObTenantSnapshotID& tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshot *tenant_snapshot = nullptr;
  ObArray<ObLSID> creating_ls_id_arr;
  common::ObCurTraceId::TraceId trace_id;

  if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(tenant_snapshot_mgr_.acquire_tenant_snapshot(tenant_snapshot_id,
                                                                  tenant_snapshot))) {
    if (OB_ENTRY_EXIST == ret) {
      LOG_INFO("some concurrent request is processed", KR(ret), K(tenant_snapshot_id));
    } else {
      LOG_WARN("fail to acquire tenant snapshot", KR(ret), K(tenant_snapshot_id));
    }
  } else if (OB_FAIL(tenant_snapshot->try_start_create_tenant_snapshot_dag(creating_ls_id_arr,
                                                                           trace_id))) {
    if (OB_NO_NEED_UPDATE == ret || OB_EAGAIN == ret) {
      LOG_INFO("fail to start_create_tenant_snapshot_dag", KR(ret), KPC(tenant_snapshot));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to start_create_tenant_snapshot_dag", KR(ret), KPC(tenant_snapshot));
    }
  } else {
    ObTraceIDGuard trace_guard(trace_id);
    if (OB_FAIL(schedule_create_tenant_snapshot_dag_(tenant_snapshot_id,
                                                     creating_ls_id_arr,
                                                     trace_id))) {
      LOG_WARN("fail to schedule create tenant snapshot dag", KR(ret), K(tenant_snapshot_id));
      tenant_snapshot->finish_create_tenant_snapshot_dag();
    } else {
      LOG_INFO("schedule create tenant snapshot dag success", K(tenant_snapshot_id));
    }
  }

  if (tenant_snapshot != nullptr) {
    tenant_snapshot_mgr_.revert_tenant_snapshot(tenant_snapshot);
  }

  return ret;
}

int ObTenantSnapshotService::drop_tenant_snapshot(const obrpc::ObInnerDropTenantSnapshotArg &arg)
{
  int ret = OB_SUCCESS;

  ObTenantSnapshot *tenant_snapshot = nullptr;
  ObTenantSnapshotID tenant_snapshot_id;

  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObInnerDropTenantSnapshotArg is not valid", KR(ret), K(arg));
  } else if (FALSE_IT(tenant_snapshot_id = arg.get_tenant_snapshot_id())) {
  } else if (OB_FAIL(common_env_check_())) {
    LOG_WARN("failed to common_env_check_", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(normal_running_env_check_())) {
    LOG_WARN("failed to normal_env_check_", KR(ret), K(tenant_snapshot_id));
  } else if (!ATOMIC_LOAD(&meta_loaded_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("observer cannot process drop tenant snapshot request before load meta finished",
        KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(tenant_snapshot_mgr_.get_tenant_snapshot(tenant_snapshot_id, tenant_snapshot))){
    if (OB_TENANT_SNAPSHOT_NOT_EXIST == ret) {
      LOG_INFO("tenant snapshot already not existed", KR(ret), K(arg));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant_snapshot", KR(ret), K(arg));
    }
  } else {
    tenant_snapshot->stop();
    {
      ObThreadCondGuard guard(cond_);
      cond_.signal();
    }
    tenant_snapshot_mgr_.revert_tenant_snapshot(tenant_snapshot);
  }

  LOG_INFO("exec drop_tenant_snapshot finished", KR(ret), K(arg));
  return ret;
}

int ObTenantSnapshotService::schedule_create_tenant_snapshot_dag_(const ObTenantSnapshotID& tenant_snapshot_id,
                                                                  const ObArray<ObLSID>& creating_ls_id_arr,
                                                                  const common::ObCurTraceId::TraceId& trace_id)
{
  int ret = OB_SUCCESS;

  ObTenantDagScheduler* dag_scheduler = MTL(ObTenantDagScheduler*);

  if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (creating_ls_id_arr.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("creating_ls_id_arr is empty", KR(ret), K(tenant_snapshot_id));
  } else if (!trace_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trace_id is not valid", KR(ret), K(tenant_snapshot_id), K(trace_id));
  } else if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag_scheduler is nullptr", KR(ret));
  } else {
    ObTenantSnapshotCreateParam param(tenant_snapshot_id, creating_ls_id_arr, trace_id, &tenant_snapshot_mgr_);
    if (OB_FAIL(dag_scheduler->create_and_add_dag<ObTenantSnapshotCreateDag>(&param))) {
      LOG_WARN("fail to create ObTenantSnapshotCreateDag", KR(ret), K(param));
    } else {
      LOG_INFO("schedule ObTenantSnapshotCreateDag success", K(param));
    }
  }
  return ret;
}

int ObTenantSnapshotService::schedule_gc_tenant_snapshot_dag_(const ObTenantSnapshotID &tenant_snapshot_id,
                                                              const ObArray<ObLSID> &gc_ls_id_arr,
                                                              const bool gc_tenant_snapshot,
                                                              const common::ObCurTraceId::TraceId& trace_id)
{
  int ret = OB_SUCCESS;

  ObTenantSnapshotGCParam param(tenant_snapshot_id,
                                gc_ls_id_arr,
                                gc_tenant_snapshot,
                                trace_id,
                                &tenant_snapshot_mgr_);
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler*);
  if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler is null", KR(ret));
  } else if (OB_FAIL(dag_scheduler->create_and_add_dag<ObTenantSnapshotGCDag>(&param))) {
    LOG_WARN("fail to create ObTenantSnapshotGCDag", KR(ret), K(param));
  } else {
    LOG_INFO("schedule ObTenantSnapshotGCDag success", K(param));
  }
  return ret;
}

void ObTenantSnapshotService::run_in_clone_mode_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(clone_running_env_check_())) {
    LOG_INFO("fail to clone_running_env_check_", KR(ret), KPC(this));
  } else {
    clone_service_.run();
  }
}

void ObTenantSnapshotService::run_in_normal_mode_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(normal_running_env_check_())) {
    LOG_INFO("fail to normal_running_env_check_", KR(ret));
  }

  if (OB_SUCC(ret) && meta_loaded_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_gc_tenant_snapshot_())) {
      LOG_WARN("fail to try_gc_tenant_snapshot_", KR(tmp_ret));
    }
    if (OB_TMP_FAIL(try_create_tenant_snapshot_in_meta_table_())) {
      LOG_WARN("fail to try_create_tenant_snapshot_in_meta_table_", KR(tmp_ret));
    }
  }
}

void ObTenantSnapshotService::run_in_gc_mode_()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  if (ATOMIC_LOAD(&running_mode_) != GC) {
    ret = OB_STATE_NOT_MATCH;
    LOG_INFO("the running mode is not GC", KR(ret), KPC(this));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get_min_data_version failed", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_INFO("ObTenantSnapshotService does not work before data version upgrade to 4_3_0_0",
        KR(ret), KPC(this), K(data_version));
  }

  if (OB_SUCC(ret) && meta_loaded_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_gc_tenant_snapshot_())) {
      LOG_WARN("fail to try_gc_tenant_snapshot_", KR(tmp_ret));
    }
  }
}

int ObTenantSnapshotService::start_clone(const ObTenantSnapshotID &tenant_snapshot_id,
                                         const ObLSID &ls_id,
                                         blocksstable::MacroBlockId &tablet_meta_entry)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshot* tenant_snapshot = nullptr;

  if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is not valid", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is not valid", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else if (OB_FAIL(tenant_snapshot_mgr_.get_tenant_snapshot(tenant_snapshot_id, tenant_snapshot))) {
    LOG_WARN("fail to get tenant snapshot", KR(ret), K(tenant_snapshot_id));
  } else if (OB_ISNULL(tenant_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_snapshot is unexpected nullptr", KR(ret), K(tenant_snapshot_id));
  } else {
    if (OB_FAIL(tenant_snapshot->inc_clone_ref())) {
      LOG_WARN("fail to inc_clone_ref", KR(ret), KPC(tenant_snapshot));
    } else {
      if (OB_FAIL(tenant_snapshot->get_ls_snapshot_tablet_meta_entry(ls_id, tablet_meta_entry))) {
        LOG_WARN("fail to get_ls_snapshot_tablet_meta_entry", KR(ret), KPC(tenant_snapshot), K(ls_id));
      } else {
        LOG_INFO("start clone succ", KPC(tenant_snapshot), K(tablet_meta_entry));
      }
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(tenant_snapshot->dec_clone_ref())) {
          LOG_ERROR("fail to dec_clone_ref", KR(ret), KPC(tenant_snapshot));
        }
      }
    }

    tenant_snapshot_mgr_.revert_tenant_snapshot(tenant_snapshot);
  }
  return ret;
}

int ObTenantSnapshotService::end_clone(const ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;

  ObTenantSnapshot* tenant_snapshot = nullptr;

  if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(tenant_snapshot_mgr_.get_tenant_snapshot(tenant_snapshot_id, tenant_snapshot))) {
    LOG_WARN("fail to get tenant snapshot", KR(ret), K(tenant_snapshot_id));
  } else {
    if (OB_FAIL(tenant_snapshot->dec_clone_ref())) {
      LOG_WARN("fail to dec_clone_ref", KR(ret), KPC(tenant_snapshot));
    } else {
      LOG_INFO("end clone succ", KPC(tenant_snapshot));
    }
    tenant_snapshot_mgr_.revert_tenant_snapshot(tenant_snapshot);
  }
  return ret;
}

int ObTenantSnapshotService::try_gc_tenant_snapshot_()
{
  int ret = OB_SUCCESS;

  bool tenant_has_been_dropped = false;
  if (GC == running_mode_) {
    tenant_has_been_dropped  = true;
  }

  TryGcTenantSnapshotFunctor fn(tenant_has_been_dropped);
  if (OB_FAIL(tenant_snapshot_mgr_.for_each(fn))) {
    LOG_WARN("fail to add all try_gc dag task", KR(ret));
  }
  LOG_INFO("try_gc_tenant_snapshot finished", KR(ret), KPC(this));
  return ret;
}

bool ObTenantSnapshotService::TryGcTenantSnapshotFunctor::operator()(
  const ObTenantSnapshotID &tenant_snapshot_id, ObTenantSnapshot* tenant_snapshot)
{
  int ret = OB_SUCCESS;
  bool gc_tenant_snapshot = false;
  ObArray<ObLSID> gc_ls_id_arr;
  common::ObCurTraceId::TraceId trace_id;

  ObTenantSnapshotService *tenant_snapshot_service = MTL(ObTenantSnapshotService *);
  if (OB_ISNULL(tenant_snapshot_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantSnapshotService is null", KR(ret), K(tenant_snapshot_service));
  } else if (OB_ISNULL(tenant_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_snapshot is null", KR(ret));
  } else if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(tenant_snapshot->try_start_gc_tenant_snapshot_dag(tenant_has_been_dropped_,
                                                                       gc_tenant_snapshot,
                                                                       gc_ls_id_arr,
                                                                       trace_id))) {
    if (OB_NO_NEED_UPDATE == ret || OB_EAGAIN == ret) {
      LOG_INFO("fail to try_start_gc_tenant_snapshot_dag now, try later",
          KR(ret), K(tenant_snapshot_id), K(tenant_has_been_dropped_));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to start try_start_gc_tenant_snapshot_dag",
          KR(ret), K(tenant_snapshot_id), K(tenant_has_been_dropped_));
    }
  } else {
    ObTraceIDGuard trace_guard(trace_id);
    if (OB_FAIL(tenant_snapshot_service->schedule_gc_tenant_snapshot_dag_(tenant_snapshot_id,
                                                                          gc_ls_id_arr,
                                                                          gc_tenant_snapshot,
                                                                          trace_id))) {
      LOG_WARN("fail to schedule_gc_tenant_snapshot_dag_",
          KR(ret), KPC(tenant_snapshot), K(tenant_has_been_dropped_));
      tenant_snapshot->finish_gc_tenant_snapshot_dag();
    } else {
      LOG_INFO("schedule_gc_tenant_snapshot success",
          KR(ret), KPC(tenant_snapshot), K(tenant_has_been_dropped_));
    }
  }
  return true;
}

int ObTenantSnapshotService::try_create_tenant_snapshot_in_meta_table_()
{
  int ret = OB_SUCCESS;

  ObArray<ObTenantSnapItem> tsnap_item_arr;
  if (OB_FAIL(ObTenantSnapshotMetaTable::acquire_all_tenant_snapshots(tsnap_item_arr))) {
    LOG_WARN("fail to acquire_all_tenant_snapshot", KR(ret));
  } else {
    for (int64_t i = 0; i < tsnap_item_arr.count(); ++i) {
      const ObTenantSnapItem& tsnap_item = tsnap_item_arr.at(i);
      if (tsnap_item.get_status() != ObTenantSnapStatus::DELETING) {
        if (OB_FAIL(try_create_tenant_snapshot_(tsnap_item.get_tenant_snapshot_id()))) {
          LOG_WARN("fail to try_create_tenant_snapshot_", KR(ret), K(tsnap_item));
        }
      }
    }
  }
  return ret;
}

int ObTenantSnapshotService::try_load_meta_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(common_env_check_())) {
    LOG_INFO("failed to common_env_check_", KR(ret));
  } else if (!meta_loaded_) {
    if (OB_FAIL(load_())) {
      LOG_ERROR("fail to load ckpt meta", KR(ret), KPC(this));
    } else {
      meta_loaded_ = true;
      LOG_INFO("ObTenantSnapshotService load ckpt meta succ", KR(ret), KPC(this));
    }
  }
  return ret;
}

void ObTenantSnapshotService::run1()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  lib::set_thread_name("TSnapSvc");

  while (is_user_tenant(tenant_id) && !has_set_stop()) {
    if (OB_FAIL(common_env_check_())) {
      LOG_INFO("failed to common_env_check_", KR(ret));
    }

    if (OB_SUCC(ret) && running_mode_ != GC) {
      RUNNING_MODE tmp_running_mode = RUNNING_MODE::INVALID;
      if (OB_FAIL(decide_running_mode_(tmp_running_mode))) {
        LOG_INFO("fail to decide_running_mode_", KR(ret), KPC(this));
      } else {
        running_mode_ = tmp_running_mode;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_load_meta_())) {
        LOG_INFO("fail to try_load_meta_", KR(ret), KPC(this));
      }
    }

    if (OB_SUCC(ret) && CLONE == running_mode_) {
      if (!clone_service_.is_started()) {
        if (OB_FAIL(clone_service_.start())) {
          LOG_WARN("fail to do clone_service_ start", KR(ret));
        }
      }

      if (OB_SUCC(ret)) {
        run_in_clone_mode_();
      }
    }

    if (OB_SUCC(ret) && NORMAL == running_mode_) {
      if (clone_service_.is_started()) {
        clone_service_.stop();
        clone_service_.wait();
      }

      run_in_normal_mode_();
    }

    if (OB_SUCC(ret) && GC == running_mode_) {
      run_in_gc_mode_();
    }

    {
      ObThreadCondGuard guard(cond_);
      const uint64_t idle_time = calculate_idle_time_();
      cond_.wait(idle_time);
    }
  }

  if (has_set_stop() && clone_service_.is_started()) {
    clone_service_.stop();
    clone_service_.wait();
  }

  LOG_INFO("ObTenantSnapshotService thread stop", K_(tg_id));
}

uint64_t ObTenantSnapshotService::calculate_idle_time_()
{
  uint64_t idle_time = 60 * 1000; // ms

  if (RUNNING_MODE::INVALID == running_mode_) {
    idle_time = 1 * 1000;
  } else if (RESTORE == running_mode_) {
    idle_time = 60 * 1000;
  } else if (CLONE == running_mode_) {
    idle_time = 1 * 1000;
  } else if (NORMAL == running_mode_) {
    int tmp_ret = OB_SUCCESS;
    bool has_tenant_snapshot_stopped = true;

    if (OB_TMP_FAIL(tenant_snapshot_mgr_.
          has_tenant_snapshot_stopped(has_tenant_snapshot_stopped))) {
      idle_time = 60 * 1000;
    } else if (has_tenant_snapshot_stopped) {
      idle_time = 5 * 1000;
    } else {
      idle_time = 60 * 1000;
    }
  } else {
    idle_time = 60 * 1000;
  }

  LOG_INFO("ObTenantSnapshotService thread idle time", K(tg_id_), K(idle_time), K(running_mode_));
  return idle_time;
}

bool ObTenantSnapshotService::GetAllLSSnapshotMapKeyFunctor::operator()(
  const ObLSSnapshotMapKey &ls_snap_map_key, ObLSSnapshot *ls_snapshot)
{
  int ret = OB_SUCCESS;
  if (!ls_snap_map_key.is_valid()) {
    LOG_DEBUG("invalid ObLSSnapshotMapKey, skip", K(ls_snap_map_key));
  } else if (OB_ISNULL(ls_snapshot_key_arr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_snapshot_key_arr_ is null", KR(ret));
  } else if (OB_FAIL(ls_snapshot_key_arr_->push_back(ls_snap_map_key))){
    LOG_WARN("fail to push back ls_snap_map_key", KR(ret), K(ls_snap_map_key));
  }
  return true;
}

int ObTenantSnapshotService::get_all_ls_snapshot_keys(ObArray<ObLSSnapshotMapKey> &ls_snapshot_key_arr)
{
  int ret = OB_SUCCESS;
  ls_snapshot_key_arr.reset();
  GetAllLSSnapshotMapKeyFunctor fn(&ls_snapshot_key_arr);
  if (OB_FAIL(ls_snapshot_mgr_.for_each(fn))) {
    LOG_WARN("fail to add all ls snapshot map keys", KR(ret));
  }
  return ret;
}

int ObTenantSnapshotService::get_ls_snapshot_vt_info(const ObLSSnapshotMapKey &ls_snapshot_key,
                                                     ObLSSnapshotVTInfo &ls_snapshot_vt_info)
{
  int ret = OB_SUCCESS;
  ls_snapshot_vt_info.reset();

  ObLSSnapshot *ls_snapshot = nullptr;
  if (!ls_snapshot_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ObLSSnapshotMapKey", KR(ret), K(ls_snapshot_key));
  } else if (OB_FAIL(ls_snapshot_mgr_.get_ls_snapshot(ls_snapshot_key.tenant_snapshot_id_,
                                                      ls_snapshot_key.ls_id_,
                                                      ls_snapshot))){
    LOG_WARN("fail to get ObLSSnapshot", KR(ret), K(ls_snapshot_key));
  } else if (OB_ISNULL(ls_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_snapshot is nullptr", KR(ret), K(ls_snapshot_key));
  } else {
    if (OB_FAIL(ls_snapshot->get_ls_snapshot_vt_info(ls_snapshot_vt_info))) {
      LOG_WARN("fail to get ls_snapshot_vt_info", KR(ret), K(ls_snapshot_key));
    }
    ls_snapshot_mgr_.revert_ls_snapshot(ls_snapshot);
  }

  ObTenantSnapshot *tenant_snapshot = nullptr;
  ObTenantSnapshotID tenant_snapshot_id = ls_snapshot_key.tenant_snapshot_id_;
  if (FAILEDx(tenant_snapshot_mgr_.get_tenant_snapshot(tenant_snapshot_id, tenant_snapshot))){
    ls_snapshot_vt_info.set_has_tsnap_info(false);
    if (OB_ENTRY_NOT_EXIST == ret) {
      // tenant snapshot info may be deleted while we are collecting ls snapshot info,
      // it's ok that we do not collect tenant snapshot info related to this ls snap
      LOG_INFO("tenant snapshot entry not exist", KR(ret), K(ls_snapshot_key));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant snapshot", KR(ret), K(ls_snapshot_key));
    }
  } else if (OB_ISNULL(tenant_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_snapshot is nullptr", KR(ret), K(tenant_snapshot_id));
  } else {
    ls_snapshot_vt_info.get_tsnap_info().reset();
    ObTenantSnapshotVTInfo &tsnap_info = ls_snapshot_vt_info.get_tsnap_info();
    if (OB_FAIL(tenant_snapshot->get_tenant_snapshot_vt_info(tsnap_info))) {
      LOG_WARN("fail to get tenant_snapshot_vt_info", KR(ret), K(tenant_snapshot_id));
    } else {
      ls_snapshot_vt_info.set_has_tsnap_info(true);
    }
    tenant_snapshot_mgr_.revert_tenant_snapshot(tenant_snapshot);
  }
  return ret;
}

int ObTenantSnapshotService::check_all_tenant_snapshot_released(bool& is_released)
{
  int ret = OB_SUCCESS;

  is_released = false;
  int64_t cnt = INT64_MAX;

  if (!ATOMIC_LOAD(&meta_loaded_)) {
    is_released = false;
    FLOG_INFO("cannot process before tenant snapshot meta loaded", KR(ret), KPC(this));
  } else if (GC != ATOMIC_LOAD(&running_mode_)) {
    is_released = false;
    FLOG_INFO("running_mode_ is not switch to GC", KR(ret), KPC(this));
  } else if (OB_FAIL(tenant_snapshot_mgr_.get_tenant_snapshot_cnt(cnt))) {
    FLOG_WARN("fail to get_tenant_snapshot_cnt", KR(ret));
  } else {
    if (0 == cnt) {
      is_released = true;
    } else {
      is_released = false;
    }
  }
  FLOG_INFO("check_all_tenant_snapshot_released finished", KR(ret), K(is_released), K(cnt), KPC(this));
  return ret;
}

void ObTenantSnapshotService::notify_unit_is_deleting()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(common_env_check_())) {
    LOG_WARN("fail to common_env_check_", KR(ret));
  } else if (FALSE_IT(unit_is_deleting_ = true)) {
  } else {
    ObThreadCondGuard guard(cond_);
    cond_.signal();
  }
  LOG_INFO("notify_unit_is_deleting finished", KR(ret), KPC(this));
}

bool ObTenantSnapshotService::DumpTenantSnapInfoFunctor::operator()(
    const ObTenantSnapshotID &tenant_snapshot_id,
    ObTenantSnapshot* tenant_snapshot)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tenant_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant snapshot is unexpected null", KR(ret), K(tenant_snapshot_id));
  } else {
    LOG_INFO("dump tenant snapshot info", KPC(tenant_snapshot));
  }

  return true;
}

void ObTenantSnapshotService::dump_all_tenant_snapshot_info()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(common_env_check_())) {
    LOG_WARN("fail to common_env_check_", KR(ret));
  } else if (!ATOMIC_LOAD(&meta_loaded_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tenant snapshot meta unloaded", KR(ret), KPC(this));
  } else {
    DumpTenantSnapInfoFunctor fn;
    if (OB_FAIL(tenant_snapshot_mgr_.for_each(fn))) {
      LOG_WARN("fail to dump tenant snapshot info", KR(ret));
    }
  }
  LOG_INFO("dump tenant snapshot info finished", KR(ret), KPC(this));
}

} // storage
} // oceanbase
