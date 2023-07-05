/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
* Define DataDictionaryService
*/

#include "ob_data_dict_service.h"

#include "lib/thread/thread_mgr.h"
#include "share/ob_thread_define.h"
#include "share/schema/ob_multi_version_schema_service.h"     // ObMultiVersionSchemaService
#include "storage/tx_storage/ob_ls_service.h"                 // ObLSService
#include "storage/tx/ob_ts_mgr.h"                             // OB_TS_MGR
#include "logservice/ob_log_base_type.h"

#define IF_SERVICE_RUNNING \
    if (IS_NOT_INIT) { \
      ret = OB_NOT_INIT; \
      DDLOG(WARN, "data_dict_service not inited", KR(ret), K_(tenant_id), K_(is_inited)); \
    } else if (OB_UNLIKELY(stop_flag_)) { \
      ret = OB_NOT_RUNNING; \
      DDLOG(WARN, "data_dict_service not running", KR(ret), K_(tenant_id), K_(stop_flag)); \
    } else

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace logservice;
using namespace storage;
namespace datadict
{

const int64_t ObDataDictService::TIMER_TASK_INTERVAL = 1 * _SEC_; // schedule timer task with interval 5s
const int64_t ObDataDictService::PRINT_DETAIL_INTERVAL = 60 * _SEC_;
const int64_t ObDataDictService::SCHEMA_OP_TIMEOUT = 2 * _SEC_;
const int64_t ObDataDictService::DEFAULT_REPORT_TIMEOUT = 10 * _MIN_;

ObDataDictService::ObDataDictService()
  : is_inited_(false),
    is_leader_(false),
    stop_flag_(true),
    tenant_id_(OB_INVALID_TENANT_ID),
    allocator_("ObDataDictSvc"),
    sql_client_(),
    storage_(allocator_),
    schema_service_(NULL),
    ls_service_(NULL),
    dump_interval_(INT64_MAX),
    timer_tg_id_(-1),
    last_dump_succ_time_(OB_INVALID_TIMESTAMP),
    force_need_dump_(false)
{}

int ObDataDictService::mtl_init(ObDataDictService *&datadict_service)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = &GSCHEMASERVICE;
  ObLSService *ls_service = MTL(ObLSService*);

  if (OB_FAIL(datadict_service->init(MTL_ID(), schema_service, ls_service))) {
    DDLOG(WARN, "init datadict_service failed", KR(ret));
  }

  return ret;
}

int ObDataDictService::init(
    const uint64_t tenant_id,
    schema::ObMultiVersionSchemaService *schema_service,
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    DDLOG(WARN, "ObDataDictService init twice", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(schema_service)
      || OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid argument to init ObDataDictService", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql_client_.init(GCTX.sql_proxy_))) {
    DDLOG(WARN, "init sql_client failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(storage_.init(tenant_id))) {
    DDLOG(WARN, "init ObDataDictStorage failed", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    allocator_.set_tenant_id(tenant_id);
    schema_service_ = schema_service;
    ls_service_ = ls_service;
    is_inited_ = true;
    DDLOG(INFO, "init datadict_service", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObDataDictService::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    DDLOG(WARN, "ObDataDictService is not inited", "tenant_id", MTL_ID(), K_(is_inited));
  } else if (! is_user_tenant(tenant_id_)) {
    // skip non-user tenant(skip sys_tenant and meta_tenant).
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::DataDictTimer, timer_tg_id_))) {
    DDLOG(WARN, "create timer failed", KR(ret), K_(tenant_id), K_(timer_tg_id));
  } else if (OB_FAIL(TG_START(timer_tg_id_))) {
    DDLOG(WARN, "datadict_service_timer start failed", KR(ret), K_(tenant_id), K_(timer_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(timer_tg_id_, *this, TIMER_TASK_INTERVAL, true/*is_repeat*/))) {
    DDLOG(WARN, "schedule data_dict_service timer_task failed", KR(ret), K_(timer_tg_id));
  } else {
    disable_timeout_check(); // dump data_dict may cost too much, distable timetout check.
    stop_flag_ = false;
    DDLOG(INFO, "start datadict_service", K_(tenant_id), K_(timer_tg_id));
  }

  return ret;
}

void ObDataDictService::stop()
{
  if (IS_INIT && ! stop_flag_) {
    TG_STOP(timer_tg_id_);
    DDLOG(INFO, "stop datadict_service", K_(tenant_id), K_(is_inited), K_(stop_flag));
  }
}

void ObDataDictService::wait()
{
  if (IS_INIT && ! stop_flag_) {
    TG_WAIT(timer_tg_id_);
    stop_flag_ = true;
    DDLOG(INFO, "wait datadict_service finish", K_(tenant_id), K_(timer_tg_id));
  }
}

void ObDataDictService::destroy()
{
  if (IS_INIT) {
    TG_DESTROY(timer_tg_id_);
    force_need_dump_ = false;
    last_dump_succ_time_ = OB_INVALID_TIMESTAMP;
    timer_tg_id_ = -1;
    dump_interval_ = 0;
    ls_service_ = NULL;
    schema_service_ = NULL;
    storage_.reset();
    sql_client_.destroy();
    allocator_.reset();
    tenant_id_ = OB_INVALID_TENANT_ID;
    stop_flag_ = true;
    is_leader_ = false;
    is_inited_ = false;
    DDLOG(INFO, "destroy datadict_service");
  }
}

void ObDataDictService::runTimerTask()
{
  ObCurTraceId::init(GCONF.self_addr_);

  if (IS_INIT) {
    refresh_config_();
    bool is_leader = ATOMIC_LOAD(&is_leader_);
    const int64_t start_time = ObClockGenerator::getClock();
    const bool is_reach_time_interval = (start_time >= ATOMIC_LOAD(&last_dump_succ_time_) + ATOMIC_LOAD(&dump_interval_));
    const bool force_need_dump = ATOMIC_LOAD(&force_need_dump_);

    if (is_leader && (is_reach_time_interval || force_need_dump)) {
      int ret = OB_SUCCESS;
      uint64_t data_version = 0;

      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
        DDLOG(WARN, "get_min_data_version failed", KR(ret), K_(tenant_id), K(force_need_dump));
      } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_1_0_0)) {
        // tenant data_version less than 4100, ignore.
      } else if (OB_FAIL(do_dump_data_dict_())) {
        if (OB_STATE_NOT_MATCH == ret) {
          DDLOG(WARN, "dump_data_dict_, maybe not ls_leader or lsn not valid, ignore.", KR(ret), K_(tenant_id), K(force_need_dump));
        } else if (OB_IN_STOP_STATE != ret) {
          DDLOG(WARN, "dump_data_dict_ failed", KR(ret), K_(tenant_id), K(force_need_dump));
        }
      } else {
        const int64_t end_time = ObClockGenerator::getClock();
        ATOMIC_SET(&last_dump_succ_time_, end_time);

        if (force_need_dump) {
          DDLOG(INFO, "force dump_data_dict done", K_(last_dump_succ_time), K(start_time));
          mark_force_dump_data_dict(false);
        }

        DDLOG(INFO, "do_dump_data_dict_ success", K_(tenant_id), "cost_time", end_time - start_time);
      }
    }
  }
}

void ObDataDictService::switch_to_follower_forcedly()
{
  bool is_leader = false;
  switch_role_to_(is_leader);
}

int ObDataDictService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  bool is_leader = true;
  switch_role_to_(is_leader);
  return ret;
}

int ObDataDictService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  switch_role_to_(is_leader);
  return ret;
}

int ObDataDictService::resume_leader()
{
  int ret = OB_SUCCESS;
  bool is_leader = true;
  switch_role_to_(is_leader);
  return ret;
}

void ObDataDictService::refresh_config_()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  const int64_t dump_interval = tenant_config->dump_data_dictionary_to_log_interval;
  if (dump_interval != dump_interval_) {
    ATOMIC_SET(&dump_interval_, dump_interval);
    DDLOG(INFO, "modify dump_data_dictionary_to_log_interval", K_(dump_interval));
  }
}

OB_INLINE void ObDataDictService::switch_role_to_(bool is_leader)
{
  ATOMIC_SET(&is_leader_, is_leader);
  ATOMIC_SET(&stop_flag_, ! is_leader);
  DDLOG(INFO, "switch_role", K(is_leader));
}

int ObDataDictService::do_dump_data_dict_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSHandle ls_handle; // NOTICE: ls_handle is a guard for usage of log_handler.
  ObLS *ls = NULL;
  ObLogHandler *log_handler = NULL;
  bool is_leader = false;
  share::SCN snapshot_scn;
  palf::LSN start_lsn;
  palf::LSN end_lsn;
  bool is_cluster_status_normal = false;
  bool is_data_dict_dump_success = false;
  bool is_any_log_callback_fail = false;
  storage_.reuse();
  allocator_.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    DDLOG(WARN, "data_dict_service not inited", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_ISNULL(ls_service_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "invalid ls_service", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(stop_flag_)) {
    ret = OB_NOT_RUNNING;
    DDLOG(WARN, "data_dict_service not running", KR(ret), K_(tenant_id), K_(stop_flag));
  } else if (OB_FAIL(check_cluster_status_normal_(is_cluster_status_normal))) {
    DDLOG(TRACE, "check_cluster_status_normal_ failed", KR(ret), K(is_cluster_status_normal));
  } else if (OB_UNLIKELY(! is_cluster_status_normal)) {
    DDLOG(TRACE, "cluster_status not normal, won't dump_data_dict", K(is_cluster_status_normal));
  } else if (OB_FAIL(ls_service_->get_ls(share::SYS_LS, ls_handle, ObLSGetMod::DATA_DICT_MOD))) {
    if (OB_LS_NOT_EXIST != ret || REACH_TIME_INTERVAL_THREAD_LOCAL(PRINT_DETAIL_INTERVAL)) {
      DDLOG(WARN, "get_ls for data_dict_service from ls_service failed", KR(ret), K_(tenant_id));
    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "invalid ls get from ls_handle", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "invalid log_handler_ get from OBLS", KR(ret), K_(tenant_id));
  } else if (check_ls_leader(log_handler, is_leader)) {
    DDLOG(WARN, "check_is_sys_ls_leader failed", KR(ret));
  } else if (! is_leader) {
    DDLOG(DEBUG, "won't do_dump_data_dict_ cause not ls_leader", KR(ret), K(is_leader));
    // do nothing if not ls leader.
  } else if (OB_FAIL(get_snapshot_scn_(snapshot_scn))) {
    DDLOG(WARN, "get_snapshot_scn failed", KR(ret), K(snapshot_scn));
  } else if (OB_FAIL(storage_.prepare(snapshot_scn, log_handler))) {
    DDLOG(WARN, "storage prepare for data_dict_dump failed", KR(ret), K(snapshot_scn));
  } else if (OB_FAIL(generate_dict_and_dump_(snapshot_scn))) {
    DDLOG(WARN, "generate_dict_and_dump_", KR(ret), K_(tenant_id), K(snapshot_scn));
  } else {
    is_data_dict_dump_success = true;
  }

  if (OB_SUCCESS != (tmp_ret = storage_.finish(
      start_lsn,
      end_lsn,
      is_data_dict_dump_success,
      is_any_log_callback_fail,
      stop_flag_))) {
    if (OB_IN_STOP_STATE != tmp_ret && OB_STATE_NOT_MATCH != tmp_ret) {
      DDLOG(WARN, "finish storage for data_dict_service failed", KR(ret), KR(tmp_ret),
          K(snapshot_scn), K(start_lsn), K(end_lsn), K_(stop_flag), K_(is_inited));
    }
    ret = tmp_ret;
  } else if (is_data_dict_dump_success && ! is_any_log_callback_fail) {
    // only report when dict dump success and all log_callback success.
    const int64_t half_dump_interval = ATOMIC_LOAD(&dump_interval_) / 2;
    const int64_t report_timeout = DEFAULT_REPORT_TIMEOUT > half_dump_interval ? half_dump_interval : DEFAULT_REPORT_TIMEOUT;
    const int64_t current_time = get_timestamp_us();
    const int64_t end_time = current_time + report_timeout;

    do {
      if (OB_FAIL(sql_client_.report_data_dict_persist_info(
          tenant_id_,
          snapshot_scn,
          start_lsn,
          end_lsn))) {
        if (ATOMIC_LOAD(&stop_flag_)) {
          ret = OB_IN_STOP_STATE;
          break;
        } else {
          DDLOG(WARN, "report_data_dict_persist_info failed", KR(ret),
              K_(tenant_id), K(snapshot_scn), K(start_lsn), K(end_lsn));
          usleep(100 * _MSEC_);
          if (get_timestamp_us() > end_time) {
            ret = OB_TIMEOUT;
            break;
          }
        }
      } else {
        DDLOG(INFO, "report_data_dict_persist_info success", KR(ret),
            K(snapshot_scn), K(start_lsn), K(end_lsn));
      }
    } while (OB_FAIL(ret));
  }

  return ret;
}

int ObDataDictService::check_cluster_status_normal_(bool &is_normal)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = NULL;

  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect multi_version_schema_service valid", KR(ret));
  } else if (OB_ISNULL(schema_service = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "get ob_schema_service from multi_version_schema_service failed", KR(ret));
  } else {
    const ObClusterSchemaStatus cluster_status = schema_service->get_cluster_schema_status();
    is_normal = (ObClusterSchemaStatus::NORMAL_STATUS == cluster_status);
  }

  return ret;
}

int ObDataDictService::get_snapshot_scn_(share::SCN &snapshot_scn)
{
  int ret = OB_SUCCESS;
  static const int64_t gts_get_timeout_ns = 4 * _SEC_ * NS_CONVERSION;
  SCN gts_scn;
  const transaction::MonotonicTs stc_ahead = transaction::MonotonicTs::current_time() -
                                             transaction::MonotonicTs(GCONF._ob_get_gts_ahead_interval);
  transaction::MonotonicTs tmp_receive_gts_ts(0);
  const int64_t expire_ts_ns = get_timestamp_ns() + gts_get_timeout_ns;

  do{
    if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id_, stc_ahead, NULL, gts_scn, tmp_receive_gts_ts))) {
      if (OB_EAGAIN == ret) {
        if (expire_ts_ns < get_timestamp_ns()) {
          ret = OB_TIMEOUT;
        } else {
          ob_usleep(100);
        }
      } else {
        DDLOG(WARN, "get_gts for data_dict_service failed", KR(ret));
      }
    } else if (OB_UNLIKELY(!gts_scn.is_valid_and_not_min())) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "gts invalid", KR(ret), K(gts_scn));
    } else {
      DDLOG(TRACE, "get gts", K(gts_scn));
    }
  } while (OB_EAGAIN == ret && ! stop_flag_);

  if (OB_SUCC(ret)) {
    snapshot_scn = gts_scn;
  }

  return ret;
}

int ObDataDictService::generate_dict_and_dump_(const share::SCN &snapshot_scn)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  ObArray<uint64_t> database_ids;
  ObArray<uint64_t> table_ids;

  if (OB_FAIL(sql_client_.get_schema_version(tenant_id_, snapshot_scn, schema_version))) {
    ret = OB_SCHEMA_EAGAIN;
    DDLOG(WARN, "get_schema_version failed", KR(ret), K(snapshot_scn), K(schema_version));
    // NOTICE: SHOULD ALWAYS DUMP TENANT_META BEFORE DB/TB METAS
  } else if (OB_FAIL(handle_tenant_meta_(snapshot_scn, schema_version, database_ids, table_ids))) {
    DDLOG(WARN, "handle_tenant_meta_ failed", KR(ret), K(snapshot_scn));
  } else if (OB_FAIL(handle_database_metas_(schema_version, database_ids))) {
    DDLOG(WARN, "handle_database_metas_ failed", KR(ret), K(snapshot_scn));
  } else if (OB_FAIL(handle_table_metas_(schema_version, table_ids))) {
    DDLOG(WARN, "handle_table_metas_ failed", KR(ret), K(snapshot_scn), K(schema_version));
  }

  DDLOG(INFO, "generate_dict_and_dump_", KR(ret),
      K_(tenant_id),
      K(snapshot_scn),
      K(schema_version),
      "database_count", database_ids.count(),
      "table_count", table_ids.count());

  return ret;
}

int ObDataDictService::get_tenant_schema_guard_(
    const int64_t schema_version,
    ObSchemaGetterGuard &schema_guard,
    const bool is_force_fallback)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService::RefreshSchemaMode refresh_mode = ObMultiVersionSchemaService::RefreshSchemaMode::NORMAL;

  if (is_force_fallback) {
    refresh_mode = ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_FALLBACK;
  } else {
    refresh_mode = ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_LAZY;
  }

  RETRY_FUNC_ON_ERROR(OB_SCHEMA_EAGAIN, stop_flag_, *schema_service_, get_tenant_schema_guard,
      tenant_id_,
      schema_guard,
      schema_version,
      OB_INVALID_VERSION,
      refresh_mode);

  return ret;
}

int ObDataDictService::check_tenant_status_normal_(
    share::schema::ObSchemaGetterGuard &tenant_schema_guard,
    bool &is_normal)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;

  if (OB_FAIL(tenant_schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    DDLOG(WARN, "get_tenant_schema failed", KR(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "invalid tenant_schema", KR(ret), K_(tenant_id));
  } else {
    is_normal = tenant_schema->is_valid() && tenant_schema->is_normal();
    DDLOG(TRACE, "check_tenant_status", K(is_normal), KPC(tenant_schema));
  }

  return ret;
}

int ObDataDictService::handle_tenant_meta_(
    const share::SCN &snapshot_scn,
    const int64_t schema_version,
    ObIArray<uint64_t> &database_ids,
    ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  ObLSArray ls_array;
  database_ids.reset();
  table_ids.reset();
  const ObTenantSchema *tenant_schema = NULL;
  bool is_normal = false;
  ObSchemaGetterGuard tenant_schema_guard;
  ObDictTenantMeta tenant_meta(&allocator_);
  ObDictMetaHeader header(ObDictMetaType::TENANT_META);

  if (OB_FAIL(sql_client_.get_ls_info(tenant_id_, snapshot_scn, ls_array))) {
    DDLOG(WARN, "get_ls_info failed", KR(ret), K_(tenant_id), K(snapshot_scn), K(ls_array));
  } else if (OB_FAIL(get_tenant_schema_guard_(schema_version, tenant_schema_guard, true/*is_force_fallback*/))) {
    DDLOG(WARN, "get_tenant_schema_guard failed", KR(ret), K(snapshot_scn), K(schema_version));
    ret = OB_SCHEMA_EAGAIN;
  } else if (OB_FAIL(check_tenant_status_normal_(tenant_schema_guard, is_normal))) {
    DDLOG(WARN, "check_tenant_status_normal_ failed", KR(ret), K(is_normal));
  } else if (OB_UNLIKELY(! is_normal)) {
    DDLOG(INFO, "ignore non-normal status tenant for dump_data_dict_", K_(tenant_id), K(is_normal));
  } else if (OB_FAIL(tenant_schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    DDLOG(WARN, "get_tenant_schema failed", KR(ret), K(snapshot_scn));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "invalid tenant_schema", KR(ret), K_(tenant_id), K(snapshot_scn));
  } else if (OB_FAIL(tenant_meta.init_with_ls_info(*tenant_schema, ls_array))) {
    DDLOG(WARN, "init tenant_meta failed", KR(ret), K(ls_array), K(tenant_meta));
  } else if (OB_FAIL(get_database_ids_(tenant_schema_guard, database_ids))) {
    DDLOG(WARN, "get_database_ids_in_tenant failed", KR(ret), K(snapshot_scn), K(schema_version));
  } else if (OB_FAIL(tenant_schema_guard.get_table_ids_in_tenant(tenant_id_, table_ids))) {
    DDLOG(WARN, "get_table_ids_in_tenant failed", KR(ret), K(snapshot_scn), K(schema_version));
  } else if (OB_FAIL(storage_.handle_dict_meta(tenant_meta, header))) {
    DDLOG(WARN, "handle dict_tenant_meta failed", KR(ret), K(tenant_meta), K(header));
  } else {
    DDLOG(TRACE, "handle dict_tenant_meta succ", KR(ret), K(tenant_meta), K(header));
  }

  return ret;
}

int ObDataDictService::get_database_ids_(
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObIArray<uint64_t> &database_ids)
{
  int ret = OB_SUCCESS;
  common::ObArray<const ObSimpleDatabaseSchema*> database_schemas;
  database_schemas.reset();

  if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(tenant_id_, database_schemas))) {
    DDLOG(WARN, "get_database_schemas_in_tenant failed", KR(ret));
  } else {
    const int64_t database_count = database_schemas.count();

    for (int i = 0; OB_SUCC(ret) && !stop_flag_ && i < database_count; i++) {
      const ObSimpleDatabaseSchema *database_schema = NULL;
      uint64_t database_id = OB_INVALID_ID;

      if (OB_FAIL(database_schemas.at(i, database_schema))) {
        DDLOG(WARN, "get database_schema failed", KR(ret), K(i), K(database_count));
      } else if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "invalid database_schema get from schema_guard", KR(ret), K(i), K(database_count));
      } else if (OB_UNLIKELY(OB_INVALID_ID == (database_id = database_schema->get_database_id()))) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "invalid database_id get from database_schema", KR(ret), K(i), K(database_count), KPC(database_schema));
      } else if (OB_FAIL(database_ids.push_back(database_id))) {
        DDLOG(WARN, "push_back database_id into list failed", KR(ret), K(i), K(database_count), KPC(database_schema));
      }
    }
  }

  return ret;
}

int ObDataDictService::handle_database_metas_(
    const int64_t schema_version,
    const ObIArray<uint64_t> &database_ids)
{
  int ret = OB_SUCCESS;
  const int64_t database_count = database_ids.count();
  schema::ObSchemaGetterGuard schema_guard; // will reset while getting schem_guard

  for (int i = 0; OB_SUCC(ret) && !stop_flag_ && i < database_count; i++) {
    uint64_t database_id = OB_INVALID_ID;
    const ObDatabaseSchema *database_schema = NULL;
    ObDictDatabaseMeta db_meta(&allocator_);
    ObDictMetaHeader header(ObDictMetaType::DATABASE_META);

    if (OB_FAIL(database_ids.at(i, database_id))) {
      DDLOG(WARN, "get_database_id failed", KR(ret), K(i), K(database_count), K(database_ids));
    } else if (OB_UNLIKELY(OB_INVALID_ID == database_id)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "invalid database_id", KR(ret), K(i), K(database_count), K(database_ids));
    } else if (OB_FAIL(get_tenant_schema_guard_(schema_version, schema_guard, false/*is_force_fallback=false*/))) {
      DDLOG(WARN, "get_tenant_schema_guard_ in lazy mode failed", KR(ret), K(schema_version), K(database_id));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, database_id, database_schema))) {
      DDLOG(WARN, "get_database_schema failed", KR(ret), K(schema_version));
    } else if (OB_ISNULL(database_schema)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "invalid database_schema", KR(ret), K(i), K(database_count), K(database_id), KP(database_schema));
    } else if (OB_FAIL(db_meta.init(*database_schema))) {
      DDLOG(WARN, "init database_meta failed", KR(ret), KPC(database_schema));
    } else if (OB_FAIL(storage_.handle_dict_meta(db_meta, header))) {
      DDLOG(WARN, "handle dict_db_meta failed", KR(ret), K(db_meta), K(header), KPC(database_schema));
    } else {
      DDLOG(TRACE, "handle dict_db_meta success", KR(ret), K(db_meta), K(header), KPC(database_schema));
    }
  }

  return ret;
}

int ObDataDictService::handle_table_metas_(
    const int64_t schema_version,
    const ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tb_meta_allocator("ObDatDictTbMeta");
  schema::ObSchemaGetterGuard schema_guard; // will reset while getting schem_guard
  static const int64_t batch_table_meta_size = 200;

  for (int i = 0; OB_SUCC(ret) && ! stop_flag_ && i < table_ids.count(); i++) {
    const ObTableSchema *table_schema = NULL;
    uint64_t table_id = OB_INVALID_ID;
    // NOTICE: get schema_guard for each table_meta in case of too much memory usage in schema_service.
    bool is_filtered = false;
    ObDictTableMeta table_meta(&tb_meta_allocator);
    ObDictMetaHeader header(ObDictMetaType::TABLE_META);

    if (i % batch_table_meta_size == 0) {
      tb_meta_allocator.reset();
    }

    if (OB_FAIL(table_ids.at(i, table_id))) {
      DDLOG(WARN, "get_table_id failed", KR(ret), K(schema_version), K(i), K(table_ids));
    } else if (OB_FAIL(get_tenant_schema_guard_(schema_version, schema_guard, false/*is_force_fallback=false*/))) {
      DDLOG(WARN, "get_tenant_schema_guard_ in lazy mode failed", KR(ret), K(schema_version), K(table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
      DDLOG(WARN, "get_table_schema failed", KR(ret), K(table_id), K(schema_version));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      DDLOG(WARN, "invalid table_schema", KR(ret), K(schema_version), K(table_id));
    } else if (OB_FAIL(filter_table_(*table_schema, is_filtered))) {
      DDLOG(WARN, "filter_table_ failed", KR(ret), K(is_filtered), KPC(table_schema));
    } else if (is_filtered) {
      DDLOG(DEBUG, "filter_table_",
          K(schema_version),
          "table_id", table_schema->get_table_id(),
          "table_name", table_schema->get_table_name(),
          "table_type", table_schema->get_table_type());
      // ignore this table.
    } else if (OB_FAIL(table_meta.init(*table_schema))) {
      DDLOG(WARN, "init table_meta failed", KR(ret), K(schema_version), KPC(table_schema));
    } else if (OB_FAIL(storage_.handle_dict_meta(table_meta, header))) {
      DDLOG(WARN, "handle dict_table_meta failed", KR(ret), K(table_meta), K(header), KPC(table_schema));
    } else {
      DDLOG(DEBUG, "handle dict_table_meta succ", KR(ret), K(table_meta), K(header), KPC(table_schema));
    }
  }

  return ret;
}

int ObDataDictService::filter_table_(const share::schema::ObTableSchema &table_schema, bool &is_filtered)
{
  int ret = OB_SUCCESS;

  is_filtered =
      ! (table_schema.has_tablet()
      || table_schema.is_user_table()
      || table_schema.is_unique_index()
      || table_schema.is_tmp_table());

  return ret;
}

} // namespace datadict
} // namespace oceanbase
