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

#define USING_LOG_PREFIX DATA_DICT

#include "ob_data_dict_service.h"

#include "storage/tx_storage/ob_ls_service.h"                 // ObLSService
#include "storage/tx/ob_ts_mgr.h"                             // OB_TS_MGR

#define IF_SERVICE_RUNNING \
    if (IS_NOT_INIT) { \
      ret = OB_NOT_INIT; \
      LOG_WARN("data_dict_service not inited", KR(ret), K_(tenant_id), K_(is_inited)); \
    } else if (OB_UNLIKELY(stop_flag_)) { \
      ret = OB_NOT_RUNNING; \
      LOG_WARN("data_dict_service not running", KR(ret), K_(tenant_id), K_(stop_flag)); \
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
    expected_dump_snapshot_scn_(),
    last_dump_succ_snapshot_scn_(),
    data_dict_dump_history_retention_sec_(OB_INVALID_TIMESTAMP),
    force_need_dump_(false)
{}

int ObDataDictService::mtl_init(ObDataDictService *&datadict_service)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = &GSCHEMASERVICE;
  ObLSService *ls_service = MTL(ObLSService*);

  if (OB_FAIL(datadict_service->init(MTL_ID(), schema_service, ls_service))) {
    LOG_WARN("init datadict_service failed", KR(ret));
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
    LOG_WARN("ObDataDictService init twice", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(schema_service)
      || OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to init ObDataDictService", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql_client_.init(GCTX.sql_proxy_))) {
    LOG_WARN("init sql_client failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(storage_.init(tenant_id))) {
    LOG_WARN("init ObDataDictStorage failed", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    allocator_.set_tenant_id(tenant_id);
    schema_service_ = schema_service;
    ls_service_ = ls_service;
    is_inited_ = true;
    LOG_INFO("init datadict_service", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObDataDictService::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDataDictService is not inited", "tenant_id", MTL_ID(), K_(is_inited));
  } else if (! is_user_tenant(tenant_id_)) {
    // skip non-user tenant(skip sys_tenant and meta_tenant).
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::DataDictTimer, timer_tg_id_))) {
    LOG_WARN("create timer failed", KR(ret), K_(tenant_id), K_(timer_tg_id));
  } else if (OB_FAIL(TG_START(timer_tg_id_))) {
    LOG_WARN("datadict_service_timer start failed", KR(ret), K_(tenant_id), K_(timer_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(timer_tg_id_, *this, TIMER_TASK_INTERVAL, true/*is_repeat*/))) {
    LOG_WARN("schedule data_dict_service timer_task failed", KR(ret), K_(timer_tg_id));
  } else {
    disable_timeout_check(); // dump data_dict may cost too much, distable timetout check.
    stop_flag_ = false;
    LOG_INFO("start datadict_service", K_(tenant_id), K_(timer_tg_id));
  }

  return ret;
}

void ObDataDictService::stop()
{
  if (IS_INIT && ! stop_flag_) {
    TG_STOP(timer_tg_id_);
    LOG_INFO("stop datadict_service", K_(tenant_id), K_(is_inited), K_(stop_flag));
  }
}

void ObDataDictService::wait()
{
  if (IS_INIT && ! stop_flag_) {
    TG_WAIT(timer_tg_id_);
    stop_flag_ = true;
    LOG_INFO("wait datadict_service finish", K_(tenant_id), K_(timer_tg_id));
  }
}

void ObDataDictService::destroy()
{
  if (IS_INIT) {
    TG_DESTROY(timer_tg_id_);
    force_need_dump_ = false;
    data_dict_dump_history_retention_sec_ = OB_INVALID_TIMESTAMP;
    last_dump_succ_snapshot_scn_.reset();
    expected_dump_snapshot_scn_.reset();
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
    LOG_INFO("destroy datadict_service");
  }
}

void ObDataDictService::runTimerTask()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    refresh_config_();
    bool is_leader = ATOMIC_LOAD(&is_leader_);
    const int64_t start_time = ObClockGenerator::getClock();
    const int64_t dump_interval = ATOMIC_LOAD(&dump_interval_);
    const bool enable_dump_by_interval = dump_interval > 0;
    const bool is_reach_time_interval = enable_dump_by_interval
        && ((start_time >= ATOMIC_LOAD(&last_dump_succ_time_) + dump_interval) || last_dump_succ_time_ <=0);
    const bool need_dump_as_expected = expected_dump_snapshot_scn_.is_valid()
        && (! last_dump_succ_snapshot_scn_.is_valid() || last_dump_succ_snapshot_scn_ < expected_dump_snapshot_scn_);
    const bool force_need_dump = ATOMIC_LOAD(&force_need_dump_);

    // Check if data dictionary table has records, if not, need to dump
    bool has_data_dict_record = true;
    if (is_leader && OB_FAIL(sql_client_.check_has_data_dict_record(tenant_id_, has_data_dict_record))) {
      LOG_WARN("check_has_data_dict_record failed", KR(ret), K_(tenant_id));
      // assume already has valid data_dict record and set ret = OB_SUCCESS
      ret = OB_SUCCESS;
    } else if (!has_data_dict_record) {
      LOG_INFO("no valid dump rececord, maybe a new tenant, need trigger dump", K_(tenant_id));
    }

    if (is_leader && (is_reach_time_interval || force_need_dump || need_dump_as_expected || !has_data_dict_record)) {
      LOG_INFO("begin do_dump_data_dict_", K(start_time), K(is_reach_time_interval), K(force_need_dump), K(need_dump_as_expected), K(has_data_dict_record));
      uint64_t data_version = 0;

      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
        LOG_WARN("get_min_data_version failed", KR(ret), K_(tenant_id), K(force_need_dump));
      } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_1_0_0)) {
        // tenant data_version less than 4100, ignore.
      } else if (OB_FAIL(do_dump_data_dict_())) {
        if (OB_STATE_NOT_MATCH == ret) {
          LOG_WARN("dump_data_dict_, maybe not ls_leader or lsn not valid, ignore.", KR(ret), K_(tenant_id), K(force_need_dump), K(need_dump_as_expected));
        } else if (OB_IN_STOP_STATE != ret) {
          LOG_WARN("dump_data_dict_ failed", KR(ret), K_(tenant_id), K(force_need_dump), K(need_dump_as_expected));
        }
      } else {
        const int64_t end_time = ObClockGenerator::getClock();
        ATOMIC_SET(&last_dump_succ_time_, end_time);

        if (force_need_dump) {
          LOG_INFO("force dump_data_dict done", K_(last_dump_succ_time), K(start_time));
          mark_force_dump_data_dict(false);
        }

        if (need_dump_as_expected && last_dump_succ_snapshot_scn_ >= expected_dump_snapshot_scn_) {
          expected_dump_snapshot_scn_.reset();
        }

        LOG_INFO("do_dump_data_dict_ success", K_(tenant_id), "cost_time", end_time - start_time);

        try_recycle_dict_history_();
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

void ObDataDictService::mark_force_dump_data_dict(const bool need_dump)
{
  ATOMIC_SET(&force_need_dump_, need_dump);
  LOG_INFO("mark force_dump_data_dict", K(need_dump));
}

void ObDataDictService::update_expected_dump_scn(const share::SCN &expected_dump_scn)
{
  if (OB_UNLIKELY(!expected_dump_scn.is_valid())) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "ignore invalid expected_dump_scn", K(expected_dump_scn));
  } else {
    expected_dump_snapshot_scn_.atomic_set(share::SCN::max(expected_dump_scn, expected_dump_snapshot_scn_));
    LOG_INFO("update expected dump scn", K_(expected_dump_snapshot_scn));
  }
}

void ObDataDictService::update_data_dict_dump_history_retention(const int64_t data_dict_dump_history_retention_sec)
{
  if (data_dict_dump_history_retention_sec >= 0) {
    ATOMIC_STORE(&data_dict_dump_history_retention_sec_, data_dict_dump_history_retention_sec);
    LOG_INFO("update data_dict_dump_history_retention_sec", K(data_dict_dump_history_retention_sec));
  } else {
    LOG_INFO("won't update data_dict_dump_history_retention_sec cause invalid value", K(data_dict_dump_history_retention_sec));
  }
}

void ObDataDictService::refresh_config_()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  const int64_t dump_interval = tenant_config->dump_data_dictionary_to_log_interval;
  if (dump_interval != dump_interval_) {
    ATOMIC_SET(&dump_interval_, dump_interval);
    LOG_INFO("modify dump_data_dictionary_to_log_interval", K_(dump_interval));
  }
}

OB_INLINE void ObDataDictService::switch_role_to_(bool is_leader)
{
  ATOMIC_SET(&is_leader_, is_leader);
  ATOMIC_SET(&stop_flag_, ! is_leader);
  LOG_INFO("switch_role", K(is_leader));
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
  int64_t start_proposal_id = 0;
  int64_t end_proposal_id = 0;
  bool is_cluster_status_normal = false;
  bool is_data_dict_dump_success = false;
  bool is_any_log_callback_fail = false;
  storage_.reuse();
  allocator_.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("data_dict_service not inited", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_ISNULL(ls_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls_service", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(stop_flag_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("data_dict_service not running", KR(ret), K_(tenant_id), K_(stop_flag));
  } else if (OB_FAIL(check_cluster_status_normal_(is_cluster_status_normal))) {
    LOG_TRACE("check_cluster_status_normal_ failed", KR(ret), K(is_cluster_status_normal));
  } else if (OB_UNLIKELY(! is_cluster_status_normal)) {
    LOG_TRACE("cluster_status not normal, won't dump_data_dict", K(is_cluster_status_normal));
  } else if (OB_FAIL(ls_service_->get_ls(share::SYS_LS, ls_handle, ObLSGetMod::DATA_DICT_MOD))) {
    if (OB_LS_NOT_EXIST != ret || REACH_TIME_INTERVAL_THREAD_LOCAL(PRINT_DETAIL_INTERVAL)) {
      LOG_WARN("get_ls for data_dict_service from ls_service failed", KR(ret), K_(tenant_id));
    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls get from ls_handle", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid log_handler_ get from OBLS", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_ls_leader(log_handler, is_leader, start_proposal_id))) {
    LOG_WARN("check_is_sys_ls_leader failed", KR(ret), K(start_proposal_id));
  } else if (! is_leader) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("won't do_dump_data_dict_ cause not ls_leader", KR(ret), K(is_leader), K(start_proposal_id));
    // do nothing if not ls leader.
  } else if (OB_FAIL(get_snapshot_scn_(snapshot_scn))) {
    LOG_WARN("get_snapshot_scn failed", KR(ret), K(snapshot_scn));
  } else if (OB_FAIL(storage_.prepare(snapshot_scn, log_handler))) {
    LOG_WARN("storage prepare for data_dict_dump failed", KR(ret), K(snapshot_scn));
  } else if (OB_FAIL(generate_dict_and_dump_(snapshot_scn))) {
    LOG_WARN("generate_dict_and_dump_", KR(ret), K_(tenant_id), K(snapshot_scn), K(start_proposal_id));
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
      LOG_WARN("finish storage for data_dict_service failed", KR(ret), KR(tmp_ret),
          K(snapshot_scn), K(start_lsn), K(end_lsn), K_(stop_flag), K_(is_inited));
    }
    ret = tmp_ret;
  } else if (OB_UNLIKELY(! is_data_dict_dump_success || is_any_log_callback_fail)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_INFO("won't report data_dict persist info cause data_dict dump failed or log_callback failed",
        KR(ret), K(is_data_dict_dump_success), K(is_any_log_callback_fail));
  } else if (OB_FAIL(check_ls_leader(log_handler, is_leader, end_proposal_id))) {
    LOG_WARN("check_is_sys_ls_leader failed", KR(ret), K(start_proposal_id), K(end_proposal_id));
  } else if (OB_UNLIKELY(! is_leader || start_proposal_id != end_proposal_id)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_INFO("won't report data_dict persist info cause currently not ls_leader or not the same election term",
        KR(ret), K(is_leader), K(start_proposal_id), K(end_proposal_id));
  } else {
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
          LOG_WARN("report_data_dict_persist_info failed", KR(ret),
              K_(tenant_id), K(snapshot_scn), K(start_lsn), K(end_lsn));
          ob_usleep(100 * _MSEC_);
          if (get_timestamp_us() > end_time) {
            ret = OB_TIMEOUT;
            break;
          }
        }
      } else {
        LOG_INFO("report_data_dict_persist_info success", KR(ret),
            K(snapshot_scn), K(start_lsn), K(end_lsn));
      }
    } while (OB_FAIL(ret));

    if (OB_SUCC(ret)) {
      last_dump_succ_snapshot_scn_.atomic_set(snapshot_scn);
    }
  }

  return ret;
}

int ObDataDictService::check_cluster_status_normal_(bool &is_normal)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = NULL;

  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect multi_version_schema_service valid", KR(ret));
  } else if (OB_ISNULL(schema_service = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ob_schema_service from multi_version_schema_service failed", KR(ret));
  } else {
    const ObClusterSchemaStatus cluster_status = schema_service->get_cluster_schema_status();
    is_normal = (ObClusterSchemaStatus::NORMAL_STATUS == cluster_status);
  }

  return ret;
}

int ObDataDictService::get_snapshot_scn_(share::SCN &snapshot_scn)
{
  int ret = OB_SUCCESS;
  static const int64_t gts_get_timeout = 10 * _SEC_;
  SCN gts_scn;
  const transaction::MonotonicTs stc = transaction::MonotonicTs::current_time();
  transaction::MonotonicTs tmp_receive_gts_ts(0);
  const int64_t expire_ts = get_timestamp_us() + gts_get_timeout;
  int64_t retry_cnt = 0;
  const static int64_t PRINT_INTERVAL = 10;

  do{
    if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id_, stc, NULL, gts_scn, tmp_receive_gts_ts))) {
      if (get_timestamp_us() > expire_ts) {
        int tmp_ret = OB_TIMEOUT;
        LOG_WARN("get_gts for data_dict_service failed and timeout", KR(ret), KR(tmp_ret), K(expire_ts), K(gts_scn));
        ret = tmp_ret;
      } else if (OB_EAGAIN == ret) {
        ob_usleep(100 * _MSEC_);
        if (++retry_cnt % PRINT_INTERVAL == 0) {
          LOG_WARN("retry get_gts for data_dict_service failed", KR(ret), K(retry_cnt), K(expire_ts));
        }
      } else {
        LOG_WARN("get_gts for data_dict_service failed", KR(ret), K(expire_ts));
      }
    } else if (OB_UNLIKELY(!gts_scn.is_valid_and_not_min())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gts invalid", KR(ret), K(gts_scn));
    } else {
      LOG_TRACE("get gts", K(gts_scn));
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
  int64_t filter_table_count = 0;

  if (OB_FAIL(sql_client_.get_schema_version(tenant_id_, snapshot_scn, schema_version))) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get_schema_version failed", KR(ret), K(snapshot_scn), K(schema_version));
    // NOTICE: SHOULD ALWAYS DUMP TENANT_META BEFORE DB/TB METAS
  } else if (OB_FAIL(handle_tenant_meta_(snapshot_scn, schema_version, database_ids, table_ids))) {
    LOG_WARN("handle_tenant_meta_ failed", KR(ret), K(snapshot_scn));
  } else if (OB_FAIL(handle_database_metas_(schema_version, database_ids))) {
    LOG_WARN("handle_database_metas_ failed", KR(ret), K(snapshot_scn));
  } else if (OB_FAIL(handle_table_metas_(schema_version, table_ids, filter_table_count))) {
    LOG_WARN("handle_table_metas_ failed", KR(ret), K(snapshot_scn), K(schema_version));
  }

  LOG_INFO("generate_dict_and_dump_", KR(ret),
      K_(tenant_id),
      K(snapshot_scn),
      K(schema_version),
      "database_count", database_ids.count(),
      "table_count", table_ids.count(),
      K(filter_table_count));

  return ret;
}

int ObDataDictService::get_tenant_schema_guard_(
    const int64_t schema_version,
    ObSchemaGetterGuard &schema_guard,
    const bool is_force_fallback)
{
  int ret = OB_SUCCESS;
  const int64_t sleep_ts_on_schema_err = 100 * _MSEC_;
  ObMultiVersionSchemaService::RefreshSchemaMode refresh_mode = ObMultiVersionSchemaService::RefreshSchemaMode::NORMAL;

  if (is_force_fallback) {
    refresh_mode = ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_FALLBACK;
  } else {
    refresh_mode = ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_LAZY;
  }

  RETRY_FUNC_ON_ERROR_WITH_SLEEP(OB_SCHEMA_EAGAIN, sleep_ts_on_schema_err, stop_flag_, *schema_service_, get_tenant_schema_guard,
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
    LOG_WARN("get_tenant_schema failed", KR(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_schema", KR(ret), K_(tenant_id));
  } else {
    is_normal = tenant_schema->is_valid() && tenant_schema->is_normal();
    LOG_TRACE("check_tenant_status", K(is_normal), KPC(tenant_schema));
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
    LOG_WARN("get_ls_info failed", KR(ret), K_(tenant_id), K(snapshot_scn), K(ls_array));
  } else if (OB_FAIL(get_tenant_schema_guard_(schema_version, tenant_schema_guard, true/*is_force_fallback*/))) {
    LOG_WARN("get_tenant_schema_guard failed", KR(ret), K(snapshot_scn), K(schema_version));
    ret = OB_SCHEMA_EAGAIN;
  } else if (OB_FAIL(check_tenant_status_normal_(tenant_schema_guard, is_normal))) {
    LOG_WARN("check_tenant_status_normal_ failed", KR(ret), K(is_normal));
  } else if (OB_UNLIKELY(! is_normal)) {
    LOG_INFO("ignore non-normal status tenant for dump_data_dict_", K_(tenant_id), K(is_normal));
  } else if (OB_FAIL(tenant_schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("get_tenant_schema failed", KR(ret), K(snapshot_scn));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_schema", KR(ret), K_(tenant_id), K(snapshot_scn));
  } else if (OB_FAIL(tenant_meta.init_with_ls_info(*tenant_schema, ls_array))) {
    LOG_WARN("init tenant_meta failed", KR(ret), K(ls_array), K(tenant_meta));
  } else if (OB_FAIL(get_database_ids_(tenant_schema_guard, database_ids))) {
    LOG_WARN("get_database_ids_in_tenant failed", KR(ret), K(snapshot_scn), K(schema_version));
  } else if (OB_FAIL(tenant_schema_guard.get_table_ids_in_tenant(tenant_id_, table_ids))) {
    LOG_WARN("get_table_ids_in_tenant failed", KR(ret), K(snapshot_scn), K(schema_version));
  } else if (OB_FAIL(storage_.handle_dict_meta(tenant_meta, header))) {
    LOG_WARN("handle dict_tenant_meta failed", KR(ret), K(tenant_meta), K(header));
  } else {
    LOG_TRACE("handle dict_tenant_meta succ", KR(ret), K(tenant_meta), K(header));
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
    LOG_WARN("get_database_schemas_in_tenant failed", KR(ret));
  } else {
    const int64_t database_count = database_schemas.count();

    for (int i = 0; OB_SUCC(ret) && !stop_flag_ && i < database_count; i++) {
      const ObSimpleDatabaseSchema *database_schema = NULL;
      uint64_t database_id = OB_INVALID_ID;

      if (OB_FAIL(database_schemas.at(i, database_schema))) {
        LOG_WARN("get database_schema failed", KR(ret), K(i), K(database_count));
      } else if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid database_schema get from schema_guard", KR(ret), K(i), K(database_count));
      } else if (OB_UNLIKELY(OB_INVALID_ID == (database_id = database_schema->get_database_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid database_id get from database_schema", KR(ret), K(i), K(database_count), KPC(database_schema));
      } else if (OB_FAIL(database_ids.push_back(database_id))) {
        LOG_WARN("push_back database_id into list failed", KR(ret), K(i), K(database_count), KPC(database_schema));
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
      LOG_WARN("get_database_id failed", KR(ret), K(i), K(database_count), K(database_ids));
    } else if (OB_UNLIKELY(OB_INVALID_ID == database_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid database_id", KR(ret), K(i), K(database_count), K(database_ids));
    } else if (OB_FAIL(get_tenant_schema_guard_(schema_version, schema_guard, false/*is_force_fallback=false*/))) {
      LOG_WARN("get_tenant_schema_guard_ in lazy mode failed", KR(ret), K(schema_version), K(database_id));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, database_id, database_schema))) {
      LOG_WARN("get_database_schema failed", KR(ret), K(schema_version));
    } else if (OB_ISNULL(database_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid database_schema", KR(ret), K(i), K(database_count), K(database_id), KP(database_schema));
    } else if (OB_FAIL(db_meta.init(*database_schema))) {
      LOG_WARN("init database_meta failed", KR(ret), KPC(database_schema));
    } else if (OB_FAIL(storage_.handle_dict_meta(db_meta, header))) {
      LOG_WARN("handle dict_db_meta failed", KR(ret), K(db_meta), K(header), KPC(database_schema));
    } else {
      LOG_TRACE("handle dict_db_meta success", KR(ret), K(db_meta), K(header), KPC(database_schema));
    }
  }

  return ret;
}

int ObDataDictService::handle_table_metas_(
    const int64_t schema_version,
    const ObIArray<uint64_t> &table_ids,
    int64_t &filter_table_count)
{
  int ret = OB_SUCCESS;
  const int64_t total_table_count = table_ids.count();
  const int64_t start_time = get_timestamp_us();
  lib::ObMemAttr mem_attr(tenant_id_, "ObDatDictTbMeta");
  ObArenaAllocator tb_meta_allocator(mem_attr);
  static const int64_t batch_table_meta_size = 200;
  filter_table_count = 0;
  int64_t dump_succ_tb_cnt = 0;
  const static int64_t print_interval = 10 * _SEC_;
  schema::ObSchemaGetterGuard schema_guard; // will reset while getting schem_guard

  for (int i = 0; OB_SUCC(ret) && i < total_table_count; i++) {
    const ObTableSchema *table_schema = NULL;
    uint64_t table_id = OB_INVALID_ID;
    // NOTICE: get schema_guard for each table_meta in case of too much memory usage in schema_service.
    bool is_filtered = false;
    ObDictTableMeta table_meta(&tb_meta_allocator);
    ObDictMetaHeader header(ObDictMetaType::TABLE_META);

    if (i % batch_table_meta_size == 0) {
      tb_meta_allocator.reset();
      ob_usleep(100 * _MSEC_); // sleep 100ms to avoid too much cpu usage
    }

    if (OB_FAIL(table_ids.at(i, table_id))) {
      LOG_WARN("get_table_id failed", KR(ret), K(schema_version), K(i), K(table_ids));
    } else if (OB_FAIL(get_tenant_schema_guard_(schema_version, schema_guard, false/*is_force_fallback=false*/))) {
      LOG_WARN("get_tenant_schema_guard_ in lazy mode failed", KR(ret), K(schema_version), K(table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
      LOG_WARN("get_table_schema failed", KR(ret), K(table_id), K(schema_version));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid table_schema", KR(ret), K(schema_version), K(table_id));
    } else if (OB_FAIL(filter_table_(*table_schema, is_filtered))) {
      LOG_WARN("filter_table_ failed", KR(ret), K(is_filtered), KPC(table_schema));
    } else if (is_filtered) {
      filter_table_count++;
      LOG_DEBUG("filter_table_",
          K(schema_version),
          "table_id", table_schema->get_table_id(),
          "table_name", table_schema->get_table_name(),
          "table_type", table_schema->get_table_type());
      // ignore this table.
    } else if (OB_FAIL(table_meta.init(*table_schema, schema_version))) {
      LOG_WARN("init table_meta failed", KR(ret), K(schema_version), KPC(table_schema));
    } else if (OB_FAIL(storage_.handle_dict_meta(table_meta, header))) {
      LOG_WARN("handle dict_table_meta failed", KR(ret), K(table_meta), K(header), KPC(table_schema));
    } else {
      dump_succ_tb_cnt ++;
      LOG_DEBUG("handle dict_table_meta succ", KR(ret), K(table_meta), K(header), KPC(table_schema));
      if (REACH_TIME_INTERVAL(print_interval)) {
        int64_t cost_sec = (get_timestamp_us() - start_time) / _SEC_;
        _LOG_INFO("dump table_meta(schema_version=%ld) progress: %ld/%ld, success: %ld, filtered: %ld, cost: %ld sec",
          schema_version, i, total_table_count, dump_succ_tb_cnt, filter_table_count, cost_sec);
      }
      if (i == 0) {
        DEBUG_SYNC(BEFORE_DATA_DICT_DUMP_FINISH);
      }
    }

    if (OB_SUCC(ret) && stop_flag_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("data_dict service marked stop_flag, may already switch to follower", KR(ret), K_(stop_flag));
    }
  }

  LOG_INFO("handle_table_metas_ done", KR(ret), K(total_table_count), K(dump_succ_tb_cnt), K(filter_table_count), K_(stop_flag));

  return ret;
}

int ObDataDictService::filter_table_(const share::schema::ObTableSchema &table_schema, bool &is_filtered)
{
  int ret = OB_SUCCESS;

  is_filtered =
      ! (table_schema.has_tablet()
      || table_schema.is_user_table()
      || table_schema.is_unique_index()
      || table_schema.is_tmp_table()
      || table_schema.is_external_table());

  return ret;
}

void ObDataDictService::try_recycle_dict_history_()
{
  int ret = OB_SUCCESS;
  const int64_t data_dict_dump_history_retention_sec = ATOMIC_LOAD(&data_dict_dump_history_retention_sec_);

  if (OB_UNLIKELY(data_dict_dump_history_retention_sec<=0)) {
    LOG_INFO("won't recycle dict history because data_dict_dump_history_retention_sec is invalid", K_(data_dict_dump_history_retention_sec));
  } else {
    uint64_t delta = data_dict_dump_history_retention_sec * _SEC_ * NS_CONVERSION;
    share::SCN recycle_until_scn = share::SCN::minus(last_dump_succ_snapshot_scn_, delta);
    int64_t recycle_cnt = 0;

    if (OB_UNLIKELY(!recycle_until_scn.is_valid())) {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "recycle_until_scn is invalid",
          K(data_dict_dump_history_retention_sec), K(delta),
          K_(last_dump_succ_snapshot_scn), K(recycle_until_scn));
    } else if (OB_FAIL(sql_client_.recycle_hisotry_dict_info(tenant_id_, recycle_until_scn, recycle_cnt))) {
      LOG_WARN("execute recycle_hisotry_dict_info failed", KR(ret), K_(tenant_id), K(recycle_until_scn));
    } else {
      LOG_INFO("recycle_dict_history_ success", K(recycle_until_scn), K(data_dict_dump_history_retention_sec), K(recycle_cnt));
    }
  }
}

} // namespace datadict
} // namespace oceanbase
