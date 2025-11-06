// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX SERVER
#include "observer/ob_server.h"
#include "ob_temporary_table_cleaner.h"
#include "rootserver/ob_root_service.h"
#include "share/ob_table_access_helper.h"
#include "storage/tx_storage/ob_ls_service.h"


namespace oceanbase
{
namespace observer
{
ObNormalSessionTableCleanUpTask::ObNormalSessionTableCleanUpTask(ObSessionTmpTableCleaner &cleaner)
  : cleaner_(cleaner)
{
}

void ObNormalSessionTableCleanUpTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleaner_.normal_table_clean())) {
    LOG_WARN("fail to normal table clean", KR(ret));
  }
  if (OB_FAIL(cleaner_.schedule_normal_table_clean())) {
    // overwrite ret
    LOG_WARN("fail to schedule normal table clean", KR(ret));
  }
}

ObStandAloneSessionTableCleanUpTask::ObStandAloneSessionTableCleanUpTask(ObSessionTmpTableCleaner &cleaner)
  : cleaner_(cleaner)
{
}

void ObStandAloneSessionTableCleanUpTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleaner_.standalone_table_clean())) {
    LOG_WARN("fail to standalone table clean", KR(ret));
  }
  if (OB_FAIL(cleaner_.schedule_standalone_table_clean())) {
    // overwrite ret
    LOG_WARN("fail to schedule standalone table clean", KR(ret));
  }
}

ObDynamicScheduleCleanUpTask::ObDynamicScheduleCleanUpTask(ObSessionTmpTableCleaner &cleaner)
  : cleaner_(cleaner)
{
}

void ObDynamicScheduleCleanUpTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = cleaner_.get_tenant_id();
  int64_t schedule_interval = cleaner_.get_schedule_interval();
  int64_t interval = -1;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cleaner tenant id", KR(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    interval = tenant_config->_session_temp_table_clean_interval;
    if (schedule_interval != interval) {
      LOG_INFO("interval not match", K(schedule_interval), K(interval));
      cleaner_.set_schedule_interval(interval);
      if (OB_FAIL(cleaner_.cancel_and_schedule_task())) {
        cleaner_.set_schedule_interval(schedule_interval);
        LOG_WARN("fail to cancle task", KR(ret));
      }
    }
  }
}

int ObSessionTmpTableCleaner::mtl_init(ObSessionTmpTableCleaner *&cleaner)
{
  return cleaner->init(GCTX.schema_service_);
}

int ObSessionTmpTableCleaner::init(share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(schema_service)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("schema_service is null", KR(ret));
    } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::CTASCleanUpTimer, clean_tg_id_))) {
      LOG_WARN("TG_CREATE_TENANT CTASCleanupTimer failed", KR(ret));
    } else if (OB_FAIL(tenant_sessions_.create(10, attr_))) {
      LOG_WARN("hashset create fail", KR(ret));
    } else {
      tenant_id_ = MTL_ID();
      schema_service_ = schema_service;
      inited_ = true;
    }
  }
  return ret;
}

int ObSessionTmpTableCleaner::start()
{
  int ret = OB_SUCCESS;
  const int64_t SCHEDULE_INTERVAL = 10L * 1000L * 1000L;//10s
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_FAIL(TG_START(clean_tg_id_))) {
      LOG_WARN("fail to start clean_tg_id_", KR(ret), K_(clean_tg_id));
    } else if (OB_FAIL(schedule_normal_table_clean())) {
      LOG_WARN("fail to schedule normal table clean", KR(ret));
    } else if (OB_FAIL(schedule_standalone_table_clean())) {
      LOG_WARN("fail to schedule stand alone table clean", KR(ret));
    } else if (OB_FAIL(TG_SCHEDULE(
                       clean_tg_id_,
                       dynamic_schedule_,
                       SCHEDULE_INTERVAL,
                       true /*repeat*/))) {
      LOG_WARN("TG_SCHEDULE dynamic schedule task failed", KR(ret), K_(clean_tg_id), K(SCHEDULE_INTERVAL));
    } else {
      stopped_ = false;
      LOG_INFO("ObSessionTmpTableCleaner start success", KR(ret));
    }
  }
  return ret;
}

void ObSessionTmpTableCleaner::stop()
{
  if (OB_LIKELY(inited_)) {
    stopped_ = true;
    TG_STOP(clean_tg_id_);
    LOG_INFO("ObSessionTmpTableCleaner stop finished",
              K_(tenant_id), K_(clean_tg_id));
  }
}

void ObSessionTmpTableCleaner::wait()
{
  if (OB_LIKELY(inited_)) {
    stopped_ = true;
    TG_WAIT(clean_tg_id_);
    LOG_INFO("ObSessionTmpTableCleaner wait finished",
              K_(tenant_id), K_(clean_tg_id));
  }
}

void ObSessionTmpTableCleaner::destroy()
{
  if (inited_) {
    tenant_sessions_.destroy();
    schema_service_ = nullptr;
    inited_ = false;
    stopped_ = true;
    TG_DESTROY(clean_tg_id_);
    clean_tg_id_ = OB_INVALID_INDEX;
    tenant_id_ = OB_INVALID_TENANT_ID;
  }
}

ObSessionTmpTableCleaner::~ObSessionTmpTableCleaner()
{
  destroy();
}

int ObSessionTmpTableCleaner::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  bool is_standby_tenant = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_servier_ is null", KR(ret));
  }
  return ret;
}

int ObSessionTmpTableCleaner::cancel_and_schedule_task()
{
  int ret = OB_SUCCESS;
  bool exist = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(TG_TASK_EXIST(clean_tg_id_, normal_table_clean_, exist))) {
    LOG_WARN("fail to check normal table clean task exist", KR(ret), K_(clean_tg_id));
  } else if (exist) {
    TG_CANCEL_TASK(clean_tg_id_, normal_table_clean_);
    LOG_INFO("cancel task normal table clean");
  }
  if (FAILEDx(schedule_normal_table_clean())) {
    LOG_WARN("TG_SCHEDULE normal_table_clean_ failed", KR(ret), K_(clean_tg_id), K_(schedule_interval));
  }
  exist = false;
  if (OB_FAIL(TG_TASK_EXIST(clean_tg_id_, standalone_table_clean_, exist))) { // overwrite ret
    LOG_WARN("fail to check standalone table clean task exist", KR(ret), K_(clean_tg_id));
  } else if (exist) {
    TG_CANCEL_TASK(clean_tg_id_, standalone_table_clean_);
    LOG_INFO("cancel task standalone table clean");
  }
  if (FAILEDx(schedule_standalone_table_clean())) {
    LOG_WARN("TG_SCHEDULE standalone_table_clean_ failed", KR(ret), K_(clean_tg_id), K_(schedule_interval));
  }
  return ret;
}

int ObSessionTmpTableCleaner::schedule_normal_table_clean()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObSessionTmpTableCleaner is stopped", KR(ret), K_(tenant_id), K_(clean_tg_id));
  } else if (0 == schedule_interval_) {
    // do nothing
  } else if (OB_FAIL(TG_SCHEDULE(
                     clean_tg_id_,
                     normal_table_clean_,
                     schedule_interval_,
                     false /*repeat*/))) {
    LOG_WARN("TG_SCHEDULE normal_table_clean_ failed", KR(ret), K_(clean_tg_id), K(schedule_interval_));
  }
  return ret;
}
int ObSessionTmpTableCleaner::schedule_standalone_table_clean()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObSessionTmpTableCleaner is stopped", KR(ret), K_(tenant_id), K_(clean_tg_id));
  } else if (0 == schedule_interval_) {
    // do nothing
  } else if (OB_FAIL(TG_SCHEDULE(
                     clean_tg_id_,
                     standalone_table_clean_,
                     schedule_interval_,
                     false /*repeat*/))) {
    LOG_WARN("TG_SCHEDULE normal_table_clean_ failed", KR(ret), K_(clean_tg_id), K(schedule_interval_));
  }
  return ret;
}

int ObSessionTmpTableCleaner::register_task(const uint64_t table_session_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(tenant_sessions_.set_refactored(table_session_id))) {
    LOG_WARN("fail to register session", KR(ret), K(table_session_id));
  }
  return ret;
}

int ObSessionTmpTableCleaner::remove_task(const uint64_t table_session_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(tenant_sessions_.erase_refactored(table_session_id))) {
    LOG_WARN("fail to register session", KR(ret), K(table_session_id));
  }
  return ret;
}

int ObSessionTmpTableCleaner::check_enable_clean_( bool &enable_clean)
{
  int ret = OB_SUCCESS;
  enable_clean = false;
  bool is_oracle_mode = false;
  uint64_t data_version = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
    LOG_INFO("not primary tenant, temp table clean skip", K_(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("get tenant data version failed", KR(ret), K_(tenant_id));
  } else if (data_version < DATA_VERSION_4_3_5_4) {
    // skip
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode with tenant_id ", KR(ret), K_(tenant_id));
  } else if (is_oracle_mode) {
    // skip
  } else {
    enable_clean = true;
  }
  return ret;
}

int ObSessionTmpTableCleaner::normal_table_clean()
{
  int ret = OB_SUCCESS;
  bool enable_clean = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_enable_clean_(enable_clean))) {
    LOG_WARN("fail to check enable clean", KR(ret));
  } else if (enable_clean) {
    ObArray<uint64_t> session_ids;
    {
      SpinRLockGuard guard(lock_);
      common::hash::ObHashSet<uint64_t>::const_iterator it = tenant_sessions_.begin();
      for(; it != tenant_sessions_.end() && OB_SUCC(ret); it ++) {
        uint64_t table_session_id = it->first;
        if (OB_FAIL(session_ids.push_back(table_session_id))) {
          LOG_WARN("fail to push back table_sessions", KR(ret));
        }
      }
    }
    for (int i = 0; i < session_ids.count() && OB_SUCC(ret); ++i) {
      int tmp_ret = OB_SUCCESS;
      uint64_t table_session_id = session_ids[i];
      const uint32_t sessid = table_sid_to_client_sid_(table_session_id);
      LOG_TRACE("clean session", K_(tenant_id), K(table_session_id), K(ObSessionAliveChecker::is_proxy_sid(sessid)));
      if (!ObSessionAliveChecker::is_proxy_sid(sessid)) {
        if (OB_TMP_FAIL(clean_up_session_invalid_tables_(table_session_id, true /*check host*/))) {
          LOG_WARN("fail to clean session invalid table", KR(tmp_ret), K_(tenant_id), K(table_session_id));
        }
      } else {
        bool is_alive = true;
        if (OB_TMP_FAIL(detect_session_alive_(table_session_id, false /*local_only*/, nullptr /*dest_server*/, is_alive))) {
          LOG_WARN("fail to detect_session_alive", KR(tmp_ret), K_(tenant_id), K(table_session_id));
        } else if (!is_alive && OB_TMP_FAIL(clean_up_session_invalid_tables_(table_session_id, true /*check host*/))) {
          LOG_WARN("fail to clean session invalid table", KR(tmp_ret), K_(tenant_id), K(table_session_id));
        }
      }
    }
  } else {
    LOG_TRACE("not enable to clean", KR(ret));
  }
  return ret;
}

int ObSessionTmpTableCleaner::standalone_table_clean()
{
  int ret = OB_SUCCESS;
  bool enable_clean = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_enable_clean_(enable_clean))) {
    LOG_WARN("fail to check enable clean", KR(ret));
  } else if (enable_clean) {

    if (OB_FAIL(gather_residual_sessions_local_())) {
      LOG_WARN("fail to gather sessions", KR(ret));
    }
    // ignore write
    ret = OB_SUCCESS;
    bool is_leader = false;
    if (OB_FAIL(is_sys_ls_leader_(is_leader))) {
      LOG_WARN("fail to get ls leader", KR(ret));
    } else if (is_leader && OB_FAIL(gather_residual_sessions_global_())) {
      LOG_WARN("fail to gather sessions", KR(ret));
    }
  } else {
    LOG_TRACE("not enable to clean", KR(ret));
  }
  return ret;
}

int ObSessionTmpTableCleaner::clean_up_session_invalid_tables_(const uint64_t table_session_id,
                                                               const bool check_host)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tables;
  LOG_TRACE("clean session", K_(tenant_id), K(table_session_id));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(gather_tables_(table_session_id, check_host, tables))) {
    LOG_WARN("fail to gather tables", KR(ret), K_(tenant_id), K(table_session_id));
  } else if (OB_FAIL(drop_tables_(table_session_id, tables))) {
    LOG_WARN("fail to drop tables", KR(ret));
  } else if (OB_FAIL(remove_task(table_session_id))) {
    LOG_WARN("fail to remove task", KR(ret), K_(tenant_id), K(table_session_id));
  }
  return ret;
}

int ObSessionTmpTableCleaner::gather_residual_sessions_local_()
{
  int ret = OB_SUCCESS;
  char full_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  const uint64_t cond_length = MAX_IP_PORT_SQL_LENGTH + 128;
  char where_cond[cond_length] = {'\0'};
  char create_host_str[OB_MAX_HOST_NAME_LENGTH];
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(MYADDR.ip_port_to_string(create_host_str, OB_MAX_HOST_NAME_LENGTH))) {
    LOG_WARN("fail to do ip port to string", KR(ret));
  } else if (OB_FAIL(databuff_printf(full_table_name,
    OB_MAX_TABLE_NAME_BUF_LENGTH,
    "%s.%s",
    OB_SYS_DATABASE_NAME,
    OB_ALL_TEMP_TABLE_TNAME))) {
    LOG_WARN("generate full table_name failed", K(OB_SYS_DATABASE_NAME), K(OB_ALL_TEMP_TABLE_TNAME));
  } else if (OB_FAIL(databuff_printf(where_cond, cond_length, "where create_host = \"%s\" group by table_session_id", create_host_str))) {
    LOG_WARN("generate where condition failed", KR(ret));
  } else {
    ObArray<ObTuple<uint64_t>> table_session_ids;
    if (OB_FAIL(ObTableAccessHelper::read_multi_row(tenant_id_,
                                                    {"table_session_id"},
                                                    full_table_name,
                                                    where_cond,
                                                    table_session_ids))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("read from inner table __all_temp_table failed", KR(ret), K_(tenant_id));
      }
    } else {
      for (int64_t i = 0; i < table_session_ids.count(); i++) {
        uint64_t table_session_id = table_session_ids[i].element<0>();
        bool is_alive = true;
        int tmp_ret = OB_SUCCESS;
        const uint32_t sessid = table_sid_to_client_sid_(table_session_id);
        if (OB_TMP_FAIL(detect_session_alive_(table_session_id, !ObSessionAliveChecker::is_proxy_sid(sessid), nullptr/*dest_server*/, is_alive))) {
          LOG_WARN("fail to detect_session_alive", KR(ret), K_(tenant_id), K(table_session_id));
        } else if (!is_alive && OB_TMP_FAIL(clean_up_session_invalid_tables_(table_session_id, true/*check host*/))) {
          LOG_WARN("fail to clean session invalid table", KR(ret), K_(tenant_id), K(table_session_id));
        }
      }
    }
  }
  return ret;
}

int ObSessionTmpTableCleaner::gather_residual_sessions_global_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  char full_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  ObArray<ObAddr> server_list;
  const uint64_t cond_length = MAX_IP_PORT_SQL_LENGTH * MAX_SERVER_COUNT + 128;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObTableLockDetectFuncList::get_tenant_servers(tenant_id_, server_list))) {
    LOG_WARN("fail to get tenant server", KR(ret), K_(tenant_id));
  } else {
    ObSqlString sql;
    int count = server_list.count();
    for (uint64_t i = 0; i < count && OB_SUCC(ret); i++) {
      char create_host_str[OB_MAX_HOST_NAME_LENGTH];
      if (OB_FAIL(server_list[i].ip_port_to_string(create_host_str, OB_MAX_HOST_NAME_LENGTH))) {
        LOG_WARN("fail to do ip port to string", KR(ret));
      } else if (OB_FAIL(sql.append_fmt("%s \"%s\"", 0 == i ? "" : ", ",
        create_host_str))) {
          LOG_WARN("append sql failed", KR(ret));
        }
      }
    SMART_VAR(char[cond_length], where_cond) {
      where_cond[0] = '\0';
      if (FAILEDx(databuff_printf(full_table_name,
        OB_MAX_TABLE_NAME_BUF_LENGTH,
        "%s.%s",
        OB_SYS_DATABASE_NAME,
        OB_ALL_TEMP_TABLE_TNAME))) {
        LOG_WARN("generate full table_name failed", K(OB_SYS_DATABASE_NAME), K(OB_ALL_TEMP_TABLE_TNAME));
      } else if (OB_FAIL(databuff_printf(where_cond, cond_length, "where create_host not in (%s) order by create_host", sql.ptr()))) {
        LOG_WARN("generate where condition failed", KR(ret));
      } else {
        ObArray<ObTuple<uint64_t, int64_t, ObStringHolder >> session_id_table_id_hosts;
        if (OB_FAIL(ObTableAccessHelper::read_multi_row(tenant_id_,
                                                        {"table_session_id", "table_id", "create_host"},
                                                          full_table_name,
                                                          where_cond,
                                                          session_id_table_id_hosts))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("read from inner table __all_temp_table failed", KR(ret), K_(tenant_id));
          }
        } else {
          for (int64_t i = 0; i < session_id_table_id_hosts.count(); i++) {
            uint64_t session_id = session_id_table_id_hosts[i].element<0>();
            uint64_t table_id = static_cast<uint64_t>(session_id_table_id_hosts[i].element<1>());
            ObString create_host = session_id_table_id_hosts[i].element<2>().get_ob_string();
            ObAddr addr;
            if (OB_FAIL(addr.parse_from_string(create_host))) {
              LOG_WARN("fail to parse addr from string", KR(ret));
            } else if (OB_FAIL(clean_residual_table_(addr, session_id, table_id, create_host, server_list))) {
              LOG_WARN("fail to clean residul table", KR(ret), K(addr), K(session_id), K(table_id), K(create_host), K(server_list));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSessionTmpTableCleaner::clean_residual_table_(const ObAddr addr, const uint64_t session_id, const uint64_t table_id,
                                                    const ObString &create_host, const ObArray<ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tables;
  ObServerInfoInTable server_info;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(ObServerTableOperator::get(*GCTX.sql_proxy_, addr, server_info))) {
    if (OB_SERVER_NOT_IN_WHITE_LIST == ret) {
      ret = OB_SUCCESS;
      const uint32_t sessid = table_sid_to_client_sid_(session_id);
      if (!ObSessionAliveChecker::is_proxy_sid(sessid)) {
        if (OB_FAIL(tables.push_back(table_id))) {
          LOG_WARN("fail to push back table id", KR(ret), K(table_id));
        }
      } else {
        bool is_alive = true;
        if (OB_FAIL(detect_session_alive_(session_id, false /*only local*/,
                                          nullptr /*dest svr*/, is_alive))) {
            LOG_WARN("fail to detect session alive", KR(ret), K(session_id), K(server_list));
        } else if (!is_alive) {
          if (OB_FAIL(tables.push_back(table_id))) {
            LOG_WARN("fail to push back table id", KR(ret), K(table_id));
          }
        }
      }
    } else {
      LOG_WARN("fail to get server info", KR(ret));
    }
  } else {
    const uint32_t sessid = table_sid_to_client_sid_(session_id);
    ObArray<ObAddr> temp_server_list;
    if (server_info.is_active()) {
      bool is_alive = true;
      if (OB_FAIL(temp_server_list.assign(server_list))) {
        LOG_WARN("fail to assign server list", KR(ret));
      } else if (OB_FAIL(temp_server_list.push_back(addr))) {
        LOG_WARN("fail to push back addr", KR(ret), K(addr));
      } else if (OB_FAIL(detect_session_alive_(session_id, false /*only local*/,
                                               &temp_server_list, is_alive))) {
        LOG_WARN("fail to detect session alive", KR(ret), K(session_id), K(temp_server_list));
      } else if (!is_alive) {
        if (OB_FAIL(tables.push_back(table_id))) {
          LOG_WARN("fail to push back table id", KR(ret), K(table_id));
        }
      }
    } else {
      LOG_WARN("server is inactive or permanent_offline skip it", K(addr), K(sessid));
      // can't figure out what happend, do nothing
    }
  }
  if (FAILEDx(drop_tables_(session_id, tables))) {
    LOG_WARN("fail to drop table", KR(ret), K(session_id), K(tables));
  }
  return ret;
}

int ObSessionTmpTableCleaner::gather_tables_(const uint64_t table_session_id,
                                             const bool check_host,
                                             ObArray<uint64_t> &tables)
{
  int ret = OB_SUCCESS;
  const uint64_t cond_length = MAX_IP_PORT_SQL_LENGTH + 128;
  char where_cond[cond_length] = {'\0'};
  ObArray<ObTuple<int64_t, ObStringHolder >> table_id_host;
  char full_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  char create_host_str[OB_MAX_HOST_NAME_LENGTH];
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(MYADDR.ip_port_to_string(create_host_str, OB_MAX_HOST_NAME_LENGTH))) {
    LOG_WARN("fail to do ip port to string", KR(ret));
  } else if (OB_FAIL(databuff_printf(full_table_name,
                              OB_MAX_TABLE_NAME_BUF_LENGTH,
                              "%s.%s",
                              OB_SYS_DATABASE_NAME,
                              OB_ALL_TEMP_TABLE_TNAME))) {
    LOG_WARN("generate full table_name failed", K(OB_SYS_DATABASE_NAME), K(OB_ALL_TEMP_TABLE_TNAME));
  } else if (OB_FAIL(databuff_printf(where_cond, cond_length, "WHERE table_session_id = %ld", table_session_id))) {
    LOG_WARN("generate where condition failed", K(table_session_id));
  } else if (OB_FAIL(ObTableAccessHelper::read_multi_row(tenant_id_,
                                                         {"table_id", "create_host"},
                                                         full_table_name,
                                                         where_cond,
                                                         table_id_host))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("read from inner table __all_temp_table failed", KR(ret), K_(tenant_id));
    }
  } else {
    for (int64_t i = 0; i < table_id_host.count() && OB_SUCC(ret); i++) {
      uint64_t table_id = static_cast<uint64_t>(table_id_host[i].element<0>());
      ObString create_host = table_id_host[i].element<1>().get_ob_string();
      if (check_host && (0 != create_host.compare(create_host_str))) {
        LOG_INFO("different observer with the same server session, just skip", K(create_host), K(create_host_str));
      } else if (OB_FAIL(tables.push_back(table_id))) {
        LOG_WARN("push back table_id failed", KR(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObSessionTmpTableCleaner::drop_tables_(const uint64_t table_session_id, const ObArray<uint64_t> &tables)
{
  int ret = OB_SUCCESS;
  obrpc::ObDropTableArg drop_table_arg;
  drop_table_arg.if_exist_ = false;
  drop_table_arg.tenant_id_ = tenant_id_;
  drop_table_arg.to_recyclebin_ = false;
  drop_table_arg.table_type_ = share::schema::TMP_TABLE;
  drop_table_arg.session_id_ = table_session_id;
  drop_table_arg.exec_tenant_id_ = tenant_id_;
  bool &is_parallel_drop = drop_table_arg.is_parallel_;
  for (uint64_t i = 0; i < tables.count(); i++) {
    uint64_t table_id = tables.at(i);
    obrpc::ObTableItem table_item;
    ObSchemaGetterGuard schema_guard;
    const ObSimpleDatabaseSchema *database_schema = nullptr;
    const ObSimpleTableSchemaV2 *table_schema = nullptr;
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", KR(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret));
    } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", KR(ret),  K_(tenant_id), K(table_id));
    } else if (table_session_id != table_schema->get_session_id()) {
      LOG_WARN("session id not match skip drop", K_(tenant_id), K(table_schema->get_table_name()),
               K(table_id), K(table_session_id), K(table_schema->get_session_id()));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, table_schema->get_database_id(), database_schema))) {
      LOG_WARN("fail to get database schema", KR(ret), K_(tenant_id), K(table_schema->get_database_id()));
    } else if (table_schema->is_mysql_tmp_table() || table_schema->is_ctas_tmp_table()) {
      if (USER_TABLE == table_schema->get_table_type()) {
        // CTAS table
        drop_table_arg.table_type_ = share::schema::USER_TABLE;
      }
      table_item.database_name_ = database_schema->get_database_name();
      table_item.table_name_ = table_schema->get_table_name();
      if (OB_FAIL(drop_table_arg.tables_.push_back(table_item))) {
        LOG_WARN("failed to add table item!", KR(ret), K(table_item));
      }
    }
  }

  uint64_t start_time = ObTimeUtility::current_time();
  obrpc::ObDDLRes drop_table_res;
  obrpc::ObDropTableRes parallel_drop_table_res;
  if (OB_SUCC(ret) && !tables.empty()) {
    if (!ObSchemaUtils::is_support_parallel_drop(drop_table_arg.table_type_)) {
      is_parallel_drop = false;
    }
    if (OB_FAIL(ObParallelDDLControlMode::is_parallel_ddl_enable(ObParallelDDLControlMode::DROP_TABLE, tenant_id_, is_parallel_drop))) {
      LOG_WARN("fail to check whether parallel drop table enable", KR(ret), K(tenant_id_));
    } else if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KP(GCTX.rs_rpc_proxy_));
    } else if (!is_parallel_drop && OB_FAIL(GCTX.rs_rpc_proxy_
               ->to(GCTX.self_addr())
               .timeout(GCONF._ob_ddl_timeout)
               .drop_table(drop_table_arg, drop_table_res))) {
      LOG_WARN("failed to drop temporary table", KR(ret), K(drop_table_arg));
    } else if (is_parallel_drop && OB_FAIL(GCTX.rs_rpc_proxy_
               ->to(GCTX.self_addr())
               .timeout(GCONF._ob_ddl_timeout)
               .parallel_drop_table(drop_table_arg, parallel_drop_table_res))) {
               LOG_WARN("failed to drop temporary table", KR(ret), K(drop_table_arg));
    } else {
      uint64_t cost = ObTimeUtility::current_time() - start_time;
      LOG_INFO("temp table is successfully dropped", K(drop_table_arg), K(drop_table_res), K(parallel_drop_table_res), K(cost));
    }
  }
  return ret;
}

int ObSessionTmpTableCleaner::detect_session_alive_(const uint64_t table_session_id, bool local_only, const ObArray<ObAddr> *dest_server, bool &is_alive)
{
  int ret = OB_SUCCESS;
  const uint32_t sessid = table_sid_to_client_sid_(table_session_id);
  if (local_only) {
    obrpc::Bool tmp_is_alive;
    if (OB_FAIL(ObTableLockDetectFuncList::do_session_at_least_one_alive_detect_for_rpc(sessid, tmp_is_alive))) {
      LOG_WARN("fail to detect session_alive", KR(ret), K(sessid));
    } else {
      is_alive = tmp_is_alive;
    }
  } else {
    if (OB_FAIL(ObTableLockDetectFuncList::do_session_at_least_one_alive_detect(tenant_id_, sessid, dest_server, is_alive))) {
      LOG_WARN("fail to detect session alive", KR(ret), K_(tenant_id), K(sessid), K(dest_server));
    } else {
      LOG_TRACE("detect session success", K(table_session_id), K(sessid), K(is_alive));
    }
  }
  return ret;
}

int ObSessionTmpTableCleaner::is_sys_ls_leader_(bool &is_leader)
{
  int ret = OB_SUCCESS;
  is_leader = false;
  common::ObRole role= FOLLOWER;
  int64_t proposal_id = 0;
  MTL_SWITCH(tenant_id_) {
    ObLSService *ls_svr = MTL(ObLSService*);
    ObLS *ls = NULL;
    ObLSHandle handle;
    logservice::ObLogHandler *log_handler = NULL;
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mtl ObLSService should not be null", KR(ret), KP(ls_svr));
    } else if (OB_FAIL(ls_svr->get_ls(SYS_LS, handle, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("get ls failed", KR(ret));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be null", KR(ret));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log_handler is null", KR(ret), KP(log_handler));
    } else if (OB_FAIL(log_handler->get_role(role, proposal_id))) {
      LOG_WARN("fail to get role and epoch", KR(ret));
    } else {
      is_leader = is_strong_leader(role);
    }
  }
  return ret;
}

} // end observer
} // end oceanbase
