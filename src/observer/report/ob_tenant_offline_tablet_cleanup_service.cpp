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

#define USING_LOG_PREFIX SERVER

#include "ob_tenant_offline_tablet_cleanup_service.h"
#include "share/tablet/ob_tablet_table_operator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/omt/ob_multi_tenant.h"
#include "rootserver/ob_lost_replica_checker.h"
#include "logservice/ob_log_service.h"
#include "share/ob_ls_id.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace observer
{

// ==================== ObTenantOfflineTabletCleanupTask ====================
void ObTenantOfflineTabletCleanupTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = service_.get_tenant_id();
  if (service_.is_stop()) {
    LOG_INFO("service is stopped, skip cleanup", K(tenant_id));
  } else if (!is_sys_ls_leader_()) {
    // Only sys ls leader should execute the cleanup task to avoid duplicate execution
    LOG_DEBUG("current server is not SYS LS leader, skip cleanup", K(tenant_id));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    if (OB_FAIL(cleanup_offline_tablet_meta_and_checksum_())) {
      LOG_WARN("failed to cleanup offline tablet meta and checksum", KR(ret), K(tenant_id));
    }
    const int64_t cost_time = ObTimeUtility::current_time() - start_time;
    LOG_INFO("finish cleanup offline tablet meta table and replica checksum table", KR(ret), K(tenant_id), K(cost_time));
  }
}

int ObTenantOfflineTabletCleanupTask::cleanup_offline_tablet_meta_and_checksum_()
{
  int ret = OB_SUCCESS;
  ObArray<std::pair<ObLSID, ObAddr>> offline_tablet_meta_ls_servers;
  if (OB_FAIL(get_offline_servers_(offline_tablet_meta_ls_servers))) {
    LOG_WARN("failed to get offline servers", KR(ret));
  } else if (offline_tablet_meta_ls_servers.size() > 0) {
    FOREACH_CNT_X(pair, offline_tablet_meta_ls_servers, OB_SUCC(ret)) {
      const ObLSID &ls_id = pair->first;
      const ObAddr &server = pair->second;
      if (OB_FAIL(cleanup_offline_tablet_table_(ls_id, server, OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME))) {
        LOG_WARN("failed to cleanup tablet replica checksum table", KR(ret), K(ls_id), K(server));
      } else if (OB_FAIL(cleanup_offline_tablet_table_(ls_id, server, OB_ALL_TABLET_META_TABLE_TNAME))) {
        LOG_WARN("failed to cleanup tablet meta table", KR(ret), K(ls_id), K(server));
      } else {
        LOG_INFO("successfully cleaned up tablet info for offline server", "tenant_id", service_.get_tenant_id(), K(ls_id), K(server));
      }
    }
  }
  return ret;
}

int ObTenantOfflineTabletCleanupTask::get_offline_servers_(
    ObArray<std::pair<ObLSID, ObAddr>> &offline_servers)
{
  int ret = OB_SUCCESS;
  ObArray<std::pair<ObLSID, ObAddr>> tablet_meta_ls_servers;
  if (OB_FAIL(get_candidate_offline_pairs_(tablet_meta_ls_servers))) {
    LOG_WARN("failed to get ls server pairs", KR(ret));
  } else {
    FOREACH_CNT_X(pair, tablet_meta_ls_servers, OB_SUCC(ret)) {
      const ObAddr &server = pair->second;
      bool is_lost_server = false;
      if (OB_FAIL(rootserver::ObLostReplicaChecker::check_lost_server(server, is_lost_server))) {
        LOG_WARN("failed to check lost server", KR(ret), K(server));
      } else if (is_lost_server && OB_FAIL(offline_servers.push_back(std::make_pair(pair->first, server)))) {
        LOG_WARN("failed to push back offline server", KR(ret), K(server));
      }
    }
  }
  return ret;
}

int ObTenantOfflineTabletCleanupTask::get_candidate_offline_pairs_(
    ObArray<std::pair<ObLSID, ObAddr>> &pairs)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = service_.get_tenant_id();
  ObSqlString sql;
  ObTabletTableOperator *tt_operator = GCTX.tablet_operator_;
  if (OB_ISNULL(tt_operator) || OB_ISNULL(tt_operator->get_sql_client())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet table operator or sql client is null", KR(ret), KP(tt_operator));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT DISTINCT t.ls_id, t.svr_ip, t.svr_port "
      "FROM %s t "
      "WHERE t.tenant_id = %lu "
      "AND NOT EXISTS ("
        "SELECT 1 FROM %s l "
        "WHERE l.tenant_id = t.tenant_id "
        "AND l.ls_id = t.ls_id "
        "AND l.svr_ip = t.svr_ip "
        "AND l.svr_port = t.svr_port"
      ")",
      OB_ALL_TABLET_META_TABLE_TNAME,
      tenant_id,
      OB_ALL_LS_META_TABLE_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql), K(tenant_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(tt_operator->get_sql_client()->read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret), K(sql), K(meta_tenant_id), K(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            }
            LOG_WARN("failed to get next row", KR(ret), K(sql), K(tenant_id));
          } else {
            int64_t ls_id = 0;
            ObAddr server;
            int64_t tmp_real_str_len = 0;
            char ip_buf[common::MAX_IP_ADDR_LENGTH + 2] = "";
            int64_t port = 0;
            EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
            EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip_buf, common::MAX_IP_ADDR_LENGTH + 2, tmp_real_str_len);
            EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int64_t);
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to extract field", KR(ret), K(tenant_id), K(sql));
            } else if (OB_UNLIKELY(!server.set_ip_addr(ip_buf, static_cast<int32_t>(port)))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to set server addr", KR(ret), K(tenant_id), K(ip_buf), K(port));
            } else if (OB_FAIL(pairs.push_back(std::make_pair(ObLSID(ls_id), server)))) {
              LOG_WARN("failed to push back pair", KR(ret), K(tenant_id), K(ls_id), K(server));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantOfflineTabletCleanupTask::cleanup_offline_tablet_table_(
    const ObLSID &ls_id,
    const ObAddr &server,
    const char *table_name)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = service_.get_tenant_id();
  ObTabletTableOperator *tt_operator = GCTX.tablet_operator_;
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(tenant_id), K(server));
  } else if (OB_ISNULL(table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table name", KR(ret), K(tenant_id), KP(table_name));
  } else if (OB_ISNULL(tt_operator) || OB_ISNULL(tt_operator->get_sql_client())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet table operator or sql client is null", KR(ret), K(tenant_id), KP(tt_operator));
  } else {
    const int64_t batch_limit = 500;
    int64_t affected_rows = 0;
    int64_t total_affected_rows = 0;
    do {
      affected_rows = 0;
      if (OB_FAIL(tt_operator->remove_residual_tablet(*tt_operator->get_sql_client(),
                                                       tenant_id, ls_id, server, batch_limit,
                                                       table_name, affected_rows))) {
        LOG_WARN("failed to remove residual tablet", KR(ret), K(tenant_id), K(ls_id), K(server), K(table_name));
      } else {
        total_affected_rows += affected_rows;
      }
    } while (OB_SUCC(ret) && (affected_rows == batch_limit));
    if (OB_SUCC(ret) && total_affected_rows > 0) {
      LOG_INFO("finish cleanup offline tablet table", K(tenant_id), K(ls_id), K(server), K(table_name), K(total_affected_rows));
    }
  }
  return ret;
}

bool ObTenantOfflineTabletCleanupTask::is_sys_ls_leader_()
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
  ObRole role = ObRole::INVALID_ROLE;
  int64_t proposal_id = palf::INVALID_PROPOSAL_ID;
  bool is_leader = false;
  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logservice is null, unexpected", KR(ret), "tenant_id", service_.get_tenant_id());
  } else if (OB_FAIL(log_service->get_palf_role(SYS_LS, role, proposal_id))) {
    LOG_WARN("failed to get sys_ls role", KR(ret), "tenant_id", service_.get_tenant_id());
  } else {
    is_leader = is_leader_by_election(role);
  }
  return is_leader;
}

// ==================== ObTenantTabletCleanupService ====================
ObTenantTabletCleanupService::ObTenantTabletCleanupService(uint64_t tenant_id)
  : tenant_id_(tenant_id),
    cleanup_task_(*this),
    timer_(),
    is_stop_(false),
    is_inited_(false)
{
}

ObTenantTabletCleanupService::~ObTenantTabletCleanupService()
{
  destroy();
}

int ObTenantTabletCleanupService::mtl_new(ObTenantTabletCleanupService *&service)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObMemAttr attr(tenant_id, ObModIds::OMT_TENANT);
  service = OB_NEW(ObTenantTabletCleanupService, attr, tenant_id);
  if (OB_ISNULL(service)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObTenantTabletCleanupService", K(ret), K(tenant_id));
  }
  return ret;
}

void ObTenantTabletCleanupService::mtl_destroy(ObTenantTabletCleanupService *&service)
{
  if (OB_UNLIKELY(nullptr == service)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "service is nullptr", KP(service));
  } else {
    service->destroy();
    OB_DELETE(ObTenantTabletCleanupService, ObModIds::OMT_TENANT, service);
    service = nullptr;
  }
}

int ObTenantTabletCleanupService::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(tenant_id));
  } else {
    is_inited_ = true;
    LOG_INFO("ObTenantTabletCleanupService init success", K_(tenant_id));
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_OFFLINE_TABLET_CLEANUP_SPEED_UP);
int ObTenantTabletCleanupService::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(timer_.set_run_wrapper_with_ret(MTL_CTX()))) {
    LOG_WARN("fail to set timer's run wrapper", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(timer_.init("OfflnClnTSer", ObMemAttr(tenant_id_, "OfflnClnTSer")))) {
    LOG_WARN("fail to init timer", KR(ret), K_(tenant_id));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to start ObTenantTabletCleanupService", KR(ret), K_(tenant_id));
#ifdef ERRSIM
  } else if (ERRSIM_OFFLINE_TABLET_CLEANUP_SPEED_UP) {
    const int64_t FAST_CLEANUP_INTERVAL = 1 * 60 * 1000 * 1000; // 1 minute
    if (OB_FAIL(timer_.schedule(cleanup_task_, FAST_CLEANUP_INTERVAL, true))) { // use to obtest
      LOG_WARN("fail to schedule cleanup task", KR(ret), K_(tenant_id));
    } else {
      LOG_INFO("ObTenantTabletCleanupService start success with errsim ERRSIM_OFFLINE_TABLET_CLEANUP_SPEED_UP", K_(tenant_id));
    }
#endif
  } else if (OB_FAIL(timer_.schedule(cleanup_task_, CLEANUP_INTERVAL, true))) {
    LOG_WARN("fail to schedule cleanup task", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("ObTenantTabletCleanupService start success", K_(tenant_id));
  }
  return ret;
}

int ObTenantTabletCleanupService::stop()
{
  int ret = OB_SUCCESS;
  is_stop_ = true;
  timer_.stop();
  LOG_INFO("ObTenantTabletCleanupService stop", K_(tenant_id));
  return ret;
}

void ObTenantTabletCleanupService::wait()
{
  timer_.wait();
  LOG_INFO("ObTenantTabletCleanupService wait", K_(tenant_id));
}

void ObTenantTabletCleanupService::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    timer_.destroy();
    LOG_INFO("ObTenantTabletCleanupService destroy", K_(tenant_id));
  }
}

} // end namespace observer
} // end namespace oceanbase
