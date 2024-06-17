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

#define USING_LOG_PREFIX SQL_SESSION

#include "sql/session/ob_user_resource_mgr.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/thread/thread_mgr.h"
#include "share/ob_get_compat_mode.h"
#include "ob_sql_session_info.h"
#include "share/ob_thread_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {

static const char *MEMORY_LABEL = "UserResourceMgr";

ObConnectResource* ObConnectResAlloc::alloc_value()
{
  ObMemAttr attr(OB_SERVER_TENANT_ID, MEMORY_LABEL);
  SET_USE_500(attr);
  return OB_NEW(ObConnectResource, attr);
}

void ObConnectResAlloc::free_value(ObConnectResource* tz_info)
{
  OB_DELETE(ObConnectResource, MEMORY_LABEL, tz_info);
  tz_info = NULL;
}

ObConnectResHashNode* ObConnectResAlloc::alloc_node(ObConnectResource* value)
{
  int64_t tenant_id = OB_ISNULL(value) ? OB_SERVER_TENANT_ID : value->tenant_id_;
  ObMemAttr attr(tenant_id, MEMORY_LABEL);
  return OB_NEW(ObConnectResHashNode, attr);
}

void ObConnectResAlloc::free_node(ObConnectResHashNode* node)
{
  if (NULL != node) {
    OB_DELETE(ObConnectResHashNode, MEMORY_LABEL, node);
    node = NULL;
  }
}

ObConnectResourceMgr::ObConnectResourceMgr()
: inited_(false), user_res_map_(), tenant_res_map_(), schema_service_(nullptr),
  cleanup_task_(*this)
{
}

ObConnectResourceMgr::~ObConnectResourceMgr()
{}

int ObConnectResourceMgr::init(ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(user_res_map_.init("UserResCtrl"))) {
    LOG_WARN("fail to init user resource map", K(ret));
  } else if (OB_FAIL(tenant_res_map_.init("TenantResCtrl"))) {
    LOG_WARN("fail to init tenant resource map", K(ret));
  } else {
    schema_service_ = &schema_service;
    inited_ = true;
    const int64_t delay = ConnResourceCleanUpTask::SLEEP_USECONDS;
    const bool repeat = false;
    if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, cleanup_task_, delay, repeat))) {
      LOG_WARN("schedual connect resource mgr failed", K(ret));
    }
  }
  return ret;
}

int ObConnectResourceMgr::apply_for_tenant_conn_resource(const uint64_t tenant_id,
                                         const ObPrivSet &priv,
                                         const uint64_t max_tenant_connections)
{
  int ret = OB_SUCCESS;
  ObConnectResource *tenant_res = NULL;
  ObTenantUserKey tenant_key(tenant_id, 0);
  if (OB_FAIL(tenant_res_map_.get(tenant_key, tenant_res))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // not exist, alloc and insert
      ObMemAttr attr(tenant_id, MEMORY_LABEL);
      if (OB_ISNULL(tenant_res = OB_NEW(ObConnectResource, attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate tenant resource failed", K(ret));
      } else {
        tenant_res->cur_connections_ = 0;
        tenant_res->tenant_id_ = tenant_id;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tenant_res_map_.insert_and_get(tenant_key, tenant_res))) {
        LOG_WARN("insert and get failed", K(ret));
        OB_DELETE(ObConnectResource, MEMORY_LABEL, tenant_res);
        tenant_res = NULL;
        // 1. tenant resouce already exist because of concurrent insert, just get it.
        // 2. may also fail because of oom.
        if (OB_ENTRY_EXIST == ret && OB_FAIL(tenant_res_map_.get(tenant_key, tenant_res))) {
          // may happen with very very little probability: insert failed and then tenant is dropped
          // and value in the map is deleted by periodly task.
          LOG_WARN("get tenant conn res map failed", K(ret), K(tenant_id));
        }
      }
    } else {
      LOG_WARN("get tenant resource failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(tenant_res)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant resource is null", K(ret));
    } else {
      // check and update cur_connections.
      ObLatchWGuard wr_guard(tenant_res->rwlock_, ObLatchIds::DEFAULT_MUTEX);
      if (tenant_res->cur_connections_ < max_tenant_connections
          || (max_tenant_connections == tenant_res->cur_connections_
              && OB_PRIV_HAS_ANY(priv, OB_PRIV_SUPER))) {
        // only user with super privilege is permitted to connect when reach max tenant connections.
        tenant_res->cur_connections_++;
      } else {
        ret = OB_ERR_CON_COUNT_ERROR;;
        LOG_WARN("too many connections", K(ret), K(tenant_res->cur_connections_),
          K(max_tenant_connections));
      }
    }
  }
  if (OB_NOT_NULL(tenant_res)) {
    tenant_res_map_.revert(tenant_res);
  }
  return ret;
}

void ObConnectResourceMgr::release_tenant_conn_resource(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantUserKey tenant_key(tenant_id, 0);
  ObConnectResource *tenant_res = NULL;
  bool has_insert = false;
  if (OB_FAIL(tenant_res_map_.get(tenant_key, tenant_res))) {
    LOG_WARN("get tenant res map failed", K(ret));
  } else {
    ObLatchWGuard wr_guard(tenant_res->rwlock_, ObLatchIds::DEFAULT_MUTEX);
    if (OB_UNLIKELY(0 == tenant_res->cur_connections_)) {
      LOG_ERROR("tenant current connections is zero when release resource", K(tenant_id));
    } else {
      tenant_res->cur_connections_--;
    }
    tenant_res_map_.revert(tenant_res);
  }
}

int ObConnectResourceMgr::get_tenant_cur_connections(const uint64_t tenant_id,
                                                     bool &tenant_exists,
                                                     uint64_t &cur_connections)
{
  int ret = OB_SUCCESS;
  tenant_exists = false;
  cur_connections = 0;
  ObTenantUserKey tenant_key(tenant_id, 0);
  ObConnectResource *tenant_res = NULL;
  if (OB_FAIL(tenant_res_map_.get(tenant_key, tenant_res))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get tenant resource failed", K(ret));
    }
  } else {
    tenant_exists = true;
  }
  if (OB_SUCC(ret) && tenant_exists) {
    if (OB_ISNULL(tenant_res)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant resource is null", K(ret));
    } else {
      cur_connections = tenant_res->cur_connections_;
      tenant_res_map_.revert(tenant_res);
    }
  }
  return ret;
}

// get user resource from hash map, insert if not exist.
int ObConnectResourceMgr::get_or_insert_user_resource(const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t max_user_connections,
      const uint64_t max_connections_per_hour,
      ObConnectResource *&user_res)
{
  int ret = OB_SUCCESS;
  user_res = NULL;
  ObTenantUserKey user_key(tenant_id, user_id);
  if (OB_FAIL(user_res_map_.get(user_key, user_res))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // not exist, alloc and insert
      ObMemAttr attr(tenant_id, MEMORY_LABEL);
      if (OB_ISNULL(user_res = OB_NEW(ObConnectResource, attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate user resource failed", K(ret));
      } else {
        user_res->cur_connections_ = 0;
        user_res->history_connections_ = 0;
        user_res->start_time_ = 0;
        user_res->tenant_id_ = tenant_id;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(user_res_map_.insert_and_get(user_key, user_res))) {
        LOG_WARN("insert and get failed", K(ret));
        OB_DELETE(ObConnectResource, MEMORY_LABEL, user_res);
        user_res = NULL;
        // 1. user resouce already exist because of concurrent insert, just get it.
        // 2. may also fail because of oom.
        if (OB_ENTRY_EXIST == ret && OB_FAIL(user_res_map_.get(user_key, user_res))) {
          // may happen with very very little probability: insert failed and then user is dropped
          // and value in the map is deleted by periodly task.
          LOG_WARN("user not exists", K(ret));
        }
      }
    } else {
      LOG_WARN("get user resource failed", K(ret));
    }
  }
  return ret;
}

int ObConnectResourceMgr::increase_user_connections_count(
      const uint64_t max_user_connections,
      const uint64_t max_connections_per_hour,
      const ObString &user_name,
      ObConnectResource *user_res,
      bool &user_conn_increased)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(user_res)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user resource is null", K(ret));
  } else {
    const static int64_t usec_per_hour = static_cast<int64_t>(1000000) * 3600;
    // check and update cur_connections and connections in one hour.
    ObLatchWGuard wr_guard(user_res->rwlock_, ObLatchIds::DEFAULT_MUTEX);
    if (0 != max_connections_per_hour) {
      int64_t cur_time = ObTimeUtility::current_time();
      if (cur_time - user_res->start_time_ > usec_per_hour) {
        user_res->start_time_ = cur_time;
        user_res->history_connections_ = 0;
      } else if (user_res->history_connections_ >= max_connections_per_hour) {
        ret = OB_ERR_USER_EXCEED_RESOURCE;
        LOG_WARN("user exceed max connections per hour", K(ret), KPC(user_res));
        LOG_USER_ERROR(OB_ERR_USER_EXCEED_RESOURCE, user_name.length(), user_name.ptr(),
                "max_connections_per_hour", user_res->history_connections_);
      }
    }
    if (OB_SUCC(ret) && 0 != max_user_connections) {
      if (user_res->cur_connections_ >= max_user_connections) {
        ret = OB_ERR_USER_EXCEED_RESOURCE;
        LOG_WARN("user exceed max user connections", K(ret), KPC(user_res));
        LOG_USER_ERROR(OB_ERR_USER_EXCEED_RESOURCE, user_name.length(), user_name.ptr(),
                "max_user_connections", user_res->cur_connections_);
      }
    }
    if (OB_SUCC(ret)) {
      user_res->history_connections_ += 0 == max_connections_per_hour ? 0 : 1;
      user_res->cur_connections_ += 0 == max_user_connections ? 0 : 1;
      user_conn_increased = 0 != max_user_connections;
    }
  }
  return ret;
}

// max_connections: max connections per hour.
// max_user_connections: max concurrent connections.
// 0 means no limit.
int ObConnectResourceMgr::on_user_connect(
      const uint64_t tenant_id,
      const uint64_t user_id,
      const ObPrivSet &priv,
      const ObString &user_name,
      const uint64_t max_connections_per_hour,
      const uint64_t max_user_connections,
      const uint64_t max_tenant_connections,
      ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::ORACLE;
  if (!session.is_user_session()) {
    // do not limit connection count for inner sesion.
  } else if (OB_FAIL(share::ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("get tenant mode failed", K(ret), K(tenant_id));
  } else if (compat_mode == lib::Worker::CompatMode::MYSQL) {
    if (!session.has_got_tenant_conn_res()) {
      if (OB_FAIL(apply_for_tenant_conn_resource(tenant_id, priv, max_tenant_connections))) {
        LOG_WARN("reach teannt max connections", K(ret));
      } else {
        session.set_got_tenant_conn_res(true);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (session.has_got_user_conn_res()) {
    } else if (0 == max_connections_per_hour && 0 == max_user_connections) {
    } else {
      // According to document of MySQL:
      // "Resource-use counting takes place when any account has a nonzero limit placed on its use of any of the resources."
      // only increase cur_connections_ if max_user_connections is not zero
      // only record history_connections_ if max_connections_per_hour is not zero.
      ObConnectResource *user_res = NULL;
      bool user_conn_increased = false;
      if (OB_FAIL(get_or_insert_user_resource(tenant_id, user_id, max_user_connections,
                                              max_connections_per_hour, user_res))) {
        LOG_WARN("get or insert user resource failed", K(ret));
      } else if (OB_FAIL(increase_user_connections_count(max_user_connections, max_connections_per_hour,
            user_name, user_res, user_conn_increased))) {
        LOG_WARN("increase user connection count failed", K(ret));
      }
      if (user_conn_increased) {
        session.set_got_user_conn_res(true);
        session.set_conn_res_user_id(user_id);
      }
      if (OB_NOT_NULL(user_res)) {
        user_res_map_.revert(user_res);
        user_res = NULL;
      }
    }
  } else {
    if (!session.has_got_tenant_conn_res()) {
      if (OB_FAIL(apply_for_tenant_conn_resource(tenant_id, priv, UINT64_MAX))) {
        LOG_WARN("reach teannt max connections", K(ret));
      } else {
        session.set_got_tenant_conn_res(true);
      }
    }
  }
  return ret;
}

// Whether need decrease cur_connections_, that's a question.
// It depends on whether cur_connections_ is increased when create the connection.
// Since max_user_connections is only allowed to be modified globally, which means it remains
// unchanged from connection to disconnection, we can use it decide whether decrease cur_connections_.
int ObConnectResourceMgr::on_user_disconnect(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (!session.is_user_session()) {
    // do not limit connection count for inner sesion.
    if (OB_UNLIKELY(session.has_got_tenant_conn_res() || session.has_got_user_conn_res())) {
      LOG_ERROR("inner session expect no connection resource", K(session.get_conn_res_user_id()),
                K(session.has_got_tenant_conn_res()));
    }
  } else {
    uint64_t tenant_id = session.get_login_tenant_id();
    if (session.has_got_tenant_conn_res()) {
      release_tenant_conn_resource(tenant_id);
      session.set_got_tenant_conn_res(false);
    }
    if (session.has_got_user_conn_res()) {
      uint64_t user_id = session.get_conn_res_user_id();
      ObConnectResource *user_res = NULL;
      ObTenantUserKey user_key(tenant_id, user_id);
      if (OB_FAIL(user_res_map_.get(user_key, user_res))) {
        // maybe already dropped.
        ret = OB_SUCCESS;
      } else if (OB_ISNULL(user_res)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("user resource is null", K(ret));
      } else {
        ObLatchWGuard wr_guard(user_res->rwlock_, ObLatchIds::DEFAULT_MUTEX);
        if (OB_UNLIKELY(0 == user_res->cur_connections_)) {
          LOG_ERROR("current connections is zero when disconnect", K(user_id));
        } else {
          user_res->cur_connections_--;
        }
        user_res_map_.revert(user_res);
      }
      session.set_got_user_conn_res(false);
    }
  }
  return ret;
}

int ObConnectResourceMgr::erase_tenant_conn_res_map(int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  EraseTenantMapFunc func(tenant_id);
  int64_t erase_user_cnt = 0;
  int64_t erase_tenant_cnt = 0;
  if (OB_FAIL(user_res_map_.remove_if(func))) {
    LOG_WARN("remove_if failed", K(ret), K(tenant_id));
  } else if (FALSE_IT(erase_user_cnt = func.erase_cnt_)) {
  } else if (OB_FAIL(tenant_res_map_.remove_if(func))) {
    LOG_WARN("remove_if failed", K(ret), K(tenant_id));
  } else {
    erase_tenant_cnt = func.erase_cnt_ - erase_user_cnt;
    user_res_map_.purge();
    tenant_res_map_.purge();
  }
  LOG_INFO("erase tenant conn result map", K(tenant_id), K(erase_tenant_cnt), K(erase_user_cnt));
  return ret;
}

bool ObConnectResourceMgr::CleanUpConnResourceFunc::operator() (
    ObTenantUserKey key, ObConnectResource *conn_res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(conn_res)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user res is NULL", K(ret), K(conn_res));
  } else {
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(schema_guard_.get_user_info(key.tenant_id_, key.user_id_, user_info))) {
      if (OB_TENANT_NOT_EXIST != ret) {
        LOG_ERROR("get user info failed", K(ret), K(key.tenant_id_), K(key.user_id_));
      } else {
        ret = OB_SUCCESS;
        conn_res_map_.del(key);
      }
    } else if (OB_ISNULL(user_info)) {
      conn_res_map_.del(key);
    }
  }
  return OB_SUCCESS == ret;
}

// task for cleanup periodly. Remove dropped tenant and user from tenant_res_map_ and user_res_map_.
void ObConnectResourceMgr::ConnResourceCleanUpTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(conn_res_mgr_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema service is null", K(ret));
  //FIXME: Here, key may confict when tenant_id is removed from used_id.
  //       This logic should execute by tenant and don't use get_cluster_schema_guard() again.
  } else if (OB_FAIL(conn_res_mgr_.schema_service_->get_cluster_schema_guard(schema_guard))) {
    LOG_WARN("get sys tenant schema guard failed", K(ret));
  } else {
    LOG_INFO("clean up connection resource", K(schema_guard.get_tenant_id()),
              K(conn_res_mgr_.user_res_map_.size()), K(conn_res_mgr_.tenant_res_map_.size()));
    CleanUpConnResourceFunc user_func(schema_guard, conn_res_mgr_.user_res_map_);
    if (OB_FAIL(conn_res_mgr_.user_res_map_.for_each(user_func))) {
      LOG_WARN("cleanup dropped user failed", K(ret));
    }
  }
  const int64_t delay = SLEEP_USECONDS;
  const bool repeat = false;
  if (OB_SUCC(ret) && OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, *this, delay, repeat))) {
    LOG_ERROR("schedule connect resource cleanup task failed", K(ret));
  }
}

}  // namespace sql
}  // namespace oceanbase
