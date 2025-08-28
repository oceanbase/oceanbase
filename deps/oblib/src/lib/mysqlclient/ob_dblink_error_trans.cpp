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

#define USING_LOG_PREFIX LIB_MYSQLC
#include "lib/mysqlclient/ob_dblink_error_trans.h"

int __attribute__((weak)) get_oracle_errno(int index)
{
  return oceanbase::OB_SUCCESS;
}

int __attribute__((weak)) get_mysql_errno(int index)
{
  return oceanbase::OB_SUCCESS;
}

const char* __attribute__((weak)) get_oracle_str_error(int index)
{
  return NULL;
}

const char* __attribute__((weak)) get_mysql_str_error(int index)
{
  return NULL;
}

bool __attribute__((weak)) get_dblink_reuse_connection_cfg()
{
  return true;
}

bool __attribute__((weak)) get_enable_dblink_cfg()
{
  return true;
}

uint64_t __attribute__((weak)) get_current_tenant_id_for_dblink()
{
  return oceanbase::OB_INVALID_ID;
}

uint64_t __attribute__((weak)) get_max_dblink_conn_per_observer()
{
  return 256;
}

namespace oceanbase
{
namespace common
{
namespace sqlclient
{

int sqlclient::ObDblinkErrorTrans::external_errno_to_ob_errno(bool is_oracle_err, 
                                                   int external_errno, 
                                                   const char *external_errmsg, 
                                                   int &ob_errno) {
  int ret = OB_SUCCESS;
  external_errno = abs(external_errno);
  if (OB_SUCCESS != external_errno) {
    const char *oracle_msg_prefix = "ORA";
    if (external_errno >= 2000 && // google "Client Error Message Reference" 
        external_errno <= 2075 && // you will known errno in [2000, 2075] is client error at dev.mysql.com
        (!is_oracle_err ||  
        (is_oracle_err && 
        (OB_NOT_NULL(external_errmsg) && 0 != STRLEN(external_errmsg)) && 
        0 != std::memcmp(oracle_msg_prefix, external_errmsg, 
        std::min(STRLEN(oracle_msg_prefix), STRLEN(external_errmsg)))))) {
      ob_errno = external_errno; // do not map, show user client errno directly.
    } else if (is_oracle_err
               && -external_errno >= OB_MIN_RAISE_APPLICATION_ERROR
               && -external_errno <= OB_MAX_RAISE_APPLICATION_ERROR) {
      ob_errno = OB_APPLICATION_ERROR_FROM_REMOTE;
      LOG_USER_ERROR(OB_APPLICATION_ERROR_FROM_REMOTE, (int)STRLEN(external_errmsg), external_errmsg);
    } else {
      int64_t match_count = 0;
      for (int i = 0; i < oceanbase::common::OB_MAX_ERROR_CODE; ++i) {
        if (external_errno == (is_oracle_err ? get_oracle_errno(i) : get_mysql_errno(i))) {
          ob_errno = -i;
          ++match_count;
        }
      }
      if (1 != match_count) {
        // default ob_errno, if external_errno can not map to any valid ob_errno
        ob_errno = OB_ERR_DBLINK_REMOTE_ECODE;
	const char *errmsg = external_errmsg;
	if (NULL == errmsg) {
		errmsg = "empty error message";
	}
        int msg_len = STRLEN(errmsg);
        LOG_USER_ERROR(OB_ERR_DBLINK_REMOTE_ECODE, external_errno, msg_len, errmsg);
      } else if (1 == match_count) {
        if (is_oracle_err && OB_TRANS_XA_BRANCH_FAIL == ob_errno) {
          ob_errno = OB_TRANS_NEED_ROLLBACK;
        }
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_DBLINK
int ObTenantDblinkKeeper::clean_dblink_conn(uint32_t sessid, bool force_disconnect)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  if (!dblink_conn_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("dblink_conn_map_ is not inited", K(ret), K(tenant_id_), K(sessid));
  } else if (OB_FAIL(dblink_conn_map_.erase_refactored(sessid, &value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to erase_refactored", K(ret), K(tenant_id_), K(sessid));
    }
  } else {
    common::sqlclient::ObISQLConnection *connection = reinterpret_cast<common::sqlclient::ObISQLConnection *>(value);
    while (OB_NOT_NULL(connection) && OB_SUCC(ret)) {
      common::sqlclient::ObISQLConnection *next = connection->get_next_conn();
      connection->dblink_wlock(); //prevent connection still in use
      connection->set_reverse_link_creadentials(false);  
      common::sqlclient::ObCommonServerConnectionPool * server_conn_pool = connection->get_common_server_pool();
      const bool need_disconnect = force_disconnect || !connection->usable();
      if (NULL == server_conn_pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server_conn_pool of dblink connection is NULL", K(tenant_id_), K(sessid), K(connection), K(ret));
      } else if (OB_FAIL(server_conn_pool->release(connection, !need_disconnect))) {
        LOG_WARN("session failed to release dblink connection", K(tenant_id_), K(sessid), K(connection), K(ret));
      } else {
        LOG_TRACE("session succ to release dblink connection", K(tenant_id_), K(sessid), K(connection), K(ret));
      }
      connection->dblink_unwlock();
      connection = next;
    }
  }
  return ret;
}

int ObTenantDblinkKeeper::AppendDblinkConnCall::operator() (common::hash::HashMapPair<uint32_t, int64_t> &entry)
{
  int ret = OB_SUCCESS;
  common::sqlclient::ObISQLConnection *connection = reinterpret_cast<common::sqlclient::ObISQLConnection *>(entry.second);
  if (OB_ISNULL(connection)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is null", K(ret));
  } else {
    while (OB_NOT_NULL(connection)) {
      if ((&dblink_conn_) == connection) {
        break;
      }
      connection = connection->get_next_conn();
    }
    if (OB_NOT_NULL(connection)) {
      //already exists, do nothing
    } else {
      common::sqlclient::ObISQLConnection *header = reinterpret_cast<common::sqlclient::ObISQLConnection *>(entry.second);
      common::sqlclient::ObISQLConnection *temp = header->get_next_conn();
      header->set_next_conn(&dblink_conn_);
      dblink_conn_.set_next_conn(temp);
      LOG_TRACE("session succ to hold a dblink connection", K(&dblink_conn_), K(entry.first), K(ret));
    }
  }
  return ret;
}

int ObTenantDblinkKeeper::set_dblink_conn(uint32_t sessid, common::sqlclient::ObISQLConnection *dblink_conn)
{
  int ret = OB_SUCCESS;
  if (!dblink_conn_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("dblink_conn_map_ is not inited", K(ret), K(tenant_id_), K(sessid));
  } else if (OB_ISNULL(dblink_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret), K(tenant_id_), K(sessid));
  } else {
    dblink_conn->set_next_conn(NULL);
    AppendDblinkConnCall append_call(*dblink_conn);
    if (OB_FAIL(dblink_conn_map_.set_or_update(sessid, reinterpret_cast<int64_t>(dblink_conn), append_call))) {
      LOG_WARN("set or update failed", K(ret), K(sessid));
    }
  }
  return ret;
}

void ObTenantDblinkKeeper::GetDblinkConnCall::operator() (common::hash::HashMapPair<uint32_t, int64_t> &entry)
{
  int ret = OB_SUCCESS;
  dblink_conn_ = NULL;
  uint32_t sessid = entry.first;
  common::sqlclient::ObISQLConnection *connection = reinterpret_cast<common::sqlclient::ObISQLConnection *>(entry.second);
  if (OB_ISNULL(connection)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is null", K(ret));
  } else {
    while (OB_NOT_NULL(connection)) {
      if (dblink_id_ == connection->get_dblink_id()) {
        break;
      }
      connection = connection->get_next_conn();
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(connection)) {
      dblink_conn_ = connection;
      dblink_conn_->dblink_rlock();
    }
    LOG_TRACE("session get a dblink connection", K(ret), K(dblink_id_), KP(dblink_conn_), K(sessid));
  }
}

int ObTenantDblinkKeeper::get_dblink_conn(uint32_t sessid, uint64_t dblink_id,
                                          common::sqlclient::ObISQLConnection *&dblink_conn)
{
  int ret = OB_SUCCESS;
  dblink_conn = NULL;
  ObArray<int64_t> *dblink_conn_array = NULL;
  int64_t value = 0;
  GetDblinkConnCall get_conn_call(dblink_id);
  if (!dblink_conn_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("dblink_conn_map_ is not inited", K(ret), K(tenant_id_), K(sessid));
  } else if (OB_FAIL(dblink_conn_map_.atomic_refactored(sessid, get_conn_call))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get connection", K(ret), K(sessid));
    }
  } else if (NULL != get_conn_call.dblink_conn_) {
    if (OB_SUCCESS != get_conn_call.dblink_conn_->ping()) {
      ret = OB_ERR_DBLINK_SESSION_KILLED;
      get_conn_call.dblink_conn_->dblink_unrlock();
      LOG_WARN("connection is invalid", K(ret), K(get_conn_call.dblink_conn_->usable()),
               KP(get_conn_call.dblink_conn_), K(sessid), K(tenant_id_));
    } else {
      dblink_conn = get_conn_call.dblink_conn_;
    }
    LOG_TRACE("session get a dblink connection", K(ret), K(dblink_id), K(tenant_id_), KP(dblink_conn), K(sessid));
  }
  return ret;
}

int ObTenantDblinkKeeper::init(uint64_t tenant_id)
{
  static int SESSION_COUNT = 1024;
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  ObMemAttr attr(tenant_id, "DblinkKeeperBkt");
  if (OB_FAIL(dblink_conn_map_.create(SESSION_COUNT, attr, attr))) {
    LOG_WARN("fail init pool map", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantDblinkKeeper::mtl_new(ObTenantDblinkKeeper *&dblink_keeper)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = tenant_id = get_current_tenant_id_for_dblink();
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant id", K(ret), KP(get_current_tenant_id_for_dblink));
  } else if (FALSE_IT(dblink_keeper = OB_NEW(ObTenantDblinkKeeper, ObMemAttr(tenant_id, "DblinkKeeper")))) {
  } else if (OB_ISNULL(dblink_keeper)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc meme", K(ret));
  }
  return ret;
}

int ObTenantDblinkKeeper::mtl_init(ObTenantDblinkKeeper *&dblink_keeper)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = tenant_id = get_current_tenant_id_for_dblink();
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant id", K(ret), KP(get_current_tenant_id_for_dblink));
  } else if (OB_ISNULL(dblink_keeper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else {
    dblink_keeper->init(tenant_id);
    LOG_TRACE("init dblink_keeper", K(tenant_id), KP(dblink_keeper), K(ret));
  }
  return ret;
}

void ObTenantDblinkKeeper::mtl_destroy(ObTenantDblinkKeeper *&dblink_keeper)
{
  common::ob_delete(dblink_keeper);
  dblink_keeper = nullptr;
}

int ObTenantDblinkKeeper::destroy()
{
  int ret = OB_SUCCESS;
  CleanDblinkArrayFunc clean_dblink_func;
  if (OB_FAIL(dblink_conn_map_.foreach_refactored(clean_dblink_func))) {
    LOG_WARN("failed to do foreach", K(ret));
  } else {
    dblink_conn_map_.destroy();
    tenant_id_ = OB_INVALID_ID;
  }
  return ret;
}

int ObTenantDblinkKeeper::CleanDblinkArrayFunc::operator() (common::hash::HashMapPair<uint32_t, int64_t> &kv)
{
  int ret = OB_SUCCESS;
  common::sqlclient::ObISQLConnection *connection = reinterpret_cast<common::sqlclient::ObISQLConnection *>(kv.second);
  while (OB_SUCC(ret) && OB_NOT_NULL(connection)) {
    common::sqlclient::ObISQLConnection *next = connection->get_next_conn();
    connection->set_reverse_link_creadentials(false);
    common::sqlclient::ObCommonServerConnectionPool * server_conn_pool = NULL;
    server_conn_pool = connection->get_common_server_pool();
    uint32_t sessid = connection->get_sessid();
    if (NULL == server_conn_pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server_conn_pool of dblink connection is NULL", K(sessid), KP(connection), K(ret));
    } else {
      if (OB_FAIL(server_conn_pool->release(connection, false))) {
        LOG_WARN("session failed to release dblink connection", K(sessid), KP(connection), K(ret));
      } else {
        LOG_TRACE("session succ to release dblink connection", K(sessid), KP(connection), K(ret));
      }
    }
    connection = next;
  }
  if (OB_SUCC(ret)) {
    kv.second = 0;
  }
  return ret;
}
#endif

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase
