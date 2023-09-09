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

#include "ob_inner_sql_connection_pool.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/ob_sql.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/objectpool/ob_resource_pool.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;

namespace observer
{
ObInnerSQLConnectionPool::ObInnerSQLConnectionPool()
    : inited_(false), stop_(false), total_conn_cnt_(0),
      free_conn_list_(), used_conn_list_(),
      allocator_(SET_USE_500(ObMemAttr(OB_SERVER_TENANT_ID, ObModIds::OB_INNER_SQL_CONN_POOL))),
      schema_service_(NULL),
      ob_sql_(NULL),
      vt_iter_creator_(NULL),
      config_(NULL)
{
}

ObInnerSQLConnectionPool::~ObInnerSQLConnectionPool()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ObThreadCondGuard guard(cond_);
    if (free_conn_list_.get_size() != total_conn_cnt_) {
      LOG_ERROR("not all connection been freed", K_(total_conn_cnt),
          "free_conn_cnt", free_conn_list_.get_size());
    }

    while (!free_conn_list_.is_empty()) {
      LinkNode *node = free_conn_list_.remove_first();
      if (NULL == node) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL connection", K(ret));
      } else {
        node->~LinkNode();
        allocator_.free(node);
        node = NULL;
      }
    }
  }
}

int ObInnerSQLConnectionPool::init(ObMultiVersionSchemaService *schema_service,
                                   ObSql *ob_sql,
                                   ObVTIterCreator *vt_iter_creator,
                                   common::ObServerConfig *config,
                                   const bool is_ddl)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (NULL == schema_service ||
      NULL == ob_sql ||
      NULL == vt_iter_creator) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_service), KP(ob_sql),
             KP(vt_iter_creator));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::INNER_CONNECTION_POOL_COND_WAIT))) {
    LOG_WARN("fail to init cond, ", K(ret));
  } else {
    schema_service_ = schema_service;
    ob_sql_ = ob_sql;
    vt_iter_creator_ = vt_iter_creator;
    config_ = config;
    is_ddl_ = is_ddl;
    inited_ = true;
  }
  return ret;
}

int ObInnerSQLConnectionPool::acquire(const uint64_t tenant_id, common::sqlclient::ObISQLConnection *&conn, ObISQLClient *client_addr, const int32_t group_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  ObInnerSQLConnection *inner_sql_conn = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(alloc_conn(inner_sql_conn))) {
    LOG_WARN("alloc connection from pool failed", K(ret));
  } else if (OB_FAIL(inner_sql_conn->init(this, schema_service_, ob_sql_, vt_iter_creator_,
                                          config_, nullptr /* session_info */, client_addr, nullptr/*sql modifer*/, is_ddl_,
                                          false /*is_oracle_mode*/, group_id))) {
    LOG_WARN("init connection failed", K(ret));
  } else if (OB_FAIL(add_to_used_conn_list(inner_sql_conn))) {
    LOG_WARN("add_to_used_conn_list failed", K(ret));
  } else {
    inner_sql_conn->ref();
    conn = inner_sql_conn;
  }

  if (OB_FAIL(ret)) {
    if (NULL != inner_sql_conn) {
      int tmp_ret = remove_from_used_conn_list(inner_sql_conn);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("remove_from_used_conn_list failed", "ret", tmp_ret);
      }
      tmp_ret = inner_sql_conn->destroy();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("destroy connection failed", "ret", tmp_ret);
      }
      // continue executing while destroy error.
      tmp_ret = free_conn(inner_sql_conn);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("free connection failed", "ret", tmp_ret);
      }
    }
  }

  return ret;
}

//@notice: performance optimization
//spi inner sql connection will be called frequently by PL(test in TPCC PL)
//before this, spi inner sql connection was management by inner sql connection pool
//when acquire inner sql connection, need wrlock to protect concurrency problem
//this action has serious performance problems
//cache SPI inner sql connection to ObServerObjectPool
//ObServerObjectPool has independent allocator on each core
//so it can reduce the conflict of threads acquiring spi connection
int ObInnerSQLConnectionPool::acquire_spi_conn(sql::ObSQLSessionInfo *session_info, ObInnerSQLConnection *&conn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(conn = rp_alloc(ObInnerSQLConnection, ObInnerSQLConnection::LABEL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate spi connection failed", K(ret));
  } else if (OB_FAIL(conn->init(this,
                                schema_service_,
                                ob_sql_,
                                vt_iter_creator_,
                                config_,
                                session_info,
                                nullptr /* client_addr */,
                                nullptr /* sql_modifier */,
                                true /* use_static_engine */))) {
    LOG_WARN("init connection failed", K(ret));
  } else {
    conn->ref();
    conn->set_spi_connection(true);
  }
  return ret;
}

int ObInnerSQLConnectionPool::acquire(
    sql::ObSQLSessionInfo *session_info,
    common::sqlclient::ObISQLConnection *&conn,
    const bool is_oracle_mode/* = false */)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *inner_sql_conn = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(alloc_conn(inner_sql_conn))) {
    LOG_WARN("alloc connection from pool failed", K(ret));
  } else if (OB_FAIL(inner_sql_conn->init(this, schema_service_, ob_sql_, vt_iter_creator_, config_,
                     session_info, NULL, NULL, false, is_oracle_mode))) {
    LOG_WARN("init connection failed", K(ret));
  } else if (OB_FAIL(add_to_used_conn_list(inner_sql_conn))) {
    LOG_WARN("add_to_used_conn_list failed", K(ret));
  } else {
    if (0 != inner_sql_conn->get_ref()) {
      LOG_WARN("ref is not ZERO after acquire", KP(inner_sql_conn),
               "ref_cnt", inner_sql_conn->get_ref(), K(lbt()));
    }
    inner_sql_conn->ref();
    conn = inner_sql_conn;
  }

  if (OB_FAIL(ret)) {
    if (NULL != inner_sql_conn) {
      int tmp_ret = remove_from_used_conn_list(inner_sql_conn);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("remove_from_used_conn_list failed", "ret", tmp_ret);
      }
      tmp_ret = inner_sql_conn->destroy();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("destroy connection failed", "ret", tmp_ret);
      }
      // continue executing while destroy error.
      tmp_ret = free_conn(inner_sql_conn);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("free connection failed", "ret", tmp_ret);
      }
    }
  }
  return ret;
}

int ObInnerSQLConnectionPool::release(common::sqlclient::ObISQLConnection *conn, const bool success)
{
  // alway try to destroy connection, ignore success flag.
  UNUSEDx(success);
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (NULL == conn) {
    // ignore NULL connection release
  } else {
    static_cast<ObInnerSQLConnection *>(conn)->unref();
  }
  return ret;
}

int ObInnerSQLConnectionPool::revert(ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    LOG_WARN("not init", K(ret));
  } else if (NULL == conn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    if (conn->is_spi_conn()) {
      //spi connection come from ObServerObjectPool, so release it to ObServerObjectPool
      rp_free(conn, ObInnerSQLConnection::LABEL);
    } else if (OB_FAIL(remove_from_used_conn_list(conn))) {
      LOG_WARN("remove_from_used_conn_list failed", K(ret));
    } else if (OB_FAIL(conn->destroy())) {
      LOG_WARN("connection destroy failed", K(ret));
    } else if (OB_FAIL(free_conn(conn))) {
      LOG_WARN("free connection failed", K(ret));
    }
  }
  return ret;
}

// TODO baihua: implement
int ObInnerSQLConnectionPool::escape(const char *from, const int64_t from_size,
    char *to, const int64_t to_size, int64_t &out_size)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (NULL != from && from_size > 0) {
      if (to_size < from_size * 2) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("string buffer not enough", K(ret), K(from_size), K(to_size));
      } else if (NULL == to) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("to buffer is NULL", K(ret), KP(to), KP(from), K(from_size));
      } else {
        MEMCPY(to, from, from_size);
        out_size = from_size;
      }
    } else {
      out_size = 0;
    }
  }
  return ret;
}

int ObInnerSQLConnectionPool::alloc_conn(ObInnerSQLConnection *&conn)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *inner_sql_conn = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stop_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("connection pool stoped", K(ret));
  } else {
    ObThreadCondGuard guard(cond_);
    if (free_conn_list_.is_empty()) {
      void *mem = allocator_.alloc(sizeof(*conn));
      if (NULL == mem) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K_(total_conn_cnt));
      } else {
        total_conn_cnt_++;
        inner_sql_conn = new (mem) ObInnerSQLConnection();
      }
    } else {
      LinkNode *node = free_conn_list_.remove_first();
      node->~LinkNode();
      inner_sql_conn = new (node) ObInnerSQLConnection();
    }
    if (OB_SUCC(ret)) {
      // leak checking before add connection to used list.
      const int64_t leak_check_minutes
          = std::abs(EVENT_CALL(EventTable::EN_INNER_SQL_CONN_LEAK_CHECK));
      if (OB_LOG_NEED_TO_PRINT(WARN)
          && (total_conn_cnt_ >= WARNNING_CONNECTION_CNT || 0 != leak_check_minutes)) {
        const int64_t now = ObTimeUtility::current_time();
        const int64_t duration = leak_check_minutes * 60 * 1000000;
        if (total_conn_cnt_ >= WARNNING_CONNECTION_CNT) {
          LOG_WARN("allocated too many connections, may be connection leak",
                   K(ret), K_(total_conn_cnt), "free_conn_size", free_conn_list_.get_size());
          dump_used_conn_list();
        } else if (!used_conn_list_.is_empty()
                   && used_conn_list_.get_first()->get_init_timestamp() > 0
                   && now - used_conn_list_.get_first()->get_init_timestamp() >= duration) {
          LOG_ERROR("found connection used more than leak check minutes, may be connection leak",
                   K(ret), K_(total_conn_cnt), "free_conn_size", free_conn_list_.get_size());
          dump_used_conn_list();
        }
      }
      conn = inner_sql_conn;
    }
  }

  return ret;
}

int ObInnerSQLConnectionPool::free_conn(ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == conn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(conn));
  } else {
    ObThreadCondGuard guard(cond_);
    conn->~ObInnerSQLConnection();
    LinkNode *node = new (conn) LinkNode();
    if (!free_conn_list_.add_last(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add connection to free connection list failed", K(ret));
    } else {
      if (stop_ && 0 == used_conn_list_.get_size()) {
        cond_.signal();
      }
    }
  }

  return ret;
}

int ObInnerSQLConnectionPool::add_to_used_conn_list(ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == conn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(conn));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_UNLIKELY(false == used_conn_list_.add_last(conn))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add connection to used connection list failed", K(ret));
    }
  }
  return ret;
}

int ObInnerSQLConnectionPool::remove_from_used_conn_list(ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == conn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(conn));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_UNLIKELY(NULL == used_conn_list_.remove(conn))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("remove connection from used connection list failed", K(ret));
    }
  }

  return ret;
}

int ObInnerSQLConnectionPool::wait()
{
  int ret = OB_SUCCESS;
  const int64_t WAIT_TIME_MS = 1000;
  const int64_t WARNNING_TIME_US = 5 * 1000 * 1000;
  const int64_t begin = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!stop_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("not stoped", K(ret));
  } else {
    ObThreadCondGuard guard(cond_);
    while (used_conn_list_.get_size() > 0) {
      const int64_t now = ObTimeUtility::current_time();
      if (now - begin > WARNNING_TIME_US) {
        LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "too much time used to wait connection release, may be connection leak",
            "used_time_ms", now - begin, "used connection count", used_conn_list_.get_size());
        dump_used_conn_list();
      }
      cond_.wait(WAIT_TIME_MS);
    }
  }
  return ret;
}

void ObInnerSQLConnectionPool::dump_used_conn_list()
{
  int64_t dump_size = MIN(used_conn_list_.get_size(), MAX_DUMP_SIZE);
  LOG_WARN_RET(OB_SUCCESS, "====== DUMP USED CONNECTIONS' BACKTRACE INFO ====== ",
      K(dump_size));
  DLIST_FOREACH_X(conn, used_conn_list_, (--dump_size >= 0)) {
    if (OB_ISNULL(conn)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "node is null");
    } else {
      conn->dump_conn_bt_info();
    }
  }
}

int ObInnerSQLConnectionPool::on_client_inactive(ObISQLClient *client_addr)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  // do not check ret in loop, continue on error happen.
  ObThreadCondGuard guard(cond_);
  DLIST_FOREACH_NORET(conn, used_conn_list_) {
    if (conn->get_associated_client() == client_addr) {
      if (NULL == ob_sql_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL partition service", K(ret));
      } else {
        int tmp_ret = sql::ObSQLSessionMgr::kill_query(conn->get_session(),
                                                       ObSQLSessionState::QUERY_KILLED);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("kill query failed", K(tmp_ret));
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
        }
        cnt++;
      }
    }
  }
  if (cnt > 0) {
    LOG_INFO("kill inner query", K(ret), K(cnt));
  }
  return ret;
}

} // end namespace observer
} // end namespace oceanbase
