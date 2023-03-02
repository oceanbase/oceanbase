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

#define USING_LOG_PREFIX SQL_OPT

#include "sql/dblink/ob_dblink_utils.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_hex_utils_base.h"
#include "share/ob_errno.h"
#include "sql/session/ob_sql_session_info.h"
#include "common/ob_smart_call.h"
#include "common/object/ob_object.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::share;

int ObDblinkService::check_lob_in_result(common::sqlclient::ObMySQLResult *result, bool &have_lob)
{
  int ret = OB_SUCCESS;
  have_lob = false;
  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else {
    int64_t column_count = result->get_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      ObObjMeta type;
      if (OB_FAIL(result->get_type(i, type))) {
        LOG_WARN("failed to get column meta", K(i), K(ret));
      } else if (ObLobType == type.get_type()) {
        have_lob = true;
        break;
      }
    }
  }
  return ret;
}
int ObDblinkService::get_length_from_type_text(ObString &type_text, int32_t &length)
{
  int ret = OB_SUCCESS;
  int64_t text_len = type_text.length();
  int64_t digit_start = 0;
  int64_t digit_length = 0;
  int64_t idx = 0;
  const char * text_ptr = type_text.ptr();
  length = 0;
  while (idx < text_len && '(' != text_ptr[idx]) { ++idx; }
  while(idx < text_len) {
    if (text_ptr[idx] >= '0' && text_ptr[idx] <= '9') {
      digit_start = idx;
      while(idx < text_len) {
        if (text_ptr[idx] >= '0' && text_ptr[idx] <= '9') {
          ++digit_length;
          ++idx;
        } else {
          break;
        }
      }
      break;
    } else {
      ++idx;
    }
  }
  int64_t digit_idx = 0;
  while (digit_idx < digit_length) {
    length *= 10;
    length += text_ptr[digit_start + digit_idx] - '0';
    ++digit_idx;
  }
  if (0 == length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to fetch length from type text", K(ret));
  }
  return ret;
}

ObReverseLink::ObReverseLink(common::ObIAllocator &alloc)
  : allocator_(alloc),
    user_(),
    tenant_(),
    cluster_(),
    passwd_(),
    addr_(),
    self_addr_(),
    tx_id_(0),
    tm_sessid_(0),
    is_close_(true)
{
}

ObReverseLink::~ObReverseLink()
{
}

OB_DEF_SERIALIZE(ObReverseLink)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              user_,
              tenant_,
              cluster_,
              passwd_,
              addr_,
              self_addr_,
              tx_id_,
              tm_sessid_);
  return ret;
}

OB_DEF_DESERIALIZE(ObReverseLink)
{
  int ret = OB_SUCCESS;
  char *tmp_buf = NULL;
  int64_t tmp_len = 0;
  LST_DO_CODE(OB_UNIS_DECODE,
              user_,
              tenant_,
              cluster_,
              passwd_,
              addr_,
              self_addr_,
              tx_id_,
              tm_sessid_);
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(tmp_len = user_.length())) {
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator_.alloc(tmp_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc tmp_buf", K(ret), K(tmp_len));
  } else {
    MEMCPY(tmp_buf, user_.ptr(), tmp_len);
    user_.assign(tmp_buf, static_cast<int32_t>(tmp_len));
  }
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(tmp_len = tenant_.length())) {
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator_.alloc(tmp_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc tmp_buf", K(ret), K(tmp_len));
  } else {
    MEMCPY(tmp_buf, tenant_.ptr(), tmp_len);
    tenant_.assign(tmp_buf, static_cast<int32_t>(tmp_len));
  }
  if (OB_FAIL(ret) || 0 == cluster_.length()) {
  } else if (FALSE_IT(tmp_len = cluster_.length())) {
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator_.alloc(tmp_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc tmp_buf", K(ret), K(tmp_len));
  } else {
    MEMCPY(tmp_buf, cluster_.ptr(), tmp_len);
    cluster_.assign(tmp_buf, static_cast<int32_t>(tmp_len));
  }
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(tmp_len = passwd_.length())) {
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator_.alloc(tmp_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc tmp_buf", K(ret), K(tmp_len));
  } else {
    MEMCPY(tmp_buf, passwd_.ptr(), tmp_len);
    passwd_.assign(tmp_buf, static_cast<int32_t>(tmp_len));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObReverseLink)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              user_,
              tenant_,
              cluster_,
              passwd_,
              addr_,
              self_addr_,
              tx_id_,
              tm_sessid_);
  return len;
}

const char *ObReverseLink::SESSION_VARIABLE = "_set_reverse_dblink_infos";
const int64_t ObReverseLink::VARI_LENGTH = STRLEN(SESSION_VARIABLE);
const ObString ObReverseLink::SESSION_VARIABLE_STRING(VARI_LENGTH, SESSION_VARIABLE);
const int64_t ObReverseLink::LONG_QUERY_TIMEOUT = 120*1000*1000; //120 seconds
int ObReverseLink::open(int64_t session_sql_req_level)
{
  int ret = OB_SUCCESS;
  if (!is_close_) {
    // nothing to do
  } else if (tenant_.empty() || user_.empty() || passwd_.empty() /*|| db_name.empty()*/
      || OB_UNLIKELY(cluster_.length() >= OB_MAX_CLUSTER_NAME_LENGTH)
      || OB_UNLIKELY(tenant_.length() >= OB_MAX_TENANT_NAME_LENGTH)
      || OB_UNLIKELY(user_.length() >= OB_MAX_USER_NAME_LENGTH)
      || OB_UNLIKELY(passwd_.length() >= OB_MAX_PASSWORD_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
             K(cluster_), K(tenant_), K(user_), K(passwd_));
  } else {
    if (cluster_.empty()) {
      (void)snprintf(db_user_, sizeof(db_user_), "%.*s@%.*s", user_.length(), user_.ptr(),
                    tenant_.length(), tenant_.ptr());
    } else {
      (void)snprintf(db_user_, sizeof(db_user_), "%.*s@%.*s#%.*s", user_.length(), user_.ptr(),
                    tenant_.length(), tenant_.ptr(),
                    cluster_.length(), cluster_.ptr());
    }
    (void)snprintf(db_pass_, sizeof(db_pass_), "%.*s", passwd_.length(), passwd_.ptr());
    LOG_DEBUG("open reverse link connection", K(ret), K(db_user_), K(db_pass_), K(addr_));
    if (OB_FAIL(reverse_conn_.connect(db_user_, db_pass_, "", addr_, 10, true, session_sql_req_level))) { //just set connect timeout to 10s, read and write have not timeout
      ObDblinkUtils::process_dblink_errno(common::sqlclient::DBLINK_DRV_OB, ret);
      LOG_WARN("failed to open reverse link connection", K(ret), K(db_user_), K(db_pass_), K(addr_));
    } else if (OB_FAIL(reverse_conn_.set_timeout_variable(LONG_QUERY_TIMEOUT, common::sqlclient::ObMySQLConnectionPool::DEFAULT_TRANSACTION_TIMEOUT_US))) {
      ObDblinkUtils::process_dblink_errno(common::sqlclient::DBLINK_DRV_OB, ret);
      LOG_WARN("failed to set reverse link connection's timeout", K(ret));
    } else if (OB_FAIL(ObDbLinkProxy::execute_init_sql(&reverse_conn_, common::sqlclient::DBLINK_DRV_OB))) {
      ObDblinkUtils::process_dblink_errno(common::sqlclient::DBLINK_DRV_OB, ret);
      LOG_WARN("failed to init reverse link connection", K(ret));
    } else {
      is_close_ = false;
      LOG_TRACE("reversesucc to open reverse link connection", K(*this), K(ret), K(lbt()));
    }
    // close reverse_conn_ if need to
    if (OB_FAIL(ret) && !reverse_conn_.is_closed()) {
      reverse_conn_.close();
    }
  }
  return ret;
}

int ObReverseLink::read(const char *sql, ObISQLClient::ReadResult &res)
{
  int ret = OB_SUCCESS;
  if (is_close_) {
    ret = OB_NOT_INIT;
    LOG_WARN("reverse link connection is closed", K(ret));
  } else if (OB_FAIL(reverse_conn_.execute_read(OB_INVALID_TENANT_ID, sql, res))) {
    int dblink_errno = ret;
    ObDblinkUtils::process_dblink_errno(common::sqlclient::DblinkDriverProto::DBLINK_DRV_OB, &reverse_conn_, ret);
    LOG_WARN("faild to read by reverse link connection", K(ret), K(sql));
  } else {
    LOG_DEBUG("succ to read by reverse link connection", K(ret), K(sql));
  }
  return ret;
}

int ObReverseLink::close()
{
  int ret = OB_SUCCESS;
  if (!is_close_) {
    reverse_conn_.close();
    is_close_ = true;
    LOG_DEBUG("reversesucc to close reverse link connection", K(ret), K(lbt()));
  }
  return ret;
}

int ObDblinkUtils::process_dblink_errno(common::sqlclient::DblinkDriverProto dblink_type, common::sqlclient::ObISQLConnection *dblink_conn, int &ob_errno) {
  // The purpose of adding the process_dblink_errno function
  // is to distinguish the errno processing when dblink connects to other types of databases.
  const int orcl_errno = ob_errno;
  switch (dblink_type) {
    case common::sqlclient::DblinkDriverProto::DBLINK_DRV_OB: {
      if (OB_UNLIKELY(NULL == dblink_conn)) {
        get_ob_errno_from_oracle_errno(orcl_errno, NULL, ob_errno);
      } else {
        get_ob_errno_from_oracle_errno(orcl_errno, mysql_error(static_cast<common::sqlclient::ObMySQLConnection *>(dblink_conn)->get_handler()), ob_errno);
      }
      break;
    }
    case common::sqlclient::DblinkDriverProto::DBLINK_DRV_OCI: {
      get_ob_errno_from_oracle_errno(orcl_errno, NULL, ob_errno);
    }
    default: {
      //nothing
      break;
    }
  }
  // some oracle error code can not translate to oceanbase error code,
  // so use OB_ERR_DBLINK_REMOTE_ECODE to represent those oracle error code.
  if (orcl_errno == ob_errno &&
      // error code -40xx will report from code in deps/, need skip it
      (ob_errno != -4016 || ob_errno != -4012 ||
       ob_errno != -4013 || ob_errno != -4002 || ob_errno != -4007)) {
    LOG_USER_ERROR(OB_ERR_DBLINK_REMOTE_ECODE, orcl_errno);
    ob_errno = OB_ERR_DBLINK_REMOTE_ECODE;
  }
  return OB_SUCCESS;
}

int ObDblinkUtils::process_dblink_errno(common::sqlclient::DblinkDriverProto dblink_type, int &ob_errno) {
  // The purpose of adding the process_dblink_errno function
  // is to distinguish the errno processing when dblink connects to other types of databases.
  const int orcl_errno = ob_errno;
  get_ob_errno_from_oracle_errno(orcl_errno, NULL, ob_errno);
  // some oracle error code can not translate to oceanbase error code,
  // so use OB_ERR_DBLINK_REMOTE_ECODE to represent those oracle error code.
  if (-4016 == ob_errno ||
      -4012 == ob_errno ||
      -4002 == ob_errno) {
    // do nothing
  } else if (orcl_errno == ob_errno) {
    LOG_USER_ERROR(OB_ERR_DBLINK_REMOTE_ECODE, orcl_errno);
    ob_errno = OB_ERR_DBLINK_REMOTE_ECODE;
  }
  return OB_SUCCESS;
}

int ObDblinkUtils::has_reverse_link(const ObDMLStmt *stmt, bool &has) {
  int ret = OB_SUCCESS;
  const common::ObIArray<TableItem*> &table_items = stmt->get_table_items();
  ObArray<ObSelectStmt*> child_stmts;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    const TableItem *table_item = table_items.at(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null ptr", K(ret));
    } else if (table_item->is_reverse_link_) {
      has = true;
      LOG_DEBUG("succ to find reverse link", K(table_item), K(i));
      break;
    } else if (table_item->is_temp_table()) {
      if (OB_FAIL(SMART_CALL(has_reverse_link(table_item->ref_query_, has)))) {
          LOG_WARN("failed to exec has_reverse_link", K(ret));
      } else if (has) {
        break;
      }
    }
  }
  if (OB_SUCC(ret) && !has) {
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret), KP(stmt));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        ObDMLStmt *child_stmt = child_stmts.at(i);
        if (OB_ISNULL(child_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null ptr", K(ret));
        } else if (OB_FAIL(SMART_CALL(has_reverse_link(child_stmt, has)))) {
          LOG_WARN("failed to exec has_reverse_link", K(ret));
        } else if (has) {
          break;
        }
      }
    }
  }
  return ret;
}

/**
 The meaning of register is to record the ObServerConnectionPool used by this session in the array through pointers.
 The pointer values stored in this array are unique to each other.

 If the pointer cannot be stored in the array, then the ObServerConnectionPool corresponding to this pointer will
 free the connection used by this session.
 */
int ObDblinkCtxInSession::register_dblink_conn_pool(common::sqlclient::ObCommonServerConnectionPool *dblink_conn_pool)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret));
  } else if (OB_ISNULL(dblink_conn_pool)) {
    //do nothing
  } else if (OB_FAIL(add_var_to_array_no_dup(dblink_conn_pool_array_, dblink_conn_pool))) {
    LOG_WARN("register dblink conn pool failed in session", K(dblink_conn_pool), K(session_info_->get_sessid()), K(ret));
    // directly free dblink connection in dblink_conn_pool
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = dblink_conn_pool->free_dblink_session(session_info_->get_sessid()))) {
      LOG_WARN("register dblink conn pool failed in session, then free dblink conn pool failed", K(dblink_conn_pool), K(session_info_->get_sessid()), K(tmp_ret));
    }
  } else {
    LOG_TRACE("register_dblink_conn_pool", KP(this), K(session_info_->get_sessid()), KP(dblink_conn_pool), K(dblink_conn_pool_array_.count()), K(dblink_conn_pool_array_), K(lbt()));
  }
  return ret;
}

// When the session is about to be destroyed, the session will release all connections
// from ObServerConnectionPool through this interface
int ObDblinkCtxInSession::free_dblink_conn_pool()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dblink_conn_pool_array_.count(); ++i) {
    common::sqlclient::ObCommonServerConnectionPool *dblink_conn_pool = dblink_conn_pool_array_.at(i);
    if (OB_UNLIKELY(NULL == dblink_conn_pool)) {
      //do nothing
    } else if (OB_FAIL(dblink_conn_pool->free_dblink_session(session_info_->get_sessid()))) {
      LOG_WARN("free dblink conn pool failed", K(dblink_conn_pool), K(session_info_->get_sessid()), K(ret));
    } else {
      LOG_TRACE("free and close dblink connection in session", KP(this), K(session_info_->get_sessid()), K(i), K(dblink_conn_pool_array_.count()), K(dblink_conn_pool_array_), KP(dblink_conn_pool), K(lbt()));
    }
  }
  if (OB_SUCC(ret)) {
    dblink_conn_pool_array_.reset();
  }
  return ret;
}

int ObDblinkCtxInSession::get_dblink_conn(uint64_t dblink_id, common::sqlclient::ObISQLConnection *&dblink_conn)
{
  int ret = OB_SUCCESS;
  dblink_conn = NULL;
  common::sqlclient::ObISQLConnection *conn =NULL;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dblink_conn_holder_array_.count(); ++i) {
    conn = reinterpret_cast<common::sqlclient::ObISQLConnection *>(dblink_conn_holder_array_.at(i));
    if (OB_UNLIKELY(NULL == conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connection in dblink_conn_holder_array_ is NULL", K(session_info_->get_sessid()), K(i), K(ret));
    } else if (dblink_id == conn->get_dblink_id()) {
      dblink_conn = conn;
      break;
    }
  }
  return ret;
}

int ObDblinkCtxInSession::set_dblink_conn(common::sqlclient::ObISQLConnection *dblink_conn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret));
  } else if (OB_ISNULL(dblink_conn)) {
    //do nothing
  } else if (OB_FAIL(add_var_to_array_no_dup(dblink_conn_holder_array_, reinterpret_cast<int64_t>(dblink_conn)))) {
    LOG_WARN("session faild to hold a dblink connection", KP(dblink_conn), K(session_info_->get_sessid()), K(ret));
  } else {
    LOG_WARN("session succ to hold a dblink connection", KP(dblink_conn), K(session_info_->get_sessid()), K(ret));
  }
  return ret;
}

int ObDblinkCtxInSession::clean_dblink_conn()
{
  int ret = OB_SUCCESS;
  common::sqlclient::ObISQLConnection *dblink_conn =NULL;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dblink_conn_holder_array_.count(); ++i) {
    dblink_conn = reinterpret_cast<common::sqlclient::ObISQLConnection *>(dblink_conn_holder_array_.at(i));
    if (OB_UNLIKELY(NULL == dblink_conn)) {
      // do nothing
    } else {
      dblink_conn->set_reverse_link_creadentials(false);
      common::sqlclient::ObCommonServerConnectionPool * server_conn_pool = NULL;
      server_conn_pool = dblink_conn->get_common_server_pool();
      if (NULL == server_conn_pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server_conn_pool of dblink connection is NULL", K(this), K(dblink_conn), K(i), K(ret));
      } else {
        if (OB_FAIL(server_conn_pool->release(dblink_conn, true, session_info_->get_sessid()))) {
          LOG_WARN("session failed to release dblink connection", K(session_info_->get_sessid()), K(this), KP(dblink_conn), K(i), K(ret));
        } else {
          LOG_TRACE("session succ to release dblink connection", K(session_info_->get_sessid()), K(this), KP(dblink_conn), K(i), K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    dblink_conn_holder_array_.reset();
  }
  return ret;
}

int ObDblinkCtxInSession::get_reverse_link(ObReverseLink *&reverse_dblink)
{
  int ret = OB_SUCCESS;
  reverse_dblink = NULL;
  ObString value;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret));
  } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR__SET_REVERSE_DBLINK_INFOS, value))) {
    LOG_WARN("failed to get SYS_VAR_SET_REVERSE_DBLINK_INFOS", K(value), K(ret));
  } else if (NULL == reverse_dblink_ || 0 != last_reverse_info_values_.compare(value)) {
    if (!value.empty()){ // get a new valid REVERSE_DBLINK_INFOS, need create or update ObReverseLink
      void *ptr = NULL;
      void *last_new_value_ptr = NULL;
      int64_t last_new_value_length = value.length();
      if (OB_ISNULL(ptr = arena_alloc_.alloc(sizeof(ObReverseLink)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(sizeof(ObReverseLink)));
      } else if (OB_ISNULL(last_new_value_ptr = arena_alloc_.alloc(last_new_value_length))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(last_new_value_length));
      } else {
        MEMCPY(last_new_value_ptr, value.ptr(), last_new_value_length);
        last_reverse_info_values_.assign((char *)last_new_value_ptr, last_new_value_length);
        reverse_dblink_ = new(ptr) ObReverseLink(arena_alloc_);
        char *new_buff = NULL;
        int64_t new_size = 0;
        int64_t pos = 0;
        LOG_DEBUG("get SYS_VAR_SET_REVERSE_DBLINK_INFOS", K(last_reverse_info_values_));
        if (OB_FAIL(ObHexUtilsBase::unhex(last_reverse_info_values_, arena_alloc_, new_buff, new_size))) {
          LOG_WARN("failed to unhex", K(last_reverse_info_values_), K(new_size), K(ret));
        } else if (OB_FAIL(reverse_dblink_->deserialize(new_buff, new_size, pos))) {
          LOG_WARN("failed to deserialize reverse_dblink_", K(new_size), K(ret));
        } else {
          reverse_dblink = reverse_dblink_;
          LOG_DEBUG("succ to get reverse link from seesion", K(session_info_->get_sessid()), K(*reverse_dblink), KP(reverse_dblink));
        }
      }
    } else if (OB_ISNULL(reverse_dblink_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL ptr", K(ret));
    } else {
      reverse_dblink = reverse_dblink_;
      LOG_DEBUG("succ to get reverse link from seesion", K(session_info_->get_sessid()), K(*reverse_dblink), KP(reverse_dblink));
    }
  } else {
    reverse_dblink = reverse_dblink_;
    LOG_DEBUG("succ to get reverse link from seesion", K(session_info_->get_sessid()), K(*reverse_dblink), KP(reverse_dblink));
  }
  return ret;
}