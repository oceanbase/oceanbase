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
#include "lib/mysqlclient/ob_dblink_error_trans.h"
#ifdef OB_BUILD_DBLINK
#include "observer/omt/ob_multi_tenant.h"
#include "share/rc/ob_tenant_base.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#endif

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::share;

#ifdef OB_BUILD_DBLINK

bool get_dblink_reuse_connection_cfg()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  // default value of _enable_dblink_reuse_connection is true, if !tenant_config.is_valid() return true
  return tenant_config.is_valid() ? tenant_config->_enable_dblink_reuse_connection : true;
}

bool get_enable_dblink_cfg()
{
  return GCONF.enable_dblink;
}

uint64_t get_max_dblink_conn_per_observer()
{
   omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  return tenant_config.is_valid() ? tenant_config->_max_dblink_conn_per_observer : 256;
}

uint64_t get_current_tenant_id_for_dblink()
{
  return MTL_ID();
}

oceanbase::common::sqlclient::ObTenantOciEnvs * ObDblinkService::get_tenant_oci_envs()
{
  return MTL(oceanbase::common::sqlclient::ObTenantOciEnvs*);
}

int ObDblinkService::init_oci_envs_func_ptr()
{
  int ret = common::OB_SUCCESS;
  OciEnvironment::get_tenant_oci_envs_ = get_tenant_oci_envs;
  return ret;
}

bool g_dblink_oci_func_ptr_inited = ObDblinkService::init_oci_envs_func_ptr();
#endif

const char * ObDblinkService::SET_ISOLATION_LEVEL_READ_COMMITTED = "ALTER SESSION SET ISOLATION_LEVEL = READ COMMITTED";
const char * ObDblinkService::SET_ISOLATION_LEVEL_REPEATABLE_READ = "ALTER SESSION SET ISOLATION_LEVEL = REPEATABLE READ";
const char * ObDblinkService::SET_ISOLATION_LEVEL_SERIALIZABLE = "ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE";
const char * ObDblinkService::SET_ISOLATION_LEVEL_READ_COMMITTED_MYSQL_MODE = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED";
const char * ObDblinkService::SET_ISOLATION_LEVEL_REPEATABLE_READ_MYSQL_MODE = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ";
const char * ObDblinkService::SET_ISOLATION_LEVEL_SERIALIZABLE_MYSQL_MODE = "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE";
const char * ObDblinkService::SET_RESULTS_CHARSET_NULL = "SET CHARACTER_SET_RESULTS = NULL";
const char * ObDblinkService::SET_CLIENT_CHARSET_UTF8MB4 = "SET CHARACTER_SET_CLIENT = UTF8MB4";
const char * ObDblinkService::SET_CLIENT_CHARSET_GBK = "SET CHARACTER_SET_CLIENT = GBK";
const char * ObDblinkService::SET_CLIENT_CHARSET_BINARY = "SET CHARACTER_SET_CLIENT = BINARY";
const char * ObDblinkService::SET_CONNECTION_CHARSET_UTF8MB4 = "SET CHARACTER_SET_CONNECTION = UTF8MB4";
const char * ObDblinkService::SET_CONNECTION_CHARSET_GBK = "SET CHARACTER_SET_CONNECTION = GBK";
const char * ObDblinkService::SET_CONNECTION_CHARSET_BINARY = "SET CHARACTER_SET_CONNECTION = BINARY";
const char * ObDblinkService::SET_RESULTS_CHARSET_UTF8MB4 = "SET CHARACTER_SET_RESULTS = UTF8MB4";
const char * ObDblinkService::SET_RESULTS_CHARSET_GBK = "SET CHARACTER_SET_RESULTS = GBK";
const char * ObDblinkService::SET_RESULTS_CHARSET_BINARY = "SET CHARACTER_SET_RESULTS = BINARY";

int ObDblinkService::get_charset_id(ObSQLSessionInfo *session_info,
                             uint16_t &charset_id,
                             uint16_t &ncharset_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null session info", K(ret));
  } else {
    ObCollationType coll_type = session_info->get_nls_collation();
    ObCollationType ncoll_type = session_info->get_nls_collation_nation();
    ObCharsetType cs_type = ObCharset::charset_type_by_coll(coll_type);
    ObCharsetType ncs_type = ObCharset::charset_type_by_coll(ncoll_type);
    if (CHARSET_INVALID == cs_type || CHARSET_INVALID == ncs_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get charset id", K(ret), K(coll_type));
    } else {
      charset_id = static_cast<uint16_t>(ObCharset::charset_type_to_ora_charset_id(cs_type));
      ncharset_id = static_cast<uint16_t>(ObCharset::charset_type_to_ora_charset_id(ncs_type));
      LOG_TRACE("succ to get dblink charset id", K(ret), K(charset_id), K(ncharset_id),
                                  K(cs_type), K(ncs_type), K(coll_type), K(ncoll_type));
    }
  }
  return ret;
}

int ObDblinkService::init_dblink_param_ctx(common::sqlclient::dblink_param_ctx &param_ctx,
                                           sql::ObSQLSessionInfo *session_info,
                                           ObIAllocator &allocator,
                                           uint64_t dblink_id,
                                           common::sqlclient::DblinkDriverProto link_type,
                                           common::sqlclient::DblinkPoolType pool_type)
{
  int ret = OB_SUCCESS;
  uint16_t charset_id = 0;
  uint16_t ncharset_id = 0;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (OB_FAIL(get_local_session_vars(session_info, allocator, param_ctx))) {
    LOG_WARN("failed to get local session vars", K(ret), K(dblink_id));
  } else if (OB_FAIL(get_charset_id(session_info, charset_id, ncharset_id))) {
    LOG_WARN("failed to get session charset id", K(ret), K(dblink_id));
  } else {
    param_ctx.charset_id_ = charset_id;
    param_ctx.ncharset_id_ = ncharset_id;
    param_ctx.pool_type_ = pool_type;
    param_ctx.tenant_id_ = session_info->get_effective_tenant_id();
    param_ctx.sessid_ = session_info->get_sessid();
    param_ctx.sql_request_level_ = session_info->get_next_sql_request_level();
    param_ctx.dblink_id_ = dblink_id;
    param_ctx.link_type_ = link_type;
  }
  return ret;
}

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

int ObDblinkService::get_set_sql_mode_cstr(sql::ObSQLSessionInfo *session_info, const char *&set_sql_mode_cstr, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSqlString set_sql_mode_sql;
  ObObj sql_mode_int_obj;
  ObObj sql_mode_str_obj;
  const char *set_sql_mode_fmt = "SET SESSION sql_mode = '%.*s'";
  void *buf = NULL;
  int64_t copy_len = 0;
  if (lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only used in mysql mode", K(ret));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR_SQL_MODE, sql_mode_int_obj))) {
    LOG_WARN("failed to get SYS_VAR_SET_REVERSE_DBLINK_INFOS", K(sql_mode_int_obj), K(ret));
  } else if (OB_FAIL(ob_sql_mode_to_str(sql_mode_int_obj, sql_mode_str_obj, &allocator))) {
    LOG_WARN("failed sql mode to str", K(sql_mode_int_obj), K(ret));
  } else if (OB_FAIL(set_sql_mode_sql.append_fmt(set_sql_mode_fmt,
                                                 sql_mode_str_obj.val_len_,
                                                 sql_mode_str_obj.v_.string_))) {
    LOG_WARN("append sql failed", K(ret), K(sql_mode_str_obj));
  } else if (FALSE_IT([&]{ copy_len = set_sql_mode_sql.length(); }())) {
  } else if (OB_ISNULL(buf = allocator.alloc(copy_len + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(copy_len));
  } else {
    MEMCPY(buf, set_sql_mode_sql.ptr(), copy_len);
    char *sql_cstr = static_cast<char *>(buf);
    sql_cstr[copy_len] = 0;
    set_sql_mode_cstr = sql_cstr;
  }
  return ret;
}

int ObDblinkService::get_set_transaction_isolation_cstr(sql::ObSQLSessionInfo *session_info,
                                                        const char *&set_isolation_level)
{
  int ret = OB_SUCCESS;
  common::ObString val;
  bool is_oracle_mode = lib::is_oracle_mode();
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR_TRANSACTION_ISOLATION, val))) {
    LOG_WARN("failed to get transaction isolation", K(ret));
  //} else if (0 == val.compare("UNCOMMITTED")) {
  //  set_isolation_level = ObDblinkService::SET_ISOLATION_LEVEL_UNCOMMITTED;
  } else if (0 == val.compare("READ-COMMITTED")) {
    set_isolation_level = is_oracle_mode ? ObDblinkService::SET_ISOLATION_LEVEL_READ_COMMITTED : ObDblinkService::SET_ISOLATION_LEVEL_READ_COMMITTED_MYSQL_MODE;
  } else if (0 == val.compare("REPEATABLE-READ")) {
    set_isolation_level = is_oracle_mode ? ObDblinkService::SET_ISOLATION_LEVEL_REPEATABLE_READ : ObDblinkService::SET_ISOLATION_LEVEL_REPEATABLE_READ_MYSQL_MODE;
  } else if (0 == val.compare("SERIALIZABLE")) {
    set_isolation_level = is_oracle_mode ? ObDblinkService::SET_ISOLATION_LEVEL_SERIALIZABLE : ObDblinkService::SET_ISOLATION_LEVEL_SERIALIZABLE_MYSQL_MODE;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected transaction isolation value", K(ret), K(val));
  }
  LOG_TRACE("dblink get isolation level from session", K(ret), K(session_info), K(set_isolation_level));
  return ret;
}

int ObDblinkService::get_set_names_cstr(sql::ObSQLSessionInfo *session_info,
                                        const char *&set_client_charset,
                                        const char *&set_connection_charset,
                                        const char *&set_results_charset)
{
  int ret = OB_SUCCESS;
  set_client_charset = NULL;
  set_connection_charset = NULL;
  set_results_charset = NULL;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else {
    ObCollationType tenant_collation = is_oracle_mode() ? session_info->get_nls_collation() : ObCollationType::CS_TYPE_UTF8MB4_BIN;
    switch(ObCharset::charset_type_by_coll(tenant_collation)) {
      case ObCharsetType::CHARSET_UTF8MB4:
        set_client_charset = "set character_set_client = utf8mb4";
        set_connection_charset = "set character_set_connection = utf8mb4";
        set_results_charset = "set character_set_results = utf8mb4";
        break;
      case ObCharsetType::CHARSET_GBK:
        set_client_charset = "set character_set_client = gbk";
        set_connection_charset = "set character_set_connection = gbk";
        set_results_charset = "set character_set_results = gbk";
        break;
      case ObCharsetType::CHARSET_BINARY:
        set_client_charset = "set character_set_client = binary";
        set_connection_charset = "set character_set_connection = binary";
        set_results_charset = "set character_set_results = binary";
        break;
      default:
        // do nothing
        break;
    }
  }
  return ret;
}

int ObDblinkService::get_local_session_vars(sql::ObSQLSessionInfo *session_info,
                                            ObIAllocator &allocator,
                                            common::sqlclient::dblink_param_ctx &param_ctx)
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode() && OB_FAIL(get_set_sql_mode_cstr(session_info, param_ctx.set_sql_mode_cstr_, allocator))) {
    LOG_WARN("failed to get set_sql_mode_cstr", K(ret));
  } else if (OB_FAIL(get_set_transaction_isolation_cstr(session_info,
                                                        param_ctx.set_transaction_isolation_cstr_))) {
    LOG_WARN("failed to get set_transaction_isolation_cstr", K(ret));
  } else if (OB_FAIL(get_set_names_cstr(session_info,
                                        param_ctx.set_client_charset_cstr_,
                                        param_ctx.set_connection_charset_cstr_,
                                        param_ctx.set_results_charset_cstr_))) {
    LOG_WARN("failed to get set_names_cstr", K(ret));
  } else {
    LOG_TRACE("succ to get local session vars", K(param_ctx.set_client_charset_cstr_),
                                                K(param_ctx.set_connection_charset_cstr_),
                                                K(param_ctx.set_results_charset_cstr_),
                                                K(param_ctx.set_sql_mode_cstr_),
                                                K(param_ctx.dblink_id_),
                                                K(ret));
  }
  return ret;
}

int ObDblinkService::get_spell_collation_type(ObSQLSessionInfo *session, ObCollationType &spell_coll)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (lib::is_oracle_mode()) {
    spell_coll = session->get_nls_collation();
  } else {
    spell_coll = ObCollationType::CS_TYPE_UTF8MB4_BIN;
  }
  return ret;
}

ObReverseLink::ObReverseLink()
  : user_(),
    tenant_(),
    cluster_(),
    passwd_(),
    addr_(),
    self_addr_(),
    tx_id_(0),
    tm_sessid_(0),
    is_close_(true),
    host_name_(),
    port_(0)
{
}

ObReverseLink::~ObReverseLink()
{
  allocator_.reset();
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
              tm_sessid_,
              host_name_,
              port_);
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
              tm_sessid_,
              host_name_,
              port_);
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
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(tmp_len = host_name_.length())) {
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator_.alloc(tmp_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc tmp_buf", K(ret), K(tmp_len));
  } else {
    MEMCPY(tmp_buf, host_name_.ptr(), tmp_len);
    host_name_.assign(tmp_buf, static_cast<int32_t>(tmp_len));
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
              tm_sessid_,
              host_name_,
              port_);
  return len;
}

const char *ObReverseLink::SESSION_VARIABLE = "_set_reverse_dblink_infos";
const int64_t ObReverseLink::VARI_LENGTH = STRLEN(SESSION_VARIABLE);
const ObString ObReverseLink::SESSION_VARIABLE_STRING(VARI_LENGTH, SESSION_VARIABLE);
const int64_t ObReverseLink::LONG_QUERY_TIMEOUT = 120*1000*1000; //120 seconds
int ObReverseLink::open(int64_t session_sql_req_level)
{
  int ret = OB_SUCCESS;
  common::sqlclient::dblink_param_ctx param_ctx;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (!is_close_) {
    // nothing to do
  } else if (tenant_.empty() || user_.empty() || passwd_.empty() /*|| db_name.empty()*/
      || OB_UNLIKELY(cluster_.length() >= OB_MAX_CLUSTER_NAME_LENGTH)
      || OB_UNLIKELY(tenant_.length() >= OB_MAX_TENANT_NAME_LENGTH)
      || OB_UNLIKELY(user_.length() >= OB_MAX_USER_NAME_LENGTH)
      || OB_UNLIKELY(passwd_.length() >= OB_MAX_PASSWORD_LENGTH)
      || OB_UNLIKELY(host_name_.length() >= OB_MAX_DOMIN_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
             K(cluster_), K(tenant_), K(user_), K(passwd_), K(host_name_));
  } else if (OB_FAIL(ObDblinkService::get_local_session_vars(session_info_, allocator_, param_ctx))) {
      LOG_WARN("failed to get local session vars", K(ret));
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
    (void)snprintf(host_name_cstr_, sizeof(host_name_), "%.*s", host_name_.length(), host_name_.ptr());
    LOG_DEBUG("open reverse link connection", K(ret), K(db_user_), K(db_pass_), K(addr_));
    param_ctx.link_type_ = common::sqlclient::DBLINK_DRV_OB;
    if (OB_FAIL(reverse_conn_.connect(db_user_, db_pass_, "", host_name_cstr_, port_, 10, true, session_sql_req_level))) { //just set connect timeout to 10s, read and write have not timeout
      LOG_WARN("failed to open reverse link connection", K(ret), K(db_user_), K(db_pass_), K(addr_));
    } else if (OB_FAIL(reverse_conn_.set_timeout_variable(LONG_QUERY_TIMEOUT, common::sqlclient::ObMySQLConnectionPool::DEFAULT_TRANSACTION_TIMEOUT_US))) {
      LOG_WARN("failed to set reverse link connection's timeout", K(ret));
    } else if (OB_FAIL(ObDbLinkProxy::execute_init_sql(param_ctx, &reverse_conn_))) {
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

int ObReverseLink::read(const ObString &sql, ObISQLClient::ReadResult &res)
{
  int ret = OB_SUCCESS;
  if (is_close_) {
    ret = OB_NOT_INIT;
    LOG_WARN("reverse link connection is closed", K(ret));
  } else if (OB_FAIL(reverse_conn_.execute_read(OB_INVALID_TENANT_ID, sql, res))) {
    LOG_WARN("faild to read by reverse link connection", K(ret), K(sql));
  } else {
    LOG_DEBUG("succ to read by reverse link connection", K(ret), K(sql));
  }
  return ret;
}

int ObReverseLink::ping()
{
  int ret = OB_SUCCESS;
  if (is_close_) {
    ret = OB_NOT_INIT;
    LOG_WARN("reverse link connection is closed", K(ret));
  } else if (OB_FAIL(reverse_conn_.ping())) {
    LOG_WARN("faild to ping reverse link connection", K(ret));
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

int ObDblinkUtils::has_reverse_link_or_any_dblink(const ObDMLStmt *stmt, bool &has, bool enable_check_any_dblink)
{
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt*> child_stmts;
  ObSEArray<ObRawExpr*, 2> seq_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(stmt->get_sequence_exprs(seq_exprs))) {
    LOG_WARN("failed to get sequence exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && enable_check_any_dblink && !has && i < seq_exprs.count(); ++i) {
    ObRawExpr *expr = seq_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else {
      ObSequenceRawExpr *seq_expr = static_cast<ObSequenceRawExpr*>(expr);
      if (OB_INVALID_ID != seq_expr->get_dblink_id()) {
        has = true;
      }
    }
  }
  if (OB_SUCC(ret) && !has && enable_check_any_dblink) {
    ObSEArray<ObRawExpr*, 2> udf_exprs;
    if (OB_FAIL(stmt->get_udf_exprs(udf_exprs))) {
      LOG_WARN("failed to get udf exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !has && i < udf_exprs.count(); ++i) {
      ObRawExpr *expr = udf_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (expr->is_udf_expr()) {
        ObUDFRawExpr *udf_expr = static_cast<ObUDFRawExpr*>(expr);
        if (OB_INVALID_ID != udf_expr->get_dblink_id()) {
          has = true;
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_items().count(); ++i) {
    const TableItem *table_item = stmt->get_table_items().at(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null ptr", K(ret));
    } else if (enable_check_any_dblink && (table_item->is_reverse_link_ || OB_INVALID_ID != table_item->dblink_id_)) {
      has = true;
      LOG_DEBUG("succ to find reverse link", K(table_item), K(i));
      break;
    } else if (!enable_check_any_dblink && table_item->is_reverse_link_) {
      has = true;
      LOG_DEBUG("succ to find reverse link", K(table_item), K(i));
      break;
    } else if (table_item->is_temp_table()) {
      if (OB_FAIL(SMART_CALL(has_reverse_link_or_any_dblink(table_item->ref_query_, has, enable_check_any_dblink)))) {
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
        } else if (OB_FAIL(SMART_CALL(has_reverse_link_or_any_dblink(child_stmt, has, enable_check_any_dblink)))) {
          LOG_WARN("failed to exec has_reverse_link", K(ret));
        } else if (has) {
          break;
        }
      }
    }
  }
  return ret;
}

int ObDblinkUtils::gather_dblink_id(const ObDMLStmt *stmt, ObIArray<int64_t> &dblink_id_array)
{
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt*> child_stmts;
  ObSEArray<ObRawExpr*, 2> seq_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(stmt->get_sequence_exprs(seq_exprs))) {
    LOG_WARN("failed to get sequence exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < seq_exprs.count(); ++i) {
    ObRawExpr *expr = seq_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else {
      ObSequenceRawExpr *seq_expr = static_cast<ObSequenceRawExpr*>(expr);
      int64_t dblink_id = seq_expr->get_dblink_id();
      if (OB_INVALID_ID != dblink_id &&
          0 != dblink_id &&
          OB_FAIL(add_var_to_array_no_dup(dblink_id_array, dblink_id))) {
        LOG_WARN("failed to add dblink id to array", K(ret), K(dblink_id));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_items().count(); ++i) {
    const TableItem *table_item = stmt->get_table_items().at(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null ptr", K(ret));
    } else if (OB_INVALID_ID != table_item->dblink_id_ &&
               0 != table_item->dblink_id_ &&
               OB_FAIL(add_var_to_array_no_dup(dblink_id_array, table_item->dblink_id_))) {
      LOG_WARN("failed to add dblink id to array", K(ret), K(table_item->dblink_id_));
    } else if (table_item->is_temp_table()) {
      if (OB_FAIL(SMART_CALL(gather_dblink_id(table_item->ref_query_, dblink_id_array)))) {
          LOG_WARN("failed to exec gather_dblink_id", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret), KP(stmt));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        ObDMLStmt *child_stmt = child_stmts.at(i);
        if (OB_ISNULL(child_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null ptr", K(ret));
        } else if (OB_FAIL(SMART_CALL(gather_dblink_id(child_stmt, dblink_id_array)))) {
          LOG_WARN("failed to exec gather_dblink_id", K(ret));
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
    LOG_DEBUG("register_dblink_conn_pool", KP(this), K(session_info_->get_sessid()), KP(dblink_conn_pool), K(dblink_conn_pool_array_.count()), K(dblink_conn_pool_array_), K(lbt()));
  }
  return ret;
}

// When the session is about to be reset, the session will release all connections
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
  dblink_conn_pool_array_.reset();
  return ret;
}

/**
 * after call get_dblink_conn , you need call revert_dblink_conn to free rlock which is in connection
*/
int ObDblinkCtxInSession::get_dblink_conn(uint64_t dblink_id, common::sqlclient::ObISQLConnection *&dblink_conn, uint32_t tm_sessid)
{
  int ret = OB_SUCCESS;
  dblink_conn = NULL;
#ifdef OB_BUILD_DBLINK
  uint32_t sessid = 0;
  common::sqlclient::ObISQLConnection *conn =NULL;
  oceanbase::common::sqlclient::ObTenantDblinkKeeper *tenant_dblink_keeper = MTL(oceanbase::common::sqlclient::ObTenantDblinkKeeper*);
  if (OB_ISNULL(session_info_) || OB_ISNULL(tenant_dblink_keeper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret), KP(session_info_), KP(tenant_dblink_keeper));
  } else if (FALSE_IT(sessid = session_info_->get_sessid())) {
  } else if (OB_FAIL(tenant_dblink_keeper->get_dblink_conn((0 != tm_sessid) ? tm_sessid : sessid, dblink_id, conn))) {
    LOG_WARN("failed to get dblink conn", K(ret), K(tm_sessid), K(sessid), K(dblink_id), KP(tenant_dblink_keeper));
  } else if (OB_ISNULL(conn)) {
    // do nothing
  } else if (OB_SUCCESS != (conn->ping())) {
    ret = OB_ERR_DBLINK_SESSION_KILLED;
    LOG_WARN("connection is invalid", K(ret), K(dblink_conn->usable()), KP(dblink_conn));
  } else {
    dblink_conn = conn;
  }
  LOG_TRACE("get_dblink_conn", K(ret), K(dblink_id), KP(dblink_conn), K(tm_sessid), K(sessid), K(lbt()));
#endif
  return ret;
}

int ObDblinkCtxInSession::revert_dblink_conn(common::sqlclient::ObISQLConnection *&dblink_conn)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_DBLINK
  if (OB_ISNULL(dblink_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else {
    dblink_conn->dblink_unrlock();
  }
#endif
  return ret;
}

int ObDblinkCtxInSession::set_dblink_conn(common::sqlclient::ObISQLConnection *dblink_conn)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_DBLINK
  uint32_t sessid = 0;
  oceanbase::common::sqlclient::ObTenantDblinkKeeper *tenant_dblink_keeper = MTL(oceanbase::common::sqlclient::ObTenantDblinkKeeper*);
  if (OB_ISNULL(session_info_) || OB_ISNULL(tenant_dblink_keeper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret), KP(session_info_), KP(tenant_dblink_keeper));
  } else if (FALSE_IT(sessid = session_info_->get_sessid())) {
  } else if (OB_ISNULL(dblink_conn)) {
    //do nothing
  } else if (OB_FAIL(tenant_dblink_keeper->set_dblink_conn(sessid, dblink_conn))) {
    LOG_WARN("failed to set dblink", KP(dblink_conn), K(sessid), KP(tenant_dblink_keeper), K(ret));
  }
  LOG_TRACE("set_dblink_conn", K(ret), KP(dblink_conn), K(sessid), K(lbt()));
#endif
  return ret;
}

int ObDblinkCtxInSession::clean_dblink_conn(const bool force_disconnect)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_DBLINK
  uint32_t sessid = 0;
  common::sqlclient::ObISQLConnection *dblink_conn = NULL;
  // Why do not use MTL(oceanbase::common::sqlclient::ObTenantDblinkKeeper*) ?
  // MTL(xxx) accesses the ptr in the cache, we need to access the ptr in the memory.
  // In the following scene，we need to know if MTL(ObTenantDblinkKeeper*) has been destroyed or not:
  // MTL(ObTenantDblinkKeeper*) destroy firstly, MTL(ObTableApiSessPoolMgr*) destroy later.
  // When MTL(ObTableApiSessPoolMgr*) destroy, it will destruct ObSQLSessionInfo which will clean dblink in reset(),
  // so we need to know if MTL(ObTenantDblinkKeeper*) has been destroyed to avoid accessing the memory that has been destroyed
  if (OB_ISNULL(MTL_CTX())) {
    // do nothing
  } else {
    oceanbase::common::sqlclient::ObTenantDblinkKeeper *tenant_dblink_keeper = MTL_CTX()->get<oceanbase::common::sqlclient::ObTenantDblinkKeeper *>();
    if (OB_ISNULL(tenant_dblink_keeper)) {
      // do nothing
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret), KP(session_info_), KP(tenant_dblink_keeper));
    } else if (FALSE_IT(sessid = session_info_->get_sessid())) {
    } else if (OB_FAIL(tenant_dblink_keeper->clean_dblink_conn(sessid, force_disconnect))) {
      LOG_WARN("failed to set dblink", KP(dblink_conn), K(sessid), KP(tenant_dblink_keeper), K(force_disconnect), K(ret));
    }
    tx_id_.reset();
    arena_alloc_.reset();
    reverse_dblink_ = NULL;
    reverse_dblink_buf_ = NULL;
    sys_var_reverse_info_buf_ = NULL;
    sys_var_reverse_info_buf_size_ = 0;
  }
#endif
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
      int64_t sys_var_length = value.length();
      if (OB_ISNULL(reverse_dblink_buf_) &&
                OB_ISNULL(reverse_dblink_buf_ = arena_alloc_.alloc(sizeof(ObReverseLink)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(sizeof(ObReverseLink)));
      } else if (sys_var_length > sys_var_reverse_info_buf_size_ &&
          OB_ISNULL(sys_var_reverse_info_buf_ = arena_alloc_.alloc(2 * sys_var_length))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(2 * sys_var_length));
      } else {
        sys_var_reverse_info_buf_size_ = 2 * sys_var_length;
        MEMCPY(sys_var_reverse_info_buf_, value.ptr(), sys_var_length);
        last_reverse_info_values_.assign((char *)sys_var_reverse_info_buf_, sys_var_length);
        if (OB_NOT_NULL(reverse_dblink_)) {
          reverse_dblink_->~ObReverseLink();
        }
        reverse_dblink_ = new(reverse_dblink_buf_) ObReverseLink();
        char *new_buff = NULL;
        int64_t new_size = 0;
        int64_t pos = 0;
        LOG_DEBUG("get SYS_VAR_SET_REVERSE_DBLINK_INFOS", K(last_reverse_info_values_));
        if (OB_FAIL(ObHexUtilsBase::unhex(last_reverse_info_values_, arena_alloc_, new_buff, new_size))) {
          LOG_WARN("failed to unhex", K(last_reverse_info_values_), K(new_size), K(ret));
        } else if (OB_FAIL(reverse_dblink_->deserialize(new_buff, new_size, pos))) {
          LOG_WARN("failed to deserialize reverse_dblink_", K(new_size), K(ret));
        } else {
          reverse_dblink_->set_session_info(session_info_);
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

OB_SERIALIZE_MEMBER(ObParamPosIdx, pos_, idx_, type_value_);

int ObLinkStmtParam::write(char *buf, int64_t buf_len, int64_t &pos, int64_t param_idx, int8_t type_value)
{
  /*
   * we need 4 bytes for every const param:
   * 1 byte:  '\0' for meta info flag. '\0' can not appear in any sql stmt fmt.
   * 1 byte:  meta info type. now we used 0 to indicate const param.
   * 2 bytes: uint16 for param index.
   */
  int ret = OB_SUCCESS;
  if (buf_len - pos < PARAM_LEN) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buffer is not enough", K(ret), K(buf_len), K(pos));
  } else if (type_value < -1 || type_value > static_cast<int8_t>(ObObjType::ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type_value", K(type_value), K(ret));
  } else if (param_idx < 0 || param_idx > UINT16_MAX) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("param count should be between 0 and UINT16_MAX", K(ret), K(param_idx));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "param count not in the range 0 - UINT16_MAX");
  } else {
    buf[pos++] = 0;   // meta flag.
    buf[pos++] = type_value;   // meta type.
    *(uint16_t*)(buf + pos) = param_idx;
    pos += sizeof(uint16_t);
  }
  return ret;
}

int ObLinkStmtParam::read_next(const char *buf, int64_t buf_len, int64_t &pos, int64_t &param_idx, int8_t &type_value)
{
  int ret = OB_SUCCESS;
  const char *ch = buf + pos;
  const char *buf_end = buf + buf_len - PARAM_LEN + 1;
  param_idx = -1;
  while (OB_SUCC(ret) && param_idx < 0 && ch < buf_end) {
    if (0 != ch[0]) {
      ch++;
    } else {
      type_value = static_cast<int8_t>(ch[1]);
      if (type_value < -1 || type_value > static_cast<int8_t>(ObObjType::ObMaxType)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid type_value", K(type_value), K(ret));
      } else {
        param_idx = static_cast<int64_t>(*(uint16_t*)(ch + 2));
      }
    }
  }
  pos = ch - buf;
  return ret;
}

int64_t ObLinkStmtParam::get_param_len()
{
  return PARAM_LEN;
}

const int64_t ObLinkStmtParam::PARAM_LEN = sizeof(char) * 2 + sizeof(uint16_t);
