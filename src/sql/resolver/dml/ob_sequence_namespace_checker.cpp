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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_sequence_namespace_checker.h"
#include "lib/charset/ob_charset.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace sql
{

ObSequenceNamespaceChecker::ObSequenceNamespaceChecker(ObResolverParams &resolver_params)
    : schema_checker_(resolver_params.schema_checker_),
      session_info_(resolver_params.session_info_)
{}

ObSequenceNamespaceChecker::ObSequenceNamespaceChecker(const ObSchemaChecker *schema_checker,
                                                       const ObSQLSessionInfo *session_info)
  : schema_checker_(schema_checker), session_info_(session_info)
{}
int ObSequenceNamespaceChecker::check_sequence_namespace(const ObQualifiedName &q_name,
                                                        ObSynonymChecker &syn_checker,
                                                        uint64_t &sequence_id,
                                                        uint64_t *dblink_id)
{
  return check_sequence_namespace(q_name, syn_checker, 
                                  session_info_, 
                                  schema_checker_, 
                                  sequence_id,
                                  dblink_id);
}

int ObSequenceNamespaceChecker::check_sequence_namespace(const ObQualifiedName &q_name,
                                                         ObSynonymChecker &syn_checker,
                                                         const ObSQLSessionInfo *session_info,
                                                         const ObSchemaChecker *schema_checker,
                                                         uint64_t &sequence_id,
                                                         uint64_t *dblink_id_ptr)
{
  int ret = OB_NOT_IMPLEMENT;
  const ObString &sequence_name = q_name.tbl_name_;
  const ObString &sequence_expr = q_name.col_name_;
  if (OB_ISNULL(schema_checker) || OB_ISNULL(session_info)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is null", K(ret));
  } else if (!is_curr_or_next_val(q_name.col_name_)) {
    ret = OB_ERR_BAD_FIELD_ERROR;
  } else if (q_name.database_name_.empty() && session_info->get_database_name().empty()) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("No database selected", K(q_name), K(ret));
  } else if (0 != sequence_expr.case_compare("NEXTVAL") &&
             0 != sequence_expr.case_compare("CURRVAL")) {
    ret = OB_ERR_BAD_FIELD_ERROR;
  } else if (q_name.dblink_name_.empty()) {
    const ObString &database_name = q_name.database_name_.empty() ?
        session_info->get_database_name() : q_name.database_name_;
    uint64_t tenant_id = session_info->get_effective_tenant_id();
    uint64_t database_id = OB_INVALID_ID;
    bool exist = false;
    if (OB_FAIL(schema_checker->get_database_id(tenant_id, database_name, database_id))) {
      LOG_WARN("failed to get database id", K(ret), K(tenant_id), K(database_name));
    } else if (OB_FAIL(check_sequence_with_synonym_recursively(tenant_id, database_id, sequence_name,
                                                               syn_checker, schema_checker, exist, sequence_id))) {
      LOG_WARN("fail recursively check sequence with name", K(q_name), K(database_name), K(ret));
    } else if (!exist) {
      ret = OB_ERR_BAD_FIELD_ERROR;
    }
  } else {
    bool exist = false;
    bool has_currval = false;
    uint64_t tenant_id = session_info->get_effective_tenant_id();
    const ObDbLinkSchema *dblink_schema = NULL;
    uint64_t dblink_id = OB_INVALID_ID;
    number::ObNumber currval;
    ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo*>(session_info);
    const ObSequenceSchema* seq_schema = NULL;
    if (OB_FAIL(const_cast<ObSchemaChecker*>(schema_checker)->get_dblink_schema(tenant_id,
                                                                                q_name.dblink_name_,
                                                                                dblink_schema))) {
      LOG_WARN("failed to get dblink schema", K(ret));
    } else if (OB_ISNULL(dblink_schema)) {
      ret = OB_DBLINK_NOT_EXIST_TO_ACCESS;
      LOG_WARN("cat not find dblink", K(q_name.dblink_name_), K(ret));
    } else if (OB_FALSE_IT(dblink_id = dblink_schema->get_dblink_id())) {
    } else if (OB_FAIL(session_info->get_dblink_sequence_id(sequence_name,
                                                            dblink_id,
                                                            sequence_id))) {
      LOG_WARN("failed to get dblink sequence id", K(ret));
    } else if (OB_FAIL(check_link_sequence_exists(dblink_schema,
                                                  session,
                                                  q_name.database_name_,
                                                  sequence_name,
                                                  exist,
                                                  has_currval,
                                                  currval))) {
      LOG_WARN("failed to check link sequence exists", K(ret));
    } else if (!exist) {
      //remove currval cache
      if (OB_INVALID_ID == sequence_id) {
        //do nothing
      } else if (OB_FAIL(session->drop_sequence_value_if_exists(sequence_id))) {
        LOG_WARN("failed to drop sequence value", K(ret));
      }
    } else if (OB_INVALID_ID != sequence_id &&
               OB_FAIL(session->get_dblink_sequence_schema(sequence_id, seq_schema))) {
      LOG_WARN("failed to check sequence", K(ret));
    } else if (NULL == seq_schema &&
               OB_FAIL(fetch_dblink_sequence_schema(tenant_id,
                                                    session_info->get_database_id(),
                                                    dblink_id,
                                                    sequence_name,
                                                    session,
                                                    sequence_id))) {
      LOG_WARN("fail to add dblink sequence", K(q_name), K(ret));
    } else if (NULL != dblink_id_ptr) {
      *dblink_id_ptr = dblink_id;
    }
    if (OB_SUCC(ret) && exist && has_currval) {
      ObSequenceValue seq_value;
      if (OB_FAIL(seq_value.set(currval))) {
        LOG_WARN("failed to set value", K(ret));
      } else if (OB_FAIL(session->set_sequence_value(tenant_id, sequence_id, seq_value))) {
        LOG_WARN("failed to set sequence value", K(ret));
      }
    }
  }
  return ret;
}

int ObSequenceNamespaceChecker::fetch_dblink_sequence_schema(const uint64_t tenant_id,
                                                            const uint64_t db_id,
                                                            const uint64_t dblink_id,
                                                            const common::ObString &sequence_name,
                                                            ObSQLSessionInfo *session_info,
                                                            uint64_t &sequence_id)
{
  int ret = OB_SUCCESS;
  sequence_id = OB_INVALID_ID;
  bool session_has_sequence_id = false;
  if (OB_INVALID_ID == tenant_id
      ||OB_INVALID_ID == dblink_id
      || sequence_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(dblink_id), K(sequence_name));
  } else if (OB_FAIL(session_info->get_dblink_sequence_id(sequence_name, dblink_id, sequence_id))) {
    LOG_WARN("failed to get dblink sequence id", K(ret));
  } else if (OB_FALSE_IT(session_has_sequence_id = (OB_INVALID_ID != sequence_id))) {
  } else if (!session_has_sequence_id &&
             OB_FAIL(session_info->get_next_sequence_id(sequence_id))) {
    LOG_WARN("failed to fetch new_sequence_id", K(tenant_id), K(ret));
  } else if (!session_has_sequence_id &&
            OB_FAIL(session_info->set_dblink_sequence_id(sequence_name, dblink_id, sequence_id))) {
    LOG_WARN("failed to set dblink sequence id", K(ret));
  } else {
    ObSequenceSchema seq_schema;
    seq_schema.set_tenant_id(tenant_id);
    seq_schema.set_database_id(db_id);
    seq_schema.set_dblink_id(dblink_id);
    seq_schema.set_sequence_name(sequence_name);
    seq_schema.set_sequence_id(sequence_id);
    seq_schema.set_schema_version(OB_INVALID_VERSION);
    if (OB_FAIL(session_info->add_dblink_sequence_schema(&seq_schema))) {
      LOG_WARN("failed to add sequence", K(ret));
    } else {
      LOG_TRACE("allocate new sequence schema", K(sequence_id));
    }
  }
  return ret;
}

int ObSequenceNamespaceChecker::check_sequence_with_synonym_recursively(const uint64_t tenant_id,
                                                                        const uint64_t database_id,
                                                                        const common::ObString &sequence_name,
                                                                        ObSynonymChecker &syn_checker,
                                                                        const ObSchemaChecker *schema_checker,
                                                                        bool &exists,
                                                                        uint64_t &sequence_id)
{
  int ret = OB_SUCCESS;
  bool exist_with_synonym = false;
  ObString object_seq_name;
  uint64_t object_db_id;
  uint64_t synonym_id;
  if (OB_FAIL(schema_checker->check_sequence_exist_with_name(tenant_id,
                                                                      database_id,
                                                                      sequence_name,
                                                                      exists,
                                                                      sequence_id))) {
    LOG_WARN("failed to check sequence with name", K(ret), K(sequence_name), K(database_id));
  } else if (!exists) { // check synonym
    if (OB_FAIL(schema_checker->get_synonym_schema(tenant_id, database_id, sequence_name, object_db_id,
                                                            synonym_id, object_seq_name, exist_with_synonym))) {
      LOG_WARN("get synonym failed", K(ret), K(tenant_id), K(database_id), K(sequence_name));
    } else if (exist_with_synonym) {
      syn_checker.set_synonym(true);
      if (OB_FAIL(syn_checker.add_synonym_id(synonym_id, database_id))) {
        LOG_WARN("failed to add synonym id", K(ret));
      } else {
        if (OB_FAIL(check_sequence_with_synonym_recursively(tenant_id, object_db_id, object_seq_name, syn_checker,
                                                            schema_checker, exists, sequence_id))) {
          LOG_WARN("failed to check sequence with synonym recursively", K(ret));
        }
      }
    } else {
      exists = false;
      LOG_INFO("sequence object does not exist", K(sequence_name), K(tenant_id), K(database_id));
    }
  }
  return ret;
}


// link table.
int ObSequenceNamespaceChecker::check_link_sequence_exists(const ObDbLinkSchema *dblink_schema,
                                                          sql::ObSQLSessionInfo *session_info,
                                                          const ObString &database_name,
                                                          const ObString &sequence_name,
                                                          bool &exists,
                                                          bool &has_currval,
                                                          number::ObNumber &currval)
{
  int ret = OB_SUCCESS;
  exists = false;
  has_currval = false;
  dblink_param_ctx param_ctx;
  common::ObArenaAllocator allocator;
  int64_t sql_request_level = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  DblinkDriverProto link_type = DBLINK_DRV_OB;
  ObDbLinkProxy *dblink_proxy = GCTX.dblink_proxy_;
  if (OB_ISNULL(dblink_proxy) || OB_ISNULL(session_info) || OB_ISNULL(dblink_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(dblink_proxy), KP(session_info));
  } else if (database_name.empty() || sequence_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sequence name or database name is empty", K(ret), K(database_name), K(sequence_name));
  } else if (FALSE_IT(sql_request_level = session_info->get_next_sql_request_level())) {
  } else if (sql_request_level < 1 || sql_request_level > 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql_request_level", K(sql_request_level), K(ret));
  } else if (OB_FAIL(ObDblinkService::init_dblink_param_ctx(param_ctx,
                                                            session_info,
                                                            allocator, //useless in oracle mode
                                                            dblink_schema->get_dblink_id(),
                                                            static_cast<DblinkDriverProto>(dblink_schema->get_driver_proto()),
                                                            DblinkPoolType::DBLINK_POOL_SCHEMA))) {
    LOG_WARN("failed to init dblink param ctx", K(ret), K(param_ctx));
  } else {
    // param_ctx.charset_id_ and param_ctx.ncharset_id_, default value is what we need.
    param_ctx.charset_id_ = common::ObNlsCharsetId::CHARSET_AL32UTF8_ID;
    param_ctx.ncharset_id_ = common::ObNlsCharsetId::CHARSET_AL32UTF8_ID;
    LOG_TRACE("get link sequence schema", K(param_ctx), K(sequence_name), K(database_name), KP(dblink_schema), K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSqlString sql;
    sqlclient::ObMySQLResult *result = NULL;
    static const char * sql_str_fmt_array[] = { // for Oracle mode dblink
      "/*$BEFPARSEdblink_req_level=1*/ SELECT \"%.*s\".\"%.*s\".CURRVAL FROM DUAL",
      "/*$BEFPARSEdblink_req_level=2*/ SELECT \"%.*s\".\"%.*s\".CURRVAL FROM DUAL",
      "/*$BEFPARSEdblink_req_level=3*/ SELECT \"%.*s\".\"%.*s\".CURRVAL FROM DUAL",
    };
    static const char * sql_str_fmt_array_mysql_mode[] = { // for MySql mode dblink
      "/*$BEFPARSEdblink_req_level=1*/ SELECT `%.*s`.`%.*s`.CURRVAL",
      "/*$BEFPARSEdblink_req_level=2*/ SELECT `%.*s`.`%.*s`.CURRVAL",
      "/*$BEFPARSEdblink_req_level=3*/ SELECT `%.*s`.`%.*s`.CURRVAL",
    };
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      const int64_t col_idx = 0;
      common::sqlclient::ObISQLConnection *dblink_conn = NULL;
      if (OB_FAIL(sql.append_fmt(lib::is_oracle_mode() ?
                                sql_str_fmt_array[sql_request_level - 1] :
                                sql_str_fmt_array_mysql_mode[sql_request_level - 1],
                                database_name.length(), database_name.ptr(),
                                sequence_name.length(), sequence_name.ptr()))) {
        LOG_WARN("append sql failed", K(ret), K(database_name), K(sequence_name));
      } else if (OB_FAIL(dblink_proxy->create_dblink_pool(param_ctx,
                                                    dblink_schema->get_host_addr(),
                                                    dblink_schema->get_tenant_name(),
                                                    dblink_schema->get_user_name(),
                                                    dblink_schema->get_plain_password(),
                                                    database_name,
                                                    dblink_schema->get_conn_string(),
                                                    dblink_schema->get_cluster_name()))) {
        LOG_WARN("create dblink pool failed", K(ret), K(param_ctx));
      } else if (OB_FAIL(session_info->get_dblink_context().get_dblink_conn(param_ctx.dblink_id_, dblink_conn))) {
        LOG_WARN("failed to get dblink connection from session", K(ret));
      } else if (NULL == dblink_conn) {
        if (OB_FAIL(dblink_proxy->acquire_dblink(param_ctx, dblink_conn))) {
          LOG_WARN("failed to acquire dblink", K(ret), K(param_ctx));
        } else if (OB_FAIL(session_info->get_dblink_context().register_dblink_conn_pool(dblink_conn->get_common_server_pool()))) {
          LOG_WARN("failed to register dblink conn pool to current session", K(ret));
        } else if (OB_FAIL(session_info->get_dblink_context().set_dblink_conn(dblink_conn))) {
          LOG_WARN("failed to set dblink connection to session", K(ret));
        } else if (OB_FAIL(session_info->get_dblink_context().get_dblink_conn(param_ctx.dblink_id_, dblink_conn))) { // will add a rlock on dblink conn, means this dblink_conn is inuse
          LOG_WARN("failed to get dblink connection from session", K(ret), K(param_ctx.dblink_id_));
        } else {
          LOG_TRACE("link sequence get connection from dblink pool", K(lbt()));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(dblink_proxy->dblink_read(dblink_conn, res, sql.ptr()))) {
        if (OB_ERR_SEQ_NOT_EXIST != ret) {
          exists = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("read link failed", K(ret), K(param_ctx), K(sql.ptr()));
        }
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result. ", K(ret));
      } else if (OB_FAIL(result->set_expected_charset_id(static_cast<uint16_t>(common::ObNlsCharsetId::CHARSET_AL32UTF8_ID),
                                                        static_cast<uint16_t>(common::ObNlsCharsetId::CHARSET_AL32UTF8_ID)))) {
        // dblink will convert the result to AL32UTF8 when pulling schema meta
        LOG_WARN("failed to set expected charset id", K(ret));
      }  else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get next val", K(ret));
      } else if (OB_FAIL(result->get_number(col_idx, currval, session_info->get_allocator()))) {
        LOG_WARN("failed to get number result", K(ret));
      } else {
        exists = true;
        has_currval = true;
      }
      if (NULL != dblink_conn) {
        int tmp_ret = OB_SUCCESS;
        if (DBLINK_DRV_OB == param_ctx.link_type_ &&
            NULL != result &&
            OB_SUCCESS != (tmp_ret = result->close())) {
          LOG_WARN("failed to close result", K(tmp_ret));
        }
  #ifdef OB_BUILD_DBLINK
        if (DBLINK_DRV_OCI == param_ctx.link_type_ &&
            OB_SUCCESS != (tmp_ret = static_cast<ObOciConnection *>(dblink_conn)->free_oci_stmt())) {
          LOG_WARN("failed to close oci result", K(tmp_ret));
        }
  #endif
        // release rlock on dblink_conn
        if (OB_SUCCESS != (tmp_ret = ObDblinkCtxInSession::revert_dblink_conn(dblink_conn))) {
          LOG_WARN("failed to revert dblink conn", K(tmp_ret), KP(dblink_conn));
        }
        dblink_conn = NULL;
        //release dblink connection by session
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
