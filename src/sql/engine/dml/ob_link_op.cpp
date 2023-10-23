/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/dml/ob_link_dml_op.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_dblink_mgr.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/ob_sql_utils.h"
namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

namespace sql
{
ObLinkSpec::ObLinkSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
    allocator_(alloc),
    param_infos_(alloc),
    stmt_fmt_(),
    stmt_fmt_buf_(NULL),
    stmt_fmt_len_(0),
    dblink_id_(OB_INVALID_ID),
    is_reverse_link_(false)
{}

OB_DEF_SERIALIZE(ObLinkSpec)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObLinkSpec, ObOpSpec));
  LST_DO_CODE(OB_UNIS_ENCODE,
              param_infos_,
              stmt_fmt_,
              dblink_id_,
              is_reverse_link_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLinkSpec)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObLinkSpec, ObOpSpec));
  LST_DO_CODE(OB_UNIS_DECODE,
              param_infos_,
              stmt_fmt_,
              dblink_id_,
              is_reverse_link_);
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(stmt_fmt_len_ = stmt_fmt_.length())) {
    // nothing.
  } else if (OB_ISNULL(stmt_fmt_buf_ = static_cast<char *>(allocator_.alloc(stmt_fmt_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc stmt_fmt_buf", K(ret), K(stmt_fmt_len_));
  } else {
    MEMCPY(stmt_fmt_buf_, stmt_fmt_.ptr(), stmt_fmt_len_);
    stmt_fmt_.assign(stmt_fmt_buf_, static_cast<int32_t>(stmt_fmt_len_));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLinkSpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObLinkSpec, ObOpSpec));
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              param_infos_,
              stmt_fmt_,
              dblink_id_,
              is_reverse_link_);
  return len;
}
const int64_t ObLinkOp::STMT_BUF_BLOCK = 1024L;
const char * ObLinkOp::head_comment_fmt_ = "/*$BEFPARSEdblink_req_level=%d*/";
// %d will be counted as 2 byte length, but after print number(6/7/8) to %d, it only need 1 byte to print number.
// Last 1 byte reserved for snprintf to print \0.
const int64_t ObLinkOp::head_comment_length_ = STRLEN(head_comment_fmt_);
// for proxy to route dblink reverse sql req
const char *ObLinkOp::proxy_route_info_fmt_ = "/*ODP: target_db_server=%s*/";
const int64_t ObLinkOp::proxy_route_info_fmt_length_ = STRLEN(proxy_route_info_fmt_);
const int64_t ObLinkOp::proxy_route_ip_port_size_ = 64; // 64 byte is enough for ipv4:port or ipv6:port string

ObLinkOp::ObLinkOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    tenant_id_(OB_INVALID_ID),
    dblink_id_(OB_INVALID_ID),
    sessid_(0),
    dblink_schema_(NULL),
    dblink_proxy_(NULL),
    dblink_conn_(NULL),
    allocator_(exec_ctx.get_allocator()),
    stmt_buf_(NULL),
    stmt_buf_len_(STMT_BUF_BLOCK),
    next_sql_req_level_(0),
    link_type_(DBLINK_DRV_OB),
    in_xa_trascaction_(false)
{}

int ObLinkOp::init_dblink(uint64_t dblink_id, ObDbLinkProxy *dblink_proxy, bool in_xa_trascaction)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  dblink_param_ctx param_ctx;
  ObSQLSessionInfo * my_session = NULL;
  common::sqlclient::ObISQLConnection *dblink_conn = NULL;
  my_session = ctx_.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  in_xa_trascaction_ = in_xa_trascaction;
  dblink_id_ = dblink_id;
  if (OB_NOT_NULL(dblink_proxy_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("link scan ctx already inited", K(ret));
  } else if (OB_ISNULL(dblink_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dblink_proxy is NULL", K(ret));
  } else if (OB_ISNULL(my_session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_session or plan_ctx is NULL", K(my_session), K(plan_ctx), K(ret));
  } else if (FALSE_IT(sessid_ = my_session->get_sessid())) {
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_dblink_schema(tenant_id_, dblink_id, dblink_schema_))) {
    LOG_WARN("failed to get dblink schema", K(ret), K(tenant_id_), K(dblink_id));
  } else if (OB_ISNULL(dblink_schema_)) {
    ret = OB_DBLINK_NOT_EXIST_TO_ACCESS;
    LOG_WARN("dblink schema is NULL", K(ret), K(dblink_id));
  } else if (FALSE_IT(set_link_driver_proto(static_cast<DblinkDriverProto>(dblink_schema_->get_driver_proto())))) {
    // do nothing
  } else if (OB_FAIL(ObLinkOp::init_dblink_param_ctx(ctx_,
                                                     param_ctx,
                                                     link_type_,
                                                     tenant_id_,
                                                     dblink_id_,
                                                     sessid_,
                                                     next_sql_req_level_))) {
    LOG_WARN("failed to init dblink param ctx", K(ret));
  } else if (OB_FAIL(dblink_proxy->create_dblink_pool(param_ctx,
                                                      dblink_schema_->get_host_addr(),
                                                      dblink_schema_->get_tenant_name(),
                                                      dblink_schema_->get_user_name(),
                                                      dblink_schema_->get_plain_password(),
                                                      dblink_schema_->get_database_name(),
                                                      dblink_schema_->get_conn_string(),
                                                      dblink_schema_->get_cluster_name()))) {
    LOG_WARN("failed to create dblink pool", K(ret));
  } else if (OB_FAIL(my_session->get_dblink_context().get_dblink_conn(dblink_id, dblink_conn))) {
    LOG_WARN("failed to get dblink connection from session", K(my_session), K(sessid_), K(ret));
  } else {
    if (NULL == dblink_conn) {
      if (OB_FAIL(ObDblinkService::get_local_session_vars(my_session, allocator_, param_ctx))) {
      LOG_WARN("failed to get local session vars", K(ret));
      } else if (OB_FAIL(dblink_proxy->acquire_dblink(param_ctx, dblink_conn_))) {
        LOG_WARN("failed to acquire dblink", K(ret), K(param_ctx));
      } else if (OB_FAIL(my_session->get_dblink_context().register_dblink_conn_pool(dblink_conn_->get_common_server_pool()))) {
        LOG_WARN("failed to register dblink conn pool to current session", K(ret));
      } else if (in_xa_trascaction_ && lib::is_oracle_mode() &&
                 OB_FAIL(my_session->get_dblink_context().set_dblink_conn(dblink_conn_))) {
        LOG_WARN("failed to set dblink connection to session", K(in_xa_trascaction_), K(my_session), K(sessid_), K(ret));
      } else {
        LOG_TRACE("link op get connection from dblink pool", K(in_xa_trascaction_), KP(dblink_conn_), K(lbt()));
      }
    } else {
      dblink_conn_ = dblink_conn;
      in_xa_trascaction_ = true; //to tell link scan op don't release dblink_conn_
      LOG_TRACE("link op get connection from xa trasaction", K(dblink_id), KP(dblink_conn_));
    }
    if (OB_SUCC(ret)) {
      dblink_proxy_ = dblink_proxy;
    }
  }
  return ret;
}

int ObLinkOp::execute_link_stmt(const ObString &link_stmt_fmt,
                       const ObIArray<ObParamPosIdx> &param_infos,
                       const ObParamStore &param_store,
                       ObReverseLink *reverse_link)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(combine_link_stmt(link_stmt_fmt,
                                param_infos,
                                param_store,
                                reverse_link))) {
    LOG_WARN("failed to gen link stmt", K(ret), K(link_stmt_fmt));
  } else if (OB_FAIL(inner_execute_link_stmt(stmt_buf_))) {
    LOG_WARN("failed to execute link stmt", K(ret));
  }
  return ret;
}

int ObLinkOp::combine_link_stmt(const ObString &link_stmt_fmt,
                                const ObIArray<ObParamPosIdx> &param_infos,
                                const ObParamStore &param_store,
                                ObReverseLink *reverse_link)
{
  // combine link_stmt_fmt and parameter strings to final link stmt.
  int ret = OB_SUCCESS;
  // reserve head_comment_length_ byte length for head comment
  int64_t link_stmt_pos = head_comment_length_;
  int64_t reserve_proxy_route_space = 0;
  int64_t next_param = 0;
  int64_t stmt_fmt_pos = 0;
  int64_t stmt_fmt_next_param_pos = (next_param < param_infos.count() ?
                                     param_infos.at(next_param).pos_ : link_stmt_fmt.length());
  char proxy_route_ip_port_str[proxy_route_ip_port_size_] = { 0 };
  if (OB_NOT_NULL(reverse_link)) {
    if (OB_FAIL(reverse_link->get_self_addr().ip_port_to_string(proxy_route_ip_port_str,
                                                 proxy_route_ip_port_size_))) {
      LOG_WARN("failed to print self addr");
    } else {
      // %s proxy_route_info_fmt_ in  will be count as 2 byte, but only need 1 byte reserved for snprintf to print \0
      // STRLEN(proxy_route_ip_port_str) will be reserver for snprintf to print proxy_route_ip_port_str
      reserve_proxy_route_space += proxy_route_info_fmt_length_ + STRLEN(proxy_route_ip_port_str) - 1;
    }
  }
  link_stmt_pos += reserve_proxy_route_space;
  while (OB_SUCC(ret) && stmt_fmt_pos <  link_stmt_fmt.length()) {
    // copy from link_stmt_fmt.
    if (stmt_fmt_pos < stmt_fmt_next_param_pos) {
      int64_t copy_len = stmt_fmt_next_param_pos - stmt_fmt_pos;
      if (link_stmt_pos + copy_len > stmt_buf_len_ &&
          OB_FAIL(extend_stmt_buf(link_stmt_pos + copy_len))) {
        LOG_WARN("failed to extend stmt buf", K(ret));
      }
      if (OB_SUCC(ret)) {
        MEMCPY(stmt_buf_ + link_stmt_pos, link_stmt_fmt.ptr() + stmt_fmt_pos, copy_len);
        link_stmt_pos += copy_len;
        stmt_fmt_pos = stmt_fmt_next_param_pos;
      }
    } else if (stmt_fmt_pos == stmt_fmt_next_param_pos) {
      // copy from param_store.
      int64_t saved_stmt_pos = link_stmt_pos;
      int64_t param_idx = param_infos.at(next_param).idx_;
      int8_t param_type_value = param_infos.at(next_param).type_value_;
      const ObObjParam &param = param_store.at(param_idx);
      ObObjPrintParams obj_print_params = CREATE_OBJ_PRINT_PARAM(ctx_.get_my_session());
      if (param_type_value < 0 || param_type_value > static_cast<int8_t>(ObObjType::ObMaxType)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param type_value", K(param_type_value), K(ret));
      } else if (param.is_null()) {
        obj_print_params.ob_obj_type_  = ObObjType(param_type_value);
        obj_print_params.print_null_string_value_ = 1;
      }
      if (DBLINK_DRV_OCI == link_type_) {
        // Ensure that when oceanbase connects to oracle,
        // the target character set of param is the same as that of oci connection.
        obj_print_params.cs_type_ = ctx_.get_my_session()->get_nls_collation();
      }
      obj_print_params.need_cast_expr_ = true;
      obj_print_params.print_const_expr_type_ = true;
      while (OB_SUCC(ret) && link_stmt_pos == saved_stmt_pos) {
        if (128 > (stmt_buf_len_ - link_stmt_pos) && // ensure all params has sufficient mem to print as literal sql, avoiding lose precision
             OB_FAIL(extend_stmt_buf())) {
          LOG_WARN("failed to extend stmt buf", K(ret));
        }
        //Previously, the format parameter of the print sql literal function was NULL.
        //In the procedure scenario, when dblink reverse spell trunc(date type), it will treat the date type as a string,
        //so correct formatting parameter obj_print_params need to be given.
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(param.print_sql_literal(stmt_buf_, stmt_buf_len_, link_stmt_pos, obj_print_params))) {
          if (ret == OB_SIZE_OVERFLOW) {
            ret = OB_SUCCESS;
            if (OB_FAIL(extend_stmt_buf())) {
              LOG_WARN("failed to extend stmt buf", K(ret), K(param));
            } else {
              // databuff_printf() will set link_stmt_pos to stmt_buf_len_ - 1,
              // so we need load the saved_stmt_pos and retry.
              link_stmt_pos = saved_stmt_pos;
            }
          } else {
            LOG_WARN("failed to print param", K(ret), K(param));
          }
        } else {
          next_param++;
          stmt_fmt_pos += ObLinkStmtParam::get_param_len();
          stmt_fmt_next_param_pos = (next_param < param_infos.count() ?
                                     param_infos.at(next_param).pos_ : link_stmt_fmt.length());
        }
      } // while
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fmt_pos should not be greater than fmt_next_param_pos", K(ret),
               K(stmt_fmt_pos), K(stmt_fmt_next_param_pos));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (link_stmt_pos >= stmt_buf_len_ && OB_FAIL(extend_stmt_buf(link_stmt_pos + 1))) {
      LOG_WARN("failed to extend stmt buf", K(ret), K(link_stmt_pos), K(stmt_buf_len_));
  } else {
    if (link_stmt_pos + 1 >= stmt_buf_len_ && OB_FAIL(extend_stmt_buf())) {
      LOG_WARN("failed to extend stmt buf", K(ret));
    } else {
      stmt_buf_[link_stmt_pos++] = 0;
      LOG_DEBUG("succ to combine link sql", K(stmt_buf_), K(link_stmt_pos));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (DBLINK_DRV_OB == link_type_) {
      snprintf(stmt_buf_, head_comment_length_, head_comment_fmt_, next_sql_req_level_);
      // after snprint only head_comment_length_ - 1 byte was printed by head comment
      // pos of head_comment_length_ - 1 is '\0', need filled as ' '
      stmt_buf_[head_comment_length_ - 1] = ' ';
      if (OB_NOT_NULL(reverse_link)) {
        snprintf(stmt_buf_ + head_comment_length_,
                 reserve_proxy_route_space,
                 proxy_route_info_fmt_,
                 proxy_route_ip_port_str);
        stmt_buf_[head_comment_length_ + reserve_proxy_route_space - 1] = ' ';
        LOG_DEBUG("succ to combine link sql", K(stmt_buf_), K(link_stmt_pos), K(proxy_route_ip_port_str));
      }
    } else {
      stmt_buf_ += head_comment_length_ + reserve_proxy_route_space;
      link_stmt_pos -= head_comment_length_ + reserve_proxy_route_space;
    }
    LOG_TRACE("succ to combine link sql", K(stmt_buf_), K(link_stmt_pos));
  }
  return ret;
}

int ObLinkOp::extend_stmt_buf(int64_t need_size)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = (need_size > stmt_buf_len_) ?
                        (need_size / STMT_BUF_BLOCK + 1) * STMT_BUF_BLOCK :
                        stmt_buf_len_ + STMT_BUF_BLOCK;
  char *alloc_buf = static_cast<char *>(allocator_.alloc(alloc_size));
  if (OB_ISNULL(alloc_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to extend stmt buf", K(ret), K(alloc_size));
  } else {
    MEMCPY(alloc_buf, stmt_buf_, stmt_buf_len_);
    allocator_.free(stmt_buf_);
    stmt_buf_ = alloc_buf;
    stmt_buf_len_ = alloc_size;
  }
  return ret;
}

int ObLinkSpec::set_stmt_fmt(const char *stmt_fmt_buf, int64_t stmt_fmt_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_fmt_buf) || stmt_fmt_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt_fmt_buf is null or stmt_fmt_len is less than 0",
             K(ret), KP(stmt_fmt_buf), K(stmt_fmt_len));
  } else if (OB_ISNULL(stmt_fmt_buf_ = static_cast<char *>(allocator_.alloc(stmt_fmt_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc stmt_fmt_buf", K(ret), K(stmt_fmt_len));
  } else {
    MEMCPY(stmt_fmt_buf_, stmt_fmt_buf, stmt_fmt_len);
    stmt_fmt_len_ = stmt_fmt_len;
    stmt_fmt_.assign(stmt_fmt_buf_, static_cast<int32_t>(stmt_fmt_len_));
  }
  return ret;
}

int ObLinkSpec::set_param_infos(const ObIArray<ObParamPosIdx> &param_infos)
{
  int ret = OB_SUCCESS;
  param_infos_.reset();
  if (param_infos.count() > 0 && OB_FAIL(param_infos_.init(param_infos.count()))) {
    LOG_WARN("failed to init fixed array", K(param_infos.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_infos.count(); i++) {
    if (OB_FAIL(param_infos_.push_back(param_infos.at(i)))) {
      LOG_WARN("failed to push back param info", K(ret), K(param_infos.at(i)));
    }
  }
  return ret;
}

int ObLinkOp::init_dblink_param_ctx(ObExecContext &exec_ctx,
                                   common::sqlclient::dblink_param_ctx &param_ctx,
                                   common::sqlclient::DblinkDriverProto link_type,
                                   uint64_t tenant_id,
                                   uint64_t dblink_id,
                                   uint32_t session_id,
                                   int64_t next_sql_req_level)
{
  int ret = OB_SUCCESS;
  uint16_t charset_id = 0;
  uint16_t ncharset_id = 0;
  if (OB_FAIL(get_charset_id(exec_ctx, charset_id, ncharset_id))) {
    LOG_WARN("failed to get session charset id", K(ret));
  } else {
    param_ctx.charset_id_ = charset_id;
    param_ctx.ncharset_id_ = ncharset_id;
    param_ctx.pool_type_ = DblinkPoolType::DBLINK_POOL_DEF;
    param_ctx.tenant_id_ = tenant_id;
    param_ctx.dblink_id_ = dblink_id;
    param_ctx.link_type_ = link_type;
    param_ctx.sessid_ = session_id;
    param_ctx.sql_request_level_ = next_sql_req_level;
  }
  return ret;
}

int ObLinkOp::get_charset_id(ObExecContext &exec_ctx,
                             uint16_t &charset_id,
                             uint16_t &ncharset_id)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *sess_info = NULL;
  if (OB_ISNULL(sess_info = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null session info", K(ret));
  } else {
    ObCollationType coll_type = sess_info->get_nls_collation();
    ObCollationType ncoll_type = sess_info->get_nls_collation_nation();
    ObCharsetType cs_type = ObCharset::charset_type_by_coll(coll_type);
    ObCharsetType ncs_type = ObCharset::charset_type_by_coll(ncoll_type);
    if (CHARSET_INVALID == cs_type || CHARSET_INVALID == ncs_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get charset id", K(ret), K(coll_type));
    } else {
      charset_id = static_cast<uint16_t>(ObCharset::charset_type_to_ora_charset_id(cs_type));
      ncharset_id = static_cast<uint16_t>(ObCharset::charset_type_to_ora_charset_id(ncs_type));
      LOG_DEBUG("get charset id", K(ret), K(charset_id), K(ncharset_id),
                                  K(cs_type), K(ncs_type), K(coll_type), K(ncoll_type));
    }
  }
  return ret;
}

void ObLinkOp::reset_link_sql()
{
  if (OB_NOT_NULL(stmt_buf_)) {
    stmt_buf_[0] = 0;
  }
  stmt_buf_len_ = 0;
}

int ObLinkOp::set_next_sql_req_level()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo * my_session = NULL;
  my_session = ctx_.get_my_session();
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_session is NULL", K(my_session), K(ret));
  } else if (FALSE_IT(next_sql_req_level_ = my_session->get_next_sql_request_level())) {
  } else if (next_sql_req_level_ < 1 || next_sql_req_level_ > 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid next_sql_req_level", K(next_sql_req_level_), K(ret));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
