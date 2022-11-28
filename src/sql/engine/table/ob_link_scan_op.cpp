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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/table/ob_link_scan_op.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_dblink_mgr.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "sql/ob_sql_utils.h"
namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

namespace sql
{

const int64_t ObLinkScanOp::STMT_BUF_BLOCK = 1024L;

ObLinkScanOp::ObLinkScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    tenant_id_(OB_INVALID_ID),
    dblink_id_(OB_INVALID_ID),
    dblink_proxy_(NULL),
    dblink_conn_(NULL),
    result_(NULL),
    allocator_(exec_ctx.get_allocator()),
    tz_info_(NULL),
    stmt_buf_(NULL),
    stmt_buf_len_(STMT_BUF_BLOCK),
    iter_end_(false),
    row_allocator_(),
    link_type_(DBLINK_DRV_OB),
    elapse_time_(0),
    sessid_(0),
    iterated_rows_(-1)
{}

void ObLinkScanOp::reset()
{
  tz_info_ = NULL;
  reset_result();
  reset_stmt();
  reset_dblink();
  row_allocator_.reset();
}

int ObLinkScanOp::init_dblink(uint64_t dblink_id, ObDbLinkProxy *dblink_proxy)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObDbLinkSchema *dblink_schema = NULL;
  dblink_param_ctx param_ctx;
  ObSQLSessionInfo * my_session = NULL;
  my_session = ctx_.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
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
  } else if (OB_FAIL(schema_guard.get_dblink_schema(tenant_id_, dblink_id, dblink_schema))) {
    LOG_WARN("failed to get dblink schema", K(ret), K(tenant_id_), K(dblink_id));
  } else if (OB_ISNULL(dblink_schema)) {
    ret = OB_DBLINK_NOT_EXIST_TO_ACCESS;
    LOG_WARN("dblink schema is NULL", K(ret), K(dblink_id));
  } else if (FALSE_IT(set_link_driver_proto(static_cast<DblinkDriverProto>(dblink_schema->get_driver_proto())))) {
    // do nothing
  } else if (OB_FAIL(ObLinkScanOp::init_dblink_param_ctx(ctx_, param_ctx))) {
    LOG_WARN("failed to init dblink param ctx", K(ret));
  } else if (OB_FAIL(dblink_proxy->create_dblink_pool(tenant_id_,
                                                      dblink_id,
                                                      link_type_,
                                                      dblink_schema->get_host_addr(),
                                                      dblink_schema->get_tenant_name(),
                                                      dblink_schema->get_user_name(),
                                                      dblink_schema->get_password(),
                                                      ObString(""),
                                                      dblink_schema->get_conn_string(),
                                                      dblink_schema->get_cluster_name(),
                                                      param_ctx))) {
    LOG_WARN("failed to create dblink pool", K(ret));
  } else if (OB_FAIL(dblink_proxy->acquire_dblink(dblink_id, link_type_, dblink_conn_, sessid_, (plan_ctx->get_timeout_timestamp() - ObTimeUtility::current_time()) / 1000000))) {
    ObDblinkUtils::process_dblink_errno(link_type_, dblink_conn_, ret);
    LOG_WARN("failed to acquire dblink", K(ret), K(dblink_id));
  } else if (OB_FAIL(my_session->register_dblink_conn_pool(dblink_conn_->get_common_server_pool()))) {
    LOG_WARN("failed to register dblink conn pool to current session", K(ret));
  } else if (OB_ISNULL(stmt_buf_ = static_cast<char *>(allocator_.alloc(stmt_buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to init stmt buf", K(ret), K(stmt_buf_len_));
  } else {
    dblink_id_ = dblink_id;
    dblink_proxy_ = dblink_proxy;
  }
  return ret;
}

int ObLinkScanOp::init_tz_info(const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tz_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tz info is NULL", K(ret));
  } else {
    tz_info_ = tz_info;
  }
  return ret;
}

/*
 * see these two plans below:
 *
 * the 1st one is simple scan for remote table, upper operator WILL NOT call rescan().
 *
 * the 2nd one is subplan filter with rescan for remote table, upper operator WILL call rescan(),
 * and the most important thing is we can get valid param_store ONLY after rescan().
 *
 * A. so we should call read() in inner_open() for 1st case, and in rescan() for 2nd case,
 * B. or we should call read() in inner_get_next_row() for both cases?
 *
 * I have tried plan A, but I found param_count.count() is NOT 0 in both cases, and there is
 * nothing else can help make this decision, so I choose plan B now.
 *
 * explain
 * select * from s1 where c1 = concat('a', 'aa');
 *
 * ======================================
 * |ID|OPERATOR   |NAME|EST. ROWS|COST  |
 * --------------------------------------
 * |0 |LINK       |    |0        |113109|
 * |1 | TABLE SCAN|T1  |990      |113109|
 * ======================================
 *
 * Outputs & filters:
 * -------------------------------------
 *   0 - output([T1.PK], [T1.C1], [T1.C2], [T1.C3]), filter(nil), dblink_id=1100611139403793,
 *       link_stmt=select "T1"."PK", "T1"."C1", "T1"."C2", "T1"."C3" from T1 where ("T1"."C1" = CONCAT('a','aa'))
 *   1 - output([T1.C1], [T1.PK], [T1.C2], [T1.C3]), filter([T1.C1 = ?]),
 *       access([T1.C1], [T1.PK], [T1.C2], [T1.C3]), partitions(p0)
 *
 * explain
 * select * from t1 where pk = (select pk from s2 where c1 = t1.c1);
 *
 * =========================================
 * |ID|OPERATOR      |NAME|EST. ROWS|COST  |
 * -----------------------------------------
 * |0 |SUBPLAN FILTER|    |1        |221841|
 * |1 | TABLE SCAN   |T1  |2        |37    |
 * |2 | LINK         |    |0        |110903|
 * |3 |  TABLE SCAN  |T2  |990      |110903|
 * =========================================
 *
 * Outputs & filters:
 * -------------------------------------
 *   0 - output([T1.PK], [T1.C1], [T1.C2], [T1.C3]), filter([T1.PK = subquery(1)]),
 *       exec_params_([T1.C1]), onetime_exprs_(nil), init_plan_idxs_(nil)
 *   1 - output([T1.C1], [T1.PK], [T1.C2], [T1.C3]), filter(nil),
 *       access([T1.C1], [T1.PK], [T1.C2], [T1.C3]), partitions(p0)
 *   2 - output([T2.PK]), filter(nil), dblink_id=1100611139403794,
 *       link_stmt=select "T2"."PK" from T2 where ("T2"."C1" = $000)
 *   3 - output([T2.PK]), filter([T2.C1 = ?]),
 *       access([T2.C1], [T2.PK]), partitions(p0)
 *
 *
 */
int ObLinkScanOp::read(const ObString &link_stmt_fmt,
                       const ObIArray<ObParamPosIdx> &param_infos,
                       const ObParamStore &param_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(combine_link_stmt(link_stmt_fmt, param_infos, param_store))) {
    LOG_WARN("failed to gen link stmt", K(ret), K(link_stmt_fmt));
  } else if (OB_FAIL(read(stmt_buf_))) {
    LOG_WARN("failed to execute link stmt", K(ret));
  }
  return ret;
}

int ObLinkScanOp::read(const char *link_stmt)
{
  int ret = OB_SUCCESS;
  int read_ret = OB_SUCCESS;
  const char *read_errmsg = NULL;
  ObCommonServerConnectionPool *common_server_pool_ptr = NULL;
  uint16_t charset_id = 0;
  uint16_t ncharset_id = 0;
  if (OB_ISNULL(dblink_proxy_) || OB_ISNULL(link_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink_proxy or link_stmt is NULL", K(ret), KP(dblink_proxy_), KP(link_stmt));
  } else if (OB_FAIL(dblink_proxy_->dblink_read(dblink_conn_, res_, link_stmt))) { 
    ObDblinkUtils::process_dblink_errno(link_type_, dblink_conn_, ret);
    LOG_WARN("read failed", K(ret), K(link_stmt));
  } else if (OB_ISNULL(result_ = res_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get result", K(ret));
  } else if (OB_FAIL(ObLinkScanOp::get_charset_id(ctx_, charset_id, ncharset_id))) {
    LOG_WARN("failed to get charset id", K(ret));
  } else if (OB_FAIL(result_->set_expected_charset_id(charset_id, ncharset_id))) {
    LOG_WARN("failed to set result set expected charset", K(ret), K(charset_id), K(ncharset_id));
  }
  return ret;
}

int ObLinkScanOp::rollback()
{
  int ret = OB_SUCCESS;
  reset_result();
  reset_stmt();
  if (OB_ISNULL(dblink_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink_proxy is NULL", K(ret), KP(dblink_proxy_));
  } else if (OB_FAIL(dblink_proxy_->rollback(dblink_conn_))) {
    LOG_WARN("failed to rollback", K(ret));
  }
  return ret;
}

int ObLinkScanOp::combine_link_stmt(const ObString &link_stmt_fmt,
                                    const ObIArray<ObParamPosIdx> &param_infos,
                                    const ObParamStore &param_store)
{
  // combine link_stmt_fmt and parameter strings to final link stmt.
  int ret = OB_SUCCESS;
  int64_t link_stmt_pos = 0;
  int64_t next_param = 0;
  int64_t stmt_fmt_pos = 0;
  int64_t stmt_fmt_next_param_pos = (next_param < param_infos.count() ?
                                     param_infos.at(next_param).pos_ : link_stmt_fmt.length());
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
      const ObObjParam &param = param_store.at(param_idx);
      ObObjPrintParams obj_print_params = CREATE_OBJ_PRINT_PARAM(ctx_.get_my_session());
      if (DBLINK_DRV_OCI == link_type_) {
        // Ensure that when oceanbase connects to oracle, 
        // the target character set of param is the same as that of oci connection.
        obj_print_params.cs_type_ = ctx_.get_my_session()->get_nls_collation();
      }
      obj_print_params.need_cast_expr_ = true;
      while (OB_SUCC(ret) && link_stmt_pos == saved_stmt_pos) {
        //Previously, the format parameter of the print sql literal function was NULL. 
        //In the procedure scenario, when dblink reverse spell trunc(date type), it will treat the date type as a string,
        //so correct formatting parameter obj_print_params need to be given.
        if (OB_FAIL(param.print_sql_literal(stmt_buf_, stmt_buf_len_, link_stmt_pos, obj_print_params))) {
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
  if (OB_SUCC(ret)) {
    stmt_buf_[link_stmt_pos++] = 0;
  }
  return ret;
}

int ObLinkScanOp::extend_stmt_buf(int64_t need_size)
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

void ObLinkScanOp::reset_dblink()
{
  if (OB_NOT_NULL(dblink_proxy_) && OB_NOT_NULL(dblink_conn_)) {
    dblink_proxy_->release_dblink(link_type_, dblink_conn_, sessid_);
  }
  tenant_id_ = OB_INVALID_ID;
  dblink_id_ = OB_INVALID_ID;
  dblink_proxy_ = NULL;
  dblink_conn_ = NULL;
  sessid_ = 0;
}

void ObLinkScanOp::reset_stmt()
{
  if (OB_NOT_NULL(stmt_buf_)) {
    stmt_buf_[0] = 0;
  }
}

void ObLinkScanOp::reset_result()
{
  if (OB_NOT_NULL(result_)) {
    if (DBLINK_DRV_OB == link_type_) {
      result_->close();
    }
    result_ = NULL;
    res_.reset();
  }
}

OB_DEF_SERIALIZE(ObLinkScanSpec)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObLinkScanSpec, ObOpSpec));
  LST_DO_CODE(OB_UNIS_ENCODE,
              param_infos_,
              stmt_fmt_,
              dblink_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLinkScanSpec)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObLinkScanSpec, ObOpSpec));
  LST_DO_CODE(OB_UNIS_DECODE,
              param_infos_,
              stmt_fmt_,
              dblink_id_);
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

OB_DEF_SERIALIZE_SIZE(ObLinkScanSpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObLinkScanSpec, ObOpSpec));
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              param_infos_,
              stmt_fmt_,
              dblink_id_);
  return len;
}

ObLinkScanSpec::ObLinkScanSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
    allocator_(alloc),
    param_infos_(alloc),
    stmt_fmt_(),
    stmt_fmt_buf_(NULL),
    stmt_fmt_len_(0),
    dblink_id_(OB_INVALID_ID)
{}

int ObLinkScanSpec::set_stmt_fmt(const char *stmt_fmt_buf, int64_t stmt_fmt_len)
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

int ObLinkScanSpec::set_param_infos(const ObIArray<ObParamPosIdx> &param_infos)
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

int ObLinkScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session or plan_ctx is NULL", K(ret), KP(session), KP(plan_ctx));
  } else if (FALSE_IT(tenant_id_ = session->get_effective_tenant_id())) {
  } else if (OB_FAIL(init_dblink(MY_SPEC.dblink_id_, GCTX.dblink_proxy_))) {
    LOG_WARN("failed to init dblink", K(ret), K(MY_SPEC.dblink_id_));
  } else if (OB_FAIL(init_tz_info(TZ_INFO(session)))) {
    LOG_WARN("failed to tz info", K(ret), KP(session));
  } else {
    row_allocator_.set_tenant_id(tenant_id_);
    row_allocator_.set_label("linkoprow");
    row_allocator_.set_ctx_id(ObCtxIds::WORK_AREA);
  }
  return ret;
}

#define DEFINE_CAST_CTX(cs_type)                              \
  ObCollationType cast_coll_type = cs_type;                   \
  ObCastMode cm = CM_NONE;                                    \
  ObSQLSessionInfo *session = ctx_.get_my_session();          \
  if (NULL != session) {                                      \
    if (common::OB_SUCCESS != ObSQLUtils::set_compatible_cast_mode(session, cm)) {  \
      LOG_ERROR("fail to get compatible mode for cast_mode");                       \
    } else {                                                                        \
      if (is_allow_invalid_dates(session->get_sql_mode())) {                        \
        cm |= CM_ALLOW_INVALID_DATES;                                               \
      }                                                                             \
      if (is_no_zero_date(session->get_sql_mode())) {                               \
        cm |= CM_NO_ZERO_DATE;                                                      \
      }                                                                             \
    }                                                                               \
  }                                                                                 \
  ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session); \
  dtc_params.nls_collation_ = cs_type;                              \
  dtc_params.nls_collation_nation_ = cs_type;                       \
  ObCastCtx cast_ctx(&row_allocator_,                               \
                     &dtc_params,                                   \
                     get_cur_time(ctx_.get_physical_plan_ctx()),    \
                     cm,                \
                     cast_coll_type,    \
                     NULL);


int ObLinkScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  row_allocator_.reuse();
  const ObString &stmt_fmt = MY_SPEC.stmt_fmt_;
  const ObIArray<ObParamPosIdx> &param_infos = MY_SPEC.param_infos_;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan ctx", K(ret));
  } else if (need_read() && OB_FAIL(read(stmt_fmt, param_infos, plan_ctx->get_param_store()))) {
    LOG_WARN("failed to execute link stmt", K(ret), K(stmt_fmt), K(param_infos));
  } else if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("result is NULL", K(ret));
  } else if (0 == (++iterated_rows_ % CHECK_STATUS_ROWS_INTERVAL)
             && OB_FAIL(ctx_.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(result_->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    const ObIArray<ObExpr *> &output = spec_.output_;
    for (int64_t i = 0; OB_SUCC(ret) && i < output.count(); i++) {
      ObObj value;
      ObObj new_value;
      ObObj *res_obj = &value;
      ObExpr *expr = output.at(i);
      ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
      if (OB_FAIL(result_->get_obj(i, value, tz_info_, &row_allocator_))) {
        LOG_WARN("failed to get obj", K(ret), K(i));
      } else if (OB_UNLIKELY(ObNullType != value.get_type() &&    // use get_type(), do not use get_type_class() here.
                            (value.get_type() != expr->obj_meta_.get_type() || 
                                  (ob_is_string_or_lob_type(value.get_type()) &&
                                   ob_is_string_or_lob_type(expr->obj_meta_.get_type()) &&
                                   value.get_type() == expr->obj_meta_.get_type() && 
                                   value.get_collation_type() != expr->obj_meta_.get_collation_type())))) {
        DEFINE_CAST_CTX(expr->datum_meta_.cs_type_);
        if (OB_FAIL(ObObjCaster::to_type(expr->obj_meta_.get_type(), cast_ctx, value, new_value))) {
          LOG_WARN("cast obj failed", K(ret), K(value), K(expr->obj_meta_));
        } else {
          res_obj = &new_value;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(datum.from_obj(*res_obj))) {
          LOG_WARN("from obj failed", K(ret));
        } else {
          expr->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }

  return ret;
}


int ObLinkScanOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rollback())) {
    LOG_WARN("failed to execute rollback", K(ret));
  } else {
    reset();
  }
  return ret;
}

int ObLinkScanOp::inner_rescan()
{
  reset_result();
  reset_stmt();
  iter_end_ = false;
  iterated_rows_ = -1;
  return ObOperator::inner_rescan();
}

int ObLinkScanOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t row_cnt = 0;
  if (iter_end_) {
    brs_.size_ = 0;
    brs_.end_ = true;
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    auto loop_cnt = common::min(max_row_cnt, MY_SPEC.max_batch_size_);
    while (row_cnt < loop_cnt && OB_SUCC(ret)) {
      batch_info_guard.set_batch_idx(row_cnt);
      if (OB_FAIL(inner_get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner get next row failed", K(ret));
        }
      } else {
        const ObIArray<ObExpr *> &output = spec_.output_;
        for (int64_t i = 0; OB_SUCC(ret) && i < output.count(); i++) {
          ObExpr *expr = output.at(i);
          if (T_QUESTIONMARK != expr->type_ &&
              (ob_is_string_or_lob_type(expr->datum_meta_.type_) ||
              ob_is_raw(expr->datum_meta_.type_) || ob_is_json(expr->datum_meta_.type_))) {
            ObDatum &datum = expr->locate_expr_datum(eval_ctx_);
            char *buf = NULL;
            if (OB_ISNULL(buf = expr->get_str_res_mem(eval_ctx_, datum.len_))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            } else {
              MEMCPY(buf, datum.ptr_, datum.len_);
              datum.ptr_ = buf;
            }
          }
        }
        row_cnt++;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      iter_end_ = true;
    }
    if (OB_SUCC(ret)) {
      brs_.size_ = row_cnt;
      brs_.end_ = iter_end_;
      brs_.skip_->reset(row_cnt);
      const ObIArray<ObExpr *> &output = spec_.output_;
      for (int64_t i = 0; OB_SUCC(ret) && i < output.count(); i++) {
        ObExpr *expr = output.at(i);
        if (expr->is_batch_result()) {
          ObBitVector &eval_flags = expr->get_evaluated_flags(eval_ctx_);
          eval_flags.set_all(row_cnt);
        }
      }
    }
  }
  return ret;
}

int ObLinkScanOp::init_dblink_param_ctx(ObExecContext &exec_ctx, dblink_param_ctx &param_ctx)
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
  }
  return ret;
}

int ObLinkScanOp::get_charset_id(ObExecContext &exec_ctx,
                                 uint16_t &charset_id, uint16_t &ncharset_id)
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

} // end namespace sql
} // end namespace oceanbase
