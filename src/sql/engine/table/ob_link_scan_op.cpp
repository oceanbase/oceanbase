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
#include "sql/dblink/ob_tm_service.h"
#include "sql/dblink/ob_dblink_utils.h"
#include "lib/string/ob_hex_utils_base.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#ifdef OB_BUILD_DBLINK
#include "lib/oracleclient/ob_oci_connection.h"
#endif
namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

namespace sql
{

ObLinkScanSpec::ObLinkScanSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObLinkSpec(alloc, type), has_for_update_(false), select_exprs_(alloc)
{}

OB_SERIALIZE_MEMBER((ObLinkScanSpec, ObLinkSpec), has_for_update_, select_exprs_);

ObLinkScanOp::ObLinkScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObLinkOp(exec_ctx, spec, input),
    result_(NULL),
    tz_info_(NULL),
    iter_end_(false),
    row_allocator_(),
    iterated_rows_(-1),
    tm_session_(NULL),
    tm_rm_connection_(NULL),
    reverse_link_(NULL),
    conn_type_(sql::DblinkGetConnType::DBLINK_POOL),
    snapshot_created_(NULL)
{
}

void ObLinkScanOp::reset()
{
  tz_info_ = NULL;
  dblink_schema_ = NULL;
  reset_result();
  reset_link_sql();
  reset_dblink();
  row_allocator_.reset();
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
#ifdef OB_BUILD_DBLINK
int ObLinkScanOp::init_conn_snapshot(bool &new_snapshot)
{
  int ret = OB_SUCCESS;
  new_snapshot = false;
  void *snapshot = NULL;
  bool snapshot_inited = false;
  if (NULL == dblink_conn_ || DBLINK_DRV_OCI != link_type_) {
    // do nothing.
  } else {
    hash::ObHashMap<uint64_t, void*> &dblink_snapshot_map = ctx_.get_dblink_snapshot_map();
    ObOciConnection *ob_oci_conn = static_cast<ObOciConnection*>(dblink_conn_);
    if (!dblink_snapshot_map.created() && OB_FAIL(dblink_snapshot_map.create(8, "dblinksnapshot"))) {
      LOG_WARN("create hash map failed", K(ret));
    } else if (OB_FAIL(dblink_snapshot_map.get_refactored(dblink_id_, snapshot))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get dblink snapshot failed", K(ret));
      } else if (OB_FAIL(ob_oci_conn->create_snapshot(snapshot))) {
        LOG_WARN("init snapshot failed", K(ret));
      } else if (OB_ISNULL(snapshot)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("create null snapshot", K(ret), K(get_spec().get_id()), K(link_type_));
      } else {
        snapshot_created_ = snapshot;
        new_snapshot = true;
        snapshot_inited = false;
        LOG_TRACE("init conn snapshot, create one", K(get_spec().get_id()), K(ob_oci_conn),
                  K(ob_oci_conn->get_oci_connection()), K(snapshot));
      }
    } else {
      snapshot_inited = true;
      LOG_TRACE("init conn snapshot, fetch one", K(get_spec().get_id()), K(ob_oci_conn),
                K(ob_oci_conn->get_oci_connection()), K(snapshot));
    }
    if (OB_SUCC(ret) && OB_FAIL(ob_oci_conn->set_conn_snapshot(snapshot, snapshot_inited))) {
      LOG_ERROR("set conn snapshot failed", K(ret));
    }
  }
  return ret;
}

int ObLinkScanOp::free_snapshot()
{
  int ret = OB_SUCCESS;
  if (NULL == snapshot_created_) {
    // do nothing.
  } else if (OB_UNLIKELY(NULL == dblink_conn_ || DBLINK_DRV_OCI != link_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink conn is null or unexpected dblink type", K(ret), K(dblink_conn_), K(link_type_));
  } else {
    ObOciConnection *ob_oci_conn = static_cast<ObOciConnection*>(dblink_conn_);
    void *snapshot = NULL;
    bool snapshot_inited = false;
    if (OB_FAIL(ob_oci_conn->free_snapshot(snapshot_created_))) {
      LOG_WARN("set conn snapshot failed", K(ret));
    }
    snapshot_created_ = NULL;
  }
  return ret;
}
#endif
int ObLinkScanOp::inner_execute_link_stmt(const char *link_stmt)
{
  int ret = OB_SUCCESS;
  uint16_t charset_id = 0;
  uint16_t ncharset_id = 0;
  ObSQLSessionInfo * my_session = NULL;
  my_session = ctx_.get_my_session();
  transaction::ObTransID tx_id;
  bool have_lob = false;
  res_.set_enable_use_result(true);
  bool new_snapshot = false;
  if (OB_ISNULL(link_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), KP(link_stmt));
  } else if (sql::DblinkGetConnType::TM_CONN == conn_type_) {
    if (OB_FAIL(tm_rm_connection_->execute_read(OB_INVALID_TENANT_ID, link_stmt, res_))) {
      LOG_WARN("failed to read table data by tm_rm_connection", K(ret), K(link_stmt), K(tm_rm_connection_->get_dblink_driver_proto()));
    } else {
      LOG_DEBUG("succ to read table data by tm_rm_connection", K(link_stmt), K(tm_rm_connection_->get_dblink_driver_proto()));
    }
  } else if (sql::DblinkGetConnType::TEMP_CONN == conn_type_) {
    if (OB_FAIL(reverse_link_->read(link_stmt, res_))) {
      LOG_WARN("failed to read table data by reverse_link", K(ret));
    } else {
      LOG_DEBUG("succ to read table data by reverse_link");
    }
  } else if (OB_ISNULL(dblink_proxy_) || OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), KP(dblink_proxy_), KP(my_session));
  } else if (need_tx(my_session) && OB_FAIL(ObTMService::tm_rm_start(ctx_, link_type_, dblink_conn_, tx_id))) {
    LOG_WARN("failed to tm_rm_start", K(ret), K(dblink_id_), K(dblink_conn_), K(sessid_));
#ifdef OB_BUILD_DBLINK
  } else if (OB_FAIL(init_conn_snapshot(new_snapshot))) {
    LOG_WARN("init conn snapshot failed", K(ret));
#endif
  } else {
    if (OB_FAIL(dblink_proxy_->dblink_read(dblink_conn_, res_, link_stmt))) {
      LOG_WARN("read failed", K(ret), K(link_type_), K(dblink_conn_), K(link_stmt));
#ifdef OB_BUILD_DBLINK
    } else if (new_snapshot && OB_FAIL(ctx_.get_dblink_snapshot_map().set_refactored(dblink_id_, snapshot_created_))) {
      LOG_WARN("set snapshot failed", K(ret));
    }
    if (OB_FAIL(ret) && new_snapshot) {
      int tmp_ret = OB_SUCCESS;
      // when fail and have created new snapshot before, need free this snapshot.
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = free_snapshot()))) {
        LOG_ERROR("free dblink snapshot failed", K(tmp_ret));
      }
#endif
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(result_ = res_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get result", K(ret));
  } else if (DBLINK_DRV_OCI == link_type_ && ObDblinkService::check_lob_in_result(result_, have_lob)) {
    LOG_WARN("failed to check lob result", K(ret));
  } else if (have_lob) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("dblink not support lob type", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dblink fetch lob type data");
  } else if (OB_FAIL(ObLinkOp::get_charset_id(ctx_, charset_id, ncharset_id))) {
    LOG_WARN("failed to get charset id", K(ret));
  } else if (OB_FAIL(result_->set_expected_charset_id(charset_id, ncharset_id))) {// for oci dblink set expected result charset, actually useless...
    LOG_WARN("failed to set result set expected charset", K(ret), K(charset_id), K(ncharset_id));
  } else {
    LOG_DEBUG("succ to dblink read", K(link_stmt), KP(dblink_conn_));
  }
  return ret;
}

void ObLinkScanOp::reset_dblink()
{
  int tmp_ret = OB_SUCCESS;
#ifdef OB_BUILD_DBLINK
  if (DBLINK_DRV_OCI == link_type_ &&
      NULL != dblink_conn_) {
    if (OB_SUCCESS != (tmp_ret = static_cast<ObOciConnection *>(dblink_conn_)->free_oci_stmt())) {
      LOG_WARN_RET(tmp_ret, "failed to close oci result", K(tmp_ret));
    }
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = free_snapshot()))) {
      LOG_WARN_RET(tmp_ret, "free dblink snapshot failed");
    }
  } else if (NULL != tm_rm_connection_ &&
             DblinkDriverProto::DBLINK_DRV_OCI == tm_rm_connection_->get_dblink_driver_proto() &&
             OB_SUCCESS != (tmp_ret = static_cast<ObOciConnection *>(tm_rm_connection_)->free_oci_stmt())) {
    LOG_WARN_RET(tmp_ret, "failed to close oci result", K(tmp_ret));
  }
#endif
  if (OB_NOT_NULL(dblink_proxy_) && OB_NOT_NULL(dblink_conn_) && !in_xa_trascaction_ &&
             OB_SUCCESS != (tmp_ret = dblink_proxy_->release_dblink(link_type_, dblink_conn_))) {
    LOG_WARN_RET(tmp_ret, "failed to release connection", K(tmp_ret));
  }
  if (OB_NOT_NULL(reverse_link_)) {
    reverse_link_->close();
  }
  tenant_id_ = OB_INVALID_ID;
  dblink_id_ = OB_INVALID_ID;
  dblink_proxy_ = NULL;
  dblink_conn_ = NULL;
  tm_rm_connection_ = NULL;
  reverse_link_ = NULL;
  sessid_ = 0;
  conn_type_ = sql::DblinkGetConnType::DBLINK_POOL;
}

void ObLinkScanOp::reset_result()
{
  if (OB_NOT_NULL(result_)) {
    if (DBLINK_DRV_OB == link_type_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = result_->close())) {
        LOG_WARN_RET(tmp_ret, "failed to close result", K(tmp_ret));
      }
    }
    result_ = NULL;
    res_.reset();
  }
}

int ObLinkScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  const ObPhysicalPlan *phy_plan = MY_SPEC.get_phy_plan();
  int64_t tm_sessid = -1;
  reverse_link_ = NULL;
  stmt_buf_len_ += head_comment_length_;
  if (NULL != phy_plan) {
    tm_sessid = phy_plan->tm_sessid_;
  }
  if (OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session or plan_ctx is NULL", K(ret), KP(session), KP(plan_ctx));
  } else if (OB_FAIL(set_next_sql_req_level())) {
    LOG_WARN("failed to set next sql req level", K(ret));
  } else if (FALSE_IT(tenant_id_ = session->get_effective_tenant_id())) {
  } else if (MY_SPEC.is_reverse_link_) {
    // RM process sql within @! and @xxxx! send by TM
    LOG_DEBUG("link scan op, RM process sql within @! and @xxxx! send by TM");
    conn_type_ = sql::DblinkGetConnType::TEMP_CONN;
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get plan ctx", K(ret));
    } else if (OB_FAIL(session->get_dblink_context().get_reverse_link(reverse_link_))) {
      LOG_WARN("fail to get reverse_link", K(ret));
    } else if (NULL == reverse_link_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reverse_link_ is NULL", K(ret), KP(reverse_link_), K(session->get_sessid()));
    } else if (OB_FAIL(reverse_link_->open(next_sql_req_level_))) {
      LOG_WARN("failed to open reverse_link", K(ret));
    }
  } else if (-1 != tm_sessid) { // TM process sql within @xxxx send by RM
    LOG_DEBUG("link scan op, TM process sql within @xxxx send by RM", K(tm_sessid));
    sql::ObSQLSessionMgr *session_mgr = GCTX.session_mgr_;
    if (OB_ISNULL(session_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), KP(session_mgr));
    } else if (OB_FAIL(session_mgr->get_session(static_cast<uint32_t>(tm_sessid), tm_session_))) {
      LOG_WARN("failed to get session", K(ret), K(tm_sessid));
    } else {
      if (NULL != tm_session_ &&
              tm_session_->get_dblink_context().get_dblink_conn(MY_SPEC.dblink_id_, tm_rm_connection_)) {
        LOG_WARN("failed to get dblink connection from session", KP(tm_session_), K(ret));
      } else if (NULL != tm_rm_connection_){
        conn_type_ = sql::DblinkGetConnType::TM_CONN;
        LOG_DEBUG("get tm sesseion and connection", KP(tm_session_), KP(tm_rm_connection_));
      }
      session_mgr->revert_session(tm_session_);
      tm_session_ = NULL;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sql::DblinkGetConnType::DBLINK_POOL == conn_type_ &&
             OB_FAIL(init_dblink(MY_SPEC.dblink_id_, GCTX.dblink_proxy_, MY_SPEC.has_for_update_))) {
    LOG_WARN("failed to init dblink", K(ret), K(MY_SPEC.dblink_id_), K(MY_SPEC.is_reverse_link_));
  } else if (OB_FAIL(init_tz_info(TZ_INFO(session)))) {
    LOG_WARN("failed to tz info", K(ret), KP(session));
  } else {
    row_allocator_.set_tenant_id(tenant_id_);
    row_allocator_.set_label("linkoprow");
    row_allocator_.set_ctx_id(ObCtxIds::WORK_AREA);
  }
  if (OB_SUCC(ret) && OB_ISNULL(stmt_buf_ = static_cast<char *>(allocator_.alloc(stmt_buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to init stmt buf", K(ret), K(stmt_buf_len_));
  }
  // close reverse_link
  if (NULL != reverse_link_ && OB_FAIL(ret)) {
    reverse_link_->close();
    LOG_DEBUG("close reverse link", KP(reverse_link_), K(ret));
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
  row_allocator_.reuse();
  clear_evaluated_flag();
  return fetch_row();
}

int ObLinkScanOp::fetch_row()
{
  int ret = OB_SUCCESS;
  const ObString &stmt_fmt = MY_SPEC.stmt_fmt_;
  const ObIArray<ObParamPosIdx> &param_infos = MY_SPEC.param_infos_;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan ctx", K(ret));
  } else if (need_read() &&
             OB_FAIL(execute_link_stmt(stmt_fmt,
                                       param_infos,
                                       plan_ctx->get_param_store(),
                                       reverse_link_))) {
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
    } else {
      reset_result();
    }
  } else {
    const ObIArray<ObExpr *> &select_exprs =
          (MY_SPEC.select_exprs_.empty() ? spec_.output_ : MY_SPEC.select_exprs_);
    for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
      ObExpr *expr = select_exprs.at(i);
      if (!expr->is_const_expr()) {
        ObObj value;
        ObObj new_value;
        ObObj *res_obj = &value;
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
          } else if (is_lob_storage(res_obj->get_type()) &&
                    OB_FAIL(ob_adjust_lob_datum(*res_obj, expr->obj_meta_,
                                                get_exec_ctx().get_allocator(), datum))) {
            LOG_WARN("adjust lob datum failed", K(ret), K(i), K(res_obj->get_meta()), K(expr->obj_meta_));
          }
        }
      } else {
        ObDatum *datum = NULL;
        if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
          LOG_WARN("expr evaluate failed", K(ret), KPC(expr));
        }
      }
      if (OB_SUCC(ret)) {
        expr->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}


int ObLinkScanOp::inner_close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

int ObLinkScanOp::inner_rescan()
{
  reset_result();
  reset_link_sql();
  iter_end_ = false;
  iterated_rows_ = -1;
  int tmp_ret = OB_SUCCESS;
#ifdef OB_BUILD_DBLINK
  if (DBLINK_DRV_OCI == link_type_ &&
      NULL != dblink_conn_) {
    if (OB_SUCCESS != (tmp_ret = static_cast<ObOciConnection *>(dblink_conn_)->free_oci_stmt())) {
      LOG_WARN_RET(tmp_ret, "failed to close oci result", K(tmp_ret));
    }
  }
#endif
  return ObOperator::inner_rescan();
}

int ObLinkScanOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t row_cnt = 0;
  clear_evaluated_flag();
  if (iter_end_) {
    brs_.size_ = 0;
    brs_.end_ = true;
    reset_result();
  } else {
    row_allocator_.reuse();
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    auto loop_cnt = common::min(max_row_cnt, MY_SPEC.max_batch_size_);
    while (row_cnt < loop_cnt && OB_SUCC(ret)) {
      batch_info_guard.set_batch_idx(row_cnt);
      if (OB_FAIL(fetch_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner get next row failed", K(ret));
        }
      } else {
        const ObIArray<ObExpr *> &select_exprs =
          (MY_SPEC.select_exprs_.empty() ? spec_.output_ : MY_SPEC.select_exprs_);
        for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
          ObExpr *expr = select_exprs.at(i);
          if (expr->is_const_expr()) {
            // do nothing
          } else if (T_QUESTIONMARK != expr->type_ &&
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
    }
  }
  return ret;
}

// PLEASE check the input parameter in public interface
bool ObLinkScanOp::need_tx(const ObSQLSessionInfo *my_session) const
{
  bool ret_bool = false;
  if (MY_SPEC.has_for_update_) {
    // case 1, if select for update, tm_rm_start is required to be called.
    ret_bool = true;
  } else if (false && my_session->is_in_transaction()) {
    // case 2, if select in transaction, tm_rm_start is required to be called.
    ret_bool = true;
  }
  return ret_bool;
}

} // end namespace sql
} // end namespace oceanbase
