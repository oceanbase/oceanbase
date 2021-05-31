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

#include "sql/engine/table/ob_link_scan.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_dblink_mgr.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

namespace sql {

const int64_t ObLinkScan::ObLinkScanCtx::STMT_BUF_BLOCK = 1024L;

ObLinkScan::ObLinkScanCtx::ObLinkScanCtx(ObExecContext& ctx)
    : ObPhyOperatorCtx(ctx),
      dblink_id_(OB_INVALID_ID),
      dblink_proxy_(NULL),
      dblink_conn_(NULL),
      result_(NULL),
      allocator_(ctx.get_allocator()),
      tz_info_(NULL),
      stmt_buf_(NULL),
      stmt_buf_len_(STMT_BUF_BLOCK)
{}

void ObLinkScan::ObLinkScanCtx::reset()
{
  tz_info_ = NULL;
  reset_result();
  reset_stmt();
  reset_dblink();
}

int ObLinkScan::ObLinkScanCtx::init_dblink(uint64_t dblink_id, ObDbLinkProxy* dblink_proxy)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(dblink_id);
  ObSchemaGetterGuard schema_guard;
  const ObDbLinkSchema* dblink_schema = NULL;

  if (OB_NOT_NULL(dblink_proxy_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("link scan ctx already inited", K(ret));
  } else if (OB_ISNULL(dblink_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dblink_proxy is NULL", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_dblink_schema(dblink_id, dblink_schema))) {
    LOG_WARN("failed to get dblink schema", K(ret), K(dblink_id));
  } else if (OB_ISNULL(dblink_schema)) {
    ret = OB_DBLINK_NOT_EXIST_TO_ACCESS;
    LOG_WARN("dblink schema is NULL", K(ret), K(dblink_id));
  } else if (OB_FAIL(dblink_proxy->create_dblink_pool(dblink_id,
                 dblink_schema->get_host_addr(),
                 dblink_schema->get_tenant_name(),
                 dblink_schema->get_user_name(),
                 dblink_schema->get_password(),
                 ObString("")))) {
    LOG_WARN("failed to create dblink pool", K(ret));
  } else if (OB_FAIL(dblink_proxy->acquire_dblink(dblink_id, dblink_conn_))) {
    LOG_WARN("failed to acquire dblink", K(ret), K(dblink_id));
  } else if (OB_ISNULL(stmt_buf_ = static_cast<char*>(allocator_.alloc(stmt_buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to init stmt buf", K(ret), K(stmt_buf_len_));
  } else {
    dblink_id_ = dblink_id;
    dblink_proxy_ = dblink_proxy;
  }
  return ret;
}

int ObLinkScan::ObLinkScanCtx::init_tz_info(const ObTimeZoneInfo* tz_info)
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
int ObLinkScan::ObLinkScanCtx::read(
    const ObString& link_stmt_fmt, const ObIArray<ObParamPosIdx>& param_infos, const ObParamStore& param_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(combine_link_stmt(link_stmt_fmt, param_infos, param_store))) {
    LOG_WARN("failed to gen link stmt", K(ret), K(link_stmt_fmt));
  } else if (OB_FAIL(read(stmt_buf_))) {
    LOG_WARN("failed to execute link stmt", K(ret));
  }
  return ret;
}

int ObLinkScan::ObLinkScanCtx::read(const char* link_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dblink_proxy_) && OB_ISNULL(link_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink_proxy or link_stmt is NULL", K(ret), KP(dblink_proxy_), KP(link_stmt));
  } else if (OB_FAIL(dblink_proxy_->dblink_read(dblink_conn_, res_, link_stmt))) {
    LOG_WARN("read failed", K(ret));
  } else if (OB_ISNULL(result_ = res_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get result", K(ret));
  }
  return ret;
}

int ObLinkScan::ObLinkScanCtx::rollback()
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

int ObLinkScan::ObLinkScanCtx::get_next(const ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("result is NULL", K(ret));
  } else if (OB_FAIL(result_->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_row_.get_count(); i++) {
    if (OB_FAIL(result_->get_obj(i, cur_row_.get_cell(i), tz_info_, &allocator_))) {
      LOG_WARN("failed to get obj", K(ret), K(i), K(cur_row_));
    }
  }
  row = &cur_row_;
  return ret;
}

int ObLinkScan::ObLinkScanCtx::rescan()
{
  reset_result();
  reset_stmt();
  return OB_SUCCESS;
}

int ObLinkScan::ObLinkScanCtx::combine_link_stmt(
    const ObString& link_stmt_fmt, const ObIArray<ObParamPosIdx>& param_infos, const ObParamStore& param_store)
{
  // combine link_stmt_fmt and parameter strings to final link stmt.
  int ret = OB_SUCCESS;
  int64_t link_stmt_pos = 0;
  int64_t next_param = 0;
  int64_t stmt_fmt_pos = 0;
  int64_t stmt_fmt_next_param_pos =
      (next_param < param_infos.count() ? param_infos.at(next_param).pos_ : link_stmt_fmt.length());
  while (OB_SUCC(ret) && stmt_fmt_pos < link_stmt_fmt.length()) {
    // copy from link_stmt_fmt.
    if (stmt_fmt_pos < stmt_fmt_next_param_pos) {
      int64_t copy_len = stmt_fmt_next_param_pos - stmt_fmt_pos;
      if (link_stmt_pos + copy_len > stmt_buf_len_ && OB_FAIL(extend_stmt_buf(link_stmt_pos + copy_len))) {
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
      const ObObjParam& param = param_store.at(param_idx);
      while (OB_SUCC(ret) && link_stmt_pos == saved_stmt_pos) {
        if (OB_FAIL(param.print_sql_literal(stmt_buf_, stmt_buf_len_, link_stmt_pos, NULL))) {
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
          stmt_fmt_next_param_pos =
              (next_param < param_infos.count() ? param_infos.at(next_param).pos_ : link_stmt_fmt.length());
        }
      }  // while
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "fmt_pos should not be greater than fmt_next_param_pos", K(ret), K(stmt_fmt_pos), K(stmt_fmt_next_param_pos));
    }
  }
  if (OB_SUCC(ret)) {
    stmt_buf_[link_stmt_pos++] = 0;
  }
  return ret;
}

int ObLinkScan::ObLinkScanCtx::extend_stmt_buf(int64_t need_size)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size =
      (need_size > stmt_buf_len_) ? (need_size / STMT_BUF_BLOCK + 1) * STMT_BUF_BLOCK : stmt_buf_len_ + STMT_BUF_BLOCK;
  char* alloc_buf = static_cast<char*>(allocator_.alloc(alloc_size));
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

void ObLinkScan::ObLinkScanCtx::reset_dblink()
{
  if (OB_NOT_NULL(dblink_proxy_) && OB_NOT_NULL(dblink_conn_)) {
    dblink_proxy_->release_dblink(dblink_conn_);
  }
  dblink_id_ = OB_INVALID_ID;
  dblink_proxy_ = NULL;
  dblink_conn_ = NULL;
}

void ObLinkScan::ObLinkScanCtx::reset_stmt()
{
  if (OB_NOT_NULL(stmt_buf_)) {
    stmt_buf_[0] = 0;
  }
}

void ObLinkScan::ObLinkScanCtx::reset_result()
{
  if (OB_NOT_NULL(result_)) {
    result_->close();
    result_ = NULL;
    res_.reset();
  }
}

OB_DEF_SERIALIZE(ObLinkScan)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, param_infos_, stmt_fmt_, dblink_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLinkScan)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, param_infos_, stmt_fmt_, dblink_id_);
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(stmt_fmt_len_ = stmt_fmt_.length())) {
    // nothing.
  } else if (OB_ISNULL(stmt_fmt_buf_ = static_cast<char*>(allocator_.alloc(stmt_fmt_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc stmt_fmt_buf", K(ret), K(stmt_fmt_len_));
  } else {
    MEMCPY(stmt_fmt_buf_, stmt_fmt_.ptr(), stmt_fmt_len_);
    stmt_fmt_.assign(stmt_fmt_buf_, static_cast<int32_t>(stmt_fmt_len_));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLinkScan)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, param_infos_, stmt_fmt_, dblink_id_);
  return len;
}

ObLinkScan::ObLinkScan(ObIAllocator& allocator)
    : ObNoChildrenPhyOperator(allocator),
      allocator_(allocator),
      param_infos_(allocator),
      stmt_fmt_(),
      stmt_fmt_buf_(NULL),
      stmt_fmt_len_(0),
      dblink_id_(OB_INVALID_ID)
{}

ObLinkScan::~ObLinkScan()
{}

void ObLinkScan::reset()
{
  reset_inner();
  ObNoChildrenPhyOperator::reset();
}

void ObLinkScan::reuse()
{
  reset_inner();
  ObNoChildrenPhyOperator::reuse();
}

void ObLinkScan::reset_inner()
{
  //  res_types_.reset();
  param_infos_.reset();
  stmt_fmt_.reset();
  if (OB_NOT_NULL(stmt_fmt_buf_)) {
    allocator_.free(stmt_fmt_buf_);
    stmt_fmt_buf_ = NULL;
  }
  stmt_fmt_len_ = 0;
  dblink_id_ = OB_INVALID_ID;
}

int ObLinkScan::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObLinkScanCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc op ctx", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("create current row failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObLinkScan::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObLinkScanCtx* link_scan_ctx = NULL;
  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("failed to init link scan ctx", K(ret));
  } else if (OB_ISNULL(link_scan_ctx = GET_PHY_OPERATOR_CTX(ObLinkScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get link scan ctx", K(ret));
  } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session or plan_ctx is NULL", K(ret), KP(session), KP(plan_ctx));
  } else if (OB_FAIL(link_scan_ctx->init_dblink(dblink_id_, GCTX.dblink_proxy_))) {
    LOG_WARN("failed to init dblink", K(ret), K(dblink_id_));
  } else if (OB_FAIL(link_scan_ctx->init_tz_info(TZ_INFO(session)))) {
    LOG_WARN("failed to tz info", K(ret), KP(session));
  }
  return ret;
}

int ObLinkScan::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObLinkScanCtx* link_scan_ctx = GET_PHY_OPERATOR_CTX(ObLinkScanCtx, ctx, get_id());
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  if (OB_ISNULL(link_scan_ctx) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get link scan ctx or plan ctx", K(ret));
  } else if (link_scan_ctx->need_read() &&
             OB_FAIL(link_scan_ctx->read(stmt_fmt_, param_infos_, plan_ctx->get_param_store()))) {
    LOG_WARN("failed to execute link stmt", K(ret), K(stmt_fmt_), K(param_infos_));
  } else if (OB_FAIL(link_scan_ctx->get_next(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObLinkScan::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObLinkScanCtx* link_scan_ctx = NULL;
  if (OB_ISNULL(link_scan_ctx = GET_PHY_OPERATOR_CTX(ObLinkScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get link scan ctx", K(ret));
  } else if (OB_FAIL(link_scan_ctx->rollback())) {
    LOG_WARN("failed to execute rollback", K(ret));
  } else {
    link_scan_ctx->reset();
  }
  return ret;
}

int ObLinkScan::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObLinkScanCtx* link_scan_ctx = NULL;
  if (OB_ISNULL(link_scan_ctx = GET_PHY_OPERATOR_CTX(ObLinkScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get link scan ctx", K(ret));
  } else if (OB_FAIL(link_scan_ctx->rescan())) {
    LOG_WARN("failed to rescan link scan", K(ret));
  }
  return ret;
}

int ObLinkScan::set_param_infos(const ObIArray<ObParamPosIdx>& param_infos)
{
  int ret = OB_SUCCESS;
  param_infos_.reset();
  if (OB_FAIL(init_array_size<>(param_infos_, param_infos.count()))) {
    LOG_WARN("failed to init fixed array", K(param_infos.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_infos.count(); i++) {
    if (OB_FAIL(param_infos_.push_back(param_infos.at(i)))) {
      LOG_WARN("failed to push back param info", K(ret), K(param_infos.at(i)));
    }
  }
  return ret;
}

int ObLinkScan::set_stmt_fmt(const char* stmt_fmt_buf, int64_t stmt_fmt_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_fmt_buf) || stmt_fmt_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt_fmt_buf is null or stmt_fmt_len is less than 0", K(ret), KP(stmt_fmt_buf), K(stmt_fmt_len));
  } else if (OB_ISNULL(stmt_fmt_buf_ = static_cast<char*>(allocator_.alloc(stmt_fmt_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc stmt_fmt_buf", K(ret), K(stmt_fmt_len));
  } else {
    MEMCPY(stmt_fmt_buf_, stmt_fmt_buf, stmt_fmt_len);
    stmt_fmt_len_ = stmt_fmt_len;
    stmt_fmt_.assign(stmt_fmt_buf_, static_cast<int32_t>(stmt_fmt_len_));
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
