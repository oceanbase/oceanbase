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
#include "share/ob_autoincrement_service.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_spliter.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/ob_sql_trans_control.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "observer/ob_inner_sql_connection_pool.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace storage;
using namespace share;
using namespace share::schema;
using namespace observer;
namespace sql {
OB_SERIALIZE_MEMBER(DMLPartInfo, partition_key_, part_row_cnt_);

int SeRowkeyItem::init(const ObExprPtrIArray& row, ObEvalCtx& ctx, ObIAllocator& alloc, const int64_t rowkey_cnt)
{
  int ret = OB_SUCCESS;
  CK(rowkey_cnt <= row.count());
  if (OB_SUCC(ret)) {
    // the %alloc is arena allocator, no need to free.
    if (OB_ISNULL(datums_ = static_cast<ObDatum*>(alloc.alloc(rowkey_cnt * sizeof(*datums_))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      new (datums_) ObDatum[rowkey_cnt];
    }
    row_ = row.get_data();
    cnt_ = rowkey_cnt;
  }
  ObDatum* datum = NULL;
  for (int i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    if (OB_FAIL((row.at(i)->eval(ctx, datum)))) {
      LOG_WARN("evaluate expression failed", K(ret));
    } else {
      datums_[i] = *datum;
    }
  }
  return ret;
}

bool SeRowkeyItem::operator==(const SeRowkeyItem& other) const
{
  bool equal = true;
  if (cnt_ != other.cnt_) {
    equal = false;
  } else {
    for (int64_t i = 0; equal && i < cnt_; ++i) {
      ObExprCmpFuncType cmp_func = row_[i]->basic_funcs_->null_first_cmp_;
      equal = cmp_func(datums_[i], other.datums_[i]) == 0;
    }
  }
  return equal;
}

uint64_t SeRowkeyItem::hash() const
{
  uint64_t hash_val = 0;
  for (int64_t i = 0; i < cnt_; ++i) {
    ObExprHashFuncType hash_func = row_[i]->basic_funcs_->default_hash_;
    hash_val = hash_func(datums_[i], hash_val);
  }
  return hash_val;
}

int SeRowkeyItem::copy_datum_data(ObIAllocator& alloc)
{
  // the %alloc is arena allocator, no need to free.
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != datums_)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
      if (OB_FAIL(datums_[i].deep_copy(datums_[i], alloc))) {
        LOG_WARN("copy datum data failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableModifyInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  bool from_multi_table_dml = false;
  if (OB_UNLIKELY(!op.is_dml_operator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is invalid", K(op));
  } else if (OB_UNLIKELY(ObTaskSpliter::INVALID_SPLIT == task_info.get_task_split_type())) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid task split type", K(task_info.get_task_split_type()));
  } else if (ObTaskSpliter::DISTRIBUTED_SPLIT != task_info.get_task_split_type() &&
             ObTaskSpliter::INSERT_SPLIT != task_info.get_task_split_type()) {
    is_single_part_ = true;
  } else {
    is_single_part_ = false;
  }
  if (OB_SUCC(ret)) {
    location_idx_ = task_info.get_location_idx();
    from_multi_table_dml = static_cast<const ObTableModify&>(op).from_multi_table_dml();
  }
  if (OB_SUCC(ret) && from_multi_table_dml) {
    int64_t part_key_cnt = task_info.get_range_location().part_locs_.count();
    part_infos_.reset();
    part_infos_.set_allocator(&ctx.get_allocator());
    if (OB_FAIL(part_infos_.init(part_key_cnt))) {
      LOG_WARN("init part keys array failed", K(ret), K(part_key_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_key_cnt; ++i) {
      uint64_t part_key_ref_id = task_info.get_range_location().part_locs_.at(i).part_key_ref_id_;
      const ObPartitionKey& part_key = task_info.get_range_location().part_locs_.at(i).partition_key_;
      ObRowStore* row_store = task_info.get_range_location().part_locs_.at(i).row_store_;
      if (OB_ISNULL(row_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row_store is null");
      } else if (part_key_ref_id == op.get_id()) {
        DMLPartInfo part_info;
        part_info.partition_key_ = part_key;
        part_info.part_row_cnt_ = row_store->get_row_count();
        if (OB_FAIL(part_infos_.push_back(part_info))) {
          LOG_WARN("store partition info failed", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTableModifyInput, location_idx_, part_infos_);

void ObTableModify::ObTableModifyCtx::destroy()
{
  close_inner_conn();
  ObPhyOperatorCtx::destroy_base();
  call_dtor(saved_session_);
  if (nullptr != rowkey_dist_ctx_) {
    rowkey_dist_ctx_->destroy();
    rowkey_dist_ctx_ = nullptr;
  }
}

int ObTableModify::ObTableModifyCtx::open_inner_conn()
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnectionPool* pool = NULL;
  ObSQLSessionInfo* session = NULL;
  ObISQLConnection* conn;
  if (OB_ISNULL(sql_proxy_ = exec_ctx_.get_sql_proxy())) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is NULL", K(ret));
  } else if (OB_ISNULL(session = exec_ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is NULL", K(ret));
  } else if (NULL != session->get_inner_conn()) {
    // do nothing.
  } else if (OB_ISNULL(pool = static_cast<ObInnerSQLConnectionPool*>(sql_proxy_->get_pool()))) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection pool is NULL", K(ret));
  } else if (INNER_POOL != pool->get_type()) {
    LOG_WARN("connection pool type is not inner", K(ret), K(pool->get_type()));
  } else if (OB_FAIL(pool->acquire(session, conn))) {
    LOG_WARN("failed to acquire inner connection", K(ret));
  } else {
    /**
     * session is the only data struct which can pass through multi layer nested sql,
     * so we put inner conn in session to share it within multi layer nested sql.
     */
    session->set_inner_conn(conn);
    need_close_conn_ = true;
  }
  if (OB_SUCC(ret)) {
    inner_conn_ = static_cast<ObInnerSQLConnection*>(session->get_inner_conn());
    tenant_id_ = session->get_effective_tenant_id();
    is_nested_session_ = session->is_nested_session();
  }
  return ret;
}

int ObTableModify::ObTableModifyCtx::close_inner_conn()
{
  /**
   * we can call it even if open_inner_conn() failed, because only the one who call
   * open_inner_conn() succeed first will do close job by "if (need_close_conn_)".
   */
  int ret = OB_SUCCESS;
  if (need_close_conn_) {
    ObSQLSessionInfo* session = exec_ctx_.get_my_session();
    if (OB_ISNULL(sql_proxy_) || OB_ISNULL(session)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql_proxy of session is NULL", K(ret), KP(sql_proxy_), KP(session));
    } else {
      OZ(sql_proxy_->close(static_cast<ObInnerSQLConnection*>(session->get_inner_conn()), true));
      OX(session->set_inner_conn(NULL));
    }
    need_close_conn_ = false;
  }
  sql_proxy_ = NULL;
  inner_conn_ = NULL;
  return ret;
}

int ObTableModify::ObTableModifyCtx::begin_nested_session(bool skip_cur_stmt_tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inner_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner connection is NULL", K(ret));
  } else if (OB_FAIL(inner_conn_->begin_nested_session(get_saved_session(), saved_conn_, skip_cur_stmt_tables))) {
    LOG_WARN("failed to begin nested session", K(ret));
  }
  return ret;
}

int ObTableModify::ObTableModifyCtx::end_nested_session()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inner_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner connection is NULL", K(ret));
  } else if (OB_FAIL(inner_conn_->end_nested_session(get_saved_session(), saved_conn_))) {
    LOG_WARN("failed to end nested session", K(ret));
  }
  return ret;
}

int ObTableModify::ObTableModifyCtx::set_foreign_key_cascade(bool is_cascade)
{
  int ret = OB_SUCCESS;
  OV(OB_NOT_NULL(inner_conn_));
  OZ(inner_conn_->set_foreign_key_cascade(is_cascade));
  return ret;
}

int ObTableModify::ObTableModifyCtx::get_foreign_key_cascade(bool& is_cascade) const
{
  int ret = OB_SUCCESS;
  OV(OB_NOT_NULL(inner_conn_));
  OZ(inner_conn_->get_foreign_key_cascade(is_cascade));
  return ret;
}

int ObTableModify::ObTableModifyCtx::set_foreign_key_check_exist(bool is_check_exist)
{
  int ret = OB_SUCCESS;
  OV(OB_NOT_NULL(inner_conn_));
  OZ(inner_conn_->set_foreign_key_check_exist(is_check_exist));
  return ret;
}

int ObTableModify::ObTableModifyCtx::get_foreign_key_check_exist(bool& is_check_exist) const
{
  int ret = OB_SUCCESS;
  OV(OB_NOT_NULL(inner_conn_));
  OZ(inner_conn_->get_foreign_key_check_exist(is_check_exist));
  return ret;
}

int ObTableModify::ObTableModifyCtx::execute_write(const char* sql)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_ISNULL(inner_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner connection is NULL", K(ret));
  } else if (OB_ISNULL(sql)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql is NULL");
  } else if (OB_FAIL(inner_conn_->execute_write(tenant_id_, sql, affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(tenant_id_), K(sql));
  }
  return ret;
}

int ObTableModify::ObTableModifyCtx::execute_read(const char* sql, ObMySQLProxy::MySQLResult& res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inner_conn_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner connection or sql proxy is NULL", K(ret), KP(inner_conn_), KP(sql_proxy_));
  } else if (OB_ISNULL(sql)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql is NULL");
  } else if (OB_FAIL(inner_conn_->execute_read(tenant_id_, sql, res))) {
    LOG_WARN("failed to execute sql", K(ret), K(tenant_id_), K(sql));
  }
  return ret;
}

int ObTableModify::ObTableModifyCtx::check_stack()
{
  int ret = OB_SUCCESS;
  const int max_stack_deep = 16;
  bool is_stack_overflow = false;
  ObSQLSessionInfo* session = exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("sql session is NULL", K(ret));
  } else if (session->get_nested_count() >= max_stack_deep) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(max_stack_deep), K(session->get_nested_count()));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("fail to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  }
  return ret;
}

/**
 * TODO
 * after we support nested table like: http://www.orafaq.com/wiki/NESTED_TABLE,
 * we must consider :PARENT if base table is nested table, so some const variable
 * below should change, and before_row / after_row procedure should add parameter
 * for :PARENT.
 */
// const int64_t ObTableModify::ObTableModifyCtx::ALL_PARAM_COUNT = 3;
// const int64_t ObTableModify::ObTableModifyCtx::PARAM_OLD_IDX = 0;
// const int64_t ObTableModify::ObTableModifyCtx::PARAM_NEW_IDX = 1;
// const int64_t ObTableModify::ObTableModifyCtx::PARAM_EVENT_IDX = 2;
// :OLD, :NEW.
// const int64_t ObTableModify::ObTableModifyCtx::WHEN_POINT_PARAM_OFFSET = 0;
// const int64_t ObTableModify::ObTableModifyCtx::WHEN_POINT_PARAM_COUNT = 2;
// dml_event.
// const int64_t ObTableModify::ObTableModifyCtx::STMT_POINT_PARAM_OFFSET = 2;
// const int64_t ObTableModify::ObTableModifyCtx::STMT_POINT_PARAM_COUNT = 1;
// :OLD, :NEW, dml_event.
// const int64_t ObTableModify::ObTableModifyCtx::ROW_POINT_PARAM_OFFSET = 0;
// const int64_t ObTableModify::ObTableModifyCtx::ROW_POINT_PARAM_COUNT = 3;

OB_DEF_SERIALIZE(ObTableModify)
{
  int ret = OB_SUCCESS;
  int64_t fake_part_id = 0;           // In order to be compatible with the old version
  uint64_t fake_child_table_id = 0;   // In order to be compatible with the old version
  uint64_t fake_parent_table_id = 0;  // In order to be compatible with the old version
  BASE_SER((ObTableModify, ObSingleChildPhyOperator));
  OB_UNIS_ENCODE(table_id_);
  OB_UNIS_ENCODE(column_ids_);
  OB_UNIS_ENCODE(column_infos_);
  OB_UNIS_ENCODE(is_ignore_);
  OB_UNIS_ENCODE(column_conv_infos_);
  OB_UNIS_ENCODE(fake_part_id);
  OB_UNIS_ENCODE(fake_child_table_id);
  OB_UNIS_ENCODE(fake_parent_table_id);
  OB_UNIS_ENCODE(primary_key_ids_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_dlist(returning_exprs_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize returning_exprs_", K(ret));
    }
  }
  OB_UNIS_ENCODE(from_multi_table_dml_);
  OB_UNIS_ENCODE(index_tid_);
  OB_UNIS_ENCODE(fk_args_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_param_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize table_param_", K(ret));
    } else if (OB_FAIL(serialize_dlist(check_constraint_exprs_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize check_constraint_exprs_", K(ret));
    }
  }
  OB_UNIS_ENCODE(need_filter_null_row_);
  OB_UNIS_ENCODE(distinct_algo_);
  OB_UNIS_ENCODE(gi_above_);
  OB_UNIS_ENCODE(is_returning_);
  OB_UNIS_ENCODE(need_check_pk_is_null_);
  bool has_old_row_rowid_ = (NULL != old_row_rowid_);
  OB_UNIS_ENCODE(has_old_row_rowid_);
  if (has_old_row_rowid_) {
    OB_UNIS_ENCODE(*old_row_rowid_);
  }
  bool has_new_row_rowid_ = (NULL != new_row_rowid_);
  OB_UNIS_ENCODE(has_new_row_rowid_);
  if (has_new_row_rowid_) {
    OB_UNIS_ENCODE(*new_row_rowid_);
  }
  OB_UNIS_ENCODE(need_skip_log_user_error_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_dlist(tsc_virtual_column_exprs_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize startup_exprs_", K(ret));
    }
  }
  OB_UNIS_ENCODE(is_pdml_index_maintain_);
  OB_UNIS_ENCODE(table_location_uncertain_);
  OB_UNIS_ENCODE(is_pdml_index_maintain_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableModify)
{
  int ret = OB_SUCCESS;
  int64_t fake_part_id = 0;
  uint64_t fake_child_table_id = 0;
  uint64_t fake_parent_table_id = 0;
  BASE_DESER((ObTableModify, ObSingleChildPhyOperator));
  if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan is null", K(ret));
  }
  OB_UNIS_DECODE(table_id_);
  OB_UNIS_DECODE(column_ids_);
  OB_UNIS_DECODE(column_infos_);
  OB_UNIS_DECODE(is_ignore_);
  OB_UNIS_DECODE(column_conv_infos_);
  OB_UNIS_DECODE(fake_part_id);
  OB_UNIS_DECODE(fake_child_table_id);
  OB_UNIS_DECODE(fake_parent_table_id);
  OB_UNIS_DECODE(primary_key_ids_);
  OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, returning_exprs_, my_phy_plan_);
  OB_UNIS_DECODE(from_multi_table_dml_);
  OB_UNIS_DECODE(index_tid_);
  if (OB_SUCC(ret) && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100) {
    // index_tid_ is just added in the version 2.1, in order to compatible with the plan of old server
    // if index_tid_ is OB_INVALID_ID, use the value of table_id_ instead
    index_tid_ = table_id_;
  }
  OB_UNIS_DECODE(fk_args_);

  // compatibility code
  if (OB_SUCC(ret)) {
    if (pos < data_len) {
      if (OB_FAIL(table_param_.deserialize(buf, data_len, pos))) {
        LOG_WARN("deserialize table_param_ failed", K(ret));
      } else if (table_param_.is_valid() && OB_FAIL(table_param_.prepare_storage_param(column_ids_))) {
        LOG_WARN("convert storage param fail", K(ret), K(column_ids_), K(table_param_));
      }
    } else {
      table_param_.reset();
    }
  }
  OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, check_constraint_exprs_, my_phy_plan_);
  OB_UNIS_DECODE(need_filter_null_row_);
  OB_UNIS_DECODE(distinct_algo_);
  OB_UNIS_DECODE(gi_above_);
  OB_UNIS_DECODE(is_returning_);
  OB_UNIS_DECODE(need_check_pk_is_null_);
  bool has_old_row_rowid = false;
  OB_UNIS_DECODE(has_old_row_rowid);
  if (OB_SUCC(ret)) {
    if (has_old_row_rowid) {
      if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, old_row_rowid_))) {
        LOG_WARN("make sql expression failed", K(ret));
      } else if (OB_LIKELY(old_row_rowid_ != NULL)) {
        OB_UNIS_DECODE(*old_row_rowid_);
      }
    } else {
      old_row_rowid_ = NULL;
    }
  }
  bool has_new_row_rowid = false;
  OB_UNIS_DECODE(has_new_row_rowid);
  if (OB_SUCC(ret)) {
    if (has_new_row_rowid) {
      if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, new_row_rowid_))) {
        LOG_WARN("make sql expression failed", K(ret));
      } else if (OB_LIKELY(new_row_rowid_ != NULL)) {
        OB_UNIS_DECODE(*new_row_rowid_);
      }
    } else {
      new_row_rowid_ = NULL;
    }
  }
  OB_UNIS_DECODE(need_skip_log_user_error_);
  OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, tsc_virtual_column_exprs_, my_phy_plan_);
  OB_UNIS_DECODE(is_pdml_index_maintain_);
  OB_UNIS_DECODE(table_location_uncertain_);
  OB_UNIS_DECODE(is_pdml_index_maintain_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableModify)
{
  int64_t len = 0;
  int64_t fake_part_id = 0;
  uint64_t fake_child_table_id = 0;
  uint64_t fake_parent_table_id = 0;
  BASE_ADD_LEN((ObTableModify, ObSingleChildPhyOperator));
  OB_UNIS_ADD_LEN(table_id_);
  OB_UNIS_ADD_LEN(column_ids_);
  OB_UNIS_ADD_LEN(column_infos_);
  OB_UNIS_ADD_LEN(is_ignore_);
  OB_UNIS_ADD_LEN(column_conv_infos_);
  OB_UNIS_ADD_LEN(fake_part_id);
  OB_UNIS_ADD_LEN(fake_child_table_id);
  OB_UNIS_ADD_LEN(fake_parent_table_id);
  OB_UNIS_ADD_LEN(primary_key_ids_);
  len += get_dlist_serialize_size(returning_exprs_);
  OB_UNIS_ADD_LEN(from_multi_table_dml_);
  OB_UNIS_ADD_LEN(index_tid_);
  OB_UNIS_ADD_LEN(fk_args_);
  len += table_param_.get_serialize_size();
  len += get_dlist_serialize_size(check_constraint_exprs_);
  OB_UNIS_ADD_LEN(need_filter_null_row_);
  OB_UNIS_ADD_LEN(distinct_algo_);
  OB_UNIS_ADD_LEN(gi_above_);
  OB_UNIS_ADD_LEN(is_returning_);
  OB_UNIS_ADD_LEN(need_check_pk_is_null_);
  bool has_old_row_rowid = (NULL != old_row_rowid_);
  OB_UNIS_ADD_LEN(has_old_row_rowid);
  if (has_old_row_rowid) {
    OB_UNIS_ADD_LEN(*old_row_rowid_);
  }
  bool has_new_row_rowid = (NULL != new_row_rowid_);
  OB_UNIS_ADD_LEN(has_new_row_rowid);
  if (has_new_row_rowid) {
    OB_UNIS_ADD_LEN(*new_row_rowid_);
  }
  OB_UNIS_ADD_LEN(need_skip_log_user_error_);
  len += get_dlist_serialize_size(tsc_virtual_column_exprs_);
  OB_UNIS_ADD_LEN(is_pdml_index_maintain_);
  OB_UNIS_ADD_LEN(table_location_uncertain_);
  OB_UNIS_ADD_LEN(is_pdml_index_maintain_);
  return len;
}

OB_SERIALIZE_MEMBER(ObColumnConvInfo, type_, column_flags_, str_values_);

int ObTableModify::ObDMLRowIterator::init()
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (OB_LIKELY(op_.get_projector_size() > 0)) {
    // create projector row space
    if (OB_ISNULL(ptr = ctx_.get_allocator().alloc(sizeof(ObObj) * op_.get_projector_size()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc projector row failed");
    } else {
      project_row_.cells_ = static_cast<ObObj*>(ptr);
      project_row_.count_ = op_.get_projector_size();
    }
  }
  return ret;
}

int ObTableModify::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTableModifyInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create input", K(ret), "op_id", get_id(), "op_type", get_type(), "op_name", get_name());
  }
  UNUSED(input);
  return ret;
}

int ObTableModify::ObDMLRowIterator::get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  const ObNewRow* input_row = NULL;
  bool got_row = false;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  // switch bind array iterator in DML plan
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("plan ctx is null", K(ret));
  }
  while (OB_SUCC(ret) && !got_row) {
    if (OB_FAIL(op_.get_next_row(ctx_, input_row))) {
      if (OB_ITER_END == ret) {
        if (plan_ctx->get_bind_array_count() <= 0) {
          // not contain bind array, do nothing
        } else if (OB_FAIL(op_.switch_iterator(ctx_))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("switch op iterator failed", K(ret), "op_type", ob_phy_operator_type_str(op_.get_type()));
          }
        }
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("get next row from operator failed", K(ret));
      }
    } else {
      got_row = true;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(0 >= input_row->projector_size_) || OB_ISNULL(project_row_.cells_) ||
             OB_ISNULL(input_row->cells_)) {
    // || OB_UNLIKELY(project_row_.count_ > input_row->projector_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid projector or projector_size",
        K(project_row_.cells_),
        K(project_row_.count_),
        K(input_row->cells_),
        K(input_row->projector_size_));
  } else {
    if (PHY_INSERT_ON_DUP == op_.get_type() || PHY_MERGE == op_.get_type()) {
      // for insert on duplicate statement
      // project_row.count_ is the number of inserted columns
      // input_row is the number of update columns
      for (int64_t i = 0; i < input_row->get_count(); i++) {
        project_row_.cells_[i] = input_row->get_cell(i);
      }
      project_row_.count_ = input_row->get_count();
    } else {
      for (int64_t i = 0; i < project_row_.count_; ++i) {
        // project cells, not need deep copy
        project_row_.cells_[i] = input_row->get_cell(i);
      }
    }
    row = const_cast<ObNewRow*>(&project_row_);
    LOG_DEBUG("output row", K(project_row_));
  }
  return ret;
}

int ObTableModify::ObDMLRowIterator::get_next_rows(ObNewRow*& row, int64_t& row_count)
{
  int ret = OB_SUCCESS;
  const ObNewRow* input_rows = NULL;
  bool got_rows = false;
  // swtich bind array iterator in DML plan
  while (OB_SUCC(ret) && !got_rows) {
    if (OB_FAIL(op_.get_next_rows(ctx_, input_rows, row_count))) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(op_.switch_iterator(ctx_))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("switch op iterator failed", K(ret), "op_type", ob_phy_operator_type_str(op_.get_type()));
          }
        }
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("get next row from operator failed", K(ret));
      }
    } else {
      got_rows = true;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(0 >= input_rows->projector_size_) || OB_ISNULL(project_row_.cells_) ||
             OB_ISNULL(input_rows->cells_)) {
    // || OB_UNLIKELY(project_row_.count_ > input_row->projector_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid projector or projector_size",
        K(project_row_.cells_),
        K(project_row_.count_),
        K(input_rows->cells_),
        K(input_rows->projector_size_));
  } else {
    if (PHY_INSERT_ON_DUP == op_.get_type()) {
      // for insert on duplicate statement
      // project_row.count_ is the number of inserted columns
      // input_row is the number of update columns
      for (int64_t i = 0; i < row_count; i++) {
        for (int64_t j = 0; j < input_rows->get_count(); j++) {
          project_row_.cells_[j] = input_rows[i].get_cell(j);
        }

        for (int64_t j = 0; j < input_rows->get_count(); j++) {
          input_rows[i].cells_[j] = project_row_.cells_[j];
        }
      }
    } else {
      for (int64_t i = 0; i < row_count; i++) {
        for (int64_t j = 0; j < project_row_.count_; j++) {
          // project cells, not need deep copy
          project_row_.cells_[j] = input_rows[i].get_cell(j);
        }

        for (int64_t j = 0; j < project_row_.count_; j++) {
          input_rows[i].cells_[j] = project_row_.cells_[j];
        }
        (const_cast<ObNewRow*>(&input_rows[i]))->count_ = project_row_.count_;
        (const_cast<ObNewRow*>(&input_rows[i]))->projector_ = NULL;
        (const_cast<ObNewRow*>(&input_rows[i]))->projector_size_ = 0;
      }
    }
    row = const_cast<ObNewRow*>(input_rows);
  }
  return ret;
}

void ObTableModify::ObDMLRowIterator::reset()
{
  if (OB_LIKELY(project_row_.cells_)) {
    ctx_.get_allocator().free(project_row_.cells_);
    project_row_.cells_ = NULL;
    project_row_.count_ = 0;
  }
}

ObTableModify::ObTableModify(ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc),
      table_id_(OB_INVALID_ID),
      index_tid_(OB_INVALID_ID),
      //    child_table_id_(OB_INVALID_ID),
      //    parent_table_id_(OB_INVALID_ID),
      is_ignore_(false),
      from_multi_table_dml_(false),
      column_ids_(alloc),
      primary_key_ids_(alloc),
      column_infos_(alloc),
      column_conv_infos_(alloc),
      returning_exprs_(),
      check_constraint_exprs_(),
      fk_args_(alloc),
      tg_event_(0),
      table_param_(alloc),
      need_filter_null_row_(false),
      distinct_algo_(T_DISTINCT_NONE),
      gi_above_(false),
      is_returning_(false),
      stmt_id_idx_(OB_INVALID_INDEX),
      need_check_pk_is_null_(false),
      old_row_rowid_(NULL),
      new_row_rowid_(NULL),
      need_skip_log_user_error_(false),
      tsc_virtual_column_exprs_(),
      is_pdml_index_maintain_(false),
      table_location_uncertain_(false)
{}

ObTableModify::~ObTableModify()
{}

void ObTableModify::reset()
{
  table_id_ = OB_INVALID_ID;
  index_tid_ = OB_INVALID_ID;
  //  child_table_id_ = OB_INVALID_ID;
  //  parent_table_id_ = OB_INVALID_ID;
  is_ignore_ = false;
  column_infos_.reset();
  column_ids_.reset();
  primary_key_ids_.reset();
  column_conv_infos_.reset();
  returning_exprs_.reset();
  check_constraint_exprs_.reset();
  fk_args_.reset();
  table_param_.reset();
  ObSingleChildPhyOperator::reset();
  from_multi_table_dml_ = false;
  need_filter_null_row_ = false;
  distinct_algo_ = T_DISTINCT_NONE;
  stmt_id_idx_ = OB_INVALID_INDEX;
  need_check_pk_is_null_ = false;
  old_row_rowid_ = NULL;
  new_row_rowid_ = NULL;
  need_skip_log_user_error_ = false;
  tsc_virtual_column_exprs_.reset();
  is_pdml_index_maintain_ = false;
  table_location_uncertain_ = false;
}

void ObTableModify::reuse()
{
  table_id_ = OB_INVALID_ID;
  index_tid_ = OB_INVALID_ID;
  //  child_table_id_ = OB_INVALID_ID;
  //  parent_table_id_ = OB_INVALID_ID;
  column_infos_.reuse();
  is_ignore_ = false;
  from_multi_table_dml_ = false;
  column_ids_.reuse();
  primary_key_ids_.reuse();
  column_conv_infos_.reuse();
  returning_exprs_.reset();
  fk_args_.reuse();
  table_param_.reset();
  need_filter_null_row_ = false;
  ObSingleChildPhyOperator::reuse();
  distinct_algo_ = T_DISTINCT_NONE;
  stmt_id_idx_ = OB_INVALID_INDEX;
  need_check_pk_is_null_ = false;
  old_row_rowid_ = NULL;
  new_row_rowid_ = NULL;
  need_skip_log_user_error_ = false;
  tsc_virtual_column_exprs_.reset();
  is_pdml_index_maintain_ = false;
  table_location_uncertain_ = false;
}

int ObTableModify::switch_iterator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableModifyCtx* table_modify_ctx = GET_PHY_OPERATOR_CTX(ObTableModifyCtx, ctx, get_id());
  if (OB_ISNULL(table_modify_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table modify ctx is null", K(ret), K(table_modify_ctx));
  } else if (table_modify_ctx->rowkey_dist_ctx_ != nullptr) {
    table_modify_ctx->rowkey_dist_ctx_->reuse();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSingleChildPhyOperator::switch_iterator(ctx))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("switch iterator failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableModify::set_autoinc_param_pkey(ObExecContext& ctx, const ObPartitionKey& pkey) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  int64_t auto_increment_cache_size = -1;
  if (OB_ISNULL((plan_ctx = ctx.get_physical_plan_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(my_session->get_auto_increment_cache_size(auto_increment_cache_size))) {
    LOG_WARN("fail to get increment factor", K(ret));
  } else {
    ObIArray<AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      autoinc_params.at(i).pkey_ = pkey;
      autoinc_params.at(i).is_ignore_ = is_ignore_;
      autoinc_params.at(i).auto_increment_cache_size_ = auto_increment_cache_size;
    }
  }
  return ret;
}

int ObTableModify::add_column_info(const ColumnContent& column)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_infos_.push_back(column))) {
    LOG_WARN("failed to push column info", K(ret), K(column));
  }
  return ret;
}

int ObTableModify::add_column_conv_info(const ObExprResType& res_type, const uint64_t column_flags,
    ObIAllocator& allocator, const ObIArray<ObString>* str_values /*NULL*/, const ObString* column_info_str /*NULL*/)
{
  int ret = OB_SUCCESS;
  ObColumnConvInfo column_info(allocator);
  column_info.column_flags_ = column_flags;
  ObExprResType dst_type = res_type;
  if (ObLobType == dst_type.get_type()) {
    dst_type.set_type(ObLongTextType);
  }
  if (OB_FAIL(column_info.type_.assign(dst_type))) {
    LOG_WARN("copy res type failed", K(ret), K(dst_type));
  } else if (OB_NOT_NULL(column_info_str) &&
             OB_FAIL(ob_write_string(allocator, *column_info_str, column_info.column_info_))) {
    LOG_WARN("fail to write string", K(ret));
  } else if (NULL != str_values) {
    if (OB_FAIL(column_info.str_values_.reserve(str_values->count()))) {
      LOG_WARN("fail to reserve array", K(ret), K(str_values->count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < str_values->count(); ++i) {
      ObString str_copy;
      if (OB_FAIL(ob_write_string(allocator, str_values->at(i), str_copy))) {
        LOG_WARN("fail to write string", K(ret));
      } else if (OB_FAIL(column_info.str_values_.push_back(str_copy))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(column_conv_infos_.push_back(column_info))) {
    LOG_WARN("failed to push column conv info", K(ret), K(column_info.type_), K_(column_info.column_flags));
  }
  return ret;
}

int ObTableModify::add_column_id(uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_id(column_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_id));
  } else if (OB_FAIL(column_ids_.push_back(column_id))) {
    LOG_WARN("fail to push back to column_ids", K(ret), K(column_id));
  }
  return ret;
}

int ObTableModify::set_primary_key_ids(const ObIArray<uint64_t>& column_ids)
{
  return primary_key_ids_.assign(column_ids);
}

int ObTableModify::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init operator context failed", K(ret), K_(id));
  } else if (OB_FAIL(handle_op_ctx(ctx))) {
    LOG_WARN("handle op ctx failed", K(ret));
  } else if (OB_FAIL(init_foreign_key_operation(ctx))) {
    LOG_WARN("failed to init foreign key operation", K(ret));
  }
  return ret;
}

int ObTableModify::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  // release cache_handle for auto-increment
  share::ObAutoincrementService& auto_service = share::ObAutoincrementService::get_instance();
  ObTableModifyCtx* modify_ctx = GET_PHY_OPERATOR_CTX(ObTableModifyCtx, ctx, get_id());
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid physical plan ctx", K(plan_ctx));
  } else {
    ObIArray<share::AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
    for (int64_t i = 0; i < autoinc_params.count(); ++i) {
      if (NULL != autoinc_params.at(i).cache_handle_) {
        auto_service.release_handle(autoinc_params.at(i).cache_handle_);
      }
    }
  }
  // see ObMultiPartUpdate::inner_open().
  if (has_foreign_key() && NULL != modify_ctx && modify_ctx->need_foreign_key_checks()) {
    modify_ctx->close_inner_conn();
  }
  return ret;
}

int ObTableModify::lock_row(
    ObExecContext& ctx, const ObNewRow& row, storage::ObDMLBaseParam& dml_param, const ObPartitionKey& pkey) const
{
  int ret = OB_SUCCESS;
  ObPartitionService* partition_service = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else {
    ObLockFlag lock_flag = LF_NONE;
    // There is not only the primary key column in the row, but we ensure that the primary key is listed first
    // partition_service internally obtains the number of primary key columns from the schema
    bool old_flag = dml_param.only_data_table_;
    dml_param.only_data_table_ = true;  // row lock can be added to the main table
    if (OB_FAIL(partition_service->lock_rows(
            my_session->get_trans_desc(), dml_param, dml_param.timeout_, pkey, row, lock_flag))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to lock rows", K(ret));
      }
    } else {
      SQL_ENG_LOG(DEBUG, "lock rows", K(row));
    }
    dml_param.only_data_table_ = old_flag;
  }
  return ret;
}

int ObTableModify::calculate_virtual_column(ObExprCtx& expr_ctx, ObNewRow& calc_row, int64_t row_num) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPhyOperator::calculate_virtual_column(expr_ctx, calc_row))) {
    if (OB_ISNULL(expr_ctx.exec_ctx_)) {
      LOG_WARN("exec ctx of expr ctx is null", K(ret));
    } else {
      log_user_error_inner(ret, expr_ctx.err_col_idx_, row_num, *expr_ctx.exec_ctx_);
    }
  }
  return ret;
}

int ObTableModify::calc_returning_row(ObExprCtx& expr_ctx, const ObNewRow& cur_row, ObNewRow& return_row) const
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  const ObColumnExpression* p = NULL;
  CK(return_row.get_count() == returning_exprs_.get_size());
  DLIST_FOREACH(node, returning_exprs_)
  {
    p = static_cast<const ObColumnExpression*>(node);
    CK(OB_NOT_NULL(p = static_cast<const ObColumnExpression*>(node)));
    OZ(p->calc(expr_ctx, cur_row, return_row.get_cell(i++)));
  }
  return ret;
}

int ObTableModify::save_returning_row(
    ObExprCtx& expr_ctx, const ObNewRow& row, ObNewRow& return_row, ObRowStore& store) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_returning_row(expr_ctx, row, return_row))) {
    LOG_WARN("failed to calc returning row", K(ret));
  } else if (OB_FAIL(store.add_row(return_row, false))) {
    LOG_WARN("failed to add row into row store", K(ret));
  }
  return ret;
}

int ObTableModify::validate_row(ObExprCtx& expr_ctx, ObCastCtx& column_conv_ctx, ObNewRow& calc_row) const
{
  return validate_row(expr_ctx, column_conv_ctx, calc_row, true, true);
}

int ObTableModify::validate_normal_column(ObExprCtx& expr_ctx, ObCastCtx& column_conv_ctx, ObNewRow& calc_row) const
{
  return validate_row(expr_ctx, column_conv_ctx, calc_row, true, false);
}

int ObTableModify::validate_virtual_column(ObExprCtx& expr_ctx, ObNewRow& calc_row, int64_t row_num) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(validate_row(expr_ctx, expr_ctx.column_conv_ctx_, calc_row, false, true))) {
    if (OB_ISNULL(expr_ctx.exec_ctx_)) {
      LOG_WARN("exec ctx is null", K(ret));
    } else {
      log_user_error_inner(ret, expr_ctx.err_col_idx_, row_num, *expr_ctx.exec_ctx_);
    }
  }
  return ret;
}

int ObTableModify::validate_row(ObExprCtx& expr_ctx, ObCastCtx& column_conv_ctx, ObNewRow& calc_row,
    bool check_normal_column, bool check_virtual_column) const
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(column_conv_infos_.count() > 0)) {
    bool is_strict = true;
    // yeah, get the true value of is_strict from the cast_mode
    if (column_conv_ctx.cast_mode_ & CM_NO_RANGE_CHECK) {
      is_strict = false;
      column_conv_ctx.cast_mode_ &= ~(CM_NO_RANGE_CHECK);
    }
    column_conv_ctx.is_ignore_ = is_ignore_;
    if (share::is_oracle_mode()) {
      column_conv_ctx.is_ignore_ = true;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < calc_row.get_count(); ++i) {
      uint64_t column_flags = column_conv_infos_.at(i).column_flags_;
      // rowid pseudo column is virtual column, but cannot insert rowid pseudo column,
      // so we ignore rowid column here
      bool is_virtual_column = ObSchemaUtils::is_virtual_generated_column(column_flags) ||
                               ObSchemaUtils::is_stored_generated_column(column_flags);
      if ((check_normal_column && is_virtual_column) || (check_virtual_column && !is_virtual_column)) {
        // nothing.
      } else {
        ObObj& obj = calc_row.get_cell(i);
        const ObExprResType& res_type = column_conv_infos_.at(i).type_;
        const ObString* column_info = &(column_conv_infos_.at(i).column_info_);
        const ObIArray<ObString>* str_values = &(column_conv_infos_.at(i).str_values_);
        if (share::is_oracle_mode() && (ob_is_blob(res_type.get_type(), res_type.get_collation_type()) ||
                                           ob_is_blob_locator(res_type.get_type(), res_type.get_collation_type()))) {
          column_conv_ctx.cast_mode_ |= CM_ENABLE_BLOB_CAST;
        }
        if (OB_UNLIKELY(res_type.is_null())) {
          // The column type cannot be null, and the result type is null, indicating that
          // the conversion type of the column is empty, and no type conversion is required
          // do nothing.
        } else if (OB_UNLIKELY(ObSchemaUtils::is_fulltext_column(column_flags))) {
          // do nothing, inner fulltext column
        } else if (OB_FAIL(ObExprColumnConv::convert_skip_null_check(
                       obj, obj, res_type, is_strict, column_conv_ctx, str_values, column_info))) {
          expr_ctx.err_col_idx_ = i;
          LOG_WARN("validate row failed",
              K(ret),
              K(column_conv_infos_.count()),
              K(res_type),
              K(i),
              K(calc_row.get_cell(i)),
              K(calc_row.get_count()));
        } else {
        }
      }
    }
  }
  return ret;
}

int ObTableModify::check_row_null(
    ObExecContext& ctx, const ObNewRow& calc_row, const ObIArray<ColumnContent>& column_infos) const
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2220) {
    OV(calc_row.get_count() == column_infos.count(), OB_ERR_UNEXPECTED, calc_row, column_infos);
    for (int i = 0; OB_SUCC(ret) && i < column_infos.count(); i++) {
      bool is_nullable = column_infos.at(i).is_nullable_;
      bool is_cell_null =
          calc_row.get_cell(i).is_null() || (lib::is_oracle_mode() && calc_row.get_cell(i).is_null_oracle());
      if (!is_nullable && is_cell_null) {
        if (is_ignore_) {
          if (is_oracle_mode()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dml with ignore not supported in oracle mode");
          } else if (OB_FAIL(ObObjCaster::get_zero_value(column_infos.at(i).column_type_,
                         column_infos.at(i).coll_type_,
                         const_cast<ObObj&>(calc_row.get_cell(i))))) {
            LOG_WARN("get column default zero value failed", K(ret), K(column_infos.at(i)));
          } else {
            // output warning msg
            ObString column_name = column_infos.at(i).column_name_;
            ObSQLUtils::copy_and_convert_string_charset(ctx.get_allocator(),
                column_name,
                column_name,
                CS_TYPE_UTF8MB4_BIN,
                ctx.get_my_session()->get_local_collation_connection());
            LOG_USER_WARN(OB_BAD_NULL_ERROR, column_name.length(), column_name.ptr());
          }
        } else {
          ObString column_name = column_infos.at(i).column_name_;
          ObSQLUtils::copy_and_convert_string_charset(ctx.get_allocator(),
              column_name,
              column_name,
              CS_TYPE_UTF8MB4_BIN,
              ctx.get_my_session()->get_local_collation_connection());
          ret = OB_BAD_NULL_ERROR;
          LOG_USER_ERROR(OB_BAD_NULL_ERROR, column_name.length(), column_name.ptr());
        }
      }
    }
  }
  return ret;
}

int ObTableModify::get_part_location(
    ObExecContext& ctx, const ObPhyTableLocation& table_location, const ObPartitionReplicaLocation*& out) const
{
  int ret = OB_SUCCESS;
  out = NULL;
  int64_t location_idx = 0;
  if (table_location.get_partition_location_list().count() > 1) {
    ObTableModifyInput* modify_input = NULL;
    if ((modify_input = GET_PHY_OP_INPUT(ObTableModifyInput, ctx, get_id())) != NULL) {
      location_idx = modify_input->get_location_idx();
    } else {
      ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
      LOG_WARN("Multi-partition DML not supported", K(ret), K(table_location));
    }
  }
  CK(location_idx >= 0 && location_idx < table_location.get_partition_cnt());
  if (OB_SUCC(ret)) {
    out = &(table_location.get_partition_location_list().at(location_idx));
    LOG_DEBUG("get part location", KPC(out), K(location_idx));
  }
  LOG_DEBUG("get part location", K(ret), KPC(out), K(location_idx), K(table_location));
  return ret;
}

int ObTableModify::get_part_location(ObExecContext& ctx, ObIArray<DMLPartInfo>& part_keys) const
{
  int ret = OB_SUCCESS;
  const ObPhyTableLocation* table_location = NULL;
  const ObPartitionReplicaLocation* part_location = NULL;
  ObTaskExecutorCtx& executor_ctx = ctx.get_task_exec_ctx();
  ObTableModifyInput* dml_input = GET_PHY_OP_INPUT(ObTableModifyInput, ctx, get_id());
  if (dml_input != NULL && !dml_input->part_infos_.empty()) {
    if (OB_FAIL(part_keys.assign(dml_input->part_infos_))) {
      LOG_WARN("assign part keys failed", K(ret));
    }
    LOG_DEBUG("get part location assign part keys", K(ret), K(part_keys));
  } else if (OB_FAIL(
                 ObTaskExecutorCtxUtil::get_phy_table_location(executor_ctx, table_id_, index_tid_, table_location))) {
    LOG_WARN("failed to get physical table location", K(table_id_), K(index_tid_));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy table location", K(ret));
  } else if (table_location->get_partition_location_list().count() < 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN(
        "non-partition dml is not supported!", "# partitions", table_location->get_partition_location_list().count());
  } else if (OB_FAIL(get_part_location(ctx, *table_location, part_location))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (OB_ISNULL(part_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part_location", K(part_location), K(ret));
  } else {
    DMLPartInfo part_info;
    part_info.partition_key_.init(index_tid_, part_location->get_partition_id(), part_location->get_partition_cnt());
    part_info.part_row_cnt_ = -1;
    if (OB_FAIL(part_keys.push_back(part_info))) {
      LOG_WARN("store partition key failed", K(ret));
    }
  }
  return ret;
}

int ObTableModify::extend_dml_stmt(ObExecContext& ctx, const ObIArrayWrap<ObTableDMLInfo>& dml_table_infos,
    const ObIArrayWrap<ObTableDMLCtx>& dml_table_ctxs)
{
  int ret = OB_SUCCESS;
  CK(dml_table_infos.count() == dml_table_ctxs.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < dml_table_infos.count(); ++i) {
    const ObArrayWrap<ObGlobalIndexDMLInfo>& global_index_infos = dml_table_infos.at(i).index_infos_;
    const ObArrayWrap<ObGlobalIndexDMLCtx>& global_index_ctxs = dml_table_ctxs.at(i).index_ctxs_;
    for (int64_t j = 0; OB_SUCC(ret) && j < global_index_infos.count(); ++j) {
      uint64_t table_location_key = global_index_infos.at(j).table_id_;
      uint64_t ref_table_id = global_index_infos.at(j).index_tid_;
      CK(global_index_infos.at(j).table_locs_.count() >= 1 ||
          global_index_infos.at(j).calc_part_id_exprs_.count() >= 1);
      if (OB_SUCC(ret) && !global_index_ctxs.at(j).partition_ids_.empty()) {
        OZ(ObTableLocation::append_phy_table_location(
            ctx, table_location_key, ref_table_id, false, global_index_ctxs.at(j).partition_ids_, UNORDERED));
      }
    }
  }
  return ret;
}

OperatorOpenOrder ObTableModify::get_operator_open_order(ObExecContext& ctx) const
{
  OperatorOpenOrder open_order = OPEN_CHILDREN_FIRST;
  ObTableModifyInput* modify_input = GET_PHY_OP_INPUT(ObTableModifyInput, ctx, get_id());
  if (OB_UNLIKELY(modify_input != NULL)) {
    if (OB_UNLIKELY(from_multi_table_dml())) {
      // For multi-partition dml, since the data is dynamically generated, the plan
      // can only be generated in advance
      if (OB_UNLIKELY(modify_input->part_infos_.empty())) {
        open_order = OPEN_NONE;
      }
    } else {
      if (get_phy_plan()->is_use_px()) {
        // If px is turned on in dml, there are two situations:
        // 1. PDML: in pdml, pdml-op does not need its corresponding input to provide runtime parameters, so it will
        // return directly open_order = OPEN_CHILDREN_FIRST
        // 2. DML+PX: in this case, the parameters of the dml operation are completely plugged in by the above GI
        // operator, such as this plan:
        //   PX COORD
        //    PX TRANSMIT
        //      GI (FULL PARTITION WISE)
        //       DELETE
        //        TSC
        //  such a plan requires GI to drive deletion. Every time GI iterates a new task, rescan delete it.
        //  the information of the corresponding task is stuffed into the delete operator
        open_order = OPEN_CHILDREN_FIRST;
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == modify_input->get_location_idx())) {
        open_order = OPEN_NONE;
      }
    }
  }
  return open_order;
}

ObString ObTableModify::get_duplicated_rowkey_buffer(
    const ObIArray<uint64_t>& rowkey_ids, const ObNewRow& row, const ObTimeZoneInfo* tz_info)
{
  int ret = OB_SUCCESS;
  char* buffer = get_sql_string_buffer();
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_ids.count(); ++i) {
    uint64_t rowkey_id = rowkey_ids.at(i);
    if (!is_shadow_column(rowkey_id)) {
      const ObObj& obj = row.get_cell(i);
      if (OB_FAIL(obj.print_plain_str_literal(buffer, CSTRING_BUFFER_LEN, pos, tz_info))) {
        LOG_WARN("print plain string literal failed", K(ret), K(pos));
      } else if (i < rowkey_ids.count() - 1) {
        if (OB_FAIL(databuff_printf(buffer, CSTRING_BUFFER_LEN, pos, "-"))) {
          LOG_WARN("databuff print failed", K(ret), K(pos));
        }
      }
    }
  }
  if (buffer != nullptr) {
    if (pos < CSTRING_BUFFER_LEN) {
      buffer[pos] = '\0';
    } else {
      buffer[CSTRING_BUFFER_LEN - 1] = '\0';
      pos = CSTRING_BUFFER_LEN;
    }
  }
  return common::ObString(pos, buffer);
}

int ObTableModify::init_dml_param(ObExecContext& ctx, const uint64_t table_id, const ObPhyOperator& phy_op,
    const bool only_data_table, const share::schema::ObTableDMLParam* table_param, storage::ObDMLBaseParam& dml_param)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = -1;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  const ObPhysicalPlan* phy_plan = NULL;
  CK(OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)),
      OB_NOT_NULL(my_session = GET_MY_SESSION(ctx)),
      OB_NOT_NULL(phy_plan = plan_ctx->get_phy_plan()));
  OZ(phy_plan->get_base_table_version(table_id, schema_version));
  OZ(my_session->get_binlog_row_image(binlog_row_image));
  if (!my_session->use_static_typing_engine()) {
    OZ(phy_op.wrap_expr_ctx(ctx, dml_param.expr_ctx_));
  }
  if (OB_SUCC(ret)) {
    dml_param.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    dml_param.schema_version_ = schema_version;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
    dml_param.tz_info_ = TZ_INFO(my_session);
    dml_param.sql_mode_ = my_session->get_sql_mode();
    dml_param.only_data_table_ = only_data_table;
  }
  dml_param.virtual_columns_.reset();
  DLIST_FOREACH(node, phy_op.get_virtual_column_exprs())
  {
    const ObColumnExpression* expr = static_cast<const ObColumnExpression*>(node);
    OZ(dml_param.virtual_columns_.push_back(expr));
  }
  dml_param.table_param_ = table_param;
  dml_param.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
  return ret;
}

int ObTableModify::init_dml_param_se(ObExecContext& ctx, const uint64_t table_id, const bool only_data_table,
    const share::schema::ObTableDMLParam* table_param, storage::ObDMLBaseParam& dml_param)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = -1;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  const ObPhysicalPlan* phy_plan = NULL;
  CK(OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)),
      OB_NOT_NULL(my_session = GET_MY_SESSION(ctx)),
      OB_NOT_NULL(phy_plan = plan_ctx->get_phy_plan()));
  OZ(phy_plan->get_base_table_version(table_id, schema_version));
  OZ(my_session->get_binlog_row_image(binlog_row_image));
  if (OB_SUCC(ret)) {
    dml_param.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    dml_param.schema_version_ = schema_version;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
    dml_param.tz_info_ = TZ_INFO(my_session);
    dml_param.sql_mode_ = my_session->get_sql_mode();
    dml_param.only_data_table_ = only_data_table;
  }
  dml_param.virtual_columns_.reset();
  dml_param.table_param_ = table_param;
  dml_param.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
  return ret;
}

// update full row contain old column scanned from storage and updated column calculated by update statement
int ObTableModify::check_row_value(
    const ObIArrayWrap<ColumnContent>& update_column_infos, const ObNewRow& old_row, ObNewRow& new_row)
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, update_start_check_row);
  bool is_updated = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_updated && i < update_column_infos.count(); ++i) {
    // In the update operation, the index listed in the projector will be recorded.
    // The location of the new row and the old row can be found through the index of the projector
    if (OB_LIKELY(!update_column_infos.at(i).auto_filled_timestamp_)) {
      int64_t projector_index = update_column_infos.at(i).projector_index_;
      CK(projector_index >= 0 && projector_index < old_row.get_count());
      CK(projector_index >= 0 && projector_index < new_row.get_count());
      if (OB_SUCC(ret)) {
        const ObObj& old_value = old_row.get_cell(projector_index);
        const ObObj& new_value = new_row.get_cell(projector_index);
        is_updated = !old_value.strict_equal(new_value);
        LOG_DEBUG("check value", K(is_updated), K(i), K(projector_index), K(old_value), K(new_value));
      }
    }
  }  // end for
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_updated)) {
    OZ(mark_lock_row_flag(update_column_infos, new_row));
  }
  NG_TRACE_TIMES(2, update_end_check_row);
  return ret;
}

int ObTableModify::mark_lock_row_flag(const ObIArrayWrap<ColumnContent>& update_column_infos, ObNewRow& new_row)
{
  int ret = OB_SUCCESS;
  // if old row is not updated, it only needs to be locked,
  // this time, the updated column will be set to flag OP_LOCKR_ROW
  for (int64_t i = 0; OB_SUCC(ret) && i < update_column_infos.count(); ++i) {
    int64_t projector_index = update_column_infos.at(i).projector_index_;
    CK(projector_index >= 0 && projector_index < new_row.get_count());
    if (OB_SUCC(ret)) {
      ObObj& new_value = const_cast<ObObj&>(new_row.get_cell(projector_index));
      new_value.set_ext(ObActionFlag::OP_LOCK_ROW);
    }
  }
  return ret;
}

int ObTableModify::check_rowkey_whether_distinct(ObExecContext& ctx, const ObNewRow& dml_row, int64_t rowkey_cnt,
    DistinctType distinct_algo, RowkeyDistCtx*& rowkey_dist_ctx, bool& is_dist) const
{
  int ret = OB_SUCCESS;
  is_dist = true;
  if (T_DISTINCT_NONE != distinct_algo) {
    if (T_HASH_DISTINCT == distinct_algo) {
      ObIAllocator& allocator = ctx.get_allocator();
      if (OB_ISNULL(rowkey_dist_ctx)) {
        // create rowkey distinct context
        void* buf = allocator.alloc(sizeof(RowkeyDistCtx));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), "size", sizeof(RowkeyDistCtx));
        } else {
          ObSQLSessionInfo* my_session = ctx.get_my_session();
          if (OB_ISNULL(my_session)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("my session is null", K(ret));
          } else {
            rowkey_dist_ctx = new (buf) RowkeyDistCtx();
            int64_t match_rows =
                get_rows() > MIN_ROWKEY_DISTINCT_BUCKET_NUM ? get_rows() : MIN_ROWKEY_DISTINCT_BUCKET_NUM;
            // Limited to 64k to prevent internal insufficient errors
            const int64_t max_bucket_num =
                match_rows > MAX_ROWKEY_DISTINCT_BUCKET_NUM ? MAX_ROWKEY_DISTINCT_BUCKET_NUM : match_rows;
            if (OB_FAIL(rowkey_dist_ctx->create(max_bucket_num,
                    ObModIds::OB_DML_CHECK_ROWKEY_DISTINCT_BUCKET,
                    ObModIds::OB_DML_CHECK_ROWKEY_DISTINCT_NODE,
                    my_session->get_effective_tenant_id()))) {
              LOG_WARN("create rowkey distinct context failed", K(ret), "rows", get_rows());
            }
          }
        }
      }
      RowkeyItem rowkey_item;
      if (OB_SUCC(ret)) {
        rowkey_item.rowkey_ = dml_row;
        if (OB_UNLIKELY(rowkey_item.rowkey_.get_count() < rowkey_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dml row is invalid", K(ret), K(dml_row), K(rowkey_cnt));
        } else if (OB_ISNULL(rowkey_item.rowkey_.projector_)) {
          rowkey_item.rowkey_.count_ = rowkey_cnt;
        } else {
          rowkey_item.rowkey_.projector_size_ = rowkey_cnt;
        }
      }
      if (OB_SUCC(ret)) {
        // check whether duplicated
        ret = rowkey_dist_ctx->exist_refactored(rowkey_item);
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
          is_dist = false;
          LOG_DEBUG("duplicate rowkey", "rowkey", rowkey_item.rowkey_);
        } else if (OB_HASH_NOT_EXIST == ret) {
          RowkeyItem store_rowkey;
          if (OB_FAIL(ob_write_row_by_projector(allocator, rowkey_item.rowkey_, store_rowkey.rowkey_))) {
            LOG_WARN("write row by projector failed", K(ret), K(rowkey_item.rowkey_));
          } else if (OB_FAIL(rowkey_dist_ctx->set_refactored(store_rowkey))) {
            LOG_WARN("store rowkey item failed", K(ret));
          } else {
            LOG_DEBUG("distinct rowkey", "rowkey", store_rowkey.rowkey_);
          }
        } else {
          LOG_WARN("check rowkey distinct failed", K(ret));
        }
      }
    } else if (T_MERGE_DISTINCT == distinct_algo) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Check Update/Delete row with merge distinct");
    }
  }
  return ret;
}

int ObMultiDMLInfo::add_table_dml_info(int64_t idx, const ObTableDMLInfo& table_dml_info)
{
  int ret = OB_SUCCESS;
  CK(idx >= 0 && idx < table_dml_infos_.count());
  if (OB_SUCC(ret)) {
    table_dml_infos_.at(idx) = table_dml_info;
  }
  return ret;
}

int ObMultiDMLInfo::shuffle_dml_row(
    ObExecContext& ctx, ObMultiDMLCtx& multi_dml_ctx, const ObExprPtrIArray& row, int64_t dml_op) const
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  const ObIArrayWrap<ObGlobalIndexDMLInfo>* global_index_infos = NULL;
  ObIArrayWrap<ObGlobalIndexDMLCtx>* global_index_ctxs = NULL;
  CK(table_dml_infos_.count() == 1);
  CK(multi_dml_ctx.table_dml_ctxs_.count() == 1);
  CK(OB_NOT_NULL(ctx.get_eval_ctx()));
  if (OB_SUCC(ret)) {
    global_index_infos = &(table_dml_infos_.at(0).index_infos_);
    global_index_ctxs = &(multi_dml_ctx.table_dml_ctxs_.at(0).index_ctxs_);
  }
  ObDatum* partition_id_datum = NULL;
  int64_t part_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos->count(); ++i) {
    const ObExpr* calc_part_id_expr = global_index_infos->at(i).calc_part_id_exprs_.at(0);
    const SeDMLSubPlanArray& dml_subplans = global_index_infos->at(i).se_subplans_;
    const SeDMLSubPlan& dml_subplan = dml_subplans.at(dml_op);
    if (dml_subplan.subplan_root_ != NULL) {
      ObIArray<int64_t>& part_ids = global_index_ctxs->at(i).partition_ids_;
      CK(OB_NOT_NULL(calc_part_id_expr));
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(
                     ObSQLUtils::clear_evaluated_flag(global_index_infos->at(i).calc_exprs_, *ctx.get_eval_ctx()))) {
        LOG_WARN("fail to calc part id", K(ret));
      } else if (OB_FAIL(calc_part_id_expr->eval(*ctx.get_eval_ctx(), partition_id_datum))) {
        LOG_WARN("fail to calc part id", K(ret), K(calc_part_id_expr));
      } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == partition_id_datum->get_int()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("no partition matched", K(ret));
      } else {
        if (OB_FAIL(add_var_to_array_no_dup(part_ids, partition_id_datum->get_int(), &part_idx))) {
          LOG_WARN("Failed to add var to array no dup", K(ret));
        }
      }
      if (OB_SUCC(ret) && 0 == i && global_index_infos->at(0).hint_part_ids_.count() > 0) {
        // for the data table, if partition hint ids are not empty,
        // should deal dml partition selection
        if (!has_exist_in_array(global_index_infos->at(0).hint_part_ids_, partition_id_datum->get_int())) {
          ret = OB_PARTITION_NOT_MATCH;
          LOG_DEBUG("Partition not match", K(ret), K(*partition_id_datum));
        }
      }
      OZ(multi_dml_ctx.multi_dml_plan_mgr_.add_part_row(0, i, part_idx, dml_op, dml_subplan.access_exprs_));
      LOG_DEBUG("shuffle dml row",
          K(ret),
          K(part_idx),
          K(part_ids),
          K(row),
          K(dml_op),
          "access_exprs",
          ROWEXPR2STR(*ctx.get_eval_ctx(), dml_subplan.access_exprs_));
    }
  }
  return ret;
}

int ObMultiDMLInfo::shuffle_dml_row(
    ObExecContext& ctx, ObPartMgr& part_mgr, ObMultiDMLCtx& multi_dml_ctx, const ObNewRow& row, int64_t dml_op) const
{
  int ret = OB_SUCCESS;
  const ObIArrayWrap<ObGlobalIndexDMLInfo>* global_index_infos = NULL;
  ObIArrayWrap<ObGlobalIndexDMLCtx>* global_index_ctxs = NULL;
  ObNewRow cur_row;
  CK(table_dml_infos_.count() == 1);
  CK(multi_dml_ctx.table_dml_ctxs_.count() == 1);
  if (OB_SUCC(ret)) {
    cur_row.cells_ = row.cells_;
    cur_row.count_ = row.count_;
    global_index_infos = &(table_dml_infos_.at(0).index_infos_);
    global_index_ctxs = &(multi_dml_ctx.table_dml_ctxs_.at(0).index_ctxs_);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos->count(); ++i) {
    const ObTableLocation* cur_table_location = global_index_infos->at(i).table_locs_.at(0);
    const DMLSubPlanArray& dml_subplans = global_index_infos->at(i).dml_subplans_;
    const DMLSubPlan& dml_subplan = dml_subplans.at(dml_op);
    if (dml_subplan.subplan_root_ != NULL) {
      ObIArray<int64_t>& part_ids = global_index_ctxs->at(i).partition_ids_;
      int64_t part_idx = OB_INVALID_INDEX;
      CK(OB_NOT_NULL(cur_table_location));
      OZ(cur_table_location->calculate_partition_ids_by_row(ctx, &part_mgr, row, part_ids, part_idx));
      if (OB_SUCC(ret) && 0 == i) {
        OZ(cur_table_location->deal_dml_partition_selection(part_ids.at(part_idx)), part_ids, part_idx);
      }
      cur_row.projector_ = dml_subplan.value_projector_;
      cur_row.projector_size_ = dml_subplan.value_projector_size_;
      OZ(multi_dml_ctx.multi_dml_plan_mgr_.add_part_row(0, i, part_idx, dml_op, cur_row));
      LOG_DEBUG("shuffle dml row", K(ret), K(part_idx), K(part_ids), K(row), K(dml_op), K(dml_subplan));
    }
  }
  return ret;
}

void ObMultiDMLCtx::release_multi_part_shuffle_info()
{
  for (int64_t i = 0; i < table_dml_ctxs_.count(); ++i) {
    for (int64_t j = 0; j < table_dml_ctxs_.at(i).index_ctxs_.count(); ++j) {
      table_dml_ctxs_.at(i).index_ctxs_.at(j).partition_ids_.reset();
    }
  }
  multi_dml_plan_mgr_.release_part_row_store();
}

bool ObMultiDMLInfo::subplan_has_foreign_key() const
{
  /**
   * need not set error code if subplan_root is NULL, it will be catch by other code later soon.
   */
  bool has_foreign_key = false;
  for (int64_t i = 0; !has_foreign_key && i < table_dml_infos_.count(); i++) {
    const DMLSubPlanArray& dml_subplans = table_dml_infos_.at(i).index_infos_.at(0).dml_subplans_;
    for (int64_t j = 0; !has_foreign_key && j < dml_subplans.count(); j++) {
      ObTableModify* subplan_root = dml_subplans.at(j).subplan_root_;
      if (!OB_ISNULL(subplan_root) && subplan_root->get_fk_args().count() > 0) {
        has_foreign_key = true;
      }
    }
  }
  return has_foreign_key;
}

bool ObMultiDMLInfo::sesubplan_has_foreign_key() const
{
  /**
   * need not set error code if subplan_root is NULL, it will be catch by other code later soon.
   */
  bool has_foreign_key = false;
  for (int64_t i = 0; !has_foreign_key && i < table_dml_infos_.count(); i++) {
    const SeDMLSubPlanArray& se_subplans = table_dml_infos_.at(i).index_infos_.at(0).se_subplans_;
    for (int64_t j = 0; !has_foreign_key && j < se_subplans.count(); j++) {
      ObTableModifySpec* subplan_root = se_subplans.at(j).subplan_root_;
      if (!OB_ISNULL(subplan_root) && subplan_root->get_fk_args().count() > 0) {
        has_foreign_key = true;
      }
    }
  }
  return has_foreign_key;
}

int ObMultiDMLInfo::wait_all_task(ObMultiDMLCtx* dml_ctx, ObPhysicalPlanCtx* plan_ctx) const
{
  int ret = OB_SUCCESS;
  /**
   * wait_all_task() will be called in close() of multi table dml operators, and close()
   * will be called if open() has been called, no matter open() return OB_SUCCESS or not.
   * so if we get a NULL multi_dml_ctx or plan_ctx here, just ignore it.
   */
  if (OB_NOT_NULL(dml_ctx) && OB_NOT_NULL(plan_ctx) &&
      OB_FAIL(dml_ctx->mini_task_executor_.wait_all_task(plan_ctx->get_timeout_timestamp()))) {
    LOG_WARN("wait all task failed", K(ret));
  }
  return ret;
}

int ObMultiDMLCtx::init_multi_dml_ctx(ObExecContext& ctx, const ObIArrayWrap<ObTableDMLInfo>& table_dml_infos,
    const ObPhysicalPlan* phy_plan, const ObPhyOperator* subplan_root, const ObOpSpec* se_subplan_root /*= NULL*/)
{
  int ret = OB_SUCCESS;
  ObExecutorRpcImpl* exec_rpc = NULL;
  if (OB_FAIL(table_dml_ctxs_.allocate_array(allocator_, table_dml_infos.count()))) {
    LOG_WARN("init dml table info failed", K(ret));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, exec_rpc))) {
    LOG_WARN("get task executor rpc failed", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(mini_task_executor_.init(*ctx.get_my_session(), exec_rpc))) {
    LOG_WARN("init mini task executor failed", K(ret));
  } else {
    returning_row_store_.set_tenant_id(ctx.get_my_session()->get_effective_tenant_id());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_dml_infos.count(); ++i) {
    const ObArrayWrap<ObGlobalIndexDMLInfo>& global_index_infos = table_dml_infos.at(i).index_infos_;
    ObArrayWrap<ObGlobalIndexDMLCtx>& global_index_ctxs = table_dml_ctxs_.at(i).index_ctxs_;
    OZ(global_index_ctxs.allocate_array(allocator_, global_index_infos.count()));
    for (int64_t j = 0; OB_SUCC(ret) && j < global_index_infos.count(); ++j) {
      global_index_ctxs.at(j).table_id_ = global_index_infos.at(j).table_id_;
      global_index_ctxs.at(j).index_tid_ = global_index_infos.at(j).index_tid_;
      global_index_ctxs.at(j).part_cnt_ = global_index_infos.at(j).part_cnt_;
      global_index_ctxs.at(j).is_table_ = (0 == j);
      global_index_ctxs.at(j).dml_subplans_ = global_index_infos.at(j).dml_subplans_;
      global_index_ctxs.at(j).se_subplans_ = global_index_infos.at(j).se_subplans_;
      global_index_ctxs.at(j).partition_ids_.reset();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(multi_dml_plan_mgr_.init(&ctx, &table_dml_ctxs_, phy_plan, subplan_root, se_subplan_root))) {
      LOG_WARN("init multi dml plan mgr failed", K(ret));
    }
  }
  return ret;
}

int ObTableModify::init_foreign_key_args(int64_t fk_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fk_args_.init(fk_count))) {
    LOG_WARN("failed to init foreign key stmts", K(ret), K(fk_count));
  }
  return ret;
}

int ObTableModify::add_foreign_key_arg(const ObForeignKeyArg& fk_arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fk_args_.push_back(fk_arg))) {
    LOG_WARN("failed to push foreign key stmt arg", K(fk_arg), K(ret));
  }
  return ret;
}

OB_INLINE int ObTableModify::init_foreign_key_operation(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableModifyCtx* modify_ctx = GET_PHY_OPERATOR_CTX(ObTableModifyCtx, ctx, get_id());
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx);
  if (OB_ISNULL(modify_ctx) || OB_ISNULL(phy_plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("modify_ctx or phy_plan_ctx is NULL", K(ret), KP(modify_ctx), KP(phy_plan_ctx));
  } else if ((share::is_mysql_mode() && phy_plan_ctx->need_foreign_key_checks()) || share::is_oracle_mode()) {
    if (has_foreign_key() && OB_FAIL(modify_ctx->open_inner_conn())) {
      LOG_WARN("failed to open inner conn", K(ret));
    }
    modify_ctx->set_foreign_key_checks();
  }
  return ret;
}

int ObTableModify::get_gi_task(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObGranuleTaskInfo gi_task_info;
  GIPrepareTaskMap* gi_prepare_map = nullptr;
  ObTableModifyInput* modify_input = nullptr;
  ObTableModifyCtx* table_modify_ctx = nullptr;
  bool use_px = get_phy_plan()->is_use_px();
  // In PX mode: if it is currently an INSERT/REPLACE operator, get the corresponding GI Task
  // through the op id of the operator itself
  int64_t op_id = OB_INVALID_INDEX;
  if (IS_DML(get_type())) {
    op_id = get_id();
  }
  if (OB_ISNULL(table_modify_ctx = GET_PHY_OPERATOR_CTX(ObTableModifyCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get op ctx", K(ret));
  } else if (OB_FAIL(ctx.get_gi_task_map(gi_prepare_map))) {
    LOG_WARN("Failed to get gi task map", K(ret));
  } else if (use_px && op_id == OB_INVALID_INDEX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan op id is invalid", K(ret), K(use_px));
  } else if (use_px && OB_FAIL(gi_prepare_map->get_refactored(op_id, gi_task_info))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to get prepare gi task", K(ret), K(get_id()));
    } else {
      LOG_DEBUG("no prepared task info, set table modify to end", K(get_id()), K(op_id), K(this), K(lbt()));
      // The current DML operator cannot get the task from GI, which means the current DML operator iter end
      table_modify_ctx->iter_end_ = true;
      ret = OB_SUCCESS;
    }
  } else if (!use_px && OB_FAIL(gi_prepare_map->get_refactored(get_id(), gi_task_info))) {
    LOG_WARN("failed to get prepare gi task", K(ret), K(get_id()));
  } else if (OB_ISNULL(modify_input = GET_PHY_OP_INPUT(ObTableModifyInput, ctx, get_id()))) {
    // local plan, no modify input, do nothing
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::translate_pid_to_ldx(ctx.get_task_exec_ctx(),
                 gi_task_info.partition_id_,
                 table_id_,
                 index_tid_,
                 modify_input->location_idx_))) {
    LOG_WARN("failed to do gi task scan", K(ret));
  } else {
    ctx.set_expr_partition_id(gi_task_info.partition_id_);
    LOG_DEBUG("DML operator consume a task", K(ret), K(gi_task_info), KPC(modify_input), K(get_id()), K(modify_input));
  }
  return ret;
}

bool ObTableModify::has_foreign_key() const
{
  return fk_args_.count() > 0;
}

int32_t ObTableModify::get_column_idx(uint64_t column_id)
{
  int32_t column_idx = -1;
  for (int i = 0; i < column_ids_.count(); i++) {
    if (column_id == column_ids_.at(i)) {
      column_idx = i;
    }
  }
  if (-1 == column_idx) {
    LOG_WARN("can not find column id", K(column_id), K(column_ids_));
  }
  return column_idx;
}

int ObTableModify::check_rowkey_is_null(const ObNewRow& row, int64_t rowkey_cnt, bool& is_null) const
{
  int ret = OB_SUCCESS;
  is_null = false;
  if (OB_UNLIKELY(row.get_count() < rowkey_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rowkey_cnt), K(row));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_null && i < rowkey_cnt; ++i) {
    // each column of primary key cannot be null
    is_null = row.get_cell(i).is_null();
  }
  return ret;
}

void ObTableModify::log_user_error_inner(int ret, int64_t col_idx, int64_t row_num, ObExecContext& ctx) const
{
  if (OB_DATA_OUT_OF_RANGE == ret && (0 <= col_idx && col_idx < column_infos_.count())) {
    ObString column_name = column_infos_.at(col_idx).column_name_;
    ObSQLUtils::copy_and_convert_string_charset(ctx.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx.get_my_session()->get_local_collation_connection());
    LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, column_name.length(), column_name.ptr(), row_num);
  } else if (OB_ERR_DATA_TOO_LONG == ret && (0 <= col_idx && col_idx < column_infos_.count())) {
    ObString column_name = column_infos_.at(col_idx).column_name_;
    ObSQLUtils::copy_and_convert_string_charset(ctx.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx.get_my_session()->get_local_collation_connection());
    if (lib::is_mysql_mode() || !need_skip_log_user_error_) {
      // compatible with old code
      LOG_USER_ERROR(OB_ERR_DATA_TOO_LONG, column_name.length(), column_name.ptr(), row_num);
    }
  } else if (OB_ERR_VALUE_LARGER_THAN_ALLOWED == ret && col_idx >= 0) {
    const ObString& column_name = column_infos_.at(col_idx).column_name_;
    LOG_USER_ERROR(OB_ERR_VALUE_LARGER_THAN_ALLOWED);
  } else if ((OB_INVALID_DATE_VALUE == ret || OB_INVALID_DATE_FORMAT == ret) &&
             (0 <= col_idx && col_idx < column_infos_.count())) {
    ObString column_name = column_infos_.at(col_idx).column_name_;
    ObSQLUtils::copy_and_convert_string_charset(ctx.get_allocator(),
        column_name,
        column_name,
        CS_TYPE_UTF8MB4_BIN,
        ctx.get_my_session()->get_local_collation_connection());
    LOG_USER_ERROR(OB_ERR_INVALID_DATE_MSG_FMT_V2, column_name.length(), column_name.ptr(), row_num);
  } else {
    LOG_WARN("fail to operate row", K(ret));
  }
}

int ObTableModify::ForeignKeyHandle::do_handle_old_row(
    ObTableModify* modify_op, ObTableModifyCtx& modify_ctx, const ObNewRow& old_row)
{
  int ret = OB_SUCCESS;
  static const ObNewRow tmp_new_row;
  if (OB_ISNULL(modify_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("modify_op is null", K(ret));
  } else if (OB_FAIL(do_handle(modify_ctx, modify_op->get_fk_args(), old_row, tmp_new_row))) {
    LOG_WARN("fail to handle foreign key", K(ret), K(old_row));
  }
  return ret;
}

int ObTableModify::ForeignKeyHandle::do_handle_old_row(
    ObTableModifyCtx& modify_ctx, const ObForeignKeyArgArray& fk_args, const ObNewRow& old_row)
{
  static const ObNewRow tmp_new_row;
  return do_handle(modify_ctx, fk_args, old_row, tmp_new_row);
}

int ObTableModify::ForeignKeyHandle::do_handle_new_row(
    ObTableModify* modify_op, ObTableModifyCtx& modify_ctx, const ObNewRow& new_row)
{
  int ret = OB_SUCCESS;
  static const ObNewRow tmp_old_row;
  if (OB_ISNULL(modify_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("modify_op is null", K(ret));
  } else if (OB_FAIL(do_handle(modify_ctx, modify_op->get_fk_args(), tmp_old_row, new_row))) {
    LOG_WARN("fail to handle foreign key", K(ret), K(new_row));
  }
  return ret;
}

int ObTableModify::ForeignKeyHandle::do_handle_new_row(
    ObTableModifyCtx& modify_ctx, const ObForeignKeyArgArray& fk_args, const ObNewRow& new_row)
{
  static const ObNewRow tmp_old_row;
  return do_handle(modify_ctx, fk_args, tmp_old_row, new_row);
}

int ObTableModify::ForeignKeyHandle::do_handle(
    ObTableModify* modify_op, ObTableModifyCtx& modify_ctx, const ObNewRow& old_row, const ObNewRow& new_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(modify_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("modify_op is null", K(ret));
  } else if (OB_FAIL(do_handle(modify_ctx, modify_op->get_fk_args(), old_row, new_row))) {
    LOG_WARN("fail to handle foreign key", K(ret), K(old_row), K(new_row));
  }
  return ret;
}

int ObTableModify::ForeignKeyHandle::do_handle(
    ObTableModifyCtx& modify_ctx, const ObForeignKeyArgArray& fk_args, const ObNewRow& old_row, const ObNewRow& new_row)
{
  int ret = OB_SUCCESS;
  if (modify_ctx.need_foreign_key_checks()) {
    LOG_DEBUG("do foreign_key_handle", K(fk_args), K(old_row), K(new_row));
    if (OB_FAIL(modify_ctx.check_stack())) {
      LOG_WARN("fail to check stack", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < fk_args.count(); i++) {
      const ObForeignKeyArg& fk_arg = fk_args.at(i);
      if (OB_SUCC(ret) && new_row.is_valid()) {
        if (ACTION_CHECK_EXIST == fk_arg.ref_action_) {
          // insert or update.
          bool is_foreign_key_cascade = false;
          if (OB_FAIL(modify_ctx.get_foreign_key_cascade(is_foreign_key_cascade))) {
            LOG_WARN("failed to get foreign key cascade", K(ret), K(fk_arg), K(new_row));
          } else if (is_foreign_key_cascade) {
            // nested update can not check parent row.
            LOG_DEBUG("skip foreign_key_check_exist in nested session");
          } else if (OB_FAIL(check_exist(modify_ctx, fk_arg, new_row, false))) {
            LOG_WARN("failed to check exist", K(ret), K(fk_arg), K(new_row));
          }
        }
      }  // if (new_row.is_valid())
      if (OB_SUCC(ret) && old_row.is_valid()) {
        if (ACTION_RESTRICT == fk_arg.ref_action_ || ACTION_NO_ACTION == fk_arg.ref_action_) {
          // update or delete.
          bool has_changed = false;
          if (OB_FAIL(value_changed(fk_arg.columns_, old_row, new_row, has_changed))) {
            LOG_WARN("failed to check if foreign key value has changed", K(ret), K(fk_arg), K(old_row), K(new_row));
          } else if (!has_changed) {
            // nothing.
          } else if (OB_FAIL(check_exist(modify_ctx, fk_arg, old_row, true))) {
            LOG_WARN("failed to check exist", K(ret), K(fk_arg), K(old_row));
          }
        } else if (ACTION_CASCADE == fk_arg.ref_action_) {
          // update or delete.
          bool is_self_ref = is_self_ref_row(old_row, fk_arg);
          if (new_row.is_invalid() && is_self_ref && modify_ctx.is_nested_session()) {
            // delete self refercnced row should not cascade delete.
          } else if (OB_FAIL(cascade(modify_ctx, fk_arg, old_row, new_row))) {
            LOG_WARN("failed to cascade", K(ret), K(fk_arg), K(old_row), K(new_row));
          } else if (new_row.is_valid() && is_self_ref) {
            // update self refercnced row need adjust some value after cascade update.
            for (int64_t i = 0; OB_SUCC(ret) && i < fk_arg.columns_.count(); i++) {
              const ObObj& updated_value = new_row.get_cell(fk_arg.columns_.at(i).idx_);
              ObObj& new_row_name_col = const_cast<ObObj&>(new_row.get_cell(fk_arg.columns_.at(i).name_idx_));
              ObObj& old_row_name_col = const_cast<ObObj&>(old_row.get_cell(fk_arg.columns_.at(i).name_idx_));
              new_row_name_col = updated_value;
              old_row_name_col = updated_value;
            }
          }
        }
      }  // if (old_row.is_valid())
    }    // for
  } else {
    LOG_DEBUG("skip foreign_key_handle");
  }
  return ret;
}

int ObTableModify::ForeignKeyHandle::value_changed(
    const ObIArray<ObForeignKeyColumn>& columns, const ObNewRow& old_row, const ObNewRow& new_row, bool& has_changed)
{
  int ret = OB_SUCCESS;
  has_changed = false;
  if (old_row.is_valid() && new_row.is_valid()) {
    for (int64_t i = 0; OB_SUCC(ret) && !has_changed && i < columns.count(); i++) {
      const ObObj& old_value = old_row.get_cell(columns.at(i).idx_);
      const ObObj& new_value = new_row.get_cell(columns.at(i).idx_);
      has_changed = (0 != old_value.compare(new_value, CS_TYPE_BINARY));
    }
  } else {
    has_changed = true;
  }
  return ret;
}

int ObTableModify::ForeignKeyHandle::check_exist(
    ObTableModifyCtx& modify_ctx, const ObForeignKeyArg& fk_arg, const ObNewRow& row, bool expect_zero)
{
  static const char* SELECT_FMT_MYSQL = "select 1 from `%.*s`.`%.*s` where %.*s limit 2 for update";
  static const char* SELECT_FMT_ORACLE = "select 1 from %.*s.%.*s where %.*s and rownum <= 2 for update";
  const char* select_fmt = lib::is_mysql_mode() ? SELECT_FMT_MYSQL : SELECT_FMT_ORACLE;
  int ret = OB_SUCCESS;
  char stmt_buf[2048] = {0};
  char where_buf[2048] = {0};
  int64_t stmt_pos = 0;
  int64_t where_pos = 0;
  const ObString& database_name = fk_arg.database_name_;
  const ObString& table_name = fk_arg.table_name_;
  if (row.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is invalid", K(ret));
  } else if (OB_FAIL(gen_where(
                 where_buf, sizeof(where_buf), where_pos, fk_arg.columns_, row, modify_ctx.get_obj_print_params()))) {
    if (OB_LIKELY(OB_ERR_NULL_VALUE == ret)) {
      // skip check exist if any column in foreign key is NULL.
      ret = OB_SUCCESS;
      stmt_pos = 0;
    } else {
      LOG_WARN("failed to gen foreign key where", K(ret), K(row), K(fk_arg.columns_));
    }
  } else if (OB_FAIL(databuff_printf(stmt_buf,
                 sizeof(stmt_buf),
                 stmt_pos,
                 select_fmt,
                 database_name.length(),
                 database_name.ptr(),
                 table_name.length(),
                 table_name.ptr(),
                 static_cast<int>(where_pos),
                 where_buf))) {
    LOG_WARN("failed to print stmt", K(ret), K(table_name), K(where_buf));
  } else {
    stmt_buf[stmt_pos++] = 0;
  }
  if (OB_SUCC(ret) && stmt_pos > 0) {
    LOG_DEBUG("foreign_key_check_exist", "stmt", stmt_buf, K(row), K(fk_arg));
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      if (OB_FAIL(modify_ctx.begin_nested_session(fk_arg.is_self_ref_))) {
        LOG_WARN("failed to begin nested session", K(ret), K(stmt_buf));
      } else if (OB_FAIL(modify_ctx.set_foreign_key_check_exist(true))) {
        LOG_WARN("failed to set foreign key cascade", K(ret));
      } else {
        // must call end_nested_session() if begin_nested_session() success.
        bool is_zero = false;
        if (OB_FAIL(modify_ctx.execute_read(stmt_buf, res))) {
          LOG_WARN("failed to execute stmt", K(ret), K(stmt_buf));
        } else {
          // must call res.get_result()->close() if execute_read() success.
          if (OB_ISNULL(res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is NULL", K(ret));
          } else if (OB_FAIL(res.get_result()->next())) {
            if (OB_ITER_END == ret) {
              is_zero = true;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to get next", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (is_zero != expect_zero) {
              // oracle for update does not support aggregate functions, so when is_zero is false
              // judge whether only one row is affected by whether the second record can be obtained
              bool is_affect_only_one = false;
              if (!is_zero && OB_FAIL(res.get_result()->next())) {
                if (OB_ITER_END == ret) {
                  is_affect_only_one = true;
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("failed to get next", K(ret));
                }
              }
              if (!is_self_ref_row(row, fk_arg)) {
                /**
                 *  expect_zero is false when fk_arg.ref_action_ is equal to
                 *  ACTION_CHECK_EXIST, if the is_zero != expect_zero condition is
                 *  true, then is_zero is true, need to exclude the case of self reference.
                 *  other cases return OB_ERR_NO_REFERENCED_ROW.
                 *
                 *  expect_zero is true when fk_arg.ref_action_ is equal to
                 *  ACTION_RESTRICT or ACTION_NO_ACTION, if is_zero != expect_zero condition
                 *  is true, then is_zero is false, need to exclude the case of self reference
                 *  and only affect one row. other cases return OB_ERR_ROW_IS_REFERENCED.
                 */
                if (is_zero) {
                  ret = OB_ERR_NO_REFERENCED_ROW;
                  LOG_WARN("parent row is not exist", K(ret), K(fk_arg), K(row));
                } else {
                  ret = OB_ERR_ROW_IS_REFERENCED;
                  LOG_WARN("child row is exist", K(ret), K(fk_arg), K(row));
                }
              } else if (!is_zero && !is_affect_only_one) {
                ret = OB_ERR_ROW_IS_REFERENCED;
                LOG_WARN("child row is exist", K(ret), K(fk_arg), K(row));
              }
            }
          }
          int close_ret = res.get_result()->close();
          if (OB_SUCCESS != close_ret) {
            LOG_WARN("failed to close", K(close_ret));
            if (OB_SUCCESS == ret) {
              ret = close_ret;
            }
          }
        }
        int reset_ret = modify_ctx.set_foreign_key_check_exist(false);
        if (OB_SUCCESS != reset_ret) {
          LOG_WARN("failed to reset foreign key cascade", K(reset_ret));
          if (OB_SUCCESS == ret) {
            ret = reset_ret;
          }
        }
        int end_ret = modify_ctx.end_nested_session();
        if (OB_SUCCESS != end_ret) {
          LOG_WARN("failed to end nested session", K(end_ret));
          if (OB_SUCCESS == ret) {
            ret = end_ret;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableModify::ForeignKeyHandle::cascade(
    ObTableModifyCtx& modify_ctx, const ObForeignKeyArg& fk_arg, const ObNewRow& old_row, const ObNewRow& new_row)
{
  static const char* UPDATE_FMT_MYSQL = "update `%.*s`.`%.*s` set %.*s where %.*s";
  static const char* UPDATE_FMT_ORACLE = "update %.*s.%.*s set %.*s where %.*s";
  static const char* DELETE_FMT_MYSQL = "delete from `%.*s`.`%.*s` where %.*s";
  static const char* DELETE_FMT_ORACLE = "delete from %.*s.%.*s where %.*s";
  int ret = OB_SUCCESS;
  char stmt_buf[2048] = {0};
  char where_buf[2048] = {0};
  int64_t stmt_pos = 0;
  int64_t where_pos = 0;
  const ObString& database_name = fk_arg.database_name_;
  const ObString& table_name = fk_arg.table_name_;
  if (old_row.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("old row is invalid", K(ret));
  } else if (OB_FAIL(gen_where(where_buf,
                 sizeof(where_buf),
                 where_pos,
                 fk_arg.columns_,
                 old_row,
                 modify_ctx.get_obj_print_params()))) {
    if (OB_LIKELY(OB_ERR_NULL_VALUE == ret)) {
      // skip cascade if any column in foreign key is NULL.
      ret = OB_SUCCESS;
      stmt_pos = 0;
    } else {
      LOG_WARN("failed to gen foreign key where", K(ret), K(old_row), K(fk_arg.columns_));
    }
  } else {
    if (new_row.is_valid()) {
      // update.
      const char* update_fmt = lib::is_mysql_mode() ? UPDATE_FMT_MYSQL : UPDATE_FMT_ORACLE;
      char set_buf[2048] = {0};
      int64_t set_pos = 0;
      if (OB_FAIL(gen_set(
              set_buf, sizeof(set_buf), set_pos, fk_arg.columns_, new_row, modify_ctx.get_obj_print_params()))) {
        LOG_WARN("failed to gen foreign key set", K(ret), K(new_row), K(fk_arg.columns_));
      } else if (OB_FAIL(databuff_printf(stmt_buf,
                     sizeof(stmt_buf),
                     stmt_pos,
                     update_fmt,
                     database_name.length(),
                     database_name.ptr(),
                     table_name.length(),
                     table_name.ptr(),
                     static_cast<int>(set_pos),
                     set_buf,
                     static_cast<int>(where_pos),
                     where_buf))) {
        LOG_WARN("failed to print stmt", K(ret), K(table_name), K(set_buf), K(where_buf));
      } else {
        stmt_buf[stmt_pos++] = 0;
      }
    } else {
      // delete.
      const char* delete_fmt = lib::is_mysql_mode() ? DELETE_FMT_MYSQL : DELETE_FMT_ORACLE;
      if (OB_FAIL(databuff_printf(stmt_buf,
              sizeof(stmt_buf),
              stmt_pos,
              delete_fmt,
              database_name.length(),
              database_name.ptr(),
              table_name.length(),
              table_name.ptr(),
              static_cast<int>(where_pos),
              where_buf))) {
        LOG_WARN("failed to print stmt", K(ret), K(table_name), K(where_buf));
      } else {
        stmt_buf[stmt_pos++] = 0;
      }
    }
  }
  if (OB_SUCC(ret) && stmt_pos > 0) {
    LOG_DEBUG("foreign_key_cascade", "stmt", stmt_buf, K(old_row), K(new_row), K(fk_arg));
    if (OB_FAIL(modify_ctx.begin_nested_session(fk_arg.is_self_ref_))) {
      LOG_WARN("failed to begin nested session", K(ret));
    } else {
      // must call end_nested_session() if begin_nested_session() success.
      // skip modify_ctx.set_foreign_key_cascade when cascade update and self ref.
      if (!(fk_arg.is_self_ref_ && new_row.is_valid()) && OB_FAIL(modify_ctx.set_foreign_key_cascade(true))) {
        LOG_WARN("failed to set foreign key cascade", K(ret));
      } else if (OB_FAIL(modify_ctx.execute_write(stmt_buf))) {
        LOG_WARN("failed to execute stmt", K(ret), K(stmt_buf));
      }
      int reset_ret = modify_ctx.set_foreign_key_cascade(false);
      if (OB_SUCCESS != reset_ret) {
        LOG_WARN("failed to reset foreign key cascade", K(reset_ret));
        if (OB_SUCCESS == ret) {
          ret = reset_ret;
        }
      }
      int end_ret = modify_ctx.end_nested_session();
      if (OB_SUCCESS != end_ret) {
        LOG_WARN("failed to end nested session", K(end_ret));
        if (OB_SUCCESS == ret) {
          ret = end_ret;
        }
      }
    }
  }
  return ret;
}

int ObTableModify::ForeignKeyHandle::gen_set(char* buf, int64_t len, int64_t& pos,
    const ObIArray<ObForeignKeyColumn>& columns, const ObNewRow& row, const ObObjPrintParams& print_params)
{
  return gen_column_value(buf, len, pos, columns, row, ", ", print_params, false);
}

int ObTableModify::ForeignKeyHandle::gen_where(char* buf, int64_t len, int64_t& pos,
    const ObIArray<ObForeignKeyColumn>& columns, const ObNewRow& row, const ObObjPrintParams& print_params)
{
  return gen_column_value(buf, len, pos, columns, row, " and ", print_params, true);
}

int ObTableModify::ForeignKeyHandle::gen_column_value(char* buf, int64_t len, int64_t& pos,
    const ObIArray<ObForeignKeyColumn>& columns, const ObNewRow& row, const char* delimiter,
    const ObObjPrintParams& print_params, bool forbid_null)
{
  static const char* COLUMN_FMT_MYSQL = "`%.*s` = ";
  static const char* COLUMN_FMT_ORACLE = "%.*s = ";
  const char* column_fmt = lib::is_mysql_mode() ? COLUMN_FMT_MYSQL : COLUMN_FMT_ORACLE;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_ISNULL(delimiter) || OB_ISNULL(print_params.tz_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), KP(delimiter), K(print_params.tz_info_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    const ObString& col_name = columns.at(i).name_;
    const ObObj& col_value = row.get_cell(columns.at(i).idx_);
    if (forbid_null && col_value.is_null()) {
      ret = OB_ERR_NULL_VALUE;
      // NO LOG.
    } else if (OB_FAIL(databuff_printf(buf, len, pos, column_fmt, col_name.length(), col_name.ptr()))) {
      LOG_WARN("failed to print column name", K(ret), K(col_name));
    } else if (OB_FAIL(col_value.print_sql_literal(buf, len, pos, print_params))) {
      LOG_WARN("failed to print column value", K(ret), K(col_value));
    } else if (OB_FAIL(databuff_printf(buf, len, pos, delimiter))) {
      LOG_WARN("failed to print delimiter", K(ret), K(delimiter));
    }
  }
  if (OB_SUCC(ret)) {
    pos -= STRLEN(delimiter);
    buf[pos++] = 0;
  }
  return ret;
}

bool ObTableModify::ForeignKeyHandle::is_self_ref_row(const ObNewRow& row, const ObForeignKeyArg& fk_arg)
{
  bool is_self_ref = fk_arg.is_self_ref_;
  for (int64_t i = 0; is_self_ref && i < fk_arg.columns_.count(); i++) {
    const ObObj& name_col = row.get_cell(fk_arg.columns_.at(i).name_idx_);
    const ObObj& value_col = row.get_cell(fk_arg.columns_.at(i).idx_);
    if (name_col != value_col) {
      is_self_ref = false;
    }
  }
  return is_self_ref;
}

int ObTableModify::calc_row_for_pdml(ObExecContext& ctx, ObNewRow& cur_row) const
{
  int ret = OB_SUCCESS;
  ObTableModifyCtx* op_ctx = nullptr;
  LOG_DEBUG("check before processed row", "op_name", get_name(), K(cur_row));
  CK(OB_NOT_NULL(op_ctx = GET_PHY_OPERATOR_CTX(ObTableModifyCtx, ctx, get_id())));
  // OZ (calculate_row_inner(op_ctx->expr_ctx_, insert_row, inner_calc_exprs_));
  // OZ (validate_normal_column(op_ctx->expr_ctx_, op_ctx->expr_ctx_.column_conv_ctx_, insert_row));
  op_ctx->expr_ctx_.row_ctx_.reset();
  OZ(calculate_virtual_column(op_ctx->expr_ctx_, cur_row, 0));
  LOG_DEBUG("check processed row", "op_name", get_name(), K(cur_row));
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
