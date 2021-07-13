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
#include "sql/engine/sequence/ob_sequence.h"
#include "share/sequence/ob_sequence_cache.h"
#include "lib/utility/utility.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql {
class ObSequence::ObSequenceCtx : public ObPhyOperatorCtx {
public:
  explicit ObSequenceCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx), sequence_cache_(nullptr)
  {
    sequence_cache_ = &share::ObSequenceCache::get_instance();
    if (OB_ISNULL(sequence_cache_)) {
      LOG_ERROR("fail alloc memory for ObSequenceCache instance");
    }
  }

  void reset()
  {
    sequence_cache_ = NULL;
    seq_schemas_.reset();
  }

  virtual void destroy()
  {
    sequence_cache_ = NULL;
    seq_schemas_.reset();
    ObPhyOperatorCtx::destroy_base();
  }

private:
  share::ObSequenceCache* sequence_cache_;
  common::ObSEArray<ObSequenceSchema, 1> seq_schemas_;
  friend class ObSequence;
};

ObSequence::ObSequence(ObIAllocator& alloc) : ObMultiChildrenPhyOperator(alloc), nextval_seq_ids_()
{}

ObSequence::~ObSequence()
{
  reset();
}

void ObSequence::reset()
{
  ObMultiChildrenPhyOperator::reset();
}

void ObSequence::reuse()
{
  reset();
}

bool ObSequence::is_valid() const
{
  bool bret = false;
  if (get_child_num() == 1) {
    bret = get_child(ObPhyOperator::FIRST_CHILD) != NULL && get_column_count() > 0 &&
           get_child(ObPhyOperator::FIRST_CHILD)->get_column_count() > 0;
  } else if (get_child_num() == 0) {
    bret = get_column_count() > 0;
  } else {
    // invalid
  }
  return bret;
}

int ObSequence::add_uniq_nextval_sequence_id(uint64_t seq_id)
{
  int ret = OB_SUCCESS;
  FOREACH_X(sid, nextval_seq_ids_, OB_SUCC(ret))
  {
    if (seq_id == *sid) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("should not add duplicated seq id to ObSequence operator", K(seq_id), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(nextval_seq_ids_.push_back(seq_id))) {
      LOG_WARN("fail add seq id to nextval seq id set", K(seq_id), K(ret));
    }
  }
  return ret;
}

int ObSequence::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObSequenceCtx* sequence_ctx = NULL;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sequence operator is invalid", K_(nextval_seq_ids));
  } else if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_ISNULL(sequence_ctx = GET_PHY_OPERATOR_CTX(ObSequenceCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K_(id));
  }
  return ret;
}

int ObSequence::inner_close(ObExecContext& ctx) const
{
  ObSequenceCtx* sequence_ctx = NULL;
  if (OB_NOT_NULL(sequence_ctx = GET_PHY_OPERATOR_CTX(ObSequenceCtx, ctx, get_id()))) {
    sequence_ctx->reset();
  }
  return OB_SUCCESS;
}

int ObSequence::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObSequenceCtx* op_ctx = NULL;
  ObTaskExecutorCtx* task_ctx = NULL;
  share::schema::ObMultiVersionSchemaService* schema_service = NULL;
  ObSQLSessionInfo* my_session = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObSequenceCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create SequenceCtx", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  } else if (OB_ISNULL(task_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task executor ctx is null", K(ret));
  } else if (OB_ISNULL(schema_service = task_ctx->schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(my_session->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else {
    uint64_t tenant_id = my_session->get_effective_tenant_id();
    const ObIArray<uint64_t>& ids = nextval_seq_ids_;
    ARRAY_FOREACH_X(ids, idx, cnt, OB_SUCC(ret))
    {
      const uint64_t seq_id = ids.at(idx);
      const ObSequenceSchema* seq_schema = nullptr;
      if (OB_FAIL(schema_guard.get_sequence_schema(tenant_id, seq_id, seq_schema))) {
        LOG_WARN("fail get sequence schema", K(seq_id), K(ret));
      } else if (OB_ISNULL(seq_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null unexpected", K(ret));
      } else if (OB_FAIL(op_ctx->seq_schemas_.push_back(*seq_schema))) {
        LOG_WARN("cache seq_schema fail", K(tenant_id), K(seq_id), K(ret));
      }
    }
  }
  return ret;
}

int ObSequence::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObSequenceCtx* sequence_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  const ObIArray<uint64_t>& ids = nextval_seq_ids_;
  if (OB_ISNULL(sequence_ctx = GET_PHY_OPERATOR_CTX(ObSequenceCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator ctx failed");
  } else if (OB_ISNULL(sequence_ctx->sequence_cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("seq cache not init", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(try_get_next_row(ctx, row))) {
    LOG_WARN_IGNORE_ITER_END(ret, "fail get next row", K(ret));
  } else if (ids.count() != sequence_ctx->seq_schemas_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("id count does not match schema count",
        "id_cnt",
        ids.count(),
        "schema_cnt",
        sequence_ctx->seq_schemas_.count(),
        K(ret));
  } else {
    uint64_t tenant_id = my_session->get_effective_tenant_id();
    ObArenaAllocator allocator;  // nextval temporary calculation memory
    ARRAY_FOREACH_X(ids, idx, cnt, OB_SUCC(ret))
    {
      const uint64_t seq_id = ids.at(idx);
      ObSequenceValue seq_value;
      if (OB_FAIL(sequence_ctx->sequence_cache_->nextval(sequence_ctx->seq_schemas_.at(idx), allocator, seq_value))) {
        LOG_WARN("fail get nextval for seq", K(tenant_id), K(seq_id), K(ret));
      } else if (OB_FAIL(my_session->set_sequence_value(tenant_id, seq_id, seq_value))) {
        LOG_WARN(
            "save seq_value to session as currval for later read fail", K(tenant_id), K(seq_id), K(seq_value), K(ret));
      } else {
      }
    }
  }
  return ret;
}

int ObSequence::try_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObPhyOperator* child = nullptr;
  ObSequenceCtx* sequence_ctx = NULL;
  if (get_child_num() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not have more than 1 child", K(ret));
  } else if (OB_ISNULL(sequence_ctx = GET_PHY_OPERATOR_CTX(ObSequenceCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator ctx failed");
  } else if (get_child_num() == 0) {
    // insert stmt, no child, give an empty row
    row = &(sequence_ctx->get_cur_row());
  } else if (OB_ISNULL(child = get_child(ObPhyOperator::FIRST_CHILD))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child should not be null", K(ret));
  } else if (OB_FAIL(child->get_next_row(ctx, row))) {
    LOG_WARN_IGNORE_ITER_END(ret, "fail get next row", K(ret));
  } else if (OB_FAIL(copy_cur_row_by_projector(*sequence_ctx, row))) {
    LOG_WARN("copy current row failed", K(ret));
  }
  return ret;
}

int ObSequence::add_filter(ObSqlExpression* expr)
{
  UNUSED(expr);
  LOG_ERROR("sequence operator should have no filter expr");
  return OB_NOT_SUPPORTED;
}

OB_SERIALIZE_MEMBER((ObSequence, ObMultiChildrenPhyOperator), nextval_seq_ids_);

}  // namespace sql
}  // namespace oceanbase
