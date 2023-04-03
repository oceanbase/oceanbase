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
#include "sql/engine/sequence/ob_sequence_op.h"
#include "lib/utility/utility.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

OB_SERIALIZE_MEMBER((ObSequenceSpec, ObOpSpec), nextval_seq_ids_);

ObSequenceSpec::ObSequenceSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
    nextval_seq_ids_(alloc)
{
}

int ObSequenceSpec::add_uniq_nextval_sequence_id(uint64_t seq_id)
{
  int ret = OB_SUCCESS;
  for (uint64_t i = 0; i < nextval_seq_ids_.count() && OB_SUCC(ret); ++i) {
    if (seq_id == nextval_seq_ids_.at(i)) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("should not add duplicated seq id to ObSequence operator",
               K(seq_id), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(nextval_seq_ids_.push_back(seq_id))) {
      LOG_WARN("fail add seq id to nextval seq id set", K(seq_id), K(ret));
    }
  }
  return ret;
}

ObSequenceOp::ObSequenceOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    sequence_cache_(nullptr)
{
  sequence_cache_ = &share::ObSequenceCache::get_instance();
  if (OB_ISNULL(sequence_cache_)) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "fail alloc memory for ObSequenceCache instance");
  }
}

ObSequenceOp::~ObSequenceOp()
{
}

int ObSequenceOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op())) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_ISNULL(sequence_cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("seq cache not init", K(ret));
  } else if (get_child_cnt() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not have more than 1 child", K(ret));
  } else if (0 < MY_SPEC.filters_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sequence operator should have no filter expr", K(ret));
  }
  return ret;
}

int ObSequenceOp::inner_close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

int ObSequenceOp::init_op()
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_ctx = NULL;
  share::schema::ObMultiVersionSchemaService *schema_service = NULL;
  ObSQLSessionInfo *my_session = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(task_ctx = GET_TASK_EXECUTOR_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task executor ctx is null", K(ret));
  } else if (OB_ISNULL(schema_service = task_ctx->schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(
              my_session->get_effective_tenant_id(),
              schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else {
    uint64_t tenant_id = my_session->get_effective_tenant_id();
    const ObIArray<uint64_t> &ids = MY_SPEC.nextval_seq_ids_;
    ARRAY_FOREACH_X(ids, idx, cnt, OB_SUCC(ret)) {
      const uint64_t seq_id = ids.at(idx);
      const ObSequenceSchema *seq_schema = nullptr;
      if (OB_FAIL(schema_guard.get_sequence_schema(
                  tenant_id,
                  seq_id,
                  seq_schema))) {
        LOG_WARN("fail get sequence schema", K(seq_id), K(ret));
      } else if (OB_ISNULL(seq_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null unexpected", K(ret));
      } else if (OB_FAIL(seq_schemas_.push_back(*seq_schema))) {
        // 注意：这里将 schema 缓存到数组里，会自动深拷贝 sequence name
        //       即使 schema guard 释放，sequence name 的内存也还有效，直到请求结束
        LOG_WARN("cache seq_schema fail", K(tenant_id), K(seq_id), K(ret));
      }
    }
  }
  return ret;
}

int ObSequenceOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
  const ObIArray<uint64_t> &ids = MY_SPEC.nextval_seq_ids_;
  if (OB_FAIL(try_get_next_row())) {
    LOG_WARN_IGNORE_ITER_END(ret, "fail get next row", K(ret));
  } else if (ids.count() != seq_schemas_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("id count does not match schema count",
             "id_cnt", ids.count(),
             "schema_cnt", seq_schemas_.count(),
             K(ret));
  } else {
    uint64_t tenant_id = my_session->get_effective_tenant_id();
    ObArenaAllocator allocator; // nextval 临时计算内存
    // 当且仅当 select item 中有 nextval 时才需要去 cache 中更新 nextval
    // 否则直接取用 session 中的值
    ARRAY_FOREACH_X(ids, idx, cnt, OB_SUCC(ret)) {
      const uint64_t seq_id = ids.at(idx);
      // int64_t dummy_seq_value = 10240012435; // TODO: xiaochu, 设置 number 到 session 中
      ObSequenceValue seq_value;
      // 注意：这里 schema 的顺序和 ids 里面 id 的顺序是一一对应的
      //       所以可以直接用下标来寻址
      if (OB_FAIL(sequence_cache_->nextval(
                  seq_schemas_.at(idx),
                  allocator,
                  seq_value))) {
        LOG_WARN("fail get nextval for seq", K(tenant_id), K(seq_id), K(ret));
      } else if (OB_FAIL(my_session->set_sequence_value(tenant_id, seq_id, seq_value))) {
        LOG_WARN("save seq_value to session as currval for later read fail",
                 K(tenant_id), K(seq_id), K(seq_value), K(ret));
      }
    }
  }
  return ret;
}

int ObSequenceOp::try_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (get_child_cnt() == 0) {
    // insert stmt, no child, give an empty row
    // 这里是否要将所有ObExpr全部设置为null
  } else if (OB_FAIL(child_->get_next_row())) {
    LOG_WARN_IGNORE_ITER_END(ret, "fail get next row", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
