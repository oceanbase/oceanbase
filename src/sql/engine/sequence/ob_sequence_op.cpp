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
#include "sql/engine/dml/ob_link_op.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace common::sqlclient;
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

ObLocalSequenceExecutor::ObLocalSequenceExecutor()
  :ObSequenceExecutor(),
  sequence_cache_(nullptr)
{
  sequence_cache_ = &share::ObSequenceCache::get_instance();
  if (OB_ISNULL(sequence_cache_)) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "fail alloc memory for ObSequenceCache instance");
  }
}

ObLocalSequenceExecutor::~ObLocalSequenceExecutor()
{
  destroy();
}

int ObLocalSequenceExecutor::init(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_ctx = NULL;
  share::schema::ObMultiVersionSchemaService *schema_service = NULL;
  ObSQLSessionInfo *my_session = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(task_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
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
    ARRAY_FOREACH_X(seq_ids_, idx, cnt, OB_SUCC(ret)) {
      const uint64_t seq_id = seq_ids_.at(idx);
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

void ObLocalSequenceExecutor::reset()
{

}

void ObLocalSequenceExecutor::destroy()
{
  sequence_cache_ = NULL;
}

int ObLocalSequenceExecutor::get_nextval(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  if (seq_ids_.count() != seq_schemas_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("id count does not match schema count",
             "id_cnt", seq_ids_.count(),
             "schema_cnt", seq_schemas_.count(),
             K(ret));
  } else {
    uint64_t tenant_id = my_session->get_effective_tenant_id();
    ObArenaAllocator allocator; // nextval 临时计算内存
    // 当且仅当 select item 中有 nextval 时才需要去 cache 中更新 nextval
    // 否则直接取用 session 中的值
    ARRAY_FOREACH_X(seq_ids_, idx, cnt, OB_SUCC(ret)) {
      const uint64_t seq_id = seq_ids_.at(idx);
      // int64_t dummy_seq_value = 10240012435; // TODO: xiaochu, 设置 number 到 session 中
      ObSequenceValue seq_value;
      // 注意：这里 schema 的顺序和 ids 里面 id 的顺序是一一对应的
      //       所以可以直接用下标来寻址
      if (OB_FAIL(sequence_cache_->nextval(seq_schemas_.at(idx),
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

ObRemoteSequenceExecutor::ObRemoteSequenceExecutor()
  :ObSequenceExecutor(),
  sessid_(0),
  link_type_(DBLINK_UNKNOWN),
  format_sql_(NULL),
  format_sql_length_(0),
  dblink_conn_(NULL)
{

}

ObRemoteSequenceExecutor::~ObRemoteSequenceExecutor()
{
  destroy();
}

int ObRemoteSequenceExecutor::init(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = NULL;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  }
  ARRAY_FOREACH_X(seq_ids_, idx, cnt, OB_SUCC(ret)) {
    const uint64_t seq_id = seq_ids_.at(idx);
    const ObSequenceSchema *seq_schema = nullptr;
    if (OB_FAIL(my_session->get_dblink_sequence_schema(seq_id, seq_schema))) {
      LOG_WARN("failed to get dblink sequence schema", K(ret));
    } else if (OB_ISNULL(seq_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else if (OB_FAIL(seq_schemas_.push_back(*seq_schema))) {
      // 注意：这里将 schema 缓存到数组里，会自动深拷贝 sequence name
      //       即使 schema guard 释放，sequence name 的内存也还有效，直到请求结束
      LOG_WARN("cache seq_schema fail", K(seq_id), K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_dblink_connection(ctx))) {
    LOG_WARN("failed to init dblink connection", K(ret));
  } else if (OB_FAIL(init_sequence_sql(ctx))) {
    LOG_WARN("failed to init sequence sql", K(ret));
  }
  return ret;
}

int ObRemoteSequenceExecutor::init_dblink_connection(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo * my_session = ctx.get_my_session();
  common::sqlclient::ObISQLConnection *dblink_conn = NULL;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObDbLinkProxy *dblink_proxy = GCTX.dblink_proxy_;
  const ObDbLinkSchema *dblink_schema = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  ObSchemaGetterGuard schema_guard;
  dblink_param_ctx param_ctx;
  if (OB_ISNULL(dblink_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dblink_proxy is NULL", K(ret));
  } else if (OB_ISNULL(my_session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_session or plan_ctx is NULL", K(my_session), K(plan_ctx), K(ret));
  } else if (FALSE_IT(sessid_ = my_session->get_sessid())) {
  } else if (FALSE_IT(tenant_id = my_session->get_effective_tenant_id())) {
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_dblink_schema(tenant_id, dblink_id_, dblink_schema))) {
    LOG_WARN("failed to get dblink schema", K(ret), K(tenant_id), K(dblink_id_));
  } else if (OB_ISNULL(dblink_schema)) {
    ret = OB_DBLINK_NOT_EXIST_TO_ACCESS;
    LOG_WARN("dblink schema is NULL", K(ret), K(dblink_id_));
  } else if (FALSE_IT(link_type_ = static_cast<DblinkDriverProto>(dblink_schema->get_driver_proto()))) {
    // do nothing
  } else if (OB_FAIL(ObLinkOp::init_dblink_param_ctx(ctx,
                                                     param_ctx,
                                                     link_type_,
                                                     tenant_id,
                                                     dblink_id_,
                                                     sessid_,
                                                     my_session->get_next_sql_request_level()))) {
    LOG_WARN("failed to init dblink param ctx", K(ret));
  } else if (OB_FAIL(dblink_proxy->create_dblink_pool(param_ctx,
                                                      dblink_schema->get_host_addr(),
                                                      dblink_schema->get_tenant_name(),
                                                      dblink_schema->get_user_name(),
                                                      dblink_schema->get_plain_password(),
                                                      dblink_schema->get_database_name(),
                                                      dblink_schema->get_conn_string(),
                                                      dblink_schema->get_cluster_name()))) {
    LOG_WARN("failed to create dblink pool", K(ret));
  } else if (OB_FAIL(ObDblinkService::get_local_session_vars(my_session, ctx.get_allocator(), param_ctx))) {
    LOG_WARN("failed to get local session vars", K(ret));
  } else if (OB_FAIL(dblink_proxy->acquire_dblink(param_ctx,
                                                  dblink_conn_))) {
    LOG_WARN("failed to acquire dblink", K(ret), K(dblink_id_));
  } else if (OB_FAIL(my_session->get_dblink_context().register_dblink_conn_pool(dblink_conn_->get_common_server_pool()))) {
    LOG_WARN("failed to register dblink conn pool to current session", K(ret));
  } else {
    LOG_TRACE("link op get connection from dblink pool", KP(dblink_conn_), K(lbt()));
  }
  return ret;
}

int ObRemoteSequenceExecutor::init_sequence_sql(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.append("SELECT "))) {
    LOG_WARN("failed to append string", K(ret));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < seq_schemas_.count(); ++i) {
    //const ObSequenceSchema *seq_schema = nullptr;
    if (OB_FAIL(sql.append_fmt(" %.*s.NEXTVAL ",
                            seq_schemas_.at(i).get_sequence_name().length(),
                            seq_schemas_.at(i).get_sequence_name().ptr()))) {
      LOG_WARN("failed to append string", K(ret));
    } else if (i == seq_ids_.count() - 1) {
      //do nothing
    } else if (OB_FAIL(sql.append(", "))) {
      LOG_WARN("failed to append string", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append(" FROM DUAL"))) {
      LOG_WARN("failed to append string", K(ret));
    } else if (OB_FALSE_IT(format_sql_length_ = sql.length() + 1)) {
    } else if (OB_ISNULL(format_sql_ = static_cast<char*>(ctx.get_allocator().alloc(format_sql_length_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      MEMSET(format_sql_, 0, format_sql_length_);
      MEMCPY(format_sql_, sql.ptr(), sql.length());
    }
  }
  return ret;
}

void ObRemoteSequenceExecutor::reset()
{

}

void ObRemoteSequenceExecutor::destroy()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_DBLINK
  if (DBLINK_DRV_OCI == link_type_ &&
      NULL != dblink_conn_ &&
      OB_FAIL(static_cast<ObOciConnection *>(dblink_conn_)->free_oci_stmt())) {
    LOG_WARN("failed to close oci result", K(ret));
  }
#endif
  if (OB_NOT_NULL(GCTX.dblink_proxy_) &&
      OB_NOT_NULL(dblink_conn_) &&
      OB_FAIL(GCTX.dblink_proxy_->release_dblink(link_type_, dblink_conn_))) {
    LOG_WARN("failed to release connection", K(ret));
  }
  sessid_ = 0;
  dblink_conn_ = NULL;
}

int ObRemoteSequenceExecutor::rescan()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_DBLINK
  if (DBLINK_DRV_OCI == link_type_ &&
      NULL != dblink_conn_ &&
      OB_FAIL(static_cast<ObOciConnection *>(dblink_conn_)->free_oci_stmt())) {
    LOG_WARN("failed to close oci result", K(ret));
  }
#endif
  return ret;
}

int ObRemoteSequenceExecutor::get_nextval(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  sqlclient::ObMySQLResult *result = NULL;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(rescan())) {
      LOG_WARN("failed to rescan dblink reset", K(ret));
    } else if (OB_FAIL(GCTX.dblink_proxy_->dblink_read(dblink_conn_, res, format_sql_))) {
      LOG_WARN("read failed", K(ret), K(link_type_), K(dblink_conn_), K(format_sql_));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next val", K(ret));
    } else {
      uint64_t tenant_id = my_session->get_effective_tenant_id();
      for (int64_t i = 0; OB_SUCC(ret) && i < seq_ids_.count(); i++) {
        number::ObNumber num_val;
        ObSequenceValue seq_value;
        if (OB_FAIL(result->get_number(i, num_val, ctx.get_allocator()))) {
          LOG_WARN("failed to sequence val", K(ret));
        } else if (OB_FAIL(seq_value.set(num_val))) {
          LOG_WARN("failed to set value", K(ret));
        } else if (OB_FAIL(my_session->set_sequence_value(tenant_id, seq_ids_.at(i), seq_value))) {
          LOG_WARN("failed to set sequence value", K(ret));
        }
      }
  }
  }
  return ret;
}

ObSequenceOp::ObSequenceOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input)
{
}

ObSequenceOp::~ObSequenceOp()
{
}

void ObSequenceOp::destroy()
{
  for (int64_t i = 0; i < seq_executors_.count(); ++i) {
    seq_executors_.at(i)->destroy();
  }
  seq_executors_.reset();
  ObOperator::destroy();
}

int ObSequenceOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = NULL;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(init_op())) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (get_child_cnt() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not have more than 1 child", K(ret));
  } else if (0 < MY_SPEC.filters_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sequence operator should have no filter expr", K(ret));
  }
  ARRAY_FOREACH_X(MY_SPEC.nextval_seq_ids_, idx, cnt, OB_SUCC(ret)) {
    const uint64_t seq_id = MY_SPEC.nextval_seq_ids_.at(idx);
    const ObSequenceSchema *seq_schema = nullptr;
    uint64_t dblink_id = OB_INVALID_ID;
    ObSequenceExecutor *executor = NULL;
    if (OB_FAIL(my_session->get_dblink_sequence_schema(seq_id, seq_schema))) {
      LOG_WARN("failed to get dblink sequence schema", K(ret));
    } else if (NULL != seq_schema) {
      dblink_id = seq_schema->get_dblink_id();
    }
    for (int64_t i = 0; NULL==executor && OB_SUCC(ret) && i < seq_executors_.count(); ++i) {
      if (seq_executors_.at(i)->get_dblink_id() == dblink_id) {
        executor = seq_executors_.at(i);
      }
    }
    if (NULL != executor) {
      if (OB_FAIL(executor->add_sequence_id(seq_id))) {
        LOG_WARN("failed to add sequence id", K(ret));
      }
    } else if (OB_INVALID_ID == dblink_id) {
      //add local executor
      void *tmp = NULL;
      if (OB_ISNULL(tmp=ctx_.get_allocator().alloc(sizeof(ObLocalSequenceExecutor)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else {
        executor = new(tmp) ObLocalSequenceExecutor();
        executor->set_dblink_id(dblink_id);
        if (OB_FAIL(executor->add_sequence_id(seq_id))) {
          LOG_WARN("failed to add sequence id", K(ret));
        } else if (OB_FAIL(seq_executors_.push_back(executor))) {
          LOG_WARN("failed to push back executor", K(ret));
        }
      }
    } else {
      //add remote executor
      void *tmp = NULL;
      if (OB_ISNULL(tmp=ctx_.get_allocator().alloc(sizeof(ObRemoteSequenceExecutor)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else {
        executor = new(tmp) ObRemoteSequenceExecutor();
        executor->set_dblink_id(dblink_id);
        if (OB_FAIL(executor->add_sequence_id(seq_id))) {
          LOG_WARN("failed to add sequence id", K(ret));
        } else if (OB_FAIL(seq_executors_.push_back(executor))) {
          LOG_WARN("failed to push back executor", K(ret));
        }
      }
    }
  }
  ARRAY_FOREACH_X(seq_executors_, idx, cnt, OB_SUCC(ret)) {
    if (OB_FAIL(seq_executors_.at(idx)->init(ctx_))) {
      LOG_WARN("failed to init executor", K(ret));
    }
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
  return ret;
}

int ObSequenceOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_get_next_row())) {
    LOG_WARN_IGNORE_ITER_END(ret, "fail get next row", K(ret));
  } else {
    ARRAY_FOREACH_X(seq_executors_, idx, cnt, OB_SUCC(ret)) {
      ObSequenceExecutor *executor = seq_executors_.at(idx);
      if (OB_FAIL(executor->get_nextval(ctx_))) {
        LOG_WARN("fail get nextval for seq", K(ret));
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
