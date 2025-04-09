//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX RS
#include "rootserver/truncate_info/ob_truncate_info_service.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "observer/ob_inner_sql_connection.h"
#include "rootserver/ob_ddl_service.h"
#include "share/ob_debug_sync_point.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace schema;
using namespace observer;
using namespace obrpc;
using namespace storage;
namespace rootserver
{
ERRSIM_POINT_DEF(EN_TRUNCATE_TABLET_TRANS);

int ObTruncateTabletArg::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, info_, ls_id_, index_tablet_id_, truncate_info_);
  return ret;
}

int64_t ObTruncateTabletArg::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, info_, ls_id_, index_tablet_id_, truncate_info_);
  return len;
}

int ObTruncateTabletArg::deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, info_, ls_id_, index_tablet_id_);
  if (FAILEDx(truncate_info_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize truncate arg", KR(ret));
  }
  return ret;
}
/*
* ObTruncatePartKeyInfo
*/
ObTruncatePartKeyInfo::ObTruncatePartKeyInfo(ObIAllocator &allocator)
  : session_(nullptr),
    free_session_ctx_(),
    expr_factory_(allocator),
    part_expr_(nullptr),
    subpart_expr_(nullptr)
{}

ObTruncatePartKeyInfo::~ObTruncatePartKeyInfo()
{
  release_tmp_session(free_session_ctx_, session_);
}

int ObTruncatePartKeyInfo::init(
    ObIAllocator &allocator,
    const uint64_t tenant_id,
    const ObTableSchema &data_table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObPartitionLevel part_level = data_table_schema.get_part_level();
  const uint64_t data_table_id = data_table_schema.get_table_id();
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("Failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(create_tmp_session(tenant_id, lib::is_oracle_mode(), schema_guard, free_session_ctx_, session_))) {
    LOG_WARN("Failed to create temp session", K(ret));
  } else if (OB_UNLIKELY(ObPartitionLevel::PARTITION_LEVEL_ONE != part_level
      && ObPartitionLevel::PARTITION_LEVEL_TWO != part_level)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part level", K(ret), K(part_level), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(resolve_part_expr(allocator, schema_guard, data_table_schema, PARTITION_LEVEL_ONE, part_expr_))) {
    LOG_WARN("Failed to resolve part expr", K(ret), K(tenant_id), K(data_table_id));
  } else if (ObPartitionLevel::PARTITION_LEVEL_TWO == part_level
      && OB_FAIL(resolve_part_expr(allocator, schema_guard, data_table_schema, PARTITION_LEVEL_TWO, subpart_expr_))) {
    LOG_WARN("Failed to resolve subpart expr", K(ret), K(tenant_id), K(data_table_id));
  }
  return ret;
}

int ObTruncatePartKeyInfo::check_only_have_ref_columns(
    const ObTableSchema &data_table_schema,
    const ObPartitionLevel check_part_level,
    bool &only_ref_columns)
{
  int ret = OB_SUCCESS;
  only_ref_columns = false;
  const ObPartitionLevel schema_part_level = data_table_schema.get_part_level();
  const uint64_t data_table_id = data_table_schema.get_table_id();
  if (OB_UNLIKELY(check_part_level > schema_part_level
    || (ObPartitionLevel::PARTITION_LEVEL_ONE != check_part_level
        && ObPartitionLevel::PARTITION_LEVEL_TWO != check_part_level))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part level", K(ret), K(schema_part_level), K(check_part_level));
  } else if (OB_FAIL(inner_check_only_have_ref_columns(data_table_schema, PARTITION_LEVEL_ONE, only_ref_columns))) {
    LOG_WARN("Failed to check only have ref columns", K(ret), K(data_table_id));
  } else if (only_ref_columns && ObPartitionLevel::PARTITION_LEVEL_TWO == check_part_level
      && OB_FAIL(inner_check_only_have_ref_columns(data_table_schema, PARTITION_LEVEL_TWO, only_ref_columns))) {
    LOG_WARN("Failed to check only have ref columns", K(ret), K(data_table_id));
  }
  return ret;
}

int ObTruncatePartKeyInfo::inner_check_column_schema(
  const ObTableSchema &data_table_schema,
  ObRawExpr &expr,
  bool &only_ref_columns)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *ref_expr = static_cast<ObColumnRefRawExpr *>(&expr);
  const uint64_t ref_col_id = ref_expr->get_column_id();
  const ObColumnSchemaV2 *column_schema = nullptr;
  if (OB_UNLIKELY(nullptr == ref_expr ||
                  nullptr == (column_schema = data_table_schema.get_column_schema(ref_col_id)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not found column schema by column id", KR(ret),
             K(ref_col_id), KPC(ref_expr), KPC(column_schema));
  } else if (!column_schema->is_column_stored_in_sstable()) {
    only_ref_columns = false;
    LOG_INFO("[TRUNCATE INFO] expr is not stored in data schema", KR(ret), KPC(column_schema));
  } else if (OB_FAIL(ref_column_ids_.push_back(ref_col_id))) {
    LOG_WARN("failed to push back ref_column_id", KR(ret), K(ref_col_id));
  }
  return ret;
}

int ObTruncatePartKeyInfo::inner_check_only_have_ref_columns(
    const ObTableSchema &data_table_schema,
    const ObPartitionLevel check_part_level,
    bool &only_ref_columns)
{
  int ret = OB_SUCCESS;
  only_ref_columns = false;
  ObPartitionFuncType part_type = ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX;
  ObRawExpr *check_part_expr = nullptr;
  if (PARTITION_LEVEL_ONE == check_part_level) {
    part_type = data_table_schema.get_part_option().get_part_func_type();
    check_part_expr = part_expr_;
  } else if (PARTITION_LEVEL_TWO == check_part_level) {
    part_type = data_table_schema.get_sub_part_option().get_part_func_type();
    check_part_expr = subpart_expr_;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part level", KR(ret), K(check_part_level));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(check_part_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null part expr", KR(ret), K(check_part_level), KP(check_part_expr));
  } else if (T_OP_ROW == check_part_expr->get_expr_type() || T_REF_COLUMN == check_part_expr->get_expr_type()) {
    only_ref_columns = true;
    if (0 == check_part_expr->get_param_count() && OB_FAIL(inner_check_column_schema(data_table_schema, *check_part_expr, only_ref_columns))) {
      LOG_WARN("failed to check column schema", KR(ret), KPC(check_part_expr));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < check_part_expr->get_param_count() && only_ref_columns; ++i) {
      ObRawExpr *param_expr = nullptr;
      if (OB_ISNULL(param_expr = check_part_expr->get_param_expr(i))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("null expr in part_expr", KR(ret), K(i));
      } else if (T_REF_COLUMN == param_expr->get_expr_type()) {
        if (OB_FAIL(inner_check_column_schema(data_table_schema, *param_expr, only_ref_columns))) {
          LOG_WARN("failed to check column schema", KR(ret), KPC(check_part_expr));
        }
      } else {
        only_ref_columns = false;
      }
    } // for
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[TRUNCATE INFO]check_only_have_ref_columns", KR(ret), K(part_type), K(check_part_level), K(only_ref_columns), KPC(check_part_expr), K(check_part_expr->get_param_count()));
  }
  return ret;
}

int ObTruncatePartKeyInfo::check_stored_ref_columns_for_index(
    const share::schema::ObTableSchema &index_table_schema,
    bool &stored_ref_columns)
{
  int ret = OB_SUCCESS;
  stored_ref_columns = true;
  const int64_t index_table_id = index_table_schema.get_table_id();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < ref_column_ids_.count() && stored_ref_columns; ++idx) {
    const ObColumnSchemaV2 *column_schema = nullptr;
    const uint64_t ref_column_id = ref_column_ids_.at(idx);
    if (OB_ISNULL(column_schema = index_table_schema.get_column_schema(ref_column_id))) {
      stored_ref_columns = false;
      LOG_INFO("[TRUNCATE INFO] ref column is not found in index schema", KR(ret), K(idx), K(ref_column_id), K(index_table_id));
    } else if (!column_schema->is_column_stored_in_sstable()) {
      stored_ref_columns = false;
      LOG_INFO("[TRUNCATE INFO] ref column is not stored in index schema", KR(ret), K(idx), K(index_table_id), KPC(column_schema));
    }
  } // for
  LOG_TRACE("[TRUNCATE INFO] check_stored_ref_columns_for_index", KR(ret), K(index_table_id), K(stored_ref_columns), K(index_table_schema), K_(ref_column_ids));
  return ret;
}

int ObTruncatePartKeyInfo::resolve_part_expr(
    ObIAllocator &allocator,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &table_schema,
    const ObPartitionLevel build_part_level,
    ObRawExpr *&raw_part_expr)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  if (OB_FAIL(schema_checker.init(schema_guard))) {
    LOG_WARN("Failed to init schema_checker", K(ret));
  } else if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    ObStmtFactory stmt_factory(allocator);
    TableItem table_item;
    ObResolverParams resolver_ctx;
    resolver_ctx.allocator_ = &allocator;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = session_;
    resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
    resolver_ctx.expr_factory_ = &expr_factory_;
    resolver_ctx.stmt_factory_ = &stmt_factory;
    resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
    table_item.table_id_ = table_schema.get_table_id();
    table_item.ref_id_ = table_schema.get_table_id();
    table_item.type_ = TableItem::BASE_TABLE;
    ObPartitionFuncType part_type = ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX;

    SMART_VARS_3 ((ObDeleteResolver, delete_resolver, resolver_ctx),
                  (sql::ObExecContext, exec_ctx, allocator),
                  (sql::ObPhysicalPlanCtx, phy_plan_ctx, allocator)) {
      LinkExecCtxGuard link_guard(*session_, exec_ctx);
      exec_ctx.set_my_session(session_);
      exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
      ObString part_str;
      ObDeleteStmt *delete_stmt = delete_resolver.create_stmt<ObDeleteStmt>();
      if (OB_ISNULL(delete_stmt) || OB_ISNULL(resolver_ctx.query_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete_stmt or query_ctx is NULL", K(delete_stmt), K(resolver_ctx.query_ctx_));
      } else if (OB_FAIL(delete_stmt->get_table_items().push_back(&table_item))) {
        LOG_WARN("Failed to push back table item", K(ret));
      } else if (OB_FAIL(delete_stmt->set_table_bit_index(table_schema.get_table_id()))) {
        LOG_WARN("Failed to set table bit index", K(ret));
      } else if (ObPartitionLevel::PARTITION_LEVEL_ONE == build_part_level) {
        part_str = table_schema.get_part_option().get_part_func_expr_str();
        part_type = table_schema.get_part_option().get_part_func_type();
      } else if (ObPartitionLevel::PARTITION_LEVEL_TWO == build_part_level) {
        part_str = table_schema.get_sub_part_option().get_part_func_expr_str();
        part_type = table_schema.get_sub_part_option().get_part_func_type();
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(delete_resolver.resolve_partition_expr(table_item,
                                                           table_schema,
                                                           part_type,
                                                           part_str,
                                                           raw_part_expr))) {
          LOG_WARN("Failed to resolve partition expr", K(ret), K(part_type));
        } else if (OB_ISNULL(raw_part_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("raw_part_expr is NULL", K(ret), K(part_type));
        }
      }
      exec_ctx.set_physical_plan_ctx(NULL);
    } // end smart_var
  }
  return ret;
}

int ObTruncatePartKeyInfo::create_tmp_session(
    const uint64_t tenant_id,
    const bool is_oracle_mode,
    ObSchemaGetterGuard &schema_guard,
    ObFreeSessionCtx &free_session_ctx,
    ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  session = nullptr;
  uint32_t sid = ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;
  const schema::ObTenantSchema *tenant_info = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("Failed to create sess id", K(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(
             OB_SYS_TENANT_ID, sid, proxy_sid, ObTimeUtility::current_time(), session))) {
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session = nullptr;
    LOG_WARN("Failed to create session", K(ret), K(sid), K(tenant_id));
  } else {
    free_session_ctx.sessid_ = sid;
    free_session_ctx.proxy_sessid_ = proxy_sid;
  }
  if (FAILEDx(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("Failed to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null tenant schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->load_default_sys_variable(false, false))) {
    LOG_WARN("Failed to load default sys variable", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->load_default_configs_in_pc())) {
    LOG_WARN("Failed to load default configs in pc", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->init_tenant(tenant_info->get_tenant_name(), tenant_id))) {
     LOG_WARN("Failed to init tenant in session", K(ret), K(tenant_id));
  } else {
    session->set_inner_session();
    session->set_compatibility_mode(is_oracle_mode ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE);
  }
  if (OB_FAIL(ret)) {
    release_tmp_session(free_session_ctx, session);
  }
  return ret;
}

void ObTruncatePartKeyInfo::release_tmp_session(ObFreeSessionCtx &free_session_ctx, ObSQLSessionInfo *&session)
{
  if (nullptr != session) {
    session->set_session_sleep();
    GCTX.session_mgr_->revert_session(session);
    GCTX.session_mgr_->free_session(free_session_ctx);
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
    session = nullptr;
  }
}

int ObTruncatePartKeyInfo::extract_col_ref_expr(
  ObRawExpr *part_expr,
  ObIArray<ObRawExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter", K(ret));
  } else if (T_REF_COLUMN == part_expr->get_expr_type()) {
    if (!is_contain(exprs, part_expr) && OB_FAIL(exprs.push_back(part_expr))) {
      LOG_WARN("Failed to push back", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_expr->get_param_count(); ++i) {
      if (OB_FAIL(extract_col_ref_expr(part_expr->get_param_expr(i), exprs))) {
        LOG_WARN("Failed to extract column ref expr", K(ret), K(i), KPC(part_expr));
      }
    }
  }
  return ret;
}

int ObTruncatePartKeyInfo::get_ref_column_id_array(
    const ObTableSchema &index_table_schema,
    const ObPartitionLevel part_level,
    ObIArray<int64_t> &ref_column_idx_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObPartitionLevel::PARTITION_LEVEL_ONE != part_level
      && ObPartitionLevel::PARTITION_LEVEL_TWO != part_level)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part level", K(ret), K(part_level));
  } else {
    ObRawExpr *raw_part_expr = ObPartitionLevel::PARTITION_LEVEL_ONE == part_level ? part_expr_ : subpart_expr_;
    ObSEArray<ObColDesc, 4> index_rowkey_col_desc;
    if (OB_ISNULL(raw_part_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part expr is unexpected null", KR(ret), KP(raw_part_expr), K(part_level), KPC(part_expr_), KPC(subpart_expr_));
    } else if (OB_FAIL(index_table_schema.get_multi_version_column_descs(index_rowkey_col_desc))) {
      LOG_WARN("Failed to get index rowkey col id", K(ret));
    } else {
      ObSEArray<ObRawExpr*, 4> col_ref_exprs;
      if (OB_FAIL(extract_col_ref_expr(raw_part_expr, col_ref_exprs))) {
        LOG_WARN("Failed to extract column ref exprs", K(ret));
      } else if (OB_UNLIKELY(col_ref_exprs.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty col ref expr", KR(ret), KPC(raw_part_expr));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < col_ref_exprs.count(); ++i) {
        ObColumnRefRawExpr *ref_expr = static_cast<ObColumnRefRawExpr*>(col_ref_exprs.at(i));
        const uint64_t expr_col_id = ref_expr->get_column_id();
        bool found = false;
        for (int64_t j = 0; OB_SUCC(ret) && !found && j < index_rowkey_col_desc.count(); ++j) {
          if (index_rowkey_col_desc.at(j).col_id_ == expr_col_id) {
            if (OB_FAIL(ref_column_idx_array.push_back(j))) {
              LOG_WARN("Failed to push back", K(ret));
            } else {
              found = true;
            }
          }
        } // for
        if (OB_UNLIKELY(!found)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected state, not found index rowkey projector for part expr", K(ret), K(i), K(expr_col_id),
                  K(index_rowkey_col_desc), K(col_ref_exprs), "index_table_id", index_table_schema.get_table_id());
        }
      } // for
    }
  }
  return ret;
}

/*
* ObTruncateInfoService
*/
ObTruncateInfoService::ObTruncateInfoService(
    const obrpc::ObAlterTableArg &arg,
    const ObTableSchema &data_table_schema)
    : allocator_("truncateInfoSer"),
      loop_allocator_(),
      arg_(arg),
      data_table_schema_(data_table_schema),
      index_tablet_array_(),
      ls_id_array_(),
      part_key_info_(allocator_),
      ddl_task_id_(0),
      is_inited_(false)
{}

int ObTruncateInfoService::init(ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(part_key_info_.init(allocator_, get_tenant_id(), data_table_schema_))) {
    LOG_WARN("failed to init part_key_info", KR(ret));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(sql_proxy, get_tenant_id(), ddl_task_id_))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTruncateInfoService::check_only_have_ref_columns(
    const obrpc::ObAlterTableArg::AlterPartitionType &alter_type,
    bool &only_ref_columns)
{
  int ret = OB_SUCCESS;
  bool tmp_only_ref_columns = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTruncateInfoService is not inited", K(ret));
  } else if (obrpc::ObAlterTableArg::DROP_PARTITION == alter_type ||
      obrpc::ObAlterTableArg::TRUNCATE_PARTITION == alter_type) {
    ret = part_key_info_.check_only_have_ref_columns(data_table_schema_, ObPartitionLevel::PARTITION_LEVEL_ONE, tmp_only_ref_columns);
  } else if (obrpc::ObAlterTableArg::DROP_SUB_PARTITION == alter_type ||
             obrpc::ObAlterTableArg::TRUNCATE_SUB_PARTITION == alter_type) {
    if (OB_SUCC(part_key_info_.check_only_have_ref_columns(
            data_table_schema_, ObPartitionLevel::PARTITION_LEVEL_ONE,
            tmp_only_ref_columns)) && tmp_only_ref_columns) {
      ret = part_key_info_.check_only_have_ref_columns(data_table_schema_, ObPartitionLevel::PARTITION_LEVEL_TWO, tmp_only_ref_columns);
    }
  }
  if (OB_SUCC(ret)) {
    only_ref_columns = tmp_only_ref_columns;
  }
  return ret;
}

int ObTruncateInfoService::check_stored_ref_columns_for_index(
    const share::schema::ObTableSchema &index_table_schema,
    bool &stored_ref_columns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTruncateInfoService is not inited", K(ret));
  } else if (OB_UNLIKELY(!part_key_info_.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ObTruncatePartKeyInfo is invalid", KR(ret), K_(part_key_info));
  } else {
    ret = part_key_info_.check_stored_ref_columns_for_index(index_table_schema, stored_ref_columns);
  }
  return ret;
}

uint64_t ObTruncateInfoService::get_tenant_id() const
{
  return arg_.exec_tenant_id_;
}

#ifdef ERRSIM
int errsim_truncate_trans(const int64_t data_table_id, const int64_t index_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(EN_TRUNCATE_TABLET_TRANS)) {
    ret = OB_ERR_UNEXPECTED;
    FLOG_INFO("ERRSIM EN_TRUNCATE_TABLET_TRANS", KR(ret), K(data_table_id), K(index_table_id));
  }
  return ret;
}

#endif
int ObTruncateInfoService::execute(ObMySQLTransaction &trans,
                                   ObDDLOperator &ddl_operator,
                                   ObTableSchema &index_table_schema) {
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *conn = nullptr;
  bool is_column_store = false;
  index_tablet_array_.reuse();
  ls_id_array_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTruncateInfoService is not inited", K(ret));
  } else if (OB_UNLIKELY(!index_table_schema.is_global_index_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input index schema is not global index", KR(ret), K(index_table_schema));
  } else if (OB_FAIL(index_table_schema.get_is_column_store(is_column_store))) {
    LOG_WARN("failed to get is column store", KR(ret), K(index_table_schema));
  } else if (OB_UNLIKELY(is_column_store)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported write truncate info for column store global index", KR(ret), K(is_column_store), K(index_table_schema));
  } else if (OB_FAIL(ObDDLLock::lock_for_modify_truncate_info_in_trans(get_tenant_id(), index_table_schema.get_table_id(), trans))) {
    LOG_WARN("failed to lock global index", KR(ret), "index_table_id", index_table_schema.get_table_id());
  } else if (OB_FAIL(index_table_schema.get_tablet_ids(index_tablet_array_))) {
    LOG_WARN("failed to get tablet id from index schema", KR(ret), K(index_table_schema));
  } else if (index_tablet_array_.empty()) {
    // do nothing
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(trans, get_tenant_id(), index_tablet_array_, ls_id_array_))) {
    LOG_WARN("failed to get ls id array", KR(ret), K(get_tenant_id()), K_(index_tablet_array));
  } else if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is NULL", KR(ret));
  } else if (OB_FAIL(gen_new_schema_version_for_index_(trans, ddl_operator, index_table_schema))) {
    LOG_WARN("failed to gen new schema version for index", KR(ret));
  } else if (OB_FAIL(loop_part_to_register_mds_(*conn, index_table_schema))) {
    LOG_WARN("failed to loop register", KR(ret));
  } else {
    DEBUG_SYNC(TRUNCATE_PARTITION_TRANS);
#ifdef ERRSIM
    ret = errsim_truncate_trans(data_table_schema_.get_table_id(), index_table_schema.get_table_id());
#endif
    LOG_INFO("[TRUNCATE_INFO] success to write truncate info for index schema", K(get_tenant_id()), K_(arg),
      "data_table_id", data_table_schema_.get_table_id(),
      "index_table_id", index_table_schema.get_table_id());
  }
  return ret;
}

int ObTruncateInfoService::gen_new_schema_version_for_index_(
  ObMySQLTransaction &trans,
  ObDDLOperator &ddl_operator,
  ObTableSchema &index_table_schema)
{
  int ret = OB_SUCCESS;
  const int64_t old_schema_version = index_table_schema.get_schema_version();
  if (OB_FAIL(ddl_operator.update_table_attribute(index_table_schema, trans, OB_DDL_UPDATE_TABLE_SCHEMA_VERSION))) {
    LOG_WARN("failed to update_table_attribute", KR(ret), K(index_table_schema));
  } else {
    LOG_INFO("[TRUNCATE INFO] success to gen new schema version for index", KR(ret), K(old_schema_version),
      "index_new_schema_version", index_table_schema.get_schema_version());
  }
  return ret;
}

class ObTruncatePartSchemaUtil final
{
public:
  template <typename T>
  static int build_truncate_part_of_default_part(
    ObIAllocator &allocator,
    const T &alter_part,
    const ObPartitionLevel build_part_level,
    const ObTableSchema &data_table_schema,
    ObTruncatePartition &truncate_part);
  template <typename T>
  static int build_truncate_part(
    ObIAllocator &allocator,
    const ObPartitionFuncType part_type,
    const T &alter_part,
    const ObPartitionLevel build_part_level,
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    const T *input_found_part,
    const ObIArray<int64_t> &ref_part_id_array,
    ObTruncatePartition &truncate_part);
  static ObTruncatePartition::TruncatePartType
    get_truncate_part_type(const ObPartitionFuncType part_type);
  static int check_is_default_part(
    const ObListRowValues &list_row_values,
    bool &is_default_part);
private:
  template <typename T>
  static int build_truncate_list_part_(
    ObIAllocator &allocator,
    const ObPartitionFuncType part_type,
    const T &alter_part,
    const ObPartitionLevel build_part_level,
    const ObTableSchema &data_table_schema,
    const T *input_found_part,
    ObTruncatePartition &truncate_part);
    template <typename T>
  static int build_truncate_range_part_(
    ObIAllocator &allocator,
    const ObPartitionFuncType part_type,
    const T &alter_part,
    const ObPartitionLevel build_part_level,
    const ObTableSchema &data_table_schema,
    ObTruncatePartition &truncate_part);
};

template <typename T>

int ObTruncatePartSchemaUtil::build_truncate_part_of_default_part(
  ObIAllocator &allocator,
  const T &alter_part,
  const ObPartitionLevel build_part_level,
  const ObTableSchema &data_table_schema,
  ObTruncatePartition &truncate_part)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObBasePartition *, 8> other_part_array;
  if (OB_UNLIKELY(!alter_part.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_table_schema), K(alter_part));
  } else if (OB_FAIL(data_table_schema.get_other_part_by_name(alter_part.get_part_name(), build_part_level, other_part_array))) {
    LOG_WARN("failed to get other part by name", KR(ret), K(alter_part.get_part_name()));
  } else {
    ObListRowValues list_row_values(allocator);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < other_part_array.count(); ++idx) {
      if (OB_ISNULL(other_part_array.at(idx))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid part ptr in array", KR(ret), K(idx), K(other_part_array.at(idx)));
      } else {
        const common::ObIArray<common::ObNewRow>& values = other_part_array.at(idx)->get_list_row_values();
        for (int64_t j = 0; OB_SUCC(ret) && j < values.count(); ++j) {
          if (OB_FAIL(list_row_values.push_back(values.at(j)))) {
            LOG_WARN("failed to push list row values", KR(ret), K(list_row_values), K(j));
          }
        } // for
      }
    } // for
    if (FAILEDx(list_row_values.sort_array())) {
      LOG_WARN("failed to sort list row values", KR(ret), K(list_row_values));
    } else if (OB_FAIL(truncate_part.init_list_part(
      allocator,
      ObTruncatePartition::LIST_PART,
      0 == list_row_values.count() ? ObTruncatePartition::ALL :ObTruncatePartition::EXCEPT,
      list_row_values))) {
      LOG_WARN("failed to init list part", KR(ret), K(list_row_values));
    }
  }
  return ret;
}

ObTruncatePartition::TruncatePartType ObTruncatePartSchemaUtil::get_truncate_part_type(const ObPartitionFuncType part_type)
{
  ObTruncatePartition::TruncatePartType ret_type = ObTruncatePartition::PART_TYPE_MAX;
  if (PARTITION_FUNC_TYPE_RANGE == part_type) {
    ret_type = ObTruncatePartition::RANGE_PART;
  } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type) {
    ret_type = ObTruncatePartition::RANGE_COLUMNS_PART;
  } else if (PARTITION_FUNC_TYPE_LIST == part_type) {
    ret_type = ObTruncatePartition::LIST_PART;
  } else if (PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
    ret_type = ObTruncatePartition::LIST_COLUMNS_PART;
  }
  return ret_type;
}

int ObTruncatePartSchemaUtil::check_is_default_part(
  const ObListRowValues &list_row_values,
  bool &is_default_part)
{
  int ret = OB_SUCCESS;
  is_default_part = false;
  if (1 == list_row_values.count() && 1 == list_row_values.at(0).get_count()) {
    const ObObj &obj = list_row_values.at(0).get_cell(0);
    if (obj.is_max_value()) {
      is_default_part = true;
    }
  }
  return ret;
}

template <typename T>
int ObTruncatePartSchemaUtil::build_truncate_part(
  ObIAllocator &allocator,
  const ObPartitionFuncType part_type,
  const T &alter_part,
  const ObPartitionLevel build_part_level,
  const ObTableSchema &data_table_schema,
  const ObTableSchema &index_table_schema,
  const T *input_found_part,
  const ObIArray<int64_t> &ref_part_id_array,
  ObTruncatePartition &truncate_part)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!data_table_schema.is_valid() || !alter_part.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_table_schema), K(alter_part));
  } else if (OB_FAIL(truncate_part.part_key_idxs_.init(allocator, ref_part_id_array))) {
    LOG_WARN("failed to init part key idxs", KR(ret), K(ref_part_id_array));
  } else if (is_range_part(part_type)) {
    ret = build_truncate_range_part_(allocator, part_type, alter_part, build_part_level, data_table_schema, truncate_part);
  } else if (is_list_part(part_type)) {
    ret = build_truncate_list_part_(allocator, part_type, alter_part, build_part_level, data_table_schema, input_found_part, truncate_part);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part type", KR(ret), K(part_type));
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!truncate_part.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid truncate part", KR(ret), K(truncate_part));
  }
  return ret;
}

template <typename T>
int ObTruncatePartSchemaUtil::build_truncate_range_part_(
  ObIAllocator &allocator,
  const ObPartitionFuncType part_type,
  const T &alter_part,
  const ObPartitionLevel build_part_level,
  const ObTableSchema &data_table_schema,
  ObTruncatePartition &truncate_part)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  const ObBasePartition *part = nullptr;
  const ObBasePartition *prev_part = nullptr;
  if (OB_UNLIKELY(!is_range_part(part_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_table_schema), K(alter_part));
  } else if (OB_FAIL(data_table_schema.get_partition_and_prev_by_name(
      alter_part.get_part_name(),
      build_part_level,
      part,
      prev_part))) {
    LOG_WARN("failed to get partition and prev part", KR(ret), K(alter_part.get_part_name()));
  } else if (OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("found part is null", KR(ret), K(alter_part), K(idx), KP(part), KP(prev_part));
  } else {
    const T *prev_part_ptr = (nullptr != prev_part) ? static_cast<const T*>(prev_part) : nullptr;
    ret = truncate_part.init_truncate_part(
      allocator,
      get_truncate_part_type(part_type),
      *static_cast<const T*>(part),
      prev_part_ptr);
  }
  return ret;
}


template <typename T>
int ObTruncatePartSchemaUtil::build_truncate_list_part_(
  ObIAllocator &allocator,
  const ObPartitionFuncType part_type,
  const T &alter_part,
  const ObPartitionLevel build_part_level,
  const ObTableSchema &data_table_schema,
  const T *input_found_part,
  ObTruncatePartition &truncate_part)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  const T *found_part = nullptr;
  bool is_default_part = false;
  const ObBasePartition *part = nullptr;
  const ObBasePartition *prev_part_placeholder = nullptr;
  if (OB_UNLIKELY(!is_list_part(part_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(alter_part), K(part_type));
  } else if (nullptr != input_found_part) {
    found_part = input_found_part;
  } else if (OB_FAIL(data_table_schema.get_partition_and_prev_by_name(
      alter_part.get_part_name(),
      build_part_level,
      part,
      prev_part_placeholder))) {
    LOG_WARN("failed to get partition and prev part", KR(ret), K(alter_part.get_part_name()));
  } else {
    found_part = static_cast<const T*>(part);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(found_part)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("found part is null", KR(ret), KPC(input_found_part), K(idx), KPC(found_part));
  } else if (OB_FAIL(ObTruncatePartSchemaUtil::check_is_default_part(found_part->get_list_row_values_struct(), is_default_part))) {
    LOG_WARN("failed to check is default part", KR(ret), KPC(found_part));
  } else if (is_default_part) {
    if (OB_FAIL(build_truncate_part_of_default_part(allocator, *found_part, build_part_level, data_table_schema, truncate_part))) {
      LOG_WARN("failed to build default part", KR(ret), KPC(found_part));
    }
  } else {
    const T *unused_prev_part = nullptr;
    ret = truncate_part.init_truncate_part(
      allocator,
      get_truncate_part_type(part_type),
      *found_part,
      unused_prev_part);
  }
  return ret;
}

int ObTruncateInfoService::loop_part_to_register_mds_(
  ObInnerSQLConnection &conn,
  const ObTableSchema &index_table_schema)
{
  int ret = OB_SUCCESS;
  const int64_t index_new_schema_version = index_table_schema.get_schema_version();
  const AlterTableSchema &alter_table_schema = arg_.alter_table_schema_;
  ObPartition **alter_part_array = alter_table_schema.get_part_array();
  const int64_t alter_part_num = alter_table_schema.get_partition_num();
  const ObPartitionFuncType part_type = data_table_schema_.get_part_option().get_part_func_type();
  const ObAlterTableArg::AlterPartitionType &alter_type = arg_.alter_part_type_;
  if (OB_UNLIKELY(nullptr == alter_part_array || alter_part_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partitiion array is unexpected null or empty", KR(ret), K(alter_table_schema),
      KP(alter_part_array), K(alter_part_num));
  } else if (ObAlterTableArg::DROP_SUB_PARTITION == alter_type ||
      ObAlterTableArg::TRUNCATE_SUB_PARTITION == alter_type) {
    if (OB_FAIL(loop_subpart_to_register_mds_(conn, index_table_schema))) {
      LOG_WARN("failed to loop subpart", KR(ret));
    }
  } else if (ObAlterTableArg::DROP_PARTITION == alter_type ||
      ObAlterTableArg::TRUNCATE_PARTITION == alter_type) {
    ObSEArray<int64_t, 4> part_ref_column_idx_array;
    if (OB_FAIL(part_key_info_.get_ref_column_id_array(index_table_schema, PARTITION_LEVEL_ONE, part_ref_column_idx_array))) {
      LOG_WARN("failed to get ref column id array", KR(ret));
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < alter_part_num; ++idx) {
      loop_allocator_.reuse();
      const ObPartition *alter_part = alter_part_array[idx];
      const ObPartition *found_part_placeholder = nullptr;
      ObTruncateTabletArg truncate_arg;
      ObTruncateInfo &truncate_info = truncate_arg.truncate_info_;
      if (OB_ISNULL(alter_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alter partitiion is unexpected null", KR(ret), KP(alter_part_array), K(idx));
      } else if (FALSE_IT(truncate_info.allocator_ = &loop_allocator_)) {
      } else if (OB_FAIL(ObTruncatePartSchemaUtil::build_truncate_part(
                     loop_allocator_, part_type, *alter_part,
                     PARTITION_LEVEL_ONE, data_table_schema_, index_table_schema,
                     found_part_placeholder, part_ref_column_idx_array,
                     truncate_info.truncate_part_))) {
        LOG_WARN("failed to build truncate info arg", KR(ret), KPC(alter_part), K_(data_table_schema));
      } else {
        truncate_info.schema_version_ = index_new_schema_version;
        truncate_info.key_.inc_seq_ = idx;
        if (OB_FAIL(loop_index_tablet_id_to_register_(conn, truncate_arg))) {
          LOG_WARN("failed to loop index tablet id to register", KR(ret));
        }
      }
    } // for
  } else {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid alter type", KR(ret), K(alter_type), K_(arg));
  }
  return ret;
}

int ObTruncateInfoService::loop_index_tablet_id_to_register_(
  ObInnerSQLConnection &conn,
  ObTruncateTabletArg &truncate_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ls_id_array_.count() != index_tablet_array_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id array and ls id array is not equal", KR(ret), K_(ls_id_array), K_(index_tablet_array));
  } else {
    truncate_arg.truncate_info_.key_.tx_id_ = ddl_task_id_;
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < index_tablet_array_.count(); ++j) {
    // for same index tablet, different truncate key have different inc_seq
    truncate_arg.ls_id_ = ls_id_array_.at(j);
    truncate_arg.index_tablet_id_ = index_tablet_array_.at(j);
    if (OB_FAIL(register_mds_(conn, truncate_arg))) {
      LOG_WARN("failed to register mds", KR(ret), K(get_tenant_id()), K(j),
               K_(index_tablet_array), K_(ls_id_array), K(truncate_arg));
    }
  } // for
  return ret;
}

int ObTruncateInfoService::register_mds_(
  ObInnerSQLConnection &conn,
  const ObTruncateTabletArg &arg)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = arg.get_serialize_size();
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_ISNULL(buf = (char *)loop_allocator_.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail alloc memory", KR(ret), K(buf_len));
  } else if (OB_FAIL(arg.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), K(arg));
  } else if (OB_FAIL(retry_register_mds_(conn, arg, buf, buf_len))) {
    LOG_WARN("fail to register mds", KR(ret), K(get_tenant_id()), K(arg));
  }
  return ret;
}

int ObTruncateInfoService::retry_register_mds_(
    ObInnerSQLConnection &conn,
    const ObTruncateTabletArg &arg,
    const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  const int64_t default_timeout_ts = GCONF.rpc_timeout;
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout_ts))) {
    LOG_WARN("fail to set timeout ctx", KR(ret), K(default_timeout_ts));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    do {
      if (ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", KR(ret), K(ctx));
      } else if (OB_FAIL(conn.register_multi_data_source(
                    get_tenant_id(), arg.ls_id_,
                    transaction::ObTxDataSourceType::SYNC_TRUNCATE_INFO,
                    buf, buf_len))) {
        if (need_retry_errno(ret)) {
          LOG_INFO("fail to register_tx_data, try again", KR(ret), K(get_tenant_id()), K(arg));
          ob_usleep(SLEEP_INTERVAL);
        } else {
          LOG_WARN("fail to register_tx_data", KR(ret), K(arg), K(buf), K(buf_len));
        }
      }
    } while (need_retry_errno(ret));
    if (OB_SUCC(ret)) {
      LOG_INFO("[TRUNCATE_INFO] success to register mds", KR(ret), K(buf_len), K(arg),
        "cost_ts", ObTimeUtility::current_time() - start_time);
    }
  }
  return ret;
}

int ObTruncateInfoService::loop_subpart_to_register_mds_(
    observer::ObInnerSQLConnection &conn,
   const ObTableSchema &index_table_schema)
{
  int ret = OB_SUCCESS;
  const AlterTableSchema &alter_table_schema = arg_.alter_table_schema_;
  ObPartition **alter_part_array = alter_table_schema.get_part_array();
  if (OB_UNLIKELY(nullptr == alter_part_array || 1 != alter_table_schema.get_partition_num())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("truncate subpart should in same part info", KR(ret), KP(alter_part_array), K(alter_table_schema));
  } else {
    ObSubPartition **sub_partition_array = alter_part_array[0]->get_subpart_array();
    const int64_t subpart_num = alter_part_array[0]->get_sub_part_num();
    const ObPartitionFuncType part_type = data_table_schema_.get_part_option().get_part_func_type();
    const ObPartitionFuncType subpart_type = data_table_schema_.get_sub_part_option().get_part_func_type();
    ObSEArray<int64_t, 4> part_ref_column_idx_array;
    ObSEArray<int64_t, 4> subpart_ref_column_idx_array;
    if (OB_FAIL(part_key_info_.get_ref_column_id_array(index_table_schema, PARTITION_LEVEL_ONE, part_ref_column_idx_array))) {
      LOG_WARN("failed to get ref column id array", KR(ret), K_(part_key_info));
    } else if (OB_FAIL(part_key_info_.get_ref_column_id_array(index_table_schema, PARTITION_LEVEL_TWO, subpart_ref_column_idx_array))) {
      LOG_WARN("failed to get ref column id array", KR(ret), K_(part_key_info));
    } else if (OB_UNLIKELY(nullptr == sub_partition_array || subpart_num <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KP(sub_partition_array), K(subpart_num));
    } else {
      const ObPartition *alter_part_in_schema = nullptr;
      const ObSubPartition *alter_subpart_in_schema = nullptr;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < subpart_num; ++idx) {
        loop_allocator_.reuse();
        ObTruncateTabletArg truncate_arg;
        ObTruncateInfo &truncate_info = truncate_arg.truncate_info_;
        if (OB_FAIL(data_table_schema_.get_subpartition_by_name(
            sub_partition_array[idx]->get_part_name(), alter_part_in_schema, alter_subpart_in_schema))) {
          LOG_WARN("failed to get part idx by name", KR(ret), K(idx), KPC(sub_partition_array[idx]));
        } else if (FALSE_IT(truncate_info.allocator_ = &loop_allocator_)) {
        } else if (OB_FAIL(ObTruncatePartSchemaUtil::build_truncate_part(
                       loop_allocator_, part_type, *alter_part_in_schema,
                       PARTITION_LEVEL_ONE, data_table_schema_, index_table_schema,
                       alter_part_in_schema /*found_part*/, part_ref_column_idx_array,
                       truncate_info.truncate_part_))) {
          LOG_WARN("failed to build truncate part", KR(ret), KPC(alter_part_in_schema));
        } else if (OB_FAIL(ObTruncatePartSchemaUtil::build_truncate_part(
                       loop_allocator_, subpart_type,
                       *alter_subpart_in_schema,
                       PARTITION_LEVEL_TWO, data_table_schema_, index_table_schema,
                       alter_subpart_in_schema /*found_part*/, subpart_ref_column_idx_array,
                       truncate_info.truncate_subpart_))) {
          LOG_WARN("failed to build truncate subpart", KR(ret), KPC(alter_subpart_in_schema));
        } else {
          truncate_info.is_sub_part_ = true;
          truncate_info.schema_version_ = index_table_schema.get_schema_version();
          truncate_info.key_.inc_seq_ = idx;
          if (OB_FAIL(loop_index_tablet_id_to_register_(conn, truncate_arg))) {
            LOG_WARN("failed to loop index tablet id to register", KR(ret));
          }
        }
      } // for
    }
  }
  return ret;
}

bool ObTruncateInfoService::need_retry_errno(int ret)
{
  return is_location_service_renew_error(ret) || OB_NOT_MASTER == ret;
}

} // namespace rootserver
} // namespace oceanbase
