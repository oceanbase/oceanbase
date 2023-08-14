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
#include "sql/ob_sql_context.h"
#include <algorithm>
#include "lib/container/ob_se_array_iterator.h"
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/ob_sql_define.h"
#include "sql/optimizer/ob_log_plan.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/dblink/ob_dblink_utils.h"
#include "src/storage/tx/ob_trans_define_v4.h"

using namespace ::oceanbase::common;
namespace oceanbase
{
using namespace share::schema;
namespace sql
{
bool LocationConstraint::operator==(const LocationConstraint &other) const {
  return key_ == other.key_ && phy_loc_type_ == other.phy_loc_type_ && constraint_flags_ == other.constraint_flags_ ;
}

bool LocationConstraint::operator!=(const LocationConstraint &other) const {
  return !(*this == other);
}

int LocationConstraint::calc_constraints_inclusion(const ObLocationConstraint *left,
                                                   const ObLocationConstraint *right,
                                                   InclusionType &inclusion_result)
{
  int ret = OB_SUCCESS;
  inclusion_result = NotSubset;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(left), K(right));
  } else {
    const ObLocationConstraint *set1 = NULL, *set2 = NULL;
    bool is_subset = true;
    // insure set1.count() >= set2.count()
    if (left->count() >= right->count()) {
      inclusion_result = LeftIsSuperior;
      set1 = left;
      set2 = right;
    } else {
      inclusion_result = RightIsSuperior;
      set1 = right;
      set2 = left;
    }

    for (int64_t i = 0; is_subset && i < set2->count(); i++) {
      bool detected = false;
      for (int64_t j = 0; !detected && j < set1->count(); j++) {
        if (set2->at(i) == set1->at(j)) {
          detected = true;
        }
      }
      // if the element is not in set1, set1 can not contain all the elements in set2
      if (!detected) {
        is_subset = false;
      }
    }
    if (!is_subset) {
      inclusion_result = NotSubset;
    }
  }

  return ret;
}

int ObLocationConstraintContext::calc_constraints_inclusion(const ObPwjConstraint *left,
                                                            const ObPwjConstraint *right,
                                                            InclusionType &inclusion_result)
{
  int ret = OB_SUCCESS;
  inclusion_result = NotSubset;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(left), K(right));
  } else {
    const ObPwjConstraint *set1 = NULL, *set2 = NULL;
    bool is_subset = true;
    // insure set1.count() >= set2.count()
    if (left->count() >= right->count()) {
      inclusion_result = LeftIsSuperior;
      set1 = left;
      set2 = right;
    } else {
      inclusion_result = RightIsSuperior;
      set1 = right;
      set2 = left;
    }

    for (int64_t i = 0; is_subset && i < set2->count(); i++) {
      bool detected = false;
      for (int64_t j = 0; !detected && j < set1->count(); j++) {
        if (set2->at(i) == set1->at(j)) {
          detected = true;
        }
      }
      // if the element is not in set1, set1 can not contain all the elements in set2
      if (!detected) {
        is_subset = false;
      }
    }
    if (!is_subset) {
      inclusion_result = NotSubset;
    }
  }

  return ret;
}

int ObQueryRetryInfo::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObQueryRetryInfo::reset()
{
  inited_ = false;
  is_rpc_timeout_ = false;
  last_query_retry_err_ = OB_SUCCESS;
  retry_cnt_ = 0;
  query_switch_leader_retry_timeout_ts_ = 0;
}

void ObQueryRetryInfo::clear()
{
  // 这里不能将inited_设为false
  is_rpc_timeout_ = false;
  //last_query_retry_err_ = OB_SUCCESS;
}

void ObQueryRetryInfo::set_is_rpc_timeout(bool is_rpc_timeout)
{
  is_rpc_timeout_ = is_rpc_timeout;
}

bool ObQueryRetryInfo::is_rpc_timeout() const
{
  return is_rpc_timeout_;
}

ObSqlCtx::ObSqlCtx()
  : session_info_(NULL),
    schema_guard_(NULL),
    secondary_namespace_(NULL),
    plan_cache_hit_(false),
    self_add_plan_(false),
    disable_privilege_check_(PRIV_CHECK_FLAG_NORMAL),
    force_print_trace_(false),
    is_show_trace_stmt_(false),
    retry_times_(OB_INVALID_COUNT),
    exec_type_(InvalidType),
    is_prepare_protocol_(false),
    is_pre_execute_(false),
    is_prepare_stage_(false),
    is_dynamic_sql_(false),
    is_dbms_sql_(false),
    is_cursor_(false),
    is_remote_sql_(false),
    statement_id_(common::OB_INVALID_ID),
    stmt_type_(stmt::T_NONE),
    is_restore_(false),
    need_late_compile_(false),
    all_plan_const_param_constraints_(nullptr),
    all_possible_const_param_constraints_(nullptr),
    all_equal_param_constraints_(nullptr),
    all_pre_calc_constraints_(nullptr),
    all_expr_constraints_(nullptr),
    all_priv_constraints_(nullptr),
    need_match_all_params_(false),
    is_ddl_from_primary_(false),
    cur_stmt_(NULL),
    cur_plan_(nullptr),
    can_reroute_sql_(false),
    is_sensitive_(false),
    is_protocol_weak_read_(false),
    flashback_query_expr_(nullptr),
    is_execute_call_stmt_(false),
    enable_sql_resource_manage_(false),
    res_map_rule_id_(OB_INVALID_ID),
    res_map_rule_param_idx_(OB_INVALID_INDEX),
    res_map_rule_version_(0),
    is_text_ps_mode_(false),
    first_plan_hash_(0),
    is_bulk_(false),
    ins_opt_ctx_(),
    flags_(0),
    reroute_info_(nullptr)
{
  sql_id_[0] = '\0';
  sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
}

void ObSqlCtx::reset()
{
  multi_stmt_item_.reset();
  session_info_ = NULL;
  schema_guard_ = NULL;
  plan_cache_hit_ = false;
  self_add_plan_ = false;
  disable_privilege_check_ = PRIV_CHECK_FLAG_NORMAL;
  force_print_trace_ = false;
  is_show_trace_stmt_ = false;
  retry_times_ = OB_INVALID_COUNT;
  sql_id_[0] = '\0';
  sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
  exec_type_ = InvalidType;
  is_prepare_protocol_ = false;
  is_pre_execute_ = false;
  is_prepare_stage_ = false;
  is_dynamic_sql_ = false;
  is_remote_sql_ = false;
  is_restore_ = false;
  need_late_compile_ = false;
  all_plan_const_param_constraints_ = nullptr;
  all_possible_const_param_constraints_ = nullptr;
  all_equal_param_constraints_ = nullptr;
  all_pre_calc_constraints_ = nullptr;
  all_expr_constraints_ = nullptr;
  all_priv_constraints_ = nullptr;
  need_match_all_params_ = false;
  is_ddl_from_primary_ = false;
  can_reroute_sql_ = false;
  is_sensitive_ = false;
  enable_sql_resource_manage_ = false;
  res_map_rule_id_ = OB_INVALID_ID;
  res_map_rule_param_idx_ = OB_INVALID_INDEX;
  res_map_rule_version_ = 0;
  is_protocol_weak_read_ = false;
  first_plan_hash_ = 0;
  first_outline_data_.reset();
  if (nullptr != reroute_info_) {
    reroute_info_->reset();
    op_reclaim_free(reroute_info_);
    reroute_info_ = nullptr;
  }
  clear();
  flashback_query_expr_ = nullptr;
  stmt_type_ = stmt::T_NONE;
  cur_plan_ = nullptr;
  is_execute_call_stmt_ = false;
  is_text_ps_mode_ = false;
  enable_strict_defensive_check_ = false;
  enable_user_defined_rewrite_ = false;
  is_bulk_ = false;
  ins_opt_ctx_.reset();
}

//release dynamic allocated memory
void ObSqlCtx::clear()
{
  partition_infos_.reset();
  related_user_var_names_.reset();
  base_constraints_.reset();
  strict_constraints_.reset();
  non_strict_constraints_.reset();
  dup_table_replica_cons_.reset();
  multi_stmt_rowkey_pos_.reset();
  spm_ctx_.bl_key_.reset();
  cur_stmt_ = nullptr;
  is_text_ps_mode_ = false;
  ins_opt_ctx_.clear();
}

OB_SERIALIZE_MEMBER(ObSqlCtx, stmt_type_);

void ObSqlSchemaGuard::reset()
{
  table_schemas_.reset();
  schema_guard_ = NULL;
  allocator_.reset();
  next_link_table_id_ = 1;
  dblink_scn_.reuse();
}

TableItem *ObSqlSchemaGuard::get_table_item_by_ref_id(const ObDMLStmt *stmt, uint64_t ref_table_id)
{
  TableItem *table_item = NULL;
  if (NULL != stmt) {
   const common::ObIArray<sql::TableItem*> &table_items = stmt->get_table_items();
    int64_t num = table_items.count();
    for (int64_t i = 0; i < num; ++i) {
      if (table_items.at(i) != NULL && table_items.at(i)->ref_id_ == ref_table_id) {
        table_item = table_items.at(i);
        break;
      }
    }
  }
  return table_item;
}

bool ObSqlSchemaGuard::is_link_table(const ObDMLStmt *stmt, uint64_t table_id)
{
  bool is_link = false;
  TableItem *table_item = NULL;
  if (NULL != stmt) {
    table_item = stmt->get_table_item_by_id(table_id);
    is_link = (NULL == table_item) ? false : table_item->is_link_table();
  }
  return is_link;
}

int ObSqlSchemaGuard::get_dblink_schema(const uint64_t tenant_id,
                                        const uint64_t dblink_id,
                                        const share::schema::ObDbLinkSchema *&dblink_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null schema guard", K(ret));
  } else if (OB_FAIL(schema_guard_->get_dblink_schema(tenant_id,
                                                      dblink_id,
                                                      dblink_schema))) {
    LOG_WARN("failed to get dblink schema", K(ret));
  }
  return ret;
}

int ObSqlSchemaGuard::set_link_table_schema(uint64_t dblink_id,
                                            const common::ObString &database_name,
                                            share::schema::ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  table_schema->set_dblink_id(dblink_id);
  table_schema->set_table_type(share::schema::ObTableType::USER_TABLE);
  OX(table_schema->set_link_database_name(database_name);)
  OX (table_schema->set_table_id(next_link_table_id_++));
  OX (table_schema->set_link_table_id(table_schema->get_table_id()));
  OV (table_schema->get_table_id() != OB_INVALID_ID,
      OB_ERR_UNEXPECTED, dblink_id, next_link_table_id_);
  if (OB_FAIL(table_schemas_.push_back(table_schema))) {
    LOG_WARN("failed to push back table schema", K(ret));
  }
  return ret;
}

int ObSqlSchemaGuard::get_table_schema(uint64_t dblink_id,
                                         const ObString &database_name,
                                         const ObString &table_name,
                                         const ObTableSchema *&table_schema,
                                         sql::ObSQLSessionInfo *session_info,
                                         const ObString &dblink_name,
                                         bool is_reverse_link)
{
  int ret = OB_SUCCESS;
  int64_t schema_count = table_schemas_.count();
  table_schema = NULL;
  const uint64_t tenant_id = MTL_ID();
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(table_schema) && i < schema_count; i++) {
    // database_name和table_name直接调用compare接口，使用memcmp语义比较，避免多字符集导致各种问题。
    const ObTableSchema *tmp_schema = table_schemas_.at(i);
    OV (OB_NOT_NULL(tmp_schema));
    if (OB_SUCC(ret) && dblink_id == tmp_schema->get_dblink_id() &&
        0 == database_name.compare(tmp_schema->get_link_database_name()) &&
        0 == table_name.compare(tmp_schema->get_table_name_str())) {
      table_schema = tmp_schema;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(table_schema)) {
    ObTableSchema *tmp_schema = NULL;
    OV (OB_NOT_NULL(schema_guard_), OB_NOT_INIT);
    uint64_t current_scn = OB_INVALID_ID;
    uint64_t *scn = NULL;
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(session_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session info is null", K(ret));
      } else {
         bool use_scn = (session_info->is_in_transaction() &&
          transaction::ObTxIsolationLevel::RC == session_info->get_tx_desc()->get_isolation_level())
          || !session_info->is_in_transaction();
        if (use_scn && OB_FAIL(get_link_current_scn(dblink_id, tenant_id, session_info, current_scn))) {
          if (OB_HASH_NOT_EXIST == ret) {
            scn = &current_scn;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get link current scn failed", K(ret));
          }
        }
      }
    }
    OZ (schema_guard_->get_link_table_schema(tenant_id,
                                             dblink_id,
                                             database_name, table_name,
                                             allocator_, tmp_schema,
                                             session_info,
                                             dblink_name,
                                             is_reverse_link,
                                             scn));
    if (OB_SUCC(ret) && (NULL != scn)) {
      if (OB_FAIL(dblink_scn_.set_refactored(dblink_id, *scn))) {
        LOG_WARN("set refactored failed", K(ret));
      } else {
        LOG_TRACE("set dblink current scn", K(dblink_id), K(*scn));
      }
    }
    OV (OB_NOT_NULL(tmp_schema));
    OX (tmp_schema->set_table_id(next_link_table_id_++));
    OX (tmp_schema->set_link_table_id(tmp_schema->get_table_id()));
    OV (tmp_schema->get_table_id() != OB_INVALID_ID,
        OB_ERR_UNEXPECTED, dblink_id, next_link_table_id_);
    OZ (table_schemas_.push_back(tmp_schema));
    OX (table_schema = tmp_schema);
  }
  return ret;
}

int ObSqlSchemaGuard::get_table_schema(uint64_t table_id,
                                      uint64_t ref_table_id,
                                      const ObDMLStmt *stmt,
                                      const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else {
    const TableItem *item = stmt->get_table_item_by_id(table_id);
    if (NULL != item && item->is_link_table()) {
      if (OB_FAIL(get_link_table_schema(ref_table_id, table_schema))) {
        LOG_WARN("failed to get link table schema", K(table_id), K(ret));
      }
    } else if (OB_FAIL(get_table_schema(ref_table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(table_id), K(ret));
    }
  }
  return ret;
}

int ObSqlSchemaGuard::get_table_schema(uint64_t table_id,
                                      const TableItem *table_item,
                                      const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item) ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ret), K(table_item));
  } else if (table_item->is_link_table()) {
    if (OB_FAIL(get_link_table_schema(table_id, table_schema))) {
      LOG_WARN("failed to get link table schema", K(table_id), K(ret));
    }
  } else if (OB_FAIL(get_table_schema(table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(table_id), K(ret));
  }
  return ret;
}

int ObSqlSchemaGuard::get_table_schema(uint64_t table_id,
                                         const ObTableSchema *&table_schema,
                                         bool is_link /* = false*/) const
{
  int ret = OB_SUCCESS;
  if (is_link) {
    OZ (get_link_table_schema(table_id, table_schema), table_id, is_link);
  } else {
    const uint64_t tenant_id = MTL_ID();
    OV (OB_NOT_NULL(schema_guard_));
    OZ (schema_guard_->get_table_schema(tenant_id, table_id, table_schema), table_id, is_link);
  }
  return ret;
}

int ObSqlSchemaGuard::get_column_schema(uint64_t table_id, const ObString &column_name,
                                          const ObColumnSchemaV2 *&column_schema,
                                          bool is_link /* = false */) const
{
  int ret = OB_SUCCESS;
  if (is_link) {
    OZ (get_link_column_schema(table_id, column_name, column_schema),
        table_id, column_name, is_link);
  } else {
    const uint64_t tenant_id = MTL_ID();
    OV (OB_NOT_NULL(schema_guard_));
    OZ (schema_guard_->get_column_schema(tenant_id, table_id, column_name, column_schema),
        table_id, column_name, is_link);
  }
  return ret;
}

int ObSqlSchemaGuard::get_column_schema(uint64_t table_id, uint64_t column_id,
                                          const ObColumnSchemaV2 *&column_schema,
                                          bool is_link /* = false */) const
{
  int ret = OB_SUCCESS;
  if (is_link) {
    OZ (get_link_column_schema(table_id, column_id, column_schema),
        table_id, column_id, is_link);
  } else {
    const uint64_t tenant_id = MTL_ID();
    OV (OB_NOT_NULL(schema_guard_));
    OZ (schema_guard_->get_column_schema(tenant_id, table_id, column_id, column_schema),
        table_id, column_id, is_link);
  }
  return ret;
}

int ObSqlSchemaGuard::get_table_schema_version(const uint64_t table_id,
                                               int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  OV (OB_NOT_NULL(schema_guard_));
  OZ (schema_guard_->get_schema_version(TABLE_SCHEMA, tenant_id, table_id, schema_version), table_id);
  return ret;
}

int ObSqlSchemaGuard::get_can_read_index_array(uint64_t table_id,
                                                 uint64_t *index_tid_array,
                                                 int64_t &size,
                                                 bool with_mv,
                                                 bool with_global_index,
                                                 bool with_domain_index,
                                                 bool with_spatial_index)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  OV (OB_NOT_NULL(schema_guard_));
  OZ (schema_guard_->get_can_read_index_array(tenant_id, table_id,
                                              index_tid_array, size, with_mv,
                                              with_global_index, with_domain_index,
                                              with_spatial_index));
  return ret;
}

int ObSqlSchemaGuard::get_link_table_schema(uint64_t table_id,
                                              const ObTableSchema *&table_schema) const
{
  int ret = OB_SUCCESS;
  int64_t schema_count = table_schemas_.count();
  const ObTableSchema *tmp_schema = NULL;
  table_schema = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(table_schema) && i < schema_count; i++) {
    OX (tmp_schema = table_schemas_.at(i));
    OV (OB_NOT_NULL(tmp_schema));
    if (OB_SUCC(ret) && table_id == tmp_schema->get_table_id()) {
      table_schema = tmp_schema;
    }
  }
  return ret;
}

int ObSqlSchemaGuard::get_link_column_schema(uint64_t table_id, const ObString &column_name,
                                               const ObColumnSchemaV2 *&column_schema) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  OZ (get_link_table_schema(table_id, table_schema), table_id);
  if (OB_NOT_NULL(table_schema)) {
    OX (column_schema = table_schema->get_column_schema(column_name));
  }
  return ret;
}

int ObSqlSchemaGuard::get_link_column_schema(uint64_t table_id, uint64_t column_id,
                                               const ObColumnSchemaV2 *&column_schema) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  OZ (get_link_table_schema(table_id, table_schema), table_id);
  if (OB_NOT_NULL(table_schema)) {
    OX (column_schema = table_schema->get_column_schema(column_id));
  }
  return ret;
}

int ObSqlSchemaGuard::get_link_current_scn(uint64_t dblink_id, uint64_t tenant_id,
                                           ObSQLSessionInfo *session_info,
                                           uint64_t &current_scn)
{
  int ret = OB_SUCCESS;
  current_scn = OB_INVALID_ID;
  if (!dblink_scn_.created()) {
    if (OB_FAIL(dblink_scn_.create(4, "DblinkScnMap", "DblinkScnMap", tenant_id))) {
      LOG_WARN("create hash map failed", K(ret));
    } else {
      ret = OB_HASH_NOT_EXIST;
    }
  } else {
    if (OB_FAIL(dblink_scn_.get_refactored(dblink_id, current_scn))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get dblink scn failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSqlCtx::set_partition_infos(const ObTablePartitionInfoArray &info, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t count = info.count();
  partition_infos_.reset();
  if (count > 0) {
    partition_infos_.set_allocator(&allocator);
    if (OB_FAIL(partition_infos_.init(count))) {
      LOG_WARN("init partition info failed", K(ret), K(count));
    } else {
      for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
        if (OB_FAIL(partition_infos_.push_back(info.at(i)))) {
          LOG_WARN("push partition info failed", K(ret), K(count));
        }
      }
    }
  }
  return ret;
}

int ObSqlCtx::set_related_user_var_names(const ObIArray<ObString> &user_var_names,
                                         ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (user_var_names.count() > 0) {
    related_user_var_names_.reset();
    related_user_var_names_.set_allocator(&allocator);
    if (OB_FAIL(related_user_var_names_.init(user_var_names.count()))) {
      LOG_WARN("failed to init related_user_var_names", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < user_var_names.count(); i++) {
        if (OB_FAIL(related_user_var_names_.push_back(user_var_names.at(i)))) {
          LOG_WARN("failed to push back user var names", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    related_user_var_names_.reset();
  }
  return ret;
}

int ObSqlCtx::set_location_constraints(const ObLocationConstraintContext &location_constraint,
                                       ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  base_constraints_.reset();
  strict_constraints_.reset();
  non_strict_constraints_.reset();
  dup_table_replica_cons_.reset();
  const ObIArray<LocationConstraint> &base_constraints = location_constraint.base_table_constraints_;
  const ObIArray<ObPwjConstraint *> &strict_constraints = location_constraint.strict_constraints_;
  const ObIArray<ObPwjConstraint *> &non_strict_constraints = location_constraint.non_strict_constraints_;
  const ObIArray<ObDupTabConstraint> &dup_table_replica_cons = location_constraint.dup_table_replica_cons_;
  if (base_constraints.count() > 0) {
    base_constraints_.set_allocator(&allocator);
    if (OB_FAIL(base_constraints_.init(base_constraints.count()))) {
      LOG_WARN("init base constraints failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < base_constraints.count(); i++) {
        if (OB_FAIL(base_constraints_.push_back(base_constraints.at(i)))) {
          LOG_WARN("failed to push back base constraint", K(ret));
        } else {
          // table_partition_info_仅在计划生成阶段使用
          base_constraints_.at(i).table_partition_info_ = NULL;
        }
      }
      LOG_DEBUG("set base constraints", K(base_constraints.count()));
    }
  }
  if (OB_SUCC(ret) && strict_constraints.count() > 0) {
    strict_constraints_.set_allocator(&allocator);
    if (OB_FAIL(strict_constraints_.init(strict_constraints.count()))) {
      LOG_WARN("init strict constraints failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < strict_constraints.count(); i++) {
        if (OB_FAIL(strict_constraints_.push_back(strict_constraints.at(i)))) {
          LOG_WARN("failed to push back location constraint", K(ret));
        }
      }
      LOG_DEBUG("set strict constraints", K(strict_constraints.count()));
    }
  }
  if (OB_SUCC(ret) && non_strict_constraints.count() > 0) {
    non_strict_constraints_.set_allocator(&allocator);
    if (OB_FAIL(non_strict_constraints_.init(non_strict_constraints.count()))) {
      LOG_WARN("init non strict constraints failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < non_strict_constraints.count(); i++) {
        if (OB_FAIL(non_strict_constraints_.push_back(non_strict_constraints.at(i)))) {
          LOG_WARN("failed to push back location constraint", K(ret));
        }
      }
      LOG_DEBUG("set non strict constraints", K(non_strict_constraints.count()));
    }
  }
  if (OB_SUCC(ret) && dup_table_replica_cons.count() > 0) {
    dup_table_replica_cons_.set_allocator(&allocator);
    if (OB_FAIL(dup_table_replica_cons_.init(dup_table_replica_cons.count()))) {
      LOG_WARN("init duplicate table replica constraints failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < dup_table_replica_cons.count(); i++) {
        if (OB_FAIL(dup_table_replica_cons_.push_back(dup_table_replica_cons.at(i)))) {
          LOG_WARN("failed to push back location constraint", K(ret));
        }
      }
      LOG_DEBUG("set duplicate table replica constraints", K(dup_table_replica_cons.count()));
    }
  }
  return ret;
}

int ObSqlCtx::set_multi_stmt_rowkey_pos(const common::ObIArray<int64_t> &multi_stmt_rowkey_pos,
                                        common::ObIAllocator &alloctor)
{
  int ret = OB_SUCCESS;
  if (!multi_stmt_rowkey_pos.empty()) {
    multi_stmt_rowkey_pos_.set_allocator(&alloctor);
    if (OB_FAIL(multi_stmt_rowkey_pos_.init(multi_stmt_rowkey_pos.count()))) {
      LOG_WARN("failed to init rowkey count", K(ret));
    } else if (OB_FAIL(append(multi_stmt_rowkey_pos_, multi_stmt_rowkey_pos))) {
      LOG_WARN("failed to append multi stmt rowkey pos", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

}
}
