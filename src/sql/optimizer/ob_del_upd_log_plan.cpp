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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_del_upd_log_plan.h"
#include "ob_insert_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_link_dml.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/dblink/ob_dblink_utils.h"
#include "sql/engine/cmd/ob_table_direct_insert_service.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::sql::log_op_def;
using share::schema::ObTableSchema;
using share::schema::ObColumnSchemaV2;
using share::schema::ObSchemaGetterGuard;

int ObDelUpdLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  /*do nothing*/
  return ret;
}

int ObDelUpdLogPlan::compute_dml_parallel()
{
  int ret = OB_SUCCESS;
  use_pdml_ = false;
  max_dml_parallel_ = ObGlobalHint::UNSET_PARALLEL;
  const ObOptimizerContext &opt_ctx = get_optimizer_context();
  const ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_optimizer_context().get_session_info()));
  } else if (!opt_ctx.can_use_pdml()) {
    max_dml_parallel_ = ObGlobalHint::DEFAULT_PARALLEL;
    use_pdml_ = false;
    if (opt_ctx.is_online_ddl()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("a online ddl expect PDML enabled. but it does not!", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "online ddl without pdml");
    }
  } else {
    int64_t dml_parallel = ObGlobalHint::UNSET_PARALLEL;
    if (OB_FAIL(get_parallel_info_from_candidate_plans(dml_parallel))) {
      LOG_WARN("failed to get parallel info from candidate plans", K(ret));
    } else if (OB_UNLIKELY(ObGlobalHint::DEFAULT_PARALLEL > dml_parallel)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected parallel", K(ret), K(dml_parallel), K(opt_ctx.get_parallel_rule()));
    } else {
      max_dml_parallel_ = dml_parallel;
      use_pdml_ = (opt_ctx.is_online_ddl() ||
                  (ObGlobalHint::DEFAULT_PARALLEL < dml_parallel &&
                  is_strict_mode(session_info->get_sql_mode())));
    }
  }
  LOG_TRACE("finish compute dml parallel", K(use_pdml_), K(max_dml_parallel_),
                              K(opt_ctx.can_use_pdml()), K(opt_ctx.is_online_ddl()),
                              K(opt_ctx.get_parallel_rule()), K(opt_ctx.get_parallel()));
  return ret;
}

int ObDelUpdLogPlan::get_parallel_info_from_candidate_plans(int64_t &dop) const
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *op = NULL;
  dop = get_optimizer_context().get_parallel();
  int64_t child_parallel = ObGlobalHint::UNSET_PARALLEL;
  if (OB_UNLIKELY(candidates_.candidate_plans_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
    if (OB_ISNULL(op = candidates_.candidate_plans_.at(i).plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      child_parallel = op->is_single() ? op->get_available_parallel() : op->get_parallel();
      dop = std::max(dop, child_parallel);
    }
  }
  LOG_DEBUG("finish get max dop from candidate plans", K(dop));
  return ret;
}

int ObDelUpdLogPlan::get_pdml_parallel_degree(const int64_t target_part_cnt,
                                              int64_t &dop) const
{
  int ret = OB_SUCCESS;
  dop = ObGlobalHint::UNSET_PARALLEL;
  if (OB_UNLIKELY(!use_pdml_ || ObGlobalHint::DEFAULT_PARALLEL > max_dml_parallel_
                  || target_part_cnt < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(use_pdml_), K(max_dml_parallel_), K(target_part_cnt));
  } else if (ObGlobalHint::DEFAULT_PARALLEL == max_dml_parallel_
             || !get_optimizer_context().is_use_auto_dop()
             || target_part_cnt >= max_dml_parallel_) {
    dop = max_dml_parallel_;
  } else if (OB_FAIL(OB_E(EventTable::EN_ENABLE_AUTO_DOP_FORCE_PARALLEL_PLAN) OB_SUCCESS)) {
    ret = OB_SUCCESS;
    dop = max_dml_parallel_;
  } else {
    OPT_TRACE("Decided PDML DOP by Auto DOP.");
    dop = std::min(max_dml_parallel_, target_part_cnt * PDML_DOP_LIMIT_PER_PARTITION);
    OPT_TRACE("PDML target partition count:", target_part_cnt, "Max dml parallel", max_dml_parallel_);
  }
  OPT_TRACE("Get final PDML DOP: ", dop);
  return ret;
}

int ObDelUpdLogPlan::check_fullfill_safe_update_mode(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  bool is_sql_safe_updates = false;
  bool is_not_fullfill = false;
  const ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_optimizer_context().get_session_info()));
  } else if (OB_FAIL(session_info->get_sql_safe_updates(is_sql_safe_updates))) {
    LOG_WARN("failed to get is safe update mode", K(ret));
  } else if (!is_sql_safe_updates) {
    /*do nothing*/
  } else if (OB_FAIL(do_check_fullfill_safe_update_mode(op, is_not_fullfill))) {
    LOG_WARN("failed to check fullfill safe update mode", K(ret));
  } else if (is_not_fullfill) {
    ret = OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT;
    LOG_WARN("using safe update mode need WHERE or LIMIT", K(ret));
  }
  return ret;
}

int ObDelUpdLogPlan::do_check_fullfill_safe_update_mode(ObLogicalOperator *op, bool &is_not_fullfill)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(op), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (is_not_fullfill) {
    /*do nothing*/
  } else if (op->get_type() == log_op_def::LOG_TABLE_SCAN) {
    ObLogTableScan *table_scan = static_cast<ObLogTableScan *>(op);
    is_not_fullfill |= table_scan->is_whole_range_scan();
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_not_fullfill && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(do_check_fullfill_safe_update_mode(op->get_child(i), is_not_fullfill))) {
        LOG_WARN("failed to check fullfill safe update mode", K(ret));
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::prepare_dml_infos()
{
  return OB_SUCCESS;
}

int ObDelUpdLogPlan::generate_dblink_raw_plan()
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt *stmt = get_stmt();
  ObLogicalOperator *top = NULL;
  if (!lib::is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("mysql dblink not support dml", K(ret));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (OB_FAIL(allocate_link_dml_as_top(top))) {
    LOG_WARN("failed to allocate link dml as top", K(ret));
  } else if (OB_FAIL(make_candidate_plans(top))) {
    LOG_WARN("failed to make candidate plans", K(ret));
  } else if (OB_FAIL(static_cast<ObLogLink *>(top)->set_link_stmt())) {
    LOG_WARN("failed to set link stmt", K(ret));
  } else {
    set_plan_root(top);
    bool has_reverse_link = false;
    if (OB_FAIL(ObDblinkUtils::has_reverse_link_or_any_dblink(stmt, has_reverse_link))) {
      LOG_WARN("failed to exec has_reverse_link", K(ret));
    } else {
      uint64_t dblink_id = stmt->get_dblink_id();
      top->set_dblink_id(dblink_id);
      static_cast<ObLogLinkDml *>(top)->set_reverse_link(has_reverse_link);
      static_cast<ObLogLinkDml *>(top)->set_dml_type(stmt->get_stmt_type());
    }
  }
  return ret;
}

// check_table_rowkey_distinct 从逻辑上看计划是否可能有一行数据被更新多次
// 如果有可能，则会产生一个 distinct 运算，以去重。去哪一行，未定义。
//
// Q: 什么场景下一行数据会被更新多次呢？
// A: 一种可能的场景是可更新视图，不过这种场景在创建 view 时就被拦下来了
//    另一种场景是 MySQL 的 multi table update，例如：
//
//      UPDATE Books, Orders
//      SET Orders.Quantity = Orders.Quantity+2,
//          Books.InStock = Books.InStock-2
//      WHERE
//          Books.BookID = Orders.BookID
//
//   这时就有 join 发生了，并且 join 结果的顺序并不保证

// Q: 做 UPDATE 时，如果不检查重复行，会有什么后果？
// A: 一行数据可能被更新多次。虽然存储层不会报错，
//    但是有可能会导致主表和索引表数据不一致。
//    (因为主表、索引表更新顺序可能不同，导致终态不一致)
//
// Q: 使用 HASH DISTINCT 去重，保留谁、丢掉谁，有讲究吗？
// A: 我们是保留第一个。MySQL说随机一行，但它实际也是第一行
//    不过，这个第一行根据不同的join算法有随机性。
//
int ObDelUpdLogPlan::check_table_rowkey_distinct(
    const ObIArray<IndexDMLInfo *> &index_dml_infos)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *best_plan = NULL;
  const ObDelUpdStmt *del_upd_stmt = NULL;
  ObSEArray<ObRawExpr *, 8> rowkey_exprs;
  if (OB_ISNULL(del_upd_stmt = get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(del_upd_stmt));
  } else if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!del_upd_stmt->is_dml_table_from_join() ||
             del_upd_stmt->has_instead_of_trigger()) {
    //dml语句中不包含join条件，可以保证dml涉及到的行都来自于target table，不存在重复行，因此不需要去重
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
      IndexDMLInfo *index_dml_info = index_dml_infos.at(i);
      bool is_unique = false;
      rowkey_exprs.reuse();
      if (OB_ISNULL(index_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret));
      } else if (!use_pdml() && !index_dml_info->is_primary_index_) {
        // for PDML, primary table & index table both need unique checker.
      } else if (OB_FAIL(index_dml_info->get_rowkey_exprs(rowkey_exprs))) {
        LOG_WARN("failed to get rowkey exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(rowkey_exprs,
                                                          best_plan->get_table_set(),
                                                          best_plan->get_fd_item_set(),
                                                          best_plan->get_output_equal_sets(),
                                                          best_plan->get_output_const_exprs(),
                                                          is_unique))) {
        LOG_WARN("check dml is order unique failed", K(ret));
      } else if (!is_unique) {
        index_dml_info->distinct_algo_ = T_HASH_DISTINCT;
      } else {
        index_dml_info->distinct_algo_ = T_DISTINCT_NONE;
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::calculate_insert_table_location_and_sharding(ObTablePartitionInfo *&insert_table_part,
                                                                  ObShardingInfo *&insert_sharding)
{
  int ret = OB_SUCCESS;
  bool trigger_exist = false;
  const ObDelUpdStmt *del_upd_stmt = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObObjectID, 4> part_ids;
  ObSEArray<const ObDmlTableInfo*, 1> dml_table_infos;
  insert_table_part = NULL;
  insert_sharding = NULL;
  if (OB_ISNULL(del_upd_stmt = get_stmt()) ||
      OB_ISNULL(schema_guard = get_optimizer_context().get_schema_guard()) ||
      OB_ISNULL(session_info = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(del_upd_stmt), K(schema_guard), K(session_info), K(ret));
  } else if (del_upd_stmt->has_instead_of_trigger()) {
    /*do nothing*/
  } else if (del_upd_stmt->is_insert_stmt() &&
             !static_cast<const ObInsertStmt*>(del_upd_stmt)->value_from_select()) {
    /*do nothing*/
  } else if (OB_FAIL(del_upd_stmt->get_dml_table_infos(dml_table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  } else if (OB_UNLIKELY(dml_table_infos.count() != 1) || OB_ISNULL(dml_table_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected dml table infos", K(ret), K(dml_table_infos));
  } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                    dml_table_infos.at(0)->ref_table_id_,
                                                    table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(table_schema->has_before_insert_row_trigger(*schema_guard, trigger_exist))) {
    LOG_WARN("call has_before_insert_row_trigger failed", K(ret));
  } else if (OB_FAIL(calculate_table_location_and_sharding(*del_upd_stmt,
                                                           del_upd_stmt->get_sharding_conditions(),
                                                           dml_table_infos.at(0)->loc_table_id_,
                                                           dml_table_infos.at(0)->ref_table_id_,
                                                           &dml_table_infos.at(0)->part_ids_,
                                                           insert_sharding,
                                                           insert_table_part))) {
    if (ret != OB_NO_PARTITION_FOR_GIVEN_VALUE) {
      LOG_WARN("failed to calculate table location and sharding", K(ret));
    } else if (del_upd_stmt->is_merge_stmt()) {
      insert_sharding = NULL; // get null insert sharding, use multi part merge into
      ret = OB_SUCCESS;
    } else if (trigger_exist || (del_upd_stmt->is_insert_stmt() && static_cast<const ObInsertStmt*>(del_upd_stmt)->value_from_select())) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to calculate table location and sharding", K(ret));
    }
  }
  return ret;
}

int ObDelUpdLogPlan::calculate_table_location_and_sharding(const ObDelUpdStmt &stmt,
                                                           const ObIArray<ObRawExpr*> &filters,
                                                           const uint64_t table_id,
                                                           const uint64_t ref_table_id,
                                                           const ObIArray<ObObjectID> *part_ids,
                                                           ObShardingInfo *&sharding_info,
                                                           ObTablePartitionInfo *&table_partition_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_partition_info = reinterpret_cast<ObTablePartitionInfo*>(
                                       allocator_.alloc(sizeof(ObTablePartitionInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(table_partition_info), K(ret));
  } else if (OB_ISNULL(sharding_info = reinterpret_cast<ObShardingInfo*>(
                                       allocator_.alloc(sizeof(ObShardingInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(sharding_info), K(ret));
  } else {
    sharding_info = new(sharding_info) ObShardingInfo();
    table_partition_info = new(table_partition_info) ObTablePartitionInfo(allocator_);
    ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
    ObAddr &server = get_optimizer_context().get_local_server_addr();
    table_partition_info->get_table_location().set_check_no_partiton(stmt.is_merge_stmt());
    if (OB_FAIL(calculate_table_location(stmt,
                                         filters,
                                         table_id,
                                         ref_table_id,
                                         part_ids,
                                         *table_partition_info))) {
      LOG_WARN("failed to calculate table location", K(ret));
    } else if (OB_FAIL(table_partition_info->get_location_type(server, location_type))) {
      LOG_WARN("get location type failed", K(ret));
    } else if (FALSE_IT(sharding_info->set_location_type(location_type))) {
      // do nothing
    } else if (OB_FAIL(sharding_info->init_partition_info(get_optimizer_context(),
                                                          stmt,
                                                          table_id,
                                                          ref_table_id,
                                                          table_partition_info->get_phy_tbl_location_info_for_update()))) {
      LOG_WARN("set partition key failed", K(ret));
    } else {
      LOG_TRACE("succeed to generate target sharding info", K(*sharding_info),
          K(*table_partition_info));
    }
  }
  return ret;
}

int ObDelUpdLogPlan::calculate_table_location(const ObDelUpdStmt &stmt,
                                              const ObIArray<ObRawExpr*> &filters,
                                              const uint64_t table_id,
                                              const uint64_t ref_table_id,
                                              const ObIArray<ObObjectID> *part_ids,
                                              ObTablePartitionInfo &table_partition_info)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *sql_schema_guard = get_optimizer_context().get_sql_schema_guard();
  const ParamStore *params = get_optimizer_context().get_params();
  ObExecContext *exec_ctx = get_optimizer_context().get_exec_ctx();
  const common::ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(
      get_optimizer_context().get_session_info());
  ObArray<ObRawExpr *> correlated_filters;
  ObArray<ObRawExpr *> uncorrelated_filters;
  // initialized the table location
  if (OB_ISNULL(sql_schema_guard) || OB_ISNULL(exec_ctx) ||
      OB_ISNULL(params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(sql_schema_guard), K(exec_ctx),
        K(params));
  } else if (OB_FAIL(ObOptimizerUtil::extract_parameterized_correlated_filters(
            filters,
            correlated_filters,
            uncorrelated_filters))) {
    LOG_WARN("Failed to extract correlated filters", K(ret));
  } else if (OB_FAIL(table_partition_info.init_table_location(*sql_schema_guard,
                                                               stmt,
                                                               exec_ctx,
                                                               uncorrelated_filters,
                                                               table_id,
                                                               ref_table_id,
                                                               part_ids,
                                                               dtc_params,
                                                               true))) {
    LOG_WARN("Failed to initialize table location", K(ret));
  } else if (OB_FAIL(table_partition_info.calc_phy_table_loc_and_select_leader(*exec_ctx,
                                                                                *params,
                                                                                dtc_params))) {
    //对于insert而言，计算出来的partition顺序保持跟value row对应，不应该重排序
    LOG_WARN("failed to calculate table location", K(ret));
  } else {
    LOG_TRACE("succeed to compute table location", K(table_partition_info), K(filters));
  }
  return ret;
}

int ObDelUpdLogPlan::compute_exchange_info_for_pdml_del_upd(const ObShardingInfo &source_sharding,
                                                            const ObTablePartitionInfo &target_table_partition,
                                                            const IndexDMLInfo &index_dml_info,
                                                            bool is_index_maintenance,
                                                            ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  exch_info.server_list_.reuse();
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(session = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(session), K(ret));
  } else if (OB_FAIL(exch_info.repartition_keys_.assign(source_sharding.get_partition_keys()))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(exch_info.repartition_sub_keys_.assign(source_sharding.get_sub_partition_keys()))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(exch_info.repartition_func_exprs_.assign(source_sharding.get_partition_func()))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(compute_hash_dist_exprs_for_pdml_del_upd(exch_info, index_dml_info))) {
    LOG_WARN("failed to compute pdml hash dist exprs", K(ret));
  } else if (OB_FAIL(target_table_partition.get_all_servers(exch_info.server_list_))) {
    LOG_WARN("failed to get all servers", K(ret));
  } else if (OB_FAIL(get_pdml_parallel_degree(target_table_partition.get_phy_tbl_location_info().get_partition_cnt(),
                                              exch_info.parallel_))) {
    LOG_WARN("failed to get pdml parallel degree", K(ret));
  } else {
    exch_info.server_cnt_ = exch_info.server_list_.count();
    share::schema::ObPartitionLevel part_level = source_sharding.get_part_level();
    if (share::schema::PARTITION_LEVEL_ZERO != part_level) {
      exch_info.slice_count_ = source_sharding.get_part_cnt();
      exch_info.repartition_ref_table_id_ = index_dml_info.ref_table_id_;
      exch_info.repartition_table_id_ = index_dml_info.loc_table_id_;
      exch_info.repartition_table_name_ = index_dml_info.index_name_;
    }
    if (share::schema::PARTITION_LEVEL_ONE == part_level) {
      exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL;
      if (!get_stmt()->is_merge_stmt() &&
          get_optimizer_context().is_pdml_heap_table() && !is_index_maintenance) {
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_RANDOM;
      } else {
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_HASH;
      }
      LOG_TRACE("partition level is one, use pkey reshuffle method");
    } else if (share::schema::PARTITION_LEVEL_TWO == part_level) {
      exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_TWO_LEVEL;
      if (!get_stmt()->is_merge_stmt() &&
          get_optimizer_context().is_pdml_heap_table() && !is_index_maintenance) {
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_RANDOM;
      } else {
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_HASH;
      }
      LOG_TRACE("partition level is two, use pkey reshuffle method");
    } else if (share::schema::PARTITION_LEVEL_ZERO == part_level) {
      exch_info.repartition_type_ = OB_REPARTITION_NO_REPARTITION;
      if (!get_stmt()->is_merge_stmt() &&
          get_optimizer_context().is_pdml_heap_table() && !is_index_maintenance) {
        exch_info.dist_method_ = ObPQDistributeMethod::RANDOM;
      } else {
        exch_info.dist_method_ = ObPQDistributeMethod::HASH;
      }
      LOG_TRACE("partition level is zero, use reduce reshuffle method");
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected partition level", K(part_level) ,K(ret));
    }

    // 为static engine 初始化calc part id exprs的内容
    if (OB_SUCC(ret) &&
        (part_level == share::schema::PARTITION_LEVEL_ONE ||
         part_level == share::schema::PARTITION_LEVEL_TWO)) {
      ObRawExpr *part_id_expr = NULL;
      if (OB_FAIL(gen_calc_part_id_expr(index_dml_info.loc_table_id_,
                                        index_dml_info.ref_table_id_,
                                        CALC_TABLET_ID,
                                        part_id_expr))) {
        LOG_WARN("failed to gen calc part id expr", K(ret));
      } else if (OB_ISNULL(part_id_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null part expr", K(ret));
      } else {
        part_id_expr->set_may_add_interval_part(MayAddIntervalPart::YES);
        exch_info.may_add_interval_part_ = MayAddIntervalPart::YES;
        exch_info.set_calc_part_id_expr(part_id_expr);
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::compute_hash_dist_exprs_for_pdml_del_upd(ObExchangeInfo &exch_info,
                                                              const IndexDMLInfo &dml_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> rowkey_exprs;
  if (OB_UNLIKELY(dml_info.get_real_uk_cnt() > dml_info.column_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", K(dml_info.get_real_uk_cnt()), K(dml_info.column_exprs_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dml_info.get_real_uk_cnt(); ++i) {
      // 为了让主键（new value）相同的行交给同一个线程处理，需要对 new value 做 hash
      // 对于 update：如果目标表的列被更新了
      // 那么就选择更新后的值，否则选择更新前的值。
      // 对于 delete，因为 assignment 为空，所以会直接 push rowkey
      ObRawExpr *target_expr = dml_info.column_exprs_.at(i);
      if (OB_FAIL(replace_assignment_expr_from_dml_info(dml_info, target_expr))) {
        LOG_WARN("failed to replace assignment expr", K(ret));
      } else if (OB_FAIL(rowkey_exprs.push_back(target_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      ret = exch_info.append_hash_dist_expr(rowkey_exprs);
    }
  }
  return ret;
}

int ObDelUpdLogPlan::compute_exchange_info_for_pdml_insert(const ObShardingInfo &target_sharding,
                                                           const ObTablePartitionInfo &target_table_partition,
                                                           const IndexDMLInfo &index_dml_info,
                                                           bool is_index_maintenance,
                                                           ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  exch_info.server_list_.reuse();
  if (OB_ISNULL(get_stmt()) ||
      OB_ISNULL(session = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session), K(ret));
  } else if (share::schema::PARTITION_LEVEL_ZERO != target_sharding.get_part_level() &&
             OB_FAIL(compute_repartition_info_for_pdml_insert(
                             index_dml_info,
                             target_sharding,
                             get_optimizer_context().get_expr_factory(),
                             exch_info))) {
    LOG_WARN("failed to compute repartition func info", K(ret));
  } else if (OB_FAIL(target_table_partition.get_all_servers(exch_info.server_list_))) {
    LOG_WARN("failed to get all servers", K(ret));
  } else if (OB_FAIL(get_pdml_parallel_degree(target_table_partition.get_phy_tbl_location_info().get_partition_cnt(),
                                              exch_info.parallel_))) {
    LOG_WARN("failed to get pdml parallel degree", K(ret));
  } else {
    exch_info.server_cnt_ = exch_info.server_list_.count();
    share::schema::ObPartitionLevel part_level = target_sharding.get_part_level();
    if (share::schema::PARTITION_LEVEL_ZERO != part_level) {
      exch_info.repartition_ref_table_id_ = index_dml_info.ref_table_id_;
      exch_info.repartition_table_id_ = index_dml_info.loc_table_id_;
      exch_info.repartition_table_name_ = index_dml_info.index_name_;
      exch_info.slice_count_ = target_sharding.get_part_cnt();
    }
    if (share::schema::PARTITION_LEVEL_ONE == part_level) {
      // pdml op对应的表是分区表，分区内并行处理，使用pkey random shuffle方式
      exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL;
      if ((!get_stmt()->is_merge_stmt() || 
           !static_cast<const ObMergeStmt*>(get_stmt())->has_update_clause()) &&
           ((get_optimizer_context().is_online_ddl() && get_optimizer_context().is_heap_table_ddl())
           || (get_optimizer_context().is_pdml_heap_table() && !is_index_maintenance))) {
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_RANDOM;
      } else if (get_optimizer_context().is_online_ddl()) {
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_RANGE;
      } else {
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_HASH;
      }
    } else if (share::schema::PARTITION_LEVEL_TWO == part_level) {
      // pdml op对应的表是分区表，分区内并行处理，使用pkey random shuffle方式
      exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_TWO_LEVEL;
      if ((!get_stmt()->is_merge_stmt() || 
           !static_cast<const ObMergeStmt*>(get_stmt())->has_update_clause()) &&
           ((get_optimizer_context().is_online_ddl() && get_optimizer_context().is_heap_table_ddl())
           || (get_optimizer_context().is_pdml_heap_table() && !is_index_maintenance))) {
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_RANDOM;
      } else if (get_optimizer_context().is_online_ddl()) {
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_RANGE;
      } else {
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_HASH;
      }
    } else if (share::schema::PARTITION_LEVEL_ZERO == part_level) {
      // pdml op对应的表是非分区表，分区内并行处理，使用random shuffle方式
      exch_info.repartition_type_ = OB_REPARTITION_NO_REPARTITION;
      if ((!get_stmt()->is_merge_stmt() || 
           !static_cast<const ObMergeStmt*>(get_stmt())->has_update_clause()) &&
          ((get_optimizer_context().is_online_ddl() && get_optimizer_context().is_heap_table_ddl())
           || (get_optimizer_context().is_pdml_heap_table() && !is_index_maintenance))) {
        exch_info.dist_method_ = ObPQDistributeMethod::RANDOM;
      } else if (get_optimizer_context().is_online_ddl()) {
        exch_info.dist_method_ = ObPQDistributeMethod::RANGE;
      } else {
        exch_info.dist_method_ = ObPQDistributeMethod::HASH;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected partition level", K(target_sharding.get_part_level()));
    }
    if (OB_SUCC(ret)) {
      if (get_optimizer_context().is_online_ddl() && !get_optimizer_context().is_heap_table_ddl()) {
        int64_t sample_sort_column_count = 0;
        ObArray<OrderItem> ddl_sort_keys;
        if (OB_UNLIKELY(!get_stmt()->is_insert_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(get_stmt()), K(ret));
        } else if (OB_FAIL(static_cast<const ObInsertStmt*>(get_stmt())->get_ddl_sort_keys(ddl_sort_keys))) {
          LOG_WARN("fail to get ddl sort key", K(ret));
        } else if (OB_FAIL(get_ddl_sample_sort_column_count(sample_sort_column_count))) {
          LOG_WARN("get ddl sample sort column count failed", K(ret));
        } else if (sample_sort_column_count > ddl_sort_keys.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sample sort column count is larger than ddl sort keys", K(ret), K(ddl_sort_keys.count()), K(sample_sort_column_count));
        } else {
          exch_info.sort_keys_.reset();
          if (sample_sort_column_count > 0) {
            if (OB_FAIL(exch_info.sort_keys_.reserve(sample_sort_column_count))) {
              LOG_WARN("reserve sort keys array failed", K(ret), K(sample_sort_column_count));
            }
            for (int64_t i = 0; OB_SUCC(ret) && i < sample_sort_column_count; ++i) {
              if (OB_FAIL(exch_info.sort_keys_.push_back(ddl_sort_keys.at(i)))) {
                LOG_WARN("push back sort item failed", K(ret), K(i), K(sample_sort_column_count));
              }
            }
          } else if (OB_FAIL(exch_info.sort_keys_.assign(ddl_sort_keys))) {
            LOG_WARN("assign ddl sort keys failed", K(ret), K(ddl_sort_keys.count()));
          }
          if (OB_SUCC(ret)) {
            if (exch_info.dist_method_ == ObPQDistributeMethod::PARTITION_RANGE &&
                OB_FAIL(exch_info.repart_all_tablet_ids_.assign(target_sharding.get_all_tablet_ids()))) {
              LOG_WARN("failed to get all partition ids", K(ret));
            } else { /*do nothing*/ }
          }
        }
      } else {
        if (OB_FAIL(compute_hash_dist_exprs_for_pdml_insert(exch_info,
                                                            index_dml_info))) {
          LOG_WARN("failed to compute hash dist for pdml insert", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::compute_repartition_info_for_pdml_insert(const IndexDMLInfo &index_dml_info,
                                                              const ObShardingInfo &target_sharding,
                                                              ObRawExprFactory &expr_factory,
                                                              ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt;
  ObSEArray<ObRawExpr*, 4> case_when_exprs;
  ObSEArray<ObRawExpr*, 4> repart_exprs;
  ObSEArray<ObRawExpr*, 4> repart_sub_exprs;
  ObSEArray<ObRawExpr*, 4> repart_func_exprs;
  ObRawExprCopier copier(expr_factory);
  ObRawExpr *part_id_expr = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_UNLIKELY(index_dml_info.column_convert_exprs_.count() !=
                  index_dml_info.column_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column count", K(index_dml_info.column_convert_exprs_.count()),
                                            K(index_dml_info.column_exprs_.count()), K(ret));
  } else if (stmt->is_merge_stmt() &&
             static_cast<const ObMergeStmt*>(stmt)->has_update_clause()) {
    if (OB_FAIL(build_merge_stmt_repartition_hash_key(index_dml_info,
                                                      expr_factory,
                                                      case_when_exprs))) {
      LOG_WARN("failed to build merge stmtment repartition hash key", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr((const ObIArray<ObRawExpr *> &)index_dml_info.column_exprs_,
                                                case_when_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
  } else {
    if (OB_FAIL(copier.add_replaced_expr((const ObIArray<ObRawExpr *> &)index_dml_info.column_exprs_,
                                        index_dml_info.column_convert_exprs_))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } 
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(copier.copy_on_replace(target_sharding.get_partition_keys(),
                                            repart_exprs))) {
    LOG_WARN("failed to generate repart exprs", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(target_sharding.get_sub_partition_keys(),
                                            repart_sub_exprs))) {
    LOG_WARN("failed to generate subrepart exprs", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(target_sharding.get_partition_func(),
                                            repart_func_exprs))) {
    LOG_WARN("failed to generate repart func exprs", K(ret));
  } else if (OB_FAIL(exch_info.repartition_keys_.assign(repart_exprs)) ||
             OB_FAIL(exch_info.repartition_sub_keys_.assign(repart_sub_exprs)) ||
             OB_FAIL(exch_info.repartition_func_exprs_.assign(repart_func_exprs))) {
    LOG_WARN("failed to set repartition info", K(ret));
  } else if (OB_FAIL(gen_calc_part_id_expr(index_dml_info.loc_table_id_,
                                           index_dml_info.ref_table_id_,
                                           CALC_TABLET_ID,
                                           part_id_expr))) {
    LOG_WARN("failed to gen calc part id expr", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(part_id_expr,
                                            part_id_expr))) {
    LOG_WARN("failed to generate repart exprs", K(ret));
  } else if (OB_ISNULL(part_id_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null part expr", K(ret));
  } else {
    part_id_expr->set_may_add_interval_part(MayAddIntervalPart::YES);
    exch_info.may_add_interval_part_ = MayAddIntervalPart::YES;
    exch_info.set_calc_part_id_expr(part_id_expr);
  }
  return ret;
}

int ObDelUpdLogPlan::build_merge_stmt_repartition_hash_key(const IndexDMLInfo &index_dml_info,
                                                           ObRawExprFactory &expr_factory,
                                                           ObIArray<ObRawExpr*> &case_when_exprs)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt;
  ObRawExpr* when_expr = NULL;
  JoinedTable* joined_table = NULL;
  // merge stmt add case when expr
  // hash key => case when (condition expr) then (source hash key) else (target hash key)
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_UNLIKELY(stmt->get_from_item_size() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected from item size", K(ret));
  } else if (OB_ISNULL(joined_table = stmt->get_joined_table(stmt->get_from_item(0).table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get joined table", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(expr_factory, 
                                                    joined_table->get_join_conditions(),
                                                    when_expr))) {
    LOG_WARN("failed to build and expr", K(ret));
  } else if (OB_ISNULL(when_expr)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.column_exprs_.count(); ++i) {
      if (OB_FAIL(case_when_exprs.push_back(index_dml_info.column_exprs_.at(i)))) {
        LOG_WARN("failed to push bash case when exprs");
      }
    }
  } else if (OB_FAIL(when_expr->formalize(get_optimizer_context().get_session_info()))) {
    LOG_WARN("failed to formalize when expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.column_exprs_.count(); ++i) {
      ObRawExpr* column_expr = NULL;
      ObRawExpr* column_convert_expr = NULL;
      if (OB_ISNULL(column_expr = index_dml_info.column_exprs_.at(i)) ||
          OB_ISNULL(column_convert_expr = index_dml_info.column_convert_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory,
                                                              when_expr,
                                                              column_expr,
                                                              column_convert_expr,
                                                              column_expr))) {
        LOG_WARN("failed to build case when expr", K(ret));
      } else if (OB_FAIL(column_expr->formalize(get_optimizer_context().get_session_info()))) {
        LOG_WARN("failed to formalize case when expr", K(ret));
      } else if (OB_FAIL(case_when_exprs.push_back(column_expr))) {
        LOG_WARN("failed to push bash case when exprs");
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::compute_hash_dist_exprs_for_pdml_insert(ObExchangeInfo &exch_info,
                                                             const IndexDMLInfo &dml_info)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt;
  ObSEArray<ObRawExpr *, 8> rowkey_exprs;
  if (OB_ISNULL(stmt = get_stmt()) ||
      OB_UNLIKELY(dml_info.get_real_uk_cnt() > dml_info.column_convert_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(dml_info.get_real_uk_cnt()),
             K(dml_info.column_convert_exprs_.count()), K(ret));
  } else if (stmt->is_merge_stmt()) {
    if (OB_FAIL(build_merge_stmt_hash_dist_exprs(dml_info,
                                                 rowkey_exprs))) {
      LOG_WARN("failed to build merge stmt hash dist exprs", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dml_info.get_real_uk_cnt(); i++) {
      ObRawExpr *raw_expr = NULL;
      if (OB_ISNULL(raw_expr = dml_info.column_convert_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(rowkey_exprs.push_back(raw_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
    
  }
  if (OB_SUCC(ret) &&
      OB_FAIL(exch_info.append_hash_dist_expr(rowkey_exprs))) {
    LOG_WARN("failed to append hash dist expr to exch info", K(ret));
  }
  return ret;
}

int ObDelUpdLogPlan::replace_assignment_expr_from_dml_info(const IndexDMLInfo &dml_info,
                                                           ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < dml_info.assignments_.count(); ++j) {
    const ObAssignment &assignment = dml_info.assignments_.at(j);
    if (OB_ISNULL(assignment.expr_) || OB_ISNULL(assignment.column_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (expr == assignment.column_expr_
               && OB_NOT_NULL(assignment.expr_)
               && assignment.expr_->get_expr_type() != T_TABLET_AUTOINC_NEXTVAL) {
      expr = assignment.expr_;
      break;
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObDelUpdLogPlan::build_merge_stmt_hash_dist_exprs(const IndexDMLInfo &dml_info,
                                                      ObIArray<ObRawExpr*> &rowkey_exprs)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt;
  ObRawExpr* when_expr = NULL;
  ObConstRawExpr* const_expr = NULL;
  JoinedTable* joined_table = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(stmt->get_from_item_size() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected from item size", K(ret));
  } else if (OB_ISNULL(joined_table = stmt->get_joined_table(stmt->get_from_item(0).table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get joined table", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(get_optimizer_context().get_expr_factory(), 
                                                    joined_table->get_join_conditions(),
                                                    when_expr))) {
    LOG_WARN("failed to build and expr", K(ret));
  } else if (OB_ISNULL(when_expr)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < dml_info.get_real_uk_cnt(); i++) {
      ObRawExpr *target_expr = dml_info.column_exprs_.at(i);
      if (OB_FAIL(replace_assignment_expr_from_dml_info(dml_info, target_expr))) {
        LOG_WARN("failed to replace assignment expr", K(ret));
      } else if (OB_FAIL(rowkey_exprs.push_back(target_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
  } else if (OB_FAIL(when_expr->formalize(get_optimizer_context().get_session_info()))) {
    LOG_WARN("failed to formalize when expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(get_optimizer_context().get_expr_factory(), 
                                                          ObUInt64Type,
                                                          1, 
                                                          const_expr))) {
    LOG_WARN("failed to build const number expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dml_info.get_real_uk_cnt(); i++) {
      ObRawExpr *raw_expr = NULL;
      ObRawExpr *target_expr = dml_info.column_exprs_.at(i);
      if (OB_FAIL(replace_assignment_expr_from_dml_info(dml_info, target_expr))) {
        LOG_WARN("failed to replace assignment expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(get_optimizer_context().get_expr_factory(),
                                                              when_expr,
                                                              target_expr,
                                                              get_optimizer_context().is_pdml_heap_table() ? 
                                                                const_expr : dml_info.column_convert_exprs_.at(i),
                                                              raw_expr))) {
        LOG_WARN("failed to build case when expr", K(ret));
      } else if (OB_FAIL(raw_expr->formalize(get_optimizer_context().get_session_info()))) {
        LOG_WARN("failed to formalize case when expr", K(ret));
      } else if (OB_FAIL(rowkey_exprs.push_back(raw_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

/*
 * need barrier: row-movement update, delete should be processed before insert
 */
int ObDelUpdLogPlan::candi_allocate_one_pdml_delete(bool is_index_maintain,
                                                    bool is_last_dml_op,
                                                    bool is_pdml_update_split,
                                                    IndexDMLInfo *index_dml_info)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  ObSEArray<ObRawExpr*, 1> dummy_filters;
  ObShardingInfo *source_sharding = NULL;
  ObTablePartitionInfo *source_table_partition = NULL;
  ObSEArray<CandidatePlan, 8> best_plans;
  ObSEArray<IndexDMLInfo *, 1> tmp;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(index_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(index_dml_info), K(ret));
  } else if (OB_FAIL(tmp.push_back(index_dml_info))) {
    LOG_WARN("failed to push back index dml info", K(ret));
  } else if (OB_FAIL(check_table_rowkey_distinct(tmp))) {
    LOG_WARN("failed to check table rowkey distinct", K(ret));
  } else if (OB_FAIL(calculate_table_location_and_sharding(
                     *get_stmt(),
                     is_index_maintain ? dummy_filters : get_stmt()->get_condition_exprs(),
                     index_dml_info->loc_table_id_,
                     index_dml_info->ref_table_id_,
                     NULL,
                     source_sharding,
                     source_table_partition))) {
    LOG_WARN("failed to calculate table location and sharding", K(ret));
  } else if (OB_ISNULL(source_sharding) || OB_ISNULL(source_table_partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_sharding), K(source_table_partition), K(ret));
  } else if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_,
                                                 best_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else if (OB_FAIL(compute_exchange_info_for_pdml_del_upd(*source_sharding,
                                                            *source_table_partition,
                                                            *index_dml_info,
                                                            is_index_maintain,
                                                            exch_info))) {
    LOG_WARN("failed to compute pdml exchange info for delete/update operator", K(ret));
  } else {
    bool need_partition_id = source_sharding->get_part_level() == share::schema::PARTITION_LEVEL_ONE ||
                        source_sharding->get_part_level() == share::schema::PARTITION_LEVEL_TWO;
    for (int64_t i = 0; OB_SUCC(ret) && i < best_plans.count(); i++) {
      if (OB_FAIL(create_pdml_delete_plan(best_plans.at(i).plan_tree_,
                                          exch_info,
                                          source_table_partition,
                                          is_index_maintain,
                                          is_last_dml_op,
                                          need_partition_id,
                                          is_pdml_update_split,
                                          index_dml_info))) {
        LOG_WARN("failed to create delete plan", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prune_and_keep_best_plans(best_plans))) {
        LOG_WARN("failed to prune and keep best plans", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::create_pdml_delete_plan(ObLogicalOperator *&top,
                                             const ObExchangeInfo &exch_info,
                                             ObTablePartitionInfo *source_table_partition,
                                             bool is_index_maintenance,
                                             bool is_last_dml_op,
                                             bool need_partition_id,
                                             bool is_pdml_update_split,
                                             IndexDMLInfo *index_dml_info)
{
  int ret = OB_SUCCESS;
  bool need_exchange = true;
  if (OB_ISNULL(source_table_partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_table_partition), K(ret));
  } else if (!is_index_maintenance &&
             OB_FAIL(check_need_exchange_for_pdml_del_upd(top, exch_info,
                                                          source_table_partition->get_table_id(),
                                                          need_exchange))) {
    LOG_WARN("failed to check whether pdml need exchange for del upd", K(ret));
  } else if (need_exchange && OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
    LOG_WARN("failed to allocate exchange as top", K(ret));
  } else if (OB_FAIL(allocate_pdml_delete_as_top(top,
                                                 is_index_maintenance,
                                                 is_last_dml_op,
                                                 need_partition_id,
                                                 is_pdml_update_split,
                                                 source_table_partition,
                                                 index_dml_info))) {
    LOG_WARN("failed to allocate pdml delete as top", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObDelUpdLogPlan::allocate_pdml_delete_as_top(ObLogicalOperator *&top,
                                                 bool is_index_maintain,
                                                 bool is_last_dml_op,
                                                 bool need_partition_id,
                                                 bool is_pdml_update_split,
                                                 ObTablePartitionInfo *table_partition_info,
                                                 IndexDMLInfo *index_dml_info)
{
  int ret = OB_SUCCESS;
  ObLogDelete *delete_op = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table_partition_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(get_stmt()),
        K(table_partition_info), K(ret));
  } else if (OB_ISNULL(delete_op = static_cast<ObLogDelete*>(
                                   get_log_op_factory().allocate(*this, LOG_DELETE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate delete operator", K(ret));
  } else if (OB_FAIL(delete_op->get_index_dml_infos().push_back(index_dml_info))) {
    LOG_WARN("failed to add index dml info", K(ret));
  } else {
    delete_op->set_is_pdml(true);
    if (is_last_dml_op && get_stmt()->is_delete_stmt()) {
      delete_op->set_pdml_is_returning(get_stmt()->is_returning());
      delete_op->set_is_returning(get_stmt()->is_returning());
    } else {
      delete_op->set_pdml_is_returning(true);
    }
    if (get_stmt()->is_update_stmt()) {
      delete_op->set_need_barrier(true);
    }
    delete_op->set_first_dml_op(!is_index_maintain);
    delete_op->set_index_maintenance(is_index_maintain);
    delete_op->set_table_partition_info(table_partition_info);
    delete_op->set_need_allocate_partition_id_expr(need_partition_id);
    delete_op->set_pdml_update_split(is_pdml_update_split);
    delete_op->set_child(ObLogicalOperator::first_child, top);
    if (OB_FAIL(delete_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = delete_op;
    }
  }
  return ret;
}

int ObDelUpdLogPlan::candi_allocate_one_pdml_insert(bool is_index_maintenance,
                                                    bool is_last_dml_op,
                                                    bool is_pdml_update_split,
                                                    IndexDMLInfo *index_dml_info,
                                                    OSGShareInfo *osg_info /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  ObSEArray<OrderItem, 8> sort_keys;
  ObSEArray<OrderItem, 8> sample_sort_keys;
  ObSEArray<OrderItem, 8> px_coord_sort_keys;
  ObSEArray<ObRawExpr*, 1> sharding_conditions;
  ObShardingInfo *target_sharding = NULL;
  ObTablePartitionInfo *target_table_partition = NULL;
  ObSEArray<CandidatePlan, 8> best_plans;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(index_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(index_dml_info), K(ret));
  } else {
    if (get_stmt()->is_insert_stmt()) {
      const ObInsertStmt *insert_stmt = static_cast<const ObInsertStmt*>(get_stmt());
      if (!is_index_maintenance && OB_FAIL(sharding_conditions.assign(
                                            insert_stmt->get_sharding_conditions()))) {
        LOG_WARN("failed to assign sharding conditions", K(ret));
      }
    }
    if (OB_FAIL(calculate_table_location_and_sharding(
                       *get_stmt(),
                       sharding_conditions,
                       index_dml_info->loc_table_id_,
                       index_dml_info->ref_table_id_,
                       get_stmt()->is_insert_stmt() ? &index_dml_info->part_ids_ : NULL,
                       target_sharding,
                       target_table_partition))) {
      LOG_WARN("failed to calculate table location and sharding", K(ret), KPC(index_dml_info));
    } else if (OB_ISNULL(target_sharding) || OB_ISNULL(target_table_partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(target_sharding), K(target_table_partition), K(ret));
    } else if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_,
                                                   best_plans))) {
      LOG_WARN("failed to get minimal cost candidates", K(ret));
    } else if (OB_FAIL(compute_exchange_info_for_pdml_insert(*target_sharding,
                                                             *target_table_partition,
                                                             *index_dml_info,
                                                             is_index_maintenance,
                                                             exch_info))) {
      LOG_WARN("failed to compute exchange info for insert", K(ret));
    } else if (get_optimizer_context().is_online_ddl() && !get_optimizer_context().is_heap_table_ddl() &&
               OB_FAIL(get_ddl_sort_keys_with_part_expr(exch_info, sort_keys, sample_sort_keys))) {
      LOG_WARN("failed to get ddl sort keys", K(ret));
    } else if (!sample_sort_keys.empty() && OB_FAIL(gen_px_coord_sampling_sort_keys(
                sample_sort_keys, px_coord_sort_keys))) {
      LOG_WARN("generate px coord sort order items failed", K(ret));
    } else {
      bool need_partition_id = target_sharding->get_part_level() == share::schema::PARTITION_LEVEL_ONE ||
                          target_sharding->get_part_level() == share::schema::PARTITION_LEVEL_TWO;
      for (int64_t i = 0; OB_SUCC(ret) && i < best_plans.count(); i++) {
        if (get_optimizer_context().is_online_ddl()) {
          exch_info.sample_type_ = OBJECT_SAMPLE;
          if (OB_FAIL(create_online_ddl_plan(best_plans.at(i).plan_tree_,
                                             exch_info,
                                             target_table_partition,
                                             sort_keys,
                                             sample_sort_keys,
                                             px_coord_sort_keys,
                                             is_index_maintenance,
                                             is_last_dml_op,
                                             need_partition_id,
                                             is_pdml_update_split,
                                             index_dml_info))) {
            LOG_WARN("failed to create online ddl plan", K(ret));
          } else { /*do nothing*/ }
        } else {
          if (OB_FAIL(create_pdml_insert_plan(best_plans.at(i).plan_tree_,
                                              exch_info,
                                              target_table_partition,
                                              is_index_maintenance,
                                              is_last_dml_op,
                                              need_partition_id,
                                              is_pdml_update_split,
                                              index_dml_info,
                                              osg_info))) {
            LOG_WARN("failed to create delete plan", K(ret));
          } else { /*do nothing*/ }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(prune_and_keep_best_plans(best_plans))) {
          LOG_WARN("failed to prune and keep best plans", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::create_pdml_insert_plan(ObLogicalOperator *&top,
                                             const ObExchangeInfo &exch_info,
                                             ObTablePartitionInfo *table_partition_info,
                                             bool is_index_maintenance,
                                             bool is_last_dml_op,
                                             bool need_partition_id,
                                             bool is_pdml_update_split,
                                             IndexDMLInfo *index_dml_info,
                                             OSGShareInfo *osg_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(top) || OB_ISNULL(table_partition_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(table_partition_info), K(ret));
  } else if (OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
    LOG_WARN("failed to allocate exchange as top", K(ret));
  } else if (osg_info != NULL &&
             OB_FAIL(allocate_optimizer_stats_gathering_as_top(top, *osg_info))) {
    LOG_WARN("failed to allocate optimizer stats gathering");
  } else if (OB_FAIL(allocate_pdml_insert_as_top(top,
                                                 is_index_maintenance,
                                                 is_last_dml_op,
                                                 need_partition_id,
                                                 is_pdml_update_split,
                                                 table_partition_info,
                                                 index_dml_info))) {
    LOG_WARN("failed to allocate pdml insert as top", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObDelUpdLogPlan::allocate_optimizer_stats_gathering_as_top(ObLogicalOperator *&old_top,
                                                               OSGShareInfo &info)
{
  int ret = OB_SUCCESS;
  ObLogOptimizerStatsGathering *osg = NULL;
  if (OB_ISNULL(old_top)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Get unexpected null", K(ret), K(old_top));
  } else if (OB_ISNULL(osg = static_cast<ObLogOptimizerStatsGathering *>(get_log_op_factory().
                                        allocate(*this, LOG_OPTIMIZER_STATS_GATHERING)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate sequence operator", K(ret));
  } else {
    OSG_TYPE type = old_top->need_osg_merge() ? OSG_TYPE::MERGE_OSG :
                    old_top->is_distributed() ? OSG_TYPE::GATHER_OSG : OSG_TYPE::NORMAL_OSG;
    osg->set_child(ObLogicalOperator::first_child, old_top);
    osg->set_osg_type(type);
    osg->set_table_id(info.table_id_);
    osg->set_part_level(info.part_level_);
    osg->set_generated_column_exprs(info.generated_column_exprs_);
    osg->set_col_conv_exprs(info.col_conv_exprs_);
    osg->set_column_ids(info.column_ids_);
    if (type == OSG_TYPE::GATHER_OSG) {
      osg->set_need_osg_merge(true);
    }
    if (type != OSG_TYPE::MERGE_OSG && info.part_level_ != share::schema::PARTITION_LEVEL_ZERO) {
      osg->set_calc_part_id_expr(info.calc_part_id_expr_);
    }
    if (OB_FAIL(osg->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      old_top = osg;
    }
  }
  return ret;
}

int ObDelUpdLogPlan::create_online_ddl_plan(ObLogicalOperator *&top,
                                            const ObExchangeInfo &exch_info,
                                            ObTablePartitionInfo *table_partition_info,
                                            const ObIArray<OrderItem> &sort_keys,
                                            const ObIArray<OrderItem> &sample_sort_keys,
                                            const ObIArray<OrderItem> &px_coord_sort_keys,
                                            bool is_index_maintenance,
                                            bool is_last_dml_op,
                                            bool need_partition_id,
                                            bool is_pdml_update_split,
                                            IndexDMLInfo *index_dml_info)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo final_exch_info;
  if (OB_ISNULL(top) || OB_ISNULL(table_partition_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(table_partition_info), K(ret));
  } else if (get_optimizer_context().is_heap_table_ddl()) {
    if (OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_FAIL(allocate_pdml_insert_as_top(top,
                                                  is_index_maintenance,
                                                  is_last_dml_op,
                                                  need_partition_id,
                                                  is_pdml_update_split,
                                                  table_partition_info,
                                                  index_dml_info))) {
      LOG_WARN("failed to allocate pdml insert as top", K(ret));
    } else if (OB_FAIL(allocate_exchange_as_top(top, final_exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    }
  } else {
    if (OB_FAIL(allocate_stat_collector_as_top(top,
        ObStatCollectorType::SAMPLE_SORT, sample_sort_keys,
        table_partition_info->get_part_level()))) {
      LOG_WARN("fail to allocate stat collector as top", K(ret));
    } else if (OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_FAIL(allocate_sort_as_top(top, sort_keys))) {
      LOG_WARN("failed to allocate sort as top", K(ret));
    } else if (OB_FAIL(allocate_pdml_insert_as_top(top,
                                                   is_index_maintenance,
                                                   is_last_dml_op,
                                                   need_partition_id,
                                                   is_pdml_update_split,
                                                   table_partition_info,
                                                   index_dml_info))) {
      LOG_WARN("failed to allocate pdml insert as top", K(ret));
    } else if (OB_FAIL(allocate_exchange_as_top(top, final_exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_ISNULL(top) || OB_UNLIKELY(LOG_EXCHANGE != top->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(top), K(ret));
    } else if (OB_FAIL(static_cast<ObLogExchange*>(top)->get_sort_keys().assign(
                px_coord_sort_keys))) {
      LOG_WARN("failed to assign sort keys", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObDelUpdLogPlan::get_ddl_sample_sort_column_count(int64_t &sample_sort_column_count)
{
  int ret = OB_SUCCESS;
  sample_sort_column_count = 0;
  const ObInsertStmt *ins_stmt = static_cast<const ObInsertStmt *>(get_stmt());
  if (OB_ISNULL(ins_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert stmt is null", K(ret), KP(ins_stmt));
  } else if (2 != ins_stmt->get_table_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, table item size is not as expected", K(ret), "table_item_size", ins_stmt->get_table_size());
  } else {
    TableItem* table_item = ins_stmt->get_table_item_by_id(ins_stmt->get_insert_table_info().table_id_);
    const uint64_t tenant_id = optimizer_context_.get_session_info()->get_effective_tenant_id();
    ObSchemaGetterGuard *schema_guard = nullptr;
    const ObTableSchema *table_schema = nullptr;
    if (OB_ISNULL(schema_guard = optimizer_context_.get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_guard is null", K(ret), KP(schema_guard));
    } else if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item of insert stmt is null", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id, table_item->ddl_table_id_, table_schema))) {
      LOG_WARN("get target table schema failed", K(ret), K(tenant_id), KPC(table_item));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("target table not exist", K(ret), K(tenant_id), KPC(table_item));
    } else if (table_schema->is_unique_index()) {
      sample_sort_column_count = table_schema->get_index_column_num();
    }
  }
  return ret;
}

int ObDelUpdLogPlan::get_ddl_sort_keys_with_part_expr(ObExchangeInfo &exch_info,
                                                      common::ObIArray<OrderItem> &sort_keys,
                                                      common::ObIArray<OrderItem> &sample_sort_keys)
{
  int ret = OB_SUCCESS;
  sort_keys.reset();
  sample_sort_keys.reset();
  ObArray<OrderItem> tmp_sort_keys;
  const ObInsertStmt *ins_stmt = static_cast<const ObInsertStmt *>(get_stmt());
  int64_t sample_sort_column_count = 0;
  if (2 != ins_stmt->get_table_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, table item size is not as expected", K(ret), "table_item_size", ins_stmt->get_table_size());
  } else if (OB_FAIL(ins_stmt->get_ddl_sort_keys(tmp_sort_keys))) {
    LOG_WARN("get ddl sort keys failed", K(ret));
  } else if (OB_FAIL(get_ddl_sample_sort_column_count(sample_sort_column_count))) {
    LOG_WARN("get ddl sample sort column count failed", K(ret));
  } else if (OB_NOT_NULL(exch_info.calc_part_id_expr_)) {
    OrderItem item;
    item.expr_ = exch_info.calc_part_id_expr_;
    item.order_type_ = NULLS_FIRST_ASC;
    if (OB_FAIL(sort_keys.push_back(item))) {
      LOG_WARN("push back sort keys with part failed", K(ret));
    } else if (OB_FAIL(sample_sort_keys.push_back(item))) {
      LOG_WARN("push back sample sort keys with part failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_sort_keys.count(); ++i) {
    if (OB_FAIL(sort_keys.push_back(tmp_sort_keys.at(i)))) {
      LOG_WARN("push back sort keys failed", K(ret));
    } else if (sample_sort_column_count > 0 && i >= sample_sort_column_count) {
      // skip
    } else if (OB_FAIL(sample_sort_keys.push_back(tmp_sort_keys.at(i)))) {
      LOG_WARN("push back sample sort keys failed", K(ret));
    }
  }
  LOG_INFO("get ddl sort keys and sample sort keys", K(ret), K(sort_keys.count()), K(sample_sort_keys.count()), K(sample_sort_column_count));
  return ret;
}

int ObDelUpdLogPlan::allocate_pdml_insert_as_top(ObLogicalOperator *&top,
                                                 bool is_index_maintenance,
                                                 bool is_last_dml_op,
                                                 bool need_partition_id,
                                                 bool is_pdml_update_split,
                                                 ObTablePartitionInfo *table_partition_info,
                                                 IndexDMLInfo *dml_info)
{
  int ret = OB_SUCCESS;
  // 从 table_columns 中拿到 index_dml_infos_，里面包含了这个索引中的所有列
  // 这些列数据类型都是严格类型，无需在外面包装 conv function
  ObLogInsert *insert_op = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table_partition_info) || OB_ISNULL(dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(get_stmt()), K(table_partition_info), K(ret));
  } else if (OB_ISNULL(insert_op = static_cast<ObLogInsert*>(log_op_factory_.allocate(*this, LOG_INSERT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate insert operator", K(ret));
  } else if (OB_FAIL(insert_op->get_index_dml_infos().push_back(dml_info))) {
    LOG_WARN("failed to add index dml info", K(ret));
  } else {
    insert_op->set_child(ObLogicalOperator::first_child, top);
    insert_op->set_is_pdml(true);
    insert_op->set_index_maintenance(is_index_maintenance);
    insert_op->set_pdml_update_split(is_pdml_update_split);
    if (is_last_dml_op) {
      insert_op->set_pdml_is_returning(get_stmt()->is_returning());
      insert_op->set_is_returning(get_stmt()->is_returning());
    } else {
      insert_op->set_pdml_is_returning(true); // 默认pdml的每一个delete都需要向上吐/返回数据
    }
    insert_op->set_table_partition_info(table_partition_info);
    insert_op->set_need_allocate_partition_id_expr(need_partition_id);
    if (get_stmt()->is_insert_stmt()) {
      const ObInsertStmt *insert_stmt = static_cast<const ObInsertStmt*>(get_stmt());
      insert_op->set_first_dml_op(!is_index_maintenance);
      insert_op->set_replace(insert_stmt->is_replace());
      insert_op->set_ignore(insert_stmt->is_ignore());
      insert_op->set_is_insert_select(insert_stmt->value_from_select());
      if (OB_NOT_NULL(insert_stmt->get_table_item(0))) {
        insert_op->set_append_table_id(insert_stmt->get_table_item(0)->ref_id_);
      }
      if (OB_FAIL(insert_stmt->get_view_check_exprs(insert_op->get_view_check_exprs()))) {
        LOG_WARN("failed to get view check exprs", K(ret));
      }
    } else if (get_stmt()->is_update_stmt()) {
      const ObUpdateStmt *update_stmt = static_cast<const ObUpdateStmt*>(get_stmt());
      insert_op->set_table_location_uncertain(true);
      if (!is_index_maintenance) {
        if (OB_FAIL(update_stmt->get_view_check_exprs(insert_op->get_view_check_exprs()))) {
          LOG_WARN("failed to get view check exprs", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    }
    if (OB_FAIL(insert_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = insert_op;
    }
  }
  return ret;
}

int ObDelUpdLogPlan::candi_allocate_one_pdml_update(bool is_index_maintenance,
                                                    bool is_last_dml_op,
                                                    IndexDMLInfo *index_dml_info)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  ObSEArray<ObRawExpr*, 1> dummy_filters;
  ObShardingInfo *source_sharding = NULL;
  ObTablePartitionInfo *source_table_partition = NULL;
  ObSEArray<CandidatePlan, 8> best_plans;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(index_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(calculate_table_location_and_sharding(
                     *get_stmt(),
                     is_index_maintenance ? dummy_filters : get_stmt()->get_condition_exprs(),
                     index_dml_info->loc_table_id_,
                     index_dml_info->ref_table_id_,
                     NULL,
                     source_sharding,
                     source_table_partition))) {
    LOG_WARN("failed to calculate table location and sharding", K(ret));
  } else if (OB_ISNULL(source_sharding) || OB_ISNULL(source_table_partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_sharding), K(source_table_partition), K(ret));
  } else if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_,
                                                 best_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else if (OB_FAIL(compute_exchange_info_for_pdml_del_upd(*source_sharding,
                                                            *source_table_partition,
                                                            *index_dml_info,
                                                            is_index_maintenance,
                                                            exch_info))) {
    LOG_WARN("failed to compute pdml exchange info for delete/update operator", K(ret));
  } else {
    bool need_partition_id = source_sharding->get_part_level() == share::schema::PARTITION_LEVEL_ONE ||
                             source_sharding->get_part_level() == share::schema::PARTITION_LEVEL_TWO;
    for (int64_t i = 0; OB_SUCC(ret) && i < best_plans.count(); i++) {
      if (OB_FAIL(create_pdml_update_plan(best_plans.at(i).plan_tree_,
                                          exch_info,
                                          source_table_partition,
                                          is_index_maintenance,
                                          is_last_dml_op,
                                          need_partition_id,
                                          index_dml_info))) {
        LOG_WARN("failed to create delete plan", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prune_and_keep_best_plans(best_plans))) {
        LOG_WARN("failed to prune and keep best plans", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::create_pdml_update_plan(ObLogicalOperator *&top,
                                             const ObExchangeInfo &exch_info,
                                             ObTablePartitionInfo *source_table_partition,
                                             bool is_index_maintenance,
                                             bool is_last_dml_op,
                                             bool need_partition_id,
                                             IndexDMLInfo *index_dml_info)
{
  int ret = OB_SUCCESS;
  bool need_exchange = true;
  if (OB_ISNULL(source_table_partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_table_partition), K(ret));
  } else if (!is_index_maintenance &&
             OB_FAIL(check_need_exchange_for_pdml_del_upd(top, exch_info,
                                                          source_table_partition->get_table_id(),
                                                          need_exchange))) {
    LOG_WARN("failed to check pdml need exchange for del upd", K(ret));
  } else if (OB_FAIL(need_exchange && allocate_exchange_as_top(top, exch_info))) {
    LOG_WARN("failed to allocate exchange as top", K(ret));
  } else if (OB_FAIL(allocate_pdml_update_as_top(top,
                                                 is_index_maintenance,
                                                 is_last_dml_op,
                                                 need_partition_id,
                                                 source_table_partition,
                                                 index_dml_info))) {
    LOG_WARN("failed to allocate pdml delete as top", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObDelUpdLogPlan::allocate_pdml_update_as_top(ObLogicalOperator *&top,
                                                 bool is_index_maintenance,
                                                 bool is_last_dml_op,
                                                 bool need_partition_id,
                                                 ObTablePartitionInfo *table_partition_info,
                                                 IndexDMLInfo *index_dml_info)
{
  int ret = OB_SUCCESS;
  ObLogUpdate *update_op = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(get_stmt()) || OB_ISNULL(table_partition_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(get_stmt()), K(table_partition_info), K(ret));
  } else if (OB_UNLIKELY(!get_stmt()->is_update_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stmt type", K(get_stmt()->get_stmt_type()), K(ret));
  } else if (OB_ISNULL(update_op = static_cast<ObLogUpdate*>(log_op_factory_.allocate(*this, LOG_UPDATE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate update operator", K(ret));
  } else if (OB_FAIL(update_op->get_index_dml_infos().push_back(index_dml_info))) {
    LOG_WARN("failed to add index dml info", K(ret));
  } else {
    const ObUpdateStmt *update_stmt = static_cast<const ObUpdateStmt*>(get_stmt());
    update_op->set_child(ObLogicalOperator::first_child, top);
    update_op->set_is_pdml(true);
    if (is_last_dml_op) {
      update_op->set_pdml_is_returning(update_stmt->is_returning());
      update_op->set_is_returning(update_stmt->is_returning());
    } else {
      update_op->set_pdml_is_returning(true); // 默认pdml的每一个delete都需要向上吐/返回数据
    }
    update_op->set_first_dml_op(!is_index_maintenance);
    update_op->set_index_maintenance(is_index_maintenance);
    update_op->set_ignore(update_stmt->is_ignore());
    update_op->set_table_partition_info(table_partition_info);
    update_op->set_need_allocate_partition_id_expr(need_partition_id);
    if (!is_index_maintenance) {
      if (OB_FAIL(update_stmt->get_view_check_exprs(update_op->get_view_check_exprs()))) {
        LOG_WARN("failed to get view check exprs", K(ret));
      }
    }
    if (OB_FAIL(update_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = update_op;
    }
  }
  return ret;
}

int ObDelUpdLogPlan::check_need_exchange_for_pdml_del_upd(ObLogicalOperator *top,
                                                          const ObExchangeInfo &exch_info,
                                                          uint64_t table_id,
                                                          bool &need_exchange)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  ObShardingInfo *top_sharding = NULL;
  ObShardingInfo *source_sharding = NULL;
  ObTablePartitionInfo *source_table_part = NULL;
  need_exchange = false;
  if (OB_ISNULL(top) || OB_ISNULL(top_sharding = top->get_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(top->get_sharding()), K(ret));
  } else if (OB_FAIL(get_source_table_info(*top, table_id, source_sharding, source_table_part))) {
    LOG_WARN("failed to get source sharding info", K(ret));
  } else if (NULL == source_sharding) {
    need_exchange = true;
  } else if (OB_FAIL(ObShardingInfo::is_sharding_equal(top_sharding,
                                                       source_sharding,
                                                       top->get_output_equal_sets(),
                                                       is_equal))) {
    LOG_WARN("failed to check if sharding is equal", K(ret));
  } else if (!is_equal) {
    need_exchange = true;
  } else {
    need_exchange = false;
  }
  return ret;
}

int ObDelUpdLogPlan::split_update_index_dml_info(const IndexDMLInfo &upd_dml_info,
                                                 IndexDMLInfo *&del_dml_info,
                                                 IndexDMLInfo *&ins_dml_info)
{
  int ret = OB_SUCCESS;
  IndexDMLInfo tmp_dml_info;
  if (OB_FAIL(tmp_dml_info.assign_basic(upd_dml_info))) {
    LOG_WARN("assign tmp dml info failed", K(ret));
  } else if (OB_FAIL(tmp_dml_info.init_column_convert_expr(upd_dml_info.assignments_))) {
    LOG_WARN("init column convert expr failed", K(ret));
  } else {
    tmp_dml_info.assignments_.reset();
    if (OB_FAIL(create_index_dml_info(tmp_dml_info, ins_dml_info))) {
      LOG_WARN("create index dml info failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    tmp_dml_info.column_convert_exprs_.reset();
    if (OB_FAIL(create_index_dml_info(tmp_dml_info, del_dml_info))) {
      LOG_WARN("create index dml info failed", K(ret));
    }
  }
  return ret;
}

int ObDelUpdLogPlan::create_index_dml_info(const IndexDMLInfo &orgi_dml_info,
                                           IndexDMLInfo *&opt_dml_info)
{
  int ret = OB_SUCCESS;
  void *ptr= NULL;
  if (OB_ISNULL(ptr = get_optimizer_context().get_allocator().alloc(sizeof(IndexDMLInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    opt_dml_info = new (ptr) IndexDMLInfo();
    if (OB_FAIL(opt_dml_info->assign_basic(orgi_dml_info))) {
      LOG_WARN("failed to assign dml info", K(ret));
    } else if (OB_FAIL(prune_virtual_column(*opt_dml_info))) {
      LOG_WARN("prune virtual column failed", K(ret));
    } else if (!optimizer_context_.get_session_info()->get_ddl_info().is_ddl() //create index not need to collect related index
        && OB_FAIL(collect_related_local_index_ids(*opt_dml_info))) {
      LOG_WARN("collect related local index ids failed", K(ret));
    }
  }
  return ret;
}

int ObDelUpdLogPlan::prune_virtual_column(IndexDMLInfo &index_dml_info)
{
  int ret = OB_SUCCESS;
  if (index_dml_info.is_primary_index_) {
    ObBitSet<> column_remove_flags;
    ObBitSet<> assign_remove_flags;
    const ObDelUpdStmt* stmt = get_stmt();
    if (OB_ISNULL(optimizer_context_.get_session_info()) || OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get session info", KR(ret), K(stmt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.column_exprs_.count(); ++i) {
      const ObColumnRefRawExpr *col_expr = index_dml_info.column_exprs_.at(i);
      if (lib::is_oracle_mode() &&
          (col_expr->is_virtual_generated_column() || col_expr->is_xml_column()) && // udt/xml column need remove
          !optimizer_context_.has_trigger()) {
        //why need to exclude trigger here?
        //see the issue:
        //key column means it is the rowkey column or part key column or index column
        //outside these scenarios with Oracle mode,
        //DML operator will not touch the virtual column
        bool is_key_column = false;
        bool need_remove = true;
        const uint64_t tenant_id = optimizer_context_.get_session_info()->get_effective_tenant_id();
        ObSchemaGetterGuard *schema_guard = optimizer_context_.get_schema_guard();
        ObColumnRefRawExpr *base_col_expr = index_dml_info.column_exprs_.at(i);
        if (OB_FAIL(ObTransformUtils::get_base_column(stmt, base_col_expr))) {
          LOG_WARN("failed to get base column", K(ret));
        } else if (OB_FAIL(schema_guard->column_is_key(tenant_id,
                                                       index_dml_info.ref_table_id_,
                                                       base_col_expr->get_column_id(),
                                                       is_key_column))) {
          LOG_WARN("check column whether is key failed", K(ret));
        } else if (is_key_column) {
          need_remove = false;
        } else if (col_expr->is_not_null_for_write() && !col_expr->is_xml_column()) {
          //has not null constraint on this column, need to check
          // xmltype column has not null constraint on this column, still need remove
          need_remove = false;
        }
        for (int64_t j = 0; OB_SUCC(ret) && need_remove && j < index_dml_info.ck_cst_exprs_.count(); ++j) {
          ObRawExpr **addr_matched_expr = nullptr;
          if (ObOptimizerUtil::is_sub_expr(col_expr, index_dml_info.ck_cst_exprs_.at(j), addr_matched_expr)) {
            //reference by check constraint expr, cannot be pruned
            need_remove = false;
          }
        }
        int64_t assign_idx = -1;
        for (int64_t j = 0; OB_SUCC(ret) && need_remove && j < index_dml_info.assignments_.count(); ++j) {
          ObRawExpr **addr_matched_expr = nullptr;
          if (ObOptimizerUtil::is_sub_expr(col_expr, index_dml_info.assignments_.at(j).expr_, addr_matched_expr)) {
            //reference by update clause, cannot be pruned
            need_remove = false;
          } else if (col_expr == index_dml_info.assignments_.at(j).column_expr_) {
            assign_idx = j;
          }
        }
        if (OB_SUCC(ret) && need_remove) {
          if (OB_FAIL(column_remove_flags.add_member(i))) {
            LOG_WARN("add remove flag failed", K(ret));
          } else if (assign_idx >= 0 && OB_FAIL(assign_remove_flags.add_member(assign_idx))) {
            LOG_WARN("add assign remove flag failed", K(ret));
          }
        }
      }
    }
    int64_t saved_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.column_exprs_.count(); ++i) {
      //now to prune virtual column
      if (!column_remove_flags.has_member(i)) {
        index_dml_info.column_exprs_.at(saved_idx) = index_dml_info.column_exprs_.at(i);
        if (!index_dml_info.column_convert_exprs_.empty()) {
          index_dml_info.column_convert_exprs_.at(saved_idx) = index_dml_info.column_convert_exprs_.at(i);
        }
        ++ saved_idx;
      }
    }
    int64_t pop_cnt = index_dml_info.column_exprs_.count() - saved_idx;
    for (int64_t i = 0; OB_SUCC(ret) && i < pop_cnt; ++i) {
      index_dml_info.column_exprs_.pop_back();
      if (!index_dml_info.column_convert_exprs_.empty()) {
        index_dml_info.column_convert_exprs_.pop_back();
      }
    }
    saved_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.assignments_.count(); ++i) {
      //now to prune assignment expr
      if (!assign_remove_flags.has_member(i)) {
        index_dml_info.assignments_.at(saved_idx++) = index_dml_info.assignments_.at(i);
      }
    }
    pop_cnt = index_dml_info.assignments_.count() - saved_idx;
    for (int64_t i = 0; OB_SUCC(ret) && i < pop_cnt; ++i) {
      index_dml_info.assignments_.pop_back();
    }
  }
  return ret;
}

int ObDelUpdLogPlan::collect_related_local_index_ids(IndexDMLInfo &primary_dml_info)
{
  int ret = OB_SUCCESS;
  //project the local index dml info from data table
  const ObTableSchema *index_schema = nullptr;
  ObSchemaGetterGuard *schema_guard = nullptr;
  const ObDelUpdStmt *stmt = get_stmt();
  int64_t index_tid_array_size = OB_MAX_INDEX_PER_TABLE;
  uint64_t index_tid_array[OB_MAX_INDEX_PER_TABLE];
  ObArray<uint64_t> base_column_ids;
  const uint64_t tenant_id = optimizer_context_.get_session_info()->get_effective_tenant_id();
  ObInsertLogPlan *insert_plan = dynamic_cast<ObInsertLogPlan*>(this);
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_guard = optimizer_context_.get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is nullptr", K(ret), K(stmt), K(schema_guard));
  } else if (NULL != insert_plan && insert_plan->is_direct_insert()) {
    index_tid_array_size = 0; // no need building index
  } else if (OB_FAIL(schema_guard->get_can_write_index_array(tenant_id,
                                                             primary_dml_info.ref_table_id_,
                                                             index_tid_array,
                                                             index_tid_array_size,
                                                             false /*only global*/))) {
    LOG_WARN("get can write index array failed", K(ret), K(primary_dml_info.ref_table_id_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < primary_dml_info.assignments_.count(); ++i) {
    ObColumnRefRawExpr *column_expr = primary_dml_info.assignments_.at(i).column_expr_;
    ColumnItem *column_item = nullptr;
    if (OB_ISNULL(column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column expr", K(ret));
    } else if (OB_ISNULL(column_item = stmt->get_column_item_by_id(column_expr->get_table_id(),
                                                                   column_expr->get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column item", K(ret), KPC(column_expr));
    } else if (OB_FAIL(base_column_ids.push_back(column_item->base_cid_))) {
      LOG_WARN("failed to push back base cid", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_tid_array_size; ++i) {
    if (OB_FAIL(schema_guard->get_table_schema(tenant_id, index_tid_array[i], index_schema))) {
      LOG_WARN("get index schema failed", K(ret), K(index_tid_array[i]), K(i));
    } else if (index_schema->is_index_local_storage()) {
      //only need to attach local index and primary index in the same DAS Task
      if (primary_dml_info.assignments_.empty()) {
        //is insert or delete, need to add to the related index ids
        if (OB_FAIL(primary_dml_info.related_index_ids_.push_back(index_schema->get_table_id()))) {
          LOG_WARN("add related index ids failed", K(ret));
        }
      } else {
        bool found_col = false;
        //in update clause, need to check this local index whether been updated
        for (int64_t j = 0; OB_SUCC(ret) && !found_col && j < base_column_ids.count(); ++j) {
          found_col = (index_schema->get_column_schema(base_column_ids.at(j)) != nullptr);
        }
        if (OB_SUCC(ret) && found_col) {
          //update clause will modify this local index, need to add it to related_index_ids_
          if (OB_FAIL(primary_dml_info.related_index_ids_.push_back(index_schema->get_table_id()))) {
            LOG_WARN("store index id to related index ids failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::gen_px_coord_sampling_sort_keys(const ObIArray<OrderItem> &src,
                                                     ObIArray<OrderItem> &dst)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext &ctx = get_optimizer_context();
  FOREACH_CNT_X(it, src, OB_SUCC(ret)) {
    CK(NULL != it->expr_);
    OZ(it->expr_->formalize(ctx.get_session_info()));
    ObOpPseudoColumnRawExpr *expr = NULL;
    // We should distinguish T_FUN_SYS_CALC_PARTITION_ID with other exprs, because we will
    // remove it later in CG (ObStaticEngineCG::filter_sort_keys)
    const ObItemType expr_type = it->expr_->is_calc_part_expr()
        ? T_PSEUDO_CALC_PART_SORT_KEY : T_PSEUDO_SORT_KEY;
    OZ(ObRawExprUtils::build_op_pseudo_column_expr(ctx.get_expr_factory(),
                                                   expr_type,
                                                   "PSEUDO_SORT_KEY",
                                                   it->expr_->get_result_type(),
                                                   expr));
    CK(NULL != expr);
    OZ(expr->formalize(ctx.get_session_info()));
    if (OB_SUCC(ret)) {
      OrderItem item = *it;
      item.expr_ = expr;
      OZ(dst.push_back(item));
    }
  }
  return ret;
}

int ObDelUpdLogPlan::prepare_table_dml_info_basic(const ObDmlTableInfo& table_info,
                                                  IndexDMLInfo*& table_dml_info,
                                                  ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                  const bool has_tg)
{
  int ret = OB_SUCCESS;
  void *ptr= NULL;
  IndexDMLInfo* index_dml_info = NULL;
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  const ObTableSchema* index_schema = NULL;
  if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",K(ret), K(schema_guard), K(session_info));
  } else if (OB_ISNULL(ptr = get_optimizer_context().get_allocator().alloc(sizeof(IndexDMLInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    table_dml_info = new (ptr) IndexDMLInfo();
    if (OB_FAIL(table_dml_info->assign(table_info))) {
      LOG_WARN("failed to assign table info", K(ret));
    } else if (has_tg) {
      table_dml_info->is_primary_index_ = true;
    } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                      table_info.ref_table_id_, index_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table schema", K(table_info), K(ret));
    } else {
      table_dml_info->rowkey_cnt_ = index_schema->get_rowkey_column_num();
      table_dml_info->is_primary_index_ = true;
      ObExecContext *exec_ctx = get_optimizer_context().get_exec_ctx();
      if (OB_ISNULL(exec_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exec_cts is null", K(ret));
      } else if (exec_ctx->get_sql_ctx()->get_enable_strict_defensive_check() &&
          !(optimizer_context_.get_session_info()->is_inner()) &&
          (stmt_->is_update_stmt() || stmt_->is_delete_stmt()) &&
          GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0) {
        // 1: Is strict defensive check mode
        // 2: Not inner_sql
        // 3: Now only support delete and update statement
        // 4: disable it when upgrade
        // Only when the three conditions are met can the defensive_check information be added
        TableItem *table_item = nullptr;
        ObOpPseudoColumnRawExpr *trans_info_expr = nullptr;
        if (OB_ISNULL(table_item = stmt_->get_table_item_by_id(table_info.table_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(table_info.table_id_), KPC(stmt_));
        } else if (OB_FAIL(ObOptimizerUtil::generate_pseudo_trans_info_expr(get_optimizer_context(),
                                                                            table_item->get_table_name(),
                                                                            trans_info_expr))) {
          LOG_WARN("fail to generate pseudo trans info expr", K(ret), K(table_item->get_table_name()));
        } else if (OB_ISNULL(trans_info_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null pointer", K(ret));
        } else {
          table_dml_info->trans_info_expr_ = trans_info_expr;
        }
      }
    }
  }
  if (OB_SUCC(ret) && !has_tg) {
    uint64_t index_tid[OB_MAX_INDEX_PER_TABLE];
    int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;
    ObInsertLogPlan *insert_plan = dynamic_cast<ObInsertLogPlan*>(this);
    if (NULL != insert_plan && insert_plan->is_direct_insert()) {
      index_cnt = 0; // no need building index
    } else if (OB_FAIL(schema_guard->get_can_write_index_array(session_info->get_effective_tenant_id(),
                                                        table_info.ref_table_id_, index_tid, index_cnt, true))) {
      LOG_WARN("failed to get can read index array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
      if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                 index_tid[i], index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table schema", K(index_tid[i]), K(ret));
      } else if (OB_ISNULL(ptr = get_optimizer_context().get_allocator().alloc(sizeof(IndexDMLInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        index_dml_info = new (ptr) IndexDMLInfo();
        if (OB_FAIL(index_schema->get_index_name(index_dml_info->index_name_))) {
          LOG_WARN("failed to get index name", K(ret));
        } else {
          index_dml_info->table_id_ = table_info.table_id_;
          index_dml_info->loc_table_id_ = table_info.loc_table_id_;
          index_dml_info->ref_table_id_ = index_tid[i];
          index_dml_info->rowkey_cnt_ = index_schema->get_rowkey_column_num();
          index_dml_info->spk_cnt_ = index_schema->get_shadow_rowkey_column_num();
          // Trans_info_expr_ on the main table is recorded in all index_dml_info
          index_dml_info->trans_info_expr_ = table_dml_info->trans_info_expr_;
          ObSchemaObjVersion table_version;
          table_version.object_id_ = index_tid[i];
          table_version.object_type_ = DEPENDENCY_TABLE;
          table_version.version_ = index_schema->get_schema_version();
          if (OB_FAIL(index_dml_infos.push_back(index_dml_info))) {
            LOG_WARN("failed to push back table dml info", K(ret));
          } else if (OB_FAIL(extra_dependency_tables_.push_back(table_version))) {
            LOG_WARN("add global dependency table failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::prepare_table_dml_info_special(const ObDmlTableInfo& table_info,
                                                    IndexDMLInfo* table_dml_info,
                                                    ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                    ObIArray<IndexDMLInfo*> &all_index_dml_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table dml info", K(ret));
  } else if (OB_FAIL(prune_virtual_column(*table_dml_info))) {
    LOG_WARN("failed to prune virtual column", K(ret));
  //create index not need to collect related index
  } else if (!optimizer_context_.get_session_info()->get_ddl_info().is_ddl() &&
             OB_FAIL(collect_related_local_index_ids(*table_dml_info))) {
    LOG_WARN("collect related local index ids failed", K(ret));
  } else if (OB_FAIL(all_index_dml_infos.push_back(table_dml_info))) {
    LOG_WARN("failed to push back table dml info", K(ret));
  } else if (OB_FAIL(append(all_index_dml_infos, index_dml_infos))) {
    LOG_WARN("failed to append index dml infos", K(ret));
  }
  return ret;
}

int ObDelUpdLogPlan::generate_part_key_ids(const ObTableSchema &index_schema,
                                           ObIArray<uint64_t> &array) const
{
  int ret = OB_SUCCESS;
  array.reuse();
  if (!index_schema.is_partitioned_table()) {
    // do nothing
  } else if (index_schema.get_partition_key_info().get_size() > 0 &&
             OB_FAIL(index_schema.get_partition_key_info().get_column_ids(array))) {
    LOG_WARN("failed to get column ids", K(ret));
  } else if (index_schema.get_subpartition_key_info().get_size() > 0 &&
             OB_FAIL(index_schema.get_subpartition_key_info().get_column_ids(array))) {
    LOG_WARN("failed to get column ids", K(ret));
  }
  return ret;
}

int ObDelUpdLogPlan::generate_index_column_exprs(const uint64_t table_id,
                                                 const ObTableSchema &index_schema,
                                                 const ObAssignments &assignments,
                                                 ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt *stmt = get_stmt();
  ObSQLSessionInfo *session_info = optimizer_context_.get_session_info();
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  uint64_t column_id = 0;
  if (OB_ISNULL(stmt) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(session_info));
  } else if (OB_FAIL(session_info->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    // 1. Firstly, add all rowkey columns
    ObSEArray<ObColumnRefRawExpr*, 4> spk_related_columns;
    ObArray<uint64_t> key_ids; //key ids: contain rowkey id, part key id
    ObColumnRefRawExpr *col_expr = NULL;
    const ColumnItem *col_item = NULL;
    bool is_modify_key = false;
    if (OB_FAIL(index_schema.get_rowkey_partkey_column_ids(key_ids))) {
      LOG_WARN("get column ids in rowkey info failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ids.count(); ++i) {
      uint64_t column_id = key_ids.at(i);
      if (OB_ISNULL(col_item = ObResolverUtils::find_col_by_base_col_id(*stmt, table_id, column_id,
                                                                        OB_INVALID_ID, true))) {
        // TODO: @yibo, after resolver view base item is not valid, find another way to get column
        uint64_t ref_rowkey_id = column_id - OB_MIN_SHADOW_COLUMN_ID;
        if (!is_shadow_column(column_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column expr by id failed", K(ret), K(table_id), K(column_id), K(*stmt));
        } else if (OB_ISNULL(col_item = ObResolverUtils::find_col_by_base_col_id(*stmt, table_id, ref_rowkey_id,
                                                                                 OB_INVALID_ID, true))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column item by id failed", K(ret), K(table_id), K(ref_rowkey_id));
        } else if (OB_ISNULL(col_expr = col_item->expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr of column item is null", K(ret), K(*col_item));
        } else if (OB_FAIL(add_var_to_array_no_dup(spk_related_columns, col_expr))) {
          LOG_WARN("failed to add var to array no dup", K(ret));
        }
      } else if (OB_ISNULL(col_expr = col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr of column item is null", K(ret), K(*col_item));
      } else if (OB_FAIL(column_exprs.push_back(col_expr))) {
        LOG_WARN("store column expr to column exprs failed", K(ret));
      } else if (!is_modify_key &&
                 OB_FAIL(ObResolverUtils::check_whether_assigned(stmt, assignments, table_id, column_id, is_modify_key))) {
        LOG_WARN("check whether assigned rowkey failed", K(ret), K(table_id), K(column_id), K(assignments));
      }
    }
    if (OB_SUCC(ret)) {
      if (is_modify_key || ObBinlogRowImage::FULL == binlog_row_image) {
        //modify rowkey, need add all columns in schema
        ObTableSchema::const_column_iterator iter = index_schema.column_begin();
        ObTableSchema::const_column_iterator end = index_schema.column_end();
        for (; OB_SUCC(ret) && iter != end; ++iter) {
          const ObColumnSchemaV2 *column = *iter;
          // skip all rowkeys
          if (OB_ISNULL(column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column schema", K(column));
          } else if (column->is_rowkey_column()) {
            // do nothing
          } else if (OB_ISNULL(col_item = ObResolverUtils::find_col_by_base_col_id(*stmt, table_id,
                                                                                   column->get_column_id(),
                                                                                   OB_INVALID_ID, true))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get column item by id failed", K(ret), K(table_id), K(column->get_column_id()));
          } else if (OB_FAIL(column_exprs.push_back(col_item->expr_))) {
            LOG_WARN("store column expr to column exprs failed", K(ret));
          }
        }
      } else {
        //没有修改主键，只添加自己被修改的列及主表的rowkey
        for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
          col_expr = const_cast<ObColumnRefRawExpr*>(assignments.at(i).column_expr_);
          if (OB_ISNULL(col_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("assignment column expr is null");
          } else if (OB_ISNULL(index_schema.get_column_schema(col_expr->get_column_id()))) {
            //该列不存在于该表的schema中，忽略掉
          } else if (OB_FAIL(column_exprs.push_back(col_expr))) {
            LOG_WARN("store column expr to column exprs failed", K(ret));
          }
        }
        if (FAILEDx(append_array_no_dup(column_exprs, spk_related_columns))) {
          LOG_WARN("failed to append array no dup", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::generate_index_column_exprs(uint64_t table_id,
                                                 const ObTableSchema &index_schema,
                                                 ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(stmt));
  } else if (OB_FAIL(generate_index_rowkey_exprs(table_id, index_schema, column_exprs, false))) {
    LOG_WARN("resolve index rowkey exprs failed", K(ret), K(table_id));
  } else {
    const ColumnItem *col_item = NULL;
    ObTableSchema::const_column_iterator iter = index_schema.column_begin();
    ObTableSchema::const_column_iterator end = index_schema.column_end();
    for (; OB_SUCC(ret) && iter != end; ++iter) {
      const ObColumnSchemaV2 *column = *iter;
      // skip all rowkeys
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", K(column));
      } else if (column->is_rowkey_column()) {
        // do nothing
      } else if (OB_ISNULL(col_item = stmt->get_column_item_by_base_id(table_id, column->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column item by id failed", K(ret), K(table_id), K(column->get_column_id()));
      } else if (OB_FAIL(column_exprs.push_back(col_item->expr_))) {
        LOG_WARN("store column expr to column exprs failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::generate_index_rowkey_exprs(uint64_t table_id,
                                                 const ObTableSchema &index_schema,
                                                 ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                                 bool need_spk)
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt *del_upd_stmt = get_stmt();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(del_upd_stmt), K(session_info));
  } else {
    uint64_t rowkey_column_id = 0;
    const ObRowkeyInfo &rowkey_info = index_schema.get_rowkey_info();
    const ColumnItem *col = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("get rowkey column id failed", K(ret));
      } else if (OB_ISNULL(col = ObResolverUtils::find_col_by_base_col_id(*del_upd_stmt,
                                                                          table_id,
                                                                          rowkey_column_id,
                                                                          OB_INVALID_ID, true))) {
        ObColumnRefRawExpr *spk_col = nullptr;
        if (!is_shadow_column(rowkey_column_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column expr by id failed", K(table_id), K(rowkey_column_id));
        } else if (!need_spk) {
          //do nothing
        } else if (OB_FAIL(ObRawExprUtils::build_shadow_pk_expr(table_id, rowkey_column_id,
                                                                *del_upd_stmt,
                                                                optimizer_context_.get_expr_factory(),
                                                                *session_info,
                                                                index_schema, spk_col))) {
          LOG_WARN("failed to build shadow pk expr", K(ret), K(table_id), K(rowkey_column_id));
        } else if (OB_FAIL(column_exprs.push_back(spk_col))) {
          LOG_WARN("store shadow pk column failed", K(ret));
        }
      } else if (OB_FAIL(column_exprs.push_back(col->expr_))) {
        LOG_WARN("store column expr to column exprs failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::check_index_update(ObAssignments assigns,
                                        const ObTableSchema& index_schema,
                                        const bool is_update_view,
                                        bool& need_update)
{
  int ret = OB_SUCCESS;
  need_update = false;
  const ObColumnRefRawExpr* column_expr = nullptr;
  const ColumnItem* column_item = nullptr;
  const ObDelUpdStmt* stmt = get_stmt();
  for (int64_t i = 0; OB_SUCC(ret) && !need_update && i < assigns.count(); ++i) {
    if (OB_ISNULL(column_expr = assigns.at(i).column_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column expr", K(ret));
    } else if (is_update_view) {
      if (OB_ISNULL(column_item = stmt->get_column_item_by_id(column_expr->get_table_id(),
                                                              column_expr->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column item", K(ret));
      } else if (OB_FAIL(index_schema.has_column(column_item->base_cid_, need_update))) {
        LOG_WARN("failed to check index has column", K(ret), KPC(column_expr));
      }
    } else if (OB_FAIL(index_schema.has_column(column_expr->get_column_id(), need_update))) {
      LOG_WARN("failed to check index has column", K(ret), KPC(column_expr));
    }
  }
  return ret;
}

int ObDelUpdLogPlan::fill_index_column_convert_exprs(ObRawExprCopier &copier,
                                                     const ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                                     ObIArray<ObRawExpr *> &column_convert_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
    ObRawExpr *new_value_expr = NULL;
    if (!is_shadow_column(column_exprs.at(i)->get_column_id())) {
      if (OB_FAIL(copier.copy_on_replace(column_exprs.at(i), new_value_expr))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      }
    } else {
      if (OB_FAIL(copier.copy(column_exprs.at(i)->get_dependant_expr(), new_value_expr))) {
        LOG_WARN("failed to copy and replace dependant expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(column_convert_exprs.push_back(new_value_expr))) {
      LOG_WARN("failed to push back new value expr", K(ret));
    }
  }
  LOG_TRACE("check column convert expr", K(column_exprs), K(column_convert_exprs));
  return ret;
}

int ObDelUpdLogPlan::add_extra_dependency_table() const
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = const_cast<ObDelUpdStmt *>(get_stmt());
  if (OB_ISNULL(del_upd_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid insert stmt", K(del_upd_stmt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_dependency_tables_.count(); ++i) {
      if (OB_FAIL(del_upd_stmt->add_global_dependency_table(extra_dependency_tables_.at(i)))) {
        LOG_WARN("add global dependency table failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::check_update_unique_key(const ObTableSchema* index_schema,
                                              IndexDMLInfo*& index_dml_info) const
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt *stmt = get_stmt();
  ObSEArray<uint64_t, 8> pk_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(index_schema) || OB_ISNULL(index_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(index_schema), K(index_dml_info));
  } else if (!share::schema::ObSimpleTableSchemaV2::is_global_unique_index_table(index_schema->get_index_type())) {
    // do nothing
  } else if (OB_FAIL(index_schema->get_rowkey_info().get_column_ids(pk_ids))) {
    LOG_WARN("failed to get rowkey column ids", K(ret));
  } else {
    for (int64_t i = 0; i < index_dml_info->assignments_.count(); ++i) {
      ObColumnRefRawExpr *column_expr = index_dml_info->assignments_.at(i).column_expr_;
      ColumnItem *column_item = nullptr;
      if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column expr", K(ret));
      } else if (OB_ISNULL(column_item = stmt->get_column_item_by_id(column_expr->get_table_id(),
                                                                     column_expr->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column item", K(ret), KPC(column_expr));
      } else if (has_exist_in_array(pk_ids, column_item->base_cid_)) {
        index_dml_info->is_update_unique_key_ = true;
        break;
      }
    }
  }
  return ret;
}

int ObDelUpdLogPlan::check_update_part_key(const ObTableSchema* index_schema,
                                           IndexDMLInfo*& index_dml_info) const
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt *stmt = get_stmt();
  ObSEArray<uint64_t, 8> part_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(index_schema) || OB_ISNULL(index_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(index_schema), K(index_dml_info));
  } else if (OB_FAIL(generate_part_key_ids(*index_schema, part_ids))) {
    LOG_WARN("failed to get rowkey column ids", K(ret));
  } else if (!part_ids.empty()) {
    for (int64_t i = 0; i < index_dml_info->assignments_.count(); ++i) {
      ObColumnRefRawExpr *column_expr = index_dml_info->assignments_.at(i).column_expr_;
      ColumnItem *column_item = nullptr;
      if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column expr", K(ret));
      } else if (OB_ISNULL(column_item = stmt->get_column_item_by_id(column_expr->get_table_id(),
                                                                     column_expr->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column item", K(ret), KPC(column_expr));
      } else if (has_exist_in_array(part_ids, column_item->base_cid_)) {
        index_dml_info->is_update_part_key_ = true;
        break;
      }
    }
  }
  return ret;
}
int ObDelUpdLogPlan::allocate_link_dml_as_top(ObLogicalOperator *&old_top)
{
  int ret = OB_SUCCESS;
  ObLogLinkDml *link_dml = NULL;
  if (OB_ISNULL(link_dml = static_cast<ObLogLinkDml *>(get_log_op_factory().
                                  allocate(*this, LOG_LINK_DML)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate link dml operator", K(ret));
  } else if (OB_FAIL(link_dml->compute_property())) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    old_top = link_dml;
  }
  return ret;
}
