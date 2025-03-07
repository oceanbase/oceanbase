/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_transform_mv_rewrite.h"
#include "sql/rewrite/ob_transform_pre_process.h"
#include "sql/rewrite/ob_transform_mv_rewrite_prepare.h"
#include "src/sql/optimizer/ob_log_plan.h"

namespace oceanbase
{
namespace sql
{

int ObTransformMVRewrite::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                             ObDMLStmt *&stmt,
                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_MV_STMT_GEN = 10;
  trans_happened = false;
  parent_stmts_ = &parent_stmts;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(stmt));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else {
    ObSelectStmt *ori_stmt = static_cast<ObSelectStmt*>(stmt);
    ObSelectStmt *new_stmt = NULL;
    const ObCollationType cs_type = stmt->get_query_ctx()->get_query_hint().cs_type_;
    const ObMVRewriteHint *myhint = static_cast<const ObMVRewriteHint*>(get_hint(stmt->get_stmt_hint()));
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->mv_infos_.count(); ++i) {
      bool is_happened = false;
      bool is_match_hint = true;
      MvInfo &mv_info = ctx_->mv_infos_.at(i);
      if (NULL != myhint && OB_FAIL(myhint->check_mv_match_hint(cs_type,
                                                                mv_info.mv_schema_,
                                                                mv_info.db_schema_,
                                                                is_match_hint))) {
        LOG_WARN("failed to check mv match hint", K(ret));
      } else if (!is_match_hint) {
        // do nothing
      } else if (NULL == mv_info.view_stmt_ && ++ctx_->mv_stmt_gen_count_ > MAX_MV_STMT_GEN) {
        OPT_TRACE("reach the mv stmt generate count limit!");
      } else if (NULL == mv_info.view_stmt_
                 && OB_FAIL(ObTransformMVRewritePrepare::generate_mv_stmt(mv_info, ctx_, &mv_temp_query_ctx_))) {
        LOG_WARN("failed to generate mv stmt", K(ret), K(mv_info));
      } else if (OB_FAIL(try_transform_with_one_mv(ori_stmt, mv_info, new_stmt, is_happened))) {
        LOG_WARN("failed to try one mv", K(ret));
      } else if (is_happened) {
        ori_stmt = new_stmt;
        trans_happened = true;
        break;
      }
    }
    if (OB_SUCC(ret) && trans_happened) {
      stmt = ori_stmt;
    }
  }
  return ret;
}

int ObTransformMVRewrite::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                         const int64_t current_level,
                                         const ObDMLStmt &stmt,
                                         bool &need_trans)
{
  int ret = OB_SUCCESS;
  int64_t query_rewrite_enabled = QueryRewriteEnabledType::REWRITE_ENABLED_FALSE;
  bool hint_rewrite = false;
  bool hint_no_rewrite = false;
  bool is_stmt_valid = false;
  need_trans = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexprcted null", K(ret), K(ctx_), K(stmt.get_query_ctx()));
  } else if (OB_FAIL(ObTransformRule::need_transform(parent_stmts,
                                                     current_level,
                                                     stmt,
                                                     need_trans))) {
    LOG_WARN("failed to check need transformation", K(ret));
  } else if (!need_trans) {
    // do nothing
  } else if (ctx_->iteration_level_ > 0) {
    need_trans = false;
    OPT_TRACE("only do mv rewrite in the first iteration");
  } else if (stmt.is_select_stmt() && static_cast<const ObSelectStmt*>(&stmt)->is_expanded_mview()) {
    need_trans = false;
  } else if (OB_FAIL(check_basic_validity(stmt, is_stmt_valid))) {
    LOG_WARN("failed to check basic validity", K(ret));
  } else if (!is_stmt_valid) {
    need_trans = false;
    OPT_TRACE("stmt can not do mv rewrite");
  } else if (ctx_->mv_infos_.empty()) {
    need_trans = false;
    OPT_TRACE("there is no mv to perform rewrite");
  } else if (OB_FAIL(check_hint_valid(stmt, hint_rewrite, hint_no_rewrite))) {
    LOG_WARN("failed to check mv rewrite hint", K(ret));
  } else if (hint_rewrite) {
    need_trans = true;
    OPT_TRACE("hint force mv rewrite");
  } else if (hint_no_rewrite) {
    need_trans = false;
    OPT_TRACE("hint reject mv rewrite");
  } else if (OB_FAIL(ctx_->session_info_->get_query_rewrite_enabled(query_rewrite_enabled))) {
    LOG_WARN("failed to get query rewrite enabled", K(ret));
  } else if (QueryRewriteEnabledType::REWRITE_ENABLED_FALSE == query_rewrite_enabled) {
    need_trans = false;
    OPT_TRACE("system variable reject mv rewrite");
  } else {
    need_trans = true;
  }
  return ret;
}

int ObTransformMVRewrite::check_hint_valid(const ObDMLStmt &stmt,
                                           bool &force_rewrite,
                                           bool &force_no_rewrite)
{
  int ret = OB_SUCCESS;
  force_rewrite = false;
  force_no_rewrite = false;
  const ObQueryHint *query_hint = NULL;
  const ObMVRewriteHint *myhint = static_cast<const ObMVRewriteHint*>(get_hint(stmt.get_stmt_hint()));
  if (OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query hint is null", K(ret), K(stmt));
  } else {
    force_rewrite = NULL != myhint && myhint->is_enable_hint();
    force_no_rewrite = !force_rewrite && query_hint->has_outline_data();
    // force_no_rewrite does not need to handle (NULL != myhint && myhint->is_disable_hint()),
    // because it has been handled in ObTransformRule::check_hint_status
  }
  return ret;
}

int ObTransformMVRewrite::check_basic_validity(const ObDMLStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> seq_exprs;
  is_valid = true;
  if (!stmt.is_select_stmt()) {
    OPT_TRACE("stmt can not do mv rewrite, not a select stmt");
    is_valid = false;
  } else if (stmt.is_set_stmt()) {
    OPT_TRACE("stmt can not do mv rewrite, it is a set stmt");
    is_valid = false;
  } else if (stmt.is_hierarchical_query()) {
    OPT_TRACE("stmt can not do mv rewrite, it is a hierarchical query");
    is_valid = false;
  } else if (stmt.is_contains_assignment()) {
    OPT_TRACE("stmt can not do mv rewrite, it contains assignment");
    is_valid = false;
  } else if (stmt.has_for_update()) {
    OPT_TRACE("stmt can not do mv rewrite, it contains for update");
    is_valid = false;
  } else if (stmt.has_ora_rowscn()) {
    OPT_TRACE("stmt can not do mv rewrite, it contains ora rowscn");
    is_valid = false;
  } else if (stmt.is_unpivot_select()) {
    OPT_TRACE("stmt can not do mv rewrite, it contains unpivot");
    is_valid = false;
  } else if (!stmt.get_pseudo_column_like_exprs().empty()) {
    OPT_TRACE("stmt can not do mv rewrite, it contains pseudo column");
    is_valid = false;
  } else if (!stmt.get_semi_infos().empty()) {
    OPT_TRACE("stmt can not do mv rewrite, it contains semi join");
    is_valid = false;
  } else if (OB_FAIL(stmt.get_sequence_exprs(seq_exprs))) {
    LOG_WARN("failed to get sequence exprs", K(ret));
  } else if (!seq_exprs.empty()) {
    OPT_TRACE("stmt can not do mv rewrite, it contains sequence");
    is_valid = false;
  }
  return ret;
}

int ObTransformMVRewrite::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  const MvInfo *mv_info = NULL;
  ObMVRewriteHint *hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)
      || OB_ISNULL(mv_info = static_cast<const MvInfo*>(trans_params))
      || OB_ISNULL(mv_info->mv_schema_) || OB_ISNULL(mv_info->db_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(trans_params));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_MV_REWRITE, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(stmt.get_stmt_hint())))) {
    LOG_WARN("failed to add used trans hint", K(ret));
  } else if (OB_FAIL(hint->get_mv_list().push_back(
      ObTableInHint(NULL, mv_info->db_schema_->get_database_name(), mv_info->mv_schema_->get_table_name())))) {
    LOG_WARN("failed to push back hint table", K(ret));
  } else {
    hint->set_qb_name(ctx_->src_qb_name_);
  }
  return ret;
}

/**
 * @brief ObTransformMVRewrite::gen_base_table_map
 *
 * Generate the table map between from_tables and to_tables based on ref_id.
 *
 * e.g. INPUT  from_tables: t1, t1, t1, t2, t3
 *             to_tables:   t1, t1, t2, t4
 *
 *      OUTPUT table_maps: [0, 1, -1, 2, -1],
 *                         [0, -1, 1, 2, -1],
 *                         [1, 0, -1, 2, -1],
 *                         [1, -1, 0, 2, -1],
 *                         [-1, 0, 1, 2, -1],
 *                         [-1, 1, 0, 2, -1]
 */
int ObTransformMVRewrite::gen_base_table_map(const ObIArray<TableItem*> &from_tables,
                                             const ObIArray<TableItem*> &to_tables,
                                             int64_t max_map_num,
                                             ObIArray<ObSEArray<int64_t,4>> &table_maps)
{
  int ret = OB_SUCCESS;
  if (max_map_num <= 0 || from_tables.count() <= 0 || to_tables.count() <= 0) {
    // do nothing
  } else if (OB_FAIL(table_maps.reserve(max_map_num))) {
    LOG_WARN("failed to reserve array", K(ret));
  } else {
    // prepare the auxiliary data
    hash::ObHashMap<uint64_t, int64_t> from_table_num;    // map ref_id to the number of tables that have same ref_id in from_tables
    hash::ObHashMap<uint64_t, int64_t> to_table_map;      // map ref_id to index of to_table_ids array in which table's ref_id is same as hash key
    ObSEArray<ObSEArray<int64_t,4>, 4> to_table_ids;      // store index of to_tables with the same ref_id
    if (OB_FAIL(from_table_num.create(from_tables.count(), "MvRewrite"))) {
      LOG_WARN("failed to init stmt map", K(ret));
    } else if (OB_FAIL(to_table_map.create(to_tables.count(), "MvRewrite"))) {
      LOG_WARN("failed to init stmt map", K(ret));
    }
    // collect the number of from_tables that have same ref_id
    for (int64_t i = 0; OB_SUCC(ret) && i < from_tables.count(); ++i) {
      const TableItem *from_table = from_tables.at(i);
      const int64_t *num = NULL;
      if (OB_ISNULL(from_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret), K(i));
      } else if (!(from_table->is_basic_table() || from_table->is_link_table())
                 || OB_INVALID_ID == from_table->ref_id_) {
        // do nothing
      } else if (NULL == (num = from_table_num.get(from_table->ref_id_))) {
        if (OB_FAIL(from_table_num.set_refactored(from_table->ref_id_, 1))) {
          LOG_WARN("failed to set refactored", K(ret), K(from_table->ref_id_));
        }
      } else if (OB_FAIL(from_table_num.set_refactored(from_table->ref_id_, (*num) + 1, 1))) {
        LOG_WARN("failed to push back table rel id", K(ret));
      }
    }
    // collect to_table index, group by ref_id
    for (int64_t i = 0; OB_SUCC(ret) && i < to_tables.count(); ++i) {
      const TableItem *to_table = to_tables.at(i);
      const int64_t *idx = NULL;
      if (OB_ISNULL(to_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret), K(i));
      } else if (!(to_table->is_basic_table() || to_table->is_link_table())
                 || OB_INVALID_ID == to_table->ref_id_) {
        // do nothing
      } else if (NULL == (idx = to_table_map.get(to_table->ref_id_))) {
        ObSEArray<int64_t,4> temp_array;
        if (OB_FAIL(temp_array.push_back(i))) {
          LOG_WARN("failed to push back rel id", K(ret));
        } else if (OB_FAIL(to_table_ids.push_back(temp_array))) {
          LOG_WARN("failed to push back array", K(ret), K(temp_array));
        } else if (OB_FAIL(to_table_map.set_refactored(to_table->ref_id_, to_table_ids.count() - 1))) {
          LOG_WARN("failed to set refactored", K(ret), K(to_table->ref_id_), K(to_table_ids));
        }
      } else if (OB_FAIL(to_table_ids.at(*idx).push_back(i))) {
        LOG_WARN("failed to push back table rel id", K(ret));
      }
    }

    // recursive generate base table map
    ObSqlBitSet<> used_to_table;
    ObSEArray<int64_t, 4> empty_map;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(empty_map.prepare_allocate(from_tables.count()))) {
      LOG_WARN("failed to prepare allocate map array", K(ret), K(from_tables.count()));
    } else if (OB_FAIL(inner_gen_base_table_map(0,
                                                from_tables,
                                                to_tables,
                                                from_table_num,
                                                to_table_ids,
                                                to_table_map,
                                                used_to_table,
                                                max_map_num,
                                                empty_map,
                                                table_maps))) {
      LOG_WARN("failed to generate base table map", K(ret));
    } else if (OB_FAIL(from_table_num.destroy())) {
      LOG_WARN("failed to destroy from table num hash map", K(ret));
    } else if (OB_FAIL(to_table_map.destroy())) {
      LOG_WARN("failed to destroy to table map hash map", K(ret));
    }
  }
  return ret;
}

// generate a base table map for from_table_idx-th from table
int ObTransformMVRewrite::inner_gen_base_table_map(int64_t from_table_idx,
                                                   const ObIArray<TableItem*> &from_tables,
                                                   const ObIArray<TableItem*> &to_tables,
                                                   hash::ObHashMap<uint64_t, int64_t> &from_table_num,
                                                   const ObIArray<ObSEArray<int64_t,4>> &to_table_ids,
                                                   const hash::ObHashMap<uint64_t, int64_t> &to_table_map,
                                                   ObSqlBitSet<> &used_to_table,
                                                   int64_t max_map_num,
                                                   ObIArray<int64_t> &current_map,
                                                   ObIArray<ObSEArray<int64_t,4>> &table_maps)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(from_table_idx < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected from table idx", K(ret), K(from_table_idx), K(from_tables));
  } else if (OB_UNLIKELY(current_map.count() != from_tables.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected map array count", K(ret), K(current_map.count()), K(from_tables.count()));
  } else if (from_table_idx >= from_tables.count()) {
    // add current map into table_maps
    ObSEArray<int64_t, 4> new_table_map;
    if (OB_FAIL(new_table_map.assign(current_map))) {
      LOG_WARN("failed to assign table map", K(ret), K(current_map));
    } else if (OB_FAIL(table_maps.push_back(new_table_map))) {
      LOG_WARN("failed to push back current_map", K(ret), K(new_table_map));
    } else {
      LOG_DEBUG("generated one table map", K(new_table_map));
    }
  } else {
    const TableItem *from_table = from_tables.at(from_table_idx);
    const int64_t *to_idx = NULL;
    if (OB_ISNULL(from_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("from table item is NULL", K(ret), K(from_table_idx));
    } else if (!(from_table->is_basic_table() || from_table->is_link_table())
               || OB_INVALID_ID == from_table->ref_id_
               || NULL == (to_idx = to_table_map.get(from_table->ref_id_))) {
      // table does not exists in to_tables, map from_table to nothing
      current_map.at(from_table_idx) = -1;
      if (OB_FAIL(SMART_CALL(inner_gen_base_table_map(from_table_idx + 1,
                                                      from_tables,
                                                      to_tables,
                                                      from_table_num,
                                                      to_table_ids,
                                                      to_table_map,
                                                      used_to_table,
                                                      max_map_num,
                                                      current_map,
                                                      table_maps)))) {
        LOG_WARN("failed to inner gen base table map", K(ret), K(from_table_idx));
      }
    } else if (OB_UNLIKELY(*to_idx < 0 || *to_idx >= to_table_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected to_idx", K(ret), K(*to_idx));
    } else {
      bool has_mapped = false;
      // try to map from_table to to_table which has same ref_id and not be used
      for (int64_t i = 0; OB_SUCC(ret) && max_map_num > table_maps.count()
           && i < to_table_ids.at(*to_idx).count(); ++i) {
        int64_t to_table_idx = to_table_ids.at(*to_idx).at(i);
        QueryRelation relation = QueryRelation::QUERY_UNCOMPARABLE;
        if (used_to_table.has_member(to_table_idx)) {
          // do nothing, to_table has been used
        } else if (OB_FAIL(ObStmtComparer::compare_basic_table_item(from_tables.at(from_table_idx),
                                                                    to_tables.at(to_table_idx),
                                                                    relation))) {
          LOG_WARN("failed to compare basic table", K(ret), K(from_table_idx), K(to_table_idx));
        } else if (QueryRelation::QUERY_EQUAL != relation) {
          // do nothing, from table and to table are not equal
        } else if (OB_FALSE_IT(has_mapped = true)) {
        } else if (OB_FAIL(used_to_table.add_member(to_table_idx))) {
          LOG_WARN("failed to add member", K(ret));
        } else if (OB_FALSE_IT(current_map.at(from_table_idx) = to_table_idx)) {
        } else if (OB_FAIL(SMART_CALL(inner_gen_base_table_map(from_table_idx + 1,
                                                               from_tables,
                                                               to_tables,
                                                               from_table_num,
                                                               to_table_ids,
                                                               to_table_map,
                                                               used_to_table,
                                                               max_map_num,
                                                               current_map,
                                                               table_maps)))) {
          LOG_WARN("failed to inner gen base table map", K(ret), K(from_table_idx));
        } else if (OB_FAIL(used_to_table.del_member(to_table_idx))) {
          LOG_WARN("failed to del member", K(ret));
        }
      }
      // try to map from_table to nothing
      int64_t from_num; // number of from tables with the same ref_id minus table has been mapped to -1
      if (OB_FAIL(ret) || max_map_num <= table_maps.count()) {
        // do nothing
      } else if (OB_FAIL(from_table_num.get_refactored(from_table->ref_id_, from_num))) {
        LOG_WARN("failed to get from table num", K(ret), KPC(from_table));
      } else if (has_mapped && from_num <= to_table_ids.at(*to_idx).count()) {
        // do nothing, the number of remaining unmapped from tables is less than or equal to
        // the number of remaining to tables, should not map from_table to nothing.
      } else if (OB_FAIL(from_table_num.set_refactored(from_table->ref_id_, from_num - 1, 1))) {
        LOG_WARN("failed to set from table num", K(ret), KPC(from_table));
      } else if (OB_FALSE_IT(current_map.at(from_table_idx) = -1)) {
      } else if (OB_FAIL(SMART_CALL(inner_gen_base_table_map(from_table_idx + 1,
                                                             from_tables,
                                                             to_tables,
                                                             from_table_num,
                                                             to_table_ids,
                                                             to_table_map,
                                                             used_to_table,
                                                             max_map_num,
                                                             current_map,
                                                             table_maps)))) {
        LOG_WARN("failed to inner gen base table map", K(ret), K(from_table_idx));
      } else if (OB_FAIL(from_table_num.set_refactored(from_table->ref_id_, from_num, 1))) {
        LOG_WARN("failed to set from table num", K(ret), KPC(from_table));
      }
    }
  }
  return ret;
}

int ObTransformMVRewrite::try_transform_with_one_mv(ObSelectStmt *origin_stmt,
                                                    MvInfo &mv_info,
                                                    ObSelectStmt *&new_stmt,
                                                    bool &transform_happened)
{
  int ret = OB_SUCCESS;
  transform_happened = false;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(mv_info.mv_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(origin_stmt), K(mv_info.mv_schema_));
  } else if (!mv_info.view_stmt_->is_spjg()) {
    // do nothing
    // TODO: try_transform_complete_mode
    OPT_TRACE(mv_info.mv_schema_->get_table_name(), "is not spjg");
  } else if (OB_FAIL(try_transform_contain_mode(origin_stmt, mv_info, new_stmt, transform_happened))) {
    LOG_WARN("failed to try transform contain mode", K(ret));
  } else if (!transform_happened) {
    // do nothing
  } else if (OB_FAIL(add_transform_hint(*new_stmt, &mv_info))) {
    LOG_WARN("failed to add transform hint", K(ret));
  } else {
    LOG_DEBUG("succeed to transform with one mv", KPC(new_stmt));
  }
  return ret;
}

int ObTransformMVRewrite::try_transform_contain_mode(ObSelectStmt *origin_stmt,
                                                     MvInfo &mv_info,
                                                     ObSelectStmt *&new_stmt,
                                                     bool &transform_happened)
{
  int ret = OB_SUCCESS;
  const int MAX_MAP_NUM = 10;
  ObSEArray<ObSEArray<int64_t,4>,10> base_table_maps;
  ObTryTransHelper try_trans_helper;
  ObDMLStmt *query_stmt = NULL;
  new_stmt = NULL;
  transform_happened = false;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(mv_info.mv_schema_) || OB_ISNULL(mv_info.view_stmt_)
      || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(origin_stmt), K(mv_info.mv_schema_), K(ctx_));
  } else if (OB_FAIL(gen_base_table_map(
                 origin_stmt->get_table_items(), mv_info.view_stmt_->get_table_items(), MAX_MAP_NUM, base_table_maps))) {
    LOG_WARN("failed to gen base table map", K(ret));
  } else if (OB_FAIL(try_trans_helper.fill_helper(origin_stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  } else {
    // try to rewrite use every base table map
    OPT_TRACE("try rewrite use", mv_info.mv_schema_->get_table_name());
    LOG_TRACE("begin try rewrite contain mode", K(mv_info.mv_schema_->get_table_name_str()), K(base_table_maps.count()));
    for (int64_t i = 0; OB_SUCC(ret) && !transform_happened && i < base_table_maps.count(); ++i) {
      bool is_valid = true;
      SMART_VAR(MvRewriteHelper,
                helper,
                *origin_stmt,
                mv_info,
                static_cast<ObSelectStmt*>(query_stmt),
                base_table_maps.at(i)) {
        if (NULL == query_stmt) {
          if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                       *ctx_->expr_factory_,
                                                       origin_stmt,
                                                       query_stmt))) {
            LOG_WARN("failed to deep copy stmt", K(ret));
          } else if (OB_ISNULL(helper.query_stmt_ = static_cast<ObSelectStmt*>(query_stmt))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("deep copy stmt is null", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(check_mv_rewrite_validity(helper, is_valid))) {
          LOG_WARN("failed to check mv rewrite validity", K(ret), K(base_table_maps.at(i)));
        } else if (!is_valid) {
          LOG_TRACE("does not pass the rewrite validity check", K(base_table_maps.at(i)));
        } else if (OB_FAIL(generate_rewrite_stmt_contain_mode(helper))) {
          LOG_WARN("failed to generate rewrite stmt contain mode", K(ret), K(base_table_maps.at(i)));
        } else if (OB_FAIL(check_rewrite_expected(helper, is_valid))) {
          LOG_WARN("failed to check rewrite expected", K(ret), K(base_table_maps.at(i)));
        } else if (!is_valid) {
          if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, query_stmt))) {
            LOG_WARN("failed to free stmt", K(ret));
          } else {
            // query_stmt has been rewrited in generate_rewrite_stmt_contain_mode, need to re-copy stmt
            query_stmt = NULL;
            LOG_TRACE("does not pass the rewrite expected check", K(base_table_maps.at(i)));
          }
        } else if (OB_FAIL(add_param_constraint(helper))) {
          LOG_WARN("failed to add param constraint", K(ret));
        } else {
          new_stmt = helper.query_stmt_;
          transform_happened = true;
        }
        if (OB_FAIL(ret)){
        } else if (OB_FAIL(try_trans_helper.finish(transform_happened, origin_stmt->get_query_ctx(), ctx_))) {
          LOG_WARN("failed to finish try trans helper", K(ret));
        }
      }
    }
    if (OB_FAIL(ret) || transform_happened) {
      // do nothing
    } else if (NULL != query_stmt
               && OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, query_stmt))) {
      LOG_WARN("failed to free stmt", K(ret));
    } else if (MAX_MAP_NUM == base_table_maps.count()) {
      OPT_TRACE(mv_info.mv_schema_->get_table_name(), ": reach the base table map try count limit!");
    } else {
      OPT_TRACE(mv_info.mv_schema_->get_table_name(), ": stmt is not matched or cost raised");
    }
  }
  return ret;
}

int ObTransformMVRewrite::check_mv_rewrite_validity(MvRewriteHelper &helper,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_delta_table(helper, is_valid))) {
    LOG_WARN("failed to check delta table", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("does not pass the mv delta table check");
  } else if (OB_FAIL(check_join_compatibility(helper, is_valid))) {
    LOG_WARN("failed to check join compatibility", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("does not pass the join compatibility check");
  } else if (OB_FAIL(check_predicate_compatibility(helper, is_valid))) {
    LOG_WARN("failed to check predicate compatibility", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("does not pass the predicate compatibility check");
  } else if (!helper.query_compensation_preds_.empty()) {
    // not support now
    // TODO: try union rewrite
    is_valid = false;
    LOG_TRACE("does not pass the predicate compatibility check, has query compensation predicate");
  } else if (OB_FAIL(check_group_by_col(helper, is_valid))) {
    LOG_WARN("failed to check group by column", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("does not pass the group by column check");
  } else if (OB_FAIL(compute_stmt_expr_map(helper, is_valid))) {
    LOG_WARN("failed to compute expr map", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("does not pass the expr map check");
  } else if (OB_FAIL(check_opt_feat_ctrl(helper, is_valid))) {
    LOG_WARN("failed to check optimizer feature control", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("does not pass the optimizer feature control check");
  }
  return ret;
}

int ObTransformMVRewrite::check_delta_table(MvRewriteHelper &helper,
                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *mv_stmt = helper.mv_info_.view_stmt_;
  ObSqlBitSet<> mv_common_table; // table exists in both query and mv, used to collect mv delta tables
  is_valid = true;
  if (OB_ISNULL(mv_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(helper.mv_info_));
  }
  // collect query delta tables, but do not need to check. query delta table
  // validity will be checked in build_query_join_tree().
  for (int64_t i = 0; OB_SUCC(ret) && i < helper.base_table_map_.count(); ++i) {
    if (helper.base_table_map_.at(i) == -1) {
      if (OB_FAIL(helper.query_delta_table_.add_member(i + 1))) {
        LOG_WARN("failed to add member to query delta table", K(ret));
      }
    } else if (OB_FAIL(mv_common_table.add_member(helper.base_table_map_.at(i) + 1))) {
      LOG_WARN("failed to add member to mv common table", K(ret));
    }
  }
  if (OB_SUCC(ret) && !helper.query_delta_table_.is_empty() && mv_stmt->has_group_by()) {
    // not allow query delta table when mv has group by
    is_valid = false;
  }
  // collect and check the mv delta tables.
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < mv_stmt->get_table_size(); ++i) {
    if (mv_common_table.has_member(i + 1)) {
      // do nothing
    } else if (OB_FAIL(helper.mv_delta_table_.add_member(i + 1))) {
      LOG_WARN("failed to add member to mv delta table", K(ret));
    } else {
      is_valid = false;
      // TODO: need to check whether mv delta table is legal here
      // we do not allow mv delta table now
    }
  }
  return ret;
}

int ObTransformMVRewrite::check_join_compatibility(MvRewriteHelper &helper,
                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(helper.query_stmt_) || OB_ISNULL(helper.mv_info_.view_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else {
    // generate conflict detectors
    ObSEArray<TableItem*, 8> mv_from_tables;
    ObSEArray<TableItem*, 8> query_from_tables;
    ObSEArray<ObRawExpr*,4> mv_conditions;
    ObSEArray<ObRawExpr*,4> query_conditions;
    ObSEArray<ObConflictDetector*, 8> mv_conflict_detectors;
    ObSEArray<ObSEArray<ObRawExpr*,4>, 8> mv_baserel_filters;
    ObSEArray<ObConflictDetector*, 8> query_conflict_detectors;
    ObSEArray<ObSEArray<ObRawExpr*,4>, 8> query_baserel_filters;
    ObSEArray<ObRelIds, 8> bushy_tree_infos;
    ObSEArray<ObRawExpr*, 4> new_or_quals;
    ObSEArray<TableDependInfo, 1> fake_depend_info; // WARNING query should not have depend info
    ObConflictDetectorGenerator generator(*ctx_->allocator_,
                                          *ctx_->expr_factory_,
                                          ctx_->session_info_,
                                          NULL,  /* onetime_copier */
                                          true,  /* should_deduce_conds */
                                          false, /* should_pushdown_const_filters */
                                          fake_depend_info,
                                          bushy_tree_infos,
                                          new_or_quals);
    STOP_OPT_TRACE;
    if (OB_FAIL(helper.mv_info_.view_stmt_->get_from_tables(mv_from_tables))) {
      LOG_WARN("failed to get mv from table items", K(ret));
    } else if (OB_FAIL(helper.query_stmt_->get_from_tables(query_from_tables))) {
      LOG_WARN("failed to get query from table items", K(ret));
    } else if (OB_FAIL(pre_process_quals(helper.mv_info_.view_stmt_->get_condition_exprs(),
                                         mv_conditions,
                                         helper.mv_conds_))) {
      LOG_WARN("failed to pre process mv quals", K(ret));
    } else if (OB_FAIL(pre_process_quals(helper.query_stmt_->get_condition_exprs(),
                                         query_conditions,
                                         helper.query_delta_table_.is_empty() ?
                                         helper.query_conds_ :
                                         helper.query_other_conds_))) {
      LOG_WARN("failed to pre process query quals", K(ret));
    } else if (OB_FAIL(generator.generate_conflict_detectors(helper.mv_info_.view_stmt_,
                                                             mv_from_tables,
                                                             helper.mv_info_.view_stmt_->get_semi_infos(),
                                                             mv_conditions,
                                                             mv_baserel_filters,
                                                             mv_conflict_detectors))) {
      LOG_WARN("failed to generate mv conflict detectors", K(ret));
    } else if (OB_FAIL(generator.generate_conflict_detectors(helper.query_stmt_,
                                                             query_from_tables,
                                                             helper.query_stmt_->get_semi_infos(),
                                                             query_conditions,
                                                             query_baserel_filters,
                                                             query_conflict_detectors))) {
      LOG_WARN("failed to generate query conflict detectors", K(ret));
    }
    RESUME_OPT_TRACE;

    // build and check join tree
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(build_mv_join_tree(helper, mv_baserel_filters, mv_conflict_detectors))) {
      LOG_WARN("failed to build mv join tree", K(ret));
    } else if (OB_FAIL(build_query_join_tree(helper, query_baserel_filters, query_conflict_detectors, is_valid))) {
      LOG_WARN("failed to build query join tree", K(ret));
    } else if (!is_valid) {
      LOG_DEBUG("does not pass the query join tree build");
    } else if (OB_FAIL(compare_join_tree(helper,
                                         helper.mv_tree_,
                                         helper.query_tree_mv_part_,
                                         true, /* can_compensate */
                                         is_valid))) {
      LOG_WARN("failed to compare join tree", K(ret));
    } else if (!is_valid) {
      LOG_DEBUG("does not pass the join tree compare");
    }
  }
  return ret;
}

int ObTransformMVRewrite::pre_process_quals(const ObIArray<ObRawExpr*> &input_conds,
                                            ObIArray<ObRawExpr*> &normal_conds,
                                            ObIArray<ObRawExpr*> &other_conds)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_conds.count(); ++i) {
    ObRawExpr *cond = input_conds.at(i);
    if (OB_ISNULL(cond)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (cond->has_flag(CNT_ROWNUM) || cond->has_flag(CNT_SUB_QUERY) ||
               !cond->is_deterministic() || cond->is_const_expr()) {
      if (OB_FAIL(other_conds.push_back(cond))) {
        LOG_WARN("failed to push back other conds", K(ret));
      }
    } else if (OB_FAIL(normal_conds.push_back(cond))) {
      LOG_WARN("failed to push back other conds", K(ret));
    }
  }
  return ret;
}

int ObTransformMVRewrite::build_mv_join_tree(MvRewriteHelper &helper,
                                             ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                                             ObIArray<ObConflictDetector*> &conflict_detectors)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *mv_stmt = helper.mv_info_.view_stmt_;
  // here we only need one array to store used conflict detectors
  ObSEArray<ObConflictDetector*, 8> used_detectors;
  JoinTreeNode *root_node = NULL;
  if (OB_ISNULL(mv_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_stmt->get_from_item_size(); ++i) {
    JoinTreeNode *current_node = NULL;
    JoinTreeNode *new_joined_node = NULL;
    bool is_valid = false;
    if (OB_FAIL(inner_build_mv_join_tree(helper,
                                         mv_stmt->get_table_item(mv_stmt->get_from_item(i)),
                                         baserel_filters,
                                         conflict_detectors,
                                         used_detectors,
                                         current_node))) {
      LOG_WARN("failed to build mv join tree", K(ret), KPC(mv_stmt->get_table_item(mv_stmt->get_from_item(i))));
    } else if (OB_ISNULL(current_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join tree node is null", K(ret), K(current_node));
    } else if (i == 0) {
      root_node = current_node;
    } else if (OB_FAIL(build_join_tree_node(root_node,     /* left child node */
                                            current_node,  /* right child node */
                                            conflict_detectors,
                                            used_detectors,
                                            new_joined_node,
                                            is_valid))) {
      LOG_WARN("failed to build tree node", K(ret));
    } else if (OB_UNLIKELY(!is_valid)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no valid detector when generating mv join tree", K(ret));
    } else {
      root_node = new_joined_node;
    }
  }
  if (OB_SUCC(ret)) {
    helper.mv_tree_ = root_node;
    LOG_DEBUG("build mv join tree", KPC(helper.mv_tree_));
  }
  return ret;
}

int ObTransformMVRewrite::inner_build_mv_join_tree(MvRewriteHelper &helper,
                                                   const TableItem *table,
                                                   ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                                                   ObIArray<ObConflictDetector*> &conflict_detectors,
                                                   ObIArray<ObConflictDetector*> &used_detectors,
                                                   JoinTreeNode *&node)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *mv_stmt = helper.mv_info_.view_stmt_;
  int64_t rel_id;
  if (OB_ISNULL(table) || OB_ISNULL(mv_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table), K(mv_stmt));
  } else if (table->is_joined_table()) {
    const JoinedTable *join_table = static_cast<const JoinedTable*>(table);
    JoinTreeNode *left_node = NULL;
    JoinTreeNode *right_node = NULL;
    bool is_valid = false;
    if (OB_FAIL(SMART_CALL(inner_build_mv_join_tree(helper,
                                                    join_table->left_table_,
                                                    baserel_filters,
                                                    conflict_detectors,
                                                    used_detectors,
                                                    left_node)))) {
      LOG_WARN("failed to build left mv join tree", K(ret), KPC(join_table->left_table_));
    } else if (OB_FAIL(SMART_CALL(inner_build_mv_join_tree(helper,
                                                           join_table->right_table_,
                                                           baserel_filters,
                                                           conflict_detectors,
                                                           used_detectors,
                                                           right_node)))) {
      LOG_WARN("failed to build right mv join tree", K(ret), KPC(join_table->right_table_));
    } else if (OB_FAIL(build_join_tree_node(left_node,
                                            right_node,
                                            conflict_detectors,
                                            used_detectors,
                                            node,
                                            is_valid))) {
      LOG_WARN("failed to build tree node", K(ret));
    } else if (OB_UNLIKELY(!is_valid)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no valid detector when generating mv join tree", K(ret));
    }
  } else if (OB_FALSE_IT(rel_id = mv_stmt->get_table_bit_index(table->table_id_))) {
  } else if (OB_FAIL(build_leaf_tree_node(rel_id, table->table_id_, baserel_filters, node))) {
    LOG_WARN("failed to build leaf tree node", K(ret), K(rel_id));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv join tree node is null", K(ret), KPC(table));
  } else {
    node->ori_table_ = table;
  }
  return ret;
}

/**
 * For  MV:    select ... from t1 left join t2 on t1.c1 = t2.c2;
 *      Query: select ... from t1 left join t2 on t1.c1 = t2.c2, t3 where t3.c1 = t1.c2;
 *
 * The join trees of MV and Query are:
 *  MV:                    Query:
 *                                            Inner Join    ─┐
 *        Left Join                            /      \      ├─ (delta part)
 *        /       \                   ┌─   Left Join    t3  ─┘
 *       t1       t2       (mv part) ─┤    /       \
 *                                    └─  t1       t2
 *
 * The Left Join node in Query join tree is called query_tree_mv_part_, which has same
 * structure as the MV join tree.
 */
int ObTransformMVRewrite::build_query_join_tree(MvRewriteHelper &helper,
                                                ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                                                ObIArray<ObConflictDetector*> &conflict_detectors,
                                                bool &is_valid)
{
  int ret = OB_SUCCESS;
  // here we only need one array to store used conflict detectors
  ObSEArray<ObConflictDetector*, 8> used_detectors;
  // build mv part
  // post order traversal the mv join tree, and build join tree with the same structure
  if (OB_ISNULL(helper.query_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query stmt in null", K(ret));
  } else if (OB_FAIL(inner_build_query_tree_mv_part(helper,
                                                    helper.mv_tree_,
                                                    baserel_filters,
                                                    conflict_detectors,
                                                    used_detectors,
                                                    helper.query_tree_mv_part_,
                                                    is_valid))) {
    LOG_WARN("failed to build query tree mv part", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("query tree mv part is invalid");
  } else if (OB_ISNULL(helper.query_tree_ = helper.query_tree_mv_part_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query tree in null", K(ret));
  } else {
    // build query delta part
    // post order traversal each from table, and build join tree node for query delta table
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < helper.query_stmt_->get_from_item_size(); ++i) {
      JoinTreeNode *current_node = NULL;
      JoinTreeNode *new_joined_node = NULL;
      bool is_delta_table;
      if (OB_FAIL(inner_build_query_tree_delta_part(helper,
                                                    helper.query_stmt_->get_table_item(helper.query_stmt_->get_from_item(i)),
                                                    baserel_filters,
                                                    conflict_detectors,
                                                    used_detectors,
                                                    current_node,
                                                    is_delta_table,
                                                    is_valid))) {
        LOG_WARN("failed to build query tree delta part", K(ret), KPC(helper.query_stmt_->get_table_item(helper.query_stmt_->get_from_item(i))));
      } else if (!is_valid) {
        LOG_TRACE("query tree delta part is invalid");
      } else if (!is_delta_table) {
        // do nothing, delta table has been merged in query tree
      } else if (OB_FAIL(build_join_tree_node(helper.query_tree_, /* left child node */
                                              current_node,       /* right child node */
                                              conflict_detectors,
                                              used_detectors,
                                              new_joined_node,
                                              is_valid))) {
        LOG_WARN("failed to build query tree node, delta part", K(ret));
      } else if (!is_valid) {
        LOG_TRACE("no valid detector when generating query join tree, delta part");
      } else {
        helper.query_tree_ = new_joined_node;
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      LOG_DEBUG("build query join tree", KPC(helper.query_tree_));
    }
  }
  return ret;
}

int ObTransformMVRewrite::inner_build_query_tree_mv_part(MvRewriteHelper &helper,
                                                         JoinTreeNode *mv_node,
                                                         ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                                                         ObIArray<ObConflictDetector*> &conflict_detectors,
                                                         ObIArray<ObConflictDetector*> &used_detectors,
                                                         JoinTreeNode *&node,
                                                         bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1> mv_relids;
  int64_t query_relid;
  TableItem *base_table;
  is_valid = true;
  if (OB_ISNULL(helper.query_stmt_) || OB_ISNULL(mv_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(helper.query_stmt_), K(mv_node));
  } else if (mv_node->join_info_.join_type_ != UNKNOWN_JOIN) {
    // not a leaf node
    JoinTreeNode *left_node = NULL;
    JoinTreeNode *right_node = NULL;
    if (OB_FAIL(SMART_CALL(inner_build_query_tree_mv_part(helper,
                                                          mv_node->left_child_,
                                                          baserel_filters,
                                                          conflict_detectors,
                                                          used_detectors,
                                                          left_node,
                                                          is_valid)))) {
      LOG_WARN("failed to build left child", K(ret));
    } else if (!is_valid) {
      LOG_DEBUG("left child is invalid");
    } else if (OB_FAIL(SMART_CALL(inner_build_query_tree_mv_part(helper,
                                                                 mv_node->right_child_,
                                                                 baserel_filters,
                                                                 conflict_detectors,
                                                                 used_detectors,
                                                                 right_node,
                                                                 is_valid)))) {
      LOG_WARN("failed to build right child", K(ret));
    } else if (!is_valid) {
      LOG_DEBUG("right child is invalid");
    } else if (OB_FAIL(build_join_tree_node(left_node,
                                            right_node,
                                            conflict_detectors,
                                            used_detectors,
                                            node,
                                            is_valid))) {
      LOG_WARN("failed to build tree node", K(ret));
    } else if (!is_valid) {
      LOG_DEBUG("no valid detector when generating query join tree, mv part");
    }
  } else if (OB_FAIL(mv_node->table_set_.to_array(mv_relids))) {
    LOG_WARN("failed to convert table set to array", K(ret));
  } else if (OB_UNLIKELY(1 != mv_relids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv leaf node has multi tables", K(ret), K(mv_relids.count()));
  } else if (OB_FALSE_IT(query_relid = find_query_rel_id(helper, mv_relids.at(0)))) {
    // mv delta table has been handled in check_delta_table(), here query_relid should not be -1
  } else if (OB_UNLIKELY(query_relid < 1 || query_relid > helper.query_stmt_->get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected query table rel id", K(ret), K(query_relid), K(helper.query_stmt_->get_table_size()), K(mv_relids.at(0)));
  } else if (OB_ISNULL(base_table = helper.query_stmt_->get_table_item(query_relid - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(query_relid));
  } else if (OB_FAIL(build_leaf_tree_node(query_relid,
                                          base_table->table_id_,
                                          baserel_filters,
                                          node))) {
    LOG_WARN("failed to build query tree leaf node, mv part", K(ret), K(query_relid), K(mv_relids.at(0)));
  }
  return ret;
}

/**
 * @param is_delta_table:
 * TRUE: all child tables are query delta table, generate a new join node,
 *       and node has not been merged into helper.query_tree_
 * FALSE: table is not a delta table or some of child tables are not delta table,
 *        all child nodes has been merged into helper.query_tree_
 */
int ObTransformMVRewrite::inner_build_query_tree_delta_part(MvRewriteHelper &helper,
                                                            const TableItem *table,
                                                            ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                                                            ObIArray<ObConflictDetector*> &conflict_detectors,
                                                            ObIArray<ObConflictDetector*> &used_detectors,
                                                            JoinTreeNode *&node,
                                                            bool &is_delta_table,
                                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  int64_t query_relid;
  is_delta_table = false;
  is_valid = true;
  if (OB_ISNULL(helper.query_stmt_) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table));
  } else if (table->is_joined_table()) {
    // if both left and right child are delta tables, generate a new join node
    // if only one child is delta table, merge the child tree node into query tree
    const JoinedTable *join_table = static_cast<const JoinedTable*>(table);
    JoinTreeNode *left_node = NULL;
    JoinTreeNode *right_node = NULL;
    bool left_is_delta_table = false;
    bool right_is_delta_table = false;
    if (OB_FAIL(SMART_CALL(inner_build_query_tree_delta_part(helper,
                                                             join_table->left_table_,
                                                             baserel_filters,
                                                             conflict_detectors,
                                                             used_detectors,
                                                             left_node,
                                                             left_is_delta_table,
                                                             is_valid)))) {
      LOG_WARN("failed to build left child", K(ret));
    } else if (!is_valid) {
      LOG_DEBUG("left child is invalid");
    } else if (OB_FAIL(SMART_CALL(inner_build_query_tree_delta_part(helper,
                                                                    join_table->right_table_,
                                                                    baserel_filters,
                                                                    conflict_detectors,
                                                                    used_detectors,
                                                                    right_node,
                                                                    right_is_delta_table,
                                                                    is_valid)))) {
      LOG_WARN("failed to build right child", K(ret));
    } else if (!is_valid) {
      LOG_DEBUG("right child is invalid");
    } else if (left_is_delta_table && right_is_delta_table) {
      if (OB_FAIL(build_join_tree_node(left_node,
                                       right_node,
                                       conflict_detectors,
                                       used_detectors,
                                       node,
                                       is_valid))) {
        LOG_WARN("failed to build query tree node, delta part", K(ret));
      } else if (!is_valid) {
        LOG_DEBUG("no valid detector when generating query join tree, delta part");
      } else {
        is_delta_table = true;
      }
    } else if (left_is_delta_table || right_is_delta_table) {
      if (OB_FAIL(build_join_tree_node(helper.query_tree_,
                                       left_is_delta_table ? left_node : right_node,
                                       conflict_detectors,
                                       used_detectors,
                                       node,
                                       is_valid))) {
        LOG_WARN("failed to build query tree node, delta part", K(ret));
      } else if (!is_valid) {
        LOG_DEBUG("no valid detector when generating query join tree, delta part");
      } else {
        helper.query_tree_ = node;
        is_delta_table = false;
      }
    } else {
      // not a query delta table
      is_delta_table = false;
    }
  } else if (OB_FALSE_IT(query_relid = helper.query_stmt_->get_table_bit_index(table->table_id_))) {
  } else if (!helper.query_delta_table_.has_member(query_relid)) {
    // not a query delta table
    is_delta_table = false;
  } else if (OB_FAIL(build_leaf_tree_node(query_relid, table->table_id_, baserel_filters, node))) {
    LOG_WARN("failed to build query tree leaf node, delta part", K(ret), K(query_relid));
  } else {
    is_delta_table = true;
  }
  return ret;
}

int ObTransformMVRewrite::build_join_tree_node(JoinTreeNode *left_node,
                                               JoinTreeNode *right_node,
                                               ObIArray<ObConflictDetector*> &conflict_detectors,
                                               ObIArray<ObConflictDetector*> &used_detectors,
                                               JoinTreeNode *&new_node,
                                               bool &is_valid)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  is_valid = true;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(left_node) || OB_ISNULL(right_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(left_node), K(right_node));
  } else if (OB_ISNULL(ptr = ctx_->allocator_->alloc(sizeof(JoinTreeNode)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate join tree node", K(ret));
  } else if (OB_FALSE_IT(new_node = new(ptr) JoinTreeNode())) {
  } else if (OB_FAIL(new_node->table_set_.add_members(left_node->table_set_))) {
    LOG_WARN("failed to add left table rel ids", K(ret));
  } else if (OB_FAIL(new_node->table_set_.add_members(right_node->table_set_))) {
    LOG_WARN("failed to add right table rel ids", K(ret));
  } else {
    new_node->left_child_ = left_node;
    new_node->right_child_ = right_node;
    // generate join info for new node
    bool is_strict_order;
    ObSEArray<ObConflictDetector*, 4> valid_detectors;
    // fake conflict detectors is a empty array to imitate right_used_detectors
    ObSEArray<ObConflictDetector*, 1> fake_conflict_detectors;
    // WARNING query should not have depend info
    ObSEArray<TableDependInfo, 1> fake_depend_info;
    if (OB_FAIL(ObConflictDetector::choose_detectors(left_node->table_set_,
                                                     right_node->table_set_,
                                                     used_detectors,
                                                     fake_conflict_detectors,
                                                     fake_depend_info,
                                                     conflict_detectors,
                                                     valid_detectors,
                                                     false, /* delay_cross_product */
                                                     is_strict_order))) {
      LOG_WARN("failed to choose join info", K(ret));
    } else if (valid_detectors.empty()) {
      // can not generate valid join tree
      is_valid = false;
      LOG_TRACE("no valid detector when generating join tree");
    } else if (OB_FAIL(append(used_detectors, valid_detectors))) {
      LOG_WARN("failed to append used conflict detectors", K(ret));
    } else if (OB_FAIL(ObConflictDetector::merge_join_info(valid_detectors, new_node->join_info_))) {
      LOG_WARN("failed to merge join info", K(ret));
    } else if (!is_strict_order) {
      new_node->join_info_.join_type_ = get_opposite_join_type(new_node->join_info_.join_type_);
    }
  }
  return ret;
}

int ObTransformMVRewrite::build_leaf_tree_node(int64_t rel_id,
                                               uint64_t table_id,
                                               ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                                               JoinTreeNode *&leaf_node)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (OB_ISNULL(ptr = ctx_->allocator_->alloc(sizeof(JoinTreeNode)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate join tree node", K(ret));
  } else if (OB_FALSE_IT(leaf_node = new(ptr) JoinTreeNode())) {
  } else if (OB_FAIL(leaf_node->table_set_.add_member(rel_id))) {
    LOG_WARN("failed to add table rel id", K(ret));
  } else if (OB_UNLIKELY(rel_id < 1 || rel_id > baserel_filters.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected query table rel id", K(ret), K(rel_id), K(baserel_filters.count()));
  } else if (OB_FAIL(append(leaf_node->join_info_.where_conditions_,
                            baserel_filters.at(rel_id - 1)))) {
    LOG_WARN("failed to append where conditions", K(ret));
  } else {
    leaf_node->table_id_ = table_id;
    leaf_node->join_info_.join_type_ = UNKNOWN_JOIN;
  }
  return ret;
}

int ObTransformMVRewrite::find_query_rel_id(MvRewriteHelper &helper, int64_t mv_relid)
{
  int query_rel_id = -1;
  if (mv_relid > 0) {
    for (int64_t i = 0; i < helper.base_table_map_.count(); ++i) {
      if (helper.base_table_map_.at(i) == mv_relid - 1) {
        query_rel_id = i + 1;
        break;
      }
    }
  }
  return query_rel_id;
}

int ObTransformMVRewrite::compare_join_tree(MvRewriteHelper &helper,
                                            JoinTreeNode *mv_node,
                                            JoinTreeNode *query_node,
                                            bool can_compensate,
                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(mv_node) || OB_ISNULL(query_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_node), K(query_node));
  } else if (mv_node->join_info_.join_type_ == UNKNOWN_JOIN) {
    // is leaf node
    ObSEArray<int64_t, 1> mv_relids;
    ObSEArray<int64_t, 1> query_relids;
    if (OB_UNLIKELY(query_node->join_info_.join_type_ != UNKNOWN_JOIN)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query trees are not matched", K(ret), KPC(mv_node), KPC(query_node));
    } else if (OB_FAIL(mv_node->table_set_.to_array(mv_relids))) {
      LOG_WARN("failed to convert mv table set to array", K(ret));
    } else if (OB_UNLIKELY(1 != mv_relids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv leaf node has multi tables", K(ret), K(mv_relids.count()));
    } else if (OB_FAIL(query_node->table_set_.to_array(query_relids))) {
      LOG_WARN("failed to convert query table set to array", K(ret));
    } else if (OB_UNLIKELY(1 != query_relids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query leaf node has multi tables", K(ret), K(query_relids.count()));
    } else if (OB_UNLIKELY(query_relids.at(0) != find_query_rel_id(helper, mv_relids.at(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query trees are not matched", K(ret), KPC(mv_node), KPC(query_node));
    } else if (can_compensate) {
      if (OB_FAIL(append(helper.mv_conds_, mv_node->join_info_.where_conditions_))) {
        LOG_WARN("failed to append mv conditions", K(ret));
      } else if (OB_FAIL(append(helper.query_conds_, query_node->join_info_.where_conditions_))) {
        LOG_WARN("failed to append query conditions", K(ret));
      }
    } else if (OB_FAIL(compare_join_conds(helper,
                                          mv_node->join_info_.where_conditions_,
                                          query_node->join_info_.where_conditions_,
                                          is_valid))) {
      LOG_WARN("failed to compare join conditions", K(ret));
    }
  } else if (OB_FAIL(SMART_CALL(compare_join_tree(helper,
                                                  mv_node->left_child_,
                                                  query_node->left_child_,
                                                  can_compensate &&
                                                  (INNER_JOIN == query_node->join_info_.join_type_
                                                   || LEFT_OUTER_JOIN == query_node->join_info_.join_type_
                                                   || LEFT_SEMI_JOIN == query_node->join_info_.join_type_
                                                   || LEFT_ANTI_JOIN == query_node->join_info_.join_type_),
                                                  is_valid)))) {
    LOG_WARN("failed to compare left join tree", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(SMART_CALL(compare_join_tree(helper,
                                                  mv_node->right_child_,
                                                  query_node->right_child_,
                                                  can_compensate &&
                                                  (INNER_JOIN == query_node->join_info_.join_type_
                                                   || RIGHT_OUTER_JOIN == query_node->join_info_.join_type_
                                                   || RIGHT_SEMI_JOIN == query_node->join_info_.join_type_
                                                   || RIGHT_ANTI_JOIN == query_node->join_info_.join_type_),
                                                  is_valid)))) {
    LOG_WARN("failed to compare right join tree", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (can_compensate) {
    // check join type is compatible, compare join conditions and pull up join filter
    if (OB_FAIL(compare_join_type(helper, mv_node, query_node, is_valid))) {
      LOG_WARN("failed to compare join type", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (INNER_JOIN == query_node->join_info_.join_type_) {
      // inner join does not need to compare join conditions
      // just pull up join filter simply
      // mv_node may no be inner join, we also need to pull up on_conditions_
      if (OB_FAIL(append(helper.mv_conds_, mv_node->join_info_.where_conditions_))) {
        LOG_WARN("failed to append mv where conditions", K(ret));
      } else if (OB_FAIL(append(helper.mv_conds_, mv_node->join_info_.on_conditions_))) {
        LOG_WARN("failed to append mv on conditions", K(ret));
      } else if (OB_FAIL(append(helper.query_conds_, query_node->join_info_.where_conditions_))) {
        LOG_WARN("failed to append query conditions", K(ret));
      }
    } else if (OB_FAIL(compare_join_conds(helper,
                                          mv_node->join_info_.on_conditions_,
                                          query_node->join_info_.on_conditions_,
                                          is_valid))) {
      LOG_WARN("failed to compare join conditions", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(append(helper.mv_conds_, mv_node->join_info_.where_conditions_))) {
      LOG_WARN("failed to append mv conditions", K(ret));
    } else if (OB_FAIL(append(helper.query_conds_, query_node->join_info_.where_conditions_))) {
      LOG_WARN("failed to append query conditions", K(ret));
    }
  } else if (mv_node->join_info_.join_type_ != query_node->join_info_.join_type_) {
    is_valid = false;
    LOG_TRACE("join type not match", K(mv_node->join_info_), K(query_node->join_info_));
  } else if (OB_FAIL(compare_join_conds(helper,
                                        mv_node->join_info_.where_conditions_,
                                        query_node->join_info_.where_conditions_,
                                        is_valid))) {
    LOG_WARN("failed to compare join conditions", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(compare_join_conds(helper,
                                        mv_node->join_info_.on_conditions_,
                                        query_node->join_info_.on_conditions_,
                                        is_valid))) {
    LOG_WARN("failed to compare join conditions", K(ret));
  } else if (!is_valid) {
    // do nothing
  }
  return ret;
}

// check whether mv_conds and query_conds are same
int ObTransformMVRewrite::compare_join_conds(MvRewriteHelper &helper,
                                             const ObIArray<ObRawExpr*> &mv_conds,
                                             const ObIArray<ObRawExpr*> &query_conds,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSqlBitSet<> mv_checked_conds;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < query_conds.count(); ++i) {
    bool has_found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !has_found && j < mv_conds.count(); ++j) {
      if (OB_FAIL(is_same_condition(helper, query_conds.at(i), mv_conds.at(j), has_found))) {
        LOG_WARN("failed to compare join conditions", K(ret));
      } else if (!has_found) {
        // do nothing
      } else if (OB_FAIL(mv_checked_conds.add_member(j))) {
        LOG_WARN("failed to add member", K(ret));
      }
    }
    if (OB_SUCC(ret) && !has_found) {
      is_valid = false;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < mv_conds.count(); ++i) {
    bool has_found = false;
    if (mv_checked_conds.has_member(i)) {
      has_found = true;
    }
    for (int64_t j = 0; OB_SUCC(ret) && !has_found && j < query_conds.count(); ++j) {
      if (OB_FAIL(is_same_condition(helper, query_conds.at(j), mv_conds.at(i), has_found))) {
        LOG_WARN("failed to compare join conditions", K(ret));
      }
    }
    if (OB_SUCC(ret) && !has_found) {
      is_valid = false;
    }
  }
  return ret;
}

// check whether the join type is compatible and add a not null compensation predicate
int ObTransformMVRewrite::compare_join_type(MvRewriteHelper &helper,
                                            JoinTreeNode *mv_node,
                                            JoinTreeNode *query_node,
                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(mv_node) || OB_ISNULL(query_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_node), K(query_node));
  } else if (mv_node->join_info_.join_type_ == query_node->join_info_.join_type_) {
    // do nothing
  } else {
    switch(query_node->join_info_.join_type_) {
      case INNER_JOIN:
        if (LEFT_OUTER_JOIN == mv_node->join_info_.join_type_) {
          if (OB_FAIL(add_not_null_compensate(helper, mv_node, false, is_valid))) {
            LOG_WARN("failed to add not null compensate", K(ret));
          }
        } else if (RIGHT_OUTER_JOIN == mv_node->join_info_.join_type_) {
          if (OB_FAIL(add_not_null_compensate(helper, mv_node, true, is_valid))) {
            LOG_WARN("failed to add not null compensate", K(ret));
          }
        } else if (FULL_OUTER_JOIN == mv_node->join_info_.join_type_) {
          if (OB_FAIL(add_not_null_compensate(helper, mv_node, true, is_valid))) {
            LOG_WARN("failed to add not null compensate", K(ret));
          } else if (!is_valid) {
            // do nothing
          } else if (OB_FAIL(add_not_null_compensate(helper, mv_node, false, is_valid))) {
            LOG_WARN("failed to add not null compensate", K(ret));
          }
        } else {
          is_valid = false;
        }
        break;
      case LEFT_OUTER_JOIN:
      case RIGHT_OUTER_JOIN:
        if (FULL_OUTER_JOIN == mv_node->join_info_.join_type_) {
          if (OB_FAIL(add_not_null_compensate(helper,
                                              mv_node,
                                              LEFT_OUTER_JOIN == query_node->join_info_.join_type_,
                                              is_valid))) {
            LOG_WARN("failed to add not null compensate", K(ret));
          }
        } else {
          is_valid = false;
        }
        break;
      case LEFT_SEMI_JOIN:
      case RIGHT_SEMI_JOIN:
        // not support now
        is_valid = false;
        break;
      case LEFT_ANTI_JOIN:
      case RIGHT_ANTI_JOIN:
        // not support now
        is_valid = false;
        break;
      default:
        is_valid = false;
        break;
    }
  }
  return ret;
}

int ObTransformMVRewrite::add_not_null_compensate(MvRewriteHelper &helper,
                                                  JoinTreeNode *mv_upper_node,
                                                  bool comp_left_child,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObRawExpr *not_null_expr = NULL;
  ObRawExpr *compensate_expr = NULL;
  is_valid = true;
  if (OB_ISNULL(mv_upper_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_upper_node));
  } else if (OB_FAIL(find_mv_not_null_expr(helper,
                                           mv_upper_node,
                                           comp_left_child,
                                           not_null_expr))) {
    LOG_WARN("failed to find mv not null expr", K(ret));
  } else if (NULL == not_null_expr) {
    is_valid = false;
    LOG_TRACE("can not find mv not null expr to compensate");
  } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_,
                                                       not_null_expr,
                                                       compensate_expr))) {
    LOG_WARN("failed to build is not null expr", K(ret));
  } else if (OB_ISNULL(compensate_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("build null compensate expr", K(ret));
  } else if (OB_FAIL(helper.mv_compensation_preds_.push_back(compensate_expr))) {
    LOG_WARN("failed to push back compensate expr", K(ret));
  }
  return ret;
}

/**
 * @brief ObTransformMVRewrite::find_mv_not_null_expr
 *
 * Find an MV select item, which is not null before mv_upper_node's left/right
 * (depends on comp_left_child) child, and is null in the outer join fill NULL
 * rows.
 *
 * @param comp_left_child True: find not null expr in mv_upper_node's left child,
 *                        False: find not null expr in mv_upper_node's right child
 */
int ObTransformMVRewrite::find_mv_not_null_expr(MvRewriteHelper &helper,
                                                JoinTreeNode *mv_upper_node,
                                                bool comp_left_child,
                                                ObRawExpr *&not_null_expr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *mv_stmt = helper.mv_info_.view_stmt_;
  JoinTreeNode *mv_comp_node = NULL;
  not_null_expr = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(mv_stmt) || OB_ISNULL(mv_upper_node)
      || OB_ISNULL(mv_comp_node = comp_left_child ? mv_upper_node->left_child_ : mv_upper_node->right_child_)
      || OB_ISNULL(mv_comp_node->ori_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(mv_stmt), KPC(mv_upper_node), KPC(mv_comp_node));
  } else {
    ObNotNullContext not_null_context(*ctx_, mv_stmt);
    if (mv_comp_node->ori_table_->is_joined_table() &&
        OB_FAIL(not_null_context.add_joined_table(static_cast<const JoinedTable *>(mv_comp_node->ori_table_)))) {
      LOG_WARN("failed to add joined table into context", K(ret), KPC(mv_comp_node->ori_table_));
    } else if (OB_FAIL(not_null_context.add_filter(mv_upper_node->join_info_.on_conditions_))) {
      LOG_WARN("failed to add null reject conditions", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && NULL == not_null_expr && i < mv_stmt->get_select_item_size(); ++i) {
      ObRawExpr *select_expr = mv_stmt->get_select_item(i).expr_;
      ObSEArray<ObRawExpr*, 4> col_exprs;
      bool is_null_propagate = false;
      bool is_not_null = false;
      if (OB_ISNULL(select_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (select_expr->is_const_expr()) {
        // do nothing
      } else if (!select_expr->get_relation_ids().is_subset(mv_comp_node->table_set_)) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(select_expr, col_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret), KPC(select_expr));
      } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(select_expr,
                                                                  col_exprs,
                                                                  is_null_propagate))) {
        LOG_WARN("failed to check is null propagate expr", K(ret), KPC(select_expr));
      } else if (!is_null_propagate) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_context,
                                                            select_expr,
                                                            is_not_null,
                                                            NULL /* constraints */ ))) {
        LOG_WARN("failed to check expr not null", K(ret));
      } else if (is_not_null) {
        not_null_expr = select_expr;
        break;
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformMVRewrite::check_predicate_compatibility
 * Check whether mv predicate and query predicate are same, and generate
 * compensation predicates.
 */
int ObTransformMVRewrite::check_predicate_compatibility(MvRewriteHelper &helper,
                                                        bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<ObRawExpr*, 8> mv_equal_conds;
  ObSEArray<ObRawExpr*, 8> mv_other_conds;
  ObSEArray<ObRawExpr*, 8> query_equal_conds;
  ObSEArray<ObRawExpr*, 8> query_other_conds;
  if (OB_FAIL(classify_predicates(helper.mv_conds_,
                                  mv_equal_conds,
                                  mv_other_conds))) {
    LOG_WARN("failed to split predicate", K(ret));
  } else if (OB_FAIL(classify_predicates(helper.query_conds_,
                                         query_equal_conds,
                                         query_other_conds))) {
    LOG_WARN("failed to split predicate", K(ret));
  } else if (OB_FAIL(check_equal_predicate(helper, mv_equal_conds, query_equal_conds, is_valid))) {
    LOG_WARN("failed to check equal predicate", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(check_other_predicate(helper, mv_other_conds, query_other_conds))) {
    LOG_WARN("failed to check other predicate", K(ret));
  } else if (OB_FAIL(check_compensation_preds_validity(helper, is_valid))) {
    LOG_WARN("failed to check compensation preds validity", K(ret));
  }
  return ret;
}

int ObTransformMVRewrite::check_equal_predicate(MvRewriteHelper &helper,
                                                const ObIArray<ObRawExpr*> &mv_conds,
                                                const ObIArray<ObRawExpr*> &query_conds,
                                                bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> mv_column_exprs;
  ObSEArray<ObRawExpr*, 16> query_column_exprs;
  is_valid = true;
  if (OB_ISNULL(helper.query_stmt_) || OB_ISNULL(helper.mv_info_.view_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(helper.query_stmt_), K(helper.mv_info_.view_stmt_));
  } else if (OB_FAIL(helper.mv_info_.view_stmt_->get_column_exprs(mv_column_exprs))) {
    LOG_WARN("failed to get mv column exprs", K(ret));
  } else if (OB_FAIL(helper.query_stmt_->get_column_exprs(query_column_exprs))) {
    LOG_WARN("failed to get query column exprs", K(ret));
  } else if (OB_FAIL(build_equal_sets(mv_conds,
                                      mv_column_exprs,
                                      helper.mv_equal_sets_,
                                      helper.mv_es_map_))) {
    LOG_WARN("failed to build equal class", K(ret));
  } else if (OB_FAIL(build_equal_sets(query_conds,
                                      query_column_exprs,
                                      helper.query_equal_sets_,
                                      helper.query_es_map_))) {
    LOG_WARN("failed to build equal class", K(ret));
  } else if (OB_FAIL(generate_equal_compensation_preds(helper, is_valid))) {
    LOG_WARN("failed to compare equal sets", K(ret));
  }
  return ret;
}

int ObTransformMVRewrite::build_equal_sets(const ObIArray<ObRawExpr*> &input_conds,
                                           const ObIArray<ObRawExpr*> &all_columns,
                                           EqualSets &equal_sets,
                                           hash::ObHashMap<PtrKey, int64_t> &equal_set_map)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(ctx_->allocator_,
                                                        input_conds,
                                                        equal_sets))) {
    LOG_WARN("failed to compute equal set", K(ret));
  } else if (OB_FAIL(equal_set_map.create(all_columns.count(), "MvRewrite"))) {
    LOG_WARN("failed to init equal set map", K(ret));
  }
  // generate equal set map
  for (int64_t i = 0; OB_SUCC(ret) && i < equal_sets.count(); ++i) {
    ObIArray<ObRawExpr*> *es = equal_sets.at(i);
    if (OB_ISNULL(es)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("equal set is null", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < es->count(); ++j) {
      const ObRawExpr *expr = es->at(j);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(equal_set_map.set_refactored(expr, i))) {
        LOG_WARN("failed to set equal set map", K(ret), K(expr), K(i));
      }
    }
  }
  // map columns that are not in equal sets into -1
  for (int64_t i = 0; OB_SUCC(ret) && i < all_columns.count(); ++i) {
    ObRawExpr *col_expr = all_columns.at(i);
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (NULL != equal_set_map.get(col_expr)) {
      // do nothing, col_expr already exists in equal set
    } else if (OB_FAIL(equal_set_map.set_refactored(col_expr, -1))) {
      LOG_WARN("failed to set equal set map", K(ret), K(col_expr));
    }
  }
  return ret;
}

int ObTransformMVRewrite::generate_equal_compensation_preds(MvRewriteHelper &helper,
                                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *mv_stmt = helper.mv_info_.view_stmt_;
  ObSEArray<ObSEArray<int64_t,4>,16> mv_es_to_query_es; // use to check mv equal set map to multi query equal set
  ObSEArray<ObColumnRefRawExpr*, 16> mv_column_exprs;
  hash::ObHashSet<PtrKey> visited_mv_column_exprs;
  is_valid = true;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(mv_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(mv_stmt));
  } else if (OB_FAIL(helper.equal_sets_map_.prepare_allocate(helper.query_equal_sets_.count()))) {
    LOG_WARN("failed to prepare allocate array", K(ret), K(helper.query_equal_sets_.count()));
  } else if (OB_FAIL(mv_es_to_query_es.prepare_allocate(helper.mv_equal_sets_.count()))) {
    LOG_WARN("failed to prepare allocate array", K(ret), K(helper.mv_equal_sets_.count()));
  } else if (OB_FAIL(mv_stmt->get_column_exprs(mv_column_exprs))) {
    LOG_WARN("failed to get mv column exprs", K(ret));
  } else if (OB_FAIL(visited_mv_column_exprs.create(mv_column_exprs.count(), "MvRewrite", "HashNode"))) {
    LOG_WARN("failed to init visited mv expr hash set", K(ret));
  }
  // check each query equal set, make sure every single expr in the set map to
  // the same mv equal set, otherwise add mv compensation predicate
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < helper.query_equal_sets_.count(); ++i) {
    ObIArray<ObRawExpr*> *query_es = helper.query_equal_sets_.at(i);
    ObSqlBitSet<> &mv_es_ids = helper.equal_sets_map_.at(i);
    ObColumnRefRawExpr *first_mv_expr = NULL;
    if (OB_ISNULL(query_es)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query equal set is null", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < query_es->count(); ++j) {
      ObColumnRefRawExpr *query_expr = NULL;
      ObColumnRefRawExpr *mv_expr = NULL;
      ObRawExpr *compensate_expr = NULL;
      int64_t mv_es_id;
      if (OB_ISNULL(query_es->at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query expr is null", K(ret));
      } else if (OB_UNLIKELY(!query_es->at(j)->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query expr is not a column ref expr", K(ret), KPC(query_es->at(j)));
      } else if (OB_FALSE_IT(query_expr = static_cast<ObColumnRefRawExpr*>(query_es->at(j)))) {
      } else if (OB_FAIL(map_to_mv_column(helper, query_expr, mv_expr))) {
        LOG_WARN("failed to get mv column expr", K(ret), KPC(query_expr));
      } else if (NULL == mv_expr) {
        // column does not exists in MV
        // we can not build the compensation predicate
        is_valid = false;
        LOG_TRACE("query column does not exists in mv", KPC(query_expr));
      } else if (OB_FAIL(visited_mv_column_exprs.set_refactored(mv_expr))) {
        LOG_WARN("failed to set visited mv expr hash set", K(ret), KPC(mv_expr));
      } else if (OB_FAIL(helper.mv_es_map_.get_refactored(mv_expr, mv_es_id))) {
        LOG_WARN("failed to get mv equal set index", K(ret), KPC(mv_expr));
      } else if (-1 == mv_es_id) {
        // MV column does not appear in equal predicates,
        // For the first expr, add not null compensation,
        // For the other expr, add equal compensation.
        int tmp_ret;
        if (NULL == first_mv_expr) {
          if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_,
                                                        mv_expr,
                                                        compensate_expr))) {
            LOG_WARN("failed to build is not null expr", K(ret));
          } else if (OB_FAIL(helper.mv_compensation_preds_.push_back(compensate_expr))) {
            LOG_WARN("failed to push back compensate expr", K(ret));
          } else {
            first_mv_expr = mv_expr;
          }
        } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*ctx_->expr_factory_,
                                                             ctx_->session_info_,
                                                             first_mv_expr,
                                                             mv_expr,
                                                             compensate_expr))) {
          LOG_WARN("failed to create equal expr", K(ret), KPC(first_mv_expr), KPC(mv_expr));
        } else if (OB_FAIL(helper.mv_compensation_preds_.push_back(compensate_expr))) {
          LOG_WARN("failed to push back compensate expr", K(ret));
        }
      } else if (OB_UNLIKELY(mv_es_id < 0 || mv_es_id >= mv_es_to_query_es.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mv equal set index is unexpected", K(ret), K(mv_es_id), K(mv_es_to_query_es.count()), K(helper.mv_equal_sets_.count()));
      } else if (OB_FAIL(check_mv_equal_set_used(helper,
                                                 mv_es_to_query_es.at(mv_es_id),
                                                 i))) {
        LOG_WARN("failed to check mv equal set used", K(ret));
      } else if (NULL == first_mv_expr) {
        if (OB_FAIL(mv_es_ids.add_member(mv_es_id))) {
          LOG_WARN("failed to add members", K(ret));
        } else {
          first_mv_expr = mv_expr;
        }
      } else if (mv_es_ids.has_member(mv_es_id)) {
        // do nothing
      } else if (OB_FAIL(mv_es_ids.add_member(mv_es_id))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*ctx_->expr_factory_,
                                                           ctx_->session_info_,
                                                           first_mv_expr,
                                                           mv_expr,
                                                           compensate_expr))) {
        LOG_WARN("failed to create equal expr", K(ret), KPC(first_mv_expr), KPC(mv_expr));
      } else if (OB_FAIL(helper.mv_compensation_preds_.push_back(compensate_expr))) {
        LOG_WARN("failed to push back compensate expr", K(ret));
      }
    }
  }
  // Because the stmt may no contain all the columns, we need to check each mv
  // column expr, make sure every single expr has been visited, and add query
  // compensation predicate if needed.
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < mv_column_exprs.count(); ++i) {
    ObColumnRefRawExpr *mv_expr = mv_column_exprs.at(i);
    int64_t mv_es_id;
    int tmp_ret;
    if (OB_ISNULL(mv_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv column expr is null", K(ret), K(i));
    } else if (mv_expr->get_relation_ids().is_subset(helper.mv_delta_table_)) {
      // do nothing, it is a mv delta table expr
    } else if (OB_FALSE_IT(tmp_ret = visited_mv_column_exprs.exist_refactored(mv_expr))) {
    } else if (OB_HASH_EXIST == tmp_ret) {
      // do nothing, expr has been visited
    } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp_ret)) {
      ret = tmp_ret;
      LOG_WARN("failed to check mv expr hash set", K(ret), KPC(mv_expr));
    } else if (OB_FAIL(helper.mv_es_map_.get_refactored(mv_expr, mv_es_id))) {
      LOG_WARN("failed to get mv equal set index", K(ret), KPC(mv_expr));
    } else if (-1 == mv_es_id) {
      // do nothing, mv column does not appear in equal predicates
    } else if (OB_UNLIKELY(mv_es_id < 0 || mv_es_id >= helper.mv_equal_sets_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv equal set index is unexpected", K(ret), K(mv_es_id), K(helper.mv_equal_sets_.count()));
    } else if (OB_ISNULL(helper.mv_equal_sets_.at(mv_es_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv equal set is null", K(ret), K(mv_es_id));
    } else {
      // TODO: need to generate query compensation predicate, but the corresponding
      // query expr does not exist. Now just simply set to invalid.
      is_valid = false;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(visited_mv_column_exprs.destroy())) {
    LOG_WARN("failed to destroy visited mv expr hash set", K(ret));
  }
  return ret;
}

/**
 * @brief ObTransformMVRewrite::check_mv_equal_set_used
 * Check whether mv equal set map to multi query equal set, if so, generate
 * a query compensation predicate.
 */
int ObTransformMVRewrite::check_mv_equal_set_used(MvRewriteHelper &helper,
                                                  ObIArray<int64_t> &mv_map_query_ids,
                                                  int64_t current_query_es_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (mv_map_query_ids.empty()) {
    if (OB_FAIL(mv_map_query_ids.push_back(current_query_es_id))) {
      LOG_WARN("failed to push back query es id", K(ret));
    }
  } else if (is_contain(mv_map_query_ids, current_query_es_id)) {
    // do nothing
  } else {
    // mv equal set has already mapped to the other query equal set
    // generate query compensation predicate
    ObIArray<ObRawExpr*> *current_es = helper.query_equal_sets_.at(current_query_es_id);
    if (OB_ISNULL(current_es) || OB_UNLIKELY(current_es->empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query equal set is unexpected", K(ret), K(current_es));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_map_query_ids.count(); ++i) {
      ObIArray<ObRawExpr*> *query_es = helper.query_equal_sets_.at(mv_map_query_ids.at(i));
      ObRawExpr *equal_expr = NULL;
      ObRawExpr *compensate_expr = NULL;
      if (OB_ISNULL(query_es)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query equal set is unexpected", K(ret), K(query_es));
      } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*ctx_->expr_factory_,
                                                           ctx_->session_info_,
                                                           query_es->at(0),
                                                           current_es->at(0),
                                                           equal_expr))) {
        LOG_WARN("failed to create equal expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*ctx_->expr_factory_,
                                                          equal_expr,
                                                          compensate_expr))) {
        LOG_WARN("failed to build lnnvl expr", K(ret));
      } else if (OB_ISNULL(compensate_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("built compensate expr is null", K(ret), KPC(equal_expr));
      } else if (OB_FAIL(compensate_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(helper.query_compensation_preds_.push_back(compensate_expr))) {
        LOG_WARN("failed to push back compensate expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(mv_map_query_ids.push_back(current_query_es_id))) {
      LOG_WARN("failed to push back query id", K(ret));
    }
  }
  return ret;
}

int ObTransformMVRewrite::map_to_mv_column(MvRewriteHelper &helper,
                                           const ObColumnRefRawExpr *query_expr,
                                           ObColumnRefRawExpr *&mv_expr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *mv_stmt = helper.mv_info_.view_stmt_;
  int64_t query_rel_id;
  int64_t mv_tbl_idx; // = rel id - 1
  TableItem *mv_table_item;
  ColumnItem *mv_column_item;
  if (OB_ISNULL(query_expr) || OB_ISNULL(helper.query_stmt_) || OB_ISNULL(mv_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_expr), K(helper.query_stmt_), K(mv_stmt));
  } else if (OB_FALSE_IT(query_rel_id = helper.query_stmt_->get_table_bit_index(query_expr->get_table_id()))) {
  } else if (OB_UNLIKELY(query_rel_id < 1 || query_rel_id > helper.base_table_map_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected query rel id", K(ret), K(query_rel_id));
  } else if (OB_FALSE_IT(mv_tbl_idx = helper.base_table_map_.at(query_rel_id - 1))) {
  } else if (-1 == mv_tbl_idx) {
    // it is query delta table
    mv_expr = NULL;
  } else if (OB_UNLIKELY(mv_tbl_idx < 0 || mv_tbl_idx >= mv_stmt->get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected mv table idx", K(ret), K(mv_tbl_idx), K(mv_stmt->get_table_size()), K(query_rel_id));
  } else if (OB_ISNULL(mv_table_item = mv_stmt->get_table_item(mv_tbl_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null table item", K(ret), K(mv_tbl_idx));
  } else if (NULL == (mv_column_item
                       = mv_stmt->get_column_item(mv_table_item->table_id_, query_expr->get_column_id()))) {
    mv_expr = NULL;
  } else {
    mv_expr = mv_column_item->expr_;
  }
  return ret;
}

int ObTransformMVRewrite::check_other_predicate(MvRewriteHelper &helper,
                                                const ObIArray<ObRawExpr*> &mv_conds,
                                                const ObIArray<ObRawExpr*> &query_conds)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> mv_common_conds;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  }
  // check query conditions and generate mv compensation predicate
  for (int64_t i = 0; OB_SUCC(ret) && i < query_conds.count(); ++i) {
    bool has_found = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < mv_conds.count(); ++j) {
      bool is_same = false;
      if (has_found && mv_common_conds.has_member(j)) {
        // do nothing
      } else if (OB_FAIL(is_same_condition(helper, query_conds.at(i), mv_conds.at(j), is_same))) {
        LOG_WARN("failed to compare condition", K(ret));
      } else if (!is_same) {
        // do nothing
      } else if (OB_FAIL(mv_common_conds.add_member(j))) {
        LOG_WARN("failed to add members", K(ret));
      } else {
        has_found = true;
        // continue to check the rest of mv exprs to find all common conds
      }
    }
    if (OB_SUCC(ret) && !has_found) {
      if (OB_FAIL(helper.mv_compensation_preds_.push_back(query_conds.at(i)))) {
        LOG_WARN("failed to push back compensate expr", K(ret));
      }
    }
  }
  // check mv conditions and generate query compensation predicate
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_conds.count(); ++i) {
    ObRawExpr *compensate_expr;
    if (mv_common_conds.has_member(i)) {
      // do nothing
    } else if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*ctx_->expr_factory_,
                                                        mv_conds.at(i),
                                                        compensate_expr))) {
      LOG_WARN("failed to build lnnvl expr", K(ret));
    } else if (OB_FAIL(compensate_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(helper.query_compensation_preds_.push_back(compensate_expr))) {
      LOG_WARN("failed to push back compensate expr", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformMVRewrite::check_compensation_preds_validity
 *
 * If mv has group by, check all columns in mv compensation predicates are mv's
 * group by column.
 */
int ObTransformMVRewrite::check_compensation_preds_validity(MvRewriteHelper &helper,
                                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> comp_col_exprs;
  ObSelectStmt *mv_stmt = helper.mv_info_.view_stmt_;
  is_valid = true;
  if (OB_ISNULL(mv_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_stmt));
  } else if (helper.mv_compensation_preds_.empty()
             || !mv_stmt->has_group_by()) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(helper.mv_compensation_preds_, comp_col_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < comp_col_exprs.count(); ++i) {
      bool has_found = false;
      for (int64_t j = 0; OB_SUCC(ret) && !has_found && j < mv_stmt->get_group_expr_size(); ++j) {
        if (OB_FAIL(is_same_condition(helper,
                                      comp_col_exprs.at(i),
                                      mv_stmt->get_group_exprs().at(j),
                                      has_found))) {
          LOG_WARN("failed to compare group by expr", K(ret));
        }
      }
      if (OB_SUCC(ret) && !has_found) {
        is_valid = false;
      }
    }
  }
  return ret;
}

int ObTransformMVRewrite::classify_predicates(const ObIArray<ObRawExpr*> &input_conds,
                                              ObIArray<ObRawExpr*> &equal_conds,
                                              ObIArray<ObRawExpr*> &other_conds)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_conds.count(); ++i) {
    ObRawExpr *expr = input_conds.at(i);
    if (is_equal_cond(expr)) {
      if (OB_FAIL(equal_conds.push_back(expr))) {
        LOG_WARN("failed to push back equal expr", K(ret));
      }
    } else {
      if (OB_FAIL(other_conds.push_back(expr))) {
        LOG_WARN("failed to push back other expr", K(ret));
      }
    }
  }
  return ret;
}

// t1.c1 = t2.c2     -> true
// t1.c1 = t1.c1     -> false
// t1.c1 > t2.c2     -> false
// t1.c1 = t2.c2 +1  -> false
bool ObTransformMVRewrite::is_equal_cond(ObRawExpr *expr)
{
  bool bret = true;
  if (OB_ISNULL(expr)) {
    bret = false;
  } else if (T_OP_EQ != expr->get_expr_type()) {
    bret = false;
  } else if (OB_UNLIKELY(2 != expr->get_param_count())) {
    bret = false;
  } else if (OB_ISNULL(expr->get_param_expr(0))) {
    bret = false;
  } else if (!expr->get_param_expr(0)->is_column_ref_expr()) {
    bret = false;
  } else if (OB_ISNULL(expr->get_param_expr(1))) {
    bret = false;
  } else if (!expr->get_param_expr(1)->is_column_ref_expr()) {
    bret = false;
  } else if (expr->get_param_expr(0) == expr->get_param_expr(1)) {
    // ObEqualAnalysis::compute_equal_set will discard such expr
    bret = false;
  }
  return bret;
}

// t1.c1 > 20      -> true
// t1.c1 > t2.c2   -> false
bool ObTransformMVRewrite::is_range_cond(ObRawExpr *expr)
{
  bool bret = true;
  bool has_colref = false;
  bool has_const = false;
  if (OB_ISNULL(expr)) {
    bret = false;
  } else if (expr->get_expr_type() < T_OP_LE
             || expr->get_expr_type() > T_OP_GT) {
    bret = false;
  } else if (OB_UNLIKELY(2 != expr->get_param_count())) {
    bret = false;
  } else {
    if (OB_ISNULL(expr->get_param_expr(0))) {
      bret = false;
    } else if (expr->get_param_expr(0)->is_column_ref_expr()) {
      has_colref = true;
    } else if (expr->get_param_expr(0)->is_const_expr()) {
      if (expr->get_type_class() >= ObIntTC
          && expr->get_type_class() <= ObYearTC) {
        has_const = true;
      }
    }
    if (OB_ISNULL(expr->get_param_expr(1))) {
      bret = false;
    } else if (expr->get_param_expr(1)->is_column_ref_expr()) {
      has_colref = true;
    } else if (expr->get_param_expr(1)->is_const_expr()) {
      if (expr->get_type_class() >= ObIntTC
          && expr->get_type_class() <= ObYearTC) {
        has_const = true;
      }
    }
  }
  if (bret && has_colref && has_const) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

int ObTransformMVRewrite::is_same_condition(MvRewriteHelper &helper,
                                            const ObRawExpr *left,
                                            const ObRawExpr *right,
                                            bool &is_same)
{
  int ret = OB_SUCCESS;
  MvRewriteCheckCtx context(helper);
  is_same = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(compare_expr(context, left, right, is_same))) {
    LOG_WARN("failed to compare expr", K(ret));
  } else if (!is_same) {
    // do nothing
  } else if (OB_FAIL(append_constraint_info(helper, context))) {
    LOG_WARN("failed to append constraint info", K(ret));
  }
  return ret;
}

int ObTransformMVRewrite::append_constraint_info(MvRewriteHelper &helper,
                                                 const MvRewriteCheckCtx &context)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(helper.equal_param_info_, context.equal_param_info_))) {
    LOG_WARN("failed to append equal param", K(ret));
  } else if (OB_FAIL(append(helper.const_param_info_, context.const_param_info_))) {
    LOG_WARN("failed to append const param", K(ret));
  } else if (OB_FAIL(append(helper.expr_cons_info_, context.expr_cons_info_))) {
    LOG_WARN("failed to append expr param", K(ret));
  }
  return ret;
}

int ObTransformMVRewrite::compare_expr(MvRewriteCheckCtx &context,
                                       const ObRawExpr *left,
                                       const ObRawExpr *right,
                                       bool &is_same)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (!(is_same = left->same_as(*right, &context))) {
    context.equal_param_info_.reset();
    context.const_param_info_.reset();
    context.expr_cons_info_.reset();
    if (!IS_COMMON_COMPARISON_OP(left->get_expr_type()) ||
        get_opposite_compare_type(left->get_expr_type()) != right->get_expr_type()) {
      // do nothing
    } else if (OB_ISNULL(left->get_param_expr(0)) ||
               OB_ISNULL(left->get_param_expr(1)) ||
               OB_ISNULL(right->get_param_expr(0)) ||
               OB_ISNULL(right->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param exprs are null", K(ret));
    } else if (!left->get_param_expr(0)->same_as(*right->get_param_expr(1), &context)) {
      // do nothing
    } else if (!left->get_param_expr(1)->same_as(*right->get_param_expr(0), &context)) {
      // do nothing
    } else {
      is_same = true;
    }
  }
  return ret;
}

int ObTransformMVRewrite::check_group_by_col(MvRewriteHelper &helper,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *mv_stmt = helper.mv_info_.view_stmt_;
  is_valid = true;
  if (OB_ISNULL(helper.query_stmt_) || OB_ISNULL(mv_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(helper.query_stmt_), K(mv_stmt));
  } else if (!mv_stmt->has_group_by()) {
    if (helper.query_stmt_->has_group_by()) {
      helper.need_group_by_roll_up_ = true;
    } else {
      helper.need_group_by_roll_up_ = false;
    }
  } else if (!helper.query_stmt_->has_group_by()) {
    is_valid = false;
  } else {
    ObSqlBitSet<> found_mv_gby_col;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < helper.query_stmt_->get_group_expr_size(); ++i) {
      bool has_found = false;
      for (int64_t j = 0; OB_SUCC(ret) && !has_found && j < mv_stmt->get_group_expr_size(); ++j) {
        if (OB_FAIL(is_same_condition(helper,
                                      helper.query_stmt_->get_group_exprs().at(i),
                                      mv_stmt->get_group_exprs().at(j),
                                      has_found))) {
          LOG_WARN("failed to compare group by expr", K(ret));
        } else if (!has_found) {
          // do nothing
        } else if (OB_FAIL(found_mv_gby_col.add_member(j))) {
          LOG_WARN("failed to add member", K(ret), K(j));
        }
      }
      if (OB_SUCC(ret) && !has_found) {
        is_valid = false;
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      helper.need_group_by_roll_up_ = (found_mv_gby_col.num_members() != mv_stmt->get_group_expr_size());
      if (helper.need_group_by_roll_up_ && mv_stmt->has_having()) {
        is_valid = false;
      }
    }
  }
  return ret;
}

int ObTransformMVRewrite::check_opt_feat_ctrl(MvRewriteHelper &helper,
                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  uint64_t opt_version = LASTED_COMPAT_VERSION;
  is_valid = true;
  if (OB_ISNULL(helper.ori_stmt_.get_query_ctx()) || OB_ISNULL(helper.mv_info_.view_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(helper.ori_stmt_.get_query_ctx()), K(helper.mv_info_.view_stmt_));
  } else if (OB_FALSE_IT(opt_version = helper.ori_stmt_.get_query_ctx()->optimizer_features_enable_version_)) {
  } else if (opt_version >= LASTED_COMPAT_VERSION) {
    // do nothing
  } else if (opt_version < COMPAT_VERSION_4_3_3
             && (!helper.query_delta_table_.is_empty()
                 || helper.mv_info_.view_stmt_->has_group_by())) {
    is_valid = false;
    LOG_TRACE("optimizer feature is lower than 4.3.3", K(opt_version));
  }
  return ret;
}

int ObTransformMVRewrite::compute_stmt_expr_map(MvRewriteHelper &helper,
                                                bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(helper.query_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query stmt is null", K(ret));
  } else if (OB_FAIL(compute_join_expr_map(helper, is_valid))) {
    LOG_WARN("failed to compute join expr", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("compute join expr does not pass");
  } else if (OB_FAIL(compute_select_expr_map(helper, is_valid))) {
    LOG_WARN("failed to compute select expr", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("compute select expr does not pass");
  } else if (OB_FAIL(inner_compute_exprs_map(helper, helper.query_other_conds_, is_valid))) {
    LOG_WARN("failed to compute query other conditions expr", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("compute query other conditions expr does not pass");
  } else if (OB_FAIL(inner_compute_exprs_map(helper, helper.mv_compensation_preds_, is_valid))) {
    LOG_WARN("failed to compute compensation expr", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("compute compensation expr does not pass");
  } else if (OB_FAIL(inner_compute_exprs_map(helper, helper.query_stmt_->get_having_exprs(), is_valid))) {
    LOG_WARN("failed to compute having expr", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("compute having expr does not pass");
  } else if (OB_FAIL(compute_group_by_expr_map(helper, is_valid))) {
    LOG_WARN("failed to compute group by expr", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("compute group by expr does not pass");
  } else if (OB_FAIL(compute_order_by_expr_map(helper, is_valid))) {
    LOG_WARN("failed to compute order by expr", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("compute order by expr does not pass");
  }
  return ret;
}

int ObTransformMVRewrite::compute_join_expr_map(MvRewriteHelper &helper,
                                                bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<JoinTreeNode*, 8> nodes_stack;
  is_valid = true;
  if (OB_FAIL(nodes_stack.push_back(helper.query_tree_))) {
    LOG_WARN("failed to push back node", K(ret));
  }
  while (OB_SUCC(ret) && is_valid && !nodes_stack.empty()) {
    JoinTreeNode *current_node = NULL;
    if (OB_FAIL(nodes_stack.pop_back(current_node))) {
      LOG_WARN("failed to pop back node", K(ret));
    } else if (OB_ISNULL(current_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current node unexpected null", K(ret));
    } else if (current_node == helper.query_tree_mv_part_) {
      // do nothing, do not need check mv part
    } else if (OB_FAIL(inner_compute_exprs_map(helper, current_node->join_info_.on_conditions_, is_valid))) {
      LOG_WARN("failed to compute on exprs map", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(inner_compute_exprs_map(helper, current_node->join_info_.where_conditions_, is_valid))) {
      LOG_WARN("failed to compute where exprs map", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (UNKNOWN_JOIN == current_node->join_info_.join_type_) {
      // do nothing, is leaf node
    } else if (OB_FAIL(nodes_stack.push_back(current_node->left_child_))) {
      LOG_WARN("failed to push back left node", K(ret));
    } else if (OB_FAIL(nodes_stack.push_back(current_node->right_child_))) {
      LOG_WARN("failed to push back right node", K(ret));
    }
  }
  return ret;
}

int ObTransformMVRewrite::compute_select_expr_map(MvRewriteHelper &helper,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  if (OB_ISNULL(helper.query_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(helper.query_stmt_->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(inner_compute_exprs_map(helper, select_exprs, is_valid))) {
    LOG_WARN("failed to compute exprs map", K(ret));
  }
  return ret;
}

int ObTransformMVRewrite::compute_group_by_expr_map(MvRewriteHelper &helper,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(helper.query_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!helper.need_group_by_roll_up_) {
    // do nothing
  } else if (OB_FAIL(inner_compute_exprs_map(helper, helper.query_stmt_->get_group_exprs(), is_valid))) {
    LOG_WARN("failed to compute group exprs map", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(inner_compute_exprs_map(helper, helper.query_stmt_->get_rollup_exprs(), is_valid))) {
    LOG_WARN("failed to compute rollup exprs map", K(ret));
  }
  return ret;
}

int ObTransformMVRewrite::compute_order_by_expr_map(MvRewriteHelper &helper,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> orderby_exprs;
  if (OB_ISNULL(helper.query_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(helper.query_stmt_->get_order_exprs(orderby_exprs))) {
    LOG_WARN("failed to get order by exprs", K(ret));
  } else if (OB_FAIL(inner_compute_exprs_map(helper, orderby_exprs, is_valid))) {
    LOG_WARN("failed to compute group exprs map", K(ret));
  }
  return ret;
}

int ObTransformMVRewrite::inner_compute_exprs_map(MvRewriteHelper &helper,
                                                  ObIArray<ObRawExpr*> &exprs,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  MvRewriteCheckCtx context(helper);
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < exprs.count(); ++i) {
    if (OB_FAIL(inner_compute_expr_map(helper,
                                       exprs.at(i),
                                       context,
                                       is_valid))) {
      LOG_WARN("failed to compute join condition", K(ret));
    }
  }
  if (OB_FAIL(ret) || !is_valid) {
    // do nothing
  } else if (OB_FAIL(append_constraint_info(helper, context))) {
    LOG_WARN("failed to append constraint info", K(ret));
  }
  return ret;
}

int ObTransformMVRewrite::inner_compute_expr_map(MvRewriteHelper &helper,
                                                 ObRawExpr *query_expr,
                                                 MvRewriteCheckCtx &context,
                                                 bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *mv_stmt = helper.mv_info_.view_stmt_;
  ObRawExpr *mv_select_expr = NULL;
  is_valid = false;
  if (OB_ISNULL(query_expr) || OB_ISNULL(mv_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_expr), K(mv_stmt));
  } else if (query_expr->is_const_expr()) {
    // const expr does not need to compute
    is_valid = true;
  } else if (helper.query_expr_replacer_.is_existed(query_expr)) {
    // the input expr has been computed
    is_valid = true;
  } else if (mv_stmt->has_group_by() && query_expr->is_aggr_expr()) {
    if (OB_FAIL(compute_agg_expr_map(helper, static_cast<ObAggFunRawExpr*>(query_expr), context, is_valid))) {
      LOG_WARN("failed to compute agg expr map", K(ret));
    }
  } else if (query_expr->get_relation_ids().is_subset(helper.query_delta_table_)) {
    // column only appear in query, does not need to compute
    is_valid = true;
  } else if (OB_FAIL(find_mv_select_expr(helper, query_expr, context, mv_select_expr))) {
    LOG_WARN("failed to find mv select expr", K(ret));
  } else if (NULL != mv_select_expr) {
    if (OB_FAIL(helper.query_expr_replacer_.add_replace_expr(query_expr, mv_select_expr))) {
      LOG_WARN("failed to add replaceed expr", K(ret), KPC(query_expr), KPC(mv_select_expr));
    } else {
      is_valid = true;
    }
  } else if (query_expr->get_param_count() > 0) {
    // Try to match each param expr
    bool param_match = true;
    MvRewriteCheckCtx new_ctx(helper);
    for (int64_t i = 0; OB_SUCC(ret) && param_match && i < query_expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(inner_compute_expr_map(helper,
                                                    query_expr->get_param_expr(i),
                                                    new_ctx,
                                                    param_match)))) {
        LOG_WARN("failed to check param expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && param_match) {
      is_valid = true;
      if (OB_FAIL(context.append_constraint_info(new_ctx))) {
        LOG_WARN("failed to append constraint info", K(ret));
      }
    }
  }
  return ret;
}


/**
 * @brief ObTransformMVRewrite::compute_agg_expr_map
 *
 * If MV has group by, we can not just simply replace param expr for aggregate
 * functions. Hence we need to handle the agg expr separately.
 *
 * 1. If need group by roll up, check whether query_expr can roll up.
 * 2. Find the query_expr in MV select exprs.
 * 3. If need group by roll up, wrap the roll up agg func.
 */
int ObTransformMVRewrite::compute_agg_expr_map(MvRewriteHelper &helper,
                                               ObAggFunRawExpr *query_expr,
                                               MvRewriteCheckCtx &context,
                                               bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObRawExpr *mv_select_expr = NULL;
  is_valid = true;
  if (OB_ISNULL(query_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_expr));
  } else if (helper.need_group_by_roll_up_ && OB_FAIL(check_agg_roll_up_expr(query_expr, is_valid))) {
    LOG_WARN("failed to check agg roll up expr", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(find_mv_select_expr(helper, query_expr, context, mv_select_expr))) {
    LOG_WARN("failed to find mv select expr", K(ret));
  } else if (NULL == mv_select_expr) {
    is_valid = false;
  } else if (helper.need_group_by_roll_up_ && OB_FAIL(wrap_agg_roll_up_expr(query_expr, mv_select_expr))) {
    LOG_WARN("failed to wrap agg roll up expr", K(ret));
  } else if (OB_FAIL(helper.query_expr_replacer_.add_replace_expr(query_expr, mv_select_expr))) {
    LOG_WARN("failed to add replaceed expr", K(ret), KPC(query_expr), KPC(mv_select_expr));
  }
  return ret;
}

int ObTransformMVRewrite::find_mv_select_expr(MvRewriteHelper &helper,
                                              const ObRawExpr *query_expr,
                                              MvRewriteCheckCtx &context,
                                              ObRawExpr *&select_expr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *mv_stmt = helper.mv_info_.view_stmt_;
  ObSEArray<ObRawExpr*, 4> mv_select_exprs;
  bool is_match = false;
  select_expr = NULL;
  if (OB_ISNULL(query_expr) || OB_ISNULL(mv_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_expr), K(mv_stmt));
  } else if (OB_FAIL(mv_stmt->get_select_exprs(mv_select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < mv_select_exprs.count(); ++i) {
    MvRewriteCheckCtx new_ctx(helper);
    if (OB_FAIL(compare_expr(new_ctx, query_expr, mv_select_exprs.at(i), is_match))) {
      LOG_WARN("failed to check is condition equal", K(ret));
    } else if (!is_match) {
      // do nothing
    } else if (OB_FAIL(context.append_constraint_info(new_ctx))) {
      LOG_WARN("failed to append constraint info", K(ret));
    } else {
      select_expr = mv_select_exprs.at(i);
    }
  }
  return ret;
}

int ObTransformMVRewrite::check_agg_roll_up_expr(ObRawExpr *agg_expr,
                                                 bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(agg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(agg_expr));
  } else {
    switch (agg_expr->get_expr_type()) {
      case T_FUN_SUM:
      case T_FUN_COUNT:
      case T_FUN_COUNT_SUM:
      case T_FUN_SYS_BIT_AND:
      case T_FUN_SYS_BIT_OR:
      case T_FUN_SYS_BIT_XOR:
        is_valid = !static_cast<ObAggFunRawExpr*>(agg_expr)->is_param_distinct();
        break;
      case T_FUN_MIN:
      case T_FUN_MAX:
        is_valid = true;
        break;
      default:
        is_valid = false;
        break;
    }
  }
  return ret;
}

/**
 * @brief ObTransformMVRewrite::wrap_agg_roll_up_expr
 *
 * @param ori_expr the origin aggregate expr, used to determine the roll up func type
 * @param output_expr the expr found in mv stmt, roll up will be performed on this expr
 */
int ObTransformMVRewrite::wrap_agg_roll_up_expr(ObRawExpr *ori_expr,
                                                ObRawExpr *&output_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_expr) || OB_ISNULL(output_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ori_expr), K(output_expr));
  } else {
    ObAggFunRawExpr *tmp_expr = NULL;
    ObItemType wrap_type = T_INVALID;
    switch (ori_expr->get_expr_type()) {
      case T_FUN_COUNT:
        wrap_type = T_FUN_COUNT_SUM;
        break;
      case T_FUN_SUM:
      case T_FUN_COUNT_SUM:
      case T_FUN_SYS_BIT_AND:
      case T_FUN_SYS_BIT_OR:
      case T_FUN_SYS_BIT_XOR:
      case T_FUN_MIN:
      case T_FUN_MAX:
        wrap_type = ori_expr->get_expr_type();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input expr does not support aggregate roll up", K(ret), KPC(ori_expr), KPC(output_expr));
        break;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::create_aggr_expr(ctx_, wrap_type, tmp_expr, output_expr))) {
      LOG_WARN("failed to create sum expr", K(ret));
    } else {
      output_expr = tmp_expr;
    }
  }
  return ret;
}

int ObTransformMVRewrite::generate_rewrite_stmt_contain_mode(MvRewriteHelper &helper)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)
      || OB_ISNULL(helper.mv_info_.mv_schema_) || OB_ISNULL(helper.query_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(helper.mv_info_), K(helper.query_stmt_));
  } else if (OB_FAIL(clear_mv_common_tables(helper))) {
    LOG_WARN("failed to clear rewrite stmt", K(ret));
  } else if (OB_FAIL(create_mv_table_item(helper))) {
    LOG_WARN("failed to create mv table item", K(ret));
  } else if (OB_FAIL(create_mv_column_item(helper))) {
    LOG_WARN("failed to create mv column item", K(ret));
  } else if (OB_FAIL(adjust_rewrite_stmt(helper))) {
    LOG_WARN("failed to adjust rewrite stmt", K(ret));
  } else if (OB_FAIL(adjust_aggr_winfun_expr(helper.query_stmt_))) {
    LOG_WARN("failed to adjust aggr winfun expr", K(ret));
  } else if (OB_FAIL(adjust_mv_item(helper))) {
    LOG_WARN("failed to adjust mv item", K(ret));
  } else if (OB_FAIL(helper.query_stmt_->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item rel id", K(ret));
  } else if (OB_FAIL(helper.query_stmt_->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else {
    OPT_TRACE("generate rewrite stmt use", helper.mv_info_.mv_schema_->get_table_name(), ":", helper.query_stmt_);
  }
  return ret;
}

/**
 * @brief ObTransformMVRewrite::clear_mv_common_tables
 *
 * Clear the tables that appear in both mv and origin query with the redundant
 * column items, part exprs, etc.
 */
int ObTransformMVRewrite::clear_mv_common_tables(MvRewriteHelper &helper) {
  int ret = OB_SUCCESS;
  ObSelectStmt *rewrite_stmt = helper.query_stmt_;
  ObSEArray<TableItem*, 4> mv_common_tables;
  if (OB_ISNULL(rewrite_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(rewrite_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rewrite_stmt->get_table_size(); ++i) {
    TableItem *table = rewrite_stmt->get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i));
    } else if (helper.query_delta_table_.has_member(i + 1)) {
      // do nothing
    } else if (OB_FAIL(mv_common_tables.push_back(table))) {
      LOG_WARN("push back table item failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(rewrite_stmt->remove_table_info(mv_common_tables))) {
    LOG_WARN("failed to remove query delta table info", K(ret));
  } else {
    rewrite_stmt->get_joined_tables().reuse();
    rewrite_stmt->clear_from_items();
  }
  return ret;
}

int ObTransformMVRewrite::create_mv_table_item(MvRewriteHelper &helper)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *rewrite_stmt = helper.query_stmt_;
  MvInfo &mv_info = helper.mv_info_;
  const TableItem *mv_ori_item = NULL;
  TableItem *mv_item = NULL;
  ObString qb_name;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(rewrite_stmt) || OB_ISNULL(rewrite_stmt->get_query_ctx())
      || OB_ISNULL(mv_info.mv_schema_) || OB_ISNULL(mv_info.data_table_schema_)
      || OB_ISNULL(mv_info.select_mv_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), KPC(rewrite_stmt), K(mv_info));
  } else if (OB_FAIL(rewrite_stmt->get_qb_name(qb_name))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else if (OB_UNLIKELY(NULL == (mv_item = rewrite_stmt->create_table_item(*ctx_->allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create table item failed", K(ret));
  } else if (OB_UNLIKELY(!mv_info.select_mv_stmt_->is_single_table_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select mv stmt is not single table stmt", K(ret), KPC(mv_info.select_mv_stmt_));
  } else if (OB_ISNULL(mv_ori_item = mv_info.select_mv_stmt_->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv ori item is null", K(ret));
  } else {
    ObRawExprCopier copier(*ctx_->expr_factory_);

    ObSchemaObjVersion table_version;
    table_version.object_id_ = mv_info.mv_schema_->get_table_id();
    table_version.object_type_ = mv_info.mv_schema_->is_view_table() ? DEPENDENCY_VIEW : DEPENDENCY_TABLE;
    table_version.version_ = mv_info.mv_schema_->get_schema_version();
    table_version.is_db_explicit_ = true;
    uint64_t dep_db_id = mv_info.mv_schema_->get_database_id();

    ObSchemaObjVersion data_table_version;
    data_table_version.object_id_ = mv_info.data_table_schema_->get_table_id();
    data_table_version.object_type_ = DEPENDENCY_TABLE;
    data_table_version.version_ = mv_info.data_table_schema_->get_schema_version();
    data_table_version.is_db_explicit_ = true;
    uint64_t data_dep_db_id = mv_info.data_table_schema_->get_database_id();

    if (OB_FAIL(mv_item->deep_copy(copier, *mv_ori_item, ctx_->allocator_))) {
      LOG_WARN("failed to deep copy table item", K(ret));
    } else if (OB_FALSE_IT(mv_item->table_id_ = rewrite_stmt->get_query_ctx()->available_tb_id_--)) {
    } else if (OB_FALSE_IT(mv_item->qb_name_ = qb_name)) {
    } else if (OB_FAIL(ObTransformUtils::add_table_item(rewrite_stmt, mv_item))) {
      LOG_WARN("failed to add mv table item", K(ret));
    } else if (OB_FAIL(rewrite_stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuid table hash", K(ret));
    } else if (OB_FAIL(rewrite_stmt->add_global_dependency_table(table_version))) {
      LOG_WARN("add global dependency table failed", K(ret));
    } else if (OB_FAIL(rewrite_stmt->add_ref_obj_version(mv_info.mv_schema_->get_table_id(),
                                                         dep_db_id,
                                                         ObObjectType::VIEW,
                                                         table_version,
                                                         *ctx_->allocator_))) {
      LOG_WARN("failed to add ref obj version", K(ret));
    } else if (OB_FAIL(rewrite_stmt->add_global_dependency_table(data_table_version))) {
      LOG_WARN("add global dependency table failed", K(ret));
    } else if (OB_FAIL(rewrite_stmt->add_ref_obj_version(mv_info.mv_schema_->get_table_id(),
                                                         data_dep_db_id,
                                                         ObObjectType::VIEW,
                                                         data_table_version,
                                                         *ctx_->allocator_))) {
      LOG_WARN("failed to add ref obj version", K(ret));
    } else {
      LOG_DEBUG("succ to fill table_item", K(mv_item));
    }
  }
  if (OB_SUCC(ret)) {
    helper.mv_upper_stmt_ = rewrite_stmt;
    helper.mv_item_ = mv_item;
  }
  return ret;
}

int ObTransformMVRewrite::create_mv_column_item(MvRewriteHelper &helper)
{
  int ret = OB_SUCCESS;
  MvInfo &mv_info = helper.mv_info_;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(helper.query_stmt_) || OB_ISNULL(helper.mv_item_)
      || OB_ISNULL(mv_info.view_stmt_) || OB_ISNULL(mv_info.select_mv_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(helper.query_stmt_), K(helper.mv_item_), K(mv_info));
  } else {
    ObRawExprCopier copier(*ctx_->expr_factory_);
    // 1. create mv column item
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_info.select_mv_stmt_->get_column_items().count(); ++i) {
      ColumnItem column_item;
      if (OB_FAIL(column_item.deep_copy(copier, mv_info.select_mv_stmt_->get_column_items().at(i)))) {
        LOG_WARN("build column expr failed", K(ret));
      } else {
        column_item.expr_->set_ref_id(helper.mv_item_->table_id_, column_item.expr_->get_column_id());
        column_item.table_id_ = column_item.expr_->get_table_id();
        if (OB_FAIL(helper.query_stmt_->add_column_item(column_item))) {
          LOG_WARN("add column item to stmt failed", K(ret));
        } else if (OB_FAIL(column_item.expr_->pull_relation_id())) {
          LOG_WARN("failed to pullup relation ids", K(ret));
        } else {
          LOG_DEBUG("succ to fill column_item", K(column_item));
        }
      }
    }
    // 2. add column expr to copier
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_info.view_stmt_->get_select_item_size(); ++i) {
      ObColumnRefRawExpr *col_expr = NULL;
      if (OB_ISNULL(col_expr = helper.query_stmt_->get_column_expr_by_id(helper.mv_item_->table_id_, OB_APP_MIN_COLUMN_ID + i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get column id", K(ret), K(helper.mv_item_->table_id_), K(OB_APP_MIN_COLUMN_ID + i));
      } else if (OB_FAIL(helper.mv_select_replacer_.add_replace_expr(mv_info.view_stmt_->get_select_item(i).expr_, col_expr))) {
        LOG_WARN("failed to add replaced expr", K(ret), KPC(mv_info.view_stmt_->get_select_item(i).expr_), KPC(col_expr));
      }
    }
    // 3. fill part expr
    // 3.1. fill expr copier, from select_mv_stmt_ column to query_stmt_ column
    if (OB_SUCC(ret) && mv_info.select_mv_stmt_->get_part_exprs().count() > 0) {
      ObRawExprCopier part_expr_copier(*ctx_->expr_factory_); // FROM select_mv_stmt_ TO query_stmt_
      ObSEArray<ObColumnRefRawExpr*, 4> mv_col_exprs; // column expr in select_mv_stmt_
      // make part_expr_copier which maps select_mv_stmt_ columns to query_stmt_ columns
      if (OB_FAIL(mv_info.select_mv_stmt_->get_column_exprs(mv_col_exprs))) {
        LOG_WARN("failed to get column exprs", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < mv_col_exprs.count(); ++i) {
        ObColumnRefRawExpr *col_expr = NULL; // column expr in query_stmt_
        if (OB_ISNULL(mv_col_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column expr is null", K(ret), K(i));
        } else if (OB_ISNULL(col_expr = helper.query_stmt_->get_column_expr_by_id(helper.mv_item_->table_id_,
                                                                          mv_col_exprs.at(i)->get_column_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get column id", K(ret), K(helper.mv_item_->table_id_), K(i));
        } else if (OB_FAIL(part_expr_copier.add_replaced_expr(mv_col_exprs.at(i), col_expr))) {
          LOG_WARN("failed to add replaced expr", K(ret), KPC(mv_col_exprs.at(i)), KPC(col_expr));
        }
      }
      // 3.2. do fill part expr for query_stmt_
      for (int64_t i = 0; OB_SUCC(ret) && i < mv_info.select_mv_stmt_->get_part_exprs().count(); ++i) {
        ObDMLStmt::PartExprItem &part_item = mv_info.select_mv_stmt_->get_part_exprs().at(i);
        ObRawExpr *part_expr = NULL;
        ObRawExpr *subpart_expr = NULL;
        if (OB_FAIL(part_expr_copier.copy_on_replace(part_item.part_expr_, part_expr))) {
          LOG_WARN("failed to copy part expr", K(ret), K(part_item));
        } else if (NULL != part_item.subpart_expr_
                  && OB_FAIL(part_expr_copier.copy_on_replace(part_item.subpart_expr_, subpart_expr))) {
          LOG_WARN("failed to copy subpart expr", K(ret), K(part_item));
        } else if (OB_FAIL(helper.query_stmt_->set_part_expr(helper.mv_item_->table_id_,
                                                             part_item.index_tid_,
                                                             part_expr,
                                                             subpart_expr))) {
          LOG_WARN("set part expr to new stmt failed", K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformMVRewrite::adjust_rewrite_stmt
 *
 * Generate the rewrite stmt, and replace column expr of tables in mv into mv column.
 *
 * 1. Generate new from items and joined tables for rewrite stmt based on query
 *    join tree.
 * 2. Reset the where conditions based on query join tree.
 * 3. Reset group by exprs if do not need_group_by_roll_up.
 * 4. Replace all mv relation exprs into mv column.
 */
int ObTransformMVRewrite::adjust_rewrite_stmt(MvRewriteHelper &helper)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *rewrite_stmt = helper.query_stmt_;
  TableItem *from_table = NULL;
  ObSEArray<ObRawExpr*, 4> where_conditions;
  helper.query_expr_replacer_.set_relation_scope();
  helper.query_expr_replacer_.set_recursive(false);
  helper.mv_select_replacer_.set_relation_scope();
  helper.mv_select_replacer_.set_recursive(false);
  if (OB_ISNULL(rewrite_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(rewrite_stmt));
  } else if (OB_FAIL(recursive_build_from_table(helper,
                                                helper.query_tree_,
                                                from_table,
                                                where_conditions))) {
    LOG_WARN("failed to build from table", K(ret));
  } else if (OB_ISNULL(from_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("from table is null", K(ret));
  } else if (from_table->is_joined_table() &&
             OB_FAIL(rewrite_stmt->add_joined_table(static_cast<JoinedTable*>(from_table)))) {
    LOG_WARN("failed to add joined table", K(ret));
  } else if (OB_FAIL(rewrite_stmt->add_from_item(from_table->table_id_,
                                                 from_table->is_joined_table()))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FALSE_IT(rewrite_stmt->get_condition_exprs().reuse())) {
  } else if (OB_FAIL(append(rewrite_stmt->get_condition_exprs(), where_conditions))) {
    LOG_WARN("failed to append where conditions", K(ret));
  } else if (OB_FAIL(append(rewrite_stmt->get_condition_exprs(), helper.query_other_conds_))) {
    LOG_WARN("failed to append query other conditions", K(ret));
  } else if (!helper.need_group_by_roll_up_) {
    rewrite_stmt->get_group_exprs().reset();
  }
  // replace relation exprs
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(rewrite_stmt->iterate_stmt_expr(helper.query_expr_replacer_))) {
    LOG_WARN("failed to iterate stmt expr, query expr replacer", K(ret));
  } else if (OB_FAIL(rewrite_stmt->iterate_stmt_expr(helper.mv_select_replacer_))) {
    LOG_WARN("failed to iterate stmt expr, mv select replacer", K(ret));
  }
  return ret;
}

int ObTransformMVRewrite::recursive_build_from_table(MvRewriteHelper &helper,
                                                     JoinTreeNode *node,
                                                     TableItem *&output_table,
                                                     ObIArray<ObRawExpr*> &filters)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *rewrite_stmt = helper.query_stmt_;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  ObSEArray<ObRawExpr*, 4> left_filters;
  ObSEArray<ObRawExpr*, 4> right_filters;
  JoinedTable *new_joined_table;
  output_table = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)
      || OB_ISNULL(node) || OB_ISNULL(rewrite_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(node), K(rewrite_stmt));
  } else if (node == helper.query_tree_mv_part_) {
    output_table = helper.mv_item_;
  } else if (UNKNOWN_JOIN == node->join_info_.join_type_) {
    // is leaf node
    if (OB_ISNULL(output_table = rewrite_stmt->get_table_item_by_id(node->table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table item", K(ret), K(node->table_id_));
    } else if (OB_FAIL(append(filters, node->join_info_.where_conditions_))) {
      LOG_WARN("failed to append base table filters", K(ret));
    }
  } else if (OB_FAIL(SMART_CALL(recursive_build_from_table(helper,
                                                           node->left_child_,
                                                           left_table,
                                                           left_filters)))) {
    LOG_WARN("failed to recursive build left from table", K(ret));
  } else if (OB_ISNULL(left_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left table is null", K(ret));
  } else if (OB_FAIL(SMART_CALL(recursive_build_from_table(helper,
                                                           node->right_child_,
                                                           right_table,
                                                           right_filters)))) {
    LOG_WARN("failed to recursive build right from table", K(ret));
  } else if (OB_ISNULL(right_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("right table is null", K(ret));
  } else if (OB_ISNULL(new_joined_table = ObDMLStmt::create_joined_table(*ctx_->allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create joined table", K(ret));
  } else if (OB_FALSE_IT(new_joined_table->table_id_ = rewrite_stmt->get_query_ctx()->available_tb_id_--)) {
  } else if (OB_FAIL(ObTransformUtils::add_joined_table_single_table_ids(*new_joined_table, *left_table))) {
    LOG_WARN("failed to add joined table left single table ids", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_joined_table_single_table_ids(*new_joined_table, *right_table))) {
    LOG_WARN("failed to add joined table right single table ids", K(ret));
  } else if (OB_FAIL(append(new_joined_table->join_conditions_, node->join_info_.on_conditions_))) {
    LOG_WARN("failed to append join conditions", K(ret));
  } else {
    new_joined_table->joined_type_ = node->join_info_.join_type_;
    new_joined_table->left_table_ = left_table;
    new_joined_table->right_table_ = right_table;
    output_table = new_joined_table;
    switch (new_joined_table->joined_type_) {
      case INNER_JOIN:
        if (OB_FAIL(append(new_joined_table->join_conditions_, node->join_info_.where_conditions_))) {
          LOG_WARN("failed to append join where conditions", K(ret));
        } else if (OB_FAIL(append(new_joined_table->join_conditions_, left_filters))) {
          LOG_WARN("failed to append join left filters", K(ret));
        } else if (OB_FAIL(append(new_joined_table->join_conditions_, right_filters))) {
          LOG_WARN("failed to append join right filters", K(ret));
        }
        break;
      case LEFT_OUTER_JOIN:
      case RIGHT_OUTER_JOIN: {
        ObIArray<ObRawExpr*> &upper_filters = (LEFT_OUTER_JOIN == new_joined_table->joined_type_ ? left_filters : right_filters);
        ObIArray<ObRawExpr*> &join_on_conds = (LEFT_OUTER_JOIN == new_joined_table->joined_type_ ? right_filters : left_filters);
        if (OB_FAIL(append(filters, node->join_info_.where_conditions_))) {
          LOG_WARN("failed to append join where conditions", K(ret));
        } else if (OB_FAIL(append(filters, upper_filters))) {
          LOG_WARN("failed to append join filters", K(ret));
        } else if (OB_FAIL(append(new_joined_table->join_conditions_, join_on_conds))) {
          LOG_WARN("failed to append join conditions", K(ret));
        }
        break;
      }
      case FULL_OUTER_JOIN:
        if (OB_UNLIKELY(!left_filters.empty() || !right_filters.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("outer join filters is not empty", K(ret), K(left_filters), K(right_filters));
        } else if (OB_FAIL(append(filters, node->join_info_.where_conditions_))) {
          LOG_WARN("failed to append join where conditions", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected join type", K(ret), K(new_joined_table->joined_type_));
        break;
    }
  }
  return ret;
}

int ObTransformMVRewrite::append_on_replace(MvRewriteHelper &helper,
                                            ObIArray<ObRawExpr*> &dst,
                                            const ObIArray<ObRawExpr*> &src)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
    ObRawExpr *tmp_expr = src.at(i);
    if (OB_FAIL(helper.query_expr_replacer_.do_visit(tmp_expr))) {
      LOG_WARN("failed to replace query expr", K(ret));
    } else if (OB_FAIL(helper.mv_select_replacer_.do_visit(tmp_expr))) {
      LOG_WARN("failed to replace mv select expr", K(ret));
    } else if (OB_FAIL(dst.push_back(tmp_expr))) {
      LOG_WARN("failed to push back dst expr", K(ret));
    }
  }
  return ret;
}

int ObTransformMVRewrite::adjust_aggr_winfun_expr(ObSelectStmt *rewrite_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> exprs;
  if (OB_ISNULL(rewrite_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(rewrite_stmt));
  } else if (OB_FAIL(rewrite_stmt->get_select_exprs(exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(append(exprs, rewrite_stmt->get_having_exprs()))) {
    LOG_WARN("failed to append having exprs", K(ret));
  } else if (OB_FAIL(rewrite_stmt->get_order_exprs(exprs))) {
    LOG_WARN("failed to get order exprs", K(ret));
  } else {
    rewrite_stmt->clear_aggr_item();
    rewrite_stmt->get_window_func_exprs().reuse();
  }
  /* For MV:      SELECT count(1) a0 FROM t1 GROUP BY c1, c2;
   *     Query:   SELECT count(1) a1, count(1) a2 FROM t1 GROUP BY c1;
   *     Rewrite: SELECT count_sum(mv.a0) ra1, count_sum(mv.a0) ra2 FROM mv GROUP BY c1;
   * Because of the parameterization, "a1" and "a2" will have different pointers.
   * Which makes "ra1" and "ra2" have different pointers, although they are the same.
   */
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(ObTransformUtils::add_aggr_winfun_expr(rewrite_stmt, exprs.at(i), false))) {
      LOG_WARN("failed to add aggr winfun expr", K(ret), KPC(exprs.at(i)));
    }
  }
  return ret;
}

/**
 * @brief ObTransformMVRewrite::adjust_mv_item
 *
 * 1. If there is mv compensation predicates, create an inline view and add
 *    compensation predicates into view.
 * 2. Expand real-time mv if needed.
 */
int ObTransformMVRewrite::adjust_mv_item(MvRewriteHelper &helper)
{
  int ret = OB_SUCCESS;
  ObString src_qb_name;
  int64_t rewrite_integrity = QueryRewriteIntegrityType::REWRITE_INTEGRITY_ENFORCED;
  // wrap mv table into inline view if needed
  if (!helper.mv_compensation_preds_.empty()) {
    TableItem *view_table = NULL;
    ObSEArray<ObRawExpr*, 4> view_conds;
    if (OB_FAIL(append_on_replace(helper, view_conds, helper.mv_compensation_preds_))) {
      LOG_WARN("failed to append on replace query expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                                 helper.query_stmt_,
                                                                 view_table,
                                                                 helper.mv_item_))) {
      LOG_WARN("failed to replace with empty view", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                            helper.query_stmt_,
                                                            view_table,
                                                            helper.mv_item_,
                                                            &view_conds))) {
      LOG_WARN("failed to create inline view", K(ret));
    } else if (OB_ISNULL(view_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new view table is null", K(ret));
    } else {
      helper.mv_upper_stmt_ = view_table->ref_query_;
    }
  }
  // expand real-time mv if needed
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(helper.mv_item_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(helper.mv_item_));
  } else if (OB_FAIL(ctx_->session_info_->get_query_rewrite_integrity(rewrite_integrity))) {
    LOG_WARN("failed to get query rewrite integrity", K(ret));
  } else if (QueryRewriteIntegrityType::REWRITE_INTEGRITY_ENFORCED != rewrite_integrity) {
    // do nothing, do not need to expand
  } else if (OB_FALSE_IT(src_qb_name = ctx_->src_qb_name_)) {
  } else if (OB_FALSE_IT(helper.mv_item_->need_expand_rt_mv_ = true)) {
  } else if (OB_FAIL(ObTransformUtils::expand_mview_table(ctx_, helper.mv_upper_stmt_, helper.mv_item_))) {
    LOG_WARN("failed to expand mv table", K(ret));
  } else {
    ctx_->src_qb_name_ = src_qb_name;
  }
  return ret;
}

int ObTransformMVRewrite::check_rewrite_expected(MvRewriteHelper &helper,
                                                 bool &is_expected)
{
  int ret = OB_SUCCESS;
  bool hint_rewrite = false;
  bool hint_no_rewrite = false;
  bool is_match_index = false;
  bool accepted = false;
  bool need_check_cost = !helper.query_delta_table_.is_empty();
  int64_t query_rewrite_enabled = QueryRewriteEnabledType::REWRITE_ENABLED_TRUE;
  ObDMLStmt *ori_stmt = &helper.ori_stmt_;
  ObSelectStmt *rewrite_stmt = helper.query_stmt_;
  is_expected = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)
      || OB_ISNULL(parent_stmts_) || OB_ISNULL(ori_stmt) || OB_ISNULL(rewrite_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(parent_stmts_), K(ori_stmt), K(rewrite_stmt));
  } else if (OB_FAIL(check_hint_valid(helper.ori_stmt_, hint_rewrite, hint_no_rewrite))) {
    LOG_WARN("failed to check mv rewrite hint", K(ret));
  } else if (hint_rewrite) {
    is_expected = true;
    OPT_TRACE("hint force mv rewrite, skip cost check");
  } else if (OB_FAIL(ctx_->session_info_->get_query_rewrite_enabled(query_rewrite_enabled))) {
    LOG_WARN("failed to get query rewrite enabled", K(ret));
  } else if (QueryRewriteEnabledType::REWRITE_ENABLED_FORCE == query_rewrite_enabled) {
    is_expected = true;
    OPT_TRACE("system variable force mv rewrite, skip cost check");
  } else if (OB_FAIL(check_condition_match_index(helper, is_match_index))) {
    LOG_WARN("failed to check condition match index", K(ret));
  } else if (!is_match_index) {
    is_expected = false;
    OPT_TRACE("condition does not match index, can not rewrite");
  } else if (OB_FAIL(accept_transform(*parent_stmts_, ori_stmt, rewrite_stmt,
                                      !need_check_cost, false, accepted))) {
    LOG_WARN("failed to accept transform", K(ret));
  } else if (!accepted) {
    is_expected = false;
    OPT_TRACE("does not pass the cost check, can not rewrite");
  } else {
    is_expected = true;
  }
  return ret;
}

int ObTransformMVRewrite::check_condition_match_index(MvRewriteHelper &helper,
                                                      bool &is_match_index)
{
  int ret = OB_SUCCESS;
  is_match_index = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(helper.query_stmt_) || OB_ISNULL(helper.mv_upper_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(helper.query_stmt_), K(helper.mv_upper_stmt_));
  } else if (helper.query_stmt_ == helper.mv_upper_stmt_) {
    // there is no mv compensation predicates
    is_match_index = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_match_index && i < helper.mv_upper_stmt_->get_condition_size(); ++i) {
    ObSEArray<ObRawExpr*, 8> column_exprs;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(helper.mv_upper_stmt_->get_condition_expr(i), column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !is_match_index && j < column_exprs.count(); ++j) {
      ObRawExpr *e = column_exprs.at(j);
      ObColumnRefRawExpr *col_expr = NULL;
      if (OB_ISNULL(e) || OB_UNLIKELY(!e->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(e))) {
      } else if (OB_FAIL(ObTransformUtils::check_column_match_index(helper.query_stmt_,
                                                                    helper.mv_upper_stmt_,
                                                                    ctx_->sql_schema_guard_,
                                                                    col_expr,
                                                                    is_match_index))) {
        LOG_WARN("failed to check column expr is match index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformMVRewrite::add_param_constraint(MvRewriteHelper &helper)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(append_array_no_dup(ctx_->equal_param_constraints_, helper.equal_param_info_))) {
    LOG_WARN("failed to add equal param constraint", K(ret));
  } else if (OB_FAIL(append_array_no_dup(ctx_->plan_const_param_constraints_, helper.const_param_info_))) {
    LOG_WARN("failed to add const param constraint", K(ret));
  } else if (OB_FAIL(append_array_no_dup(ctx_->expr_constraints_, helper.expr_cons_info_))) {
    LOG_WARN("failed to add expr param constraint", K(ret));
  }
  return ret;
}

bool ObTransformMVRewrite::MvRewriteCheckCtx::compare_column(const ObColumnRefRawExpr &inner,
                                                             const ObColumnRefRawExpr &outer)
{
  int &ret = err_code_;
  bool bret = false;
  bool is_inner_from_query;
  bool is_outer_from_query;
  if (OB_ISNULL(helper_.query_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(helper_.query_stmt_));
  } else {
    const ColumnItem *col_item_inner = helper_.query_stmt_->get_column_item(inner.get_table_id(),
                                                                            inner.get_column_id());
    const ColumnItem *col_item_outer = helper_.query_stmt_->get_column_item(outer.get_table_id(),
                                                                            outer.get_column_id());
    is_inner_from_query = NULL != col_item_inner && col_item_inner->get_expr() == &inner;
    is_outer_from_query = NULL != col_item_outer && col_item_outer->get_expr() == &outer;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if ((is_inner_from_query && is_outer_from_query)
      || (!is_inner_from_query && !is_outer_from_query)) {
    // inner and outer are from the same stmt
    bret = &inner == &outer;
  } else {
    int64_t query_rel_id;
    int64_t mv_tbl_idx; // = rel id - 1
    const TableItem *mv_table_item;
    const ObColumnRefRawExpr *mv_col = is_inner_from_query ? &outer : &inner;
    const ObColumnRefRawExpr *query_col = is_inner_from_query ? &inner : &outer;
    if (mv_col->get_column_id() != query_col->get_column_id()) {
      // do nothig, not match
    } else if (OB_ISNULL(helper_.mv_info_.view_stmt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv stmt is null", K(ret));
    } else if (OB_FALSE_IT(query_rel_id = helper_.query_stmt_->get_table_bit_index(query_col->get_table_id()))) {
    } else if (OB_UNLIKELY(query_rel_id < 1 || query_rel_id > helper_.base_table_map_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected query rel id", K(ret), K(query_rel_id));
    } else if (OB_FALSE_IT(mv_tbl_idx = helper_.base_table_map_.at(query_rel_id - 1))) {
    } else if (OB_UNLIKELY(mv_tbl_idx < 0 || mv_tbl_idx >= helper_.mv_info_.view_stmt_->get_table_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mv table idx", K(ret), K(mv_tbl_idx), K(query_rel_id));
    } else if (OB_ISNULL(mv_table_item =
               helper_.mv_info_.view_stmt_->get_table_item(mv_tbl_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table item", K(ret), K(mv_tbl_idx), K(query_rel_id));
    } else if (mv_table_item->table_id_ == mv_col->get_table_id()) {
      // column matched
      bret = true;
    } else {
      // do nothing, not match
    }
  }
  return bret;
}

bool ObTransformMVRewrite::MvRewriteCheckCtx::compare_const(const ObConstRawExpr &left,
                                                            const ObConstRawExpr &right)
{
  int &ret = err_code_;
  bool bret = false;
  if (OB_FAIL(ret) || left.get_result_type() != right.get_result_type()) {
    // do nothing
  } else if (&left == &right) {
    bret = true;
  } else if (left.is_param_expr() && right.is_param_expr()) {
    if (!left.has_flag(IS_DYNAMIC_PARAM) || !right.has_flag(IS_DYNAMIC_PARAM)) {
      // one of left or right is mv expr, would not be unknown_expr
      bret = false;
    } else {
      const ObRawExpr *l_expr = static_cast<const ObExecParamRawExpr*>(&left)->get_ref_expr();
      const ObRawExpr *r_expr = static_cast<const ObExecParamRawExpr*>(&right)->get_ref_expr();
      bret = l_expr->same_as(*r_expr, this);
    }
  } else if (left.is_param_expr() || right.is_param_expr()) {
    bret = ObExprEqualCheckContext::compare_const(left, right);
    if (bret && (left.get_value().is_unknown() || right.get_value().is_unknown())) {
      const ObConstRawExpr &unkonwn_expr = left.get_value().is_unknown() ? left : right;
      ObPCConstParamInfo const_param_info;
      if (OB_FAIL(const_param_info.const_idx_.push_back(unkonwn_expr.get_value().get_unknown()))) {
        LOG_WARN("failed to push back element", K(ret));
      } else if (OB_FAIL(const_param_info.const_params_.push_back(unkonwn_expr.get_result_type().get_param()))) {
        LOG_WARN("failed to psuh back param const value", K(ret));
      } else if (OB_FAIL(const_param_info_.push_back(const_param_info))) {
        LOG_WARN("failed to push back const param info", K(ret));
      }
    }
  } else {
    bret = left.get_value().is_equal(right.get_value(), CS_TYPE_BINARY);
  }
  return bret;
}

bool ObTransformMVRewrite::MvRewriteCheckCtx::compare_query(const ObQueryRefRawExpr &first,
                                                            const ObQueryRefRawExpr &second)
{
  int &ret = err_code_;
  bool bret = false;
  ObStmtMapInfo stmt_map_info;
  QueryRelation relation = QueryRelation::QUERY_UNCOMPARABLE;
  const ObSelectStmt *first_sel = NULL;
  const ObSelectStmt *second_sel = NULL;
  if (&first == &second) {
    bret = true;
  } else if (first.is_set() != second.is_set() || first.is_multiset() != second.is_multiset() ||
             OB_ISNULL(first_sel = first.get_ref_stmt()) ||
             OB_ISNULL(second_sel = second.get_ref_stmt())) {
    bret = false;
  } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(first_sel,
                                                            second_sel,
                                                            stmt_map_info,
                                                            relation,
                                                            true, /* is_strict_select_list */
                                                            true, /* need_check_select_items */
                                                            false /* is_in_same_stmt */ ))) {
    LOG_WARN("failed to compute stmt relationship", K(ret));
  } else if (stmt_map_info.is_select_item_equal_ && QueryRelation::QUERY_EQUAL == relation) {
    bret = true;
    if (OB_FAIL(append(equal_param_info_, stmt_map_info.equal_param_map_))) {
      LOG_WARN("failed to append equal param", K(ret));
    } else if (OB_FAIL(append(const_param_info_, stmt_map_info.const_param_map_))) {
      LOG_WARN("failed to append const param", K(ret));
    } else if (OB_FAIL(append(expr_cons_info_, stmt_map_info.expr_cons_map_))) {
      LOG_WARN("failed to append expr param", K(ret));
    }
  }
  return bret;
}

int ObTransformMVRewrite::MvRewriteCheckCtx::append_constraint_info(const MvRewriteCheckCtx &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(equal_param_info_, other.equal_param_info_))) {
    LOG_WARN("failed to append equal param", K(ret));
  } else if (OB_FAIL(append(const_param_info_, other.const_param_info_))) {
    LOG_WARN("failed to append const param", K(ret));
  } else if (OB_FAIL(append(expr_cons_info_, other.expr_cons_info_))) {
    LOG_WARN("failed to append expr param", K(ret));
  }
  return ret;
}

ObTransformMVRewrite::MvRewriteHelper::~MvRewriteHelper() {
  int ret = OB_SUCCESS;
  if (mv_es_map_.created() && OB_FAIL(mv_es_map_.destroy())) {
    LOG_WARN("failed to destroy mv equal set map", K(ret));
  } else if (query_es_map_.created() && OB_FAIL(query_es_map_.destroy())) {
    LOG_WARN("failed to destroy query equal set map", K(ret));
  }
}

} //namespace sql
} //namespace oceanbase