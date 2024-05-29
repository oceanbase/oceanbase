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

#ifndef _OB_TRANSFORM_MV_REWRITE_H
#define _OB_TRANSFORM_MV_REWRITE_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "objit/common/ob_item_type.h"

namespace oceanbase
{

namespace common
{
class ObIAllocator;
template <typename T>
class ObIArray;
}//common

namespace sql
{

enum QueryRewriteEnabledType {
  REWRITE_ENABLED_FALSE = 0,
  REWRITE_ENABLED_TRUE     ,
  REWRITE_ENABLED_FORCE
};

enum QueryRewriteIntegrityType {
  REWRITE_INTEGRITY_ENFORCED        = 0,
  REWRITE_INTEGRITY_STALE_TOLERATED
};

/**
 * @brief ObTransformMVRewrite
 *
 * Rewrite query with materialized view(s)
 *
 * e.g.
 * create materialized view mv as
 * select * from t_info join t_item on t_info.item_id = t_item.item_id;
 *
 * select * from t_info join t_item on t_info.item_id = t_item.item_id where user_id = 'xxx';
 * rewrite ==>
 * select * from mv where user_id = 'xxx';
 */
class ObTransformMVRewrite : public ObTransformRule
{
public:
  ObTransformMVRewrite(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::PRE_ORDER, T_MV_REWRITE),
      is_mv_info_generated_(false),
      mv_stmt_gen_count_(0) {}
  virtual ~ObTransformMVRewrite();
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;

protected:

private:
  struct MvInfo {
    MvInfo() : mv_id_(common::OB_INVALID_ID),
               data_table_id_(common::OB_INVALID_ID),
               mv_schema_(NULL),
               data_table_schema_(NULL),
               db_schema_(NULL),
               view_stmt_(NULL),
               select_mv_stmt_(NULL) {}

    MvInfo(uint64_t mv_id,
           uint64_t data_table_id,
           const ObTableSchema *mv_schema,
           const ObTableSchema *data_table_schema,
           const ObDatabaseSchema *db_schema,
           ObSelectStmt *view_stmt)
          : mv_id_(mv_id),
            data_table_id_(data_table_id),
            mv_schema_(mv_schema),
            data_table_schema_(data_table_schema),
            db_schema_(db_schema),
            view_stmt_(view_stmt),
            select_mv_stmt_(NULL) {}

    TO_STRING_KV(
      K_(mv_id),
      K_(data_table_id),
      K_(mv_schema),
      K_(data_table_schema),
      K_(db_schema),
      K_(view_stmt),
      K_(select_mv_stmt)
    );

    uint64_t mv_id_;
    uint64_t data_table_id_;
    const ObTableSchema *mv_schema_;
    const ObTableSchema *data_table_schema_;
    const ObDatabaseSchema *db_schema_;
    ObSelectStmt *view_stmt_;
    ObSelectStmt *select_mv_stmt_;
  };

  struct GenerateStmtHelper {
    GenerateStmtHelper(ObRawExprFactory &expr_factory) : new_stmt_(NULL),
                                                         mv_item_(NULL),
                                                         map_info_(NULL),
                                                         relation_(QUERY_UNCOMPARABLE),
                                                         compute_expr_copier_(expr_factory),
                                                         col_copier_(expr_factory) {}
    ObSelectStmt *new_stmt_;
    TableItem *mv_item_;
    ObStmtMapInfo *map_info_;
    QueryRelation relation_;
    ObRawExprCopier compute_expr_copier_;
    ObRawExprCopier col_copier_;
  };

private:
  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt,
                             bool &need_trans) override;
  int check_hint_valid(const ObDMLStmt &stmt,
                       bool &force_rewrite,
                       bool &force_no_rewrite);
  int check_table_has_mv(const ObDMLStmt &stmt,
                         bool &has_mv);
  int check_basic_validity(const ObDMLStmt &stmt,
                           bool &is_valid);
  int prepare_mv_info(const ObDMLStmt *root_stmt,
                      const ObDMLStmt *stmt);
  int get_mv_list(const ObDMLStmt *root_stmt,
                  ObIArray<uint64_t> &mv_list);
  int get_base_table_id_string(const ObDMLStmt *stmt,
                               ObSqlString &table_ids);
  int get_all_base_table_id(const ObDMLStmt *stmt,
                            ObIArray<uint64_t> &table_ids);
  int generate_mv_info(const ObDMLStmt *stmt,
                       ObIArray<uint64_t> &mv_list);
  int generate_mv_stmt(MvInfo &mv_info);
  int quick_rewrite_check(const ObTableSchema *mv_schema,
                          bool allow_stale,
                          bool &is_valid);
  int check_mv_stmt_basic_validity(const MvInfo &mv_info,
                                   bool &is_valid);
  int try_transform_with_one_mv(ObSelectStmt *origin_stmt,
                                MvInfo &mv_info,
                                ObSelectStmt *&new_stmt,
                                bool &transform_happened);
  int do_transform(ObDMLStmt *&stmt,
                   bool &trans_happened);
  int do_transform_use_one_mv(ObSelectStmt *origin_stmt,
                              const MvInfo &mv_info,
                              ObStmtMapInfo &map_info,
                              ObSelectStmt *&new_stmt,
                              bool &is_valid_transform);
  int create_mv_table_item(const MvInfo &mv_info,
                           GenerateStmtHelper &helper);
  int create_mv_column_item(const MvInfo &mv_info,
                            GenerateStmtHelper &helper);
  int fill_from_item(ObSelectStmt *origin_stmt,
                     const MvInfo &mv_info,
                     GenerateStmtHelper &helper,
                     bool &is_valid);
  int fill_select_item(ObSelectStmt *origin_stmt,
                       const MvInfo &mv_info,
                       GenerateStmtHelper &helper,
                       bool &is_valid);
  int fill_condition_exprs(ObSelectStmt *origin_stmt,
                           const MvInfo &mv_info,
                           GenerateStmtHelper &helper,
                           bool &is_valid);
  int fill_having_exprs(ObSelectStmt *origin_stmt,
                        const MvInfo &mv_info,
                        GenerateStmtHelper &helper,
                        bool &is_valid);
  int fill_groupby_exprs(ObSelectStmt *origin_stmt,
                         const MvInfo &mv_info,
                         GenerateStmtHelper &helper,
                         bool &is_valid);
  int fill_distinct(ObSelectStmt *origin_stmt,
                    const MvInfo &mv_info,
                    GenerateStmtHelper &helper,
                    bool &is_valid);
  int fill_orderby_exprs(ObSelectStmt *origin_stmt,
                         const MvInfo &mv_info,
                         GenerateStmtHelper &helper,
                         bool &is_valid);
  int fill_limit(ObSelectStmt *origin_stmt,
                 const MvInfo &mv_info,
                 GenerateStmtHelper &helper,
                 bool &is_valid);
  int expand_rt_mv_table(GenerateStmtHelper &helper);
  int check_rewrite_expected(const ObSelectStmt *origin_stmt,
                             const ObSelectStmt *new_stmt,
                             const MvInfo &mv_info,
                             bool &is_expected);
  int check_condition_match_index(const ObSelectStmt *new_stmt,
                                  bool &is_match_index);
  int add_param_constraint(const ObStmtMapInfo &map_info);

  int check_mv_has_multi_part(const MvInfo &mv_info,
                              bool &has_multi_part);

private:
  ObSEArray<MvInfo, 4> mv_infos_;
  bool is_mv_info_generated_;
  int64_t mv_stmt_gen_count_;
  ObQueryCtx mv_temp_query_ctx_; // used for generating mv stmt

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformMVRewrite);

};

} //namespace sql
} //namespace oceanbase
#endif