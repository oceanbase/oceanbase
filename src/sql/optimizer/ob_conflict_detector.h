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

#ifndef _OB_CONFLICT_DETECTOR_H
#define _OB_CONFLICT_DETECTOR_H

#include "lib/container/ob_se_array.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{

struct TableDependInfo;
class ObJoinOrder;

/*
 * 用于指示inner join未来的连接条件
 */
struct JoinInfo
{
  JoinInfo() :
      table_set_(),
      on_conditions_(),
      where_conditions_(),
      equal_join_conditions_(),
      join_type_(UNKNOWN_JOIN)
      {}

  JoinInfo(ObJoinType join_type) :
      table_set_(),
      on_conditions_(),
      where_conditions_(),
      equal_join_conditions_(),
      join_type_(join_type)
      {}

  virtual ~JoinInfo() {};
  TO_STRING_KV(K_(join_type),
                K_(table_set),
                K_(on_conditions),
                K_(where_conditions),
                K_(equal_join_conditions));
  ObRelIds table_set_; //要连接的表集合（即包含在join_qual_中的，除自己之外的所有表）
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> on_conditions_; //来自on的条件，如果是outer join
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> where_conditions_; //来自where的条件，如果是outer join，则是join filter，如果是inner join，则是join condition
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> equal_join_conditions_;//是连接条件（outer的on condition，inner join的where condition）的子集，仅简单等值，在预测未来的mergejoin所需的序的时候使用
  ObJoinType join_type_;
};

class ObConflictDetector
{
  friend class ObConflictDetectorGenerator;
public:
  ObConflictDetector() :
    join_info_(),
    CR_(),
    cross_product_rule_(),
    delay_cross_product_rule_(),
    L_TES_(),
    R_TES_(),
    L_DS_(),
    R_DS_(),
    is_degenerate_pred_(false),
    is_commutative_(false),
    is_redundancy_(false)
    {}

  virtual ~ObConflictDetector() {}

  TO_STRING_KV(K_(join_info),
               K_(CR),
               K_(cross_product_rule),
               K_(delay_cross_product_rule),
               K_(L_TES),
               K_(R_TES),
               K_(L_DS),
               K_(R_DS),
               K_(is_degenerate_pred),
               K_(is_commutative),
               K_(is_redundancy));

public:
  inline JoinInfo& get_join_info() {return join_info_;}
  inline const JoinInfo& get_join_info() const {return join_info_;}

  static int build_confict(common::ObIAllocator &allocator,
                           ObConflictDetector* &detector);

  static int satisfy_associativity_rule(const ObConflictDetector &left,
                                        const ObConflictDetector &right,
                                        bool &is_satisfy);

  static int satisfy_left_asscom_rule(const ObConflictDetector &left,
                                      const ObConflictDetector &right,
                                      bool &is_satisfy);

  static int satisfy_right_asscom_rule(const ObConflictDetector &left,
                                       const ObConflictDetector &right,
                                       bool &is_satisfy);

  int check_join_legal(const ObRelIds &left_set,
                       const ObRelIds &right_set,
                       const ObRelIds &combined_set,
                       bool delay_cross_product,
                       ObIArray<TableDependInfo> &table_depend_infos,
                       bool &legal);


private:
  //table set包含的是当前join condition所引用的所有表，也就是SES
  JoinInfo join_info_;
  //conflict rules: R1 -> R2
  common::ObSEArray<std::pair<ObRelIds, ObRelIds> , 4, common::ModulePageAllocator, true> CR_;
  common::ObSEArray<std::pair<ObRelIds, ObRelIds> , 4, common::ModulePageAllocator, true> cross_product_rule_;
  common::ObSEArray<std::pair<ObRelIds, ObRelIds> , 4, common::ModulePageAllocator, true> delay_cross_product_rule_;
  //left total eligibility set
  ObRelIds L_TES_;
  //right total eligibility set
  ObRelIds R_TES_;
  //left degenerate set，用于检查join condition为退化谓词的合法性，存放的是左子树的所有表集
  ObRelIds L_DS_;
  //right degenerate set，存放的是右子树的所有表集
  ObRelIds R_DS_;
  bool is_degenerate_pred_;
  //当前join是否可交换左右表
  bool is_commutative_;
  //为hint生成的冗余笛卡尔积
  bool is_redundancy_;
};

class ObConflictDetectorGenerator
{
public:
  ObConflictDetectorGenerator(common::ObIAllocator &allocator,
                              ObRawExprFactory &expr_factory,
                              ObSQLSessionInfo *session_info,
                              ObRawExprCopier *onetime_copier,
                              common::ObIArray<TableDependInfo> &table_depend_infos,
                              common::ObIArray<ObRelIds> &bushy_tree_infos,
                              common::ObIArray<ObRawExpr*> &new_or_quals) :
    allocator_(allocator),
    expr_factory_(expr_factory),
    session_info_(session_info),
    onetime_copier_(onetime_copier),
    table_depend_infos_(table_depend_infos),
    bushy_tree_infos_(bushy_tree_infos),
    new_or_quals_(new_or_quals)
    {}

  virtual ~ObConflictDetectorGenerator() {}

public:
  int generate_conflict_detectors(const ObDMLStmt *stmt,
                                  const ObIArray<TableItem*> &table_items,
                                  const ObIArray<SemiInfo*> &semi_infos,
                                  ObIArray<ObRawExpr*> &quals,
                                  ObIArray<ObJoinOrder*> &baserels,
                                  ObIArray<ObConflictDetector*> &conflict_detectors);

  inline common::ObIArray<ObRawExpr*> &get_new_or_quals() {return new_or_quals_;}
  inline common::ObIArray<ObRelIds> &get_bushy_tree_infos() {return bushy_tree_infos_;}

private:
  int add_conflict_rule(const ObRelIds &left,
                        const ObRelIds &right,
                        ObIArray<std::pair<ObRelIds, ObRelIds>> &rules);

  int generate_conflict_rule(ObConflictDetector *parent,
                             ObConflictDetector *child,
                             bool is_left_child,
                             ObIArray<std::pair<ObRelIds, ObRelIds>> &rules);

  int generate_semi_join_detectors(const ObDMLStmt *stmt,
                                   const ObIArray<SemiInfo*> &semi_infos,
                                   ObRelIds &left_rel_ids,
                                   const ObIArray<ObConflictDetector*> &inner_join_detectors,
                                   ObIArray<ObConflictDetector*> &semi_join_detectors);

  int generate_inner_join_detectors(const ObDMLStmt *stmt,
                                    const ObIArray<TableItem*> &table_items,
                                    ObIArray<ObRawExpr*> &quals,
                                    ObIArray<ObJoinOrder*> &baserels,
                                    ObIArray<ObConflictDetector*> &inner_join_detectors);

  int generate_outer_join_detectors(const ObDMLStmt *stmt,
                                    TableItem *table_item,
                                    ObIArray<ObRawExpr*> &table_filter,
                                    ObIArray<ObJoinOrder*> &baserels,
                                    ObIArray<ObConflictDetector*> &outer_join_detectors);

  int distribute_quals(const ObDMLStmt *stmt,
                       TableItem *table_item,
                       const ObIArray<ObRawExpr*> &table_filter,
                       ObIArray<ObJoinOrder*> &baserels);

  int flatten_inner_join(TableItem *table_item,
                         ObIArray<ObRawExpr*> &table_filter,
                         ObIArray<TableItem*> &table_items);

  int inner_generate_outer_join_detectors(const ObDMLStmt *stmt,
                                          JoinedTable *joined_table,
                                          ObIArray<ObRawExpr*> &table_filter,
                                          ObIArray<ObJoinOrder*> &baserels,
                                          ObIArray<ObConflictDetector*> &outer_join_detectors);

  int pushdown_where_filters(const ObDMLStmt *stmt,
                             JoinedTable *joined_table,
                             ObIArray<ObRawExpr*> &table_filter,
                             ObIArray<ObRawExpr*> &left_quals,
                             ObIArray<ObRawExpr*> &right_quals);

  int pushdown_on_conditions(const ObDMLStmt *stmt,
                             JoinedTable *joined_table,
                             ObIArray<ObRawExpr*> &left_quals,
                             ObIArray<ObRawExpr*> &right_quals,
                             ObIArray<ObRawExpr*> &join_quals);

  int generate_cross_product_detector(const ObDMLStmt *stmt,
                                      const ObIArray<TableItem*> &table_items,
                                      ObIArray<ObRawExpr*> &quals,
                                      ObIArray<ObConflictDetector*> &inner_join_detectors);

  int generate_cross_product_conflict_rule(const ObDMLStmt *stmt,
                                           ObConflictDetector *cross_product_detector,
                                           const ObIArray<TableItem*> &table_items,
                                           const ObIArray<ObRawExpr*> &join_conditions);

  int check_join_info(const ObRelIds &left,
                      const ObRelIds &right,
                      const ObIArray<ObRelIds> &base_table_ids,
                      bool &is_connected);

  int deduce_redundant_join_conds(const ObDMLStmt *stmt,
                                  const ObIArray<ObRawExpr*> &quals,
                                  const ObIArray<TableItem*> &table_items,
                                  ObIArray<ObRawExpr*> &redundant_quals);

  int deduce_redundant_join_conds_with_equal_set(const ObIArray<ObRawExpr*> &equal_set,
                                                 ObIArray<ObRelIds> &connect_infos,
                                                 ObIArray<ObRelIds> &single_table_ids,
                                                 ObIArray<ObRawExpr*> &redundancy_quals);

  int find_inner_conflict_detector(const ObIArray<ObConflictDetector*> &inner_conflict_detectors,
                                   const ObRelIds &rel_ids,
                                   ObConflictDetector* &detector);

  bool has_depend_table(const ObRelIds& table_ids);

  int find_base_rel(ObIArray<ObJoinOrder*> &base_level, int64_t table_idx, ObJoinOrder *&base_rel);

private:
  common::ObIAllocator &allocator_;
  ObRawExprFactory &expr_factory_;
  ObSQLSessionInfo *session_info_;
  ObRawExprCopier *onetime_copier_;
  common::ObIArray<TableDependInfo> &table_depend_infos_;
  common::ObIArray<ObRelIds> &bushy_tree_infos_;
  common::ObIArray<ObRawExpr*> &new_or_quals_;
};


} // namespace sql
} // namespace oceanbase

#endif /* _OB_CONFLICT_DETECTOR_H */