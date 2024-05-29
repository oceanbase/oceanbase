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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_conflict_detector.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

#include "sql/optimizer/ob_join_property.map"

int ObConflictDetector::build_confict(ObIAllocator &allocator, ObConflictDetector* &detector)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(detector = static_cast<ObConflictDetector*>(allocator.alloc(sizeof(ObConflictDetector))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for conflict detector failed");
  } else {
    new(detector) ObConflictDetector();
  }
  return ret;
}


/**
 * 检查两个join运算是否满足结合律
 * 如果是left outer join assoc left outer join
 * 需要满足第二个连接的条件拒绝第二张表的空值
 * 如果是full outer join assoc left outer join
 * 需要满足第二个连接的条件拒绝第二张表的空值
 * 如果是full outer join assoc full outer join
 * 需要满足第二个连接的条件拒绝第二张表的空值
 */
int ObConflictDetector::satisfy_associativity_rule(const ObConflictDetector &left,
                                                   const ObConflictDetector &right,
                                                   bool &is_satisfy)

{
  int ret = OB_SUCCESS;
  if (!ASSOC_PROPERTY[left.join_info_.join_type_][right.join_info_.join_type_]) {
    is_satisfy = false;
  } else if ((LEFT_OUTER_JOIN == left.join_info_.join_type_ ||
              FULL_OUTER_JOIN == left.join_info_.join_type_) &&
             LEFT_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p23 reject null on A(e2)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                        right.join_info_.on_conditions_,
                        left.R_DS_.is_subset(right.L_DS_) ? left.R_DS_ : right.L_DS_,
                        is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ &&
             FULL_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p12、p23 reject null on A(e2)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                        left.join_info_.on_conditions_,
                        left.R_DS_.is_subset(right.L_DS_) ? left.R_DS_ : right.L_DS_,
                        is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    } else if (!is_satisfy) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                        right.join_info_.on_conditions_,
                        left.R_DS_.is_subset(right.L_DS_) ? left.R_DS_ : right.L_DS_,
                        is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else {
    is_satisfy = true;
  }
  LOG_TRACE("succeed to check assoc", K(left), K(right), K(is_satisfy));
  return ret;
}

/**
 * 检查两个join运算是否满足左交换律
 * 对于left outer join、full outer join
 * 需要满足额外的空值拒绝条件
 */
int ObConflictDetector::satisfy_left_asscom_rule(const ObConflictDetector &left,
                                                 const ObConflictDetector &right,
                                                 bool &is_satisfy)

{
  int ret = OB_SUCCESS;
  if (!L_ASSCOM_PROPERTY[left.join_info_.join_type_][right.join_info_.join_type_]) {
    is_satisfy = false;
  } else if (LEFT_OUTER_JOIN == left.join_info_.join_type_ &&
             FULL_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p12 reject null on A(e1)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                          left.join_info_.on_conditions_,
                                          left.L_DS_,
                                          is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ &&
             LEFT_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p13 reject null on A(e1)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                          right.join_info_.on_conditions_,
                                          left.L_DS_,
                                          is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ &&
             FULL_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p12、p13 reject null on A(e1)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                          left.join_info_.on_conditions_,
                                          left.L_DS_,
                                          is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    } else if (!is_satisfy) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                                right.join_info_.on_conditions_,
                                                left.L_DS_,
                                                is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else {
    is_satisfy = true;
  }
  LOG_TRACE("succeed to check l-asscom", K(left), K(right), K(is_satisfy));
  return ret;
}

/**
 * 检查两个join运算是否满足右交换律
 * 对于left outer join、full outer join
 * 需要满足额外的空值拒绝条件
 */
int ObConflictDetector::satisfy_right_asscom_rule(const ObConflictDetector &left,
                                                  const ObConflictDetector &right,
                                                  bool &is_satisfy)

{
  int ret = OB_SUCCESS;
  if (!R_ASSCOM_PROPERTY[left.join_info_.join_type_][right.join_info_.join_type_]) {
    is_satisfy = false;
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ &&
             FULL_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p12、p23 reject null on A(e3)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                          left.join_info_.on_conditions_,
                                          right.R_DS_,
                                          is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    } else if (!is_satisfy) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                                right.join_info_.on_conditions_,
                                                right.R_DS_,
                                                is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else {
    is_satisfy = true;
  }
  LOG_TRACE("succeed to check r-asscom", K(left), K(right), K(is_satisfy));
  return ret;
}


int ObConflictDetector::check_join_legal(const ObRelIds &left_set,
                                         const ObRelIds &right_set,
                                         const ObRelIds &combined_set,
                                         bool delay_cross_product,
                                         ObIArray<TableDependInfo> &table_depend_infos,
                                         bool &legal)
{
  int ret = OB_SUCCESS;
  legal = true;
  if (INNER_JOIN == join_info_.join_type_) {
    if (!join_info_.table_set_.is_subset(combined_set)) {
      //对于inner join来说只需要检查SES是否包含于left_set u right_set
      legal = false;
    }
  } else {
    if (L_TES_.is_subset(left_set) &&
        R_TES_.is_subset(right_set)) {
      //do nothing
    } else if (!is_commutative_) {
      legal = false;
    } else if (R_TES_.is_subset(left_set) &&
               L_TES_.is_subset(right_set)) {
      //do nothing
    } else {
      legal = false;
    }
    if (legal && !is_commutative_) {
      if (left_set.overlap(R_DS_) ||
          right_set.overlap(L_DS_)) {
        legal = false;
      }
    }
  }
  //如果连接谓词是退化谓词，例如t1 left join t2 on 1=t2.c1，需要额外的检查
  if (OB_FAIL(ret) || !legal) {
    //do nothing
  } else if (is_degenerate_pred_) {
    //check T(left(o)) n S1 != empty ^ T(right(o)) n S2 != empty
    if (L_DS_.overlap(left_set) &&
        R_DS_.overlap(right_set)) {
      //do nothing
    } else if (!is_commutative_) {
      legal = false;
    } else if (R_DS_.overlap(left_set) &&
               L_DS_.overlap(right_set)) {
      //do nothing
    } else {
      legal = false;
    }
  }
  //冲突规则检查
  if (OB_FAIL(ret) || !legal) {
    //do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && legal && i < CR_.count(); ++i) {
      const ObRelIds& T1 = CR_.at(i).first;
      const ObRelIds& T2 = CR_.at(i).second;
      if (T1.overlap(combined_set) && !T2.is_subset(combined_set)) {
        legal = false;
      }
    }
    //检查笛卡尔积的约束
    if (OB_SUCC(ret) && legal) {
      for (int64_t i = 0; OB_SUCC(ret) && legal && i < cross_product_rule_.count(); ++i) {
        const ObRelIds& T1 = cross_product_rule_.at(i).first;
        const ObRelIds& T2 = cross_product_rule_.at(i).second;
        if (T1.overlap(left_set) && !T2.is_subset(left_set)) {
          legal = false;
        } else if (T1.overlap(right_set) && !T2.is_subset(right_set)) {
          legal = false;
        }
      }
    }
    //如果需要延迟笛卡尔积，需要检查是否满足延迟条件
    if (OB_SUCC(ret) && delay_cross_product && legal) {
      for (int64_t i = 0; OB_SUCC(ret) && legal && i < delay_cross_product_rule_.count(); ++i) {
        const ObRelIds& T1 = delay_cross_product_rule_.at(i).first;
        const ObRelIds& T2 = delay_cross_product_rule_.at(i).second;
        if (T1.overlap(left_set) && !T2.is_subset(left_set)) {
          legal = false;
        } else if (T1.overlap(right_set) && !T2.is_subset(right_set)) {
          legal = false;
        }
      }
    }
    //检查dependent function table的约束
    for (int64_t i = 0; OB_SUCC(ret) && legal && i < table_depend_infos.count(); ++i) {
      TableDependInfo &info = table_depend_infos.at(i);
      if (left_set.has_member(info.table_idx_)) {
        legal = info.depend_table_set_.is_subset(left_set);
      } else if (right_set.has_member(info.table_idx_)) {
        legal = info.depend_table_set_.is_subset(left_set) || info.depend_table_set_.is_subset(right_set);
      }
    }
  }
  return ret;
}


int ObConflictDetectorGenerator::generate_conflict_detectors(const ObDMLStmt *stmt,
                                                             const ObIArray<TableItem*> &table_items,
                                                             const ObIArray<SemiInfo*> &semi_infos,
                                                             ObIArray<ObRawExpr*> &quals,
                                                             ObIArray<ObJoinOrder*> &baserels,
                                                             ObIArray<ObConflictDetector*> &conflict_detectors)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  ObSEArray<ObConflictDetector*, 8> semi_join_detectors;
  ObSEArray<ObConflictDetector*, 8> inner_join_detectors;
  LOG_TRACE("start to generate conflict detector", K(table_items), K(semi_infos), K(quals));
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(table_items, table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(generate_inner_join_detectors(stmt,
                                                   table_items,
                                                   quals,
                                                   baserels,
                                                   inner_join_detectors))) {
    LOG_WARN("failed to generate inner join detectors", K(ret));
  } else if (OB_FAIL(generate_semi_join_detectors(stmt,
                                                  semi_infos,
                                                  table_ids,
                                                  inner_join_detectors,
                                                  semi_join_detectors))) {
    LOG_WARN("failed to generate semi join detectors", K(ret));
  } else if (OB_FAIL(append(conflict_detectors, semi_join_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append(conflict_detectors, inner_join_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else {
    LOG_TRACE("succeed to generate confilct detectors",
                                        K(semi_join_detectors),
                                        K(inner_join_detectors));
  }
  return ret;
}

/**
 * 加入冲突规则left --> right
 * 简化规则：
 * A -> B, A -> C
 * 简化为：A -> (B,C)
 * A -> B, C -> B
 * 简化为：(A,C) -> B
 *
 */
int ObConflictDetectorGenerator::add_conflict_rule(const ObRelIds &left,
                                                   const ObRelIds &right,
                                                   ObIArray<std::pair<ObRelIds, ObRelIds>> &rules)
{
  int ret = OB_SUCCESS;
  ObRelIds *left_handle_rule = NULL;
  ObRelIds *right_handle_rule = NULL;
  bool find = false;
  for (int64_t i =0; !find && i < rules.count(); ++i) {
    if (rules.at(i).first.equal(left) && rules.at(i).second.equal(right)) {
      find = true;
    } else if (rules.at(i).first.equal(left)) {
      left_handle_rule = &rules.at(i).second;
    } else if (rules.at(i).second.equal(right)) {
      right_handle_rule = &rules.at(i).first;
    }
  }
  if (find) {
    //do nothing
  } else if (NULL != left_handle_rule) {
    if (OB_FAIL(left_handle_rule->add_members(right))) {
      LOG_WARN("failed to add members", K(ret));
    }
  } else if (NULL != right_handle_rule) {
    if (OB_FAIL(right_handle_rule->add_members(left))) {
      LOG_WARN("failed to add members", K(ret));
    }
  } else {
    std::pair<ObRelIds, ObRelIds> rule;
    if (OB_FAIL(rule.first.add_members(left))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(rule.second.add_members(right))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(rules.push_back(rule))) {
      LOG_WARN("failed to push back rule", K(ret));
    }
  }
  return ret;
}

int ObConflictDetectorGenerator::generate_conflict_rule(ObConflictDetector *parent,
                                                       ObConflictDetector *child,
                                                       bool is_left_child,
                                                       ObIArray<std::pair<ObRelIds, ObRelIds>> &rules)
{
  int ret = OB_SUCCESS;
  bool is_satisfy = false;
  ObRelIds ids;
  if (OB_ISNULL(parent) || OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null detector", K(ret));
  } else if (child->is_redundancy_) {
    //do nothing
  } else if (is_left_child) {
    LOG_TRACE("generate left child conflict rule for ", K(*parent), K(*child));
    //check assoc(o^a, o^b)
    if (OB_FAIL(ObConflictDetector::satisfy_associativity_rule(*child, *parent, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy assoc");
    } else if (OB_FAIL(ids.intersect(child->L_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      //CR += T(right(o^a)) -> T(left(o^a))
      if (OB_FAIL(add_conflict_rule(child->R_DS_, child->L_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else if (OB_FAIL(add_conflict_rule(child->R_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict1", K(rules));
      }
    } else {
      //CR += T(right(o^a)) -> T(left(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->R_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else if (OB_FAIL(add_conflict_rule(child->R_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict2", K(rules));
      }
    }
    //check l-asscom(o^a, o^b)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObConflictDetector::satisfy_left_asscom_rule(*child, *parent, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy l-asscom");
    } else if (OB_FAIL(ids.intersect(child->R_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      //CR += T(left(o^a)) -> T(right(o^a))
      if (OB_FAIL(add_conflict_rule(child->L_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict3", K(rules));
      }
    } else {
      //CR += T(left(o^a)) -> T(right(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->L_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict4", K(rules));
      }
    }
  } else {
    LOG_TRACE("generate right child conflict rule for ", K(*parent), K(*child));
    //check assoc(o^b, o^a)
    if (OB_FAIL(ObConflictDetector::satisfy_associativity_rule(*parent, *child, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy assoc");
    } else if (OB_FAIL(ids.intersect(child->R_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      //CR += T(left(o^a)) -> T(right(o^a))
      if (OB_FAIL(add_conflict_rule(child->L_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict5", K(rules));
      }
    } else {
      //CR += T(left(o^a)) -> T(right(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->L_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict6", K(rules));
      }
    }
    //check r-asscom(o^b, o^a)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObConflictDetector::satisfy_right_asscom_rule(*parent, *child, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy r-asscom");
    } else if (OB_FAIL(ids.intersect(child->L_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      //CR += T(right(o^a)) -> T(left(o^a))
      if (OB_FAIL(add_conflict_rule(child->R_DS_, child->L_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict7", K(rules));
      }
    } else {
      //CR += T(right(o^a)) -> T(left(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->R_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict8", K(rules));
      }
    }
  }
  return ret;
}

int ObConflictDetectorGenerator::generate_semi_join_detectors(const ObDMLStmt *stmt,
                                                              const ObIArray<SemiInfo*> &semi_infos,
                                                              ObRelIds &left_rel_ids,
                                                              const ObIArray<ObConflictDetector*> &inner_join_detectors,
                                                              ObIArray<ObConflictDetector*> &semi_join_detectors)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> right_rel_ids;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
    right_rel_ids.reuse();
    SemiInfo *info = semi_infos.at(i);
    ObConflictDetector *detector = NULL;
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null semi info", K(ret));
      //1. create conflict detector
    } else if (OB_FAIL(ObConflictDetector::build_confict(allocator_, detector))) {
      LOG_WARN("failed to build conflict detector", K(ret));
    } else if (OB_ISNULL(detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null conflict detector", K(ret));
    } else if (OB_FAIL(detector->L_DS_.add_members(left_rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(stmt->get_table_rel_ids(info->right_table_id_, right_rel_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(detector->R_DS_.add_members(right_rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(semi_join_detectors.push_back(detector))) {
      LOG_WARN("failed to push back detector", K(ret));
    } else {
      detector->join_info_.join_type_ = info->join_type_;
      // 2. add equal join conditions
      ObRawExpr *expr = NULL;
      for (int64_t j = 0; OB_SUCC(ret) && j < info->semi_conditions_.count(); ++j) {
        if (OB_ISNULL(expr = info->semi_conditions_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(expr));
        } else if (NULL != onetime_copier_
                   && OB_FAIL(ObRawExprUtils::copy_and_formalize(expr, onetime_copier_, session_info_))) {
          LOG_WARN("failed to try replace onetime subquery", K(ret));
        } else if (OB_FAIL(detector->join_info_.table_set_.add_members(expr->get_relation_ids()))) {
          LOG_WARN("failed to add members", K(ret));
        } else if (OB_FAIL(detector->join_info_.where_conditions_.push_back(expr))) {
          LOG_WARN("failed to push back exprs", K(ret));
        } else if (expr->has_flag(IS_JOIN_COND) &&
                   OB_FAIL(detector->join_info_.equal_join_conditions_.push_back(expr))) {
          LOG_WARN("failed to push back qual", K(ret));
        }
      }
      // 3. add other infos to conflict detector
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(detector->L_TES_.intersect(detector->join_info_.table_set_,
                                                    detector->L_DS_))) {
        LOG_WARN("failed to generate L-TES", K(ret));
      } else if (OB_FAIL(detector->R_TES_.intersect(detector->join_info_.table_set_,
                                                    detector->R_DS_))) {
        LOG_WARN("failed to generate R-TES", K(ret));
      } else if (detector->join_info_.table_set_.overlap(detector->L_DS_) &&
                 detector->join_info_.table_set_.overlap(detector->R_DS_)) {
        detector->is_degenerate_pred_ = false;
        detector->is_commutative_ = COMM_PROPERTY[detector->join_info_.join_type_];
      } else {
        detector->is_degenerate_pred_ = true;
        detector->is_commutative_ = COMM_PROPERTY[detector->join_info_.join_type_];
      }
    }
    // 4. 生成冲突规则
    for (int64_t j = 0; OB_SUCC(ret) && j < inner_join_detectors.count(); ++j) {
      if (OB_FAIL(generate_conflict_rule(detector,
                                         inner_join_detectors.at(j),
                                         true,
                                         detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
  }
  return ret;
}

int ObConflictDetectorGenerator::generate_inner_join_detectors(const ObDMLStmt *stmt,
                                                               const ObIArray<TableItem*> &table_items,
                                                               ObIArray<ObRawExpr*> &quals,
                                                               ObIArray<ObJoinOrder*> &baserels,
                                                               ObIArray<ObConflictDetector*> &inner_join_detectors)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObConflictDetector*, 8> outer_join_detectors;
  ObSEArray<ObRawExpr*, 4> all_table_filters;
  ObSEArray<ObRawExpr*, 4> table_filters;
  ObSEArray<ObRawExpr*, 4> redundant_quals;
  ObSEArray<ObRawExpr*, 4> all_quals;
  ObRelIds all_table_ids;
  ObRelIds table_ids;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::split_or_quals(stmt,
                                                     expr_factory_,
                                                     session_info_,
                                                     table_items,
                                                     quals,
                                                     new_or_quals_))) {
    LOG_WARN("failed to split or quals", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(table_items, all_table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(deduce_redundant_join_conds(stmt,
                                                 quals,
                                                 table_items,
                                                 redundant_quals))) {
    LOG_WARN("failed to deduce redundancy quals", K(ret));
  } else if (OB_FAIL(all_quals.assign(quals))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(append(all_quals, redundant_quals))) {
    LOG_WARN("failed to append array", K(ret));
  } else {
    OPT_TRACE("deduce redundant qual:", redundant_quals);
  }
  //1. 生成单个table item内部的冲突规则
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    table_filters.reuse();
    table_ids.reuse();
    if( OB_ISNULL(table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i));
    } else if (OB_FAIL(stmt->get_table_rel_ids(*table_items.at(i), table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    }
    //找到table item的过滤谓词
    for (int64_t j = 0; OB_SUCC(ret) && j < quals.count(); ++j) {
      ObRawExpr *expr = quals.at(j);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!expr->get_relation_ids().is_subset(table_ids) ||
                  expr->has_flag(CNT_SUB_QUERY)) {
        //do nothing
      } else if (expr->get_relation_ids().is_empty() &&
                 !expr->is_const_expr() &&
                 ObOptimizerUtil::find_item(all_table_filters, expr)) {
        //bug fix:48314988
      } else if (OB_FAIL(table_filters.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(all_table_filters.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_outer_join_detectors(stmt,
                                                table_items.at(i),
                                                table_filters,
                                                baserels,
                                                outer_join_detectors))) {
        LOG_WARN("failed to generate outer join detectors", K(ret));
      }
    }
  }
  //2. 生成from item之间的inner join的冲突检测器
  ObRawExpr *expr = NULL;
  ObConflictDetector *detector = NULL;
  ObSEArray<ObRawExpr*, 4> join_conditions;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_quals.count(); ++i) {
    if (OB_ISNULL(expr = all_quals.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (ObOptimizerUtil::find_item(all_table_filters, expr)) {
      //do nothing
    } else if (OB_FAIL(join_conditions.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(find_inner_conflict_detector(inner_join_detectors,
                                                    expr->get_relation_ids(),
                                                    detector))) {
      LOG_WARN("failed to find conflict detector", K(ret));
    } else if (NULL == detector) {
      if (OB_FAIL(ObConflictDetector::build_confict(allocator_, detector))) {
        LOG_WARN("failed to build conflict detector", K(ret));
      } else if (OB_ISNULL(detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detector", K(ret));
      } else if (OB_FAIL(detector->join_info_.where_conditions_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (expr->has_flag(IS_JOIN_COND) &&
                  OB_FAIL(detector->join_info_.equal_join_conditions_.push_back(expr))) {
        LOG_WARN("failed to push back qual", K(ret));
      } else if (OB_FAIL(detector->join_info_.table_set_.add_members(expr->get_relation_ids()))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (expr->has_flag(CNT_SUB_QUERY) &&
                 OB_FAIL(detector->join_info_.table_set_.add_members(all_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(inner_join_detectors.push_back(detector))) {
        LOG_WARN("failed to push back detector", K(ret));
      } else {
        //检查连接谓词是否是退化谓词
        if (detector->join_info_.table_set_.num_members() > 1) {
          detector->is_degenerate_pred_ = false;
        } else {
          detector->is_degenerate_pred_ = true;
          if (OB_FAIL(detector->L_DS_.add_members(all_table_ids))) {
            LOG_WARN("failed to generate R-TES", K(ret));
          } else if (OB_FAIL(detector->R_DS_.add_members(all_table_ids))) {
            LOG_WARN("failed to generate R-TES", K(ret));
          }
        }
        detector->is_commutative_ = COMM_PROPERTY[INNER_JOIN];
        detector->join_info_.join_type_ = INNER_JOIN;
      }
    } else if (OB_FAIL(detector->join_info_.where_conditions_.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (expr->has_flag(IS_JOIN_COND) &&
               OB_FAIL(detector->join_info_.equal_join_conditions_.push_back(expr))) {
        LOG_WARN("failed to push back qual", K(ret));
    } else if (expr->has_flag(CNT_SUB_QUERY) &&
               OB_FAIL(detector->join_info_.table_set_.add_members(all_table_ids))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  //3. 生成inner join的冲突规则
  for (int64_t i = 0; OB_SUCC(ret) && i < inner_join_detectors.count(); ++i) {
    ObConflictDetector *inner_detector = inner_join_detectors.at(i);
    const ObRelIds &table_set = inner_detector->join_info_.table_set_;
    //对于inner join来说，满足交换律，所以不需要区分L_TES、R_TES
    //为了方便之后统一applicable算法，L_TES、R_TES都等于SES
    if (OB_ISNULL(inner_detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null detector", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < table_items.count(); ++j) {
      table_ids.reuse();
      if (OB_ISNULL(table_items.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret), K(j));
      } else if (OB_FAIL(stmt->get_table_rel_ids(*table_items.at(j), table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (!table_ids.overlap(table_set)) {
        //do nothing
      } else if (OB_FAIL(inner_detector->L_DS_.add_members(table_ids))) {
        LOG_WARN("failed to generate R-TES", K(ret));
      } else if (OB_FAIL(inner_detector->R_DS_.add_members(table_ids))) {
        LOG_WARN("failed to generate R-TES", K(ret));
      }
    }
    //对于inner join来说，满足交换律，所以不需要区分L_TES、R_TES
    //为了方便之后统一applicable算法，L_TES、R_TES都等于SES
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inner_detector->L_TES_.add_members(table_set))) {
      LOG_WARN("failed to generate L-TES", K(ret));
    } else if (OB_FAIL(inner_detector->R_TES_.add_members(table_set))) {
      LOG_WARN("failed to generate R-TES", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < outer_join_detectors.count(); ++j) {
      ObConflictDetector *outer_detector = outer_join_detectors.at(j);
      if (OB_ISNULL(outer_detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detector", K(ret));
      } else if (!IS_OUTER_OR_CONNECT_BY_JOIN(outer_detector->join_info_.join_type_)) {
        //inner join与inner join之前没有冲突，do nothing
      } else if (OB_FAIL(generate_conflict_rule(inner_detector,
                                                outer_detector,
                                                true,
                                                inner_detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      } else if (OB_FAIL(generate_conflict_rule(inner_detector,
                                                outer_detector,
                                                false,
                                                inner_detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
  }
  //4. 生成可选的笛卡尔积
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(inner_join_detectors, outer_join_detectors))) {
      LOG_WARN("failed to append detectors", K(ret));
    } else if (OB_FAIL(generate_cross_product_detector(stmt,
                                                       table_items,
                                                       join_conditions,
                                                       inner_join_detectors))) {
      LOG_WARN("failed to generate cross product detector", K(ret));
    }
  }
  return ret;
}

int ObConflictDetectorGenerator::generate_outer_join_detectors(const ObDMLStmt *stmt,
                                                               TableItem *table_item,
                                                               ObIArray<ObRawExpr*> &table_filter,
                                                               ObIArray<ObJoinOrder*> &baserels,
                                                               ObIArray<ObConflictDetector*> &outer_join_detectors)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = static_cast<JoinedTable*>(table_item);
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table_item->is_joined_table()) {
    //如果是基表，直接把过程谓词分发到join order里
    if (OB_FAIL(distribute_quals(stmt, table_item, table_filter, baserels))) {
      LOG_WARN("failed to distribute table filter", K(ret));
    }
  } else if (INNER_JOIN == joined_table->joined_type_) {
    //抚平joined table内部的inner join
    ObSEArray<TableItem *, 4> table_items;
    ObSEArray<ObConflictDetector*, 4> detectors;
    if (OB_FAIL(flatten_inner_join(table_item, table_filter, table_items))) {
      LOG_WARN("failed to flatten inner join", K(ret));
    } else if (OB_FAIL(SMART_CALL(generate_inner_join_detectors(stmt,
                                                     table_items,
                                                     table_filter,
                                                     baserels,
                                                     detectors)))) {
      LOG_WARN("failed to generate inner join detectors", K(ret));
    } else if (OB_FAIL(append(outer_join_detectors, detectors))) {
      LOG_WARN("failed to append detectors", K(ret));
    }
  } else if (OB_FAIL(inner_generate_outer_join_detectors(stmt,
                                                         joined_table,
                                                         table_filter,
                                                         baserels,
                                                         outer_join_detectors))) {
    LOG_WARN("failed to generate outer join detectors", K(ret));
  }
  return ret;
}

int ObConflictDetectorGenerator::distribute_quals(const ObDMLStmt *stmt,
                                                  TableItem *table_item,
                                                  const ObIArray<ObRawExpr*> &table_filter,
                                                  ObIArray<ObJoinOrder*> &baserels)
{
  int ret = OB_SUCCESS;
  ObJoinOrder *cur_rel = NULL;
  ObSEArray<int64_t, 8> relids;
  ObRelIds table_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(stmt), K(table_item));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*table_item, table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(table_ids.to_array(relids))) {
    LOG_WARN("to_array error", K(ret));
  }  else if (1 != relids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect basic table item", K(ret));
  } else if (OB_FAIL(find_base_rel(baserels, relids.at(0), cur_rel))) {
    LOG_WARN("find_base_rel fails", K(ret));
  } else if (OB_ISNULL(cur_rel)) {
    ret = OB_SQL_OPT_ERROR;
    LOG_WARN("failed to distribute qual to rel", K(baserels), K(relids), K(ret));
  } else if (OB_FAIL(append(cur_rel->get_restrict_infos(), table_filter))) {
    LOG_WARN("failed to distribute qual to rel", K(ret));
  }
  return ret;
}

int ObConflictDetectorGenerator::flatten_inner_join(TableItem *table_item,
                                                    ObIArray<ObRawExpr*> &table_filter,
                                                    ObIArray<TableItem*> &table_items)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = static_cast<JoinedTable*>(table_item);
  ObSEArray<ObRawExpr*, 16> new_conditions;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table_item->is_joined_table() ||
             INNER_JOIN != joined_table->joined_type_) {
    ret = table_items.push_back(table_item);
  } else if (OB_FAIL(SMART_CALL(flatten_inner_join(joined_table->left_table_,
                                                   table_filter,
                                                   table_items)))) {
    LOG_WARN("failed to faltten inner join", K(ret));
  } else if (OB_FAIL(SMART_CALL(flatten_inner_join(joined_table->right_table_,
                                                   table_filter,
                                                   table_items)))) {
    LOG_WARN("failed to faltten inner join", K(ret));
  } else if (NULL != onetime_copier_
             && OB_FAIL(ObRawExprUtils::copy_and_formalize(joined_table->join_conditions_,
                                                           new_conditions,
                                                           onetime_copier_,
                                                           session_info_))) {
    LOG_WARN("failed to adjust join conditions with onetime", K(ret));
  } else if (OB_FAIL(append(table_filter, new_conditions))) {
    LOG_WARN("failed to append exprs", K(ret));
  }
  return ret;
}

int ObConflictDetectorGenerator::inner_generate_outer_join_detectors(const ObDMLStmt *stmt,
                                                                     JoinedTable *joined_table,
                                                                     ObIArray<ObRawExpr*> &table_filter,
                                                                     ObIArray<ObJoinOrder *> &baserels,
                                                                     ObIArray<ObConflictDetector*> &outer_join_detectors)
{
  int ret = OB_SUCCESS;
  ObRelIds table_set;
  ObRelIds left_table_ids;
  ObRelIds right_table_ids;
  ObConflictDetector *detector = NULL;
  ObSEArray<ObRawExpr *, 4> left_quals;
  ObSEArray<ObRawExpr *, 4> right_quals;
  ObSEArray<ObRawExpr *, 4> join_quals;
  ObSEArray<ObConflictDetector*, 4> left_detectors;
  ObSEArray<ObConflictDetector*, 4> right_detectors;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_rel_ids(joined_table->join_conditions_,
                                                             table_set))) {
    LOG_WARN("failed to get table ids", K(ret));
    //1. pushdown where condition
  } else if (OB_FAIL(pushdown_where_filters(stmt,
                                            joined_table,
                                            table_filter,
                                            left_quals,
                                            right_quals))) {
    LOG_WARN("failed to pushdown where filters", K(ret));
    //2. pushdown on condition
  } else if (OB_FAIL(pushdown_on_conditions(stmt,
                                            joined_table,
                                            left_quals,
                                            right_quals,
                                            join_quals))) {
    LOG_WARN("failed to pushdown on conditions", K(ret));
    //3. generate left child detectors
  } else if (OB_FAIL(SMART_CALL(generate_outer_join_detectors(stmt,
                                                   joined_table->left_table_,
                                                   left_quals,
                                                   baserels,
                                                   left_detectors)))) {
    LOG_WARN("failed to generate outer join detectors", K(ret));
  } else if (OB_FAIL(append(outer_join_detectors, left_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
    //4. generate right child detectors
  } else if (OB_FAIL(SMART_CALL(generate_outer_join_detectors(stmt,
                                                   joined_table->right_table_,
                                                   right_quals,
                                                   baserels,
                                                   right_detectors)))) {
    LOG_WARN("failed to generate outer join detectors", K(ret));
  } else if (OB_FAIL(append(outer_join_detectors, right_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
    //5. create outer join detector
  } else if (OB_FAIL(ObConflictDetector::build_confict(allocator_, detector))) {
    LOG_WARN("failed to build conflict detector", K(ret));
  } else if (OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null detector", K(ret));
  } else if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret), KPC(joined_table));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->left_table_, left_table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->right_table_, right_table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(detector->join_info_.table_set_.add_members(table_set))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(detector->L_DS_.add_members(left_table_ids))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(detector->R_DS_.add_members(right_table_ids))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(append(detector->join_info_.on_conditions_,
                            join_quals))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(outer_join_detectors.push_back(detector))) {
    LOG_WARN("failed to push back detecotor", K(ret));
  } else {
    //检查连接是否具备交换律
    detector->is_commutative_ = COMM_PROPERTY[joined_table->joined_type_];
    detector->join_info_.join_type_ = joined_table->joined_type_;
    //检查连接谓词是否是退化谓词
    if (table_set.overlap(left_table_ids) &&
        table_set.overlap(right_table_ids)) {
      detector->is_degenerate_pred_ = false;
    } else {
      detector->is_degenerate_pred_ = true;
    }
    //connect by join强制依赖所有的表，与连接谓词无关
    if (CONNECT_BY_JOIN == joined_table->joined_type_) {
      if (OB_FAIL(detector->join_info_.table_set_.add_members(left_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(detector->join_info_.table_set_.add_members(right_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(detector->L_TES_.intersect(detector->join_info_.table_set_,
                                             detector->L_DS_))) {
        LOG_WARN("failed to generate L-TES", K(ret));
      } else if (OB_FAIL(detector->R_TES_.intersect(detector->join_info_.table_set_,
                                                    detector->R_DS_))) {
        LOG_WARN("failed to generate R-TES", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < join_quals.count(); ++i) {
      ObRawExpr *expr = join_quals.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (expr->has_flag(IS_JOIN_COND) &&
                  OB_FAIL(detector->join_info_.equal_join_conditions_.push_back(expr))) {
        LOG_WARN("failed to push back qual", K(ret));
      }
    }
    //6. generate conflict rules
    for (int64_t i = 0; OB_SUCC(ret) && i < left_detectors.count(); ++i) {
      if (OB_FAIL(generate_conflict_rule(detector,
                                          left_detectors.at(i),
                                          true,
                                          detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_detectors.count(); ++i) {
      if (OB_FAIL(generate_conflict_rule(detector,
                                          right_detectors.at(i),
                                          false,
                                          detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
    //7. generate conflict detector for table filter
    if (OB_SUCC(ret) && !table_filter.empty()) {
      if (OB_FAIL(ObConflictDetector::build_confict(allocator_, detector))) {
        LOG_WARN("failed to build conflict detector", K(ret));
      } else if (OB_ISNULL(detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detector", K(ret));
      } else if (OB_FAIL(append(detector->join_info_.where_conditions_, table_filter))) {
        LOG_WARN("failed to append expr", K(ret));
      } else if (OB_FAIL(detector->join_info_.table_set_.add_members(left_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(detector->join_info_.table_set_.add_members(right_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(outer_join_detectors.push_back(detector))) {
        LOG_WARN("failed to push back detector", K(ret));
      } else {
        detector->is_degenerate_pred_ = false;
        detector->is_commutative_ = COMM_PROPERTY[INNER_JOIN];
        detector->join_info_.join_type_ = INNER_JOIN;
      }
    }
  }
  return ret;
}


int ObConflictDetectorGenerator::pushdown_where_filters(const ObDMLStmt *stmt,
                                                        JoinedTable* joined_table,
                                                        ObIArray<ObRawExpr*> &table_filter,
                                                        ObIArray<ObRawExpr*> &left_quals,
                                                        ObIArray<ObRawExpr*> &right_quals)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret), K(stmt), K(joined_table));
  } else {
    ObRelIds left_table_set;
    ObRelIds right_table_set;
    int64_t N = table_filter.count();
    ObSEArray<ObRawExpr*, 4> new_quals;
    ObJoinType join_type = joined_table->joined_type_;
    if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret), KPC(joined_table));
    } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->left_table_, left_table_set))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->right_table_, right_table_set))) {
      LOG_WARN("failed to get table ids", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      ObRawExpr *qual =  table_filter.at(i);
      if (OB_ISNULL(qual)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(qual), K(ret));
      } else if (qual->has_flag(CNT_ROWNUM)) {
        if (OB_FAIL(new_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (qual->get_relation_ids().is_empty()) {
        if (OB_FAIL(left_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (qual->is_const_expr() &&
                   OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (LEFT_OUTER_JOIN == join_type &&
                 qual->get_relation_ids().is_subset(left_table_set)) {
        if (OB_FAIL(left_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (RIGHT_OUTER_JOIN == join_type &&
                 qual->get_relation_ids().is_subset(right_table_set)) {
        if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(new_quals.push_back(qual))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (T_OP_OR == qual->get_expr_type()) {
        ObOpRawExpr *or_qual = static_cast<ObOpRawExpr *>(qual);
        if (LEFT_OUTER_JOIN == join_type
            && OB_FAIL(ObOptimizerUtil::try_split_or_qual(stmt,
                                                          expr_factory_,
                                                          session_info_,
                                                          left_table_set,
                                                          *or_qual,
                                                          left_quals,
                                                          new_or_quals_))) {
          LOG_WARN("failed to split or qual on left table", K(ret));
        } else if (RIGHT_OUTER_JOIN ==join_type
                  && OB_FAIL(ObOptimizerUtil::try_split_or_qual(stmt,
                                                                expr_factory_,
                                                                session_info_,
                                                                right_table_set,
                                                                *or_qual,
                                                                right_quals,
                                                                new_or_quals_))) {
          LOG_WARN("failed to split or qual on right table", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_filter.assign(new_quals))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObConflictDetectorGenerator::pushdown_on_conditions(const ObDMLStmt *stmt,
                                                        JoinedTable* joined_table,
                                                        ObIArray<ObRawExpr*> &left_quals,
                                                        ObIArray<ObRawExpr*> &right_quals,
                                                        ObIArray<ObRawExpr*> &join_quals)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret), K(stmt), K(joined_table));
  } else {
    ObRawExpr *qual = NULL;
    ObRelIds left_table_set;
    ObRelIds right_table_set;
    ObJoinType join_type = joined_table->joined_type_;
    int64_t N = joined_table->join_conditions_.count();
    ObSEArray<ObRawExpr*, 16> new_conditions;
    if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret), KPC(joined_table));
    } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->left_table_, left_table_set))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->right_table_, right_table_set))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (NULL != onetime_copier_
               && OB_FAIL(ObRawExprUtils::copy_and_formalize(joined_table->join_conditions_,
                                                             new_conditions,
                                                             onetime_copier_,
                                                             session_info_))) {
      LOG_WARN("failed to adjust join conditions with onetime", K(ret));
    } else if (OB_UNLIKELY(new_conditions.count() != N)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr count mismatch", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_ISNULL(qual = new_conditions.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(qual), K(ret));
      } else if (qual->has_flag(CNT_ROWNUM) || qual->has_flag(CNT_SUB_QUERY)) {
        if (OB_FAIL(join_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (RIGHT_OUTER_JOIN == join_type &&
                 qual->get_relation_ids().is_subset(left_table_set)) {
        if (OB_FAIL(left_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (LEFT_OUTER_JOIN == join_type &&
                 qual->get_relation_ids().is_subset(right_table_set)) {
        if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (CONNECT_BY_JOIN == join_type &&
                 !qual->get_relation_ids().is_empty() &&
                 qual->get_relation_ids().is_subset(right_table_set)
                 && !qual->has_flag(CNT_LEVEL)
                 && !qual->has_flag(CNT_PRIOR)
                 && !qual->has_flag(CNT_ROWNUM)) {
        if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(join_quals.push_back(qual))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (!qual->get_relation_ids().is_empty() &&
                  T_OP_OR == qual->get_expr_type()) {
        ObOpRawExpr *or_qual = static_cast<ObOpRawExpr *>(qual);
        if (LEFT_OUTER_JOIN == join_type &&
            OB_FAIL(ObOptimizerUtil::try_split_or_qual(stmt,
                                                       expr_factory_,
                                                       session_info_,
                                                       right_table_set,
                                                       *or_qual,
                                                       right_quals,
                                                       new_or_quals_))) {
          LOG_WARN("failed to split or qual on right table", K(ret));
        } else if (RIGHT_OUTER_JOIN ==join_type &&
                    OB_FAIL(ObOptimizerUtil::try_split_or_qual(stmt,
                                                               expr_factory_,
                                                               session_info_,
                                                               left_table_set,
                                                               *or_qual,
                                                               left_quals,
                                                               new_or_quals_))) {
          LOG_WARN("failed to split or qual on left table", K(ret));
        } else if (CONNECT_BY_JOIN == join_type &&
                    !qual->has_flag(CNT_LEVEL) &&
                    !qual->has_flag(CNT_ROWNUM) &&
                    !qual->has_flag(CNT_PRIOR) &&
                    OB_FAIL(ObOptimizerUtil::try_split_or_qual(stmt,
                                                               expr_factory_,
                                                               session_info_,
                                                               right_table_set,
                                                               *or_qual,
                                                               right_quals,
                                                               new_or_quals_))) {
          LOG_WARN("failed to split or qual on right table", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObConflictDetectorGenerator::generate_cross_product_detector(const ObDMLStmt *stmt,
                                                                 const ObIArray<TableItem*> &table_items,
                                                                 ObIArray<ObRawExpr*> &quals,
                                                                 ObIArray<ObConflictDetector*> &inner_join_detectors)
{
  int ret = OB_SUCCESS;
  //生成笛卡尔积的冲突检测器
  //在OB中，笛卡尔积也是inner join
  ObRelIds table_ids;
  ObConflictDetector *detector = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (table_items.count() < 2) {
    //do nothing
  } else if (OB_FAIL(stmt->get_table_rel_ids(table_items, table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(ObConflictDetector::build_confict(allocator_, detector))) {
    LOG_WARN("failed to build conflict detector", K(ret));
  } else if (OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null conflict detector", K(ret));
  } else if (OB_FAIL(detector->L_DS_.add_members(table_ids))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(detector->R_DS_.add_members(table_ids))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(generate_cross_product_conflict_rule(stmt, detector, table_items, quals))) {
    LOG_WARN("failed to generate cross product conflict rule", K(ret));
  } else if (OB_FAIL(inner_join_detectors.push_back(detector))) {
    LOG_WARN("failed to push back detector", K(ret));
  } else {
    detector->join_info_.join_type_ = INNER_JOIN;
    detector->is_degenerate_pred_ = true;
    detector->is_commutative_ = true;
  }
  return ret;
}

int ObConflictDetectorGenerator::generate_cross_product_conflict_rule(const ObDMLStmt *stmt,
                                                                      ObConflictDetector *cross_product_detector,
                                                                      const ObIArray<TableItem*> &table_items,
                                                                      const ObIArray<ObRawExpr*> &join_conditions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(cross_product_detector)) {
    ret = OB_SUCCESS;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(cross_product_detector));
  } else {
    ObRelIds table_ids;
    bool have_new_connect_info = true;
    ObSEArray<ObRelIds, 8> base_table_ids;
    //连通图信息
    ObSEArray<ObRelIds, 8> connect_infos;
    //初始化base table，joined table看做整体
    //笛卡尔积应该在所有的joined table枚举完再枚举
    LOG_TRACE("start generate cross product conflict rule", K(table_items), K(join_conditions));
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      table_ids.reuse();
      if (OB_ISNULL(table_items.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret), K(i));
      } else if (OB_FAIL(stmt->get_table_rel_ids(*table_items.at(i), table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (OB_FAIL(base_table_ids.push_back(table_ids))) {
        LOG_WARN("failed to push back rel ids", K(ret));
      } else if (OB_FAIL(connect_infos.push_back(table_ids))) {
        LOG_WARN("failed to push back rel ids", K(ret));
      }
    }
    /**
     * 计算连通图信息，核心算法：
     * 初始状态：每个base table为一个独立的图
     * 连通原则：如果某一个join condition引用的表对于某个图而言
     * 只增加了一张表A，那么我们认为这个图通过这个join condition
     * 连通了表A，表A加入这张表。
     * 如果某一个join condition连通了多个图，那么我们认为这些图之间是连通的，
     * 需要合并这些图。
     * 算法思想：由于每个join condition可能相互依赖，所以采用迭代算法，
     * 只要连通图的状态发生变化，就需要遍历未使用的join condition，直到
     * 连通图的状态稳定。
     */
    ObSqlBitSet<> used_join_conditions;
    ObSqlBitSet<> used_infos;
    ObSqlBitSet<> connect_tables;
    while (have_new_connect_info) {
      have_new_connect_info = false;
      //遍历未使用的join condition，检查是否连通某个图
      for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions.count(); ++i) {
        ObRawExpr *expr = join_conditions.at(i);
        if (used_join_conditions.has_member(i)) {
          //do nothing
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (has_depend_table(expr->get_relation_ids())) {
          //do nothing
        } else {
          used_infos.reuse();
          connect_tables.reuse();
          if (OB_FAIL(connect_tables.add_members(expr->get_relation_ids()))) {
            LOG_WARN("failed to add members", K(ret));
          }
          //遍历所有的连通图，检查join condition是否连通某个图
          for (int64_t j = 0; OB_SUCC(ret) && j < connect_infos.count(); ++j) {
            bool is_connected = false;
            if (OB_FAIL(check_join_info(expr->get_relation_ids(),
                                        connect_infos.at(j),
                                        base_table_ids,
                                        is_connected))) {
              LOG_WARN("failed to check join info", K(ret));
            } else if (!is_connected) {
              //do nothing
            } else if (OB_FAIL(used_infos.add_member(j))) {
              LOG_WARN("failed to add member", K(ret));
            } else if (OB_FAIL(connect_tables.add_members(connect_infos.at(j)))) {
              LOG_WARN("failed to add members", K(ret));
            }
          }
          /**
           * 合并连通图，实际上并没有删除冗余的图，
           * 这个最后再统一删除，避免容器反复变更size引起数据拷贝
           */
          if (OB_SUCC(ret) && !used_infos.is_empty()) {
            have_new_connect_info = true;
            if (OB_FAIL(used_join_conditions.add_member(i))) {
              LOG_WARN("failed to add member", K(ret));
            }
            for (int64_t j = 0; OB_SUCC(ret) && j < connect_infos.count(); ++j) {
              if (!used_infos.has_member(j)) {
                //do nothing
              } else if (OB_FAIL(connect_infos.at(j).add_members(connect_tables))) {
                LOG_WARN("failed to add members", K(ret));
              }
            }
            LOG_TRACE("succeed to add new connect info", K(*expr), K(connect_infos));
          }
        }
      }
    }
    //去重
    ObSEArray<ObRelIds, 8> new_connect_infos;
    for (int64_t i = 0; OB_SUCC(ret) && i < connect_infos.count(); ++i) {
      bool find = false;
      for (int64_t j =0; !find && j < new_connect_infos.count(); ++j) {
        if (new_connect_infos.at(j).equal(connect_infos.at(i))) {
          find = true;
        }
      }
      if (!find) {
        ret = new_connect_infos.push_back(connect_infos.at(i));
      }
    }
    //使用连通图信息生成冲突规则
    for (int64_t i = 0; OB_SUCC(ret) && i < base_table_ids.count(); ++i) {
      if (base_table_ids.at(i).num_members() < 2) {
        //do nothing
      } else if (OB_FAIL(add_conflict_rule(base_table_ids.at(i),
                                          base_table_ids.at(i),
                                          cross_product_detector->cross_product_rule_))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_connect_infos.count(); ++i) {
      if (new_connect_infos.at(i).num_members() < 2) {
        //do nothing
      } else if (OB_FAIL(add_conflict_rule(new_connect_infos.at(i),
                                            new_connect_infos.at(i),
                                            cross_product_detector->delay_cross_product_rule_))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      }
    }
    //为了能够延迟笛卡尔积，需要生成bushy tree info辅助join order枚举
    //例如(A inner join B on xxx) inner join (C inner join D on xxx)
    //需要生成bushy tree info(A,B,C,D)
    for (int64_t i = 0; OB_SUCC(ret) && i < new_connect_infos.count(); ++i) {
      if (new_connect_infos.at(i).num_members() > 1) {
        for (int64_t j = i+1; OB_SUCC(ret) && j < new_connect_infos.count(); ++j) {
          if (new_connect_infos.at(j).num_members() > 1) {
            ObRelIds bushy_info;
            if (OB_FAIL(bushy_info.add_members(new_connect_infos.at(i)))) {
              LOG_WARN("failed to add members", K(ret));
            } else if (OB_FAIL(bushy_info.add_members(new_connect_infos.at(j)))) {
              LOG_WARN("failed to add members", K(ret));
            } else if (OB_FAIL(bushy_tree_infos_.push_back(bushy_info))) {
              LOG_WARN("failed to push back info", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (new_connect_infos.count() == 1) {
        //全连通图，意味着这个笛卡尔是冗余的，leading hint使用
        cross_product_detector->is_redundancy_ = true;
      }
    }
    LOG_TRACE("update bushy tree info", K(bushy_tree_infos_));
  }
  return ret;
}

int ObConflictDetectorGenerator::check_join_info(const ObRelIds &left,
                                                 const ObRelIds &right,
                                                 const ObIArray<ObRelIds> &base_table_ids,
                                                 bool &is_connected)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_ids;
  is_connected = false;
  if (!left.overlap(right)) {
    //do nothing
  } else if (OB_FAIL(table_ids.except(left, right))) {
    LOG_WARN("failed to cal except for rel ids", K(ret));
  } else if (table_ids.is_empty()) {
    is_connected = true;
  } else {
    int64_t N = base_table_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && !is_connected && i < N; ++i) {
      if (table_ids.is_subset(base_table_ids.at(i))) {
        is_connected = true;
      }
    }
  }
  return ret;
}

int ObConflictDetectorGenerator::deduce_redundant_join_conds(const ObDMLStmt *stmt,
                                                             const ObIArray<ObRawExpr*> &quals,
                                                             const ObIArray<TableItem*> &table_items,
                                                             ObIArray<ObRawExpr*> &redundant_quals)
{
  int ret = OB_SUCCESS;
  EqualSets all_equal_sets;
  ObSEArray<ObRelIds, 8> connect_infos;
  ObSEArray<ObRelIds, 8> single_table_ids;
  ObRelIds table_ids;
  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_EQUAL_SETS, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_FAIL(ObEqualAnalysis::compute_equal_set(&allocator,
                                                 quals,
                                                 all_equal_sets))) {
    LOG_WARN("failed to compute equal set", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    if (OB_ISNULL(quals.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(connect_infos,
                                               quals.at(i)->get_relation_ids()))) {
      LOG_WARN("failed to add var to array no dup", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    table_ids.reuse();
    if (OB_ISNULL(table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i));
    } else if (OB_FAIL(stmt->get_table_rel_ids(*table_items.at(i), table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(single_table_ids.push_back(table_ids))) {
      LOG_WARN("failed to push back table ids", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_equal_sets.count(); ++i) {
    ObIArray<ObRawExpr*> *esets = all_equal_sets.at(i);
    if (OB_ISNULL(esets)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(deduce_redundant_join_conds_with_equal_set(*esets,
                                                                  connect_infos,
                                                                  single_table_ids,
                                                                  redundant_quals))) {
      LOG_WARN("failed to deduce redundancy quals with equal set", K(ret));
    }
  }
  return ret;
}

int ObConflictDetectorGenerator::deduce_redundant_join_conds_with_equal_set(const ObIArray<ObRawExpr*> &equal_set,
                                                                            ObIArray<ObRelIds> &connect_infos,
                                                                            ObIArray<ObRelIds> &single_table_ids,
                                                                            ObIArray<ObRawExpr*> &redundant_quals)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  ObRawExpr *new_expr = NULL;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  for (int64_t m = 0; OB_SUCC(ret) && m < equal_set.count() - 1; ++m) {
    for (int64_t n = m + 1; OB_SUCC(ret) && n < equal_set.count(); ++n) {
      table_ids.reuse();
      new_expr = NULL;
      if (OB_ISNULL(equal_set.at(m)) ||
          OB_ISNULL(equal_set.at(n))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (equal_set.at(m)->get_result_meta() !=
                  equal_set.at(n)->get_result_meta()) {
        // do nothing
      } else if (!equal_set.at(m)->has_flag(CNT_COLUMN) ||
                 !equal_set.at(n)->has_flag(CNT_COLUMN) ||
                 equal_set.at(m)->get_relation_ids().overlap(equal_set.at(n)->get_relation_ids())) {
        // do nothing
      } else if (OB_FAIL(table_ids.add_members(equal_set.at(m)->get_relation_ids())) ||
                 OB_FAIL(table_ids.add_members(equal_set.at(n)->get_relation_ids()))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (ObOptimizerUtil::find_item(connect_infos, table_ids)) {
        // do nothing
      } else if (ObOptimizerUtil::find_superset(table_ids, single_table_ids)) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                         expr_factory_,
                         session_info_,
                         T_OP_EQ,
                         new_expr,
                         equal_set.at(m),
                         equal_set.at(n)))) {
          LOG_WARN("failed to create double op expr", K(ret));
      } else if (OB_ISNULL(new_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(new_expr->pull_relation_id())) {
        LOG_WARN("failed to pull releation id");
      } else if (OB_FAIL(connect_infos.push_back(table_ids))) {
          LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(redundant_quals.push_back(new_expr))) {
        LOG_WARN("failed to push back array", K(ret));
      }
    }
  }
  return ret;
}

int ObConflictDetectorGenerator::find_inner_conflict_detector(const ObIArray<ObConflictDetector*> &inner_conflict_detectors,
                                                              const ObRelIds &rel_ids,
                                                              ObConflictDetector* &detector)
{
  int ret = OB_SUCCESS;
  detector = NULL;
  int64_t N = inner_conflict_detectors.count();
  for (int64_t i = 0; OB_SUCC(ret) && NULL == detector && i < N; ++i) {
    ObConflictDetector* temp_detector = inner_conflict_detectors.at(i);
    if (OB_ISNULL(temp_detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null detector", K(ret));
    } else if (temp_detector->get_join_info().join_type_ != INNER_JOIN) {
      //do nothing
    } else if (temp_detector->get_join_info().table_set_.equal(rel_ids)) {
      detector = temp_detector;
    }
  }
  return ret;
}

bool ObConflictDetectorGenerator::has_depend_table(const ObRelIds& table_ids)
{
  bool b_ret = false;
  for (int64_t i = 0; !b_ret && i < table_depend_infos_.count(); ++i) {
    TableDependInfo &info = table_depend_infos_.at(i);
    if (table_ids.has_member(info.table_idx_)) {
      b_ret = true;
    }
  }
  return b_ret;
}

int ObConflictDetectorGenerator::find_base_rel(ObIArray<ObJoinOrder*> &base_level,
                                               int64_t table_idx,
                                               ObJoinOrder *&base_rel)
{
  int ret = OB_SUCCESS;
  ObJoinOrder *cur_rel = NULL;
  bool find = false;
  base_rel = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < base_level.count(); ++i) {
    //如果是OJ，这里table_set_可能不止一项，所以不能认为cur_rel->table_set_.num_members() == 1
    if (OB_ISNULL(cur_rel = base_level.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_rel));
    } else if (cur_rel->get_tables().has_member(table_idx)){
      find = true;
      base_rel = cur_rel;
      LOG_TRACE("succeed to find base rel", K(cur_rel->get_tables()), K(table_idx));
    } else { /* do nothing */ }
  }
  return ret;
}