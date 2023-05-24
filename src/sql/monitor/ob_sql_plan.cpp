// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines implementation of sql plan manager


#define USING_LOG_PREFIX SQL

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/resolver/ddl/ob_explain_stmt.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_log_plan.h"
#include "lib/time/ob_time_utility.h"
#include "lib/json/ob_json.h"
#include "ob_sql_plan_manager.h"
#include "lib/rc/ob_rc.h"
#include "ob_sql_plan.h"
using namespace oceanbase::common;
using namespace oceanbase::json;

#define NEW_PLAN_STR(plan_strs)                                                   \
do {                                                                              \
  if (OB_FAIL(ret)) {                                                             \
  } else if ((OB_FAIL(plan_strs.push_back(ObString(plan_text.pos_ - last_pos,     \
                                          plan_text.buf_ + last_pos))))) {        \
    LOG_WARN("failed to push back obstring", K(ret));                             \
  } else {                                                                        \
    last_pos = plan_text.pos_;                                                    \
  }                                                                               \
} while (0);                                                                      \

static const char *ExplainColumnName[] =
{
  "ID",
  "OPERATOR",
  "NAME",
  "EST.ROWS",
  "EST.TIME(us)",
  "REAL.ROWS",
  "REAL.TIME(us)",
  "IO TIME(us)",
  "CPU TIME(us)"
};
enum ExplainColumnEnumType{
  Id=0,
  Operator,
  Name,
  EstRows,
  EstCost,
  RealRows,
  RealCost,
  IoTime,
  CpuTime,
  MaxPlanColumn
};

namespace oceanbase
{
namespace sql
{

ObSqlPlan::ObSqlPlan(common::ObIAllocator &allocator)
  :allocator_(allocator),
  session_info_(NULL)
{

}

ObSqlPlan::~ObSqlPlan()
{

}

int ObSqlPlan::store_sql_plan(ObLogPlan* plan,
                              int64_t plan_id,
                              uint64_t plan_hash,
                              ObString &sql_id)
{
  int ret = OB_SUCCESS;
  PlanText plan_text;
  ObSEArray<ObSqlPlanItem*, 16> sql_plan_infos;
  if (OB_FAIL(init_buffer(plan_text))) {
    LOG_WARN("failed to init buffer", K(ret));
  } else if (OB_FAIL(get_sql_plan_infos(plan_text,
                                        plan,
                                        sql_plan_infos))) {
    LOG_WARN("failed to get sql plan infos", K(ret));
  } else if (OB_FAIL(set_plan_id_for_excute(sql_plan_infos,
                                            plan_id,
                                            plan_hash,
                                            sql_id,
                                            plan_text))) {
    LOG_WARN("failed to set plan id", K(ret));
  // } else if (OB_FAIL(inner_store_sql_plan(sql_plan_infos, false))) {
  //   LOG_WARN("failed to store sql plan", K(ret));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to store sql plan", K(ret));
    ret = OB_SUCCESS;
  }
  destroy_buffer(plan_text);
  return ret;
}

int ObSqlPlan::store_sql_plan_for_explain(ObLogPlan* plan,
                                          ExplainType type,
                                          const ObExplainDisplayOpt& option,
                                          ObIArray<common::ObString> &plan_strs)
{
  int ret = OB_SUCCESS;
  PlanText plan_text;
  PlanText out_plan_text;
  plan_text.type_ = type;
  ObSEArray<ObSqlPlanItem*, 16> sql_plan_infos;
  ObSEArray<ObPlanRealInfo, 2> dummy_plan_infos;
  if (OB_FAIL(init_buffer(plan_text))) {
    LOG_WARN("failed to init buffer", K(ret));
  } else if (OB_FAIL(get_sql_plan_infos(plan_text,
                                        plan,
                                        sql_plan_infos))) {
    LOG_WARN("failed to get sql plan infos", K(ret));
  } else if (OB_FAIL(set_plan_id_for_explain(sql_plan_infos))) {
    LOG_WARN("failed to set plan id", K(ret));
  // } else if (OB_FAIL(inner_store_sql_plan(sql_plan_infos, true))) {
  //   LOG_WARN("failed to store sql plan", K(ret));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to store sql plan", K(ret));
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(format_sql_plan(sql_plan_infos,
                                     type,
                                     option,
                                     dummy_plan_infos,
                                     out_plan_text))) {
    LOG_WARN("failed to format sql plan", K(ret));
  } else if (OB_FAIL(plan_text_to_strings(out_plan_text, plan_strs))) {
    LOG_WARN("failed to convert plan text to strings", K(ret));
  }

  destroy_buffer(plan_text);
  return ret;
}

int ObSqlPlan::print_sql_plan(ObLogPlan* plan,
                              ExplainType type,
                              const ObExplainDisplayOpt& option,
                              ObIArray<common::ObString> &plan_strs)
{
  int ret = OB_SUCCESS;
  PlanText plan_text;
  PlanText out_plan_text;
  plan_text.type_ = type;
  ObSEArray<ObSqlPlanItem*, 16> sql_plan_infos;
  ObSEArray<ObPlanRealInfo, 2> dummy_plan_infos;
  if (OB_FAIL(init_buffer(plan_text))) {
    LOG_WARN("failed to init buffer", K(ret));
  } else if (OB_FAIL(get_sql_plan_infos(plan_text,
                                        plan,
                                        sql_plan_infos))) {
    LOG_WARN("failed to get sql plan infos", K(ret));
  } else if (OB_FAIL(format_sql_plan(sql_plan_infos,
                                     type,
                                     option,
                                     dummy_plan_infos,
                                     out_plan_text))) {
    LOG_WARN("failed to format sql plan", K(ret));
  } else if (OB_FAIL(plan_text_to_strings(out_plan_text, plan_strs))) {
    LOG_WARN("failed to convert plan text to strings", K(ret));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to store sql plan", K(ret));
    ret = OB_SUCCESS;
  }
  destroy_buffer(plan_text);
  return ret;
}

int ObSqlPlan::get_sql_plan(const ObString &sql_id,
                            int64_t plan_id,
                            ExplainType type,
                            const ObExplainDisplayOpt& option,
                            ObIArray<ObPlanRealInfo> &plan_infos,
                            PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSqlPlanItem*, 16> sql_plan_infos;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session info", K(ret));
  } else {
    ObPlanItemMgr *mgr = session_info_->get_sql_plan_manager();
    if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan manager", K(ret));
    } else if (OB_FAIL(mgr->get_plan(sql_id,
                                     plan_id,
                                     sql_plan_infos))) {
      LOG_WARN("failed to get sql plan", K(ret));
    } else if (OB_FAIL(format_sql_plan(sql_plan_infos,
                                      type,
                                      option,
                                      plan_infos,
                                      plan_text))) {
      LOG_WARN("failed to format sql plan", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_sql_plan_by_hash(const ObString &sql_id,
                                    uint64_t plan_hash,
                                    ExplainType type,
                                    const ObExplainDisplayOpt& option,
                                    ObIArray<ObPlanRealInfo> &plan_infos,
                                    PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSqlPlanItem*, 16> sql_plan_infos;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session info", K(ret));
  } else {
    ObPlanItemMgr *mgr = session_info_->get_sql_plan_manager();
    if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan manager", K(ret));
    } else if (OB_FAIL(mgr->get_plan_by_hash(sql_id,
                                             plan_hash,
                                             sql_plan_infos))) {
      LOG_WARN("failed to get sql plan", K(ret));
    } else if (OB_FAIL(format_sql_plan(sql_plan_infos,
                                      type,
                                      option,
                                      plan_infos,
                                      plan_text))) {
      LOG_WARN("failed to format sql plan", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_last_explain_plan(ExplainType type,
                                    const ObExplainDisplayOpt& option,
                                    PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSqlPlanItem*, 16> sql_plan_infos;
  ObSEArray<ObPlanRealInfo, 2> dummy_plan_infos;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session info", K(ret));
  } else {
    ObPlanItemMgr *mgr = session_info_->get_plan_table_manager();
    int64_t plan_id = 0;
    if (OB_ISNULL(mgr)) {
      //do nothing
    } else if (OB_FAIL(mgr->get_plan(mgr->get_last_plan_id(),
                                     sql_plan_infos))) {
      LOG_WARN("failed to get sql plan", K(ret));
    } else if (OB_FAIL(format_sql_plan(sql_plan_infos,
                                      type,
                                      option,
                                      dummy_plan_infos,
                                      plan_text))) {
      LOG_WARN("failed to format sql plan", K(ret));
    }
  }
  return ret;
}

void ObSqlPlan::set_session_info(ObSQLSessionInfo *session_info)
{
  session_info_ = session_info;
}

int ObSqlPlan::get_plan_outline_info_one_line(PlanText &plan_text,
                                              ObLogPlan* plan)
{
  int ret = OB_SUCCESS;
  const ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(plan) || OB_ISNULL(query_ctx = plan->get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(plan), K(query_ctx));
  } else {
    plan_text.is_used_hint_ = false;
    plan_text.is_oneline_ = true;
    plan_text.is_outline_data_ = true;
    BUF_PRINT_CONST_STR("/*+BEGIN_OUTLINE_DATA", plan_text);
    const ObQueryHint &query_hint = query_ctx->get_query_hint();
    if (OB_FAIL(reset_plan_tree_outline_flag(plan->get_plan_root()))) {
      LOG_WARN("failed to reset plan tree outline flag", K(ret));
    } else if (OB_FAIL(get_plan_tree_outline(plan_text, plan->get_plan_root()))) {
      LOG_WARN("failed to get plan tree outline", K(ret));
    } else if (OB_FAIL(query_hint.print_transform_hints(plan_text))) {
      LOG_WARN("failed to print all transform hints", K(ret));
    } else if (OB_FAIL(query_hint.get_global_hint().print_global_hint(plan_text, /*ignore_parallel*/false))) {
      LOG_WARN("failed to print global hint", K(ret));
    } else {
      BUF_PRINT_CONST_STR(" END_OUTLINE_DATA*/", plan_text);
      plan_text.is_outline_data_ = false;
    }
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSqlPlan::set_plan_id_for_explain(ObIArray<ObSqlPlanItem*> &sql_plan_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session info", K(ret));
  } else {
    ObPlanItemMgr *mgr = session_info_->get_plan_table_manager();
    int64_t timestamp = common::ObTimeUtility::current_time();
    int64_t plan_id = 0;
    if (OB_ISNULL(mgr)) {
      //do nothing
    } else {
      plan_id = mgr->get_next_plan_id();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
      ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
      if (OB_ISNULL(plan_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null plan item", K(ret));
      } else {
        plan_item->plan_id_ = plan_id;
        plan_item->gmt_create_ = timestamp;
      }
    }
  }
  return ret;
}

int ObSqlPlan::set_plan_id_for_excute(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                      int64_t plan_id,
                                      uint64_t plan_hash,
                                      ObString &sql_id,
                                      PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session info", K(ret));
  } else {
    ObPlanItemMgr *mgr = session_info_->get_sql_plan_manager();
    int64_t timestamp = common::ObTimeUtility::current_time();
    if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan manager", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
      ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
      if (OB_ISNULL(plan_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null plan item", K(ret));
      } else {
        plan_item->db_id_ = session_info_->get_database_id();
        plan_item->plan_id_ = plan_id;
        plan_item->plan_hash_ = plan_hash;
        plan_item->gmt_create_ = timestamp;
        BUF_PRINT_OB_STR(sql_id.ptr(),
                         sql_id.length(),
                         plan_item->sql_id_,
                         plan_item->sql_id_len_);
      }
    }
  }
  return ret;
}

int ObSqlPlan::get_sql_plan_infos(PlanText &plan_text,
                                  ObLogPlan* plan,
                                  ObIArray<ObSqlPlanItem*> &sql_plan_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  //get operator tree info
  } else if (OB_FAIL(get_plan_tree_infos(plan_text,
                                         plan->get_plan_root(),
                                         sql_plan_infos,
                                         0,
                                         1,
                                         false))) {
    LOG_WARN("failed to get plan tree infos", K(ret));
  } else if (sql_plan_infos.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  //get used hint、outline info
  } else if (OB_FAIL(get_plan_used_hint_info(plan_text,
                                             plan,
                                             sql_plan_infos.at(0)))) {
    LOG_WARN("failed to get plan outline info", K(ret));
  } else if (OB_FAIL(get_qb_name_trace(plan_text,
                                      plan,
                                      sql_plan_infos.at(0)))) {
    LOG_WARN("failed to get qb name trace", K(ret));
  } else if (OB_FAIL(get_plan_outline_info(plan_text,
                                           plan,
                                           sql_plan_infos.at(0)))) {
    LOG_WARN("failed to get plan outline info", K(ret));
  } else if (OB_FAIL(get_plan_other_info(plan_text,
                                         plan,
                                         sql_plan_infos.at(0)))) {
    LOG_WARN("failed to get plan other info", K(ret));
  }
  return ret;
}

int ObSqlPlan::get_plan_tree_infos(PlanText &plan_text,
                                  ObLogicalOperator* op,
                                  ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                  int depth,
                                  int position,
                                  bool is_last_child)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan_text.buf_) || OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null op", K(ret));
  } else if (plan_text.buf_len_ - plan_text.pos_ < sizeof(ObSqlPlanItem)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer size is not enough", K(ret));
  } else {
    ObSqlPlanItem *plan_item = new(plan_text.buf_ + plan_text.pos_)ObSqlPlanItem();
    plan_text.pos_ += sizeof(ObSqlPlanItem);
    plan_item->depth_ = depth;
    plan_item->position_ = position;
    plan_item->is_last_child_ = is_last_child;
    if (OB_FAIL(op->get_plan_item_info(plan_text,
                                       *plan_item))) {
      LOG_WARN("failed to get plan item info", K(ret));
    } else if (OB_FAIL(sql_plan_infos.push_back(plan_item))) {
      LOG_WARN("failed to push back sql plan item", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(get_plan_tree_infos(plan_text,
                                                 op->get_child(i),
                                                 sql_plan_infos,
                                                 depth + 1,
                                                 i+1,
                                                 i+1 == op->get_num_of_child())))) {
        LOG_WARN("failed to get child plan tree infos", K(ret));
      }
    }
  }
  return ret;
}

int ObSqlPlan::get_plan_used_hint_info(PlanText &plan_text,
                                      ObLogPlan* plan,
                                      ObSqlPlanItem* sql_plan_item)
{
  int ret = OB_SUCCESS;
  const ObQueryCtx *query_ctx = NULL;
  //print_plan_tree:print_used_hint
  if (OB_ISNULL(plan) || OB_ISNULL(sql_plan_item) ||
      OB_ISNULL(query_ctx = plan->get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(plan), K(query_ctx));
  } else {
    const ObQueryHint &query_hint = query_ctx->get_query_hint();
    PlanText temp_text;
    temp_text.is_used_hint_ = true;
    temp_text.buf_ = plan_text.buf_ + plan_text.pos_;
    temp_text.buf_len_ = plan_text.buf_len_ - plan_text.pos_;
    temp_text.pos_ = 0;
    BUF_PRINT_CONST_STR("/*+\n      ", temp_text);
    if (OB_FAIL(get_plan_tree_used_hint(temp_text, plan->get_plan_root()))) {
      LOG_WARN("failed to get plan tree used hint", K(ret));
    } else if (OB_FAIL(query_hint.print_qb_name_hints(temp_text))) {
      LOG_WARN("failed to print qb name hints", K(ret));
    } else if (OB_FAIL(query_hint.print_transform_hints(temp_text))) {
      LOG_WARN("failed to print all transform hints", K(ret));
    } else if (OB_FAIL(query_hint.get_global_hint().print_global_hint(temp_text, false))) {
      LOG_WARN("failed to print global hint", K(ret));
    } else {
      BUF_PRINT_CONST_STR("\n*/", temp_text);
      sql_plan_item->other_tag_ = temp_text.buf_;
      sql_plan_item->other_tag_len_ = temp_text.pos_;
      plan_text.pos_ += temp_text.pos_;
    }
  }
  return ret;
}

int ObSqlPlan::get_plan_tree_used_hint(PlanText &plan_text,
                                       ObLogicalOperator* op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null op", K(ret));
  } else if (OB_FAIL(op->print_used_hint(plan_text))) {
    LOG_WARN("failed to get plan used hint", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
    if (OB_FAIL(SMART_CALL(get_plan_tree_used_hint(plan_text,
                                                   op->get_child(i))))) {
      LOG_WARN("failed to get child plan tree used hint", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_qb_name_trace(PlanText &plan_text,
                                ObLogPlan* plan,
                                ObSqlPlanItem* sql_plan_item)
{
  int ret = OB_SUCCESS;
  const ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(plan) || OB_ISNULL(sql_plan_item) ||
      OB_ISNULL(query_ctx = plan->get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(plan), K(query_ctx));
  } else {
    PlanText temp_text;
    temp_text.buf_ = plan_text.buf_ + plan_text.pos_;
    temp_text.buf_len_ = plan_text.buf_len_ - plan_text.pos_;
    temp_text.pos_ = 0;
    char *buf = temp_text.buf_;
    int64_t &buf_len = temp_text.buf_len_;
    int64_t &pos = temp_text.pos_;
    const ObIArray<QbNames> &stmt_id_map = query_ctx->get_query_hint().stmt_id_map_;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt_id_map.count(); ++i) {
      if (OB_FAIL(BUF_PRINTF("  stmt_id:%ld, ", i))) {
      } else if (OB_FAIL(stmt_id_map.at(i).print_qb_names(temp_text))) {
        LOG_WARN("failed to print qb names", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
        /* Do nothing */
      }
    }
    if (OB_SUCC(ret)) {
      sql_plan_item->remarks_ = temp_text.buf_;
      sql_plan_item->remarks_len_ = temp_text.pos_;
      plan_text.pos_ += temp_text.pos_;
    }
  }
  return ret;
}

int ObSqlPlan::get_plan_outline_info(PlanText &plan_text,
                                    ObLogPlan* plan,
                                    ObSqlPlanItem* sql_plan_item)
{
  int ret = OB_SUCCESS;
  const ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(plan) || OB_ISNULL(sql_plan_item) ||
      OB_ISNULL(query_ctx = plan->get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(plan), K(query_ctx));
  } else {
    PlanText temp_text;
    temp_text.is_used_hint_ = false;
    temp_text.is_outline_data_ = true;
    temp_text.buf_ = plan_text.buf_ + plan_text.pos_;
    temp_text.buf_len_ = plan_text.buf_len_ - plan_text.pos_;
    temp_text.pos_ = 0;
    BUF_PRINT_CONST_STR("/*+\n      BEGIN_OUTLINE_DATA", temp_text);
    const ObQueryHint &query_hint = query_ctx->get_query_hint();
    if (OB_FAIL(reset_plan_tree_outline_flag(plan->get_plan_root()))) {
      LOG_WARN("failed to reset plan tree outline flag", K(ret));
    } else if (OB_FAIL(get_plan_tree_outline(temp_text, plan->get_plan_root()))) {
      LOG_WARN("failed to get plan tree outline", K(ret));
    } else if (OB_FAIL(query_hint.print_transform_hints(temp_text))) {
      LOG_WARN("failed to print all transform hints", K(ret));
    } else if (OB_FAIL(query_hint.get_global_hint().print_global_hint(temp_text, false))) {
      LOG_WARN("failed to print global hint", K(ret));
    } else {
      BUF_PRINT_CONST_STR("\n      END_OUTLINE_DATA\n*/", temp_text);
      sql_plan_item->other_xml_ = temp_text.buf_;
      sql_plan_item->other_xml_len_ = temp_text.pos_;
      plan_text.pos_ += temp_text.pos_;
    }
  }
  return ret;
}

int ObSqlPlan::reset_plan_tree_outline_flag(ObLogicalOperator* op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(op->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null op", K(ret));
  } else {
    op->get_plan()->reset_outline_print_flags();
  }
  for (int i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
    if (OB_FAIL(SMART_CALL(reset_plan_tree_outline_flag(op->get_child(i))))) {
      LOG_WARN("failed to reset child plan tree outline flag", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_plan_tree_outline(PlanText &plan_text,
                                     ObLogicalOperator* op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null op", K(ret));
  } else if (OB_FAIL(op->print_outline_data(plan_text))) {
    LOG_WARN("failed to get plan outline", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
    if (OB_FAIL(SMART_CALL(get_plan_tree_outline(plan_text,
                                                 op->get_child(i))))) {
      LOG_WARN("failed to get child plan tree outline", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_plan_other_info(PlanText &plan_text,
                                  ObLogPlan* plan,
                                  ObSqlPlanItem* sql_plan_item)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(plan) || OB_ISNULL(sql_plan_item) ||
      OB_ISNULL(stmt=plan->get_stmt()) || OB_ISNULL(query_ctx=stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else {
    ObObjPrintParams print_params(query_ctx->get_timezone_info());
    BEGIN_BUF_PRINT
    ObPhyPlanType plan_type = plan->get_optimizer_context().get_phy_plan_type();
    if (OB_FAIL(BUF_PRINTF("Plan Type:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                  ob_plan_type_str(plan_type).length(),
                                  ob_plan_type_str(plan_type).ptr()))) {
    } else {
      ret = BUF_PRINTF(NEW_LINE);
    }

    const ParamStore *params = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(params = plan->get_optimizer_context().get_params())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (params->count() <= 0) {
      //do nothing
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF("Parameters:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else { /* Do nothing */ }
    for (int64_t i = 0; OB_SUCC(ret) && NULL != params && i < params->count(); i++) {
      if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      } else if (OB_FAIL(BUF_PRINTF(":%ld => ", i))) {
      } else {
        int64_t save_pos = pos;
        if (OB_FAIL(params->at(i).print_sql_literal(buf,
                                                    buf_len,
                                                    pos,
                                                    print_params))) {
          //ignore size overflow error
          pos = save_pos;
        }
        if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
        } else { /* Do nothing */ }
      }
    }

    const ObPlanNotes &notes = plan->get_optimizer_context().get_plan_notes();
    if (notes.count() <= 0 || OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) { /* Do nothing */
    } else if (OB_FAIL(BUF_PRINTF("Note:"))) { /* Do nothing */
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) { /* Do nothing */
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < notes.count(); i++) {
      if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      } else {
        pos += notes.at(i).to_string(buf + pos, buf_len - pos);
        ret = BUF_PRINTF(NEW_LINE);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_constraint_info(buf, buf_len, pos, *query_ctx))) {
        LOG_WARN("failed to get constraint info", K(ret));
      }
    }
    END_BUF_PRINT(sql_plan_item->other_, sql_plan_item->other_len_);
  }
  // statis version
  // constraint
  return ret;
}

int ObSqlPlan::get_constraint_info(char *buf,
                                  int64_t buf_len,
                                  int64_t &pos,
                                  const ObQueryCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (!ctx.all_plan_const_param_constraints_.empty()) {
    if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF("Const Parameter Constraints:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.all_plan_const_param_constraints_.count(); ++i) {
      if (OB_FAIL(print_constraint_info(buf,
                                        buf_len,
                                        pos,
                                        ctx.all_plan_const_param_constraints_.at(i)))) {
        LOG_WARN("failed to print constraint info", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !ctx.all_possible_const_param_constraints_.empty()) {
    if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF("Possible Const Parameter Constraints:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.all_possible_const_param_constraints_.count(); ++i) {
      if (OB_FAIL(print_constraint_info(buf,
                                        buf_len,
                                        pos,
                                        ctx.all_possible_const_param_constraints_.at(i)))) {
        LOG_WARN("failed to print constraint info", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !ctx.all_equal_param_constraints_.empty()) {
    if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF("Equal Parameter Constraints:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.all_equal_param_constraints_.count(); ++i) {
      if (OB_FAIL(print_constraint_info(buf,
                                        buf_len,
                                        pos,
                                        ctx.all_equal_param_constraints_.at(i)))) {
        LOG_WARN("failed to print constraint info", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !ctx.all_expr_constraints_.empty()) {
    if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF("Expr Constraints:"))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.all_expr_constraints_.count(); ++i) {
      if (OB_FAIL(print_constraint_info(buf,
                                        buf_len,
                                        pos,
                                        ctx.all_expr_constraints_.at(i)))) {
        LOG_WARN("failed to print constraint info", K(ret));
      }
    }
  }
  return ret;
}

int ObSqlPlan::print_constraint_info(char *buf,
                                    int64_t buf_len,
                                    int64_t &pos,
                                    const ObPCConstParamInfo &info)
{
  int ret = OB_SUCCESS;
  if (info.const_idx_.count() != info.const_params_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect constraint info", K(info), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < info.const_idx_.count(); ++i) {
    if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (OB_FAIL(BUF_PRINTF(":%ld = ", info.const_idx_.at(i)))) {
    } else if (OB_FAIL(info.const_params_.at(i).print_sql_literal(buf, buf_len, pos))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObSqlPlan::print_constraint_info(char *buf,
                                    int64_t buf_len,
                                    int64_t &pos,
                                    const ObPCParamEqualInfo &info)
{
  int ret = OB_SUCCESS;
  if (info.use_abs_cmp_) {
    if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (OB_FAIL(BUF_PRINTF(":%ld = abs( :%ld )",
                          info.first_param_idx_,
                          info.second_param_idx_))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else { /* Do nothing */ }
  } else {
    if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (OB_FAIL(BUF_PRINTF(":%ld = :%ld",
                          info.first_param_idx_,
                          info.second_param_idx_))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObSqlPlan::print_constraint_info(char *buf,
                                    int64_t buf_len,
                                    int64_t &pos,
                                    const ObExprConstraint &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info.pre_calc_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
  } else if (OB_FAIL(info.pre_calc_expr_->get_name(buf, buf_len, pos))) {
    LOG_WARN("failed to print expr", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(" result is "))) {
  } else {
    if (PRE_CALC_RESULT_NULL == info.expect_result_) {
      ret = BUF_PRINTF("NULL");
    } else if (PRE_CALC_RESULT_NOT_NULL == info.expect_result_) {
      ret = BUF_PRINTF("NOT NULL");
    } else if (PRE_CALC_RESULT_TRUE == info.expect_result_) {
      ret = BUF_PRINTF("TRUE");
    } else if (PRE_CALC_RESULT_FALSE == info.expect_result_) {
      ret = BUF_PRINTF("FALSE");
    } else if (PRE_CALC_RESULT_NO_WILDCARD == info.expect_result_) {
      ret = BUF_PRINTF("NO_WILDCARD");
    } else if (PRE_CALC_PRECISE == info.expect_result_) {
      ret = BUF_PRINTF("PRECISE");
    } else if (PRE_CALC_NOT_PRECISE == info.expect_result_) {
      ret = BUF_PRINTF("NOT_PRECISE");
    } else if (PRE_CALC_ROWID == info.expect_result_) {
      ret = BUF_PRINTF("ROWID");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObSqlPlan::inner_store_sql_plan(ObIArray<ObSqlPlanItem*> &sql_plan_infos, bool for_explain)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null session info", K(ret));
  } else {
    ObPlanItemMgr *mgr = NULL;
    if (for_explain) {
      mgr = session_info_->get_plan_table_manager();
    } else {
      mgr = session_info_->get_sql_plan_manager();
    }
    if (OB_ISNULL(mgr) && !for_explain) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan manager", K(ret));
    } else if (OB_ISNULL(mgr) && for_explain) {
      //do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
        ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
        if (OB_ISNULL(plan_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null plan item", K(ret));
        } else if (OB_FAIL(mgr->handle_plan_item(*plan_item))) {
          LOG_WARN("failed to handle plan item", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObSqlPlan::format_sql_plan(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                              ExplainType type,
                              const ObExplainDisplayOpt& option,
                              ObIArray<ObPlanRealInfo> &plan_infos,
                              PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_buffer(plan_text))) {
    LOG_WARN("failed to init buffer", K(ret));
  } else if (sql_plan_infos.empty()) {
    //do nothing
  } else {
    if (OB_FAIL(ret)) {
    } else if (EXPLAIN_BASIC == type) {
      if (OB_FAIL(format_basic_plan_table(sql_plan_infos, option, plan_text))) {
        LOG_WARN("failed to print plan", K(ret));
      } else if (OB_FAIL(format_plan_output(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print plan output", K(ret));
      }
    } else if (EXPLAIN_OUTLINE == type) {
      if (OB_FAIL(format_plan_table(sql_plan_infos, option, plan_text))) {
        LOG_WARN("failed to print plan", K(ret));
      } else if (OB_FAIL(format_plan_output(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print plan output", K(ret));
      } else if (OB_FAIL(format_outline(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print outline", K(ret));
      }
    } else if (EXPLAIN_EXTENDED == type) {
      if (option.with_real_info_ &&
          sql_plan_infos.count() == plan_infos.count()) {
        if (OB_FAIL(format_real_plan_table(sql_plan_infos,
                                          option,
                                          plan_infos,
                                          plan_text))) {
          LOG_WARN("failed to print plan", K(ret));
        }
      } else {
        if (OB_FAIL(format_plan_table(sql_plan_infos, option, plan_text))) {
          LOG_WARN("failed to print plan", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(format_plan_output(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print plan output", K(ret));
      } else if (OB_FAIL(format_used_hint(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print used hint", K(ret));
      } else if (OB_FAIL(format_qb_name_trace(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print qb name trace", K(ret));
      } else if (OB_FAIL(format_outline(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print outline", K(ret));
      } else if (OB_FAIL(format_optimizer_info(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print optimizer info", K(ret));
      } else if (OB_FAIL(format_other_info(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print other info", K(ret));
      }
    } else if (EXPLAIN_FORMAT_JSON == type) {
      if (OB_FAIL(format_plan_to_json(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print plan to json", K(ret));
      }
    } else {
      if (OB_FAIL(format_plan_table(sql_plan_infos, option, plan_text))) {
        LOG_WARN("failed to print plan", K(ret));
      } else if (OB_FAIL(format_plan_output(sql_plan_infos, plan_text))) {
        LOG_WARN("failed to print plan output", K(ret));
      }
    }
    BUF_PRINT_CONST_STR(NEW_LINE, plan_text);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(refine_buffer(plan_text))) {
      LOG_WARN("failed to refine buffer", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::PlanFormatHelper::init()
{
  int ret = OB_SUCCESS;
  operator_prefix_.reuse();
  column_len_.reuse();
  total_len_ = 0;
  if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[Id])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[Operator])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[Name])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[EstRows])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[EstCost])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[RealRows])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[RealCost])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[IoTime])))) {
    LOG_WARN("failed to push back data", K(ret));
  } else if (OB_FAIL(column_len_.push_back(strlen(ExplainColumnName[CpuTime])))) {
    LOG_WARN("failed to push back data", K(ret));
  }
  return ret;
}

int ObSqlPlan::get_plan_table_formatter(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                        const ObExplainDisplayOpt& option,
                                        ObSqlPlan::PlanFormatHelper &format_helper)
{
  int ret = OB_SUCCESS;
  int32_t length = 0;
  char buffer[50];
  if (OB_FAIL(format_helper.init())) {
    LOG_WARN("failed to init format helper", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    }
    //ID
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%d", plan_item->id_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(Id)) {
        format_helper.column_len_.at(Id) = length;
      }
    }
    //OPERATOR
    if (OB_SUCC(ret)) {
      if (plan_item->operation_len_ + plan_item->depth_ > format_helper.column_len_.at(Operator)) {
        format_helper.column_len_.at(Operator) = plan_item->operation_len_ + plan_item->depth_;
      }
    }
    //NAME
    if (OB_SUCC(ret)) {
      if (plan_item->object_alias_len_ > format_helper.column_len_.at(Name)) {
        format_helper.column_len_.at(Name) = plan_item->object_alias_len_;
      }
    }
    //EST ROWS
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->cardinality_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(EstRows)) {
        format_helper.column_len_.at(EstRows) = length;
      }
    }
    //EST COST
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->cost_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(EstCost)) {
        format_helper.column_len_.at(EstCost) = length;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_operator_prefix(sql_plan_infos, option, format_helper))) {
      LOG_WARN("failed to get operator prefix", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_real_plan_table_formatter(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                            const ObExplainDisplayOpt& option,
                                            ObIArray<ObPlanRealInfo> &plan_infos,
                                            ObSqlPlan::PlanFormatHelper &format_helper)
{
  int ret = OB_SUCCESS;
  int32_t length = 0;
  char buffer[50];
  if (OB_FAIL(format_helper.init())) {
    LOG_WARN("failed to init format helper", K(ret));
  } else if (plan_infos.count() != sql_plan_infos.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect plan info size", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
    ObPlanRealInfo &plan_info = plan_infos.at(i);
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    }
    //ID
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%d", plan_item->id_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(Id)) {
        format_helper.column_len_.at(Id) = length;
      }
    }
    //OPERATOR
    if (OB_SUCC(ret)) {
      if (plan_item->operation_len_ + plan_item->depth_ > format_helper.column_len_.at(Operator)) {
        format_helper.column_len_.at(Operator) = plan_item->operation_len_ + plan_item->depth_;
      }
    }
    //NAME
    if (OB_SUCC(ret)) {
      if (plan_item->object_alias_len_ > format_helper.column_len_.at(Name)) {
        format_helper.column_len_.at(Name) = plan_item->object_alias_len_;
      }
    }
    //EST ROWS
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->cardinality_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(EstRows)) {
        format_helper.column_len_.at(EstRows) = length;
      }
    }
    //EST COST
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_item->cost_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(EstCost)) {
        format_helper.column_len_.at(EstCost) = length;
      }
    }
    //REAL ROWS
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_info.real_cardinality_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(RealRows)) {
        format_helper.column_len_.at(RealRows) = length;
      }
    }
    //REAL COST
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_info.real_cost_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(RealCost)) {
        format_helper.column_len_.at(RealCost) = length;
      }
    }
    //IO TIME
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_info.io_cost_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(IoTime)) {
        format_helper.column_len_.at(IoTime) = length;
      }
    }
    //CPU TIME
    if (OB_SUCC(ret)) {
      snprintf(buffer, sizeof(buffer), "%ld", plan_info.cpu_cost_);
      length = (int32_t) strlen(buffer);
      if (length > format_helper.column_len_.at(CpuTime)) {
        format_helper.column_len_.at(CpuTime) = length;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_operator_prefix(sql_plan_infos, option, format_helper))) {
      LOG_WARN("failed to get operator prefix", K(ret));
    }
  }
  return ret;
}

int ObSqlPlan::get_operator_prefix(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                  const ObExplainDisplayOpt& option,
                                  PlanFormatHelper &format_helper)
{
  int ret = OB_SUCCESS;
  static const char *colors[] = {
    "\033[32m", // GREEN
    "\033[33m", // ORANGE
    "\033[35m", // PURPLE
    "\033[91m", // LIGHTRED
    "\033[92m", // LIGHTGREEN
    "\033[93m", // YELLOW
    "\033[94m", // LIGHTBLUE
    "\033[95m", // PINK
    "\033[96m", // LIGHTCYAN
    "\033[1;31m", // RED
    "\033[1;34m" // BLUE
  };
  const char *color_end = "\033[0m";
  ObSEArray<int64_t, 4> plan_item_idxs;
  ObSEArray<int64_t, 4> color_idxs;
  ObSEArray<bool, 4> with_line;
  int32_t cur_color_idx = 0;
  int64_t plan_level = 0;
  char *buf = NULL;
  int64_t buf_len =0;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    } else {
      if (plan_item->depth_ > plan_level) {
        //child
        ++plan_level;
      } else if (plan_item->depth_ == plan_level) {
        //brother
      } else {
        //parent
        while (plan_item->depth_ <= plan_level) {
          plan_item_idxs.pop_back();
          with_line.pop_back();
          color_idxs.pop_back();
          --plan_level;
        }
        ++plan_level;
      }
      bool need_line = false;
      if (option.with_tree_line_) {
        if (i + 2 < sql_plan_infos.count()) {
          ObSqlPlanItem *child1 = sql_plan_infos.at(i+1);
          ObSqlPlanItem *child2 = sql_plan_infos.at(i+2);
          if (OB_ISNULL(child1) || OB_ISNULL(child2)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null item", K(ret));
          } else if (child1->depth_ > plan_item->depth_ &&
                      child2->depth_ > child1->depth_ &&
                      !child1->is_last_child_) {
            need_line = true;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!need_line) {
        if (OB_FAIL(with_line.push_back(false))) {
          LOG_WARN("failed to push back value", K(ret));
        } else if (OB_FAIL(color_idxs.push_back(-1))) {
          LOG_WARN("failed to push back value", K(ret));
        }
      } else {
        if (OB_FAIL(with_line.push_back(true))) {
          LOG_WARN("failed to push back value", K(ret));
        } else if (option.with_color_ &&
                    OB_FAIL(color_idxs.push_back(cur_color_idx++))) {
          LOG_WARN("failed to push back value", K(ret));
        } else if (!option.with_color_ &&
                    OB_FAIL(color_idxs.push_back(-1))) {
          LOG_WARN("failed to push back value", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(plan_item_idxs.push_back(i))) {
        LOG_WARN("failed to push back value", K(ret));
      }
      buf_len = plan_level * 15;
      pos = 0;
      if (plan_level > 0) {
        buf = static_cast<char*>(allocator_.alloc(buf_len));
        if (OB_ISNULL(buf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("allocate buffer failed", K(ret));
        }
      }
      //get prefix info
      for (int64_t j = 0; OB_SUCC(ret) && j < plan_level; ++j) {
        ObSqlPlanItem *parent_item = NULL;
        if (j >= with_line.count() ||
            j >= plan_item_idxs.count() ||
            j >= color_idxs.count() ||
            plan_item_idxs.at(j) >= sql_plan_infos.count() ||
            OB_ISNULL(parent_item=sql_plan_infos.at(plan_item_idxs.at(j)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect idx", K(ret));
        } else if (with_line.at(j)) {
          const char *txt = "│";
          if (plan_item->parent_id_ == parent_item->id_) {
            if (plan_item->is_last_child_) {
              txt = "└";
              with_line.at(j) = false;
            } else {
              txt = "├";
            }
          }
          if (color_idxs.at(j) >= 0) {
            ret = BUF_PRINTF("%s%s%s",
                    colors[color_idxs.at(j) % ARRAYSIZEOF(colors)],
                    txt,
                    color_end);
          } else {
            ret = BUF_PRINTF("%s", txt);
          }
        } else {
          ret = BUF_PRINTF(" ");
        }
      }
      ObString prefix(pos, buf);
      format_helper.operator_prefix_.push_back(prefix);
    }
  }
  return ret;
}

int ObSqlPlan::format_basic_plan_table(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                 const ObExplainDisplayOpt& option,
                                 PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObSqlPlan::PlanFormatHelper format_helper;
  if (OB_FAIL(get_plan_table_formatter(sql_plan_infos,
                                       option,
                                       format_helper))) {
    LOG_WARN("failed to get plan table formatter", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    for (int64_t i = 0; OB_SUCC(ret) && i < BASIC_PLAN_TABLE_COLUMN_CNT; ++i) {
      format_helper.total_len_ += format_helper.column_len_.at(i);
    }
    format_helper.total_len_ += BASIC_PLAN_TABLE_COLUMN_CNT+1;
    // print plan text header line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      /* Do nothing */
    } else {
      ret = BUF_PRINTF(COLUMN_SEPARATOR);
    }
    // print column n~ames
    for (int i = 0; OB_SUCC(ret) && i < BASIC_PLAN_TABLE_COLUMN_CNT; i++) {
      if (OB_FAIL(BUF_PRINTF("%-*s", format_helper.column_len_.at(i),
          ExplainColumnName[i]))) { /* Do nothing */
      } else {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }

    // print line separator
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(LINE_SEPARATOR);
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
      ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
      if (OB_ISNULL(plan_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null plan item", K(ret));
      }
      //ID
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*d",
                         format_helper.column_len_.at(Id),
                         plan_item->id_);
      }
      //OPERATOR
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && format_helper.operator_prefix_.at(i).length() > 0) {
        ret = BUF_PRINTF("%*s",
                         format_helper.operator_prefix_.at(i).length(),
                         format_helper.operator_prefix_.at(i).ptr());
      }
      if (OB_SUCC(ret) && plan_item->operation_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Operator) -
                         plan_item->depth_,
                         static_cast<int>(plan_item->operation_len_),
                         plan_item->operation_);
      }
      //NAME
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && plan_item->object_alias_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Name),
                         static_cast<int>(plan_item->object_alias_len_),
                         plan_item->object_alias_);
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      } else if (OB_SUCC(ret)) {
        ret = BUF_PRINTF("%-*s",
                         format_helper.column_len_.at(Name),
                         "");
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret))  {
        ret = BUF_PRINTF(NEW_LINE);
      }
    }
    // print plan text footer line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_SUCC(ret))  {
      ret = BUF_PRINTF(NEW_LINE);
    }
  }
  return ret;
}

int ObSqlPlan::format_plan_table(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                 const ObExplainDisplayOpt& option,
                                 PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObSqlPlan::PlanFormatHelper format_helper;
  if (OB_FAIL(get_plan_table_formatter(sql_plan_infos,
                                       option,
                                       format_helper))) {
    LOG_WARN("failed to get plan table formatter", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    for (int64_t i = 0; OB_SUCC(ret) && i < PLAN_TABLE_COLUMN_CNT; ++i) {
      format_helper.total_len_ += format_helper.column_len_.at(i);
    }
    format_helper.total_len_ += PLAN_TABLE_COLUMN_CNT+1;
    // print plan text header line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else {
      ret = BUF_PRINTF(COLUMN_SEPARATOR);
    }
    // print column n~ames
    for (int i = 0; OB_SUCC(ret) && i < PLAN_TABLE_COLUMN_CNT; i++) {
      if (OB_FAIL(BUF_PRINTF("%-*s", format_helper.column_len_.at(i),
          ExplainColumnName[i]))) {
      } else {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }

    // print line separator
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(LINE_SEPARATOR);
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
      ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
      if (OB_ISNULL(plan_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null plan item", K(ret));
      }
      //ID
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*d",
                         format_helper.column_len_.at(Id),
                         plan_item->id_);
      }
      //OPERATOR
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && format_helper.operator_prefix_.at(i).length() > 0) {
        ret = BUF_PRINTF("%*s",
                         format_helper.operator_prefix_.at(i).length(),
                         format_helper.operator_prefix_.at(i).ptr());
      }
      if (OB_SUCC(ret) && plan_item->operation_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Operator) -
                         plan_item->depth_,
                         static_cast<int>(plan_item->operation_len_),
                         plan_item->operation_);
      }
      //NAME
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && plan_item->object_alias_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Name),
                         static_cast<int>(plan_item->object_alias_len_),
                         plan_item->object_alias_);
      } else if (OB_SUCC(ret)) {
        ret = BUF_PRINTF("%-*s",
                         format_helper.column_len_.at(Name),
                         "");
      }
      //ROWS
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(EstRows),
                         plan_item->cardinality_);
      }
      //COST
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(EstCost),
                         plan_item->cost_);
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret))  {
        ret = BUF_PRINTF(NEW_LINE);
      }
    }
    // print plan text footer line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_SUCC(ret))  {
      ret = BUF_PRINTF(NEW_LINE);
    }
  }
  return ret;
}

int ObSqlPlan::format_real_plan_table(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                      const ObExplainDisplayOpt& option,
                                      ObIArray<ObPlanRealInfo> &plan_infos,
                                      PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObSqlPlan::PlanFormatHelper format_helper;
  if (plan_infos.count() != sql_plan_infos.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect plan info size", K(ret));
  } else if (OB_FAIL(get_real_plan_table_formatter(sql_plan_infos,
                                                  option,
                                                  plan_infos,
                                                  format_helper))) {
    LOG_WARN("failed to get plan table formatter", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    for (int64_t i = 0; OB_SUCC(ret) && i < REAL_PLAN_TABLE_COLUMN_CNT; ++i) {
      format_helper.total_len_ += format_helper.column_len_.at(i);
    }
    format_helper.total_len_ += REAL_PLAN_TABLE_COLUMN_CNT+1;
    // print plan text header line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else {
      ret = BUF_PRINTF(COLUMN_SEPARATOR);
    }
    // print column n~ames
    for (int i = 0; OB_SUCC(ret) && i < REAL_PLAN_TABLE_COLUMN_CNT; i++) {
      if (OB_FAIL(BUF_PRINTF("%-*s", format_helper.column_len_.at(i),
          ExplainColumnName[i]))) {
      } else {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }

    // print line separator
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(LINE_SEPARATOR);
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */ }
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
      ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
      ObPlanRealInfo &plan_info = plan_infos.at(i);
      if (OB_ISNULL(plan_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null plan item", K(ret));
      }
      //ID
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*d",
                         format_helper.column_len_.at(Id),
                         plan_item->id_);
      }
      //OPERATOR
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && format_helper.operator_prefix_.at(i).length() > 0) {
        ret = BUF_PRINTF("%*s",
                         format_helper.operator_prefix_.at(i).length(),
                         format_helper.operator_prefix_.at(i).ptr());
      }
      if (OB_SUCC(ret) && plan_item->operation_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Operator) -
                         plan_item->depth_,
                         static_cast<int>(plan_item->operation_len_),
                         plan_item->operation_);
      }
      //NAME
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret) && plan_item->object_alias_len_ > 0) {
        ret = BUF_PRINTF("%-*.*s",
                         format_helper.column_len_.at(Name),
                         static_cast<int>(plan_item->object_alias_len_),
                         plan_item->object_alias_);
      } else if (OB_SUCC(ret)) {
        ret = BUF_PRINTF("%-*s",
                         format_helper.column_len_.at(Name),
                         "");
      }
      //EST ROWS
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(EstRows),
                         plan_item->cardinality_);
      }
      //EST COST
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(EstCost),
                         plan_item->cost_);
      }
      //REAL ROWS
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(RealRows),
                         plan_info.real_cardinality_);
      }
      //REAL COST
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(RealCost),
                         plan_info.real_cost_);
      }
      //IO COST
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(IoTime),
                         plan_info.io_cost_);
      }
      //CPU COST
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
        ret = BUF_PRINTF("%-*ld",
                         format_helper.column_len_.at(CpuTime),
                         plan_info.cpu_cost_);
        ret = BUF_PRINTF(COLUMN_SEPARATOR);
      }
      if (OB_SUCC(ret))  {
        ret = BUF_PRINTF(NEW_LINE);
      }
    }
    // print plan text footer line
    for (int i = 0; OB_SUCC(ret) && i < format_helper.total_len_; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_SUCC(ret))  {
      ret = BUF_PRINTF(NEW_LINE);
    }
  }
  return ret;
}

int ObSqlPlan::format_plan_output(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("Outputs & filters:"))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    ObSqlPlanItem *plan_item = sql_plan_infos.at(i);
    //print output info
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%3ld - ", i))) {
      //do nothing
    } else if (plan_item->projection_len_ <= 0 &&
               OB_FAIL(BUF_PRINTF("output(nil)"))) {
    } else if (plan_item->projection_len_ > 0 &&
               OB_FAIL(BUF_PRINTF("%.*s",
                                  static_cast<int>(plan_item->projection_len_),
                                  plan_item->projection_))) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
    } else if (plan_item->filter_predicates_len_ <= 0 &&
               OB_FAIL(BUF_PRINTF("filter(nil)"))) {
    } else if (plan_item->filter_predicates_len_ > 0 &&
               OB_FAIL(BUF_PRINTF("%.*s",
                                  static_cast<int>(plan_item->filter_predicates_len_),
                                  plan_item->filter_predicates_))) {
    } else if (plan_item->startup_predicates_len_ <= 0) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
    } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                  static_cast<int>(plan_item->startup_predicates_len_),
                                  plan_item->startup_predicates_))) {
    }
    if (OB_FAIL(ret)) {
    } else if (plan_item->rowset_ <= 1) {
      //do nothing
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
    } else if (OB_FAIL(BUF_PRINTF("rowset=%ld", plan_item->rowset_))) {
    }
    //print access info
    if (OB_FAIL(ret)) {
    } else if (plan_item->partition_start_len_ <= 0 &&
               plan_item->access_predicates_len_ <= 0) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (plan_item->access_predicates_len_ <= 0 &&
               OB_FAIL(BUF_PRINTF("access(nil)"))) {
    } else if (plan_item->access_predicates_len_ > 0 &&
               OB_FAIL(BUF_PRINTF("%.*s",
                                  static_cast<int>(plan_item->access_predicates_len_),
                                  plan_item->access_predicates_))) {
    } else if (plan_item->partition_start_len_ <= 0) {
      //do nothing
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
    } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                  static_cast<int>(plan_item->partition_start_len_),
                                  plan_item->partition_start_))) {
    }
    //print special info
    if (OB_FAIL(ret)) {
    } else if (plan_item->special_predicates_len_ <= 0) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                  static_cast<int>(plan_item->special_predicates_len_),
                                  plan_item->special_predicates_))) {
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
  }
  return ret;
}

int ObSqlPlan::format_used_hint(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_ISNULL(sql_plan_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan item", K(ret));
  } else if (sql_plan_infos.at(0)->other_tag_len_ <= 0) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("Used Hint:"))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                static_cast<int>(sql_plan_infos.at(0)->other_tag_len_),
                                sql_plan_infos.at(0)->other_tag_))) {
  }
  return ret;
}

int ObSqlPlan::format_qb_name_trace(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_ISNULL(sql_plan_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan item", K(ret));
  } else if (sql_plan_infos.at(0)->remarks_len_ <= 0) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("Qb name trace:"))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                static_cast<int>(sql_plan_infos.at(0)->remarks_len_),
                                sql_plan_infos.at(0)->remarks_))) {
  }
  return ret;
}

int ObSqlPlan::format_outline(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_ISNULL(sql_plan_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan item", K(ret));
  } else if (sql_plan_infos.at(0)->other_xml_len_ <= 0) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("Outline Data:"))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                static_cast<int>(sql_plan_infos.at(0)->other_xml_len_),
                                sql_plan_infos.at(0)->other_xml_))) {
  }
  return ret;
}

int ObSqlPlan::format_optimizer_info(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("Optimization Info:"))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
    if (OB_ISNULL(sql_plan_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    } else if (sql_plan_infos.at(i)->optimizer_len_ <= 0) {
      //do nothing
    } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                  static_cast<int>(sql_plan_infos.at(i)->optimizer_len_),
                                  sql_plan_infos.at(i)->optimizer_))) {
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    }
  }
  return ret;
}

int ObSqlPlan::format_other_info(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (sql_plan_infos.empty()) {
    //do nothing
  } else if (OB_ISNULL(sql_plan_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan item", K(ret));
  } else if (sql_plan_infos.at(0)->other_len_ <= 0) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
  } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                static_cast<int>(sql_plan_infos.at(0)->other_len_),
                                sql_plan_infos.at(0)->other_))) {
  }
  return ret;
}

int ObSqlPlan::format_plan_to_json(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  json::Value *ret_val = NULL;
  if (OB_FAIL(inner_format_plan_to_json(sql_plan_infos, 0, ret_val))) {
    LOG_WARN("failed to format plan to json", K(ret));
  } else {
    json::Tidy tidy(ret_val);
    plan_text.pos_ += tidy.to_string(plan_text.buf_ + plan_text.pos_,
                                     plan_text.buf_len_ - plan_text.pos_);
    if (plan_text.buf_len_ - plan_text.pos_ > 0) {
      plan_text.buf_[plan_text.pos_ + 1] = '\0';
    } else {
      plan_text.buf_[plan_text.buf_len_ - 1] = '\0';
    }
  }
  return ret;
}

int ObSqlPlan::inner_format_plan_to_json(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                        int64_t info_idx,
                                        json::Value *&ret_val)
{
  int ret = OB_SUCCESS;
  ret_val = NULL;
  ObSqlPlanItem *plan_item = NULL;
  if (info_idx < 0 || info_idx >= sql_plan_infos.count() ||
      OB_ISNULL(plan_item=sql_plan_infos.at(info_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect info idx", K(ret));
  } else {
    ObIAllocator *allocator = &allocator_;
    json::Pair *id = NULL;
    json::Pair *op = NULL;
    json::Pair *name = NULL;
    json::Pair *rows = NULL;
    json::Pair *cost = NULL;
    json::Pair *output = NULL;

    Value *id_value = NULL;
    Value *op_value = NULL;
    Value *name_value = NULL;
    Value *rows_value = NULL;
    Value *cost_value = NULL;
    Value *output_value = NULL;
    if (OB_ISNULL(ret_val = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(id = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(op = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(name = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(rows = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(cost = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(output = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(id_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(op_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(name_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(rows_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(cost_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(output_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(ret_val = new(ret_val) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(id = new(id) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(op = new(op) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(name = new(name) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(rows = new(rows) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(cost = new(cost) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(output = new(output) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(id_value = new(id_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(op_value = new(op_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(name_value = new(name_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(rows_value = new(rows_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(cost_value = new(cost_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(output_value = new(output_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else {
      ret_val->set_type(JT_OBJECT);
      id_value->set_type(JT_NUMBER);
      // TBD
      id_value->set_int(plan_item->id_);
      id->name_ = ExplainColumnName[Id];
      id->value_ = id_value;

      op_value->set_type(JT_STRING);
      // TBD
      op_value->set_string(plan_item->operation_,
                           plan_item->operation_len_);
      op->name_ = ExplainColumnName[Operator];
      op->value_ = op_value;

      name_value->set_type(JT_STRING);
      // TBD
      name_value->set_string(plan_item->object_alias_,
                             plan_item->object_alias_len_);
      name->name_ = ExplainColumnName[Name];
      name->value_ = name_value;

      rows_value->set_type(JT_NUMBER);
      // TBD
      rows_value->set_int(plan_item->cardinality_);
      rows->name_ = ExplainColumnName[EstRows];
      rows->value_ = rows_value;

      cost_value->set_type(JT_NUMBER);
      // TBD
      cost_value->set_int(plan_item->cost_);
      cost->name_ = ExplainColumnName[EstCost];
      cost->value_ = cost_value;

      output_value->set_type(JT_STRING);
      output_value->set_string(plan_item->projection_,
                               plan_item->projection_len_);

      output->name_ = "output";
      output->value_ = output_value;
      ret_val->object_add(id);
      ret_val->object_add(op);
      ret_val->object_add(name);
      ret_val->object_add(rows);
      ret_val->object_add(cost);
      ret_val->object_add(output);
      // child operator
      Pair *child = NULL;
      const uint64_t OB_MAX_JSON_CHILD_NAME_LENGTH = 64;
      char name_buf[OB_MAX_JSON_CHILD_NAME_LENGTH];
      int64_t name_buf_size = OB_MAX_JSON_CHILD_NAME_LENGTH;
      for (int64_t i = 0; OB_SUCC(ret) && i < sql_plan_infos.count(); ++i) {
        ObSqlPlanItem *child_plan = sql_plan_infos.at(i);
        if (OB_ISNULL(child_plan)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null child plan", K(ret));
        } else if (plan_item->id_ != child_plan->parent_id_) {
          //do nothing
        } else {
          int64_t child_name_pos = snprintf(name_buf, name_buf_size, "CHILD_%d", child_plan->position_);
          ObString child_name(child_name_pos, name_buf);
          if (OB_ISNULL(child = (Pair *)allocator->alloc(sizeof(Pair)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("no memory");
          } else if (OB_ISNULL(child = new(child) Pair())) {
            ret = OB_ERROR;
            LOG_WARN("failed to new json Pair");
          } else if (OB_FAIL(ob_write_string(*allocator, child_name, child->name_))) {
            LOG_WARN("failed to write string", K(ret));
            /* Do nothing */
          } else if (OB_FAIL(SMART_CALL(inner_format_plan_to_json(sql_plan_infos,
                                                                  i,
                                                                  child->value_)))) {
            LOG_WARN("to_json fails", K(ret), K(i));
          } else {
            ret_val->object_add(child);
          }
        }
      }
    }
  }
  return ret;
}

int ObSqlPlan::init_buffer(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  plan_text.buf_len_ = 1024 * 1024;
  plan_text.buf_ = static_cast<char*>(allocator_.alloc(plan_text.buf_len_));
  if (OB_ISNULL(plan_text.buf_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to allocate buffer", "buffer size", plan_text.buf_len_, K(ret));
  }
  return ret;
}

void ObSqlPlan::destroy_buffer(PlanText &plan_text)
{
  if (NULL != plan_text.buf_) {
    allocator_.free(plan_text.buf_);
  }
}

int ObSqlPlan::refine_buffer(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  int64_t new_len = plan_text.pos_;
  char* new_buf = static_cast<char*>(allocator_.alloc(new_len));
  if (OB_ISNULL(new_buf)) {
    //do nothing
  } else {
    MEMCPY(new_buf, plan_text.buf_, new_len);
    destroy_buffer(plan_text);
    plan_text.buf_ = new_buf;
    plan_text.buf_len_ = new_len;
    plan_text.pos_ = new_len;
  }
  return ret;
}

int ObSqlPlan::plan_text_to_string(PlanText &plan_text,
                                  common::ObString &plan_str)
{
  int ret = OB_SUCCESS;
  plan_str = ObString(plan_text.pos_, plan_text.buf_);
  return ret;
}

int ObSqlPlan::plan_text_to_strings(PlanText &plan_text,
                                    ObIArray<common::ObString> &plan_strs)
{
  int ret = OB_SUCCESS;
  int64_t last_pos = 0;
  int64_t MAX_LEN = 150;
  const char line_stop_symbol = '\n';
  const char max_length_symbol = ' ' ;
  for (int64_t i = 0; OB_SUCC(ret) && i < plan_text.pos_; ++i) {
    if (i - last_pos <= MAX_LEN &&
        plan_text.buf_[i] != line_stop_symbol) {
      //keep going
    } else if (i - last_pos > MAX_LEN &&
               plan_text.buf_[i] != line_stop_symbol &&
               plan_text.buf_[i] != max_length_symbol) {
      //keep going
    } else if (i > last_pos &&
               OB_FAIL(plan_strs.push_back(ObString(i - last_pos,
                                                    plan_text.buf_ + last_pos)))) {
      LOG_WARN("failed to push back plan text", K(ret));
    } else {
      last_pos = i + 1;
    }
  }
  if (OB_SUCC(ret) &&
      plan_strs.empty() &&
      OB_FAIL(plan_strs.push_back(""))) {
    LOG_WARN("failed to push back plan text", K(ret));
  }
  return ret;
}

} // end of namespace sql
} // end of namespace oceanbase
