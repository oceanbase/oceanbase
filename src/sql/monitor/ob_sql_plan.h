// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines interface of sql plan manager

#ifndef SRC_OBSERVER_SQL_PLAN_H_
#define SRC_OBSERVER_SQL_PLAN_H_
#include "sql/ob_sql_define.h"
#include "ob_plan_info_manager.h"

namespace oceanbase
{
namespace sql
{
class ObLogPlan;
class ObSQLSessionInfo;
class ObLogicalOperator;
struct ObSqlPlanItem;
struct ObQueryCtx;
struct ObExplainDisplayOpt;

#define SEPARATOR         "-------------------------------------"
#define OUTPUT_PREFIX     "      "
#define NEW_LINE          "\n"
#define COLUMN_SEPARATOR  "|"
#define PLAN_WRAPPER      "="
#define LINE_SEPARATOR    "-"
#define BASIC_PLAN_TABLE_COLUMN_CNT   3
#define PLAN_TABLE_COLUMN_CNT         5
#define REAL_PLAN_TABLE_COLUMN_CNT    9



#define BEGIN_BUF_PRINT                             \
    char *buf = plan_text.buf_;                     \
    int64_t &buf_len = plan_text.buf_len_;          \
    int64_t &pos = plan_text.pos_;                  \
    int64_t start_pos = pos;                        \
    ExplainType type = plan_text.type_;             \

#define END_BUF_PRINT(ptr, ptr_len)                 \
    if (OB_SUCC(ret)) {                             \
      ptr = plan_text.buf_ + start_pos;             \
      ptr_len = pos - start_pos;                   \
    }

#define BUF_PRINT_STR(str, ptr, ptr_len)                                                    \
do {                                                                                        \
  BEGIN_BUF_PRINT;                                                                          \
  if (OB_FAIL(ret)) { /* Do nothing */                                                      \
  } else if (OB_FAIL(BUF_PRINTF(str))) { /* Do nothing */                                   \
  } else {                                                                                  \
    END_BUF_PRINT(ptr, ptr_len)                                                             \
  }                                                                                         \
} while (0);

#define BUF_PRINT_OB_STR(str, str_len, ptr, ptr_len)                                        \
do {                                                                                        \
  BEGIN_BUF_PRINT;                                                                          \
  if (OB_FAIL(ret)) { /* Do nothing */                                                      \
  } else if (OB_FAIL(BUF_PRINTF("%.*s", str_len, str))) { /* Do nothing */                  \
  } else {                                                                                  \
    END_BUF_PRINT(ptr, ptr_len)                                                             \
  }                                                                                         \
} while (0);

#define BUF_PRINT_CONST_STR(str, plan_text)                                                 \
do {                                                                                        \
  char *buf = plan_text.buf_ + plan_text.pos_;                                              \
  int64_t buf_len = plan_text.buf_len_ - plan_text.pos_;                                    \
  int64_t pos = 0;                                                                          \
  if (OB_FAIL(ret)) { /* Do nothing */                                                      \
  } else if (OB_FAIL(BUF_PRINTF(str))) { /* Do nothing */                                   \
  } else {                                                                                  \
    plan_text.pos_ += pos;                                                                  \
  }                                                                                         \
} while (0);

struct PlanText
{
public:
  PlanText()
    : buf_(NULL),
      buf_len_(0),
      pos_(0),
      is_oneline_(false),
      is_used_hint_(false),
      is_outline_data_(false),
      type_(EXPLAIN_UNINITIALIZED)
  {}
  virtual ~PlanText() {}

  char *buf_;
  int64_t buf_len_;
  int64_t pos_;
  bool is_oneline_;
  bool is_used_hint_;
  bool is_outline_data_;
  ExplainType type_;
};

class ObSqlPlan
{
private:
  struct PlanFormatHelper {
    PlanFormatHelper()
      :operator_prefix_(),
      column_len_(),
      total_len_(0)
    {}
    int init();
    ObSEArray<ObString, 4> operator_prefix_;
    ObSEArray<int, 4> column_len_;
    int64_t total_len_;
  };
public:
  ObSqlPlan(common::ObIAllocator &allocator);
  virtual ~ObSqlPlan();
  int store_sql_plan(ObLogPlan* plan,
                     int64_t plan_id,
                     uint64_t plan_hash,
                     ObString &sql_id);

  int store_sql_plan_for_explain(ObLogPlan* plan,
                                 ExplainType type,
                                 const ObExplainDisplayOpt& option,
                                 ObIArray<common::ObString> &plan_strs);

  int print_sql_plan(ObLogPlan* plan,
                     ExplainType type,
                     const ObExplainDisplayOpt& option,
                     ObIArray<common::ObString> &plan_strs);

  int get_sql_plan(const ObString &sql_id,
                   int64_t plan_id,
                   ExplainType type,
                   const ObExplainDisplayOpt& option,
                   ObIArray<ObPlanRealInfo> &plan_infos,
                   PlanText &plan_text);

  int get_sql_plan_by_hash(const ObString &sql_id,
                          uint64_t plan_hash,
                          ExplainType type,
                          const ObExplainDisplayOpt& option,
                          ObIArray<ObPlanRealInfo> &plan_infos,
                          PlanText &plan_text);

  int get_last_explain_plan(ExplainType type,
                            const ObExplainDisplayOpt& option,
                            PlanText &plan_text);

  void set_session_info(ObSQLSessionInfo *session_info);

  static int get_plan_outline_info_one_line(PlanText &plan_text,
                                            ObLogPlan* plan);

  static int plan_text_to_string(PlanText &plan_text,
                                 common::ObString &plan_str);

  static int plan_text_to_strings(PlanText &plan_text,
                                  ObIArray<common::ObString> &plan_strs);

private:
  int set_plan_id_for_explain(ObIArray<ObSqlPlanItem*> &sql_plan_infos);

  int set_plan_id_for_excute(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                             int64_t plan_id,
                             uint64_t plan_hash,
                             ObString &sql_id,
                             PlanText &plan_text);

  int get_sql_plan_infos(PlanText &plan_text,
                         ObLogPlan* plan,
                         ObIArray<ObSqlPlanItem*> &sql_plan_infos);

  int get_plan_tree_infos(PlanText &plan_text,
                         ObLogicalOperator* op,
                         ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                         int depth,
                         int position,
                         bool is_last_child);

  int get_plan_used_hint_info(PlanText &plan_text,
                              ObLogPlan* plan,
                              ObSqlPlanItem* sql_plan_item);

  int get_plan_tree_used_hint(PlanText &plan_text,
                              ObLogicalOperator* op);

  int get_qb_name_trace(PlanText &plan_text,
                        ObLogPlan* plan,
                        ObSqlPlanItem* sql_plan_item);

  int get_plan_outline_info(PlanText &plan_text,
                            ObLogPlan* plan,
                            ObSqlPlanItem* sql_plan_item);

  static int reset_plan_tree_outline_flag(ObLogicalOperator* op);

  static int get_plan_tree_outline(PlanText &plan_text,
                            ObLogicalOperator* op);

  int get_plan_other_info(PlanText &plan_text,
                          ObLogPlan* plan,
                          ObSqlPlanItem* sql_plan_item);

  int get_constraint_info(char *buf,
                          int64_t buf_len,
                          int64_t &pos,
                          const ObQueryCtx &ctx);

  int print_constraint_info(char *buf,
                            int64_t buf_len,
                            int64_t &pos,
                            const ObPCConstParamInfo &info);

  int print_constraint_info(char *buf,
                            int64_t buf_len,
                            int64_t &pos,
                            const ObPCParamEqualInfo &info);

  int print_constraint_info(char *buf,
                            int64_t buf_len,
                            int64_t &pos,
                            const ObExprConstraint &info);

  int inner_store_sql_plan(ObIArray<ObSqlPlanItem*> &sql_plan_infos, bool for_explain);

  int format_sql_plan(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                      ExplainType type,
                      const ObExplainDisplayOpt& option,
                      ObIArray<ObPlanRealInfo> &plan_infos,
                      PlanText &plan_text);

  int get_plan_table_formatter(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                               const ObExplainDisplayOpt& option,
                               PlanFormatHelper &format_helper);

  int get_real_plan_table_formatter(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                    const ObExplainDisplayOpt& option,
                                    ObIArray<ObPlanRealInfo> &plan_infos,
                                    PlanFormatHelper &format_helper);

  int get_operator_prefix(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                          const ObExplainDisplayOpt& option,
                          PlanFormatHelper &format_helper);

  int format_basic_plan_table(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                              const ObExplainDisplayOpt& option,
                              PlanText &plan_text);

  int format_plan_table(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                        const ObExplainDisplayOpt& option,
                        PlanText &plan_text);

  int format_real_plan_table(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                            const ObExplainDisplayOpt& option,
                            ObIArray<ObPlanRealInfo> &plan_infos,
                            PlanText &plan_text);

  int format_plan_output(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text);

  int format_used_hint(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text);

  int format_qb_name_trace(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text);

  int format_outline(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text);

  int format_optimizer_info(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text);

  int format_other_info(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text);

  int format_plan_to_json(ObIArray<ObSqlPlanItem*> &sql_plan_infos, PlanText &plan_text);

  int inner_format_plan_to_json(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                int64_t info_idx,
                                json::Value *&ret_val);

  int init_buffer(PlanText &plan_text);

  void destroy_buffer(PlanText &plan_text);

  int refine_buffer(PlanText &plan_text);

  DISALLOW_COPY_AND_ASSIGN(ObSqlPlan);

private:
  common::ObIAllocator &allocator_;
  ObSQLSessionInfo *session_info_;
};

} // end of namespace sql
} // end of namespace oceanbase



#endif /* SRC_OBSERVER_SQL_PLAN_H_ */
