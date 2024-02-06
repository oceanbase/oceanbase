/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

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
class ObPhysicalPlan;
class ObExecContext;
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
  int store_sql_plan(ObLogPlan* log_plan, ObPhysicalPlan* phy_plan);

  int store_sql_plan_for_explain(ObExecContext *ctx,
                                 ObLogPlan* plan,
                                 ExplainType type,
                                 const ObString& plan_table,
                                 const ObString& statement_id,
                                 const ObExplainDisplayOpt& option,
                                 ObIArray<common::ObString> &plan_strs);

  int print_sql_plan(ObLogPlan* plan,
                     ExplainType type,
                     const ObExplainDisplayOpt& option,
                     ObIArray<common::ObString> &plan_strs);


  int format_sql_plan(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                      ExplainType type,
                      const ObExplainDisplayOpt& option,
                      PlanText &plan_text);

  static int get_plan_outline_info_one_line(PlanText &plan_text,
                                            ObLogPlan* plan);

  static int plan_text_to_string(PlanText &plan_text,
                                 common::ObString &plan_str);

  static int plan_text_to_strings(PlanText &plan_text,
                                  ObIArray<common::ObString> &plan_strs);

private:
  int inner_store_sql_plan_for_explain(ObExecContext *ctx,
                                      const ObString& plan_table,
                                      const ObString& statement_id,
                                      ObIArray<ObSqlPlanItem*> &sql_plan_infos);

  int escape_quotes(ObSqlPlanItem &plan_item);

  int inner_escape_quotes(char* &ptr, int64_t &length);

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
  static int get_global_hint_outline(PlanText &plan_text, ObLogPlan &plan);
  static int construct_outline_global_hint(ObLogPlan &plan, ObGlobalHint &outline_global_hint);

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

  int get_plan_table_formatter(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                               const ObExplainDisplayOpt& option,
                               PlanFormatHelper &format_helper);

  int get_real_plan_table_formatter(ObIArray<ObSqlPlanItem*> &sql_plan_infos,
                                    const ObExplainDisplayOpt& option,
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

  bool is_exchange_out_operator(ObSqlPlanItem *item);

  int init_buffer(PlanText &plan_text);

  void destroy_buffer(PlanText &plan_text);

  int refine_buffer(PlanText &plan_text);

  int prepare_and_store_session(ObSQLSessionInfo *session,
                                ObSQLSessionInfo::StmtSavedValue *&session_value,
                                transaction::ObTxDesc *&tx_desc,
                                int64_t &nested_count);

  int restore_session(ObSQLSessionInfo *session,
                      ObSQLSessionInfo::StmtSavedValue *&session_value,
                      transaction::ObTxDesc *tx_desc,
                      int64_t nested_count);

public:
  static int format_one_output_expr(char *buf,
                                    int64_t buf_len,
                                    int64_t &pos,
                                    int &line_begin_pos,
                                    const char* expr_info,
                                    int expr_len);

  DISALLOW_COPY_AND_ASSIGN(ObSqlPlan);

private:
  common::ObIAllocator &allocator_;
};

} // end of namespace sql
} // end of namespace oceanbase



#endif /* SRC_OBSERVER_SQL_PLAN_H_ */
