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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TRIGGER_HANDLER_OP_
#define OCEANBASE_SQL_ENGINE_DML_OB_TRIGGER_HANDLER_OP_

#include "sql/engine/ob_operator.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"

namespace oceanbase
{
namespace sql
{
class ObTableModifyOp;

class TriggerHandle
{
public:
  static int set_rowid_into_row(const ObTriggerColumnsInfo &cols,
                                const ObObj &rowid_val,
                                ObObj* cells);
  static int set_rowid_into_row(const ObTriggerColumnsInfo &cols,
                                ObEvalCtx &eval_ctx,
                                ObExpr *src_expr,
                                ObObj* cells);
  static int init_param_rows(ObEvalCtx &eval_ctx,
                            const ObTrigDMLCtDef &trig_ctdef,
                            ObTrigDMLRtDef &trig_rtdef);
  static int init_param_old_row(ObEvalCtx &eval_ctx,
                                const ObTrigDMLCtDef &trig_ctdef,
                                ObTrigDMLRtDef &trig_rtdef);
  static int init_param_new_row(ObEvalCtx &eval_ctx,
                                const ObTrigDMLCtDef &trig_ctdef,
                                ObTrigDMLRtDef &trig_rtdef);
  static int do_handle_before_row(ObTableModifyOp &dml_op,
                                  ObDASDMLBaseCtDef &das_base_ctdef,
                                  const ObTrigDMLCtDef &trig_ctdef,
                                  ObTrigDMLRtDef &trig_rtdef);
  static int do_handle_after_row(ObTableModifyOp &dml_op,
                                  const ObTrigDMLCtDef &trig_ctdef,
                                  ObTrigDMLRtDef &trig_rtdef,
                                  uint64_t tg_event);
  static int do_handle_before_stmt(ObTableModifyOp &dml_op,
                                    const ObTrigDMLCtDef &trig_ctdef,
                                    ObTrigDMLRtDef &trig_rtdef,
                                    uint64_t tg_event);
  static int do_handle_after_stmt(ObTableModifyOp &dml_op,
                                  const ObTrigDMLCtDef &trig_ctdef,
                                  ObTrigDMLRtDef &trig_rtdef,
                                  uint64_t tg_event);
  static int init_trigger_params(ObDMLRtCtx &das_ctx,
                                uint64_t trigger_event,
                                const ObTrigDMLCtDef &trig_ctdef,
                                ObTrigDMLRtDef &trig_rtdef);
  static int64_t get_routine_param_count(const uint64_t routine_id);
  inline static bool is_trigger_body_routine(const uint64_t package_id,
                                             const uint64_t routine_id,
                                             pl::ObProcType type)
  {
    bool is_trg_routine = false;
    if (schema::ObTriggerInfo::is_trigger_body_package_id(package_id) && pl::ObProcType::PACKAGE_PROCEDURE == type) {
      if (lib::is_oracle_mode()) {
        is_trg_routine = (routine_id >= ROUTINE_IDX_CALC_WHEN && routine_id <= ROUTINE_IDX_AFTER_STMT);
      } else {
        is_trg_routine = (routine_id >= ROUTINE_IDX_BEFORE_ROW_MYSQL && routine_id <= ROUTINE_IDX_AFTER_ROW_MYSQL);
      }
    }
    return is_trg_routine;
  }
private:
  // trigger
  static int init_trigger_row(ObIAllocator &alloc, int64_t rowtype_col_count, pl::ObPLRecord *&record);
  static int calc_when_condition(ObTableModifyOp &dml_op,
                                  ObTrigDMLRtDef &trig_rtdef,
                                  uint64_t trigger_id,
                                  bool &need_fire);
  static int calc_before_row(ObTableModifyOp &dml_op, ObTrigDMLRtDef &trig_rtdef, uint64_t trigger_id);
  static int calc_after_row(ObTableModifyOp &dml_op, ObTrigDMLRtDef &trig_rtdef, uint64_t trigger_id);
  static int calc_before_stmt(ObTableModifyOp &dml_op,
                              ObTrigDMLRtDef &trig_rtdef,
                              uint64_t trigger_id);
  static int calc_after_stmt(ObTableModifyOp &dml_op, ObTrigDMLRtDef &trig_rtdef, uint64_t trigger_id);
  static int calc_trigger_routine(ObExecContext &exec_ctx,
                                  uint64_t trigger_id,
                                  uint64_t routine_id,
                                  ParamStore &params);
  static int calc_trigger_routine(ObExecContext &exec_ctx,
                                  uint64_t trigger_id,
                                  uint64_t routine_id,
                                  ParamStore &params,
                                  ObObj &result);
  static int check_and_update_new_row(ObTableModifyOp *self_op,
                                      const ObTriggerColumnsInfo &columns,
                                      ObEvalCtx &eval_ctx,
                                      const ObIArray<ObExpr *> &new_row_exprs,
                                      pl::ObPLRecord *new_record,
                                      bool check);
  static int do_handle_rowid_before_row(ObTableModifyOp &dml_op,
                                        const ObTrigDMLCtDef &trig_ctdef,
                                        ObTrigDMLRtDef &trig_rtdef,
                                        uint64_t tg_event);
  static int do_handle_rowid_after_row(ObTableModifyOp &dml_op,
                                        const ObTrigDMLCtDef &trig_ctdef,
                                        ObTrigDMLRtDef &trig_rtdef,
                                        uint64_t tg_event);
  static inline int destroy_compound_trigger_state(ObExecContext &exec_ctx, const ObTrigDMLCtDef &trig_ctdef);

private:
  static const uint64_t ROUTINE_IDX_CALC_WHEN = 1;
  static const uint64_t ROUTINE_IDX_BEFORE_STMT = 2;
  static const uint64_t ROUTINE_IDX_BEFORE_ROW = 3;
  static const uint64_t ROUTINE_IDX_AFTER_ROW = 4;
  static const uint64_t ROUTINE_IDX_AFTER_STMT = 5;
  static const uint64_t ROUTINE_IDX_BEFORE_ROW_MYSQL = 1;
  static const uint64_t ROUTINE_IDX_AFTER_ROW_MYSQL = 2;

  static const int64_t WHEN_POINT_PARAM_OFFSET = 0;
  static const int64_t WHEN_POINT_PARAM_COUNT = 2;
  static const int64_t STMT_POINT_PARAM_OFFSET = 2;
  static const int64_t STMT_POINT_PARAM_COUNT = 0;
  static const int64_t ROW_POINT_PARAM_OFFSET = 0;
  static const int64_t ROW_POINT_PARAM_COUNT = 2;
  static const int64_t ROW_POINT_PARAM_COUNT_MYSQL = 2;
};


}  // namespace sql
}  // namespace oceanbase
#endif
