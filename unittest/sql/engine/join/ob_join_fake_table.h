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

#ifndef OB_JOIN_FAKE_TABLE_H_
#define OB_JOIN_FAKE_TABLE_H_
#include "sql/engine/join/ob_join.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_define.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
namespace test
{
enum TableType {
  TT_UNKNOWN,
  TT_LEFT_TABLE,
  TT_RIGHT_TABLE,
  TT_OUT_TABLE
};

enum JoinOpTestType
{
  MERGE_JOIN_TEST,
  NL_JOIN_TEST,
  BNL_JOIN_TEST,
  HASH_JOIN_TEST,
  JOIN_TEST_TYPE_NUM,
};

static const int DATA_COUNT = 16;
typedef struct {
  int64_t left_[DATA_COUNT][2];
  int64_t right_[DATA_COUNT][2];
  int64_t out_inner_[DATA_COUNT * DATA_COUNT][4];
  int64_t out_left_[DATA_COUNT * DATA_COUNT][4];
  int64_t out_right_[DATA_COUNT * DATA_COUNT][4];
  int64_t out_full_[DATA_COUNT * DATA_COUNT][4];
} JoinData;

class ObQueryRangeDummy{
public:
  ObQueryRangeDummy() : scan_key_value_(0) {}
  ~ObQueryRangeDummy() {}
  void set_scan_key_value(int64_t join_key_value) { scan_key_value_ = join_key_value; }
  int64_t get_scan_key_value() { return scan_key_value_; }
private:
  int64_t scan_key_value_;
};

class ObJoinFakeTableScanInput : public ObIPhyOperatorInput
{
public:
  virtual int init(ObExecContext &ctx, ObTaskInfo &task_info, ObPhyOperator &op)
  {
    UNUSED(ctx);
    UNUSED(task_info);
    UNUSED(op);
    return OB_SUCCESS;
  }

  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_TABLE_SCAN;
  }

  ObQueryRangeDummy &get_query_range()
  {
    return query_range_;
  }

private:
  ObQueryRangeDummy query_range_;
};
static ObArenaAllocator alloc_;
class ObJoinFakeTable: public ObPhyOperator
{
protected:
  class ObJoinFakeTableCtx: public ObPhyOperatorCtx
  {
    friend class ObJoinFakeTable;
  public:
    ObJoinFakeTableCtx(ObExecContext &exex_ctx) : ObPhyOperatorCtx(exex_ctx), iter_(0) {}
    virtual void destroy() { return ObPhyOperatorCtx::destroy_base(); }
  private:
    int64_t iter_;
    common::ObAddr server_;
  };
public:
  ObJoinFakeTable();
  virtual ~ObJoinFakeTable();
  int init(JoinOpTestType join_op_type);
  virtual ObPhyOperatorType get_type() const { return op_type_; }
  virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
  virtual ObPhyOperator *get_child(int32_t child_idx) const;
  virtual int32_t get_child_num() const { return 0; }
  virtual int inner_open(ObExecContext &exec_ctx) const;
  virtual int rescan(ObExecContext &exec_ctx) const;
  virtual int init_op_ctx(ObExecContext &exec_ctx) const;
  virtual int create_operator_input(ObExecContext &exec_ctx) const;
  virtual int inner_get_next_row(ObExecContext &exec_ctx, const common::ObNewRow *&row) const;
public:
  int prepare_data(int64_t case_id,
                   TableType table_type,
                   ObJoinType join_type);
  void set_type(ObPhyOperatorType op_type) { op_type_ = op_type; }
private:
  int cons_row(int64_t col1, int64_t col2, common::ObNewRow &row) const;
  int cons_row(int64_t col1, int64_t col2, int64_t col3, int64_t col4, common::ObNewRow &row) const;
private:
  int64_t (*left_data_)[2];
  int64_t (*right_data_)[2];
  int64_t (*out_data_)[4];
  JoinOpTestType join_op_type_;
  ObPhyOperatorType op_type_;
  bool is_inited_;
};

} // namespace test
} // namespace sql
} // namespace oceanbase

#endif /* OB_JOIN_FAKE_TABLE_H_ */
