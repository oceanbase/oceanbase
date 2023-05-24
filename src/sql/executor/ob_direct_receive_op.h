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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_DIRECT_RECEIVE_OP_
#define OCEANBASE_SQL_EXECUTOR_OB_DIRECT_RECEIVE_OP_

#include "sql/engine/px/exchange/ob_receive_op.h"
#include "share/ob_scanner.h"
namespace oceanbase
{
namespace sql
{
class ObExecContext;

class ObDirectReceiveSpec : public ObReceiveSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObDirectReceiveSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObReceiveSpec(alloc, type), dynamic_const_exprs_(alloc)
  {}
  ExprFixedArray dynamic_const_exprs_; // const expr which contain dynamic param
  virtual ~ObDirectReceiveSpec() {};
};

class ObDirectReceiveOp : public ObReceiveOp
{
public:
  ObDirectReceiveOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual ~ObDirectReceiveOp() {}

  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  int inner_close();
  virtual int inner_rescan() override;
  virtual void destroy() override { ObReceiveOp::destroy(); }

private:
  int setup_next_scanner();
  int get_next_row_from_cur_scanner();
  int update_user_var();
  // clear dynamic const expr parent evaluate flag, because when dynmaic param datum
  // changed, if we don't clear dynamic const expr parent expr evaluate flag, the
  // parent expr datum ptr may point to the last dynamic param datum memory which
  // is invalid now
  OB_INLINE void clear_dynamic_const_parent_flag()
  {
    const ObDirectReceiveSpec &spec = static_cast<const ObDirectReceiveSpec &>(spec_);
    for (int64_t i = 0; i < spec.dynamic_const_exprs_.count(); i++) {
      ObDynamicParamSetter::clear_parent_evaluated_flag(
                          eval_ctx_, *spec.dynamic_const_exprs_.at(i));
    }
  }
private:
  common::ObScanner *scanner_;
  ObChunkDatumStore::Iterator scanner_iter_;
  bool all_data_empty_;
  bool cur_data_empty_;
  bool first_request_received_;
  int64_t found_rows_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectReceiveOp);
};

}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_DIRECT_RECEIVE_OP_ */
//// end of header file
