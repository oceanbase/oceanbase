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

#ifndef OCEANBASE_ENGINE_OB_BY_PASS_OPERATOR_H_
#define OCEANBASE_ENGINE_OB_BY_PASS_OPERATOR_H_

#include "src/sql/engine/ob_operator.h"
namespace oceanbase
{
namespace sql
{

class ObByPassOperator : public ObOperator
{
public:
  ObByPassOperator(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input) :
    ObOperator(exec_ctx, spec, input), by_pass_(false) {}
  virtual ~ObByPassOperator() {}

  void set_by_pass(bool by_pass) { by_pass_ = by_pass; }
  bool get_by_pass() { return by_pass_; }

  virtual int after_by_pass_next_row();
  virtual int after_by_pass_next_batch(const ObBatchRows *&batch_rows);

  virtual int get_next_row() override;
  virtual int get_next_batch(const int64_t max_row_cnt, const ObBatchRows *&batch_rows) override;

  // Every time a rescan is performed,
  // the adaptive join will re-evaluate the join method.
  virtual int inner_rescan() override;

  virtual int process_after_set_passed() { return common::OB_SUCCESS; }

private:
  bool by_pass_;

};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_OB_BY_PASS_OPERATOR_H_