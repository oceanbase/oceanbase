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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_ERROR_LOGGING_OP_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_ERROR_LOGGING_OP_
#include "sql/engine/ob_operator.h"
#include "lib/string/ob_string.h"
#include "sql/engine/dml/ob_err_log_service.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
class ObErrLogSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObErrLogSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      err_log_ct_def_(alloc),
      type_(ObDASOpType::DAS_OP_TABLE_INSERT)
  {}

public:
  ObErrLogCtDef err_log_ct_def_;
  ObDASOpType type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObErrLogSpec);
};

class ObErrLogOp : public ObOperator
{
public:
  ObErrLogOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
      : ObOperator(ctx, spec, input),
        err_log_service_(get_eval_ctx()),
        err_log_rt_def_()
  {
  }
  virtual ~ObErrLogOp() {};
  virtual void destroy() override
  {
    ObOperator::destroy();
  }
protected:
  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;

private:
  int record_err_log();
private:
  ObErrLogService err_log_service_;
  ObErrLogRtDef err_log_rt_def_;
  DISALLOW_COPY_AND_ASSIGN(ObErrLogOp);
};
} // end namespace sql
} // end namespace oceanbase
#endif
