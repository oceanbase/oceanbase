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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_EXECUTE_RESULT_
#define OCEANBASE_SQL_EXECUTOR_OB_EXECUTE_RESULT_

#include "common/row/ob_row.h"
#include "share/ob_scanner.h"
#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObPhyOperator;
class ObOperator;
class ObOpSpec;

class ObIExecuteResult
{
public:
  virtual ~ObIExecuteResult() {}

  virtual int open(ObExecContext &ctx) = 0;
  virtual int get_next_row(ObExecContext &ctx, const common::ObNewRow *&row) = 0;
  virtual int close(ObExecContext &ctx) = 0;
};

class ObExecuteResult : public ObIExecuteResult
{
  friend class ObLocalTaskExecutor;
  friend class ObExecutor;
public:
  ObExecuteResult()
    : err_code_(OB_ERR_UNEXPECTED),
      static_engine_root_(NULL) {}
  virtual ~ObExecuteResult() {}

  virtual int open(ObExecContext &ctx) override;
  virtual int get_next_row(ObExecContext &ctx, const common::ObNewRow *&row) override;
  virtual int close(ObExecContext &ctx) override;

  inline int get_err_code() { return err_code_; }

  // interface for static typing engine
  int open() const;
  int get_next_row() const;
  int close() const;
  const ObOperator *get_static_engine_root() const { return static_engine_root_; }
  void set_static_engine_root(ObOperator *op)
  {
    static_engine_root_ = op;
    br_it_.set_operator(op);
  }

private:
  int err_code_;
  ObOperator *static_engine_root_;
  // row used to adapt old get_next_row interface.
  mutable common::ObNewRow row_;
  mutable ObBatchRowIter br_it_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExecuteResult);
};

class ObAsyncExecuteResult : public ObIExecuteResult
{
public:
  ObAsyncExecuteResult();
  virtual ~ObAsyncExecuteResult() { }

  void set_result_stream(common::ObScanner *scanner, int64_t field_count)
  {
    scanner_ = scanner;
    field_count_ = field_count;
  }
  virtual int open(ObExecContext &ctx) override;
  virtual int get_next_row(ObExecContext &ctx, const common::ObNewRow *&row) override;
  virtual int close(ObExecContext &ctx) override;
  // interface for static typing engine
  const ObOpSpec* get_static_engine_spec() const { return spec_; }
  void set_static_engine_spec(const ObOpSpec *spec) { spec_ = spec; }

private:
  int64_t field_count_;
  common::ObScanner *scanner_;
  common::ObNewRow *cur_row_;
  common::ObScanner::Iterator row_iter_;
  // used for static engine
  const ObOpSpec *spec_;
  ObChunkDatumStore::Iterator datum_iter_;
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_EXECUTE_RESULT_ */

