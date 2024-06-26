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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SCAN_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

// DASScanIter is a wrapper class for storage iter, it doesn't require eval_ctx or exprs like other iters.
struct ObDASScanIterParam : public ObDASIterParam
{
public:
  ObDASScanIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_SCAN),
      scan_ctdef_(nullptr)
  {}
  const ObDASScanCtDef *scan_ctdef_;
  virtual bool is_valid() const override
  {
    return nullptr != scan_ctdef_;
  }
};

class ObDASScanIter : public ObDASIter
{
public:
  ObDASScanIter()
    : ObDASIter(ObDASIterType::DAS_ITER_SCAN),
      tsc_service_(nullptr),
      result_(nullptr),
      scan_param_(nullptr)
  {}
  virtual ~ObDASScanIter() {}
  common::ObNewRowIterator *&get_output_result_iter() { return result_; }

  void set_scan_param(storage::ObTableScanParam &scan_param) { scan_param_ = &scan_param; }
  storage::ObTableScanParam &get_scan_param() { return *scan_param_; }

  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  common::ObITabletScan *tsc_service_;
  common::ObNewRowIterator *result_;
  // must ensure the lifecycle of scan param is longer than scan iter.
  storage::ObTableScanParam *scan_param_;
};


}  // namespace sql
}  // namespace oceanbase



#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SCAN_ITER_H_ */
