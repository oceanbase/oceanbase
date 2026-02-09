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
#include "sql/das/ob_das_scan_op.h"
namespace oceanbase
{
namespace common {
class ObITabletScan;
}

namespace storage {
class ObTableScanParam;
}

namespace sql
{

class ObDASScanCtDef;

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
    return nullptr != scan_ctdef_ && ObDASIterParam::is_valid();
  }
};

class ObDASScanIter : public ObDASIter
{
public:
  ObDASScanIter()
    : ObDASIter(ObDASIterType::DAS_ITER_SCAN),
      tsc_service_(nullptr),
      result_(nullptr),
      scan_param_(nullptr),
      try_check_tick_(0),
      try_check_tick_rows_(0),
      vec_start_scan_ts_us_(-1),
      vec_pre_filtering_timeout_us_(-1)
  {}
  virtual ~ObDASScanIter() {}
  common::ObNewRowIterator *&get_output_result_iter() { return result_; }

  void set_scan_param(storage::ObTableScanParam &scan_param) { scan_param_ = &scan_param; }
  storage::ObTableScanParam &get_scan_param() { return *scan_param_; }

  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

  virtual int get_diagnosis_info(ObDiagnosisManager* diagnosis_manager) override {
    return result_->get_diagnosis_info(diagnosis_manager); 
  };
  OB_INLINE void set_pre_filtering_timeout(int64_t vec_pre_filtering_timeout) { vec_pre_filtering_timeout_us_ = vec_pre_filtering_timeout; }
  OB_INLINE bool is_vec_pre_filtering_timeout_set() { return vec_pre_filtering_timeout_us_ > 0; }
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  int try_check_vec_pre_filter_status(const int64_t row_count = 1);
  int check_vec_pre_filter_status();

private:
  static const uint64_t CHECK_STATUS_ROWS = 500;
  static const uint64_t CHECK_STATUS_TRY_TIMES = 50;

  common::ObITabletScan *tsc_service_;
  common::ObNewRowIterator *result_;
  // must ensure the lifecycle of scan param is longer than scan iter.
  storage::ObTableScanParam *scan_param_;
  uint64_t try_check_tick_; // for check vector prefilter search status by times, default 0
  uint64_t try_check_tick_rows_; // for check vector prefilter search status by rows, default 0
  int64_t vec_start_scan_ts_us_; // the start timestamp of this scan for vector prefilter search, default -1
  int64_t vec_pre_filtering_timeout_us_; // the pre-filtering timeout of vector prefilter search , default -1
};


}  // namespace sql
}  // namespace oceanbase



#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SCAN_ITER_H_ */
