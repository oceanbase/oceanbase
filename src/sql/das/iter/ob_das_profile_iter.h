/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_PROFILE_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_PROFILE_ITER_H_

#include "share/ob_i_tablet_scan.h"
#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/search/ob_das_search_context.h"
#include "share/diagnosis/ob_runtime_profile.h"

namespace oceanbase
{
namespace sql
{

struct ObDASProfileIterParam : public ObDASIterParam
{
public:
  ObDASProfileIterParam(ObDASIter *child, bool enable_profile, storage::ObTableScanParam *scan_param)
    : ObDASIterParam(ObDASIterType::DAS_ITER_PROFILE),
      enable_profile_(enable_profile),
      scan_param_(scan_param)
  {
    if (OB_NOT_NULL(child)) {
      max_size_ = child->get_max_size();
      eval_ctx_ = child->get_eval_ctx();
      exec_ctx_ = child->get_exec_ctx();
      output_ = child->get_output();
    }
  }
  virtual ~ObDASProfileIterParam() {}

  virtual bool is_valid() const override
  {
    return ObDASIterParam::is_valid() && OB_NOT_NULL(scan_param_);
  }
  bool is_enable_profile() const { return enable_profile_; }
  storage::ObTableScanParam *get_scan_param() { return scan_param_; }

private:
  bool enable_profile_;
  storage::ObTableScanParam *scan_param_;
};

class ObDASProfileIter : public ObDASIter
{
public:
  ObDASProfileIter()
    : ObDASIter(ObDASIterType::DAS_ITER_PROFILE),
      enable_profile_(false),
      scan_param_(nullptr),
      table_scan_profile_(nullptr),
      partition_profile_(nullptr)
  {}
  virtual ~ObDASProfileIter() {}

public:
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

  static int init_runtime_profile(
      common::ObProfileId profile_id,
      common::ObOpProfile<common::ObMetric> *&profile_out);

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int create_partition_profile();
  bool enable_profile_;
  storage::ObTableScanParam *scan_param_;
  common::ObOpProfile<common::ObMetric> *table_scan_profile_;
  common::ObOpProfile<common::ObMetric> *partition_profile_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_PROFILE_ITER_H_ */
