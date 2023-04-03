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

#ifndef _OB_TABLE_ROW_SAMPLE_SCAN_OP_H
#define _OB_TABLE_ROW_SAMPLE_SCAN_OP_H 1

#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase {
namespace sql {

class ObRowSampleScanOpInput : public ObTableScanOpInput
{
public:
  ObRowSampleScanOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObTableScanOpInput(ctx, spec)
  {}
};

class ObRowSampleScanSpec : public ObTableScanSpec
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObRowSampleScanSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableScanSpec(alloc, type)
  {}
  virtual ~ObRowSampleScanSpec() {}


  inline void set_sample_info(const SampleInfo &sample_info) { sample_info_ = sample_info; }
  inline SampleInfo &get_sample_info() { return sample_info_; }
  inline const SampleInfo &get_sample_info() const { return sample_info_; }

private:
  common::SampleInfo sample_info_;
};

class ObRowSampleScanOp : public ObTableScanOp
{
public:
  ObRowSampleScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObTableScanOp(exec_ctx, spec, input), need_sample_(false) {}
  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void set_need_sample(bool flag) override
  {
    need_sample_ = flag;
    tsc_rtdef_.scan_rtdef_.sample_info_ = need_sample_ ? &(MY_SPEC.get_sample_info()) : nullptr;
  }
private:
  bool need_sample_;
};

}
}



#endif /* _OB_ROW_SAMPLE_SCAN_OP_H */
