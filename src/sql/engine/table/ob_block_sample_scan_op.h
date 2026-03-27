/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_TABLE_BLOCK_SAMPLE_SCAN_OP_H
#define _OB_TABLE_BLOCK_SAMPLE_SCAN_OP_H 1

#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase {
namespace sql {

class ObBlockSampleScanOpInput : public ObTableScanOpInput
{
public:
  ObBlockSampleScanOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObTableScanOpInput(ctx, spec)
  {}
};

class ObBlockSampleScanSpec: public ObTableScanSpec
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObBlockSampleScanSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableScanSpec(alloc, type)
  {}
  virtual ~ObBlockSampleScanSpec() {}

  inline void set_sample_info(const common::SampleInfo &sample_info) {
    sample_info_ = sample_info;
  }
  inline common::SampleInfo &get_sample_info() {
    return sample_info_;
  }
  inline const common::SampleInfo &get_sample_info() const {
    return sample_info_;
  }
private:
  common::SampleInfo sample_info_;
};

class ObBlockSampleScanOp : public ObTableScanOp
{
public:
  ObBlockSampleScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
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
