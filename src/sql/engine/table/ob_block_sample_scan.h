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

#ifndef _OB_TABLE_BLOCK_SAMPLE_SCAN_H
#define _OB_TABLE_BLOCK_SAMPLE_SCAN_H 1

#include "sql/engine/table/ob_table_scan.h"

namespace oceanbase {
namespace sql {

class ObBlockSampleScanInput : public ObTableScanInput {
public:
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_BLOCK_SAMPLE_SCAN;
  }
};

class ObBlockSampleScan : public ObTableScan {
  OB_UNIS_VERSION_V(1);

public:
  class ObBlockSampleScanCtx : public ObTableScanCtx {
  public:
    ObBlockSampleScanCtx(ObExecContext& ctx) : ObTableScanCtx(ctx)
    {}
  };

  explicit ObBlockSampleScan(common::ObIAllocator& allocator);
  virtual ~ObBlockSampleScan()
  {}

  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
  inline void set_sample_info(const common::SampleInfo& sample_info)
  {
    sample_info_ = sample_info;
  }
  inline common::SampleInfo& get_sample_info()
  {
    return sample_info_;
  }
  inline const common::SampleInfo& get_sample_info() const
  {
    return sample_info_;
  }
  virtual int prepare_scan_param(ObExecContext& ctx) const;
  OB_INLINE virtual bool need_filter_row() const override
  {
    return true;
  }

private:
  common::SampleInfo sample_info_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_ROW_SAMPLE_SCAN_H */
