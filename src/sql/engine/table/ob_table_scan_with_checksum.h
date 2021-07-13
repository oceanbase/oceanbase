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

#ifndef OCEANBASE_SQL_ENGINE_TABLE_SCAN_WITH_CHECKSUM_H_
#define OCEANBASE_SQL_ENGINE_TABLE_SCAN_WITH_CHECKSUM_H_

#include "ob_table_scan.h"

namespace oceanbase {
namespace sql {

class ObTableScanWithChecksumInput : public ObTableScanInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableScanWithChecksumInput();
  virtual ~ObTableScanWithChecksumInput();
  virtual void reset() override;
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op) override;
  const ObTaskID& get_ob_task_id() const
  {
    return task_id_;
  }
  virtual ObPhyOperatorType get_phy_op_type() const override
  {
    return PHY_TABLE_SCAN_WITH_CHECKSUM;
  }

private:
  ObTaskID task_id_;
};

// when builds index, this operator is used to calculate checksum of data table
class ObTableScanWithChecksum : public ObTableScan {
public:
  explicit ObTableScanWithChecksum(common::ObIAllocator& allocator);
  virtual ~ObTableScanWithChecksum();

protected:
  virtual int inner_open(ObExecContext& ctx) const override;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const override;
  virtual int init_op_ctx(ObExecContext& ctx) const override;

protected:
  class ObTableScanWithChecksumCtx : public ObTableScan::ObTableScanCtx {
  public:
    explicit ObTableScanWithChecksumCtx(ObExecContext& ctx);
    virtual ~ObTableScanWithChecksumCtx();
    virtual void destroy();
    int get_output_col_ids(const share::schema::ObTableParam& table_param);
    int allocate_checksum_memory();
    int add_row_checksum(const common::ObNewRow* row);
    int report_checksum(const int64_t execution_id);

  public:
    int64_t* checksum_;
    ObArray<int32_t> col_ids_;
    uint64_t task_id_;
  };
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_TABLE_SCAN_WITH_CHECKSUM_H_
