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

#ifndef OB_MULTI_PARTITION_TABLE_SCAN_H_
#define OB_MULTI_PARTITION_TABLE_SCAN_H_

#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {

class ObMultiPartTableScanInput : public ObTableScanInput {
  OB_UNIS_VERSION_V(1);

public:
  ObMultiPartTableScanInput() : allocator_(common::ObModIds::OB_SQL_TABLE_LOOKUP), partitions_ranges_()
  {}
  virtual ~ObMultiPartTableScanInput();
  virtual void reset() override;
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_MULTI_PART_TABLE_SCAN;
  };
  int reassign_ranges(ObExecContext& ctx, int64_t table_location_key, int64_t ref_table_id, int64_t& partition_offset);

  common::ObArenaAllocator allocator_;
  ObMultiPartitionsRangesWarpper partitions_ranges_;
};

class ObMultiPartTableScan : public ObTableScan {
private:
  enum MultiPartScanState { DO_PARTITION_SCAN, OUTPUT_ROWS, EXECUTION_FINISHED };
  class ObMultiPartTableScanCtx : public ObTableScanCtx {
    friend class ObMultiPartTableScan;

  public:
    explicit ObMultiPartTableScanCtx(ObExecContext& ctx)
        : ObTableScanCtx(ctx), input_part_offset_(NOT_INIT), multi_part_scan_state_(DO_PARTITION_SCAN), scan_times_(0)
    {}
    virtual void destroy()
    {
      ObTableScanCtx::destroy();
    }
    inline void add_scan_times()
    {
      ++scan_times_;
    }
    int64_t scan_time()
    {
      return scan_times_;
    }
    int64_t input_part_offset_;
    MultiPartScanState multi_part_scan_state_;
    int64_t scan_times_;
  };

public:
  explicit ObMultiPartTableScan(common::ObIAllocator& allocator);
  virtual ~ObMultiPartTableScan();
  virtual int rescan(ObExecContext& ctx) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int create_operator_input(ObExecContext& ctx) const override final;
  virtual int init_op_ctx(ObExecContext& ctx) const override final;
  void get_used_range_count(ObExecContext& ctx, int64_t& range_count) const;

private:
  int do_next_partition_scan(ObExecContext& ctx) const;

private:
  static const int64_t NOT_INIT = 0;
};
}  // namespace sql
}  // namespace oceanbase

#endif
