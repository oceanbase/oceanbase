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

#ifndef OCEANBASE_SQL_ENGINE_TABLE_OB_MULTI_PARTITION_TABLE_SCAN_OP_
#define OCEANBASE_SQL_ENGINE_TABLE_OB_MULTI_PARTITION_TABLE_SCAN_OP_

#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_table_partition_ranges.h"

namespace oceanbase {
namespace sql {

class ObMultiPartTableScanOpInput : public ObTableScanOpInput {
  OB_UNIS_VERSION_V(1);
  friend ObMultiPartTableScanOp;

public:
  ObMultiPartTableScanOpInput(ObExecContext& ctx, const ObOpSpec& spec);
  virtual ~ObMultiPartTableScanOpInput()
  {
    partitions_ranges_.release();
  }

  virtual void reset() override;
  virtual int init(ObTaskInfo& task_info) override;

  int reassign_ranges(int64_t table_location_key, int64_t ref_table_id, int64_t& partition_offset);

public:
  common::ObArenaAllocator allocator_;
  ObMultiPartitionsRangesWarpper partitions_ranges_;

  DISALLOW_COPY_AND_ASSIGN(ObMultiPartTableScanOpInput);
};

class ObMultiPartTableScanSpec : public ObTableScanSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObMultiPartTableScanSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type) : ObTableScanSpec(alloc, type)
  {}

  virtual ~ObMultiPartTableScanSpec(){};

private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiPartTableScanSpec);
};

class ObMultiPartTableScanOp : public ObTableScanOp {
public:
  ObMultiPartTableScanOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  ~ObMultiPartTableScanOp()
  {
    destroy();
  };

  int inner_open() override;
  int rescan() override;
  int inner_get_next_row() override;
  int inner_close() override;
  virtual void destroy() override
  {
    ObTableScanOp::destroy();
  }

  inline void add_scan_times()
  {
    ++scan_times_;
  }
  int64_t scan_time() const
  {
    return scan_times_;
  }

  void get_used_range_count(int64_t& range_count) const;

private:
  enum MultiPartScanState { DO_PARTITION_SCAN, OUTPUT_ROWS, EXECUTION_FINISHED };
  int do_next_partition_scan();

private:
  static const int64_t NOT_INIT = 0;
  int64_t input_part_offset_;
  MultiPartScanState multi_part_scan_state_;
  int64_t scan_times_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiPartTableScanOp);
};

}  // end namespace sql
}  // end namespace oceanbase
#endif
