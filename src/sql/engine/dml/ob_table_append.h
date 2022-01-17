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

#ifndef OCEANBASE_SQL_ENGINE_OB_TABLE_APPEND_H_
#define OCEANBASE_SQL_ENGINE_OB_TABLE_APPEND_H_

#include "share/partition_table/ob_partition_location.h"
#include "common/row/ob_row_iterator.h"
#include "sql/engine/ob_single_child_phy_operator.h"

namespace oceanbase {
namespace sql {
class ObPhyTableLocation;
class ObTableAppendInput : public ObIPhyOperatorInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableAppendInput() : ObIPhyOperatorInput(), location_idx_(common::OB_INVALID_INDEX)
  {}
  virtual ~ObTableAppendInput() = default;
  virtual void reset() override
  {
    location_idx_ = common::OB_INVALID_INDEX;
  }
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op) override;
  int64_t get_location_idx() const
  {
    return location_idx_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableAppendInput);
  int64_t location_idx_;
};

class ObTableAppend : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION(1);

public:
  class Operator2RowIter : public common::ObNewRowIterator {
  public:
    Operator2RowIter(ObExecContext& ctx, const ObPhyOperator& op) : ctx_(ctx), op_(op)
    {}

    virtual ~Operator2RowIter()
    {}
    virtual int get_next_row(common::ObNewRow*& row) override;
    virtual void reset()
    {}

  private:
    ObExecContext& ctx_;
    const ObPhyOperator& op_;
  };

  explicit ObTableAppend(common::ObIAllocator& allocator);
  virtual ~ObTableAppend() = default;
  virtual bool is_dml_operator() const override
  {
    return false;
  }
  void set_table_id(const uint64_t table_id)
  {
    table_id_ = table_id;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }

protected:
  virtual int inner_open(ObExecContext& ctx) const override;
  virtual int inner_close(ObExecContext& ctx) const override;
  int get_part_location(ObExecContext& ctx, const ObPhyTableLocation& table_location,
      const share::ObPartitionReplicaLocation*& out) const;

protected:
  uint64_t table_id_;
};
}  // end namespace sql
}  // end namespace oceanbase
#endif  // OCEANBASE_SQL_ENGINE_OB_TABLE_APPEND_H_;
