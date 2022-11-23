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

#ifndef OCEANBASE_SRC_SQL_ENGINE_TABLE_OB_TABLE_SCAN_WITH_INDEX_BACK_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_TABLE_OB_TABLE_SCAN_WITH_INDEX_BACK_OP_H_
#include "sql/engine/table/ob_table_scan_op.h"
namespace oceanbase
{
namespace sql
{

class ObTableScanWithIndexBackSpec : public ObTableScanSpec
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObTableScanWithIndexBackSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  virtual ~ObTableScanWithIndexBackSpec();
  inline void set_index_scan_tree_id(uint64_t index_scan_tree_id) {
    index_scan_tree_id_ = index_scan_tree_id;
  }
  inline uint64_t get_index_scan_tree_id() const { return index_scan_tree_id_; }

private:
  uint64_t index_scan_tree_id_;
};

class ObTableScanWithIndexBackOp : public ObTableScanOp
{
private:
  enum READ_ACTION
  {
    INVALID_ACTION,
    READ_ITERATOR,
    READ_TABLE_PARTITION,
    READ_ITER_END
  };
public:
  explicit ObTableScanWithIndexBackOp(ObExecContext &exec_ctx,
                                      const ObOpSpec &spec,
                                      ObOpInput *input)
    : ObTableScanOp(exec_ctx, spec, input),
      is_index_end_(false),
      use_table_allocator_(false),
      read_action_(INVALID_ACTION),
      index_scan_tree_(NULL),
      scan_param_(),
      result_(nullptr)
  { }
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual int inner_rescan() override;
protected:
  int open_index_scan();
  int extract_range_from_index();
  int do_table_scan_with_index();
  int do_table_rescan_with_index();
private:
  bool is_index_end_;
  bool use_table_allocator_;
  READ_ACTION read_action_;
  ObOperator *index_scan_tree_;
  storage::ObTableScanParam scan_param_;
  common::ObNewRowIterator *result_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_TABLE_OB_TABLE_SCAN_WITH_INDEX_BACK_OP_H_ */
