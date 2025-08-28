/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_TABLE_SEQUENTIAL_GROUPER_H
#define OCEANBASE_TABLE_SEQUENTIAL_GROUPER_H

#include "share/table/ob_table.h"



namespace oceanbase
{
namespace table
{


template <typename OpType, typename IndexType>
class GenericSequentialGrouper
{
public:
  using ValueType = OpType;
  using KeyType = IndexType;
public:
  GenericSequentialGrouper()
    : allocator_("SeqGrouperAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      groups_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)) {}

  ~GenericSequentialGrouper()
  {
    for (int i = 0; i < groups_.count(); ++i) {
      OpGroup* group = groups_.at(i);
      if (OB_NOT_NULL(group)) {
        group->~OpGroup();
        allocator_.free(group);
      }
    }
    allocator_.clear();
  }
  struct OpInfo {
    public:
    OpInfo() : index_(0), op_(nullptr), tablet_idx_(0), op_idx_(0) {}
    OpInfo(IndexType index, OpType *op, int tablet_idx, int op_idx) : index_(index),
                                                                      op_(op),
                                                                      tablet_idx_(tablet_idx),
                                                                      op_idx_(op_idx) {}
    OpInfo(const OpInfo &other) : index_(other.index_),
                                  op_(other.op_),
                                  tablet_idx_(other.tablet_idx_),
                                  op_idx_(other.op_idx_) {}
    ~OpInfo() = default;
    TO_STRING_KV(K_(index), K_(tablet_idx), K_(op_idx), KP_(op));
    IndexType index_;   // tablet_id/index
    OpType *op_;        // operation
    int tablet_idx_;    // tablet index in lsop_res/batch_res
    int op_idx_;        // operation index in tablet_op_res/batch_res
  };
  struct OpGroup {
    OpGroup() {
      ops_.set_attr(common::ObMemAttr(MTL_ID(), "OpGroup"));
    }
    ~OpGroup() = default;
    TO_STRING_KV(K_(type), K_(ops));
    ObTableOperationType::Type type_;
    ObSEArray<OpInfo, 4> ops_; // <tablet_id/index, OpInfo{index, op, tablet_idx, op_idx}>
  };
  using BatchGroupType = OpGroup;

  /**
  * Sequential grouper groups operations sequentially by type, creating new groups when operation type changes.
  * 
  * @param index  The sequence index of the operation in original request
  * @param op     The operation to be grouped
  * @return       OB_SUCCESS if successfully added to group, error code otherwise
  *  
  * Grouping rules:
  * 1. Operations are batched together while they share the same type
  * 2. New group is created when operation type changes from previous
  * 3. Strictly preserves original operation sequence in grouping
  * 4. Groups may contain mixed indexes/tablets but maintain request order
  * 
  * Example:
  * LSOP request
  * TabletOp1: [PUT1, PUT2, GET1, PUT3]
  * TabletOp2: [PUT1, GET1, PUT2, DEL1, GET2, PUT3]
  * 
  * add operation result:
  * Group1: PUT (Tablet1.PUT1, Tablet1.PUT2)
  * Group2: GET (Tablet1.GET1)
  * Group3: PUT (Tablet1.PUT3, Tablet2.PUT1) // cross tablet
  * Group4: GET (Tablet2.GET1)
  * Group5: PUT (Tablet2.PUT2)
  * Group6: DEL (Tablet2.DEL1)  
  * Group7: GET (Tablet2.GET2)
  * Group8: PUT (Tablet2.PUT3)
  * process order:
  * Group1 → Group2 → Group3 → Group4 → Group5 → Group6 → Group7 → Group8
  * 
  * Batch request
  * TableOperations: [PUT1, PUT2, DEL1, PUT3, PUT4, DEL2, PUT5, DEL3, PUT6]
  * 
  * add operation result:
  * Group1: PUT (PUT1, PUT2)
  * Group2: DEL (DEL1)
  * Group3: PUT (PUT3, PUT4)
  * Group4: DEL (DEL2)
  * Group5: PUT (PUT5)
  * Group6: DEL (DEL3)
  * Group7: PUT (PUT6)
  * process order:
  * Group1 → Group2 → Group3 → Group4 → Group5 → Group6 → Group7
  */
  int add_operation(IndexType index, int tablet_idx, int op_idx, OpType &op);
  template <typename Func> int process_groups(Func &&processor)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < groups_.count(); ++i) {
      if (OB_FAIL(processor(*groups_.at(i)))) {
        SERVER_LOG(WARN, "failed to process group", K(ret), KP(groups_.at(i)));
      }
    }
    return ret;
  }

  OB_INLINE ObTableOperationType::Type get_group_type(const ObTableSingleOp &op)
  {
    return op.get_op_type();
  }

  OB_INLINE ObTableOperationType::Type get_group_type(const ObTableOperation &op)
  {
    return op.type();
  }
  OB_INLINE common::ObSEArray<OpGroup*, 1> &get_groups() { return groups_; }

  int create_new_group(ObTableOperationType::Type type);

  TO_STRING_KV(K_(groups));
private:
  common::ObArenaAllocator allocator_;
  common::ObSEArray<OpGroup*, 1> groups_;
};


} // end of namespace table
} // end of namespace oceanbase
#endif
