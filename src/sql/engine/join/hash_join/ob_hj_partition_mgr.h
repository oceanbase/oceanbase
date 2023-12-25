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

#ifndef SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HJ_PARTITION_MGR_H_
#define SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HJ_PARTITION_MGR_H_

#include "lib/list/ob_list.h"
#include "sql/engine/join/hash_join/ob_hj_partition.h"

namespace oceanbase
{
namespace sql
{

struct ObHJPartitionPair
{
  ObHJPartition *left_;
  ObHJPartition *right_;

  ObHJPartitionPair() : left_(NULL), right_(NULL) {}
};

class ObHJPartitionMgr {
public:
  ObHJPartitionMgr(common::ObIAllocator &alloc, uint64_t tenant_id) :
    total_dump_count_(0),
    total_dump_size_(0),
    part_count_(0),
    tenant_id_(tenant_id),
    alloc_(alloc),
    part_pair_list_(alloc)
  {}

  virtual ~ObHJPartitionMgr();
  void reset();

  typedef common::ObList<ObHJPartitionPair, common::ObIAllocator> ObHJPartitionPairList;

  int next_part_pair(ObHJPartitionPair &part_pair);

  int64_t get_part_list_size() { return part_pair_list_.size(); }
  int remove_undumped_part(int64_t cur_dumped_partition,
                            int32_t part_round);
  int get_or_create_part(int32_t level,
                         int64_t part_shift,
                         int64_t partno,
                         bool is_left,
                         ObHJPartition *&part,
                         bool only_get = false);

  void free(ObHJPartition *&part) {
    if (NULL != part) {
      part->~ObHJPartition();
      alloc_.free(part);
      part = NULL;
      part_count_ --;
    }
  }

public:
  int64_t total_dump_count_;
  int64_t total_dump_size_;
  int64_t part_count_;

private:
  static const int64_t PARTITION_IDX_MASK = 0x00000000FFFFFFFF;
  uint64_t tenant_id_;
  common::ObIAllocator &alloc_;
  ObHJPartitionPairList part_pair_list_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HJ_PARTITION_MGR_H_*/
