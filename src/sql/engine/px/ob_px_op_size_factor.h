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

#ifndef _OB_PX_OP_SIZE_FACTOR_H_
#define _OB_PX_OP_SIZE_FACTOR_H_

#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace sql
{

struct PxOpSizeFactor
{
  OB_UNIS_VERSION(1);
public:
  PxOpSizeFactor() :
    block_granule_child_(false), block_granule_parent_(false),
    partition_granule_child_(false), partition_granule_parent_(false),
    single_partition_table_scan_(false), broadcast_exchange_(false),
    pk_exchange_(false), reserved_(0)
  {}
  TO_STRING_KV(K_(block_granule_child),
    K_(block_granule_parent),
    K_(partition_granule_child),
    K_(partition_granule_parent),
    K_(single_partition_table_scan),
    K_(broadcast_exchange),
    K_(pk_exchange));
  void revert_all() { factor_ = 0; }
  void merge_factor(PxOpSizeFactor src_factor) { factor_ |= src_factor.factor_; }
  bool has_exchange() const
  { return pk_exchange_ || broadcast_exchange_; }
  bool has_granule() const
  {
    return (block_granule_child_ ||
            block_granule_parent_ ||
            partition_granule_child_ ||
            partition_granule_parent_);
  }
  void revert_exchange()
  {
    broadcast_exchange_ = false;
    pk_exchange_ = false;
  }
  bool has_leaf_granule() const
  { return has_granule() || single_partition_table_scan_; }
  bool has_granule_child_factor() const
  { return block_granule_child_ || partition_granule_child_; }
  bool has_partition_granule() const
  { return partition_granule_child_ || partition_granule_parent_; }
  PxOpSizeFactor get_granule_child_factor()
  {
    PxOpSizeFactor tmp_factor;
    tmp_factor.block_granule_child_ = block_granule_child_;
    tmp_factor.partition_granule_child_ = partition_granule_child_;
    return tmp_factor;
  }
  bool has_block_granule() const
  { return block_granule_child_ || block_granule_parent_; }
  void revert_leaf_factor()
  {
    block_granule_child_ = false;
    block_granule_parent_ = false;
    partition_granule_child_ = false;
    partition_granule_parent_ = false;
    single_partition_table_scan_ = false;
  }

  union
  {
    uint32_t factor_;
    struct {
      uint32_t block_granule_child_ :1;
      uint32_t block_granule_parent_ :1;
      uint32_t partition_granule_child_ :1;
      uint32_t partition_granule_parent_ :1;
      uint32_t single_partition_table_scan_ :1;
      uint32_t broadcast_exchange_ :1;
      uint32_t pk_exchange_ :1;
      uint32_t reserved_ :25 ;
    };
  };
};

}
}

#endif // _OB_PX_OP_SIZE_FACTOR_H_
