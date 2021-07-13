/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software
 * according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *
 * http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A
 * PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_COMMON_OB_MASK_SET_
#define OCEANBASE_COMMON_OB_MASK_SET_

#include "common/ob_partition_key.h"
#include "lib/container/ob_bit_set.h"

namespace oceanbase {
namespace common {

class ObMaskSet {
public:
  ObMaskSet()
      : is_inited_(false),
        is_bounded_staleness_read_(false),
        partitions_(ObModIds::OB_TRANS_PARTITION_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE),
        bitset_()
  {}
  ~ObMaskSet()
  {}
  int init(const ObPartitionLeaderArray& pla);
  int init(const ObPartitionArray& partitions);
  void reset();

public:
  int mask(const ObPartitionKey& partition);
  int multi_mask(const ObPartitionArray& partitions);
  bool is_all_mask() const;
  int get_not_mask(ObPartitionArray& partitions) const;
  int get_mask(ObPartitionArray& partitions) const;
  bool is_mask(const ObPartitionKey& partition);
  void clear_set();
  bool is_inited() const
  {
    return is_inited_;
  };

  // for slave read
  int mask(const ObPartitionKey& partition, const ObAddr& addr);
  int get_not_mask(ObPartitionLeaderArray& pla) const;
  bool is_mask(const ObPartitionKey& partition, const ObAddr& addr);

protected:
  bool is_inited_;
  bool is_bounded_staleness_read_;
  ObPartitionLeaderArray pla_;
  ObPartitionArray partitions_;
  ObBitSet<> bitset_;
};

}  // namespace common
}  // namespace oceanbase
#endif  // OCEANBASE_COMMON_OB_MASK_SET_
