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

#ifndef _OB_PARTITION_ID_ROW_PAIR_H
#define _OB_PARTITION_ID_ROW_PAIR_H

#include <stdint.h>
#include "src/share/schema/ob_column_schema.h"

namespace oceanbase
{

namespace share
{
struct ObPartitionIdRowPair {
  ObPartitionIdRowPair()
    : part_id_(common::OB_INVALID_ID),
      list_row_value_() {}

  TO_STRING_KV(K_(part_id), K_(list_row_value));

  int64_t part_id_;
  common::ObNewRow list_row_value_;
};

class ObPartitionIdRowPairArray
{
public:
  ObPartitionIdRowPairArray(common::ObIAllocator &alloc)
    : pairs_(),
      allocator_(alloc) {}

  ~ObPartitionIdRowPairArray() {}

  TO_STRING_KV(K_(pairs));

  int set_part_pair_by_idx(const int64_t idx, ObPartitionIdRowPair &pair);

  int reserve(const int64_t capacity);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t count() const { return pairs_.count(); }

  const ObPartitionIdRowPair &at(const int64_t idx) const { return pairs_.at(idx); }

private:
  common::ObArrayWrap<ObPartitionIdRowPair> pairs_;
  common::ObIAllocator &allocator_;
};
}
}
#endif /* _OB_PARTITION_ID_ROW_PAIR_H */
