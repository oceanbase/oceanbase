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
#ifndef _OB_EXTERNAL_TABLE_PART_INFO_H
#define _OB_EXTERNAL_TABLE_PART_INFO_H

#include <stdint.h>
#include "src/share/schema/ob_column_schema.h"

namespace oceanbase
{

namespace share
{
struct ObExternalTablePartInfo {
  ObExternalTablePartInfo() 
    : part_id_(common::OB_INVALID_ID),
      list_row_value_() {}

  TO_STRING_KV(K_(part_id), K_(list_row_value), K_(partition_spec));

  int64_t part_id_;
  common::ObNewRow list_row_value_;
  common::ObString partition_spec_;
};

class ObExternalTablePartInfoArray 
{
public:
  ObExternalTablePartInfoArray(common::ObIAllocator &alloc)
    : part_infos_(),
      allocator_(alloc) {}
  
  ~ObExternalTablePartInfoArray() {}

  TO_STRING_KV(K_(part_infos));

  int set_part_pair_by_idx(const int64_t idx, ObExternalTablePartInfo &pair);

  int reserve(const int64_t capacity);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t count() const { return part_infos_.count(); }

  const ObExternalTablePartInfo &at(const int64_t idx) const { return part_infos_.at(idx); }

private:
  common::ObArrayWrap<ObExternalTablePartInfo> part_infos_;
  common::ObIAllocator &allocator_;
};
}
}
#endif /* _OB_EXTERNAL_TABLE_PART_INFO_H */
