
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

#ifndef OCEANBASE_STORAGE_TABLET_OB_TABLET_SPACE_USAGE_H_
#define OCEANBASE_STORAGE_TABLET_OB_TABLET_SPACE_USAGE_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{


// used for record data size of a tablet
struct ObTabletSpaceUsage final
{
public:
  ObTabletSpaceUsage():occupy_bytes_(0), required_bytes_(0) {}
  ObTabletSpaceUsage(int64_t occupy_bytes, int64_t required_bytes)
  {
    occupy_bytes_ = occupy_bytes;
    required_bytes_ = required_bytes;
  }
  void reset()
  {
    occupy_bytes_ = 0;
    required_bytes_ = 0;
  }
  bool is_valid() const
  {
    return (OB_INVALID_SIZE != occupy_bytes_) && (OB_INVALID_SIZE != required_bytes_);
  }
  TO_STRING_KV(K_(occupy_bytes),
               K_(required_bytes));
  // serialize & deserialize
  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;
public:
  int64_t occupy_bytes_; // 8B
  // required_bytes_ will be upgraded to the field data_size_ in 4.3
  int64_t required_bytes_; // 8B
}; // 16B

} // storage
} // oceanbase

#endif