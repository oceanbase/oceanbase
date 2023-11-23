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

class ObTabletMacroInfo;
struct ObTabletSpaceUsage final
{
public:
  ObTabletSpaceUsage()
    : shared_data_size_(0), data_size_(0), shared_meta_size_(0), meta_size_(0)
  {
  }
  void reset()
  {
    shared_data_size_ = 0;
    data_size_ = 0;
    shared_meta_size_ = 0;
    meta_size_ = 0;
  }
  TO_STRING_KV(K_(shared_data_size), K_(data_size), K_(shared_meta_size), K_(meta_size));
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int32_t get_serialize_size() const;
public:
  static const int32_t TABLET_SPACE_USAGE_INFO_VERSION = 1;
public:
  int64_t shared_data_size_; // shared (data block) size
  int64_t data_size_;
  int64_t shared_meta_size_; // shared (meta block) size
  int64_t meta_size_;
};
}
}

#endif