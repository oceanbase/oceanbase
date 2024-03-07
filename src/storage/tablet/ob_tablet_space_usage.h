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
struct ObTabletSpaceUsage final
{
public:
  ObTabletSpaceUsage()
    : shared_data_size_(0), data_size_(0), shared_meta_size_(0), meta_size_(0), occupy_bytes_(0)
  {
  }
  bool is_valid() const;
  void reset()
  {
    shared_data_size_ = 0;
    data_size_ = 0;
    shared_meta_size_ = 0;
    meta_size_ = 0;
    occupy_bytes_ = 0;
  }
  TO_STRING_KV(K_(shared_data_size), K_(data_size), K_(shared_meta_size), K_(meta_size), K_(occupy_bytes));
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int32_t get_serialize_size() const;
public:
  static const int32_t TABLET_SPACE_USAGE_INFO_VERSION = 1;
public:
  int64_t shared_data_size_; // compat from master, unused in branch 4_2_x_release 8B
  int64_t data_size_; // required_size_ 8B
  int64_t shared_meta_size_; // shared (meta block) size; compat from master, unused in branch 4_2_x_release 8B
  int64_t meta_size_; // compat from master, unused in branch 4_2_x_release 8B
  int64_t occupy_bytes_; // real data size 8B
}; // 5*8=40;

struct ObTabletSimpleSpaceUsage final
{
public:
  ObTabletSimpleSpaceUsage(): occupy_bytes_(0), required_bytes_(0) {}
  ~ObTabletSimpleSpaceUsage() = default;
  void init(const ObTabletSpaceUsage &space_usage) {
    occupy_bytes_ = space_usage.occupy_bytes_;
    required_bytes_ = space_usage.data_size_;
  }

  TO_STRING_KV(K_(occupy_bytes), K_(required_bytes));

public:
  int64_t occupy_bytes_;
  int64_t required_bytes_;
};

}
}

#endif