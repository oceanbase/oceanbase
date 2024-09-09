//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_COMPACTION_NEW_MICRO_INFO_H_
#define OB_SHARE_COMPACTION_NEW_MICRO_INFO_H_
#include "/usr/include/stdint.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace compaction
{

// will collect new generated micro info when major
struct ObNewMicroInfo
{
  ObNewMicroInfo()
    : version_(NEW_MICRO_INFO_V1),
      reserved_(0),
      meta_micro_size_(0),
      data_micro_size_(0)
  {}
  void reset()
  {
    meta_micro_size_ = 0;
    data_micro_size_ = 0;
  }
  bool is_empty() const
  {
    return meta_micro_size_ > 0 && data_micro_size_ >= 0;
  }
  void add(const ObNewMicroInfo &input_info);
  int64_t get_data_micro_size() const { return data_micro_size_; }
  int64_t get_meta_micro_size() const { return meta_micro_size_; }
  void add_data_micro_size(const int64_t data_micro_size) { data_micro_size_ += data_micro_size; }
  void add_meta_micro_size(const int64_t meta_micro_size) { meta_micro_size_ += meta_micro_size; }
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(meta_micro_size), K_(data_micro_size));
  static const int32_t SRCS_ONE_BYTE = 8;
  static const int32_t SRCS_RESERVED_BITS = 56;
  static const int64_t NEW_MICRO_INFO_V1 = 1;
private:
  union {
    uint64_t info_;
    struct {
      uint64_t version_   : SRCS_ONE_BYTE;
      uint64_t reserved_  : SRCS_RESERVED_BITS;
    };
  };
  int64_t meta_micro_size_;
  int64_t data_micro_size_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_COMPACTION_NEW_MICRO_INFO_H_
