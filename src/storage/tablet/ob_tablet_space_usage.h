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
    : all_sstable_data_occupy_size_(0),
      all_sstable_data_required_size_(0),
      all_sstable_meta_size_(0),
      ss_public_sstable_occupy_size_(0),
      tablet_clustered_meta_size_(0),
      tablet_clustered_sstable_data_size_(0),
      backup_bytes_(0)
  {
  }
  void reset()
  {
    all_sstable_data_occupy_size_ = 0;
    all_sstable_data_required_size_ = 0;
    all_sstable_meta_size_ = 0;
    ss_public_sstable_occupy_size_ = 0;
    tablet_clustered_meta_size_ = 0;
    tablet_clustered_sstable_data_size_ = 0;
    backup_bytes_ = 0;
  }
  TO_STRING_KV(K_(all_sstable_data_occupy_size), K_(all_sstable_data_required_size), K_(all_sstable_meta_size), K_(ss_public_sstable_occupy_size), K_(tablet_clustered_meta_size), K_(tablet_clustered_sstable_data_size), K_(backup_bytes));
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int32_t get_serialize_size() const;
  bool is_valid() const
  {
    return (OB_INVALID_SIZE != all_sstable_data_occupy_size_)
        && (OB_INVALID_SIZE != all_sstable_data_required_size_)
        && (OB_INVALID_SIZE != all_sstable_meta_size_)
        && (OB_INVALID_SIZE != ss_public_sstable_occupy_size_)
        && (OB_INVALID_SIZE != tablet_clustered_meta_size_)
        && (OB_INVALID_SIZE != tablet_clustered_sstable_data_size_)
        && (OB_INVALID_SIZE != backup_bytes_);
  }
public:
  static const int32_t TABLET_SPACE_USAGE_INFO_VERSION = 1;
public:
  // all sstable data occupy_size, include major sstable
  // <data_block real_size> + <small_sstable_nest_size (in share_nothing)>
  int64_t all_sstable_data_occupy_size_; // compat : occupy_bytes_
  // all sstable data requred_size, data_block_count * 2MB, include major sstable
  int64_t all_sstable_data_required_size_; // compat : data_size_
  // sstable_meta_block_count * 2MB
  int64_t all_sstable_meta_size_; // compat : meta_size_
  // used_by shared_storage
  // major sstable data occupy_size
  int64_t ss_public_sstable_occupy_size_; // new feild in shared_storage
  // shared_meta_size in shared_nothing
  // tablet_meta_size in shared_storage
  int64_t tablet_clustered_meta_size_; // compat : shared_meta_size_
  // used_by shared_nothing
  // small sstable data_size
  int64_t tablet_clustered_sstable_data_size_;  // compat : shared_data_size_
  int64_t backup_bytes_; // new field in quick_retore
};
}
}

#endif
