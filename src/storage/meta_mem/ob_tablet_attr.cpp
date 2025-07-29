/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/meta_mem/ob_tablet_attr.h"

namespace oceanbase
{
namespace storage
{

template <typename T, std::size_t expected_size, std::size_t real_size = sizeof(T)>
void check_obj_size()
{
  static_assert(expected_size == real_size,
      "The size will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
}

ObTabletFastIterAttr::ObTabletFastIterAttr()
  : v_(0)
{
#if defined(__x86_64__)
   check_obj_size<ObTabletFastIterAttr, 16>();
#endif
   initial_state_ = true;
}

OB_SERIALIZE_MEMBER(ObTabletFastIterAttr, v_);

ObTabletAttr::ObTabletAttr()
  : iter_attr_(),
    ha_status_(0),
    all_sstable_data_occupy_size_(0),
    all_sstable_data_required_size_(0),
    tablet_meta_size_(0),
    ss_public_sstable_occupy_size_(0),
    backup_bytes_(0),
    ss_change_version_(share::SCN::min_scn()),
    last_match_tablet_meta_version_(0),
    auto_part_size_(OB_INVALID_SIZE),
    notify_ss_change_version_(share::SCN::min_scn()),
    tablet_max_checkpoint_scn_(share::SCN::invalid_scn())
{
#if defined(__x86_64__)
   check_obj_size<ObTabletAttr, 104>();
#endif
}

OB_SERIALIZE_MEMBER(ObTabletAttr,
                    iter_attr_,
                    ha_status_,
                    all_sstable_data_occupy_size_,
                    all_sstable_data_required_size_,
                    tablet_meta_size_,
                    ss_public_sstable_occupy_size_,
                    backup_bytes_,
                    ss_change_version_,
                    last_match_tablet_meta_version_);
} // namespace storage
} // namespace oceanbase
