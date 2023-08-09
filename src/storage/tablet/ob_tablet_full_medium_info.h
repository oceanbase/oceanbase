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

#ifndef OCEANBASE_STORAGE_OB_TABLET_FULL_MEDIUM_INFO
#define OCEANBASE_STORAGE_OB_TABLET_FULL_MEDIUM_INFO

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/compaction/ob_extra_medium_info.h"
#include "storage/tablet/ob_tablet_dumped_medium_info.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{
class ObTabletFullMediumInfo
{
public:
  ObTabletFullMediumInfo();
  ~ObTabletFullMediumInfo() = default;
  ObTabletFullMediumInfo(const ObTabletFullMediumInfo &) = delete;
  ObTabletFullMediumInfo &operator=(const ObTabletFullMediumInfo &) = delete;
public:
  void reset();
  int assign(common::ObIAllocator &allocator, const ObTabletFullMediumInfo &other);

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(extra_medium_info), K_(medium_info_list));
public:
  compaction::ObExtraMediumInfo extra_medium_info_;
  ObTabletDumpedMediumInfo medium_info_list_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_FULL_MEDIUM_INFO
