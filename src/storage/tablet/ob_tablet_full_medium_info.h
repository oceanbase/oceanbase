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
#include "storage/tablet/ob_tablet_dumped_medium_info.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{
class ObTaletExtraMediumInfo
{
public:
  ObTaletExtraMediumInfo();
  ~ObTaletExtraMediumInfo() = default;
public:
  void reset();

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(info),
               K_(compat),
               K_(last_compaction_type),
               K_(wait_check_flag),
               K_(last_medium_scn));
private:
  static const int64_t MEDIUM_LIST_VERSION = 1;
  static const int32_t MEDIUM_LIST_INFO_RESERVED_BITS = 51;
public:
  union
  {
    uint64_t info_;
    struct
    {
      uint64_t compat_                  : 8;
      uint64_t last_compaction_type_    : 4; // check inner_table when last_compaction is major
      uint64_t wait_check_flag_         : 1; // true: need check finish, false: don't need check
      uint64_t reserved_                : MEDIUM_LIST_INFO_RESERVED_BITS;
    };
  };
  int64_t last_medium_scn_;
};

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
  ObTaletExtraMediumInfo extra_medium_info_;
  ObTabletDumpedMediumInfo medium_info_list_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_FULL_MEDIUM_INFO
