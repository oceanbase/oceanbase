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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MAP_KEY
#define OCEANBASE_STORAGE_OB_TABLET_MAP_KEY

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage
{
class ObTabletMapKey final
{
public:
  ObTabletMapKey();
  ObTabletMapKey(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id);
  ~ObTabletMapKey();

  void reset();
  bool is_valid() const;

  bool operator ==(const ObTabletMapKey &other) const;
  bool operator !=(const ObTabletMapKey &other) const;
  bool operator <(const ObTabletMapKey &other) const;
  int hash(uint64_t &hash_val) const;
  uint64_t hash() const;

  TO_STRING_KV(K_(ls_id), K_(tablet_id));
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
};

inline bool ObTabletMapKey::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid();
}

inline bool ObTabletMapKey::operator ==(const ObTabletMapKey &other) const
{
  return ls_id_ == other.ls_id_ && tablet_id_ == other.tablet_id_;
}

inline bool ObTabletMapKey::operator !=(const ObTabletMapKey &other) const
{
  return !(*this == other);
}

inline bool ObTabletMapKey::operator <(const ObTabletMapKey &other) const
{
  return ls_id_ < other.ls_id_ && tablet_id_ < other.tablet_id_;
}
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MAP_KEY
