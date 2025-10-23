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
#include "share/scn.h"

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

class ObDieingTabletMapKey final
{
public:
  ObDieingTabletMapKey();
  ObDieingTabletMapKey(const uint64_t tablet_id, const int32_t transfer_epoch);
  ObDieingTabletMapKey(const ObTabletMapKey &tablet_map_key, const int32_t transfer_epoch);
  ~ObDieingTabletMapKey();

  void reset();
  bool is_valid() const;

  bool operator ==(const ObDieingTabletMapKey &other) const;
  bool operator !=(const ObDieingTabletMapKey &other) const;
  bool operator <(const ObDieingTabletMapKey &other) const;
  int hash(uint64_t &hash_val) const;
  uint64_t hash() const;

  TO_STRING_KV(K_(tablet_id), K_(transfer_epoch));
private:
  uint64_t tablet_id_;
  int32_t transfer_epoch_;
};

inline bool ObDieingTabletMapKey::is_valid() const
{
  return ObTabletID::INVALID_TABLET_ID != tablet_id_ && transfer_epoch_ >= 0;
}

inline bool ObDieingTabletMapKey::operator ==(const ObDieingTabletMapKey &other) const
{
  return tablet_id_ == other.tablet_id_ && transfer_epoch_ == other.transfer_epoch_;
}

inline bool ObDieingTabletMapKey::operator !=(const ObDieingTabletMapKey &other) const
{
  return !(*this == other);
}

inline bool ObDieingTabletMapKey::operator <(const ObDieingTabletMapKey &other) const
{
  return tablet_id_ < other.tablet_id_ && transfer_epoch_ < other.transfer_epoch_;
}

class ObSSTabletMapKey final
{
public:
  ObSSTabletMapKey(){};
  ~ObSSTabletMapKey();
  void reset();
  bool is_valid() const;

  bool operator ==(const ObSSTabletMapKey &other) const;
  bool operator !=(const ObSSTabletMapKey &other) const;
  bool operator <(const ObSSTabletMapKey &other) const;
  int hash(uint64_t &hash_val) const;
  uint64_t hash() const;
  int set_common_tablet_key(const ObTabletID &tablet_id, const uint64_t transfer_scn);
  int set_inner_tablet_key(const ObTabletID &tablet_id, const share::ObLSID ls_id);
  TO_STRING_KV(K_(tablet_id), K_(transfer_scn), K_(ls_id));

private:
  uint64_t tablet_id_;
  union {
    uint64_t transfer_scn_;
    int64_t ls_id_;
  };
};

inline bool ObSSTabletMapKey::is_valid() const
{
  bool is_valid = true;

  is_valid = ObTabletID::INVALID_TABLET_ID != tablet_id_;
  if (is_valid && ObTabletID(tablet_id_).is_ls_inner_tablet()) {
    is_valid = share::ObLSID::INVALID_LS_ID != ls_id_;
  } else if (is_valid && !ObTabletID(tablet_id_).is_ls_inner_tablet()) {
    is_valid = share::OB_INVALID_SCN_VAL != transfer_scn_;
  }

  return is_valid;
}

inline bool ObSSTabletMapKey::operator ==(const ObSSTabletMapKey &other) const
{
  return (tablet_id_ == other.tablet_id_) && (ls_id_ == other.ls_id_);
}

inline bool ObSSTabletMapKey::operator !=(const ObSSTabletMapKey &other) const
{
  return !(*this == other);
}

inline bool ObSSTabletMapKey::operator <(const ObSSTabletMapKey &other) const
{
  bool lt;
  if (tablet_id_ != other.tablet_id_) {
    lt = tablet_id_ < other.tablet_id_;
  } else if (ObTabletID(tablet_id_).is_ls_inner_tablet()) {
    lt = ls_id_ < other.ls_id_;
  } else {
    lt = transfer_scn_ < other.transfer_scn_;
  }
  return lt;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MAP_KEY
