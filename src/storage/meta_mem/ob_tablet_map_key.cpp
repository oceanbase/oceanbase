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

#include "storage/meta_mem/ob_tablet_map_key.h"
#include "share/transfer/ob_transfer_info.h" // OB_INVALID_TRANSFER_SEQ

namespace oceanbase
{
namespace storage
{
ObTabletMapKey::ObTabletMapKey()
  : ls_id_(),
    tablet_id_()
{
}

ObTabletMapKey::ObTabletMapKey(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
  : ls_id_(ls_id),
    tablet_id_(tablet_id)
{
}

ObTabletMapKey::~ObTabletMapKey()
{
  reset();
}

void ObTabletMapKey::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
}

int ObTabletMapKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

uint64_t ObTabletMapKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  return hash_val;
}

ObDieingTabletMapKey::ObDieingTabletMapKey()
  : tablet_id_(ObTabletID::INVALID_TABLET_ID),
    transfer_seq_(share::OB_INVALID_TRANSFER_SEQ)
{
}

ObDieingTabletMapKey::ObDieingTabletMapKey(
    const uint64_t tablet_id,
    const int64_t transfer_seq)
  : tablet_id_(tablet_id),
    transfer_seq_(transfer_seq)
{
}

ObDieingTabletMapKey::ObDieingTabletMapKey(const ObTabletMapKey &tablet_map_key, const int64_t transfer_seq)
  : tablet_id_(tablet_map_key.tablet_id_.id()),
    transfer_seq_(transfer_seq)
{
}

ObDieingTabletMapKey::~ObDieingTabletMapKey()
{
  reset();
}

void ObDieingTabletMapKey::reset()
{
  tablet_id_ = ObTabletID::INVALID_TABLET_ID;
  transfer_seq_ = -1;
}

int ObDieingTabletMapKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

uint64_t ObDieingTabletMapKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = common::murmurhash(&transfer_seq_, sizeof(transfer_seq_), hash_val);
  return hash_val;
}

} // namespace storage
} // namespace oceanbase
