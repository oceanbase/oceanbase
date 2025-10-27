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

#define USING_LOG_PREFIX STORAGE

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
    transfer_epoch_(share::OB_INVALID_TRANSFER_SEQ)
{
}

ObDieingTabletMapKey::ObDieingTabletMapKey(
    const uint64_t tablet_id,
    const int32_t transfer_epoch)
  : tablet_id_(tablet_id),
    transfer_epoch_(transfer_epoch)
{
}

ObDieingTabletMapKey::ObDieingTabletMapKey(const ObTabletMapKey &tablet_map_key, const int32_t transfer_epoch)
  : tablet_id_(tablet_map_key.tablet_id_.id()),
    transfer_epoch_(transfer_epoch)
{
}

ObDieingTabletMapKey::~ObDieingTabletMapKey()
{
  reset();
}

void ObDieingTabletMapKey::reset()
{
  tablet_id_ = ObTabletID::INVALID_TABLET_ID;
  transfer_epoch_ = -1;
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
  hash_val = common::murmurhash(&transfer_epoch_, sizeof(transfer_epoch_), hash_val);
  return hash_val;
}

ObSSTabletMapKey::~ObSSTabletMapKey()
{
  reset();
}

void ObSSTabletMapKey::reset()
{
  tablet_id_ = ObTabletID::INVALID_TABLET_ID;
  transfer_scn_ = UINT64_MAX;
}

int ObSSTabletMapKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

uint64_t ObSSTabletMapKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  if (ObTabletID(tablet_id_).is_inner_tablet()) {
    hash_val = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  } else {
    hash_val = common::murmurhash(&transfer_scn_, sizeof(transfer_scn_), hash_val);
  }
  return hash_val;
}

int ObSSTabletMapKey::set_common_tablet_key(const ObTabletID &tablet_id, const uint64_t transfer_scn) {
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid() || share::OB_INVALID_SCN_VAL == transfer_scn || tablet_id.is_inner_tablet()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(transfer_scn));
  } else {
    tablet_id_ = tablet_id.id();
    transfer_scn_ = transfer_scn;
  }
  return ret;
}

int ObSSTabletMapKey::set_inner_tablet_key(const ObTabletID &tablet_id, const share::ObLSID ls_id) {
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !tablet_id.is_valid() || !tablet_id.is_inner_tablet()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(ls_id));
  } else {
    tablet_id_ = tablet_id.id();
    ls_id_ = ls_id.id();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
