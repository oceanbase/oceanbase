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

#include "storage/tablet/ob_tablet_binding_info.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{
ObTabletBindingInfo::ObTabletBindingInfo()
  : redefined_(false),
    snapshot_version_(INT64_MAX),
    schema_version_(INT64_MAX),
    data_tablet_id_(),
    hidden_tablet_ids_(),
    lob_meta_tablet_id_(),
    lob_piece_tablet_id_()
{
}

void ObTabletBindingInfo::reset()
{
  redefined_ = false;
  snapshot_version_ = INT64_MAX;
  schema_version_ = INT64_MAX;
  data_tablet_id_.reset();
  hidden_tablet_ids_.reset();
  lob_meta_tablet_id_.reset();
  lob_piece_tablet_id_.reset();
}

bool ObTabletBindingInfo::is_valid() const
{
  bool valid = true;

  if (INT64_MAX == snapshot_version_) {
    valid = false;
  } else if (INT64_MAX == schema_version_) {
    valid = false;
  }

  return valid;
}

int ObTabletBindingInfo::assign(const ObTabletBindingInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hidden_tablet_ids_.assign(other.hidden_tablet_ids_))) {
    LOG_WARN("failed to assign hidden tablet ids", K(ret));
  } else {
    redefined_ = other.redefined_;
    snapshot_version_ = other.snapshot_version_;
    schema_version_ = other.schema_version_;
    data_tablet_id_ = other.data_tablet_id_;
    lob_meta_tablet_id_ = other.lob_meta_tablet_id_;
    lob_piece_tablet_id_ = other.lob_piece_tablet_id_;
  }
  return ret;
}

int ObTabletBindingInfo::deep_copy(
    const memtable::ObIMultiSourceDataUnit *src,
    ObIAllocator *allocator)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src info", K(ret));
  } else if (OB_UNLIKELY(src->type() != type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret));
  } else if (OB_FAIL(assign(*static_cast<const ObTabletBindingInfo *>(src)))) {
    LOG_WARN("failed to copy tablet binding info", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    ObTabletBindingInfo,
    redefined_,
    snapshot_version_,
    schema_version_,
    data_tablet_id_,
    hidden_tablet_ids_,
    lob_meta_tablet_id_,
    lob_piece_tablet_id_);
} // namespace storage
} // namespace oceanbase