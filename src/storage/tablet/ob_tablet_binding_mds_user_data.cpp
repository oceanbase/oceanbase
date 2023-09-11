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

#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tablet/ob_tablet_binding_info.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{
ObTabletBindingMdsUserData::ObTabletBindingMdsUserData()
  : snapshot_version_(INT64_MAX),
    schema_version_(INT64_MAX),
    data_tablet_id_(),
    hidden_tablet_id_(),
    lob_meta_tablet_id_(),
    lob_piece_tablet_id_(),
    redefined_(false)
{
}

void ObTabletBindingMdsUserData::reset()
{
  redefined_ = false;
  snapshot_version_ = INT64_MAX;
  schema_version_ = INT64_MAX;
  data_tablet_id_.reset();
  hidden_tablet_id_.reset();
  lob_meta_tablet_id_.reset();
  lob_piece_tablet_id_.reset();
}

void ObTabletBindingMdsUserData::set_default_value()
{
  redefined_ = false;
  snapshot_version_ = 0;
  schema_version_ = 0;
  data_tablet_id_.reset();
  hidden_tablet_id_.reset();
  lob_meta_tablet_id_.reset();
  lob_piece_tablet_id_.reset();
}

bool ObTabletBindingMdsUserData::is_valid() const
{
  return snapshot_version_ != INT64_MAX && schema_version_ != INT64_MAX;
}

int ObTabletBindingMdsUserData::assign(const ObTabletBindingMdsUserData &other)
{
  int ret = OB_SUCCESS;
  redefined_ = other.redefined_;
  snapshot_version_ = other.snapshot_version_;
  schema_version_ = other.schema_version_;
  data_tablet_id_ = other.data_tablet_id_;
  hidden_tablet_id_ = other.hidden_tablet_id_;
  lob_meta_tablet_id_ = other.lob_meta_tablet_id_;
  lob_piece_tablet_id_ = other.lob_piece_tablet_id_;
  return ret;
}

int ObTabletBindingMdsUserData::assign_from_tablet_meta(const ObTabletBindingInfo &other)
{
  int ret = OB_SUCCESS;

  if (other.hidden_tablet_ids_.count() == 0) {
    hidden_tablet_id_.reset();
  } else if (other.hidden_tablet_ids_.count() == 1) {
    hidden_tablet_id_ = other.hidden_tablet_ids_.at(0);
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCC(ret)) {
    redefined_ = other.redefined_;
    snapshot_version_ = other.snapshot_version_;
    schema_version_ = other.schema_version_;
    data_tablet_id_ = other.data_tablet_id_;
    lob_meta_tablet_id_ = other.lob_meta_tablet_id_;
    lob_piece_tablet_id_ = other.lob_piece_tablet_id_;
  }

  return ret;
}

int ObTabletBindingMdsUserData::dump_to_tablet_meta(ObTabletBindingInfo &other)
{
  int ret = OB_SUCCESS;

  other.hidden_tablet_ids_.reset();
  if (hidden_tablet_id_.is_valid() && OB_FAIL(other.hidden_tablet_ids_.push_back(hidden_tablet_id_))) {
    LOG_WARN("failed to push back hidden tablet id", K(ret));
  } else {
    other.redefined_ = redefined_;
    other.snapshot_version_ = snapshot_version_;
    other.schema_version_ = schema_version_;
    other.data_tablet_id_ = data_tablet_id_;
    other.lob_meta_tablet_id_ = lob_meta_tablet_id_;
    other.lob_piece_tablet_id_ = lob_piece_tablet_id_;
  }

  return ret;
}

void ObTabletBindingMdsUserData::on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
{
  if (OB_INVALID_VERSION == snapshot_version_) {
    // unbind has set the mds with snapshot_version_ of -1, indicating that we need to fill in the commit version here
    snapshot_version_ = commit_version.get_val_for_tx();
  }
  LOG_INFO("binding mds commit", K(redefined_), K(snapshot_version_), K(commit_version));
  return;
}

OB_SERIALIZE_MEMBER(
  ObTabletBindingMdsUserData,
  redefined_,
  snapshot_version_,
  schema_version_,
  data_tablet_id_,
  hidden_tablet_id_,
  lob_meta_tablet_id_,
  lob_piece_tablet_id_);
} // namespace storage
} // namespace oceanbase
