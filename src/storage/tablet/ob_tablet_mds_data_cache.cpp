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

#include "storage/tablet/ob_tablet_mds_data_cache.h"

#include "storage/tx/ob_trans_define.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"

using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{
ObTabletStatusCache::ObTabletStatusCache()
  : tablet_status_(ObTabletStatus::MAX),
    create_commit_version_(ObTransVersion::INVALID_TRANS_VERSION),
    delete_commit_version_(ObTransVersion::INVALID_TRANS_VERSION)
{
}

void ObTabletStatusCache::set_value(
    const ObTabletStatus &tablet_status,
    const int64_t create_commit_version,
    const int64_t delete_commit_version)
{
  tablet_status_ = tablet_status;
  create_commit_version_ = create_commit_version;
  delete_commit_version_ = delete_commit_version;
}

void ObTabletStatusCache::set_value(const ObTabletCreateDeleteMdsUserData &user_data)
{
  tablet_status_ = user_data.tablet_status_;
  create_commit_version_ = user_data.create_commit_version_;
  delete_commit_version_ = user_data.delete_commit_version_;
}

void ObTabletStatusCache::reset()
{
  tablet_status_ = ObTabletStatus::MAX;
  create_commit_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  delete_commit_version_ = ObTransVersion::INVALID_TRANS_VERSION;
}


ObDDLInfoCache::ObDDLInfoCache()
  : redefined_(false),
    schema_version_(INT64_MAX),
    snapshot_version_(INT64_MAX)
{
}

void ObDDLInfoCache::set_value(
    const bool redefined,
    const int64_t schema_version,
    const int64_t snapshot_version)
{
  redefined_ = redefined;
  schema_version_ = schema_version;
  snapshot_version_ = snapshot_version;
}

void ObDDLInfoCache::set_value(const ObTabletBindingMdsUserData &user_data)
{
  redefined_ = user_data.redefined_;
  schema_version_ = user_data.schema_version_;
  snapshot_version_ = user_data.snapshot_version_;
}

void ObDDLInfoCache::reset()
{
  redefined_ = false;
  schema_version_ = INT64_MAX;
  snapshot_version_ = INT64_MAX;
}
} // namespace storage
} // namespace oceanbase