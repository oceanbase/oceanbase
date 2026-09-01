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

#include "storage/tablet/ob_tablet_truncate_mds_user_data.h"
#include "storage/tx/ob_trans_define.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{

ObTabletTruncateMdsUserData::ObTabletTruncateMdsUserData()
  : truncate_commit_scn_(share::SCN::invalid_scn()),
    truncate_commit_version_(ObTransVersion::INVALID_TRANS_VERSION),
    schema_version_(0)
{
}

void ObTabletTruncateMdsUserData::reset()
{
  truncate_commit_scn_.set_invalid();
  truncate_commit_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  schema_version_ = 0;
}

bool ObTabletTruncateMdsUserData::is_valid() const
{
  return schema_version_ > 0;
}

bool ObTabletTruncateMdsUserData::is_default() const
{
  return truncate_commit_scn_.is_min()
         && 0 == truncate_commit_version_
         && INT64_MAX == schema_version_;
}

void ObTabletTruncateMdsUserData::set_default_value()
{
  truncate_commit_scn_.set_min();
  truncate_commit_version_ = 0;
  schema_version_ = INT64_MAX;
}

int ObTabletTruncateMdsUserData::assign(const ObTabletTruncateMdsUserData &other)
{
  int ret = OB_SUCCESS;
  truncate_commit_scn_ = other.truncate_commit_scn_;
  truncate_commit_version_ = other.truncate_commit_version_;
  schema_version_ = other.schema_version_;
  return ret;
}

void ObTabletTruncateMdsUserData::on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
{
  if (OB_INVALID_VERSION == truncate_commit_version_) {
    truncate_commit_scn_ = commit_scn;
    truncate_commit_version_ = commit_version.get_val_for_tx();
  }
  LOG_INFO("truncate mds commit", KPC(this));
  return;
}


OB_SERIALIZE_MEMBER(
    ObTabletTruncateMdsUserData,
    truncate_commit_scn_,
    truncate_commit_version_,
    schema_version_)

} // namespace storage
} // namespace oceanbase
