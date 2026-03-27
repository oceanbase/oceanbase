/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_ddl_info.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
ObTabletDDLInfo::ObTabletDDLInfo()
  : ddl_schema_version_(0),
    ddl_schema_refreshed_ts_(OB_INVALID_TIMESTAMP),
    schema_version_change_scn_(SCN::min_scn()),
    rwlock_(common::ObLatchIds::TABLET_DDL_INFO_LOCK)
{
}

ObTabletDDLInfo &ObTabletDDLInfo::operator=(const ObTabletDDLInfo &other)
{
  ddl_schema_version_ = other.ddl_schema_version_;
  ddl_schema_refreshed_ts_ = other.ddl_schema_refreshed_ts_;
  schema_version_change_scn_ = other.schema_version_change_scn_;
  return *this;
}

void ObTabletDDLInfo::reset()
{
  ddl_schema_version_ = 0;
  ddl_schema_refreshed_ts_ = OB_INVALID_TIMESTAMP;
  schema_version_change_scn_.set_min();
}

int ObTabletDDLInfo::get(int64_t &schema_version, int64_t &schema_refreshed_ts)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(rwlock_);
  schema_version = ddl_schema_version_;
  schema_refreshed_ts = ddl_schema_refreshed_ts_;
  return ret;
}
int ObTabletDDLInfo::update(const int64_t schema_version,
                            const SCN &scn,
                            int64_t &schema_refreshed_ts)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(rwlock_);
  if (schema_version <= 0 || !scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(schema_version), K(scn));
  } else if (ddl_schema_version_ < schema_version) {
    ddl_schema_refreshed_ts_ = common::max(ObTimeUtility::current_time(), ddl_schema_refreshed_ts_);
    schema_version_change_scn_ = scn;
    ddl_schema_version_ = schema_version;
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    schema_refreshed_ts = ddl_schema_refreshed_ts_;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
