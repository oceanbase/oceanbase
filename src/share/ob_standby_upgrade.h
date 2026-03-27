/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_STANDBY_UPGRADE_H_
#define OCEANBASE_SHARE_OB_STANDBY_UPGRADE_H_

#include "lib/utility/ob_print_utils.h"       // Print*
#include "lib/utility/ob_unify_serialize.h"       // OB_UNIS_VERSION
#include "share/ob_cluster_version.h"
#include "storage/multi_data_source/buffer_ctx.h"

namespace oceanbase
{

namespace share
{

struct ObStandbyUpgrade
{
  OB_UNIS_VERSION(1);
 public:
  ObStandbyUpgrade(): data_version_(0) {}
  ObStandbyUpgrade(const uint64_t data_version): data_version_(data_version) {}
  ~ObStandbyUpgrade() {}
  bool is_valid() const
  {
    return ObClusterVersion::check_version_valid_(data_version_);
  }
  uint64_t get_data_version() const
  {
    return data_version_;
  }

  TO_STRING_KV(K_(data_version));
private:
  uint64_t data_version_;
};

class ObUpgradeDataVersionMDSHelper
{
public:
  static int on_register(
      const char* buf,
      const int64_t len,
      storage::mds::BufferCtx &ctx);
  static int on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      storage::mds::BufferCtx &ctx);
};

}
}

#endif /* !OCEANBASE_SHARE_OB_STANDBY_UPGRADE_H_ */
