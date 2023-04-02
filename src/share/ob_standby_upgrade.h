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

#ifndef OCEANBASE_SHARE_OB_STANDBY_UPGRADE_H_
#define OCEANBASE_SHARE_OB_STANDBY_UPGRADE_H_

#include "lib/utility/ob_print_utils.h"       // Print*
#include "lib/utility/ob_unify_serialize.h"       // OB_UNIS_VERSION
#include "share/ob_cluster_version.h"

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

}
}

#endif /* !OCEANBASE_SHARE_OB_STANDBY_UPGRADE_H_ */
