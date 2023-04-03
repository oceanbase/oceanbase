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

#ifndef OCEANBASE_COMMON_OB_GTS_INFO_H_
#define OCEANBASE_COMMON_OB_GTS_INFO_H_

#include "common/ob_region.h"
#include "common/ob_member_list.h"
#include "share/ob_gts_name.h"

namespace oceanbase
{
namespace common
{
class ObGtsInfo
{
public:
  ObGtsInfo();
  ~ObGtsInfo() {}
  void reset();
  bool is_valid() const;
  int assign(const ObGtsInfo &that);
public:
  uint64_t gts_id_;
  common::ObGtsName gts_name_;
  common::ObRegion region_;
  int64_t epoch_id_;
  common::ObMemberList member_list_;
  common::ObAddr standby_;
  int64_t heartbeat_ts_;

  TO_STRING_KV(K(gts_id_), K(gts_name_), K(region_), K(epoch_id_),
               K(member_list_), K(standby_), K(heartbeat_ts_));
};

class ObGtsTenantInfo
{
public:
  ObGtsTenantInfo();
  ~ObGtsTenantInfo() {}
  void reset();
  bool is_valid() const;
public:
  uint64_t gts_id_;
  uint64_t tenant_id_;
  common::ObMemberList member_list_;

  TO_STRING_KV(K(gts_id_), K(tenant_id_), K(member_list_));
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_GTS_INFO_H_
