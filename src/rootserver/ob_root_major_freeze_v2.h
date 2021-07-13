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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_MAJOR_FREEZE_H
#define OCEANBASE_ROOTSERVER_OB_ROOT_MAJOR_FREEZE_H
#include "share/ob_define.h"
namespace oceanbase {
namespace common {
class ObAddr;
}
namespace rootserver {
class ObFreezeInfoManager;
class ObZoneManager;
class ObRootMajorFreezeV2 {
public:
  ObRootMajorFreezeV2(ObFreezeInfoManager& freeze_info)
      : inited_(false), freeze_info_manager_(freeze_info), zone_manager_(NULL)
  {}
  int init(ObZoneManager& zone_manager);
  virtual ~ObRootMajorFreezeV2()
  {}
  int global_major_freeze(int64_t tgt_try_frozen_version, const common::ObAddr& addr, const uint64_t tenant_id,
      const common::ObAddr& rs_addr);

  int launch_major_freeze(const int64_t frozen_version, const common::ObAddr& addr, const uint64_t tenant_id,
      const common::ObAddr& rs_addr);

  int check_frozen_version(const int64_t tgt_try_frozen_version, int64_t& proposal_frozen_version);

  static const int64_t MAX_UNMERGED_VERSION_NUM = 16;

private:
  static const int64_t UNMERGED_VERSION_LIMIT = 1;

private:
  bool inited_;
  ObFreezeInfoManager& freeze_info_manager_;
  ObZoneManager* zone_manager_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootMajorFreezeV2);
};
}  // namespace rootserver
}  // namespace oceanbase

#endif
