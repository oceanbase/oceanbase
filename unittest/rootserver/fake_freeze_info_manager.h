/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "rootserver/freeze/ob_major_merge_info_manager.h"

namespace oceanbase
{
namespace rootserver
{
class FakeFreezeInfoManager : public ObMajorMergeInfoManager
{
public:
  FakeFreezeInfoManager();
  virtual ~FakeFreezeInfoManager() {}

  void set_leader_cluster(bool is_primary_cluster) { is_primary_cluster_ = is_primary_cluster; }
  bool is_primary_cluster() const;

private:
  bool is_primary_cluster_;
};
} // namespace rootserver
} // namespace oceanbase
