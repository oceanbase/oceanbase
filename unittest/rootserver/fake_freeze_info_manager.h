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
