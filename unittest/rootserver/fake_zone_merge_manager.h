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

#include "rootserver/freeze/ob_zone_merge_manager.h"

namespace oceanbase
{
namespace rootserver
{
class FakeZoneMergeManager : public ObZoneMergeManager
{
public:
  FakeZoneMergeManager() {}
  virtual ~FakeZoneMergeManager() {}

  void set_is_loaded(const bool is_loaded) { is_loaded_ = is_loaded; }
  int add_zone_merge_info(const share::ObZoneMergeInfo& zone_merge_info);
  int update_zone_merge_info(const share::ObZoneMergeInfo& zone_merge_info);
  int set_global_merge_info(const share::ObGlobalMergeInfo &global_merge_info);

};
} // namespace rootserver
} // namespace oceanbase