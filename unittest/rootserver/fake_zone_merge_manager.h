/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  int set_global_merge_info(const share::ObGlobalMergeInfo &global_merge_info);
};
} // namespace rootserver
} // namespace oceanbase
