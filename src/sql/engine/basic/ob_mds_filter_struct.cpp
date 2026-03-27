/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_mds_filter_struct.h"
#include "ob_ttl_filter_struct.h"
#include "ob_base_version_filter_struct.h"
#include "ob_truncate_filter_struct.h"

namespace oceanbase
{
namespace sql
{

ObIMDSFilterExecutor *ObIMDSFilterExecutor::cast(ObPushdownFilterExecutor *executor)
{
  ObIMDSFilterExecutor *mds_executor = nullptr;

  // don't use reinterpret_cast here, this is multi-inheritance class
  if (OB_NOT_NULL(executor)) {
    switch (executor->get_type()) {
      case PushdownExecutorType::TTL_WHITE_FILTER_EXECUTOR:
        mds_executor = static_cast<ObTTLWhiteFilterExecutor *>(executor);
        break;
      case PushdownExecutorType::BASE_VERSION_FILTER_EXECUTOR:
        mds_executor = static_cast<ObBaseVersionFilterExecutor *>(executor);
        break;
      case PushdownExecutorType::TRUNCATE_WHITE_FILTER_EXECUTOR:
        mds_executor = static_cast<ObTruncateWhiteFilterExecutor *>(executor);
        break;
      case PushdownExecutorType::TRUNCATE_BLACK_FILTER_EXECUTOR:
        mds_executor = static_cast<ObTruncateBlackFilterExecutor *>(executor);
        break;
      default:
        break;
    }
  }

  return mds_executor;
}


}
} // namespace oceanbase
