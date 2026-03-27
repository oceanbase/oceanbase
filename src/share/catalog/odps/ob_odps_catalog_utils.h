/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __SHARE_OB_ODPS_CATALOG_UTILS_H__
#define __SHARE_OB_ODPS_CATALOG_UTILS_H__

#ifdef OB_BUILD_CPP_ODPS
#include <odps/odps_api.h>
#endif

namespace oceanbase
{
namespace sql
{
class ObODPSGeneralFormat;
}

namespace share
{
class ObODPSCatalogProperties;

class ObODPSCatalogUtils
{
public:
#ifdef OB_BUILD_CPP_ODPS
  static int create_odps_conf(const ObODPSCatalogProperties &odps_format, apsara::odps::sdk::Configuration &conf);
#endif
};

} // namespace share
} // namespace oceanbase

#endif // __SHARE_OB_ODPS_CATALOG_UTILS_H__