/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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