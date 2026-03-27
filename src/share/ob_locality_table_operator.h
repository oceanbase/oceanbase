/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_LOCALITY_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_LOCALITY_TABLE_OPERATOR_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_locality_info.h"
#include "share/ob_server_locality_cache.h"
namespace oceanbase
{
namespace share
{
class ObLocalityTableOperator
{
public:
  int load_region(const common::ObAddr &addr, const bool &is_self_cluster,
                  common::ObISQLClient &sql_client,
                  ObLocalityInfo &locality_info, 
                  ObServerLocalityCache &server_locality_cache);
};
}//end namespace share
}//end namespace oceanbase
#endif
