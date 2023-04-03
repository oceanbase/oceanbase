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
