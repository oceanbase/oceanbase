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

#ifndef OCEANBASE_SHARE_OB_GLOBAL_MERGE_TABLE_OPERATOR_
#define OCEANBASE_SHARE_OB_GLOBAL_MERGE_TABLE_OPERATOR_

#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "common/ob_zone.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
}
namespace share
{
struct ObGlobalMergeInfo;

// CRUD operation to __all_merge_info table
class ObGlobalMergeTableOperator
{
public:
  static int load_global_merge_info(common::ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    share::ObGlobalMergeInfo &info,
                                    const bool print_sql = false);
  static int insert_global_merge_info(common::ObISQLClient &sql_client,
                                      const uint64_t tenant_id,
                                      const share::ObGlobalMergeInfo &info);
  // According to each filed's <need_update_> to decide whether need to be updated
  static int update_partial_global_merge_info(common::ObISQLClient &sql_client,
                                              const uint64_t tenant_id,
                                              const share::ObGlobalMergeInfo &info);

private:
  static int check_scn_revert(common::ObISQLClient &sql_client,
                              const uint64_t tenant_id,
                              const share::ObGlobalMergeInfo &info);
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_GLOBAL_MERGE_TABLE_OPERATOR_
