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

#ifndef OCEANBASE_SHARE_OB_INDEX_BUILD_STAT_H_
#define OCEANBASE_SHARE_OB_INDEX_BUILD_STAT_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
namespace oceanbase {
namespace share {
class ObIndexBuildStatOperator {
public:
  static int get_snapshot_version(const uint64_t data_table_id, const uint64_t index_table_id,
      common::ObMySQLProxy& proxy, int64_t& snapshot_version);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_INDEX_BUILD_STAT_H_
