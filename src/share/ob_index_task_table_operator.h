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

#ifndef OB_INDEX_TASK_TABLE_OPERATOR_H_
#define OB_INDEX_TASK_TABLE_OPERATOR_H_

#include "lib/net/ob_addr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase {
namespace share {
class ObIndexTaskTableOperator {

public:
  ObIndexTaskTableOperator();
  virtual ~ObIndexTaskTableOperator();
  static int get_build_index_server(
      const uint64_t index_table_id, const int64_t partition_id, common::ObMySQLProxy& sql_proxy, common::ObAddr& addr);
  static int generate_new_build_index_record(const uint64_t index_table_id, const int64_t partition_id,
      const common::ObAddr& addr, const int64_t snapshot_version, const int64_t frozen_version,
      common::ObMySQLProxy& sql_proxy);
  static int get_build_index_version(const uint64_t index_id, const int64_t partition_id, const common::ObAddr& addr,
      common::ObMySQLProxy& sql_proxy, int64_t& snapshot_version, int64_t& frozen_version);
  static int update_frozen_version(const uint64_t index_id, const int64_t partition_id, const common::ObAddr& addr,
      const int64_t frozen_version, common::ObMySQLProxy& sql_proxy);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OB_INDEX_TASK_TABLE_OPERATOR_H_
