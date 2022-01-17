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

#ifndef OB_INDEX_STATUS_REPORTER_H_
#define OB_INDEX_STATUS_REPORTER_H_

#include "lib/net/ob_addr.h"
#include "common/ob_role.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {
namespace share {
class ObIndexStatusTableOperator {
public:
  struct ObBuildIndexStatus {
  public:
    ObBuildIndexStatus()
        : index_status_(share::schema::ObIndexStatus::INDEX_STATUS_NOT_FOUND),
          ret_code_(common::OB_SUCCESS),
          role_(common::FOLLOWER)
    {}
    bool operator==(const ObBuildIndexStatus& other) const;
    bool operator!=(const ObBuildIndexStatus& other) const;
    bool is_valid() const;
    TO_STRING_KV(K_(index_status), K_(ret_code), K_(role));
    share::schema::ObIndexStatus index_status_;
    int ret_code_;
    common::ObRole role_;
  };
  ObIndexStatusTableOperator();
  virtual ~ObIndexStatusTableOperator();
  static int get_build_index_status(const uint64_t index_table_id, const int64_t partition_id,
      const common::ObAddr& addr, common::ObMySQLProxy& sql_proxy, ObBuildIndexStatus& status);
  static int check_final_status_reported(
      const uint64_t index_table_id, const int64_t partition_id, common::ObMySQLProxy& sql_proxy, bool& is_reported);
  static int report_build_index_status(const uint64_t index_table_id, const int64_t partition_id,
      const common::ObAddr& addr, const ObBuildIndexStatus& status, common::ObMySQLProxy& sql_proxy);
};
}  // end namespace share
}  // end namespace oceanbase

#endif  // OB_INDEX_STATUS_REPORTER_H_
