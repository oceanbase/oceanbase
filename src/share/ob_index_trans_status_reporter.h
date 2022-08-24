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

#ifndef OB_INDEX_WAIT_STATUS_REPORTER_H_
#define OB_INDEX_WAIT_STATUS_REPORTER_H_

#include "lib/net/ob_addr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase {
namespace share {

struct ObIndexTransStatus {
  ObIndexTransStatus() : server_(), trans_status_(-1), snapshot_version_(-1), frozen_version_(-1), schema_version_(-1)
  {}
  bool is_valid() const
  {
    return server_.is_valid() && snapshot_version_ >= 0 && frozen_version_ >= 0 && schema_version_ > 0;
  }
  void reset()
  {
    server_.reset();
    trans_status_ = -1;
    snapshot_version_ = -1;
    frozen_version_ = -1;
    schema_version_ = -1;
  }
  TO_STRING_KV(K_(server), K_(trans_status), K_(snapshot_version), K_(frozen_version), K_(schema_version));
  common::ObAddr server_;
  int trans_status_;
  int64_t snapshot_version_;
  int64_t frozen_version_;
  int64_t schema_version_;
};

class ObIndexTransStatusReporter {
public:
  enum ServerType {
    INVALID_SERVER_TYPE = 0,
    OB_SERVER = 1,
    ROOT_SERVICE = 2,
  };
  ObIndexTransStatusReporter();
  virtual ~ObIndexTransStatusReporter();
  static int report_wait_trans_status(const uint64_t index_table_id, const int svr_type, const int64_t partition_id,
      const ObIndexTransStatus &status, common::ObISQLClient &mysql_client);
  static int get_wait_trans_status(const uint64_t index_table_id, const int svr_type, const int64_t partition_id,
      common::ObMySQLProxy &sql_proxy, ObIndexTransStatus &status);
  static int delete_wait_trans_status(const uint64_t index_table_id, common::ObISQLClient &mysql_client);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OB_INDEX_WAIT_STATUS_REPORTER_H_
