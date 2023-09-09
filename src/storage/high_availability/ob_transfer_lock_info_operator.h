
/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OB_TRANSFER_TASK_LOCK_INFO_OPERATOR_H
#define OCEANBASE_OB_TRANSFER_TASK_LOCK_INFO_OPERATOR_H

#include "share/ob_dml_sql_splicer.h"
#include "share/ob_ls_id.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "ob_transfer_struct.h"

namespace oceanbase {
namespace storage {

class ObTransferLockInfoOperator final {
public:
  ObTransferLockInfoOperator() = default;
  ~ObTransferLockInfoOperator() = default;

  static int insert(const ObTransferTaskLockInfo &lock_info, const int32_t group_id, common::ObISQLClient &sql_proxy);
  static int remove(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t task_id,
      const ObTransferLockStatus &status, const int32_t group_id, common::ObISQLClient &sql_proxy);
  static int get(const ObTransferLockInfoRowKey &row_key, const int64_t task_id, const ObTransferLockStatus &status,
      const bool for_update, const int32_t group_id, ObTransferTaskLockInfo &lock_info, common::ObISQLClient &sql_proxy);
  static int fetch_all(common::ObISQLClient &sql_proxy, const uint64_t tenant_id, const int32_t group_id,
      common::ObArray<ObTransferTaskLockInfo> &lock_infos);

private:
  static int fill_dml_splicer_(const ObTransferTaskLockInfo &lock_info, share::ObDMLSqlSplicer &dml_splicer);
  static int construct_result_(common::sqlclient::ObMySQLResult &res, ObTransferTaskLockInfo &lock_info);
  static int parse_sql_result_(common::sqlclient::ObMySQLResult &res, ObTransferTaskLockInfo &lock_info);
  static int parse_sql_results_(common::sqlclient::ObMySQLResult &res, common::ObArray<ObTransferTaskLockInfo> &lock_infos);
};

}  // namespace storage
}  // namespace oceanbase

#endif
