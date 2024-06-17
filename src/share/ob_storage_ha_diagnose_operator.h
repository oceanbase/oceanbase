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

#ifndef OCEANBASE_SHARE_HA_DIAGNOSE_OPERATOR_
#define OCEANBASE_SHARE_HA_DIAGNOSE_OPERATOR_

#include "share/ob_storage_ha_diagnose_struct.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;

namespace sqlclient
{
class ObMySQLResult;
}
}

namespace share
{
class ObDMLSqlSplicer;

class ObStorageHADiagOperator final
{
public:
  ObStorageHADiagOperator();
  ~ObStorageHADiagOperator() {}

  int init();

  void reset();
  int get_batch_row_keys(
          common::ObISQLClient &sql_proxy,
          const uint64_t tenant_id,
          const ObStorageHADiagModule &module,
          const int64_t last_end_timestamp,
          ObIArray<int64_t> &timestamp_array) const;

  int insert_row(
          common::ObISQLClient &sql_proxy,
          const uint64_t tenant_id,
          const uint64_t report_tenant_id,
          const ObStorageHADiagInfo &info_row,
          const ObStorageHADiagModule &module);
  int do_batch_delete(
          common::ObISQLClient &sql_proxy,
          const uint64_t tenant_id,
          const ObStorageHADiagModule &module,
          const ObIArray<int64_t> &timestamp_array,
          const int64_t delete_timestamp,
          int64_t &delete_index) const;
  int check_transfer_task_exist(
          common::ObISQLClient &sql_proxy,
          const uint64_t tenant_id,
          const share::ObTransferTaskID &task_id,
          int64_t &result_count) const;

private:
  int fill_dml_(
          const uint64_t tenant_id,
          const uint64_t report_tenant_id,
          const ObStorageHADiagInfo &input_info,
          ObDMLSqlSplicer &dml);

  int get_table_name_(const ObStorageHADiagModule &module, const char *&table_name) const;
  int gen_event_ts_(int64_t &event_ts);
  int parse_result_(
    common::sqlclient::ObMySQLResult &result,
    int64_t &result_count) const;

private:
  bool is_inited_;
  int64_t last_event_ts_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageHADiagOperator);
};

} // end namespace share
} // end namespace oceanbase
#endif
