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

#ifndef OCEANBASE_STORAGE_CACHE_PARTITION_SQL_HELPER_H_
#define OCEANBASE_STORAGE_CACHE_PARTITION_SQL_HELPER_H_

#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{

// alter partition storage cache policy
class ObAlterIncPartPolicyHelper
{
public:
  ObAlterIncPartPolicyHelper(const ObPartitionSchema *ori_table,
                             const ObPartitionSchema *inc_table,
                             const int64_t schema_version,
                             common::ObISQLClient &sql_client)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client) {}
  virtual ~ObAlterIncPartPolicyHelper() {}
  int alter_partition_policy();
private:
  const ObPartitionSchema *ori_table_;
  const ObPartitionSchema *inc_table_;
  int64_t schema_version_;
  common::ObISQLClient &sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObAlterIncPartPolicyHelper);
};

class ObAlterIncSubpartPolicyHelper
{
public:
  ObAlterIncSubpartPolicyHelper(const ObPartitionSchema *ori_table,
                          const ObPartitionSchema *inc_table,
                          const int64_t schema_version,
                          common::ObISQLClient &sql_client)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client) {}
  virtual ~ObAlterIncSubpartPolicyHelper() {}
  int alter_subpartition_policy();
private:
  const ObPartitionSchema *ori_table_;
  const ObPartitionSchema *inc_table_;
  int64_t schema_version_;
  common::ObISQLClient &sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObAlterIncSubpartPolicyHelper);
};
} // namespace schema
} // namespace share
} // namespace oceanbase
#endif 