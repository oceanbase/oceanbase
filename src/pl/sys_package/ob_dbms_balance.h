/**
 * Copyright (c) 2024 Beijing OceanBase Technology Co., Ltd.
 * OceanBase CE is licensed under Mulan PubL v1.
 * You can use this software according to the terms and conditions of the Mulan PubL v1.
 * You may obtain a copy of Mulan PubL v1 at:
 *         http://license.coscl.org.cn/MulanPubL-1.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v1 for more details.
 */
#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_BALANCE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_BALANCE_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{
class ObDBMSBalance
{
public:
  ObDBMSBalance() {}
  virtual ~ObDBMSBalance() {}

  static int trigger_partition_balance(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int set_balance_weight(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int clear_balance_weight(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

private:
  static int pre_check_for_balance_weight_(const uint64_t tenant_id, const char *op_str);
  static int get_id_by_name_(
      const uint64_t tenant_id,
      const common::ObString &database_name,
      const common::ObString &table_name,
      const common::ObString &partition_name,
      const common::ObString &subpartition_name,
      ObObjectID &table_id,
      ObObjectID &part_id,
      ObObjectID &subpart_id);

};
} // end of pl
} // end of oceanbase
#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_BALANCE_H_