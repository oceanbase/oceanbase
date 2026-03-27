/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PARTITION_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PARTITION_H_

#include "sql/ob_sql_define.h"

namespace oceanbase
{
namespace pl
{
class ObPLExecCtx;
class ObDBMSPartition
{
public:
  static int manage_dynamic_partition(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
private:
  static int get_and_check_params_(const sql::ParamStore &params, common::ObString &precreate_time_str, common::ObArray<common::ObString> &time_unit_strs);
};

} // end of pl
} // end of oceanbase
#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PARTITION_H_ */
