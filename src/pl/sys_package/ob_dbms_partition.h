/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
