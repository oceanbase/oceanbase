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

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_upgrade.h"
#include "pl/ob_pl_package_manager.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace pl
{

int ObDBMSUpgrade::upgrade_single(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString package_name;
  ObCompatibilityMode mode = lib::is_oracle_mode() ? ObCompatibilityMode::ORACLE_MODE
                                                   : ObCompatibilityMode::MYSQL_MODE;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_LIKELY(1 == params.count()));
  OV (params.at(0).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(0).get_string(package_name));
  OV (!package_name.empty(), OB_INVALID_ARGUMENT);
  OZ (ObPLPackageManager::load_sys_package(*ctx.get_sql_proxy(), package_name, mode));
  return ret;
}

int ObDBMSUpgrade::upgrade_all(
  sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObCompatibilityMode mode = lib::is_oracle_mode() ? ObCompatibilityMode::ORACLE_MODE
                                                   : ObCompatibilityMode::MYSQL_MODE;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_LIKELY(0 == params.count()));
  OZ (ObPLPackageManager::load_all_common_sys_package(*ctx.get_sql_proxy(), mode));
  return ret;
}

} // end of pl
} // end oceanbase
