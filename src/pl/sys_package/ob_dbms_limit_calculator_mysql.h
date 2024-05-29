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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_LIMIT_CALCULATOR_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_LIMIT_CALCULATOR_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace share
{
struct ObUserResourceCalculateArg;
}
namespace obrpc
{
struct ObGetTenantResArg;
struct ObTenantLogicalRes;
}
namespace pl
{

class ObDBMSLimitCalculator
{
public:
  static int phy_res_calculate_by_logic_res(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
  static int phy_res_calculate_by_unit(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
  static int phy_res_calculate_by_standby_tenant(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
private:
  static int cal_tenant_logical_res_for_standby_(
      const uint64_t tenant_id,
      const int64_t standby_unit_num,
      ObUserResourceCalculateArg &arg);
  static int get_tenant_resource_server_for_calc_(
      const uint64_t tenant_id,
      common::ObIArray<ObAddr> &servers);
  static int get_server_resource_info_(
      const common::ObIArray<ObAddr> &servers,
      const uint64_t tenant_id,
      common::ObIArray<obrpc::ObTenantLogicalRes> &res);
  static int check_server_resource_(
      const uint64_t tenant_id,
      common::ObIArray<obrpc::ObTenantLogicalRes> &res);
  static int parse_dict_like_args_(
      const char* ptr,
      share::ObUserResourceCalculateArg &arg);
  static int get_json_result_(
      const ObMinPhyResourceResult &res,
      char *buf,
      const int64_t buf_len,
      int64_t &pos);
  static int get_json_result_(
      const int64_t tenant_id,
      const ObAddr &addr,
      const ObMinPhyResourceResult &res,
      char *buf,
      const int64_t buf_len,
      int64_t &pos);
  static int get_max_ls_count_of_server_(
      const uint64_t tenant_id,
      int64_t &ls_count);
  static int get_max_value_of_logical_res_(
      const int64_t &logical_type,
      const int64_t &server_cnt,
      const common::ObIArray<obrpc::ObTenantLogicalRes> &res,
      int64_t &max_logical_value);
};

} // namespace pl
} // namespace oceanbase

#endif
