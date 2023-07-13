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

#ifndef _OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_RESOURCE_MANANGER_PL_H_
#define _OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_RESOURCE_MANANGER_PL_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObPlDBMSResourceManager
{
public:
  ObPlDBMSResourceManager() {}
  virtual ~ObPlDBMSResourceManager() {}
public:
  static int create_plan(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
  static int delete_plan(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
  static int create_consumer_group(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
  static int delete_consumer_group(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
  static int create_plan_directive(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
  static int delete_plan_directive(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
  static int update_plan_directive(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
  static int set_consumer_group_mapping(
      sql::ObExecContext &ctx,
      sql::ParamStore &params,
      common::ObObj &result);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPlDBMSResourceManager);
};

}
}
#endif /* _OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_RESOURCE_MANANGER_PL_H_ */
//// end of header file
