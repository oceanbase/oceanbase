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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PLAN_CACHE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PLAN_CACHE_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSPlanCache
{
public:
  ObDBMSPlanCache() {}
  virtual ~ObDBMSPlanCache() {}
public:
  // purge single plan via sql_id
  // param format like following:
  // DBMS_PLAN_CACHE.PURGE (
  // SQL_ID        VARCHAR2 NOT NULL,
  // SCHEMA        VARCHAR2 DEFAULT Null
  // GLOBAL        BOOLEAN  DEFAULT FALSE);
  static int purge_single_plan_by_sql_id(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  // purge all plans
  // param format like following:
  // DBMS_PLAN_CACHE.PURGE_ALL (
  // GLOBAL        BOOLEAN  DEFAULT FALSE);
  static int purge_all_plan(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_PLAN_CACHE_H_ */
