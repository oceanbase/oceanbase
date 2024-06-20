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

#pragma once

#include "sql/engine/ob_exec_context.h"
#include "lib/mysqlclient/ob_isql_client.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSMViewStatsMysql
{
public:
  ObDBMSMViewStatsMysql() {}
  virtual ~ObDBMSMViewStatsMysql() {}

#define DECLARE_FUNC(func) \
  static int func(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(set_system_default);
  DECLARE_FUNC(set_mvref_stats_params);
  DECLARE_FUNC(purge_refresh_stats);

#undef DECLARE_FUNC
};

} // namespace pl
} // namespace oceanbase
