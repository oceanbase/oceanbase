/**
 * Copyright (c) 2023 OceanBase
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

#include "pl/ob_pl.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSVectorMySql
{
public:
  ObDBMSVectorMySql() {}
  virtual ~ObDBMSVectorMySql() {}

#define DECLARE_FUNC(func) \
  static int func(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(refresh_index);
  DECLARE_FUNC(rebuild_index);

#undef DECLARE_FUNC
};

} // namespace pl
} // namespace oceanbase