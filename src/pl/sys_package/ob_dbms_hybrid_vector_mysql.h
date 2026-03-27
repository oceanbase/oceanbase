/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "pl/ob_pl.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSHybridVectorMySql
{
public:
  ObDBMSHybridVectorMySql() {}
  virtual ~ObDBMSHybridVectorMySql() {}

#define DECLARE_FUNC(func) \
  static int func(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  DECLARE_FUNC(search);
  DECLARE_FUNC(get_sql);
#undef DECLARE_FUNC
};

} // namespace pl
} // namespace oceanbase
