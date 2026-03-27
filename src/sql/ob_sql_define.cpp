/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_sql_define.h"

namespace oceanbase
{
namespace sql
{
DEFINE_ENUM_FUNC(ObPQDistributeMethod::Type, type, PQ_DIST_METHOD_DEF, ObPQDistributeMethod::);

ObOrderDirection default_asc_direction()
{
  return lib::is_oracle_mode() ? NULLS_LAST_ASC : NULLS_FIRST_ASC;
}

ObOrderDirection default_desc_direction()
{
  return lib::is_oracle_mode() ? NULLS_FIRST_DESC : NULLS_LAST_DESC;
}

} // namespace sql
} // namespace oceanbase
