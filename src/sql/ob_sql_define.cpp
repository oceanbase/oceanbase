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

#include "ob_sql_define.h"
#include "lib/worker.h"

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
