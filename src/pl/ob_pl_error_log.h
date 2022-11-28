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

#ifndef SRC_PL_ERROR_LOG_H
#define SRC_PL_ERROR_LOG_H

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace common
{
class ObString;
}

namespace pl
{
using namespace common;
using namespace sql;
using namespace share;
using namespace number;

class PlPackageErrorLog final
{
public:
  static int create_error_log(sql::ObExecContext &ctx,
                   sql::ParamStore &params,
                   common::ObObj &result);
  static int resolve_column_info(ObString &column_define, const ObColumnSchemaV2 *column_schema);
  static int resolve_table_name(ObString &base_table_name, ObIAllocator &allocator, bool &small_letter);
};

} // namespace pl
} // namespace oceanbase

#endif
