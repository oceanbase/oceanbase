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

#ifndef OCEANBASE_SQL_TABLE_FORMAT_JDBC_OB_JDBC_FILTER_UTILS_H_
#define OCEANBASE_SQL_TABLE_FORMAT_JDBC_OB_JDBC_FILTER_UTILS_H_

#include "common/object/ob_object.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;

namespace jdbc
{

class ObJdbcFilterUtils
{
public:
  static int check_filter_requires_local(const ObRawExpr *filter,
                                         const common::ParamStore &params,
                                         bool &requires_local);

private:
  static bool is_local_filter_type(const common::ObObjType type);
};

} // namespace jdbc
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_TABLE_FORMAT_JDBC_OB_JDBC_FILTER_UTILS_H_
