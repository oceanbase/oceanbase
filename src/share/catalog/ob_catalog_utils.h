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

#ifndef OB_CATALOG_UTILS_H
#define OB_CATALOG_UTILS_H

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{

class ObCatalogUtils
{
public:
  // 使用的名字来源于 sql，没有处理过
  static bool is_internal_catalog_name(const common::ObString &name_from_sql, const ObNameCaseMode &case_mode);
  // 使用的名字来源于 CatalogSchema，名字大小写已经转换好了
  static bool is_internal_catalog_name(const common::ObString &name_from_meta);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogUtils);
};

} // namespace share
} // namespace oceanbase

#endif // OB_CATALOG_UTILS_H
