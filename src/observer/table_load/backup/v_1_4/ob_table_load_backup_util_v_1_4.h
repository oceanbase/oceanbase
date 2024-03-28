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
#include "share/rc/ob_tenant_base.h"
#include "share/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace observer
{

class ObTableLoadBackupUtil_V_1_4
{
public:
  ObTableLoadBackupUtil_V_1_4() {}
  ~ObTableLoadBackupUtil_V_1_4() {}
  static int get_column_ids_from_create_table_sql(const ObString &sql, ObIArray<int64_t> &column_ids);
};

} // namespace observer
} // namespace oceanbase
