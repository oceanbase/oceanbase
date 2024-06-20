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

#include "lib/number/ob_number_v2.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace storage
{
class ObMViewExecutorUtil
{
public:
  static int number_to_int64(const number::ObNumber &number, int64_t &int64);
  static int number_to_uint64(const number::ObNumber &number, uint64_t &uint64);

  static int check_min_data_version(const uint64_t tenant_id, const uint64_t min_data_version,
                                    const char *errmsg);
  static int split_table_list(const common::ObString &table_list,
                              common::ObIArray<common::ObString> &tables);
  static int resolve_table_name(const common::ObCollationType cs_type,
                                const ObNameCaseMode case_mode, const bool is_oracle_mode,
                                const common::ObString &name, common::ObString &database_name,
                                common::ObString &table_name);
  static void upper_db_table_name(const ObNameCaseMode case_mode, const bool is_oracle_mode,
                                  common::ObString &name);

  static int to_refresh_method(const char c, share::schema::ObMVRefreshMethod &refresh_method);
  static int to_collection_level(const common::ObString &str,
                                 share::schema::ObMVRefreshStatsCollectionLevel &collection_level);

  static int generate_refresh_id(const uint64_t tenant_id, int64_t &refresh_id);

  static bool is_mview_refresh_retry_ret_code(int ret_code);
};

} // namespace storage
} // namespace oceanbase
