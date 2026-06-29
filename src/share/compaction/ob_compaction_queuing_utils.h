// Copyright (c) 2026 OceanBase
// SPDX-License-Identifier: Apache-2.0
// owner: ouyanghongrong.oyh

#ifndef OCEANBASE_SHARE_OB_COMPACTION_QUEUING_UTILS_H_
#define OCEANBASE_SHARE_OB_COMPACTION_QUEUING_UTILS_H_

#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace share
{

struct ObCompactionQueuingUtil final
{
public:
  static void assign_random_table_mode_for_test(
      const bool user_specified_table_mode,
      const bool is_external_table,
      const bool is_oracle_temp_table,
      const uint64_t tenant_id,
      schema::ObTableMode &table_mode,
      const schema::ObTableSchema &table_schema);

private:
  static schema::ObTableModeFlag pick_random_table_mode_flag_(const int64_t rand_val);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_COMPACTION_QUEUING_UTILS_H_
