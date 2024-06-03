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
#define USING_LOG_PREFIX SHARE

#include "ob_balance_define.h"

namespace oceanbase
{
namespace share
{
bool need_balance_table(const schema::ObSimpleTableSchemaV2 &table_schema)
{
  bool need_balance = false;
  const char* table_type_str = NULL;
  need_balance = check_if_need_balance_table(table_schema, table_type_str);
  return need_balance;
}

bool check_if_need_balance_table(
    const schema::ObSimpleTableSchemaV2 &table_schema,
    const char *&table_type_str)
{
  bool need_balance = false;
  if (table_schema.is_duplicate_table()) {
    table_type_str = "DUPLICATE TABLE";
  } else if (table_schema.is_index_table() && !table_schema.is_global_index_table()) {
    table_type_str = "LOCAL INDEX";
  } else {
    table_type_str = ob_table_type_str(table_schema.get_table_type());
  }
  need_balance = table_schema.is_user_table()
      || table_schema.is_global_index_table()
      || table_schema.is_tmp_table();
  return need_balance;
}

ObBalanceStrategy &ObBalanceStrategy::operator=(const STRATEGY &val)
{
  val_ = val;
  return *this;
}

const char* ObBalanceStrategy::BALANCE_STRATEGY_STR_ARRAY[MAX_STRATEGY] =
{
  "LS balance by migrate",
  "LS balance by alter",
  "LS balance by expand",
  "LS balance by shrink",
  "manual transfer partition",
  "partition balance", // compatible with old versions
  "partition attribution alignment",
  "intragroup partition count balance",
  "intergroup partition count balance",
  "partition disk balance",
};

const char *ObBalanceStrategy::str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(BALANCE_STRATEGY_STR_ARRAY) == static_cast<int64_t>(MAX_STRATEGY),
      "BALANCE_STRATEGY_STR_ARRAY size mismatch STRATEGY count");
  OB_ASSERT(val_ >= LB_MIGRATE && val_ < MAX_STRATEGY);
  return BALANCE_STRATEGY_STR_ARRAY[val_];
}

int ObBalanceStrategy::parse_from_str(const ObString &str)
{
  int ret = OB_SUCCESS;
  val_ = MAX_STRATEGY;
  for (int64_t i = LB_MIGRATE; i < ARRAYSIZEOF(BALANCE_STRATEGY_STR_ARRAY); ++i) {
    if (0 == str.case_compare(BALANCE_STRATEGY_STR_ARRAY[i])) {
      val_ = static_cast<STRATEGY>(i);
      break;
    }
  }
  if (MAX_STRATEGY == val_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid str", KR(ret), K(str), K(val_));
  }
  return ret;
}

bool ObBalanceStrategy::can_be_next_partition_balance_strategy(const ObBalanceStrategy &old_strategy) const
{
  return is_partition_balance_strategy()
      && old_strategy.is_partition_balance_strategy()
      && !is_partition_balance_compatible_strategy()
      && !old_strategy.is_partition_balance_compatible_strategy()
      && val_ > old_strategy.val_;
}

}
}
