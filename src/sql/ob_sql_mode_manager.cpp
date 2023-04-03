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

#include "lib/string/ob_string.h"
#include "sql/ob_sql_mode_manager.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

int compatibility_mode2index(const common::ObCompatibilityMode mode, int64_t &index)
{
  static const ObCompatibilityMode modes[] = { OCEANBASE_MODE, MYSQL_MODE, ORACLE_MODE };
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < ARRAYSIZEOF(modes); ++i) {
    if (mode == modes[i]) {
      index = i;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

/*
int ObSQLModeManager::get_compatibility_index(ObCompatibilityMode comp_mode, int64_t &index) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; i < COMPATIBILITY_MODE_COUNT && OB_ENTRY_NOT_EXIST == ret; i++) {
    if (comp_mode == sql_standard_[i].comp_mode_) {
      index = i;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSQLModeManager::set_compatibility_mode(ObCompatibilityMode comp_mode)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  if (OB_FAIL(get_compatibility_index(comp_mode, index))) {
    OB_LOG(WARN, "fail to get sql mode", K(ret), K(comp_mode));
  } else {
    current_mode_index_ = index;
  }
  return ret;
}

ObCompatibilityMode ObSQLModeManager::get_compatibility_mode() const
{
  return sql_standard_[current_mode_index_].comp_mode_;
}

void ObSQLModeManager::reset()
{
  sql_standard_[0].sql_mode_ = DEFAULT_OCEANBASE_MODE;
  sql_standard_[0].comp_mode_ = ObCompatibilityMode::OCEANBASE_MODE;
  sql_standard_[1].sql_mode_ = DEFAULT_MYSQL_MODE;
  sql_standard_[1].comp_mode_ = ObCompatibilityMode::MYSQL_MODE;
  sql_standard_[2].sql_mode_ = DEFAULT_ORACLE_MODE;
  sql_standard_[2].comp_mode_ = ObCompatibilityMode::ORACLE_MODE;
}

ObSQLMode ObSQLModeManager::get_sql_mode() const
{
  return sql_standard_[current_mode_index_].sql_mode_;
}

void ObSQLModeManager::set_sql_mode(ObSQLMode mode)
{
  sql_standard_[current_mode_index_].sql_mode_ = mode;
}
*/
}
}
