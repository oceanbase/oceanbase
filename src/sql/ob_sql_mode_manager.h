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

#ifndef OCEANBASE_SQL_OB_SQL_MODE_MANAGER_H
#define OCEANBASE_SQL_OB_SQL_MODE_MANAGER_H

#include "common/sql_mode/ob_sql_mode.h"
namespace oceanbase
{
namespace sql
{

// Get compatibility mode index of ObSQLModeManager, used to be compatible with old version
// which ObSQLModeManager is used.
extern int compatibility_mode2index(const common::ObCompatibilityMode mode, int64_t &index);

//
// ObSQLModeManager try to set default sql mode when compatibility mode switched, but hard to use.
// When loading system variable sql mode is set before compatibility mode, sql mode lost when
// compatibility mode switched.
//

/*
class ObSQLModePair
{
public:
  ObSQLModePair() : comp_mode_(common::ObCompatibilityMode::OCEANBASE_MODE), sql_mode_(0)
  {}
  ~ObSQLModePair()
  {}
public:
  common::ObCompatibilityMode comp_mode_;
  ObSQLMode sql_mode_;
};

class ObSQLModeManager
{
public:
  static const int64_t COMPATIBILITY_MODE_COUNT = 3;
  ObSQLModeManager():current_mode_index_(1)
  {
    reset();
  }
  ~ObSQLModeManager(){}
  int set_compatibility_mode(common::ObCompatibilityMode comp_mode);
  common::ObCompatibilityMode get_compatibility_mode() const;
  void reset();
  void  set_sql_mode(ObSQLMode mode);
  ObSQLMode get_sql_mode() const;
private:
  int get_compatibility_index(common::ObCompatibilityMode comp_mode, int64_t &index) const;
public:
  ObSQLModePair sql_standard_[COMPATIBILITY_MODE_COUNT];
  int64_t current_mode_index_;
};
*/
}
}
#endif
