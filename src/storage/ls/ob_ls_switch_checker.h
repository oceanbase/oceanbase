/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_OB_LS_SWITCH_CHECKER_
#define OCEABASE_STORAGE_OB_LS_SWITCH_CHECKER_
#include <stdint.h>

namespace oceanbase
{
namespace storage
{
class ObLS;

class ObLSSwitchChecker
{
public:
  ObLSSwitchChecker() : ls_(nullptr), record_switch_epoch_(UINT64_MAX) {}
  int check_online(ObLS *ls);
  int check_ls_switch_state(ObLS *ls, bool &is_online);
  int double_check_epoch(bool &is_online) const;
private:
  ObLS *ls_;
  uint64_t record_switch_epoch_;
};

}
}
#endif