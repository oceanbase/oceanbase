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
  int check_ls_switch_state(ObLS *ls, bool &online_state);
  int double_check_epoch() const;
private:
  ObLS *ls_;
  uint64_t record_switch_epoch_;
};

}
}
#endif