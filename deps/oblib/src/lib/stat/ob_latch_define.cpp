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

#include "lib/stat/ob_latch_define.h"
namespace oceanbase
{
namespace common
{

#define LATCH_DEF_true(def, id, name, policy, max_spin_cnt, max_yield_cnt) \
    {id, name, ObLatchPolicy::policy, max_spin_cnt, max_yield_cnt},
#define LATCH_DEF_false(def, id, name, policy, max_spin_cnt, max_yield_cnt)

const ObLatchDesc OB_LATCHES[] __attribute__ ((init_priority(102))) = {
#define LATCH_DEF(def, id, name, policy, max_spin_cnt, max_yield_cnt, enable) \
LATCH_DEF_##enable(def, id, name, policy, max_spin_cnt, max_yield_cnt)
#include "lib/stat/ob_latch_define.h"
#undef LATCH_DEF
};
#undef LATCH_DEF_true
#undef LATCH_DEF_false

static_assert(ARRAYSIZEOF(OB_LATCHES) == 240, "DO NOT delete latch defination");
static_assert(ObLatchIds::LATCH_END == ARRAYSIZEOF(OB_LATCHES) - 1, "update id of LATCH_END before adding your defination");

}
}
