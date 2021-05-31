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

#include "lib/alloc/abit_set.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;

// find first least significant bit start from start(includ).
int ABitSet::find_first_significant(int start) const
{
  int ret = 0;
  if (start >= nbits_) {
    // overflow
  } else {
    int maxseg = ((nbits_ - 1) >> 6) + 1;
    int seg = start >> 6;
    int pos = start & ((1 << 6) - 1);
    ret = myffsl(second_level_[seg], pos);
    if (ret > 0) {
      // found directly in second level
      ret += seg << 6;
    } else if (seg >= maxseg) {
      // not found
    } else {
      maxseg = ((maxseg - 1) >> 6) + 1;
      pos = (seg + 1) & ((1 << 6) - 1);
      seg = (seg + 1) >> 6;
      ret = myffsl(first_level_[seg], pos);
      if (ret > 0) {
        // found directly in first level
        int sseg = (seg << 6) + ret - 1;
        int ret2 = myffsl(second_level_[sseg], 0);
        ret = (sseg << 6) + ret2;
      } else if (seg >= maxseg) {
        // not found
      } else if (use_zero_level_) {
        ret = myffsl(zero_level_, seg + 1);
        if (ret <= 0) {
          // not found
        } else {
          int ret2 = myffsl(first_level_[ret - 1], 0);
          if (ret2 <= 0) {
            // not found
          } else {
            int sseg = ((ret - 1) << 6) + ret2 - 1;
            int ret3 = myffsl(second_level_[sseg], 0);
            ret = (sseg << 6) + ret3;
          }
        }
      }
    }
  }
  OB_ASSERT(ret - 1 >= start || 0 == ret);
  return ret - 1;
}