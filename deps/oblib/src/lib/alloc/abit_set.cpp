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

ABitSet::ABitSet(int nbits, char *buf)
  : nbits_(nbits),
    use_zero_level_(nbits_ >= (1 << 6 << 6)),
    first_level_((uint64_t *)buf),
    n_first_level_(n_first_level(nbits_)),
    second_level_(first_level_ + n_first_level_),
    n_second_level_(n_second_level(nbits_))
{
  abort_unless(nbits_ > (1 << 6));
  abort_unless(nbits_ <= (1 << 6 << 6 << 6));
  clear();
}

// find first least significant bit start from start(includ).
int ABitSet::find_first_significant(int start) const
{
  int ret = 0;
  if (start >= nbits_) {
    // overflow
  } else {
    int maxseg = ((nbits_-1) >> 6) + 1;
    int seg = start >> 6;
    int pos = start & ((1 << 6) - 1);
    ret = myffsl(second_level_[seg], pos);
    if (ret > 0) {
      // found directly in second level
      ret += seg << 6;
    } else if (seg >= maxseg - 1) {
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
      } else if (seg >= maxseg - 1) {
        // not found
      } else if (use_zero_level_) {
        ret = myffsl(zero_level_, seg + 1);
        if (ret <= 0) {
          // not found
        } else {
          int ret2 = myffsl(first_level_[ret-1], 0);
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

// find first most significant bit start from end(includ).
int ABitSet::find_first_most_significant(int end) const
{
  int ret = 0;
  if (end < 0) {
    // underflow
  } else {
    int minseg = 0;
    int seg = end >> 6;
    int pos = end & ((1 << 6) - 1);
    ret = myrffsl(second_level_[seg], pos);
    if (ret > 0) {
      // found directly in second level
      ret += seg << 6;
    } else if (seg <= minseg) {
      // not found
    } else {
      pos = (seg - 1) & ((1 << 6) - 1);
      seg = (seg - 1) >> 6;
      ret = myrffsl(first_level_[seg], pos);
      if (ret > 0) {
        // found directly in first level
        int sseg = (seg << 6) + ret - 1;
        int ret2 = myrffsl(second_level_[sseg], (1 << 6) - 1);
        ret = (sseg << 6) + ret2;
      } else if (seg <= minseg) {
        // not found
      } else if (use_zero_level_) {
        ret = myrffsl(zero_level_, seg - 1);
        if (ret <= 0) {
          // not found
        } else {
          int ret2 = myrffsl(first_level_[ret-1], (1 << 6) - 1);
          if (ret2 <= 0) {
            // not found
          } else {
            int sseg = ((ret - 1) << 6) + ret2 - 1;
            int ret3 = myrffsl(second_level_[sseg], (1 << 6) - 1);
            ret = (sseg << 6) + ret3;
          }
        }
      }
    }
  }
  OB_ASSERT(ret - 1 <= end || 0 == ret);
  return ret - 1;
}
