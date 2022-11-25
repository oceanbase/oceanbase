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

#include "lib/random/ob_random.h"
#include <stdlib.h>
#include <math.h>

namespace oceanbase
{
namespace common
{
ObRandom::ObRandom()
    : seed_()
{
  seed_[0] = (uint16_t)rand(0, 65535);
  seed_[1] = (uint16_t)rand(0, 65535);
  seed_[2] = (uint16_t)rand(0, 65535);
  seed48(seed_);
}

ObRandom::~ObRandom()
{
}

int64_t ObRandom::rand(const int64_t a, const int64_t b)
{
  struct Wrapper {
    uint16_t v_[3];
  };
  // NOTE: thread local random seed
  RLOCAL(Wrapper, wrapper);
  uint16_t *seed = (&wrapper)->v_;
  if (0 == seed[0] && 0 == seed[1] && 0 == seed[2]) {
    seed[0] = static_cast<uint16_t>(GETTID());
    seed[1] = seed[0];
    seed[2] = seed[1];
    seed48(seed);
  }
  const int64_t r1 = jrand48(seed);
  const int64_t r2 = jrand48(seed);
  int64_t min = a < b ? a : b;
  int64_t max = a < b ? b : a;
  return min + labs((r1 << 32) | r2) % (max - min + 1);
}

int64_t ObRandom::get()
{
  const int64_t r1 = jrand48(seed_);
  const int64_t r2 = jrand48(seed_);
  return ((r1 << 32) | r2);
}

int64_t ObRandom::get(const int64_t a, const int64_t b)
{
  int64_t min = a < b ? a : b;
  int64_t max = a < b ? b : a;
  return min + labs(get()) % (max - min + 1);
}

int32_t ObRandom::get_int32()
{
  return static_cast<int32_t>(jrand48(seed_));
}

} /* namespace common */
} /* namespace oceanbase */
