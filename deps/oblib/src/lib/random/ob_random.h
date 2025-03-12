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

#ifndef OB_RANDOM_H_
#define OB_RANDOM_H_

#include <stdint.h>
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

class ObRandom
{
public:
  ObRandom();
  virtual ~ObRandom();
  inline void gen_seed();
  //get a random int64_t number in [min(a,b), max(a,b)]
  void seed(const uint64_t seed);
  static int64_t rand(const int64_t a, const int64_t b);
  //get a random int64_t number
  int64_t get();
  //get a random int64_t number in [min(a,b), max(a,b)]
  int64_t get(const int64_t a, const int64_t b);
  //get a random int32_t number
  int32_t get_int32();
private:
  uint16_t seed_[3];
  bool is_inited;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRandom);
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_RANDOM_H_ */
