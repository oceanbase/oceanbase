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

#ifndef _OB_MYSQL_RANDOM_H_
#define _OB_MYSQL_RANDOM_H_

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "lib/ob_define.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{
class ObMysqlRandom
{
public:
  ObMysqlRandom() { MEMSET(this, 0, sizeof(*this)); }

  void init(const uint64_t seed1, const uint64_t seed2);
  int create_random_string(char *buf, const int64_t length);
  double get_double();
  uint64_t get_uint64();
  bool is_inited() const { return is_inited_;}

public:
  bool is_inited_;
  uint64_t seed1_;
  uint64_t seed2_;
  uint64_t max_value_;
  double   max_value_double_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlRandom);
};

/*
  New (MySQL 3.21+) random generation structure initialization
  SYNOPSIS
    randominit()
    rand_st    OUT  Structure to initialize
    seed1      IN   First initialization parameter
    seed2      IN   Second initialization parameter
*/
inline void ObMysqlRandom::init(const uint64_t seed1, const uint64_t seed2)
{
  max_value_ = 0x3FFFFFFF;
  max_value_double_ = (double)(max_value_);
  seed1_ = seed1 % max_value_;
  seed2_ = seed2 % max_value_;
  is_inited_ = true;
}


/**
 **************** MySQL 4.1.1 authentication routines *************
  Generate string of printable pseudo random characters of requested length.

  @param buffer[out] Buffer for generation; must be at least length+1 bytes
                     long; result string is always null-terminated
  @param length[in]  How many random characters to put in buffer
  @param rand        Structure used for number generation

  @note This function is restricted for use with
    native_password_authenticate() because of security reasons.

  DON'T RELY ON THIS FUNCTION FOR A UNIFORMLY DISTRIBUTION OF BITS!
*/
inline int ObMysqlRandom::create_random_string(char *buf, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t end  = length - 1;
    /*
      Warning: my_rnd() is a fast prng, but it doesn't necessarily have a uniform
      distribution.
    */
    for (int64_t i = 0; i < end; ++i) {
      buf[i] = static_cast<char>(get_double() * 94 + 33);
    }
    buf[end] = '\0';
  }
  return ret;
}

/**
  Generate random number.

  @param rand [INOUT] Structure used for number generation.

  @retval                Generated pseudo random number.
*/
inline double ObMysqlRandom::get_double()
{
  double ret_double = 0;
  if (OB_LIKELY(is_inited())) {
    seed1_ = (seed1_ * 3 + seed2_) % max_value_;
    seed2_ = (seed1_ + seed2_ + 33) % max_value_;
    ret_double = ((static_cast<double>(seed1_)) / max_value_double_);
  }
  return ret_double;
}

inline uint64_t ObMysqlRandom::get_uint64()
{
  return static_cast<uint64_t>(get_double() * 0xFFFFFFFF);
}


}// namespace common
}//namespace oceanbase
#endif
