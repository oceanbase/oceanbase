/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_COMMON_META_PROGRAMMING_OB_META_SWAP_H
#define SRC_COMMON_META_PROGRAMMING_OB_META_SWAP_H

#include "ob_type_traits.h"

namespace oceanbase
{
namespace common
{
namespace meta
{

// try use member swap() method first
template <typename T, typename Member, ENABLE_IF_SWAPABLE(Member)>
void swap(T &lhs, T &rhs, Member &member) noexcept
{
  if (OB_LIKELY((char *)&member >= (char *)&lhs && (char *)&member < (char *)&lhs + sizeof(lhs))) {
    member.swap(*(Member *)((char *)&rhs + ((char *)&member - (char *)&lhs)));
  } else {
    ob_abort();
  }
}

// otherwise decay to standard swap, that is : temp = a; a = b; b = temp;
template <typename T, typename Member, ENABLE_IF_NOT_SWAPABLE(Member)>
void swap(T &lhs, T &rhs, Member &member) noexcept
{
  if (OB_LIKELY((char *)&member >= (char *)&lhs && (char *)&member < (char *)&lhs + sizeof(lhs))) {
    std::swap(member, *(Member *)((char *)&rhs + ((char *)&member - (char *)&lhs)));
  } else {
    ob_abort();
  }
}

template <typename T, typename Head, typename ...Others>
void swap(T &lhs, T &rhs, Head &head_member, Others &...other_members) noexcept
{
  swap(lhs, rhs, head_member);
  swap(lhs, rhs, other_members...);
}

}
}
}
#endif