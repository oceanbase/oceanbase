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

#include "lib/hash/ob_hashutils.h"
namespace oceanbase {
namespace common {
namespace hash {
const int64_t PRIME_LIST[PRIME_NUM] =
  {
    53l, 97l, 193l, 389l, 769l,
    1543l, 3079l, 6151l, 12289l, 24593l,
    49157l, 98317l, 196613l, 393241l, 786433l,
    1572869l, 3145739l, 6291469l, 12582917l, 25165843l,
    50331653l, 100663319l, 201326611l, 402653189l, 805306457l,
    1610612741l, 3221225473l, 4294967291l
  };
}
}
}

