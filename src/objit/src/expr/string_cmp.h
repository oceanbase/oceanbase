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

#ifndef EXPRJ_STRING_CMP_H
#define EXPRJ_STRING_CMP_H

namespace oceanbase {
namespace jit {
namespace expr {
extern int jit_strcmpsp(const char *str1,
             int64_t str1_len,
             const char *str2,
             int64_t str2_len,
             bool cmp_endspace);
}  // expr
}  // jit
}  // oceanbase

#endif
