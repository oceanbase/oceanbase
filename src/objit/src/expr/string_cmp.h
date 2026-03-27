/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
