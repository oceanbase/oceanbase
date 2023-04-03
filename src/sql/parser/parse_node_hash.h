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

#ifndef OCEANBASE_SQL_PARSER_PARSENODE_HASH_
#define OCEANBASE_SQL_PARSER_PARSENODE_HASH_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
// this is a C wrapper to call murmurhash in C++ definition
uint64_t murmurhash(const void *data, int32_t len, uint64_t hash);

#ifdef __cplusplus
}
#endif

#endif /*OCEANBASE_SQL_PARSER_PARSENODE_HASH_*/
