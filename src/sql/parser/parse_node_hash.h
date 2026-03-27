/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
