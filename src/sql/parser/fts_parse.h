/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef FTS_BASE_H
#define FTS_BASE_H

#include <stdint.h>
#include <assert.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include "fts_base.h"

#ifdef __cplusplus
extern "C" {
#endif
// this is a C wrapper to call murmurhash in C++ definition
void fts_parse_docment(const char *input, const int length, void *pool, FtsParserResult *ss);

#ifdef __cplusplus
}
#endif

#endif // FTS_BASE_H
