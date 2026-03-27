/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_UTILS_CHAR_UTILS_H_
#define _OCEANBASE_STORAGE_FTS_UTILS_CHAR_UTILS_H_

#define true_word_char(ctype, character) ((ctype) & (_MY_U | _MY_L | _MY_NMR) || (character) == '_')

#endif // _OCEANBASE_STORAGE_FTS_UTILS_CHAR_UTILS_H_