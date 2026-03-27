/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_TRANS_CHARACTER_H
#define OCEANBASE_COMMON_OB_TRANS_CHARACTER_H

#ifdef __cplusplus
extern "C"
{
#endif

//never change the value;
typedef enum ObTransCharacter
{
  OB_WITH_CONSTISTENT_SNAPSHOT = 1,
  OB_TRANS_READ_ONLY = 2,
  OB_TRANS_READ_WRITE = 4,
} ObTransCharacter;

#define IS_READ_ONLY(mode) (mode & OB_TRANS_READ_ONLY)
#define IS_READ_WRITE(mode) (mode & OB_TRANS_READ_WRITE)
#define IS_WITH_SNAPSHOT(mode) (mode & OB_WITH_CONSTISTENT_SNAPSHOT)

#ifdef __cplusplus
}
#endif
#endif //OCEANBASE_COMMON_OB_TRANS_CHARACTER_H
