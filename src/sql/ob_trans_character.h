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
