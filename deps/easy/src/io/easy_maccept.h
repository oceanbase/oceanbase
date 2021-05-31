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

#ifndef __EASY_MACCEPT_H__
#define __EASY_MACCEPT_H__

#include "easy_define.h"
EASY_CPP_START
extern void easy_ma_init(int port);
extern int easy_ma_start();
extern void easy_ma_stop();
extern int easy_ma_regist(int gid, int idx);
extern int easy_ma_handshake(int fd, int id);

EASY_CPP_END
#endif /* __EASY_MACCEPT_H__ */
