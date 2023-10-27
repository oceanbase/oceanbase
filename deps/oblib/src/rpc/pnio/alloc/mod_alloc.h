/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

extern void mod_report(format_t* f);
extern void* mod_alloc(int64_t sz, int mod);
extern void mod_free(void* p);
extern void* salloc(int64_t sz);
extern void sfree(void* p);
enum {
#define MOD_DEF(name) MOD_ ## name, //keep
#include "mod_define.h"
#undef MOD_DEF
};
