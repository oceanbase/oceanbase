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

extern inline bool dlink_is_empty(dlink_t* n);
extern inline void dlink_init(dlink_t* n);
extern inline void __dlink_insert(dlink_t* prev, dlink_t* next, dlink_t* n);
extern inline void __dlink_delete(dlink_t* prev, dlink_t* next);
extern inline void dlink_insert(dlink_t* head, dlink_t* n);
extern inline void dlink_insert_before(dlink_t* head, dlink_t* n);
extern inline void dlink_delete(dlink_t* n);;
