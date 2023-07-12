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

extern inline void ussl_link_init(ussl_link_t* n);
extern inline int ussl_link_is_empty(ussl_link_t* n);
extern inline ussl_link_t* ussl_link_insert(ussl_link_t* prev, ussl_link_t* t);
extern inline ussl_link_t* ussl_link_delete(ussl_link_t* prev);
extern ussl_link_t* ussl_link_pop(ussl_link_t* h);