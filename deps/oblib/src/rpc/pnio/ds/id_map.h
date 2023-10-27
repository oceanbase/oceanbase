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

typedef struct idm_item_t {
  link_t link;
  uint64_t id;
  void* data;
} idm_item_t;
typedef struct idm_t {
  uint64_t capacity;
  link_t free_list;
  idm_item_t table[0];
} idm_t;

extern void idm_init(idm_t* idm, int64_t capacity);
inline void* idm_get(idm_t* idm, uint64_t id) {
  idm_item_t* pi = idm->table + (id % idm->capacity);
  return id == pi->id? pi->data: NULL;
}
extern uint64_t idm_set(idm_t* idm, void* data);
extern void idm_del(idm_t* idm, uint64_t id);
