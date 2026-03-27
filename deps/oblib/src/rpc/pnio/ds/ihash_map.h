/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

extern link_t* ihash_insert(hash_t* map, link_t* klink);
extern link_t* ihash_del(hash_t* map, void* key);
extern link_t* ihash_get(hash_t* map, void* key);
