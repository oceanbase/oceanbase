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

void* ref_alloc(int64_t sz, int mod) {
  int64_t* ref = (int64_t*)mod_alloc(sz + sizeof(int64_t), mod);
  if (ref) {
    *ref = 0;
    return ref + 1;
  }
  return NULL;
}

void ref_free(void* p) {
  if (NULL == p) return;
  int64_t* ref = (int64_t*)p - 1;
  if (0 == AAF(ref, -1)) {
    mod_free(ref);
  }
}
