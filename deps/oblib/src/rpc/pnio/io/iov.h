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
 
#include <sys/uio.h>
inline void iov_set(struct iovec* iov, char* b, int64_t s) {
  iov->iov_base = b;
  iov->iov_len = s;
}

inline void iov_set_from_str(struct iovec* iov, str_t* s) {
  iov_set(iov, s->b, s->s);
}

inline void iov_consume_one(struct iovec* iov, int64_t bytes) {
  iov->iov_base = (void*)((uint64_t)iov->iov_base + bytes);
  iov->iov_len -= bytes;
}
