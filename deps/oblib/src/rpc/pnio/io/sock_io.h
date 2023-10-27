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

extern int sk_read(sock_t* s, char* buf, size_t size, ssize_t* rbytes);
extern int sk_readv(sock_t* s, struct iovec* iov, int cnt, ssize_t* rbytes);
extern int sk_write(sock_t* s, const char* buf, size_t size, ssize_t* wbytes);
extern int sk_writev(sock_t* s, struct iovec* iov, int cnt, ssize_t* wbytes);
