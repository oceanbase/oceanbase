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

#define PNIO_ENABLE_CRC 0
#define PNIO_ENABLE_DELAY_WARN 1

#if PNIO_ENABLE_CRC
#define PNIO_CRC(...) __VA_ARGS__
#else
#define PNIO_CRC(...)
#endif

#if PNIO_ENABLE_DELAY_WARN
#define PNIO_DELAY_WARN(...) __VA_ARGS__
#else
#define PNIO_DELAY_WARN(...)
#endif


#define FLUSH_DELAY_WARN_US 500000
#define HANDLE_DELAY_WARN_US 500000
#define ELOOP_WARN_US 500000
#define EPOLL_HANDLE_TIME_LIMIT 500000
#define MAX_REQ_QUEUE_COUNT   4096
#define MAX_WRITE_QUEUE_COUNT 4096
#define MAX_CATEG_COUNT 1024
