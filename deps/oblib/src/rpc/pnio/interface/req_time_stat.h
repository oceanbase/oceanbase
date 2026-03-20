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

#ifndef PNIO_INTERFACE_REQ_TIME_STAT_H_
#define PNIO_INTERFACE_REQ_TIME_STAT_H_

#include <stdint.h>

typedef struct pn_req_time_stat
{
  int64_t rpc_start_ts;           // Base absolute timestamp
  int32_t submit_io_diff;         // Delta: submit_io_ts - rpc_start_ts
  int32_t submit_sock_queue_diff; // Delta: submit_sock_queue_ts - rpc_start_ts
  int32_t write_sock_diff;        // Delta: write_sock_ts - rpc_start_ts
  int32_t read_resp_diff;         // Delta: read_resp_ts - rpc_start_ts
  int32_t handle_cb_diff;         // Delta: handle_cb_ts - rpc_start_ts
} pn_req_time_stat;

#endif /* PNIO_INTERFACE_REQ_TIME_STAT_H_ */
