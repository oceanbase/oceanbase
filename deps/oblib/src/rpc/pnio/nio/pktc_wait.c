/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

extern void pktc_wait_cb(const char* b, int64_t s, void* arg);
extern char* pktc_wait(pktc_wait_t* w, int64_t* sz);
