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
#define MAX_GROUP_STAT 30
DEF_COUNT_LIMIT(B1R, 10000);
ROOT(root);
SHARED(net_in, root, 1);
SHARED(net_out, root, 1);
SHARED(tt1r, net_in, 1);
SHARED(tt2r, net_in, 2);
SHARED(tt1w, net_out, 1);
SHARED(tt2w, net_out, 2);
FIFO(tt1r1, tt1r, 1);
FIFO(tt1r2, tt1r, 2);
FIFO(tt2r1, tt2r, 1);
FIFO(tt2r2, tt2r, 2);
FIFO(tt1w1, tt1w, 1);
FIFO(tt1w2, tt1w, 2);
FIFO(tt2w1, tt2w, 1);
FIFO(tt2w2, tt2w, 2);
LIMIT(tt1r1, B1R);
LIMIT(tt2r1, B1R);
SCHED();
FILL(tt1r1);
FILL(tt1r2);
FILL(tt2r1);
FILL(tt2r2);
FILL(tt1w1);
FILL(tt1w2);
FILL(tt2w1);
FILL(tt2w2);
