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
ROOT(root);

SHARED(t1, root, 1);

FIFO(g1, t1, 1);
FIFO(g2, t1, 4);
LIMIT_SET(t1, 100 * 1000L * 1000L);

RESERVE_SET(g1, 80 * 1000L * 1000L);

SCHED();
FILL_SOME_TIME(g1);
FILL(g2);

