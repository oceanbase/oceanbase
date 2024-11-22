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
DEF_COUNT_LIMIT(L1, 8000);
DEF_COUNT_LIMIT(L2, 10000);
ROOT(root);
FIFO(g1, root, 1);
FIFO(g2, root, 2);
LIMIT(root, L2);
RESERVE(g1, L1);
RESERVE(g2, L1);
SCHED();
FILL(g1);
FILL(g2);
