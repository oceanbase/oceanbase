/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
