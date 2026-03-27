/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
DEF_COUNT_LIMIT(L1, 80000);
ROOT(root);
FIFO(g1, root, 1);
FIFO(g2, root, 2);
RESERVE(g1, L1);
SCHED();
FILL(g1);
FILL(g2);
