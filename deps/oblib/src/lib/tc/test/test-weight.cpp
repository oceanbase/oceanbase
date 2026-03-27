/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

DEF_COUNT_LIMIT(L1, 5000);
ROOT(root);
FIFO(g1, root, 1);
FIFO(g2, root, 2);
LIMIT(root, L1);
SCHED();
FILL(g1, 100);
FILL(g2, 100);
