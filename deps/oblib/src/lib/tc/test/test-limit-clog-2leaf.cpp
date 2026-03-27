/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

ROOT(root);

SHARED(t1, root, 1);

FIFO(g1, t1, 1);
FIFO(g2, t1, 4);
LIMIT_SET(t1, 10 * 1000L * 1000L);

RESERVE_SET(g1, 4 * 1000L * 1000L);

SCHED();
FILL_SOME_TIME(g1);
FILL(g2);
