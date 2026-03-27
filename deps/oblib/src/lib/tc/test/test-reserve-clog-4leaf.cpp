/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
ROOT(root);

SHARED(t1, root, 1);
SHARED(t2, root, 2);
FIFO(g1, t1, 1);
FIFO(g2, t1, 4);
FIFO(g3, t2, 4);
FIFO(g4, t2, 4);
LIMIT_SET(t1, 100 * 1000L * 1000L);

RESERVE_SET(g1, 80 * 1000L * 1000L);

SCHED();
FILL_SOME_TIME(g1);
FILL(g2);
FILL(g3);
FILL(g4);
