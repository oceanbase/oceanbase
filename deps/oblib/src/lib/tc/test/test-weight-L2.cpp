/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
ROOT(root);
SHARED(tt1, root, 1);
FIFO(tt1g1, tt1, 1);
FIFO(tt1g2, tt1, 1);
SHARED(tt2, root, 1);
FIFO(tt2g1, tt2, 1);
FIFO(tt2g2, tt2, 1);
SCHED();
FILL(tt1g1);
FILL(tt1g2);
FILL(tt2g1);
FILL(tt2g2);
