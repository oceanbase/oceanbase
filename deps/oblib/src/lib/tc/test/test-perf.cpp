/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
ROOT(root);
int fifos = cfgi("fifos", "4");
SCHED();
MFIFO(f, fifos, root);
MFILL(f, fifos);
