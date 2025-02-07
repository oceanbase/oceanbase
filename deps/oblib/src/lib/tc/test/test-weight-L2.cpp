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
