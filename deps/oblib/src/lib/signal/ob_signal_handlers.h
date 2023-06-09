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

#ifndef OCEANBASE_SIGNAL_HANDLERS_H_
#define OCEANBASE_SIGNAL_HANDLERS_H_

#include <stdio.h>
#include <stdint.h>
#include <time.h>

namespace oceanbase
{
namespace common
{
extern int minicoredump(int sig, int64_t tid, pid_t& pid);
extern int faststack();
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SIGNAL_HANDLERS_H_
