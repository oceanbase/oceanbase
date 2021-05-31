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

#ifndef CO_THREAD_FAIR_SCHED_H
#define CO_THREAD_FAIR_SCHED_H

#include "lib/coro/co_base_sched.h"

namespace oceanbase {
namespace lib {

// CoThreadFairSched is a thread containing Co-Routine scheduler with
// fair policy.
//
// The scheduler is simply process worker based on FIFO, that is,
// worker with earlier runnable status would be scheduled before the
// later.
//
// Before the thread start real schedule, prepare function will
// called. After the schduler is able to schedule existing
// workers. When all workers finish and exit scheduler calls postrun
// function and exit itself.
//
// How To Use:
//
// 1) Inherit CoThreadFairSched::Worker and implement run function.
// 2) Inherit CoThreadFairSched and override prepare function create
//    workers defined in step 1. If need destroy workers created in
//    prepare function, code can be added into postrun function. Don't
//    forget initialize workers by call their init function otherwise
//    scheduler can't be aware of existence of them.
// 3) Create your scheduler and start it by calling start. Then wait
//    for its completion.
//
// Example is in test_co_thread_fir_sched.cpp file.
//
using CoThreadFairSched = CoBaseSched;

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_THREAD_FAIR_SCHED_H */
