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

#ifndef _OCEANBASE_TESTBENCH_MACROS_
#define _OCEANBASE_TESTBENCH_MACROS_

namespace oceanbase
{
  // log printer of instance
#define MPRINT(format, ...) fprintf(stdout, format "\n", ##__VA_ARGS__)
#define MPRINTx(format, ...)     \
  MPRINT(format, ##__VA_ARGS__); \
  exit(1)

// workload type macros
#define WORKLOAD_TYPES                                  \
  X(DISTRIBUTED_TRANSACTION, "distributed_transaction") \
  X(CONTENTION, "contention")                           \
  X(DEADLOCK, "deadlock")

#define ADD_WORKLOAD_OPTS(opts, id, src_opt_str) opts.push_back(OB_NEW(ObWorkloadOptions<WorkloadType::id>, "WorkloadOptions", src_opt_str));

#define X(key, value) key,
  enum WorkloadType
  {
    WORKLOAD_TYPES
  };
#undef X

#define X(key, value) value,
  const char * const wl_types[] = {
      WORKLOAD_TYPES};
#undef X

// global workload options macros
#define GLOBAL_OPTIONS       \
  X(STARTTIME, "start_time") \
  X(DURATION, "duration")

// distributed transaction workload options macros
#define DTXN_OPTIONS              \
  GLOBAL_OPTIONS                  \
  X(PARTICIPANTS, "participants") \
  X(OPERATIONS, "operations")     \
  X(AFFECTROWS, "affect_rows")    \
  X(END, "end")

#define X(key, value) key,
  enum DTxnOption
  {
    DTXN_OPTIONS
  };
#undef X

#define X(key, value) value,
  const char * const dtxn_opts[] = {
      DTXN_OPTIONS};
#undef X
}

#endif