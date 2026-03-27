/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_TC_OB_TC_WRAPPER_H_
#define OCEANBASE_TC_OB_TC_WRAPPER_H_
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "deps/oblib/src/lib/allocator/ob_malloc.h"
#define TC_INFO(...) _OB_LOG(INFO, __VA_ARGS__)
#define ALLOCATE_QDTABLE(size, name) static_cast<void**>(oceanbase::ob_malloc(size, name))
#define MAX_CPU_NUM oceanbase::common::OB_MAX_CPU_NUM
#define tc_itid oceanbase::common::get_itid
#include "ob_tc.cpp"
#endif /* OCEANBASE_TC_OB_TC_WRAPPER_H_ */
