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
