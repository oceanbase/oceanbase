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

#ifndef OCEANBASE_LOGSERVICE_IPALF_OPTIONS_
#define OCEANBASE_LOGSERVICE_IPALF_OPTIONS_

#include <cstdint>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace ipalf
{

// Palf支持在三种模式中来回切换
//
// APPEND: 该模式下，PALF为待提交日志分配LSN和TS
//
// RAW_WRITE: 该模式下, PALF不具备为待提交日志分配LSN和TS的能力
//
// FLASHBACK: 该模式下, PALF不具备日志写入能力,且各副本间不响应拉日志请求
// PREPARE_FLASHBACK: 该模式下，PALF不具备日志写入能力,各副本间可以互相同步日志
enum class AccessMode {
  INVALID_ACCESS_MODE = 0,
  APPEND = 1,
  RAW_WRITE = 2,
  FLASHBACK = 3,
  PREPARE_FLASHBACK = 4,
};

enum class SyncMode {
  INVALID_SYNC_MODE = 0,
  SYNC = 1,
  ASYNC = 2,
  PRE_ASYNC = 3,
};

struct PalfAppendOptions
{
    // Palf的使用者在提交日志时，有两种不同的使用方式：
    //
    // 1. 阻塞提交（类比于io系统调用的BLOCK语义）。这种使用方式在提交日志量超过Palf的处理能力时，
    //    会占住线程，极端场景下可能会使调用线程"永远阻塞"；
    //
    // 优点：使用简单，不需要处理超出处理能力时的报错；
    //
    // 缺点：会占住调用线程；
    //
    // 典型使用场景：提交事务的redo日志;
    //
    // 2. 非阻塞提交（类比于io系统调用的NONBLOCK语义）。这种使用方式在提交日志量超过Palf的处理能力时，
    //    append调用返回OB_EAGAIN错误码，不会占住调用线程；
    //
    // 优点：不会占住调用线程；
    //
    // 缺点：调用者需要处理OB_EAGAIN错误；
    //
    // 典型使用场景：提交事务的prepare/commit日志，返回OB_EAGAIN后由两阶段状态机推进状态；
    //
    // 默认值为NONBLOCK
    bool need_nonblock = true;
    bool need_check_proposal_id = true;
    int64_t proposal_id = 0;
    TO_STRING_KV(K(need_nonblock), K(need_check_proposal_id), K(proposal_id));
};

} // end namespace ipalf
} // end namespace oceanbase

#endif