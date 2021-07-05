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

#ifndef _OCEANBASE_UNITTEST_CLOG_MOCK_OB_LOG_SLIDING_WINDOW_H_
#define _OCEANBASE_UNITTEST_CLOG_MOCK_OB_LOG_SLIDING_WINDOW_H_

#include "lib/atomic/atomic128.h"
#include "clog/ob_log_define.h"
#include "clog/ob_log_state_mgr.h"
#include "clog/ob_log_entry.h"
#include "clog/ob_log_entry_header.h"
#include "clog/ob_log_task.h"
#include "clog/ob_log_flush_task.h"
#include "clog/ob_i_disk_log_buffer.h"
#include "clog/ob_log_checksum_V2.h"
#include "clog/ob_fetch_log_engine.h"

namespace oceanbase {
namespace clog {
class ObILogReplayEngineWrapper;
class ObIOutOfBandLogHandler;
class ObILogStateMgrForSW;

class MockObSlidingCallBack : public ObILogTaskCallBack {
public:
  MockObSlidingCallBack()
  {}
  virtual ~MockObSlidingCallBack()
  {}
  int sliding_cb(const int64_t sn, const ObILogExtRingBufferData* data)
  {
    UNUSED(sn);
    UNUSED(data);
    return OB_SUCCESS;
  }
  void inc_task_num()
  {}
  void dec_task_num()
  {}
};
}  // namespace clog
}  // namespace oceanbase
#endif  //_OCEANBASE_UNITTEST_CLOG_MOCK_OB_LOG_SLIDING_WINDOW_H_
