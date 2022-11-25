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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_PUB_LOG_SLIDING_WINDOW_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_PUB_LOG_SLIDING_WINDOW_

#define private public
#include "logservice/palf/log_sliding_window.h"
#undef private

namespace oceanbase
{
namespace palf
{
class PalfFSCbWrapper;

class MockPublicLogSlidingWindow : public LogSlidingWindow
{
public:
  MockPublicLogSlidingWindow()
  {}
  virtual ~MockPublicLogSlidingWindow()
  {}
};

} // namespace palf
} // namespace oceanbase

#endif
