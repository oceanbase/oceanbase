/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
