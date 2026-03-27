/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef MOCK_OB_BASE_STORAGE_LOGGER_H_
#define MOCK_OB_BASE_STORAGE_LOGGER_H_

#include <gmock/gmock.h>

namespace oceanbase {
namespace blocksstable {

class MockObBaseStorageLogger : public ObBaseStorageLogger {
 public:
  MOCK_METHOD1(begin, int(const enum LogCommand cmd));
  MOCK_METHOD4(write_log, int(ActiveTransEntry &trans_entry, const int64_t subcmd, const ObStorageLogAttribute &log_attr, ObIBaseStorageLogEntry &data));
  MOCK_METHOD3(write_log, int(const int64_t subcmd, const ObStorageLogAttribute &log_attr, ObIBaseStorageLogEntry &data));
  MOCK_METHOD1(commit, int(int64_t &log_seq_num));
  MOCK_METHOD0(abort, int());
};

}  // namespace storage
}  // namespace oceanbase


#endif
