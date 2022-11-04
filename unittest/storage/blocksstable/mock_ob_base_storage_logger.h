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
