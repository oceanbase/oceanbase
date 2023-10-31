//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "share/compaction/ob_compaction_time_guard.h"

namespace oceanbase
{
namespace compaction
{

void ObCompactionTimeGuard::add_time_guard(const ObCompactionTimeGuard &other)
{
  // last_click_ts_ is not useflu
  ObCompactionTimeGuard time_guard;
  int i = 0;
  int j = 0;
  while (i < idx_ && j < other.idx_) {
    if (line_array_[i] == other.line_array_[j]) {
      time_guard.line_array_[time_guard.idx_] = line_array_[i];
      time_guard.click_poinsts_[time_guard.idx_++] = click_poinsts_[i++] + other.click_poinsts_[j++];
    } else if (line_array_[i] < other.line_array_[j]) {
      time_guard.line_array_[time_guard.idx_] = line_array_[i];
      time_guard.click_poinsts_[time_guard.idx_++] = click_poinsts_[i++];
    } else {
      time_guard.line_array_[time_guard.idx_] = other.line_array_[j];
      time_guard.click_poinsts_[time_guard.idx_++] = other.click_poinsts_[j++];
    }
  }
  while (i < idx_) {
    time_guard.line_array_[time_guard.idx_] = line_array_[i];
    time_guard.click_poinsts_[time_guard.idx_++] = click_poinsts_[i++];
  }
  while (j < other.idx_) {
    time_guard.line_array_[time_guard.idx_] = other.line_array_[j];
    time_guard.click_poinsts_[time_guard.idx_++] = other.click_poinsts_[j++];
  }
  *this = time_guard;
}

ObCompactionTimeGuard & ObCompactionTimeGuard::operator=(const ObCompactionTimeGuard &other)
{
  last_click_ts_ = other.last_click_ts_;
  idx_ = other.idx_;
  for (int i = 0; i < other.idx_; ++i) {
    line_array_[i] = other.line_array_[i];
    click_poinsts_[i] = other.click_poinsts_[i];
  }
  return *this;
}

} // namespace compaction
} // namespace oceanbase
