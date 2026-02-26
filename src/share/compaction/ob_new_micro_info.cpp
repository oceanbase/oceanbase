//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "share/compaction/ob_new_micro_info.h"

namespace oceanbase
{
namespace compaction
{
OB_SERIALIZE_MEMBER_SIMPLE(ObNewMicroInfo, info_, meta_micro_size_, data_micro_size_);

void ObNewMicroInfo::add(const ObNewMicroInfo &input_info)
{
  meta_micro_size_ += input_info.meta_micro_size_;
  data_micro_size_ += input_info.data_micro_size_;
}

} // namespace compaction
} // namespace oceanbase
