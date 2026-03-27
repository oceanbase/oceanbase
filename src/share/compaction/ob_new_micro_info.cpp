// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
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
