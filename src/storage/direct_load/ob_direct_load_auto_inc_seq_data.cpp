/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/direct_load/ob_direct_load_auto_inc_seq_data.h"

namespace oceanbase
{
namespace storage
{

ObDirectLoadAutoIncSeqData::ObDirectLoadAutoIncSeqData()
 : seq_val_(-1)
{
}

ObDirectLoadAutoIncSeqData::~ObDirectLoadAutoIncSeqData()
{
  reset();
}

int ObDirectLoadAutoIncSeqData::set_seq_val(const int64_t seq_val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(seq_val < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("seq val is invalid", KR(ret), K(seq_val));
  } else {
    seq_val_ = seq_val;
  }
  return ret;
}

bool ObDirectLoadAutoIncSeqData::is_valid() const
{
  return seq_val_ >= 0;
}

void ObDirectLoadAutoIncSeqData::reset()
{
  seq_val_ = -1;
}

OB_SERIALIZE_MEMBER(ObDirectLoadAutoIncSeqData, seq_val_);

} // namespace storage
} // namespace oceanbase