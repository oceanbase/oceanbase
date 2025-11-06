/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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