/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_DATA_H_
#define OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_DATA_H_

#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadAutoIncSeqData
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadAutoIncSeqData();
  ~ObDirectLoadAutoIncSeqData();
  int set_seq_val(const int64_t seq_val);
  bool is_valid() const;
  OB_INLINE int64_t get_seq_val() const { return seq_val_; }
  void reset();
  TO_STRING_KV(K_(seq_val));
private:
  int64_t seq_val_;
};

} // namespace storage
} // namespace oceanbase

#endif /* OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_DATA_H_ */