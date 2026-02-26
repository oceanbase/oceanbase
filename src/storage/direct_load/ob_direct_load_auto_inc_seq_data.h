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