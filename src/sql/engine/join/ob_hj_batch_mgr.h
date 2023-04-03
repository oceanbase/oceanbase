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

#ifndef _OB_HJ_BATCH_MGR_H
#define _OB_HJ_BATCH_MGR_H 1

#include "ob_hj_batch.h"
#include "lib/list/ob_list.h"
#include "sql/engine/join/ob_hj_buf_mgr.h"

namespace oceanbase
{
namespace sql
{
namespace join
{

struct ObHJBatchPair
{
  ObHJBatch *left_;
  ObHJBatch *right_;

  ObHJBatchPair() : left_(NULL), right_(NULL) {}
};

class ObHJBatchMgr {
public:
  ObHJBatchMgr(common::ObIAllocator &alloc, ObHJBufMgr *buf_mgr, uint64_t tenant_id) :
    total_dump_count_(0),
    total_dump_size_(0),
    batch_count_(0),
    tenant_id_(tenant_id),
    alloc_(alloc),
    batch_list_(alloc),
    buf_mgr_(buf_mgr)
  {}

  virtual ~ObHJBatchMgr();
  void reset();

  typedef common::ObList<ObHJBatchPair, common::ObIAllocator> hj_batch_pair_list_type;

  int next_batch(ObHJBatchPair &batch_pair);

  int64_t get_batch_list_size() { return batch_list_.size(); }
  int remove_undumped_batch();
  int get_or_create_batch(int32_t level,
                          int64_t part_shift,
                          int32_t batchno,
                          bool is_left,
                          ObHJBatch *&batch,
                          bool only_get = false);

  void free(ObHJBatch *batch) {
    if (NULL != batch) {
      batch->~ObHJBatch();
      alloc_.free(batch);
      batch = NULL;
      batch_count_ --;
    }
  }

public:
  int64_t total_dump_count_;
  int64_t total_dump_size_;
  int64_t batch_count_;

private:
  uint64_t tenant_id_;
  common::ObIAllocator &alloc_;
  hj_batch_pair_list_type batch_list_;
  ObHJBufMgr *buf_mgr_;
};

}
}
}

#endif /* _OB_HJ_BATCH_MGR_H */


