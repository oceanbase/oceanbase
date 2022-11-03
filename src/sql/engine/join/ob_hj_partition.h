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

#ifndef _OB_HJ_PARTITION_H
#define _OB_HJ_PARTITION_H 1

#include "ob_hj_buf_mgr.h"
#include "ob_hj_batch_mgr.h"
#include "lib/list/ob_dlist.h"

namespace oceanbase
{
namespace sql
{
namespace join
{

// Same with StoredRow, just for StoredRow::hash_val_ interpretation.
struct ObStoredJoinRow : public sql::ObChunkRowStore::StoredRow
{
  const static int64_t HASH_VAL_BIT = 63;
  const static int64_t HASH_VAL_MASK = UINT64_MAX >> (64 - HASH_VAL_BIT);
  struct ExtraInfo
  {
    uint64_t hash_val_:HASH_VAL_BIT;
    uint64_t is_match_:1;
  };

  ExtraInfo &get_extra_info()
  {
    static_assert(sizeof(ObStoredJoinRow) == sizeof(sql::ObChunkRowStore::StoredRow),
        "sizeof StoredJoinRow must be the save with StoredRow");
    return *reinterpret_cast<ExtraInfo *>(get_extra_payload());
  }
  const ExtraInfo &get_extra_info() const { return *reinterpret_cast<const ExtraInfo *>(get_extra_payload()); }

  uint64_t get_hash_value() const { return get_extra_info().hash_val_; }
  void set_hash_value(const uint64_t hash_val) { get_extra_info().hash_val_ = hash_val & HASH_VAL_MASK; }
  bool is_match() const { return get_extra_info().is_match_; }
  void set_is_match(bool is_match) { get_extra_info().is_match_ = is_match; }
};


struct ObRowStoreHeader : public common::ObDLinkBase<ObRowStoreHeader>
{
public:
  ObRowStoreHeader() :
    row_store_(ObModIds::OB_ARENA_HASH_JOIN, OB_SERVER_TENANT_ID, true)
  {}
  virtual ~ObRowStoreHeader() {}

  common::ObRowStore row_store_;
  char buf_[0];
};

#define GET_OB_ROW_STORE_HEADER(row_store) \
((ObRowStoreHeader *)((char *)row_store - (int64_t)(&(((ObRowStoreHeader *)(0))->row_store_))))

class ObHJPartition {
public:
  ObHJPartition() :
    buf_mgr_(nullptr),
    batch_mgr_(nullptr),
    batch_(nullptr),
    part_level_(-1),
    part_id_(-1)
    {}

  ObHJPartition(ObHJBufMgr *buf_mgr, ObHJBatchMgr *batch_mgr) :
    buf_mgr_(buf_mgr),
    batch_mgr_(batch_mgr),
    batch_(nullptr),
    part_level_(-1),
    part_id_(-1)
    {}

  virtual ~ObHJPartition() {
    reset();
  }

  void set_hj_buf_mgr(ObHJBufMgr *buf_mgr) {
    buf_mgr_ = buf_mgr;
  }

  void set_hj_batch_mgr(ObHJBatchMgr *batch_mgr) {
    batch_mgr_ = batch_mgr;
  }

  int init(
    int32_t part_level,
    int64_t part_shift,
    int32_t part_id,
    bool is_left,
    ObHJBufMgr *buf_mgr,
    ObHJBatchMgr *batch_mgr,
    ObHJBatch* pre_batch,
    ObSqlMemoryCallback *callback,
    int64_t dir_id);

  int get_next_row(const ObStoredJoinRow *&stored_row);

  // payload of ObRowStore::StoredRow will be set after added.
  int add_row(const common::ObNewRow &row, ObStoredJoinRow *&stored_row);

  int finish_dump(bool memory_need_dump);
  int dump(bool all_dump);

  int init_iterator(bool is_chunk_iter);

  void set_part_level(int32_t part_level) { part_level_ = part_level; }
  void set_part_id(int32_t part_id) { part_id_ = part_id; }

  int32_t get_part_level() { return part_level_; }
  int32_t get_part_id() { return part_id_; }
  bool is_dumped() { return batch_->is_dumped(); } //需要实现

  int check();
  void reset();

  ObHJBatch *get_batch() { return batch_; }

  int64_t get_row_count_in_memory() { return batch_->get_row_count_in_memory(); }
  int64_t get_row_count_on_disk() { return batch_->get_row_count_on_disk(); }

  int64_t get_size_in_memory() { return batch_->get_size_in_memory(); }
  int64_t get_size_on_disk() { return batch_->get_size_on_disk(); }

  int record_pre_batch_info(int64_t pre_part_count, int64_t pre_bucket_number, int64_t total_size);
private:
  ObHJBufMgr *buf_mgr_;
  ObHJBatchMgr *batch_mgr_;
  ObHJBatch *batch_;
  int32_t part_level_;
  int32_t part_id_;
};


}
}
}

#endif /* _OB_HJ_PARTITION_H */


