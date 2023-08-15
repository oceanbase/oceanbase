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

#pragma once

#include "observer/table_load/ob_table_load_trans_ctx.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTransCtx;
class ObTableLoadTransBucket;
class ObTableLoadTransBucketWriter;

struct ObTableLoadCoordinatorTrans
{
  ObTableLoadCoordinatorTrans(ObTableLoadTransCtx *trans_ctx, int32_t default_session_id);
  ~ObTableLoadCoordinatorTrans();
  int init();
  OB_INLINE int32_t get_default_session_id() const { return default_session_id_; }
  OB_INLINE ObTableLoadTransCtx *get_trans_ctx() const { return trans_ctx_; }
  OB_INLINE const table::ObTableLoadTransId &get_trans_id() const
  {
    return trans_ctx_->trans_id_;
  }
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_count() { return ATOMIC_AAF(&ref_count_, -1); }
  bool is_dirty() const { return is_dirty_; }
  void set_dirty() { is_dirty_ = true; }
  TO_STRING_KV(KP_(trans_ctx), KP_(trans_bucket_writer), K_(is_dirty));
public:
  OB_INLINE int check_trans_status(table::ObTableLoadTransStatusType trans_status) const
  {
    return trans_ctx_->check_trans_status(trans_status);
  }
  OB_INLINE int set_trans_status_inited()
  {
    return advance_trans_status(table::ObTableLoadTransStatusType::INITED);
  }
  OB_INLINE int set_trans_status_running()
  {
    return advance_trans_status(table::ObTableLoadTransStatusType::RUNNING);
  }
  OB_INLINE int set_trans_status_frozen()
  {
    return advance_trans_status(table::ObTableLoadTransStatusType::FROZEN);
  }
  OB_INLINE int set_trans_status_commit()
  {
    return advance_trans_status(table::ObTableLoadTransStatusType::COMMIT);
  }
  int set_trans_status_error(int error_code);
  int set_trans_status_abort();
private:
  int advance_trans_status(table::ObTableLoadTransStatusType trans_status);
public:
  int get_bucket_writer_for_write(ObTableLoadTransBucketWriter *&bucket_writer) const;
  int get_bucket_writer_for_flush(ObTableLoadTransBucketWriter *&bucket_writer) const;
  void put_bucket_writer(ObTableLoadTransBucketWriter *bucket_writer);
private:
  int handle_write_done();
private:
  ObTableLoadTransCtx * const trans_ctx_;
  const int32_t default_session_id_;
  ObTableLoadTransBucketWriter *trans_bucket_writer_;
  int64_t ref_count_ CACHE_ALIGNED;
  volatile bool is_dirty_;
  bool is_inited_;
};

}  // namespace observer
}  // namespace oceanbase
