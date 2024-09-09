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
#ifndef OCEANBASE_STORAGE_META_STORE_OB_TENANT_SEQ_GENERATOR_H_
#define OCEANBASE_STORAGE_META_STORE_OB_TENANT_SEQ_GENERATOR_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{
class ObTenantStorageMetaPersister;
struct ObTenantMonotonicIncSeqs
{
public:
  ObTenantMonotonicIncSeqs() : object_seq_(0), tmp_file_seq_(0), write_seq_(0) {}

  TO_STRING_KV(K_(object_seq), K_(tmp_file_seq), K_(write_seq));
  void reset()
  {
    object_seq_ = 0;
    tmp_file_seq_ = 0;
    write_seq_ = 0;
  }
  void set(const uint64_t object_seq, const uint64_t tmp_file_seq, const uint64_t write_seq)
  {
    object_seq_ = object_seq;
    tmp_file_seq_ = tmp_file_seq;
    write_seq_ = write_seq;
  }

  OB_UNIS_VERSION(1);

public:
  uint64_t object_seq_;
  uint64_t tmp_file_seq_;
  uint64_t write_seq_;
};


class ObTenantSeqGenerator : public common::ObTimerTask
{
public:
  static const uint64_t BATCH_PREALLOCATE_NUM = 60000;

  ObTenantSeqGenerator()
    : is_inited_(false),
      is_shared_storage_(false),
      tg_id_(-1),
      persister_(nullptr),
      curr_seqs_(),
      preallocated_seqs_() {}

  int init(const bool is_shared_storage, ObTenantStorageMetaPersister &persister);
  int start();
  void stop();
  void wait();
  void destroy();
  int get_private_object_seq(uint64_t &seq);
  int get_tmp_file_seq(uint64_t &seq);
  int get_write_seq(uint64_t &seq);
  virtual void runTimerTask() override;

private:
  int try_preallocate_();


private:

  static const uint64_t PREALLOCATION_TRIGGER_MARGIN = 30000;
  static const uint64_t TRY_PREALLOACTE_INTERVAL = 500000; // 500ms
  static const int64_t MAX_RETRY_TIME = 30;


  bool is_inited_;
  bool is_shared_storage_;
  int tg_id_;
  ObTenantStorageMetaPersister *persister_;
  ObTenantMonotonicIncSeqs curr_seqs_;
  ObTenantMonotonicIncSeqs preallocated_seqs_;
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_META_STORE_OB_TENANT_SEQ_GENERATOR_H_