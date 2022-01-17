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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_CHECKPOINT_H_
#define OCEANBASE_STORAGE_OB_PARTITION_CHECKPOINT_H_

#include "clog/ob_i_submit_log_cb.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;

class ObCheckPoingLogCb : public clog::ObISubmitLogCb {
public:
  ObCheckPoingLogCb() : ps_(NULL), checkpoint_(0)
  {}
  ~ObCheckPoingLogCb()
  {}
  int init(ObPartitionService* ps, const int64_t checkpoint);

public:
  int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type, const uint64_t log_id,
      const int64_t version, const bool batch_committed, const bool batch_last_succeed);
  int on_finished(const common::ObPartitionKey& partition_key, const uint64_t log_id);

public:
  TO_STRING_KV(K_(checkpoint));

private:
  ObPartitionService* ps_;
  int64_t checkpoint_;
};

class ObCheckPoingLogCbFactory {
public:
  static ObCheckPoingLogCb* alloc();
  static void release(ObCheckPoingLogCb* cb);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PARTITION_CHECKPOINT_H_
