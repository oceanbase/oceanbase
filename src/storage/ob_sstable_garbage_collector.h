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

#ifndef OB_SSTABLE_GARBAGE_COLLECTOR_H
#define OB_SSTABLE_GARBAGE_COLLECTOR_H

#include "lib/thread/thread_pool.h"
#include "storage/ob_i_partition_group.h"
#include "storage/transaction/ob_trans_service.h"

namespace oceanbase {
namespace storage {

class ObSSTableGarbageCollector : public lib::ThreadPool {
public:
  ObSSTableGarbageCollector();
  virtual ~ObSSTableGarbageCollector();
  int init(transaction::ObTransService* trans_service);
  int start() override;
  void destroy();
  void run1() override;

private:
  static const int64_t SSTABLE_GC_INTERVAL = 30 * 1000 * 1000L;
  transaction::ObTransService* trans_service_;
  bool is_inited_;
};

}  // namespace storage
}  // namespace oceanbase
#endif /* OB_SSTABLE_GARBAGE_COLLECTOR_H */
