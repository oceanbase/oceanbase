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

#ifndef OCEANBASE_STORAGE_PARTITION_MEMSTORE_INFO_RECORD_
#define OCEANBASE_STORAGE_PARTITION_MEMSTORE_INFO_RECORD_

#include <stdint.h>

#include "lib/lock/ob_spin_rwlock.h"
#include "storage/ob_data_storage_info.h"

namespace oceanbase {

namespace storage {

class ObMemstoreInfoRecord {
public:
  int push(const ObDataStorageInfo& data_info, const int64_t readable_ts);
  int front(ObDataStorageInfo& data_info) const;
  void reset();

  ObMemstoreInfoRecord();
  ~ObMemstoreInfoRecord();
  ObMemstoreInfoRecord(const ObMemstoreInfoRecord&) = delete;
  ObMemstoreInfoRecord& operator=(const ObMemstoreInfoRecord&) = delete;

private:
  int64_t get_next_pos() const
  {
    return 1L - head_;
  }

  ObDataStorageInfo data_info_[2];
  int64_t head_;
  common::SpinRWLock lock_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_PARTITION_MEMSTORE_INFO_RECORD_ */
