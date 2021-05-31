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

#include "storage/ob_partition_memstore_info_record.h"

#include "share/ob_define.h"

namespace oceanbase {

using namespace common;

namespace storage {

ObMemstoreInfoRecord::ObMemstoreInfoRecord() : data_info_(), head_(0L), lock_()
{}

ObMemstoreInfoRecord::~ObMemstoreInfoRecord()
{}

int ObMemstoreInfoRecord::push(const ObDataStorageInfo& data_info, const int64_t readable_ts)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(lock_);

  int64_t next_ts = data_info_[get_next_pos()].get_publish_version();
  if (0 != next_ts && next_ts > readable_ts) {
    // ensure the snapshot of migration is readable
    ret = OB_REPLICA_NOT_READABLE;
  } else {
    data_info_[head_] = data_info;
    head_ = get_next_pos();
  }

  return ret;
}

int ObMemstoreInfoRecord::front(ObDataStorageInfo& data_info) const
{
  int ret = OB_SUCCESS;

  SpinRLockGuard guard(lock_);

  if (0 == data_info_[head_].get_publish_version()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    data_info = data_info_[head_];
  }

  return ret;
}

void ObMemstoreInfoRecord::reset()
{
  SpinWLockGuard guard(lock_);

  data_info_[0].reset();
  data_info_[1].reset();
  head_ = 0L;
}

}  // namespace storage
}  // namespace oceanbase
