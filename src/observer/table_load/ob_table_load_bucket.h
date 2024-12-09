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

#include "lib/net/ob_addr.h"
#include "share/table/ob_table_load_row_array.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace common
{
class ObObj;
}  // namespace common
namespace observer
{

class ObTableLoadBucket
{
public:
  ObTableLoadBucket() : is_inited_(false), row_size_(0), sequence_no_(0) {}

  int add_row(const common::ObTabletID &tablet_id,
              const table::ObTableLoadObjRow &obj_row,
              int64_t batch_size,
              int64_t row_size,
              bool &flag);

  void reset() {
    is_inited_ = false;
    leader_addr_.reset();
    row_array_.reset();
    row_size_ = 0;
  }

  void clear_data() {
    row_array_.reset();
    row_size_ = 0;
  }

  bool is_inited() const {
    return is_inited_;
  }
  int init(const common::ObAddr &leader_addr);

  TO_STRING_KV(K_(leader_addr), K_(sequence_no));

public:
  // data members
  bool is_inited_;
  common::ObAddr leader_addr_;
  table::ObTableLoadTabletObjRowArray row_array_;
  int64_t row_size_;
  uint64_t sequence_no_;
};

}  // namespace observer
}  // namespace oceanbase
