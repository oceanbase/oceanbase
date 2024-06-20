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

#include "common/ob_tablet_id.h"
#include "storage/direct_load/ob_direct_load_i_table.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadFastHeapTableCreateParam
{
public:
  ObDirectLoadFastHeapTableCreateParam();
  ~ObDirectLoadFastHeapTableCreateParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(row_count));
public:
  common::ObTabletID tablet_id_;
  int64_t row_count_;
};

struct ObDirectLoadFastHeapTableMeta
{
  common::ObTabletID tablet_id_;
  int64_t row_count_;
  TO_STRING_KV(K_(tablet_id), K_(row_count));
};

class ObDirectLoadFastHeapTable : public ObIDirectLoadPartitionTable
{
public:
  ObDirectLoadFastHeapTable();
  virtual ~ObDirectLoadFastHeapTable();
  int init(const ObDirectLoadFastHeapTableCreateParam &param);
  const common::ObTabletID &get_tablet_id() const override { return meta_.tablet_id_; }
  int64_t get_row_count() const override { return meta_.row_count_; }
  bool is_valid() const override { return is_inited_; }
  void release_data() override { /*do nothing*/ }
  const ObDirectLoadFastHeapTableMeta &get_meta() const { return meta_; }
  TO_STRING_KV(K_(meta));
private:
  ObDirectLoadFastHeapTableMeta meta_;
  bool is_inited_;
  DISABLE_COPY_ASSIGN(ObDirectLoadFastHeapTable);
};

} // namespace storage
} // namespace oceanbase
