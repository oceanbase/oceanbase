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

#include "storage/direct_load/ob_direct_load_data_block_reader.h"
#include "storage/direct_load/ob_direct_load_sstable_data_block.h"

namespace oceanbase
{
namespace storage
{

template <typename T>
class ObDirectLoadSSTableDataBlockReader
  : public ObDirectLoadDataBlockReader<ObDirectLoadSSTableDataBlock::Header, T>
{
public:
  ObDirectLoadSSTableDataBlockReader() = default;
  virtual ~ObDirectLoadSSTableDataBlockReader() = default;
  int get_next_row(const T *&row);
  int get_last_row(const T *&row);
};

template <typename T>
int ObDirectLoadSSTableDataBlockReader<T>::get_next_row(const T *&row)
{
  return this->get_next_item(row);
}

template <typename T>
int ObDirectLoadSSTableDataBlockReader<T>::get_last_row(const T *&row)
{
  int ret = common::OB_SUCCESS;
  const ObDirectLoadSSTableDataBlock::Header &header = this->data_block_reader_.get_header();
  this->curr_item_.reuse();
  if (OB_FAIL(this->data_block_reader_.read_item(header.last_row_pos_, this->curr_item_))) {
    STORAGE_LOG(WARN, "fail to read item", KR(ret));
  } else {
    row = &this->curr_item_;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
