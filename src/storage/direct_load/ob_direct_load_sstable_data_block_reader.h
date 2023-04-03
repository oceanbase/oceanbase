// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

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
