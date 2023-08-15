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

#include "storage/direct_load/ob_direct_load_data_block.h"
#include "storage/direct_load/ob_direct_load_data_block_writer.h"

namespace oceanbase
{
namespace storage
{

template <typename T>
class ObDirectLoadExternalBlockWriter
  : public ObDirectLoadDataBlockWriter<ObDirectLoadDataBlock::Header, T>
{
  typedef ObDirectLoadDataBlockWriter<ObDirectLoadDataBlock::Header, T> ParentType;
public:
  ObDirectLoadExternalBlockWriter() {}
  virtual ~ObDirectLoadExternalBlockWriter() {}
  int init(int64_t data_block_size, common::ObCompressorType compressor_type, char *extra_buf,
           int64_t extra_buf_size)
  {
    return ParentType::init(data_block_size, compressor_type, extra_buf, extra_buf_size, nullptr);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadExternalBlockWriter);
};

} // namespace storage
} // namespace oceanbase
