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
#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_external_table.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

/**
 * ObDirectLoadExternalTableCreateParam
 */

ObDirectLoadExternalTableCreateParam::ObDirectLoadExternalTableCreateParam()
  : data_block_size_(0), row_count_(0), max_data_block_size_(0)
{
}

ObDirectLoadExternalTableCreateParam::~ObDirectLoadExternalTableCreateParam()
{
}

bool ObDirectLoadExternalTableCreateParam::is_valid() const
{
  return data_block_size_ > 0 && data_block_size_ % DIO_ALIGN_SIZE == 0 && row_count_ > 0 &&
         max_data_block_size_ > 0 && max_data_block_size_ % DIO_ALIGN_SIZE == 0 &&
         !fragments_.empty();
}

/**
 * ObDirectLoadExternalTableMeta
 */

ObDirectLoadExternalTableMeta::ObDirectLoadExternalTableMeta()
  : data_block_size_(0), row_count_(0), max_data_block_size_(0)
{
}

ObDirectLoadExternalTableMeta::~ObDirectLoadExternalTableMeta()
{
}

void ObDirectLoadExternalTableMeta::reset()
{
  tablet_id_.reset();
  data_block_size_ = 0;
  row_count_ = 0;
  max_data_block_size_ = 0;
}

/**
 * ObDirectLoadExternalTable
 */

ObDirectLoadExternalTable::ObDirectLoadExternalTable()
  : is_inited_(false)
{
}

ObDirectLoadExternalTable::~ObDirectLoadExternalTable()
{
}

void ObDirectLoadExternalTable::reset()
{
  meta_.reset();
  fragments_.reset();
  is_inited_ = false;
}

int ObDirectLoadExternalTable::init(const ObDirectLoadExternalTableCreateParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadExternalTable init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    meta_.tablet_id_ = param.tablet_id_;
    meta_.data_block_size_ = param.data_block_size_;
    meta_.row_count_ = param.row_count_;
    meta_.max_data_block_size_ = param.max_data_block_size_;
    if (OB_FAIL(fragments_.assign(param.fragments_))) {
      LOG_WARN("fail to assign fragments", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObDirectLoadExternalTable::release_data()
{
  fragments_.reset();
}

int ObDirectLoadExternalTable::copy(const ObDirectLoadExternalTable &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(other));
  } else {
    reset();
    meta_ = other.meta_;
    if (OB_FAIL(fragments_.assign(other.fragments_))) {
      LOG_WARN("fail to assign fragments", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
