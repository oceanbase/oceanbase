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

#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

ObDirectLoadMultipleDatumRange::ObDirectLoadMultipleDatumRange()
{
}

ObDirectLoadMultipleDatumRange::~ObDirectLoadMultipleDatumRange()
{
}

void ObDirectLoadMultipleDatumRange::reset()
{
  start_key_.reset();
  end_key_.reset();
  border_flag_.set_data(0);
}

int ObDirectLoadMultipleDatumRange::deep_copy(const ObDirectLoadMultipleDatumRange &src,
                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src));
  } else {
    reset();
    if (OB_FAIL(start_key_.deep_copy(src.start_key_, allocator))) {
      LOG_WARN("fail to deep copy rowkey", KR(ret));
    } else if (OB_FAIL(end_key_.deep_copy(src.end_key_, allocator))) {
      LOG_WARN("fail to deep copy rowkey", KR(ret));
    } else {
      border_flag_ = src.border_flag_;
    }
  }

  return ret;
}

ObDirectLoadMultipleDatumRange &ObDirectLoadMultipleDatumRange::operator=(
  const ObDirectLoadMultipleDatumRange &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other.is_valid())) {
    reset();
    start_key_ = other.start_key_;
    end_key_ = other.end_key_;
    border_flag_ = other.border_flag_;
  }
  return *this;
}

int ObDirectLoadMultipleDatumRange::assign(const ObDirectLoadMultipleDatumRange &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(other));
  } else {
    reset();
    start_key_ = other.start_key_;
    end_key_ = other.end_key_;
    border_flag_ = other.border_flag_;
  }
  return ret;
}

int ObDirectLoadMultipleDatumRange::assign(const ObTabletID &tablet_id,
                                           const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), K(range));
  } else {
    reset();
    if (OB_FAIL(
          start_key_.assign(tablet_id, range.start_key_.datums_, range.start_key_.datum_cnt_))) {
      LOG_WARN("fail to assign rowkey", KR(ret));
    } else if (OB_FAIL(
                 end_key_.assign(tablet_id, range.end_key_.datums_, range.end_key_.datum_cnt_))) {
      LOG_WARN("fail to assign rowkey", KR(ret));
    } else {
      border_flag_ = range.border_flag_;
    }
  }
  return ret;
}

void ObDirectLoadMultipleDatumRange::set_whole_range()
{
  start_key_.set_min_rowkey();
  end_key_.set_max_rowkey();
  border_flag_.set_all_open();
}

int ObDirectLoadMultipleDatumRange::set_tablet_whole_range(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(start_key_.set_tablet_min_rowkey(tablet_id))) {
    LOG_WARN("fail to set tablet min rowkey", KR(ret));
  } else if (OB_FAIL(end_key_.set_tablet_max_rowkey(tablet_id))) {
    LOG_WARN("fail to set tablet max rowkey", KR(ret));
  } else {
    border_flag_.set_all_open();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
