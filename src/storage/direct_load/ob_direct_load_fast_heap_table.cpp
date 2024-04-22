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

#include "storage/direct_load/ob_direct_load_fast_heap_table.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

/**
 * ObDirectLoadFastHeapTableCreateParam
 */

ObDirectLoadFastHeapTableCreateParam::ObDirectLoadFastHeapTableCreateParam()
  : row_count_(0)
{
}

ObDirectLoadFastHeapTableCreateParam::~ObDirectLoadFastHeapTableCreateParam()
{
}

bool ObDirectLoadFastHeapTableCreateParam::is_valid() const
{
  return tablet_id_.is_valid() && row_count_ >= 0;
}

/**
 * ObDirectLoadFastHeapTable
 */

ObDirectLoadFastHeapTable::ObDirectLoadFastHeapTable()
  : is_inited_(false)
{
}

ObDirectLoadFastHeapTable::~ObDirectLoadFastHeapTable()
{
}

int ObDirectLoadFastHeapTable::init(const ObDirectLoadFastHeapTableCreateParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadFastHeapTable init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    meta_.tablet_id_ = param.tablet_id_;
    meta_.row_count_ = param.row_count_;
    is_inited_ = true;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
