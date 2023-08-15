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
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

/**
 * ObDirectLoadFastHeapTableCreateParam
 */

ObDirectLoadFastHeapTableCreateParam::ObDirectLoadFastHeapTableCreateParam()
  : row_count_(0) , column_stat_array_(nullptr)
{
}

ObDirectLoadFastHeapTableCreateParam::~ObDirectLoadFastHeapTableCreateParam()
{
}

bool ObDirectLoadFastHeapTableCreateParam::is_valid() const
{
  return tablet_id_.is_valid() && row_count_ >= 0 && nullptr != column_stat_array_ ;
}

/**
 * ObDirectLoadFastHeapTable
 */

ObDirectLoadFastHeapTable::ObDirectLoadFastHeapTable()
  : allocator_("TLD_FastHTable"), is_inited_(false)
{
}

ObDirectLoadFastHeapTable::~ObDirectLoadFastHeapTable()
{
  for (int64_t i = 0; i < column_stat_array_.count(); ++i) {
    ObOptOSGColumnStat *col_stat = column_stat_array_.at(i);
    col_stat->~ObOptOSGColumnStat();
    col_stat = nullptr;
  }
}

int ObDirectLoadFastHeapTable::copy_col_stat(const ObDirectLoadFastHeapTableCreateParam &param)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret)&& i < param.column_stat_array_->count(); ++i) {
    ObOptOSGColumnStat *col_stat = param.column_stat_array_->at(i);
    ObOptOSGColumnStat *copied_col_stat = NULL;
    if (OB_ISNULL(col_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null");
    } else if (OB_ISNULL(copied_col_stat = ObOptOSGColumnStat::create_new_osg_col_stat(allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory");
    } else if (OB_FAIL(copied_col_stat->deep_copy(*col_stat))) {
      LOG_WARN("fail to copy colstat", KR(ret));
    } else if (OB_FAIL(column_stat_array_.push_back(copied_col_stat))) {
      LOG_WARN("fail to add table", KR(ret));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(copied_col_stat)) {
      copied_col_stat->~ObOptOSGColumnStat();
      copied_col_stat = nullptr;
    }
  }
  return ret;
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
    allocator_.set_tenant_id(MTL_ID());
    if (OB_FAIL(copy_col_stat(param))){
      LOG_WARN("fail to inner init", KR(ret), K(param));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
