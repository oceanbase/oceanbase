// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

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
    ObOptColumnStat *col_stat = column_stat_array_.at(i);
    col_stat->~ObOptColumnStat();
    col_stat = nullptr;
  }
}

int ObDirectLoadFastHeapTable::copy_col_stat(const ObDirectLoadFastHeapTableCreateParam &param)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret)&& i < param.column_stat_array_->count(); ++i) {
    ObOptColumnStat *col_stat = param.column_stat_array_->at(i);
    if (col_stat != nullptr) {
      ObOptColumnStat *copied_col_stat = nullptr;
      int64_t size = col_stat->size();
      char *new_buf = nullptr;
      if (OB_ISNULL(new_buf = static_cast<char *>(allocator_.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate buffer", KR(ret), K(size));
      } else if (OB_FAIL(col_stat->deep_copy(new_buf, size, copied_col_stat))) {
        LOG_WARN("fail to copy colstat", KR(ret));
      } else if (OB_FAIL(column_stat_array_.push_back(copied_col_stat))) {
        LOG_WARN("fail to add table", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (copied_col_stat != nullptr) {
          copied_col_stat->~ObOptColumnStat();
          copied_col_stat = nullptr;
        }
        if(new_buf != nullptr) {
          allocator_.free(new_buf);
          new_buf = nullptr;
        }
      }
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
