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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/sort/ob_sort_round.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::storage;

ObSortRun::ObSortRun()
    : writer_(),
      iters_(),
      is_inited_(false),
      buf_size_(0),
      expire_timestamp_(0),
      allocator_(NULL),
      tenant_id_(OB_INVALID_TENANT_ID),
      dir_id_(-1),
      is_writer_opened_(false)
{}

ObSortRun::~ObSortRun()
{}

void ObSortRun::reset()
{
  is_inited_ = false;
  buf_size_ = 0;
  expire_timestamp_ = 0;
  allocator_ = NULL;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_writer_opened_ = false;
}

int ObSortRun::init(
    const int64_t buf_size, const int64_t expire_timestamp, ObIAllocator* allocator, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sort run has been inited", K(ret));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id_))) {
    STORAGE_LOG(WARN, "fail to alloc dir", K(ret));
  } else {
    is_inited_ = true;
    buf_size_ = buf_size;
    expire_timestamp_ = expire_timestamp;
    allocator_ = allocator;
    tenant_id_ = tenant_id;
    is_writer_opened_ = false;
  }
  return ret;
}

// open writer
int ObSortRun::reuse()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort run has not been inited", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < iters_.count(); ++i) {
      if (NULL != iters_.at(i)) {
        if (common::OB_SUCCESS != (tmp_ret = iters_.at(i)->clean_up())) {
          LOG_WARN("fail to do reader clean up", K(tmp_ret), K(i));
        }
        iters_.at(i)->~ObFragmentReaderV2();
      }
    }
    iters_.reset();
    if (OB_FAIL(writer_.sync())) {
      LOG_WARN("finish writer failed", K(ret));
    } else {
      writer_.reset();
      is_writer_opened_ = false;
      if (!is_writer_opened_ && OB_FAIL(writer_.open(buf_size_, expire_timestamp_, tenant_id_, dir_id_))) {
        LOG_WARN("failed to open writer", K(ret));
      } else {
        is_writer_opened_ = true;
      }
    }
  }
  return ret;
}

int ObSortRun::clean_up()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (NULL != iters_.at(i)) {
      if (common::OB_SUCCESS != (tmp_ret = iters_.at(i)->clean_up())) {
        LOG_WARN("fail to do reader clean up", K(tmp_ret), K(i));
      }
      iters_.at(i)->~ObFragmentReaderV2();
    }
  }
  iters_.reset();
  if (OB_FAIL(writer_.sync())) {
    LOG_WARN("finish writer failed", K(ret));
  } else {
    writer_.reset();
    is_writer_opened_ = false;
  }
  is_inited_ = false;
  buf_size_ = 0;
  expire_timestamp_ = 0;
  allocator_ = NULL;
  return ret;
}

int ObSortRun::prefetch(FragmentIteratorList& iters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < iters.count(); ++i) {
    if (OB_ISNULL(iters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null", K(ret));
    } else if (OB_FAIL(iters.at(i)->open())) {
      STORAGE_LOG(WARN, "fail to prefetch", K(ret));
    }
  }
  return ret;
}

int ObSortRun::add_item(const common::ObNewRow& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSortRun has not been inited", K(ret));
  } else if (ObExternalSortConstant::is_timeout(expire_timestamp_)) {
    ret = common::OB_TIMEOUT;
    STORAGE_LOG(WARN, "ObSortRun timeout", K(ret), K(expire_timestamp_));
  } else if (!is_writer_opened_ && OB_FAIL(writer_.open(buf_size_, expire_timestamp_, tenant_id_, dir_id_))) {
    LOG_WARN("failed to open writer", K(ret));
  } else {
    is_writer_opened_ = true;
    if (OB_FAIL(writer_.write_item(item))) {
      STORAGE_LOG(WARN, "fail to write item", K(ret));
    }
  }
  return ret;
}

int ObSortRun::build_fragment()
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  FragmentReader* reader = NULL;
  ret = E(EventTable::EN_MERGE_SORT_READ_MSG) OB_SUCCESS;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(FragmentReader)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc size for FragmentReader failed", K(ret));
  } else if (OB_ISNULL(reader = new (buf) FragmentReader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader is null", K(ret));
  } else if (OB_FAIL(writer_.sync())) {
    LOG_WARN("fail to sync macro file", K(ret));
  } else if (OB_FAIL(reader->init(
                 writer_.get_fd(), dir_id_, expire_timestamp_, tenant_id_, writer_.get_sample_item(), buf_size_))) {
    LOG_WARN("init read failed", K(ret), K(writer_.get_fd()));
  } else if (OB_FAIL(iters_.push_back(reader))) {
    reader->clean_up();
    reader->~ObFragmentReaderV2();
    LOG_WARN("fail to push back reader", K(ret));
  } else {
    // do nothing
    writer_.reset();
    is_writer_opened_ = false;
  }
  return ret;
}
