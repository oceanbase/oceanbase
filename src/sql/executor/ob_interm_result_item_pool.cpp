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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_interm_result_item_pool.h"
#include "sql/executor/ob_task_event.h"
#include "lib/alloc/alloc_func.h"
using namespace oceanbase::common;
namespace oceanbase {
namespace sql {
ObIntermResultItemPool* ObIntermResultItemPool::instance_ = NULL;

ObIntermResultItemPool::ObIntermResultItemPool() : inited_(false), mem_item_allocator_(), disk_item_allocator_()
{}

ObIntermResultItemPool::~ObIntermResultItemPool()
{
  reset();
}

void ObIntermResultItemPool::reset()
{
  int ret = OB_SUCCESS;
  inited_ = false;
  if (OB_FAIL(mem_item_allocator_.destroy())) {
    LOG_ERROR("fail to destroy allocator", K(ret));
  } else if (OB_FAIL(disk_item_allocator_.destroy())) {
    LOG_ERROR("fail to destroy allocator", K(ret));
  }
}

int ObIntermResultItemPool::build_instance()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != instance_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("instance is not NULL, build twice", K(ret));
  } else if (OB_ISNULL(instance_ = OB_NEW(ObIntermResultItemPool, ObModIds::OB_SQL_EXECUTOR))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("instance is NULL, unexpected", K(ret));
  } else if (OB_FAIL(instance_->init())) {
    instance_->reset();
    OB_DELETE(ObIntermResultItemPool, ObModIds::OB_SQL_EXECUTOR, instance_);
    instance_ = NULL;
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to init scanner pool", K(ret));
  } else {
  }
  return ret;
}

ObIntermResultItemPool* ObIntermResultItemPool::get_instance()
{
  ObIntermResultItemPool* instance = NULL;
  if (OB_ISNULL(instance_) || OB_UNLIKELY(!instance_->inited_)) {
    LOG_ERROR("instance is NULL or not inited", K(instance_));
  } else {
    instance = instance_;
  }
  return instance;
}

int ObIntermResultItemPool::init()
{
  const static int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(mem_item_allocator_.init(sizeof(ObIntermResultItem),
                 ObModIds::OB_SQL_EXECUTOR,
                 OB_SERVER_TENANT_ID,
                 block_size,
                 1,
                 get_capacity()))) {
    LOG_WARN("fail to init allocator", K(ret), "capacity", get_capacity());
  } else if (OB_FAIL(disk_item_allocator_.init(sizeof(ObDiskIntermResultItem),
                 ObModIds::OB_SQL_EXECUTOR,
                 OB_SERVER_TENANT_ID,
                 block_size,
                 1,
                 get_capacity()))) {
    LOG_WARN("fail to init allocator", K(ret), "capacity", get_capacity());
  } else {
    inited_ = true;
    LOG_INFO("initialize scanner pool", "size", get_capacity());
  }
  return ret;
}

int ObIntermResultItemPool::alloc_mem_item(ObIntermResultItem*& ir_item, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void* ir_item_ptr = NULL;

  if (OB_ISNULL(ir_item_ptr = mem_item_allocator_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc scanner from obj pool", K(ret));
  } else if (OB_ISNULL(ir_item = new (ir_item_ptr)
                           ObIntermResultItem(ObModIds::OB_SQL_EXECUTOR_INTERM_RESULT_ITEM, tenant_id))) {
    LOG_WARN("fail to new ObIntermResultItem", K(ret), K(tenant_id));
  }

  return ret;
}

int ObIntermResultItemPool::alloc_disk_item(ObDiskIntermResultItem*& item, const uint64_t tenant_id, const int64_t fd,
    const int64_t dir_id, const int64_t offset)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    void* mem = disk_item_allocator_.alloc();
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to alloc disk interim result item", K(ret));
    } else {
      ObDiskIntermResultItem* it = new (mem) ObDiskIntermResultItem();
      if (OB_FAIL(it->init(tenant_id, fd, dir_id, offset))) {
        LOG_WARN("init disk interim result item failed", K(ret), K(tenant_id), K(fd), K(offset));
        it->~ObDiskIntermResultItem();
        it = NULL;
        disk_item_allocator_.free(mem);
        mem = NULL;
      } else {
        item = it;
      }
    }
  }
  return ret;
}

int ObIntermResultItemPool::free(ObIIntermResultItem* item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(item)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("interm result item is NULL", K(ret));
  } else {
    ObSmallAllocator& allocator = get_allocator(item->in_memory());
    item->~ObIIntermResultItem();
    allocator.free(item);
    item = NULL;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
