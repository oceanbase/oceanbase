/**
 * Copyright (c) 2023 OceanBase
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

#include "storage/direct_load/ob_direct_load_mem_context.h"
#include "storage/direct_load/ob_direct_load_mem_loader.h"
#include "storage/direct_load/ob_direct_load_mem_dump.h"

namespace oceanbase
{
namespace storage
{

int ObMemDumpQueue::push(void *p)
{
  int ret = OB_SUCCESS;
  Item *item = op_alloc_args(Item, p);
  int32_t count = 0;
  while (OB_SUCC(ret)) {
    ret = queue_.push(item, 10000000);
    if (OB_TIMEOUT == ret) {
      ret = OB_SUCCESS;
      count ++;
      STORAGE_LOG(WARN, "the push operation has been timeout n times", K(count));
      continue;
    } else if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "fail to push item", KR(ret));
    } else {
      break;
    }
  }
  return ret;
}

int ObMemDumpQueue::pop(void *&p)
{
  int ret = OB_SUCCESS;
  void *tmp = nullptr;
  int32_t count = 0;
  while (OB_SUCC(ret)) {
    ret = queue_.pop(tmp, 10000000);
    if (OB_SUCCESS == ret) {
      Item *item_ptr = (Item *)tmp;
      p = item_ptr->ptr_;
      op_free(item_ptr);
      break;
    } else if (ret == OB_ENTRY_NOT_EXIST) { //queue超时返回的错误码是这个，只能将错就错了
      ret = OB_SUCCESS; //防止超时
      count ++;
      STORAGE_LOG(WARN, "the pop operation has been timeout n times", K(count));
      continue;
    } else {
      STORAGE_LOG(WARN, "fail to pop queue", KR(ret));
    }
  }
  return ret;
}

ObMemDumpQueue::~ObMemDumpQueue()
{
  int ret = OB_SUCCESS;
  int64_t queue_size = queue_.size();
  if (queue_size > 0) {
    STORAGE_LOG(ERROR, "mem dump queue should be empty", K(queue_size));
  }
  for (int64_t i = 0; i < queue_size; i ++) {
    void *tmp = nullptr;
    queue_.pop(tmp);
    if (tmp != nullptr) {
      Item *item_ptr = (Item *)tmp;
      op_free(item_ptr);
    }
  }
}

void ObDirectLoadMemContext::reset()
{
  table_data_desc_.reset();
  datum_utils_ = nullptr;
  need_sort_ = false;
  mem_load_task_count_ = 0;
  column_count_ = 0;
  file_mgr_ = nullptr;
  fly_mem_chunk_count_ = 0;
  finish_compact_count_ = 0;
  mem_dump_task_count_ = 0;
  has_error_ = false;

  ObArray<ObDirectLoadMemWorker *> loader_array;
  mem_loader_queue_.pop_all(loader_array);
  for (int64_t i = 0; i < loader_array.count(); i ++) {
    ObDirectLoadMemWorker *tmp = loader_array.at(i);
    if (tmp != nullptr) {
      tmp->~ObDirectLoadMemWorker(); //是由area_allocator分配的，所以不需要free
    }
  }

  ObArray<ObDirectLoadExternalMultiPartitionRowChunk *> chunk_array;
  mem_chunk_queue_.pop_all(chunk_array);
  for (int64_t i = 0; i < chunk_array.count(); i ++) {
    ObDirectLoadExternalMultiPartitionRowChunk *chunk = chunk_array.at(i);
    if (chunk != nullptr) {
      chunk->~ObDirectLoadExternalMultiPartitionRowChunk();
      ob_free(chunk);
    }
  }

  int64_t queue_size = mem_dump_queue_.size();
  for (int64_t i = 0; i < queue_size; i ++) {
    void *p = nullptr;
    mem_dump_queue_.pop(p);
    if (p != nullptr) {
      ObDirectLoadMemDump *mem_dump = (ObDirectLoadMemDump *)p;
      mem_dump->~ObDirectLoadMemDump();
      ob_free(mem_dump);
    }
  }

  for (int64_t i = 0; i < tables_.count(); i ++) {
    ObIDirectLoadPartitionTable *table = tables_.at(i);
    if (table != nullptr) {
      table->~ObIDirectLoadPartitionTable(); //table是由allocator_分配的，是area allocator，不用free
    }
  }
  tables_.reset();
  allocator_.reset();
}

ObDirectLoadMemContext::~ObDirectLoadMemContext()
{
  reset();
}

int ObDirectLoadMemContext::init()
{
  int ret = OB_SUCCESS;
  allocator_.set_tenant_id(MTL_ID());
  if (OB_FAIL(mem_dump_queue_.init(1024))) {
    STORAGE_LOG(WARN, "fail to init mem dump queue", KR(ret));
  }
  return ret;
}

int ObDirectLoadMemContext::add_tables_from_table_builder(ObIDirectLoadPartitionTableBuilder &builder)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  ObArray<ObIDirectLoadPartitionTable *> table_array;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(builder.get_tables(table_array, allocator_))) {
      LOG_WARN("fail to get tables", KR(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); i ++) {
    if (OB_FAIL(tables_.push_back(table_array.at(i)))) {
      LOG_WARN("fail to push table", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadMemContext::add_tables_from_table_compactor(
  ObIDirectLoadTabletTableCompactor &compactor)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  ObIDirectLoadPartitionTable *table = nullptr;
  if (OB_FAIL(compactor.get_table(table, allocator_))) {
    LOG_WARN("fail to get table", KR(ret));
  } else if (OB_FAIL(tables_.push_back(table))) {
    LOG_WARN("fail to push table", KR(ret));
  }
  return ret;
}

}
}
