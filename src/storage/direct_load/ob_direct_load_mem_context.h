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
#ifndef OB_DIRECT_LOAD_MEM_CONTEXT_H_
#define OB_DIRECT_LOAD_MEM_CONTEXT_H_

#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_easy_queue.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_mem_define.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadMemWorker;

class ObMemDumpQueue
{
  struct Item
  {
    void *ptr_;
    Item() : ptr_(nullptr) {}
    Item(void *ptr) : ptr_(ptr) {}
  };
public:
  ~ObMemDumpQueue();
  int push(void *p);
  int pop(void *&p);
  int init(int64_t capacity) {
    return queue_.init(capacity);
  }
  int64_t size() const {
    return queue_.size();
  }
private:
  common::LightyQueue queue_;
};



class ObDirectLoadMemContext
{
public:
  ObDirectLoadMemContext() : datum_utils_(nullptr),
                             need_sort_(false),
                             mem_load_task_count_(0),
                             column_count_(0),
                             dml_row_handler_(nullptr),
                             file_mgr_(nullptr),
                             fly_mem_chunk_count_(0),
                             finish_compact_count_(0),
                             mem_dump_task_count_(0),
                             running_dump_count_(0),
                             allocator_("TLD_mem_ctx"),
                             has_error_(false) {}

  ~ObDirectLoadMemContext();

public:
  int init();
  void reset();
  int add_tables_from_table_builder(ObIDirectLoadPartitionTableBuilder &builder);
  int add_tables_from_table_compactor(ObIDirectLoadTabletTableCompactor &compactor);

public:
  static const int64_t MIN_MEM_LIMIT = 8LL * 1024 * 1024; // 8MB

public:

  ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  bool need_sort_; // false: sstable, true: external_table
  int32_t mem_load_task_count_;
  int32_t column_count_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
  ObDirectLoadTmpFileManager *file_mgr_;
  sql::ObLoadDupActionType dup_action_;
  ObDirectLoadEasyQueue<storage::ObDirectLoadExternalMultiPartitionRowChunk *> mem_chunk_queue_;
  int64_t fly_mem_chunk_count_;

  ObDirectLoadEasyQueue<ObDirectLoadMemWorker *> mem_loader_queue_;
  int64_t finish_compact_count_;
  int64_t mem_dump_task_count_;
  int64_t running_dump_count_;
  ObMemDumpQueue mem_dump_queue_;

  ObArenaAllocator allocator_;
  ObArray<ObIDirectLoadPartitionTable *> tables_;
  lib::ObMutex mutex_;

  volatile bool has_error_;
};

}
}

#endif /* OB_DIRECT_LOAD_MEM_CONTEXT_H_ */
