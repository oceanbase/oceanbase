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

#include "share/ob_order_perserving_encoder.h"
#include "share/schema/ob_table_param.h"
#include "storage/direct_load/ob_direct_load_easy_queue.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_mem_define.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadDMLRowHandler;
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
  static int init_enc_param(const ObColDesc &col_desc, share::ObEncParam &param);
  static int init_enc_params(const common::ObIArray<share::schema::ObColDesc> &column_descs,
                             const int64_t rowkey_column_num,
                             const sql::ObLoadDupActionType dup_action,
                             ObIArray<share::ObEncParam> &enc_params);

  typedef ObDirectLoadExternalMultiPartitionRowChunk ChunkType;
  ObDirectLoadMemContext() : datum_utils_(nullptr),
                             dml_row_handler_(nullptr),
                             file_mgr_(nullptr),
                             table_mgr_(nullptr),
                             dup_action_(sql::ObLoadDupActionType::LOAD_INVALID_MODE),
                             exe_mode_(observer::ObTableLoadExeMode::MAX_TYPE),
                             merge_count_per_round_(0),
                             max_mem_chunk_count_(0),
                             mem_chunk_size_(0),
                             heap_table_mem_chunk_size_(0),
                             total_thread_cnt_(0),
                             dump_thread_cnt_(0),
                             load_thread_cnt_(0),
                             finish_load_thread_cnt_(0),
                             running_dump_task_cnt_(0),
                             fly_mem_chunk_count_(0),
                             has_error_(false)
  {
  }

  ~ObDirectLoadMemContext();

public:
  int init();
  int init_enc_params(const sql::ObLoadDupActionType dup_acton,
                      const common::ObIArray<share::schema::ObColDesc> &column_descs);
  void reset();
  int add_tables_from_table_builder(ObIDirectLoadPartitionTableBuilder &builder);
  int add_tables_from_table_compactor(ObIDirectLoadTabletTableCompactor &compactor);
  int add_tables_from_table_array(ObDirectLoadTableHandleArray &table_array);
  int acquire_chunk(ChunkType *&chunk);
  void release_chunk(ChunkType *chunk);

public:
  static const int64_t MIN_MEM_LIMIT = 8LL * 1024 * 1024; // 8MB

public:
  ObDirectLoadTableDataDesc table_data_desc_;
  ObArray<share::ObEncParam> enc_params_; // for ObAdaptiveQS
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
  ObDirectLoadTmpFileManager *file_mgr_;
  ObDirectLoadTableManager *table_mgr_;
  sql::ObLoadDupActionType dup_action_;

  observer::ObTableLoadExeMode exe_mode_;
  int64_t merge_count_per_round_;
  int64_t max_mem_chunk_count_;
  int64_t mem_chunk_size_;
  int64_t heap_table_mem_chunk_size_;

  int64_t total_thread_cnt_; // 总的线程数目
  int64_t dump_thread_cnt_; // dump线程数目
  int64_t load_thread_cnt_; // load线程数目, 在pre_sort中没有实际意义, 只用做sample线程退出标志

  int64_t finish_load_thread_cnt_; // 已经结束的load线程数目
  int64_t running_dump_task_cnt_; // 还在运行的dump任务数目
  int64_t fly_mem_chunk_count_; // 当前存在的chunk数目, 包含还在写的和已经close的chunk

  ObDirectLoadEasyQueue<int64_t> pre_sort_chunk_queue_; // presort任务队列
  ObDirectLoadEasyQueue<ObDirectLoadMemWorker *> mem_loader_queue_; // loader任务队列
  ObMemDumpQueue mem_dump_queue_; // dump任务队列
  ObDirectLoadEasyQueue<storage::ObDirectLoadExternalMultiPartitionRowChunk *> mem_chunk_queue_; // 已经close的chunk队列

  // save result
  lib::ObMutex mutex_;
  ObDirectLoadTableHandleArray tables_handle_;

  volatile bool has_error_;
};

} // namespace storage
} // namespace oceanbase
