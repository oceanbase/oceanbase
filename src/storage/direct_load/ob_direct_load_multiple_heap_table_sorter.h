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
#ifndef OB_DIRECT_LOAD_MULTIPLE_HEAP_TABLE_SORTER_H_
#define OB_DIRECT_LOAD_MULTIPLE_HEAP_TABLE_SORTER_H_

#include "storage/direct_load/ob_direct_load_external_fragment.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"
#include "storage/direct_load/ob_direct_load_mem_worker.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadMultipleHeapTable;
class ObDirectLoadMultipleHeapTableMap;

class ObDirectLoadMultipleHeapTableSorter : public ObDirectLoadMemWorker
{
public:
  ObDirectLoadMultipleHeapTableSorter(ObDirectLoadMemContext *mem_ctx);
  virtual ~ObDirectLoadMultipleHeapTableSorter();

  int init();
  int add_table(ObIDirectLoadPartitionTable *table) override;
  void set_work_param(int64_t index_dir_id, int64_t data_dir_id,
                      common::ObIArray<ObDirectLoadMultipleHeapTable *> &heap_table_array,
                      common::ObIAllocator &heap_table_allocator)
  {
    index_dir_id_ = index_dir_id;
    data_dir_id_ = data_dir_id;
    heap_table_array_ = &heap_table_array;
    heap_table_allocator_ = &heap_table_allocator;
  }
  int work() override;
  VIRTUAL_TO_STRING_KV(KP(mem_ctx_), K_(fragments));

private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadMultipleHeapTableSorter);

  int close_chunk(ObDirectLoadMultipleHeapTableMap *&chunk);
  int get_tables(ObIDirectLoadPartitionTableBuilder &table_builder);

private:
  // data members
  ObDirectLoadMemContext *mem_ctx_;
  ObDirectLoadExternalFragmentArray fragments_;
  ObArenaAllocator allocator_;
  char *extra_buf_;
  int64_t index_dir_id_;
  int64_t data_dir_id_;
  common::ObIArray<ObDirectLoadMultipleHeapTable *> *heap_table_array_;
  common::ObIAllocator *heap_table_allocator_;
};

}
}

#endif /* OB_DIRECT_LOAD_MULTIPLE_HEAP_TABLE_SORTER_H_ */
