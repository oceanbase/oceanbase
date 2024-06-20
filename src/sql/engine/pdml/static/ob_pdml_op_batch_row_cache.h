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

#ifndef __OB_SQL_PDML_BATCH_ROW_CACHE_H__
#define __OB_SQL_PDML_BATCH_ROW_CACHE_H__

#include "lib/container/ob_se_array.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/ob_io_event_observer.h"

namespace oceanbase
{
namespace common
{
class ObNewRow;
}

namespace sql
{
struct ObDASTabletLoc;
class ObExecContext;

// 单个分区的新引擎数据缓存器
class ObPDMLOpRowIterator
{
public:
    friend class ObPDMLOpBatchRowCache;
    ObPDMLOpRowIterator() : eval_ctx_(nullptr) {}
    virtual ~ObPDMLOpRowIterator() = default;
    // 获得row_store_it_中的下一行数据
    // 返回的数据是对应存储数据的exprs
    int get_next_row(const ObExprPtrIArray &row);
    void close() { row_store_it_.reset(); }
private:
    int init_data_source(ObChunkDatumStore &row_datum_store,
                         ObEvalCtx *eval_ctx);
private:
  ObChunkDatumStore::Iterator row_store_it_;
  ObEvalCtx *eval_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObPDMLOpRowIterator);
};


// PDML缓存
// 缓存多个partition的数据
class ObPDMLOpBatchRowCache final
{
public:
  explicit ObPDMLOpBatchRowCache(ObEvalCtx *eval_ctx, ObMonitorNode &op_monitor_info);
  ~ObPDMLOpBatchRowCache();
public:
  int init(uint64_t tenant_id, int64_t part_cnt, bool with_barrier, const ObTableModifySpec &spec);
  // ObBatachRowCache 不需要支持落盘，一旦内存不足 add_row 失败，调用者会
  // 负责将缓存的所有数据插入到存储层，并释放缓存的内存。
  // @desc part_id 表示行所在分区在 location 结构中的偏移
  // @return 如果缓存满，则返回 OB_SIZE_OVERFLOW；如果缓存成功则返回OB_SUCCESS；否则返回其他类型错误
  int add_row(const ObExprPtrIArray &row, common::ObTabletID tablet_id);

  // @desc 获取当前缓存中所有 part_id
  int get_part_id_array(ObTabletIDArray &arr);

  // @desc 通过part_id获得对应分区的所有数据
  int get_row_iterator(common::ObTabletID tablet_id, ObPDMLOpRowIterator *&iterator);

  // @desc 释放 part_id 对应分区的数据和内存
  // TODO: jiangting.lk 在整个cache框架中，其实不需要一个清理单partition数据的接口
  // 只需要一个清理整个缓存状态的接口，比如reuse。每次重新填充数据的之前，调用reuse方法，
  // 清理整个cache的状态即可。
  // int free_rows(int64_t part_id);
  // @desc no row cached
  bool empty() const;
  // @desc 重置 cache状态，不会释放内存空间，方便后期对cache的重用
  int reuse_after_rows_processed();
  // @desc 释放 ObBatchRowCache 内部结构（如hashmap）占用的所有内存
  void destroy();
private:
  int init_row_store(ObChunkDatumStore *&chunk_row_store);
  int create_new_bucket(common::ObTabletID tablet_id, ObChunkDatumStore *&row_store);
  int process_dump();
  int dump_all_datum_store();
  bool need_dump() const;
  int free_datum_store_memory();
private:
  typedef common::hash::ObHashMap<common::ObTabletID, ObChunkDatumStore *,
                                  common::hash::NoPthreadDefendMode> PartitionStoreMap;
  // HashMap: part_id => chunk_datum_store_ ptr
  // dynamic add row store to map, when meet a new partition
  common::ObArenaAllocator row_allocator_; // 用于给 cache 里的 store 分配内存
  ObEvalCtx *eval_ctx_;
  PartitionStoreMap pstore_map_;
  ObPDMLOpRowIterator iterator_; // 用于构造对应的iter
  int64_t cached_rows_num_; // 表示当前cache的row的行数
  int64_t cached_rows_size_; // bytes cached. used to control max memory used by batchRowCache
  int64_t cached_in_mem_rows_num_; // 表示当前cache的in memory row的行数，不包含已 dump 的行
  uint64_t tenant_id_;
  bool with_barrier_;

  lib::MemoryContext mem_context_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObIOEventObserver io_event_observer_;
  DISALLOW_COPY_AND_ASSIGN(ObPDMLOpBatchRowCache);
};


class ObDMLOpDataReader
{
public:
  // 从 DML 算子中读入一行数据，一般是来自 child op
  // 同时，负责计算出这一行数据所属的分区。
  // 一般来说，分区id存储在行中的一个伪列里
  virtual int read_row(ObExecContext &ctx,
                       const ObExprPtrIArray *&row,
                       common::ObTabletID &tablet_id,
                       bool &is_skipped) = 0;
};

class ObDMLOpDataWriter
{
public:
  // 将数据批量写入到存储层
  virtual int write_rows(ObExecContext &ctx,
                         const ObDASTabletLoc *tablet_loc,
                         ObPDMLOpRowIterator &iterator) = 0;
};

}
}
#endif /* __OB_SQL_PDML_BATCH_ROW_CACHE_H__ */
//// end of header file

