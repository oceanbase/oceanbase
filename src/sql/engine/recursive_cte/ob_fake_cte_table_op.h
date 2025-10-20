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

#ifndef _OB_FAKE_CTE_TABLE_OP_H
#define _OB_FAKE_CTE_TABLE_OP_H 1

#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "ob_search_method_op.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_ra_datum_store.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

class ObExecContext;
class ObFakeCTETableSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObFakeCTETableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
      : ObOpSpec(alloc, type), column_involved_offset_(alloc), column_involved_exprs_(alloc),
        is_bulk_search_(false), identify_seq_expr_(nullptr), is_union_distinct_(false)
  {
  }

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(column_involved_offset), K_(column_involved_exprs),
                       K_(is_bulk_search), K_(identify_seq_expr), K_(is_union_distinct));

  virtual ~ObFakeCTETableSpec() {}

  //数组下标指的是output_里面的下标，数组内容是在cte表中原始列的偏移位置
  common::ObFixedArray<int64_t, common::ObIAllocator> column_involved_offset_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> column_involved_exprs_;
  //for breadth search first
  bool is_bulk_search_;
  ObExpr *identify_seq_expr_;
  bool is_union_distinct_;
};

class ObFakeCTETableOp : public ObOperator
{
public:
  explicit ObFakeCTETableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
      : ObOperator(exec_ctx, spec, input),
        empty_(false),
        pump_row_(nullptr),
        read_bluk_cnt_(0),
        cur_identify_seq_(0),
        bulk_rows_(),
        next_read_row_id_(0),
        round_limit_(0),
        intermedia_table_(&allocator_),
        intermedia_data_reader_(intermedia_table_),
        mem_context_(nullptr),
        profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
        sql_mem_processor_(profile_, op_monitor_info_)
    {
    }
  common::ObArray<ObChunkDatumStore::StoredRow *> &get_bulk_rows() { return bulk_rows_; };
  void reuse();
  virtual void destroy();
  virtual int inner_rescan() override;
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  inline void set_empty() { empty_ = true; }
  inline void update_status()
  {
    read_bluk_cnt_ = 0;
    if (MY_SPEC.column_involved_offset_.empty()) {
      empty_ = false;
    } else if (!bulk_rows_.empty()) {
      empty_ = false;
    }
  }
  int get_next_single_row();
  int get_next_bulk_row();
  int get_next_single_batch(const int64_t max_row_cnt);
  int get_next_bulk_batch(const int64_t max_row_cnt);
  /*
   * @Brief: Get data from intermedia table which might be in disk
   *         Called by consumer -- CTE operator
   */
  int get_next_batch_from_intermedia_table(const int64_t max_row_cnt, bool is_called_by_get_next_row_interface);
  int add_single_row(ObChunkDatumStore::StoredRow *row);
  /*
   * @Brief: Write data into intermedia table which might be dumped to disk
   *         Called by producer -- Recursive Union operator
   */
  int add_single_row_to_intermedia_table(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx *ctx);
  int copy_datums(ObChunkDatumStore::StoredRow *row, common::ObDatum *datums,
                  int64_t cnt, const common::ObIArray<int64_t> &chosen_datums,
                  char *buf, const int64_t size, const int64_t row_size,
                  const uint32_t row_extend_size);
  //从src_row中拷贝chosen_index中包含的cell到dst_row中
  int deep_copy_row(const ObChunkDatumStore::StoredRow *src_row,
                    const ObChunkDatumStore::StoredRow *&dst_row,
                    const common::ObIArray<int64_t> &chosen_index,
                    int64_t extra_size,
                    common::ObIAllocator &allocator);
  int to_expr(const common::ObIArray<ObExpr*> &exprs,
              const common::ObIArray<int64_t> &chosen_datums,
              ObChunkDatumStore::StoredRow *row, ObEvalCtx &ctx);

  int to_expr(const common::ObIArray<ObExpr *> &exprs,
              const common::ObIArray<int64_t> &chosen_index, ObRADatumStore::StoredRow *row,
              ObEvalCtx &ctx);
  int attach_rows(const common::ObIArray<ObExpr*> &exprs,
                  const common::ObIArray<int64_t > &chosen_index,
                  const common::ObArray<ObChunkDatumStore::StoredRow *> &srows,
                  const int64_t rows_offset, ObEvalCtx &ctxm, const int64_t read_rows);

  inline ObRADatumStore *get_intermedia_table()
  {
    return &intermedia_table_;
  }
  inline bool is_no_more_data()
  {
    return next_read_row_id_ == intermedia_data_reader_.get_row_cnt();
  }

  inline int64_t get_intermedia_table_row_size()
  {
    return intermedia_data_reader_.get_row_cnt();
  }

  inline void update_round_limit()
  {
    round_limit_ = intermedia_table_.get_row_cnt() - 1;
  }

  inline int64_t get_round_limit()
  {
    return round_limit_;
  }

  const static int64_t ROW_EXTRA_SIZE = 0;

private:
  int init_mem_context();

  void destroy_mem_context();

  inline bool need_dump() const
  {
    return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound();
  }

  int process_dump();

  class ObLamdaSubstituteRCTEMaxAvailableMemChecker
  {
  public:
    explicit ObLamdaSubstituteRCTEMaxAvailableMemChecker(int64_t cur_cnt) : cur_cnt_(cur_cnt)
    {}
    bool operator()(int64_t max_row_count)
    {
      return cur_cnt_ > max_row_count;
    }

  private:
    int64_t cur_cnt_;
  };

  class ObLamdaSubstituteRCTEExtendMaxMemChecker
  {
  public:
    explicit ObLamdaSubstituteRCTEExtendMaxMemChecker(int64_t cur_memory_size) : cur_memory_size_(cur_memory_size)
    {}
    bool operator()(int64_t max_memory_size)
    {
      return cur_memory_size_ > max_memory_size;
    }

  private:
    int64_t cur_memory_size_;
  };

private:
  bool empty_;
  const ObChunkDatumStore::StoredRow* pump_row_;
  ObMalloc allocator_;
  // for batch search recursive cte
  int64_t read_bluk_cnt_;
  int64_t cur_identify_seq_;
  common::ObArray<ObChunkDatumStore::StoredRow *> bulk_rows_;

  //Below variables only used in mysql mode
  //Used to support data temperory disk storage
  int64_t next_read_row_id_;                            //next_read_row_id_ is not always equal to intermedia_data_reader_.get_row_cnt()
                                                        //because next_read_row_id_ represent current read data size by *CTE operator*
                                                        //while intermedia_data_reader_.get_row_cnt() is incresing when *Recursive Union Operator* appending data into it
  
  int64_t round_limit_;                                 //round_limit_ is the upper bound of next_read_row_id_ in a round of iteration
                                                        //next_read_row_id_ should never exceed round_limit_
                                                        //because the data after that is not this round of data
                                                        //for more detail to see explanation below

  ObRADatumStore intermedia_table_;                     //CTE Table

  // Suppose we are computing data of round 3 base on round 2 CTE table data
  // From the very beginning round_limit_ == intermedia_data_reader_.get_row_cnt() - 1
  // Then Recursive Union Operator begin execute, it needs to :
  // 1. use *next_read_row_id_* to get data from child operator
  //    and 
  // 2. keep appending data into CTE table which increase intermedia_data_reader_.get_row_cnt()
  // So the general state of these variables at execution time is as follows:
  //
  //                            intermedia_data_reader_.get_row_cnt()
  //     next_read_row_id_               round_limit_             
  //           |                              |                          
  //  ---------v------------------------------v---------------------------------
  // | round 1 |          round 2             |           round 3    ...
  //  --------------------------------------------------------------------------
  //                         intermedia_table_
  //
  //                                |
  //                            executing
  //                                |
  //                                v
  //
  //               next_read_row_id_     round_limit_     intermedia_data_reader_.get_row_cnt()        
  //                      |                   |                          |
  //  --------------------v-------------------v--------------------------v------
  // | round 1 |          round 2             |           round 3    ...
  //  --------------------------------------------------------------------------
  //                         intermedia_table_
  //
  //                                |
  //                   end of this round of execution
  //                                |
  //                                v
  //                                                                    intermedia_data_reader_.get_row_cnt()    
  //                                    next_read_row_id_                             round_limit_
  //                                          |                                            |
  //  ----------------------------------------v--------------------------------------------v--------------
  // | round 1 |          round 2             |                    round 3                 |  round 4...
  //  ----------------------------------------------------------------------------------------------------
  //                         intermedia_table_
  //
  // After a round of iteration end, Recursive Union Operator will set 'round_limit_ = intermedia_data_reader_.get_row_cnt()'
  // then start a new round of iteration

 
  ObRADatumStore::Reader intermedia_data_reader_;       //intermedia_data_reader_ used to read data from intermedia_table_
  ObRADatumStore::IterationAge reader_age_;             //make sure every batch of CTE data remain in memory 

  //Automatic memory management
  lib::MemoryContext mem_context_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
};
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_FAKE_CTE_TABLE_OP_H */
