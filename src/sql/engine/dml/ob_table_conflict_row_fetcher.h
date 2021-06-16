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

#ifndef OBDEV_SRC_SQL_ENGINE_DML_OB_TABLE_CONFLICT_ROW_FETCHER_H_
#define OBDEV_SRC_SQL_ENGINE_DML_OB_TABLE_CONFLICT_ROW_FETCHER_H_
#include "sql/engine/ob_no_children_phy_operator.h"
#include "common/ob_partition_key.h"
#include "common/row/ob_row_store.h"
namespace oceanbase {
namespace share {
class ObPartitionReplicaLocation;
}
namespace storage {
class ObDMLBaseParam;
}
namespace sql {
class ObPhyTableLocation;

struct ObPartConflictRowStore {
  OB_UNIS_VERSION(1);

public:
  ObPartConflictRowStore() : part_key_(), conflict_row_store_(NULL)
  {}
  TO_STRING_KV(K_(part_key), KPC_(conflict_row_store));

  common::ObPartitionKey part_key_;
  common::ObRowStore* conflict_row_store_;
};

class ObTCRFetcherInput : public ObIPhyOperatorInput {
  friend class ObTableConflictRowFetcher;
  OB_UNIS_VERSION(1);

public:
  ObTCRFetcherInput() : ObIPhyOperatorInput(), part_conflict_rows_(), allocator_(NULL)
  {}
  virtual ~ObTCRFetcherInput()
  {}
  virtual void reset() override
  {
    part_conflict_rows_.reset();
  }
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_TABLE_CONFLICT_ROW_FETCHER;
  }
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  void set_deserialize_allocator(common::ObIAllocator* allocator);

private:
  common::ObSEArray<ObPartConflictRowStore, 4> part_conflict_rows_;
  common::ObIAllocator* allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTCRFetcherInput);
};

class ObTableConflictRowFetcher : public ObNoChildrenPhyOperator {
  OB_UNIS_VERSION(1);

public:
  class ObTCRFetcherCtx : public ObPhyOperatorCtx {
    friend class ObTableConflictRowFetcher;

  public:
    explicit ObTCRFetcherCtx(ObExecContext& ctx)
        : ObPhyOperatorCtx(ctx), duplicated_iter_array_(), curr_row_index_(0), curr_rowkey_id_(0)
    {}
    ~ObTCRFetcherCtx()
    {}
    virtual void destroy() override
    {
      ObPhyOperatorCtx::destroy_base();
      duplicated_iter_array_.reset();
    }

  public:
    common::ObSEArray<common::ObNewRowIterator*, 4> duplicated_iter_array_;
    int64_t curr_row_index_;
    int64_t curr_rowkey_id_;
  };

  class ObConflictRowIterator : public common::ObNewRowIterator {
  public:
    ObConflictRowIterator(common::ObRowStore::Iterator iter) : row_iter_(iter)
    {}
    virtual int get_next_row(common::ObNewRow*& row) override
    {
      return row_iter_.get_next_row(row, NULL);
    }
    virtual void reset() override
    {
      row_iter_.reset();
    }

  private:
    common::ObRowStore::Iterator row_iter_;
  };

public:
  explicit ObTableConflictRowFetcher(common::ObIAllocator& alloc);
  ~ObTableConflictRowFetcher();
  void set_table_id(uint64_t table_id)
  {
    table_id_ = table_id;
  }
  void set_index_tid(uint64_t index_tid)
  {
    index_tid_ = index_tid;
  }
  void set_only_data_table(bool only_data_table)
  {
    only_data_table_ = only_data_table;
  }
  int init_conflict_column_ids(int64_t column_cnt)
  {
    return conflict_column_ids_.init(column_cnt);
  }
  int init_access_column_ids(int64_t column_cnt)
  {
    return access_column_ids_.init(column_cnt);
  }
  int add_conflict_column_ids(uint64_t column_id)
  {
    return conflict_column_ids_.push_back(column_id);
  }
  int add_access_column_ids(uint64_t column_id)
  {
    return access_column_ids_.push_back(column_id);
  }
  int create_operator_input(ObExecContext& ctx) const;

protected:
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;

  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  int fetch_conflict_rows(ObExecContext& ctx, storage::ObDMLBaseParam& dml_param) const;

private:
  uint64_t table_id_;
  uint64_t index_tid_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> conflict_column_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> access_column_ids_;
  bool only_data_table_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableConflictRowFetcher);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_ENGINE_DML_OB_TABLE_CONFLICT_ROW_FETCHER_H_ */
