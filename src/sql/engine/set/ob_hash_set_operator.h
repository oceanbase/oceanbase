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

#ifndef OB_HASH_SET_OPERATOR_H
#define OB_HASH_SET_OPERATOR_H

#include "sql/engine/set/ob_set_operator.h"
#include "common/row/ob_row.h"
#include "lib/hash/ob_hashset.h"
#include "sql/engine/basic/ob_hash_partitioning_infrastructure.h"

namespace oceanbase {
namespace sql {
class ObHashSetOperator : public ObSetOperator {
  OB_UNIS_VERSION_V(1);

protected:
  class HashCols {
  public:
    explicit HashCols() : row_(NULL), col_collation_(NULL)
    {}
    ~HashCols()
    {}
    int init(const common::ObNewRow* row, const common::ObIArray<common::ObCollationType>* col_collation);
    uint64_t hash() const;
    bool operator==(const HashCols& other) const;

  public:
    const common::ObNewRow* row_;
    const common::ObIArray<common::ObCollationType>* col_collation_;
  };

  class ObHashSetOperatorCtx : public ObPhyOperatorCtx {
  public:
    static const int64_t MIN_BUCKET_COUNT = 10000;
    static const int64_t MAX_BUCKET_COUNT = 500000;
    static const int64_t HASH_SET_BUCKET_RATIO = 10;

    explicit ObHashSetOperatorCtx(ObExecContext& ctx)
        : ObPhyOperatorCtx(ctx),
          first_get_left_(true),
          has_got_part_(false),
          iter_end_(false),
          first_left_row_(NULL),
          left_op_(NULL),
          profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
          sql_mem_processor_(profile_),
          hp_infras_()
    {}
    virtual ~ObHashSetOperatorCtx()
    {}

    virtual void reset();
    void destroy() override
    {
      hp_infras_.~ObBasicHashPartInfrastructure();
      ObPhyOperatorCtx::destroy_base();
    }

    int is_left_has_row(ObExecContext& ctx, bool& left_has_row);
    int get_left_row(ObExecContext& ctx, const common::ObNewRow*& cur_row);

  private:
    DISALLOW_COPY_AND_ASSIGN(ObHashSetOperatorCtx);
    friend class ObHashSetOperator;

  protected:
    // used by intersect and except
    bool first_get_left_;
    bool has_got_part_;
    bool iter_end_;
    const ObNewRow* first_left_row_;
    ObPhyOperator* left_op_;
    ObSqlWorkAreaProfile profile_;
    ObSqlMemMgrProcessor sql_mem_processor_;
    ObBasicHashPartInfrastructure<HashPartCols, ObPartStoredRow> hp_infras_;
  };

public:
  explicit ObHashSetOperator(common::ObIAllocator& alloc);
  ~ObHashSetOperator();

  virtual int rescan(ObExecContext& ctx) const;

protected:
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;

  int build_hash_table(ObExecContext& ctx, bool from_child) const;

  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int init_hash_partition_infras(ObExecContext& ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHashSetOperator);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OB_HASH_SET_OPERATOR_H
