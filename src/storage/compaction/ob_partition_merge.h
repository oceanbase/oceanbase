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

#ifndef OB_COMPACTION_PARTITION_MERGE_FUSE_H_
#define OB_COMPACTION_PARTITION_MERGE_FUSE_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_raw_se_array.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_row_fuse.h"
#include "storage/compaction/ob_partition_merge_builder.h"
#include "storage/ob_multi_version_col_desc_generate.h"
#include "ob_column_checksum_calculator.h"
#include "storage/memtable/ob_nop_bitmap.h"

namespace oceanbase {
namespace storage {
struct ObMergeParameter;
}
namespace compaction {
class ObMacroRowIterator;

class ObIPartitionMergeFuser {
public:
  static const int64_t DEFAULT_ITER_COUNT = 16;
  typedef common::ObRawSEArray<ObMacroRowIterator*, DEFAULT_ITER_COUNT> MERGE_ITER_ARRAY;
  ObIPartitionMergeFuser()
      : schema_rowkey_column_cnt_(0),
        column_cnt_(0),
        checksum_method_(blocksstable::CCM_UNKOWN),
        has_lob_column_(false),
        result_row_(NULL),
        nop_pos_(),
        compact_type_(ObCompactRowType::T_INVALID),
        allocator_(ObModIds::OB_CS_MERGER),
        schema_column_ids_(common::OB_MAX_COLUMN_NUMBER, allocator_),
        is_inited_(false),
        purged_count_(0)
  {}
  virtual ~ObIPartitionMergeFuser();
  virtual int init(const storage::ObMergeParameter& merge_param);
  virtual void reset();
  virtual bool is_valid() const;
  virtual int fuse_row(MERGE_ITER_ARRAY& macro_row_iters) = 0;
  virtual int find_minimum_iters(const MERGE_ITER_ARRAY& macro_row_iters, MERGE_ITER_ARRAY& minimum_iters) = 0;
  virtual int calc_column_checksum(const bool rewrite);
  inline ObCompactRowType::ObCompactRowTypeEnum get_compact_type() const
  {
    return compact_type_;
  }
  inline bool has_lob_column() const
  {
    return has_lob_column_;
  }
  virtual inline const storage::ObStoreRow* get_result_row() const
  {
    return result_row_;
  }
  inline const common::ObIArray<share::schema::ObColDesc>& get_schema_column_ids() const
  {
    return schema_column_ids_;
  }
  virtual inline const common::ObIArray<share::schema::ObColDesc>& get_column_ids() const
  {
    return schema_column_ids_;
  }
  virtual OB_INLINE int64_t get_purged_count() const
  {
    return purged_count_;
  }
  virtual const char* get_fuser_name() const = 0;
  VIRTUAL_TO_STRING_KV(K_(schema_rowkey_column_cnt), K_(column_cnt), K_(checksum_method), K_(has_lob_column),
      KP_(result_row), K_(compact_type), K_(schema_column_ids), K_(is_inited));

protected:
  int check_merge_param(const storage::ObMergeParameter& merge_param);
  int base_init(const storage::ObMergeParameter& merge_param);
  virtual int inner_check_merge_param(const storage::ObMergeParameter& merge_param) = 0;
  virtual int inner_init(const storage::ObMergeParameter& merge_param) = 0;
  int compare_row_iters_simple(ObMacroRowIterator* base_iter, ObMacroRowIterator* macro_row_iter, int64_t& cmp_ret);
  int compare_row_iters_range(ObMacroRowIterator* base_iter, ObMacroRowIterator* macro_row_iter, int64_t& cmp_ret);
  int next_row_iters(MERGE_ITER_ARRAY& macro_row_iters);
  void reset_store_row(storage::ObStoreRow& store_row);
  int fuse_compact_type(const MERGE_ITER_ARRAY& macro_row_iters);
  virtual int fuse_delete_row(
      ObMacroRowIterator* row_iter, storage::ObStoreRow* row, const int64_t rowkey_column_cnt) = 0;
  virtual int malloc_row(int64_t column_count, storage::ObStoreRow*& row) = 0;

private:
protected:
  int64_t schema_rowkey_column_cnt_;
  int64_t column_cnt_;
  int64_t checksum_method_;
  bool has_lob_column_;
  storage::ObStoreRow* result_row_;
  storage::ObNopPos nop_pos_;
  ObCompactRowType::ObCompactRowTypeEnum compact_type_;
  common::ObArenaAllocator allocator_;
  common::ObArray<share::schema::ObColDesc, common::ObIAllocator&> schema_column_ids_;
  bool is_inited_;
  int64_t purged_count_;
};

class ObMajorPartitionMergeFuser : public ObIPartitionMergeFuser {
public:
  ObMajorPartitionMergeFuser()
      : ObIPartitionMergeFuser(),
        default_row_(NULL),
        column_changed_(NULL),
        table_schema_(NULL),
        dependent_exprs_(),
        expr_allocator_(ObModIds::OB_SQL_EXPR_CALC),
        expr_ctx_(),
        need_fuse_generate_(true),
        is_full_merge_(false)
  {}
  virtual ~ObMajorPartitionMergeFuser();
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual int fuse_row(MERGE_ITER_ARRAY& macro_row_iters) override;
  virtual int find_minimum_iters(const MERGE_ITER_ARRAY& macro_row_iters, MERGE_ITER_ARRAY& minimum_iters) override;
  virtual const char* get_fuser_name() const override
  {
    return "ObMajorPartitionMergeFuser";
  }
  INHERIT_TO_STRING_KV("ObIPartitionMergeFuser", ObIPartitionMergeFuser, KP_(default_row), KP_(column_changed),
      KP_(table_schema), K_(need_fuse_generate));

protected:
  virtual int inner_check_merge_param(const storage::ObMergeParameter& merge_param);
  virtual int inner_init(const storage::ObMergeParameter& merge_param) override;
  virtual int fuse_generate_exprs();
  virtual int fuse_old_row(ObMacroRowIterator* row_iter, storage::ObStoreRow* row);
  int check_row_iters_purge(ObMacroRowIterator* base_iter, ObMacroRowIterator* macro_row_iter, bool& can_purged);
  virtual int fuse_delete_row(
      ObMacroRowIterator* row_iter, storage::ObStoreRow* row, const int64_t rowkey_column_cnt) override;
  virtual int malloc_row(int64_t column_count, storage::ObStoreRow*& row) override;

protected:
  storage::ObStoreRow* default_row_;
  bool* column_changed_;  // only used in incre major fuser
  const share::schema::ObTableSchema* table_schema_;
  common::ObArray<common::ObISqlExpression*> dependent_exprs_;
  common::ObArenaAllocator expr_allocator_;
  common::ObExprCtx expr_ctx_;
  bool need_fuse_generate_;
  bool is_full_merge_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMajorPartitionMergeFuser);
};

class ObIncrementMajorPartitionMergeFuser : public ObMajorPartitionMergeFuser {
public:
  ObIncrementMajorPartitionMergeFuser()
      : ObMajorPartitionMergeFuser(), row_changed_(false), old_row_(NULL), checksum_calculator_(NULL)
  {}
  virtual ~ObIncrementMajorPartitionMergeFuser();
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual int fuse_row(MERGE_ITER_ARRAY& macro_row_iters) override;
  virtual int calc_column_checksum(const bool rewrite) override;
  virtual const char* get_fuser_name() const override
  {
    return "ObIncrementMajorPartitionMergeFuser";
  }
  INHERIT_TO_STRING_KV(
      "ObMajorPartitionMergeFuser", ObMajorPartitionMergeFuser, K_(row_changed), KP_(old_row), K_(checksum_method));

protected:
  virtual int inner_check_merge_param(const storage::ObMergeParameter& merge_param) override;
  virtual int inner_init(const storage::ObMergeParameter& merge_param) override;
  virtual int prepare_old_row(const MERGE_ITER_ARRAY& macro_row_iters);

protected:
  bool row_changed_;
  storage::ObStoreRow* old_row_;
  ObColumnChecksumCalculator* checksum_calculator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIncrementMajorPartitionMergeFuser);
};

class ObMinorPartitionMergeFuser : public ObIPartitionMergeFuser {
public:
  ObMinorPartitionMergeFuser()
      : ObIPartitionMergeFuser(),
        col_id_set_(),
        out_cols_project_(NULL),
        multi_version_col_desc_gen_(),
        multi_version_row_info_(NULL),
        column_ids_(common::OB_MAX_COLUMN_NUMBER, allocator_),
        need_check_curr_row_last_(true),
        cur_first_dml_(storage::T_DML_UNKNOWN)
  {}
  virtual ~ObMinorPartitionMergeFuser();
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual int fuse_row(MERGE_ITER_ARRAY& macro_row_iters) = 0;
  virtual int find_minimum_iters(const MERGE_ITER_ARRAY& macro_row_iters, MERGE_ITER_ARRAY& minimum_iters) override;
  virtual inline const common::ObIArray<share::schema::ObColDesc>& get_column_ids() const
  {
    return column_ids_;
  }
  virtual inline const storage::ObMultiVersionRowInfo* get_multi_version_row_info() const
  {
    return multi_version_row_info_;
  }
  virtual const char* get_fuser_name() const override
  {
    return "ObMinorPartitionMergeFuser";
  }
  INHERIT_TO_STRING_KV("ObIPartitionMergeFuser", ObIPartitionMergeFuser, KP_(out_cols_project),
      KP_(multi_version_row_info), K_(column_ids));

protected:
  virtual int inner_check_merge_param(const storage::ObMergeParameter& merge_param) override;
  virtual int inner_init(const storage::ObMergeParameter& merge_param) override;
  virtual int compare_row_iters(ObMacroRowIterator* base_iter, ObMacroRowIterator* macro_row_iter, int64_t& cmp_ret,
      memtable::ObNopBitMap& nop_pos);
  int set_multi_version_row_flag(const MERGE_ITER_ARRAY& macro_row_iters, storage::ObStoreRow& store_row);
  void set_dml(const MERGE_ITER_ARRAY& macro_row_iters, storage::ObStoreRow& store_row);
  virtual int fuse_delete_row(
      ObMacroRowIterator* row_iter, storage::ObStoreRow* row, const int64_t rowkey_column_cnt) = 0;
  virtual int malloc_row(int64_t column_count, storage::ObStoreRow*& row) = 0;
  bool is_committed_row_(const storage::ObStoreRow& row) const;

protected:
  common::ObBitSet<> col_id_set_;
  const common::ObIArray<int32_t>* out_cols_project_;
  storage::ObMultiVersionColDescGenerate multi_version_col_desc_gen_;
  const storage::ObMultiVersionRowInfo* multi_version_row_info_;
  common::ObArray<share::schema::ObColDesc, common::ObIAllocator&> column_ids_;
  bool need_check_curr_row_last_;
  storage::ObRowDml cur_first_dml_;
  DISALLOW_COPY_AND_ASSIGN(ObMinorPartitionMergeFuser);
};

class ObFlatMinorPartitionMergeFuser : public ObMinorPartitionMergeFuser {
public:
  ObFlatMinorPartitionMergeFuser()
  {}
  virtual ~ObFlatMinorPartitionMergeFuser()
  {}
  virtual int fuse_delete_row(
      ObMacroRowIterator* row_iter, storage::ObStoreRow* row, const int64_t rowkey_column_cnt) override;
  virtual int malloc_row(int64_t column_count, storage::ObStoreRow*& row) override;
  virtual int fuse_row(MERGE_ITER_ARRAY& macro_row_iters) override;
  virtual const char* get_fuser_name() const override
  {
    return "ObFlatMinorPartitionMergeFuser";
  }
  INHERIT_TO_STRING_KV("ObFlatMinorPartitionMergeFuser", ObIPartitionMergeFuser, KP_(out_cols_project),
      KP_(multi_version_row_info), K_(column_ids));
};

class ObSparseMinorPartitionMergeFuser : public ObMinorPartitionMergeFuser {
public:
  ObSparseMinorPartitionMergeFuser()
  {}
  virtual ~ObSparseMinorPartitionMergeFuser()
  {}
  virtual int fuse_delete_row(
      ObMacroRowIterator* row_iter, storage::ObStoreRow* row, const int64_t rowkey_column_cnt) override;
  virtual int malloc_row(int64_t column_count, storage::ObStoreRow*& row) override;
  virtual int fuse_row(MERGE_ITER_ARRAY& macro_row_iters) override;
  virtual const char* get_fuser_name() const override
  {
    return "ObSparseMinorPartitionMergeFuser";
  }
  INHERIT_TO_STRING_KV("ObSparseMinorPartitionMergeFuser", ObIPartitionMergeFuser, KP_(out_cols_project),
      KP_(multi_version_row_info), K_(column_ids));
};

}  // namespace compaction
}  // namespace oceanbase

#endif /* OB_COMPACTION_PARTITION_MERGE_FUSE_H_ */
