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

#ifndef OB_COMPACTION_PARTITION_MERGE_FUSER_H_
#define OB_COMPACTION_PARTITION_MERGE_FUSER_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_raw_se_array.h"
#include "ob_partition_merge_iter.h"
#include "ob_tablet_merge_task.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "storage/ob_row_fuse.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace sql
{
struct ObTempExpr;
}
namespace compaction
{
template <typename T, typename... Args> T *alloc_helper(common::ObIAllocator &allocator, Args&... args)
{
  static_assert(std::is_constructible<T, Args&...>::value, "invalid construct arguments");
  void *buf = nullptr;
  T *rows_merger = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(T)))) {
  } else {
    rows_merger = new (buf) T(args...);
  }

  return rows_merger;
}

class ObMergeFuser
{
public:
  ObMergeFuser(common::ObIAllocator &allocator)
    : is_inited_(false),
      allocator_(allocator),
      column_cnt_(0),
      result_row_(),
      nop_pos_()
  {}
  virtual ~ObMergeFuser() {}
  virtual int init(const int64_t column_count) { return OB_NOT_SUPPORTED; }
  virtual int init(const ObMergeParameter &merge_param, const bool is_fuse_row_flag = false) { return OB_NOT_SUPPORTED; }
  virtual bool is_valid() const;
  inline bool is_inited() { return is_inited_; }
  int set_multi_version_flag(const blocksstable::ObMultiVersionRowFlag &row_flag);
  template<typename T, typename... Args>
  int fuse_rows(const T& row, const Args&... args);
  int fuse_row(MERGE_ITER_ARRAY &macro_row_iters);
  inline const blocksstable::ObDatumRow &get_result_row() const { return result_row_; }
  int make_result_row_shadow(const int64_t sql_sequence_col_idx);
  VIRTUAL_TO_STRING_KV(K_(column_cnt), K_(result_row), K_(is_inited));
protected:
  int base_init(const bool is_fuse_row_flag = false);
  void clean_nop_pos_and_result_row();
  inline void set_trans_id(const transaction::ObTransID& trans_id) { result_row_.trans_id_ = trans_id; }
  int add_fuse_row(const blocksstable::ObDatumRow &row, bool &final_result);
  int add_fuse_rows() { return OB_SUCCESS; }
  template<typename T, typename... Args>
  int add_fuse_rows(const T& row, const Args&... args);
  int fuse_delete_row(const blocksstable::ObDatumRow &del_row, const int64_t rowkey_column_cnt);
  virtual int preprocess_fuse_row(const blocksstable::ObDatumRow &row, bool &is_need_fuse);
  virtual int end_fuse_row(const storage::ObNopPos &nop_pos, blocksstable::ObDatumRow &result_row);
protected:
  bool is_inited_;
  common::ObIAllocator &allocator_;
  int64_t column_cnt_;
  bool is_fuse_row_flag_;
  blocksstable::ObDatumRow result_row_;
  storage::ObNopPos nop_pos_;
};

template<typename T, typename... Args>
int ObMergeFuser::add_fuse_rows(const T& row, const Args&... args)
{
  static_assert(std::is_same<blocksstable::ObDatumRow, T>::value, "typename T not ObDatumRow");
  int ret = OB_SUCCESS;
  bool final_result = false;
  if (OB_FAIL(add_fuse_row(row, final_result))) {
    STORAGE_LOG(WARN, "Failed to fuse row", K(ret));
  } else if (final_result) {
  } else if (OB_FAIL(add_fuse_rows(args...))) {
    STORAGE_LOG(WARN, "failed to push fuse row", K(ret));
  }

  return ret;
}

template<typename T, typename... Args>
int ObMergeFuser::fuse_rows(const T& row, const Args&... args)
{
  static_assert(std::is_same<blocksstable::ObDatumRow, T>::value, "typename T not ObDatumRow");
  int ret = OB_SUCCESS;
  bool is_need_fuse = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret =OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMergeFuser not init", K(ret));
  } else if (OB_FAIL(preprocess_fuse_row(row, is_need_fuse))) {
    STORAGE_LOG(WARN, "failed to preprocess_fuse_row", K(ret));
  } else if (!is_need_fuse) {
  } else if (OB_FAIL(add_fuse_rows(row, args...))) {
    STORAGE_LOG(WARN, "Failed to fuse default row", K(ret));
  } else if (OB_FAIL(end_fuse_row(nop_pos_, result_row_))) {
    STORAGE_LOG(WARN, "failed to end_fuse_row", K(ret));
  }
  return ret;
}

class ObDefaultMergeFuser : public ObMergeFuser
{
public:
  ObDefaultMergeFuser(common::ObIAllocator &allocator) : ObMergeFuser(allocator) {}
  virtual ~ObDefaultMergeFuser() {}
  virtual int init(const int64_t column_count) override final;
  INHERIT_TO_STRING_KV("ObMergeFuser", ObMergeFuser, "cur_fuser", "ObDefaultMergeFuser");
};

class ObIPartitionMergeFuser : public ObMergeFuser
{
public:
  ObIPartitionMergeFuser(common::ObIAllocator &allocator) : ObMergeFuser(allocator)
  {}
  virtual ~ObIPartitionMergeFuser() {}
  virtual int init(const ObMergeParameter &merge_param, const bool is_fuse_row_flag = false) override final;
  virtual bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObMergeFuser", ObMergeFuser, "cur_fuser", "ObIPartitionMergeFuser");
protected:
  virtual int inner_init(const ObMergeParameter &merge_param) = 0;
};

class ObMajorPartitionMergeFuser : public ObIPartitionMergeFuser
{
public:
  ObMajorPartitionMergeFuser(common::ObIAllocator &allocator, const int64_t cluster_version)
    : ObIPartitionMergeFuser(allocator),
      default_row_(),
      generated_cols_(allocator_),
      cluster_version_(cluster_version)
  {}
  virtual ~ObMajorPartitionMergeFuser();
  virtual int end_fuse_row(const storage::ObNopPos &nop_pos, blocksstable::ObDatumRow &result_row) override;
  INHERIT_TO_STRING_KV("ObIPartitionMergeFuser", ObIPartitionMergeFuser, K_(default_row));
protected:
  virtual int inner_init(const ObMergeParameter &merge_param) override;
protected:
  blocksstable::ObDatumRow default_row_;
  ObFixedArray<int32_t, ObIAllocator> generated_cols_;
  const int64_t cluster_version_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMajorPartitionMergeFuser);
};

class ObMinorPartitionMergeFuser : public ObIPartitionMergeFuser
{
public:
  ObMinorPartitionMergeFuser(common::ObIAllocator &allocator)
    : ObIPartitionMergeFuser(allocator),
      multi_version_rowkey_column_cnt_(0)
  {}
  virtual ~ObMinorPartitionMergeFuser() {}
  virtual bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObIPartitionMergeFuser", ObIPartitionMergeFuser,
      K_(multi_version_rowkey_column_cnt));
protected:
  virtual int inner_init(const ObMergeParameter &merge_param) override;
  virtual int preprocess_fuse_row(const blocksstable::ObDatumRow &row, bool &is_need_fuse) override;
  virtual int end_fuse_row(const storage::ObNopPos &nop_pos, blocksstable::ObDatumRow &result_row) override;
protected:
  int64_t multi_version_rowkey_column_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObMinorPartitionMergeFuser);
};

class ObMergeFuserBuilder {
public:
  static int build(const ObMergeParameter &merge_param,
                   const int64_t cluster_version,
                   ObIAllocator &allocator,
                   ObIPartitionMergeFuser *&partition_fuser);
};
} //compaction
} //oceanbase


#endif /* OB_COMPACTION_PARTITION_MERGE_FUSER_H_ */
