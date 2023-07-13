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
#include "ob_column_checksum_calculator.h"
#include "ob_partition_merge_iter.h"
#include "ob_tablet_merge_task.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "storage/memtable/ob_nop_bitmap.h"
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
class ObIPartitionMergeFuser
{
public:
  ObIPartitionMergeFuser()
    : schema_rowkey_column_cnt_(0),
      column_cnt_(0),
      result_row_(),
      nop_pos_(),
      allocator_("MergeFuser"),
      multi_version_column_ids_(common::OB_MAX_COLUMN_NUMBER, allocator_),
      is_inited_(false)
  {}
  virtual ~ObIPartitionMergeFuser();
  virtual int init(const ObMergeParameter &merge_param);
  virtual void reset();
  virtual bool is_valid() const;
  virtual int fuse_row(MERGE_ITER_ARRAY &macro_row_iters) = 0;
  virtual int calc_column_checksum(const bool rewrite);
  virtual inline const blocksstable::ObDatumRow *get_result_row() const { return &result_row_; }
  inline const common::ObIArray<share::schema::ObColDesc> &get_multi_version_column_ids() const
  {
    return multi_version_column_ids_;
  }
  virtual const char *get_fuser_name() const = 0;
  virtual int set_multi_version_flag(const blocksstable::ObMultiVersionRowFlag &row_flag);
  VIRTUAL_TO_STRING_KV(K_(schema_rowkey_column_cnt), K_(column_cnt),
      K_(result_row), K_(multi_version_column_ids), K_(is_inited));
protected:
  int check_merge_param(const ObMergeParameter &merge_param);
  int base_init(const ObMergeParameter &merge_param);
  virtual int inner_check_merge_param(const ObMergeParameter &merge_param) = 0;
  virtual int inner_init(const ObMergeParameter &merge_param) = 0;

  void reset_store_row(blocksstable::ObDatumRow &store_row);
  virtual int fuse_delete_row(ObPartitionMergeIter *row_iter, blocksstable::ObDatumRow &row,
                      const int64_t rowkey_column_cnt) = 0;
private:
protected:
  int64_t schema_rowkey_column_cnt_;
  int64_t column_cnt_;
  blocksstable::ObDatumRow result_row_;
  storage::ObNopPos nop_pos_;
  common::ObArenaAllocator allocator_;
  // for major: store all columns' description
  // for mini & minor: store multi_version rowkey column description
  common::ObArray<share::schema::ObColDesc, common::ObIAllocator &> multi_version_column_ids_;
  bool is_inited_;
};

class ObMajorPartitionMergeFuser : public ObIPartitionMergeFuser
{
public:
  ObMajorPartitionMergeFuser()
      : ObIPartitionMergeFuser(),
      default_row_(),
      generated_cols_(allocator_)
  {}
  virtual ~ObMajorPartitionMergeFuser();
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual int fuse_row(MERGE_ITER_ARRAY &macro_row_iters) override;
  virtual const char *get_fuser_name() const override { return "ObMajorPartitionMergeFuser"; }
  INHERIT_TO_STRING_KV("ObIPartitionMergeFuser", ObIPartitionMergeFuser, K_(default_row));
protected:
  virtual int inner_check_merge_param(const ObMergeParameter &merge_param);
  virtual int inner_init(const ObMergeParameter &merge_param) override;
  virtual int fuse_delete_row(ObPartitionMergeIter *row_iter, blocksstable::ObDatumRow &row,
                              const int64_t rowkey_column_cnt) override;
protected:
  blocksstable::ObDatumRow default_row_;
  ObFixedArray<int32_t, ObIAllocator> generated_cols_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMajorPartitionMergeFuser);
};

class ObMetaPartitionMergeFuser : public ObMajorPartitionMergeFuser
{
public:
  ObMetaPartitionMergeFuser() {}
  virtual ~ObMetaPartitionMergeFuser() {}
  virtual const char *get_fuser_name() const override { return "ObMetaPartitionMergeFuser"; }
  INHERIT_TO_STRING_KV("ObMajorPartitionMergeFuser", ObMajorPartitionMergeFuser,
      "cur_fuser", "ObMetaPartitionMergeFuser");
protected:
  virtual int inner_check_merge_param(const ObMergeParameter &merge_param) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMetaPartitionMergeFuser);
};


class ObMinorPartitionMergeFuser : public ObIPartitionMergeFuser
{
public:
  ObMinorPartitionMergeFuser()
    : ObIPartitionMergeFuser(),
      multi_version_rowkey_column_cnt_(0)
  {}
  virtual ~ObMinorPartitionMergeFuser();
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual int fuse_row(MERGE_ITER_ARRAY &macro_row_iters) = 0;
  virtual const char *get_fuser_name() const override { return "ObMinorPartitionMergeFuser"; }
  INHERIT_TO_STRING_KV("ObIPartitionMergeFuser", ObIPartitionMergeFuser,
      K_(multi_version_rowkey_column_cnt));
protected:
  virtual int inner_check_merge_param(const ObMergeParameter &merge_param) override;
  virtual int inner_init(const ObMergeParameter &merge_param) override;

  virtual int fuse_delete_row(ObPartitionMergeIter *row_iter, blocksstable::ObDatumRow &row,
                      const int64_t rowkey_column_cnt) = 0;
protected:
  int64_t multi_version_rowkey_column_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObMinorPartitionMergeFuser);
};

class ObFlatMinorPartitionMergeFuser : public ObMinorPartitionMergeFuser
{
public:
  ObFlatMinorPartitionMergeFuser(){}
  virtual ~ObFlatMinorPartitionMergeFuser(){}
  virtual int fuse_delete_row(ObPartitionMergeIter *row_iter, blocksstable::ObDatumRow &row,
                      const int64_t rowkey_column_cnt) override;
  virtual int fuse_row(MERGE_ITER_ARRAY &macro_row_iters) override;
  virtual const char *get_fuser_name() const override { return "ObFlatMinorPartitionMergeFuser"; }
  INHERIT_TO_STRING_KV("ObFlatMinorPartitionMergeFuser", ObIPartitionMergeFuser,
      K_(multi_version_rowkey_column_cnt));
};

} //compaction
} //oceanbase


#endif /* OB_COMPACTION_PARTITION_MERGE_FUSER_H_ */
