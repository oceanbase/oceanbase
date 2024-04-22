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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_SIMPLE_OP_H
#define OBDEV_SRC_SQL_DAS_OB_DAS_SIMPLE_OP_H
#include "sql/das/ob_das_task.h"

namespace oceanbase
{
namespace common
{
class ObStoreRange;
}
namespace sql
{

class ObDASSimpleOp : public ObIDASTaskOp
{
  OB_UNIS_VERSION(1);
public:
  ObDASSimpleOp(common::ObIAllocator &op_alloc);
  virtual ~ObDASSimpleOp() = default;
  virtual int open_op() = 0;
  virtual int fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit) = 0;
  virtual int decode_task_result(ObIDASTaskResult *task_result) = 0;

  virtual int release_op() override;
  virtual int init_task_info(uint32_t row_extend_size) override;
  virtual int swizzling_remote_task(ObDASRemoteInfo *remote_info) override;
};

class ObDASEmptyOp : public ObDASSimpleOp
{
public:
  ObDASEmptyOp(common::ObIAllocator &op_alloc)
    : ObDASSimpleOp(op_alloc)
  {
  }
  virtual ~ObDASEmptyOp() = default;
  virtual int open_op() override { return common::OB_NOT_IMPLEMENT; }
  virtual int fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit) override
  {
    UNUSEDx(task_result, has_more, memory_limit);
    return common::OB_NOT_IMPLEMENT;
  }
  virtual int decode_task_result(ObIDASTaskResult *task_result) override
  {
    UNUSEDx(task_result);
    return common::OB_NOT_IMPLEMENT;
  }
};

class ObDASEmptyResult : public ObIDASTaskResult
{
public:
  ObDASEmptyResult() {}
  virtual ~ObDASEmptyResult() {}
  virtual int init(const ObIDASTaskOp &op, common::ObIAllocator &alloc) override
  {
    UNUSEDx(op, alloc);
    return common::OB_NOT_IMPLEMENT;
  }
  virtual int reuse() override
  {
    return common::OB_NOT_IMPLEMENT;
  }
};

struct ObDASEmptyCtDef : ObDASBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASEmptyCtDef(common::ObIAllocator &alloc)
    : ObDASBaseCtDef(DAS_OP_INVALID) {}
};

struct ObDASEmptyRtDef : ObDASBaseRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASEmptyRtDef()
    : ObDASBaseRtDef(DAS_OP_INVALID) {}
};

class ObDASSplitRangesOp : public ObDASSimpleOp
{
  OB_UNIS_VERSION(1);
public:
  ObDASSplitRangesOp(common::ObIAllocator &op_alloc);
  virtual ~ObDASSplitRangesOp() = default;
  virtual int open_op() override;
  virtual int fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit) override;
  virtual int decode_task_result(ObIDASTaskResult *task_result) override;
  int init(const common::ObIArray<ObStoreRange> &ranges, int64_t expected_task_count, const int64_t timeout_us);
  const ObArrayArray<ObStoreRange> &get_split_array() { return multi_range_split_array_; }
  INHERIT_TO_STRING_KV("parent", ObDASSimpleOp,
                        K_(ranges),
                        K_(expected_task_count),
                        K_(multi_range_split_array));
private:
  common::ObSEArray<ObStoreRange, 16> ranges_;
  int64_t expected_task_count_;
  ObArrayArray<ObStoreRange> multi_range_split_array_;
  int64_t timeout_us_;
};

class ObDASSplitRangesResult : public ObIDASTaskResult
{
  OB_UNIS_VERSION_V(1);
public:
  ObDASSplitRangesResult();
  virtual ~ObDASSplitRangesResult();
  virtual int init(const ObIDASTaskOp &op, common::ObIAllocator &alloc) override;
  virtual int reuse() override;
  const ObArrayArray<ObStoreRange> &get_split_array() const { return multi_range_split_array_; }
  ObArrayArray<ObStoreRange> &get_split_array() { return multi_range_split_array_; }
  int assign(const ObArrayArray<ObStoreRange> &array);
  INHERIT_TO_STRING_KV("parent", ObIDASTaskResult,
                        K_(multi_range_split_array));
private:
  ObArrayArray<ObStoreRange> multi_range_split_array_;
  common::ObIAllocator *result_alloc_;
};

class ObDASRangesCostOp : public ObDASSimpleOp
{
  OB_UNIS_VERSION(1);
public:
  ObDASRangesCostOp(common::ObIAllocator &op_alloc);
  virtual ~ObDASRangesCostOp() = default;
  virtual int open_op() override;
  virtual int fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit) override;
  virtual int decode_task_result(ObIDASTaskResult *task_result) override;
  int init(const common::ObIArray<ObStoreRange> &ranges, const int64_t timeout_us);
  int64_t get_total_size() const { return total_size_; }
  INHERIT_TO_STRING_KV("parent", ObDASSimpleOp,
                        K_(ranges),
                        K_(total_size));
private:
  common::ObSEArray<ObStoreRange, 16> ranges_;
  int64_t total_size_;
  int64_t timeout_us_;
};

class ObDASRangesCostResult : public ObIDASTaskResult
{
  OB_UNIS_VERSION_V(1);
public:
  ObDASRangesCostResult();
  virtual ~ObDASRangesCostResult() = default;
  virtual int init(const ObIDASTaskOp &op, common::ObIAllocator &alloc) override;
  virtual int reuse() override;
  int64_t get_total_size() const { return total_size_; }
  void set_total_size(int64_t total_size) { total_size_ = total_size; }
  INHERIT_TO_STRING_KV("parent", ObIDASTaskResult,
                        K_(total_size));
private:
  int64_t total_size_;
};

class ObDASSimpleUtils
{
public:
  static int split_multi_ranges(ObExecContext &exec_ctx,
                                ObDASTabletLoc *tablet_loc,
                                const common::ObIArray<ObStoreRange> &ranges,
                                const int64_t expected_task_count,
                                ObArrayArray<ObStoreRange> &multi_range_split_array);

  static int get_multi_ranges_cost(ObExecContext &exec_ctx,
                                   ObDASTabletLoc *tablet_loc,
                                   const common::ObIArray<common::ObStoreRange> &ranges,
                                   int64_t &total_size);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_SIMPLE_OP_H */
