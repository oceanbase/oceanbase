/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/list/ob_dlist.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTabletMergeCtx;
class ObITabletSliceRowIterator;

class ObDirectLoadIMergeTask : public common::ObDLinkBase<ObDirectLoadIMergeTask>
{
public:
  ObDirectLoadIMergeTask() = default;
  virtual ~ObDirectLoadIMergeTask() = default;
  virtual int process() = 0;
  virtual void stop() = 0;
  virtual int init_iterator(ObITabletSliceRowIterator *&row_iterator) = 0;
  virtual ObDirectLoadTabletMergeCtx *get_merge_ctx() = 0;
};

} // namespace storage
} // namespace oceanbase
