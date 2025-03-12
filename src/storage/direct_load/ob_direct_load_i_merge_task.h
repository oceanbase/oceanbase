/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/list/ob_dlist.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTabletMergeCtx;

class ObDirectLoadIMergeTask : public common::ObDLinkBase<ObDirectLoadIMergeTask>
{
public:
  ObDirectLoadIMergeTask() = default;
  virtual ~ObDirectLoadIMergeTask() = default;
  virtual int process() = 0;
  virtual void stop() = 0;
  virtual ObDirectLoadTabletMergeCtx *get_merge_ctx() = 0;
};

} // namespace storage
} // namespace oceanbase
