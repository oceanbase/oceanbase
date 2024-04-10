/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * ob_das_attach_define.h
 *
 *      Author: yuming<>
 */
#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_ATTACH_DEFINE_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_ATTACH_DEFINE_H_
#include "sql/das/ob_das_define.h"
#include "share/ob_define.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace sql
{

struct ObDASAttachSpec
{
  OB_UNIS_VERSION(1);
public:
   ObDASAttachSpec(common::ObIAllocator &alloc, ObDASBaseCtDef *scan_ctdef)
    : attach_loc_metas_(alloc),
      scan_ctdef_(nullptr),
      allocator_(alloc),
      attach_ctdef_(nullptr)
  {
  }
  common::ObList<ObDASTableLocMeta*, common::ObIAllocator> attach_loc_metas_;
  ObDASBaseCtDef *scan_ctdef_; //This ctdef represents the main task information executed by the DAS Task.
  common::ObIAllocator &allocator_;
  ObDASBaseCtDef *attach_ctdef_; //The attach_ctdef represents the task information that is bound to and executed on the DAS Task.

  TO_STRING_KV(K_(attach_loc_metas),
               K_(attach_ctdef));
};

}
}

#endif