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

#ifndef DEV_SRC_SQL_DAS_OB_DAS_DEF_REG_H_
#define DEV_SRC_SQL_DAS_OB_DAS_DEF_REG_H_
#include <type_traits>
#include "sql/das/ob_das_define.h"

namespace oceanbase
{
namespace sql
{
namespace das_reg
{
template <int>
struct ObDASOpTypeTraits
{

  constexpr static bool registered_ = false;
  typedef char DASOp;
  typedef char DASOpResult;
  typedef char DASCtDef;
  typedef char DASRtDef;
};

template <typename T>
struct ObDASOpTraits
{
  constexpr static int type_ = 0;
};
}  // namespace das_reg

#define REGISTER_DAS_OP(type, op, op_result, ctdef, rtdef)                                      \
  namespace das_reg {                                                                           \
  template<>                                                                                    \
  struct ObDASOpTypeTraits<type>                                                                \
  {                                                                                             \
    constexpr static bool registered_ = true;                                                   \
    typedef op DASOp;                                                                           \
    typedef op_result DASOpResult;                                                              \
    typedef ctdef DASCtDef;                                                                     \
    typedef rtdef DASRtDef;                                                                     \
  };                                                                                            \
  template <> struct ObDASOpTraits<op> { constexpr static int type_ = type; };                  \
  }

class ObDASScanOp;
class ObDASScanResult;
struct ObDASScanCtDef;
struct ObDASScanRtDef;
REGISTER_DAS_OP(DAS_OP_TABLE_SCAN, ObDASScanOp, ObDASScanResult, ObDASScanCtDef, ObDASScanRtDef);

class ObDASInsertOp;
class ObDASInsertResult;
struct ObDASInsCtDef;
struct ObDASInsRtDef;
REGISTER_DAS_OP(DAS_OP_TABLE_INSERT, ObDASInsertOp, ObDASInsertResult, ObDASInsCtDef, ObDASInsRtDef);

class ObDASDeleteOp;
class ObDASDeleteResult;
struct ObDASDelCtDef;
struct ObDASDelRtDef;
REGISTER_DAS_OP(DAS_OP_TABLE_DELETE, ObDASDeleteOp, ObDASDeleteResult, ObDASDelCtDef, ObDASDelRtDef);

class ObDASUpdateOp;
class ObDASUpdateResult;
struct ObDASUpdCtDef;
struct ObDASUpdRtDef;
REGISTER_DAS_OP(DAS_OP_TABLE_UPDATE, ObDASUpdateOp, ObDASUpdateResult, ObDASUpdCtDef, ObDASUpdRtDef);

class ObDASLockOp;
class ObDASLockResult;
struct ObDASLockCtDef;
struct ObDASLockRtDef;
REGISTER_DAS_OP(DAS_OP_TABLE_LOCK, ObDASLockOp, ObDASLockResult, ObDASLockCtDef, ObDASLockRtDef);

class ObDASGroupScanOp;
class ObDASScanResult;
struct ObDASScanCtDef;
struct ObDASScanRtDef;
REGISTER_DAS_OP(DAS_OP_TABLE_BATCH_SCAN, ObDASGroupScanOp, ObDASScanResult, ObDASScanCtDef, ObDASScanRtDef);

class ObDASSplitRangesOp;
class ObDASSplitRangesResult;
class ObDASEmptyCtDef;
class ObDASEmptyRtDef;
REGISTER_DAS_OP(DAS_OP_SPLIT_MULTI_RANGES, ObDASSplitRangesOp, ObDASSplitRangesResult, ObDASEmptyCtDef, ObDASEmptyRtDef);

class ObDASRangesCostOp;
class ObDASRangesCostResult;
REGISTER_DAS_OP(DAS_OP_GET_RANGES_COST, ObDASRangesCostOp, ObDASRangesCostResult, ObDASEmptyCtDef, ObDASEmptyRtDef);

#undef REGISTER_DAS_OP
}  // namespace sql
}  // namespace oceanbase

#endif /* DEV_SRC_SQL_DAS_OB_DAS_DEF_REG_H_ */
