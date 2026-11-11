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
 *
 * Compact serialize opt for ObDASScanCtDef. The enclosing DAS request selects
 * the serialization family for inline and cacheable segments. The original
 * class keeps its OB_SERIALIZE_MEMBER codec for the legacy format.
 *
 * Each ScanCtDef record carries a version/body-length envelope so its body can
 * evolve independently and validate its own boundary.
 *
 * Wire format:
 *   standard UNIS version 4 + fixed 5-byte body length
 *   compact members and table-param mode/descriptor
 *   [is_external_table_] 9 external-table members
 *   [is_external_table_ || table type == VIRTUAL_TABLE] external_object_ctx_
 *
 * ObExpr* members use varint array indexes in the compact request context;
 * legacy requests keep fixed i32 references.
 */

#ifndef OCEANBASE_SQL_DAS_OB_DAS_SCAN_CTDEF_COMPACT_H_
#define OCEANBASE_SQL_DAS_OB_DAS_SCAN_CTDEF_COMPACT_H_

#include <stdint.h>
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace sql
{
using common::serialization::SerCompactState;

class ObExecContext;
struct ObDASScanCtDef;

namespace das_compact
{

enum ObScanCtDefTableParamMode : int8_t
{
  TP_INLINE = 0,
  TP_REBUILD = 1,
};

struct ObScanCtDefDecodeCtx
{
  ObExecContext *exec_ctx_;
};

struct ObScanCtDefReplayCtx
{
  ObExecContext *exec_ctx_ = nullptr;
};

struct ObDASScanCtDefCompact
{
  ObDASScanCtDefCompact() :
    ctdef_(nullptr), force_inline_table_param_(false), table_param_mode_(TP_INLINE), plan_enable_rich_format_(false),
    is_safe_filter_with_di_(true), table_read_is_oracle_mode_(false), output_sel_mask_(nullptr), mask_bytes_(0)
  {}
  explicit ObDASScanCtDefCompact(const ObDASScanCtDef &ctdef, bool force_inline_table_param = false) :
    ObDASScanCtDefCompact()
  {
    ctdef_ = const_cast<ObDASScanCtDef *>(&ctdef);
    force_inline_table_param_ = force_inline_table_param;
  }
  OB_UNIS_VERSION(common::serialization::SER_COMPACT_OPT_VERSION);

public:
  int replay_to(ObDASScanCtDef &dst, ObScanCtDefReplayCtx &) const;

  ObDASScanCtDef *ctdef_;
  bool force_inline_table_param_;
  int8_t table_param_mode_;
  bool plan_enable_rich_format_;
  bool is_safe_filter_with_di_;
  bool table_read_is_oracle_mode_;
  const char *output_sel_mask_;
  int64_t mask_bytes_;
};

bool can_rebuild_table_param(const ObDASScanCtDef &ctdef);
int rebuild_table_param_for_scan_ctdef(ObDASScanCtDef &ctdef, bool plan_enable_rich_format, bool is_safe_filter_with_di,
                                       bool table_read_is_oracle_mode, const char *output_sel_mask, int64_t mask_bytes,
                                       ObExecContext &exec_ctx);
// force_inline_table_param: compact serialize opt resend mode — encode TP_INLINE (full
// table_param) regardless of the can_rebuild predicate.
int serialize_scan_ctdef(const ObDASScanCtDef &ctdef, char *buf, int64_t buf_len, int64_t &pos,
                         bool force_inline_table_param = false);
int deserialize_scan_ctdef(ObDASScanCtDef &ctdef, const char *buf, int64_t data_len, int64_t &pos,
                           const ObScanCtDefDecodeCtx *ctx, SerCompactState *ser_compact_state = nullptr);
int64_t scan_ctdef_size(const ObDASScanCtDef &ctdef, bool force_inline_table_param = false);

} // namespace das_compact
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_DAS_OB_DAS_SCAN_CTDEF_COMPACT_H_
