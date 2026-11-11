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

#ifndef OCEANBASE_SQL_ENGINE_OB_SER_COMPACT_BINDINGS_H_
#define OCEANBASE_SQL_ENGINE_OB_SER_COMPACT_BINDINGS_H_

#include "lib/utility/ob_unis_versioned.h"
#include "sql/engine/expr/ob_expr_ser_compact.h"
#include "sql/engine/basic/ob_chunk_datum_store_compact_header.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_scan_ctdef_compact.h"

namespace oceanbase
{
namespace common
{
namespace serialization
{

template <>
struct UnisOriginalRecord<sql::ObExpr>
{
  static constexpr bool accepts(const int64_t version)
  {
    return version >= 1 && version <= 3;
  }
  static int decode_body(sql::ObExpr &expr, const int64_t version, DESERIAL_PARAMS)
  {
    CompactExprPtrGuard legacy_guard(false);
    int ret = expr.deserialize_body(buf, data_len, pos, version);
    if (OB_SUCC(ret) && data_len == 0) { expr.basic_funcs_ = nullptr; }
    return ret;
  }
};

template <>
struct UnisOriginalRecord<sql::ObDASScanCtDef>
{
  static constexpr bool accepts(const int64_t version)
  {
    return version == 1;
  }
  static int decode_body(sql::ObDASScanCtDef &ctdef, const int64_t version, DESERIAL_PARAMS)
  {
    UNUSED(version);
    CompactExprPtrGuard legacy_guard(false);
    // The enclosing record uses its declared length to skip unknown V1 tail fields.
    return ctdef.deserialize_(buf, data_len, pos);
  }
};

} // namespace serialization
} // namespace common
} // namespace oceanbase

OB_UNIS_SER_REGISTER_TYPES(oceanbase::sql::ObDatumMeta, oceanbase::sql::ObDatumMetaCompact, 4);
OB_UNIS_SER_REGISTER_TYPES(oceanbase::sql::ObExpr, oceanbase::sql::ObExprCompactHeader, 4);
OB_UNIS_SER_REGISTER_TYPES(oceanbase::sql::ObDASScanCtDef, oceanbase::sql::das_compact::ObDASScanCtDefCompact, 4);
OB_UNIS_SER_REGISTER_TYPES(oceanbase::sql::ObChunkDatumStore, oceanbase::sql::ObChunkDatumStoreCompactHeader, 4);

#endif // OCEANBASE_SQL_ENGINE_OB_SER_COMPACT_BINDINGS_H_
