/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __SQL_ENGINE_PX_SUB_TRANS_UTIL_H__
#define __SQL_ENGINE_PX_SUB_TRANS_UTIL_H__

#include "sql/ob_sql_trans_control.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObPxSqcMeta;
class ObSubTransCtrl
{
public:
  ObSubTransCtrl() = default;
  ~ObSubTransCtrl() = default;
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObSubTransCtrl);
};

class ObDDLCtrl final
{
public:
  ObDDLCtrl() : direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID), context_id_(0), in_progress_(false) {}
  ~ObDDLCtrl() = default;

  bool is_in_progress() const { return in_progress_; }
  TO_STRING_KV(K_(direct_load_type), K_(context_id), K_(in_progress));
public:
  ObDirectLoadType direct_load_type_;
  int64_t context_id_;
  // to tag whether the ddl is in progress (between start_ddl and end_ddl).
  bool in_progress_;
};
}
}
#endif /* __SQL_ENGINE_PX_SUB_TRANS_UTIL_H__ */
//// end of header file

