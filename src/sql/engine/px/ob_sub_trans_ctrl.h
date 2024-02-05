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
  ObDDLCtrl() : context_id_(0), in_progress_(false) {}
  ~ObDDLCtrl() = default;

  bool is_in_progress() const { return in_progress_; }
  TO_STRING_KV(K_(in_progress));
public:
  int64_t context_id_;
  // to tag whether the ddl is in progress (between start_ddl and end_ddl).
  bool in_progress_;
};
}
}
#endif /* __SQL_ENGINE_PX_SUB_TRANS_UTIL_H__ */
//// end of header file

