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

#include "storage/tablet/ob_batch_create_tablet_pretty_arg.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_rpc_struct.h"

using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace storage
{
ObBatchCreateTabletPrettyArg::ObBatchCreateTabletPrettyArg(const ObBatchCreateTabletArg &arg)
  : arg_(arg)
{
}

int64_t ObBatchCreateTabletPrettyArg::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV("ls_id", arg_.id_,
         "major_frozen_scn", arg_.major_frozen_scn_,
         "total_tablet_cnt", arg_.get_tablet_count());
    J_COMMA();

    BUF_PRINTF("tablets");
    J_COLON();
    J_OBJ_START();
    for (int64_t i = 0; i < arg_.tablets_.count(); ++i) {
      const ObCreateTabletInfo &info = arg_.tablets_.at(i);
      ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
      J_NEWLINE();
      BUF_PRINTF("[%ld] [", GETTID());
      BUF_PRINTO(PC(trace_id));
      BUF_PRINTF("] ");
      J_KV("data_tablet_id", info.data_tablet_id_,
           "tablet_ids", info.tablet_ids_,
           "compat_mode", info.compat_mode_,
           "is_create_bind_hidden_tablets", info.is_create_bind_hidden_tablets_);
    }
    J_NEWLINE();
    J_OBJ_END();
    J_OBJ_END();
  }
  return pos;
}
} // namespace storage
} // namespace oceanbase