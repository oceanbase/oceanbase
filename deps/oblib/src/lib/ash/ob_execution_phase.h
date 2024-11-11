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

#ifndef _OB_SHARE_ASH_EXECUTION_PHASE_H_
#define _OB_SHARE_ASH_EXECUTION_PHASE_H_
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{

namespace common
{

class ObAshBuffer;

struct ObExecPhase
{
  ObExecPhase() :time_model_(0) {};
  ObExecPhase(uint64_t time_model) :time_model_(time_model) {};
  TO_STRING_KV(K_(time_model));
  union {
    uint64_t time_model_; // phase of execution bitmap
    struct {
      uint64_t in_parse_          : 1;
      uint64_t in_pl_parse_       : 1;
      uint64_t in_get_plan_cache_ : 1;
      uint64_t in_sql_optimize_   : 1;
      uint64_t in_sql_execution_  : 1;
      uint64_t in_px_execution_   : 1;
      uint64_t in_sequence_load_  : 1;
      uint64_t in_committing_     : 1;
      uint64_t in_storage_read_   : 1;
      uint64_t in_storage_write_  : 1;
      uint64_t in_das_remote_exec_: 1;
      uint64_t in_plsql_compilation_  : 1;
      uint64_t in_plsql_execution_  : 1;
      uint64_t in_filter_rows_: 1;
      uint64_t in_rpc_encode_: 1;
      uint64_t in_rpc_decode_: 1;
      uint64_t in_connection_mgr_: 1;
      uint64_t in_check_row_confliction_: 1;
    };
  };
};

}
}
#endif /* _OB_SHARE_ASH_EXECUTION_PHASE_H_ */
//// end of header file
