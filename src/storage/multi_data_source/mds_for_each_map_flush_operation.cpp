/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "mds_for_each_map_flush_operation.h"
#include "mds_table_base.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

bool FlushOp::operator()(const ObTabletID &, MdsTableBase *&mds_table)
{
  int ret = OB_SUCCESS;
  if (mds_table->is_switched_to_empty_shell()) {
    MDS_LOG(INFO, "skip empty shell tablet mds_table flush",
            KPC(mds_table), K(scan_mds_table_cnt_), K_(max_consequent_callbacked_scn));
  } else if (checkpoint::INVALID_TRACE_ID != trace_id_ && FALSE_IT(mds_table->set_trace_id(trace_id_))) {
  } else if (OB_FAIL(mds_table->flush(do_flush_limit_scn_, max_consequent_callbacked_scn_))) {
    MDS_LOG(WARN, "flush mds table failed",
            KR(ret), KPC(mds_table), K_(scan_mds_table_cnt), K_(max_consequent_callbacked_scn));
    if (OB_SIZE_OVERFLOW == ret) {
      is_dag_full_ = true;
    }
  } else {
    ++scan_mds_table_cnt_;
  }
  return !is_dag_full_;// true means iterating the next mds table
}

}
}
}
