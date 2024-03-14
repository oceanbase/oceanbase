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

#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_FOR_EACH_MAP_FLUSH_OPERATION_H
#define STORAGE_MULTI_DATA_SOURCE_MDS_FOR_EACH_MAP_FLUSH_OPERATION_H
#include "share/scn.h"
#include "common/ob_tablet_id.h"
#include "storage/checkpoint/ob_checkpoint_diagnose.h"
namespace oceanbase
{
namespace storage
{
namespace mds
{
class MdsTableBase;
struct FlushOp {
  FlushOp(share::SCN do_flush_limit_scn, int64_t &scan_mds_table_cnt, share::SCN max_consequent_callbacked_scn, int64_t trace_id = checkpoint::INVALID_TRACE_ID)
  : do_flush_limit_scn_(do_flush_limit_scn),
  scan_mds_table_cnt_(scan_mds_table_cnt),
  max_consequent_callbacked_scn_(max_consequent_callbacked_scn),
  is_dag_full_(false),
  trace_id_(trace_id) {}
  bool operator()(const ObTabletID &, MdsTableBase *&mds_table);
  bool dag_full() const { return is_dag_full_; }
  share::SCN do_flush_limit_scn_;
  int64_t &scan_mds_table_cnt_;
  share::SCN max_consequent_callbacked_scn_;
  bool is_dag_full_;
  int64_t trace_id_;
};

}
}
}
#endif
