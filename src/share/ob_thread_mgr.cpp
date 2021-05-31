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

#include "share/ob_thread_mgr.h"
#include "lib/thread/thread_mgr.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/executor/ob_distributed_scheduler.h"
#include "clog/ob_log_scan_runnable.h"
#include "clog/ob_clog_config.h"
#include "clog/ob_clog_mgr.h"
#include "clog/ob_remote_log_query_engine.h"
#include "clog/ob_clog_history_reporter.h"
#include "storage/transaction/ob_clog_adapter.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/ob_build_index_scheduler.h"
#include "storage/transaction/ob_gts_worker.h"
#include "storage/replayengine/ob_log_replay_engine.h"
#include "storage/ob_replay_status.h"
#include "rootserver/ob_index_builder.h"
#include "observer/ob_sstable_checksum_updater.h"
#include "observer/ob_srv_deliver.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;
namespace oceanbase {
namespace share {
void ob_init_create_func()
{
#define TG_DEF(id, name, desc, scope, type, args...)            \
  lib::create_funcs_[lib::TGDefIDs::id] = []() {                \
    auto ret = OB_NEW(TGCLSMap<TGType::type>::CLS, "tg", args); \
    ret->attr_ = {#name, desc, TGScope::scope, TGType::type};   \
    return ret;                                                 \
  };
#include "share/ob_thread_define.h"
#undef TG_DEF
}
}  // end of namespace share

namespace lib {
void init_create_func()
{
  lib_init_create_func();
  share::ob_init_create_func();
}
}  // namespace lib

}  // end of namespace oceanbase
