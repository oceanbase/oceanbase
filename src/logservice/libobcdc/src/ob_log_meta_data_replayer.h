/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OB_LOG_META_DATA_REPLAYER_H_
#define OCEANBASE_OB_LOG_META_DATA_REPLAYER_H_

#include "lib/utility/ob_macro_utils.h"         // DISALLOW_COPY_AND_ASSIGN, CACHE_ALIGNED
#include "ob_log_part_trans_task_queue.h"       // SafePartTransTaskQueue
#include "ob_log_meta_data_struct.h"            // ObDictTenantInfo
#include "ob_log_schema_incremental_replay.h"   // ObLogSchemaIncReplay
#include "ob_log_part_trans_parser.h"           // IObLogPartTransParser

namespace oceanbase
{
namespace libobcdc
{
class PartTransTask;
class IObLogMetaDataReplayer
{
public:
  virtual ~IObLogMetaDataReplayer() {}

  virtual int push(PartTransTask *task, const int64_t timeout) = 0;

  virtual int replay(
      const uint64_t tenant_id,
      const int64_t start_timestamp_ns,
      ObDictTenantInfo &tenant_info) = 0;
};

class ObLogMetaDataReplayer : public IObLogMetaDataReplayer
{
public:
  ObLogMetaDataReplayer();
  virtual ~ObLogMetaDataReplayer();

  virtual int push(PartTransTask *task, const int64_t timeout);

  virtual int replay(
      const uint64_t tenant_id,
      const int64_t start_timestamp_ns,
      ObDictTenantInfo &tenant_info);

public:
  int init(IObLogPartTransParser &part_trans_parser);
  void destroy();

private:
  struct ReplayInfoStat
  {
    ReplayInfoStat() { reset(); }
    ~ReplayInfoStat() { reset(); }

    void reset()
    {
      total_part_trans_task_count_ = 0;
      ddl_part_trans_task_toal_count_ = 0;
      ddl_part_trans_task_replayed_count_ = 0;
      ls_op_part_trans_task_count_ = 0;
    }

    int64_t total_part_trans_task_count_;
    int64_t ddl_part_trans_task_toal_count_;
    int64_t ddl_part_trans_task_replayed_count_;
    int64_t ls_op_part_trans_task_count_;
  };

  // handle DDL transaction
  int handle_ddl_trans_(
      const int64_t start_timestamp_ns,
      ObDictTenantInfo &tenant_info,
      PartTransTask &part_trans_task,
      ReplayInfoStat &replay_info_stat);
  // handle LogStream operation transaction
  int handle_ls_op_trans_(
      const int64_t start_timestamp_ns,
      ObDictTenantInfo &tenant_info,
      PartTransTask &part_trans_task,
      ReplayInfoStat &replay_info_stat);

  bool need_remove_by_op_type_(const ObSchemaOperationType op_type)
  {
    return OB_DDL_DROP_TABLE == op_type || OB_DDL_DROP_INDEX == op_type || OB_DDL_DROP_GLOBAL_INDEX == op_type;
  }

private:
  bool is_inited_;
  SafePartTransTaskQueue queue_;
  ObLogSchemaIncReplay schema_inc_replay_;
  IObLogPartTransParser *part_trans_parser_;

  DISALLOW_COPY_AND_ASSIGN(ObLogMetaDataReplayer);
};

// IObLogSysLsTaskHandler

} // namespace libobcdc
} // namespace oceanbase

#endif
