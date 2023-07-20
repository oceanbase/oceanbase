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
#define USING_LOG_PREFIX OBLOG

#include "ob_log_meta_data_replayer.h"
#include "ob_log_part_trans_task.h"

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[LOG_META_DATA] [REPALYER] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[LOG_META_DATA] [REPALYER] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)
#define DSTAT(fmt, args...) STAT(DEBUG, fmt, ##args)

namespace oceanbase
{
namespace libobcdc
{
ObLogMetaDataReplayer::ObLogMetaDataReplayer() :
    is_inited_(false),
    queue_(),
    schema_inc_replay_()
{
}

ObLogMetaDataReplayer::~ObLogMetaDataReplayer()
{
  destroy();
}

int ObLogMetaDataReplayer::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_FAIL(schema_inc_replay_.init(true/*is_start_progress*/))) {
    LOG_ERROR("schema_inc_replay_ init failed", KR(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObLogMetaDataReplayer::destroy()
{
  if (IS_INIT) {
    queue_.reset();
    schema_inc_replay_.destroy();
    is_inited_ = false;
  }
}

int ObLogMetaDataReplayer::push(PartTransTask *task, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  UNUSED(timeout);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataSQLQueryer is not initialized", KR(ret));
  } else if (OB_UNLIKELY(! task->is_task_info_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), KPC(task));
  } else {
    ISTAT("push task success", KPC(task));
    queue_.push(task);
  }

  return ret;
}

int ObLogMetaDataReplayer::replay(
    const uint64_t tenant_id,
    const int64_t start_timestamp_ns,
    ObDictTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataSQLQueryer is not initialized", KR(ret));
  } else {
    ISTAT("BEGIN", K(tenant_id), K(start_timestamp_ns), "start_timestamp", NTS_TO_STR(start_timestamp_ns));
    bool is_done = false;
    ReplayInfoStat replay_info_stat;

    while (OB_SUCC(ret) && ! is_done) {
      PartTransTask *part_trans_task = queue_.pop();

      if (nullptr == part_trans_task) {
        ISTAT("Current The PartTransTask has not been received, need wait", K(tenant_id), K(start_timestamp_ns));
        usec_sleep(50 * 1000L);
      } else {
        const uint64_t task_tenant_id = part_trans_task->get_tenant_id();
        replay_info_stat.total_part_trans_task_count_++;

        if (OB_UNLIKELY(tenant_id != task_tenant_id)) {
          ret = OB_STATE_NOT_MATCH;
          LOG_ERROR("tenant_id is not equal to task_tenant_id", KR(ret), K(tenant_id), K(task_tenant_id),
              KPC(part_trans_task));
        } else if (part_trans_task->is_ddl_trans()) {
          if (OB_FAIL(handle_ddl_trans_(start_timestamp_ns, tenant_info, *part_trans_task, replay_info_stat))) {
            LOG_ERROR("handle_ddl_trans_ failed", KR(ret), K(tenant_id), K(start_timestamp_ns), K(tenant_info),
                KPC(part_trans_task));
          }
        } else if (part_trans_task->is_ls_op_trans()) {
          if (OB_FAIL(handle_ls_op_trans_(start_timestamp_ns, tenant_info, *part_trans_task, replay_info_stat))) {
            LOG_ERROR("handle_ls_op_trans_ failed", KR(ret), K(tenant_id), K(start_timestamp_ns), K(tenant_info),
                KPC(part_trans_task));
          } else {
            LOG_TRACE("handle_ls_op_trans_ succ", K(tenant_id), KPC(part_trans_task), K(tenant_info));
          }
        } else if (part_trans_task->is_offline_ls_task()) {
          is_done = true;
          LOG_INFO("Offline part_trans_task", KPC(part_trans_task));
        } else {
          // Other tasks, do nothing
        }

        if (nullptr != part_trans_task) {
          part_trans_task->revert();
        }
      }
    } // while

    _ISTAT("END tenant_id=%ld start_timestamp_ns=%ld(%s) "
        "TRANS_COUNT(TOTAL=%ld DDL_TRANS=%ld/%ld LS_OP=%ld)",
        tenant_id, start_timestamp_ns, NTS_TO_STR(start_timestamp_ns),
        replay_info_stat.total_part_trans_task_count_,
        replay_info_stat.ddl_part_trans_task_repalyed_count_,
        replay_info_stat.ddl_part_trans_task_toal_count_,
        replay_info_stat.ls_op_part_trans_task_count_);
  }

  return ret;
}

int ObLogMetaDataReplayer::handle_ddl_trans_(
    const int64_t start_timestamp_ns,
    ObDictTenantInfo &tenant_info,
    PartTransTask &part_trans_task,
    ReplayInfoStat &replay_info_stat)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! part_trans_task.is_ddl_trans())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("PartTransTask is not ddl trans, unexpected", KR(ret), K(part_trans_task));
  } else {
    replay_info_stat.ddl_part_trans_task_toal_count_++;
    const int64_t trans_commit_version = part_trans_task.get_trans_commit_version();

    // Only DDL transactions less than or equal to the start timestamp need to be replayed
    if (trans_commit_version <= start_timestamp_ns) {
      DSTAT("handle DDL_TRANS to be replayed", K(part_trans_task));
      replay_info_stat.ddl_part_trans_task_repalyed_count_++;

      if (OB_FAIL(part_trans_task.parse_multi_data_source_data_for_ddl("ObLogMetaDataReplayer"))) {
        LOG_ERROR("parse_multi_data_source_data_for_ddl failed", KR(ret), K(part_trans_task));
      } else {
        DictTenantArray &tenant_metas = part_trans_task.get_dict_tenant_array();
        DictDatabaseArray &database_metas = part_trans_task.get_dict_database_array();
        DictTableArray &table_metas = part_trans_task.get_dict_table_array();

        if (OB_FAIL(schema_inc_replay_.replay(part_trans_task, tenant_metas, database_metas, table_metas, tenant_info))) {
          LOG_ERROR("schema_inc_replay_ replay failed", KR(ret), K(part_trans_task), K(tenant_info));
        } else {}
      }

    } else {
      ISTAT("ignore DDL_TRANS PartTransTask which trans commit verison is greater than start_timestamp_ns",
          "tenant_id", part_trans_task.get_tenant_id(),
          "trans_id", part_trans_task.get_trans_id(),
          K(trans_commit_version), K(start_timestamp_ns));
    }
  }

  return ret;
}

int ObLogMetaDataReplayer::handle_ls_op_trans_(
    const int64_t start_timestamp_ns,
    ObDictTenantInfo &tenant_info,
    PartTransTask &part_trans_task,
    ReplayInfoStat &replay_info_stat)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! part_trans_task.is_ls_op_trans())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("PartTransTask is not ls op trans, unexpected", KR(ret), K(part_trans_task));
  } else {
    const int64_t part_trans_commit_version = part_trans_task.get_trans_commit_version();
    replay_info_stat.ls_op_part_trans_task_count_++;
    const share::ObLSAttrArray &ls_attr_arr = part_trans_task.get_ls_attr_arr();
    int64_t idx = 0;
    int64_t ls_attr_cnt = 0;
    if (part_trans_commit_version <= start_timestamp_ns) {
      ARRAY_FOREACH_N(ls_attr_arr, idx, ls_attr_cnt)
      {
        const share::ObLSAttr &ls_attr = ls_attr_arr.at(idx);
        if (OB_UNLIKELY(! ls_attr.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("ls_attr is not valid", KR(ret), K(idx), K(ls_attr_cnt), K(ls_attr), K(ls_attr_arr));
        } else if (OB_FAIL(tenant_info.incremental_data_update(ls_attr))) {
          LOG_ERROR("tenant_info incremental_data_update failed", KR(ret), K(part_trans_task), K(tenant_info),
              K(idx), K(ls_attr), K(ls_attr_cnt), K(ls_attr_arr));
        }
      }
    } else {
      ISTAT("ignore LS_OP_TRANS PartTransTask which trans commit verison is greater than start_timestamp_ns",
          "tenant_id", part_trans_task.get_tenant_id(),
          "trans_id", part_trans_task.get_trans_id(),
          K(ls_attr_arr), K(part_trans_commit_version), K(start_timestamp_ns));
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

#undef _STAT
#undef STAT
#undef _ISTAT
#undef ISTAT
#undef _DSTAT
#undef DSTAT
