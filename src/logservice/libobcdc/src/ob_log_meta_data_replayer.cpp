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
#include "ob_log_instance.h"
#include "ob_log_resource_collector.h"

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[LOG_META_DATA] [REPLAYER] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[LOG_META_DATA] [REPLAYER] " fmt, ##args)
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
    schema_inc_replay_(),
    part_trans_parser_(NULL)
{
}

ObLogMetaDataReplayer::~ObLogMetaDataReplayer()
{
  destroy();
}

int ObLogMetaDataReplayer::init(IObLogPartTransParser &part_trans_parser)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_FAIL(schema_inc_replay_.init(true/*is_start_progress*/))) {
    LOG_ERROR("schema_inc_replay_ init failed", KR(ret));
  } else {
    part_trans_parser_ = &part_trans_parser;
    is_inited_ = true;
  }

  return ret;
}

void ObLogMetaDataReplayer::destroy()
{
  if (IS_INIT) {
    queue_.reset();
    schema_inc_replay_.destroy();
    part_trans_parser_ = NULL;
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
        replay_info_stat.ddl_part_trans_task_replayed_count_,
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
  bool stop_flag = false;

  if (OB_UNLIKELY(! part_trans_task.is_ddl_trans())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("PartTransTask is not ddl trans, unexpected", KR(ret), K(part_trans_task));
  } else {
    replay_info_stat.ddl_part_trans_task_toal_count_++;
    const int64_t trans_commit_version = part_trans_task.get_trans_commit_version();

    // Only DDL transactions less than or equal to the start timestamp need to be replayed
    if (trans_commit_version <= start_timestamp_ns) {
      DSTAT("handle DDL_TRANS to be replayed", K(part_trans_task));
      replay_info_stat.ddl_part_trans_task_replayed_count_++;

      if (OB_FAIL(part_trans_task.parse_multi_data_source_data_for_ddl("ObLogMetaDataReplayer"))) {
        LOG_ERROR("parse_multi_data_source_data_for_ddl failed", KR(ret), K(part_trans_task));
      } else {
        DictTenantArray &tenant_metas = part_trans_task.get_dict_tenant_array();
        DictDatabaseArray &database_metas = part_trans_task.get_dict_database_array();
        DictTableArray &table_metas = part_trans_task.get_dict_table_array();
        const common::ObCompatibilityMode &compatible_mode = tenant_info.get_compatibility_mode();
        lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;

        if (OB_FAIL(schema_inc_replay_.replay(part_trans_task, tenant_metas, database_metas,
            table_metas, tenant_info))) {
          LOG_ERROR("schema_inc_replay_ replay failed", KR(ret), K(part_trans_task), K(tenant_info));
        } else if (OB_FAIL(convert_to_compat_mode(compatible_mode, compat_mode))) {
          LOG_ERROR("convert to compat mode fail", KR(ret), K(compatible_mode));
        } else {
          lib::CompatModeGuard g(compat_mode);
          bool is_build_baseline = true;
          if (OB_FAIL(part_trans_parser_->parse(part_trans_task, is_build_baseline, stop_flag))) {
            LOG_ERROR("parse DDL task fail", KR(ret), K(part_trans_task), K(is_build_baseline));
          } else {
            // Iterate through each statement of the DDL
            IStmtTask *stmt_task = part_trans_task.get_stmt_list().head_;
            while (NULL != stmt_task && OB_SUCCESS == ret) {
              DdlStmtTask *ddl_stmt = dynamic_cast<DdlStmtTask *>(stmt_task);
              if (OB_UNLIKELY(! stmt_task->is_ddl_stmt()) || OB_ISNULL(ddl_stmt)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("invalid DDL statement", KR(ret), KPC(stmt_task), K(ddl_stmt),
                    "trans_id", part_trans_task.get_trans_id());
              } else {
                const ObSchemaOperationType op_type = (ObSchemaOperationType) ddl_stmt->get_operation_type();
                const uint64_t op_table_id = ddl_stmt->get_op_table_id();
                if (need_remove_by_op_type_(op_type)) {
                  if (OB_FAIL(tenant_info.remove_table_meta(op_table_id))) {
                    LOG_ERROR("ddl stmt is DROP_TABLE or DROP_INDEX and remove table meta failed", KR(ret),
                        K(op_table_id), KPC(ddl_stmt), "trans_id", part_trans_task.get_trans_id());
                  } else {
                    ISTAT("remove table meta success",
                        "ddl_op_tenant_id", ddl_stmt->get_op_tenant_id(),
                        "ddl_op_databse_id", ddl_stmt->get_op_database_id(),
                        "ddl_op_table_id", ddl_stmt->get_op_table_id(),
                        "ddl_op_tablegroup_id", ddl_stmt->get_op_tablegroup_id(),
                        "ddl_operation_type", ddl_stmt->get_operation_type(),
                        "ddl_op_schema_version", ddl_stmt->get_op_schema_version(),
                        "ddl_stmt_str", ddl_stmt->get_ddl_stmt_str(),
                        "ddl_exec_tenant_id", ddl_stmt->get_exec_tenant_id(),
                        "trans_id", part_trans_task.get_trans_id());
                  }
                }

                if (OB_SUCC(ret)) {
                  stmt_task = stmt_task->get_next();
                }
              }
            } // while

            // revert all binlog records
            if (OB_SUCC(ret)) {
              IObLogResourceCollector *resource_collector = TCTX.resource_collector_;
              if (OB_ISNULL(resource_collector)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("resource_collector is NULL", KR(ret), K(resource_collector));
              } else if (OB_FAIL(resource_collector->revert_dll_all_binlog_records(true/*is_build_baseline*/, &part_trans_task))) {
                LOG_ERROR("revert_ddl_all_binlog_records failed", KR(ret), K(part_trans_task));
              } else {
                LOG_INFO("revert_ddl_all_binlog_records success",
                    "tls_id", part_trans_task.get_tls_id(),
                    "trans_id", part_trans_task.get_trans_id(),
                    "commit_version", part_trans_task.get_trans_commit_version());
              }
            }
          }
        } // else replay
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
