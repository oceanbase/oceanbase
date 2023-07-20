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

#ifdef FLT_DEF_SPAN

#ifdef __HIGH_LEVEL_SPAN
FLT_DEF_SPAN(ObServer, "server")
FLT_DEF_SPAN(ObProxy, "proxy")
FLT_DEF_SPAN(ObSql, "sql")
FLT_DEF_SPAN(ObTrans, "transaction")
FLT_DEF_SPAN(ObStorage, "storage")

FLT_DEF_SPAN(com_query_process, "com_query process")
  // **** for sql ****
  FLT_DEF_SPAN(mpquery_single_stmt, "process one statement")
    FLT_DEF_SPAN(sql_compile, "compile one query")
      FLT_DEF_SPAN(pc_get_plan, "get plan from plan cache")
      FLT_DEF_SPAN(pc_add_plan, "adds plan to plan cache")
      FLT_DEF_SPAN(hard_parse, "do hard parse")
      FLT_DEF_SPAN(parse, "generate syntax tree")
      FLT_DEF_SPAN(resolve, "resolve syntax tree's semantics and generate statement")
      FLT_DEF_SPAN(rewrite, "transform statement")
      FLT_DEF_SPAN(optimize, "do cost-base optimization and generate log plan")
      FLT_DEF_SPAN(code_generate, "generate physical plan according to log plan")
    FLT_DEF_SPAN(sql_execute, "execute physical plan")
      //TODO shengle code of open interface need refator
      FLT_DEF_SPAN(open, "open plan")
      FLT_DEF_SPAN(response_result, "process plan and get result")
        FLT_DEF_SPAN(px_schedule, "schedule tasks divided by px")
        FLT_DEF_SPAN(px_task, "execution of px's schedule")
      FLT_DEF_SPAN(close, "close plan")
    FLT_DEF_SPAN(cmd_execute, "command execute")
      FLT_DEF_SPAN(cmd_open, "command open")
    FLT_DEF_SPAN(remote_execute, "remote execute")
    FLT_DEF_SPAN(remote_compile, "compile for remote sql")
  // **** for sql end ****

  // **** for pl ****
  FLT_DEF_SPAN(pl_entry, "pl process")
    FLT_DEF_SPAN(pl_compile, "compile one pl object")
    FLT_DEF_SPAN(pc_get_pl_object, "get pl object from plan cache")
    FLT_DEF_SPAN(pc_add_pl_object, "add pl object to plan cache")
    FLT_DEF_SPAN(pl_execute, "execute pl object")
      FLT_DEF_SPAN(pl_spi_query, "execute pl spi query")
      FLT_DEF_SPAN(pl_spi_prepare, "prepare phase of pl execution")
      FLT_DEF_SPAN(pl_spi_execute, "execute phase of pl execution")
  // **** for pl end ****

  // **** for inner sql ****
  FLT_DEF_SPAN(inner_prepare, "prepare phase of inner sql")
  FLT_DEF_SPAN(inner_execute, "execute phase of inner sql")
  FLT_DEF_SPAN(inner_execute_read, "execute read inner sql")
  FLT_DEF_SPAN(inner_execute_write, "execute write inner sql")
  FLT_DEF_SPAN(inner_commit, "commit inner sql transaction")
  FLT_DEF_SPAN(inner_rollback, "rollback inner sql transaction")
  // **** for inner sql end****

  // for transaction
  FLT_DEF_SPAN(end_transaction, "end of transaction")
  // for transaction end

  // for ps
  FLT_DEF_SPAN(ps_prepare, "prepare phase of ps protocol")
  FLT_DEF_SPAN(ps_execute, "execute phase of ps protocol")
  FLT_DEF_SPAN(ps_close, "close phase of ps protocol")
  // for ps end

  // for das
  FLT_DEF_SPAN(do_local_das_task, "execute local das task")
  FLT_DEF_SPAN(do_async_remote_das_task, "execute async remote das task")
    FLT_DEF_SPAN(das_async_rpc_process, "das task async rpc process")
  FLT_DEF_SPAN(do_sync_remote_das_task, "execute sync remote das task")
    FLT_DEF_SPAN(das_sync_rpc_process, "das task sync rpc process")
  FLT_DEF_SPAN(close_das_task, "close das task")
  FLT_DEF_SPAN(fetch_das_extra_result, "fetch das extra result")
    FLT_DEF_SPAN(fetch_das_result_process, "fetch das result process")

  // for ddl task
  FLT_DEF_SPAN(ddl_prepare, "prepare phase of ddl task")
  FLT_DEF_SPAN(ddl_failure_cleanup, "cleanup phase of ddl task failure")
  FLT_DEF_SPAN(ddl_success, "success check of ddl task")
  FLT_DEF_SPAN(ddl_wait_trans_end, "wait for all previous transaction finish")
  FLT_DEF_SPAN(ddl_lock_table, "lock table")
  FLT_DEF_SPAN(ddl_redefinition, "redefinition phase of ddl task")
  FLT_DEF_SPAN(ddl_copy_table_dependent_objects, "copy table dependent object")
  FLT_DEF_SPAN(ddl_modify_autoinc, "modify autoincrease column")
  FLT_DEF_SPAN(ddl_take_effect, "index take effect")
  FLT_DEF_SPAN(ddl_check_table_empty, "check table empty")

  // build index task
  FLT_DEF_SPAN(ddl_build_index, "ddl build index task")
  FLT_DEF_SPAN(ddl_validate_checksum, "ddl task verify checksum")

  // drop index task
  FLT_DEF_SPAN(ddl_drop_index, "ddl drop index task")
  FLT_DEF_SPAN(ddl_set_write_only, "set index to write only mode")
  FLT_DEF_SPAN(ddl_set_unusable, "set index to unusable state")
  FLT_DEF_SPAN(ddl_drop_schema, "drop index schema")

  // drop primary key task
  FLT_DEF_SPAN(ddl_drop_primary_key, "ddl drop primary key task")

  // table redefinition task
  FLT_DEF_SPAN(ddl_table_redefinition, "ddl table redefinition task")

  // column redefinition task
  FLT_DEF_SPAN(ddl_column_redefinition, "ddl column redefinition task")

  // ddl constraint task
  FLT_DEF_SPAN(ddl_constraint, "ddl constraint task")
  FLT_DEF_SPAN(ddl_check_constraint_valid, "check constraint valid")
  FLT_DEF_SPAN(ddl_set_constraint_valid, "set constraint valid")

  // ddl modify autoinc
  FLT_DEF_SPAN(ddl_modify_autoinc_task, "ddl modify autoincrease column task")

  // ddl retry task
  FLT_DEF_SPAN(ddl_retry_task, "ddl_ retry task")

#endif // __HIGH_LEVEL_SPAN

#ifdef __MIDDLE_LEVEL_SPAN
  // for das
  FLT_DEF_SPAN(get_das_id, "fetch das task id")
  FLT_DEF_SPAN(rescan_das_task, "rescan das task")
#endif // __MIDDLE_LEVEL_SPAN

#ifdef __LOW_LEVEL_SPAN
#endif // __LOW_LEVEL_SPAN

#endif

#ifdef FLT_DEF_TAG

#ifdef __HIGH_LEVEL_TAG
FLT_DEF_TAG(sess_id, "session id of current request")
FLT_DEF_TAG(receive_ts, "timestamp of receive request")
FLT_DEF_TAG(log_trace_id, "trace id in log for current request")
FLT_DEF_TAG(sql_text, "SQL text")
FLT_DEF_TAG(sql_id, "SQL identifier")
FLT_DEF_TAG(hit_plan, "plan cache hit plan")
FLT_DEF_TAG(err_code, "error code of current request")
FLT_DEF_TAG(database_id, "database id of current request")
FLT_DEF_TAG(plan_hash, "hash value of current request's execution plan")
FLT_DEF_TAG(client_info, "client info of current session")
FLT_DEF_TAG(module_name, "module name of current session")
FLT_DEF_TAG(action_name, "action name of current session")

// optimizor

// px TODO shengle SET_TAG
FLT_DEF_TAG(dfo_id, "data flow operation id, dfo consists of a partial subplan")
FLT_DEF_TAG(sqc_id, "sub query coordinator id, sqc is for scheduling the execution of multiple tasks of this server")
FLT_DEF_TAG(qc_id, "id of query coordinator(qc), qc is for scheduling the execution of the plan")
FLT_DEF_TAG(used_worker_cnt, "count of used worker")
FLT_DEF_TAG(task_id, "px logical task id")
FLT_DEF_TAG(group_id, "resource group id, the current thread belongs to")
FLT_DEF_TAG(sqc_resp_finish_ts, "sqc finish response qc timestamp")

// transaction  TODO shengle SET_TAG
FLT_DEF_TAG(trans_id, "transaction identifier")
FLT_DEF_TAG(trans_type, "transaction type : 1 is for sp trans and 2 is for dist trans")
FLT_DEF_TAG(participant_num, "total participant number")
FLT_DEF_TAG(trans_expired_time, "transaction expired time")
FLT_DEF_TAG(trans_start_ts, "transaction start ts")
FLT_DEF_TAG(session_id, "session id")
FLT_DEF_TAG(ls_id, "logstream_id")
FLT_DEF_TAG(sche_addr, "transaction session service addr")
FLT_DEF_TAG(commit_version, "transaction commit version")
FLT_DEF_TAG(part_trans_action, "transaction decision from user")
FLT_DEF_TAG(state, "transaction 2pc state")

// storage TODO shengle SET_TAG
FLT_DEF_TAG(logical_row_cnt, "logical row scan count")
FLT_DEF_TAG(phy_row_cnt, "physical row scan count")
FLT_DEF_TAG(memtable_row_cnt, "memtable row scan count")
FLT_DEF_TAG(blockscan_row_cnt, "blockscaned row count")
FLT_DEF_TAG(pushdown_expr_filter_row_cnt, "row count filtered by pushdown expr filter")
FLT_DEF_TAG(pushdown_storage_filter_row_cnt, "row count filtered by pushdown storage filter")
FLT_DEF_TAG(row_filter_row_cnt, "row count filtered by row filter")
FLT_DEF_TAG(vector_store_row_cnt, "row count filled in vector store")
FLT_DEF_TAG(mvcc_read_row_cnt, "row count scaned by multi version read")
FLT_DEF_TAG(disk_io_cnt, "disk io count")
FLT_DEF_TAG(disk_sync_io_cnt, "disk sync io count without prefetch")
FLT_DEF_TAG(data_block_read_cnt, "data micro block read count")
FLT_DEF_TAG(index_block_read_cnt, "index micro block read count")
FLT_DEF_TAG(blockscan_block_cnt, "blockscaned micro block read count")
FLT_DEF_TAG(data_block_cache_hits, "data block cache hit count")
FLT_DEF_TAG(row_cache_hits, "row cache hit count")
FLT_DEF_TAG(fuse_row_cache_hits, "fuse row cache hit count")
FLT_DEF_TAG(bloomfilter_cache_hits, "bloomfilter cache hit count")
FLT_DEF_TAG(index_block_cache_hits, "index block cache hit count")
FLT_DEF_TAG(sstable_read_cnt, "sstable count involved in the scan")
FLT_DEF_TAG(rescan_read_cnt, "table scan iterator rescan count")

// ddl task
FLT_DEF_TAG(ddl_task_id, "ddl task id")
FLT_DEF_TAG(ddl_parent_task_id, "ddl parent task id")
FLT_DEF_TAG(ddl_data_table_id, "data table id")
FLT_DEF_TAG(ddl_index_table_id, "index table id")
FLT_DEF_TAG(ddl_snapshot_version, "snapshot version")
FLT_DEF_TAG(ddl_schema_version, "schema version")
FLT_DEF_TAG(ddl_is_unique_index, "is unique index")
FLT_DEF_TAG(ddl_is_global_index, "is global index")
FLT_DEF_TAG(ddl_need_verify, "need verify")
FLT_DEF_TAG(ddl_check_unique_snapshot, "check unique snapshot")
FLT_DEF_TAG(ddl_ret_code, "return value")

// debug
FLT_DEF_TAG(span_back_trace, "full link tracing debug")
#endif // __HIGH_LEVEL_TAG

#ifdef __MIDDLE_LEVEL_TAG
#endif // __MIDDLE_LEVEL_TAG

#ifdef __LOW_LEVEL_TAG
FLT_DEF_TAG(table_id, "place holder")
FLT_DEF_TAG(partition_id, "place holder")
FLT_DEF_TAG(column_id, "place holder")
#endif // __LOW_LEVEL_TAG

#endif

#ifndef _OB_TRACE_DEF_H
#define _OB_TRACE_DEF_H

#include <cstdint>
#include "lib/utility/ob_macro_utils.h"

enum ObTagType
{
#define FLT_DEF_TAG(name, comment) flt_##name,
#define __HIGH_LEVEL_TAG
#define __MIDDLE_LEVEL_TAG
#define __LOW_LEVEL_TAG
#include "lib/trace/ob_trace_def.h"
#undef __LOW_LEVEL_TAG
#undef __MIDDLE_LEVEL_TAG
#undef __HIGH_LEVEL_TAG
#undef FLT_DEF_TAG
};

namespace oceanbase
{
namespace trace
{
#define GET_SPANLEVEL(ID) (::oceanbase::trace::__SpanIdMapper<ID>::level)
#define GET_TAGLEVEL(ID) (::oceanbase::trace::__tag_level_mapper[ID])

enum ObSpanType
{
#define __HIGH_LEVEL_SPAN
#define __MIDDLE_LEVEL_SPAN
#define __LOW_LEVEL_SPAN
#define FLT_DEF_SPAN(name, comment) flt_##name,
#include "lib/trace/ob_trace_def.h"
#undef FLT_DEF_SPAN
#undef __LOW_LEVEL_SPAN
#undef __MIDDLE_LEVEL_SPAN
#undef __HIGH_LEVEL_SPAN
};

static const uint8_t __tag_level_mapper[] = {
#define FLT_DEF_TAG(ID, comment) 1,
#define __HIGH_LEVEL_TAG
#include "lib/trace/ob_trace_def.h"
#undef __HIGH_LEVEL_TAG
#undef FLT_DEF_TAG

#define FLT_DEF_TAG(ID, comment) 2,
#define __MIDDLE_LEVEL_TAG
#include "lib/trace/ob_trace_def.h"
#undef __MIDDLE_LEVEL_TAG
#undef FLT_DEF_TAG

#define FLT_DEF_TAG(ID, comment) 3,
#define __LOW_LEVEL_TAG
#include "lib/trace/ob_trace_def.h"
#undef __LOW_LEVEL_TAG
#undef FLT_DEF_TAG
};

static const char* __tag_name_mapper[] = {
#define FLT_DEF_TAG(name, comment) #name,
#define __HIGH_LEVEL_TAG
#define __MIDDLE_LEVEL_TAG
#define __LOW_LEVEL_TAG
#include "lib/trace/ob_trace_def.h"
#undef __LOW_LEVEL_TAG
#undef __MIDDLE_LEVEL_TAG
#undef __HIGH_LEVEL_TAG
#undef FLT_DEF_TAG
};

template <ObSpanType ID>
class __SpanIdMapper {};

#define FLT_DEF_SPAN(ID, comment)     \
template <>                           \
class __SpanIdMapper<ObSpanType::flt_##ID>  \
{                                     \
public:                               \
  static constexpr uint8_t level = 1; \
};
#define __HIGH_LEVEL_SPAN
#include "lib/trace/ob_trace_def.h"
#undef __HIGH_LEVEL_SPAN
#undef FLT_DEF_SPAN

#define FLT_DEF_SPAN(ID, comment)     \
template <>                           \
class __SpanIdMapper<ObSpanType::flt_##ID>  \
{                                     \
public:                               \
  static constexpr uint8_t level = 2; \
};
#define __MIDDLE_LEVEL_SPAN
#include "lib/trace/ob_trace_def.h"
#undef __MIDDLE_LEVEL_SPAN
#undef FLT_DEF_SPAN

#define FLT_DEF_SPAN(ID, comment)     \
template <>                           \
class __SpanIdMapper<ObSpanType::flt_##ID>  \
{                                     \
public:                               \
  static constexpr uint8_t level = 3; \
};
#define __LOW_LEVEL_SPAN
#include "lib/trace/ob_trace_def.h"
#undef __LOW_LEVEL_SPAN
#undef FLT_DEF_SPAN

} // trace
} // oceanbase

#endif /* _OB_TRACE_DEF_H */
