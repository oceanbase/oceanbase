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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/px/ob_px_interruption.h"
#include "sql/engine/px/ob_dfo.h"
#include "lib/time/ob_time_utility.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

OB_SERIALIZE_MEMBER(ObPxInterruptID, query_interrupt_id_, px_interrupt_id_);

ObPxInterruptGuard::ObPxInterruptGuard(const ObInterruptibleTaskID &interrupt_id)
{
  interrupt_id_ = interrupt_id;
  interrupt_reg_ret_ = SET_INTERRUPTABLE(interrupt_id_);
}

ObPxInterruptGuard::~ObPxInterruptGuard()
{
  if (OB_SUCCESS == interrupt_reg_ret_) {
    UNSET_INTERRUPTABLE(interrupt_id_);
  }
}

int ObInterruptUtil::broadcast_px(ObIArray<ObDfo *> &dfos, int int_code)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t idx = 0; idx < dfos.count(); ++idx) {
    if (OB_SUCCESS != (tmp_ret = broadcast_dfo(dfos.at(idx), int_code))) {
      LOG_WARN("fail interrupt dfo", K(idx), K(ret));
    }
  }
  return ret;
}

int ObInterruptUtil::broadcast_dfo(ObDfo *dfo, int code)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInterruptCode int_code(code,
                           GETTID(),
                           GCTX.self_addr(),
                           "PX ABORT DFO");
  ObGlobalInterruptManager *manager = ObGlobalInterruptManager::getInstance();
  ObSEArray<ObPxSqcMeta *, 32> sqcs;
  if (OB_ISNULL(dfo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL ptr unexpected", K(ret));
  } else if (OB_FAIL(dfo->get_sqcs(sqcs))) {
    LOG_WARN("fail to get addrs", K(ret));
  } else {
    // 暂存上次的 id，inc_seqnum 将修改 px_interrupt_id_
    ObInterruptibleTaskID interrupt_id = dfo->get_interrupt_id().px_interrupt_id_;
    for (int64_t j = 0; j < sqcs.count(); ++j) {
      const ObAddr &addr = sqcs.at(j)->get_exec_addr();
      if(OB_SUCCESS != (tmp_ret = manager->interrupt(addr, interrupt_id, int_code))) {
        ret = tmp_ret;
        LOG_WARN("fail to send interrupt message to other server",
                K(ret), K(int_code), K(addr), K(interrupt_id));
      } else {
        LOG_INFO("success to send interrupt message",
                  K(int_code), K(addr), K(interrupt_id));
      }
    }
  }
  return ret;
}

int ObInterruptUtil::regenerate_interrupt_id(ObDfo &dfo)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPxSqcMeta *, 32> sqcs;
  if (OB_FAIL(dfo.get_sqcs(sqcs))) {
    LOG_WARN("fail to get addrs", K(ret));
  } else {
    // 每次发送完中断后，需要将中断号的 sequence 加 1，并设置到 sqc 结构中，
    // 避免误中断重试的 sqc
    ObDfoInterruptIdGen::inc_seqnum(dfo.get_interrupt_id().px_interrupt_id_);

    ARRAY_FOREACH_X(sqcs, j, cnt, OB_SUCC(ret)) {
      sqcs.at(j)->set_interrupt_id(dfo.get_interrupt_id());
    }
  }
  return ret;
}

// 兜底函数，SQC 通知 task 尽快退出
int ObInterruptUtil::interrupt_tasks(ObPxSqcMeta &sqc, int code)
{
  int ret = OB_SUCCESS;
  ObInterruptCode int_code(code,
                           GETTID(),
                           GCTX.self_addr(),
                           "SQC ABORT TASK");
  ObGlobalInterruptManager *manager = ObGlobalInterruptManager::getInstance();
  ObInterruptibleTaskID interrupt_id = sqc.get_interrupt_id().px_interrupt_id_;
  if(OB_FAIL(manager->interrupt(interrupt_id, int_code))) {
    LOG_WARN("fail to send interrupt message to other server",
             K(ret), K(int_code), K(interrupt_id));
  } else {
    LOG_INFO("success to send interrupt message to local tasks",
             K(int_code), K(interrupt_id));
  }
  return ret;
}

void ObInterruptUtil::update_schema_error_code(ObExecContext *exec_ctx, int &code)
{
  int ret = OB_SUCCESS;
  if (is_schema_error(code) && OB_NOT_NULL(exec_ctx) && OB_NOT_NULL(exec_ctx->get_my_session())) {
    uint64_t tenant_id = exec_ctx->get_my_session()->get_effective_tenant_id();
    ObSchemaGetterGuard schema_guard;
    int64_t local_schema_version = -1;
    int64_t query_tenant_begin_schema_version =
      exec_ctx->get_task_exec_ctx().get_query_tenant_begin_schema_version();
    if (query_tenant_begin_schema_version == OB_INVALID_VERSION) {
      code = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant_schema_version", K(ret), K(query_tenant_begin_schema_version));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get tenant schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_schema_version))) {
      LOG_WARN("get schema version failed", K(ret));
    }

    if ((OB_SUCC(ret) && local_schema_version != query_tenant_begin_schema_version)
        || ret == OB_TENANT_NOT_EXIST || ret == OB_SCHEMA_ERROR || ret == OB_SCHEMA_EAGAIN) {
      code = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
    }

    // overwrite to make sure sql will retry
    if (OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == code
        && GSCHEMASERVICE.is_schema_error_need_retry(NULL, tenant_id)) {
      code = OB_ERR_REMOTE_SCHEMA_NOT_FULL;
    }

    LOG_TRACE("update_schema_error_code, exec_ctx is not null", K(tenant_id), K(local_schema_version),
              K(exec_ctx->get_task_exec_ctx().get_query_tenant_begin_schema_version()), K(lbt()));
  } else {
    LOG_TRACE("update_schema_error_code, exec_ctx is null", K(lbt()));
  }
}

// SQC 向 QC 发送中断
int ObInterruptUtil::interrupt_qc(ObPxSqcMeta &sqc, int code, ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  ObInterruptCode int_code(code,
                           GETTID(),
                           GCTX.self_addr(),
                           "SQC ABORT QC");
  ObInterruptCode orig_int_code = int_code;
  ObGlobalInterruptManager *manager = ObGlobalInterruptManager::getInstance();
  ObInterruptibleTaskID interrupt_id = sqc.get_interrupt_id().query_interrupt_id_;

  update_schema_error_code(exec_ctx, int_code.code_);
  if (OB_ISNULL(manager)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(manager->interrupt(sqc.get_qc_addr(),
                                        interrupt_id,
                                        int_code))) {
    LOG_WARN("fail send interrupt signal to qc",
              "addr", sqc.get_qc_addr(),
              K(orig_int_code),
              K(int_code),
              K(ret));
  } else {
    LOG_TRACE("sqc notify qc to interrupt",
              "qc_addr", sqc.get_qc_addr(),
              "qc_id", sqc.get_qc_id(),
              "interrupt_id", interrupt_id,
              "sqc_id", sqc.get_sqc_id(),
              K(orig_int_code),
              K(int_code));
  }
  return ret;
}


// Task 向 QC 发送中断
int ObInterruptUtil::interrupt_qc(ObPxTask &task, int code, ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  ObInterruptCode int_code(code,
                           GETTID(),
                           GCTX.self_addr(),
                           "TASK ABORT QC");
  ObInterruptCode orig_int_code = int_code;
  ObGlobalInterruptManager *manager = ObGlobalInterruptManager::getInstance();
  ObInterruptibleTaskID interrupt_id = task.get_interrupt_id().query_interrupt_id_;

  update_schema_error_code(exec_ctx, int_code.code_);
  if (OB_ISNULL(manager)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(manager->interrupt(task.get_qc_addr(),
                                        interrupt_id,
                                        int_code))) {
    LOG_WARN("fail send interrupt signal to qc",
              "addr", task.get_qc_addr(),
              K(int_code),
              K(orig_int_code),
              K(ret));
  } else {
    LOG_TRACE("task notify qc to interrupt",
              "qc_addr", task.get_sqc_addr(),
              "qc_id", task.get_qc_id(),
              "task_id", task.get_task_id(),
              "task_co_id", task.get_task_co_id(),
              "interrupt_id", interrupt_id,
              K(orig_int_code),
              K(int_code));
  }
  return ret;
}

int ObInterruptUtil::generate_query_interrupt_id(const uint32_t server_id,
                                                 const uint64_t px_sequence_id,
                                                 ObInterruptibleTaskID &interrupt_id)
{
  int ret = OB_SUCCESS;
  uint64_t timestamp = ObTimeUtility::current_time();
  // 取低12位
  timestamp = (uint64_t)0xfff & timestamp;
  interrupt_id.first_ = px_sequence_id;
  // [ server_id (32bits) ][ timestamp (12bits) ]
  interrupt_id.last_ = ((uint64_t)server_id) << 32 | (uint64_t)timestamp;
  return ret;
}

int ObInterruptUtil::generate_px_interrupt_id(const uint32_t server_id,
                                              const uint32_t qc_id,
                                              const uint64_t px_sequence_id,
                                              const int64_t dfo_id,
                                              ObInterruptibleTaskID &interrupt_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(qc_id <= 0 ||
                  dfo_id < 0 ||
                  dfo_id > ObDfo::MAX_DFO_ID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("QC id is less than or equal 0 in generate px interrupt id", K(qc_id), K(dfo_id));
  } else {
    uint64_t timestamp = ObTimeUtility::current_time();
    // 取低12位
    timestamp = (uint64_t)0xfff & timestamp;
    interrupt_id.first_ = px_sequence_id;
    //
    // [ server_id (32bits) ][ qc_id (10bits)][ dfo_id (10bits) ][ timestamp (12bits) ]
    interrupt_id.last_ = ((uint64_t)server_id) << 32 | (uint64_t)qc_id << 22 |
        (uint64_t)dfo_id << 12 | (uint64_t)timestamp;
  }
  return ret;
}

void ObDfoInterruptIdGen::inc_seqnum(common::ObInterruptibleTaskID &px_interrupt_id)
{
  // 将 seq 值 (last_的低 12 位） patch 最低 12 位，每调用一次都加 1
  uint64_t last = px_interrupt_id.last_;
  px_interrupt_id.last_ = (last & (0xffffffff << 12)) | (((last & 0xfff) + 1) & 0xfff);
}

