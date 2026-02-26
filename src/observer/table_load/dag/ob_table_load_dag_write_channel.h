/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "observer/table_load/dag/ob_table_load_dag_task.h"
#include "share/table/ob_table_load_row_array.h"
#include "storage/ddl/ob_pipeline.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadDatumRow;
class ObDirectLoadBatchRows;
} // namespace storage
namespace observer
{
class ObTableLoadStoreTrans;
class ObTableLoadTransStoreWriter;
class ObTableLoadDagChunkWriter;

// 线程不安全
// 对于px路径, px_write和close都是同步调用的
// 对于非px路径, 控制节点会通过检查trans状态来确认后台的write和close结束
class ObTableLoadDagWriter
{
public:
  ObTableLoadDagWriter() = default;
  virtual ~ObTableLoadDagWriter() = default;
  virtual int write(const table::ObTableLoadTabletObjRowArray &row_array) = 0;
  virtual int px_write(common::ObIVector *tablet_id_vector,
                       const storage::ObDirectLoadBatchRows &batch_rows) = 0;
  virtual int close() = 0;
};

class ObTableLoadDagWriteChannel
{
public:
  ObTableLoadDagWriteChannel();
  virtual ~ObTableLoadDagWriteChannel() = default;
  int create_writer(ObTableLoadStoreTrans *trans, ObTableLoadTransStoreWriter *store_writer,
                    const int32_t session_id, ObTableLoadDagWriter *&writer,
                    ObIAllocator &allocator);
  // 控制节点保证所有writer都被close后再调用flush
  int flush();
  int close();

  bool is_flushed() const { return is_flushed_; }

protected:
  int inner_init();
  int inner_flush();
  virtual int create_writer(ObTableLoadDagChunkWriter *&writer, ObIAllocator &allocator) = 0;
  virtual int do_flush() { return OB_SUCCESS; }
  virtual int do_close() = 0;

protected:
  class FlushTask final : public share::ObITask
  {
  public:
    FlushTask(ObTableLoadDagWriteChannel *write_channel)
      : ObITask(TASK_TYPE_DIRECT_LOAD_WRITE_CHANNEL_FLUSH), write_channel_(write_channel)
    {
    }
    virtual ~FlushTask() = default;
    int process() override { return write_channel_->inner_flush(); }

  private:
    ObTableLoadDagWriteChannel *write_channel_;
  };

public:
  class FinishTask : public share::ObITask
  {
  public:
    FinishTask(ObTableLoadDagWriteChannel *write_channel)
      : ObITask(TASK_TYPE_DIRECT_LOAD_WRITE_CHANNEL_FINISH), write_channel_(write_channel)
    {
    }
    virtual ~FinishTask() = default;
    ObITaskPriority get_priority() override
    {
      return write_channel_->is_flushed() ? TASK_PRIO_1 : TASK_PRIO_0;
    }
    int process() override { return OB_SUCCESS; }

  private:
    ObTableLoadDagWriteChannel *write_channel_;
  };

public:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadDag *dag_;

protected:
  FlushTask *flush_task_;
  bool is_flushed_;
  bool is_closed_;
  bool is_inited_;
};

class ObTableLoadDagChunkWriter : public ObTableLoadDagWriter
{
public:
  ObTableLoadDagChunkWriter();
  virtual ~ObTableLoadDagChunkWriter() = default;
  virtual int init(ObTableLoadDagWriteChannel *write_channel, ObTableLoadStoreTrans *trans,
                   ObTableLoadTransStoreWriter *store_writer, const int32_t session_id) = 0;
  int write(const table::ObTableLoadTabletObjRowArray &row_array) override;
  int px_write(common::ObIVector *tablet_id_vector,
               const storage::ObDirectLoadBatchRows &batch_rows) override;
  int close() override { return close(trans_, session_id_); }

protected:
  virtual int append_row(const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row) = 0;
  virtual int append_batch(common::ObIVector *tablet_id_vector,
                           const storage::ObDirectLoadBatchRows &batch_rows) = 0;
  virtual int close(ObTableLoadStoreTrans *trans, const int32_t session_id) = 0;

protected:
  ObTableLoadDag *dag_;
  ObTableLoadStoreTrans *trans_;
  ObTableLoadTransStoreWriter *store_writer_;
  int32_t session_id_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
