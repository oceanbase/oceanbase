/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OB_STORAGE_COLUMN_STORE_OB_CO_MERGE_LOG_OPERATOR_H_
#define OB_STORAGE_COLUMN_STORE_OB_CO_MERGE_LOG_OPERATOR_H_

#include "storage/column_store/ob_co_merge_log.h"

namespace oceanbase
{
namespace compaction
{
class ObCOMergeLogFileWriter
{
public:
  ObCOMergeLogFileWriter()
    : is_inited_(false),
      mgr_(nullptr),
      log_buffer_writer_(nullptr),
      row_buffer_writers_(nullptr),
      allocator_(nullptr),
      log_id_(0),
      cg_count_(0)
  {}
  virtual ~ObCOMergeLogFileWriter() { reset(); }
  int init(ObIAllocator &allocator, ObBasicTabletMergeCtx &ctx, const int64_t idx);
  void reset();
  int write_merge_log(const ObMergeLog &log, const ObMergeVectorStore *vector_store, const blocksstable::ObDatumRow *full_row);
  int close();
  TO_STRING_KV(K_(is_inited), K_(cg_count));
private:
  int init_buffer_writer(ObCOMergeLogBufferWriter *&buffer_writer, ObCOMergeLogFile &file, ObCOMergeProjector *projector);
  int init_row_buffer_writers(const ObStorageSchema &schema);
  int inner_log_write(const ObMergeLog &log);
  int inner_row_write(const int64_t idx, const blocksstable::ObDatumRow &full_row);
private:
  bool is_inited_;
  ObCOMergeLogFileMgr *mgr_;
  ObCOMergeLogBufferWriter *log_buffer_writer_;
  ObCOMergeLogBufferWriter **row_buffer_writers_;
  ObIAllocator *allocator_;
  int64_t log_id_;
  int64_t cg_count_;
};

class ObCOMergeLogFileReader : public ObCOMergeLogIterator
{
public:
  ObCOMergeLogFileReader(ObIAllocator &allocator)
    : ObCOMergeLogIterator(),
      cg_idx_(-1),
      merge_log_(),
      curr_row_(),
      log_buffer_reader_(nullptr),
      row_buffer_reader_(nullptr),
      allocator_(allocator),
      cost_time_(0)
  {}
  virtual ~ObCOMergeLogFileReader() { reset(); };
  virtual int init(ObBasicTabletMergeCtx &ctx, const int64_t idx, const int64_t cg_idx) override;
  virtual void reset() override;
  virtual int get_next_log(ObMergeLog &mergelog, const ObMergeVectorStore *&vector_store, const blocksstable::ObDatumRow *&row) override;
  virtual int close() override { return OB_SUCCESS; }
  VIRTUAL_TO_STRING_KV(K_(cg_idx), K_(merge_log), K_(curr_row), K_(log_buffer_reader), K_(row_buffer_reader));
private:
  int init_buffer_reader(ObCOMergeLogBufferReader *&buffer_reader, ObCOMergeLogFile &file);
  int inner_log_read(int64_t &log_id);
  int inner_row_read(int64_t &log_id);
private:
  int64_t cg_idx_;
  ObMergeLog merge_log_;
  blocksstable::ObDatumRow curr_row_;
  ObCOMergeLogBufferReader *log_buffer_reader_;
  ObCOMergeLogBufferReader *row_buffer_reader_;
  ObIAllocator &allocator_;
  int64_t cost_time_;
};

}
}
#endif