/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_WRITER_ARGS_STRUCT_H_
#define OCEANBASE_STORAGE_OB_WRITER_ARGS_STRUCT_H_

#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
namespace storage
{

enum ObWriterType {
  INVALID_WRITER_TYPE = -1,
  CG_MICRO_BLOCK_WRITER_TYPE = 0,
  DAG_CG_MACRO_BLOCK_WRITER_TYPE = 1,
  MAX_WRITER_TYPE
};

struct ObWriterArgs
{
public:
  ObWriterArgs() :
    is_inited_(false),
    parallel_idx_(-1),
    data_desc_(),
    index_builder_(true/*use_double_write_macro_buffer*/),
    macro_seq_param_(),
    pre_warm_param_(),
    object_cleaner_(nullptr),
    ddl_redo_callback_(nullptr) { }
  ~ObWriterArgs()
  {
    if (OB_NOT_NULL(ddl_redo_callback_)) {
      common::ob_delete(ddl_redo_callback_);
    }
  }
  int init(const ObWriteMacroParam &param,
           const ObWriterType writer_type);
  void reset();
  OB_INLINE bool is_need_submit_io_type(const ObWriterType writer_type) const
  {
    return ObWriterType::DAG_CG_MACRO_BLOCK_WRITER_TYPE == writer_type;
  }
  OB_INLINE bool is_need_ddl_redo_callback_type(const ObWriterType writer_type) const
  {
    return ObWriterType::DAG_CG_MACRO_BLOCK_WRITER_TYPE == writer_type;
  }
  OB_INLINE bool is_need_difference_start_seqence(const ObWriterType writer_type)
  {
    return ObWriterType::DAG_CG_MACRO_BLOCK_WRITER_TYPE == writer_type;
  }
  OB_INLINE bool is_valid_type(const ObWriterType writer_type)
  {
    return writer_type < ObWriterType::MAX_WRITER_TYPE &&
           writer_type > ObWriterType::INVALID_WRITER_TYPE;
  }
  TO_STRING_KV(K(is_inited_), K(parallel_idx_), K(data_desc_), K(index_builder_),
               K(macro_seq_param_), K(pre_warm_param_), KP(object_cleaner_));

public:
  bool is_inited_;
  int64_t parallel_idx_;
  blocksstable::ObWholeDataStoreDesc data_desc_;
  blocksstable::ObSSTableIndexBuilder index_builder_;
  blocksstable::ObMacroSeqParam macro_seq_param_;
  share::ObPreWarmerParam pre_warm_param_;
  blocksstable::ObISSTableObjectCleaner *object_cleaner_;
  blocksstable::ObIMacroBlockFlushCallback *ddl_redo_callback_;
};

} // end namespace storage
} // end namespace oceanbase
#endif // OCEANBASE_STORAGE_OB_WRITER_ARGS_STRUCT_H_