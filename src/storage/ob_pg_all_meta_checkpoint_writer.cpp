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

#define USING_LOG_PREFIX STORAGE

#include "ob_pg_all_meta_checkpoint_writer.h"
#include "blocksstable/ob_block_sstable_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

int ObPGCheckpointInfo::assign(const ObPGCheckpointInfo& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tables_handle_.assign(other.tables_handle_))) {
    LOG_WARN("fail to assign tables handle", K(ret));
  } else {
    pg_meta_buf_ = other.pg_meta_buf_;
    pg_meta_buf_len_ = other.pg_meta_buf_len_;
  }
  return ret;
}

ObPGCheckpointInfo::ObPGCheckpointInfo() : tables_handle_(), pg_meta_buf_(nullptr), pg_meta_buf_len_(0)
{}

void ObPGCheckpointInfo::reset()
{
  tables_handle_.reset();
  pg_meta_buf_ = nullptr;
  pg_meta_buf_len_ = 0;
}

ObPGAllMetaCheckpointWriter::ObPGAllMetaCheckpointWriter()
    : is_inited_(false), macro_meta_writer_(), pg_meta_writer_(), pg_checkpoint_info_(), file_(nullptr)
{}

int ObPGAllMetaCheckpointWriter::init(ObPGCheckpointInfo& pg_checkpoint_info, ObStorageFile* file,
    ObPGMetaItemWriter& macro_meta_writer, ObPGMetaItemWriter& pg_meta_writer)
{
  int ret = OB_SUCCESS;
  ObPGMetaItem item;
  item.set_serialize_buf(pg_checkpoint_info.pg_meta_buf_, pg_checkpoint_info.pg_meta_buf_len_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGAllMetaCheckpointWriter has already been inited", K(ret));
  } else if (OB_UNLIKELY(!pg_checkpoint_info.is_valid() || nullptr == file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_checkpoint_info), KP(file));
  } else if (OB_FAIL(macro_meta_writer_.init(pg_checkpoint_info.tables_handle_, macro_meta_writer))) {
    LOG_WARN("fail to init macro meta iterator", K(ret));
  } else if (OB_FAIL(pg_meta_writer_.init(item, pg_meta_writer))) {
    LOG_WARN("fail to init sstable meta iterator", K(ret));
  } else if (OB_FAIL(pg_checkpoint_info_.assign(pg_checkpoint_info))) {
    LOG_WARN("fail to assign pg checkpoint info", K(ret));
  } else {
    file_ = file;
    is_inited_ = true;
  }
  return ret;
}

int ObPGAllMetaCheckpointWriter::write_checkpoint()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGAllMetaCheckpointWriter has not been inited", K(ret));
  } else if (OB_FAIL(macro_meta_writer_.write_checkpoint())) {
    LOG_WARN("fail to write macro meta checkpoint", K(ret));
  } else if (OB_FAIL(pg_meta_writer_.write_checkpoint())) {
    LOG_WARN("fail to write pg meta checkpoint", K(ret));
  } else {
    LOG_INFO("write checkpoint", K(file_->get_tenant_id()), K(file_->get_file_id()));
  }
  return ret;
}

void ObPGAllMetaCheckpointWriter::reset()
{
  is_inited_ = false;
  macro_meta_writer_.reset();
  pg_meta_writer_.reset();
  pg_checkpoint_info_.reset();
  file_ = nullptr;
}
