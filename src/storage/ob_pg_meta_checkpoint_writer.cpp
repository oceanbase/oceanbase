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

#include "ob_pg_meta_checkpoint_writer.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObPGMetaItem::ObPGMetaItem() : buf_(nullptr), buf_len_(0)
{}

void ObPGMetaItem::set_serialize_buf(const char* buf, const int64_t buf_len)
{
  buf_ = buf;
  buf_len_ = buf_len;
}

int ObPGMetaItem::serialize(const char*& buf, int64_t& buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, buf must not be null", K(ret));
  } else {
    buf = buf_;
    buf_len = buf_len_;
  }
  return ret;
}

ObPGMetaCheckpointWriter::ObPGMetaCheckpointWriter() : is_inited_(false), writer_(nullptr), pg_meta_()
{}

int ObPGMetaCheckpointWriter::init(ObPGMetaItem& pg_meta, ObPGMetaItemWriter& writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMetaCheckpointWriter has already been inited", K(ret));
  } else if (OB_UNLIKELY(!pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pg_meta));
  } else {
    pg_meta_ = pg_meta;
    writer_ = &writer;
    is_inited_ = true;
  }
  return ret;
}

int ObPGMetaCheckpointWriter::write_checkpoint()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaCheckpointWriter has not been inited", K(ret));
  } else if (OB_FAIL(writer_->write_item(&pg_meta_))) {
    LOG_WARN("fail to write pg meta", K(ret));
  }
  return ret;
}

void ObPGMetaCheckpointWriter::reset()
{
  is_inited_ = false;
  writer_ = nullptr;
}
