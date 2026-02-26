/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/direct_load/ob_direct_load_datum_row.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;

ObDirectLoadDatumRow::ObDirectLoadDatumRow()
  : allocator_("TLD_DatumRow"),
    count_(0),
    storage_datums_(nullptr),
    seq_no_(),
    is_delete_(false),
    is_ack_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadDatumRow::~ObDirectLoadDatumRow() {}

void ObDirectLoadDatumRow::reset()
{
  count_ = 0;
  storage_datums_ = nullptr;
  seq_no_.reset();
  is_delete_ = false;
  is_ack_ = false;
  allocator_.reset();
}

void ObDirectLoadDatumRow::reuse()
{
  for (int64_t i = 0; i < count_; ++i) {
    storage_datums_[i].reuse();
  }
  seq_no_.reset();
  is_delete_ = false;
  is_ack_ = false;
}

int ObDirectLoadDatumRow::init(const int64_t count, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count_ > 0)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDatumRow init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(count));
  } else {
    void *buf = nullptr;
    allocator = nullptr != allocator ? allocator : &allocator_;
    if (OB_ISNULL(buf = allocator->alloc(sizeof(ObStorageDatum) * count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc storage datums", KR(ret), K(count));
    } else {
      count_ = count;
      storage_datums_ = new (buf) ObStorageDatum[count];
    }
  }
  return ret;
}

DEF_TO_STRING(ObDirectLoadDatumRow)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(count),
       KP_(storage_datums),
       K_(seq_no),
       K_(is_delete),
       K_(is_ack));
  if (count_ > 0 && nullptr != storage_datums_) {
    J_COMMA();
    J_NAME("datums");
    J_COLON();
    J_ARRAY_START();
    for (int64_t i = 0; i < count_; ++i) {
      if (0 == i) {
        databuff_printf(buf, buf_len, pos, "%ld:", i);
      } else {
        databuff_printf(buf, buf_len, pos, ", %ld:", i);
      }
      pos += storage_datums_[i].storage_to_string(buf + pos, buf_len - pos);
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

} // namespace storage
} // namespace oceanbase
