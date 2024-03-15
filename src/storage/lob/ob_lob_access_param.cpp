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

#include "ob_lob_util.h"
#include "storage/lob/ob_lob_meta.h"

namespace oceanbase
{
namespace storage
{

bool ObLobAccessParam::has_single_chunk() const
{
  int ret = OB_SUCCESS;
  bool res = false;
  int64_t chunk_size = 0;
  if (! lib::is_mysql_mode() || byte_size_ <= 0) {
    // skip
  } else if (OB_FAIL(get_store_chunk_size(chunk_size))) {
    LOG_WARN("get_store_chunk_size fail", K(ret), KPC(this));
  } else if (byte_size_ <= chunk_size) {
    res = true;
  }
  return res;
}

bool ObLobAccessParam::enable_block_cache() const
{
  bool res = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    res = false;
  } else {
    res = byte_size_ <= tenant_config->lob_enable_block_cache_threshold;
  }
  return res;
}

int build_rowkey_range(ObLobAccessParam &param, ObRowkey &min_row_key, ObRowkey &max_row_key, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  range.table_id_ = 0; // make sure this is correct
  range.start_key_ = min_row_key;
  range.end_key_ = max_row_key;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  return ret;
}

int build_rowkey_range(ObLobAccessParam &param, ObObj key_objs[4], ObNewRange &range)
{
  const char *lob_id_ptr = reinterpret_cast<char*>(&param.lob_data_->id_);
  key_objs[0].reset();
  key_objs[0].set_varchar(lob_id_ptr, sizeof(ObLobId)); // lob_id
  key_objs[0].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  key_objs[1] = ObObj::make_min_obj(); // seq_id set min
  ObRowkey min_row_key(key_objs, 2);

  key_objs[2].reset();
  key_objs[2].set_varchar(lob_id_ptr, sizeof(ObLobId)); // lob_id
  key_objs[2].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  key_objs[3] = ObObj::make_max_obj(); // seq_id set max
  ObRowkey max_row_key(key_objs + 2, 2);
  return build_rowkey_range(param, min_row_key, max_row_key, range);
}

int build_rowkey(ObLobAccessParam &param, ObObj key_objs[4], ObString &seq_id, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  const char *lob_id_ptr = reinterpret_cast<char*>(&param.lob_data_->id_);
  key_objs[0].reset();
  key_objs[0].set_varchar(lob_id_ptr, sizeof(ObLobId)); // lob_id
  key_objs[0].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  key_objs[1].set_varchar(seq_id); // seq_id set min
  key_objs[1].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  ObRowkey min_row_key(key_objs, 2);

  key_objs[2].reset();
  key_objs[2].set_varchar(lob_id_ptr, sizeof(ObLobId)); // lob_id
  key_objs[2].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  key_objs[3].set_varchar(seq_id); // seq_id set max
  key_objs[3].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  ObRowkey max_row_key(key_objs + 2, 2);

  return build_rowkey_range(param, min_row_key, max_row_key, range);
}

int build_rowkey(ObLobAccessParam &param, ObObj key_objs[4], ObNewRange &range)
{
  int ret = OB_SUCCESS;
  void *seq_id_buf = nullptr;
  ObString seq_id;
  if (OB_ISNULL(seq_id_buf = param.allocator_->alloc(sizeof(uint32_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc range obj failed.", K(ret));
  } else if (OB_FALSE_IT(seq_id.assign_ptr((char*)seq_id_buf, sizeof(uint32_t)))) {
  } else if (OB_FAIL(ObLobSeqId::get_seq_id(0, seq_id))) {
    LOG_WARN("get_seq_id failed.", K(ret));
  } else if (OB_FAIL(build_rowkey(param, key_objs, seq_id, range))) {
    LOG_WARN("build_rowkey_range fail", K(ret));
  }
  return ret;
}

int build_range(ObLobAccessParam &param, ObObj key_objs[4], ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if (param.has_single_chunk()) {
    if (OB_FAIL(build_rowkey(param, key_objs, range))) {
      LOG_WARN("build_rowkey fail", K(ret));
    }
  } else if (OB_FAIL(build_rowkey_range(param, key_objs, range))) {
    LOG_WARN("build_rowkey_range fail", K(ret));
  }
  return ret;
}

int ObLobAccessParam::get_rowkey_range(ObObj rowkey_objs[4], ObNewRange &range)
{
  return build_range(*this, rowkey_objs, range);
}

} // storage
} // oceanbase
