/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_sstable_block_scanner.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{
using namespace common;
using namespace share;

ObTableLoadBackupSSTableBlockScanner::ObTableLoadBackupSSTableBlockScanner()
  : allocator_("TLD_BMBS_V_3_X"),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

int ObTableLoadBackupSSTableBlockScanner::init(const char *buf, int64_t buf_size, const ObSchemaInfo &schema_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(buf == nullptr || buf_size == 0 || schema_info.column_desc_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_size), K(schema_info));
  } else if (OB_FAIL(sstable_block_reader_.init(buf, buf_size, schema_info))) {
    LOG_WARN("fail to init sstable_block_reader_", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadBackupSSTableBlockScanner::reset()
{
  sstable_block_reader_.reset();
  micro_block_scanner_.reuse();
  is_inited_ = false;
}

int ObTableLoadBackupSSTableBlockScanner::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (!micro_block_scanner_.is_valid()) {
    if (OB_FAIL(switch_next_micro_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to switch next micro block", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(micro_block_scanner_.get_next_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      } else {
        ret = OB_SUCCESS;
        if (OB_FAIL(switch_next_micro_block())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to switch next micro block", KR(ret));
          }
        } else {
          ret = micro_block_scanner_.get_next_row(row);
        }
      }
    }
  }
  return ret;
}

int ObTableLoadBackupSSTableBlockScanner::switch_next_micro_block()
{
  int ret = OB_SUCCESS;
  micro_block_scanner_.reuse();
  const ObMicroBlockData *micro_block_data = nullptr;
  if (OB_FAIL(sstable_block_reader_.get_next_micro_block(micro_block_data))) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      LOG_WARN("fail to get next micro block", KR(ret));
    }
  } else if (OB_FAIL(micro_block_scanner_.init(micro_block_data, sstable_block_reader_.get_column_map()))) {
    LOG_WARN("fail to init micro_block_scanner_", KR(ret));
  }
  return ret;
}

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
