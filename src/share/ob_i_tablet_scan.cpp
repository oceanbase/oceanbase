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

#define USING_LOG_PREFIX SHARE
#include "common/ob_range.h"
#include "common/ob_store_range.h"
#include "lib/hash_func/ob_hash_func.h"
#include "share/ob_i_tablet_scan.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace common
{

OB_SERIALIZE_MEMBER(ObLimitParam, offset_, limit_);
OB_SERIALIZE_MEMBER(SampleInfo, table_id_, method_, scope_, percent_, seed_, force_block_);
OB_SERIALIZE_MEMBER(ObEstRowCountRecord, table_id_, table_type_, version_range_, logical_row_count_, physical_row_count_);
OB_SERIALIZE_MEMBER(ObTableScanOption, io_read_batch_size_, io_read_gap_size_, storage_rowsets_size_);

uint64_t SampleInfo::hash(uint64_t seed) const
{
  seed = do_hash(table_id_, seed);
  seed = do_hash(method_, seed);
  seed = do_hash(scope_, seed);
  seed = do_hash(percent_, seed);
  seed = do_hash(force_block_, seed);
  seed = do_hash(seed_, seed);

  return seed;
}

DEF_TO_STRING(ObVTableScanParam)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tablet_id),
       K_(ls_id),
       N_COLUMN_IDS, column_ids_,
       N_INDEX_ID, index_id_,
       N_KEY_RANGES, key_ranges_,
       K_(range_array_pos),
       N_TIMEOUT, timeout_,
       N_SCAN_FLAG, scan_flag_,
       N_SQL_MODE, sql_mode_,
       N_RESERVED_CELL_COUNT, reserved_cell_count_,
       N_SCHEMA_VERSION, schema_version_,
       N_QUERY_BEGIN_SCHEMA_VERSION, tenant_schema_version_,
       N_LIMIT_OFFSET, limit_param_,
       N_FOR_UPDATE, for_update_,
       N_WAIT, for_update_wait_timeout_,
       N_FROZEN_VERSION, frozen_version_,
       K_(is_get),
       K_(pd_storage_flag),
       KPC_(output_exprs),
       KPC_(op_filters),
       K_(table_scan_opt),
       K_(external_file_format),
       K_(external_file_location),
       K_(auto_split_filter),
       K_(auto_split_params),
       K_(is_tablet_spliting));
  J_OBJ_END();
  return pos;
}
}
}
