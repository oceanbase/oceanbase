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

#include "ob_table_partition_ranges.h"
#include "sql/engine/expr/ob_expr.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

OB_DEF_SERIALIZE(ObPartitionScanRanges)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  LST_DO_CODE(OB_UNIS_ENCODE, partition_id_);
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, ranges_.count()))) {
    LOG_WARN("fail to encode ranges count", K(ret), K(ranges_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges_.count(); ++i) {
    if (OB_FAIL(ranges_.at(i).serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize key range", K(ret), K(i));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObPartitionScanRanges)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  LST_DO_CODE(OB_UNIS_DECODE, partition_id_);
  if (OB_SUCC(ret)) {
    int64_t count = 0;
    ranges_.reset();
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("fail to decode key ranges count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
      if (OB_ISNULL(deserialize_allocator_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("deserialize allocator is NULL", K(ret));
      } else {
        ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
        ObNewRange copy_range;
        ObNewRange key_range;
        copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
        copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
        if (OB_FAIL(copy_range.deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize range", K(ret));
        } else if (OB_FAIL(deep_copy_range(*deserialize_allocator_, copy_range, key_range))) {
          LOG_WARN("fail to deep copy range", K(ret));
        } else if (OB_FAIL(ranges_.push_back(key_range))) {
          LOG_WARN("fail to add key range to array", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPartitionScanRanges)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, partition_id_);
  len += serialization::encoded_length_vi64(ranges_.count());
  for (int64_t i = 0; i < ranges_.count(); ++i) {
    len += ranges_.at(i).get_serialize_size();
  }
  return len;
}

OB_DEF_SERIALIZE(ObMultiPartitionsRangesWarpper)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, partitions_ranges_.count()))) {
    LOG_WARN("fail to encode ranges count", K(ret), K(partitions_ranges_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partitions_ranges_.count(); ++i) {
    if (OB_FAIL(partitions_ranges_.at(i)->serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize key range", K(ret), K(i));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE, mode_);
  return ret;
}

OB_DEF_DESERIALIZE(ObMultiPartitionsRangesWarpper)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  if (OB_SUCC(ret)) {
    int64_t count = 0;
    partitions_ranges_.reset();
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("fail to decode partition ranges count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
      ObPartitionScanRanges *partition_ranges = nullptr;
      if (OB_FAIL(get_new_partition_ranges(partition_ranges))) {
        LOG_WARN("Failed to get new partition ranges", K(ret));
      } else {
        partition_ranges->set_des_allocator(&allocator_);
        if (OB_FAIL(partition_ranges->deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize range", K(ret));
        }
      }
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, mode_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMultiPartitionsRangesWarpper)
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(partitions_ranges_.count());
  for (int64_t i = 0; i < partitions_ranges_.count(); ++i) {
    len += partitions_ranges_.at(i)->get_serialize_size();
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, mode_);
  return len;
}

int ObMultiPartitionsRangesWarpper::init(int64_t expect_partition_cnt)
{
  int ret = OB_SUCCESS;
  if (expect_partition_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(partitions_ranges_.prepare_allocate(expect_partition_cnt))) {
    LOG_WARN("Failed to prepare allocate element", K(ret));
  }
  return ret;
}

int ObMultiPartitionsRangesWarpper::get_partition_ranges_by_idx(int64_t idx, ObPartitionScanRanges *&partition_ranges)
{
  int ret = OB_SUCCESS;
  partition_ranges = nullptr;
  if (idx >= partitions_ranges_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("Invalid idx, out of range", K(ret));
  } else if (nullptr == (partition_ranges = partitions_ranges_.at(idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get an unexpected nullptr", K(ret));
  }
  return ret;
}

int ObMultiPartitionsRangesWarpper::get_partition_ranges_by_partition_id(int64_t partition_id, ObPartitionScanRanges *&partition_ranges)
{
  int ret = OB_SUCCESS;
  partition_ranges = nullptr;
  ARRAY_FOREACH_X(partitions_ranges_, idx, cnt, OB_SUCC(ret)) {
    ObPartitionScanRanges *tmp_partition_ranges = partitions_ranges_.at(idx);
    if (nullptr == tmp_partition_ranges) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The partition ranges is null", K(ret));
    } else if (partition_id == tmp_partition_ranges->partition_id_) {
      partition_ranges = tmp_partition_ranges;
    }
  }
  if (nullptr == partition_ranges) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObMultiPartitionsRangesWarpper::get_new_partition_ranges(ObPartitionScanRanges *&partition_ranges)
{
  int ret = OB_SUCCESS;
  void *buf = allocator_.alloc(sizeof(ObPartitionScanRanges));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("No memory", K(ret));
  } else if (FALSE_IT(partition_ranges = new (buf) ObPartitionScanRanges())) {
  } else if (OB_FAIL(partitions_ranges_.push_back(partition_ranges))) {
    LOG_WARN("Failed to push back partition ranges", K(ret));
    partition_ranges = nullptr;
  }
  return ret;
}

int ObMultiPartitionsRangesWarpper::add_range(ObEvalCtx &eval_ctx,
                                              int64_t part_id,
                                              int64_t ref_table_id,
                                              const common::ObIArray<ObExpr *> &exprs,
                                              const bool table_has_hidden_pk,
                                              int64_t &part_row_cnt)
{
  int ret = OB_SUCCESS;
  ObPartitionScanRanges *partition_ranges = nullptr;
  ObNewRange main_table_rowkey_range;
  main_table_rowkey_range.table_id_ = ref_table_id;
  ObNewRange hold_range;
  hold_range.table_id_ = ref_table_id;

  if (OB_FAIL(get_partition_ranges_by_partition_id(part_id, partition_ranges))) {
    if (OB_ENTRY_NOT_EXIST == ret && OB_FAIL(get_new_partition_ranges(partition_ranges))) {
      LOG_WARN("Failed to get partition ranges", K(ret));
    } else if (OB_ISNULL(partition_ranges)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get partition ranges", K(ret));
    } else if (common::OB_INVALID_INDEX == partition_ranges->partition_id_) {
      partition_ranges->partition_id_ = part_id;
    }
  }

  // set main table rowkey
  if (OB_SUCC(ret)) {
    /*
     * Init the new main table row, for example:
     * Primary key is id, varchar(68), KEY `idx_gmt_compensate_nofity`
     * (`gmt_compensate`) STORING (`status`) GLOBAL
     * Tsc in logical plan : access([per.id], [per.status]), partitions(p0)
     * Tsc output row = {row:[{"VARCHAR":"2088201800000072862812", collation:"utf8mb4_bin"},
     * {"VARCHAR":"F", collation:"utf8mb4_bin"}], projector:[0]}
     * Main_table_rowkey_ should be {row:[{"VARCHAR":"2088201800000072862812",
     * collation:"utf8mb4_bin"}}
     * Because the main table only need the column named 'id' as rowkey.
     * Then we use this row to construct a new scan range,
     * which will be used in multi_table_scan operator.
     * */
    if (0 == main_table_rowkey_.count_) {
      // 如果主表中包含隐藏主键, 说明全局索引扫描吐出的行中除了主键还包含了分区键, 保存的时候需要将分区键剔除出去
      int64_t rowkey_count = exprs.count();
      if (table_has_hidden_pk) {
        if (OB_UNLIKELY(ARRAYSIZEOF(share::schema::HIDDEN_PK_COLUMN_IDS) > exprs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row count should large than hidden pk count",
              K(ARRAYSIZEOF(share::schema::HIDDEN_PK_COLUMN_IDS)), K(exprs.count()));
        } else {
          rowkey_count = ARRAYSIZEOF(share::schema::HIDDEN_PK_COLUMN_IDS);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(init_main_table_rowkey(rowkey_count, main_table_rowkey_))) {
        LOG_WARN("Fail to init main table row", K(ret));
      }
    }
    // shallow copy, cause we will deep copy the range later
    for (int64_t i = 0; OB_SUCC(ret) && i < main_table_rowkey_.get_count(); ++i) {
      ObDatum &col_datum = exprs.at(i)->locate_expr_datum(eval_ctx);
      if (OB_FAIL(col_datum.to_obj(main_table_rowkey_.cells_[i],
                                   exprs.at(i)->obj_meta_,
                                   exprs.at(i)->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      }
    }
  }

  // set range
  if (OB_SUCC(ret)) {
    main_table_rowkey_range.start_key_.assign(main_table_rowkey_.cells_, main_table_rowkey_.count_);
    main_table_rowkey_range.end_key_.assign(main_table_rowkey_.cells_, main_table_rowkey_.count_);
    main_table_rowkey_range.border_flag_.set_inclusive_start();
    main_table_rowkey_range.border_flag_.set_inclusive_end();
    LOG_DEBUG("construct range", K(main_table_rowkey_range));
  }

  // 将下层行的cells深考，作为主表查询ObNewRange中rowkey的objs.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(deep_copy_range(allocator_, main_table_rowkey_range, hold_range))) {
      LOG_WARN("Hold the range in this op ctx failed", K(ret));
    } else if (OB_FAIL(partition_ranges->ranges_.push_back(hold_range))) {
      LOG_WARN("Failed push back range failed", K(ret));
    } else {
      part_row_cnt = partition_ranges->ranges_.count();
    }
  }

  LOG_DEBUG("Add range", K(part_id), K(exprs), K(main_table_rowkey_range),
            K(main_table_rowkey_range.get_start_key()), K(main_table_rowkey_range.get_end_key()),
            K(hold_range), KPC(partition_ranges));

  return ret;
}

int ObMultiPartitionsRangesWarpper::add_range(int64_t part_id,
                                              int64_t ref_table_id,
                                              const common::ObNewRow *row,
                                              const bool table_has_hidden_pk,
                                              int64_t &part_row_cnt)
{
  int ret = OB_SUCCESS;
  ObPartitionScanRanges *partition_ranges = nullptr;
  ObNewRange main_table_rowkey_range;
  main_table_rowkey_range.table_id_ = ref_table_id;
  ObNewRange hold_range;
  hold_range.table_id_ = ref_table_id;

  if (OB_FAIL(get_partition_ranges_by_partition_id(part_id, partition_ranges))) {
    if (OB_ENTRY_NOT_EXIST == ret && OB_FAIL(get_new_partition_ranges(partition_ranges))) {
      LOG_WARN("Failed to get partition ranges", K(ret));
    } else if (OB_ISNULL(partition_ranges)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get partition ranges", K(ret));
    } else if (common::OB_INVALID_INDEX == partition_ranges->partition_id_) {
      partition_ranges->partition_id_ = part_id;
    }
  }

  // set main table rowkey
  if (OB_SUCC(ret)) {
    /*
     * Init the new main table row, for example:
     * Primary key is id, varchar(68), KEY `idx_gmt_compensate_nofity` (`gmt_compensate`) STORING (`status`) GLOBAL
     * Tsc in logical plan : access([per.id], [per.status]), partitions(p0)
     * Tsc output row = {row:[{"VARCHAR":"2088201800000072862812", collation:"utf8mb4_bin"}, {"VARCHAR":"F", collation:"utf8mb4_bin"}], projector:[0]}
     * Main_table_rowkey_ should be {row:[{"VARCHAR":"2088201800000072862812", collation:"utf8mb4_bin"}}
     * Because the main table only need the column named 'id' as rowkey.
     * Then we use this row to construct a new scan range, which will be used in multi_table_scan operator.
     * */
    if (0 == main_table_rowkey_.count_) {
      // 如果主表中包含隐藏主键, 说明全局索引扫描吐出的行中除了主键还包含了分区键, 保存的时候需要将分区键剔除出去
      int64_t rowkey_count = row->get_count();
      if (table_has_hidden_pk) {
        if (OB_UNLIKELY(ARRAYSIZEOF(share::schema::HIDDEN_PK_COLUMN_IDS) > row->get_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row count should large than hidden pk count",
              K(ARRAYSIZEOF(share::schema::HIDDEN_PK_COLUMN_IDS)), K(row->get_count()));
        } else {
          rowkey_count = ARRAYSIZEOF(share::schema::HIDDEN_PK_COLUMN_IDS);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(init_main_table_rowkey(rowkey_count, main_table_rowkey_))) {
          LOG_WARN("Fail to init main table row", K(ret));
        }
      }
    }
    // shallow copy, cause we will deep copy the range later
    for (int64_t i = 0; OB_SUCC(ret) && i < main_table_rowkey_.get_count(); ++i) {
      main_table_rowkey_.cells_[i] = row->get_cell(i);
    }
  }

  // set range
  if (OB_SUCC(ret)) {
    main_table_rowkey_range.start_key_.assign(main_table_rowkey_.cells_, main_table_rowkey_.count_);
    main_table_rowkey_range.end_key_.assign(main_table_rowkey_.cells_, main_table_rowkey_.count_);
    main_table_rowkey_range.border_flag_.set_inclusive_start();
    main_table_rowkey_range.border_flag_.set_inclusive_end();
    LOG_DEBUG("construct range", K(main_table_rowkey_range));
  }

  // 将下层行的cells深考，作为主表查询ObNewRange中rowkey的objs.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(deep_copy_range(allocator_, main_table_rowkey_range, hold_range))) {
      LOG_WARN("Hold the range in this op ctx failed", K(ret));
    } else if (OB_FAIL(partition_ranges->ranges_.push_back(hold_range))) {
      LOG_WARN("Failed push back range failed", K(ret));
    } else {
      part_row_cnt = partition_ranges->ranges_.count();
    }
  }

  LOG_DEBUG("Add range", KP(row), K(main_table_rowkey_range), K(main_table_rowkey_range.get_start_key()), K(main_table_rowkey_range.get_end_key()), K(hold_range), KPC(partition_ranges));

  return ret;
}

int ObMultiPartitionsRangesWarpper::init_main_table_rowkey(const int64_t column_count, common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (column_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_count));
  } else if (OB_ISNULL(ptr = allocator_.alloc(column_count * sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for row failed", "size", column_count * sizeof(ObObj));
  } else {
    row.cells_ = new(ptr) common::ObObj[column_count];
    row.count_ = column_count;
  }
  return ret;
}

void ObMultiPartitionsRangesWarpper::release()
{
  for (int64_t i = 0; i < partitions_ranges_.count(); ++i) {
    ObPartitionScanRanges *partition_ranges = nullptr;
    if (nullptr == (partition_ranges = partitions_ranges_.at(i))) {
      // do nothing
    } else {
      partition_ranges->~ObPartitionScanRanges();
    }
    main_table_rowkey_.cells_ = nullptr;
    main_table_rowkey_.count_ = 0;
  }
  partitions_ranges_.reset();
  allocator_.reset();
}

int64_t ObMultiPartitionsRangesWarpper::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  for (int64_t i = 0; i < partitions_ranges_.count(); ++i) {
    ObPartitionScanRanges *partition_ranges = nullptr;
    if (nullptr == (partition_ranges = partitions_ranges_.at(i))) {
      // do nothing
    } else {
      ::oceanbase::common::databuff_printf(buf, buf_len, pos, "{");
      ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, "partition_ranges", *partition_ranges);
      ::oceanbase::common::databuff_printf(buf, buf_len, pos, "}");
    }
  }
  return pos;
}

int ObMultiPartitionsRangesWarpper::get_next_ranges(int64_t idx,
                                                    int64_t &partition_id,
                                                    ObIArray<ObNewRange> &ranges,
                                                    int64_t &next_idx)
{
  int ret = OB_SUCCESS;
  ObPartitionScanRanges *partition_ranges = nullptr;
  const GetRangeMode mode = mode_;
  bool next_partition = false;
  if (idx < 0 || idx >= partitions_ranges_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("Invalid idx, out of range", K(ret), K(idx), K(partitions_ranges_.count()));
  } else if (nullptr == (partition_ranges = partitions_ranges_.at(idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get an unexpected nullptr", K(ret));
  } else if (PARTITION_PARTICLE == mode) {
    partition_id = partition_ranges->partition_id_;
    next_idx = idx + 1;
    if (OB_FAIL(ranges.assign(partition_ranges->ranges_))) {
      LOG_WARN("Failed to assign rangs", K(ret));
    }
  } else if (RANGE_PARTICLE == mode) {
    partition_id = partition_ranges->partition_id_;
    ranges.reset();
    if (range_offset_ >= partition_ranges->ranges_.count()) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LOG_WARN("Invalid idx, out of range", K(ret), K(range_offset_),
          K(partition_ranges->ranges_.count()));
    } else if (OB_FAIL(ranges.push_back(partition_ranges->ranges_.at(range_offset_)))) {
      LOG_WARN("Failed to push back range", K(ret));
    } else {
      ++range_offset_;
      next_partition = (range_offset_ == partition_ranges->ranges_.count());
      next_idx = next_partition ? (idx + 1) : idx;
      range_offset_ = next_partition ? 0 : range_offset_;
    }
  }
  LOG_DEBUG("Got new ranges", K(ret), K(idx), K(partition_id), K(ranges), K(next_idx));
  return ret;
}

}
}



