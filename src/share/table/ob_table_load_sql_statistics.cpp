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

#define USING_LOG_PREFIX CLIENT

#include "ob_table_load_sql_statistics.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace table
{

void ObTableLoadSqlStatistics::reset()
{
  for (int64_t i = 0; i < col_stat_array_.count(); ++i) {
    ObOptOSGColumnStat *col_stat = col_stat_array_.at(i);
    if (col_stat != nullptr) {
      col_stat->~ObOptOSGColumnStat();
    }
  }
  col_stat_array_.reset();
  for (int64_t i = 0; i < table_stat_array_.count(); ++i) {
    ObOptTableStat *table_stat = table_stat_array_.at(i);
    if (table_stat != nullptr) {
      table_stat->~ObOptTableStat();
    }
  }
  table_stat_array_.reset();
  allocator_.reset();
  sample_helper_.reset();
}

int ObTableLoadSqlStatistics::create(int64_t column_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invald args", KR(ret), K(column_count));
  } else if (OB_UNLIKELY(!is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not empty", KR(ret));
  } else {
    ObOptTableStat *table_stat = nullptr;
    ObOptOSGColumnStat *osg_col_stat = nullptr;
    if (OB_FAIL(allocate_table_stat(table_stat))) {
      LOG_WARN("fail to allocate table stat", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      ObOptOSGColumnStat *osg_col_stat = nullptr;
      if (OB_FAIL(allocate_col_stat(osg_col_stat))) {
        LOG_WARN("fail to allocate col stat", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadSqlStatistics::allocate_table_stat(ObOptTableStat *&table_stat)
{
  int ret = OB_SUCCESS;
  ObOptTableStat *new_table_stat = OB_NEWx(ObOptTableStat, (&allocator_));
  if (OB_ISNULL(new_table_stat)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to allocate buffer", KR(ret));
  } else if (OB_FAIL(table_stat_array_.push_back(new_table_stat))) {
    OB_LOG(WARN, "fail to push back", KR(ret));
  } else {
    table_stat = new_table_stat;
  }
  if (OB_FAIL(ret)) {
    if (new_table_stat != nullptr) {
      new_table_stat->~ObOptTableStat();
      allocator_.free(new_table_stat);
      new_table_stat = nullptr;
    }
  }
  return ret;
}

int ObTableLoadSqlStatistics::allocate_col_stat(ObOptOSGColumnStat *&col_stat)
{
  int ret = OB_SUCCESS;
  ObOptOSGColumnStat *new_osg_col_stat = ObOptOSGColumnStat::create_new_osg_col_stat(allocator_);
  if (OB_ISNULL(new_osg_col_stat) || OB_ISNULL(new_osg_col_stat->col_stat_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to allocate buffer", KR(ret));
  } else if (OB_FAIL(col_stat_array_.push_back(new_osg_col_stat))) {
    OB_LOG(WARN, "fail to push back", KR(ret));
  } else {
    col_stat = new_osg_col_stat;
  }
  if (OB_FAIL(ret)) {
    if (new_osg_col_stat != nullptr) {
      new_osg_col_stat->~ObOptOSGColumnStat();
      allocator_.free(new_osg_col_stat);
      new_osg_col_stat = nullptr;
    }
  }
  return ret;
}

int ObTableLoadSqlStatistics::merge(const ObTableLoadSqlStatistics &other)
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < other.table_stat_array_.count(); ++i) {
      ObOptTableStat *table_stat = other.table_stat_array_.at(i);
      ObOptTableStat *copied_table_stat = nullptr;
      if (OB_ISNULL(table_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table stat is null", KR(ret), K(i), K(other.table_stat_array_));
      } else {
        int64_t size = table_stat->size();
        char *buf = nullptr;
        if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate buffer", KR(ret), K(size));
        } else if (OB_FAIL(table_stat->deep_copy(buf, size, copied_table_stat))) {
          LOG_WARN("fail to copy table stat", KR(ret));
        } else if (OB_FAIL(table_stat_array_.push_back(copied_table_stat))) {
          LOG_WARN("fail to add table stat", KR(ret));
        }
        if (OB_FAIL(ret)) {
          if (copied_table_stat != nullptr) {
            copied_table_stat->~ObOptTableStat();
            copied_table_stat = nullptr;
          }
          if (buf != nullptr) {
            allocator_.free(buf);
            buf = nullptr;
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.col_stat_array_.count(); ++i) {
      ObOptOSGColumnStat *col_stat = other.col_stat_array_.at(i);
      ObOptOSGColumnStat *copied_col_stat = nullptr;
      if (OB_ISNULL(col_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col stat is null", KR(ret), K(i), K(other.col_stat_array_));
      } else if (OB_FAIL(allocate_col_stat(copied_col_stat))) {
        LOG_WARN("fail to allocate col stat", KR(ret));
      } else if (OB_FAIL(copied_col_stat->deep_copy(*col_stat))) {
        LOG_WARN("fail to copy col stat", KR(ret));
      }
    }
  } else {
    if (OB_UNLIKELY(other.table_stat_array_.count() != table_stat_array_.count() ||
                    other.col_stat_array_.count() != col_stat_array_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), KPC(this), K(other));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.table_stat_array_.count(); ++i) {
      ObOptTableStat *table_stat = other.table_stat_array_.at(i);
      if (OB_ISNULL(table_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table stat is null", KR(ret), K(i), K(other.table_stat_array_));
      } else if (OB_FAIL(table_stat_array_.at(i)->merge_table_stat(*table_stat))) {
        LOG_WARN("fail to merge table stat", KR(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.col_stat_array_.count(); ++i) {
      ObOptOSGColumnStat *osg_col_stat = other.col_stat_array_.at(i);
      if (OB_ISNULL(osg_col_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col stat is null", KR(ret), K(i), K(other.col_stat_array_));
      }
      // ObOptOSGColumnStat的序列化结果只序列化里面的ObOptColumnStat,
      // 需要调用ObOptColumnStat的merge函数
      else if (OB_FAIL(
                 col_stat_array_.at(i)->col_stat_->merge_column_stat(*osg_col_stat->col_stat_))) {
        LOG_WARN("fail to merge col stat", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadSqlStatistics::get_table_stat(int64_t idx, ObOptTableStat *&table_stat)
{
  int ret = OB_SUCCESS;
  table_stat = nullptr;
  if (OB_UNLIKELY(idx >= table_stat_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(idx), K(table_stat_array_.count()));
  } else {
    table_stat = table_stat_array_.at(idx);
  }
  return ret;
}

int ObTableLoadSqlStatistics::get_col_stat(int64_t idx, ObOptOSGColumnStat *&osg_col_stat)
{
  int ret = OB_SUCCESS;
  osg_col_stat = nullptr;
  if (OB_UNLIKELY(idx >= col_stat_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(idx), K(col_stat_array_.count()));
  } else {
    osg_col_stat = col_stat_array_.at(idx);
  }
  return ret;
}

int ObTableLoadSqlStatistics::get_table_stat_array(ObIArray<ObOptTableStat *> &table_stat_array) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stat_array_.count(); ++i) {
    ObOptTableStat *table_stat = nullptr;
    if (OB_ISNULL(table_stat = table_stat_array_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table stat is null", KR(ret), K(i), K(table_stat_array_));
    } else if (OB_FAIL(table_stat_array.push_back(table_stat))) {
      LOG_WARN("fail to push back table stat", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadSqlStatistics::get_col_stat_array(ObIArray<ObOptColumnStat *> &col_stat_array) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_stat_array_.count(); ++i) {
    ObOptOSGColumnStat *osg_col_stat = nullptr;
    if (OB_ISNULL(osg_col_stat = col_stat_array_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected col stat is null", KR(ret), K(i), K(col_stat_array_));
    } else if (OB_FAIL(osg_col_stat->set_min_max_datum_to_obj())) {
      LOG_WARN("fail to persistence min max", KR(ret));
    } else if (OB_FAIL(col_stat_array.push_back(osg_col_stat->col_stat_))) {
      LOG_WARN("fail to push back col stat", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadSqlStatistics::get_table_stats(TabStatIndMap &table_stats) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObOptTableStat *table_stat = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stat_array_.count(); ++i) {
    if (OB_ISNULL(table_stat = table_stat_array_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table stat is null", KR(ret));
    } else {
      ObOptTableStat::Key key(tenant_id,
                              table_stat->get_table_id(),
                              table_stat->get_partition_id());
      if (OB_FAIL(table_stats.set_refactored(key, table_stat))) {
        LOG_WARN("fail to set table stat", KR(ret), K(key), KPC(table_stat));
        if (OB_HASH_EXIST == ret) {
          ret = OB_ENTRY_EXIST;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadSqlStatistics::get_col_stats(ColStatIndMap &col_stats) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObOptOSGColumnStat *osg_col_stat = nullptr;
  ObOptColumnStat *col_stat = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_stat_array_.count(); ++i) {
    if (OB_ISNULL(osg_col_stat = col_stat_array_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected osg col stat is null", KR(ret));
    } else if (OB_ISNULL(col_stat = osg_col_stat->col_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected col stat is null", KR(ret));
    } else {
      ObOptColumnStat::Key key(tenant_id,
                               col_stat->get_table_id(),
                               col_stat->get_partition_id(),
                               col_stat->get_column_id());
      if (OB_FAIL(col_stats.set_refactored(key, col_stat))) {
        LOG_WARN("fail to set col stat", KR(ret), K(key), KPC(col_stat));
        if (OB_HASH_EXIST == ret) {
          ret = OB_ENTRY_EXIST;
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableLoadSqlStatistics)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(table_stat_array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stat_array_.count(); i++) {
    ObOptTableStat *table_stat = table_stat_array_.at(i);
    if (OB_ISNULL(table_stat)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected table stat is null", KR(ret));
    } else {
      OB_UNIS_ENCODE(*table_stat);
    }
  }
  OB_UNIS_ENCODE(col_stat_array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < col_stat_array_.count(); i++) {
    ObOptOSGColumnStat *col_stat = col_stat_array_.at(i);
    if (OB_ISNULL(col_stat)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected col stat is null", KR(ret));
    } else {
      OB_UNIS_ENCODE(*col_stat_array_.at(i));
    }
  }
  OB_UNIS_ENCODE(sample_helper_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableLoadSqlStatistics)
{
  int ret = OB_SUCCESS;
  reset();
  int64_t size = 0;
  OB_UNIS_DECODE(size)
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    ObOptTableStat table_stat;
    ObOptTableStat *copied_table_stat = nullptr;
    if (OB_FAIL(table_stat.deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize datum store failed", K(ret), K(i));
    } else {
      int64_t size = table_stat.size();
      char *new_buf = nullptr;
      if (OB_ISNULL(new_buf = static_cast<char *>(allocator_.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to allocate buffer", KR(ret), K(size));
      } else if (OB_FAIL(table_stat.deep_copy(new_buf, size, copied_table_stat))) {
        OB_LOG(WARN, "fail to copy table stat", KR(ret));
      } else if (OB_FAIL(table_stat_array_.push_back(copied_table_stat))) {
        OB_LOG(WARN, "fail to add table stat", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (copied_table_stat != nullptr) {
          copied_table_stat->~ObOptTableStat();
          copied_table_stat = nullptr;
        }
        if(new_buf != nullptr) {
          allocator_.free(new_buf);
          new_buf = nullptr;
        }
      }
    }
  }
  size = 0;
  OB_UNIS_DECODE(size)
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    ObOptOSGColumnStat *osg_col_stat = NULL;
    if (OB_ISNULL(osg_col_stat = ObOptOSGColumnStat::create_new_osg_col_stat(allocator_)) ||
        OB_ISNULL(osg_col_stat->col_stat_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to create col stat");
    } else if (OB_FAIL(osg_col_stat->deserialize(buf, data_len, pos))) {
      OB_LOG(WARN, "deserialize datum store failed", K(ret), K(i));
    } else if (OB_FAIL(osg_col_stat->deep_copy(*osg_col_stat))) {
      OB_LOG(WARN, "fail to deep copy", K(ret));
    } else if (OB_FAIL(col_stat_array_.push_back(osg_col_stat))) {
      OB_LOG(WARN, "fail to add table stat", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (osg_col_stat != nullptr) {
        osg_col_stat->~ObOptOSGColumnStat();
        osg_col_stat = nullptr;
      }
    }
  }
  OB_UNIS_DECODE(sample_helper_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLoadSqlStatistics)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  OB_UNIS_ADD_LEN(table_stat_array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stat_array_.count(); i++) {
    ObOptTableStat *table_stat = table_stat_array_.at(i);
    if (OB_ISNULL(table_stat)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected table stat is null", KR(ret));
    } else {
      OB_UNIS_ADD_LEN(*table_stat);
    }
  }
  OB_UNIS_ADD_LEN(col_stat_array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < col_stat_array_.count(); i++) {
    ObOptOSGColumnStat *col_stat = col_stat_array_.at(i);
    if (OB_ISNULL(col_stat)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected col stat is null", KR(ret));
    } else {
      OB_UNIS_ADD_LEN(*col_stat);
    }
  }
  OB_UNIS_ADD_LEN(sample_helper_);
  return len;
}

} // namespace table
} // namespace oceanbase
