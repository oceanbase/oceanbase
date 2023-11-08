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

int ObTableLoadSqlStatistics::add(const ObTableLoadSqlStatistics& other)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret)&& i < other.table_stat_array_.count(); ++i) {
    ObOptTableStat *table_stat = other.table_stat_array_.at(i);
    if (table_stat != nullptr) {
      ObOptTableStat *copied_table_stat = nullptr;
      int64_t size = table_stat->size();
      char *new_buf = nullptr;
      if (OB_ISNULL(new_buf = static_cast<char *>(allocator_.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to allocate buffer", KR(ret), K(size));
      } else if (OB_FAIL(table_stat->deep_copy(new_buf, size, copied_table_stat))) {
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
  for (int64_t i = 0; OB_SUCC(ret)&& i < other.col_stat_array_.count(); ++i) {
    ObOptOSGColumnStat *col_stat = other.col_stat_array_.at(i);
    ObOptOSGColumnStat *copied_col_stat = nullptr;
    if (OB_ISNULL(col_stat)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "get unexpected null");
    } else if (OB_ISNULL(copied_col_stat = ObOptOSGColumnStat::create_new_osg_col_stat(allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to create new col stat");
    } else if (OB_FAIL(copied_col_stat->deep_copy(*col_stat))) {
      OB_LOG(WARN, "fail to copy col stat", KR(ret));
    } else if (OB_FAIL(col_stat_array_.push_back(copied_col_stat))) {
      OB_LOG(WARN, "fail to add col stat", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (copied_col_stat != nullptr) {
        copied_col_stat->~ObOptOSGColumnStat();
        copied_col_stat = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadSqlStatistics::get_table_stat_array(ObIArray<ObOptTableStat*> &table_stat_array) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stat_array_.count(); ++i) {
    if (OB_ISNULL(table_stat_array_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "get unexpected null");
    } else if (OB_FAIL(table_stat_array.push_back(table_stat_array_.at(i)))) {
      OB_LOG(WARN, "failed to push back col stat");
    }
  }
  return ret;
}

int ObTableLoadSqlStatistics::get_col_stat_array(ObIArray<ObOptColumnStat*> &col_stat_array) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_stat_array_.count(); ++i) {
    if (OB_ISNULL(col_stat_array_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "get unexpected null");
    } else if (OB_FAIL(col_stat_array_.at(i)->set_min_max_datum_to_obj())) {
      OB_LOG(WARN, "failed to persistence min max");
    } else if (OB_FAIL(col_stat_array.push_back(col_stat_array_.at(i)->col_stat_))) {
      OB_LOG(WARN, "failed to push back col stat");
    }
  }
  return ret;
}

int ObTableLoadSqlStatistics::persistence_col_stats()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_stat_array_.count(); ++i) {
    if (OB_ISNULL(col_stat_array_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "get unexpected null");
    } else if (OB_FAIL(col_stat_array_.at(i)->set_min_max_datum_to_obj())) {
      OB_LOG(WARN, "failed to persistence min max");
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableLoadSqlStatistics)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(table_stat_array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stat_array_.count(); i++) {
    if (table_stat_array_.at(i) != nullptr) {
      OB_UNIS_ENCODE(*table_stat_array_.at(i));
    }
  }
  OB_UNIS_ENCODE(col_stat_array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < col_stat_array_.count(); i++) {
    if (col_stat_array_.at(i) != nullptr) {
      OB_UNIS_ENCODE(*col_stat_array_.at(i));
    }
  }
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
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLoadSqlStatistics)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  OB_UNIS_ADD_LEN(table_stat_array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stat_array_.count(); i++) {
    if (table_stat_array_.at(i) != nullptr) {
      OB_UNIS_ADD_LEN(*table_stat_array_.at(i));
    }
  }
  OB_UNIS_ADD_LEN(col_stat_array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < col_stat_array_.count(); i++) {
    if (col_stat_array_.at(i) != nullptr) {
      OB_UNIS_ADD_LEN(*col_stat_array_.at(i));
    }
  }
  return len;
}

}  // namespace table
}  // namespace oceanbase
