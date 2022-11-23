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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_part_mgr_util.h"

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array_iterator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/ob_cluster_version.h"

#include "lib/oblog/ob_log.h"

namespace oceanbase
{
using namespace common;

namespace share
{
namespace schema
{

int ObPartGetter::get_part_ids(const common::ObString &part_name,
                               ObIArray<ObObjectID> &part_ids)
{
  int ret = OB_SUCCESS;
  const ObPartitionLevel part_level = table_.get_part_level();
  const ObPartition *part = NULL;
  ObString cmp_part_name;
  bool find = false;
  if (part_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid part_name", K(ret), K(part_name));
  } else if (PARTITION_LEVEL_MAX == part_level) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected part level", K(ret), K(part_level));
  } else {
    const ObCheckPartitionMode mode = CHECK_PARTITION_MODE_NORMAL;
    ObPartIterator iter(table_, mode);
    while (OB_SUCC(ret) && !find && OB_SUCC(iter.next(part))) {
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null partition", K(ret));
      } else {
        cmp_part_name = part->get_part_name();
        LOG_DEBUG("cmp part name", K(cmp_part_name));
        if (ObCharset::case_insensitive_equal(part_name, cmp_part_name)) {
          // match level one part
          find = true;
          if (PARTITION_LEVEL_TWO == part_level) {
            ObSubPartIterator sub_iter(table_, *part, mode);
            const ObSubPartition *subpart = NULL;
            while (OB_SUCC(ret) && OB_SUCC(sub_iter.next(subpart))) {
              if (OB_ISNULL(subpart)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get null subpartition", K(ret));
              } else if (OB_FAIL(part_ids.push_back(subpart->get_sub_part_id()))) {
                LOG_WARN("failed to push back subpart id", K(ret));
              }
            }
            if (OB_LIKELY(OB_ITER_END == ret)) {
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(part_ids.push_back(PARTITION_LEVEL_ZERO == table_.get_part_level() ?
                                                table_.get_object_id() : part->get_part_id()))) {
            LOG_WARN("failed to push back part id", K(ret));
          }
        } else if (PARTITION_LEVEL_TWO == part_level &&
                   OB_FAIL(get_subpart_ids_in_partition(part_name, *part, part_ids, find))) {
          LOG_WARN("failed to get subpart ids in partition", K(ret));
        }
      }
    }
    if (!find && OB_ITER_END == ret) {
      ret = OB_UNKNOWN_PARTITION;
    }
  }
  return ret;
}

int ObPartGetter::get_subpart_ids(const common::ObString &part_name,
                                  ObIArray<ObObjectID> &part_ids)
{
  int ret = OB_SUCCESS;
  const ObPartitionLevel part_level = table_.get_part_level();
  const ObPartition *part = NULL;
  bool find = false;
  if (part_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid part_name", K(ret), K(part_name));
  } else if (PARTITION_LEVEL_ZERO == part_level) {
    ret = OB_ERR_NOT_PARTITIONED;
    LOG_WARN("table is not partitioned", K(ret));
  } else if (PARTITION_LEVEL_ONE == part_level) {
    // Oracle uses subpartition() on the primary partition table to report Specified subpartition does not exist
    ret = OB_UNKNOWN_SUBPARTITION;
    LOG_WARN("subpartition no exists", K(ret));
  } else if (PARTITION_LEVEL_TWO == part_level) {
    const ObCheckPartitionMode mode = CHECK_PARTITION_MODE_NORMAL;
    ObPartIterator iter(table_, mode);
    while (OB_SUCC(ret) && !find && OB_SUCC(iter.next(part))) {
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null partition", K(ret));
      } else if (OB_FAIL(get_subpart_ids_in_partition(part_name, *part, part_ids, find))) {
        LOG_WARN("failed to get subpart ids in partition", K(ret));
      }
    }
    if (!find && OB_ITER_END == ret) {
      ret = OB_UNKNOWN_SUBPARTITION;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected part level", K(ret), K(part_level));
  }
  return ret;
}

int ObPartGetter::get_subpart_ids_in_partition(const common::ObString &part_name,
                                               const ObPartition &partition,
                                               ObIArray<ObObjectID> &part_ids,
                                               bool &find)
{
  int ret = OB_SUCCESS;
  find = false;
  const ObCheckPartitionMode mode = CHECK_PARTITION_MODE_NORMAL;
  ObSubPartIterator sub_iter(table_, partition, mode);
  const ObSubPartition *subpart = NULL;
  ObString cmp_part_name;
  while (OB_SUCC(ret) && !find && OB_SUCC(sub_iter.next(subpart))) {
    if (OB_ISNULL(subpart)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null subpartition", K(ret));
    } else {
      cmp_part_name = subpart->get_part_name();
      LOG_DEBUG("cmp part name", K(cmp_part_name));
      if (ObCharset::case_insensitive_equal(part_name, cmp_part_name)) {
        if (OB_FAIL(part_ids.push_back(subpart->get_sub_part_id()))) {
          LOG_WARN("failed to push back subpart id", K(ret));
        } else {
          find = true;
        }
      }
    }
  }
  if (!find && OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPartIterator::next(const ObPartition *&part)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  part = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter not init", KR(ret));
  } else if (OB_FAIL(partition_schema_->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check if oracle mode", KR(ret), KPC_(partition_schema));
  } else {
    // The partition_array of the system table is empty and needs to be processed by part_num
    int64_t part_num = check_normal_partition(check_partition_mode_) ?
                       partition_schema_->get_first_part_num() : 0;
    int64_t hidden_part_num = check_hidden_partition(check_partition_mode_) ?
                              partition_schema_->get_hidden_partition_num() : 0;
    int64_t total_part_num = part_num + hidden_part_num;
    part_.reset();
    if (idx_++ >= total_part_num - 1) {
      ret = OB_ITER_END;
    } else if (0 <= idx_ && part_num > idx_) {
       // deal with normal partition
      int64_t idx = idx_;
      ObPartition **part_array = partition_schema_->get_part_array();
      const ObPartitionLevel part_level = partition_schema_->get_part_level();
      if (PARTITION_LEVEL_ZERO == part_level) {
        ObString part_name(!is_oracle_mode ?
                           ObPartitionSchema::MYSQL_NON_PARTITIONED_TABLE_PART_NAME:
                           ObPartitionSchema::ORACLE_NON_PARTITIONED_TABLE_PART_NAME);
        if (OB_FAIL(part_.set_part_name(part_name))) {
          LOG_WARN("fail to set part name", KR(ret), K(part_name));
        } else {
          part_.set_part_id(0);
          part = &part_;
        }
      } else if (PARTITION_LEVEL_ONE == part_level ||
                 PARTITION_LEVEL_TWO == part_level) {
        if (OB_ISNULL(part_array)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part_array is null", KR(ret), KPC_(partition_schema));
        } else {
          part = part_array[idx];
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected part level", KR(ret), K(part_level));
      }
    } else if (part_num <= idx_ && total_part_num > idx_) {
      // deal with hidden partition
      int64_t idx = idx_ - part_num;
      ObPartition **hidden_part_array = partition_schema_->get_hidden_part_array();
      if (OB_ISNULL(hidden_part_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hidden_part_array is null", KR(ret), K_(idx), K(part_num));
      } else if (idx < 0 || idx >= partition_schema_->get_hidden_partition_num()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid idx", KR(ret), K(idx), KPC_(partition_schema));
      } else {
        part = hidden_part_array[idx];
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shouldn't be here", KR(ret), K_(idx), K_(check_partition_mode),
               K(part_num), K(hidden_part_num));
    }
  }
  return ret;
}

int ObSubPartIterator::next(const ObSubPartition *&subpart)
{
  int ret = OB_SUCCESS;
  subpart = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter not init", KR(ret));
  } else {
    int64_t sub_part_num = check_normal_partition(check_partition_mode_) ?
                           part_->get_subpartition_num() : 0;
    int64_t hidden_sub_part_num = check_hidden_partition(check_partition_mode_) ?
                                   part_->get_hidden_subpartition_num() : 0;
    int64_t total_sub_part_num = sub_part_num + hidden_sub_part_num;
    if (idx_++ >= total_sub_part_num - 1) {
      ret = OB_ITER_END;
    } else {
      int64_t idx = OB_INVALID_INDEX;
      ObSubPartition **subpart_array = NULL;
      if (0 <= idx_ && sub_part_num > idx_) {
        idx = idx_;
        subpart_array = part_->get_subpart_array();
      } else if (sub_part_num <= idx_ && total_sub_part_num > idx_) {
        idx = idx_ - sub_part_num;
        subpart_array = part_->get_hidden_subpart_array();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shouldn't be here", KR(ret), K_(idx), K_(check_partition_mode),
                 K(sub_part_num), K(hidden_sub_part_num));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(subpart_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition array in partition is NULL", KR(ret));
      } else if (OB_ISNULL(subpart_array[idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition in partition is NULL", KR(ret), K(idx), K(idx_),
                 K(total_sub_part_num), K(sub_part_num), K(hidden_sub_part_num));
      } else {
        subpart = subpart_array[idx];
      }
    }
  }
  return ret;
}

ObPartitionSchemaIter::ObPartitionSchemaIter(
  const ObPartitionSchema &partition_schema,
  const ObCheckPartitionMode check_partition_mode)
  : partition_schema_(partition_schema),
    check_partition_mode_(check_partition_mode),
    part_iter_(),
    subpart_iter_(),
    part_(NULL),
    part_idx_(common::OB_INVALID_INDEX),
    subpart_idx_(common::OB_INVALID_INDEX)
{
}

int ObPartitionSchemaIter::next_tablet_id(
    ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObPartitionSchemaIter::Info info;
  if (OB_FAIL(next_partition_info(info))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next partition info", KR(ret));
    }
  } else {
    tablet_id = info.tablet_id_;
  }
  return ret;
}

int ObPartitionSchemaIter::next_object_id(
    ObObjectID &object_id)
{
  int ret = OB_SUCCESS;
  ObPartitionSchemaIter::Info info;
  if (OB_FAIL(next_partition_info(info))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next partition info", KR(ret));
    }
  } else {
    object_id = info.object_id_;
  }
  return ret;
}

int ObPartitionSchemaIter::next_partition_info(
    ObPartitionSchemaIter::Info &info)
{
  int ret = OB_SUCCESS;

  const ObPartitionLevel part_level = partition_schema_.get_part_level();
  const uint64_t schema_id = partition_schema_.get_table_id();
  if (is_virtual_table(schema_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("iterate virtual table not supported", KR(ret), K_(partition_schema));
  } else if (OB_ISNULL(part_)) {
    if (PARTITION_LEVEL_TWO == part_level) {
      // Normal partition of nontemplate secondary partitioned table may contains
      // hidden subpartitions, so we should always iterate normal partition
      // and ObSubPartIterator will filter subpartitions by check_partition_mode_.
      const ObCheckPartitionMode new_mode = static_cast<ObCheckPartitionMode>(check_partition_mode_ | CHECK_PARTITION_NORMAL_FLAG);
      part_iter_.init(partition_schema_, new_mode);
    } else {
      part_iter_.init(partition_schema_, check_partition_mode_);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(part_) || PARTITION_LEVEL_TWO != part_level) {
    if (OB_FAIL(part_iter_.next(part_))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("get next partition failed", KR(ret));
      }
    } else if (OB_ISNULL(part_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", KR(ret), KP(part_));
    } else if (PARTITION_LEVEL_TWO == part_level) {
      subpart_iter_.init(partition_schema_, *part_, check_partition_mode_);
    }
    part_idx_++;
    subpart_idx_ = OB_INVALID_INDEX;
  }

  if (OB_SUCC(ret)) {
    info.object_id_ = PARTITION_LEVEL_ZERO == part_level ?
                      partition_schema_.get_object_id() :
                      part_->get_part_id();
    info.tablet_id_ = PARTITION_LEVEL_ZERO == part_level ?
                      partition_schema_.get_tablet_id() :
                      part_->get_tablet_id();
    info.part_idx_ = part_idx_;
    info.part_ = PARTITION_LEVEL_ZERO == part_level ?  NULL : part_;
    info.partition_ = PARTITION_LEVEL_ZERO == part_level ?  NULL : part_;
    if (PARTITION_LEVEL_TWO == part_level) {
      const ObSubPartition *subpart = NULL;
      if (OB_FAIL(subpart_iter_.next(subpart))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("get next subpart failed", KR(ret));
        } else if (OB_FAIL(part_iter_.next(part_))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("get next part failed", KR(ret));
          }
        } else if (OB_ISNULL(part_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", KR(ret), KP(part_));
        } else {
          part_idx_++;
          subpart_idx_ = OB_INVALID_INDEX;
          if (FALSE_IT(subpart_iter_.init(partition_schema_,
                                          *part_,
                                          check_partition_mode_))) {
            // will never be here
          } else if (OB_FAIL(subpart_iter_.next(subpart))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("get next subpart failed", KR(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        subpart_idx_++;
        if (OB_ISNULL(subpart)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", KR(ret), K(subpart));
        } else {
          info.tablet_id_ = subpart->get_tablet_id();
          info.object_id_ = subpart->get_sub_part_id();
          info.part_idx_ = part_idx_;
          info.subpart_idx_ = subpart_idx_;
          info.part_ = part_;
          info.partition_ = subpart;
        }
      }
    }
  }

  return ret;
}

}
}
}
