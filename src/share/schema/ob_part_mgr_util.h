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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_PART_MGR_UTIL_
#define OCEANBASE_SHARE_SCHEMA_OB_PART_MGR_UTIL_

#include <stdint.h>
#include "lib/oblog/ob_log.h"
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace common
{
template<class T>
class ObIArray;
}
namespace share
{
namespace schema {

class ObTableSchema;
class ObSimpleTableSchemaV2;

class ObPartGetter
{
public:
  ObPartGetter(const ObTableSchema &table)
      : table_(table)
  {}
  int get_part_ids(const common::ObString &part_name, common::ObIArray<ObObjectID> &part_ids);
  int get_subpart_ids(const common::ObString &part_name, common::ObIArray<ObObjectID> &part_ids);
private:
  ObPartGetter();
  int get_subpart_ids_in_partition(const common::ObString &part_name,
                                   const ObPartition &partition,
                                   common::ObIArray<ObObjectID> &part_ids,
                                   bool &find);
private:
  const ObTableSchema &table_;
};

class ObPartIterator
{
public:
  ObPartIterator()
    : is_inited_(false),
      partition_schema_(NULL),
      idx_(common::OB_INVALID_INDEX), part_(),
      check_partition_mode_(CHECK_PARTITION_MODE_NORMAL)
   {}

  ObPartIterator(const ObPartitionSchema &partition_schema,
                 const ObCheckPartitionMode &mode)
  {
    (void) init(partition_schema, mode);
  }

  void init(const ObPartitionSchema &partition_schema,
            const ObCheckPartitionMode &mode)
  {
    partition_schema_ = &partition_schema;
    idx_ = common::OB_INVALID_INDEX;
    part_.reset();
    check_partition_mode_ = mode;
    is_inited_ = true;
  }
  int next(const ObPartition *&part);
private:
  bool is_inited_;
  const ObPartitionSchema *partition_schema_;
  int64_t idx_;
  // @note: For non-range partitions, the schema does not store partition objects,
  //  here is the mock out for external use
  share::schema::ObPartition part_;
  ObCheckPartitionMode check_partition_mode_;
};

class ObSubPartIterator
{
public:
  ObSubPartIterator()
    : is_inited_(false),
      partition_schema_(NULL),
      part_(NULL),
      idx_(common::OB_INVALID_INDEX), subpart_(),
      check_partition_mode_(CHECK_PARTITION_MODE_NORMAL)
  {
  }
  ObSubPartIterator(
    const ObPartitionSchema &partition_schema,
    const ObPartition &part,
    const ObCheckPartitionMode &mode)
  {
    (void) init(partition_schema, part, mode);
  }
  void init(
    const ObPartitionSchema &partition_schema,
    const ObPartition &part,
    const ObCheckPartitionMode &mode)
  {
    partition_schema_ = &partition_schema;
    part_ = &part;
    idx_ = common::OB_INVALID_INDEX;
    subpart_.reset();
    check_partition_mode_ = mode;
    is_inited_ = true;
  }
  int next(const ObSubPartition *&subpart);
private:
  bool is_inited_;
  const ObPartitionSchema *partition_schema_;
  const ObPartition *part_;
  int64_t idx_;
  // @note: For non-range partitions, the schema does not store partition objects,
  //  here is the mock out for external use
  ObSubPartition subpart_;
  ObCheckPartitionMode check_partition_mode_;
};

class ObPartitionSchemaIter
{
public:
  struct Info {
  public:
   Info()
     : object_id_(common::OB_INVALID_ID),
       tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
       part_idx_(common::OB_INVALID_INDEX),
       subpart_idx_(common::OB_INVALID_INDEX),
       part_(NULL),
       partition_(NULL) {}
   ~Info() {}
   TO_STRING_KV(K_(object_id), K_(tablet_id),
                K_(part_idx), K_(subpart_idx),
                KP_(partition));

   ObObjectID object_id_;
   ObTabletID tablet_id_;
   int64_t part_idx_;    // partition offset in partition_array
   int64_t subpart_idx_; // subparititon offset in subpartition_array
   /* first partition in partitioned table, it's null when iter non-partitioned table */
   const share::schema::ObPartition *part_;
   /*
    * When part_level = 0, it's null;
    * When part_level = 1, it's ObPartition;
    * When part_level = 2, it's ObSubPartition.
    */
   const share::schema::ObBasePartition *partition_;
  };
public:
  ObPartitionSchemaIter() = delete;
  explicit ObPartitionSchemaIter(const ObPartitionSchema &partition_schema,
                                 const ObCheckPartitionMode check_partition_mode);
  int next_partition_info(ObPartitionSchemaIter::Info &info);
  int next_tablet_id(ObTabletID &tablet_id);
  int next_object_id(ObObjectID &object_id);
  TO_STRING_KV(K_(partition_schema), K_(check_partition_mode), K_(part_idx), K_(subpart_idx));
private:
  const ObPartitionSchema &partition_schema_;
  ObCheckPartitionMode check_partition_mode_;
  ObPartIterator part_iter_;
  ObSubPartIterator subpart_iter_;
  const ObPartition *part_;
  int64_t part_idx_;
  int64_t subpart_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionSchemaIter);
};

}
}
}
#endif
