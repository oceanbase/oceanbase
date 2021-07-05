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

namespace oceanbase {
namespace common {
template <class T>
class ObIArray;
}
namespace share {
namespace schema {

class ObTableSchema;
class ObSimpleTableSchemaV2;

class ObIPartIdsGenerator {
public:
  virtual int gen(common::ObIArray<int64_t>& part_ids) = 0;
};

class ObPartIdsGenerator : public ObIPartIdsGenerator {
public:
  ObPartIdsGenerator(const ObPartitionSchema& partition_schema) : partition_schema_(partition_schema)
  {}
  virtual int gen(common::ObIArray<int64_t>& part_ids);

private:
  ObPartIdsGenerator();

private:
  const ObPartitionSchema& partition_schema_;
};

template <typename T>
class ObPartIdsGeneratorForAdd : public ObIPartIdsGenerator {
public:
  ObPartIdsGeneratorForAdd(const T& table, const T& inc_table) : table_(table), inc_table_(inc_table)
  {}
  virtual int gen(common::ObIArray<int64_t>& part_ids);

private:
  ObPartIdsGeneratorForAdd();

private:
  const T& table_;
  const T& inc_table_;
};

class ObPartGetter {
public:
  ObPartGetter(const ObTableSchema& table) : table_(table)
  {}
  int get_part_ids(const common::ObString& part_name, common::ObIArray<int64_t>& part_ids);
  int get_subpart_ids(const common::ObString& part_name, common::ObIArray<int64_t>& part_ids);

private:
  ObPartGetter();
  int get_subpart_ids_in_partition(
      const common::ObString& part_name, const ObPartition& partition, common::ObIArray<int64_t>& part_ids, bool& find);

private:
  const ObTableSchema& table_;
};

class ObPartIteratorV2 {
  const static int64_t BUF_LEN = common::OB_MAX_PARTITION_NAME_LENGTH;

public:
  ObPartIteratorV2() : is_inited_(false), partition_schema_(NULL), idx_(-1), check_dropped_schema_(false)
  {
    MEMSET(buf_, 0, BUF_LEN);
  }
  ObPartIteratorV2(const ObPartitionSchema& partition_schema, bool check_dropped_schema)
  {
    init(partition_schema, check_dropped_schema);
  }
  inline void init(const ObPartitionSchema& partition_schema, bool check_dropped_schema)
  {
    partition_schema_ = &partition_schema;
    idx_ = -1;
    MEMSET(buf_, 0, BUF_LEN);
    check_dropped_schema_ = check_dropped_schema;
    is_inited_ = true;
  }
  int next(const ObPartition*& part);

private:
  inline int check_inner_stat();

private:
  bool is_inited_;
  const ObPartitionSchema* partition_schema_;
  int64_t idx_;
  char buf_[BUF_LEN];
  // @note: For non-range partitions, the schema does not store partition objects,
  //  here is the mock out for external use
  share::schema::ObPartition part_;
  bool check_dropped_schema_;
};

class ObSubPartIteratorV2 {
  const static int64_t BUF_LEN = common::OB_MAX_PARTITION_NAME_LENGTH;

public:
  ObSubPartIteratorV2()
      : is_inited_(false),
        partition_schema_(NULL),
        part_(NULL),
        idx_(common::OB_INVALID_INDEX),
        check_dropped_partition_(false)
  {
    MEMSET(buf_, 0, BUF_LEN);
  }
  ObSubPartIteratorV2(const ObPartitionSchema& partition_schema, const ObPartition& part, bool check_dropped_partition)
  {
    init(partition_schema, part, check_dropped_partition);
  }
  ObSubPartIteratorV2(const ObPartitionSchema& partition_schema, bool check_dropped_partition)
  {
    init(partition_schema, check_dropped_partition);
  }
  inline void init(const ObPartitionSchema& partition_schema, const ObPartition& part, bool check_dropped_partition)
  {
    partition_schema_ = &partition_schema;
    part_ = &part;
    idx_ = common::OB_INVALID_INDEX;
    MEMSET(buf_, 0, BUF_LEN);
    check_dropped_partition_ = check_dropped_partition;
    is_inited_ = true;
  }
  inline void init(const ObPartitionSchema& partition_schema, bool check_dropped_partition)
  {
    partition_schema_ = &partition_schema;
    part_ = nullptr;
    idx_ = common::OB_INVALID_INDEX;
    MEMSET(buf_, 0, BUF_LEN);
    check_dropped_partition_ = check_dropped_partition;
    is_inited_ = true;
  }
  int next(const ObSubPartition*& subpart);

private:
  int next_for_template(const ObSubPartition*& subpart);
  int next_for_nontemplate(const ObSubPartition*& subpart);
  inline int check_inner_stat();

private:
  bool is_inited_;
  const ObPartitionSchema* partition_schema_;
  const ObPartition* part_;
  int64_t idx_;
  char buf_[BUF_LEN];
  // @note: For non-range partitions, the schema does not store partition objects,
  //  here is the mock out for external use
  ObSubPartition subpart_;
  bool check_dropped_partition_;
};

// FIXME:() Consider integrating with ObPartIteratorV2
class ObDroppedPartIterator {
public:
  ObDroppedPartIterator() : is_inited_(false), partition_schema_(NULL), idx_(0)
  {}
  ObDroppedPartIterator(const ObPartitionSchema& partition_schema)
  {
    init(partition_schema);
  }
  inline void init(const ObPartitionSchema& partition_schema)
  {
    partition_schema_ = &partition_schema;
    idx_ = 0;
    is_inited_ = true;
  }
  int next(const ObPartition*& part);

private:
  inline int check_inner_stat();

private:
  bool is_inited_;
  const ObPartitionSchema* partition_schema_;
  int64_t idx_;
};

class ObPartitionKeyIter {
public:
  struct Info {
  public:
    Info()
        : partition_id_(common::OB_INVALID_ID),
          drop_schema_version_(common::OB_INVALID_VERSION),
          tg_partition_id_(common::OB_INVALID_ID),
          source_part_ids_(NULL)
    {}
    ~Info()
    {}
    TO_STRING_KV(K_(partition_id), K_(drop_schema_version), K_(tg_partition_id));
    int64_t partition_id_;
    int64_t drop_schema_version_;
    int64_t tg_partition_id_;
    const common::ObIArray<int64_t>* source_part_ids_;
  };

public:
  ObPartitionKeyIter() = delete;
  explicit ObPartitionKeyIter(
      const uint64_t schema_id, const ObPartitionSchema& partition_schema, bool check_dropped_schema);
  // get partition count of table
  int64_t get_partition_cnt() const;
  int64_t get_partition_num() const;
  // iter mode
  int next_partition_id_v2(int64_t& partition_id);
  int next_partition_key_v2(common::ObPartitionKey& pkey);
  int next_partition_info(ObPartitionKeyIter::Info& info);

public:
  uint64_t get_schema_id() const
  {
    return schema_id_;
  }
  TO_STRING_KV(K_(schema_id), K_(part_level), KP_(partition_schema), K_(check_dropped_schema));

private:
  uint64_t schema_id_;
  ObPartitionLevel part_level_;
  const ObPartitionSchema* partition_schema_;
  ObPartIteratorV2 part_iter_;
  ObSubPartIteratorV2 subpart_iter_;
  const ObPartition* part_;
  bool check_dropped_schema_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionKeyIter);
};

// FIXME:() Consider integrating with ObPartitionKeyIter
class ObDroppedPartitionKeyIter {
public:
  ObDroppedPartitionKeyIter() = delete;
  explicit ObDroppedPartitionKeyIter(const uint64_t schema_id, const ObPartitionSchema& partition_schema);
  int next_partition_id(int64_t& partition_id);
  int next_partition_info(ObPartitionKeyIter::Info& info);

public:
  uint64_t get_schema_id() const
  {
    return iter_.get_schema_id();
  }
  TO_STRING_KV(K_(iter));

private:
  ObPartitionKeyIter iter_;
  DISALLOW_COPY_AND_ASSIGN(ObDroppedPartitionKeyIter);
};

class ObTablePartitionKeyIter {
public:
  explicit ObTablePartitionKeyIter(const ObSimpleTableSchemaV2& table_schema, bool check_dropped_schema);
  // get partition count of table
  int64_t get_partition_cnt() const
  {
    return partition_key_iter_.get_partition_cnt();
  }
  int64_t get_partition_num() const
  {
    return partition_key_iter_.get_partition_num();
  }
  int64_t get_table_id() const
  {
    return partition_key_iter_.get_schema_id();
  }
  // iter mode
  int next_partition_key_v2(common::ObPartitionKey& pkey);
  int next_partition_id_v2(int64_t& partition_id);

private:
  ObPartitionKeyIter partition_key_iter_;
};

class ObTablegroupPartitionKeyIter {
public:
  explicit ObTablegroupPartitionKeyIter(const ObTablegroupSchema& tablegroup_schema, bool check_dropped_schema);
  // get partition count of table
  int64_t get_partition_cnt() const
  {
    return partition_key_iter_.get_partition_cnt();
  }
  int64_t get_partition_num() const
  {
    return partition_key_iter_.get_partition_num();
  }
  int64_t get_tablegroup_id() const
  {
    return partition_key_iter_.get_schema_id();
  }
  // iter mode
  int next_partition_key_v2(common::ObPGKey& pkey);
  int next_partition_id_v2(int64_t& partition_id);

private:
  ObPartitionKeyIter partition_key_iter_;
};

class ObTablePgKeyIter {
public:
  ObTablePgKeyIter(
      const share::schema::ObSimpleTableSchemaV2& table_schema, const uint64_t tablegroup_id, bool check_dropped_schema)
      : table_schema_(table_schema),
        tablegroup_id_(tablegroup_id),
        iter_(table_schema.get_table_id(), table_schema, check_dropped_schema)
  {}
  virtual ~ObTablePgKeyIter()
  {}

public:
  int init();
  int next(common::ObPartitionKey& pkey, common::ObPGKey& pgkey);

private:
  const share::schema::ObSimpleTableSchemaV2& table_schema_;
  const uint64_t tablegroup_id_;
  ObPartitionKeyIter iter_;
};

struct ObPartitionItem {
  int64_t part_idx_;
  int64_t part_id_;
  int64_t part_num_;
  const common::ObString* part_name_;
  const common::ObRowkey* part_high_bound_;

  int64_t subpart_idx_;
  int64_t subpart_id_;
  int64_t subpart_num_;
  const common::ObString* subpart_name_;
  const common::ObRowkey* subpart_high_bound_;

  int64_t partition_idx_;
  int64_t partition_id_;
  int64_t partition_num_;

  ObPartitionItem()
  {
    reset();
  }

  void reset()
  {
    part_idx_ = -1;
    part_id_ = -1;
    part_num_ = -1;
    part_name_ = NULL;
    part_high_bound_ = NULL;

    subpart_idx_ = -1;
    subpart_id_ = -1;
    subpart_num_ = -1;
    subpart_name_ = NULL;
    subpart_high_bound_ = NULL;

    partition_idx_ = -1;
    partition_id_ = -1;
    partition_num_ = -1;
  }

  TO_STRING_KV(K(part_idx_), K(part_id_), K(part_num_), KP(part_name_), KP(part_high_bound_),

      K(subpart_idx_), K(subpart_id_), K(subpart_num_), KP(subpart_name_), KP(subpart_high_bound_),

      K(partition_idx_), K(partition_id_), K(partition_num_));
};

class ObTablePartItemIterator {
public:
  ObTablePartItemIterator();
  explicit ObTablePartItemIterator(const ObSimpleTableSchemaV2& table_schema);
  void init(const ObSimpleTableSchemaV2& table_schema);
  int next(ObPartitionItem& item);

private:
  const ObSimpleTableSchemaV2* table_schema_;
  ObPartIteratorV2 part_iter_;
  ObSubPartIteratorV2 subpart_iter_;
  const ObPartition* part_;
  int64_t partition_idx_;
  int64_t part_idx_;
  int64_t subpart_idx_;
};

class ObPartMgrUtils {
private:
  ObPartMgrUtils();
  ~ObPartMgrUtils();

public:
  static int check_part_exist(const ObPartitionSchema& partition_schema, const int64_t partition_id,
      const bool check_dropped_partition, bool& exist);
  static int get_part_diff(const ObPartitionSchema& old_table, const ObPartitionSchema& new_table,
      common::ObIArray<int64_t>& part_dropped, common::ObIArray<int64_t>& part_added);
  static int get_partition_idx_by_id(const ObPartitionSchema& partition_schema, const bool check_dropped_partition,
      const int64_t partition_id, int64_t& partition_idx);
  static int check_partition_can_remove(const ObPartitionSchema& partition_schema, const int64_t partition_id,
      const bool check_dropped_partition, bool& can);
  static int get_partition_schema(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t schema_id,
      const ObSchemaType schema_type, const share::schema::ObPartitionSchema*& partition_schema);
  static int get_partition_entity_schemas_in_tenant(share::schema::ObSchemaGetterGuard& schema_guard,
      const uint64_t tenant_id, common::ObIArray<const share::schema::ObPartitionSchema*>& partition_schemas);

private:
  static bool exist_partition_id(common::ObArray<int64_t>& partition_ids, const int64_t& partition_id);
  static bool compare_with_part_id(const ObPartition* lhs, const int64_t part_id)
  {
    return lhs->get_part_id() < part_id;
  }
};

template <typename T>
int ObPartIdsGeneratorForAdd<T>::gen(common::ObIArray<int64_t>& part_ids)
{
  int ret = common::OB_SUCCESS;
  part_ids.reset();

  const ObPartitionLevel part_level = table_.get_part_level();
  const int64_t inc_part_num = inc_table_.get_part_option().get_part_num();
  int64_t max_used_part_id = table_.get_part_option().get_max_used_part_id();
  ObPartition** part_array = inc_table_.get_part_array();
  // for compatible
  if (-1 == max_used_part_id) {
    max_used_part_id = table_.get_part_option().get_part_num() - 1;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
    ObPartition* part = part_array[i];
    if (PARTITION_LEVEL_TWO == part_level) {
      if (!table_.is_sub_part_template()) {
        if (OB_ISNULL(part)) {
          ret = common::OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN, "part_array[i] is null", KR(ret), K(i));
        } else {
          // In the non-template secondary partition, when the secondary partition is added separately,
          // the primary partition id is determined by the SQL side
          int64_t part_id = part->get_part_id();
          // In the non-template secondary partition, when the entire primary partition is added,
          // the primary partition id is generated on the rs side
          if (common::OB_INVALID_ID == part_id) {
            part_id = max_used_part_id - inc_part_num + i + 1;
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
            int64_t max_used_sub_part_id = part->get_max_used_sub_part_id();
            if (OB_ISNULL(part->get_subpart_array())) {
              ret = common::OB_ERR_UNEXPECTED;
              SHARE_LOG(WARN, "subpart array is null", KR(ret));
            } else if (OB_ISNULL(part->get_subpart_array()[j])) {
              ret = common::OB_ERR_UNEXPECTED;
              SHARE_LOG(WARN, "subpart array is null", KR(ret));
            } else {
              // In the non-template secondary partition, when the entire primary partition is added,
              // the secondary partition id is generated by the SQL side
              int64_t subpart_id = part->get_subpart_array()[j]->get_sub_part_id();
              // In the non-template secondary partition, when the secondary partition is added separately,
              // the secondary partition id is generated on the rs side
              if (common::OB_INVALID_ID == subpart_id) {
                subpart_id = max_used_sub_part_id - part->get_sub_part_num() + j + 1;
              }
              int64_t phy_part_id = generate_phy_part_id(part_id, subpart_id, part_level);
              if (OB_FAIL(part_ids.push_back(phy_part_id))) {
                SHARE_LOG(WARN, "fail to push to part_ids", KR(ret));
              }
            }
          }
        }
      } else {
        int64_t part_id = max_used_part_id - inc_part_num + i + 1;
        const int64_t subpart_num = table_.get_sub_part_option().get_part_num();
        for (int64_t j = 0; OB_SUCC(ret) && j < subpart_num; ++j) {
          int64_t phy_part_id = generate_phy_part_id(part_id, j, part_level);
          if (OB_FAIL(part_ids.push_back(phy_part_id))) {
            SHARE_LOG(WARN, "fail to push to part_ids", KR(ret));
          }
        }
      }
    } else {
      int64_t part_id = max_used_part_id - inc_part_num + i + 1;
      int64_t phy_part_id = generate_phy_part_id(part_id, 0, part_level);
      if (OB_FAIL(part_ids.push_back(phy_part_id))) {
        SHARE_LOG(WARN, "fail to push to part_ids", KR(ret));
      }
    }
  }
  return ret;
}

#define GET_PART_ID(part_array, part_idx, part_id)                                              \
  ({                                                                                            \
    int ret = OB_SUCCESS;                                                                       \
    if (part_idx < 0) {                                                                         \
      ret = OB_INVALID_ARGUMENT;                                                                \
      LOG_WARN("Invalid part idx", K(ret));                                                     \
    } else if (OB_ISNULL(part_array)) {                                                         \
      ret = OB_ERR_UNEXPECTED;                                                                  \
      LOG_WARN("Empty partition", K(ret));                                                      \
    } else if (OB_ISNULL(part_array[part_idx])) {                                               \
      ret = OB_ERR_UNEXPECTED;                                                                  \
      LOG_WARN("NULL ptr", K(ret));                                                             \
    } else if (part_array[part_idx]->get_part_id() < 0) {                                       \
      ret = OB_INVALID_ARGUMENT;                                                                \
      LOG_WARN("Invalid part id", K(ret), K(part_idx), K(part_array[part_idx]->get_part_id())); \
    } else {                                                                                    \
      part_id = part_array[part_idx]->get_part_id();                                            \
    }                                                                                           \
    ret;                                                                                        \
  })

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
#endif
