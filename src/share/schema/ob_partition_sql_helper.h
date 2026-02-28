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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_PARTITION_SQL_HELPER_H_
#define OCEANBASE_SHARE_SCHEMA_OB_PARTITION_SQL_HELPER_H_
#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"


namespace oceanbase
{
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{

class ObPartDMLGenerator
{
public:
  ObPartDMLGenerator()
  {
    MEMSET(high_bound_val_, 0, common::OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
    MEMSET(b_high_bound_val_, 0, common::OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
    MEMSET(list_val_, 0, common::OB_MAX_PARTITION_EXPR_LENGTH);
    MEMSET(b_list_val_, 0, common::OB_MAX_B_PARTITION_EXPR_LENGTH);

  }
  int gen_dml(ObDMLSqlSplicer &dml);

protected:
  struct PartInfo
  {
    uint64_t tenant_id_;
    uint64_t table_id_;
    ObTabletID tablet_id_;
    int64_t schema_version_;
    ObPartitionStatus status_;
    common::ObString part_name_;
    common::ObString high_bound_val_;
    common::ObString b_high_bound_val_;
    common::ObString list_val_;
    common::ObString b_list_val_;
    int64_t part_id_;
    int64_t part_idx_;
    uint64_t sub_part_num_;
    int64_t sub_part_id_;
    int64_t sub_part_idx_;
    PartitionType partition_type_;
    common::ObString external_location_;
    storage::ObStorageCachePolicyType part_storage_cache_policy_type_;

    TO_STRING_KV(K_(tenant_id),
                 K_(table_id),
                 K_(tablet_id),
                 K_(schema_version),
                 K_(status),
                 K_(part_name),
                 K_(high_bound_val),
                 K_(b_high_bound_val),
                 K_(list_val),
                 K_(b_list_val),
                 K_(part_id),
                 K_(part_idx),
                 K_(sub_part_num),
                 K_(sub_part_id),
                 K_(sub_part_idx),
                 K_(partition_type),
                 K_(external_location),
                 K_(part_storage_cache_policy_type));
  };
  virtual int extract_part_info(PartInfo &part_info) = 0;
  virtual int convert_to_dml(const PartInfo &part_info, ObDMLSqlSplicer &dml) = 0;
  int gen_high_bound_val_str(const bool is_oracle_mode,
                             const common::ObRowkey &high_bound_val,
                             common::ObString &high_bound_val_str,
                             common::ObString &b_high_bound_val_str,
                             uint64_t tenant_id);
  int gen_list_val_str(const bool is_oracle_mode,
                       const common::ObIArray<common::ObNewRow>& list_value,
                       common::ObString &list_val_str,
                       common::ObString &b_list_val_str,
                       uint64_t tenant_id);

private:
  char high_bound_val_[common::OB_MAX_B_HIGH_BOUND_VAL_LENGTH];
  char b_high_bound_val_[common::OB_MAX_B_HIGH_BOUND_VAL_LENGTH];
  char list_val_[common::OB_MAX_PARTITION_EXPR_LENGTH];
  char b_list_val_[common::OB_MAX_B_PARTITION_EXPR_LENGTH];
  char interval_part_name_[common::OB_MAX_PARTITION_NAME_LENGTH];
};

class ObAddIncPartDMLGenerator : public ObPartDMLGenerator
{
public:
  ObAddIncPartDMLGenerator(const ObPartitionSchema *ori_table,
                           const ObPartition &part,
                           const int64_t inc_part_num,
                           const int64_t part_idx,
                           const int64_t schema_version)
      : ObPartDMLGenerator(), ori_table_(ori_table), part_(part),
      inc_part_num_(inc_part_num), part_idx_(part_idx), schema_version_(schema_version) {}
  virtual ~ObAddIncPartDMLGenerator() {}
protected:
  int extract_part_info(PartInfo &part_info);
  int convert_to_dml(const PartInfo &part_info, ObDMLSqlSplicer &dml);
private:
  const ObPartitionSchema *ori_table_;
  const ObPartition &part_;
  int64_t inc_part_num_;
  int64_t part_idx_;
  int64_t schema_version_;
};

class ObAddIncSubPartDMLGenerator : public ObPartDMLGenerator
{
public:
  ObAddIncSubPartDMLGenerator(const ObPartitionSchema *ori_table,
                           const ObPartition &part,
                           const ObSubPartition &sub_part,
                           const int64_t inc_part_num,
                           const int64_t part_idx,
                           const int64_t subpart_idx,
                           const int64_t schema_version)
      : ObPartDMLGenerator(), ori_table_(ori_table), part_(part), sub_part_(sub_part),
      inc_part_num_(inc_part_num), part_idx_(part_idx), subpart_idx_(subpart_idx), schema_version_(schema_version) {}
  virtual ~ObAddIncSubPartDMLGenerator() {}
protected:
  int extract_part_info(PartInfo &part_info);
  int convert_to_dml(const PartInfo &part_info, ObDMLSqlSplicer &dml);
private:
  const ObPartitionSchema *ori_table_;
  const ObPartition &part_;
  const ObSubPartition &sub_part_;
  int64_t inc_part_num_;
  int64_t part_idx_;
  int64_t subpart_idx_;
  int64_t schema_version_;
};

class ObDropIncPartDMLGenerator : public ObPartDMLGenerator
{
public:
  ObDropIncPartDMLGenerator(const ObPartition &part,
                            const int64_t schema_version)
      : ObPartDMLGenerator(), part_(part),
        schema_version_(schema_version) {}
  virtual ~ObDropIncPartDMLGenerator() {}
protected:
  int extract_part_info(PartInfo &part_info);
  int convert_to_dml(const PartInfo &part_info, ObDMLSqlSplicer &dml);
private:
  const ObPartition &part_;
  int64_t schema_version_;
};

class ObDropIncSubPartDMLGenerator : public ObPartDMLGenerator
{
public:
  ObDropIncSubPartDMLGenerator(const ObSubPartition &sub_part,
                               const int64_t schema_version)
      : ObPartDMLGenerator(), sub_part_(sub_part),
        schema_version_(schema_version) {}
  virtual ~ObDropIncSubPartDMLGenerator() {}
protected:
  int extract_part_info(PartInfo &part_info);
  int convert_to_dml(const PartInfo &part_info, ObDMLSqlSplicer &dml);
private:
  const ObSubPartition &sub_part_;
  int64_t schema_version_;
};

class ObPartSqlHelper
{
public:
  ObPartSqlHelper(common::ObISQLClient &sql_client, const uint64_t tenant_id)
  : tables_(),
    sql_client_(sql_client),
    tenant_id_(tenant_id) {}
  int init(const ObPartitionSchema *table);
  int init(ObIArray<const ObPartitionSchema *> &tables);
  virtual ~ObPartSqlHelper() {}
protected:
  virtual bool is_deleted() const = 0;
  virtual int add_part_info_dml_column(const uint64_t exec_tenant_id,
                                       const ObPartitionSchema *table,
                                       ObDMLSqlSplicer &dml) = 0;
  virtual int add_part_dml_column(const uint64_t exec_tenant_id,
                                  const ObPartitionSchema *table,
                                  const ObPartition &part,
                                  ObDMLSqlSplicer &dml) = 0;
  virtual int add_subpart_dml_column(const uint64_t exec_tenant_id,
                                     const ObPartitionSchema *table,
                                     const int64_t part_id,
                                     const int64_t subpart_id,
                                     const ObSubPartition &subpart,
                                     ObDMLSqlSplicer &dml) = 0;
  virtual int add_def_subpart_dml_column(const uint64_t exec_tenant_id,
                                         const ObPartitionSchema *table,
                                         const int64_t def_subpart_idx,
                                         const ObSubPartition &subpart,
                                         ObDMLSqlSplicer &dml) = 0;

  int iterate_part_info(const bool only_history, const bool include_hidden = true);
  int iterate_all_part(const bool only_history, const bool include_hidden = true);
  int iterate_all_sub_part(const bool only_history, const bool include_hidden = true);
  int iterate_all_def_sub_part(const bool only_history, const bool include_hidden = true);
private:
  struct BatchInsertCtx {
    BatchInsertCtx() { reset(); }
    void reset() {
      count_ = 0;
      sql_.reset();
      history_sql_.reset();
    }
    ObSqlString sql_;
    ObSqlString history_sql_;
    int64_t count_;
    TO_STRING_KV(K(sql_), K(history_sql_), K(count_));
  };
  int write_batch_sql_(const bool only_history, BatchInsertCtx &ctx);
  // `dml` is used to generate header and values
  // insert values are stored in `sql`
  int generate_batch_sql_(const ObDMLSqlSplicer &dml, const char *table_name, ObSqlString &sql);
  int generate_and_batch_write_sqls_(
      ObDMLSqlSplicer &dml,
      const bool only_history,
      const char *table_name,
      const char *history_table_name,
      BatchInsertCtx &ctx);
  int iterate_part_info_(const bool only_history, const ObPartitionSchema *table,
                         BatchInsertCtx &ctx, const bool include_hidden);
  int iterate_all_part_(const bool only_history, const ObPartitionSchema *table,
                        BatchInsertCtx &ctx, const bool include_hidden);
  int iterate_all_sub_part_(const bool only_history, const ObPartitionSchema *table,
                            BatchInsertCtx &ctx, const bool include_hidden);
  int iterate_all_def_sub_part_(const bool only_history, const ObPartitionSchema *table,
                                BatchInsertCtx &ctx, const bool include_hidden);
protected:
  static const int64_t MAX_DML_NUM = 128;
  ObSEArray<const ObPartitionSchema *, 1> tables_;
  common::ObISQLClient &sql_client_;
  const uint64_t tenant_id_;
};

// for create table
class ObAddPartInfoHelper : public ObPartSqlHelper
{
public:
  ObAddPartInfoHelper(common::ObISQLClient &sql_client, const uint64_t tenant_id)
    : ObPartSqlHelper(sql_client, tenant_id),
      high_bound_val_(NULL), list_val_(NULL), allocator_() {
  }
  virtual ~ObAddPartInfoHelper() {}
  int add_partition_info();
protected:
  bool is_deleted() const { return false; }
  int add_part_info_dml_column(const uint64_t exec_tenant_id,
                               const ObPartitionSchema *table,
                               ObDMLSqlSplicer &dml);
  int add_part_dml_column(const uint64_t exec_tenant_id,
                          const ObPartitionSchema *table,
                          const ObPartition &part,
                          ObDMLSqlSplicer &dml);
  int add_subpart_dml_column(const uint64_t exec_tenant_id,
                             const ObPartitionSchema *table,
                             const int64_t part_id,
                             const int64_t subpart_id,
                             const ObSubPartition &subpart,
                             ObDMLSqlSplicer &dml);
  int add_def_subpart_dml_column(const uint64_t exec_tenant_id,
                                 const ObPartitionSchema *table,
                                 const int64_t def_subpart_idx,
                                 const ObSubPartition &subpart,
                                 ObDMLSqlSplicer &dml);
private:
  //add part high bound val column to dml
  int add_part_high_bound_val_column(const ObPartitionSchema *table,
                                     const ObBasePartition &part,
                                     share::ObDMLSqlSplicer &dml);
  //add subpart high bound val column to dml
  int add_subpart_high_bound_val_column(const ObPartitionSchema *table,
                                     const ObBasePartition &part,
                                     share::ObDMLSqlSplicer &dml);
  template<class P>  //ObPartition or ObSubPartition
  int add_high_bound_val_column(
      const ObPartitionSchema *table,
      const P &partition,
      ObDMLSqlSplicer &dml);

  //add part high bound val column to dml
  int add_part_list_val_column(const ObPartitionSchema *table,
                                     const ObBasePartition &part,
                                     share::ObDMLSqlSplicer &dml);
  //add subpart high bound val column to dml
  int add_subpart_list_val_column(const ObPartitionSchema *table,
                                     const ObBasePartition &part,
                                     share::ObDMLSqlSplicer &dml);
  int add_part_storage_cache_policy_column(const ObBasePartition &part, ObDMLSqlSplicer &dml);
  template<class P>  //ObPartition or ObSubPartition
  int add_list_val_column(
      const ObPartitionSchema *table,
      const P &partition,
      ObDMLSqlSplicer &dml);

private:
  char *high_bound_val_;
  char *list_val_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObAddPartInfoHelper);
};

// for drop table
class ObDropPartInfoHelper : public ObPartSqlHelper
{
public:
  ObDropPartInfoHelper(common::ObISQLClient &sql_client, const uint64_t tenant_id)
   : ObPartSqlHelper(sql_client, tenant_id) {}
  virtual ~ObDropPartInfoHelper() {}
  int delete_partition_info();
protected:
  bool is_deleted() const { return true; }
  int add_part_info_dml_column(const uint64_t exec_tenant_id,
                               const ObPartitionSchema *table,
                               ObDMLSqlSplicer &dml);
  int add_part_dml_column(const uint64_t exec_tenant_id,
                          const ObPartitionSchema *table,
                          const ObPartition &part,
                          ObDMLSqlSplicer &dml);
  int add_subpart_dml_column(const uint64_t exec_tenant_id,
                             const ObPartitionSchema *table,
                             const int64_t part_id,
                             const int64_t subpart_id,
                             const ObSubPartition &subpart,
                             ObDMLSqlSplicer &dml);
  int add_def_subpart_dml_column(const uint64_t exec_tenant_id,
                                 const ObPartitionSchema *table,
                                 const int64_t def_subpart_idx,
                                 const ObSubPartition &subpart,
                                 ObDMLSqlSplicer &dml);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropPartInfoHelper);
};

// update partition
class ObUpdatePartHelper
{
public:
  ObUpdatePartHelper(const ObPartitionSchema *ori_table,
                     const ObPartitionSchema *upd_table,
                     const int64_t schema_version,
                     common::ObISQLClient &sql_client)
      : ori_table_(ori_table),
        upd_table_(upd_table),
        schema_version_(schema_version),
        sql_client_(sql_client) {}
  virtual ~ObUpdatePartHelper() {}
  int update_partition_info();
private:
  const ObPartitionSchema *ori_table_;
  const ObPartitionSchema *upd_table_;
  int64_t schema_version_;
  common::ObISQLClient &sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObUpdatePartHelper);
};

// split partition
class ObAddSplitIncPartHelper
{
public:
  ObAddSplitIncPartHelper(const ObPartitionSchema *ori_table,
                          const ObPartitionSchema *inc_table,
                          const int64_t schema_version,
                          common::ObISQLClient &sql_client)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client) {}
  virtual ~ObAddSplitIncPartHelper() {}
  int add_split_partition_info();
private:
  const ObPartitionSchema *ori_table_;
  const ObPartitionSchema *inc_table_;
  int64_t schema_version_;
  common::ObISQLClient &sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObAddSplitIncPartHelper);
};

// add/truncate partition
class ObAddIncPartHelper
{
public:
  ObAddIncPartHelper(const ObPartitionSchema *ori_table,
                     const ObPartitionSchema *inc_table,
                     const int64_t schema_version,
                     common::ObISQLClient &sql_client)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client) {}
  virtual ~ObAddIncPartHelper() {}
  int add_partition_info();
private:
  const ObPartitionSchema *ori_table_;
  const ObPartitionSchema *inc_table_;
  int64_t schema_version_;
  common::ObISQLClient &sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObAddIncPartHelper);
};

// add/truncate subpartition
class ObAddIncSubPartHelper
{
public:
  ObAddIncSubPartHelper(const ObPartitionSchema *ori_table,
                     const ObPartitionSchema *inc_table,
                     const int64_t schema_version,
                     common::ObISQLClient &sql_client)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client) {}
  virtual ~ObAddIncSubPartHelper() {}
  int add_subpartition_info(const bool is_subpart_idx_specified = false);
private:
  const ObPartitionSchema *ori_table_;
  const ObPartitionSchema *inc_table_;
  int64_t schema_version_;
  common::ObISQLClient &sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObAddIncSubPartHelper);
};

// drop/truncate partition
class ObDropIncPartHelper
{
public:
  ObDropIncPartHelper(const ObPartitionSchema *ori_table,
                      const ObPartitionSchema *inc_table,
                      const int64_t schema_version,
                      common::ObISQLClient &sql_client)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client) {}
  virtual ~ObDropIncPartHelper() {}
  int drop_partition_info();
private:
  const ObPartitionSchema *ori_table_;
  const ObPartitionSchema *inc_table_;
  int64_t schema_version_;
  common::ObISQLClient &sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObDropIncPartHelper);
};

// drop/truncate subpartition
class ObDropIncSubPartHelper
{
public:
  ObDropIncSubPartHelper(const ObPartitionSchema *ori_table,
                      const ObPartitionSchema *inc_table,
                      const int64_t schema_version,
                      common::ObISQLClient &sql_client)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client) {}
  virtual ~ObDropIncSubPartHelper() {}
  int drop_subpartition_info();
private:
  const ObPartitionSchema *ori_table_;
  const ObPartitionSchema *inc_table_;
  int64_t schema_version_;
  common::ObISQLClient &sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObDropIncSubPartHelper);
};

//rename partition
class ObRenameIncPartHelper
{
public:
  ObRenameIncPartHelper(const ObPartitionSchema *ori_table,
                        const ObPartitionSchema *inc_table,
                        const int64_t schema_version,
                        common::ObISQLClient &sql_client)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client) {}
  virtual ~ObRenameIncPartHelper() {}
  int rename_partition_info(const bool update_part_idx);
private:
  const ObPartitionSchema *ori_table_;
  const ObPartitionSchema *inc_table_;
  int64_t schema_version_;
  common::ObISQLClient &sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObRenameIncPartHelper);
};

//rename subpartition
class ObRenameIncSubpartHelper
{
public:
  ObRenameIncSubpartHelper(const ObPartitionSchema *ori_table,
                        const ObPartitionSchema *inc_table,
                        const int64_t schema_version,
                        common::ObISQLClient &sql_client)
      : ori_table_(ori_table),
        inc_table_(inc_table),
        schema_version_(schema_version),
        sql_client_(sql_client) {}
  virtual ~ObRenameIncSubpartHelper() {}
  int rename_subpartition_info();
private:
  const ObPartitionSchema *ori_table_;
  const ObPartitionSchema *inc_table_;
  int64_t schema_version_;
  common::ObISQLClient &sql_client_;
  DISALLOW_COPY_AND_ASSIGN(ObRenameIncSubpartHelper);
};
} //end of schema
} //end of share
} //end of oceanbase


#endif /* OCEANBASE_SHARE_SCHEMA_OB_PARTITION_SQL_HELPER_H_ */
