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

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "observer/table_load/plan/ob_table_load_plan_common.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_table_store.h"
#include "storage/direct_load/ob_direct_load_trans_param.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadInsertTableContext;
class ObDirectLoadTransParam;
} // namespace storage
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadRowProjector;
class ObTableLoadStoreDataTableCtx;
class ObTableLoadStoreLobTableCtx;
class ObTableLoadStoreIndexTableCtx;
class ObTableLoadLobTableBuilder;
class ObTableLoadIndexTableBuilder;
class ObTableLoadDataTableBuilder;

struct ObTableLoadStoreInsertTableParam
{
public:
  ObTableLoadStoreInsertTableParam()
    : insert_sstable_type_(storage::ObDirectLoadInsertSSTableType::INVALID_INSERT_SSTABLE_TYPE),
      trans_param_(),
      need_reserved_parallel_(false),
      need_online_opt_stat_gather_(false),
      need_insert_lob_(false)
  {
  }
  TO_STRING_KV("insert_sstable_type",
               storage::ObDirectLoadInsertSSTableType::get_type_string(insert_sstable_type_),
               K_(trans_param), K_(need_reserved_parallel), K_(need_online_opt_stat_gather),
               K_(need_insert_lob));

public:
  storage::ObDirectLoadInsertSSTableType::Type insert_sstable_type_;
  storage::ObDirectLoadTransParam trans_param_; // 增量导入需要事务信息
  bool need_reserved_parallel_; // 是否需要预留data_seq, 目前只有全量快速堆表需要预留
  bool need_online_opt_stat_gather_; // 是否需要收集统计信息
  bool need_insert_lob_; // 插入主表行的时候是否需要将其中的lob列写入lob表
};

class ObTableLoadStoreTableCtx
{
private:
  typedef common::hash::ObHashMap<int64_t, ObIDirectLoadPartitionTableBuilder *> TableBuilderMap;

public:
  ObTableLoadStoreTableCtx(ObTableLoadStoreCtx *store_ctx);
  ~ObTableLoadStoreTableCtx();
  virtual int init(
    const uint64_t table_id,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId>
      &target_partition_id_array) = 0;

  int get_table_data_desc(const ObTableLoadInputDataType::Type input_data_type,
                          storage::ObDirectLoadTableDataDesc &table_data_desc);

  TO_STRING_KV(KP_(store_ctx), KPC_(schema), K_(is_inited));

protected:
  int inner_init(const uint64_t table_id);

  //////////////////////// insert_table_ctx ////////////////////////
public:
  virtual int init_insert_table_ctx(const storage::ObDirectLoadTransParam &trans_param,
                                    bool online_opt_stat_gather, bool is_insert_lob) = 0;
  virtual int close_insert_table_ctx() = 0;

  // DAG版本使用的接口
  virtual int open_insert_table_ctx(const ObTableLoadStoreInsertTableParam &param,
                                    ObIAllocator &allocator,
                                    storage::ObDirectLoadInsertTableContext *&insert_table_ctx) = 0;

  //////////////////////// table builder ////////////////////////
public:
#define DEFINE_TABLE_LOAD_STORE_TABLE_BUILD(builderType, name)                              \
public:                                                                                     \
  int init_build_##name##_table();                                                          \
  int get_##name##_table_builder(builderType *&table_builder);                              \
  int close_build_##name##_table();                                                         \
                                                                                            \
private:                                                                                    \
  storage::ObDirectLoadTableDataDesc get_##name##_table_data_desc();                        \
  void clear_##name##_table_builder();                                                      \
                                                                                            \
private:                                                                                    \
  common::ObArenaAllocator name##_table_builder_allocator_;                                 \
  common::ObSafeArenaAllocator name##_table_builder_safe_allocator_;                        \
  common::hash::ObHashMap<int64_t, builderType *> name##_table_builder_map_;                \
                                                                                            \
public:                                                                                     \
  ObDirectLoadTableStore name##_table_store_;

  //////////////////////// members ////////////////////////
public:
  ObTableLoadStoreCtx *store_ctx_;
  uint64_t table_id_;
  ObTableLoadSchema *schema_;
  common::ObArray<table::ObTableLoadLSIdAndPartitionId> ls_partition_ids_;
  common::ObArray<table::ObTableLoadLSIdAndPartitionId> target_ls_partition_ids_;
  storage::ObDirectLoadInsertTableContext *insert_table_ctx_;

protected:
  common::ObArenaAllocator allocator_;
  bool is_inited_;
};

class ObTableLoadStoreDataTableCtx : public ObTableLoadStoreTableCtx
{
public:
  ObTableLoadStoreDataTableCtx(ObTableLoadStoreCtx *store_ctx);
  virtual ~ObTableLoadStoreDataTableCtx();
  int init(const uint64_t table_id,
           const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
           const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId>
             &target_partition_id_array) override;
  ObTableLoadStoreLobTableCtx *get_lob_table_ctx() const { return lob_table_ctx_; }

private:
  int init_data_project();
  int init_ls_partition_ids(
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_id_array);
  int acquire_table_builder(ObTableLoadDataTableBuilder *&table_builder, ObIAllocator &allocator,
                            ObDirectLoadTableDataDesc table_data_desc);
  int check_tablet(const share::ObLSID &ls_id,
                   const ObTabletID &tablet_id,
                   const int64_t schema_version);
  //////////////////////// insert_table_ctx ////////////////////////
public:
  int init_insert_table_ctx(const storage::ObDirectLoadTransParam &trans_param,
                            bool online_opt_stat_gather, bool is_insert_lob) override;
  int close_insert_table_ctx() override;

  int open_insert_table_ctx(const ObTableLoadStoreInsertTableParam &param, ObIAllocator &allocator,
                            storage::ObDirectLoadInsertTableContext *&insert_table_ctx) override;

  //////////////////////// table builder ////////////////////////
  DEFINE_TABLE_LOAD_STORE_TABLE_BUILD(ObTableLoadDataTableBuilder, delete);
  DEFINE_TABLE_LOAD_STORE_TABLE_BUILD(ObTableLoadDataTableBuilder, ack);

public:
  ObTableLoadStoreLobTableCtx *lob_table_ctx_;
  ObTableLoadRowProjector *project_;
  ObDirectLoadTableStore insert_table_store_;
};

class ObTableLoadStoreLobTableCtx : public ObTableLoadStoreTableCtx
{
  friend class ObTableLoadStoreDataTableCtx;
public:
  ObTableLoadStoreLobTableCtx(ObTableLoadStoreCtx *store_ctx,
                              ObTableLoadStoreDataTableCtx *data_table_ctx);
  virtual ~ObTableLoadStoreLobTableCtx();
  int init(const uint64_t table_id,
           const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
           const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId>
             &target_partition_id_array) override;
  ObTableLoadStoreDataTableCtx *get_data_table_ctx() const { return data_table_ctx_; }
  int get_tablet_id(const ObTabletID &data_tablet_id, ObTabletID &tablet_id);

private:
  int init_ls_partition_ids(
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_id_array);
  int acquire_table_builder(ObTableLoadLobTableBuilder *&table_builder, ObIAllocator &allocator,
                            ObDirectLoadTableDataDesc table_data_desc);
  //////////////////////// insert_table_ctx ////////////////////////
public:
  int init_insert_table_ctx(const storage::ObDirectLoadTransParam &trans_param,
                            bool online_opt_stat_gather, bool is_insert_lob) override;
  int close_insert_table_ctx() override;

  int open_insert_table_ctx(const ObTableLoadStoreInsertTableParam &param, ObIAllocator &allocator,
                            storage::ObDirectLoadInsertTableContext *&insert_table_ctx) override;

  //////////////////////// table builder ////////////////////////
  DEFINE_TABLE_LOAD_STORE_TABLE_BUILD(ObTableLoadLobTableBuilder, delete);

  //////////////////////// members ////////////////////////
public:
  ObTableLoadStoreDataTableCtx *data_table_ctx_;

private:
  // data_tablet_id => lob_tablet_id
  typedef common::hash::ObHashMap<ObTabletID, ObTabletID, common::hash::NoPthreadDefendMode>
    TabletIDMap;
  TabletIDMap tablet_id_map_;
};

class ObTableLoadStoreIndexTableCtx : public ObTableLoadStoreTableCtx
{
public:
  ObTableLoadStoreIndexTableCtx(ObTableLoadStoreCtx *store_ctx);
  virtual ~ObTableLoadStoreIndexTableCtx();
  int init(const uint64_t table_id,
           const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
           const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId>
             &target_partition_id_array) override;

private:
  int init_index_projector();
  int init_ls_partition_ids(
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_id_array);
  int acquire_table_builder(ObTableLoadIndexTableBuilder *&table_builder, ObIAllocator &allocator,
                            ObDirectLoadTableDataDesc table_data_desc);
  //////////////////////// insert_table_ctx ////////////////////////
public:
  int init_insert_table_ctx(const storage::ObDirectLoadTransParam &trans_param,
                            bool online_opt_stat_gather, bool is_insert_lob) override;
  int close_insert_table_ctx() override;

  int open_insert_table_ctx(const ObTableLoadStoreInsertTableParam &param, ObIAllocator &allocator,
                            storage::ObDirectLoadInsertTableContext *&insert_table_ctx) override;

  //////////////////////// table builder ////////////////////////
  DEFINE_TABLE_LOAD_STORE_TABLE_BUILD(ObTableLoadIndexTableBuilder, insert);
  DEFINE_TABLE_LOAD_STORE_TABLE_BUILD(ObTableLoadIndexTableBuilder, delete);
  //////////////////////// members ////////////////////////
private:
  ObTableLoadRowProjector *project_;
};

} // namespace observer
} // namespace oceanbase
