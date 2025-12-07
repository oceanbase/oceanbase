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

#ifndef OCEANBASE_STORAGE_OB_TABLET_SPLIT_MDS_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_SPLIT_MDS_HELPER

#include "common/ob_tablet_id.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "src/share/ob_tablet_autoincrement_param.h"
#include "src/storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "src/storage/tablet/ob_tablet_split_mds_user_data.h"

namespace oceanbase
{
namespace obrpc
{
struct ObBatchCreateTabletArg;
struct ObBatchGetTabletSplitArg;
struct ObBatchGetTabletSplitRes;
}

namespace share
{
class SCN;
namespace schema
{
class ObTableSchema;
}
}

namespace storage
{
class ObTablet;

namespace mds
{
struct BufferCtx;
}

class ObTabletSplitMdsArg final
{
public:
  // arg with such tablet cnt cannot be more than mds buffer limit (1.5M)
  const static int64_t BATCH_TABLET_CNT = 8192;

  OB_UNIS_VERSION_V(1);

public:
  ObTabletSplitMdsArg() : tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), split_data_tablet_ids_(), split_datas_(), tablet_status_tablet_ids_(), tablet_status_(ObTabletStatus::NONE), tablet_status_data_type_(ObTabletMdsUserDataType::NONE), set_freeze_flag_tablet_ids_(), autoinc_seq_arg_() {}
  ~ObTabletSplitMdsArg() {}
  bool is_valid() const;
  int assign(const ObTabletSplitMdsArg &other);
  void reset();

  static int prepare_basic_args(
    const ObIArray<share::schema::ObTableSchema *> &new_table_schemas,
    const ObIArray<share::schema::ObTableSchema *> &upd_table_schemas,
    const ObIArray<const share::schema::ObTableSchema *> &inc_table_schemas,
    ObIArray<ObTabletID> &src_tablet_ids,
    ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
    ObIArray<blocksstable::ObDatumRowkey> &dst_end_partkey_vals,
    common::ObIAllocator &allocator);

  int init_split_start_src(
    const uint64_t tenant_id,
    const bool is_oracle_mode,
    const share::ObLSID &ls_id,
    const ObIArray<share::schema::ObTableSchema *> &new_table_schemas,
    const ObIArray<share::schema::ObTableSchema *> &upd_table_schemas,
    const ObIArray<ObTabletID> &src_tablet_ids,
    const ObIArray<ObArray<ObTabletID>> &dst_tablet_ids);
  int init_set_freeze_flag(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids);
  int init_split_start_dst(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<const share::schema::ObTableSchema *> &inc_table_schemas,
    const ObIArray<ObTabletID> &src_tablet_ids,
    const ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
    const ObIArray<blocksstable::ObDatumRowkey> &dst_end_partkey_vals);
  int set_autoinc_seq_arg(const obrpc::ObBatchSetTabletAutoincSeqArg &arg);
  int init_split_end_src(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObTabletID &src_data_tablet_id, // or global index tablets
    const ObIArray<ObTabletID> &src_local_index_tablet_ids,
    const ObIArray<ObTabletID> &src_lob_tablet_ids);
  int init_split_end_dst(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t auto_part_size,
    const ObIArray<ObTabletID> &dst_data_tablet_ids, // or global index tablets
    const ObIArray<ObSArray<ObTabletID>> &dst_local_index_tablet_ids,
    const ObIArray<ObSArray<ObTabletID>> &dst_lob_tablet_ids);
  int init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObIArray<ObTabletSplitMdsUserData> &split_datas);

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(split_data_tablet_ids), K_(split_datas), K_(tablet_status_tablet_ids), K_(tablet_status), K_(tablet_status_data_type), K_(set_freeze_flag_tablet_ids), K_(autoinc_seq_arg));

private:
  static bool is_split_data_table(const share::schema::ObTableSchema &table_schema);
  template<typename F>
  static int foreach_part(const share::schema::ObTableSchema &table_schema, const int64_t part_type_filter, F &&op);
  int get_partkey_projector(
    const share::schema::ObTableSchema &data_table_schema,
    const share::schema::ObTableSchema &table_schema,
    ObIArray<uint64_t> &partkey_projector);

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSArray<ObTabletID> split_data_tablet_ids_;
  ObSArray<ObTabletSplitMdsUserData> split_datas_;
  ObSArray<ObTabletID> tablet_status_tablet_ids_;
  ObTabletStatus tablet_status_;
  ObTabletMdsUserDataType tablet_status_data_type_;
  ObSArray<ObTabletID> set_freeze_flag_tablet_ids_; // set transfer freeze flag on replay
  obrpc::ObBatchSetTabletAutoincSeqArg autoinc_seq_arg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitMdsArg);
};

class ObTabletSplitMdsArgPrepareSrcOp final
{
public:
  ObTabletSplitMdsArgPrepareSrcOp(ObIArray<ObTabletID> &src_tablet_ids, ObIArray<ObArray<ObTabletID>> &dst_tablet_ids)
    : src_tablet_ids_(src_tablet_ids), dst_tablet_ids_(dst_tablet_ids) {}
  ~ObTabletSplitMdsArgPrepareSrcOp() = default;
  int operator()(const int64_t part_idx, share::schema::ObBasePartition &part);
private:
  ObIArray<ObTabletID> &src_tablet_ids_;
  ObIArray<ObArray<ObTabletID>> &dst_tablet_ids_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitMdsArgPrepareSrcOp);
};

class ObTabletSplitMdsArgPrepareDstOp final
{
public:
  ObTabletSplitMdsArgPrepareDstOp(const bool is_data_table, const ObPartitionKeyInfo &partition_key_info, ObIArray<ObTabletID> &dst_tablet_ids, ObIArray<blocksstable::ObDatumRowkey> &dst_end_partkey_vals, common::ObIAllocator &allocator)
    : is_data_table_(is_data_table), dst_tablet_ids_(dst_tablet_ids), dst_end_partkey_vals_(dst_end_partkey_vals), allocator_(allocator), partition_key_info_(partition_key_info) {}
  ~ObTabletSplitMdsArgPrepareDstOp() = default;
  int operator()(const int64_t part_idx, share::schema::ObBasePartition &part);
  int check_and_cast_end_partkey(const ObPartitionKeyInfo &partition_key_info,
    blocksstable::ObDatumRowkey &end_partkey,
    common::ObIAllocator &allocator);
private:
  bool is_data_table_;
  ObIArray<ObTabletID> &dst_tablet_ids_;
  ObIArray<blocksstable::ObDatumRowkey> &dst_end_partkey_vals_;
  common::ObIAllocator &allocator_;
  const ObPartitionKeyInfo &partition_key_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitMdsArgPrepareDstOp);
};

struct ObModifyAutoPartSizeOp final
{
  ObModifyAutoPartSizeOp(const int64_t auto_part_size) : auto_part_size_(auto_part_size) {}
  ~ObModifyAutoPartSizeOp() = default;
  int operator()(ObTabletSplitMdsUserData &data);
  int64_t auto_part_size_;
};

class ObTabletSplitMdsHelper
{
public:
  static int on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char* buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);
  static int register_mds(const ObTabletSplitMdsArg &arg, const bool need_flush_redo, ObMySQLTransaction &trans);

  static int set_auto_part_size_for_create(
    const uint64_t tenant_id,
    const obrpc::ObBatchCreateTabletArg &create_arg,
    const ObIArray<int64_t> &auto_part_size_arr,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans);
  static int modify_auto_part_size(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t auto_part_size,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans);

  static int batch_get_tablet_split(
    const int64_t abs_timeout_us,
    const obrpc::ObBatchGetTabletSplitArg &arg,
    obrpc::ObBatchGetTabletSplitRes &res);
  static int get_tablet_split_mds_by_rpc(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    ObIArray<ObTabletSplitMdsUserData> &datas);

  static int get_valid_timeout(const int64_t abs_timeout_us, int64_t &valid_timeout_us);
  static int get_split_data_with_timeout(const ObTablet &tablet, ObTabletSplitMdsUserData &split_data, const int64_t abs_timeout_us);
  static int get_is_spliting(const ObTablet &tablet, bool &is_split_dst);
  static int get_split_info_with_cache(const ObTablet &tablet, common::ObIAllocator &allocator, ObTabletSplitTscInfo &split_info);
  static int prepare_calc_split_dst(ObLS &ls, ObTablet &tablet, const int64_t abs_timeout_us, ObTabletSplitMdsUserData &src_split_data, ObIArray<ObTabletSplitMdsUserData> &dst_split_datas);
  static int calc_split_dst(ObLS &ls, ObTablet &tablet, const blocksstable::ObDatumRowkey &rowkey, const int64_t abs_timeout_us, ObTabletID &dst_tablet_id);
  static int calc_split_dst_lob(ObLS &ls, ObTablet &tablet, const blocksstable::ObDatumRow &data_row, const int64_t abs_timeout_us, ObTabletID &dst_tablet_id);

private:
  static int get_split_info(const ObTablet &tablet, common::ObIAllocator &allocator, ObTabletSplitTscInfo &split_info);
  template<typename F>
  static int modify_tablet_split_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    F &&op,
    ObMySQLTransaction &trans);
  static int modify(const ObTabletSplitMdsArg &arg, const share::SCN &scn, mds::BufferCtx &ctx);
  static int set_tablet_split_mds(const share::ObLSID &ls_id, const ObTabletID &tablet_id, const share::SCN &replay_scn, const ObTabletSplitMdsUserData &data, mds::BufferCtx &ctx);
  static int set_freeze_flag(ObLS &ls, const ObTabletID &tablet_id, const share::SCN &replay_scn);
  static int set_tablet_status(
    ObLS &ls,
    const ObTabletID &tablet_id,
    const ObTabletStatus tablet_status,
    const ObTabletMdsUserDataType data_type,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_SPLIT_MDS_HELPER
