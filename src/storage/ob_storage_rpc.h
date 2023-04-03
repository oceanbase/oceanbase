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

#ifndef OCEABASE_STORAGE_RPC
#define OCEABASE_STORAGE_RPC

#include "lib/net/ob_addr.h"
#include "lib/utility/ob_unify_serialize.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "common/ob_member.h"
#include "storage/ob_storage_struct.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_storage_schema.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/ls/ob_ls_meta_package.h"
#include "tablet/ob_tablet_meta.h"
#include "share/restore/ob_ls_restore_status.h"
#include "storage/lob/ob_lob_rpc_struct.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}

namespace storage
{
class ObLogStreamService;
}

namespace obrpc
{

//TODO(yanfeng) need use tenant module replace it in 4.1, currently use 509 tenant

struct ObCopyMacroBlockArg
{
  OB_UNIS_VERSION(2);
public:
  ObCopyMacroBlockArg();
  virtual ~ObCopyMacroBlockArg() {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(logic_macro_block_id));
  blocksstable::ObLogicMacroBlockId logic_macro_block_id_;
};

struct ObCopyMacroBlockListArg
{
  OB_UNIS_VERSION(2);
public:
  ObCopyMacroBlockListArg();
  virtual ~ObCopyMacroBlockListArg() {}

  void reset();
  bool is_valid() const;
  int assign(const ObCopyMacroBlockListArg &arg);

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(table_key), "arg_count", arg_list_.count());
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  storage::ObITable::TableKey table_key_;
  common::ObSArray<ObCopyMacroBlockArg> arg_list_;
};

struct ObCopyMacroBlockRangeArg final
{
  OB_UNIS_VERSION(2);
public:
  ObCopyMacroBlockRangeArg();
  ~ObCopyMacroBlockRangeArg() {}

  void reset();
  bool is_valid() const;
  int assign(const ObCopyMacroBlockRangeArg &arg);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(table_key), K_(data_version), K_(backfill_tx_scn), K_(copy_macro_range_info));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  storage::ObITable::TableKey table_key_;
  int64_t data_version_;
  share::SCN backfill_tx_scn_;
  storage::ObCopyMacroRangeInfo copy_macro_range_info_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyMacroBlockRangeArg);
};

struct ObCopyMacroBlockHeader
{
  OB_UNIS_VERSION(2);
public:
  ObCopyMacroBlockHeader();
  virtual ~ObCopyMacroBlockHeader() {}
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(is_reuse_macro_block), K_(occupy_size));
  bool is_reuse_macro_block_;
  int64_t occupy_size_;
};

struct ObCopyTabletInfoArg
{
  OB_UNIS_VERSION(2);
public:
  ObCopyTabletInfoArg();
  virtual ~ObCopyTabletInfoArg() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id_list), K_(need_check_seq),
      K_(ls_rebuild_seq), K_(is_only_copy_major), K_(version));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObSArray<common::ObTabletID> tablet_id_list_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  bool is_only_copy_major_;
  uint64_t version_;
};

struct ObCopyTabletInfo
{
  OB_UNIS_VERSION(2);
public:
  ObCopyTabletInfo();
  virtual ~ObCopyTabletInfo() {}
  void reset();
  int assign(const ObCopyTabletInfo &info);
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(status), K_(param), K_(data_size), K_(version));

  common::ObTabletID tablet_id_;
  storage::ObCopyTabletStatus::STATUS status_;
  storage::ObMigrationTabletParam param_;
  int64_t data_size_; //need copy ssttablet size
  uint64_t version_;
};

struct ObCopyTabletSSTableInfoArg final
{
  OB_UNIS_VERSION(2);
public:
  ObCopyTabletSSTableInfoArg();
  ~ObCopyTabletSSTableInfoArg();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(tablet_id), K_(max_major_sstable_snapshot), K_(minor_sstable_scn_range),
      K_(ddl_sstable_scn_range));

  common::ObTabletID tablet_id_;
  int64_t max_major_sstable_snapshot_;
  share::ObScnRange minor_sstable_scn_range_;
  share::ObScnRange ddl_sstable_scn_range_;
};

struct ObCopyTabletsSSTableInfoArg final
{
  OB_UNIS_VERSION(2);
public:
  ObCopyTabletsSSTableInfoArg();
  ~ObCopyTabletsSSTableInfoArg();
  bool is_valid() const;
  void reset();
  int assign(const ObCopyTabletsSSTableInfoArg &arg);

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(need_check_seq),
      K_(ls_rebuild_seq), K_(is_only_copy_major), K_(tablet_sstable_info_arg_list),
      K_(version));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  bool is_only_copy_major_;
  common::ObSArray<ObCopyTabletSSTableInfoArg> tablet_sstable_info_arg_list_;
  uint64_t version_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyTabletsSSTableInfoArg);
};

struct ObCopyTabletSSTableInfo
{
  OB_UNIS_VERSION(2);
public:
  ObCopyTabletSSTableInfo();
  virtual ~ObCopyTabletSSTableInfo() {}
  void reset();
  int assign(const ObCopyTabletSSTableInfo &info);
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(table_key), K_(param));

  common::ObTabletID tablet_id_;
  storage::ObITable::TableKey table_key_;
  blocksstable::ObMigrationSSTableParam param_;
};

struct ObCopyLSInfoArg
{
  OB_UNIS_VERSION(2);
public:
  ObCopyLSInfoArg();
  virtual ~ObCopyLSInfoArg() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(ls_id));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  uint64_t version_;
};

struct ObCopyLSInfo
{
  OB_UNIS_VERSION(2);
public:
  ObCopyLSInfo();
  virtual ~ObCopyLSInfo() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(ls_meta_package), K_(tablet_id_array), K_(is_log_sync), K_(version));
  storage::ObLSMetaPackage ls_meta_package_;
  common::ObSArray<common::ObTabletID> tablet_id_array_;
  bool is_log_sync_;
  uint64_t version_;
};

struct ObFetchLSMetaInfoArg
{
  OB_UNIS_VERSION(2);
public:
  ObFetchLSMetaInfoArg();
  virtual ~ObFetchLSMetaInfoArg() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(version));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  uint64_t version_;
};

struct ObFetchLSMetaInfoResp
{
  OB_UNIS_VERSION(2);
public:
  ObFetchLSMetaInfoResp();
  virtual ~ObFetchLSMetaInfoResp() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(ls_meta_package), K_(version));
  storage::ObLSMetaPackage ls_meta_package_;
  uint64_t version_;
};

struct ObFetchLSMemberListArg
{
  OB_UNIS_VERSION(2);
public:
  ObFetchLSMemberListArg();
  virtual ~ObFetchLSMemberListArg() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(ls_id));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

struct ObFetchLSMemberListInfo
{
  OB_UNIS_VERSION(2);
public:
  ObFetchLSMemberListInfo();
  virtual ~ObFetchLSMemberListInfo() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(member_list));
  common::ObMemberList member_list_;
};

struct ObCopySSTableMacroRangeInfoArg final
{
  OB_UNIS_VERSION(2);
public:
  ObCopySSTableMacroRangeInfoArg();
  ~ObCopySSTableMacroRangeInfoArg();
  bool is_valid() const;
  void reset();
  int assign(const ObCopySSTableMacroRangeInfoArg &arg);

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(copy_table_key_array), K_(macro_range_max_marco_count));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObSArray<ObITable::TableKey> copy_table_key_array_;
  int64_t macro_range_max_marco_count_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTableMacroRangeInfoArg);
};

struct ObCopySSTableMacroRangeInfoHeader final
{
  OB_UNIS_VERSION(2);
public:
  ObCopySSTableMacroRangeInfoHeader();
  ~ObCopySSTableMacroRangeInfoHeader();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(copy_table_key), K_(macro_range_count));

  ObITable::TableKey copy_table_key_;
  int64_t macro_range_count_;
};

struct ObCopyTabletSSTableHeader final
{
  OB_UNIS_VERSION(2);
public:
  ObCopyTabletSSTableHeader();
  ~ObCopyTabletSSTableHeader() {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(status), K_(sstable_count), K_(tablet_meta), K_(version));

  common::ObTabletID tablet_id_;
  storage::ObCopyTabletStatus::STATUS status_;
  int64_t sstable_count_;
  ObMigrationTabletParam tablet_meta_;
  uint64_t version_;
};

// Leader notify follower to restore some tablets.
struct ObNotifyRestoreTabletsArg
{
  OB_UNIS_VERSION(2);
public:
  ObNotifyRestoreTabletsArg();
  virtual ~ObNotifyRestoreTabletsArg() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id_array), K_(restore_status), K_(leader_proposal_id));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObSArray<common::ObTabletID> tablet_id_array_;
  share::ObLSRestoreStatus restore_status_; // indicate the type of data to restore
  int64_t leader_proposal_id_;
};

struct ObNotifyRestoreTabletsResp
{
  OB_UNIS_VERSION(2);
public:
  ObNotifyRestoreTabletsResp();
  virtual ~ObNotifyRestoreTabletsResp() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(restore_status));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  share::ObLSRestoreStatus restore_status_; // restore status
};


struct ObInquireRestoreResp
{
  OB_UNIS_VERSION(2);
public:
  ObInquireRestoreResp();
  virtual ~ObInquireRestoreResp() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(is_leader), K_(restore_status));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  bool is_leader_;
  share::ObLSRestoreStatus restore_status_; // leader restore status
};

struct ObInquireRestoreArg
{
  OB_UNIS_VERSION(2);
public:
  ObInquireRestoreArg();
  virtual ~ObInquireRestoreArg() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(restore_status));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  share::ObLSRestoreStatus restore_status_; // restore status
};

struct ObRestoreUpdateLSMetaArg
{
  OB_UNIS_VERSION(2);
public:
  ObRestoreUpdateLSMetaArg();
  virtual ~ObRestoreUpdateLSMetaArg() {}
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(ls_meta_package));
  uint64_t tenant_id_;
  storage::ObLSMetaPackage ls_meta_package_;
};

//src
class ObStorageRpcProxy : public obrpc::ObRpcProxy
{
public:
  static const int64_t STREAM_RPC_TIMEOUT = 30 * 1000 * 1000LL; // 30s
  DEFINE_TO(ObStorageRpcProxy);
  RPC_SS(PR5 fetch_macro_block, OB_HA_FETCH_MACRO_BLOCK, (ObCopyMacroBlockRangeArg), common::ObDataBuffer);
  RPC_SS(PR5 fetch_tablet_info, OB_HA_FETCH_TABLET_INFO, (ObCopyTabletInfoArg), common::ObDataBuffer);
  RPC_SS(PR5 fetch_tablet_sstable_info, OB_HA_FETCH_SSTABLE_INFO, (ObCopyTabletsSSTableInfoArg), common::ObDataBuffer);
  RPC_SS(PR5 fetch_sstable_macro_info, OB_HA_FETCH_SSTABLE_MACRO_INFO, (ObCopySSTableMacroRangeInfoArg), common::ObDataBuffer);
  // lob
  RPC_SS(PR5 lob_query, OB_LOB_QUERY, (ObLobQueryArg), common::ObDataBuffer);

  RPC_S(PR5 fetch_ls_member_list, OB_HA_FETCH_LS_MEMBER_LIST, (ObFetchLSMemberListArg), ObFetchLSMemberListInfo);
  RPC_S(PR5 fetch_ls_meta_info, OB_HA_FETCH_LS_META_INFO, (ObFetchLSMetaInfoArg), ObFetchLSMetaInfoResp);
  RPC_S(PR5 fetch_ls_info, OB_HA_FETCH_LS_INFO, (ObCopyLSInfoArg), ObCopyLSInfo);
  RPC_S(PR5 notify_restore_tablets, OB_HA_NOTIFY_RESTORE_TABLETS, (ObNotifyRestoreTabletsArg), ObNotifyRestoreTabletsResp);
  RPC_S(PR5 inquire_restore, OB_HA_NOTIFY_FOLLOWER_RESTORE, (ObInquireRestoreArg), ObInquireRestoreResp);
  RPC_S(PR5 update_ls_meta, OB_HA_UPDATE_LS_META, (ObRestoreUpdateLSMetaArg));
};

template <ObRpcPacketCode RPC_CODE>
class ObStorageStreamRpcP : public ObRpcProcessor<obrpc::ObStorageRpcProxy::ObRpc<RPC_CODE> >
{
public:
  explicit ObStorageStreamRpcP(common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual ~ObStorageStreamRpcP() {}
protected:
  template <typename Data>
  int fill_data(const Data &data);
  template <typename Data>
  int fill_data_list(ObIArray<Data> &data_list);
  template<typename Data>
  int fill_data_immediate(const Data &data);
  int fill_buffer(blocksstable::ObBufferReader &data);
  int flush_and_wait();
  int alloc_buffer();

  int is_follower_ls(logservice::ObLogService *log_srv, ObLS *ls, bool &is_ls_follower);
protected:
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  int64_t last_send_time_;
  common::ObArenaAllocator allocator_;
};

class ObHAFetchMacroBlockP: public ObStorageStreamRpcP<OB_HA_FETCH_MACRO_BLOCK>
{
public:
  explicit ObHAFetchMacroBlockP(common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual ~ObHAFetchMacroBlockP() {}
protected:
  int process();
private:
  int64_t total_macro_block_count_;
};

class ObFetchTabletInfoP :
    public ObStorageStreamRpcP<OB_HA_FETCH_TABLET_INFO>
{
public:
  explicit ObFetchTabletInfoP(common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual ~ObFetchTabletInfoP() {}
protected:
  int process();
};

class ObFetchSSTableInfoP :
    public ObStorageStreamRpcP<OB_HA_FETCH_SSTABLE_INFO>
{
public:
  explicit ObFetchSSTableInfoP(common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual ~ObFetchSSTableInfoP() {}
protected:
  int process();
private:
  int build_tablet_sstable_info_(ObLS *ls);
  int build_sstable_info_(
      const obrpc::ObCopyTabletSSTableInfoArg &arg,
      ObLS *ls);
};

class ObFetchLSInfoP :
    public ObStorageRpcProxy::Processor<OB_HA_FETCH_LS_INFO>
{
public:
  explicit ObFetchLSInfoP();
  virtual ~ObFetchLSInfoP() { }
protected:
  int process();
};

class ObFetchLSMetaInfoP :
    public ObStorageRpcProxy::Processor<OB_HA_FETCH_LS_META_INFO>
{
public:
  explicit ObFetchLSMetaInfoP();
  virtual ~ObFetchLSMetaInfoP() { }
protected:
  int process();
};

class ObFetchLSMemberListP:
    public ObStorageRpcProxy::Processor<OB_HA_FETCH_LS_MEMBER_LIST>
{
public:
  explicit ObFetchLSMemberListP();
  virtual ~ObFetchLSMemberListP() { }
protected:
  int process();
};

class ObFetchSSTableMacroInfoP :
    public ObStorageStreamRpcP<OB_HA_FETCH_SSTABLE_MACRO_INFO>
{
public:
  explicit ObFetchSSTableMacroInfoP(common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual ~ObFetchSSTableMacroInfoP() {}
protected:
  int process();
private:
  int fetch_sstable_macro_info_header_();
  int fetch_sstable_macro_range_info_(
      const obrpc::ObCopySSTableMacroRangeInfoHeader &header);
};

class ObNotifyRestoreTabletsP :
    public ObStorageStreamRpcP<OB_HA_NOTIFY_RESTORE_TABLETS>
{
public:
  explicit ObNotifyRestoreTabletsP(common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual ~ObNotifyRestoreTabletsP() {}
protected:
  int process();
};

class ObInquireRestoreP :
    public ObStorageStreamRpcP<OB_HA_NOTIFY_FOLLOWER_RESTORE>
{
public:
  explicit ObInquireRestoreP(common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual ~ObInquireRestoreP() {}
protected:
  int process();
};

class ObUpdateLSMetaP :
    public ObStorageStreamRpcP<OB_HA_UPDATE_LS_META>
{
public:
  explicit ObUpdateLSMetaP(common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual ~ObUpdateLSMetaP() {}
protected:
  int process();
};

class ObLobQueryP : public ObStorageStreamRpcP<OB_LOB_QUERY>
{
public:
  explicit ObLobQueryP(common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual ~ObLobQueryP() {}
protected:
  int process();
private:
  int process_read();
  int process_getlength();
};

} // obrpc


namespace storage
{
//dst
class ObIStorageRpc
{
public:
  ObIStorageRpc() {}
  virtual ~ObIStorageRpc() {}
  virtual int init(
      obrpc::ObStorageRpcProxy *rpc_proxy,
      const common::ObAddr &self,
      obrpc::ObCommonRpcProxy *rs_rpc_proxy) = 0;
  virtual void destroy() = 0;
public:
  virtual int post_ls_info_request(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &src_info,
      const share::ObLSID &ls_id,
      obrpc::ObCopyLSInfo &ls_info) = 0;
  virtual int post_ls_meta_info_request(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &src_info,
      const share::ObLSID &ls_id,
      obrpc::ObFetchLSMetaInfoResp &ls_info) = 0;
  virtual int post_ls_member_list_request(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &src_info,
      const share::ObLSID &ls_id,
      obrpc::ObFetchLSMemberListInfo &ls_info) = 0;
  virtual int post_ls_disaster_recovery_res(const common::ObAddr &server,
                           const obrpc::ObDRTaskReplyResult &res) = 0;

  // Notify follower restore some tablets from leader.
  virtual int notify_restore_tablets(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &follower_info,
      const share::ObLSID &ls_id,
      const int64_t &proposal_id,
      const common::ObIArray<common::ObTabletID>& tablet_id_array,
      const share::ObLSRestoreStatus &restore_status,
      obrpc::ObNotifyRestoreTabletsResp &restore_resp) = 0;

  // inquire restore status from src.
  virtual int inquire_restore(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &src_info,
      const share::ObLSID &ls_id,
      const share::ObLSRestoreStatus &restore_status,
      obrpc::ObInquireRestoreResp &restore_resp) = 0;

  virtual int update_ls_meta(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &dest_info,
      const storage::ObLSMetaPackage &ls_meta) = 0;
};

class ObStorageRpc: public ObIStorageRpc
{
public:
  ObStorageRpc();
  ~ObStorageRpc();
  int init(obrpc::ObStorageRpcProxy *rpc_proxy,
      const common::ObAddr &self, obrpc::ObCommonRpcProxy *rs_rpc_proxy);
  void destroy();
public:
  virtual int post_ls_info_request(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &src_info,
      const share::ObLSID &ls_id,
      obrpc::ObCopyLSInfo &ls_meta);
  virtual int post_ls_meta_info_request(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &src_info,
      const share::ObLSID &ls_id,
      obrpc::ObFetchLSMetaInfoResp &ls_info);
  virtual int post_ls_member_list_request(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &src_info,
      const share::ObLSID &ls_id,
      obrpc::ObFetchLSMemberListInfo &ls_info);
  virtual int post_ls_disaster_recovery_res(const common::ObAddr &server,
                           const obrpc::ObDRTaskReplyResult &res);

  // Notify follower restore some tablets from leader.
  virtual int notify_restore_tablets(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &follower_info,
      const share::ObLSID &ls_id,
      const int64_t &proposal_id,
      const common::ObIArray<common::ObTabletID>& tablet_id_array,
      const share::ObLSRestoreStatus &restore_status,
      obrpc::ObNotifyRestoreTabletsResp &restore_resp);

  // inquire restore status from src.
  virtual int inquire_restore(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &src_info,
      const share::ObLSID &ls_id,
      const share::ObLSRestoreStatus &restore_status,
      obrpc::ObInquireRestoreResp &restore_resp);

  virtual int update_ls_meta(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &dest_info,
      const storage::ObLSMetaPackage &ls_meta);
private:
  bool is_inited_;
  obrpc::ObStorageRpcProxy *rpc_proxy_;
  common::ObAddr self_;
  obrpc::ObCommonRpcProxy *rs_rpc_proxy_;
};

template<obrpc::ObRpcPacketCode RPC_CODE>
class ObStorageStreamRpcReader
{
public:
  ObStorageStreamRpcReader();
  virtual ~ObStorageStreamRpcReader() {}
  int init(common::ObInOutBandwidthThrottle &bandwidth_throttle);
  int fetch_next_buffer_if_need();
  int check_need_fetch_next_buffer(bool &need_fectch);
  int fetch_next_buffer();
  template<typename Data>
  int fetch_and_decode(Data &data);
  template<typename Data>
  int fetch_and_decode(common::ObIAllocator &allocator, Data &data);
  template<typename Data>
  int fetch_and_decode_list(common::ObIAllocator &allocator,
                            common::ObIArray<Data> &data_list);
  template<typename Data>
  int fetch_and_decode_list(
      const int64_t data_list_count,
      common::ObIArray<Data> &data_list);
  common::ObDataBuffer &get_rpc_buffer() { return rpc_buffer_; }
  const common::ObAddr &get_dst_addr() const { return handle_.get_dst_addr(); }
  obrpc::ObStorageRpcProxy::SSHandle<RPC_CODE> &get_handle() { return handle_; }
  void reuse()
  {
    rpc_buffer_.get_position() = 0;
    rpc_buffer_parse_pos_ = 0;
  }
private:
  bool is_inited_;
  obrpc::ObStorageRpcProxy::SSHandle<RPC_CODE> handle_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  common::ObDataBuffer rpc_buffer_;
  int64_t rpc_buffer_parse_pos_;
  common::ObArenaAllocator allocator_;
  int64_t last_send_time_;
  int64_t data_size_;
};

} // storage
} // oceanbase

#include "storage/ob_storage_rpc.ipp"

#endif //OCEANBASE_STORAGE_OB_PARTITION_SERVICE_RPC_
