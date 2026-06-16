/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef __SQL_ENG_P2P_DH_MGR_H__
#define __SQL_ENG_P2P_DH_MGR_H__
#include "lib/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_share_info.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_proxy.h"

namespace oceanbase
{
namespace sql
{

class ObPxSQCProxy;

class ObP2PDatahubManager
{
public:
  struct P2PMsgMergeCall
  {
    P2PMsgMergeCall(ObP2PDatahubMsgBase &db_msg) : ret_(OB_SUCCESS), dh_msg_(db_msg) {};
    ~P2PMsgMergeCall() = default;
    int operator() (common::hash::HashMapPair<ObP2PDhKey, ObP2PDatahubMsgBase *> &entry);
    int ret_;
    ObP2PDatahubMsgBase &dh_msg_;
  };

  struct P2PMsgGetCall
  {
    P2PMsgGetCall(ObP2PDatahubMsgBase *&db_msg) : dh_msg_(db_msg), ret_(OB_SUCCESS) {};
    ~P2PMsgGetCall() = default;
    void operator() (common::hash::HashMapPair<ObP2PDhKey, ObP2PDatahubMsgBase *> &entry);
    ObP2PDatahubMsgBase *&dh_msg_;
    int ret_;
  };

  struct P2PMsgEraseIfCall
  {
    P2PMsgEraseIfCall() : ret_(OB_SUCCESS) {};
    ~P2PMsgEraseIfCall() = default;
    bool operator() (common::hash::HashMapPair<ObP2PDhKey, ObP2PDatahubMsgBase *> &entry);
    int ret_;
  };

  struct P2PMsgSetCall
  {
    P2PMsgSetCall(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase &db_msg)
        : dh_key_(dh_key), dh_msg_(db_msg), ret_(OB_SUCCESS) {};
    ~P2PMsgSetCall() = default;
    int operator() (const common::hash::HashMapPair<ObP2PDhKey, ObP2PDatahubMsgBase *> &entry);
    ObP2PDhKey &dh_key_;
    ObP2PDatahubMsgBase &dh_msg_;
    int ret_;
  };

public:
  ObP2PDatahubManager() : map_(), is_inited_(false),
      p2p_dh_proxy_(), p2p_dh_id_(0)
  {}
  ~ObP2PDatahubManager() { destroy(); }
  static ObP2PDatahubManager &instance();
  typedef common::hash::ObHashMap<ObP2PDhKey, ObP2PDatahubMsgBase *> MsgMap;
  int init();
  void destroy();
  int process_msg(ObP2PDatahubMsgBase &msg, bool &need_free);
  int send_p2p_msg(
      ObP2PDatahubMsgBase &msg,
      ObPxSQCProxy &sqc_proxy);
  int send_local_p2p_msg(ObP2PDatahubMsgBase &msg);
  template<typename T>
  int alloc_msg(const ObMemAttr &attr, T *&msg_ptr)
  {
    int ret = OB_SUCCESS;
    void *ptr = nullptr;
    if (OB_ISNULL(ptr = (ob_malloc(sizeof(T), attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "failed to alloc memory for p2p dh msg", K(ret));
    } else {
      msg_ptr = new (ptr) T();
    }
    return ret;
  }

  int alloc_msg(common::ObIAllocator &allocator,
                ObP2PDatahubMsgBase::ObP2PDatahubMsgType type,
                ObP2PDatahubMsgBase *&msg_ptr);
  // Heap allocation for RPC receive; avoids arena + deep_copy into map (see P2PMsgSetCall).
  int alloc_msg_for_rpc_receive(ObP2PDatahubMsgBase::ObP2PDatahubMsgType type,
                                ObP2PDatahubMsgBase *&msg_ptr);
  int send_local_msg(ObP2PDatahubMsgBase *msg);
  int atomic_get_msg(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase *&msg);
  int set_msg(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase *&msg);
  int erase_msg(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase *&msg);
  int erase_msg_if(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase *&msg, bool& is_erased, bool need_unreg_dm=true);
  MsgMap &get_map() { return map_; }
  int deep_copy_msg(ObP2PDatahubMsgBase &msg, ObP2PDatahubMsgBase *&new_msg);
  void free_msg(ObP2PDatahubMsgBase *&msg);
  obrpc::ObP2PDhRpcProxy &get_proxy() { return p2p_dh_proxy_; }
  int generate_p2p_dh_id(int64_t &p2p_dh_id);
private:
  template<typename T>
  int alloc_msg(common::ObIAllocator &allocator,
                T *&msg_ptr, const ObMemAttr &mem_attr);
private:
  static const int64_t BUCKET_NUM = 131072; //2^17
private:
  MsgMap map_;
  bool is_inited_;
  obrpc::ObP2PDhRpcProxy p2p_dh_proxy_;
  int64_t p2p_dh_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObP2PDatahubManager);
};

#define PX_P2P_DH (::oceanbase::sql::ObP2PDatahubManager::instance())

} //end sql;
} //end oceanbase


#endif
