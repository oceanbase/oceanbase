/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/direct_load/ob_direct_load_auto_inc_seq_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"

namespace oceanbase
{
namespace storage
{

ObDirectLoadAutoIncSeqService &ObDirectLoadAutoIncSeqService::get_instance()
{
  static ObDirectLoadAutoIncSeqService instance;
  return instance;
}

int ObDirectLoadAutoIncSeqService::get_start_seq(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t step_size,
    ObDirectLoadAutoIncSeqData &start_seq)
{
  return get_instance().inner_get_start_seq(ls_id, tablet_id, step_size, start_seq);
}

int ObDirectLoadAutoIncSeqService::inner_get_start_seq(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t step_size,
    ObDirectLoadAutoIncSeqData &start_seq)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is nullptr", KR(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls handle is invalid", KR(ret), K(ls_handle));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is nullptr", KR(ret), K(ls_id), K(ls_handle));
  } else {
    lib::ObMutexGuard guard(init_node_mutexs_[tablet_id.id() % INIT_NODE_MUTEX_NUM]);
    ObTabletHandle tablet_handle;
    if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
      LOG_WARN("fail to get tablet", KR(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_direct_load_auto_inc_seq(start_seq))) {
      if (OB_EMPTY_RESULT != ret) {
        LOG_WARN("fail to get direct load auto inc seq", KR(ret), K(tablet_id));
      } else {
        ret = OB_SUCCESS;
        if (OB_FAIL(start_seq.set_seq_val(0))) {
          LOG_WARN("fail to set seq val", KR(ret), K(start_seq));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadAutoIncSeqData new_seq;
      if (OB_UNLIKELY(!start_seq.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("start seq is invalid", KR(ret), K(start_seq));
      } else if (OB_FAIL(new_seq.set_seq_val(start_seq.get_seq_val() + step_size))) {
        LOG_WARN("fail to set new seq", KR(ret), K(step_size));
      } else if (OB_FAIL(update_direct_load_auto_inc_seq(*ls, tablet_id, new_seq))) {
        LOG_WARN("fail to update direct load auto inc seq", KR(ret), K(tablet_id), K(new_seq));
      }
    }
  }
  return ret;
}

int ObDirectLoadAutoIncSeqService::update_direct_load_auto_inc_seq(
    const ObLS &ls,
    const ObTabletID &tablet_id,
    ObDirectLoadAutoIncSeqData &new_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!new_seq.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data is invalid", KR(ret), K(new_seq));
  } else {
    ObArenaAllocator tmp_allocator(ObMemAttr(MTL_ID(), "TLD_AUTOINC"));
    ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    ObMySQLTransaction trans;
    observer::ObInnerSQLConnection *conn = nullptr;
    const ObLSID &ls_id = ls.get_ls_id();
    ObDirectLoadAutoIncSeqArg arg;
    if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is nullptr", KR(ret));
    } else if (OB_FAIL(trans.start(sql_proxy, MTL_ID()))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans. get_connection()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connection is nullptr", KR(ret));
    } else {
      int64_t pos = 0;
      int64_t buf_len = 0;
      char *buf = nullptr;
      arg.ls_id_     = ls_id;
      arg.tablet_id_ = tablet_id;
      arg.seq_   = new_seq;
      if (OB_UNLIKELY(!arg.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg is invalid", KR(ret), K(ls_id), K(tablet_id), K(new_seq), K(arg));
      } else if (FALSE_IT(buf_len = arg.get_serialize_size())) {
      } else if (OB_ISNULL(buf = static_cast<char *>(tmp_allocator.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate buf", KR(ret), K(buf_len));
      } else if (OB_FAIL(arg.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize arg", KR(ret), K(arg));
      } else if (OB_FAIL(conn->register_multi_data_source(MTL_ID(),
                                                          ls_id,
                                                          transaction::ObTxDataSourceType::DIRECT_LOAD_AUTO_INC_SEQ,
                                                          buf,
                                                          buf_len))) {
        LOG_WARN("fail to register mds data", KR(ret), K(ls_id));
      } else if (OB_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to end trans", KR(ret));
      } else {
        LOG_INFO("success to update direct load auto inc seq", K(ls_id), K(tablet_id), K(new_seq));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase