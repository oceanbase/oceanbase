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

#ifndef OCEABASE_STORAGE_OB_LOB_MANAGER_
#define OCEABASE_STORAGE_OB_LOB_MANAGER_
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"
#include "lib/hash/ob_cuckoo_hashmap.h"
#include "ob_lob_meta.h"
#include "ob_lob_piece.h"

namespace oceanbase
{
namespace storage
{

struct ObLobCtx
{
  ObLobCtx() : lob_meta_mngr_(nullptr), lob_piece_mngr_(nullptr) {}
  ObLobMetaManager* lob_meta_mngr_;
  ObLobPieceManager* lob_piece_mngr_;
  TO_STRING_KV(KPC(lob_meta_mngr_), KPC(lob_piece_mngr_));
};

struct ObLobQueryResult {
  ObLobMetaScanResult meta_result_;
  ObLobPieceInfo piece_info_;
  TO_STRING_KV(K_(meta_result), K_(piece_info));
};


class ObLobQueryIter
{
public:
  ObLobQueryIter() : meta_iter_(), lob_ctx_(), param_(), inner_data_(),
                     cur_pos_(0), is_in_row_(false), is_inited_(false) {}
  int open(ObLobAccessParam &param, ObLobCtx& lob_ctx); // outrow open
  int open(ObString &data); // inrow open
  int get_next_row(ObString& data);
  uint64_t get_cur_pos() { return meta_iter_.get_cur_pos(); }
  void reset();
private:
  int get_next_row(ObLobQueryResult &result); // for test
private:
  // outrow ctx
  ObLobMetaScanIter meta_iter_;
  ObLobCtx lob_ctx_;
  ObLobAccessParam param_;
  // inrow ctx
  ObString inner_data_;
  uint64_t cur_pos_;
  bool is_in_row_;
  bool is_inited_;
};

class ObLobManager
{
public:
  static const int64_t LOB_AUX_TABLE_COUNT = 2; // lob aux table count for each table
  static const int64_t LOB_OUTROW_HEADER_SIZE = sizeof(ObLobCommon) + sizeof(ObLobData) + sizeof(ObLobDataOutRowCtx);
private:
  explicit ObLobManager(const uint64_t tenant_id)
    : tenant_id_(tenant_id),
      is_inited_(false),
      allocator_(tenant_id),
      lob_ctxs_(),
      lob_ctx_(),
      meta_manager_(),
      piece_manager_()
  {}
public:
  ~ObLobManager() { destroy(); }
  static int mtl_new(ObLobManager *&m);
  static void mtl_destroy(ObLobManager *&m);
  // MTL 
  static int mtl_init(ObLobManager *&m);
  int init();
  int start();
  int stop();
  void wait();
  void destroy();

  // Only use for default lob col val
  static int fill_lob_header(ObIAllocator &allocator, ObString &data, ObString &out);
  static int fill_lob_header(ObIAllocator &allocator,
                             const ObIArray<share::schema::ObColDesc> &column_ids,
                             blocksstable::ObDatumRow &datum_row);

  // Lob Locator interface
  int append(ObLobAccessParam& param,
             ObString& data);
  int query(ObLobAccessParam& param,
            ObString& data);
  int query(ObLobAccessParam& param,
            ObLobQueryIter *&result);
  int flush(const common::ObTabletID &tablet_id, common::ObNewRow& row);
  // int insert(const common::ObTabletID &tablet_id, ObObj *obj, uint64_t offset, char *data, uint64_t len);
  // int erase(const common::ObTabletID &tablet_id, ObObj *obj, uint64_t offset, uint64_t len);
  int get_real_data(ObLobAccessParam& param,
                    const ObLobQueryResult& result,
                    ObString& data);
  int erase(ObLobAccessParam& param);

private:
  // private function
  
  int erase_imple_inner(ObLobAccessParam& param);
  // write mini unit, write lob data, write meta tablet, write piece tablet
  int write_one_piece(ObLobAccessParam& param,
                      common::ObTabletID& piece_tablet_id,
                      ObLobCtx& lob_ctx,
                      ObLobMetaInfo& meta_info,
                      ObString& data,
                      bool need_alloc_macro_id);

  int update_one_piece(ObLobAccessParam& param,
                       ObLobCtx& lob_ctx,
                       ObLobMetaInfo& old_meta_info,
                       ObLobMetaInfo& new_meta_info,
                       ObLobPieceInfo& piece_info,
                       ObString& data);

  int erase_one_piece(ObLobAccessParam& param,
                      ObLobCtx& lob_ctx,
                      ObLobMetaInfo& meta_info,
                      ObLobPieceInfo& piece_info);

  void transform_query_result_charset(const common::ObCollationType& coll_type,
                                      const char* data,
                                      uint32_t len,
                                      uint32_t &byte_len,
                                      uint32_t &byte_st);
  int check_need_out_row(ObLobAccessParam& param,
                         ObString &data,
                         bool &need_out_row);
  int init_out_row_ctx(ObLobAccessParam& param, uint64_t len, ObLobDataOutRowCtx::OpType op);
  int update_out_ctx(ObLobAccessParam& param, ObLobMetaInfo *old_info, ObLobMetaInfo& new_info);
  int check_handle_size(ObLobAccessParam& param);
private:
  static const int64_t DEFAULT_LOB_META_BUCKET_CNT = 1543;
  static const int64_t LOB_IN_ROW_MAX_LENGTH = 4096; // 4K
  const uint64_t tenant_id_;
  bool is_inited_;
  common::ObFIFOAllocator allocator_;
  // key是主表的tablet_id
  common::hash::ObCuckooHashMap<common::ObTabletID, ObLobCtx> lob_ctxs_;
  // global ctx
  ObLobCtx lob_ctx_;
  ObLobMetaManager meta_manager_;
  ObLobPieceManager piece_manager_;
};

} // storage
} // oceanbase

#endif
