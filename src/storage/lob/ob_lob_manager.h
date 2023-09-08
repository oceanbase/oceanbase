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
#include "storage/ob_storage_rpc.h"

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

struct ObLobCompareParams {

  ObLobCompareParams()
    : collation_left_(CS_TYPE_INVALID),
      collation_right_(CS_TYPE_INVALID),
      offset_left_(0),
      offset_right_(0),
      compare_len_(0),
      timeout_(0),
      tx_desc_(nullptr)
  {
  }

  TO_STRING_KV(K(collation_left_),
               K(collation_right_),
               K(offset_left_),
               K(offset_right_),
               K(compare_len_),
               K(timeout_),
               K(tx_desc_));

  ObCollationType collation_left_;
  ObCollationType collation_right_;
  uint64_t offset_left_;
  uint64_t offset_right_;

  // compare length
  uint64_t compare_len_;
  int64_t timeout_;
  transaction::ObTxDesc *tx_desc_;
};

class ObLobQueryRemoteReader
{
public:
  ObLobQueryRemoteReader() : rpc_buffer_pos_(0), data_buffer_() {}
  ~ObLobQueryRemoteReader() {}
  int open(ObLobAccessParam& param, common::ObDataBuffer &rpc_buffer);
  int get_next_block(ObLobAccessParam& param,
                     common::ObDataBuffer &rpc_buffer,
                     obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> &handle,
                     ObLobQueryBlock &block,
                     ObString &data);
private:
  int do_fetch_rpc_buffer(ObLobAccessParam& param,
                          common::ObDataBuffer &rpc_buffer,
                          obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> &handle);
private:
  int64_t rpc_buffer_pos_;
  ObString data_buffer_;
};

class ObLobQueryIter
{
public:
  ObLobQueryIter() : is_reverse_(false), cs_type_(CS_TYPE_BINARY), is_end_(false),
                     meta_iter_(), lob_ctx_(), param_(), last_data_(), last_data_ptr_(nullptr), last_data_buf_len_(0),
                     inner_data_(), cur_pos_(0), is_in_row_(false), is_inited_(false),
                     is_remote_(false), handle_(), rpc_buffer_(), query_arg_(), remote_reader_() {}
  int open(ObLobAccessParam &param, ObLobCtx& lob_ctx); // outrow open
  int open(ObString &data, uint32_t byte_offset, uint32_t byte_len, ObCollationType cs, bool is_reverse = false); // inrow open
  int open(ObLobAccessParam &param, common::ObAddr dst_addr); // remote open
  int get_next_row(ObString& data);
  uint64_t get_cur_pos() { return meta_iter_.get_cur_pos(); }
  void reset();
  bool is_end() const { return is_end_; }
private:
  int get_next_row(ObLobQueryResult &result); // for test
  bool fill_buffer_to_data(ObString& data);
private:
  // common
  bool is_reverse_;
  ObCollationType cs_type_;
  bool is_end_;
  // outrow ctx
  ObLobMetaScanIter meta_iter_;
  ObLobCtx lob_ctx_;
  ObLobAccessParam param_;
  ObString last_data_;
  char *last_data_ptr_;
  uint64_t last_data_buf_len_;
  // inrow ctx
  ObString inner_data_;
  uint64_t cur_pos_;
  bool is_in_row_;
  bool is_inited_;
  // remote ctx
  bool is_remote_;
  obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> handle_;
  common::ObDataBuffer rpc_buffer_;
  ObLobQueryArg query_arg_;
  ObLobQueryRemoteReader remote_reader_;
};

class ObLobManager
{
public:
  static const int64_t LOB_AUX_TABLE_COUNT = 2; // lob aux table count for each table
  static const int64_t LOB_WITH_OUTROW_CTX_SIZE = sizeof(ObLobCommon) + sizeof(ObLobData) + sizeof(ObLobDataOutRowCtx);
  static const int64_t LOB_OUTROW_FULL_SIZE = sizeof(ObLobCommon) + sizeof(ObLobData) + sizeof(ObLobDataOutRowCtx) + sizeof(uint64_t);
  static const uint64_t LOB_READ_BUFFER_LEN = 1024L*1024L; // 1M
  static const uint64_t REMOTE_LOB_QUERY_RETRY_MAX = 10L; // 1M
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
  // fill lob locator
  static int build_tmp_delta_lob_locator(ObIAllocator &allocator,
                                         ObLobLocatorV2 *persist,
                                         const ObString &data,
                                         bool is_locator,
                                         ObLobDiffFlags flags,
                                         uint8_t op,
                                         uint64_t offset,
                                         uint64_t len,
                                         uint64_t dst_offset,
                                         ObLobLocatorV2 &out);
  static int build_tmp_full_lob_locator(ObIAllocator &allocator,
                                        const ObString &data,
                                        common::ObCollationType coll_type,
                                        ObLobLocatorV2 &out);
  int lob_remote_query_with_retry(
    ObLobAccessParam &param,
    common::ObAddr& dst_addr,
    ObLobQueryArg& arg,
    int64_t timeout,
    common::ObDataBuffer& rpc_buffer,
    obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY>& handle);
  bool is_remote_ret_can_retry(int ret);
  // Tmp Delta Lob locator interface
  int process_delta(ObLobAccessParam& param,
                    ObLobLocatorV2& lob_locator);
  // Lob data interface
  int append(ObLobAccessParam& param,
             ObString& data);
  int append(ObLobAccessParam& param,
             ObLobLocatorV2 &lob);
  int query(ObLobAccessParam& param,
            ObString& data);
  int query(ObLobAccessParam& param,
            ObLobQueryIter *&result);
  int write(ObLobAccessParam& param,
            ObString& data);
  int write(ObLobAccessParam& param,
            ObLobLocatorV2& lob,
            uint64_t offset);

  // compare lob byte wise, collation type is binary
  // @param [in] lob_left lob param of left operand for comparison
  // @param [in] collation_left collation type of left operand for comparison
  // @param [in] offset_left start position of left lob for comparison
  // @param [in] lob_right lob param of right operand for comparison
  // @param [in] collation_right collation type of right operand for comparison
  // @param [in] offset_right start position of right lob for comparison
  // @param [in] amount_len comparison length
  // @param [in] timeout lob read timeout
  // @param [out] result: 0 if the data exactly matches over the range specified by the offset and amount parameters.
  //                      -1 if the first is less than the second, and 1 if it is greater.
  int compare(ObLobLocatorV2& lob_left,
              ObLobLocatorV2& lob_right,
              ObLobCompareParams& cmp_params,
              int64_t& result);

  // int insert(const common::ObTabletID &tablet_id, ObObj *obj, uint64_t offset, char *data, uint64_t len);
  // int erase(const common::ObTabletID &tablet_id, ObObj *obj, uint64_t offset, uint64_t len);
  int get_real_data(ObLobAccessParam& param,
                    const ObLobQueryResult& result,
                    ObString& data);
  int erase(ObLobAccessParam& param);
  int getlength(ObLobAccessParam& param, uint64_t &len);
  int build_lob_param(ObLobAccessParam& param,
                      ObIAllocator &allocator,
                      ObCollationType coll_type,
                      uint64_t offset,
                      uint64_t len,
                      int64_t timeout,
                      ObLobLocatorV2 &lob);
  inline bool can_write_inrow(uint64_t len) { return len <= LOB_IN_ROW_MAX_LENGTH; }
private:
  // private function
  int write_inrow_inner(ObLobAccessParam& param, ObString& data, ObString& old_data);
  int write_inrow(ObLobAccessParam& param, ObLobLocatorV2& lob, uint64_t offset, ObString& old_data);
  int write_outrow_result(ObLobAccessParam& param, ObLobMetaWriteIter &write_iter);
  int write_outrow_inner(ObLobAccessParam& param, ObLobQueryIter *iter, ObString& read_buf, ObString& old_data);
  int write_outrow(ObLobAccessParam& param, ObLobLocatorV2& lob, uint64_t offset, ObString& old_data);

  int query_inrow_get_iter(ObLobAccessParam& param, ObString &data, uint32_t offset, bool scan_backward, ObLobQueryIter *&result);
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
                         int64_t add_len,
                         ObString &data,
                         bool need_combine_data,
                         bool alloc_inside,
                         bool &need_out_row);
  int init_out_row_ctx(ObLobAccessParam& param, uint64_t len, ObLobDataOutRowCtx::OpType op);
  int update_out_ctx(ObLobAccessParam& param, ObLobMetaInfo *old_info, ObLobMetaInfo& new_info);
  int check_handle_size(ObLobAccessParam& param);
  int erase_process_meta_info(ObLobAccessParam& param, ObLobMetaScanIter &meta_iter, ObLobQueryResult &result, ObString &tmp_buff);
  int prepare_for_write(ObLobAccessParam& param,
                        ObString &old_data,
                        bool &need_out_row);
  int prepare_write_buffers(ObLobAccessParam& param, ObString &remain_buf, ObString &tmp_buf);
  int replace_process_meta_info(ObLobAccessParam& param,
                                ObLobMetaScanIter &meta_iter,
                                ObLobQueryResult &result,
                                ObLobQueryIter *iter,
                                ObString& read_buf,
                                ObString &remain_data,
                                ObString &tmp_buf);
  int get_inrow_data(ObLobAccessParam& param, ObString& data);
  int get_ls_leader(ObLobAccessParam& param, const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObAddr &leader);
  int is_remote(ObLobAccessParam& param, bool& is_remote, common::ObAddr& dst_addr);
  int query_remote(ObLobAccessParam& param, common::ObAddr& dst_addr, ObString& data);
  int getlength_remote(ObLobAccessParam& param, common::ObAddr& dst_addr, uint64_t &len);
  int do_delete_one_piece(ObLobAccessParam& param, ObLobQueryResult &result, ObString &tmp_buff);
  int prepare_erase_buffer(ObLobAccessParam& param, ObString &tmp_buff);
  int fill_zero(char *ptr, uint64_t length, bool is_char,
                const ObCollationType coll_type, uint32_t byte_len, uint32_t byte_offset, uint32_t &char_len);
  int prepare_lob_common(ObLobAccessParam& param, bool &alloc_inside);
  bool lob_handle_has_char_len(ObLobAccessParam& param);
  int64_t* get_char_len_ptr(ObLobAccessParam& param);
  int fill_lob_locator_extern(ObLobAccessParam& param);

  int compare(ObLobAccessParam& param_left,
              ObLobAccessParam& param_right,
              int64_t& result);

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
