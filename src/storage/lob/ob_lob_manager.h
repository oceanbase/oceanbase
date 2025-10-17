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
#include "storage/lob/ob_lob_access_param.h"
#include "storage/lob/ob_lob_meta.h"
#include "storage/lob/ob_lob_piece.h"
#include "storage/lob/ob_lob_cursor.h"
#include "storage/lob/ob_lob_constants.h"
#include "storage/lob/ob_lob_iterator.h"
#include "storage/lob/ob_lob_meta_manager.h"
#include "storage/ob_storage_rpc.h"
#include "share/throttle/ob_share_resource_throttle_tool.h"
#include "share/allocator/ob_lob_ext_info_log_allocator.h"

namespace oceanbase
{
namespace storage
{

class ObLobDiskLocatorBuilder;
class ObLobDataInsertTask;

struct ObLobCtx
{
  ObLobCtx() : lob_meta_mngr_(nullptr), lob_piece_mngr_(nullptr) {}
  ObLobMetaManager* lob_meta_mngr_;
  ObLobPieceManager* lob_piece_mngr_;
  TO_STRING_KV(KPC(lob_meta_mngr_), KPC(lob_piece_mngr_));
};

class ObLobManager
{
public:
  static const int64_t LOB_AUX_TABLE_COUNT = 2; // lob aux table count for each table
  static const int64_t LOB_WITH_OUTROW_CTX_SIZE = sizeof(ObLobCommon) + sizeof(ObLobData) + sizeof(ObLobDataOutRowCtx);
  static const int64_t LOB_OUTROW_FULL_SIZE = ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE;
  static const uint64_t LOB_READ_BUFFER_LEN = 1024L*1024L; // 1M
  static const ObLobCommon ZERO_LOB; // static empty lob for zero val
private:
  explicit ObLobManager(const uint64_t tenant_id)
    : tenant_id_(tenant_id),
      is_inited_(false),
      allocator_(tenant_id),
      lob_ctx_(),
      meta_manager_(tenant_id),
      piece_manager_(tenant_id)
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
  static int fill_lob_header(ObIAllocator &allocator, ObStorageDatum &datum);
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

  // Tmp Delta Lob locator interface
  int process_delta(ObLobAccessParam& param,
                    ObLobLocatorV2& lob_locator);

  // Lob data interface
  int append(ObLobAccessParam& param,
             ObString& data);
  int append(ObLobAccessParam& param,
             ObLobLocatorV2& lob,
             ObLobMetaWriteIter &iter);
  int append(ObLobAccessParam& param,
             ObLobLocatorV2 &lob);
  int query(ObLobAccessParam& param,
            ObString& data);
  int query(ObLobAccessParam& param,
            ObLobQueryIter *&result);

  int query(ObString& data,
            ObLobQueryIter *&result);
  int query(
      ObIAllocator *allocator,
      ObLobLocatorV2 &locator,
      int64_t query_timeout_ts,
      bool is_load_all,
      ObLobPartialData *partial_data,
      ObLobCursor *&cursor);

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
  int equal(ObLobLocatorV2& lob_left,
            ObLobLocatorV2& lob_right,
            ObLobCompareParams& cmp_params,
            bool& result);
  // int insert(const common::ObTabletID &tablet_id, ObObj *obj, uint64_t offset, char *data, uint64_t len);
  // int erase(const common::ObTabletID &tablet_id, ObObj *obj, uint64_t offset, uint64_t len);
  int erase(ObLobAccessParam& param);
  int getlength(ObLobAccessParam& param, uint64_t &len);
  int build_lob_param(ObLobAccessParam& param,
                      ObIAllocator &allocator,
                      ObCollationType coll_type,
                      uint64_t offset,
                      uint64_t len,
                      int64_t timeout,
                      ObLobLocatorV2 &lob);

  common::ObIAllocator& get_ext_info_log_allocator() { return ext_info_log_allocator_; }
  share::LobExtInfoLogThrottleTool& get_ext_info_log_throttle_tool() { return throttle_tool_; }
  inline bool can_write_inrow(uint64_t len, int64_t inrow_threshold) { return len <= inrow_threshold; }

  static void transform_lob_id(uint64_t src, uint64_t &dst);
  int insert(ObLobAccessParam& param, const ObLobLocatorV2 &src_data_locator, ObArray<ObLobMetaInfo> &lob_meta_list);
  int prepare_insert_task(
      ObLobAccessParam& param,
      bool &is_outrow,
      ObLobDataInsertTask &task);

private:
  // private function
  int write_inrow_inner(ObLobAccessParam& param, ObString& data, ObString& old_data);
  int write_inrow(ObLobAccessParam& param, ObLobLocatorV2& lob, uint64_t offset, ObString& old_data);
  int write_outrow(ObLobAccessParam& param, ObLobLocatorV2& lob, uint64_t offset, ObString& old_data);

  int query_inrow_get_iter(ObLobAccessParam& param, ObString &data, uint32_t offset, bool scan_backward, ObLobQueryIter *&result);
  int erase_outrow(ObLobAccessParam& param);
  int check_need_out_row(ObLobAccessParam& param,
                         int64_t add_len,
                         ObString &data,
                         bool need_combine_data,
                         bool alloc_inside,
                         bool &need_out_row);
  int prepare_for_write(ObLobAccessParam& param,
                        ObString &old_data,
                        bool &need_out_row);

  int fill_zero(char *ptr, uint64_t length, bool is_char,
                const ObCollationType coll_type, uint32_t byte_len, uint32_t byte_offset, uint32_t &char_len);
  int prepare_lob_common(ObLobAccessParam& param, bool &alloc_inside);
  int fill_lob_locator_extern(ObLobAccessParam& param);

  int compare(ObLobAccessParam& param_left,
              ObLobAccessParam& param_right,
              int64_t& result);
  int load_all(ObLobAccessParam &param, ObLobPartialData &partial_data);
  int append_outrow(ObLobAccessParam& param, ObLobLocatorV2& lob, int64_t append_lob_len, ObString& ori_inrow_data);
  int append_outrow(ObLobAccessParam& param, bool ori_is_inrow, ObString &data);

  int query_outrow(ObLobAccessParam& param, ObLobQueryIter *&result);
  int query_outrow(ObLobAccessParam& param, ObString &data);
  int process_diff(ObLobAccessParam& param, ObLobLocatorV2& lob_locator, ObLobDiffHeader *diff_header);
  int prepare_outrow_locator(ObLobAccessParam& param, ObLobDataInsertTask &task);
  int prepare_char_len(ObLobAccessParam& param, ObLobDiskLocatorBuilder &locator_builder, ObLobDataInsertTask &task);
  int prepare_lob_id(ObLobAccessParam& param, ObLobDiskLocatorBuilder &locator_builder);
  int alloc_lob_id(ObLobAccessParam& param, ObLobId &lob_id);
  int prepare_seq_no(ObLobAccessParam& param, ObLobDiskLocatorBuilder &locator_builder, ObLobDataInsertTask &task);
private:
  const uint64_t tenant_id_;
  bool is_inited_;
  common::ObFIFOAllocator allocator_;
  // global ctx
  ObLobCtx lob_ctx_;
  ObLobMetaManager meta_manager_;
  ObLobPieceManager piece_manager_;
  share::LobExtInfoLogThrottleTool throttle_tool_;
  share::ObLobExtInfoLogAllocator ext_info_log_allocator_;
};

} // storage
} // oceanbase

#endif
