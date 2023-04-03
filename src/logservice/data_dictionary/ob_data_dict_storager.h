/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
*/

#ifndef OCEANBASE_DICT_SERVICE_DATA_DICTIONARY_STORAGER_
#define OCEANBASE_DICT_SERVICE_DATA_DICTIONARY_STORAGER_

#include "ob_data_dict_struct.h"
#include "ob_data_dict_persist_callback.h"
#include "logservice/ob_log_base_header.h"      // ObLogBaseHeader

namespace oceanbase
{
namespace logservice
{
class ObLogHandler;
}
namespace datadict
{

class ObDataDictStorage
{
public:
  ObDataDictStorage(ObIAllocator &allocator);
  ~ObDataDictStorage() { reset(); }
  void reset();
  void reuse();
public:
  int init(const uint64_t tenant_id);
  int prepare(const share::SCN &snapshot_scn, logservice::ObLogHandler *log_handler);
  // serialize dict_meta and generate meta_haeder
  // serialize meta_haeder and dict_meta to tmp_buf;
  // try memcpy tmp_buf to the inner_buf for palf if buf is enough.
  // submit_to_palf_ if buf is not enough and resuse buf for another loop
  template<class DATA_DICT_META>
  int handle_dict_meta(
      const DATA_DICT_META &data_dict_meta,
      ObDictMetaHeader &header);
  int finish(
      palf::LSN &start_lsn,
      palf::LSN &end_lsn,
      bool is_dump_success,
      bool &is_any_log_callback_fail,
      volatile bool &stop_flag);
public:
  // generate data_dict_meta for specified schemas, and serialize metas into buf, which is allocated
  // by specified allocator.
  // @param [in]  allocator        memory allocator used to alloc buf and data_dict_metas. should only
  //                               release memory after consume the buf.
  // @param [in]  tenant_schemas   tenant_schemas to handle.
  // @param [in]  database_schemas database_schemas to handle.
  // @param [in]  table_schemas    table_schemas to handle. shoule at least contains message neede by ObDictTableMeta.
  // @param [out] buf              buffer contains serialized data_dict_meta and will contains
  //                                "ddl_trans commit" if all schema_array are empty.
  // @param [out] buf_len          length of the buf, should usally times of 2M.
  // @param [out] pos              pos of serialized data_dict_meta, should be less than buf_len.
  static int gen_and_serialize_dict_metas(
      ObIAllocator &allocator,
      const ObIArray<const share::schema::ObTenantSchema*> &tenant_schemas,
      const ObIArray<const share::schema::ObDatabaseSchema*> &database_schemas,
      const ObIArray<const share::schema::ObTableSchema*> &table_schemas,
      char *&buf,
      int64_t &buf_len,
      int64_t &pos);
  // use parse_dict_metas API if all metas is serialized in the buf.
  static int parse_dict_metas(
      ObIAllocator &allocator,
      const char* buf,
      const int64_t buf_len,
      const int64_t pos,
      ObIArray<const ObDictTenantMeta*> &tenant_metas,
      ObIArray<const ObDictDatabaseMeta*> &database_metas,
      ObIArray<const ObDictTableMeta*> &table_metas);
protected:
  // protected only for unittest.
  virtual int submit_to_palf_();
private:
  int prepare_buf_();
  void reset_buf_();
  OB_INLINE bool need_new_palf_buf_(const int64_t required_size) const
  { return palf_buf_len_ - palf_pos_ < required_size; }
  int serialize_log_base_header_();
  int prepare_dict_buf_(const int64_t required_size);
  template<class DATA_DICT_META>
  int serialize_to_palf_buf_(
      const ObDictMetaHeader &header,
      const DATA_DICT_META &data_dict);

  int segment_dict_buf_to_palf_(ObDictMetaHeader &header);
  int alloc_palf_cb_(ObDataDictPersistCallback *&callback);
  int update_palf_lsn_(const palf::LSN &lsn);
  // @param bool is_any_cb_fail true if any callback failed.
  // @retval OB_SUCCESS all callback invoked or has any callback failed.
  // @revval OB_TIMEOUT timeout while waiting callback invoke.
  int wait_palf_callback_(bool &is_any_cb_fail, volatile bool &stop_flag);
  int check_callback_list_(
      bool &is_all_invoked,
      bool &has_cb_on_fail,
      bool &need_print_cb_status,
      volatile bool &stop_flag);
  void reset_cb_queue_();
private:
  static const int64_t DEFAULT_PALF_BUF_SIZE;
  static const int64_t DEFAULT_DICT_BUF_SIZE;
  static const char *DEFAULT_DDL_MDS_MSG;
  static const int64_t DEFAULT_DDL_MDS_MSG_LEN;
private:
  uint64_t tenant_id_;
  ObIAllocator &allocator_;
  share::SCN snapshot_scn_;
  palf::LSN start_lsn_;
  palf::LSN end_lsn_;
  logservice::ObLogHandler *log_handler_;
  logservice::ObLogBaseHeader log_base_header_;
  ObSpScLinkQueue cb_queue_;
  char *palf_buf_; // tmp buf for serialize and deserialize with palf
  char *dict_buf_; // dict_buf
  int64_t palf_buf_len_; // palf_buf_len
  int64_t dict_buf_len_; // dict_buf_len
  int64_t palf_pos_;
  int64_t dict_pos_;
  int64_t total_log_cnt_;
  int64_t total_dict_size_;
};
} // namespace datadict
} // namespace oceanbase
#endif
