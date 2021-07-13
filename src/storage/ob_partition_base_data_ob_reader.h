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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_BASE_DATA_OB_READER_H_
#define OCEANBASE_STORAGE_OB_PARTITION_BASE_DATA_OB_READER_H_

#include "share/ob_define.h"
#include "storage/ob_i_partition_base_data_reader.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/ob_i_table.h"
#include "storage/ob_ms_row_iterator.h"

namespace oceanbase {
namespace storage {
template <obrpc::ObRpcPacketCode RPC_CODE>
class ObStreamRpcReader {
public:
  ObStreamRpcReader();
  virtual ~ObStreamRpcReader()
  {}
  int init(common::ObInOutBandwidthThrottle& bandwidth_throttle);
  int fetch_next_buffer_if_need();
  int check_need_fetch_next_buffer(bool& need_fectch);
  int fetch_next_buffer();
  template <typename Data>
  int fetch_and_decode(Data& data);
  template <typename Data>
  int fetch_and_decode(common::ObIAllocator& allocator, Data& data);
  template <typename Data>
  int fetch_and_decode_list(common::ObIAllocator& allocator, common::ObIArray<Data>& data_list);
  template <typename Data>
  int fetch_and_decode_list(common::ObIArray<Data>& data_list);
  common::ObDataBuffer& get_rpc_buffer()
  {
    return rpc_buffer_;
  }
  const common::ObAddr& get_dst_addr() const
  {
    return handle_.get_dst_addr();
  }
  obrpc::ObPartitionServiceRpcProxy::SSHandle<RPC_CODE>& get_handle()
  {
    return handle_;
  }
  void reuse()
  {
    rpc_buffer_.get_position() = 0;
    rpc_buffer_parse_pos_ = 0;
  }

private:
  bool is_inited_;
  obrpc::ObPartitionServiceRpcProxy::SSHandle<RPC_CODE> handle_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  common::ObDataBuffer rpc_buffer_;
  int64_t rpc_buffer_parse_pos_;
  common::ObArenaAllocator allocator_;
  int64_t last_send_time_;
  int64_t data_size_;
};

template <obrpc::ObRpcPacketCode RPC_CODE>
class ObLogicStreamRpcReader {
public:
  ObLogicStreamRpcReader();
  virtual ~ObLogicStreamRpcReader()
  {}
  int init(common::ObInOutBandwidthThrottle& bandwidth_throttle, const bool has_lob = false);
  int fetch_next_buffer_if_need();
  int check_need_fetch_next_buffer(bool& need_fectch);
  int fetch_next_buffer();
  template <typename Data>
  int fetch_and_decode(Data& data);

  common::ObDataBuffer& get_rpc_buffer()
  {
    return rpc_buffer_;
  }
  const common::ObAddr& get_dst_addr() const
  {
    return handle_.get_dst_addr();
  }
  obrpc::ObPartitionServiceRpcProxy::SSHandle<RPC_CODE>& get_handle()
  {
    return handle_;
  }
  void reuse()
  {
    rpc_buffer_.get_position() = 0;
    rpc_buffer_parse_pos_ = 0;
  }
  const obrpc::ObLogicMigrateRpcHeader& get_rpc_header() const
  {
    return rpc_header_;
  }
  int decode_rpc_header();

private:
  bool is_inited_;
  obrpc::ObPartitionServiceRpcProxy::SSHandle<RPC_CODE> handle_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  common::ObDataBuffer rpc_buffer_;
  int64_t rpc_buffer_parse_pos_;
  common::ObArenaAllocator allocator_;
  int64_t last_send_time_;
  int64_t data_size_;
  int64_t object_count_;
  obrpc::ObLogicMigrateRpcHeader rpc_header_;
};

// 1.4x old rpc for get partition meta and sstable meta
// ObSavedStorageInfo is obtained through a separate rpc during init, and has nothing to do with the subsequent
// streaming interface. Access to meta is through the streaming interface Rpc sent: partition_key, version The received
// rpc: try to fill a package, partition_meta, sstable_meta, ObSSTablePair are guaranteed to be in the same package
// (OB_MALLOC_BIG_BLOCK_SIZE).
//  partition_meta
//  sstable_meta_count (N)
//  sstable_meta_1
//  ObSSTablePair list of sstable_1
//  sstable_meta_2
//  ObSSTablePair list of sstable_2
//  ...
//  sstbale_meta_N
//  ObSSTablePair list of sstable_N
class ObPartitionBaseDataMetaObReader : public ObIPhysicalBaseMetaReader {
public:
  ObPartitionBaseDataMetaObReader();
  virtual ~ObPartitionBaseDataMetaObReader();

  int init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      const ObAddr& addr, const common::ObPartitionKey& pkey, const ObVersion& version, const ObStoreType& store_type);
  int init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      const ObAddr& addr, const common::ObPartitionKey& pkey, const obrpc::ObFetchPhysicalBaseMetaArg& arg);
  int fetch_partition_meta(blocksstable::ObPartitionMeta& partition_meta, int64_t& sstable_count);
  int fetch_next_sstable_meta(
      blocksstable::ObSSTableBaseMeta& sstable_meta, common::ObIArray<blocksstable::ObSSTablePair>& macro_block_list);
  virtual int fetch_sstable_meta(blocksstable::ObSSTableBaseMeta& sstable_meta);
  virtual int fetch_macro_block_list(common::ObIArray<blocksstable::ObSSTablePair>& macro_block_list);
  virtual Type get_type() const
  {
    return BASE_DATA_META_OB_COMPAT_READER;
  }
  int64_t get_data_size() const
  {
    return data_size_;
  }

private:
  int fetch_next_buffer_if_need();

private:
  bool is_inited_;
  storage::ObSavedStorageInfo saved_storage_info_;
  obrpc::ObPartitionServiceRpcProxy::SSHandle<obrpc::OB_FETCH_BASE_DATA_META> handle_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  common::ObDataBuffer rpc_buffer_;
  int64_t rpc_buffer_parse_pos_;
  common::ObArenaAllocator allocator_;
  int64_t total_sstable_count_;
  int64_t fetched_sstable_count_;
  int64_t last_send_time_;
  int64_t data_size_;
  ObStoreType store_type_;
  ObITable::TableKey table_key_;
  common::ObArray<blocksstable::ObSSTablePair> macro_block_list_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBaseDataMetaObReader);
};

// Rpc sent:
//    List of macro blocks, ObFetchMacroBlockArgList
// The received rpc: try to fill a package as much as possible, and the meta is guaranteed to be in the same package
// (OB_MALLOC_BIG_BLOCK_SIZE).
//    macro_block_meta_1
//    macro_block_data_1
//    macro_block_meta_2
//    macro_block_data_2
//    ...
//    macro_block_meta_N
//    macro_block_data_N

class ObPartitionMacroBlockObReader : public ObIPartitionMacroBlockReader {
public:
  ObPartitionMacroBlockObReader();
  virtual ~ObPartitionMacroBlockObReader();
  int init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      const common::ObAddr& src_server, const storage::ObITable::TableKey table_key,
      const common::ObIArray<ObMigrateArgMacroBlockInfo>& list, const int64_t cluster_id);
  // read macro block from 1.4x
  int init_with_old_rpc(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy,
      common::ObInOutBandwidthThrottle& bandwidth_throttle, const common::ObAddr& src_server,
      const storage::ObITable::TableKey table_key, const common::ObIArray<ObMigrateArgMacroBlockInfo>& list);
  virtual int get_next_macro_block(blocksstable::ObFullMacroBlockMeta& meta, blocksstable::ObBufferReader& data,
      blocksstable::MacroBlockId& src_macro_id);
  virtual Type get_type() const
  {
    return MACRO_BLOCK_OB_READER;
  }
  virtual int64_t get_data_size() const
  {
    return data_size_;
  }

private:
  int alloc_buffers();
  int fetch_next_buffer_if_need();
  int fetch_next_buffer();
  int fetch_next_buffer_with_old_rpc();
  int alloc_from_memctx_first(char*& buf);

private:
  bool is_inited_;
  obrpc::ObPartitionServiceRpcProxy::SSHandle<obrpc::OB_FETCH_MACRO_BLOCK> handle_;
  obrpc::ObPartitionServiceRpcProxy::SSHandle<obrpc::OB_FETCH_MACRO_BLOCK_OLD> old_handle_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  blocksstable::ObBufferReader data_buffer_;  // Data used to assemble macroblocks
  common::ObDataBuffer rpc_buffer_;
  int64_t rpc_buffer_parse_pos_;
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator meta_allocator_;
  common::ObMacroBlockSizeMemoryContext macro_block_mem_context_;
  int64_t last_send_time_;
  int64_t data_size_;
  bool use_old_rpc_;  // use 1.4x rpc
  DISALLOW_COPY_AND_ASSIGN(ObPartitionMacroBlockObReader);
};

// This producer is responsible for the meta and data data of the macroblock, and try to fill a package as much as
// possible. The logic of filling the package is completed in ObFetchMacroBlockP::process. Try to fill a package as much
// as possible, and the meta is guaranteed to be in the same package (OB_MALLOC_BIG_BLOCK_SIZE).
class ObPartitionMacroBlockObProducer {
public:
  ObPartitionMacroBlockObProducer();
  virtual ~ObPartitionMacroBlockObProducer();

  int init(const ObITable::TableKey& table_key, const common::ObIArray<obrpc::ObFetchMacroBlockArg>& arg_list);
  int get_next_macro_block(blocksstable::ObFullMacroBlockMeta& meta, blocksstable::ObBufferReader& data);

private:
  int prefetch();
  int get_macro_read_info(const obrpc::ObFetchMacroBlockArg& arg, blocksstable::ObMacroBlockCtx& macro_block_ctx,
      blocksstable::ObMacroBlockReadInfo& read_info);

private:
  static const int64_t MAX_PREFETCH_MACRO_BLOCK_NUM = 2;
  static const int64_t MACRO_META_RESERVE_TIME = 60 * 1000 * 1000LL;  // 1minutes

  bool is_inited_;
  common::ObArray<obrpc::ObFetchMacroBlockArg> macro_list_;
  int64_t macro_idx_;
  blocksstable::ObMacroBlockHandle read_handle_[MAX_PREFETCH_MACRO_BLOCK_NUM];
  int64_t handle_idx_;
  int64_t prefetch_meta_time_;
  blocksstable::ObFullMacroBlockMeta prefetch_meta_;
  ObTableHandle store_handle_;
  ObSSTable* sstable_;
  blocksstable::ObStorageFileHandle file_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionMacroBlockObProducer);
};

// logic_table_meta
// end_key_count(n)
// end_key(1)
// end_key(2)
// ...
// end_key(n) should be MAX
class ObLogicBaseMetaProducer {
public:
  ObLogicBaseMetaProducer() : is_inited_(false), partition_service_(NULL), arg_(NULL)
  {}
  ~ObLogicBaseMetaProducer()
  {}

  int init(storage::ObPartitionService* partition_service, obrpc::ObFetchLogicBaseMetaArg* arg);
  int get_logic_endkey_list(common::ObIArray<common::ObStoreRowkey>& end_key_list);

private:
  bool is_inited_;
  storage::ObPartitionService* partition_service_;
  obrpc::ObFetchLogicBaseMetaArg* arg_;
};

class ObLogicBaseMetaReader : ObILogicBaseMetaReader {
public:
  ObLogicBaseMetaReader() : is_inited_(false), rpc_reader_(), allocator_(common::ObNewModIds::OB_PARTITION_MIGRATE)
  {}
  virtual ~ObLogicBaseMetaReader()
  {}

  int init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      const common::ObAddr& addr, const obrpc::ObFetchLogicBaseMetaArg& arg, const int64_t cluster_id);

  virtual int fetch_end_key_list(common::ObIArray<common::ObStoreRowkey>& end_key_list);

private:
  bool is_inited_;
  ObStreamRpcReader<obrpc::OB_FETCH_LOGIC_BASE_META> rpc_reader_;
  common::ObArenaAllocator allocator_;
};

class ObLogicDataChecksumCalculate {
public:
  ObLogicDataChecksumCalculate();
  virtual ~ObLogicDataChecksumCalculate()
  {}
  int init(const int64_t schema_rowkey_count, const int64_t data_checksum);
  int append_row(const ObStoreRow* store_row);
  int64_t get_data_checksum() const
  {
    return data_checksum_;
  }
  int64_t get_first_compacted_row_count() const
  {
    return first_compacted_row_count_;
  }
  ObStoreRowkey& get_last_rowkey()
  {
    return last_rowkey_;
  }

private:
  int inner_append_row(const ObStoreRow& store_row);
  void calc_data_checksum();

private:
  bool is_inited_;
  blocksstable::ObSelfBufferWriter data_buffer_;
  blocksstable::ObRowWriter row_writer_;
  ObMemBuf last_rowkey_buf_;
  ObStoreRowkey last_rowkey_;
  int64_t data_checksum_;
  int64_t schema_rowkey_count_;
  int64_t first_compacted_row_count_;
};

class ObLogicRowProducer {
public:
  ObLogicRowProducer();
  virtual ~ObLogicRowProducer()
  {}
  int init(storage::ObPartitionService* partition_service, obrpc::ObFetchLogicRowArg* arg);
  int get_next_row(const ObStoreRow*& store_row);
  int64_t get_data_checksum() const
  {
    return data_checksum_calc_.get_data_checksum();
  }
  int reset_logical_row_iter();
  bool need_reset_logic_row_iter()
  {
    return can_reset_row_iter_;
  }
  int get_schema_rowkey_count(int64_t& schema_rowkey_count);

private:
  int init_logical_row_iter(const ObVersionRange& version_range, ObExtStoreRange& key_range);
  bool can_reset_logical_row_iter();
  int inner_get_next_row(const ObStoreRow*& store_row);
  int append_row(const ObStoreRow& store_row);
  void calc_data_checksum();
  int set_new_key_range(common::ObStoreRange& new_key_range);
  int check_split_source_table(const ObTablesHandle& tables_handle_, bool& is_exist);

private:
  bool is_inited_;
  ObLogicDataChecksumCalculate data_checksum_calc_;
  storage::ObTablesHandle tables_handle_;
  ObMemBuf start_key_buf_;
  ObExtStoreRange range_;
  ObPGPartitionGuard guard_;
  common::ObArenaAllocator allocator_;
  storage::ObPartitionStorage* storage_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  const share::schema::ObTableSchema* table_schema_;
  memtable::ObMemtable* memtable_;
  obrpc::ObFetchLogicRowArg arg_;
  ObMSRowIterator ms_row_iterator_;
  bool need_check_memtable_;
  bool can_reset_row_iter_;
  memtable::ObIMemtableCtxFactory* mem_ctx_factory_;
  int64_t start_time_;
  int64_t end_time_;
  ObIPartitionGroupGuard pg_guard_;
  ObPartitionKey memtable_pkey_;
};

class ObPhysicalBaseMetaProducer {
public:
  ObPhysicalBaseMetaProducer() : is_inited_(false), partition_service_(NULL), arg_(NULL)
  {}
  ~ObPhysicalBaseMetaProducer()
  {}
  int init(storage::ObPartitionService* partition_service, obrpc::ObFetchPhysicalBaseMetaArg* arg);
  int get_sstable_meta(
      blocksstable::ObSSTableBaseMeta& sstable_meta, common::ObIArray<blocksstable::ObSSTablePair>& pair_list);

private:
  bool is_inited_;
  storage::ObPartitionService* partition_service_;
  obrpc::ObFetchPhysicalBaseMetaArg* arg_;
};

class ObPhysicalBaseMetaReader : public ObIPhysicalBaseMetaReader {
public:
  ObPhysicalBaseMetaReader() : is_inited_(false), rpc_reader_(), allocator_(common::ObNewModIds::OB_PARTITION_MIGRATE)
  {}
  virtual ~ObPhysicalBaseMetaReader()
  {}

  int init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      const common::ObAddr& src_server, const obrpc::ObFetchPhysicalBaseMetaArg& arg, const int64_t cluster_id);

  virtual int fetch_sstable_meta(blocksstable::ObSSTableBaseMeta& sstable_meta);
  virtual int fetch_macro_block_list(common::ObIArray<blocksstable::ObSSTablePair>& macro_block_list);
  virtual Type get_type() const
  {
    return BASE_DATA_META_OB_READER;
  }

private:
  bool is_inited_;
  ObStreamRpcReader<obrpc::OB_FETCH_PHYSICAL_BASE_META> rpc_reader_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalBaseMetaReader);
};

class ObLogicRowReader : public ObIStoreRowIterator {
public:
  ObLogicRowReader() : is_inited_(false), rpc_reader_(), arg_(NULL), store_row_()
  {}
  virtual ~ObLogicRowReader()
  {}

  int init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      const common::ObAddr& src_server, const obrpc::ObFetchLogicRowArg& arg);

  // the store row is valid before the next call of get_next_row
  virtual int get_next_row(const ObStoreRow*& row) override;

private:
  bool is_inited_;
  ObStreamRpcReader<obrpc::OB_FETCH_LOGIC_ROW> rpc_reader_;
  const obrpc::ObFetchLogicRowArg* arg_;
  ObStoreRow store_row_;
  ObObj buff_obj_[OB_ROW_MAX_COLUMNS_COUNT];
  DISALLOW_COPY_AND_ASSIGN(ObLogicRowReader);
};

class ObLogicRowSliceReader : public ObIStoreRowIterator {
public:
  ObLogicRowSliceReader();
  virtual ~ObLogicRowSliceReader()
  {}

  int init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      const common::ObAddr& src_server, const obrpc::ObFetchLogicRowArg& arg, const int64_t cluster_id);

  // the store row is valid before the next call of get_next_row
  virtual int get_next_row(const ObStoreRow*& row) override;

private:
  int rescan(const ObStoreRow*& row);
  int copy_rowkey();
  int inner_get_next_row(const ObStoreRow*& row);
  int fetch_logic_row(const obrpc::ObFetchLogicRowArg& rpc_arg);

private:
  bool is_inited_;
  obrpc::ObPartitionServiceRpcProxy* srv_rpc_proxy_;
  ObAddr src_server_;
  ObLogicStreamRpcReader<obrpc::OB_FETCH_LOGIC_ROW_SLICE> rpc_reader_;
  const obrpc::ObFetchLogicRowArg* arg_;
  ObStoreRow store_row_;
  ObObj buff_obj_[OB_ROW_MAX_COLUMNS_COUNT];
  uint16_t column_ids_[OB_ROW_MAX_COLUMNS_COUNT];
  common::ObMemBuf last_key_buf_;
  common::ObStoreRowkey last_key_;
  int64_t schema_rowkey_cnt_;
  int64_t cluster_id_;
  DISALLOW_COPY_AND_ASSIGN(ObLogicRowSliceReader);
};

class ObILogicRowIterator {
public:
  ObILogicRowIterator()
  {}
  virtual ~ObILogicRowIterator()
  {}
  virtual int get_next_row(const ObStoreRow*& store_row) = 0;
  virtual const obrpc::ObFetchLogicRowArg* get_fetch_logic_row_arg() = 0;
};

class ObLogicRowFetcher : public ObILogicRowIterator {
public:
  ObLogicRowFetcher();
  virtual ~ObLogicRowFetcher()
  {}
  int init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      const common::ObAddr& src_server, const obrpc::ObFetchLogicRowArg& arg, const int64_t cluster_id);
  int get_next_row(const ObStoreRow*& store_row);
  const obrpc::ObFetchLogicRowArg* get_fetch_logic_row_arg()
  {
    return arg_;
  }

private:
  bool is_inited_;
  ObLogicRowReader logic_row_reader_;
  ObLogicRowSliceReader slice_row_reader_;
  ObIStoreRowIterator* row_reader_;
  const obrpc::ObFetchLogicRowArg* arg_;
};

class ObTailoredRowIterator : public ObILogicRowIterator {
public:
  ObTailoredRowIterator();
  virtual ~ObTailoredRowIterator()
  {}
  int init(const uint64_t index_id, const ObPartitionKey& pg_key, const int64_t schema_version,
      const ObITable::TableKey& table_key, ObTablesHandle& handle);
  virtual int get_next_row(const ObStoreRow*& store_row);
  virtual const obrpc::ObFetchLogicRowArg* get_fetch_logic_row_arg()
  {
    return &arg_;
  }

private:
  bool is_inited_;
  ObMSRowIterator row_iter_;
  ObArenaAllocator allocator_;
  obrpc::ObFetchLogicRowArg arg_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  const share::schema::ObTableSchema* table_schema_;
  int64_t snapshot_version_;
};

class ObLogicDataChecksumReader {
public:
  ObLogicDataChecksumReader();
  virtual ~ObLogicDataChecksumReader()
  {}

  int init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      const common::ObAddr& src_server, const int64_t cluster_id, const obrpc::ObFetchLogicRowArg& arg);
  int get_data_checksum(int64_t& data_checksum);

private:
  int get_data_checksum_with_slice(int64_t& data_checksum);
  int get_data_checksum_without_slice(int64_t& data_checksum);
  int fetch_logic_data_checksum(const obrpc::ObFetchLogicRowArg& arg,
      ObLogicStreamRpcReader<obrpc::OB_FETCH_LOGIC_DATA_CHECKSUM_SLICE>& rpc_reader);
  int inner_get_data_checksum(
      int64_t& data_checksum, ObLogicStreamRpcReader<obrpc::OB_FETCH_LOGIC_DATA_CHECKSUM_SLICE>& rpc_reader);
  int rescan(int64_t& data_cheksum, ObLogicStreamRpcReader<obrpc::OB_FETCH_LOGIC_DATA_CHECKSUM_SLICE>& rpc_reader);

private:
  static const int64_t TIMEOUT = 30 * 1000 * 1000;  // 30s
  bool is_inited_;
  common::ObMemBuf last_key_buf_;
  common::ObStoreRowkey last_key_;
  common::ObAddr src_server_;
  obrpc::ObPartitionServiceRpcProxy* srv_rpc_proxy_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  const obrpc::ObFetchLogicRowArg* arg_;
  int64_t cluster_id_;
};

// This producer is responsible for the meta data related to the streaming interface, trying to fill a package as much
// as possible. The logic of filling the package is completed in ObFetchBaseDataMetaP::process. partition_group meta,
// all partition_meta, all_table_ids must ensure in one package(OB_MALLOC_BIG_BLOCK_SIZE)
class ObPGPartitionBaseDataMetaObProducer {
public:
  ObPGPartitionBaseDataMetaObProducer();
  virtual ~ObPGPartitionBaseDataMetaObProducer();
  int init(const ObPGKey& pg_key, const int64_t snapshot_version, const bool is_only_major_sstable,
      const int64_t log_ts, storage::ObPartitionService* partition_serivce);
  int get_next_partition_meta_info(const obrpc::ObPGPartitionMetaInfo*& pg_partition_meta_info);

private:
  int set_all_partition_meta_info(
      const int64_t snapshot_version, const bool is_only_major_sstable, const int64_t log_ts, ObPGStorage* pg_storage);
  int set_partition_meta_info(const ObPartitionKey& pkey, const int64_t snapshot_version,
      const bool is_only_major_sstable, const int64_t log_ts, ObPGStorage* pg_storage,
      obrpc::ObPGPartitionMetaInfo& meta_info);
  int set_table_info(const ObPartitionKey& pkey, const uint64_t table_id, const int64_t multi_version_start,
      const bool is_only_major_sstable, const int64_t log_ts, ObPGStorage* pg_storage,
      obrpc::ObFetchTableInfoResult& table_info);

private:
  bool is_inited_;
  common::ObArray<obrpc::ObPGPartitionMetaInfo> pg_partition_meta_info_array_;
  int64_t meta_info_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObPGPartitionBaseDataMetaObProducer);
};

class ObPGPartitionBaseDataMetaObReader : public ObIPGPartitionBaseDataMetaObReader {
public:
  ObPGPartitionBaseDataMetaObReader();
  virtual ~ObPGPartitionBaseDataMetaObReader();

  int init(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      const ObAddr& addr, const obrpc::ObFetchPGPartitionInfoArg& rpc_arg, const int64_t cluster_id);
  int fetch_pg_partition_meta_info(obrpc::ObPGPartitionMetaInfo& partition_meta_info);
  virtual Type get_type() const
  {
    return BASE_DATA_META_OB_READER;
  }

private:
  static const int64_t FETCH_BASE_META_TIMEOUT = 60 * 1000 * 1000;  // 60s
  bool is_inited_;
  ObStreamRpcReader<obrpc::OB_FETCH_PG_PARTITION_INFO> rpc_reader_;
  DISALLOW_COPY_AND_ASSIGN(ObPGPartitionBaseDataMetaObReader);
};

template <obrpc::ObRpcPacketCode RPC_CODE>
ObStreamRpcReader<RPC_CODE>::ObStreamRpcReader()
    : is_inited_(false),
      bandwidth_throttle_(NULL),
      rpc_buffer_(),
      rpc_buffer_parse_pos_(0),
      allocator_(ObNewModIds::OB_PARTITION_MIGRATE),
      last_send_time_(0),
      data_size_(0)
{}

}  // end namespace storage
}  // end namespace oceanbase
#endif /* OCEANBASE_STORAGE_OB_PARTITION_BASE_DATA_OB_READER_H_ */
