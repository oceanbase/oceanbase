/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_MINER_RECORD_H_
#define OCEANBASE_LOG_MINER_RECORD_H_

#include "lib/string/ob_string_buffer.h"
#include "lib/container/ob_se_array.h"
#include "lib/worker.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "storage/tx/ob_trans_define.h"

#include "ob_log_miner_br.h"
#include "ob_log_miner_recyclable_task.h"
#include "ob_log_miner_utils.h"

namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerBR;

typedef ObSEArray<ObString, 4> KeyArray;
class ObLogMinerRecord: public ObLogMinerRecyclableTask
{
public:
  ObLogMinerRecord();
	explicit ObLogMinerRecord(ObIAllocator *alloc);
	~ObLogMinerRecord();
    int init(ObLogMinerBR &logminer_br);
	void set_allocator(ObIAllocator *alloc);
	void destroy();
	void reset();

	bool is_inited() { return is_inited_; }

	lib::Worker::CompatMode get_compat_mode() const {
		return compat_mode_;
	}

	bool is_mysql_compat_mode() const {
		return lib::Worker::CompatMode::MYSQL == compat_mode_;
	}

	bool is_oracle_compat_mode() const {
		return lib::Worker::CompatMode::ORACLE == compat_mode_;
	}

	uint64_t get_tenant_id() const {
		return tenant_id_;
	}

	int64_t get_cluster_id() const {
		return orig_cluster_id_;
	}

	const TenantName& get_tenant_name() const {
		return tenant_name_;
	}

	const DbName& get_database_name() const {
		return database_name_;
	}

	const TableName& get_table_name() const {
		return table_name_;
	}

	const transaction::ObTransID& get_ob_trans_id() const {
		return trans_id_;
	}

	const KeyArray& get_primary_keys() const {
		return primary_keys_;
	}

	const KeyArray& get_unique_keys() const {
		return unique_keys_;
	}

	const ObString& get_row_unique_id() const {
		return row_unique_id_;
	}

	RecordType get_record_type() const {
		return record_type_;
	}

	share::SCN get_commit_scn() const {
		return commit_scn_;
	}

	const ObStringBuffer& get_redo_stmt() const {
		return redo_stmt_;
	}

	const ObStringBuffer& get_undo_stmt() const {
		return undo_stmt_;
	}

	int build_stmts(ObLogMinerBR &br);

	bool is_dml_record() const;
	bool is_ddl_record() const;

	void copy_base_info(const ObLogMinerRecord &other);

	TO_STRING_KV(
		K(is_inited_),
		K(is_filtered_),
		K(compat_mode_),
		K(tenant_id_),
		K(tenant_name_),
		K(database_name_),
		K(table_name_),
		K(trans_id_),
		K(primary_keys_),
		K(unique_keys_),
		K(record_type_),
		K(commit_scn_),
		"redo_stmt_len", redo_stmt_.length(),
		"undo_stmt_len", undo_stmt_.length()
	);

private:
	void free_keys_(KeyArray &arr);

	void free_row_unique_id_();

	int copy_string_to_array_(const ObString &str, KeyArray &array);

	int fill_row_unique_id_(ICDCRecord &cdc_record);

	int fill_data_record_fields_(ICDCRecord &cdc_record);

	int fill_tenant_db_name_(const char *tenant_db_name);

	int fill_primary_keys_(ITableMeta &tbl_meta);

	int fill_unique_keys_(ITableMeta &tbl_meta);

	int fill_keys_(const char *key_cstr, KeyArray &key_arr);

	int build_ddl_stmt_(ICDCRecord &cdc_record);

	int build_dml_stmt_(ICDCRecord &cdc_record);

	int build_insert_stmt_(ObStringBuffer &stmt,
			binlogBuf *new_cols,
			const unsigned int new_col_cnt,
			ITableMeta *tbl_meta);

	int build_insert_stmt_(ObStringBuffer &stmt,
			binlogBuf *new_cols,
			const unsigned int new_col_cnt,
			ITableMeta *tbl_meta,
			bool &has_lob_null);

	int build_update_stmt_(ObStringBuffer &stmt,
			binlogBuf *new_cols,
			const unsigned int new_col_cnt,
			binlogBuf *old_cols,
			const unsigned int old_col_cnt,
			ITableMeta *tbl_meta,
			bool &has_lob_null,
			bool &has_unsupport_type_compare);

	int build_delete_stmt_(ObStringBuffer &stmt,
			binlogBuf *old_cols,
			const unsigned int old_col_cnt,
			ITableMeta *tbl_meta,
			bool &has_lob_null,
			bool &has_unsupport_type_compare);

	int build_column_value_(ObStringBuffer &stmt,
			IColMeta *col_meta,
			binlogBuf &col_data);

	int build_where_conds_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_cnt,
		ITableMeta *tbl_meta,
		bool &has_lob_null,
		bool &has_unsupport_type_compare);

	int build_key_conds_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_cnt,
		ITableMeta *tbl_meta,
		const KeyArray &key,
		bool &has_lob_null,
		bool &has_unsupport_type_compare);

	int build_cond_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_idx,
		ITableMeta *tbl_meta,
		IColMeta *col_meta,
		bool &has_lob_null,
		bool &has_unsupport_type_compare);

	int build_lob_cond_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_idx,
		ITableMeta *tbl_meta,
		IColMeta *col_meta,
		bool &has_lob_null,
		bool &has_unsupport_type_compare);

	int build_func_cond_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_idx,
		ITableMeta *tbl_meta,
		IColMeta *col_meta,
		const char *func_name);

	int build_normal_cond_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_idx,
		ITableMeta *tbl_meta,
		IColMeta *col_meta);

	int build_hex_val_(ObStringBuffer &stmt,
			binlogBuf &col_data);

	int build_escape_char_(ObStringBuffer &stmt);
	bool is_string_type_(IColMeta *col_meta) const;

	bool is_binary_type_(IColMeta *col_meta) const;

	bool is_number_type_(IColMeta *col_meta) const;

	bool is_lob_type_(IColMeta *col_meta) const;

	bool is_geo_type_(IColMeta *col_meta) const;

	bool is_bit_type_(IColMeta *col_meta) const;

private:
    static const char *ORACLE_ESCAPE_CHAR;
	static const char *MYSQL_ESCAPE_CHAR;
	static const char *ORA_GEO_PREFIX;
	static const char *JSON_EQUAL;
	static const char *LOB_COMPARE;
	static const char *ST_EQUALS;

	bool 					is_inited_;
	bool					is_filtered_;
	ObIAllocator			*alloc_;

	lib::Worker::CompatMode	compat_mode_;
	uint64_t		        tenant_id_;
	int64_t					orig_cluster_id_;
	TenantName				tenant_name_;
	DbName					database_name_;
	TableName		        table_name_;
	transaction::ObTransID	trans_id_;
	KeyArray				primary_keys_;
	KeyArray				unique_keys_;
	ObString				row_unique_id_;
	RecordType				record_type_;
	share::SCN 				commit_scn_;
	ObStringBuffer 			redo_stmt_;
	ObStringBuffer 			undo_stmt_;
};
}
}

#endif