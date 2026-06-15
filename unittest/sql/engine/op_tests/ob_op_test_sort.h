/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SORT_H_
 #define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SORT_H_

 #include "unittest/sql/engine/op_tests/ob_op_test_base.h"
 #include "sql/engine/sort/ob_sort_op.h"
 #include "sql/engine/sort/ob_sort_vec_op.h"
 #include "sql/engine/sort/ob_sort_basic_info.h"
 #include "sql/code_generator/ob_static_engine_expr_cg.h"
 #include "sql/ob_sql_utils.h"

 namespace oceanbase
 {
 namespace sql
 {

 /**
  * @brief SortTestSpec - Test specification for Sort operator.
  *
  * Usage:
  *   SortTestSpec spec;
  *   spec.table("t", "pk int, sk int, val int")
  *       .select("pk, sk, val")
  *       .with_partition_sort(1)  // 1 partition column
  *       .order_by("sk ASC")
  *       .with_data({{1, 3, 100}, {1, 1, 101}, {2, 2, 200}})
  *       .run(engine);
  *
  * Advanced usage:
  *   // Full sort without partition
  *   spec.with_encode_sortkey(true).order_by("a ASC, b DESC").run(engine);
  *
  *   // TopN sort
  *   spec.with_topn(10).run(engine);
  */
 class SortTestSpec : public OpSpecBuilder<SortTestSpec>
 {
 public:
   SortTestSpec()
     : is_partition_sort_(false),
       part_cnt_(0),
       enable_encode_sortkey_(true),
       is_topn_sort_(false),
       topn_cnt_(INT64_MAX),
       prefix_pos_(0),
       has_addon_(false),
       compress_type_(common::ObCompressorType::NONE_COMPRESSOR),
       hash_raw_expr_(nullptr),
       encode_sortkey_raw_expr_(nullptr)
   {}
   ~SortTestSpec() = default;

   // ===== Partition Sort Configuration =====

   /**
    * @brief Enable partition sort mode with partition count.
    * @param part_cnt Number of partition columns (PARTITION BY columns)
    */
   SortTestSpec& with_partition_sort(int64_t part_cnt)
   {
     is_partition_sort_ = true;
     part_cnt_ = part_cnt;
     return *this;
   }

   // ===== Pre-expression-generation hook =====

   // Mirror CG (ob_static_engine_cg.cpp:2861-2864): partition / topn / topk sort
   // all force encode sortkey off
   // (`part_cnt > 0 || topn_expr != null || topk_limit_expr != null
   //   -> enable_encode_sortkey_opt = false`).
   // Guard the framework so a partition+encode or topn+encode request can never
   // build a spec that CG would never produce.
   bool effective_encode_sortkey() const
   {
     return enable_encode_sortkey_ && !is_topn_sort_;
   }

   /**
    * @brief Prepare hash expression for partition sort before code generation.
    * This creates the T_FUN_SYS_HASH expression that computes hash values from partition keys.
    */
   int prepare_raw_exprs(ObDMLStmt *stmt, OpTestEngine &engine)
   {
     int ret = OB_SUCCESS;
     if (OB_ISNULL(stmt) || !stmt->is_select_stmt()) {
       return OB_SUCCESS;
     }

     ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
     common::ObIArray<OrderItem> &order_items = select_stmt->get_order_items();
     if (part_cnt_ > 0 && OB_ISNULL(hash_raw_expr_)) {
       if (order_items.count() < part_cnt_) {
         LOG_WARN("partition sort requires order items >= part_cnt", K(part_cnt_), K(order_items.count()));
       } else {
         // Build hash(partition_keys...) as the first sort key:
         // [hash(part keys), part keys..., order keys...]
         ObOpRawExpr *hash_expr = nullptr;
         if (OB_FAIL(engine.expr_factory_.create_raw_expr(T_FUN_SYS_HASH, hash_expr))) {
           LOG_WARN("create hash raw expr failed", K(ret));
         } else if (OB_ISNULL(hash_expr)) {
           ret = OB_ERR_UNEXPECTED;
           LOG_WARN("hash_expr is null", K(ret));
         } else if (OB_FAIL(hash_expr->init_param_exprs(part_cnt_))) {
           LOG_WARN("init hash param exprs failed", K(ret), K(part_cnt_));
         } else {
           for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt_; ++i) {
             if (OB_ISNULL(order_items.at(i).expr_)) {
               ret = OB_ERR_UNEXPECTED;
               LOG_WARN("partition order expr is null", K(ret), K(i));
             } else if (OB_FAIL(hash_expr->add_param_expr(order_items.at(i).expr_))) {
               LOG_WARN("add hash param expr failed", K(ret), K(i));
             }
           }
         }

         if (OB_SUCC(ret) && OB_FAIL(hash_expr->formalize(&engine.get_session_info()))) {
           LOG_WARN("formalize hash expr failed", K(ret));
         } else if (OB_SUCC(ret)) {
           OrderItem hash_order_item(hash_expr);
           hash_order_item.order_type_ = order_items.at(0).order_type_;
           hash_order_item.is_not_null_ = true;
           if (OB_FAIL(order_items.push_back(hash_order_item))) {
             LOG_WARN("push back hash order item failed", K(ret));
           } else {
             for (int64_t i = order_items.count() - 1; i > 0; --i) {
               order_items.at(i) = order_items.at(i - 1);
             }
             order_items.at(0) = hash_order_item;
             hash_raw_expr_ = hash_expr;
           }
         }
       }
     }

     // Build and append encode_sortkey expression so expr codegen can generate runtime expr.
     // We append it as an extra order item and skip it in sort collations later.
     if (OB_SUCC(ret) && effective_encode_sortkey() && OB_ISNULL(encode_sortkey_raw_expr_)) {
       const int64_t encode_start_key = prefix_pos_ > 0 ? prefix_pos_ : (part_cnt_ > 0 ? part_cnt_ + 1 : 0);
       if (order_items.count() > encode_start_key) {
         OrderItem encode_sortkey;
         if (OB_FAIL(ObSQLUtils::create_encode_sortkey_expr(engine.expr_factory_,
                                                            &engine.get_exec_ctx(),
                                                            order_items,
                                                            encode_start_key,
                                                            encode_sortkey))) {
           LOG_WARN("create encode sortkey expr failed", K(ret), K(encode_start_key), K(order_items.count()));
         } else if (OB_ISNULL(encode_sortkey.expr_)) {
           ret = OB_ERR_UNEXPECTED;
           LOG_WARN("encode sortkey expr is null", K(ret));
         } else if (OB_FAIL(order_items.push_back(encode_sortkey))) {
           LOG_WARN("push back encode sortkey order item failed", K(ret));
         } else {
           encode_sortkey_raw_expr_ = encode_sortkey.expr_;
         }
       }
     }

     if (OB_FAIL(ret)) {
       LOG_WARN("prepare sort raw expr failed, fallback without optimization exprs", K(ret));
       ret = OB_SUCCESS;
     }
     return ret;
   }

   int finalize_spec_from_stmt(ObDMLStmt *stmt, OpTestEngine &engine, ObOpSpec *root_spec)
   {
     UNUSED(engine);
     int ret = OB_SUCCESS;

     if (OB_ISNULL(stmt) || OB_ISNULL(root_spec) || !stmt->is_select_stmt() || root_spec->type_ != PHY_VEC_SORT) {
       LOG_WARN("finalize_spec_from_stmt early return", KP(stmt), KP(root_spec),
                K(stmt ? stmt->is_select_stmt() : false), K(root_spec ? root_spec->type_ : PHY_INVALID));
       return ret;
     }

     ObSortVecSpec *sort_spec = static_cast<ObSortVecSpec *>(root_spec);
     ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
     const common::ObIArray<OrderItem> &order_items = select_stmt->get_order_items();

     if (order_items.count() <= 0) {
       LOG_WARN("sort test requires non-empty order items");
       return ret;
     }

     // Step 1: Collect sort key expressions (sk_exprs_) from ORDER BY
     // IMPORTANT: The order must match CG (ObStaticEngineCG::generate_encode_sort_exprs):
     // 1. If encode_sortkey is enabled, it goes to sk_exprs_[0] (but no collation for it)
     // 2. If partition sort, hash_expr goes next (with collation at index 0)
     // 3. Then partition keys and sort keys (with collations)
     //
     // When encode_sortkey + addon (== CG is_store_sortkey_separately), CG moves the
     // ENCODED sort keys (order_items index >= encode_start_key, i.e. beyond
     // prefix_pos/part_cnt) out of sk and into addon_keys, filling addon_collations_
     // for them. The leading prefix/part keys (index < encode_start_key) stay in sk.
     // The comparator (ob_sort_vec_op_impl.ipp:285) then uses addon_collations_ as the
     // tie-break set when encode_sortkey && has_addon. We mirror that routing here so
     // the sk/addon row layout and the comparator's collation set match production.
     ObSEArray<ObExpr *, 4> sk_exprs;
     ObSEArray<ObSortFieldCollation, 4> sk_collations;
     ObSEArray<ObExpr *, 4> addon_exprs;
     ObSEArray<ObSortFieldCollation, 4> addon_collations;
     uint32_t field_idx = 0;
     uint32_t addon_field_idx = 0;
     // encode_start_key boundary in order_items index space (matches prepare_raw_exprs).
     // part_cnt_ > 0 implies effective_encode_sortkey()==false, so in the encode path
     // there is no hash item and this reduces to prefix_pos_ (or 0 for full sort).
     const int64_t encode_start_key =
         prefix_pos_ > 0 ? prefix_pos_ : (part_cnt_ > 0 ? part_cnt_ + 1 : 0);
     const bool encode_separately = effective_encode_sortkey() && has_addon_;

     // First, add encode_sortkey to sk_exprs_[0] if enabled (no collation for it)
     // This matches CG behavior where encode_expr is pushed first, but fill_sort_info
     // only processes sk_keys (not encode_expr), so collations skip encode_expr.
     if (effective_encode_sortkey() && OB_NOT_NULL(encode_sortkey_raw_expr_)) {
       ObExpr *encode_rt_expr = ObStaticEngineExprCG::get_rt_expr(*encode_sortkey_raw_expr_);
       if (OB_ISNULL(encode_rt_expr)) {
         LOG_WARN("rt expr for encode sortkey is null, skip", KP(encode_sortkey_raw_expr_));
       } else if (OB_FAIL(sk_exprs.push_back(encode_rt_expr))) {
         LOG_WARN("push back encode sortkey rt expr failed", K(ret));
       } else {
         // encode_sortkey is at sk_exprs_[0], but collations start from field_idx=1
         // (because fill_sort_info in CG creates collations for sk_keys, not encode_expr)
         field_idx = 1;
       }
     }

     // Process order_items in order: hash_expr first, then partition keys, then sort keys
     for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
       const OrderItem &order_item = order_items.at(i);
       if (order_item.expr_ == encode_sortkey_raw_expr_) {
         // encode sortkey is already in sk_exprs_[0], skip it here
       } else if (order_item.expr_ == hash_raw_expr_) {
         // IMPORTANT: hash expr MUST be added to sk_exprs with a collation!
         // In CG, hash_sortkey is added to sk_keys first, then fill_sort_info creates
         // a collation for it. The hash value is stored in the row at this field_idx.
         // ObPartitionSortStrategy::build_hash_table uses sk_collations_->at(0).field_idx_
         // to get the hash value from the row.
         if (OB_ISNULL(hash_raw_expr_)) {
           ret = OB_ERR_UNEXPECTED;
           LOG_WARN("hash_raw_expr is null", K(ret));
         } else {
           ObExpr *hash_rt_expr = ObStaticEngineExprCG::get_rt_expr(*hash_raw_expr_);
           if (OB_ISNULL(hash_rt_expr)) {
             LOG_WARN("rt expr for hash is null", K(ret));
           } else if (OB_FAIL(sk_exprs.push_back(hash_rt_expr))) {
             LOG_WARN("push back hash rt expr failed", K(ret));
           } else {
             // Create collation for hash expr: field_idx points to its position in sk_exprs
             // Hash values are never null, use NULL_LAST and is_not_null=true
             ObSortFieldCollation hash_collation(field_idx++,
                                                 hash_rt_expr->datum_meta_.cs_type_,
                                                 order_item.is_ascending(),
                                                 common::NULL_LAST,  // hash value never null
                                                 true);  // is_not_null
             if (OB_FAIL(sk_collations.push_back(hash_collation))) {
               LOG_WARN("push back hash collation failed", K(ret));
             }
           }
         }
       } else if (OB_ISNULL(order_item.expr_) || order_item.expr_->is_const_expr()) {
         // keep behavior aligned with codegen: ignore const sort keys
       } else {
         ObExpr *rt_expr = ObStaticEngineExprCG::get_rt_expr(*order_item.expr_);
         if (OB_ISNULL(rt_expr)) {
           LOG_WARN("rt expr for order item is null, skip", K(i));
         } else if (encode_separately && i >= encode_start_key) {
           // CG generate_encode_sort_exprs: encoded sort key -> addon_keys with collation.
           if (OB_FAIL(addon_exprs.push_back(rt_expr))) {
             LOG_WARN("push back addon sk expr failed", K(ret), K(i));
           } else {
             ObSortFieldCollation collation(addon_field_idx++,
                                            rt_expr->datum_meta_.cs_type_,
                                            order_item.is_ascending(),
                                            (order_item.is_null_first() ^ order_item.is_ascending()) ? common::NULL_LAST
                                                                                                       : common::NULL_FIRST,
                                            order_item.is_not_null_);
             if (OB_FAIL(addon_collations.push_back(collation))) {
               LOG_WARN("push back addon sk collation failed", K(ret), K(i));
             }
           }
         } else if (OB_FAIL(sk_exprs.push_back(rt_expr))) {
           LOG_WARN("push back sk_expr failed", K(ret), K(i));
         } else {
           ObSortFieldCollation collation(field_idx++,
                                          rt_expr->datum_meta_.cs_type_,
                                          order_item.is_ascending(),
                                          (order_item.is_null_first() ^ order_item.is_ascending()) ? common::NULL_LAST
                                                                                                     : common::NULL_FIRST,
                                          order_item.is_not_null_);
           if (OB_FAIL(sk_collations.push_back(collation))) {
             LOG_WARN("push back sk collation failed", K(ret), K(i));
           }
         }
       }
     }

     // Step 2: Collect child output expressions (column expressions from table)
     // These are the expressions that mock data source outputs
     const common::ObIArray<ColumnItem> &column_items = stmt->get_column_items();
     ObSEArray<ObExpr *, 4> child_output_exprs;
     for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
       const ColumnItem &col_item = column_items.at(i);
       if (OB_NOT_NULL(col_item.expr_)) {
         ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*col_item.expr_);
         if (OB_NOT_NULL(expr) && OB_FAIL(child_output_exprs.push_back(expr))) {
           LOG_WARN("push back child output expr failed", K(ret), K(i));
         }
       }
     }

     // Step 3: Append child output columns, mirroring append_child_output_no_dup().
     // addon_exprs/addon_collations may already hold the encoded sort keys moved in
     // Step 1 (encode_separately case); child output columns append after them WITHOUT
     // collations (they are payload, not comparison keys).

     // Helper lambda to check if expr exists in array
     auto has_exist_in_array = [](const ObIArray<ObExpr *> &arr, ObExpr *expr) -> bool {
       for (int64_t i = 0; i < arr.count(); ++i) {
         if (arr.at(i) == expr) return true;
       }
       return false;
     };

     // Append child output expressions that are not already in sk_exprs or addon_exprs.
     // CG checks BOTH arrays for dedup (append_child_output_no_dup).
     for (int64_t i = 0; OB_SUCC(ret) && i < child_output_exprs.count(); ++i) {
       ObExpr *expr = child_output_exprs.at(i);
       if (has_exist_in_array(sk_exprs, expr) || has_exist_in_array(addon_exprs, expr)) {
         // Already in sort keys or addon keys, skip
       } else if (has_addon_) {
         // has_addon_=true (== is_store_sortkey_separately): non-key columns -> addon_exprs_
         if (OB_FAIL(addon_exprs.push_back(expr))) {
           LOG_WARN("push back addon_expr failed", K(ret), K(i));
         }
       } else {
         // has_addon_=false: put all columns into sk_exprs_
         if (OB_FAIL(sk_exprs.push_back(expr))) {
           LOG_WARN("push back sk_expr failed", K(ret), K(i));
         }
       }
     }

     // Step 4: Initialize spec arrays
     if (OB_FAIL(ret)) {
       // already failed
     } else if (OB_FAIL(sort_spec->sk_exprs_.init(sk_exprs.count()))) {
       LOG_WARN("init sk_exprs failed", K(ret));
     } else if (OB_FAIL(sort_spec->sk_collations_.init(sk_collations.count()))) {
       LOG_WARN("init sk_collations failed", K(ret));
     } else if (OB_FAIL(append(sort_spec->sk_exprs_, sk_exprs))) {
       LOG_WARN("append sk_exprs failed", K(ret));
     } else if (OB_FAIL(append(sort_spec->sk_collations_, sk_collations))) {
       LOG_WARN("append sk_collations failed", K(ret));
     } else if (has_addon_) {
       if (OB_FAIL(sort_spec->addon_exprs_.init(addon_exprs.count()))) {
         LOG_WARN("init addon_exprs failed", K(ret));
       } else if (OB_FAIL(sort_spec->addon_collations_.init(addon_collations.count()))) {
         LOG_WARN("init addon_collations failed", K(ret));
       } else if (OB_FAIL(append(sort_spec->addon_exprs_, addon_exprs))) {
         LOG_WARN("append addon_exprs failed", K(ret));
       } else if (OB_FAIL(append(sort_spec->addon_collations_, addon_collations))) {
         LOG_WARN("append addon_collations failed", K(ret));
       }
     }

     // Step 5: Set output_ to sk_exprs_ + addon_exprs_ (what sort operator actually outputs)
     // This is critical: sort operator outputs sk_exprs_ + addon_exprs_, not SELECT expressions
     if (OB_SUCC(ret)) {
       // Update has_addon_ based on actual addon_exprs count (like in ObStaticEngineCG)
       sort_spec->has_addon_ = (addon_exprs.count() != 0);

       ObSEArray<ObExpr *, 4> sort_output_exprs;
       if (OB_FAIL(append(sort_output_exprs, sort_spec->sk_exprs_))) {
         LOG_WARN("append sk_exprs to output failed", K(ret));
       } else if (sort_spec->has_addon_ && OB_FAIL(append(sort_output_exprs, sort_spec->addon_exprs_))) {
         LOG_WARN("append addon_exprs to output failed", K(ret));
       } else if (OB_FAIL(sort_spec->output_.init(sort_output_exprs.count()))) {
         LOG_WARN("init output failed", K(ret));
       } else if (OB_FAIL(append(sort_spec->output_, sort_output_exprs))) {
         LOG_WARN("append output failed", K(ret));
       }
     }

     // Step 6: Add sk_exprs to calc_exprs_ so they get cleared between batches
     // This is CRITICAL for partition sort: the hash expression must be re-evaluated
     // for each batch. Without this, rows get stale hash values from previous batches
     // because calc_hash_value_expr_batch() skips rows with eval_flags already set.
     if (OB_SUCC(ret)) {
       if (OB_FAIL(sort_spec->calc_exprs_.init(sk_exprs.count()))) {
         LOG_WARN("init calc_exprs failed", K(ret));
       } else if (OB_FAIL(sort_spec->calc_exprs_.prepare_allocate(sk_exprs.count()))) {
         LOG_WARN("prepare_allocate calc_exprs failed", K(ret));
       } else {
         for (int64_t i = 0; i < sk_exprs.count(); ++i) {
           sort_spec->calc_exprs_.at(i) = sk_exprs.at(i);
         }
       }
     }

     // Step 7: Decide SingleColCompare optimization, mirroring CG
     // ObStaticEngineCG::use_single_col_compare (single sort key, basic/str type).
     // create_spec() defaults enable_single_col_compare_opt_ to false because the
     // sk arrays are not yet filled there; decide it here after Step 4/5.
     if (OB_SUCC(ret) && request_single_col_compare_) {
       sort_spec->enable_single_col_compare_opt_ =
           (1 == sort_spec->sk_collations_.count() && 1 == sort_spec->sk_exprs_.count());
       if (!sort_spec->enable_single_col_compare_opt_) {
         LOG_WARN("single col compare requested but spec is not single-key, kept disabled",
                  K(sort_spec->sk_exprs_.count()), K(sort_spec->sk_collations_.count()));
       }
     }

     if (OB_FAIL(ret)) {
       LOG_WARN("finalize sort spec from stmt failed", K(ret));
       ret = OB_SUCCESS;
     }
     return ret;
   }

   /**
    * @brief Enable/disable encode sortkey optimization.
    * @param enable true to enable (default)
    */
   SortTestSpec& with_encode_sortkey(bool enable = true)
   {
     enable_encode_sortkey_ = enable;
     return *this;
   }

   // ===== Single Column Compare Configuration =====

   /**
    * @brief Request SingleColCompare optimization (sysbench fast path).
    * Mirrors CG ObStaticEngineCG::use_single_col_compare: only takes effect when
    * the finalized spec has exactly one sort key (sk_exprs==1 && sk_collations==1)
    * with a basic/string comparable type. The actual enable_single_col_compare_opt_
    * is decided in finalize_spec_from_stmt() after sk arrays are filled.
    * To reach a single sort key, pair with .with_addon(true) so non-key columns
    * go to addon_exprs_ instead of sk_exprs_, and keep encode sortkey off.
    * @param enable true to request single-col compare (default: true)
    */
   SortTestSpec& with_single_col_compare_opt(bool enable = true)
   {
     request_single_col_compare_ = enable;
     return *this;
   }

   // ===== TopN Sort Configuration =====

   /**
    * @brief Configure TopN sort (sort with limit).
    * @param n Number of top rows to keep
    */
   SortTestSpec& with_topn(int64_t n)
   {
     is_topn_sort_ = true;
     topn_cnt_ = n;
     return *this;
   }

   // ===== Prefix Sort Configuration =====

   /**
    * @brief Configure prefix sort position.
    * @param pos Prefix position for prefix sort optimization
    */
   SortTestSpec& with_prefix_pos(int64_t pos)
   {
     prefix_pos_ = pos;
     return *this;
   }

   // Alias kept for compatibility with the master baseline tests (test_prefix_sort_op).
   SortTestSpec& prefix_pos(int64_t pos)
   {
     return with_prefix_pos(pos);
   }

   // ===== Addon Columns Configuration =====

   /**
    * @brief Enable/disable addon columns mode.
    * When enabled, non-sort-key child output columns are stored in addon_exprs_
    * separately from the sort key columns (sk_exprs_). This matches the behavior
    * of ObStaticEngineCG::append_child_output_no_dup() which splits columns.
    * @param enable true to put non-sort-key columns into addon section (default: true)
    */
   SortTestSpec& with_addon(bool enable = true)
   {
     has_addon_ = enable;
     return *this;
   }

   /**
    * @brief Configure addon columns (columns that are not part of sort key).
    * @param exprs Comma-separated expression list for addon columns (informational only)
    */
   SortTestSpec& with_addon_exprs(const char *exprs)
   {
     has_addon_ = true;
     addon_exprs_ = exprs;
     return *this;
   }

   // ===== Compress Type Configuration =====

   /**
    * @brief Set compress type for temp row store.
    * @param type Compressor type
    */
   SortTestSpec& with_compress_type(common::ObCompressorType type)
   {
     compress_type_ = type;
     return *this;
   }

   // ===== Non-vectorized (1.0 / ObSortSpec) helpers =====
   // Grafted from the master baseline SortTestSpec so enable_dual_format_check()
   // can exercise the non-rich ObSortOp. Only the PHY_SORT path uses these; the
   // rich PHY_VEC_SORT path is built by create_spec()+finalize_spec_from_stmt().

   static ObOrderDirection get_order_direction(const bool ascending, const bool nulls_first)
   {
     return ascending
            ? (nulls_first ? NULLS_FIRST_ASC : NULLS_LAST_ASC)
            : (nulls_first ? NULLS_FIRST_DESC : NULLS_LAST_DESC);
   }

   int get_sort_key_specs(std::vector<SortKeySpec> &sort_specs)
   {
     int ret = OB_SUCCESS;
     if (order_by_exprs_.empty()) {
       ret = OB_INVALID_ARGUMENT;
       LOG_WARN("sort test requires order by", K(ret));
     } else if (OB_ISNULL(resolved_stmt_)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("resolved stmt is null", K(ret));
     } else {
       sort_specs = build_sort_key_specs(order_by_exprs_,
                                         resolved_stmt_->get_select_items(),
                                         resolved_stmt_->get_column_items());
       if (sort_specs.empty()) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("failed to build sort key specs", K(ret), K(order_by_exprs_.c_str()));
       }
     }
     return ret;
   }

   int fill_sort_collations(common::ObIAllocator &alloc,
                            const std::vector<SortKeySpec> &sort_specs,
                            ObSortCollations &sort_collations,
                            ObSortFuncs *sort_cmp_funcs)
   {
     int ret = OB_SUCCESS;
     if (OB_FAIL(sort_collations.init(static_cast<int64_t>(sort_specs.size())))) {
       LOG_WARN("init sort collations failed", K(ret));
     } else if (OB_NOT_NULL(sort_cmp_funcs)
                && OB_FAIL(sort_cmp_funcs->init(static_cast<int64_t>(sort_specs.size())))) {
       LOG_WARN("init sort cmp funcs failed", K(ret));
     }
     for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(sort_specs.size()); ++i) {
       const SortKeySpec &sort_spec = sort_specs[static_cast<size_t>(i)];
       const ObOrderDirection direction =
           get_order_direction(sort_spec.ascending, sort_spec.nulls_first);
       if (OB_NOT_NULL(sort_cmp_funcs)) {
         if (OB_FAIL(fill_sort_info_for_expr(sort_spec.key_expr, direction,
                                             sort_collations, *sort_cmp_funcs, alloc))) {
           LOG_WARN("fill sort info failed", K(ret), K(i));
         }
       } else {
         ObSortFuncs dummy_cmp_funcs(alloc);
         if (OB_FAIL(dummy_cmp_funcs.init(1))) {
           LOG_WARN("init dummy cmp funcs failed", K(ret));
         } else if (OB_FAIL(fill_sort_info_for_expr(sort_spec.key_expr, direction,
                                                    sort_collations, dummy_cmp_funcs, alloc))) {
           LOG_WARN("fill sort collation failed", K(ret), K(i));
         }
       }
       if (OB_SUCC(ret)) {
         sort_collations.at(i).field_idx_ = i;
       }
     }
     return ret;
   }

   int fill_exprs(const std::vector<ObExpr *> &exprs, ExprFixedArray &target)
   {
     int ret = OB_SUCCESS;
     if (OB_FAIL(target.init(static_cast<int64_t>(exprs.size())))) {
       LOG_WARN("init expr array failed", K(ret));
     } else if (OB_FAIL(target.prepare_allocate(static_cast<int64_t>(exprs.size())))) {
       LOG_WARN("prepare expr array failed", K(ret));
     } else {
       for (int64_t i = 0; i < static_cast<int64_t>(exprs.size()); ++i) {
         target.at(i) = exprs[static_cast<size_t>(i)];
       }
     }
     return ret;
   }

   int fill_nonvec_sort_spec(common::ObIAllocator &alloc,
                             const std::vector<SortKeySpec> &sort_specs,
                             const ExprFixedArray &output_exprs,
                             ObSortSpec &sort_spec)
   {
     int ret = OB_SUCCESS;
     std::vector<ObExpr *> all_exprs;
     all_exprs.reserve(sort_specs.size() + output_exprs.count());
     for (const SortKeySpec &sort_key : sort_specs) {
       all_exprs.push_back(sort_key.key_expr);
     }
     for (int64_t i = 0; i < output_exprs.count(); ++i) {
       ObExpr *expr = output_exprs.at(i);
       bool exists = false;
       for (ObExpr *existing_expr : all_exprs) {
         if (existing_expr == expr) {
           exists = true;
           break;
         }
       }
       if (!exists) {
         all_exprs.push_back(expr);
       }
     }
     if (OB_FAIL(fill_exprs(all_exprs, sort_spec.all_exprs_))) {
       LOG_WARN("fill all exprs failed", K(ret));
     } else if (OB_FAIL(fill_sort_collations(alloc, sort_specs,
                                             sort_spec.sort_collations_,
                                             &sort_spec.sort_cmp_funs_))) {
       LOG_WARN("fill sort collations failed", K(ret));
     }
     return ret;
   }

   ObOpSpec *create_nonvec_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                                const ExprFixedArray &output_exprs)
   {
     int ret = OB_SUCCESS;
     std::vector<SortKeySpec> sort_specs;
     ObSortSpec *sort_spec = nullptr;
     void *mem = nullptr;
     if (OB_FAIL(get_sort_key_specs(sort_specs))) {
       LOG_WARN("get sort key specs failed", K(ret));
     } else if (prefix_pos_ < 0 || prefix_pos_ > static_cast<int64_t>(sort_specs.size())) {
       ret = OB_INVALID_ARGUMENT;
       LOG_WARN("invalid prefix pos", K(ret), K(prefix_pos_), K(sort_specs.size()));
     } else if (OB_ISNULL(mem = alloc.alloc(sizeof(ObSortSpec)))) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
       LOG_WARN("alloc sort spec failed", K(ret));
     } else {
       sort_spec = new (mem) ObSortSpec(alloc, PHY_SORT);
       sort_spec->topn_expr_ = nullptr;
       sort_spec->topk_limit_expr_ = nullptr;
       sort_spec->topk_offset_expr_ = nullptr;
       sort_spec->minimum_row_count_ = 0;
       sort_spec->topk_precision_ = 0;
       sort_spec->prefix_pos_ = prefix_pos_;
       sort_spec->is_local_merge_sort_ = false;
       sort_spec->is_fetch_with_ties_ = false;
       sort_spec->prescan_enabled_ = false;
       sort_spec->enable_encode_sortkey_opt_ = false;
       sort_spec->part_cnt_ = 0;
       sort_spec->plan_ = child_spec->plan_;
       sort_spec->max_batch_size_ = child_spec->max_batch_size_;
       sort_spec->use_rich_format_ = false;
       sort_spec->output_ = output_exprs;
       sort_spec->rows_ = child_spec->rows_;
       sort_spec->width_ = child_spec->width_;

       void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
       if (OB_ISNULL(child_spec_mem)) {
         ret = OB_ALLOCATE_MEMORY_FAILED;
         LOG_WARN("alloc child spec array failed", K(ret));
       } else {
         ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
         children[0] = child_spec;
         if (OB_FAIL(sort_spec->set_children_pointer(children, 1))) {
           LOG_WARN("set children pointer failed", K(ret));
         }
       }
     }
     if (OB_SUCC(ret)) {
       ret = fill_nonvec_sort_spec(alloc, sort_specs, output_exprs, *sort_spec);
     }
     return OB_SUCC(ret) ? sort_spec : nullptr;
   }

   // ===== Create Spec =====

   /**
    * @brief Create ObSortVecSpec with MockDataSourceSpec as child.
    * @param alloc Allocator for spec creation
    * @param child_spec The child spec (MockDataSourceSpec)
    * @param output_exprs The real output expressions (SELECT expressions) - used for child output
    * @param limit_expr Optional LIMIT expression from stmt
    * @param offset_expr Optional OFFSET expression from stmt
    * @param use_rich_format Whether to use rich format
    * @return Pointer to created ObSortVecSpec
    */
   ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                          const ExprFixedArray &output_exprs,
                          ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
   {
     // Non-vectorized (1.0) path: enable_dual_format_check() runs the spec twice,
     // once with use_rich_format=false. Build a PHY_SORT / ObSortSpec there so the
     // 1.0 ObSortOp can run. The rich (2.0) path below stays as the project's
     // CG-faithful ObSortVecSpec builder finalized by finalize_spec_from_stmt().
     if (!use_rich_format) {
       UNUSED(limit_expr);
       UNUSED(offset_expr);
       return create_nonvec_spec(alloc, child_spec, output_exprs);
     }
     int ret = OB_SUCCESS;
     ObOpSpec *result = nullptr;
     void *mem = alloc.alloc(sizeof(ObSortVecSpec));
     if (OB_ISNULL(mem)) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
       LOG_WARN("alloc ObSortVecSpec failed", K(ret));
     } else {
       ObSortVecSpec *sort_spec = new (mem) ObSortVecSpec(alloc, PHY_VEC_SORT);

       // Basic spec setup
       sort_spec->plan_ = child_spec->plan_;
       sort_spec->max_batch_size_ = child_spec->max_batch_size_;
       sort_spec->use_rich_format_ = use_rich_format;
       // Note: output_ will be set in finalize_spec_from_stmt() based on sk_exprs_ + addon_exprs_
       sort_spec->rows_ = estimated_rows_;  // Set estimated row count

       // Sort-specific configuration
       sort_spec->topn_expr_ = is_topn_sort_ ? limit_expr : nullptr;
       sort_spec->topk_limit_expr_ = nullptr;
       sort_spec->topk_offset_expr_ = nullptr;
       sort_spec->prefix_pos_ = prefix_pos_;
       sort_spec->minimum_row_count_ = 0;
       sort_spec->topk_precision_ = 0;
       sort_spec->is_local_merge_sort_ = false;
       sort_spec->is_fetch_with_ties_ = false;
       sort_spec->prescan_enabled_ = false;
       sort_spec->enable_encode_sortkey_opt_ = effective_encode_sortkey();
       // has_addon_ will be recalculated in finalize_spec_from_stmt() based on addon_exprs_.count()
       sort_spec->has_addon_ = has_addon_;
       sort_spec->part_cnt_ = part_cnt_;
       sort_spec->compress_type_ = compress_type_;
       sort_spec->enable_single_col_compare_opt_ = false;

       // sk_exprs_, sk_collations_, addon_exprs_, addon_collations_, output_ will be filled
       // by finalize_spec_from_stmt() to match the actual sort operator behavior

       // Set up child
       void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
       if (OB_ISNULL(child_spec_mem)) {
         ret = OB_ALLOCATE_MEMORY_FAILED;
         LOG_WARN("alloc child spec array failed", K(ret));
       } else {
         ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
         children[0] = child_spec;
         if (OB_FAIL(sort_spec->set_children_pointer(children, 1))) {
           LOG_WARN("set children pointer failed", K(ret));
         } else {
           result = sort_spec;
         }
       }
     }
     return result;
   }

   // ===== Create Operator =====

   /**
    * @brief Create ObSortVecOp with MockDataSourceOp as child.
    */
   ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
   {
     // Non-vectorized (1.0) path: build ObSortOp for PHY_SORT specs.
     if (spec.type_ == PHY_SORT) {
       return default_create_op<ObSortOp>(ctx, spec, child_op);
     }
     int ret = OB_SUCCESS;
     ObOperator *result = nullptr;
     void *mem = ctx.get_allocator().alloc(sizeof(ObSortVecOp));
     if (OB_ISNULL(mem)) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
       LOG_WARN("alloc ObSortVecOp failed", K(ret));
     } else {
       ObSortVecOp *sort_op = new (mem) ObSortVecOp(ctx, spec, nullptr);

       void *children_mem = ctx.get_allocator().alloc(sizeof(ObOperator *));
       if (OB_ISNULL(children_mem)) {
         ret = OB_ALLOCATE_MEMORY_FAILED;
         LOG_WARN("alloc children array failed", K(ret));
       } else {
         ObOperator **children = reinterpret_cast<ObOperator **>(children_mem);
         children[0] = child_op;

         if (OB_FAIL(sort_op->set_children_pointer(children, 1))) {
           LOG_WARN("set children pointer failed", K(ret));
         } else {
           result = sort_op;
         }
       }
     }
     return result;
   }

   /**
    * @brief Fallback create_op when no parent spec exists.
    */
   ObOperator *create_op(ObExecContext &ctx, ObOperator *child_op)
   {
     return child_op;
   }

 private:
   bool is_partition_sort_;
   int64_t part_cnt_;
   bool enable_encode_sortkey_;
   bool request_single_col_compare_ = false;
   bool is_topn_sort_;
   int64_t topn_cnt_;
   int64_t prefix_pos_;
   bool has_addon_;
   std::string addon_exprs_;
   common::ObCompressorType compress_type_;
   ObRawExpr *hash_raw_expr_;
   ObRawExpr *encode_sortkey_raw_expr_;
   int64_t estimated_rows_ = 10000;  // Default estimated rows for sort operator

 public:
   /**
    * @brief Set estimated row count for the sort operator.
    * This is important for the sort operator to allocate memory correctly.
    * @param rows Estimated number of rows
    */
   SortTestSpec& with_estimated_rows(int64_t rows)
   {
     estimated_rows_ = rows;
     return *this;
   }
 };

 }  // namespace sql
 }  // namespace oceanbase

 #endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SORT_H_