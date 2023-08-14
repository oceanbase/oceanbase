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

int acquire_tx(ObTxDesc *&tx, const uint32_t session_id = 0);

/**
 * start_tx - explicit start transaction
 *
 * transaction can also be start implicitly:
 * when you touch any dirty data, it become active automatically
 *
 * start transaction won't cause savepoint to be released
 *
 * @tx:       the target transaction's descriptor
 * @tx_param: transaction parameters
 * @tx_id:    the txid applied for when xa start
 *
 * Return:
 * OB_SUCCESS - OK
 */
int start_tx(ObTxDesc &tx, const oceanbase::transaction::ObTxParam &tx_param, const ObTransID &tx_id = ObTransID());

/**
 * abort_tx - abort transaction
 *
 * tx aborted need user rollback explicitly
 *
 * @tx:       the target transaction's descriptor
 * @cause:    abort cause by who
 *
 * Return:
 * OB_SUCCESS - OK
 */
int abort_tx(ObTxDesc &tx, int cause);

/**
 * rollback_tx - rollback transaction
 *
 * @tx:          the target transaction's descriptor
 *
 * Return: OB_SUCCESS - OK
 */
int rollback_tx(ObTxDesc &tx);

/**
 * commit_tx - commit transaction
 *
 * this is a blocking function, for asynchrous usage,
 * use @async_commit_tx instead
 *
 * @tx:         the target transaction's descriptor
 * @expire_ts:  microseconds after which timeouted
 * @trace_info: trace info need to persistent in CommitLog
 *
 * Return:
 * OB_SUCCESS          - commit succeed
 * OB_TRANS_ROLLBACKED - transaction commit fail and was rollbacked
 * OB_TRANS_TIMEOUT    - transaction timeout and aborted internally
 * OB_TIME_OUT         - commit operation blocking wait timeouted
 */
int commit_tx(ObTxDesc &tx, const int64_t expire_ts, const ObString *trace_info = NULL);

/**
 * submit_commit_tx - start transaction commit
 *
 * the commit result was notified via @callback parameter
 *
 * if any error occurred, the @callback won't been called
 *
 * @tx:               the target transaction's descriptor
 * @expire_ts:        microseconds after which timeouted
 * @callback:         callback object
 * @trace_info:       trace info need to persistent in CommitLog
 *
 * Return:
 * OB_SUCCESS            - the commit successfully submitted,
 *                         result will notify caller via @callback
 * OB_TRANS_COMMITED    - transaction committed yet
 * OB_TRANS_ROLLBACKED   - transaction rollbacked yet
 * OB_TRANS_TIMEOUT      - transaction timeout and aborted internally
 * OB_TRANS_STMT_TIMEOUT - commit not accomplished before expire_ts
 */
int submit_commit_tx(ObTxDesc &tx,
                     const int64_t expire_ts,
                     ObITxCallback &callback,
                     const ObString *trace_info = NULL);

/**
 * release_tx - release transaction descriptor
 *
 * this is the end of lifecycle of a transaction
 *
 * @tx:         the target transaction's descriptor
 *
 * Return: OB_SUCCESS - OK
 */
int release_tx(ObTxDesc &tx);

/**
 * reuse_tx - reuse transaction descriptor
 *
 * when txn end, in stead of release txn descriptor, reuse it for
 * better performance.
 *
 * @tx:       the target transaction's descriptor
 *
 * Return: OB_SUCCESS -OK
 */
int reuse_tx(ObTxDesc &tx);

/**
 * stop_tx - stop txn immediately (for admin reason)
 *
 * this is a internal resource management interface, normal user
 * should not use it directly
 *
 * the purpose of this interface is used to make txn descriptor
 * and other resource can be released immediately, regardless of
 * it is active or in-terminate
 *
 * @tx:     the transaction descriptor to stop
 *
 * Return:
 * OB_SUCCESS     - OK
 * OB_ERR_OTHERS  - fatal error, imply a bug
 */
int stop_tx(ObTxDesc &tx);

// -----------------------------------------------------------------
// get snapshot version for in / out of transaction read
//
// * distributed calling limits:
//     can be called at any node
// -----------------------------------------------------------------

/**
 * get_read_snapshot - get a read snapshot which can be used to read
 *                     a consistency view of current state of database
 *
 * @tx:                the tx in which snapshot stationed
 * @isolation_level:   the Isolation Level setting
 * @expire_ts:         microseconds of timestamp after which acquire
 *                     action timeout
 * @snapshot:          the snapshot acquired
 *
 * Return:
 * OB_SUCCESS - OK
 * OB_TIMEOUT - if not success when expire_ts run out
 */
int get_read_snapshot(ObTxDesc &tx,
                      const ObTxIsolationLevel isolation_level,
                      const int64_t expire_ts,
                      ObTxReadSnapshot &snapshot);

/**
 * get_ls_read_snapshot - get a read snapshot which can be used to read
 *                        a consistency view of current state of data
 *                        of the specified local LogStream
 *
 * @tx:                   the tx in which snapshot stationed
 * @isolation_level:      the Isolation Level setting
 * @local_ls_id:          the local LogStream's id
 * @expire_ts:            microseconds of timestamp after which acquire
 *                        action timeout
 * @snapshot:             the snapshot acquired
 *
 * Return:
 * OB_SUCCESS    - OK
 * OB_NOT_MASTER - if local replica of @local_ls_id not leader
 * OB_TIMEOUT    - if expire_ts hit
 */
int get_ls_read_snapshot(ObTxDesc &tx,
                         const ObTxIsolationLevel isolation_level,
                         const share::ObLSID &local_ls_id,
                         const int64_t expire_ts,
                         ObTxReadSnapshot &snapshot);

// ------------------------------------------------------------------
//  get snapshot version for out of transaction read special case
// ------------------------------------------------------------------

/**
 * get_read_snapshot_version - get a read snapshot of current tenant
 *
 * the snasphot can be used to read a consistency view of the state
 * of current tenant
 *
 * @expire_ts:                 microseconds of timestamp after which
 *                             acquire action timeout
 * @snapshot_version:          the snapshot acquired
 *
 * Return:
 * OB_SUCCESS  - OK
 * OB_TIMEOUT - if expire_ts hit
 */
int get_read_snapshot_version(const int64_t expire_ts,
                              share::SCN &snapshot_version);

/**
 * get_ls_read_snapshot_version - get a read snapshot of specified
 *                                local LogStream
 *
 * the snasphot can be used to read a consistency view of the state
 * of specified log stream
 *
 * @local_ls_id:                  the local LogStream's id
 * @expire_ts:                    microseconds of timestamp after
 *                                which acquire action timeout
 * @snapshot_version:             the snapshot acquired
 *
 * Return:
 * OB_SUCCESS    - OK
 * OB_NOT_MASTER - if local replica of @local_ls_id not leader
 * OB_TIMEOUT    - if expire_ts hit
 */
int get_ls_read_snapshot_version(const share::ObLSID &local_ls_id,
                                 share::SCN &snapshot_version);
/**
 * get_weak_read_snapshot_version - get snapshot version for weak read
 *
 * max_read_stale_time              the minimal threshold of stale snapshot
 * @snapshot_version:               the snapshot acquired
 *
 * Return:
 * OB_SUCCESS              - OK
 * OB_REPLICA_NOT_READABLE - snapshot is too stale
 */
int get_weak_read_snapshot_version(const int64_t max_read_stale_time,
                                   const bool local_single_ls,
                                   share::SCN &snapshot_version);
/*
 * release_snapshot - release snapshot
 *
 * release current tx bookkeeped snapshot which used for further
 * snapshot acquire when isolation level is REPEATABLE READ or
 * SERIALIZABLE
 *
 * the transaction must in IDLE state
 *
 * @tx                the transaction descriptor
 *
 * Return:
 * OB_SUCCESS       - OK
 * OB_NOT_SUPPORTED - if transaction state is not IDLE
 */
int release_snapshot(ObTxDesc &tx);
// ------------------------------------------------------------------
// snapshot verify management
// ------------------------------------------------------------------
/**
 * register_tx_snapshot_verify - register a snapshot to verify
 *
 * the snapshot was registered to verify, and make it sens to
 * transaction state changes which cause snapshot invalid
 *
 * one use case is SQL cursor, each time cursor do fetch, it must
 * verify itself was valid, because a previouse rollback savepoint
 * or rollback transaction will cause cursor's snapshot invalid and
 * feth such cursor should report an error.
 *
 * if your snapshot won't be concurrent with rollback operation, it
 * is safe not register to verify
 *
 * @snapshot:                    the snapshot need verify
 *
 * Return: OB_SUCCESS - OK
 */
int register_tx_snapshot_verify(ObTxReadSnapshot &snapshot);

/**
 * unregister_tx_snapshot_verify - unregister a snapshot reigstered
 *                                 for verify
 * snapshot registered via @register_tx_snapshot_verify must be
 * unregistered before release its memory to make memory safe
 *
 * @snapshot:                      the snapshot to unregister
 */
void unregister_tx_snapshot_verify(ObTxReadSnapshot &snapshot);

// ------------------------------------------------------------------
// savepoint creation and rollback to and release
//
// * distributed calling limits:
//     can only be called on transaction starting node
// ------------------------------------------------------------------

/**
 * create_implicit_savepoint - establish a savepoint and which can be
 *                             used to rolling back to
 *
 * this was designed for internal use
 * the savepoint won't be saved, for efficiency
 *
 * this function won't start a transaction
 *
 * @tx:                        the target transaction's descriptor
 * @tx_param:                  the transaction parameter used to setup
 *                             transaction state if need
 * @savepoint:                 the identifier of the savepoint returned
 * @release:                   release all current implicit savepoints,
 *                             default is false
 *
 * Return:
 * OB_SUCCESS - OK
 */
int create_implicit_savepoint(ObTxDesc &tx,
                              const ObTxParam &tx_param,
                              ObTxSEQ &savepoint,
                              const bool release = false);

/**
 * create_explicit_savepoint - establish a savepoint and associate name
 *
 * it was designed for external use
 *
 * the savepoint info was saved and later be verify when rolling back to
 *
 * @tx:                        the target transaction's descriptor
 *                             which hold the savepoint
 * @savepoint:                 the name of savepoint to be created
 *
 * @session_id:                the session id to which the savepoint
 *                             belongs, used for xa
 *
 * Return:
 * OB_SUCCESS                - OK
 * OB_ERR_TOO_LONG_IDENT     - if savepoint was longer than 128 characters
 * OB_ERR_TOO_MANY_SAVEPOINT - alive savepoint count out of limit (default 255)
 */
int create_explicit_savepoint(ObTxDesc &tx,
                              const ObString &savepoint,
                              const uint32_t session_id,
                              const bool user_create);

/**
 * rollback_to_implicit_savepoint - rollback to a implicit savepoint
 *
 * any savepoints created after it were released,
 * but precedents were kept
 *
 * rolling back to a savepoint may cause transaction state change:
 * 1) create a savepoint S1 when transaction in IDLE state
 * 2) do some modifies cause transaction into IMPLICIT_ACTIVE state
 * 3) rollback to savepoint S1 cause transaction into IDLE state
 * because after execute step 3), the transaction's modify set was empty
 *
 * @tx:                             the target transaction's descriptor
 * @savepoint:                      the target savepoint
 * @expire_ts:                      microseconds after which timeouted
 * @extra_touched_ls:               indicate the LogStreams which may
 *                                  touched by this transaction after
 *                                  the savepoint but not sensed by this
 *                                  transaction for some reason
 *                                  (eg. network partition, OutOfMemory)
 *
 * Return:
 * OB_SUCCESS             - OK
 * OB_TIMEOUT             - rollback operation timeouted
 * OB_TRANS_TIMEOUT       - transaction already timeout
 * OB_TRANS_NEED_ROLLBACK - transaction internal error, need user explicit
 *                          rollback
 */
int rollback_to_implicit_savepoint(ObTxDesc &tx,
                                   const ObTxSEQ savepoint,
                                   const int64_t expire_ts,
                                   const share::ObLSArray *extra_touched_ls);

/**
 * rollback_to_explicit_savepoint - rollback to a explicit savepoint
 *
 * the savepoint and precedents was kept,
 * but any savepoints created after it were released
 *
 * any snapshots registred via @register_tx_snapshot_verify were
 * verified and invalidated if this rollback cause it invalid.
 *
 * @tx:                             the target transaction's descriptor
 * @savepoint:                      the target savepoint name
 * @expire_ts:                      microseconds after which timeouted
 *
 * Return:
 * OB_SUCCESS             - OK
 * OB_TIMEOUT             - rollback operation timeouted
 * OB_SAVEPOINT_NOT_EXIST - if savepoint was not found
 */
int rollback_to_explicit_savepoint(ObTxDesc &tx,
                                   const ObString &savepoint,
                                   const int64_t expire_ts,
                                   const uint32_t session_id);

/**
 * release_explicit_savepoint - release savepoint
 *
 * savepoints created after this were also released
 *
 * @tx:                         the target transaction's descriptor
 * @savepoint:                  the savepoint to release
 *
 * Return:
 * OB_SUCCESS             - OK
 * OB_SAVEPOINT_NOT_EXIST - savepoint not found
 */
int release_explicit_savepoint(ObTxDesc &tx, const ObString &savepoint, const uint32_t session_id);

// ------------------------------------------------------------------
// savepoints stash
//
// caller can choose stash current active savepoints and restore them
// in later, this is used in PL/SQL function and SQL Trigger in MySQL
// mode
// ------------------------------------------------------------------
/**
 * create_stash_savepoint - create a special stash savepoint
 *
 * stash savepoint is a special savepint which will make savepoints
 * before it (and after previouse stash savepoint) invisible.
 *
 * to make them visible, call release_savepoint to release this
 * stash savepoint, which will release all savepoints created after
 * it and stashed savepoints by it to be visible.
 *
 * the effect of stash just like a stack:
 * 1) the stash_savepoint is the push part
 * 2) the release_savepoint is the pop part
 *
 * @tx:              txn descriptor
 * @name:            name of this stash operation
 * Return:
 * OB_SUCCESS - OK
 */
int create_stash_savepoint(ObTxDesc &tx, const ObString &name);

/**
 * stash_pop_savepoints - restore latest stashed savepoints
 *
 * this will release all currently active savepoints before
 * restore stashed ones
 *
 * if none stashed savepoints exist do nothing.
 *
 * @tx:                  txn descriptor
 * @name:                name of poped stash returned to
 *                       caller. if none stashed savepoints
 *                       special name '<Bottom>' returned
 *
 * Return:
 * OB_SUCCESS         - OK
 **/
int stash_pop_savepoints(ObTxDesc &tx, ObString &name);

// ------------------------------------------------------------------
// distributed transaction execution participant info collection
// ------------------------------------------------------------------

/**
 * merge_tx_state - merge trans state from other trans state
 *
 * used in SQL execution which parallel execution with copied private tx state
 *
 * @to:             dest transaction descriptor
 * @from:           src transaction descriptor
 *
 * Return:
 * OB_SUCCESS - OK
 */
int merge_tx_state(ObTxDesc &to, const ObTxDesc &from);

/**
 * get_tx_exec_result - get tx execution info of transaction
 *
 * used in SQL execution on Remote Node
 * to return incremental transaction state to transaction scheduler
 *
 * @tx:                 the transaction descriptor
 * @exec_info:          the execution result info returned
 *
 * Return:
 * OB_SUCCESS - OK
 */
int get_tx_exec_result(ObTxDesc &tx, ObTxExecResult &exec_info);

/**
 * add_tx_exec_result - report touched participant to transaction mananger
 *
 *
 * used in PX:
 * 1. on SQC remote Node:
 *    a. on each PX worker finished:
 *       txs.merge_tx_state(session.tx_desc_, px_worker.session.tx_desc_)
 *    b. on SQC finished:
 *       get transaction execution info:
 *       txs.get_tx_exec_result(session.tx_desc_, pxFinishMsg.tx_exec_info)
 * 2. on QC Node:
 *    txs.add_tx_exec_result(session.tx_desc_, pxFinishMsg.tx_exec_info)
 *
 * used in Remote Execution:
 * 1. on Remote Node (runner Node):
 *    on task finish:
 *    txs.get_tx_exec_result(session.tx_desc_, remoteResult.tx_exec_info)
 * 2. on Scheduler Node (control Node):
 *    txs.add_tx_exec_result(session.tx_desc_, remoteResult.tx_exec_info)
 *
 * @tx:                the transaction descriptor
 * @exec_info:         the transaction participants information,
 *                     generated by transaction manager during access
 *                     transactional storage, and gathered by calling:
 *                       get_tx_exec_result(tx, exec_info)
 *
 * Return:
 * OB_SUCCESS - OK
 */
int add_tx_exec_result(ObTxDesc &tx, const ObTxExecResult &exec_info);

/******************************************************************************
 * observer interface
 *
 * these function is small, and used for query/get/acquire txn's information
 * so, we call them 'observer interface'
 *****************************************************************************/
/**
 * is_tx_active - test txn is active or not
 *
 * if txn is in following state, we call it active:
 * 1) IDLE
 * 2) IMPLICIT_ACTIVE
 * 3) ACTIVE
 *
 * @tx_id:        Identify of the txn to test
 * @active:       return value
 *
 * Return:
 * OB_SUCCESS - test result is confident
 * OB_ERR_XXX - internal error which is unexpected arised
 */
int is_tx_active(const ObTransID &tx_id, bool &active);

/******************************************************************************
 * SQL relative hooks
 *
 * in case of participant in XA DTP, txn state is required to be coordinated
 * between tighly couple branchs around the SQL stmt
 * these hooks place on stmt start and end position to handle these works
 *****************************************************************************/
int sql_stmt_start_hook(const ObXATransID &xid,
                        ObTxDesc &tx,
                        const uint32_t session_id,
                        const uint32_t real_session_id);
int sql_stmt_end_hook(const ObXATransID &xid, ObTxDesc &tx);
