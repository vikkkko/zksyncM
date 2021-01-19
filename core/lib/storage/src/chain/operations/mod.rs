// Built-in deps
use std::time::Instant;

// External imports
use anyhow::format_err;

use zksync_basic_types::H256;
// Workspace imports
use zksync_types::{ethereum::CompleteWithdrawalsTx, tx::TxHash, ActionType, BlockNumber};

use crate::{chain::mempool::MempoolSchema, QueryResult, StorageProcessor};

// Local imports
use self::records::{
    NewExecutedPriorityOperation, NewExecutedTransaction, NewExecutedTxAndPriorityOperation,
    NewOperation, StoredCompleteWithdrawalsTransaction, StoredExecutedPriorityOperation,
    StoredExecutedTransaction, StoredExecutedTxAndPriorityOperation, StoredOperation,
    StoredPendingWithdrawal,
};

pub mod records;

/// Operations schema is capable of storing and loading the transactions.
/// Every kind of transaction (non-executed, executed, and executed priority tx)
/// can be either saved or loaded from the database.
#[derive(Debug)]
pub struct OperationsSchema<'a, 'c>(pub &'a mut StorageProcessor<'c>);

impl<'a, 'c> OperationsSchema<'a, 'c> {
    pub async fn get_last_block_by_action(
        &mut self,
        action_type: ActionType,
        confirmed: Option<bool>,
    ) -> QueryResult<BlockNumber> {
        let start = Instant::now();
        let max_block = sqlx::query!(
            r#"SELECT max(block_number) FROM operations WHERE action_type = $1 AND confirmed IS DISTINCT FROM $2"#,
            action_type.to_string(),
            confirmed.map(|value| !value)
        )
            .fetch_one(self.0.conn())
            .await?
            .max
            .unwrap_or(0);

        metrics::histogram!(
            "sql.chain.operations.get_last_block_by_action",
            start.elapsed()
        );
        Ok(max_block as BlockNumber)
    }

    pub async fn get_operation(
        &mut self,
        block_number: BlockNumber,
        action_type: ActionType,
    ) -> Option<StoredOperation> {
        let start = Instant::now();
        let result = sqlx::query_as!(
            StoredOperation,
            "SELECT * FROM operations WHERE block_number = $1 AND action_type = $2",
            i64::from(block_number),
            action_type.to_string()
        )
        .fetch_optional(self.0.conn())
        .await
        .ok()
        .flatten();

        metrics::histogram!("sql.chain.operations.get_operation", start.elapsed());
        result
    }

    pub async fn get_executed_operation(
        &mut self,
        op_hash: &[u8],
    ) -> QueryResult<Option<StoredExecutedTransaction>> {
        let start = Instant::now();
        let op = sqlx::query_as!(
            StoredExecutedTransaction,
            "SELECT * FROM executed_transactions WHERE tx_hash = $1",
            op_hash
        )
        .fetch_optional(self.0.conn())
        .await?;

        metrics::histogram!(
            "sql.chain.operations.get_executed_operation",
            start.elapsed()
        );
        Ok(op)
    }

    pub async fn get_executed_priority_operation(
        &mut self,
        priority_op_id: u32,
    ) -> QueryResult<Option<StoredExecutedPriorityOperation>> {
        let start = Instant::now();
        let op = sqlx::query_as!(
            StoredExecutedPriorityOperation,
            "SELECT * FROM executed_priority_operations WHERE priority_op_serialid = $1",
            i64::from(priority_op_id)
        )
        .fetch_optional(self.0.conn())
        .await?;

        metrics::histogram!(
            "sql.chain.operations.get_executed_priority_operation",
            start.elapsed()
        );
        Ok(op)
    }

    pub async fn get_executed_tx_and_priority_operation(
        &mut self,
        op_type: i64,
        page: i64,
        page_size: i64,
    ) -> QueryResult<Vec<StoredExecutedTxAndPriorityOperation>> {
        let start = Instant::now();
        let op;
        if op_type <= 0 {
            op = sqlx::query_as!(
            StoredExecutedTxAndPriorityOperation,
            "SELECT * FROM executed_tx_and_priority_operations order by created_at desc limit $1 offset $2",
            page_size,
            (page-1) * page_size
            )
                .fetch_all(self.0.conn())
                .await?;
        } else {
            op = sqlx::query_as!(
            StoredExecutedTxAndPriorityOperation,
            "SELECT * FROM executed_tx_and_priority_operations WHERE op_type = $1 order by created_at desc limit $2 offset $3",
            op_type,
            page_size,
            (page-1) * page_size
            )
                .fetch_all(self.0.conn())
                .await?;
        }

        metrics::histogram!(
            "sql.chain.operations.get_executed_priority_operation",
            start.elapsed()
        );
        Ok(op)
    }

    pub async fn get_executed_tx_and_priority_operation_from(
        &mut self,
        op_type: i64,
        page: i64,
        page_size: i64,
        from: Vec<u8>,
    ) -> QueryResult<Vec<StoredExecutedTxAndPriorityOperation>> {
        let start = Instant::now();
        let op;
        if op_type <= 0 {
            op = sqlx::query_as!(
            StoredExecutedTxAndPriorityOperation,
            "SELECT * FROM executed_tx_and_priority_operations where from_account = $1 order by created_at desc limit $2 offset $3",
            from,
            page_size,
            (page-1) * page_size
            )
                .fetch_all(self.0.conn())
                .await?;
        } else {
            op = sqlx::query_as!(
            StoredExecutedTxAndPriorityOperation,
            "SELECT * FROM executed_tx_and_priority_operations WHERE from_account = $1 and op_type = $2 order by created_at desc limit $3 offset $4",
            from,
            op_type,
            page_size,
            (page-1) * page_size
            )
                .fetch_all(self.0.conn())
                .await?;
        }

        metrics::histogram!(
            "sql.chain.operations.get_executed_priority_operation",
            start.elapsed()
        );
        Ok(op)
    }

    pub async fn get_executed_tx_and_priority_operation_to(
        &mut self,
        op_type: i64,
        page: i64,
        page_size: i64,
        to: Vec<u8>,
    ) -> QueryResult<Vec<StoredExecutedTxAndPriorityOperation>> {
        let start = Instant::now();
        let op;
        if op_type <= 0 {
            op = sqlx::query_as!(
            StoredExecutedTxAndPriorityOperation,
            "SELECT * FROM executed_tx_and_priority_operations where to_account = $1 order by created_at desc limit $2 offset $3",
            to,
            page_size,
            (page-1) * page_size
            )
                .fetch_all(self.0.conn())
                .await?;
        } else {
            op = sqlx::query_as!(
            StoredExecutedTxAndPriorityOperation,
            "SELECT * FROM executed_tx_and_priority_operations WHERE to_account = $1 and op_type = $2 order by created_at desc limit $3 offset $4",
            to,
            op_type,
            page_size,
            (page-1) * page_size
            )
                .fetch_all(self.0.conn())
                .await?;
        }

        metrics::histogram!(
            "sql.chain.operations.get_executed_priority_operation",
            start.elapsed()
        );
        Ok(op)
    }

    pub async fn get_executed_tx_and_priority_operation_total(
        &mut self,
        op_type: i64,
    ) -> QueryResult<i64> {
        let start = Instant::now();
        let op;
        if op_type <= 0 {
            op = sqlx::query!("SELECT COUNT(*) FROM executed_tx_and_priority_operations")
                .fetch_one(self.0.conn())
                .await?
                .count
                .unwrap_or(0);
        } else {
            op = sqlx::query!(
                "SELECT COUNT(*) FROM executed_tx_and_priority_operations WHERE op_type = $1",
                op_type
            )
            .fetch_one(self.0.conn())
            .await?
            .count
            .unwrap_or(0);
        }

        metrics::histogram!(
            "sql.chain.operations.get_executed_priority_operation",
            start.elapsed()
        );
        Ok(op)
    }

    pub async fn get_executed_tx_and_priority_operation_total_from(
        &mut self,
        op_type: i64,
        from: Vec<u8>,
    ) -> QueryResult<i64> {
        let start = Instant::now();
        let op;
        if op_type <= 0 {
            op = sqlx::query!(
                "SELECT COUNT(*) FROM executed_tx_and_priority_operations where from_account = $1",
                from
            )
            .fetch_one(self.0.conn())
            .await?
            .count
            .unwrap_or(0);
        } else {
            op = sqlx::query!(
            "SELECT COUNT(*) FROM executed_tx_and_priority_operations WHERE op_type = $1 and from_account = $2",
            op_type,
            from
            )
                .fetch_one(self.0.conn())
                .await?
                .count
                .unwrap_or(0);
        }

        metrics::histogram!(
            "sql.chain.operations.get_executed_priority_operation",
            start.elapsed()
        );
        Ok(op)
    }

    pub async fn get_executed_tx_and_priority_operation_total_to(
        &mut self,
        op_type: i64,
        to: Vec<u8>,
    ) -> QueryResult<i64> {
        let start = Instant::now();
        let op;
        if op_type <= 0 {
            op = sqlx::query!(
                "SELECT COUNT(*) FROM executed_tx_and_priority_operations where to_account = $1",
                to
            )
            .fetch_one(self.0.conn())
            .await?
            .count
            .unwrap_or(0);
        } else {
            op = sqlx::query!(
            "SELECT COUNT(*) FROM executed_tx_and_priority_operations WHERE op_type = $1 and to_account = $2",
            op_type,
            to
            )
                .fetch_one(self.0.conn())
                .await?
                .count
                .unwrap_or(0);
        }

        metrics::histogram!(
            "sql.chain.operations.get_executed_priority_operation",
            start.elapsed()
        );
        Ok(op)
    }

    pub async fn get_executed_priority_operation_by_hash(
        &mut self,
        eth_hash: &[u8],
    ) -> QueryResult<Option<StoredExecutedPriorityOperation>> {
        let start = Instant::now();
        let op = sqlx::query_as!(
            StoredExecutedPriorityOperation,
            "SELECT * FROM executed_priority_operations WHERE eth_hash = $1",
            eth_hash
        )
        .fetch_optional(self.0.conn())
        .await?;

        metrics::histogram!(
            "sql.chain.operations.get_executed_priority_operation_by_hash",
            start.elapsed()
        );
        Ok(op)
    }

    pub(crate) async fn store_operation(
        &mut self,
        operation: NewOperation,
    ) -> QueryResult<StoredOperation> {
        let start = Instant::now();
        let op = sqlx::query_as!(
            StoredOperation,
            "INSERT INTO operations (block_number, action_type) VALUES ($1, $2)
            RETURNING *",
            operation.block_number,
            operation.action_type
        )
        .fetch_one(self.0.conn())
        .await?;
        metrics::histogram!("sql.chain.operations.store_operation", start.elapsed());
        Ok(op)
    }

    pub(crate) async fn confirm_operation(
        &mut self,
        block_number: BlockNumber,
        action_type: ActionType,
    ) -> QueryResult<()> {
        let start = Instant::now();
        sqlx::query!(
            "UPDATE operations
                SET confirmed = $1
                WHERE block_number = $2 AND action_type = $3",
            true,
            i64::from(block_number),
            action_type.to_string()
        )
        .execute(self.0.conn())
        .await?;
        metrics::histogram!("sql.chain.operations.confirm_operation", start.elapsed());
        Ok(())
    }

    /// Stores the executed transaction in the database.
    pub(crate) async fn store_executed_tx(
        &mut self,
        operation: NewExecutedTransaction,
    ) -> QueryResult<()> {
        let start = Instant::now();
        let mut transaction = self.0.start_transaction().await?;

        MempoolSchema(&mut transaction)
            .remove_tx(&operation.tx_hash)
            .await?;

        if operation.success {
            // If transaction succeed, it should replace the stored tx with the same hash.
            // The situation when a duplicate tx is stored in the database may exist only if has
            // failed previously.
            // Possible scenario: user had no enough funds for transfer, then deposited some and
            // sent the same transfer again.

            sqlx::query!(
                "INSERT INTO executed_transactions (block_number, block_index, tx, operation, tx_hash, from_account, to_account, success, fail_reason, primary_account_address, nonce, created_at, eth_sign_data, batch_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (tx_hash)
                DO UPDATE
                SET block_number = $1, block_index = $2, tx = $3, operation = $4, tx_hash = $5, from_account = $6, to_account = $7, success = $8, fail_reason = $9, primary_account_address = $10, nonce = $11, created_at = $12, eth_sign_data = $13, batch_id = $14",
                operation.block_number,
                operation.block_index,
                operation.tx,
                operation.operation,
                operation.tx_hash,
                operation.from_account,
                operation.to_account,
                operation.success,
                operation.fail_reason,
                operation.primary_account_address,
                operation.nonce,
                operation.created_at,
                operation.eth_sign_data,
                operation.batch_id,
            )
                .execute(transaction.conn())
                .await?;
        } else {
            // If transaction failed, we do nothing on conflict.
            sqlx::query!(
                "INSERT INTO executed_transactions (block_number, block_index, tx, operation, tx_hash, from_account, to_account, success, fail_reason, primary_account_address, nonce, created_at, eth_sign_data, batch_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (tx_hash)
                DO NOTHING",
                operation.block_number,
                operation.block_index,
                operation.tx,
                operation.operation,
                operation.tx_hash,
                operation.from_account,
                operation.to_account,
                operation.success,
                operation.fail_reason,
                operation.primary_account_address,
                operation.nonce,
                operation.created_at,
                operation.eth_sign_data,
                operation.batch_id,
            )
                .execute(transaction.conn())
                .await?;
        };

        transaction.commit().await?;
        metrics::histogram!("sql.chain.operations.store_executed_tx", start.elapsed());
        Ok(())
    }

    /// Stores executed priority operation in database.
    ///
    /// This method is made public to fill the database for tests, do not use it for
    /// any other purposes.
    #[doc = "hidden"]
    pub async fn store_executed_priority_op(
        &mut self,
        operation: NewExecutedPriorityOperation,
    ) -> QueryResult<()> {
        let start = Instant::now();
        sqlx::query!(
            "INSERT INTO executed_priority_operations (block_number, block_index, operation, from_account, to_account, priority_op_serialid, deadline_block, eth_hash, eth_block, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (priority_op_serialid)
            DO NOTHING",
            operation.block_number,
            operation.block_index,
            operation.operation,
            operation.from_account,
            operation.to_account,
            operation.priority_op_serialid,
            operation.deadline_block,
            operation.eth_hash,
            operation.eth_block,
            operation.created_at,
        )
            .execute(self.0.conn())
            .await?;
        metrics::histogram!(
            "sql.chain.operations.store_executed_priority_op",
            start.elapsed()
        );
        Ok(())
    }

    // //layer1
    // pub priority_op_serialid: i64,
    // pub deadline_block: i64,
    // pub eth_block: i64,
    //
    // //layer2
    // pub tx: Value,
    // pub success: bool,
    // pub fail_reason: Option<String>,
    // pub primary_account_address: Vec<u8>,
    // pub nonce: i64,
    // pub eth_sign_data: Option<serde_json::Value>,
    // pub batch_id: Option<i64>,
    //
    // //common
    // pub eth_or_tx_hash: Vec<u8>,//eth_hash or tx_hash
    // pub block_number: i64,
    // pub block_index: Option<i32>,
    // pub operation: Value,
    // pub from_account: Vec<u8>,
    // pub to_account: Option<Vec<u8>>,
    // pub created_at: DateTime<Utc>,
    /// Stores executed tx and  priority operation in database.
    #[doc = "hidden"]
    pub async fn store_executed_tx_and_priority_op(
        &mut self,
        operation: NewExecutedTxAndPriorityOperation,
    ) -> QueryResult<()> {
        let start = Instant::now();
        sqlx::query!(
                    "INSERT INTO executed_tx_and_priority_operations (priority_op_serialid, deadline_block, eth_block, tx, success, fail_reason, primary_account_address, nonce, eth_sign_data, batch_id, eth_or_tx_hash, block_number, block_index, operation, op_type, from_account, to_account, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                    ON CONFLICT (eth_or_tx_hash)
                    DO NOTHING",
                    operation.priority_op_serialid,
                    operation.deadline_block,
                    operation.eth_block,
                    operation.tx,
                    operation.success,
                    operation.fail_reason,
                    operation.primary_account_address,
                    operation.nonce,
                    operation.eth_sign_data,
                    operation.batch_id,
                    operation.eth_or_tx_hash,
                    operation.block_number,
                    operation.block_index,
                    operation.operation,
                    operation.op_type,
                    operation.from_account,
                    operation.to_account,
                    operation.created_at,
                    )
            .execute(self.0.conn())
            .await?;
        metrics::histogram!(
            "sql.chain.operations.store_executed_tx_and_priority_op",
            start.elapsed()
        );

        Ok(())
    }

    /// Parameter id should be None if id equals to the (maximum stored id + 1)
    pub async fn add_pending_withdrawal(
        &mut self,
        hash: &TxHash,
        id: Option<i64>,
    ) -> QueryResult<()> {
        let start = Instant::now();
        let pending_withdrawal_id = match id {
            Some(id) => id,
            None => {
                let max_stored_pending_withdrawal_id =
                    sqlx::query!("SELECT max(id) from pending_withdrawals",)
                        .fetch_one(self.0.conn())
                        .await?
                        .max
                        .ok_or_else(|| format_err!("there is no pending withdrawals in the db"))?;

                max_stored_pending_withdrawal_id + 1
            }
        };
        sqlx::query!(
            "INSERT INTO pending_withdrawals (id, withdrawal_hash)
            VALUES ($1, $2)
            ON CONFLICT (id)
            DO UPDATE
            SET id = $1, withdrawal_hash = $2",
            pending_withdrawal_id,
            hash.as_ref().to_vec(),
        )
        .execute(self.0.conn())
        .await?;
        metrics::histogram!(
            "sql.chain.operations.add_pending_withdrawal",
            start.elapsed()
        );
        Ok(())
    }

    pub async fn add_complete_withdrawals_transaction(
        &mut self,
        tx: CompleteWithdrawalsTx,
    ) -> QueryResult<()> {
        let start = Instant::now();
        sqlx::query!(
            "INSERT INTO complete_withdrawals_transactions (tx_hash, pending_withdrawals_queue_start_index, pending_withdrawals_queue_end_index)
            VALUES ($1, $2, $3)
            ON CONFLICT (tx_hash)
            DO UPDATE
            SET tx_hash = $1, pending_withdrawals_queue_start_index = $2, pending_withdrawals_queue_end_index = $3",
            tx.tx_hash.as_bytes().to_vec(),
            tx.pending_withdrawals_queue_start_index as i64,
            tx.pending_withdrawals_queue_end_index as i64,
        )
            .execute(self.0.conn())
            .await?;
        metrics::histogram!(
            "sql.chain.operations.add_complete_withdrawals_transaction",
            start.elapsed()
        );
        Ok(())
    }

    pub async fn no_stored_pending_withdrawals(&mut self) -> QueryResult<bool> {
        let stored_pending_withdrawals =
            sqlx::query!(r#"SELECT COUNT(*) as "count!" FROM pending_withdrawals"#,)
                .fetch_one(self.0.conn())
                .await?
                .count;

        Ok(stored_pending_withdrawals == 0)
    }

    pub async fn eth_tx_for_withdrawal(
        &mut self,
        withdrawal_hash: &TxHash,
    ) -> QueryResult<Option<H256>> {
        let start = Instant::now();
        let pending_withdrawal = sqlx::query_as!(
            StoredPendingWithdrawal,
            "SELECT * FROM pending_withdrawals WHERE withdrawal_hash = $1
            LIMIT 1",
            withdrawal_hash.as_ref().to_vec(),
        )
        .fetch_optional(self.0.conn())
        .await?;

        let res = match pending_withdrawal {
            Some(pending_withdrawal) => {
                let pending_withdrawal_id = pending_withdrawal.id;

                sqlx::query_as!(
                    StoredCompleteWithdrawalsTransaction,
                    "SELECT * FROM complete_withdrawals_transactions
                        WHERE pending_withdrawals_queue_start_index <= $1
                            AND $1 < pending_withdrawals_queue_end_index
                    LIMIT 1
                    ",
                    pending_withdrawal_id,
                )
                .fetch_optional(self.0.conn())
                .await?
                .map(|complete_withdrawals_transaction| {
                    H256::from_slice(&complete_withdrawals_transaction.tx_hash)
                })
            }
            None => None,
        };

        metrics::histogram!(
            "sql.chain.operations.eth_tx_for_withdrawal",
            start.elapsed()
        );
        Ok(res)
    }
}
