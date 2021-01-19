//! Operations part of API implementation.

// Built-in uses

// External uses
use actix_web::{
    web::{self, Json},
    Scope,
};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::Value;

// Workspace uses
use zksync_storage::{chain::operations::records, ConnectionPool, QueryResult};
use zksync_types::BlockNumber;

// Local uses
use super::{
    blocks::BlockInfo,
    client::{Client, ClientError},
    transactions::TxReceipt,
    Error as ApiError, JsonResult,
};

/// Shared data between `api/v1/operations` endpoints.
#[derive(Debug, Clone)]
struct ApiOperationsData {
    pool: ConnectionPool,
}

impl ApiOperationsData {
    pub fn new(pool: ConnectionPool) -> Self {
        Self { pool }
    }

    pub async fn priority_op(&self, serial_id: u64) -> QueryResult<Option<PriorityOpReceipt>> {
        let mut storage = self.pool.access_storage().await?;

        let executed_op = storage
            .chain()
            .operations_schema()
            .get_executed_priority_operation(serial_id as u32)
            .await?;

        let executed_op = if let Some(executed_op) = executed_op {
            executed_op
        } else {
            return Ok(None);
        };

        let blocks = storage
            .chain()
            .block_schema()
            .load_block_range(executed_op.block_number as BlockNumber, 1)
            .await?;

        let block_info = blocks.into_iter().next().map(BlockInfo::from);

        let status = match block_info {
            None => TxReceipt::Pending,
            Some(info) if info.verify_tx_hash.is_some() => TxReceipt::Verified {
                block: info.block_number,
            },
            Some(info) if info.commit_tx_hash.is_some() => TxReceipt::Committed {
                block: info.block_number,
            },
            Some(_) => TxReceipt::Executed,
        };

        Ok(Some(PriorityOpReceipt {
            status,
            index: executed_op.block_index as u64,
        }))
    }

    pub async fn tx_and_priority_op(
        &self,
        op_type: i64,
        page: i64,
        page_size: i64,
    ) -> QueryResult<Vec<records::StoredExecutedTxAndPriorityOperation>> {
        let mut storage = self.pool.access_storage().await?;
        let mut real_page = page;
        let mut real_page_size = page_size;
        if real_page < 1 {
            real_page = 1
        }
        if real_page_size < 1 {
            real_page = 10
        }
        if real_page_size > 100 {
            real_page_size = 100
        }

        storage
            .chain()
            .operations_schema()
            .get_executed_tx_and_priority_operation(op_type, real_page, real_page_size)
            .await
    }

    pub async fn tx_and_priority_op_from(
        &self,
        op_type: i64,
        page: i64,
        page_size: i64,
        from: Vec<u8>,
    ) -> QueryResult<Vec<records::StoredExecutedTxAndPriorityOperation>> {
        let mut storage = self.pool.access_storage().await?;
        let mut real_page = page;
        let mut real_page_size = page_size;
        if real_page < 1 {
            real_page = 1
        }
        if real_page_size < 1 {
            real_page = 10
        }
        if real_page_size > 100 {
            real_page_size = 100
        }

        storage
            .chain()
            .operations_schema()
            .get_executed_tx_and_priority_operation_from(op_type, real_page, real_page_size, from)
            .await
    }
    pub async fn tx_and_priority_op_to(
        &self,
        op_type: i64,
        page: i64,
        page_size: i64,
        to: Vec<u8>,
    ) -> QueryResult<Vec<records::StoredExecutedTxAndPriorityOperation>> {
        let mut storage = self.pool.access_storage().await?;
        let mut real_page = page;
        let mut real_page_size = page_size;
        if real_page < 1 {
            real_page = 1
        }
        if real_page_size < 1 {
            real_page = 10
        }
        if real_page_size > 100 {
            real_page_size = 100
        }

        storage
            .chain()
            .operations_schema()
            .get_executed_tx_and_priority_operation_to(op_type, real_page, real_page_size, to)
            .await
    }

    pub async fn tx_and_priority_op_total(&self, op_type: i64) -> QueryResult<i64> {
        let mut storage = self.pool.access_storage().await?;

        storage
            .chain()
            .operations_schema()
            .get_executed_tx_and_priority_operation_total(op_type)
            .await
    }
    pub async fn tx_and_priority_op_total_from(
        &self,
        op_type: i64,
        addr: Vec<u8>,
    ) -> QueryResult<i64> {
        let mut storage = self.pool.access_storage().await?;

        storage
            .chain()
            .operations_schema()
            .get_executed_tx_and_priority_operation_total_from(op_type, addr)
            .await
    }
    pub async fn tx_and_priority_op_total_to(
        &self,
        op_type: i64,
        addr: Vec<u8>,
    ) -> QueryResult<i64> {
        let mut storage = self.pool.access_storage().await?;

        storage
            .chain()
            .operations_schema()
            .get_executed_tx_and_priority_operation_total_to(op_type, addr)
            .await
    }
}

// Data transfer objects.

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PriorityOpReceipt {
    #[serde(flatten)]
    pub status: TxReceipt,
    pub index: u64,
}

// Client implementation

/// Operations API part.
impl Client {
    /// Gets priority operation receipt.
    pub async fn priority_op(
        &self,
        serial_id: u64,
    ) -> Result<Option<PriorityOpReceipt>, ClientError> {
        self.get(&format!("operations/priority_op/{}", serial_id))
            .send()
            .await
    }
}

// Server implementation

async fn priority_op(
    data: web::Data<ApiOperationsData>,
    web::Path(serial_id): web::Path<u64>,
) -> JsonResult<Option<PriorityOpReceipt>> {
    let receipt = data
        .priority_op(serial_id)
        .await
        .map_err(ApiError::internal)?;

    Ok(Json(receipt))
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Default)]
pub struct ReqTxAndPriorityOp {
    page: i64,
    page_size: i64,
    op_type: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ExecutedTxAndPriorityOperation {
    //layer1
    pub priority_op_serialid: i64,
    pub deadline_block: i64,
    pub eth_block: i64,

    //layer2
    pub tx: Value,
    pub success: bool,
    pub fail_reason: Option<String>,
    pub primary_account_address: String,
    pub nonce: i64,
    pub eth_sign_data: Option<serde_json::Value>,
    pub batch_id: Option<i64>,

    //common
    pub eth_or_tx_hash: String,
    //eth_hash or tx_hash
    pub block_number: i64,
    pub block_index: Option<i32>,
    pub operation: Value,
    pub op_type: i64,
    pub from_account: String,
    pub to_account: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct RspExecutedTxAndPriorityOperation {
    pub total: i64,
    pub data: Vec<ExecutedTxAndPriorityOperation>,
}

impl From<records::StoredExecutedTxAndPriorityOperation> for ExecutedTxAndPriorityOperation {
    fn from(inner: records::StoredExecutedTxAndPriorityOperation) -> Self {
        Self {
            priority_op_serialid: inner.priority_op_serialid,
            deadline_block: inner.deadline_block,
            eth_block: inner.eth_block,
            tx: inner.tx,
            success: inner.success,
            fail_reason: inner.fail_reason,
            primary_account_address: format!(
                "{}{}",
                "0x",
                hex::encode(inner.primary_account_address)
            ),
            nonce: inner.nonce,
            eth_sign_data: inner.eth_sign_data,
            batch_id: inner.batch_id,
            eth_or_tx_hash: format!("{}{}", "0x", hex::encode(inner.eth_or_tx_hash)),
            block_number: inner.block_number,
            block_index: inner.block_index,
            operation: inner.operation,
            op_type: inner.op_type,
            from_account: format!("{}{}", "0x", hex::encode(inner.from_account)),
            to_account: (|account: Option<Vec<u8>>| -> Option<String> {
                match account {
                    Some(bts) => Some(format!("{}{}", "0x", hex::encode(bts))),
                    None => None,
                }
            })(inner.to_account),
            created_at: inner.created_at,
        }
    }
}

async fn tx_and_priority_op(
    data: web::Data<ApiOperationsData>,
    web::Query(req_param): web::Query<ReqTxAndPriorityOp>,
) -> JsonResult<RspExecutedTxAndPriorityOperation> {
    let receipt = data
        .tx_and_priority_op(req_param.op_type, req_param.page, req_param.page_size)
        .await
        .map_err(ApiError::internal)?;
    let total = data
        .tx_and_priority_op_total(req_param.op_type)
        .await
        .map_err(ApiError::internal)?;

    let rs_data = receipt
        .into_iter()
        .map(ExecutedTxAndPriorityOperation::from)
        .collect();

    let rs = RspExecutedTxAndPriorityOperation {
        total,
        data: rs_data,
    };

    Ok(Json(rs))
}

async fn tx_and_priority_op_from(
    data: web::Data<ApiOperationsData>,
    web::Path(addr_str): web::Path<String>,
    web::Query(req_param): web::Query<ReqTxAndPriorityOp>,
) -> JsonResult<RspExecutedTxAndPriorityOperation> {
    let addr = match hex::decode(addr_str.strip_prefix("0x").unwrap_or_default()) {
        Ok(a) => a,
        Err(_e) => vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    };

    let receipt = data
        .tx_and_priority_op_from(
            req_param.op_type,
            req_param.page,
            req_param.page_size,
            addr.clone(),
        )
        .await
        .map_err(ApiError::internal)?;
    let total = data
        .tx_and_priority_op_total_from(req_param.op_type, addr.clone())
        .await
        .map_err(ApiError::internal)?;

    let rs_data = receipt
        .into_iter()
        .map(ExecutedTxAndPriorityOperation::from)
        .collect();

    let rs = RspExecutedTxAndPriorityOperation {
        total,
        data: rs_data,
    };

    Ok(Json(rs))
}

async fn tx_and_priority_op_to(
    data: web::Data<ApiOperationsData>,
    web::Path(addr_str): web::Path<String>,
    web::Query(req_param): web::Query<ReqTxAndPriorityOp>,
) -> JsonResult<RspExecutedTxAndPriorityOperation> {
    let addr = match hex::decode(addr_str.strip_prefix("0x").unwrap_or_default()) {
        Ok(a) => a,
        Err(_e) => vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    };

    let receipt = data
        .tx_and_priority_op_to(
            req_param.op_type,
            req_param.page,
            req_param.page_size,
            addr.clone(),
        )
        .await
        .map_err(ApiError::internal)?;
    let total = data
        .tx_and_priority_op_total_to(req_param.op_type, addr.clone())
        .await
        .map_err(ApiError::internal)?;

    let rs_data = receipt
        .into_iter()
        .map(ExecutedTxAndPriorityOperation::from)
        .collect();

    let rs = RspExecutedTxAndPriorityOperation {
        total,
        data: rs_data,
    };

    Ok(Json(rs))
}

pub fn api_scope(pool: ConnectionPool) -> Scope {
    let data = ApiOperationsData::new(pool);

    web::scope("operations")
        .data(data)
        .route("priority_op/{id}", web::get().to(priority_op))
        .route("tx_and_priority_op", web::get().to(tx_and_priority_op))
        .route(
            "tx_and_priority_op/from/{addr}",
            web::get().to(tx_and_priority_op_from),
        )
        .route(
            "tx_and_priority_op/to/{addr}",
            web::get().to(tx_and_priority_op_to),
        )
}

#[cfg(test)]
mod tests {
    use super::{
        super::test_utils::{TestServerConfig, COMMITTED_OP_SERIAL_ID, VERIFIED_OP_SERIAL_ID},
        *,
    };

    #[actix_rt::test]
    async fn test_operations_scope() -> anyhow::Result<()> {
        let cfg = TestServerConfig::default();
        cfg.fill_database().await?;

        let (client, server) = cfg.start_server(|cfg| api_scope(cfg.pool.clone()));

        let requests = vec![
            (
                VERIFIED_OP_SERIAL_ID,
                Some(PriorityOpReceipt {
                    index: 2,
                    status: TxReceipt::Verified { block: 2 },
                }),
            ),
            (
                COMMITTED_OP_SERIAL_ID,
                Some(PriorityOpReceipt {
                    index: 1,
                    status: TxReceipt::Committed { block: 4 },
                }),
            ),
        ];

        for (serial_id, expected_op) in requests {
            assert_eq!(client.priority_op(serial_id).await?, expected_op);
        }

        server.stop().await;
        Ok(())
    }
}
