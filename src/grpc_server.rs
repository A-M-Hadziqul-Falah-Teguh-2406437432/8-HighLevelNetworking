use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub mod services {
    tonic::include_proto!("services");
}

use services::payment_service_server::{PaymentService, PaymentServiceServer};
use services::{PaymentRequest, PaymentResponse};
use services::transaction_service_server::{TransactionService, TransactionServiceServer};
use services::{TransactionRequest, TransactionResponse};


#[derive(Default)]
pub struct MyPaymentService {}

#[tonic::async_trait]
impl PaymentService for MyPaymentService {
    async fn process_payment(
        &self,
        request: Request<PaymentRequest>,
    ) -> Result<Response<PaymentResponse>, Status> {
        println!("Received payment request: {:?}", request);
        Ok(Response::new(PaymentResponse { success: true }))
    }
}


#[derive(Default)]
pub struct MyTransactionService {}

#[tonic::async_trait]
impl TransactionService for MyTransactionService {
    type GetTransactionHistoryStream = ReceiverStream<Result<TransactionResponse, Status>>;

    async fn get_transaction_history(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<Self::GetTransactionHistoryStream>, Status> {
        println!("Received transaction history request: {:?}", request);

        // Membuat channel untuk streaming dengan buffer size 4
        let (tx, rx) = mpsc::channel(4);

        // Menjalankan simulasi pengiriman data di background task
        tokio::spawn(async move {
            for i in 0..30 { // Simulasi 30 record transaksi
                let response = TransactionResponse {
                    transaction_id: format!("trans_{}", i),
                    status: "Completed".to_string(),
                    amount: 100.0,
                    timestamp: "2022-01-01T12:00:00Z".to_string(),
                };

                if tx.send(Ok(response)).await.is_err() {
                    break;
                }

                // Simulasi delay setiap 10 record
                if i % 10 == 9 {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let payment_service = MyPaymentService::default();
    let transaction_service = MyTransactionService::default();

    println!("Server listening on {}", addr);

    Server::builder()
        .add_service(PaymentServiceServer::new(payment_service))
        .add_service(TransactionServiceServer::new(transaction_service))
        .serve(addr)
        .await?;

    Ok(())
}