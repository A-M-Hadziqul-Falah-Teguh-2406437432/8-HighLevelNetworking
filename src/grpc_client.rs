pub mod services {
    tonic::include_proto!("services");
}

use services::payment_service_client::PaymentServiceClient;
use services::PaymentRequest;
use services::transaction_service_client::TransactionServiceClient;
use services::TransactionRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "http://[::1]:50051";

    
    let mut payment_client = PaymentServiceClient::connect(addr).await?;
    let payment_request = tonic::Request::new(PaymentRequest {
        user_id: "user_123".to_string(),
        amount: 100.0,
    });
    let payment_response = payment_client.process_payment(payment_request).await?;
    println!("PAYMENT_RESPONSE={:?}", payment_response.into_inner());

    
    let mut transaction_client = TransactionServiceClient::connect(addr).await?;
    let transaction_request = tonic::Request::new(TransactionRequest {
        user_id: "user_123".to_string(),
    });

    let mut stream = transaction_client
        .get_transaction_history(transaction_request)
        .await?
        .into_inner();

    println!("Receiving transaction history stream...");
    while let Some(transaction) = stream.message().await? {
        println!("Transaction: {:?}", transaction);
    }

    Ok(())
}