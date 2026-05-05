pub mod services {
    tonic::include_proto!("services");
}

use services::payment_service_client::PaymentServiceClient;
use services::PaymentRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Inisialisasi Client
    let mut client = PaymentServiceClient::connect("http://[::1]:50051").await?;

    // Membuat dan mengirim request
    let request = tonic::Request::new(PaymentRequest {
        user_id: "user_123".to_string(),
        amount: 100.0,
    });

    let response = client.process_payment(request).await?;
    println!("RESPONSE={:?}", response.into_inner());

    Ok(())
}