use tonic::transport::Channel;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio::io::{self, AsyncBufReadExt};

pub mod services {
    tonic::include_proto!("services");
}

use services::payment_service_client::PaymentServiceClient;
use services::{PaymentRequest, TransactionRequest, ChatMessage};
use services::transaction_service_client::TransactionServiceClient;
use services::chat_service_client::ChatServiceClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "http://[::1]:50051";

    // 1. Test Payment (Unary)
    let mut p_client = PaymentServiceClient::connect(addr).await?;
    let p_res = p_client.process_payment(PaymentRequest { user_id: "u1".to_string(), amount: 100.0 }).await?;
    println!("PAYMENT RESPONSE={:?}", p_res.into_inner());

    // 2. Test Transaction (Server Streaming)
    let mut t_client = TransactionServiceClient::connect(addr).await?;
    let mut t_stream = t_client.get_transaction_history(TransactionRequest { user_id: "u1".to_string() }).await?.into_inner();
    while let Some(t) = t_stream.message().await? { println!("Transaction: {:?}", t); }

    // 3. Test Chat (Bi-Directional Streaming)
    let channel = Channel::from_static(addr).connect().await?;
    let mut c_client = ChatServiceClient::new(channel);
    let (tx, rx) = mpsc::channel(32);

    tokio::spawn(async move {
        let stdin = io::stdin();
        let mut reader = io::BufReader::new(stdin).lines();
        println!("Type messages for Chat Service (Ctrl+C to stop):");
        while let Ok(Some(line)) = reader.next_line().await {
            if !line.trim().is_empty() {
                let msg = ChatMessage { user_id: "user_123".to_string(), message: line };
                if tx.send(msg).await.is_err() { break; }
            }
        }
    });

    let mut c_stream = c_client.chat(ReceiverStream::new(rx)).await?.into_inner();
    while let Some(resp) = c_stream.message().await? {
        println!("Server says: {:?}", resp);
    }

    Ok(())
}