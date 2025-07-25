use fake_mp::Result;
use tonic::transport::Server;

use crate::worker::{Worker, worker_service_server::WorkerServiceServer};

pub mod worker {

    use fake_mp::{WcMapResult, wc_map, wc_reduce};
    use tokio::{fs::File, io::AsyncReadExt};
    use tonic::{Request, Response, Status};

    use crate::worker::worker_service_server::WorkerService;

    tonic::include_proto!("worker");

    #[derive(Debug, Default)]
    pub struct Worker {}

    #[tonic::async_trait]
    impl WorkerService for Worker {
        async fn map(&self, request: Request<MapTask>) -> Result<Response<MapResult>, Status> {
            let req = request.into_inner();
            let mut buf = Vec::new();
            let mut f = File::open(&req.file_name).await?;
            let _ = f.read_to_end(&mut buf).await?;
            let mut results: Vec<MapResultEntry> = wc_map(&req.file_name, buf)
                .await
                .map_err(|e| {
                    Status::new(
                        tonic::Code::Internal,
                        format!("failed to call [map]: {:?}", e),
                    )
                })?
                .into_iter()
                .map(|WcMapResult { k, v }| MapResultEntry { key: k, value: v })
                .collect();
            results.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            Ok(Response::new(MapResult { results }))
        }

        async fn reduce(
            &self,
            request: Request<ReduceTask>,
        ) -> Result<Response<ReduceResult>, Status> {
            let req = request.into_inner();
            wc_reduce(req.dst_file, req.k, req.v).await.map_err(|e| {
                Status::new(
                    tonic::Code::Internal,
                    format!("failed to call [reduce]: {:?}", e),
                )
            })?;
            Ok(Response::new(ReduceResult {}))
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "[::1]:10002".parse().unwrap();
    let worker = Worker::default();
    Server::builder()
        .add_service(
            WorkerServiceServer::new(worker)
                .max_decoding_message_size(10485760_usize)
                .max_encoding_message_size(10485760_usize),
        )
        .serve(addr)
        .await?;
    Ok(())
}
