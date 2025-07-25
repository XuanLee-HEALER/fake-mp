use tokio::runtime::Runtime;
use tonic::transport::Server;

use crate::worker::{Worker, worker_service_server::WorkerServiceServer};

pub mod worker {

    use std::{fs::File, io::Write};

    use tonic::{Request, Response, Status};

    use crate::worker::worker_service_server::WorkerService;

    tonic::include_proto!("worker");

    #[derive(Debug, Default)]
    pub struct Worker {}

    #[tonic::async_trait]
    impl WorkerService for Worker {
        async fn map(&self, request: Request<MapTask>) -> Result<Response<MapResult>, Status> {
            let contents = request.into_inner().file_data;
            let mut result = Vec::new();
            contents
                .split(|c: char| !c.is_ascii_alphabetic())
                .filter(|&v| !v.is_empty())
                .for_each(|v| {
                    result.push(MapResultEntry {
                        key: v.to_string(),
                        value: "1".to_string(),
                    });
                });
            Ok(Response::new(MapResult { results: result }))
        }

        async fn reduce(
            &self,
            request: Request<ReduceTask>,
        ) -> Result<Response<ReduceResult>, Status> {
            let req = request.into_inner();
            let mut map_res = req.map_result.unwrap();
            map_res.results.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            if map_res.results.len() <= 0 {
                eprintln!("no value in the task");
                return Ok(Response::new(ReduceResult { ok: true }));
            }
            match File::options()
                .create(true)
                .write(true)
                .truncate(true)
                .open(req.dst_file)
            {
                Ok(mut f) => {
                    let mut counter = 1;
                    let mut last = map_res.results[0].key.clone();
                    for word in map_res.results.into_iter().skip(1) {
                        if word.key == last {
                            counter += 1
                        } else {
                            if let Ok(_) = f.write_all(format!("{} {}\n", last, counter).as_bytes())
                            {
                                counter = 1;
                                last = word.key
                            } else {
                                eprintln!("failed to write result to wc.txt");
                                return Ok(Response::new(ReduceResult { ok: false }));
                            }
                        }
                    }
                    f.write_all(&format!("{} {}\n", last, counter).as_bytes())
                        .unwrap();
                    f.flush().unwrap();
                }
                Err(e) => {
                    eprintln!("failed to create the new file: {e:?}");
                    return Ok(Response::new(ReduceResult { ok: false }));
                }
            }
            Ok(Response::new(ReduceResult { ok: true }))
        }
    }
}

fn main() {
    let addr = "[::1]:10002".parse().unwrap();
    let worker = Worker::default();
    let rt = Runtime::new().unwrap();
    let server_fut = Server::builder()
        .add_service(
            WorkerServiceServer::new(worker)
                .max_decoding_message_size(10485760_usize)
                .max_encoding_message_size(10485760_usize),
        )
        .serve(addr);
    rt.block_on(server_fut)
        .expect("failed to successfully run the future on Runtime")
}
