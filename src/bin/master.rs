use std::{env, error::Error, fs::File, io::Read, thread};

use fake_mp::{MapTask, TaskStatus};

use crate::worker::{BlockingClient, MapResult, MapTask as RpcMapTask, ReduceTask};

type StdError = Box<dyn Error + Send + Sync + 'static>;
type Result<T, E = StdError> = ::std::result::Result<T, E>;

pub mod worker {

    use tokio::runtime::{Builder, Runtime};
    use tonic::transport;

    use crate::{StdError, worker::worker_service_client::WorkerServiceClient};

    tonic::include_proto!("worker");

    pub struct BlockingClient {
        client: WorkerServiceClient<transport::Channel>,
        rt: Runtime,
    }

    impl BlockingClient {
        pub fn connect<D>(dst: D) -> Result<Self, transport::Error>
        where
            D: TryInto<transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let rt = Builder::new_multi_thread().enable_all().build().unwrap();
            let client = rt.block_on(WorkerServiceClient::connect(dst))?;
            let client = client
                .max_decoding_message_size(10485760_usize)
                .max_encoding_message_size(10485760_usize);
            Ok(Self { client, rt })
        }

        pub fn map(
            &mut self,
            request: impl tonic::IntoRequest<MapTask>,
        ) -> Result<tonic::Response<MapResult>, tonic::Status> {
            self.rt.block_on(self.client.map(request))
        }

        pub fn reduce(
            &mut self,
            request: impl tonic::IntoRequest<ReduceTask>,
        ) -> Result<tonic::Response<ReduceResult>, tonic::Status> {
            self.rt.block_on(self.client.reduce(request))
        }
    }
}

/// master 做了什么？
/// 接收任务，比如文件列表
/// 将任务分配给worker进程，调用worker提供的处理任务的方法
/// map任务返回后，制作reduce任务，调用worker执行
fn main() -> Result<()> {
    let mut args = env::args();
    if args.len() == 1 {
        println!(r#"Usage: master[.exe] file1 [file2...]"#)
    } else {
        args.next();
        let file_list: Vec<String> = args.collect();
        assert!(
            file_list.len() > 0,
            "the provided file list must contain at least one item"
        );
        // 初始化客户端
        let mut client = BlockingClient::connect("http://[::1]:10002")?;
        // 对于每一个文件，我们都需要将其转换为一个Map任务
        // 这里使用线程来处理，每个线程处理一个文件，将其转换成任务
        // TODO 之后直接改成tokio的任务
        let mut all_map_result = vec![];
        for file in file_list {
            let mut buf = String::new();
            let mut f = File::open(&file).expect("failed to open the file");
            let _ = f
                .read_to_string(&mut buf)
                .expect("failed to read contents from file to buffer");
            let map_task = MapTask {
                file_name: file,
                file_data: buf,
                status: TaskStatus::Idle,
            };
            // 远程调用rpc，传递task
            match client.map(RpcMapTask {
                file_data: map_task.file_data.clone(),
                file_name: map_task.file_name.clone(),
            }) {
                Ok(resp) => {
                    let mut map_result = resp.into_inner();
                    all_map_result.append(&mut map_result.results);
                }
                Err(_) => eprintln!("map task failed"),
            };
        }

        let reduce_task = ReduceTask {
            dst_file: "wc.txt".into(),
            map_result: Some(MapResult {
                results: all_map_result,
            }),
        };
        match client.reduce(reduce_task) {
            Ok(_) => println!("map/reduce task finished successfully"),
            Err(e) => eprintln!("reduce task failed: {e:?}"),
        }
    }
    Ok(())
}
