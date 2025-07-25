use std::{env, error::Error};

use tokio::task::JoinSet;

type StdError = Box<dyn Error + Send + Sync + 'static>;
type Result<T, E = StdError> = ::std::result::Result<T, E>;

use crate::worker_service_client::WorkerServiceClient;

tonic::include_proto!("worker");

/// master 做了什么？
/// 接收任务，比如文件列表
/// 将任务分配给worker进程，调用worker提供的处理任务的方法
/// map任务返回后，制作reduce任务，调用worker执行
#[tokio::main]
async fn main() -> Result<()> {
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
        let client = WorkerServiceClient::connect("http://[::1]:10002")
            .await?
            .max_decoding_message_size(10485760_usize)
            .max_encoding_message_size(10485760_usize);

        // MapReduce中的Job表示一次完整的map和reduce调用
        // 多个文件，每个文件都需要spawn一个map调用
        // 收集所有的map结果，之后对每个map结果中的key抽取出来进行reduce调用
        let mut map_tasks = JoinSet::new();
        for file_name in file_list {
            let mut client = client.clone();
            map_tasks.spawn(async move { client.map(MapTask { file_name }).await });
        }
        let output = map_tasks.join_all().await;
        for map_task in output {
            match map_task {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    println!("map result {}", inner.results.len());
                }
                Err(e) => {
                    eprintln!("map error: {e:?}");
                }
            }
        }

        // let reduce_task = ReduceTask {
        //     dst_file: "wc.txt".into(),
        //     map_result: Some(MapResult {
        //         results: all_map_result,
        //     }),
        // };
        // match client.reduce(reduce_task) {
        //     Ok(_) => println!("map/reduce task finished successfully"),
        //     Err(e) => eprintln!("reduce task failed: {e:?}"),
        // }
    }
    Ok(())
}
