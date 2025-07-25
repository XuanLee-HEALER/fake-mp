pub struct MapTask {
    pub file_name: String,
    pub file_data: String,
    pub status: TaskStatus,
}

pub enum TaskStatus {
    Idle,
    InProgress,
    Completed,
}
