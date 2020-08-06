use thiserror::*;

use super::*;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("Received a non request packet")]
    NonRequestPacket,
    #[error("An error occured when interpreting the command")]
    InterpretCommandError(#[from] #[source] InterpretCommandError),
    #[error("An error occured when reading the packet")]
    IoError(#[from] #[source] std::io::Error),
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum InterpretCommandError {
    #[error("Malformed packet(size: {0} < 9)")]
    MalformedPacket(u16),
    #[error("Read error")]
    IoError(#[from] #[source] std::io::Error),
    #[error("An error occured when processing the command")]
    ProcessRequestError(#[from] #[source] ProcessRequestError),
}

#[non_exhaustive]
#[derive(Debug, Error, Clone)]
pub enum ProcessRequestError {
    #[error("An error occured when logging in")]
    LoginError(#[from] #[source] LoginError),
    #[error("An error occured when creating the account")]
    CreateAccountError(#[from] #[source] CreateAccountError),
    #[error("An error occcured in `get_server_list`")]
    GetServerListError(#[source] RequestError),
    #[error("An error occcured in `get_world_list`")]
    GetWorldListError(#[source] RequestError),
    #[error("An error occcured in `get_create_room`")]
    CreateRoomError(#[source] RequestError),
    #[error("An error occcured in `join_room`")]
    JoinRoomError(#[source] RequestError),
    #[error("An error occcured in `leave_room`")]
    LeaveRoomError(#[source] RequestError),
    #[error("An error occcured in `search_room`")]
    SearchRoomError(#[source] RequestError),
    #[error("An error occcured in `set_room_data_external`")]
    SetRoomDataExternalError(#[source] RequestError),
    #[error("An error occcured in `get_room_data_internal`")]
    GetRoomDataInternalError(#[source] RequestError),
    #[error("An error occcured in `set_room_data_internal`")]
    SetRoomDataInternalError(#[source] RequestError),
    #[error("An error occcured in `ping_room_owner`")]
    PingRoomOwnerError(#[source] RequestError),
    #[error("User attempted an invalid command at this stage")]
    NotAuthorized,
    #[error("An error occcured in `ping_room_owner`")]
    InvalidCommand,
    #[error("A `Terminate` command was sent")]
    Terminate,
}

#[non_exhaustive]
#[derive(Debug, Error, Copy, Clone)]
pub enum LoginError {
    #[error("Error while extracting data")]
    MalformedInput,
    #[error("A database error occured: {0:?}")]
    DatabaseError(DbError)
}

#[non_exhaustive]
#[derive(Debug, Error, Clone)]
pub enum CreateAccountError {
    #[error("Error while extracting data")]
    MalformedInput,
    #[error("Account creation failed(npid: {1}) due to a db error: {0:?}")]
    DatabaseError(DbError, String)
}

#[non_exhaustive]
#[derive(Debug, Error, Copy, Clone)]
pub enum RequestError {
    #[error("Error while extracting data")]
    MalformedInput,
    #[error("The world was not found")]
    WorldNotFound,
    #[error("The room was not found")]
    RoomNotFound,
    #[error("User was not found")]
    UserNotFound,
    #[error("A database error occured: {0:?}")]
    DatabaseError(DbError)
}

impl From<LoginError> for ErrorType {
    fn from(err: LoginError) -> Self {
        use LoginError::*;
        match err {
            MalformedInput => ErrorType::Malformed,
            DatabaseError(_) => ErrorType::ErrorLogin,
        }
    }
}

impl From<CreateAccountError> for ErrorType {
    fn from(err: CreateAccountError) -> Self {
        use CreateAccountError::*;
        match err {
            MalformedInput => ErrorType::Malformed,
            DatabaseError(_, _) => ErrorType::ErrorCreate,
        }
    }
}

impl From<RequestError> for ErrorType {
    fn from(err: RequestError) -> Self {
        use RequestError::*;
        match err {
            MalformedInput => ErrorType::Malformed,
            WorldNotFound | UserNotFound | RoomNotFound => ErrorType::NotFound,
            DatabaseError(_) => ErrorType::DbFail,
        }
    }
}

impl ProcessRequestError {
    pub fn to_error_type(err: ProcessRequestError) -> Option<ErrorType> {
        use ProcessRequestError::*;
        match err {
            LoginError(err) => Some(err.into()),
            CreateAccountError(err) => Some(err.into()),
            GetServerListError(err)
            | GetWorldListError(err)
            | CreateRoomError(err)
            | JoinRoomError(err)
            | LeaveRoomError(err)
            | SearchRoomError(err)
            | SetRoomDataExternalError(err)
            | GetRoomDataInternalError(err)
            | SetRoomDataInternalError(err)
            | PingRoomOwnerError(err) => Some(err.into()),
            InvalidCommand | NotAuthorized => Some(ErrorType::Invalid),
            Terminate => None,
        }
    }
}
