use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::num::{NonZeroU16, NonZeroU64};
use std::rc::Rc;
use mio::net::{TcpListener, TcpStream};
use mio::Poll;
use serde::{Deserialize, Serialize};
use crate::ringbuf::RingBuf;

// Login message is smaller than other messages
// to prevent exhausting memory by clients that aren't logged in.
pub const MAX_LOGIN_MSG_BODY_SIZE: usize = 512;
pub const MAX_MSG_BODY_SIZE: usize = 10_000_000;
// Maximum number of services a single client can subscribe to.
pub const MAX_SUBSCRIPTIONS: usize = 32;
pub const MAX_SERVICE_NAME_LEN: usize = 64;
// Maximum number of clients (including services) that can connect to router at the same time.
// Further connections are not accepted.
pub const MAX_CLIENTS: usize = 512;
// Maximum number of messages that can be waiting in a queue to be sent to a single client.
// Trying to enqueue an additional message causes disconnect of the client.
pub const MAX_PENDING_MESSAGES: usize = 5000;  // Must be at least `MAX_CLIENTS`.
// Maximum number of responses that can be awaited from a single client.
// Trying to await an additional response causes disconnect of the client.
// In theory, when a service starts at most `MAX_CLIENTS - 1` clients can be subscribed
// to it. This means we have to be able to store `MAX_CLIENTS - 1`
// awaited responses `ClientAddedToService`. Additionally, immediately after
// the service transitions to `READY` state it may get `PING` so we have to store
// awaited response `PONG`.
pub const MAX_AWAITED_RESPONSES: usize = MAX_CLIENTS;  // Must be at least `MAX_CLIENTS`.
pub const INTERVAL_BETWEEN_PINGS_US: u64 = 5_000_000;
pub const INTERVAL_BETWEEN_CHECKING_RESPONSES_US: u64 = 5_000_000;
pub const INTERVAL_BETWEEN_CHECKING_AND_LOGGING_CLIENTS_US: u64 = 15_000_000;
pub const MAX_RESPONSE_TIME_US: u64 = 10_000_000;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub enum ClientState {
    // The client must send a login message.
    // Sending anything else causes disconnect of the client.
    // Nothing is sent to clients in this state.
    WaitingForLogin,

    // The client has successfully logged in, and router
    // has determined whether it's an ordinary client or a service.
    Ready,

    // The client will be disconnected from the router.
    // When in this state, the client's socket is still open and registered in epoll.
    // We must notify other clients and services that are not `WaitingForClose`
    // that this client no longer exists.
    // Nothing is sent to clients in this state.
    WaitingForClose,
}

// Message waiting to be sent.
pub struct SharedPendingMessage {
    pub header: MsgHeader,
    pub body: Vec<u8>,
}

// Since 1970.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Default)]
#[repr(transparent)]
pub struct UsTime(pub u64);

pub struct AwaitedResponse {
    // One of: Login, ClientAddedToService, ClientRemovedFromService, Pong.
    pub msg_type: MsgType,
    // When the original request has been enqueued by router.
    // This is also used to determine when the response should arrive.
    // In the case of Pong, its `header.enqueued_by_router_us` must be equal to this timestamp.
    pub request_enqueued_by_router: UsTime,
    // Instance id of the added or removed client or 0.
    pub instance_id: ClientInstanceIdOrNone,
}

pub struct Client {
    // TODO: Add context for TLS encryption.

    pub instance_id: ClientInstanceId,
    pub state: ClientState,
    pub socket: TcpStream,
    pub addr: SocketAddr,

    // To which services this client has subscribed.
    // A service is added to this set immediately after the router receives `SubscribeToService`
    // from the client. Services may not even be connected to the router.
    pub subscribed_services: BTreeSet<ServiceId>,

    // Services where `AddClientToService` was enqueued.
    // When this client disconnects, we must notify these services.
    pub notified_services: BTreeSet<ClientInstanceId>,

    // Ensures that the client is alive and responds promptly.
    // Responses must arrive in the same order as the requests.
    pub awaited_responses: RingBuf<AwaitedResponse, MAX_AWAITED_RESPONSES>,

    // For receiving messages from this client.

    pub received_from_header: usize,
    pub header: MsgHeader,
    pub received_from_body: usize,
    pub body: Vec<u8>,

    // For sending messages to this client.

    // `sent_from_message` is the number of bytes from the first message
    // in `pending_messages` queue that have been sent.
    pub sent_from_message: usize,
    pub pending_messages: RingBuf<Rc<SharedPendingMessage>, MAX_PENDING_MESSAGES>,

    // --------------------------------------------------------------
    // Following fields are used only if this client is a service.
    // --------------------------------------------------------------

    pub service_id: ServiceIdOrNone,

    // Clients where `ConnectedToService` was enqueued.
    // When this service disconnects, we must notify these clients.
    //
    // Additionally, since the router enqueues `ConnectedToService` to a client
    // immediately after receiving `ClientAddedToService` from a service,
    // this set also contains all clients where the service acknowledged adding the client.
    //
    // This set is also useful when service broadcasts messages.
    // It broadcasts to all clients in `notified_clients`.
    pub notified_clients: BTreeSet<ClientInstanceId>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
#[repr(transparent)]
pub struct ServiceId(pub NonZeroU16);

pub type ServiceIdOrNone = Option<ServiceId>;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
#[repr(transparent)]
pub struct ClientInstanceId(pub NonZeroU64);

pub type ClientInstanceIdOrNone = Option<ClientInstanceId>;

pub struct Router {
    // Listening socket.
    pub server_socket: TcpListener,

    pub poll: Poll,

    // Interning service names.
    pub service_id_to_name: BTreeMap<ServiceId, String>,
    pub service_name_to_id: BTreeMap<String, ServiceId>,

    // Currently connected clients.
    pub clients: BTreeMap<ClientInstanceId, Client>,
    pub total_clients_connected: u64,  // Used for generating instance ids.

    // The last time when router sent pings.
    pub pings_sent: UsTime,
    // The last time when router checked awaited responses.
    pub responses_checked: UsTime,
    // The last time when router checked invariants of existing clients and logging existing clients.
    pub clients_checked_and_logged: UsTime,

    // Queue of clients in `WAITING_FOR_CLOSE` state.
    // Closing one client may switch another client to `WAITING_FOR_CLOSE` state.
    pub clients_waiting_for_close: BTreeSet<ClientInstanceId>,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
#[repr(u16)]
pub enum MsgType {
    // Sent as the first message from each client to router.
    // The message body contains the client's username, password and service name.
    // Service name is empty for non-service clients.
    Login = 0,

    // Sent from a client to router when the client
    // wants to receive messages from a service.
    // The message body contains the service name.
    SubscribeToService = 1,

    // Sent from router to a client when the client is connected to or disconnected from a service.
    // The header contains `instance_id` of the service.
    // The message body contains the service name.
    ConnectedToService = 2,
    DisconnectedFromService = 3,

    // Sent from router to a service to add or remove a client.
    // The header contains `instance_id` of the client.
    AddClientToService = 4,
    RemoveClientFromService = 5,

    // Sent from a service to router as confirmation
    // that the service has added or removed a client.
    // The header contains `instance_id` of the client.
    ClientAddedToService = 6,
    ClientRemovedFromService = 7,

    // Sent from a service to router and from router to all clients connected to the service.
    // When sent from a service to router, the header contains `instance_id` 0.
    // When sent from router to clients, the header contains `instance_id` of the service.
    MsgToAllClients = 8,
    // Sent from a service to router and from router to a single client connected to the service.
    // When sent from a service to router, the header contains `instance_id` of the client.
    // When sent from router to the client, the header contains `instance_id` of the service.
    MsgToOneClient = 9,

    // Sent from a client to router and from router to a service.
    // When sent from a client, the header contains `instance_id` of the destination service.
    // When sent from router, the header contains `instance_id` of the originating client.
    Request = 10,

    // Sent from router to a client.
    Ping = 11,
    // Sent from a client to router.
    // The header contains `enqueued_by_router_us` from `Ping`.
    Pong = 12,
}

#[repr(C, packed)]
pub struct MsgHeader {
    // For messages from router, `enqueued_by_router_us` contains the timestamp when router
    // enqueued the message for sending.
    // For messages from clients, it is zero except for `Pong`.
    pub enqueued_by_router: UsTime,
    // It can't be `MsgType` because when received it may contain values which are not valid `MsgType`.
    pub msg_type: u16,
    pub flags: u16,
    pub body_size: u32,
    pub instance_id: ClientInstanceIdOrNone,  // 0 when not used.
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub host_and_port: String,
}
