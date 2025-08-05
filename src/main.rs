mod ringbuf;
mod types;
mod funcs;
mod process_msg;

use std::collections::{BTreeMap, BTreeSet};
use std::{fs, io, panic};
use std::io::{Read, Write};
use std::net::ToSocketAddrs;
use std::num::NonZeroU64;
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut};
use std::rc::Rc;
use std::time::Duration;
use log::{error, info, warn};
use mio::net::{TcpListener, TcpStream};
use mio::Poll;
use crate::process_msg::process_msg;
use crate::ringbuf::RingBuf;
use crate::types::{AwaitedResponse, Client, ClientInstanceId, ClientState, Config, MsgHeader, MsgType, Router, ServiceId, SharedPendingMessage, UsTime, INTERVAL_BETWEEN_CHECKING_RESPONSES_US, INTERVAL_BETWEEN_PINGS_US, MAX_CLIENTS, MAX_LOGIN_MSG_BODY_SIZE, MAX_MSG_BODY_SIZE, MAX_RESPONSE_TIME_US};

const ROUTER_TOKEN: mio::Token = mio::Token(0);

fn add_client(router: &mut Router, client_socket: TcpStream) {
    let client_addr = match client_socket.peer_addr() {
        Ok(addr) => addr,
        Err(e) => {
            error!("Can't get client address, closing connection: {}", e);
            return
        }
    };
    if router.clients.len() >= MAX_CLIENTS {
        error!("Max number of clients connected, closing connection from {}", client_addr);
        return
    }

    let mut client = Client {
        instance_id: ClientInstanceId(NonZeroU64::new(router.total_clients_connected + 1).unwrap()),
        state: ClientState::WaitingForLogin,
        socket: client_socket,
        addr: client_addr,
        subscribed_services: BTreeSet::new(),
        notified_services: BTreeSet::new(),
        awaited_responses: RingBuf::new(),
        received_from_header: 0,
        header: MsgHeader {
            enqueued_by_router: UsTime(0),
            msg_type: MsgType::Login as u16,
            flags: 0,
            body_size: 0,
            instance_id: None,
        },
        received_from_body: 0,
        body: Vec::new(),
        sent_from_message: 0,
        pending_messages: RingBuf::new(),
        // Set after login if the client is a service.
        service_id: None,
        notified_clients: BTreeSet::new(),
    };
    client.awaited_responses.enqueue(AwaitedResponse {
        msg_type: MsgType::Login,
        request_enqueued_by_router: UsTime::get_monotonic_time(),
        instance_id: None,
    });

    router.poll.registry()
        .register(&mut client.socket, client.instance_id.to_mio_token(), mio::Interest::READABLE)
        .expect("Cannot register client socket in poll for reading");

    router.clients.insert(client.instance_id, client);
    router.total_clients_connected += 1;
}

// UGLY: It would be much easier to accept `&mut Router`,
//       but we cannot do that because some of its fields (most commonly field `clients`)
//       are borrowed by caller and Rust type system doesn't allow us in these situations
//       to pass whole `&mut Router` to another function.
//
//       To help Rust compiler see that it's safe, we have to manually pass
//       subset of router's fields which are not borrowed by caller.
//       So we have to manually extract those fields whenever we call `mark_client_for_closing`.
fn mark_client_for_closing(
    client: &mut Client,
    service_id_to_name: &BTreeMap<ServiceId, String>,
    clients_waiting_for_close: &mut BTreeSet<ClientInstanceId>,
    reason: &str,
) {
    let service_name = client.service_id
        .map(|service_id| service_id_to_name.get(&service_id).expect("Unknown service").as_str())
        .unwrap_or_default();
    info!(
        "Marking client for closing (instance {:?}, state {:?}, addr {}, service: {}): {}",
        client.instance_id, client.state, client.addr, service_name, reason);

    client.state = ClientState::WaitingForClose;
    clients_waiting_for_close.insert(client.instance_id);
}

// Zero means an error has happened and either client was marked for closing
// or we should try reading or writing to socket in the next iteration of event loop.
fn handle_read_write_result(
    result: io::Result<usize>,
    client: &mut Client,
    service_id_to_name: &BTreeMap<ServiceId, String>,
    clients_waiting_for_close: &mut BTreeSet<ClientInstanceId>,
) -> usize {
    match result {
        Ok(0) => {
            mark_client_for_closing(
                client, service_id_to_name, clients_waiting_for_close,
                "Read or wrote 0 bytes");
            0
        }
        Ok(n) => n,
        // Recoverable error.
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
        Err(e) if e.kind() == io::ErrorKind::Interrupted => 0,
        Err(e) => {
            mark_client_for_closing(
                client, service_id_to_name, clients_waiting_for_close,
                format!("Error when reading or writing: {}", e).as_str());
            0
        }
    }
}

fn send_pending_msg(router: &mut Router, client_instance_id: ClientInstanceId) {
    let client = router.clients.get_mut(&client_instance_id).expect("Unknown client");

    loop {
        // There are no more messages waiting to be sent.
        if client.pending_messages.len() == 0 {
            router.poll.registry()
                .reregister(&mut client.socket, client.instance_id.to_mio_token(), mio::Interest::READABLE)
                .expect("Cannot reregister client socket in poll for reading");
            return
        }

        // UGLY: We have to `clone` to ensure that `client` is not borrowed.
        let msg = client.pending_messages.peek().clone();
        let msg_size = size_of::<MsgHeader>() + msg.header.body_size as usize;
        if msg_size < client.sent_from_message {
            panic!("Inconsistent state: Sent more bytes than message size");
        } else if msg_size == client.sent_from_message {
            panic!("Inconsistent state: Sent message not removed");
        }

        // Send header.
        if client.sent_from_message < size_of::<MsgHeader>() {
            let header = &msg.header;
            let p = header as *const MsgHeader;
            let p = p as *const u8;
            let slice = slice_from_raw_parts(p, size_of::<MsgHeader>());
            let slice = unsafe { &*slice };
            let result = client.socket.write(slice);
            let n = handle_read_write_result(
                result, client, &router.service_id_to_name, &mut router.clients_waiting_for_close);
            if n == 0 { return }
            client.sent_from_message += n;
            // Even though we tried to send the entire header, it still hasn't been sent.
            // Let's wait for the next event loop iteration.
            if client.sent_from_message != size_of::<MsgHeader>() {
                return
            }
        }

        // At this point the entire header has been sent.
        let sent_from_body = client.sent_from_message - size_of::<MsgHeader>();
        let body: &Vec<u8> = &msg.body;
        if sent_from_body < body.len() {
            // We have to send the body.
            let slice = &body[sent_from_body..];
            let result = client.socket.write(slice);
            let n = handle_read_write_result(
                result, client, &router.service_id_to_name, &mut router.clients_waiting_for_close);
            if n == 0 { return }
            client.sent_from_message += n;
            if client.sent_from_message != msg_size {
                return
            }
        }

        // At this point the entire message has been sent.
        client.pending_messages.dequeue();
        client.sent_from_message = 0;  // Nothing has been sent from the next message.
    }
}

// Returns `true` if the entire message has been received and can be processed.
fn receive_msg(router: &mut Router, client_instance_id: ClientInstanceId) -> bool {
    let client = router.clients.get_mut(&client_instance_id).expect("Unknown client");

    // Check invariants.
    {
        let received_everything =
            client.received_from_header >= size_of::<MsgHeader>() &&
            client.received_from_body >= client.header.body_size as usize;
        if received_everything {
            panic!("receive_msg shouldn't be called until previously received message has been processed")
        }
    }

    // Receive rest of the header.
    if client.received_from_header < size_of::<MsgHeader>() {
        let header = &mut client.header;
        let p = header as *mut MsgHeader;
        let p = p as *mut u8;
        let slice = slice_from_raw_parts_mut(p, size_of::<MsgHeader>());
        let slice = unsafe { &mut *slice };
        let result = client.socket.read(&mut slice[client.received_from_header..]);
        let n = handle_read_write_result(
            result, client, &router.service_id_to_name, &mut router.clients_waiting_for_close);
        if n == 0 { return false }
        client.received_from_header += n;
    }

    if client.received_from_header == size_of::<MsgHeader>() {
        let body_size = client.header.body_size as usize;

        if body_size > 0 {
            // Buffer hasn't been allocated yet.
            if client.body.len() == 0 {
                let max_body_size = if client.state == ClientState::WaitingForLogin {
                    MAX_LOGIN_MSG_BODY_SIZE
                } else {
                    MAX_MSG_BODY_SIZE
                };
                if body_size > max_body_size {
                    mark_client_for_closing(
                        client, &router.service_id_to_name, &mut router.clients_waiting_for_close,
                        format!("Client sent too big body {}", body_size).as_str());
                    return false;
                }

                client.body = vec![0u8; body_size];
            }

            // Receive rest of the body.
            if client.received_from_body < body_size {
                let slice = client.body.as_mut_slice();
                let result = client.socket.read(&mut slice[client.received_from_body..]);
                let n = handle_read_write_result(
                    result, client, &router.service_id_to_name, &mut router.clients_waiting_for_close);
                if n == 0 { return false }
                client.received_from_body += n;
            }
        }

        client.received_from_body == body_size
    } else {
        // Header hasn't been received yet.
        false
    }
}

// Returns `true` if the message has been successfully enqueued.
// Returns `false` if `client` has been marked for closing and message hasn't been enqueued.
fn enqueue_pending_msg(
    poll: &Poll,
    client: &mut Client,
    service_id_to_name: &BTreeMap<ServiceId, String>,
    clients_waiting_for_close: &mut BTreeSet<ClientInstanceId>,
    msg: Rc<SharedPendingMessage>,
) -> bool {
    if !client.pending_messages.can_enqueue() {
        mark_client_for_closing(
            client, service_id_to_name, clients_waiting_for_close,
            "Too many pending messages");
        false
    } else {
        client.pending_messages.enqueue(msg);

        // We enqueued the first message.
        if client.pending_messages.len() == 1 {
            // Configure poll for writing.
            poll.registry()
                .reregister(&mut client.socket, client.instance_id.to_mio_token(), mio::Interest::READABLE | mio::Interest::WRITABLE)
                .expect("Cannot reregister client socket in poll for reading and writing");
        }

        true
    }
}

// Returns `true` if the response has been successfully enqueued.
fn enqueue_awaited_response(
    client: &mut Client,
    service_id_to_name: &BTreeMap<ServiceId, String>,
    clients_waiting_for_close: &mut BTreeSet<ClientInstanceId>,
    response: AwaitedResponse,
) -> bool {
    if client.awaited_responses.can_enqueue() {
        client.awaited_responses.enqueue(response);
        true
    } else {
        mark_client_for_closing(
            client, service_id_to_name, clients_waiting_for_close,
            "Too many awaited responses");
        false
    }
}

fn enqueue_pings(router: &mut Router) {
    let now = UsTime::get_monotonic_time();

    // Too early to enqueue pings.
    if now.0 < router.pings_sent.0 + INTERVAL_BETWEEN_PINGS_US {
        return
    }

    let msg = SharedPendingMessage {
        header: MsgHeader {
            enqueued_by_router: now,
            msg_type: MsgType::Ping as u16,
            flags: 0,
            body_size: 0,
            instance_id: None,
        },
        body: Vec::new(),
    };
    let msg = Rc::new(msg);

    for client in router.clients.values_mut() {
        if client.state != ClientState::Ready {
            continue
        }

        let enqueued = enqueue_pending_msg(
            &router.poll, client, &router.service_id_to_name, &mut router.clients_waiting_for_close, msg.clone());
        if enqueued {
            // PING has been enqueued. Add PONG to awaited responses.
            enqueue_awaited_response(
                client, &router.service_id_to_name, &mut router.clients_waiting_for_close,
                AwaitedResponse {
                    msg_type: MsgType::Pong,
                    request_enqueued_by_router: now,
                    instance_id: None,
                });
        }
    }

    router.pings_sent = now;
}

fn check_awaited_responses(router: &mut Router) {
    let now = UsTime::get_monotonic_time();

    // Too early to check responses.
    if now.0 < router.responses_checked.0 + INTERVAL_BETWEEN_CHECKING_RESPONSES_US {
        return
    }

    for client in router.clients.values_mut() {
        if client.state != ClientState::Ready {
            continue
        }
        if client.awaited_responses.len() == 0 {
            continue
        }
        let resp = client.awaited_responses.peek();
        if resp.request_enqueued_by_router.0 + MAX_RESPONSE_TIME_US < now.0 {
            let reason = format!("Missing awaited response {:?} with request time {:?} and instance id {:?}",
                resp.msg_type, resp.request_enqueued_by_router, resp.instance_id);
            mark_client_for_closing(
                client, &router.service_id_to_name, &mut router.clients_waiting_for_close,
                reason.as_str());
        }
    }

    router.responses_checked = now;
}

fn close_clients_waiting_for_it(router: &mut Router) {
    loop {
        let client_instance_id = match router.clients_waiting_for_close.pop_first() {
            None => return,
            Some(client_instance_id) => client_instance_id,
        };

        let mut client = router.clients.remove(&client_instance_id).expect("Unknown client");

        let service_name = client.service_id
            .map(|service_id| router.service_id_to_name.get(&service_id).expect("Unknown service").as_str())
            .unwrap_or_default();
        info!(
            "Closing client (instance {:?}, state {:?}, addr '{}', service: '{}')",
            client.instance_id, client.state, client.addr, service_name);

        if client.state != ClientState::WaitingForClose {
            panic!("Client waiting for close has invalid state")
        }

        router.poll.registry()
            .deregister(&mut client.socket)
            .expect("Cannot deregister client socket from poll");

        let now = UsTime::get_monotonic_time();

        // Remove client from services which were notified.
        // Send them `RemoveClientFromService`.
        {
            let msg = SharedPendingMessage {
                header: MsgHeader {
                    enqueued_by_router: now,
                    msg_type: MsgType::RemoveClientFromService as u16,
                    flags: 0,
                    body_size: 0,
                    instance_id: Some(client.instance_id),
                },
                body: Vec::new(),
            };
            let msg = Rc::new(msg);

            for client_instance_id_of_service in client.notified_services {
                // Service may not exist anymore.
                let service = match router.clients.get_mut(&client_instance_id_of_service) {
                    None => continue,
                    Some(service) => service,
                };
                // CONSIDER: Check allowed states. Only `Ready` and `WaitingForClose` are allowed.
                if service.state != ClientState::Ready {
                    continue
                }

                let enqueued = enqueue_pending_msg(
                    &router.poll,
                    service, &router.service_id_to_name, &mut router.clients_waiting_for_close,
                    msg.clone());
                if enqueued {
                    let response = AwaitedResponse {
                        msg_type: MsgType::ClientRemovedFromService,
                        request_enqueued_by_router: now,
                        instance_id: Some(client.instance_id),
                    };
                    let enqueued = enqueue_awaited_response(
                        service, &router.service_id_to_name, &mut router.clients_waiting_for_close, response);
                    if enqueued {
                        // Everything has been enqueued, remove the client from the service.
                        if !service.notified_clients.remove(&client.instance_id) {
                            panic!("Inconsistent state: Cannot remove closed client from notified_clients")
                        }
                    }
                }
            }
        }

        // If the client is a service, then notify the clients that are connected to it.
        if let Some(service_id) = client.service_id {
            let service_name = router.service_id_to_name.get(&service_id).expect("Unknown service");

            let mut body = vec![service_name.len() as u8];
            body.extend_from_slice(service_name.as_bytes());
            let msg = SharedPendingMessage {
                header: MsgHeader {
                    enqueued_by_router: now,
                    msg_type: MsgType::DisconnectedFromService as u16,
                    flags: 0,
                    body_size: body.len() as u32,
                    instance_id: Some(client.instance_id),
                },
                body,
            };
            let msg = Rc::new(msg);

            for client_instance_id_of_connected_client in client.notified_clients {
                // Client may not exist anymore.
                let connected_client = match router.clients.get_mut(&client_instance_id_of_connected_client) {
                    None => continue,
                    Some(connected_client) => connected_client,
                };
                if connected_client.state == ClientState::WaitingForClose { continue }
                if connected_client.state != ClientState::Ready {
                    panic!(
                        "Inconsistent state: Client {:?} has invalid state {:?}",
                        connected_client.instance_id, connected_client.state)
                }

                // Notify `connected_client` that it's no longer connected to the service.
                let enqueued = enqueue_pending_msg(
                    &router.poll,
                    connected_client, &router.service_id_to_name, &mut router.clients_waiting_for_close,
                    msg.clone());
                if enqueued {
                    if !connected_client.notified_services.remove(&client.instance_id) {
                        panic!("Inconsistent state: Cannot remove closed service from notified_services")                        
                    }
                }
            }
        }
    }
}

fn main() {
    env_logger::init();

    // We want to log panic with a logger.
    panic::set_hook(Box::new(|panic_info| {
        if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            error!("Router failed: {:?}", s);
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            error!("Router failed: {:?}", s);
        } else {
            error!("Router failed for unknown reason (this shouldn't happen)");
        }
    }));

    let config = fs::read_to_string("Config.json").expect("Cannot read Config.json");
    let config: Config = serde_json::from_str(config.as_str()).expect("Cannot parse Config.json");

    let mut addrs = config.host_and_port.to_socket_addrs().expect("Cannot turn host and port to socket addrs");
    let addr = addrs.next().expect("No socket addr");
    info!("Host and port {} resolved to {}", config.host_and_port, addr);

    let mut server_socket = TcpListener::bind(addr).expect("Cannot bind addr");
    info!("Listening on {}", addr);

    let poll = mio::Poll::new().expect("Poll creation failed");
    poll.registry()
        .register(&mut server_socket, ROUTER_TOKEN, mio::Interest::READABLE)
        .expect("Cannot register read interest on router socket");

    let mut router = Router {
        server_socket,
        poll,
        service_id_to_name: BTreeMap::new(),
        service_name_to_id: BTreeMap::new(),
        clients: BTreeMap::new(),
        total_clients_connected: 0,
        pings_sent: UsTime(0),
        responses_checked: UsTime(0),
        clients_waiting_for_close: BTreeSet::new(),
    };

    loop {
        let mut events = mio::Events::with_capacity(32);
        match router.poll.poll(&mut events, Some(Duration::from_secs(1))) {
            Ok(()) => {},
            Err(e) if e.kind() == io::ErrorKind::Interrupted => info!("Polling interrupted"),
            Err(e) => panic!("Polling failed with {}", e),
        }

        for event in events.iter() {
            // Event on server socket.
            if event.token() == ROUTER_TOKEN {
                if event.is_error() {
                    panic!("Error on server socket");
                } else if event.is_read_closed() {
                    panic!("Server socket is read closed");
                } else if event.is_write_closed() {
                    panic!("Server socket is write closed");
                } else if event.is_writable() {
                    panic!("Server socket is writable");
                } else if event.is_readable() {
                    match router.server_socket.accept() {
                        Ok((client_socket, _client_addr)) => add_client(&mut router, client_socket),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        // We're not panicking because the problem can be on client side.
                        Err(e) =>
                            warn!("Accepting on server socket failed: {}", e),
                    }
                } else {
                    error!("Unknown event on server socket {:?}", event)
                }
            } else {
                let client_instance_id = ClientInstanceId::from_mio_token(event.token()).unwrap();
                // Client must exist otherwise there's bug in our code.
                let client = router.clients.get_mut(&client_instance_id).expect("Event for unknown client");

                if client.state == ClientState::WaitingForClose {
                    // Ignore event for clients which shall be closed.
                } else if event.is_error() {
                    mark_client_for_closing(
                        client, &router.service_id_to_name, &mut router.clients_waiting_for_close,
                        "Error on client socket");
                } else if event.is_read_closed() {
                    mark_client_for_closing(
                        client, &router.service_id_to_name, &mut router.clients_waiting_for_close,
                        "Client socket is read closed");
                } else if event.is_write_closed() {
                    mark_client_for_closing(
                        client, &router.service_id_to_name, &mut router.clients_waiting_for_close,
                        "Client socket is write closed");
                } else if event.is_writable() {
                    send_pending_msg(&mut router, client_instance_id);
                } else if event.is_readable() {
                    if receive_msg(&mut router, client_instance_id) {
                        process_msg(&mut router, client_instance_id);
                    }
                } else {
                    mark_client_for_closing(
                        client, &router.service_id_to_name, &mut router.clients_waiting_for_close,
                        format!("Unknown event on client socket {:?}", event).as_str());
                }
            }
        }

        enqueue_pings(&mut router);
        check_awaited_responses(&mut router);
        close_clients_waiting_for_it(&mut router);
    }
}
