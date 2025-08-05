use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;
use std::num::NonZeroU16;
use std::rc::Rc;
use log::info;
use mio::Poll;
use crate::{enqueue_awaited_response, enqueue_pending_msg, mark_client_for_closing};
use crate::types::{AwaitedResponse, Client, ClientInstanceId, ClientState, MsgHeader, MsgType, Router, ServiceId, SharedPendingMessage, UsTime, MAX_SERVICE_NAME_LEN, MAX_SUBSCRIPTIONS};

// If the message is a response then it's checked against awaited response.
fn check_if_message_is_expected_and_header(
    msg_type: MsgType,
    client: &mut Client,
    service_id_to_name: &BTreeMap<ServiceId, String>,
    clients_waiting_for_close: &mut BTreeSet<ClientInstanceId>,
) -> bool {
    if client.state == ClientState::WaitingForClose {
        panic!("check_if_message_is_expected_and_header assumes that client is not waiting for close")
    }

    // Check client state and whether it's a service.
    match msg_type {
        MsgType::Login => {
            if client.state != ClientState::WaitingForLogin {
                mark_client_for_closing(
                    client, service_id_to_name, clients_waiting_for_close,
                    format!("Unexpected {:?} message", msg_type).as_str());
                return false
            }
        }

        // Only services can send these messages.
        MsgType::ClientAddedToService |
        MsgType::ClientRemovedFromService |
        MsgType::MsgToAllClients |
        MsgType::MsgToOneClient => {
            if client.state != ClientState::Ready || client.service_id.is_none() {
                mark_client_for_closing(
                    client, service_id_to_name, clients_waiting_for_close,
                    format!("Unexpected {:?} message", msg_type).as_str());
                return false
            }
        }

        // Any client can send these messages.
        MsgType::SubscribeToService |
        MsgType::Request |
        MsgType::Pong => {
            if client.state != ClientState::Ready {
                mark_client_for_closing(
                    client, service_id_to_name, clients_waiting_for_close,
                    format!("Unexpected {:?} message", msg_type).as_str());
                return false
            }
        }

        // The following messages can only be sent by router.
        MsgType::ConnectedToService |
        MsgType::DisconnectedFromService |
        MsgType::AddClientToService |
        MsgType::RemoveClientFromService |
        MsgType::Ping => {
            mark_client_for_closing(
                client, service_id_to_name, clients_waiting_for_close,
                format!("Client sent message of invalid type {:?}", msg_type).as_str());
            return false
        }
    }

    // Check header. If desirable check it against awaited response.
    match msg_type {
        // Check against awaited response.
        MsgType::Login |
        MsgType::ClientAddedToService |
        MsgType::ClientRemovedFromService |
        MsgType::Pong => {
            if client.awaited_responses.len() == 0 {
                mark_client_for_closing(
                    client, service_id_to_name, clients_waiting_for_close,
                    format!("Got {:?} but no response is awaited", msg_type).as_str());
                return false
            }

            let awaited_response = client.awaited_responses.dequeue();
            if awaited_response.msg_type != msg_type {
                mark_client_for_closing(
                    client, service_id_to_name, clients_waiting_for_close,
                    format!("Got {:?} but awaiting {:?}", msg_type, awaited_response.msg_type).as_str());
                return false
            }

            // Check if data from the header match data from the awaited response.
            let mut header_ok = true;
            {
                // Ensures that we don't have reference to unaligned data.
                let enqueued_by_router = client.header.enqueued_by_router;
                if msg_type == MsgType::Pong {
                    header_ok = header_ok && enqueued_by_router == awaited_response.request_enqueued_by_router;
                } else {
                    header_ok = header_ok && enqueued_by_router == UsTime(0);
                }
            }
            header_ok = header_ok && client.header.flags == 0;
            if msg_type == MsgType::Login {
                header_ok = header_ok && client.header.body_size > 0;
            } else {
                header_ok = header_ok && client.header.body_size == 0;
            }
            {
                // Ensures that we don't have reference to unaligned data.
                let instance_id = client.header.instance_id;
                match msg_type {
                    MsgType::ClientAddedToService |
                    MsgType::ClientRemovedFromService => {
                        // NOTE: `process_msg_*` function must still verify whether the client still exists.
                        header_ok = header_ok && instance_id == awaited_response.instance_id;
                    }
                    _ => {
                        header_ok = header_ok && instance_id == None;
                    }
                }
            }

            if !header_ok {
                mark_client_for_closing(
                    client, service_id_to_name, clients_waiting_for_close,
                    format!("Invalid {:?} header", msg_type).as_str());
                return false
            }
        }

        MsgType::MsgToAllClients |
        MsgType::MsgToOneClient |
        MsgType::SubscribeToService |
        MsgType::Request => {
            let mut header_ok = true;
            header_ok = header_ok && client.header.enqueued_by_router.0 == 0;
            header_ok = header_ok && client.header.flags == 0;
            // Empty body of message (`MsgToAllClients`, `MsgToOneClient`, `Request`) doesn't make sense.
            header_ok = header_ok &&  client.header.body_size > 0;
            {
                let instance_id = client.header.instance_id;
                if msg_type == MsgType::MsgToOneClient || msg_type == MsgType::Request {
                    // Concrete client or service must be specified.
                    // NOTE: `process_msg_*` function must still verify whether the client still exists
                    //       and in case of `Request` whether the client is a service.
                    header_ok = header_ok && instance_id.is_some();
                } else {
                    header_ok = header_ok && instance_id.is_none();
                }
            }

            if !header_ok {
                mark_client_for_closing(
                    client, service_id_to_name, clients_waiting_for_close,
                    format!("Invalid {:?} header", msg_type).as_str());
                return false
            }
        }

        MsgType::ConnectedToService |
        MsgType::DisconnectedFromService |
        MsgType::AddClientToService |
        MsgType::RemoveClientFromService |
        MsgType::Ping => panic!("Absurd")
    }

    true
}

fn read_short_str(start: &mut usize, str: &mut String, body: &Vec<u8>) -> bool {
    if body.len() <= *start {
        return false
    }

    let len = body[*start] as usize;
    let end_excl = *start + 1 + len;
    if body.len() < end_excl {
        return false
    }

    match String::from_utf8(Vec::from(&body[*start + 1..end_excl])) {
        Ok(s) => {
            *start = end_excl;
            *str = s;
            true
        }
        Err(_) => false,
    }
}

fn intern_service_name(router: &mut Router, name: String) -> ServiceId {
    match router.service_name_to_id.entry(name.clone()) {
        Entry::Vacant(e) => {
            let next_service_id = router.service_id_to_name.len() + 1;
            if next_service_id > u16::MAX as usize {
                panic!("Too many interned service names")
            }

            let service_id = ServiceId(NonZeroU16::new(next_service_id as u16).unwrap());
            e.insert(service_id);
            router.service_id_to_name.insert(service_id, name);
            service_id
        }
        Entry::Occupied(e) => *e.get(),
    }
}

// Returns `true` if everything has been successfully enqueued.
// Returns `false` if something wasn't enqueued.
fn enqueue_msg_and_response_add_client_to_service(
    poll: &Poll,
    service: &mut Client,
    client: &mut Client,
    service_id_to_name: &BTreeMap<ServiceId, String>,
    clients_waiting_for_close: &mut BTreeSet<ClientInstanceId>,
    now: UsTime,
) -> bool {
    let msg = SharedPendingMessage {
        header: MsgHeader {
            enqueued_by_router: now,
            msg_type: MsgType::AddClientToService as u16,
            flags: 0,
            body_size: 0,
            instance_id: Some(client.instance_id),
        },
        body: Vec::new(),
    };
    let msg = Rc::new(msg);

    let enqueued = enqueue_pending_msg(
        poll,
        service, service_id_to_name, clients_waiting_for_close,
        msg);
    if enqueued {
        let enqueued = enqueue_awaited_response(
            service, service_id_to_name, clients_waiting_for_close,
            AwaitedResponse {
                msg_type: MsgType::ClientAddedToService,
                request_enqueued_by_router: now,
                instance_id: Some(client.instance_id),
            });
        if enqueued {
            client.notified_services.insert(service.instance_id);
        }
        enqueued
    } else {
        false
    }
}

// There's no unsubscribe and the only reason a client can
// be removed from an existing service is that it is waiting for close
// or it no longer exists.
//
// So the state of the client doesn't need any update so this function
// takes only the client's instance id instead of taking whole `&mut Client`.
//
// To summarize this function only updates the state of the service.
fn enqueue_msg_and_response_remove_client_from_service(
    poll: &Poll,
    service: &mut Client,
    client: ClientInstanceId,
    service_id_to_name: &BTreeMap<ServiceId, String>,
    clients_waiting_for_close: &mut BTreeSet<ClientInstanceId>,
    now: UsTime,
) {
    let msg = SharedPendingMessage {
        header: MsgHeader {
            enqueued_by_router: now,
            msg_type: MsgType::RemoveClientFromService as u16,
            flags: 0,
            body_size: 0,
            instance_id: Some(client),
        },
        body: Vec::new(),
    };
    let msg = Rc::new(msg);

    let enqueued = enqueue_pending_msg(
        poll,
        service, service_id_to_name, clients_waiting_for_close,
        msg);
    if enqueued {
        enqueue_awaited_response(
            service, service_id_to_name, clients_waiting_for_close,
            AwaitedResponse {
                msg_type: MsgType::ClientRemovedFromService,
                request_enqueued_by_router: now,
                instance_id: Some(client),
            });
    }
}

fn process_msg_login(
    router_without_client: &mut Router,
    client: &mut Client,
    now: UsTime,
) {
    if client.awaited_responses.len() != 0 {
        panic!("Router has inconsistent state: Client WaitingForLogin had more than one awaited response")
    }

    // Read login message body.
    let mut username= String::new();
    let mut password = String::new();
    let mut service_name = String::new();
    let mut start = 0;
    let mut body_ok = true;
    body_ok = body_ok && read_short_str(&mut start, &mut username, &client.body);
    body_ok = body_ok && read_short_str(&mut start, &mut password, &client.body);
    body_ok = body_ok && read_short_str(&mut start, &mut service_name, &client.body);
    body_ok = body_ok && start == client.body.len();  // Nothing remains.
    body_ok = body_ok && username.len() > 0;
    body_ok = body_ok && password.len() > 0;
    body_ok = body_ok && service_name.len() <= MAX_SERVICE_NAME_LEN;

    if !body_ok {
        mark_client_for_closing(
            client, &router_without_client.service_id_to_name, &mut router_without_client.clients_waiting_for_close, "Invalid login body");
        return
    }

    // TODO: Verify `username` and `password`.

    // We're done if the client is not a service.
    if service_name.is_empty() {
        client.state = ClientState::Ready;
        info!(
            "Client logged in (instance {:?}, addr '{}')",
            client.instance_id, client.addr);
        return
    }

    // At this point client is a service.

    let service_id = intern_service_name(router_without_client, service_name.clone());

    // Check whether the same service doesn't already exist.
    for other_client in router_without_client.clients.values() {
        // For the purpose of detecting whether the same service is already connected,
        // we consider not only `Ready` clients but also `WaitingForClose` clients.
        if other_client.state == ClientState::WaitingForLogin { continue }
        if other_client.service_id == Some(service_id) {
            mark_client_for_closing(
                client,
                &router_without_client.service_id_to_name,
                &mut router_without_client.clients_waiting_for_close,
                "Service already connected");
            return
        }
    }

    client.service_id = Some(service_id);
    // We will set `client` to `Ready` after adding the subscribed clients to this service.

    // Send `AddClientToService` for each existing client subscribed to this service.
    for other_client in router_without_client.clients.values_mut() {
        if other_client.state != ClientState::Ready { continue }
        if !other_client.subscribed_services.contains(&service_id) { continue }

        let enqueued = !enqueue_msg_and_response_add_client_to_service(
            &router_without_client.poll,
            client,
            other_client,
            &router_without_client.service_id_to_name, &mut router_without_client.clients_waiting_for_close,
            now);
        // Both queues `pending_messages` and `awaited_responses` should have enough space
        // because the service has just started.
        if !enqueued {
            panic!("Router has inconsistent state: Queues don't have enough space after login");
        }
    }

    client.state = ClientState::Ready;
    info!(
        "Client logged in as service (instance {:?}, addr '{}', service '{}')",
        client.instance_id, client.addr, service_name);
}

fn process_msg_subscribe_to_service(
    router_without_client: &mut Router,
    client: &mut Client,
    now: UsTime,
) {
    if client.subscribed_services.len() == MAX_SUBSCRIPTIONS {
        mark_client_for_closing(
            client,
            &router_without_client.service_id_to_name,
            &mut router_without_client.clients_waiting_for_close,
            "Too many subscriptions");
        return
    }

    {
        let mut header_ok = true;
        header_ok = header_ok && client.header.enqueued_by_router.0 == 0;
        header_ok = header_ok && client.header.flags == 0;
        header_ok = header_ok && client.body.len() > 0;
        let instance_id = client.header.instance_id;
        header_ok = header_ok && instance_id.is_none();
        if !header_ok {
            mark_client_for_closing(
                client,
                &router_without_client.service_id_to_name,
                &mut router_without_client.clients_waiting_for_close,
                "Invalid SubscribeToService header");
            return
        }
    }

    // Read subscribe to service message body.
    let mut service_name = String::new();
    {
        let mut start = 0;
        let mut body_ok = true;
        body_ok = body_ok && read_short_str(&mut start, &mut service_name, &client.body);
        body_ok = body_ok && start == client.body.len();  // Nothing remains.
        body_ok = body_ok && service_name.len() > 0;
        body_ok = body_ok && service_name.len() <= MAX_SERVICE_NAME_LEN;
        if !body_ok {
            mark_client_for_closing(
                client,
                &router_without_client.service_id_to_name,
                &mut router_without_client.clients_waiting_for_close,
                "Invalid SubscribeToService body");
            return
        }
    }

    let service_id = intern_service_name(router_without_client, service_name.clone());

    // Ensure that `client` is not subscribing to itself.
    if client.service_id == Some(service_id) {
        mark_client_for_closing(
            client,
            &router_without_client.service_id_to_name,
            &mut router_without_client.clients_waiting_for_close,
            format!("Service '{}' is subscribing to itself", service_name).as_str());
        return
    }

    // Ensure that `client` is not already subscribed to this service and add subscription.
    if !client.subscribed_services.insert(service_id) {
        mark_client_for_closing(
            client,
            &router_without_client.service_id_to_name,
            &mut router_without_client.clients_waiting_for_close,
            format!("Client already subscribed to service '{}'", service_name).as_str());
        return
    }

    // If the service exists, then send `AddClientToService` to it.
    for other_client in router_without_client.clients.values_mut() {
        if other_client.state != ClientState::Ready { continue }
        if other_client.service_id != Some(service_id) { continue }

        enqueue_msg_and_response_add_client_to_service(
            &router_without_client.poll,
            other_client,
            client,
            &router_without_client.service_id_to_name, &mut router_without_client.clients_waiting_for_close,
            now);

        break
    }
}

fn process_msg_client_added_to_service(
    router_without_service: &mut Router,
    service: &mut Client,
    now: UsTime,
) {
    let service_name = &router_without_service.service_id_to_name[&service.service_id.unwrap()];

    // This instance id has already been checked against awaited response
    // by `check_if_message_is_expected_and_header`.
    let client = service.header.instance_id.expect("No client added to service");
    let client = match router_without_service.clients.get_mut(&client) {
        Some(client) if client.state == ClientState::Ready => client,
        // Client either doesn't exist anymore or it's `WaitingForClose`.
        _ => {
            // Ask the service to remove the client.
            enqueue_msg_and_response_remove_client_from_service(
                &router_without_service.poll,
                service,
                client,
                &router_without_service.service_id_to_name, &mut router_without_service.clients_waiting_for_close,
                now);
            return
        }
    };

    if !client.subscribed_services.contains(&service.service_id.unwrap()) {
        panic!(
            "Inconsistent state: Service '{}' added client {:?} which is not subscribed",
            service_name, client.instance_id)
    }

    // At this point the client exists and the service knows about it.
    // Now we need to tell the client that it's connected to the service and instance id of the service.

    // Enqueue notification `ConnectedToService` to the client.
    let mut body = vec![service_name.len() as u8];
    body.extend_from_slice(service_name.as_bytes());
    let msg = SharedPendingMessage {
        header: MsgHeader {
            enqueued_by_router: now,
            msg_type: MsgType::ConnectedToService as u16,
            flags: 0,
            body_size: body.len() as u32,
            instance_id: Some(service.instance_id),
        },
        body,
    };
    let msg = Rc::new(msg);
    let enqueued = enqueue_pending_msg(
        &router_without_service.poll,
        client, &router_without_service.service_id_to_name, &mut router_without_service.clients_waiting_for_close,
        msg);

    if enqueued {
        if !service.notified_clients.insert(client.instance_id) {
            panic!(
                "Inconsistent state: Client {:?} was already connected to service '{}'",
                client.instance_id, service_name)
        }
    }
}

fn process_msg_client_removed_from_service(
    router_without_service: &mut Router,
    service: &mut Client,
) {
    // Clients can't unsubscribe. So the only situation when router sends
    // `RemoveClientFromService` and awaits response `ClientRemovedFromService`
    // is when a client socket is closed. So in this case we don't have to notify the client
    // because it doesn't exist when `ClientRemovedFromService` is received.

    // Check that the client really doesn't exist.
    let client = service.header.instance_id.expect("No client removed from service");
    if let Some(client) = router_without_service.clients.get(&client) {
        let service_name = &router_without_service.service_id_to_name[&service.service_id.unwrap()];
        panic!(
            "Got ClientRemovedFromService from '{}' while client {:?} still exists",
            service_name, client.instance_id)
    }

    // Because we removed the client from `service.notified_clients`
    // when enqueuing `RemoveClientFromService` we don't need to do it here.
}

fn process_msg_msg_to_all_clients(
    router_without_service: &mut Router,
    service: &mut Client,
    now: UsTime,
) {
    let msg = SharedPendingMessage {
        header: MsgHeader {
            enqueued_by_router: now,
            msg_type: MsgType::MsgToAllClients as u16,
            flags: 0,
            body_size: service.body.len() as u32,
            instance_id: Some(service.instance_id),
        },
        body: service.body.clone(),
    };
    let msg = Rc::new(msg);

    for connected_client in service.notified_clients.iter() {
        let connected_client = match router_without_service.clients.get_mut(&connected_client) {
            None => panic!("Client {:?} from notified_clients doesn't exist", connected_client),
            Some(connected_client) => connected_client,
        };
        if connected_client.state == ClientState::WaitingForClose { continue }
        if connected_client.state != ClientState::Ready {
            panic!(
                "Inconsistent state: Client {:?} has invalid state {:?}",
                connected_client.instance_id, connected_client.state)
        }

        enqueue_pending_msg(
            &router_without_service.poll,
            connected_client,
            &router_without_service.service_id_to_name,
            &mut router_without_service.clients_waiting_for_close,
            msg.clone());
    }
}

fn process_msg_msg_to_one_client(
    router_without_service: &mut Router,
    service: &mut Client,
    now: UsTime,
) {
    // The presence of instance id has been checked
    // by `check_if_message_is_expected_and_header`.
    let client = service.header.instance_id.expect("No client instance id");

    if !service.notified_clients.contains(&client) {
        // It may happen that the client disconnects from the router
        // after the service sends `MsgToOneClient` but before the router
        // processes this `MsgToOneClient`.
        // In that case the client no longer exists when processing `MsgToOneClient`.

        // If the client exists, it's an error.
        // Since clients can't unsubscribe, it means that the service sent
        // the message to the client which has never been subscribed to it.
        if router_without_service.clients.contains_key(&client) {
            mark_client_for_closing(
                service,
                &router_without_service.service_id_to_name,
                &mut router_without_service.clients_waiting_for_close,
                format!(
                    "Service {:?} sent message to client {:?} which wasn't connected to it",
                    service.instance_id, client).as_str())
        }
        return
    }

    let client = match router_without_service.clients.get_mut(&client) {
        None => panic!("Client {:?} from notified_clients doesn't exist", client),
        Some(client) => client,
    };
    if client.state == ClientState::WaitingForClose { return }
    if client.state != ClientState::Ready {
        panic!(
            "Inconsistent state: Client {:?} has invalid state {:?}",
            client.instance_id, client.state)
    }

    // At this point the client exists and it's in `Ready` state.

    let msg = SharedPendingMessage {
        header: MsgHeader {
            enqueued_by_router: now,
            msg_type: MsgType::MsgToOneClient as u16,
            flags: 0,
            body_size: service.body.len() as u32,
            instance_id: Some(service.instance_id),
        },
        body: service.body.clone(),
    };
    let msg = Rc::new(msg);

    enqueue_pending_msg(
        &router_without_service.poll,
        client,
        &router_without_service.service_id_to_name,
        &mut router_without_service.clients_waiting_for_close,
        msg);
}

fn process_msg_request(
    router_without_client: &mut Router,
    client: &mut Client,
    now: UsTime,
) {
    // The presence of instance id has been checked
    // by `check_if_message_is_expected_and_header`.
    let service = client.header.instance_id.expect("No service instance id");
    let service = match router_without_client.clients.get_mut(&service) {
        // It's possible that the service no longer exists.
        None => return,
        Some(service) => service,
    };

    // `close_clients_waiting_for_it` will notify the client that the service doesn't exist.
    if service.state == ClientState::WaitingForClose { return }
    if service.state != ClientState::Ready {
        panic!(
            "Inconsistent state: Service {:?} has invalid state {:?}",
            service.instance_id, service.state)
    }

    if !service.notified_clients.contains(&client.instance_id) {
        mark_client_for_closing(
            client,
            &router_without_client.service_id_to_name,
            &mut router_without_client.clients_waiting_for_close,
            format!(
                "Client {:?} sent request to service {:?} without being connected to it",
                client.instance_id, service.instance_id).as_str());
        return
    }

    let msg = SharedPendingMessage {
        header: MsgHeader {
            enqueued_by_router: now,
            msg_type: MsgType::Request as u16,
            flags: 0,
            body_size: client.body.len() as u32,
            instance_id: Some(client.instance_id),
        },
        body: client.body.clone(),
    };
    let msg = Rc::new(msg);

    enqueue_pending_msg(
        &router_without_client.poll,
        service,
        &router_without_client.service_id_to_name,
        &mut router_without_client.clients_waiting_for_close,
        msg);
}

fn process_msg_pong() {
    // Nothing to do.
    // All work has been done in `check_if_message_is_expected_and_header`.
}

pub fn process_msg(router: &mut Router, client_instance_id: ClientInstanceId) {
    let client = router.clients.get_mut(&client_instance_id).expect("Unknown client");

    let msg_type = match MsgType::from_u16(client.header.msg_type) {
        None => {
            mark_client_for_closing(
                client, &router.service_id_to_name, &mut router.clients_waiting_for_close,
                "Client sent message of unknown type");
            return
        },
        Some(t) => t,
    };

    let ok = check_if_message_is_expected_and_header(
        msg_type, client, &router.service_id_to_name, &mut router.clients_waiting_for_close);
    if !ok { return }

    let now = UsTime::get_monotonic_time();

    // Temporarily remove `client` from `router.clients`
    // this allows us to use both mutable reference to `router` and mutable reference to `client`
    // at the same time. This also allows us to work with two clients at the same time
    // (for example one client is a service and the other one is its subscriber).
    let mut client = router.clients.remove(&client_instance_id).expect("Unknown client");

    match msg_type {
        MsgType::Login => process_msg_login(router, &mut client, now),
        MsgType::SubscribeToService => process_msg_subscribe_to_service(router, &mut client, now),
        MsgType::ClientAddedToService => process_msg_client_added_to_service(router, &mut client, now),
        MsgType::ClientRemovedFromService => process_msg_client_removed_from_service(router, &mut client),
        MsgType::MsgToAllClients => process_msg_msg_to_all_clients(router, &mut client, now),
        MsgType::MsgToOneClient => process_msg_msg_to_one_client(router, &mut client, now),
        MsgType::Request => process_msg_request(router, &mut client, now),
        MsgType::Pong => process_msg_pong(),

        MsgType::ConnectedToService |
        MsgType::DisconnectedFromService |
        MsgType::AddClientToService |
        MsgType::RemoveClientFromService |
        MsgType::Ping => panic!("check_if_message_is_expected_and_header is not working"),
    }

    // Free successfully processed message.
    client.received_from_header = 0;
    client.received_from_body = 0;
    client.body = Vec::new();

    // Put `client` back to `router`.
    router.clients.insert(client.instance_id, client);
}
