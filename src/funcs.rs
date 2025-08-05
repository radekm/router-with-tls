use std::mem::transmute;
use std::num::NonZeroU64;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::types::{ClientInstanceId, ClientInstanceIdOrNone, MsgType, UsTime};

impl ClientInstanceId {
    pub fn to_mio_token(self) -> mio::Token {
        mio::Token(self.0.get() as usize)
    }

    pub fn from_mio_token(token: mio::Token) -> ClientInstanceIdOrNone {
        NonZeroU64::new(token.0 as u64).map(|u| ClientInstanceId(u))
    }
}

static LAST_TIME: Mutex<UsTime> = Mutex::new(UsTime(0));

impl UsTime {
    pub fn get_monotonic_time() -> UsTime {
        let cur_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let cur_time = UsTime(cur_us);
        let mut last_time = LAST_TIME.lock().unwrap();

        // Ensure monotonicity.
        if *last_time >= cur_time {
            *last_time
        } else {
            *last_time = cur_time;
            cur_time
        }
    }
}

impl MsgType {
    pub fn from_u16(x: u16) -> Option<MsgType> {
        // Compare with the highest value of `MsgType`.
        if x <= MsgType::Pong as u16 {
            Some(unsafe { transmute::<u16, MsgType>(x) })
        } else {
            None
        }
    }
}
