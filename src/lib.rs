#[macro_use]
extern crate lazy_static;

use auxtools::*;

use coarsetime::{Duration, Instant};

type DeferredFunc = Box<dyn Fn() -> DMResult + Send + Sync>;

type CallbackChannel = (flume::Sender<DeferredFunc>, flume::Receiver<DeferredFunc>);

lazy_static! {
    static ref CALLBACK_CHANNELS: dashmap::DashMap<String, CallbackChannel> =
        dashmap::DashMap::new();
}

/// Gets a sender for a callback channel; inserts if doesn't exist.
/// Can deadlock if any of the other functions is happening simultaneously (not likely, but keep in mind).
pub fn callback_sender_by_id_insert(id: String) -> flume::Sender<DeferredFunc> {
    CALLBACK_CHANNELS
        .entry(id)
        .or_insert(flume::bounded(100000))
        .0
        .clone()
}

/// Gets a receiver for a callback channel; inserts if doesn't exist.
/// Can deadlock if any of the other functions is happening simultaneously (not likely, but keep in mind).
pub fn callback_receiver_by_id_insert(id: String) -> flume::Receiver<DeferredFunc> {
    CALLBACK_CHANNELS
        .entry(id)
        .or_insert(flume::bounded(100000))
        .1
        .clone()
}

/// Gets a sender for a callback channel. Returns None if doesn't already exist.
/// Can deadlock if an insert function is being called simultaneously.
pub fn callback_sender_by_id(id: String) -> Option<flume::Sender<DeferredFunc>> {
    if let Some(channel) = CALLBACK_CHANNELS.get(&id) {
        Some(channel.0.clone())
    } else {
        None
    }
}

/// Gets a receiver for a callback channel. Returns None if doesn't already exist.
/// Can deadlock if an insert function is being called simultaneously.
pub fn callback_receiver_by_id(id: String) -> Option<flume::Receiver<DeferredFunc>> {
    if let Some(channel) = CALLBACK_CHANNELS.get(&id) {
        Some(channel.1.clone())
    } else {
        None
    }
}

/// Goes through every single outstanding callback and calls them.
/// All callback processing should be called from byond. To enforce this, a context is required.
pub fn process_all_callbacks() -> DMResult<()> {
    let stack_trace = Proc::find("/proc/auxtools_stack_trace").unwrap();
    for entry in CALLBACK_CHANNELS.iter() {
        let receiver = entry.value().1.clone();
        for callback in receiver {
            if let Err(e) = callback() {
                let _ = stack_trace.call(&[&Value::from_string(e.message.as_str())?]);
            }
            drop(callback);
        }
    }
    Ok(())
}

/// Goes through every single outstanding callback and calls them, until a given time limit is reached.
pub fn process_all_callbacks_for(duration: Duration) -> DMResult<bool> {
    let now = Instant::now();
    let stack_trace = Proc::find("/proc/auxtools_stack_trace").unwrap();
    'outer: for entry in CALLBACK_CHANNELS.iter() {
        let receiver = entry.value().1.clone();
        for callback in receiver.try_iter() {
            if let Err(e) = callback() {
                let _ = stack_trace.call(&[&Value::from_string(e.message.as_str())?]);
            }
            drop(callback);
            if now.elapsed() > duration {
                break 'outer;
            }
        }
    }
    Ok(now.elapsed() > duration)
}

/// Goes through every single outstanding callback and calls them, until a given time limit in milliseconds is reached.
pub fn process_all_callbacks_for_millis(millis: u64) -> DMResult<bool> {
    process_all_callbacks_for(Duration::from_millis(millis))
}

/// Goes through all outstanding callbacks from a given ID and calls them.
pub fn process_callbacks(id: String) -> DMResult<()> {
    let receiver = callback_receiver_by_id_insert(id);
    let stack_trace = Proc::find("/proc/auxtools_stack_trace").unwrap();
    for callback in receiver.try_iter() {
        if let Err(e) = callback() {
            let _ = stack_trace.call(&[&Value::from_string(e.message.as_str())?]);
        }
        drop(callback);
    }
    Ok(())
}

/// Goes through outstanding callbacks from a given ID and calls them until all are exhausted or time limit is reached.
pub fn process_callbacks_for(id: String, duration: Duration) -> DMResult<bool> {
    let receiver = callback_receiver_by_id_insert(id);
    let now = Instant::now();
    let stack_trace = Proc::find("/proc/auxtools_stack_trace").unwrap();
    for callback in receiver.try_iter() {
        if let Err(e) = callback() {
            let _ = stack_trace.call(&[&Value::from_string(e.message.as_str())?]);
        }
        if now.elapsed() > duration {
            break;
        }
        drop(callback);
    }
    Ok(now.elapsed() > duration)
}

/// Goes through outstanding callbacks from a given ID and calls them until a given time limit in milliseconds is reached.
pub fn process_callbacks_for_millis(id: String, millis: u64) -> DMResult<bool> {
    process_callbacks_for(id, Duration::from_millis(millis))
}

// This function is to be called from byond, preferably once a tick.
// Calling with no arguments will process every outstanding callback.
// Calling with one argument will process all outstanding callbacks of the given string ID.
// Calling with two arguments will process all outstanding callbacks, with the second argument being a time limit.
// The first argument can be null; if so, it will process every callback with the time limit. Otherwise,
// it'll process only the callbacks of the given ID.
// Time limit is in milliseconds.
#[hook("/proc/process_callbacks")]
fn _process_callbacks() {
    match args.len() {
        0 => {
            process_all_callbacks()?;
            Ok(Value::null())
        }
        1 => {
            process_callbacks(args.get(0).unwrap().as_string()?)?;
            Ok(Value::null())
        }
        2 => {
            let arg_limit = args.get(1).unwrap().as_number()? as u64;
            if let Ok(arg_str) = args.get(0).unwrap().as_string() {
                Ok(Value::from(process_callbacks_for_millis(
                    arg_str, arg_limit,
                )?))
            } else {
                Ok(Value::from(process_all_callbacks_for_millis(
                    arg_limit,
                )?))
            }
        }
        _ => Err(runtime!(
            "Invalid number of arguments for callback processing; must be 0, 1 or 2"
        )),
    }
}
