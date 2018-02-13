#![feature(conservative_impl_trait)]
extern crate byteorder;
extern crate bytes;
extern crate failure;
extern crate futures;
extern crate futures_cpupool;
extern crate rand;
extern crate tokio;
extern crate tokio_io;
extern crate tokio_timer;

mod frame;

use failure::Error;
use futures::{future, Future, Sink, Stream};
use futures::sync::mpsc::{self, Sender};
use futures_cpupool::CpuPool;
use frame::{InMsg, OutMsg, UFProto};
use tokio::executor::current_thread;
use tokio::net::{UdpFramed, UdpSocket};
use tokio_timer::Timer;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::net::SocketAddr;
use std::rc::Rc;
use std::cell::RefCell;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(windows)]
use std::os::windows::prelude::*;
#[cfg(unix)]
use std::os::unix::prelude::*;

type Result<T> = ::std::result::Result<T, Error>;
type Jobs = Rc<RefCell<HashMap<(u64, SocketAddr), Job>>>;

const MAX_CONNECTIONS: usize = 100;
const CHUNK_SIZE: u16 = 1420 * 4;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_OUTBOUND_QUEUE_LEN: usize = 10_000;
static OUTBOUND_QUEUE_LEN: AtomicUsize = AtomicUsize::new(0);

fn main() {
    rmain().unwrap();
}

fn rmain() -> Result<()> {
    let job_pool_0 = Rc::new(RefCell::new(HashMap::new()));
    let cpu_pool = CpuPool::new_num_cpus();
    let timer = Timer::default();
    let ssock = UdpSocket::bind(&SocketAddr::new("0.0.0.0".parse()?, 4432))?;
    let (send, recv) = UdpFramed::new(ssock, UFProto).split();
    let (isend, irecv) = mpsc::channel(1024);
    let irecv = irecv.map_err(|_| frame::MsgFormatError);

    let job_pool = job_pool_0.clone();
    let server = recv.for_each(move |(cmd, addr)| {
        //println!("{:?} {:?}", cmd, addr);
        match cmd {
            InMsg::Corrupted { error } => {
                println!("Wrong command {}", error);
            }
            InMsg::Hello { file_name } => {
                process_hello(
                    &cpu_pool,
                    addr.clone(),
                    isend.clone(),
                    job_pool.clone(),
                    &file_name,
                );
            }
            InMsg::Req { connid, start } => {
                // Limit the queue of outbound packets
                if OUTBOUND_QUEUE_LEN.load(Ordering::Relaxed) < MAX_OUTBOUND_QUEUE_LEN {
                    process_req(
                        &cpu_pool,
                        addr.clone(),
                        isend.clone(),
                        job_pool.clone(),
                        connid,
                        start,
                    );
                }
            }
            InMsg::End { connid } => {
                job_pool.borrow_mut().remove(&(connid, addr.clone()));
                current_thread::spawn(task_send(&isend, &addr, OutMsg::End { connid }));
            }
        }
        Ok(())
    }).map_err(|e| {
        panic!("Server loop terminated unexpectedly. {}", e);
    });

    // send is not cloneable
    // channel data thru isend
    let sender = send.with(|v| -> Result<_> {
        OUTBOUND_QUEUE_LEN.fetch_sub(1, Ordering::Relaxed);
        Ok(v)
    }).send_all(irecv)
        .map_err(|e| panic!("Sender task terminated unexpectedly. {}", e))
        .then(|_| Ok(()));

    let job_pool = job_pool_0.clone();
    let timer_task = timer
        .interval(Duration::from_secs(5))
        .for_each(move |_| {
            let mut job_pool = job_pool.borrow_mut();
            let now = Instant::now();
            // Clean up timed out connections
            job_pool.retain(|_, v| now - v.last_active < CLIENT_TIMEOUT);
            println!(
                "I'm alive. Active transfers: {}. Outbound queue: {}",
                job_pool.len(),
                OUTBOUND_QUEUE_LEN.load(Ordering::Relaxed)
            );
            Ok(())
        })
        .map_err(|e| panic!("Timer task terminated unexpectedly. {}", e));

    current_thread::run(|_| {
        current_thread::spawn(server);
        current_thread::spawn(sender);
        current_thread::spawn(timer_task);
    });
    Ok(())
}

fn task_send(
    isend: &Sender<(OutMsg, SocketAddr)>,
    addr: &SocketAddr,
    msg: OutMsg,
) -> impl Future<Item = (), Error = ()> {
    OUTBOUND_QUEUE_LEN.fetch_add(1, Ordering::Relaxed);
    isend.clone().send((msg, addr.clone())).then(|_| Ok(()))
}

fn task_end(
    isend: &Sender<(OutMsg, SocketAddr)>,
    addr: &SocketAddr,
    connid: u64,
) -> impl Future<Item = (), Error = ()> {
    task_send(isend, addr, OutMsg::End { connid })
}

fn process_hello(
    cpu_pool: &CpuPool,
    addr: SocketAddr,
    isend: Sender<(OutMsg, SocketAddr)>,
    jobs: Jobs,
    file_name: &str,
) {
    if jobs.borrow().len() >= MAX_CONNECTIONS {
        current_thread::spawn(task_end(&isend, &addr, 0));
        return;
    }
    let path = PathBuf::from(file_name);
    // open file asynchronously, then send answer to the client
    let hello = cpu_pool
        .spawn_fn(move || future::ok(get_file(&path)))
        .and_then(move |res| {
            if let Some((file, file_len)) = res {
                let connid;
                loop {
                    let id = rand::random::<u64>();
                    if id != 0 {
                        connid = id;
                        break;
                    }
                }
                let job = Job {
                    file,
                    last_active: Instant::now(),
                    file_len,
                };
                jobs.borrow_mut().insert((connid, addr.clone()), job);
                let send_task = task_send(
                    &isend,
                    &addr,
                    OutMsg::Hello {
                        connid,
                        file_len,
                        block_size: CHUNK_SIZE,
                    },
                );
                current_thread::spawn(send_task);
            }
            Ok(())
        });
    current_thread::spawn(hello);
}

fn process_req(
    cpu_pool: &CpuPool,
    addr: SocketAddr,
    isend: Sender<(OutMsg, SocketAddr)>,
    jobs: Jobs,
    connid: u64,
    start: u64,
) {
    let file;
    let file_len;
    if let Some(job) = jobs.borrow_mut().get_mut(&(connid, addr.clone())) {
        file_len = job.file_len;
        // Tokio has no cross platform async file operations
        // File reading on file's clone will be performed in cpu_pool
        match job.file.try_clone() {
            Ok(f) => {
                job.last_active = Instant::now();
                file = f;
            }
            Err(e) => {
                // `jobs` is borrowed, cannot remove job here
                // Job will be terminated after timeout
                println!("Cannot clone file. {}", e);
                return;
            }
        }
    } else {
        // spurious request. No such job
        return;
    }
    if file_len < start {
        // request past the end of file
        // ignore
        println!("Request past the end ({}) from {}", start, addr);
        return;
    }

    // Read chunk in cpu_pool, then send data
    let read_and_send = cpu_pool
        .spawn_fn(move || {
            let buffer_len = u64::min(CHUNK_SIZE as u64, file_len - start) as usize;
            let mut data = vec![0; buffer_len];
            match file_read_at(&file, &mut data[..], start) {
                Ok(len) => {
                    data.truncate(len as usize);
                    future::ok(data.into_boxed_slice())
                }
                Err(e) => {
                    println!("Error reading file. {}", e);
                    future::err(e)
                }
            }
        })
        .then(move |res| {
            match res {
                Ok(data) => {
                    // file was successfully read
                    // queue packet send task
                    current_thread::spawn(task_send(
                        &isend,
                        &addr,
                        OutMsg::Resp {
                            connid,
                            start,
                            data,
                        },
                    ));
                }
                Err(_) => {
                    // error while reading file, terminate connection
                    jobs.borrow_mut().remove(&(connid, addr.clone()));
                    current_thread::spawn(task_end(&isend, &addr, connid));
                }
            }
            Ok(())
        });
    current_thread::spawn(read_and_send);
}

#[cfg(windows)]
fn file_read_at(file: &File, data: &mut [u8], start: u64) -> ::std::io::Result<usize> {
    file.seek_read(&mut data[..], start)
}

#[cfg(unix)]
fn file_read_at(file: &File, data: &mut [u8], start: u64) -> ::std::io::Result<usize> {
    file.read_at(&mut data[..], start)
}

fn get_file(path: &Path) -> Option<(File, u64)> {
    path.file_name().and_then(|file| {
        let path: &Path = file.as_ref();
        if path.is_file() {
            if let Ok(meta) = path.metadata() {
                let len = meta.len();
                if let Ok(file) = File::open(path) {
                    Some((file, len))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    })
}

struct Job {
    file: File,
    file_len: u64,
    last_active: Instant,
}
