use bytes::BytesMut;
use byteorder::{LE, ByteOrder};
use failure::{self, Error, Fail};
use tokio_io::codec::{Encoder, Decoder};
use super::Result;

pub struct UFProto;

const HELLO: u8 = 0x01;
const REQ: u8 = 0x02;
const RESP: u8 = 0x03;
const END: u8 = 0x04;

#[derive(Debug)]
pub enum InMsg {
    Hello{ file_name: String },
    Req{connid: u64, start: u64, },
    End{connid: u64, },
    Corrupted{ error: Error },
}

impl Decoder for UFProto {
    type Item = InMsg;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<InMsg>> {
        if src.len() >= 1 {
            // all data is low endian
            let op = src[0];
            match op {
                HELLO => {
                    // operation hello: 01 lenlow lenhigh string [padding to 17 bytes]
                    let len = 
                        if let Some(len) = src.get(1..3).map(LE::read_u16) {
                            len
                        } else {
                            return Ok(None);
                        };
                    let mstring = 
                        src.get(3..3+len as usize)
                            .map(|slice| {
                                ::std::str::from_utf8(slice).map(str::to_string)
                            });
                    if let Some(file_name) = mstring {
                        // Amplification attack protection
                        // Ensure that request isn't smaller than response
                        let min_len = usize::max(len as usize + 3, HELLO_OUT_LEN);
                        if src.len() < min_len {
                            Ok(Some(InMsg::Corrupted{ error: failure::err_msg("Less than minimum length") }))
                        } else {
                            // consume data
                            src.split_to(min_len);
                            match file_name {
                                Ok(file_name) => Ok(Some(InMsg::Hello{file_name})),
                                Err(e) => Ok(Some(InMsg::Corrupted{ error: e.into() }))
                            }
                        }
                    } else {
                        // It seems Ok(None) is not an option for datagram stream decoder
                        // server hangs up
                        Ok(Some(InMsg::Corrupted{ error: failure::err_msg("Incorrect file name length") }))
                    }
                }
                REQ => {
                    // operation req: 00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16
                    //                02 /       connid        / /       start         /
                    let connid;
                    let start;
                    if let Some(val) = src.get(1..9).map(LE::read_u64) {
                        connid = val;
                    } else { return Ok(None); };
                    if let Some(val) = src.get(9..17).map(LE::read_u64) {
                        start = val;
                    } else { return Ok(None); };
                    src.split_to(17);
                    Ok(Some(InMsg::Req{connid, start}))
                }
                END => {
                    // operation end: 00 01 02 03 04 05 06 07 08
                    //                04 /       connid        /
                    let connid;
                    if let Some(val) = src.get(1..9).map(LE::read_u64) {
                        connid = val;
                    } else { return Ok(None); };
                    src.split_to(9);
                    Ok(Some(InMsg::End{connid}))
                }
                _ => {
                    src.split_to(1);
                    Ok(Some(InMsg::Corrupted{ error: MsgFormatError.into() }))
                }
            }
        } else {
            Ok(Some(InMsg::Corrupted{ error: MsgFormatError.into() }))
        }
    }
}

const HELLO_OUT_LEN: usize = 19;

#[derive(Debug)]
pub enum OutMsg {
    Hello{ connid: u64, file_len: u64, block_size: u16 },
    Resp{ connid: u64, start: u64, data: Box<[u8]>, },
    End{ connid: u64, },
}

impl Encoder for UFProto {
    type Item = OutMsg;
    type Error = Error;
    fn encode(&mut self, item: OutMsg, dst: &mut BytesMut) -> Result<()> {
        match item {
            OutMsg::Hello{connid, file_len, block_size} => {
                let mut buf = [HELLO; 19];
                LE::write_u64(&mut buf[1..9], connid);
                LE::write_u64(&mut buf[9..17], file_len);
                LE::write_u16(&mut buf[17..19], block_size);
                dst.extend_from_slice(&buf[..]);
            }
            OutMsg::Resp{connid, start, data} => {
                let mut buf = [RESP; 25];
                LE::write_u64(&mut buf[1..9], connid);
                LE::write_u64(&mut buf[9..17], start);
                LE::write_u64(&mut buf[17..25], data.len() as u64);
                dst.extend_from_slice(&buf[..]);
                dst.extend_from_slice(&*data);
            }
            OutMsg::End{connid} => {
                let mut buf = [END; 9];
                LE::write_u64(&mut buf[1..9], connid);
                dst.extend_from_slice(&buf[..]);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MsgFormatError;

impl ::std::fmt::Display for MsgFormatError {
    fn fmt(&self, w: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(w, "MsgFormatError")
    }
}

impl Fail for MsgFormatError {}
