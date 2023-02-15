use crate::buf::ByteBuffer;
use crate::error::Error;
use crate::mysql::cmd::MyCmd;
use crate::mysql::conn::MyConn;
use crate::mysql::flag::StatusFlags;
use crate::mysql::packet::ErrPacket;
use crate::mysql::serde::MyDeser;
use crate::mysql::serde::NewMySer;
use crate::mysql::server::principal::Principal;
use crate::mysql::server::Server;
use futures_lite::{AsyncRead, AsyncWrite};

pub struct Session<'a, T> {
    server: &'a Server,
    conn: MyConn<T>,
    status_flags: StatusFlags,
    principal: Option<Principal>,
}

impl<'a, T: AsyncRead + AsyncWrite + Unpin> Session<'a, T> {
    #[inline]
    pub fn new(server: &'a Server, conn: MyConn<T>) -> Self {
        Session {
            server,
            conn,
            status_flags: StatusFlags::STATUS_AUTOCOMMIT,
            principal: None,
        }
    }

    #[inline]
    pub async fn start(mut self) {
        let read_buf = ByteBuffer::with_capacity(4096);
        let mut write_buf = vec![0u8; 4096];
        match self
            .conn
            .server_handshake(
                &self.server.spec,
                self.status_flags,
                &read_buf,
                &mut write_buf,
            )
            .await
        {
            Ok(principal) => {
                log::debug!("principal {:?} login suceeded", principal);
                self.principal.replace(principal);
            }
            Err(e) => {
                self.send_err(e, &mut write_buf).await;
                return;
            }
        }
        self.conn.reset_pkt_nr();
        while let Ok((payload, rg)) = self.conn.recv(&read_buf, true).await {
            match MyCmd::my_deser(self.conn.ctx_mut(), &payload) {
                Err(e) => {
                    self.send_err(e, &mut write_buf).await;
                }
                Ok((next, cmd)) => {
                    if !next.is_empty() {
                        self.send_err(Error::MalformedPacket, &mut write_buf).await;
                        continue;
                    }
                    match cmd {
                        MyCmd::Query(q) => {
                            log::debug!("start processing query command: {}", q.query);
                        }
                        MyCmd::FieldList(fl) => {
                            log::debug!("start processing field list command: {:?}", fl);
                        }
                    }
                }
            }
            rg.advance(payload.len());
            self.conn.reset_pkt_nr();
        }
    }

    #[inline]
    async fn send_err(&mut self, e: Error, write_buf: &mut [u8]) {
        let err = ErrPacket::from(&e);
        let ser = err.new_my_ser(self.conn.ctx_mut());
        if let Err(e) = self.conn.send(ser, write_buf).await {
            log::warn!("failed to send error packet to client {}", e);
        }
    }
}
