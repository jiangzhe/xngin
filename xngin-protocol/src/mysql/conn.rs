use crate::buf::{ByteBuffer, ByteBufferReadGuard};
use crate::error::{Error, Result};
use crate::mysql::auth::{AuthPlugin, AuthPluginImpl};
use crate::mysql::cmd::{CmdCode, ComFieldList, ComQuery};
use crate::mysql::col::ColumnDefinition;
use crate::mysql::flag::{CapabilityFlags, StatusFlags};
use crate::mysql::handshake::{
    ConnectAttr, HandshakeCliResp41, HandshakeSvrResp, InitialHandshake,
};
use crate::mysql::packet::{EofPacket, ErrPacket, OkPacket};
use crate::mysql::serde::{MyDeser, MyDeserExt, MySer, NewMySer, SerdeCtx};
use async_io::Timer;
use async_net::{AsyncToSocketAddrs, TcpStream};
use futures_lite::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, FutureExt};
use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

const DEFAULT_WRITE_BUF_SIZE: usize = 4096;

/// Options used to connect MySQL via TCP.
pub struct TcpOpts<T> {
    addr: T,
    write_buf_size: usize,
    login: HandshakeOpts,
    ctx: SerdeCtx,
}

impl<T> TcpOpts<T> {
    #[inline]
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.login.username = username.into().into_bytes();
        self
    }

    #[inline]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.login.password = password.into().into_bytes();
        self
    }

    #[inline]
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.login.database = database.into().into_bytes();
        self
    }

    #[inline]
    pub fn write_buf_size(mut self, write_buf_size: usize) -> Self {
        self.write_buf_size = write_buf_size;
        self
    }

    #[inline]
    pub fn connect_attr(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.login.connect_attrs.push((key.into(), value.into()));
        self
    }

    // #[inline]
    // pub fn auth_plugin(mut self, plugin_name: &str) -> Result<Self> {
    //     let plugin = AuthPluginImpl::new(plugin_name.as_bytes())?;
    //     self.login.auth_plugin.replace(plugin);
    //     Ok(self)
    // }
}

impl<T: AsyncToSocketAddrs> TcpOpts<T> {
    #[inline]
    pub fn new(addr: T) -> Self {
        TcpOpts {
            addr,
            write_buf_size: DEFAULT_WRITE_BUF_SIZE,
            login: HandshakeOpts::default(),
            ctx: SerdeCtx::default(),
        }
    }

    #[inline]
    pub async fn connect(
        self,
        read_buf: &ByteBuffer,
        connect_timeout: Option<Duration>,
    ) -> Result<MyConn<TcpStream>> {
        let conn = if let Some(timeout) = connect_timeout {
            TcpStream::connect(self.addr)
                .or(async {
                    Timer::after(timeout).await;
                    Err(std::io::ErrorKind::TimedOut.into())
                })
                .await?
        } else {
            TcpStream::connect(self.addr).await?
        };
        let mut mc = MyConn {
            id: 0,
            conn,
            ctx: self.ctx,
            write_buf: vec![0; self.write_buf_size],
        };
        mc.handshake(read_buf, self.login).await?;
        Ok(mc)
    }
}

#[derive(Default)]
struct HandshakeOpts {
    username: Vec<u8>,
    password: Vec<u8>,
    database: Vec<u8>,
    connect_attrs: Vec<(String, String)>,
    // auth_plugin: Option<AuthPluginImpl>,
}

pub struct MyConn<T> {
    // MySQL connection(thread) id.
    id: u32,
    conn: T,
    ctx: SerdeCtx,
    write_buf: Vec<u8>,
}

impl<T: AsyncWrite + Unpin> MyConn<T> {
    /// Send a packet to MySQL server.
    /// Reset packet number if necessary.
    async fn send<M: NewMySer + ?Sized>(&mut self, msg: &M, reset_pkt_nr: bool) -> Result<()> {
        if reset_pkt_nr {
            self.ctx.reset_pkt_nr();
        }
        let mut ser = msg.new_my_ser(&mut self.ctx);
        let mut total_bytes = ser.my_len(&self.ctx);
        while self.write_buf.len() < total_bytes {
            ser.my_ser_partial(&mut self.ctx, &mut self.write_buf);
            self.conn.write_all(&self.write_buf).await?;
            total_bytes -= self.write_buf.len();
        }
        if total_bytes > 0 {
            ser.my_ser(&mut self.ctx, &mut self.write_buf[..total_bytes]);
            self.conn.write_all(&self.write_buf[..total_bytes]).await?;
        }
        Ok(())
    }
}

impl<T: AsyncRead + Unpin> MyConn<T> {
    /// Receive a packet in given buffer.
    /// The buffer may be filled via previous reads, therefore the
    /// index argument indicates where to continue.
    /// If buffer can not hold a complete buffer, a new memory will be allocated.
    /// There might be cases that we do not want to allocate, we can turn
    /// it off and once space is exhausted, a [`Error::BufferFull`] is
    /// returned.
    #[allow(clippy::uninit_vec)]
    async fn recv<'a>(
        &mut self,
        buf: &'a ByteBuffer,
        vacuum: bool,
    ) -> Result<(Cow<'a, [u8]>, ByteBufferReadGuard<'a>)> {
        match self.recv_buf(buf, vacuum).await {
            Ok((pkt, rg)) => Ok((Cow::Borrowed(pkt), rg)),
            Err(Error::BufferFull { expected }) => {
                // buffer is too small to hold a complete packet
                let mut payload = Vec::with_capacity(expected);
                // SAFETY
                //
                // capacity is same as length, so it's safe.
                unsafe { payload.set_len(expected) };
                let (readable, rg) = buf.readable()?;
                payload[..readable.len()].copy_from_slice(readable);
                rg.advance(readable.len());
                self.conn.read_exact(&mut payload[readable.len()..]).await?;
                Ok((Cow::Owned(payload), buf.empty_read()))
            }
            Err(Error::PacketSplit) => {
                let payload = self.recv_split(buf).await?;
                Ok((Cow::Owned(payload), buf.empty_read()))
            }
            Err(e) => Err(e),
        }
    }

    /// Receive a packet in given buffer.
    /// Returns the payload only.
    /// If buffer is not big enough to hold a complete packet, [`Error::BufferFull`] is returned.
    async fn recv_buf<'a>(
        &mut self,
        buf: &'a ByteBuffer,
        vacuum: bool,
    ) -> Result<(&'a [u8], ByteBufferReadGuard<'a>)> {
        if vacuum {
            buf.update()?.vacuum(); // vacuum before receiving one packet.
        }
        while buf.readable_len() < 4 {
            // todo: capacity may be less than 4
            let (writable, _wg) = buf.writable()?;
            let n = self.conn.read(writable).await?;
            if n == 0 {
                return Err(Error::UnexpectedEOF);
            }
            buf.advance_w_idx(n)?;
        }
        let (readable, rg) = buf.readable()?;
        let (payload_len, pkt_nr) = parse_payload_len_and_pkt_nr(readable);
        rg.advance(4);
        self.ctx.check_and_inc_pkt_nr(pkt_nr)?;
        // Now we know the length of payload, so we can check
        // if current buffer can hold the complete packet.
        // There are two cases:
        // 1. max payload size is reached, and additional packet
        // header should be taken care of.
        // 2. buffer is smaller than packet length, just read
        // exactly packet length bytes.
        if payload_len == self.ctx.max_payload_size {
            return Err(Error::PacketSplit);
        }
        if buf.readable_capacity() < payload_len {
            return Err(Error::BufferFull {
                expected: payload_len,
            });
        }
        // check if input contains a complete packet.
        let (readable, rg) = buf.readable()?;
        if readable.len() < payload_len {
            let min_bytes_to_read = payload_len - readable.len();
            // continue to receive bytes until packet is complete.
            let (writable, _wg) = buf.writable()?;
            let n = read_at_least(&mut self.conn, writable, min_bytes_to_read).await?;
            buf.advance_w_idx(n)?;
            drop(rg); // drop previous read guard with length equal to zero.
                      // now we acquire readable again for complete payload
            let (readable, rg) = buf.readable()?;
            return Ok((&readable[..payload_len], rg));
        }
        Ok((&readable[..payload_len], rg))
    }

    /// Receive split packets and compose to a single vector.
    /// This method is triggerred by a common [`Self::recv`] call if
    /// the payload length is equal to 0xffffff by default, which is
    /// maximum packet size defined by MySQL protocol.
    #[allow(clippy::uninit_vec)]
    async fn recv_split(&mut self, prev_buf: &ByteBuffer) -> Result<Vec<u8>> {
        let max_payload_size = self.ctx.max_payload_size;
        let (readable, rg) = prev_buf.readable()?;
        // we always allocate a new vector to concat splitted packets.
        let mut data = vec![];
        // first packet must be of max packet size
        let mut last_payload_len = if readable.len() < max_payload_size {
            data.reserve(max_payload_size);
            unsafe { data.set_len(max_payload_size) };
            let idx = readable.len();
            data[..idx].copy_from_slice(readable);
            rg.advance(idx);
            self.conn.read_exact(&mut data[idx..]).await?;
            max_payload_size
        } else {
            // buffer contains more data than max packet size
            data.extend_from_slice(&readable[..max_payload_size]);
            let mut total_read_bytes = max_payload_size;
            let mut pp = PacketParser {
                b: &readable[max_payload_size..],
            };
            let last_payload_len = loop {
                let pkt = pp.next();
                match pkt.kind {
                    PacketKind::Normal => {
                        self.ctx.check_and_inc_pkt_nr(pkt.pkt_nr)?;
                        data.extend_from_slice(pkt.data);
                        total_read_bytes += pkt.data.len() + 4;
                        if (pkt.payload_len as usize) < max_payload_size {
                            break pkt.payload_len as usize;
                        }
                    }
                    PacketKind::PartialHeader => {
                        data.reserve(4);
                        let idx = data.len();
                        unsafe { data.set_len(idx + 4) };
                        data[idx..idx + pkt.data.len()].copy_from_slice(pkt.data);
                        total_read_bytes += pkt.data.len();
                        // complete the header
                        self.conn
                            .read_exact(&mut data[idx + pkt.data.len()..])
                            .await?;
                        let payload_len = self
                            .rollback_header_and_concat_payload(&mut data, idx)
                            .await?;
                        break payload_len;
                    }
                    PacketKind::PartialPayload => {
                        self.ctx.check_and_inc_pkt_nr(pkt.pkt_nr)?;
                        data.reserve(pkt.payload_len as usize);
                        let idx = data.len();
                        unsafe { data.set_len(idx + pkt.payload_len as usize) };
                        data[idx..idx + pkt.data.len()].copy_from_slice(pkt.data);
                        total_read_bytes += pkt.data.len() + 4;
                        // read payload
                        self.conn
                            .read_exact(&mut data[idx + pkt.data.len()..])
                            .await?;
                        break pkt.payload_len as usize;
                    }
                }
            };
            rg.advance(total_read_bytes); // from here, we do not need previous buffer
            last_payload_len
        };
        // continue to read from connection until payload length not equal max payload size
        while last_payload_len == max_payload_size {
            data.reserve(4);
            let idx = data.len();
            unsafe { data.set_len(idx + 4) };
            self.conn.read_exact(&mut data[idx..]).await?;
            last_payload_len = self
                .rollback_header_and_concat_payload(&mut data, idx)
                .await?;
        }
        Ok(data)
    }

    #[allow(clippy::uninit_vec)]
    #[inline]
    async fn rollback_header_and_concat_payload(
        &mut self,
        data: &mut Vec<u8>,
        idx: usize,
    ) -> Result<usize> {
        // read 4-byte header
        let (payload_len, pkt_nr) = parse_payload_len_and_pkt_nr(&data[idx..]);
        self.ctx.check_and_inc_pkt_nr(pkt_nr)?;
        // handle small payload size < 4
        data.reserve(payload_len.saturating_sub(4));
        unsafe { data.set_len(idx + payload_len) };
        // read payload
        self.conn.read_exact(&mut data[idx..]).await?;
        Ok(payload_len)
    }

    /// Parse column count packet.
    async fn col_cnt(&mut self, buf: &ByteBuffer) -> Result<u64> {
        let (pkt, rg) = self.recv(buf, false).await?;
        let col_cnt: u64 = (&*pkt).try_deser_len_enc_int()?.try_into()?;
        rg.advance(pkt.len());
        Ok(col_cnt)
    }

    /// Parse column definition packet.
    async fn col_def<'a>(
        &mut self,
        buf: &'a ByteBuffer,
        vacuum: bool,
    ) -> Result<ColumnDefinition<'a>> {
        let (payload, rg) = self.recv(buf, vacuum).await?;
        self.col_def_from_payload(payload, rg)
    }

    #[inline]
    fn col_def_from_payload<'a>(
        &mut self,
        payload: Cow<'a, [u8]>,
        rg: ByteBufferReadGuard<'a>,
    ) -> Result<ColumnDefinition<'a>> {
        match payload {
            Cow::Borrowed(slice) => {
                let (next, col_def) = ColumnDefinition::my_deser(&mut self.ctx, slice)?;
                ensure_empty(next)?;
                rg.advance(slice.len());
                Ok(col_def)
            }
            Cow::Owned(vec) => {
                // here we allocate new memory for column definitions, we can move it to context to delay its deallocation.
                let buf = ByteBuffer::from(vec);
                let pkt = unsafe { self.ctx.accept_buf(buf) };
                let (next, col_def) = ColumnDefinition::my_deser(&mut self.ctx, pkt)?;
                ensure_empty(next)?;
                Ok(col_def)
            }
        }
    }

    /// Parse metadata of text query response.
    /// See: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
    async fn parse_rows_metadata<'a>(
        &mut self,
        buf: &'a ByteBuffer,
    ) -> Result<(Vec<ColumnDefinition<'a>>, ByteBufferReadGuard<'a>)> {
        // optional metadata_follows packet
        let (buf, metadata_follows) = if self
            .ctx
            .cap_flags
            .contains(CapabilityFlags::OPTIONAL_RESULTSET_METADATA)
        {
            let (pkt, _rg) = self.recv(buf, true).await?;
            let metadata_follows = (&*pkt).try_deser_u8()?;
            (buf, metadata_follows != 0)
        } else {
            (buf, true)
        };
        // col count packet
        let col_cnt = self.col_cnt(buf).await?;
        // col definition packets
        let mut col_defs = Vec::with_capacity(col_cnt as usize);
        if metadata_follows {
            for _ in 0..col_cnt {
                let col_def = self.col_def(buf, false).await?;
                col_defs.push(col_def);
            }
        }
        if !self.ctx.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
            let (pkt, rg) = self.recv(buf, false).await?;
            // just check EOFPacket is complete
            let (next, _) = EofPacket::my_deser(&mut self.ctx, &pkt)?;
            ensure_empty(next)?;
            rg.advance(pkt.len());
        }
        // finish column definition parsing
        Ok((col_defs, buf.single_read()?))
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> MyConn<T> {
    /// process the initial handshake with MySQL server,
    /// should be called before any other commands
    /// this method will change the connect capability flags
    #[inline]
    async fn handshake<'a>(&mut self, buf: &'a ByteBuffer, opts: HandshakeOpts) -> Result<()> {
        buf.update()?.vacuum(); // vacuum before handshake
        let (pkt, rg) = self.recv(buf, true).await?;
        let (next, handshake) = InitialHandshake::my_deser(&mut self.ctx, &pkt)?;
        ensure_empty(next)?;
        self.id = handshake.connection_id;
        let mut seed = vec![];
        seed.extend(&*handshake.auth_plugin_data_1);
        seed.extend(&*handshake.auth_plugin_data_2);
        log::debug!(
            "server initialize handshake - protocol version: {}, server version: {}, connection_id: {}, auth_plugin={}, seed={:?}",
            handshake.protocol_version,
            String::from_utf8_lossy(&handshake.server_version),
            handshake.connection_id,
            String::from_utf8_lossy(&handshake.auth_plugin_name),
            seed,
        );
        self.ctx.cap_flags.insert(CapabilityFlags::PLUGIN_AUTH);
        self.ctx.cap_flags.insert(CapabilityFlags::LONG_PASSWORD);
        self.ctx.cap_flags.insert(CapabilityFlags::PROTOCOL_41);
        self.ctx.cap_flags.insert(CapabilityFlags::TRANSACTIONS);
        self.ctx.cap_flags.insert(CapabilityFlags::MULTI_RESULTS);
        self.ctx
            .cap_flags
            .insert(CapabilityFlags::SECURE_CONNECTION);
        // deprecate EOF to allow server send OK packet instead of EOF packet
        self.ctx.cap_flags.insert(CapabilityFlags::DEPRECATE_EOF);
        self.ctx
            .cap_flags
            .insert(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA);
        // disable ssl currently
        self.ctx.cap_flags.remove(CapabilityFlags::SSL);
        if !opts.database.is_empty() {
            self.ctx.cap_flags.insert(CapabilityFlags::CONNECT_WITH_DB);
        }
        if !opts.connect_attrs.is_empty() {
            self.ctx.cap_flags.insert(CapabilityFlags::CONNECT_ATTRS);
        }
        // use server suggested plugin to generate auth response
        //       e.g. MySQL 8.0.x suggests caching_sha2_password by default.
        // currently only two auth plugins are supported.
        let mut auth_plugin = AuthPluginImpl::new(&handshake.auth_plugin_name)?;
        let auth_response =
            auth_plugin.gen_init_auth_resp(&opts.username, &opts.password, &seed)?;
        let client_resp = HandshakeCliResp41 {
            cap_flags: self.ctx.cap_flags,
            // max length of three-byte word
            max_packet_size: self.ctx.max_payload_size as u32,
            // by default use utf-8
            charset: 33,
            username: Cow::Borrowed(&opts.username),
            auth_response: Cow::Owned(auth_response),
            database: Cow::Owned(opts.database),
            // we should use client auth plugin, this may be different from server's
            auth_plugin_name: Cow::Borrowed(auth_plugin.name().as_bytes()),
            connect_attrs: opts
                .connect_attrs
                .into_iter()
                .map(|(k, v)| {
                    let key = Cow::Owned(k.into_bytes());
                    let value = Cow::Owned(v.into_bytes());
                    ConnectAttr { key, value }
                })
                .collect(),
        };
        log::debug!(
            "send client response for initial handshake, catability_flags={:?}, connect_attrs={:?}",
            client_resp.cap_flags,
            client_resp.connect_attrs,
        );
        self.send(&client_resp, false).await?;
        // here we can release read guard because previous contents in the buffer are not used any more.
        rg.advance(pkt.len());
        loop {
            buf.update()?.vacuum();
            let (pkt, rg) = self.recv(buf, false).await?;
            match HandshakeSvrResp::my_deser(&mut self.ctx, &pkt)? {
                (next, HandshakeSvrResp::Ok(ok)) => {
                    ensure_empty(next)?;
                    log::debug!("handshake succeeded");
                    self.ctx.status_flags = ok.status_flags;
                    rg.advance(pkt.len());
                    // reset packet number for command phase
                    self.ctx.reset_pkt_nr();
                    break;
                }
                (next, HandshakeSvrResp::Err(err)) => {
                    ensure_empty(next)?;
                    log::debug!(
                        "handshake failed: code={}, state={}, error_message={}",
                        err.error_code,
                        String::from_utf8_lossy(&err.sql_state),
                        String::from_utf8_lossy(&err.error_message)
                    );
                    rg.advance(pkt.len());
                    return Err(err.into());
                }
                // Although we use auth plugin inferred by server initial handshake,
                // we can still have to switch to another auth plugin in below cases:
                // 1. user to login is assigned certain plugin to use.
                // 2. server may not loaded default auth plugin correctly.
                // If user level auth plugin is different from server default auth plugin,
                // MySQL always start switch process, this will add additional roundtrip
                // time for login.
                // So to improve performance, it's better to always sync user level auth
                // plugin with server default auth plugin.
                (next, HandshakeSvrResp::Switch(switch)) => {
                    ensure_empty(next)?;
                    log::debug!(
                        "server requires switching to auth_plugin {}, auth_data={:?}",
                        String::from_utf8_lossy(&switch.plugin_name),
                        switch.auth_plugin_data
                    );
                    auth_plugin = AuthPluginImpl::new(&switch.plugin_name)?;
                    // initialize auth response using new plugin.
                    let resp =
                        auth_plugin.gen_init_auth_resp(&opts.username, &opts.password, &seed)?;
                    rg.advance(pkt.len()); // after this point, switch packet will not be used.
                    if !resp.is_empty() {
                        self.send(&resp[..], false).await?;
                    }
                }
                (next, HandshakeSvrResp::More(more)) => {
                    ensure_empty(next)?;
                    log::debug!(
                        "auth more data={:?}",
                        String::from_utf8_lossy(&more.plugin_data)
                    );
                    rg.advance(pkt.len());
                    let mut resp = vec![];
                    auth_plugin.next(&more.plugin_data, &mut resp)?;
                    if !resp.is_empty() {
                        self.send(&resp[..], false).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Send a text query to MySQL server and return a result stream.
    /// The strategy is to store multiple rows in single buffer, and delay
    /// the deserialization when access.
    /// In this way, this method returns a stream instead of a complete
    /// result set.
    /// Once user provide a buffer, rows are saved in the buffer and returned.
    ///
    /// The connection itself is handed over to the stream because we must
    /// make sure no other threads access the connection conccurently.
    /// once the stream finishes, we can get back the connection via
    /// [`ResultStream::into_inner`] method.
    pub async fn query<'a>(
        mut self,
        cmd: impl Into<ComQuery<'_>>,
        buf: &'a ByteBuffer,
    ) -> Result<(
        Vec<ColumnDefinition<'a>>,
        ByteBufferReadGuard<'a>,
        RowStream<T>,
    )> {
        self.ctx.curr_cmd.replace(CmdCode::Query);
        self.send(&cmd.into(), true).await?;
        // The valid state transfer is listed below:
        // 1. ColCnt => ColDef / Err
        // 2. ColDef => OptEof (if DEPRECATED_EOF is disabled) / Rows / Err
        // 4. OptEof => Rows / Err
        // 5. Rows => Rows / Err
        let (col_defs, rg) = self.parse_rows_metadata(buf).await?;
        Ok((col_defs, rg, RowStream(self)))
    }

    /// Execute a text query.
    /// This type of query is supposed to have no rows returned.
    /// Only Ok or Err packet is returned to indicate whether this operation
    /// is successful.
    pub async fn exec<'a>(
        &mut self,
        cmd: impl Into<ComQuery<'_>>,
        buf: &'a ByteBuffer,
    ) -> Result<(ExecResp<'a>, ByteBufferReadGuard<'a>)> {
        self.ctx.curr_cmd.replace(CmdCode::Query);
        self.send(&cmd.into(), true).await?;
        // Only Ok and Err packet are allowed for execute type SQL.
        let (payload, rg) = self.recv_buf(buf, true).await?;
        if payload.is_empty() {
            return Err(Error::InvalidInput);
        }
        match payload[0] {
            0x00 => {
                let (next, ok) = OkPacket::my_deser(&mut self.ctx, payload)?;
                ensure_empty(next)?;
                rg.advance(payload.len());
                Ok((ExecResp::from(ok), buf.single_read()?))
            }
            0xff => {
                let (next, err) = ErrPacket::my_deser(&mut self.ctx, payload)?;
                if !next.is_empty() {
                    return Err(Error::MalformedPacket);
                }
                rg.advance(payload.len());
                Err(err.into())
            }
            _ => Err(Error::InvalidExecResp),
        }
    }

    /// Executea FIELD_LIST query.
    pub async fn field_list<'a>(
        &mut self,
        cmd: impl Into<ComFieldList<'_>>,
        buf: &'a ByteBuffer,
    ) -> Result<(Vec<ColumnDefinition<'a>>, ByteBufferReadGuard<'a>)> {
        self.ctx.curr_cmd.replace(CmdCode::FieldList);
        self.send(&cmd.into(), true).await?;
        buf.update()?.vacuum();
        let mut col_defs = vec![];
        loop {
            let (payload, rg) = self.recv(buf, false).await?;
            if payload.is_empty() {
                return Err(Error::EmptyPacketPayload);
            }
            let payload_len = payload.len();
            println!("payload len = {}", payload_len);
            match payload[0] {
                0xff => {
                    let (next, err) = ErrPacket::my_deser(&mut self.ctx, &payload)?;
                    if !next.is_empty() {
                        return Err(Error::MalformedPacket);
                    }
                    rg.advance(payload.len());
                    return Err(err.into());
                }
                0xfe if payload.len() <= self.ctx.max_payload_size => {
                    // deprecated EOF packet
                    let (next, _eof) = OkPacket::my_deser(&mut self.ctx, &payload)?;
                    if !next.is_empty() {
                        return Err(Error::MalformedPacket);
                    }
                    rg.advance(payload.len());
                    return Ok((col_defs, buf.single_read()?));
                }
                _ => {
                    let col_def = self.col_def_from_payload(payload, rg)?;
                    col_defs.push(col_def);
                }
            }
        }
    }
}

impl<T> fmt::Debug for MyConn<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("MyConn{..}")
    }
}

#[inline]
async fn read_at_least<'a, T: AsyncRead + Unpin>(
    rd: &mut T,
    buf: &'a mut [u8],
    min_bytes: usize,
) -> Result<usize> {
    let mut total_bytes = 0;
    loop {
        if total_bytes >= min_bytes {
            break;
        }
        let n = rd.read(&mut buf[total_bytes..]).await?;
        total_bytes += n;
    }
    Ok(total_bytes)
}

#[inline]
fn ensure_empty(b: &[u8]) -> Result<()> {
    if !b.is_empty() {
        return Err(Error::MalformedPacket);
    }
    Ok(())
}

#[inline]
fn parse_payload_len_and_pkt_nr(mut b: &[u8]) -> (usize, u8) {
    assert!(b.len() >= 4);
    let b = &mut b;
    let payload_len = b.deser_le_u24() as usize;
    let pkt_nr = b.deser_u8();
    (payload_len, pkt_nr)
}

enum PacketKind {
    Normal,
    PartialHeader,
    PartialPayload,
}

struct Packet<'a> {
    kind: PacketKind,
    payload_len: u32,
    pkt_nr: u8,
    // payload if kind == Normal, header if kind == PartialHeader
    data: &'a [u8],
}

struct PacketParser<'a> {
    b: &'a [u8],
}

impl<'a> PacketParser<'a> {
    #[inline]
    fn next(&mut self) -> Packet<'a> {
        if self.b.len() < 4 {
            return Packet {
                kind: PacketKind::PartialHeader,
                payload_len: 0,
                pkt_nr: 0,
                data: self.b,
            };
        }
        let b = &mut self.b;
        let payload_len = b.deser_le_u24();
        let pkt_nr = b.deser_u8();
        if b.len() >= payload_len as usize {
            let data = &(*b)[..payload_len as usize];
            b.advance(payload_len as usize);
            self.b = *b;
            return Packet {
                kind: PacketKind::Normal,
                payload_len,
                pkt_nr,
                data,
            };
        }
        let data = *b;
        b.advance(b.len() as usize);
        self.b = *b;
        Packet {
            kind: PacketKind::PartialPayload,
            payload_len,
            pkt_nr,
            data,
        }
    }
}

/// RowStream is a simple wrapper on MyConn to fetch rows
/// on the byte stream sent from server.
#[derive(Debug)]
pub struct RowStream<T>(MyConn<T>);

impl<T: AsyncRead + AsyncWrite + Unpin> RowStream<T> {
    /// Return next one or more rows, backed by given buffer.
    /// The input buffer can be different each time.
    /// Once buffer is full, all rows held are returned.
    /// The returned flag indicates whether the buffer is full.
    #[inline]
    pub async fn next(
        mut self,
        buf: &ByteBuffer,
    ) -> Result<(Rows<'_, T>, ByteBufferReadGuard<'_>)> {
        let mut rows = vec![];
        buf.update()?.vacuum();
        loop {
            match self.0.recv_buf(buf, false).await {
                Ok((row, rg)) => {
                    if row.is_empty() {
                        return Err(Error::EmptyPacketPayload);
                    }
                    match row[0] {
                        0xfe if row.len() <= self.0.ctx.max_payload_size => {
                            // this branch will end the row stream.
                            // 0xfe with a not very long packet, means it's end packet
                            if self
                                .0
                                .ctx
                                .cap_flags
                                .contains(CapabilityFlags::DEPRECATE_EOF)
                            {
                                let (next, ok) = OkPacket::my_deser(&mut self.0.ctx, row)?;
                                if !next.is_empty() {
                                    return Err(Error::MalformedPacket);
                                }
                                log::debug!("recv rows completed, row count {}", rows.len());
                                rg.advance(row.len());
                                let rows = Rows {
                                    kind: RowsKind::End {
                                        conn: self.0,
                                        affected_rows: ok.affected_rows,
                                        last_insert_id: ok.last_insert_id,
                                        status_flags: ok.status_flags,
                                        warnings: ok.warnings,
                                        info: ok.info.into(),
                                        session_state_changes: ok.session_state_changes.into(),
                                    },
                                    data: rows,
                                };
                                return Ok((rows, buf.single_read()?));
                            } else {
                                let (next, eof) = EofPacket::my_deser(&mut self.0.ctx, row)?;
                                if !next.is_empty() {
                                    return Err(Error::MalformedPacket);
                                }
                                rg.advance(row.len());
                                // we do not hold reference to eof packet, so no need to merge read guard
                                let rows = Rows {
                                    kind: RowsKind::End {
                                        conn: self.0,
                                        affected_rows: 0,
                                        last_insert_id: 0,
                                        status_flags: eof.status_flags,
                                        warnings: eof.warnings,
                                        info: Box::new([]),
                                        session_state_changes: Box::new([]),
                                    },
                                    data: rows,
                                };
                                return Ok((rows, buf.single_read()?));
                            }
                        }
                        0xff => {
                            let (next, err) = ErrPacket::my_deser(&mut self.0.ctx, row)?;
                            if !next.is_empty() {
                                return Err(Error::MalformedPacket);
                            }
                            rg.advance(row.len());
                            return Err(err.into());
                        }
                        _ => {
                            // row data
                            rows.push(Cow::Borrowed(row));
                            rg.advance(row.len());
                        }
                    }
                }
                Err(Error::BufferFull { expected }) => {
                    // we cannot hold even one packet in the buffer,
                    // a bigger buffer is required.
                    // returns current collected rows and this message to user.
                    let next_buf_size = if expected > buf.capacity() {
                        Some(expected)
                    } else {
                        None
                    };
                    log::debug!(
                        "recv rows stop due to full buffer, row count {}",
                        rows.len()
                    );
                    let rows = Rows {
                        kind: RowsKind::Cont {
                            next_buf_size,
                            stream: self,
                        },
                        data: rows,
                    };
                    return Ok((rows, buf.single_read()?));
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// RowsKind includes two states of row stream.
/// If row stream is completed, a OK packet will be sent and it contains
/// metadata of the query execution.
/// reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html
#[derive(Debug)]
pub enum RowsKind<T> {
    Cont {
        stream: RowStream<T>,
        next_buf_size: Option<usize>,
    },
    End {
        conn: MyConn<T>,
        affected_rows: u64,
        last_insert_id: u64,
        status_flags: StatusFlags,
        warnings: u16,
        info: Box<[u8]>,
        session_state_changes: Box<[u8]>,
    },
}

#[derive(Debug)]
pub struct Rows<'a, T> {
    pub kind: RowsKind<T>,
    pub data: Vec<Cow<'a, [u8]>>,
}

impl<T> Rows<'_, T> {
    #[inline]
    pub fn is_ended(&self) -> bool {
        matches!(self.kind, RowsKind::End { .. })
    }
}

#[derive(Debug)]
pub struct ExecResp<'a> {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: StatusFlags,
    pub warnings: u16,
    pub info: Cow<'a, [u8]>,
    pub session_state_changes: Cow<'a, [u8]>,
}

impl<'a> From<OkPacket<'a>> for ExecResp<'a> {
    #[inline]
    fn from(ok: OkPacket<'a>) -> Self {
        ExecResp {
            affected_rows: ok.affected_rows,
            last_insert_id: ok.last_insert_id,
            status_flags: ok.status_flags,
            warnings: ok.warnings,
            info: ok.info,
            session_state_changes: ok.session_state_changes,
        }
    }
}

impl_from_ref!(ExecResp:
    affected_rows, last_insert_id, status_flags, warnings;
    info, session_state_changes
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::cmd::{CmdCode, ComQuery};
    use async_io::block_on;
    use futures_lite::io::Cursor;

    #[test]
    fn test_recv() {
        let data = vec![2u8, 0, 0, 0, 3, 48, 2, 0, 0, 1, 3, 49];
        let cursor = Cursor::new(data);
        let mut conn = MyConn {
            id: 0,
            conn: cursor,
            ctx: SerdeCtx::default(),
            write_buf: Vec::with_capacity(1024),
        };
        block_on(async {
            let buf = ByteBuffer::with_capacity(64);
            let (pkt, rg) = conn.recv(&buf, false).await.unwrap();
            let (rest, cmd) = ComQuery::my_deser(&mut conn.ctx, &pkt).unwrap();
            assert!(rest.is_empty());
            rg.advance(pkt.len());
            assert_eq!(cmd.code, CmdCode::Query);
            let (pkt, rg) = conn.recv(&buf, false).await.unwrap();
            let (rest, cmd) = ComQuery::my_deser(&mut conn.ctx, &pkt).unwrap();
            assert!(rest.is_empty());
            rg.advance(pkt.len());
            assert_eq!(cmd.code, CmdCode::Query);
        });
    }

    #[test]
    fn test_recv_rows() {
        let data = vec![2u8, 0, 0, 0, 3, 48, 2, 0, 0, 1, 3, 49];
        let cursor = Cursor::new(data);
        let mut conn = MyConn {
            id: 0,
            conn: cursor,
            ctx: SerdeCtx::default(),
            write_buf: Vec::with_capacity(1024),
        };
        block_on(async {
            let buf = ByteBuffer::with_capacity(32);
            let mut rows = vec![];
            let (pkt, rg) = conn.recv(&buf, false).await.unwrap();
            rg.advance(pkt.len());
            rows.push(pkt);
            let (pkt, rg) = conn.recv(&buf, false).await.unwrap();
            rg.advance(pkt.len());
            rows.push(pkt);
            buf.single_read().unwrap();
        });
    }

    #[test]
    fn test_mysql_recv_split_packet_large_buffer() {
        let data = vec![
            4u8, 0, 0, 0, 1, 2, 3, 4, 4u8, 0, 0, 1, 5, 6, 7, 8, 2u8, 0, 0, 2, 9, 0,
        ];
        let cursor = Cursor::new(data);
        let mut ctx = SerdeCtx::default();
        ctx.set_max_payload_size(4);
        let mut conn = MyConn {
            id: 0,
            conn: cursor,
            ctx,
            write_buf: Vec::with_capacity(1024),
        };
        block_on(async {
            let buf = ByteBuffer::with_capacity(32);
            let (payload, rg) = conn.recv(&buf, false).await.unwrap();
            assert_eq!(&*payload, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]);
            rg.advance(payload.len());
        });
    }

    #[test]
    fn test_mysql_recv_split_packet_small_buffer() {
        for buf_size in [6, 10, 14] {
            let data = vec![
                4u8, 0, 0, 0, 1, 2, 3, 4, 4u8, 0, 0, 1, 5, 6, 7, 8, 2u8, 0, 0, 2, 9, 0,
            ];
            let cursor = Cursor::new(data);
            let mut ctx = SerdeCtx::default();
            ctx.set_max_payload_size(4);
            let mut conn = MyConn {
                id: 0,
                conn: cursor,
                ctx,
                write_buf: Vec::with_capacity(1024),
            };
            block_on(async {
                let buf = ByteBuffer::with_capacity(buf_size);
                let (payload, rg) = conn.recv(&buf, false).await.unwrap();
                assert_eq!(&*payload, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]);
                rg.advance(payload.len());
            });
        }
    }

    #[test]
    fn test_mysql_recv_buffer_full() {
        let data = vec![
            16u8, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8,
        ];
        let cursor = Cursor::new(data);
        let ctx = SerdeCtx::default();
        let mut conn = MyConn {
            id: 0,
            conn: cursor,
            ctx,
            write_buf: Vec::with_capacity(1024),
        };
        block_on(async {
            let buf = ByteBuffer::with_capacity(6);
            let (payload, rg) = conn.recv(&buf, false).await.unwrap();
            assert_eq!(&*payload, &[1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8]);
            rg.advance(payload.len());
        });
    }

    #[test]
    fn test_mysql_query() {
        block_on(async {
            let buf = ByteBuffer::with_capacity(1024);
            let conn = new_conn(&buf).await.unwrap();
            let (col_defs, rg, mut rs) = conn
                .query(ComQuery::new_ref("select 1"), &buf)
                .await
                .unwrap();
            dbg!(col_defs);
            drop(rg);
            let mut row_cnt = 0;
            loop {
                let (rows, rg) = rs.next(&buf).await.unwrap();
                match rows {
                    Rows {
                        kind:
                            RowsKind::Cont {
                                stream,
                                next_buf_size,
                            },
                        data,
                    } => {
                        row_cnt += data.len();
                        if let Some(next_buf_size) = next_buf_size {
                            buf.update().unwrap().reserve(next_buf_size);
                        }
                        drop(rg);
                        rs = stream;
                    }
                    Rows {
                        kind: RowsKind::End { .. },
                        data,
                    } => {
                        row_cnt += data.len();
                        drop(rg);
                        break;
                    }
                }
            }
            println!("row count {}", row_cnt);
        })
    }

    #[test]
    fn test_mysql_exec_and_field_list() {
        block_on(async {
            let buf = ByteBuffer::with_capacity(1024);
            let mut conn = new_conn(&buf).await.unwrap();
            let (resp, rg) = conn
                .exec("create database if not exists db1", &buf)
                .await
                .unwrap();
            dbg!(resp);
            drop(rg);
            assert!(unsafe { buf.readable_unchecked().is_empty() });
            let (resp, rg) = conn.exec("use db1", &buf).await.unwrap();
            dbg!(resp);
            drop(rg);
            let (resp, rg) = conn
                .exec(
                    "create table if not exists t1 (c0 int not null default 1, c1 varchar(20))",
                    &buf,
                )
                .await
                .unwrap();
            dbg!(resp);
            drop(rg);
            let (col_defs, rg) = conn.field_list(("t1", "%"), &buf).await.unwrap();
            dbg!(col_defs);
            drop(rg);
        })
    }

    #[test]
    fn test_mysql_small_write_buf() {
        block_on(async {
            let buf = ByteBuffer::with_capacity(1024);
            let mut conn = TcpOpts::new("127.0.0.1:13306")
                .username("root")
                .password("password")
                .write_buf_size(4)
                .connect(&buf, None)
                .await
                .unwrap();
            let (resp, rg) = conn
                .exec("create database if not exists db1", &buf)
                .await
                .unwrap();
            dbg!(resp);
            drop(rg);
        })
    }

    #[test]
    fn test_mysql_connect_non_existing_db() {
        block_on(async {
            let buf = ByteBuffer::with_capacity(1024);
            let conn = TcpOpts::new("127.0.0.1:13306")
                .username("root")
                .password("password")
                .database("non_existing_database")
                .connect(&buf, None)
                .await;
            assert!(conn.is_err());
            dbg!(conn.unwrap_err());
        })
    }

    #[test]
    fn test_mysql_connect_default_plugin() {
        block_on(async {
            let buf = ByteBuffer::with_capacity(1024);
            let _ = new_conn(&buf).await.unwrap();
        })
    }

    async fn new_conn(buf: &ByteBuffer) -> Result<MyConn<TcpStream>> {
        TcpOpts::new("127.0.0.1:13306")
            .username("root")
            .password("password")
            .connect(&buf, None)
            .await
    }
}
