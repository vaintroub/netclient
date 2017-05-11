using System;
using System.Collections.Generic;
using System.Collections;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MySQLClient
{

    public enum ServerResponseType
    {
        Ok,
        Error,
        ResultSet,
        AuthSwitch
    }

    class MySQLException : DbException
    {
        public readonly string SQLState;

        public MySQLException(string message) : base(message)
        {
        }

        public MySQLException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public MySQLException(string message, int errorCode, string sqlState) : base(message, errorCode)
        {
            SQLState = sqlState;
        }
    }

    class PacketReader
    {
        static readonly int MAX_SIZE = 0xffffff;
        static readonly int BUFSIZ = 512;
        public Stream stream;
        public int seqNo;
        public int size;

        internal int pos;

        byte[] header = new byte[4];
        byte[] byteBuf = new byte[BUFSIZ];
        int peekedByte = -1;

        public PacketReader(Stream s)
        {
            stream = s;
        }

        public PacketReader()
        {
        }

        private bool IsSplitPacket()
        {
            return size == MAX_SIZE;
        }
        internal int Remaining
        {
            get { return size - pos; }
        }

        private void ReadFully(Stream s, byte[] buffer, int offset, int count)
        {
            while (count > 0)
            {
                int n = s.Read(buffer, offset, count);
                if (n <= 0)
                    throw new EndOfStreamException("Read of " + count + "bytes returned " +n);
                count -= n;
                offset += n;
            }
        }

        private async Task ReadFullyAsync(Stream s, byte[] buffer, int offset, int count)
        {
            while (count > 0)
            {
                int n = await s.ReadAsync(buffer, offset, count);
                if (n <= 0)
                    throw new EndOfStreamException();
                count -= n;
                offset += n;
            }
        }

        private void SetSizeAndSeqNo()
        {
            size = (((int)header[0])) | (((int)header[1]) << 8) | (((int)header[2]) << 16);
            seqNo = header[3];
            pos = 0;
        }

        public void ReadPacketHeader()
        {
            ReadFully(stream, header, 0, 4);
            SetSizeAndSeqNo();
        }

        public async Task ReadPacketHeaderAsync()
        {
            await ReadFullyAsync(stream, header, 0, 4);
            SetSizeAndSeqNo();
        }

        private void ReadCheck()
        {
            if ((pos == size && IsSplitPacket()) || size == 0)
            {
                ReadPacketHeader();
            }
            else if (pos == size)
            {
                throw new InvalidOperationException("No more data in packet");
            }
        }

        public int PeekByte()
        {
            if (peekedByte > 0)
                return peekedByte;

            peekedByte = ReadByte();
            Debug.Assert(pos > 0);
            pos--;
            Debug.Assert(peekedByte > 0);
            return peekedByte;
        }

        public int ReadByte()
        {
            int ret;
            if (peekedByte > 0)
            {
                ret = peekedByte;
                peekedByte = -1;
                pos++;
                return ret;
            }
            ReadCheck();
            ret = stream.ReadByte();
            if (ret >= 0)
                pos++;
            else
                throw new EndOfStreamException();
            return ret;
        }

        public string ReadStringNUL()
        {
            int b;
            int bufPos = 0;
            while ((b = ReadByte()) > 0)
            {
                byteBuf[bufPos++] = (byte)b;
                if (pos == BUFSIZ)
                {
                    throw new InternalBufferOverflowException("string too large for he buffer");
                }
            }
            return Encoding.UTF8.GetString(byteBuf, 0, bufPos);
        }

        public string ReadStringEOF()
        {
            byte[] b = new byte[Remaining];
            ReadFully(b, 0, b.Length);
            return Encoding.UTF8.GetString(b);
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            if (peekedByte > 0)
            {
                buffer[offset] = (byte)ReadByte();
                return 1;
            }

            ReadCheck();
            int ret = stream.Read(buffer, offset, Math.Min(count, Remaining));
            if (ret > 0)
                pos += ret;
            else
                throw new EndOfStreamException();
            return ret;
        }



        public void ReadFully(byte[] buffer, int offset, int count)
        {
            while (count > 0)
            {
                int n = Read(buffer, offset, count);
                if (n <= 0)
                    throw new EndOfStreamException();
                count -= n;
                offset += n;
            }
        }


        public void Skip(int count)
        {
            if (count > Remaining)
                throw new EndOfStreamException();

            if (stream.CanSeek)
            {
                stream.Seek(count, SeekOrigin.Current);
            }
            else
            {
                while (count > 0)
                    count -= Read(byteBuf, 0, Math.Min(count, BUFSIZ));
            }
        }

        public void Rewind()
        {
            if (stream.CanSeek)
            {
                stream.Seek(0, SeekOrigin.Begin);
                pos = 0;
            }
            else
            {
                throw new InvalidOperationException("Non-sequential access detected");
            }
        }

        /// Skip remaining bytes in this packet
        public void Skip()
        {
            Skip(Remaining);
            /* Handle large (i.e split) packets */
            while (IsSplitPacket())
            {
                ReadPacketHeader();
                Skip(Remaining);
            }
        }

        public int ReadInt1()
        {
            return ReadByte();
        }

        public int ReadInt2()
        {
            int n1 = ReadInt1();
            int n2 = ReadInt1();
            return n1 + (n2 << 8);
        }

        public int ReadInt3()
        {
            int n1 = ReadInt1();
            int n2 = ReadInt1();
            int n3 = ReadInt1();
            return n1 + (n2 << 8) + (n3 << 16);
        }

        public int ReadInt4()
        {
            int n1 = ReadInt1();
            int n2 = ReadInt1();
            int n3 = ReadInt1();
            int n4 = ReadInt1();
            return n1 + (n2 << 8) + (n3 << 16) + (n4 << 24);
        }

        public long ReadInt8()
        {
            long low = ReadInt4();
            long high = ReadInt4();
            return low + (high << 32);
        }

        public const long NULL_VALUE_LENENC = -1;
        public long ReadIntLenenc()
        {
            int b = ReadInt1();
            if (b < 0xfb)
                return b;
            switch (b)
            {
                case 0xfc:
                    return ReadInt2();
                case 0xfd:
                    return ReadInt3();
                case 0xfe:
                    return ReadInt8();
                case 0xfb:
                    return -1; /* NULL indicator */
                default:
                    throw new InvalidDataException();
            }
        }
    }

    class PacketWriter : IDisposable
    {
        public Stream stream;
        MemoryStream mem = new MemoryStream(512);
        public PacketWriter()
        {
            mem.Position = 4;
        }
        public int seqNo;
        public int DataLength()
        {
            return (int)mem.Position - 4;
        }


        void PrepareSend()
        {
            int len = DataLength();
            mem.SetLength(mem.Position);
            mem.Seek(0, SeekOrigin.Begin);
            mem.WriteByte((byte)(len & 0xff));
            mem.WriteByte((byte)((len >> 8) & 0xff));
            mem.WriteByte((byte)((len >> 16) & 0xff));
            mem.WriteByte((byte)(seqNo & 0xff));
            mem.Position = 0;
        }
        void PostSend(bool resetSeqno = true)
        {
            mem.Position = 4;
            if (resetSeqno)
                seqNo = 0;
            else
                seqNo++;
        }

        public void SendPacket(bool resetSeqno = true)
        {
            PrepareSend();
            mem.WriteTo(stream);
            PostSend(resetSeqno);
        }

        public async Task SendPacketAsync(bool resetSeqno = true)
        {
            PrepareSend();
            await mem.CopyToAsync(stream);
            PostSend(resetSeqno);
        }

        public void WriteByte(byte b)
        {
            mem.WriteByte(b);
            if (DataLength() == 0xFFFFFF)
            {
                SendPacket(false);
            }
        }
        public void WriteString(string s)
        {
            byte[] buf = Encoding.UTF8.GetBytes(s);
            WriteBytes(buf, 0, buf.Length);
        }

        public void WriteStringNUL(string s)
        {
            WriteString(s);
            WriteByte(0);
        }

        public void WriteBytes(byte[] buffer, int offset, int count)
        {
            while (count > 0)
            {
                int toSend = Math.Min(count, 0xFFFFFF - DataLength());
                mem.Write(buffer, offset, toSend);
                if (DataLength() == 0xFFFFFF)
                {
                    SendPacket(false);
                }
                count -= toSend;
                offset += toSend;
            }
        }

        public void WriteInt2(short n)
        {
            WriteByte((byte)(n & 0xff));
            WriteByte((byte)((n >> 8) & 0xff));
        }

        public void WriteInt3(int n)
        {
            WriteByte((byte)(n & 0xff));
            WriteByte((byte)((n >> 8) & 0xff));
            WriteByte((byte)((n >> 16) & 0xff));
        }

        public void WriteInt4(int n)
        {
            WriteByte((byte)(n & 0xff));
            WriteByte((byte)((n >> 8) & 0xff));
            WriteByte((byte)((n >> 16) & 0xff));
            WriteByte((byte)((n >> 24) & 0xff));
        }

        public void Dispose()
        {
            mem.Dispose();
        }
    }
    public struct ClientParams
    {
        public string user;
        public string password;
        public string host;
        public int port;
        public string database;
    }


    public class Client:IDisposable
    {
        TcpClient tcpClient = new TcpClient();
        PacketReader reader = new PacketReader();
        PacketWriter writer = new PacketWriter();
        ValueStream valueStream = new ValueStream();
        Row row = new Row();
        bool inBatchMode;

        public bool IsClosed
        {
            get; private set;
        } = true;

        public ServerStatusFlags ServerStatus { get => serverStatusFlags; private set => serverStatusFlags = value; }

        int fieldCount;
        string serverVersion;
        int connectionId;
        CapabilityFlags serverCaps;

        long affectedRows;
        long lastInsertId;
        ServerStatusFlags serverStatusFlags;
        short warnings;
        MemoryStream batchStream = new MemoryStream();

        public void StartBatch()
        {
            inBatchMode = true;
            writer.stream = batchStream;
            batchStream.Position = 0;
        }

        public void FlushBatch()
        {
            Debug.Assert(inBatchMode);
            batchStream.SetLength(writer.stream.Position);
            writer.stream.Position = 0;
            writer.stream.CopyTo(tcpClient.GetStream());
            writer.stream = tcpClient.GetStream();
            inBatchMode = false;
        }

        void NotInBatchMode()
        {
            if (inBatchMode)
                throw new MySQLException("Operation disallowed in batch mode. Only Send() APIs are allowed until EndBatch()");
        }
        void ReadPacketHeader()
        {
            NotInBatchMode();
            try
            {
                reader.ReadPacketHeader();
            }
            catch (IOException e)
            {
                HandleIOException(e, true);
            }
        }

        async Task ReadPacketHeaderAsync()
        {
            NotInBatchMode();
            try
            {
                await reader.ReadPacketHeaderAsync();
            }
            catch (IOException e)
            {
                HandleIOException(e, true);
            }
        }

        void SendPacket()
        {
            try
            {
                writer.SendPacket();
            }
            catch (IOException e)
            {
                HandleIOException(e, false);
            }
        }

        async Task SendPacketAsync()
        {
            try
            {
                await writer.SendPacketAsync();
            }
            catch (IOException e)
            {
                HandleIOException(e, false);
            }
        }


        void HandleIOException(IOException e, bool isRead)
        {
            IsClosed = true;
            tcpClient.Close();
            throw new MySQLException(e.Message, 2013, "HY000");
        }

        byte[] ParseInitialAuthPacket()
        {
            int protocolVersion = reader.ReadInt1();
            serverVersion = reader.ReadStringNUL();
            connectionId = reader.ReadInt4();
            byte[] scramble = new byte[20];
            reader.ReadFully(scramble, 0, 8);

            reader.ReadInt1();
            int serverCapsLow = reader.ReadInt2();
            int collation = reader.ReadInt1();
            int statusFlags = reader.ReadInt2();
            int serverCapsHigh = reader.ReadInt2();
            serverCaps = (CapabilityFlags)(serverCapsLow + (serverCapsHigh << 16));

            int pluginDataLength = reader.ReadInt1();
            reader.Skip(10);

            if (serverCaps.HasFlag(CapabilityFlags.CLIENT_SECURE_CONNECTION))
            {
                reader.ReadFully(scramble, 8, Math.Max(12, pluginDataLength - 9));
            }
            reader.Skip(1);

            string authPluginName = "";
            if (serverCaps.HasFlag(CapabilityFlags.CLIENT_PLUGIN_AUTH))
            {
                authPluginName = reader.ReadStringNUL();
            }
            return scramble;
        }

        DbException ErrorPacketToException()
        {
            int code = reader.ReadInt2();
            int b = reader.ReadByte();
            string sqlState = "";
            if (b == '#')
            {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 5; i++)
                    sb.Append((char)reader.ReadByte());
                sqlState = sb.ToString();
            }
            string msg = reader.ReadStringEOF();
            return new MySQLException(msg, code, sqlState);
        }

        void ParseOK()
        {
            affectedRows = reader.ReadIntLenenc();
            lastInsertId = reader.ReadIntLenenc();
            ServerStatus = (ServerStatusFlags)reader.ReadInt2();
            warnings = (short)reader.ReadInt2();
            reader.Skip();
        }

        // Read first packet of the server response.
        // It is either OK or ERROR packet, or possibly AUTH switch
        // during login, or FieldCount for following result set.
        ServerResponseType ReadFieldCount()
        {
            int b = reader.ReadByte();
            switch (b)
            {
                case 0: // OK
                    ParseOK();
                    return ServerResponseType.Ok;
                case 0xff:
                    DbException e = ErrorPacketToException();
                    throw e;
                case 0xfe:
                    // return ServerResponseType.AuthSwitch;
                    throw new NotImplementedException();
                default:
                    fieldCount = b;
                    return ServerResponseType.ResultSet;
            }
        }

        public ServerResponseType ReceiveServerResponse()
        {
            ReadPacketHeader();
            return ReadFieldCount();
        }

        void ReadEOF()
        {
            warnings = (short)reader.ReadInt2();
            ServerStatus = (ServerStatusFlags)reader.ReadInt2();
            reader.Skip();
        }
 
        private bool HasMoreRows()
        {
            int b = reader.PeekByte();
            switch (b)
            {
                case 0xfe:
                    if (reader.size < 8)
                    {
                        reader.ReadByte();
                        ReadEOF();
                        return false;
                    }
                    break;
                case 0xff:
                    reader.ReadByte();
                    DbException e = ErrorPacketToException();
                    throw e;
            }
            return true;
        }
        public Row NextRow(RowAccessType type)
        {
            reader.Skip();
            ReadPacketHeader();
            if (HasMoreRows())
            {
                row.Init(reader, type);
                return row;
            }
            return null;
        }
        public async Task<Row> NextRowAsync(RowAccessType type)
        {
            reader.Skip();
            await ReadPacketHeaderAsync();
            if (HasMoreRows())
            {
                row.Init(reader, type);
                return row;
            }
            return null;
        }

        public string ReadString()
        {
            long len = reader.ReadIntLenenc();
            if (len == PacketReader.NULL_VALUE_LENENC)
                return null;
            valueStream.Init(reader);
            return TextEncodedValue.GetString(valueStream);
        }

        public async Task<ServerResponseType> ReceiveServerResponseAsync()
        {
            await ReadPacketHeaderAsync();

            return ReadFieldCount();
        }

        // Send And Receive
        void Transact()
        {
            writer.seqNo = reader.seqNo + 1;
            SendPacket();
            ReadPacketHeader();
        }
        async Task TransactAsync()
        {
            writer.seqNo = reader.seqNo + 1;
            await SendPacketAsync();
            await ReadPacketHeaderAsync();
        }

        void SetupTCPParams()
        {
            reader.stream = new BufferedStream(tcpClient.GetStream());
            writer.stream = tcpClient.GetStream();
            tcpClient.Client.NoDelay = true;
        }

        void ProcessInitialAuthPacket(ClientParams p)
        {
            byte[] scramble = ParseInitialAuthPacket();
            PrepareAuthResponse(p.user, p.password, p.database, scramble);
            writer.seqNo = reader.seqNo + 1;
        }

        public void Connect(ClientParams p)
        {
            tcpClient.Connect(p.host, p.port);
            SetupTCPParams();

            ReadPacketHeader();
            ProcessInitialAuthPacket(p);
            SendPacket();

            ReceiveServerResponse();
        }

        public async Task ConnectAsync(ClientParams p)
        {
            await tcpClient.ConnectAsync(p.host, p.port);
            SetupTCPParams();

            ReadPacketHeader();
            ProcessInitialAuthPacket(p);
            SendPacket();

            ReceiveServerResponse();
        }

        public void Disconnect()
        {
            writer.WriteByte((byte)ServerCommand.COM_QUIT);
            SendPacket();
            tcpClient.Close();
        }

        public async Task DisconnectAsync()
        {
            writer.WriteByte((byte)ServerCommand.COM_QUIT);
            await SendPacketAsync();
            tcpClient.Close();
        }

        public void Ping()
        {
            SendPing();
            ReceiveServerResponse();
        }

        public void SendPing()
        {
            writer.seqNo = 0;
            writer.WriteByte((byte)ServerCommand.COM_PING);
            SendPacket();
        }

        public void SendQuery(String query)
        {
            writer.seqNo = 0;
            writer.WriteByte((byte)ServerCommand.COM_QUERY);
            writer.WriteString(query);
            SendPacket();
        }

        public  void ReadResultSetMetaData(ResultSetMetaData metadata)
        {
            metadata.Read(reader, this.fieldCount, false);
        }

        public void ReadOk()
        {
            ServerResponseType responseType = ReceiveServerResponse();
            if (responseType != ServerResponseType.Ok)
            {
                throw new MySQLException("Unexpected response type " + responseType);
            }
        }
        public void SkipResultSetMetadata(ResultSetMetaData metadata)
        {
            metadata.Read(reader, this.fieldCount, true);
        }

        public async Task PingAsync()
        {
            SendPing();
            await ReceiveServerResponseAsync();
        }

        byte[] passwordHash(byte[] password, byte[] seed)
        {
            // if we have no password, then we just return 1 zero byte

            if (password.Length == 0) return null;

            SHA1 sha = SHA1.Create();

            byte[] h = sha.ComputeHash(password);

            byte[] buf = new byte[40];

            seed.CopyTo(buf, 0);
            sha.ComputeHash(h).CopyTo(buf, 20);

            byte[] h2 = sha.ComputeHash(buf);

            for (int i = 0; i < 20; i++)
                h[i] ^= h2[i];
            return h;
        }

        private void PrepareAuthResponse(string user, string password, string database, byte[] scramble)
        {
            CapabilityFlags clientFlags =
            CapabilityFlags.CLIENT_LONG_PASSWORD | CapabilityFlags.CLIENT_FOUND_ROWS | CapabilityFlags.CLIENT_LONG_FLAG |
            CapabilityFlags.CLIENT_TRANSACTIONS | CapabilityFlags.CLIENT_SECURE_CONNECTION | CapabilityFlags.CLIENT_PROTOCOL_41 |
            CapabilityFlags.CLIENT_MULTI_STATEMENTS | CapabilityFlags.CLIENT_MULTI_RESULTS | CapabilityFlags.CLIENT_PS_MULTI_RESULTS |
            CapabilityFlags.CLIENT_PLUGIN_AUTH;

            if (!string.IsNullOrEmpty(database))
            {
                clientFlags |= CapabilityFlags.CLIENT_CONNECT_WITH_DB;
            }

            clientFlags &= serverCaps;

            writer.WriteInt4((int)clientFlags);
            writer.WriteInt4(Int32.MaxValue); // max packet length
            writer.WriteByte(45); // utf8mb4_general_ci
            for (int i = 0; i < 23; i++)
            {
                writer.WriteByte(0);
            }

            writer.WriteStringNUL(user);
            if (string.IsNullOrEmpty(password))
            {
                writer.WriteByte(0);
            }
            else
            {
                byte[] hash = passwordHash(Encoding.UTF8.GetBytes(password), scramble);
                writer.WriteByte((byte)hash.Length);
                writer.WriteBytes(hash, 0, hash.Length);
            }

            if (clientFlags.HasFlag(CapabilityFlags.CLIENT_CONNECT_WITH_DB))
            {
                writer.WriteStringNUL(database);
            }

            if (clientFlags.HasFlag(CapabilityFlags.CLIENT_PLUGIN_AUTH))
            {
                writer.WriteStringNUL("mysql_native_password");
            }
        }

        public void Dispose()
        {
            tcpClient.Close();
            batchStream.Close();
        }
    }
}
