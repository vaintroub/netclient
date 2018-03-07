using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MySQLClient
{
    class ColumnDefinitionData
    {
        public ArraySegment<byte> schema;
        public ArraySegment<byte> table;
        public ArraySegment<byte> originalTable;
        public ArraySegment<byte> name;
        public ArraySegment<byte> originalName;
        public int charset;
        public int columnLength;
        public DataType type;
        public FieldFlags flags;
        public int decimals;

        private static ArraySegment<byte> ReadStringLenEnc(byte[] bytes, ref int pos)
        {
            int len = bytes[pos];
            pos++;
            ArraySegment<byte> val = new ArraySegment<byte>(bytes, pos, len);
            pos += len;
            return val;
        }

        public ColumnDefinitionData(byte[] bytes, int start, int size)
        {
            int pos = start;
            pos += bytes[pos] + 1; // skip catalog

            schema = ReadStringLenEnc(bytes, ref pos);
            table = ReadStringLenEnc(bytes, ref pos);
            originalTable = ReadStringLenEnc(bytes, ref pos);
            name = ReadStringLenEnc(bytes, ref pos);
            originalName = ReadStringLenEnc(bytes, ref pos);
            pos++;

            charset = BitConverter.ToInt16(bytes, pos);
            pos += 2;

            columnLength = BitConverter.ToInt32(bytes, pos);
            pos += 4;

            type = (DataType)bytes[pos++];

            flags = (FieldFlags)BitConverter.ToInt16(bytes, pos);
            pos += 2;

            decimals = bytes[pos];
            Debug.Assert(pos <= size);
        }
    }


    public struct ColumnDefinition
    {
        MemoryStream memstream;
        int start;
        int size;
        ColumnDefinitionData data;
        public ColumnDefinition(MemoryStream stream, int start, int size)
        {
            this.memstream = stream;
            this.start = start;
            this.size = size;
            data = null;
        }
        private ColumnDefinitionData Data
        {
            get
            {
                if (data == null)
                    data = new ColumnDefinitionData(memstream.GetBuffer(), start, size);
                return data;
            }
        }
        private String ArraySegmentToString(ArraySegment<byte> a)
        {
            return Encoding.UTF8.GetString(a.Array, a.Offset, a.Count);
        }
        public string Schema { get { return ArraySegmentToString(Data.schema); } }
        public string Table { get { return ArraySegmentToString(Data.table); } }
        public string OriginalTable { get { return ArraySegmentToString(Data.originalTable); } }
        public string Name { get { return ArraySegmentToString(Data.name); } }
        public string OriginalName { get { return ArraySegmentToString(Data.originalName); } }
        public int Charset { get { return Data.charset; } }
        public int ColumnLength { get { return Data.columnLength; } }
        public DataType Type { get { return Data.type; } }
        public FieldFlags Flags { get { return Data.flags; } }
    }

    public class ResultSetMetaData
    {
        public List<ColumnDefinition> columnDefinitions = new List<ColumnDefinition>();
        MemoryStream memStream = new MemoryStream();
        byte[] copyBuf = new byte[256];

        internal void Read(PacketReader reader, int N, bool ignoreMetadata)
        {
            memStream.Position = 0;
            memStream.SetLength(0);
            columnDefinitions.Clear();
            for (int i = 0; i < N; i++)
            {
                reader.ReadPacketHeader();
                if (ignoreMetadata)
                {
                    reader.Skip();
                    continue;
                }

                columnDefinitions.Add(new ColumnDefinition(memStream, (int)memStream.Position, reader.size));
                int remaining = reader.size;
                while (remaining > 0)
                {
                    int r = reader.Read(copyBuf, 0, Math.Min(copyBuf.Length, remaining));
                    if (r <= 0)
                        throw new EndOfStreamException();
                    memStream.Write(copyBuf, 0, r);
                    remaining -= r;
                }
            }
            // Strip off EOF packet
            reader.ReadPacketHeader();
            Debug.Assert(reader.ReadByte() == 0xfe);
            reader.Skip();
        }
    }
}
