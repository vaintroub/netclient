
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MySQLClient
{
    class ValueStream : Stream
    {
        long length;
        long position;
        internal PacketReader reader;
        public ValueStream()
        {
        }
        public void Init(PacketReader r)
        {
            this.reader = r;
            length = reader.ReadIntLenenc();
            if (length == PacketReader.NULL_VALUE_LENENC)
            {
                IsDBNull = true;
                length = 0;
            }
            else
            {
                IsDBNull = false;
            }
            position = 0;
        }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => length;
        public override long Position
        { get => position; set => throw new InvalidOperationException(); }

        public bool IsDBNull { get; private set; }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (length == position)
                return 0;
            int ret = reader.Read(buffer, offset, Math.Min(count, (int)(length - position)));
            position += ret;
            return ret;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new InvalidOperationException();
        }

        public override void SetLength(long value)
        {
            throw new InvalidOperationException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new InvalidOperationException();
        }
        public override void Close()
        {
            reader.Skip();
            position = length;
        }
        public override int ReadByte()
        {
            if (length == position)
                return -1;
            int ret = reader.ReadByte();
            position++;
            return ret;
        }

        public void Skip()
        {
            reader.Skip((int)(length - position));
            position = length;
        }
    }

    public enum RowAccessType
    {
        Random,
        Sequential
    }

    public class Row
    {
        int currentPos;
        ValueStream valueStream = new ValueStream();
        PacketReader reader;

        internal  void Init(PacketReader r, RowAccessType type)
        {
            this.reader = r;

            // Copy packet into memory stream,for random access
            if (type == RowAccessType.Random)
            {
                byte[] buf = new byte[r.Remaining];
                r.ReadFully(buf, 0, buf.Length);
                this.reader = new PacketReader();
                reader.stream = new MemoryStream(buf);
                reader.size = buf.Length;
            }
            currentPos = 0;
        }


        public Stream GetValue(int pos)
        {
            if (valueStream.Length != valueStream.Position)
            {
                valueStream.Skip();
            }

            if (currentPos > pos)
            {
                /*
                 To lookup previous value (in non-sequential access case)
                 we go back to the start of the row, and skip to current position
                */
                reader.Rewind();
                currentPos = 0;
            }

            while (currentPos < pos)
            {
                valueStream.Init(reader);
                valueStream.Skip();
                currentPos++;
            }
            valueStream.Init(reader);
            currentPos++;
            if (valueStream.IsDBNull)
                return null;
            return valueStream;
        }
    }

    public class TextEncodedValue
    {
        public static TextReader GetReader(Stream stream)
        {
            return new StreamReader(stream, Encoding.UTF8);
        }

        public static long GetLong(Stream stream)
        {
            int firstByte = stream.ReadByte();

            long sign = (firstByte == '-')?-1:1;
            long val = (firstByte == '-') ? 0 : firstByte-'0';
            for (int i = 1; i < stream.Length; i++)
            {
                int digit = stream.ReadByte() - '0';
                Debug.Assert(digit >= 0 && digit <= 9);
                val = 10 * val + sign * digit;
            }
            return val;
        }

        public static ulong GetULong(Stream stream)
        {
            ulong val = 0;
            for (int i = 0; i < stream.Length; i++)
            {
                int digit = stream.ReadByte() - '0';
                Debug.Assert(digit >= 0 && digit <= 9);
                val = 10 * val + (ulong)digit;
            }
            return val;
        }
        
        public static byte[] GetBytes(Stream stream)
        {
            byte[] b = new byte[stream.Length- stream.Position];
            stream.Read(b, 0, b.Length);
            return b;
        }

        public static string GetString(Stream stream)
        {
            return Encoding.UTF8.GetString(GetBytes(stream));
        }
    }
}
