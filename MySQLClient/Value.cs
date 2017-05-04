
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
        int position;
        PacketReader reader;
        public ValueStream(PacketReader r, long length)
        {
            this.length = length;
            position = 0;
            reader = r;
        }
        public override bool CanRead
        {
            get
            {
                return true;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return false;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return false;
            }
        }

        public override long Length
        {
            get
            {
                return length;
            }
        }

        public override long Position
        {
            get
            {
                return position;
            }

            set
            {
                throw new NotImplementedException();
            }
        }

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
        }
        public override int ReadByte()
        {
            if (length == position)
                return -1;
            int ret = reader.ReadByte();
            position++;
            return ret;
        }
    }

    internal class TextEncodedValue
    {
        public static Stream GetStream(PacketReader r, long len)
        {
            return new ValueStream(r, len);
        }

        public static TextReader GetReader(PacketReader r, long len)
        {
            return new StreamReader(GetStream(r,len), Encoding.UTF8);
        }

        public static long GetLong(PacketReader r, long len)
        {
            int firstByte = r.ReadByte();

            long sign = (firstByte == '-')?-1:1;
            long val = (firstByte == '-') ? 0 : firstByte;
            for (int i = 0; i < len - 1; i++)
            {
                int digit = r.ReadByte() - '0';
                Debug.Assert(digit >= 0 && digit <= 9);
                val = 10 * val + sign * digit;
            }
            return val;
        }

        public static ulong GetULong(PacketReader r, long len)
        {
            ulong val = 0;
            for (int i = 0; i < len - 1; i++)
            {
                int digit = r.ReadByte() - '0';
                Debug.Assert(digit >= 0 && digit <= 9);
                val = 10 * val + (ulong)digit;
            }
            return val;
        }
        
        public static byte[] GetBytes(PacketReader r, long len)
        {
            byte[] b = new byte[len];
            r.ReadFully(b, 0, (int)len);
            return b;
        }

        public static string GetString(PacketReader r, long len)
        {
            return Encoding.UTF8.GetString(GetBytes(r, len));
        }

    }
}
