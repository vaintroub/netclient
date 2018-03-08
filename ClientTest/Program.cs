using MySQLClient;
using System;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace ClientTest
{
    class Program
    {
        static volatile bool Stop = false;
        static async Task<long> timerTask(int sec)
        {
            await Task.Delay(1000 * sec);
            Stop = true;
            return 0;
        }
        static async Task<long> oneClientTaskAsync()
        {
            Client c = new MySQLClient.Client();
            ClientParams cp = new ClientParams();
            cp.host = "127.0.0.1";
            cp.password = null;
            cp.user = "root";
            cp.port = 3306;

            await c.ConnectAsync(cp);
            long count;

            int BATCH_SIZE = 200;
            for (count = 0; !Stop; count += BATCH_SIZE)
            {
                c.StartBatch();
                for (int i = 0; i < BATCH_SIZE; i++)
                {
                    c.SendPing();
                }
                c.FlushBatch();

                for (int i = 0; i < BATCH_SIZE; i++)
                {
                    await c.ReceiveServerResponseAsync();
                }
            }
            await c.DisconnectAsync();

            return count;
        }

        static async Task<long>  select1Async()
        {
            Client c = new MySQLClient.Client();
            ClientParams cp = new ClientParams();
            cp.host = "localhost";
            cp.password = "";
            cp.user = "root";
            cp.port = 3306;
            await c.ConnectAsync(cp);
            long count = 0;
            int BATCH_SIZE = 200;
            for (count = 0; !Stop; count+=BATCH_SIZE)
            {
                c.StartBatch();
                for (int i = 0; i < BATCH_SIZE; i++)
                {
                    c.SendQuery("do 1");
                }
                c.FlushBatch();

                for (int i = 0; i < BATCH_SIZE; i++)
                {
                    ServerResponseType responseType = await c.ReceiveServerResponseAsync();
                    if (responseType == ServerResponseType.ResultSet)
                    {
                        ResultSetMetaData metadata = new ResultSetMetaData();
                        c.ReadResultSetMetaData(metadata);
                        int N = metadata.columnDefinitions.Count;
                        while (await c.NextRowAsync(RowAccessType.Sequential) != null)
                        {
                            //string s = c.ReadString();
                        }
                    }
                }
            }
            await c.DisconnectAsync();
            return count;
        }

        static void Main(string[] args)
        {
            /*int clientCount = 300;
            Task<long>[] tasks =new Task<long>[clientCount+1];
            for (int i = 0; i < clientCount; i++)
                tasks[i] =  select1Async();
            tasks[clientCount] = timerTask(60);
            Task.WhenAll(tasks).Wait();
            long sum = 0;
            for (int i = 0; i < clientCount; i++)
                sum += tasks[i].Result;
            Console.WriteLine(sum);
            */


            Client c = new MySQLClient.Client();
            ClientParams cp = new ClientParams();
            cp.host = "localhost";
            cp.password = "";
            cp.user = "root";
            cp.port = 3306;
            cp.pipe = "MySQL";
            c.Connect(cp);
            c.SendQuery("set net_write_timeout=99999");
            ServerResponseType responseType = c.ReceiveServerResponse();
            Debug.Assert(responseType == ServerResponseType.Ok);
            c.SendQuery("use test");
            responseType = c.ReceiveServerResponse();
            Debug.Assert(responseType == ServerResponseType.Ok);

            byte[] query = Encoding.UTF8.GetBytes("select 1");
            ResultSetMetaData rsmd = new ResultSetMetaData();
            Row r;

            Stopwatch sw = Stopwatch.StartNew();
            for (int i = 0; i < 1000000; i++)
            {
                c.SendQuery(query);
                if (c.ReceiveServerResponse() == ServerResponseType.Ok)
                    continue;
                c.ReadResultSetMetaData(rsmd);

                while ((r = c.NextRow(RowAccessType.Sequential)) != null)
                {
                    System.IO.Stream s = r.GetValue(0);
                    s.Close();
                }

            }
            Console.WriteLine(sw.Elapsed);
            c.Disconnect();
        }
    }
}
