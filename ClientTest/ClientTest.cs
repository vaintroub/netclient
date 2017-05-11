using Xunit;
using MySQLClient;
using System;
using System.Collections.Generic;
using System.IO;

namespace MySQLClientTests
{
    public class ClientFixture : IDisposable
    {
        public ClientFixture()
        {

            ClientParams clientParams  =new ClientParams();
            clientParams.user = "root";
            clientParams.password = "";
            clientParams.port = 3306;
            clientParams.database = "test";
            clientParams.host = "localhost";
            Client = new Client();
            Client.Connect(clientParams);
        }

        public void Dispose()
        {
            Client.Disconnect();
        }

        public Client Client { get; private set; }
    }

    public class MyDatabaseTests : IClassFixture<ClientFixture>
    {
        ClientFixture fixture;
        Client client;
        public MyDatabaseTests(ClientFixture fixture)
        {
            this.fixture = fixture;
            this.client = fixture.Client;
        }

        [Fact]
        public void Ping()
        {
            client.Ping();
        }

        [Fact]
        public void SelectSequentialAccess()
        {
            client.SendQuery("select 1,2");
            Assert.Equal(client.ReceiveServerResponse(), ServerResponseType.ResultSet);
            ResultSetMetaData rsmd = new ResultSetMetaData();
            client.ReadResultSetMetaData(rsmd);
            List<ColumnDefinition> columnDefinitions = rsmd.columnDefinitions;
            Assert.Equal(columnDefinitions.Count, 2);
            Row r = client.NextRow(RowAccessType.Random);

            Stream s = r.GetValue(0);
            long l  =TextEncodedValue.GetLong(s);
            Assert.Equal(1, l);
            s = r.GetValue(1);
            l = TextEncodedValue.GetLong(s);
            Assert.Equal(2, l);
            Assert.Null(client.NextRow(RowAccessType.Random));
        }

        [Fact]
        public void SelectRandomAccess()
        {
            client.SendQuery("select 1,2");
            Assert.Equal(client.ReceiveServerResponse(), ServerResponseType.ResultSet);
            ResultSetMetaData rsmd = new ResultSetMetaData();
            client.ReadResultSetMetaData(rsmd);
            List<ColumnDefinition> columnDefinitions = rsmd.columnDefinitions;
            Assert.Equal(columnDefinitions.Count, 2);
            Row r = client.NextRow(RowAccessType.Random);
            Stream s;
            long l;
            for (int i = 0; i < 2; i++)
            {
                s = r.GetValue(0);
                l = TextEncodedValue.GetLong(s);
                Assert.Equal(1, l);
            }
            for (int i = 0; i < 2; i++)
            {
                s = r.GetValue(1);
                l = TextEncodedValue.GetLong(s);
                Assert.Equal(2, l);
            }
            s = r.GetValue(0);
            l = TextEncodedValue.GetLong(s);
            Assert.Equal(1, l);
            Assert.Null(client.NextRow(RowAccessType.Random));
        }
        [Fact]
        public void SemicolonBatch()
        {
            client.SendQuery("select 1; select 1");
            for (int i = 0; i < 2; i++)
            {
                Assert.Equal(client.ReceiveServerResponse(), ServerResponseType.ResultSet);
                ResultSetMetaData rsmd = new ResultSetMetaData();
                client.ReadResultSetMetaData(rsmd);
                Row r;
                while ((r = client.NextRow(RowAccessType.Sequential)) != null)
                {
                }
                if (i != 1)
                {
                    Assert.True(client.ServerStatus.HasFlag(ServerStatusFlags.SERVER_MORE_RESULTS_EXISTS));
                }
                else
                {
                    Assert.False(client.ServerStatus.HasFlag(ServerStatusFlags.SERVER_MORE_RESULTS_EXISTS));
                }
            }
        }
    }
}