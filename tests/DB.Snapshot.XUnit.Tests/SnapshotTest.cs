namespace DB.Snapshot.XUnit.Tests
{
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using Dapper;
    using Shouldly;
    using Xunit;

    public class SnapshotTest : DBTest
    {
        private const string Id = "6ed09e1c-675c-47e2-b6c2-6b9c3cc29345";
        private const string ConnectionString = "Data Source=.\\SQLEXPRESS;Initial Catalog=Test;Integrated Security=True;";
        public SnapshotTest() 
            : base(
                "test", 
                "C:\\test\\snapshotDb", 
                "SnapshotTest", 
                ConnectionString
            )
        {
        }

        //It is a little bit tricky but using a snapshot we should insert an element to a db, then snapshot
        //should be rollback to the previous state so the insert in next test should return an empty result
        [Theory(Skip = "Waiting to database")]
        [InlineData(1)]
        [InlineData(2)]
        public async Task AddRecordAndCheckIfItDoesntExistsThere(int _)
        {
            var select = $"SELECT TOP 1 * from [dbo].[Log] where CorrelationToken = '{Id}'";
            var insert = $"INSERT INTO [dbo].[Log] (CorrelationToken) VALUES ('{Id}')";

            var con = new SqlConnection(ConnectionString);
            await con.OpenAsync();
            var result = await con.ExecuteAsync(select);
            await con.ExecuteAsync(insert);
            
            con?.Close();
            
            result.ShouldBe(-1);
        }
    }
}